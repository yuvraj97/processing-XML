import logging
import requests
import boto3
import pandas as pd
from io import BytesIO, StringIO
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from zipfile import ZipFile
from pandas.core.frame import DataFrame
from botocore.exceptions import ClientError
from cred import *

logging.basicConfig(
    filename="logging.log", 
    level=logging.INFO,
    format="%(levelname)s:%(funcName)s:%(lineno)d:%(message)s")

class FetchXML:
    
    """
    url: str
    s3Config: dict = {
        "ACCESS_KEY_ID": ACCESS_KEY_ID,
        "ACCESS_SECURITY_KEY": ACCESS_SECURITY_KEY,
        "BUCKET_NAME": BUCKET_NAME
    }

    It take a URL(str) and s3Config(dict) as input
    It fetches the link for zip files from the xml(url)
    And then extract that xml file out of the zip file
    Then it extracts the relevent data from every xml file
    And store that data in AWS S3 bucket in csv format

    """

    def __init__(self, url: str, s3Config: dict, testing=False) -> None:
        
        """
        url: str
        s3Config: dict = {
            "ACCESS_KEY_ID": ACCESS_KEY_ID,
            "ACCESS_SECURITY_KEY": ACCESS_SECURITY_KEY,
            "BUCKET_NAME": BUCKET_NAME
        }
        testing: bool : It should be True in unit testing
        """
        
        logging.info(f"Initializing FetchXML Object for url: {url}")

        self.bucket_name = s3Config["BUCKET_NAME"]
        self.download_links = []
        if not testing:
            self.s3 = self.__initialize_s3__(s3Config)
            self.__get_links__(url)
            self.__process__()

    @staticmethod
    def __initialize_s3__(s3Config: dict):
        
        """
        It initailizes the s3 resource for the relevent s3Config variable
        s3Config: dict = {
            "ACCESS_KEY_ID": ACCESS_KEY_ID,
            "ACCESS_SECURITY_KEY": ACCESS_SECURITY_KEY,
            "BUCKET_NAME": BUCKET_NAME
        }
        """

        try:
            s3 =  boto3.resource(
                's3',
                aws_access_key_id = s3Config["ACCESS_KEY_ID"],
                aws_secret_access_key = s3Config["ACCESS_SECURITY_KEY"],
            )
            logging.info(f"Initialized s3 object")
            next(s3.buckets.pages())
        except ClientError as e:
            logging.error(e)
            logging.error("Incorrect s3Config data, unable to connect to s3 bucket")
            raise ValueError("Incorrect s3Config data, unable to connect to s3 bucket")
        
        return s3

    def __get_links__(self, url: str):

        """It fetches all the downloadable zip file links for the provided xml(url)"""

        logging.info(f"fetching zip links from {url}")

        try:
            r = requests.get(url, allow_redirects=True)
        except Exception as e:
            logging.error(e)
            logging.error(f"Unable to get {url}")
            raise ConnectionError(f"Unable to get {url}")

        r = r.content.decode()

        try:
            tree = ET.fromstring(r)
            L = tree.findall('./result/doc/str')
            for node in L:
                if node.attrib["name"] == "download_link":
                    self.download_links.append(node.text)
        except Exception as e:
            logging.error(e)
            logging.error("Unable to fetch downloadable zip links, either url is incorrect or file's format isn't what is expected")
            raise ValueError("Unable to fetch downloadable zip links, either url is incorrect or file's format isn't what is expected")

        logging.info(f"fetched zip links from {url}")
    
    def __fetch__(self):

        """__fetch__() works as an itterator which yields each zip data and it's name one by one"""

        for link in self.download_links:
            
            logging.info(f"Fetching {link}")

            try:
                resp = urlopen(link)
                zipfile = ZipFile(BytesIO(resp.read()))
                z = zipfile.open(zipfile.namelist()[0])
            except Exception as e:
                logging.error(e)
                logging.error(f"Unable to get {link}")
                raise ConnectionError(f"Unable to get {link}")

            name = link.split('/')[-1].replace('zip', 'csv')
            logging.info(f"Fetched {link}")
            yield name, z.read().decode()

    def __extract__(self, tree: ET.Element):

        """It extracts the relevent data field from the provided xml(tree)"""

        xmlns1 = '{urn:iso:std:iso:20022:tech:xsd:head.003.001.01}'
        xmlns2 = '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}'

        data = []
        finInstrmGnlAttrbts = tree.findall(f'{xmlns1}Pyld/{xmlns2}Document/{xmlns2}FinInstrmRptgRefDataDltaRpt/{xmlns2}FinInstrm/{xmlns2}TermntdRcrd/{xmlns2}FinInstrmGnlAttrbts')
        issr = tree.findall(f'{xmlns1}Pyld/{xmlns2}Document/{xmlns2}FinInstrmRptgRefDataDltaRpt/{xmlns2}FinInstrm/{xmlns2}TermntdRcrd/{xmlns2}Issr')
        for finInstrmGnlAttrbts_i, issr_i in zip(finInstrmGnlAttrbts, issr):
            sub_data = {}
            for child in list(finInstrmGnlAttrbts_i):
                idx = child.tag.index('}')
                sub_data[child.tag[idx+1:]] = child.text
            sub_data["issr"] = issr_i.text
            data.append(sub_data)
        return data

    def __put_to_s3__(self, data: DataFrame, name: str):

        """__put_to_s3__ put the provided csv(data) into the specified(in s3Config) S3 bucket"""

        logging.info(f"Putting {name} ...")
        df = pd.DataFrame(data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        self.s3.Object(self.bucket_name, name).put(Body=csv_buffer.getvalue())
        logging.info(f"Successfully put {name} to s3")

    def __process__(self):
        
        """Here we first extract relevent data from each xml then put it in the specified AWS S3 bucket"""

        for name, xml_data in self.__fetch__():
            
            logging.info(f"Processing {name} ...")
            tree = ET.fromstring(xml_data)
            
            try:
                data = self.__extract__(tree)
            except Exception as e:
                logging.error(e)
                logging.error(f"format for {name} isn't what is expected")
                raise ValueError(f"format for {name} isn't what is expected")

            try:
                self.__put_to_s3__(data, name)
            except Exception as e:
                logging.error(e)
                logging.error(f"Unable to put {name} to the specified s3 bucket please check the s3Config")
                raise ValueError(f"Unable to put {name} to the specified s3 bucket please check the s3Config")

            logging.info(f"Processed {name}")
            
XML_FILE_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
s3Config = {
    "ACCESS_KEY_ID": ACCESS_KEY_ID,
    "ACCESS_SECURITY_KEY": ACCESS_SECURITY_KEY,
    "BUCKET_NAME": BUCKET_NAME
}

# We just need to create a FetchXML class object with correct parameters
# And it will fetch data from XML and put it into s3 bucket
# FetchXML(XML_FILE_URL, s3Config)            
