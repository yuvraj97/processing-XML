import unittest
from aws import FetchXML, XML_FILE_URL, s3Config
from boto3.resources.factory import ServiceResource

class S3UnitTest(unittest.TestCase):

    def setUp(self):
        self.s3 = FetchXML(XML_FILE_URL, s3Config, testing=True)

    def test_connection(self):
        """It will raise ConnectionError if Failed"""
        self.s3.__get_links__(XML_FILE_URL)

    def test_s3Config(self):
        """It will raise ValueError if Failed"""
        self.assertIsInstance(self.s3.__initialize_s3__(s3Config), ServiceResource)
       
if __name__ == "__main__":
    unittest.main()