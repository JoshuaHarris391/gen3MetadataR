import os
import unittest
from unittest.mock import patch, mock_open, MagicMock
from gen3_metadata.parser import Gen3MetadataParser

class TestGen3MetadataParser(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data='{"key": "value"}')
    @patch('gen3_metadata.parser.requests.post')
    def setUp(self, mock_post, mock_open):
        # Mock the API response for authentication
        mock_response = MagicMock()
        mock_response.json.return_value = {'access_token': 'fake_token'}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        # Set up the environment variable for the key file path
        os.environ['credentials_path'] = '/path/to/credentials.json'

        # Initialize the Gen3MetadataParser
        self.api_url = "https://data.test.biocommons.org.au"
        self.key_file = os.getenv('credentials_path')
        self.parser = Gen3MetadataParser(self.api_url, self.key_file)

    @patch('gen3_metadata.parser.requests.get')
    def test_fetch_data(self, mock_get):
        # Mock the API response for data fetching
        mock_response = MagicMock()
        mock_response.json.return_value = {'data': [{'id': 1, 'name': 'test'}]}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Fetch data
        self.parser.fetch_data("program1", "AusDiab_Simulated", "subject", return_data=True)

        # Check if data is stored correctly
        key = "program1/AusDiab_Simulated/subject"
        self.assertIn(key, self.parser.data_store)
        self.assertEqual(self.parser.data_store[key], {'data': [{'id': 1, 'name': 'test'}]})

    def test_data_to_pd(self):
        # Add mock data to data_store
        key = "program1/AusDiab_Simulated/subject"
        self.parser.data_store[key] = {'data': [{'id': 1, 'name': 'test'}]}

        # Convert data to pandas DataFrame
        self.parser.data_to_pd()

        # Check if data is converted to DataFrame
        self.assertIn(key, self.parser.data_store_pd)
        self.assertFalse(self.parser.data_store_pd[key].empty)

if __name__ == '__main__':
    unittest.main()