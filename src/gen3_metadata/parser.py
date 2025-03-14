import json
import requests
import os

class Gen3MetadataParser:
    def __init__(self, api_url, key_file_path):
        self.api_url = api_url
        self.key_file_path = key_file_path
        self.headers = self._authenticate()
        self.data_store = {} 
        self.data_store_pd = {} # Initialize a dictionary to store fetched data

    def _load_api_key(self):
        with open(self.key_file_path) as json_file:
            return json.load(json_file)

    def _authenticate(self):
        key = self._load_api_key()
        response = requests.post(f"{self.api_url}/user/credentials/cdis/access_token", json=key)
        response.raise_for_status()  # Ensure any HTTP errors are raised
        access_token = response.json()['access_token']
        return {'Authorization': f"bearer {access_token}"}
    
    def json_to_pdf(self, json_data):
        import pandas as pd
        return pd.json_normalize(json_data)

    def fetch_data(self, program_name, project_code, node_label, return_data=False):
        try:
            url = f"{self.api_url}/api/v0/submission/{program_name}/{project_code}/export/?node_label={node_label}&format=json"
            response = requests.get(url, headers=self.headers)
            print(f"status code: {response.status_code}")
            response.raise_for_status()  # Ensure any HTTP errors are raised
            data = response.json()
            
            # Create a key from program_name, project_code, and node_label
            key = f"{program_name}/{project_code}/{node_label}"
            
            # Store the data in the dictionary with the created key
            self.data_store[key] = data
            
            if return_data:
                return data
            else:
                print(f"Data for {key} has been fetched and stored.")
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err} - Status Code: {response.status_code}")
        except Exception as err:
            print(f"An error occurred: {err}")
            
    def data_to_pd(self):
        import pandas as pd
        for key, value in self.data_store.items():
            print(f"Converting {key} to pandas dataframe...")
            self.data_store_pd[key] = self.json_to_pdf(value['data'])
        return