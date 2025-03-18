import json
import requests
import pandas as pd
import jwt


class Gen3MetadataParser:
    """
    A class to interact with Gen3 metadata API for fetching and processing data.
    """

    def __init__(self, key_file_path):
        """
        Initializes the Gen3MetadataParser with API URL and key file path.

        Args:
            key_file_path (str): The file path to the JSON key file for authentication.
        """
        self.key_file_path = key_file_path
        self.headers = {}
        self.data_store = {}
        self.data_store_pd = {}

    def _load_api_key(self) -> dict:
        """
        Loads the API key from the specified JSON file.

        Returns:
            dict: The API key loaded from the JSON file.
        """
        with open(self.key_file_path) as json_file:
            return json.load(json_file)

    def _url_from_jwt(self, cred: dict) -> str:
        """
        Extracts the URL from a JSON Web Token (JWT) credential.

        Args:
            cred (dict): The JSON Web Token (JWT) credential.

        Returns:
            str: The extracted URL.
        """
        jwt_token = cred['api_key']
        url = jwt.decode(jwt_token, options={"verify_signature": False}).get('iss', '').removesuffix("/user")
        return url


    def authenticate(self) -> dict:
        """
        Authenticates with the Gen3 API using the loaded API key.

        Returns:
            dict: Headers containing the authorization token.
        """
        try:
            key = self._load_api_key()
            api_url = self._url_from_jwt(key)
            response = requests.post(
                f"{api_url}/user/credentials/cdis/access_token", json=key
            )
            response.raise_for_status()
            access_token = response.json()['access_token']
            self.headers = {'Authorization': f"bearer {access_token}"}
            return print(f"Authentication successful: {response.status_code}")
        except requests.exceptions.HTTPError as http_err:
            print(
                f"HTTP error occurred during authentication: {http_err} - "
                f"Status Code: {response.status_code}"
            )
            raise
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred during authentication: {req_err}")
            raise
        except KeyError as key_err:
            print(
                f"Key error: {key_err} - The response may not contain 'access_token'"
            )
            raise
        except Exception as err:
            print(f"An unexpected error occurred during authentication: {err}")
            raise

    def json_to_pd(self, json_data) -> pd.DataFrame:
        """
        Converts JSON data to a pandas DataFrame.

        Args:
            json_data (dict): The JSON data to convert.

        Returns:
            pandas.DataFrame: The converted pandas DataFrame.
        """
        return pd.json_normalize(json_data)

    def fetch_data(
        self, program_name, project_code, node_label, return_data=False, api_version="v0"
    ) -> dict:
        """
        Fetches data from the Gen3 API for a specific program, project, and node label.

        Args:
            program_name (str): The name of the program.
            project_code (str): The code of the project.
            node_label (str): The label of the node.
            return_data (bool, optional): Whether to return the fetched data.
                Defaults to False.
            api_version (str, optional): The version of the API to use.
                Defaults to "v0".

        Returns:
            dict or None: The fetched data if return_data is True, otherwise None.
        """
        try:
            creds = self._load_api_key()
            api_url = self._url_from_jwt(creds)
            url = (
                f"{api_url}/api/{api_version}/submission/{program_name}/{project_code}/"
                f"export/?node_label={node_label}&format=json"
            )
            response = requests.get(url, headers=self.headers)
            print(f"status code: {response.status_code}")
            response.raise_for_status()
            data = response.json()

            key = f"{program_name}/{project_code}/{node_label}"
            self.data_store[key] = data

            if return_data:
                return data
            else:
                print(f"Data for {key} has been fetched and stored.")
        except requests.exceptions.HTTPError as http_err:
            print(
                f"HTTP error occurred: {http_err} - "
                f"Status Code: {response.status_code}"
            )
            raise
        except Exception as err:
            print(f"An error occurred: {err}")
            raise

    def data_to_pd(self) -> None:
        """
        Converts all fetched JSON data in the data store to pandas DataFrames.
        """
        for key, value in self.data_store.items():
            print(f"Converting {key} to pandas dataframe...")
            self.data_store_pd[key] = self.json_to_pd(value['data'])
        return
