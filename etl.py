from apis import endpoints
import requests
import json
import os
import polars as pl

class ETL:
    def __init__(self) -> None:
        pass
    
    def get_data(self, endpoint, query_parameters=None, parameters=None):
        if parameters:
            return requests.get(endpoints[endpoint] + parameters).json()
        return requests.get(endpoints[endpoint], params=query_parameters).json()
    
    def get_data_with_pagination(self, endpoint, page=1, query_parameters=None):
        data = []
        while page != 0:
            print(f"Getting data from page {page} for '{endpoint}'")
            response = self.get_data(endpoint, query_parameters, f"?page={page}")
            data.extend(response["items"])
            page = response["nextPage"]
        return data

    def mkdir(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def save_json(self, data, filename):
        with open(filename, "w") as file:
            json.dump(data, file, indent=4)

    def load_json(self, filename):
        with open(filename, "r") as file:
            return json.load(file)

    def load_data_from_json_to_df(self, filename):
        return pl.read_json(filename)
            