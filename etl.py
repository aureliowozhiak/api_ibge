from apis import endpoints
import requests
import json
import os

class ETL:
    def __init__(self) -> None:
        pass
    
    def get_data(self, endpoint, query_parameters=None, parameters=None):
        if parameters:
            return requests.get(endpoints[endpoint] + parameters).json()
        return requests.get(endpoints[endpoint], params=query_parameters).json()

    def mkdir(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def save_json(self, data, filename):
        with open(filename, "w") as file:
            json.dump(data, file, indent=4)