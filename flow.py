from etl import ETL
from utils import Utils
from datetime import datetime
import os
import polars as pl
from db import Database

class Flow:
    def __init__(self):
        self.etl = ETL()
        self.db = Database("ibge.db")

        
    def extract_flow(self, points):
        for point in points:
            match point:
                case "noticias":
                    data = self.etl.get_data_with_pagination(point)
                case "nomes_basica":
                    data = []
                    result = self.db.select("SELECT DISTINCT nome from nomes")
                    for row in result:
                        data.append(self.etl.get_data(point, parameters=row[0]))
                    
                    others_names = ["Victor", "Michael"]
                    for name in others_names:
                        data.append(self.etl.get_data(point, parameters=name))
                case "nomes_faixa":
                    data = []
                    result = self.db.select("SELECT DISTINCT nome from nomes")
                    for row in result:
                        data.append(self.etl.get_data(point, parameters=row[0]))
                        
                case _:
                    data = self.etl.get_data(point)

            self.etl.mkdir("data")
            self.etl.mkdir(f"data/{point}")
            current_day = datetime.now().strftime("%Y%m%d")

            try:
                files = os.listdir("data/"+point)
                last_file = sorted(files)[-1]
                last_data = self.etl.load_json(f"data/{point}/{last_file}")

                if last_data != data:
                    self.etl.save_json(data, f"data/{point}/{current_day}_{point}.json") 
                    print(f"New data saved for '{point}'") 
                else:
                    print(f"The '{point}' content is equal to the last one")
                    print("No new data to save")
            except IndexError:
                self.etl.save_json(data, f"data/{point}/{current_day}_{point}.json") 
                print(f"New data saved for '{point}'")
            
            print("")

    def transform_and_load(self, points):
        for point in points:
            files = os.listdir("data/"+point)
            last_file = sorted(files)[-1]
            print(f"Load file for '{point}': {last_file}")
            match point:
                case "noticias":
                    df = pl.DataFrame(
                        self.etl.load_data_from_json_to_df(f"data/{point}/{last_file}")
                    )
                case "nomes":
                    df = self.etl.load_data_from_json_to_df(f"data/{point}/{last_file}")
                case "localidades":
                    df = self.etl.load_data_from_json_to_df(f"data/{point}/{last_file}")
                    utils = Utils()
                    df = utils.transform_localidades(df)
                    
            self.db.drop_table(point)
            self.db.create_table(point, df.columns)
            self.db.truncate_table(point)
            self.db.insert_data(point, df)
            print("-"*50)