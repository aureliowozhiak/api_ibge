from etl import ETL
from datetime import datetime
import os
import polars as pl
from db import Database

etl = ETL()
db = Database("ibge.db")

points = ["noticias", "nomes", "localidades"]

def etl_flow():
    for point in points:
        data = etl.get_data(point)
        etl.mkdir("data")
        etl.mkdir(f"data/{point}")
        current_day = datetime.now().strftime("%Y%m%d")

        
        files = os.listdir("data/"+point)
        last_file = sorted(files)[-1]
        last_data = etl.load_json(f"data/{point}/{last_file}")

        if last_data != data:
            etl.save_json(data, f"data/{point}/{current_day}_{point}.json") 
            print(f"New data saved for '{point}'") 
        else:
            print(f"The '{point}' content is equal to the last one")
            print("No new data to save")
        
        print("")


if __name__ == "__main__":
    #etl_flow()

    for point in points:
        files = os.listdir("data/"+point)
        last_file = sorted(files)[-1]
        print(f"Load file for '{point}': {last_file}")
        match point:

            case "noticias":
                df = pl.DataFrame(
                    etl.load_data_from_json_to_df(f"data/{point}/{last_file}")["items"][0]
                )
                df.columns = ["items"]
                df = df.unnest("items")
            case "nomes":
                df = etl.load_data_from_json_to_df(f"data/{point}/{last_file}")
            case "localidades":
                df = etl.load_data_from_json_to_df(f"data/{point}/{last_file}")
                df = df[["id", "nome"]]
        
        db.create_table(point, df.columns)
        db.truncate_table(point)
        db.insert_data(point, df)
        print("-"*50)
