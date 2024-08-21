from etl import ETL
from utils import Utils
from datetime import datetime
import os
import polars as pl
from db import Database

etl = ETL()
db = Database("ibge.db")

#points = ["noticias", "nomes", "localidades"]
points = ["nomes_basica"]


def extract_flow():
    for point in points:
        match point:
            case "noticias":
                data = etl.get_data_with_pagination(point)
            case "nomes_basica":
                data = []
                result = db.select("SELECT DISTINCT nome from nomes")
                for row in result:
                    data.append(etl.get_data(point, parameters=row[0]))
                
                others_names = ["Victor", "Michael"]
                for name in others_names:
                    data.append(etl.get_data(point, parameters=name))
                    
            case _:
                data = etl.get_data(point)

        etl.mkdir("data")
        etl.mkdir(f"data/{point}")
        current_day = datetime.now().strftime("%Y%m%d")

        try:
            files = os.listdir("data/"+point)
            last_file = sorted(files)[-1]
            last_data = etl.load_json(f"data/{point}/{last_file}")

            if last_data != data:
                etl.save_json(data, f"data/{point}/{current_day}_{point}.json") 
                print(f"New data saved for '{point}'") 
            else:
                print(f"The '{point}' content is equal to the last one")
                print("No new data to save")
        except IndexError:
            etl.save_json(data, f"data/{point}/{current_day}_{point}.json") 
            print(f"New data saved for '{point}'")
        
        print("")

def transform_and_load():
    for point in points:
        files = os.listdir("data/"+point)
        last_file = sorted(files)[-1]
        print(f"Load file for '{point}': {last_file}")
        match point:
            case "noticias":
                df = pl.DataFrame(
                    etl.load_data_from_json_to_df(f"data/{point}/{last_file}")
                )
            case "nomes":
                df = etl.load_data_from_json_to_df(f"data/{point}/{last_file}")
            case "localidades":
                df = etl.load_data_from_json_to_df(f"data/{point}/{last_file}")
                utils = Utils()
                df = utils.transform_localidades(df)
                
        db.drop_table(point)
        db.create_table(point, df.columns)
        db.truncate_table(point)
        db.insert_data(point, df)
        print("-"*50)


if __name__ == "__main__":

    while True:
        option = input("Digite 'e' para extrair, 't' para transformar e carregar, 'q' para sair: ")
        
        match option:
            case "e":
                extract_flow()
            case "t":
                transform_and_load()
            case "q":
                db.close_connection()
                break
            case _:
                print("Opção inválida")
                continue
    
