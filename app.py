from etl import ETL
from datetime import datetime

etl = ETL()

points = ["nomes", "noticias", "localidades"]

for point in points:
    data = etl.get_data(point)
    etl.mkdir("data")
    etl.mkdir(f"data/{point}")
    current_day = datetime.now().strftime("%Y%m%d")
    etl.save_json(data, f"data/{point}/{current_day}_{point}.json")  
    