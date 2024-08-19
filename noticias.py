from etl import ETL
from apis import endpoints
import requests

etl = ETL()

endpoint = "noticias"
page = 190
while page != 0:
    page = requests.get(f'{endpoints[endpoint]}?page={page}').json()["nextPage"]
    print(page)