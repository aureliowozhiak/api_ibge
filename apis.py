
"""
APIs IBGE: https://servicodados.ibge.gov.br/api/docs/

 - API Nomes:
    https://servicodados.ibge.gov.br/api/docs/nomes?versao=2
    endpoint: https://servicodados.ibge.gov.br/api/v2/censos/nomes/{nome}

- API Notícias:
    https://servicodados.ibge.gov.br/api/docs/noticias?versao=3
    endpoint: https://servicodados.ibge.gov.br/api/v3/noticias

- API Localidades:
    https://servicodados.ibge.gov.br/api/docs/localidades?versao=1
    endpoint: https://servicodados.ibge.gov.br/api/v1/localidades/distritos/

"""

endpoints = {

    "nome_pt" : "https://dados.justica.gov.pt/en/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%2238b576a5-2d47-4477-a35e-4d0c075f8e49%22",
    "nomes": "https://servicodados.ibge.gov.br/api/v2/censos/nomes/",
    "nomes_basica": "https://servicodados.ibge.gov.br/api/v1/censos/nomes/basica?nome=",
    "nomes_faixa": "https://servicodados.ibge.gov.br/api/v1/censos/nomes/faixa?nome=",
    "noticias": "https://servicodados.ibge.gov.br/api/v3/noticias/",
    "localidades": "https://servicodados.ibge.gov.br/api/v1/localidades/distritos/"

}