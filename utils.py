
class Utils:
    def __init__(self) -> None:
        pass

    def transform_localidades(self, df):
        # Transformações
        # Renomeando colunas
        df.columns = ["id_localidade", "nome_localidade", "municipio"]
        # Explodindo coluna municipio
        df = df.unnest("municipio")

        # Renomeando colunas
        df.columns = ["id_localidade", "nome_localidade", "id_municipio", "nome_municipio", "microrregiao", "regiao-imediata"]
        
        # Pegando apenas as colunas necessárias
        df = df[["id_localidade", "nome_localidade", "id_municipio", "nome_municipio","regiao-imediata"]]

        # Explodindo coluna regiao-imediata
        df = df.unnest("regiao-imediata")

        # Renomeando colunas
        df.columns = ["id_localidade", "nome_localidade", "id_municipio", "nome_municipio", "id_regiao-imediata", "nome_regiao-imediata", "regiao-intermediaria"]
        
        # Explodindo coluna regiao-intermediaria
        df = df.unnest("regiao-intermediaria")

        # Pegando apenas as colunas necessárias
        df = df[["id_localidade", "nome_localidade", "id_municipio", "nome_municipio", "id_regiao-imediata", "nome_regiao-imediata", "UF"]]
        
        # Explodingo coluna UF
        df = df.unnest("UF")

        # Renomeando colunas
        df.columns = ["id_localidade", "nome_localidade", "id_municipio", "nome_municipio", "id_regiao-imediata", "nome_regiao-imediata", "id_UF", "sigla_UF", "nome_UF", "regiao"]

        # Explodindo coluna regiao
        df = df.unnest("regiao")
        
        # Renomeando colunas
        df.columns = ["id_localidade", "nome_localidade", "id_municipio", "nome_municipio", "id_regiao_imediata", "nome_regiao_imediata", "id_UF", "sigla_UF", "nome_UF", "id_regiao", "sigla_regiao", "nome_regiao"]
        
        return df