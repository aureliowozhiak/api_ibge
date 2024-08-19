import sqlite3

class Database:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()

    # to improve: 
    # -- use a dictionary to pass the values
    # -- bind values to the query - evitar sqlinjection, e anomalias
    def run_sql(self, sql):
        self.conn.execute(sql)
        self.conn.commit()

    def create_table(self, table_name, columns):
        # Inicializa a DDL de Create Table com o nome da tabela
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        # para cada nome de coluna na lista columns
        for column in columns:
            # Adiciona o nome da coluna e o tipo de dado
            ddl += f"{column} TEXT, " # por padrão todas as colunas são do tipo texto
        # Remove a última vírgula e espaço
        ddl = ddl[:-2] + ")"

        self.run_sql(ddl)

    def insert_data(self, table_name, df):
        for i in range(df.shape[0]):
            values = df.row(i)
            sql = f"INSERT INTO {table_name} VALUES {values}"
            self.run_sql(sql)

    def truncate_table(self, table_name):
        sql = f"DELETE FROM {table_name} WHERE 1=1"
        self.run_sql(sql)


    def close_connection(self):
        self.conn.close()