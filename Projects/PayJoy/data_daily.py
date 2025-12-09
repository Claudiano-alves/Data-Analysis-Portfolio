import pandas as pd
from db_connection import get_connection
sql_file = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\PayJoy\Daily_PayJoy.sql"

def df_consulta(sql_file):
    # Cria a conexão
    conn_bd2 = get_connection("SERVER_BD2", "DATABASE_BD2")
    # Caminho do arquivo SQL (rede compartilhada)

    # Lê o conteúdo da query
    with open(sql_file, "r", encoding="utf-8") as f:
        query = f.read()

    # Executa a query e carrega em um DataFrame
    df = pd.read_sql(query, conn_bd2)

    # Mostra as primeiras linhas
    return df

df_consulta_df = df_consulta(sql_file)
print(df_consulta_df.head())