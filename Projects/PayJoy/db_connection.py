from dotenv import load_dotenv
import os
import pyodbc

load_dotenv()

def get_connection(server_var, database_var):
    """Retorna uma conexão pyodbc a partir de variáveis do .env"""
    server = os.getenv(server_var)
    database = os.getenv(database_var)
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    return conn
