from dotenv import load_dotenv
import os
import pyodbc

load_dotenv()

def get_connection(server, database):
    """Retorna uma conexão pyodbc a partir de variáveis do .env"""
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    return conn
