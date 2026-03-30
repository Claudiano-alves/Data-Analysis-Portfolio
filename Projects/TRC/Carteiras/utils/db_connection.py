from dotenv import load_dotenv
import os
import pyodbc
from contextlib import contextmanager

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

@contextmanager
def get_db_connections():
    """
    Context manager para gerenciar conexões de banco de dados.
    Garante que as conexões sejam fechadas mesmo em caso de erro.
    """
    conn_trc = None
    conn_bd2 = None
    conn_src = None
    
    try:
        print("🔌 Conectando aos bancos de dados...")
        conn_trc = get_connection("SERVER_BD2", "DATABASE_TRC")
        conn_bd2 = get_connection("SERVER_BD2", "DATABASE_BD2")
        conn_src = get_connection("SERVER_SRC", "DATABASE_SRC")
        print("✅ Conexões estabelecidas")
        
        yield conn_trc, conn_bd2, conn_src
        
    finally:
        # Fecha as conexões
        for conn, name in [(conn_trc, 'TRC'), (conn_bd2, 'BD2'), (conn_src, 'SRC')]:
            if conn:
                try:
                    conn.close()
                    print(f"🔌 Conexão {name} fechada")
                except Exception as e:
                    print(f"⚠️ Erro ao fechar conexão {name}: {e}")