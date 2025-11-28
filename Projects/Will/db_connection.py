import pyodbc

def get_connection(server, database):
    """Retorna uma conexão pyodbc usando valores diretos"""
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    return conn