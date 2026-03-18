# utils/database/check.py

from datetime import date, timedelta

def verificar_ultima_data(
    tabela: str,
    conn,
    col_data: str = 'data',
    filtros: dict = None,
) -> date | None:
    """
    Retorna a última data de dado na tabela, com filtros opcionais.
    Retorna None se a tabela estiver vazia.

    Parameters:
    -----------
    tabela   : Nome da tabela
    conn     : Conexão com o banco
    col_data : Coluna de data a verificar
    filtros  : Dict de filtros exatos {coluna: valor} (ex: {'Indicador': 'Discagens'})
    """
    query = f"SELECT MAX(CAST({col_data} AS DATE)) FROM {tabela} WHERE 1=1"
    params = []

    if filtros:
        for col, valor in filtros.items():
            query += f" AND {col} = ?"
            params.append(valor)

    cursor = conn.cursor()
    cursor.execute(query, params)
    row = cursor.fetchone()

    if row is None or row[0] is None:
        return None

    return row[0] if isinstance(row[0], date) else row[0].date()


def esta_atualizado(ultima_data: date | None) -> bool:
    """Verifica se a última data é D-1 ou mais recente."""
    if ultima_data is None:
        return False
    return ultima_data >= date.today() - timedelta(days=1)


def datas_faltantes(ultima_data: date | None) -> list:
    """Retorna lista de datas faltantes entre última data e D-1."""
    if ultima_data is None:
        return []
    ontem = date.today() - timedelta(days=1)
    if ultima_data >= ontem:
        return []
    datas = []
    atual = ultima_data + timedelta(days=1)
    while atual <= ontem:
        datas.append(atual)
        atual += timedelta(days=1)
    return datas