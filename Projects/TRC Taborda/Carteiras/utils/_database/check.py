# utils/_database/check.py

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


def esta_atualizado(ultima_data: date | None, data_fim: date = None) -> bool:
    """
    Verifica se a última data é >= data_fim (ou D-1 se data_fim não informada).
    """
    if ultima_data is None:
        return False
    referencia = data_fim or (date.today() - timedelta(days=1))
    return ultima_data >= referencia


def datas_faltantes(
    tabela: str,
    col_data: str,
    conn,
    data_fim: date = None,
) -> list | None:
    """
    Retorna lista de datas faltantes para inserir.

    Retorno:
    - None       : tabela vazia — inserir tudo
    - []         : tabela já atualizada até data_fim — não inserir
    - [date, ...]: datas faltantes entre ultima_data+1 e data_fim
    
    Parameters:
    -----------
    tabela   : Nome da tabela
    col_data : Coluna de data a verificar
    conn     : Conexão com o banco
    data_fim : Data limite do ciclo. Se None, usa D-1
    """
    ultima_data = verificar_ultima_data(tabela, conn, col_data)
    data_fim = data_fim or (date.today() - timedelta(days=1))

    if ultima_data is None:
        return None  # tabela vazia, inserir tudo

    if ultima_data >= data_fim:
        return []  # já atualizado até data_fim

    datas = []
    atual = ultima_data + timedelta(days=1)
    while atual <= data_fim:
        datas.append(atual)
        atual += timedelta(days=1)

    return datas