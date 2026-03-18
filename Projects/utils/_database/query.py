# utils/database/query.py

import pandas as pd


def consultar_dataframe(
    tabela: str,
    conn,
    filtros: dict = None,
    data_inicio: str = None,
    col_data: str = None,
) -> pd.DataFrame:
    """
    Consulta genérica para qualquer tabela do banco.

    Parameters:
    -----------
    tabela      : Nome da tabela
    conn        : Conexão com o banco
    filtros     : Dict de filtros exatos {coluna: valor} (ex: {'Indicador': 'Discagens'})
    data_inicio : Data de início do filtro no formato 'YYYY-MM-DD' (opcional)
    col_data    : Coluna de data para filtro de data_inicio (obrigatório se data_inicio informado)
    """
    query = f"SELECT * FROM {tabela} WHERE 1=1"
    params = []

    if filtros:
        for col, valor in filtros.items():
            query += f" AND {col} = ?"
            params.append(valor)

    if data_inicio and col_data:
        query += f" AND CAST({col_data} AS DATE) >= ?"
        params.append(data_inicio)

    cursor = conn.cursor()
    cursor.execute(query, params)
    colunas = [col[0] for col in cursor.description]
    registros = cursor.fetchall()

    return pd.DataFrame.from_records(registros, columns=colunas)

# utils/database/query.py — adiciona essa função

def obter_ou_processar(
    tabela: str,
    col_data: str,
    conn,
    fn_processar,
    fn_processar_kwargs: dict,
    arquivo_log: str = None,
) -> pd.DataFrame | None:
    """
    Verifica se os dados estão atualizados no banco.
    Se sim, retorna do banco. Se não, processa e insere.

    Parameters:
    -----------
    tabela              : Nome da tabela no banco
    col_data            : Coluna de data para verificação
    conn                : Conexão com o banco
    fn_processar        : Função de processamento — chamada se dado desatualizado
    fn_processar_kwargs : Kwargs passados para fn_processar
    arquivo_log         : Caminho do arquivo de log
    """
    from utils._database.check import verificar_ultima_data, esta_atualizado
    from utils._database.query import consultar_dataframe
    from utils.utils import salvar_log

    def log(msg):
        print(msg)
        if arquivo_log:
            salvar_log(msg, arquivo_log)

    ultima_data = verificar_ultima_data(tabela=tabela, conn=conn, col_data=col_data)

    if esta_atualizado(ultima_data):
        log(f"INFO  » {tabela} — dados atualizados no banco, consultando...")
        return consultar_dataframe(tabela=tabela, conn=conn, col_data=col_data)

    log(f"INFO  » {tabela} — dados desatualizados, processando...")
    return fn_processar(**fn_processar_kwargs)