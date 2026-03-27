# utils/_database/operations.py

from datetime import date
from utils._database.insert import inserir_dataframe_incremental
from utils.utils import salvar_log


def inserir_analitico(
    df,
    conn,
    tabela: str,
    colunas: dict,
    col_data: str,
    tipos: dict = None,
    arquivo_log: str = None,
    data_fim: date = None,
):
    """
    Insere dados analíticos no banco de forma incremental.
    Genérico para qualquer carteira e qualquer tipo de analítico.

    Parameters:
    -----------
    df          : DataFrame com os dados
    conn        : Conexão com o banco (conn_bd2)
    tabela      : Nome da tabela destino
    colunas     : Dict {col_df: col_tabela} — seleção e renomeação
    col_data    : Nome da coluna de data no df (após renomeação)
    tipos       : Dict de conversão de tipos {coluna: dtype} (opcional)
    arquivo_log : Caminho do arquivo de log (opcional)
    data_fim    : Data limite do ciclo. Se None, usa D-1
    """
    df_preparado = df[list(colunas.keys())].rename(columns=colunas)

    return inserir_dataframe_incremental(
        df=df_preparado,
        tabela=tabela,
        col_data=col_data,
        conn=conn,
        data_fim=data_fim,
        tipos=tipos,
        arquivo_log=arquivo_log,
    )