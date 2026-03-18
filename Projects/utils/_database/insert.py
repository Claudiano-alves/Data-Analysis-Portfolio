# utils/database/insert.py

import pandas as pd
from datetime import date
from utils.utils import salvar_log

# utils/database/insert.py  — adiciona essa função

def inserir_dataframe_incremental(
    df: pd.DataFrame,
    tabela: str,
    col_data: str,
    conn,
    chunk_size: int = 10_000,
    arquivo_log: str = None,
    tipos: dict = None,
):
    """
    Verifica datas faltantes na tabela e insere apenas os registros necessários.

    Parameters:
    -----------
    df          : DataFrame completo com todos os dados
    tabela      : Nome da tabela destino
    col_data    : Nome da coluna de data no df (após renomeação)
    conn        : Conexão com o banco
    chunk_size  : Tamanho de cada lote (default 10.000)
    arquivo_log : Caminho do arquivo de log (opcional)
    tipos       : Dict de conversão de tipos {coluna: dtype}
    """
    from utils._database.check import verificar_datas_faltantes

    def log(msg):
        print(msg)
        if arquivo_log:
            salvar_log(msg, arquivo_log)

    datas_faltantes = verificar_datas_faltantes(tabela, col_data, conn)

    if not datas_faltantes:
        log(f"[{tabela}] ✅ Tabela atualizada — nenhum registro a inserir")
        return True

    log(f"[{tabela}] 📅 Datas faltantes: {datas_faltantes[0]} até {datas_faltantes[-1]} ({len(datas_faltantes)} dia(s))")

    df_filtrado = df[pd.to_datetime(df[col_data]).dt.date.isin(datas_faltantes)].copy()

    if df_filtrado.empty:
        log(f"[{tabela}] ⚠️  Nenhum registro no df para as datas faltantes")
        return False

    log(f"[{tabela}] 📋 {len(df_filtrado):,} registros a inserir")

    return inserir_dataframe(
        df=df_filtrado,
        tabela=tabela,
        conn=conn,
        chunk_size=chunk_size,
        arquivo_log=arquivo_log,
        tipos=tipos,
    )