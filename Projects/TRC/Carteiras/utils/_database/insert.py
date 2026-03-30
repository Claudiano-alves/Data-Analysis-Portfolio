# utils/_database/insert.py

import pandas as pd
from datetime import date
from utils.utils import salvar_log


def inserir_dataframe(
    df: pd.DataFrame,
    tabela: str,
    conn,
    chunk_size: int = 10_000,
    arquivo_log: str = None,
    colunas: list = None,
    tipos: dict = None,
):
    """
    Insere um DataFrame em uma tabela do banco em chunks com rollback automático.

    Parameters:
    -----------
    df          : DataFrame a ser inserido
    tabela      : Nome da tabela destino
    conn        : Conexão com o banco (pyodbc connection)
    chunk_size  : Tamanho de cada lote (default 10.000)
    arquivo_log : Caminho do arquivo de log (opcional)
    colunas     : Lista de colunas a inserir. Se None, usa todas as colunas do df
    tipos       : Dict de conversão de tipos {coluna: dtype} aplicado antes do insert
    """

    def log(msg):
        print(msg)
        if arquivo_log:
            salvar_log(msg, arquivo_log)

    # ── 1. Preparo do DataFrame ───────────────────────────────────────────────
    df_insert = df.copy()

    if tipos:
        for col, dtype in tipos.items():
            if col in df_insert.columns:
                df_insert[col] = df_insert[col].astype(dtype)

    if colunas:
        ausentes = [c for c in colunas if c not in df_insert.columns]
        for col in ausentes:
            log(f"⚠️  Coluna ausente no df (será NULL): {col}")
            df_insert[col] = None
        df_insert = df_insert[colunas]

    # Converte category para str
    cols_category = df_insert.select_dtypes(include='category').columns.tolist()
    if cols_category:
        df_insert[cols_category] = df_insert[cols_category].astype(str)

    # Converte tipos incompatíveis com pyodbc para tipos nativos Python
    def _converter_valor(v):
        if pd.isna(v) if not isinstance(v, (list, dict)) else False:
            return None
        if isinstance(v, pd.Timestamp):
            return v.date()
        if type(v).__module__ == 'numpy':
            return v.item()
        return v

    for col in df_insert.columns:
        if df_insert[col].dtype.kind in ('i', 'f', 'u', 'M'):
            df_insert[col] = df_insert[col].apply(_converter_valor)

    # Substitui NaN e inf remanescentes por None em colunas float
    import numpy as np
    float_cols = df_insert.select_dtypes(include=['float']).columns
    for col in float_cols:
        df_insert[col] = df_insert[col].apply(
            lambda v: None if (v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v)))) else v
        )

    total = len(df_insert)
    chunks = [df_insert.iloc[i:i + chunk_size] for i in range(0, total, chunk_size)]
    colunas_insert = list(df_insert.columns)
    placeholders = ', '.join(['?' for _ in colunas_insert])
    cols_sql = ', '.join(colunas_insert)
    sql = f"INSERT INTO {tabela} ({cols_sql}) VALUES ({placeholders})"

    log(f"{'─' * 55}")
    log(f"[{tabela}] Iniciando insert — {total:,} registros em {len(chunks)} lote(s)")

    # ── 2. Insert com rollback ────────────────────────────────────────────────
    dt_carga = date.today().isoformat()
    cursor = conn.cursor()

    try:
        for i, chunk in enumerate(chunks, start=1):
            registros = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
            cursor.executemany(sql, registros)
            log(f"[{tabela}] Lote {i}/{len(chunks)} inserido — {len(chunk):,} registros")

        conn.commit()
        log(f"[{tabela}] ✅ Commit realizado — {total:,} registros inseridos com sucesso")
        return True

    except Exception as e:
        conn.rollback()
        log(f"[{tabela}] ❌ Erro no lote {i}: {e}")
        log(f"[{tabela}] Rollback realizado — excluindo registros do dia {dt_carga}...")

        try:
            cursor.execute(
                f"DELETE FROM {tabela} WHERE CAST(dt_carga AS DATE) = ?",
                dt_carga
            )
            conn.commit()
            log(f"[{tabela}] 🗑️  Registros do dia {dt_carga} excluídos com sucesso")
        except Exception as e_del:
            log(f"[{tabela}] ❌ Erro ao excluir registros do dia: {e_del}")

        return False


def inserir_dataframe_incremental(
    df: pd.DataFrame,
    tabela: str,
    col_data: str,
    conn,
    data_fim: date = None,
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
    data_fim    : Data limite do ciclo. Se None, usa D-1
    chunk_size  : Tamanho de cada lote (default 10.000)
    arquivo_log : Caminho do arquivo de log (opcional)
    tipos       : Dict de conversão de tipos {coluna: dtype}
    """
    from utils._database.check import datas_faltantes

    def log(msg):
        print(msg)
        if arquivo_log:
            salvar_log(msg, arquivo_log)

    datas_pendentes = datas_faltantes(tabela, col_data, conn, data_fim=data_fim)

    if datas_pendentes is None:
        # tabela vazia — insere tudo que está no df
        log(f"[{tabela}] 📋 Tabela vazia — inserindo todos os {len(df):,} registros")
        df_filtrado = df.copy()

    elif not datas_pendentes:
        log(f"[{tabela}] ✅ Tabela atualizada — nenhum registro a inserir")
        return True

    else:
        log(f"[{tabela}] 📅 Datas faltantes: {datas_pendentes[0]} até {datas_pendentes[-1]} ({len(datas_pendentes)} dia(s))")
        df_filtrado = df[pd.to_datetime(df[col_data]).dt.date.isin(datas_pendentes)].copy()

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