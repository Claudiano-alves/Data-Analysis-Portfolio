# repositories.py

import numpy as np
import pandas as pd
import time

TABELA_DESTINO = "analytical_digital_estacio"
BATCH_SIZE = 10000

COLUNAS_INSERT = [
    'CANAL', 'DATA_DISPARO', 'CPF', 'CUSTO', 'CONTATO', 'CONTRATO',
    'CORRESPONDENCIA', 'ATRASO', 'COD_CLI', 'ID_CLIENTE', 'REGIONAL',
    'GRUPO', 'GRUPO_SEGMENTADO', 'PRODUTO_SEGMENTADO', 'SPD', 'BU',
    'MODALIDE', 'STDEBITO', 'STALUNO', 'CURSO', 'APROACAD', 'PRODUTO',
    'ULTRENOV', 'COD_PRODUT', 'FX_ATRASO', 'VALOR'
]
COLUNAS_DATETIME  = ['DATA_DISPARO', 'ULTRENOV']
COLUNAS_NUMERICAS = ['CUSTO', 'ATRASO', 'COD_CLI', 'COD_PRODUT', 'VALOR', 'CORRESPONDENCIA']
COLUNAS_DECIMAL   = ['CUSTO', 'VALOR']
COLUNAS_INT       = ['ATRASO', 'COD_CLI', 'COD_PRODUT', 'CORRESPONDENCIA']  # INT/BIGINT no banco

def para_python_nativo(v):
    if v is None:
        return None
    if v is pd.NaT:
        return None
    if isinstance(v, (float, np.floating)) and np.isnan(v):
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    return v


def preparar_tipos(df: pd.DataFrame, nome_canal: str) -> pd.DataFrame:

    df = df.copy()
    df = df.loc[:, ~df.columns.duplicated()]

    # Converte category para str
    cols_category = df.select_dtypes(include='category').columns.tolist()
    if cols_category:
        print(f"  [{nome_canal}] Convertendo category → str: {cols_category}")
        for col in cols_category:
            df[col] = df[col].astype(str)

    # Converte datas
    for col in COLUNAS_DATETIME:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Converte decimais
    for col in COLUNAS_DECIMAL:
        if col in df.columns:
            serie = pd.to_numeric(df[col], errors='coerce').round(4)
            df[col] = [None if pd.isna(v) else float(v) for v in serie]

    # Converte inteiros
    for col in COLUNAS_INT:
        if col in df.columns:
            serie = pd.to_numeric(df[col], errors='coerce')
            df[col] = [None if pd.isna(v) else int(v) for v in serie]

    # Converte strings
    cols_str = [c for c in df.columns
                if c not in COLUNAS_DATETIME + COLUNAS_DECIMAL + COLUNAS_INT
                and df[c].dtype == object]
    for col in cols_str:
        df[col] = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)
                   for v in df[col]]

    # Passo final — converte coluna por coluna para tipos Python nativos
    # usando .tolist() para extrair valores puros antes da conversão
    for col in df.columns:
        valores_puros = [para_python_nativo(v) for v in df[col].tolist()]
        df[col] = pd.array(valores_puros, dtype=object)

    return df

def limpar_inserts_do_dia(conn, nome_canal: str) -> None:
    """
    Remove todos os registros do canal inseridos hoje.
    Chamada automaticamente quando um insert falha após lotes já commitados.
    """
    sql = """
        DELETE FROM analytical_digital_estacio
        WHERE CANAL = ?
        AND CAST(DT_CARGA AS DATE) = CAST(GETDATE() AS DATE)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, nome_canal)
        deletados = cursor.rowcount
        conn.commit()
        print(f"  [{nome_canal}] 🧹 Limpeza concluída: {deletados:,} registros removidos.")
    except Exception as e:
        print(f"  [{nome_canal}] ❌ Erro ao limpar registros: {e}")

def inserir_analitico_(df: pd.DataFrame, conn, nome_canal: str) -> bool:

    if df.empty:
        print(f"  [{nome_canal}] Nenhum dado para inserir.")
        return True

    for col in COLUNAS_INSERT:
        if col not in df.columns:
            print(f"  [{nome_canal}] ⚠️  Coluna ausente, será inserida como NULL: {col}")
            df[col] = None

    df = preparar_tipos(df, nome_canal)
    df = df.loc[:, COLUNAS_INSERT]

    placeholders = ', '.join(['?' for _ in COLUNAS_INSERT])
    colunas_sql  = ', '.join(COLUNAS_INSERT)
    sql = f"INSERT INTO {TABELA_DESTINO} ({colunas_sql}) VALUES ({placeholders})"

    registros = list(df.itertuples(index=False, name=None))
    total     = len(registros)
    inseridos = 0
    inicio    = time.time()

    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        for i in range(0, total, BATCH_SIZE):
            num_lote = i // BATCH_SIZE + 1
            lote = registros[i:i + BATCH_SIZE]
            try:
                cursor.executemany(sql, lote)
                conn.commit()
                inseridos += len(lote)
                pct = (inseridos / total) * 100
                print(f"  [{nome_canal}] 📥 {inseridos:,}/{total:,} inseridos ({pct:.1f}%)")

            except Exception as e:
                conn.rollback()
                print(f"  [{nome_canal}] ❌ Erro no lote {num_lote}: {e}")

                if inseridos > 0:
                    # Lotes anteriores já foram commitados — limpa tudo do canal no dia
                    print(f"  [{nome_canal}] ⚠️  {inseridos:,} registros já estavam no banco.")
                    print(f"  [{nome_canal}] 🧹 Iniciando limpeza para evitar duplicatas no reprocessamento...")
                    limpar_inserts_do_dia(conn, nome_canal)
                else:
                    print(f"  [{nome_canal}] ↩️  Falhou no primeiro lote — nenhum dado foi inserido.")

                print(f"  [{nome_canal}] ❌ Canal não inserido. Corrija o problema e reprocesse.")
                return False

        tempo = time.time() - inicio
        print(f"  [{nome_canal}] ✅ Insert concluído: {inseridos:,} registros em {tempo:.1f}s")
        return True

    except Exception as e:
        print(f"  [{nome_canal}] ❌ Erro ao preparar cursor: {e}")
        return False

def inserir_analitico(df: pd.DataFrame, conn, nome_canal: str) -> bool:

    if df.empty:
        print(f"  [{nome_canal}] Nenhum dado para inserir.")
        return True

    for col in COLUNAS_INSERT:
        if col not in df.columns:
            print(f"  [{nome_canal}] ⚠️  Coluna ausente, será inserida como NULL: {col}")
            df[col] = None

    df = preparar_tipos(df, nome_canal)
    df = df.loc[:, COLUNAS_INSERT]

    placeholders = ', '.join(['?' for _ in COLUNAS_INSERT])
    colunas_sql  = ', '.join(COLUNAS_INSERT)
    sql = f"INSERT INTO {TABELA_DESTINO} ({colunas_sql}) VALUES ({placeholders})"

    registros = list(df.itertuples(index=False, name=None))
    total     = len(registros)
    inseridos = 0
    inicio    = time.time()

    try:
        cursor = conn.cursor()
        cursor.fast_executemany = False  # 👈 temporário para diagnóstico real

        for i in range(0, total, BATCH_SIZE):
            num_lote = i // BATCH_SIZE + 1
            lote = registros[i:i + BATCH_SIZE]
            try:
                for row in lote:
                    cursor.execute(sql, row)
                conn.commit()
                inseridos += len(lote)
                pct = (inseridos / total) * 100
                print(f"  [{nome_canal}] 📥 {inseridos:,}/{total:,} inseridos ({pct:.1f}%)")

            except Exception as e:
                conn.rollback()
                print(f"  [{nome_canal}] ❌ Erro no lote {num_lote}: {e}")

                if inseridos > 0:
                    print(f"  [{nome_canal}] ⚠️  {inseridos:,} registros já estavam no banco.")
                    print(f"  [{nome_canal}] 🧹 Iniciando limpeza...")
                    limpar_inserts_do_dia(conn, nome_canal)
                else:
                    print(f"  [{nome_canal}] ↩️  Falhou no primeiro lote — nenhum dado inserido.")

                print(f"  [{nome_canal}] ❌ Canal não inserido. Corrija o problema e reprocesse.")
                return False

        tempo = time.time() - inicio
        print(f"  [{nome_canal}] ✅ Insert concluído: {inseridos:,} registros em {tempo:.1f}s")
        return True

    except Exception as e:
        print(f"  [{nome_canal}] ❌ Erro ao preparar cursor: {e}")
        return False