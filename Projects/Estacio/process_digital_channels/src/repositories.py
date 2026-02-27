import numpy as np
import time

TABELA_DESTINO = "analytical_digital_estacio"
BATCH_SIZE = 3000

COLUNAS_INSERT = [
    'CANAL', 'DATA_DISPARO', 'CPF', 'CUSTO', 'CONTATO', 'CONTRATO',
    'CORRESPONDENCIA', 'ATRASO', 'COD_CLI', 'ID_CLIENTE', 'REGIONAL',
    'GRUPO', 'GRUPO_SEGMENTADO', 'PRODUTO_SEGMENTADO', 'SPD', 'BU',
    'MODALIDE', 'STDEBITO', 'STALUNO', 'CURSO', 'APROACAD', 'PRODUTO',
    'ULTRENOV', 'COD_PRODUT', 'FX_ATRASO'
]


def inserir_analitico(df, conn, nome_canal: str) -> bool:
    """
    Insere o DataFrame na tabela analítica em lotes com fast_executemany.
    Retorna True se bem-sucedido, False se falhou.
    """
    if df.empty:
        print(f"  [{nome_canal}] Nenhum dado para inserir.")
        return True

    # Garante colunas faltantes como NULL
    for col in COLUNAS_INSERT:
        if col not in df.columns:
            print(f"  [{nome_canal}] ⚠️  Coluna ausente, será inserida como NULL: {col}")
            df[col] = None

    placeholders = ', '.join(['?' for _ in COLUNAS_INSERT])
    colunas_sql   = ', '.join(COLUNAS_INSERT)
    sql = f"INSERT INTO {TABELA_DESTINO} ({colunas_sql}) VALUES ({placeholders})"

    df = df[COLUNAS_INSERT].replace({np.nan: None})
    registros = list(df.itertuples(index=False, name=None))

    total    = len(registros)
    inseridos = 0
    inicio   = time.time()

    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        for i in range(0, total, BATCH_SIZE):
            lote = registros[i:i + BATCH_SIZE]
            try:
                cursor.executemany(sql, lote)
                conn.commit()
                inseridos += len(lote)
                pct = (inseridos / total) * 100
                print(f"  [{nome_canal}] 📥 {inseridos:,}/{total:,} registros inseridos ({pct:.1f}%)")

            except Exception as e:
                conn.rollback()
                print(f"  [{nome_canal}] ❌ Erro no lote {i // BATCH_SIZE + 1}: {e}")
                print(f"  [{nome_canal}] ↩️  Rollback efetuado. Registros perdidos neste lote: {len(lote)}")
                return False

        tempo = time.time() - inicio
        print(f"  [{nome_canal}] ✅ Insert concluído: {inseridos:,} registros em {tempo:.1f}s")
        return True

    except Exception as e:
        print(f"  [{nome_canal}] ❌ Erro ao preparar cursor: {e}")
        return False