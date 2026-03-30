import pandas as pd
import time
from Estacio.process_digital_channels.src.repositories import inserir_analitico


def preparar_canal(df: pd.DataFrame, canal: str) -> pd.DataFrame:
    df = df.copy()
    df['CPF'] = df['CPF'].str.strip()

    if canal in ['SMS', 'RCS', 'WHATS']:
        df = df[['DATA_DISPARO', 'CPF', 'CUSTO', 'TELEFONE']].rename(columns={'TELEFONE': 'CONTATO'})

    elif canal == 'EMAIL':
        df = df[['DATA', 'CPF', 'CUSTO', 'EMAIL']].rename(columns={
            'DATA': 'DATA_DISPARO',
            'EMAIL': 'CONTATO'
        })

    df['CANAL'] = canal
    return df

def cruzar_mailing_canal(df_mailing: pd.DataFrame, df_canal: pd.DataFrame, nome_canal: str, conn) -> bool:

    inicio = time.time()
    print(f"\n{'='*55}")
    print(f"  [{nome_canal}] Iniciando cruzamento...")

    try:
        df_canal_prep = preparar_canal(df_canal, nome_canal)

        df_mailing = df_mailing.copy()
        df_mailing['CPF'] = df_mailing['CPF'].str.strip()

        cpfs_canal = df_canal_prep['CPF'].unique()
        df_mailing_filtrado = df_mailing[df_mailing['CPF'].isin(cpfs_canal)]
        print(f"  [{nome_canal}] Mailing filtrado: {df_mailing_filtrado.shape[0]:,} linhas")

        df_mailing_dedup = (
            df_mailing_filtrado
            .assign(prioridade_grupo=df_mailing_filtrado['GRUPO'].apply(lambda x: 1 if x == 'ESTACIO' else 2))
            .sort_values(['CPF', 'DATA', 'prioridade_grupo', 'ATRASO', 'CONTRATO'],
                         ascending=[True, True, True, False, True])
            .drop_duplicates(subset=['CPF', 'DATA'], keep='first')
            .drop(columns='prioridade_grupo')
        )
        print(f"  [{nome_canal}] Mailing deduplicado: {df_mailing_dedup.shape[0]:,} linhas")

        df_merge = df_canal_prep.merge(
            df_mailing_dedup,
            left_on=['CPF', 'DATA_DISPARO'],
            right_on=['CPF', 'DATA'],
            how='left',
            indicator=True
        )

        df_com = df_merge[df_merge['_merge'] == 'both'].drop(columns='_merge').reset_index(drop=True)
        df_sem = df_merge[df_merge['_merge'] == 'left_only'].drop(columns='_merge').reset_index(drop=True)

        df_com['CORRESPONDENCIA'] = 1
        df_sem['CORRESPONDENCIA'] = 0

        df_final = pd.concat([df_com, df_sem], ignore_index=True)

        # 1. Converte colunas category para str
        cols_category = df_final.select_dtypes(include='category').columns.tolist()
        if cols_category:
            print(f"  [{nome_canal}] ⚠️  Convertendo category para str: {cols_category}")
            df_final[cols_category] = df_final[cols_category].astype(str)

        # 2. Filtra apenas as colunas que existem na tabela do banco
        from Estacio.process_digital_channels.src.repositories import COLUNAS_INSERT
        colunas_presentes = list(dict.fromkeys([c for c in COLUNAS_INSERT if c in df_final.columns]))
        colunas_ausentes  = [c for c in COLUNAS_INSERT if c not in df_final.columns]

        if colunas_ausentes:
            print(f"  [{nome_canal}] ⚠️  Colunas ausentes no df (serão NULL): {colunas_ausentes}")
            for col in colunas_ausentes:
                df_final[col] = None

        # Usa .loc para seleção segura e reindexe as colunas
        df_final = df_final.loc[:, colunas_presentes].copy()

        tempo_cruz = time.time() - inicio
        print(f"  [{nome_canal}] Com correspondência : {df_com.shape[0]:,} linhas")
        print(f"  [{nome_canal}] Sem correspondência : {df_sem.shape[0]:,} linhas — custo fora: R$ {df_sem['CUSTO'].sum():,.2f}")
        print(f"  [{nome_canal}] Cruzamento concluído em {tempo_cruz:.1f}s")

        print(f"  [{nome_canal}] Iniciando insert no banco...")
        return inserir_analitico(df_final, conn, nome_canal)

    except Exception as e:
        print(f"  [{nome_canal}] ❌ Erro inesperado no cruzamento: {e}")
        return False


def cruzar_todos_canais(df_mailing: pd.DataFrame, canais: dict, conn) -> None:
    """
    Itera pelos canais, cruza e insere cada um imediatamente.
    Canais com erro são registrados mas não interrompem os demais.
    """
    from datetime import datetime
    from Estacio.process_digital_channels.src.repositories import TABELA_DESTINO

    inicio_total  = time.time()
    canais_sucesso = []
    canais_erro    = []

    for nome_canal, df_canal in canais.items():
        sucesso = cruzar_mailing_canal(df_mailing, df_canal, nome_canal, conn)
        if sucesso:
            canais_sucesso.append(nome_canal)
        else:
            canais_erro.append(nome_canal)
            print(f"\n  ⚠️  [{nome_canal}] Canal ignorado. Continuando para o próximo...")

    tempo_total = time.time() - inicio_total

    print(f"\n{'='*55}")
    print(f"  RESUMO FINAL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  ✅ Canais inseridos com sucesso : {canais_sucesso}")
    print(f"  ❌ Canais com erro              : {canais_erro}")
    print(f"  ⏱️  Tempo total                  : {tempo_total:.1f}s")

    if canais_erro:
        canais_fmt = ', '.join([f"'{c}'" for c in canais_erro])
        print(f"\n  ⚠️  ATENÇÃO: Reprocesse os canais com erro.")
        print(f"  Antes de reinserir, delete os registros do dia:")
        print(f"  DELETE FROM {TABELA_DESTINO}")
        print(f"  WHERE CANAL IN ({canais_fmt})")
        print(f"  AND CAST(DT_CARGA AS DATE) = CAST(GETDATE() AS DATE)")