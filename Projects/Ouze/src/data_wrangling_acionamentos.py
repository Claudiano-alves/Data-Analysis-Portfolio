import pandas as pd
from utils import unir_dataframes, salvar_log, registrar_tempo


# SUBSTITUÍ POR 0 E 1
@registrar_tempo("Substituindo tabulações por 0 e 1")
def tratar_acionamentos_tabulacao(df_tabualacao_aciona):
    colunas_binarias = ['CPC', 'CPCA', 'PROMESSA']

    # Converte todas para int
    df_tabualacao_aciona[colunas_binarias] = df_tabualacao_aciona[colunas_binarias].astype(int)
    salvar_log("Finalizado!")

    return df_tabualacao_aciona

# CONFERE TABULAÇÃO AOS ACIONAMENTOS
@registrar_tempo("Conferindo tabulações aos acionamentos")
def confere_tabulacao_acionamentos(df_tab_acionamentos, df_tabulacao_aciona):
    """
    Faz o merge entre tab_acionamentos e tabulacao_aciona e adiciona flags de CPC, CPCA e PROMESSA
    
    Args:
        df_tab_acionamentos (pd.DataFrame): DataFrame com acionamentos (coluna COD_ACIONA)
        df_tabulacao_aciona (pd.DataFrame): DataFrame com tabulações (coluna COD_ACIONA)
    
    Returns:
        pd.DataFrame: DataFrame merged com flags de tabulação e coluna ACIONAMENTOS
    """
    # Fazer cópia para não alterar os originais
    df_resultado = df_tab_acionamentos.copy()
    df_tabula = df_tabulacao_aciona.copy()
    
    # Garantir que os códigos estão no mesmo formato (string)
    df_resultado['COD_ACIONA'] = df_resultado['COD_ACIONA'].astype(str)
    df_tabula['COD_ACIONA'] = df_tabula['COD_ACIONA'].astype(str)
    
    salvar_log("="*80)
    salvar_log(f"📊 Antes do merge - Acionamentos: {len(df_resultado):,} | Tabulações: {len(df_tabula):,}")
    
    # Fazer o merge (trazendo DESC_ACIONA também)
    df_resultado = df_resultado.merge(
        df_tabula[['COD_ACIONA', 'DESC_ACIONA', 'CPC', 'CPCA', 'PROMESSA']],
        on='COD_ACIONA',
        how='left'
    )
    
    # Converter flags para int (substituindo NaN por 0)
    df_resultado[['CPC', 'CPCA', 'PROMESSA']] = df_resultado[['CPC', 'CPCA', 'PROMESSA']].fillna(0).astype(int)
    
    # Adicionar coluna de contagem de acionamentos
    df_resultado['ACIONAMENTOS'] = 1
    
    salvar_log(f"📊 Após merge: {len(df_resultado):,}")
    salvar_log(f"📊 Acionamentos com DESC_ACIONA: {df_resultado['DESC_ACIONA'].notna().sum():,}")
    salvar_log("="*80)
    
    return df_resultado

# ENRIQUECIMENTO DOS ACIONAMENTOS
@registrar_tempo("Enriquecendo acionamentos mailing hist e calendário")
def enriquecer_acionamentos(df_acionamentos, df_mailing_hist, df_dw_calendario, 
                           separar_inconsistencias_flag=True):
    """
    Enriquece a base de acionamentos com informações de mailing_hist e calendário
    
    Args:
        df_acionamentos (pd.DataFrame): DataFrame de acionamentos tabulados (resultado de confere_tabulacao_acionamentos)
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist com FX_ATRASO
        df_dw_calendario (pd.DataFrame): DataFrame de calendário
        separar_inconsistencias_flag (bool): Se True, separa inconsistências e retorna múltiplos DataFrames
    
    Returns:
        Se separar_inconsistencias_flag=True:
            tuple: (df_limpo, df_sem_fx_atraso, df_sem_descricao)
        Se separar_inconsistencias_flag=False:
            pd.DataFrame: DataFrame enriquecido completo
    """
    df_resultado = df_acionamentos.copy()
    salvar_log("="*80)
    # ============================================
    # CLASSIFICAR ORIGEM DO ACIONAMENTO
    # ============================================
    # Verifica se a coluna COD_RECUP existe
    if 'COD_RECUP' in df_resultado.columns:
        salvar_log(f"📊 Classificando origem do acionamento...")
        df_resultado['ORIGEM'] = df_resultado['COD_RECUP'].apply(
            lambda x: 'ROBÔ' if x == 1 else 'HUMANO'
        )
        salvar_log(f"   ✓ Robô: {(df_resultado['ORIGEM'] == 'ROBÔ').sum():,}")
        salvar_log(f"   ✓ Humano: {(df_resultado['ORIGEM'] == 'HUMANO').sum():,}")
    else:
        salvar_log(f"⚠️  Coluna COD_RECUP não encontrada. Origem não será classificada.")
        df_resultado['ORIGEM'] = None
    
    # ============================================
    # ENRIQUECER COM MAILING_HIST
    # ============================================
    df_mailing_temp = df_mailing_hist[['CONTRATO', 'DATA', 'FX_ATRASO']].copy()
    df_mailing_temp = df_mailing_temp.rename(columns={
        'CONTRATO': 'CONTRATO_FIN',
        'DATA': 'DATA_ACIONA'
    })
    
    # Padronizar data antes do merge
    df_resultado['DATA_ACIONA'] = pd.to_datetime(df_resultado['DATA_ACIONA']).dt.date
    df_mailing_temp['DATA_ACIONA'] = pd.to_datetime(df_mailing_temp['DATA_ACIONA']).dt.date
    
    salvar_log(f"📊 Merge com mailing_hist...")
    df_resultado = df_resultado.merge(
        df_mailing_temp,
        on=['CONTRATO_FIN', 'DATA_ACIONA'],
        how='left'
    )
    salvar_log(f"   ✓ Registros: {len(df_resultado):,}")
    
    # ============================================
    # REMOVER DUPLICATAS GERADAS PELO MERGE
    # ============================================
    registros_antes = len(df_resultado)
    
    # Identifica todas as colunas presentes dinamicamente (exceto as que podem ter duplicatas)
    colunas_duplicatas = [col for col in df_resultado.columns if col not in []]
    
    df_resultado = df_resultado.drop_duplicates(
        subset=colunas_duplicatas,
        keep='first'
    )
    
    duplicatas_removidas = registros_antes - len(df_resultado)
    if duplicatas_removidas > 0:
        salvar_log(f"   🔧 Removidas {duplicatas_removidas:,} duplicatas do merge")
        salvar_log(f"   ✓ Registros únicos: {len(df_resultado):,}")

    # ============================================
    # ENRIQUECER COM DW_CALENDARIO
    # ============================================
    df_resultado['DATA_ACIONA'] = pd.to_datetime(df_resultado['DATA_ACIONA']).dt.date
    df_dw_calendario['dt_data'] = pd.to_datetime(df_dw_calendario['dt_data']).dt.date

    df_calendario_reduzido = df_dw_calendario[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]

    df_resultado = df_resultado.merge(
        df_calendario_reduzido,
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='left'
    )

    df_resultado.drop(columns='dt_data', inplace=True)
    
    salvar_log(f"   ✓ Registros finais: {len(df_resultado):,}")
    salvar_log(f"   ✓ Com FX_ATRASO: {df_resultado['FX_ATRASO'].notna().sum():,}")
    salvar_log(f"   ✓ Com DESC_ACIONA: {df_resultado['DESC_ACIONA'].notna().sum():,}")
    salvar_log(f"   ✓ Com ORIGEM: {df_resultado['ORIGEM'].notna().sum():,}")
    salvar_log("="*80)

    # ============================================
    # SEPARAR INCONSISTÊNCIAS (SE SOLICITADO)
    # ============================================
    if separar_inconsistencias_flag:
        return separar_inconsistencias(df_resultado)
    else:
        return df_resultado

@registrar_tempo("Separando inconsistências")
def separar_inconsistencias(df_acionamentos):
    """
    Separa acionamentos com inconsistências em DataFrames específicos
    
    Args:
        df_acionamentos (pd.DataFrame): DataFrame de acionamentos enriquecido
    
    Returns:
        tuple: (df_limpo, df_sem_fx_atraso, df_sem_descricao, df_sem_origem)
    """
    # Identificar inconsistências
    sem_fx_atraso = df_acionamentos['FX_ATRASO'].isna()
    sem_descricao = df_acionamentos['DESC_ACIONA'].isna()
    sem_origem = df_acionamentos['ORIGEM'].isna()
    
    # Separar DataFrames
    df_sem_fx_atraso = df_acionamentos[sem_fx_atraso].copy()
    df_sem_descricao = df_acionamentos[sem_descricao].copy()
    df_sem_origem = df_acionamentos[sem_origem].copy()
    
    # DataFrame limpo (sem nenhuma inconsistência)
    df_limpo = df_acionamentos[~(sem_fx_atraso | sem_descricao | sem_origem)].copy()
    salvar_log("="*80)
    # Relatório
    salvar_log(f"\n📋 ANÁLISE DE INCONSISTÊNCIAS:")
    salvar_log(f"   ✓ Registros limpos: {len(df_limpo):,}")
    salvar_log(f"   ⚠️  Sem FX_ATRASO (fora da mailing): {len(df_sem_fx_atraso):,}")
    salvar_log(f"   ⚠️  Sem DESC_ACIONA (erro de tabulação): {len(df_sem_descricao):,}")
    salvar_log(f"   ⚠️  Sem ORIGEM (fora do discador): {len(df_sem_origem):,}")
    salvar_log(f"   📊 Total de registros: {len(df_acionamentos):,}")
    salvar_log("="*80)

    return df_limpo, df_sem_fx_atraso, df_sem_descricao, df_sem_origem

# AGRUPAMENTO E CONTAGEM DE CPFs ÚNICOS POR ORIGEM

# DAILY
def acionamentos_unique_origem_fxAtraso(df_acionamentos_enriquecido, df_dw_calendario):

    df = df_acionamentos_enriquecido.copy()
    df['TABULACAO_SCORE'] = (
        df['PROMESSA'].astype(int) * 3 +
        df['CPCA'].astype(int) * 2 +
        df['CPC'].astype(int) * 1
    )

    # Ordenar por score (maior score primeiro)
    df_ordenado = df.sort_values(
        ['CPF_DEV', 'DATA_ACIONA', 'TABULACAO_SCORE'],
        ascending=[True, True, False]
    )

    # Manter apenas o melhor score por CPF_DEV + DATA_ACIONA + ORIGEM
    df_unique_filtrado = df_ordenado.drop_duplicates(
        subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM'],
        keep='first'
    ).copy()

    df_unique_filtrado.loc[:, 'ACIONAMENTOS'] = df_unique_filtrado['ACIONAMENTOS'].astype(int)
    df_unique_filtrado.loc[:, 'CPC'] = df_unique_filtrado['CPC'].astype(int)
    df_unique_filtrado.loc[:, 'CPCA'] = df_unique_filtrado['CPCA'].astype(int)
    df_unique_filtrado.loc[:, 'PROMESSA'] = df_unique_filtrado['PROMESSA'].astype(int)

    # CPFs únicos com pelo menos 1 acionamento
    df_aciona = df_unique_filtrado[df_unique_filtrado['ACIONAMENTOS'] >= 1].drop_duplicates(subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM']).copy()

    # CPFs únicos com pelo menos 1 CPC
    df_cpc = df_unique_filtrado[df_unique_filtrado['CPC'] >= 1].drop_duplicates(subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM']).copy()

    # CPFs únicos com pelo menos 1 CPCA
    df_cpca = df_unique_filtrado[df_unique_filtrado['CPCA'] >= 1].drop_duplicates(subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM']).copy()

    # CPFs únicos com pelo menos 1 promessa
    df_promessa = df_unique_filtrado[df_unique_filtrado['PROMESSA'] >= 1].drop_duplicates(subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM']).copy()

    def contar_cpfs(df_filtrado, nome_coluna):
        return (
            df_filtrado.groupby(
                ['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
                observed=True
            )
            .agg({ 
                'CPF_DEV': 'nunique',
                'VALORPRIN_FIN': 'sum'
            })
            .rename(columns={
                'CPF_DEV': nome_coluna,
                'VALORPRIN_FIN': f'VALORPRIN_FIN_{nome_coluna}'
            })
            .reset_index()
        )

    df_contagem_aciona = contar_cpfs(df_aciona, 'ACIONAMENTOS')
    df_contagem_cpc = contar_cpfs(df_cpc, 'CPC')
    df_contagem_cpca = contar_cpfs(df_cpca, 'CPCA')
    df_contagem_promessa = contar_cpfs(df_promessa, 'PROMESSA')

    from functools import reduce

    dfs = [df_contagem_aciona, df_contagem_cpc, df_contagem_cpca, df_contagem_promessa]

    df_final = reduce(lambda left, right: pd.merge(
        left, right,
        on=['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
        how='outer'
    ), dfs)

    # Preencher apenas as colunas numéricas com 0
    colunas_metricas = ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']
    colunas_valor = ['VALORPRIN_FIN_ACIONAMENTOS', 'VALORPRIN_FIN_CPC', 'VALORPRIN_FIN_CPCA', 'VALORPRIN_FIN_PROMESSA']
    
    df_final[colunas_metricas] = df_final[colunas_metricas].fillna(0).astype(int)
    df_final[colunas_valor] = df_final[colunas_valor].fillna(0)

    # Fazer merge com o calendário usando os nomes corretos das colunas
    df_final = df_final.merge(
        df_dw_calendario[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']].drop_duplicates(),
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='left'
    ).drop('dt_data', axis=1)

    return df_final

# AGRUPAMENTO E CONTAGEM DE CPFs ÚNICOS FX_ATRASO
def acionamentos_unique_fxAtraso(df_acionamentos_com_calendario, df_dw_calendario):
    # Usar .copy() para evitar SettingWithCopyWarning
    df = df_acionamentos_com_calendario.copy()
    
    df['TABULACAO_SCORE'] = (
        df['PROMESSA'].astype(int) * 3 +
        df['CPCA'].astype(int) * 2 +
        df['CPC'].astype(int) * 1
    )
    
    # Ordenar por score (maior score primeiro)
    df_ordenado = df.sort_values(
        ['CPF_DEV', 'DATA_ACIONA', 'TABULACAO_SCORE'],
        ascending=[True, True, False]
    )
    
    # Manter apenas o melhor score por CPF_DEV + DATA_ACIONA
    df_unique_filtrado = df_ordenado.drop_duplicates(
        subset=['CPF_DEV', 'DATA_ACIONA'],
        keep='first'
    ).copy()
    
    # Usar .loc para evitar SettingWithCopyWarning
    df_unique_filtrado.loc[:, 'ACIONAMENTOS'] = df_unique_filtrado['ACIONAMENTOS'].astype(int)
    df_unique_filtrado.loc[:, 'CPC'] = df_unique_filtrado['CPC'].astype(int)
    df_unique_filtrado.loc[:, 'CPCA'] = df_unique_filtrado['CPCA'].astype(int)
    df_unique_filtrado.loc[:, 'PROMESSA'] = df_unique_filtrado['PROMESSA'].astype(int)
    
    # CPFs únicos com pelo menos 1 de cada métrica
    df_aciona = df_unique_filtrado[df_unique_filtrado['ACIONAMENTOS'] >= 1].copy()
    df_cpc = df_unique_filtrado[df_unique_filtrado['CPC'] >= 1].copy()
    df_cpca = df_unique_filtrado[df_unique_filtrado['CPCA'] >= 1].copy()
    df_promessa = df_unique_filtrado[df_unique_filtrado['PROMESSA'] >= 1].copy()
    
    def contar_cpfs(df_filtrado, nome_coluna):
        df_contagem = (
            df_filtrado.groupby(
                ['DATA_ACIONA', 'FX_ATRASO'],
                observed=True
            )
            .agg({ 
                'CPF_DEV': 'nunique',
                'VALORPRIN_FIN': 'sum'  # Adiciona a soma do valor principal
            })
            .rename(columns={
                'CPF_DEV': nome_coluna,
                'VALORPRIN_FIN': f'VALORPRIN_FIN_{nome_coluna}'  # Nome específico por métrica
            })
            .reset_index()
        )
        df_contagem.insert(2, 'ORIGEM', 'UNIQUE')
        return df_contagem
    
    df_contagem_aciona = contar_cpfs(df_aciona, 'ACIONAMENTOS')
    df_contagem_cpc = contar_cpfs(df_cpc, 'CPC')
    df_contagem_cpca = contar_cpfs(df_cpca, 'CPCA')
    df_contagem_promessa = contar_cpfs(df_promessa, 'PROMESSA')
    
    from functools import reduce
    
    dfs = [df_contagem_aciona, df_contagem_cpc, df_contagem_cpca, df_contagem_promessa]
    
    df_final_unique = reduce(lambda left, right: pd.merge(
        left, right,
        on=['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
        how='outer'
    ), dfs)
    
    # Preencher apenas as colunas numéricas com 0
    colunas_metricas = ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']
    colunas_valor = ['VALORPRIN_FIN_ACIONAMENTOS', 'VALORPRIN_FIN_CPC', 'VALORPRIN_FIN_CPCA', 'VALORPRIN_FIN_PROMESSA']
    
    df_final_unique[colunas_metricas] = df_final_unique[colunas_metricas].fillna(0).astype(int)
    df_final_unique[colunas_valor] = df_final_unique[colunas_valor].fillna(0)
    
    # Fazer merge com o calendário
    df_final_unique = df_final_unique.merge(
        df_dw_calendario[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']].drop_duplicates(),
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='left'
    ).drop('dt_data', axis=1)
    
    return df_final_unique

def acionamentos_esforco_origem_fxAtraso(df_acionamentos_enriquecido, df_dw_calendario):

    df_esforco = df_acionamentos_enriquecido.copy()

    df_esforco['TABULACAO_SCORE'] = (
        df_esforco['PROMESSA'].astype(int) * 3 +
        df_esforco['CPCA'].astype(int) * 2 +
        df_esforco['CPC'].astype(int) * 1
    )
    
    # Ordenar por score (maior score primeiro)
    df_ordenado = df_esforco.sort_values(
        ['CPF_DEV', 'DATA_ACIONA', 'TABULACAO_SCORE'],
        ascending=[True, True, False]
    )
    
    # Manter apenas o melhor score por CPF_DEV + DATA_ACIONA
    df_ordenado = df_ordenado.drop_duplicates(
        subset=['CPF_DEV', 'DATA_ACIONA', 'TABULACAO_SCORE'],
        keep='first'
    ).copy()
    
    df_esforco.loc[:, 'ACIONAMENTOS'] = df_esforco['ACIONAMENTOS'].astype(int)
    df_esforco.loc[:, 'CPC'] = df_esforco['CPC'].astype(int)
    df_esforco.loc[:, 'CPCA'] = df_esforco['CPCA'].astype(int)
    df_esforco.loc[:, 'PROMESSA'] = df_esforco['PROMESSA'].astype(int)

    # Filtrar registros com pelo menos 1 de cada métrica (sem remover duplicados)
    df_aciona = df_esforco[df_esforco['ACIONAMENTOS'] >= 1].copy()
    df_cpc = df_esforco[df_esforco['CPC'] >= 1].copy()
    df_cpca = df_esforco[df_esforco['CPCA'] >= 1].copy()
    df_promessa = df_esforco[df_esforco['PROMESSA'] >= 1].copy()

    def contar_esforco(df_filtrado, nome_coluna):
        return (
            df_filtrado.groupby(
                ['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
                observed=True
            )
            .agg({ 
                nome_coluna: 'sum',  # Soma o total de acionamentos/CPC/CPCA/PROMESSA
                'VALORPRIN_FIN': 'sum'
            })
            .rename(columns={
                'VALORPRIN_FIN': f'VALORPRIN_FIN_{nome_coluna}'
            })
            .reset_index()
        )

    df_contagem_aciona = contar_esforco(df_aciona, 'ACIONAMENTOS')
    df_contagem_cpc = contar_esforco(df_cpc, 'CPC')
    df_contagem_cpca = contar_esforco(df_cpca, 'CPCA')
    df_contagem_promessa = contar_esforco(df_promessa, 'PROMESSA')

    from functools import reduce

    dfs = [df_contagem_aciona, df_contagem_cpc, df_contagem_cpca, df_contagem_promessa]

    df_final = reduce(lambda left, right: pd.merge(
        left, right,
        on=['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
        how='outer'
    ), dfs)

    # Preencher apenas as colunas numéricas com 0
    colunas_metricas = ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']
    colunas_valor = ['VALORPRIN_FIN_ACIONAMENTOS', 'VALORPRIN_FIN_CPC', 'VALORPRIN_FIN_CPCA', 'VALORPRIN_FIN_PROMESSA']
    
    df_final[colunas_metricas] = df_final[colunas_metricas].fillna(0).astype(int)
    df_final[colunas_valor] = df_final[colunas_valor].fillna(0)

    # Fazer merge com o calendário usando os nomes corretos das colunas
    df_final = df_final.merge(
        df_dw_calendario[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']].drop_duplicates(),
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='left'
    ).drop('dt_data', axis=1)
    
    df_final['FX_ATRASO'] = 'Esforço'

    return df_final

# FUNIL
@registrar_tempo("Funil de acionamentos fxAtraso e origem humano")
def acionamentos_fxAtraso_origem_humano(df_acionamentos_enriquecido_limpo, df_dw_calendario):
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_acionamentos_enriquecido_limpo.copy()
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA_ACIONA'].sort_values().unique()
    resultados = []

    for data in datas_unicas:
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo
        df_intervalo = df[(df['DATA_ACIONA'] >= inicio_mes) & (df['DATA_ACIONA'] <= data)].copy()
        
        # 2. Calcular score
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 3 +
            df_intervalo['CPCA'].astype(int) * 2 +
            df_intervalo['CPC'].astype(int) * 1
        )
        
        # 3. Ordenar e manter melhor por CPF + FX_ATRASO + ORIGEM
        df_intervalo = df_intervalo.sort_values(
            ['CPF_DEV', 'FX_ATRASO', 'ORIGEM', 'TABULACAO_SCORE'],
            ascending=[True, True, True, False]
        )
        
        df_filtrado = df_intervalo.drop_duplicates(
            subset=['CPF_DEV', 'FX_ATRASO', 'ORIGEM'],
            keep='first'
        ).copy()
        
        # 4. Agrupar por FX_ATRASO e ORIGEM
        agrupado = df_filtrado.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA_ACIONA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Adicionar colunas TRABALHADO zeradas
    df_final['TRABALHADO'] = 0
    df_final['VALORPRIN_FIN_TRABALHADO'] = 0.0

    # Cruzar com calendário
    df_dw_calendario['dt_data'] = pd.to_datetime(df_dw_calendario['dt_data'])
    df_final = df_final.merge(
        df_dw_calendario[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA_ACIONA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    return df_final

@registrar_tempo("Funil de acionamentos unique humano")
def acionamentos_unique_humano(df_acionamentos_enriquecido_limpo, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score) por FX_ATRASO e ORIGEM.
    Versão para df_acionamentos_enriquecido_limpo (expert).
    
    Args:
        df_acionamentos_enriquecido_limpo (pd.DataFrame): DataFrame enriquecido expert
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_acionamentos_enriquecido_limpo.copy()
    
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA_ACIONA'].sort_values().unique()
    resultados = []

    salvar_log("="*80)
    salvar_log(f"📊 Processando acumulado mensal ÚNICO (melhor score por CPF) para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA_ACIONA'] >= inicio_mes) & (df['DATA_ACIONA'] <= data)].copy()
        
        # 2. Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 3 +
            df_intervalo['CPCA'].astype(int) * 2 +
            df_intervalo['CPC'].astype(int) * 1
        )
        
        # 3. Ordenar por CPF e score (maior score primeiro)
        df_intervalo = df_intervalo.sort_values(
            ['CPF_DEV', 'TABULACAO_SCORE'],
            ascending=[True, False]
        )
        
        # 4. Manter apenas o melhor score por CPF (unique)
        df_unique = df_intervalo.drop_duplicates(
            subset=['CPF_DEV'],
            keep='first'
        ).copy()
        
        # 5. Agrupar por FX_ATRASO e ORIGEM
        agrupado = df_unique.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA_ACIONA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Adicionar colunas TRABALHADO zeradas
    df_final['TRABALHADO'] = 0
    df_final['VALORPRIN_FIN_TRABALHADO'] = 0.0

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA_ACIONA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}")
    salvar_log(f"\n📈 Totais acumulados ÚNICOS (última data):")
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['ACIONAMENTOS'].sum():,}")
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPC'].sum():,}")
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPCA'].sum():,}")
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['PROMESSA'].sum():,}")
    salvar_log("="*80)

    df_final['FX_ATRASO'] = 'Unique'
    return df_final

@registrar_tempo("Funil de acionamentos esforço humano")
def acionamentos_esforco_humano(df_acionamentos_enriquecido_limpo, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos por FX_ATRASO e ORIGEM.
    Versão para df_acionamentos_enriquecido_limpo (expert).
    
    Args:
        df_acionamentos_enriquecido_limpo (pd.DataFrame): DataFrame enriquecido expert
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)
    
    df = df_acionamentos_enriquecido_limpo.copy()
    
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])
    
    # Datas únicas ordenadas
    datas_unicas = df['DATA_ACIONA'].sort_values().unique()
    resultados = []
    
    salvar_log("="*80)
    salvar_log(f"📊 Processando acumulado mensal para {len(datas_unicas)} datas...")
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA_ACIONA'] >= inicio_mes) & (df['DATA_ACIONA'] <= data)].copy()
        
        # 2. Agrupar por FX_ATRASO e ORIGEM (sem deduplicação, soma tudo)
        agrupado = df_intervalo.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA_ACIONA'] = data
        resultados.append(agrupado)
    
    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Adicionar colunas TRABALHADO zeradas
    df_final['TRABALHADO'] = 0
    df_final['VALORPRIN_FIN_TRABALHADO'] = 0.0

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])
    
    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    
    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    
    # Reordenar colunas
    colunas_ordenadas = [
        'DATA_ACIONA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}")
    salvar_log(f"\n📈 Totais acumulados:")
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['ACIONAMENTOS'].sum():,}")
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPC'].sum():,}")
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPCA'].sum():,}")
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['PROMESSA'].sum():,}")
    salvar_log("="*80)

    df_final['FX_ATRASO'] = 'Esforço'
    return df_final

def acionamentos_duplicados(df_acionamentos_enriquecido_limpo):
    df_acionamentos_score = df_acionamentos_enriquecido_limpo.copy()

    # Criar coluna de score
    df_acionamentos_score['TABULACAO_SCORE'] = (
        df_acionamentos_score['PROMESSA'].astype(int) * 3 +
        df_acionamentos_score['CPCA'].astype(int) * 2 +
        df_acionamentos_score['CPC'].astype(int) * 1
    )

    # Identificar CPF + DATA com mais de um score
    cpf_data_score_counts = df_acionamentos_score.groupby(['CONTRATO_FIN', 'DATA_ACIONA'])['TABULACAO_SCORE'].nunique()

    # Filtrar os casos com mais de um score (duplicidade com variação)
    cpf_data_com_conflito = cpf_data_score_counts[cpf_data_score_counts > 1].reset_index()

    # Remover a coluna de score para evitar duplicação no merge
    cpf_data_com_conflito = cpf_data_com_conflito.drop(columns='TABULACAO_SCORE')

    # Filtrar registros completos com esses casos
    df_conflitos_score = df_acionamentos_score.merge(cpf_data_com_conflito, on=['CONTRATO_FIN', 'DATA_ACIONA'], how='inner')
    return df_conflitos_score

def acionamentos_humano(df_tab_acionamentos, df_tabulacao_aciona, df_dw_calendario, df_maling_hist):
    df_tabulacao_aciona = tratar_acionamentos_tabulacao(df_tabulacao_aciona)
    df_acionamentos_tabulados = confere_tabulacao_acionamentos(df_tab_acionamentos, df_tabulacao_aciona)
    df_acionamentos_enriquecido_limpo, df_acion_semFaixa_humano, df_acion_semDescricao_humano, df_acion_semOrigem_humano = enriquecer_acionamentos(df_acionamentos_tabulados, df_maling_hist, df_dw_calendario)

    df_acionamentos_fxAtraso_origem_humano = acionamentos_fxAtraso_origem_humano(df_acionamentos_enriquecido_limpo, df_dw_calendario)
    df_acionamentos_unique_humano = acionamentos_unique_humano(df_acionamentos_enriquecido_limpo, df_dw_calendario)
    df_acionamentos_esforco_humano = acionamentos_esforco_humano(df_acionamentos_enriquecido_limpo, df_dw_calendario)

    df_acionamentos_humano = unir_dataframes(df_acionamentos_fxAtraso_origem_humano, df_acionamentos_unique_humano, df_acionamentos_esforco_humano)

    df_analitico_acionamentos_humano = df_acionamentos_enriquecido_limpo.copy()
    return df_acionamentos_humano, df_analitico_acionamentos_humano, df_acion_semFaixa_humano, df_acion_semDescricao_humano, df_acion_semOrigem_humano


# ===== USO DA FUNÇÃO =====
# df_resultado = gerar_visao_acionamentos(df_acionamentos_enriquecido)

# # Visualizar resultado
# print(df_resultado.head(20))
# print(f"\nTotal de registros: {len(df_resultado)}")
# print(f"\nRegistros por visão:\n{df_resultado['Visão'].value_counts()}")