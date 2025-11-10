import pandas as pd
from src.utils import unir_dataframes, salvar_log, registrar_tempo

@registrar_tempo("Enriquecimento base de discagens trestto")
def enriquecer_discagens_trestto(df_discagens_trestto, df_mailing_hist, df_dw_calendario):
    """
    Enriquece o DataFrame de discagens Trestto com dados de mailing_hist e calendário.
    Retorna dois DataFrames: um com FX_ATRASO e outro sem.
    
    Args:
        df_discagens_trestto (pd.DataFrame): DataFrame com discagens Trestto,
            deve conter as colunas 'CPF', 'DATA', 'DISCAGEM', 'ALO', 'CPC', 'CPCA', 'PROMESSA'
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing,
            deve conter as colunas 'CPF', 'DATA', 'PRODUTO', 'FX_ATRASO', 'VALORPRIN_FIN'
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário,
            deve conter as colunas 'dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    
    Returns:
        tuple: (df_com_fx_atraso, df_sem_fx_atraso)
            - df_com_fx_atraso: DataFrame enriquecido com FX_ATRASO preenchido
            - df_sem_fx_atraso: DataFrame enriquecido sem FX_ATRASO (CPFs fora da mailing)
    """
    
    df = df_discagens_trestto.copy()
    df_mailing = df_mailing_hist.copy()
    
    # Converter DATA para o mesmo tipo
    df['DATA'] = pd.to_datetime(df['DATA']).dt.date
    df_mailing['DATA'] = pd.to_datetime(df_mailing['DATA']).dt.date
    
    # Garantir que CPF está como string
    df['CPF'] = df['CPF'].astype(str).str.strip()
    df_mailing['CPF'] = df_mailing['CPF'].astype(str).str.strip()
    
    salvar_log("="*80)
    salvar_log(f"📊 Antes da consolidação - Trestto: {len(df):,}")
    
    # Consolidar Trestto antes do merge (por DATA + CPF)
    colunas_metricas = ['DISCAGEM', 'ALO', 'CPC', 'CPCA', 'PROMESSA']
    df_consolidado = df.groupby(['DATA', 'CPF'], as_index=False)[colunas_metricas].sum()
    
    # Renomear ALO para ACIONAMENTOS
    df_consolidado = df_consolidado.rename(columns={'ALO': 'ACIONAMENTOS'})
    
    salvar_log(f"📊 Após consolidação - Trestto: {len(df_consolidado):,}")
    salvar_log(f"📊 Mailing: {len(df_mailing):,}")
    
    # ============================================
    # ENRIQUECER COM MAILING_HIST (LEFT JOIN)
    # ============================================
    df_mailing_temp = df_mailing[['CPF', 'DATA', 'PRODUTO', 'FX_ATRASO', 'VALORPRIN_FIN']].drop_duplicates()
    
    salvar_log(f"\n📊 Merge com mailing_hist...")
    salvar_log(f"   Registros antes: {len(df_consolidado):,}")
    
    df_resultado = df_consolidado.merge(
        df_mailing_temp,
        on=['CPF', 'DATA'],
        how='left'
    )
    
    salvar_log(f"   ✓ Registros após merge: {len(df_resultado):,}")
    registros_sem_fx = df_resultado['FX_ATRASO'].isna().sum()
    salvar_log(f"   ⚠️  Registros sem FX_ATRASO: {registros_sem_fx:,}")
    
    # ============================================
    # ENRIQUECER COM DW_CALENDARIO
    # ============================================
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date
    
    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    salvar_log(f"   Registros antes: {len(df_resultado):,}")
    
    df_resultado = df_resultado.merge(
        df_calendario_reduzido,
        left_on='DATA',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])
    
    salvar_log(f"   ✓ Registros após merge: {len(df_resultado):,}")
    registros_sem_calendario = df_resultado['nr_dia_util'].isna().sum()
    salvar_log(f"   ⚠️  Registros sem dados de calendário: {registros_sem_calendario:,}")
    
    df_resultado.rename(columns={'DISCAGEM': 'TRABALHADO'}, inplace=True)

    # ============================================
    # SEPARAR EM DOIS DATAFRAMES
    # ============================================
    df_com_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].notna()].copy()
    df_sem_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].isna()].copy()
    
    salvar_log(f"\n📦 Separação dos DataFrames:")
    salvar_log(f"   ✓ Registros COM FX_ATRASO: {len(df_com_fx_atraso):,}")
    salvar_log(f"   ✓ Registros SEM FX_ATRASO (fora da mailing): {len(df_sem_fx_atraso):,}")
    salvar_log(f"   ✓ CPFs únicos SEM FX_ATRASO: {df_sem_fx_atraso['CPF'].nunique():,}")
    salvar_log("="*80)

    return df_com_fx_atraso, df_sem_fx_atraso

#def enriquecer_discagens_trestto(df_discagens_trestto, df_mailing_hist, df_dw_calendario):
    """
    Enriquece o DataFrame de discagens Trestto com dados de mailing_hist e calendário.
    
    Args:
        df_discagens_trestto (pd.DataFrame): DataFrame com discagens Trestto,
            deve conter as colunas 'CPF', 'DATA', 'DISCAGEM', 'ALO', 'CPC', 'CPCA', 'PROMESSA'
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing,
            deve conter as colunas 'CPF', 'DATA', 'PRODUTO', 'FX_ATRASO', 'VALORPRIN_FIN'
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário,
            deve conter as colunas 'dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    
    Returns:
        pd.DataFrame: DataFrame enriquecido com PRODUTO, FX_ATRASO, VALORPRIN_FIN e dados de calendário
    """
    
    df = df_discagens_trestto.copy()
    df_mailing = df_mailing_hist.copy()
    
    # Converter DATA para o mesmo tipo
    df['DATA'] = pd.to_datetime(df['DATA']).dt.date
    df_mailing['DATA'] = pd.to_datetime(df_mailing['DATA']).dt.date
    
    # Garantir que CPF está como string
    df['CPF'] = df['CPF'].astype(str).str.strip()
    df_mailing['CPF'] = df_mailing['CPF'].astype(str).str.strip()
    
    print(f"📊 Antes da consolidação - Trestto: {len(df):,}")
    
    # Consolidar Trestto antes do merge (por DATA + CPF)
    colunas_metricas = ['DISCAGEM', 'ALO', 'CPC', 'CPCA', 'PROMESSA']
    df_consolidado = df.groupby(['DATA', 'CPF'], as_index=False)[colunas_metricas].sum()
    
    # Renomear ALO para ACIONAMENTOS
    df_consolidado = df_consolidado.rename(columns={'ALO': 'ACIONAMENTOS'})
    
    print(f"📊 Após consolidação - Trestto: {len(df_consolidado):,}")
    print(f"📊 Mailing: {len(df_mailing):,}")
    
    # ============================================
    # ENRIQUECER COM MAILING_HIST
    # ============================================
    df_mailing_temp = df_mailing[['CPF', 'DATA', 'PRODUTO', 'FX_ATRASO', 'VALORPRIN_FIN']].drop_duplicates()
    
    print(f"\n📊 Merge com mailing_hist...")
    print(f"   Registros antes: {len(df_consolidado):,}")
    
    df_resultado = df_consolidado.merge(
        df_mailing_temp,
        on=['CPF', 'DATA'],
        how='inner'
    )
    
    print(f"   ✓ Registros após merge: {len(df_resultado):,}")
    print(f"   ✓ Registros perdidos: {len(df_consolidado) - len(df_resultado):,}")
    
    # ============================================
    # ENRIQUECER COM DW_CALENDARIO
    # ============================================
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date
    
    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]
    
    print(f"\n📅 Merge com dw_calendario...")
    print(f"   Registros antes: {len(df_resultado):,}")
    
    df_resultado = df_resultado.merge(
        df_calendario_reduzido,
        left_on='DATA',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])
    
    print(f"   ✓ Registros após merge: {len(df_resultado):,}")
    registros_sem_calendario = df_resultado['nr_dia_util'].isna().sum()
    print(f"   ⚠️  Registros sem dados de calendário: {registros_sem_calendario:,}")
    
    return df_resultado

@registrar_tempo("Funil esforço trestto")
def acionamentos_esforco_trestto(df_discagens, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos de discagens por FX_ATRASO e ORIGEM.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame com dados de discagens enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_discagens.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    df['ORIGEM'] = 'Robô'
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    salvar_log("="*80)
    salvar_log(f"📊 Processando acumulado mensal de discagens para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Agrupar por FX_ATRASO e ORIGEM (sem deduplicação, soma tudo)
        agrupado = df_intervalo.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'TRABALHADO': g['TRABALHADO'].sum(),
            'VALORPRIN_FIN_TRABALHADO': g.loc[g['TRABALHADO'] == 1, 'VALORPRIN_FIN'].sum(),
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}")
    salvar_log(f"\n📈 Totais acumulados (última data):")
    salvar_log(f"   ✓ TRABALHADO: {df_final[df_final['DATA'] == df_final['DATA'].max()]['TRABALHADO'].sum():,}")
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")
    salvar_log("="*80)

    # Setar FX_ATRASO como 'Esforço'
    df_final['FX_ATRASO'] = 'Esforço'
    
    return df_final

@registrar_tempo("Funil unique trestto")
def acionamentos_unique_trestto(df_discagens, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score) por FX_ATRASO e ORIGEM.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame com dados de discagens enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_discagens.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    df['ORIGEM'] = 'Robô'
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    salvar_log("="*80)
    salvar_log(f"📊 Processando acumulado mensal ÚNICO (melhor score por CPF) para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Calcular score de tabulação COM VALORES ORIGINAIS (não binários)
        # Prioridade: PROMESSA > CPCA > CPC > ACIONAMENTOS
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'] * 1000 +  # Peso muito maior para desempate
            df_intervalo['CPCA'] * 100 +
            df_intervalo['CPC'] * 10 +
            df_intervalo['ACIONAMENTOS'] * 1 +
            df_intervalo['TRABALHADO']
        )
        
        # 3. Ordenar por CPF e score (maior score primeiro)
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'TABULACAO_SCORE'],
            ascending=[True, False]
        )
        
        # 4. Manter apenas o melhor score por CPF (unique - 1 CPF aparece 1 vez apenas)
        df_unique = df_intervalo.drop_duplicates(
            subset=['CPF'],
            keep='first'
        ).copy()
        
        # 5. AGORA converter métricas para binário (>= 1 vira 1) APÓS a seleção
        df_unique['TRABALHADO_BIN'] = (df_unique['TRABALHADO'] >= 1).astype(int)
        df_unique['ACIONAMENTOS_BIN'] = (df_unique['ACIONAMENTOS'] >= 1).astype(int)
        df_unique['CPC_BIN'] = (df_unique['CPC'] >= 1).astype(int)
        df_unique['CPCA_BIN'] = (df_unique['CPCA'] >= 1).astype(int)
        df_unique['PROMESSA_BIN'] = (df_unique['PROMESSA'] >= 1).astype(int)
        
        # 6. Agrupar por FX_ATRASO e ORIGEM
        agrupado = df_unique.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'TRABALHADO': g['TRABALHADO_BIN'].sum(),
            'VALORPRIN_FIN_TRABALHADO': g.loc[g['TRABALHADO_BIN'] == 1, 'VALORPRIN_FIN'].sum(),
            'ACIONAMENTOS': g['ACIONAMENTOS_BIN'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS_BIN'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC_BIN'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC_BIN'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA_BIN'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA_BIN'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA_BIN'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA_BIN'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
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
    salvar_log(f"   ✓ TRABALHADO: {df_final[df_final['DATA'] == df_final['DATA'].max()]['TRABALHADO'].sum():,}")
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")
    salvar_log("="*80)

    df_final['FX_ATRASO'] = 'Unique'

    return df_final

@registrar_tempo("Funil por fxAtraso e origem trestto")
def acionamentos_fxAtraso_origem_trestto(df_discagens, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF e FX_ATRASO (melhor score) por ORIGEM.
    Permite que o mesmo CPF seja contado em faixas de atraso diferentes.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame com dados de discagens enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por CPF e faixa de atraso
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_discagens.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    df['ORIGEM'] = 'Robô'
    
    # Converter métricas para binário (>= 1 vira 1)
    df['TRABALHADO'] = (df['TRABALHADO'] >= 1).astype(int)
    df['ACIONAMENTOS'] = (df['ACIONAMENTOS'] >= 1).astype(int)
    df['CPC'] = (df['CPC'] >= 1).astype(int)
    df['CPCA'] = (df['CPCA'] >= 1).astype(int)
    df['PROMESSA'] = (df['PROMESSA'] >= 1).astype(int)
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    salvar_log("="*80)
    salvar_log(f"📊 Processando acumulado mensal ÚNICO por CPF + FX_ATRASO (melhor score) para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 4 +
            df_intervalo['CPCA'].astype(int) * 3 +
            df_intervalo['CPC'].astype(int) * 2 +
            df_intervalo['ACIONAMENTOS'].astype(int) * 1 +
            df_intervalo['TRABALHADO'].astype(int)
        )
        
        # 3. Ordenar por CPF, FX_ATRASO e score (maior score primeiro)
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'FX_ATRASO', 'TABULACAO_SCORE'],
            ascending=[True, True, False]
        )
        
        # 4. Manter apenas o melhor score por CPF + FX_ATRASO (unique por faixa)
        df_unique = df_intervalo.drop_duplicates(
            subset=['CPF', 'FX_ATRASO'],
            keep='first'
        ).copy()
        
        # 5. Agrupar por FX_ATRASO e ORIGEM
        agrupado = df_unique.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'TRABALHADO': g['TRABALHADO'].sum(),
            'VALORPRIN_FIN_TRABALHADO': g.loc[g['TRABALHADO'] == 1, 'VALORPRIN_FIN'].sum(),
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}")
    salvar_log(f"\n📈 Totais acumulados ÚNICOS por CPF+FAIXA (última data):")
    salvar_log(f"   ✓ TRABALHADO: {df_final[df_final['DATA'] == df_final['DATA'].max()]['TRABALHADO'].sum():,}")
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")
    salvar_log("="*80)

    return df_final

def interseção_cpfs_discados_trestto(df_enriquecido_discagens_trestto_limpo, df_enriquecido_discagens_trestto_semFaixa):
    
    # Nome da coluna de CPF (ajuste se necessário)
    COLUNA_CPF = 'CPF'

    # Usar um set é a forma mais eficiente para operações de intersecção e diferença.
    cpfs_com_atraso = set(df_enriquecido_discagens_trestto_limpo[COLUNA_CPF].unique())

    # Cria um conjunto (set) de CPFs únicos do DataFrame SEM FX_ATRASO
    cpfs_sem_atraso = set(df_enriquecido_discagens_trestto_semFaixa[COLUNA_CPF].unique())

    print(f"📦 CPFs únicos COM FX_ATRASO: {len(cpfs_com_atraso):,}")
    print(f"📦 CPFs únicos SEM FX_ATRASO: {len(cpfs_sem_atraso):,}")
    print("-" * 50)


    # --- 3. CÁLCULO DA INTERSECÇÃO E DIFERENÇAS ---

    # CPFs que estão em AMBOS os conjuntos
    cpfs_em_ambos = cpfs_com_atraso.intersection(cpfs_sem_atraso)

    # CPFs que estão SOMENTE no grupo COM FX_ATRASO
    cpfs_somente_com = cpfs_com_atraso.difference(cpfs_em_ambos)
    # OU: cpfs_somente_com = cpfs_com_atraso - cpfs_sem_atraso

    # CPFs que estão SOMENTE no grupo SEM FX_ATRASO
    cpfs_somente_sem = cpfs_sem_atraso.difference(cpfs_em_ambos)
    # OU: cpfs_somente_sem = cpfs_sem_atraso - cpfs_com_atraso


    # --- 4. EXIBIÇÃO DOS RESULTADOS ---
    print("📊 Resultados da Comparação de CPFs Únicos\n")
    print(f"CPFs em AMBOS os grupos (Intersecção): {len(cpfs_em_ambos):,}")
    print("-" * 25)
    print(f"CPFs SOMENTE no grupo COM FX_ATRASO: {len(cpfs_somente_com):,}")
    print(f"CPFs SOMENTE no grupo SEM FX_ATRASO: {len(cpfs_somente_sem):,}")

def acionamentos_trestto(df_discagens_trestto, df_mailing_hist, df_dw_calendario):
    df_enriquecido_discagens_trestto_limpo, df_enriquecido_discagens_trestto_semFaixa = enriquecer_discagens_trestto(df_discagens_trestto, df_mailing_hist, df_dw_calendario)
    df_acionamentos_esforco_trestto = acionamentos_esforco_trestto(df_enriquecido_discagens_trestto_limpo, df_dw_calendario)
    df_acionamentos_unique_trestto = acionamentos_unique_trestto(df_enriquecido_discagens_trestto_limpo, df_dw_calendario)
    df_acionamentos_fxAtraso_origem_trestto = acionamentos_fxAtraso_origem_trestto(df_enriquecido_discagens_trestto_limpo, df_dw_calendario)
    df_acionamentos_trestto = unir_dataframes(df_acionamentos_esforco_trestto, df_acionamentos_unique_trestto, df_acionamentos_fxAtraso_origem_trestto)

    df_analitico_trestto = df_enriquecido_discagens_trestto_limpo.copy()

    return df_acionamentos_trestto, df_analitico_trestto, df_enriquecido_discagens_trestto_semFaixa