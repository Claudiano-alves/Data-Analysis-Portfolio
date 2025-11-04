import numpy as np
import pandas as pd

# ============================================
# DICIONÁRIOS E CONSTANTES
# ============================================

DDD_ESTADO = {
    '11': 'SP', '12': 'SP', '13': 'SP', '14': 'SP', '15': 'SP', '16': 'SP', '17': 'SP', '18': 'SP', '19': 'SP',
    '21': 'RJ', '22': 'RJ', '24': 'RJ',
    '27': 'ES', '28': 'ES',
    '31': 'MG', '32': 'MG', '33': 'MG', '34': 'MG', '35': 'MG', '37': 'MG', '38': 'MG',
    '41': 'PR', '42': 'PR', '43': 'PR', '44': 'PR', '45': 'PR', '46': 'PR',
    '47': 'SC', '48': 'SC', '49': 'SC',
    '51': 'RS', '53': 'RS', '54': 'RS', '55': 'RS',
    '61': 'DF', '62': 'GO', '63': 'TO', '64': 'GO', '65': 'MT', '66': 'MT', '67': 'MS',
    '68': 'AC', '69': 'RO',
    '71': 'BA', '73': 'BA', '74': 'BA', '75': 'BA', '77': 'BA',
    '79': 'SE',
    '81': 'PE', '82': 'AL', '83': 'PB', '84': 'RN', '85': 'CE', '86': 'PI', '87': 'PE', '88': 'CE', '89': 'PI',
    '91': 'PA', '92': 'AM', '93': 'PA', '94': 'PA', '95': 'RR', '96': 'AP', '97': 'AM', '98': 'MA', '99': 'MA'
}

# ============================================
# FUNÇÕES DE TRATAMENTO - DISCAGENS
# ============================================

def adicionar_operacao(df):
    """
    Adiciona a coluna OPERACAO ao DataFrame de discagens
    
    Args:
        df (pd.DataFrame): DataFrame com coluna 'GrupoPrincipal'
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'OPERACAO'
    """
    conditions = [
        df['GrupoPrincipal'] == 4118,
        df['GrupoPrincipal'] == 4022,
        df['GrupoPrincipal'] == 4017,
        df['GrupoPrincipal'].isin([4047, 4679, 4681, 4683, 4671]),
        df['GrupoPrincipal'].isin([4433, 4504]),
        df['GrupoPrincipal'].isin([4326, 4636, 4637, 4649])
    ]
    
    choices = [
        'ATIVO',
        'MANUAL',
        'RECEPTIVO',
        'URA CPC',
        'PREVENTIVO',
        'AGV NEGOCIADORA'
    ]
    
    df['OPERACAO'] = np.select(conditions, choices, default='Outros')
    return df


def adicionar_estado_por_ddd(df, coluna_ddd='ddd'):
    """
    Adiciona a coluna ESTADO baseada no DDD
    
    Args:
        df (pd.DataFrame): DataFrame com coluna de DDD
        coluna_ddd (str): Nome da coluna que contém o DDD
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'ESTADO'
    """
    df['ESTADO'] = df[coluna_ddd].astype(str).str.zfill(2).map(DDD_ESTADO)
    return df

# TRATANDO DISCAGENS EXPERT - ADICIONAR ORIGEM (ROBO OU HUMANO)
def adicionar_definir_humano_robo(df_discagens_expert):
    """
    Adiciona a coluna ESTADO baseada no DDD
    
    Args:
        df (pd.DataFrame): DataFrame com coluna de DDD
        coluna_ddd (str): Nome da coluna que contém o DDD
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'ESTADO'
    """
    df_discagens_expert = df_discagens_expert.copy()
    df_discagens_expert['ORIGEM'] = df_discagens_expert['OPERACAO'].apply(
        lambda x: 'Robô' if x == 'AGV NEGOCIADORA' else 'Humano'
    )
    return df_discagens_expert


def tratar_base_discagens(df):
    """
    Aplica todos os tratamentos padrão para base de discagens
    
    Args:
        df (pd.DataFrame): DataFrame de discagens
    
    Returns:
        pd.DataFrame: DataFrame tratado
    """
    df = adicionar_operacao(df)
    df = adicionar_estado_por_ddd(df)
    df = adicionar_definir_humano_robo(df)
    return df

def df_acionamento_robo():
    """
    Cria um DataFrame com classificação de códigos de tabulação.
    
    Returns:
        pd.DataFrame: DataFrame com colunas:
            - COD_TABULACAO: código de tabulação
            - ALO: 1 se o código é classificado como Alô, 0 caso contrário
            - CPC: 1 se o código é classificado como CPC, 0 caso contrário
            - CPCA: 1 se o código é classificado como CPCA, 0 caso contrário
            - PROMESSA: 1 se o código é classificado como Promessa, 0 caso contrário
    """
    
    # Definir os códigos de cada categoria
    codigos_alo = [
        '10','13','15','20','21','23','24','4','45','6','16','120','152','159','161',
        '19','28','31','32','37','38','48','51','7','30','42','160','27','36','39',
        '47','1','12','14','2','5','8','83','84','85','86','88','89','9','99','113',
        '136','50','101','103','114','115','25','43','46','52','87'
    ]
    
    codigos_cpc = [
        '10','13','15','20','21','23','24','4','45','6','16','120','152','159','161',
        '19','28','31','32','37','38','48','51','7','30','42','160','27','36','39',
        '47','86','25','43','46','87'
    ]
    
    codigos_cpca = [
        '10','13','15','20','21','23','24','4','45','6','16','120','152','159','161',
        '19','28','31','32','37','38','48','51','7','42','160','25','43','46'
    ]
    
    codigos_promessa = [
        '10','13','15','20','21','23','24','4','45','6','160','25','43','46'
    ]
    
    # Obter todos os códigos únicos
    todos_codigos = sorted(set(codigos_alo + codigos_cpc + codigos_cpca + codigos_promessa))
    
    # Criar o DataFrame
    df_tabulacoes = pd.DataFrame({
        'COD_TABULACAO': todos_codigos
    })
    
    # Adicionar as colunas de classificação
    df_tabulacoes['ACIONAMENTOS'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_alo).astype(int)
    df_tabulacoes['CPC'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_cpc).astype(int)
    df_tabulacoes['CPCA'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_cpca).astype(int)
    df_tabulacoes['PROMESSA'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_promessa).astype(int)
    
    print(f"✅ DataFrame de classificação de tabulações criado!")
    print(f"   📊 Total de códigos: {len(df_tabulacoes)}")
    print(f"   ✓ ACIONAMENTOS: {df_tabulacoes['ACIONAMENTOS'].sum()} códigos")
    print(f"   ✓ CPC: {df_tabulacoes['CPC'].sum()} códigos")
    print(f"   ✓ CPCA: {df_tabulacoes['CPCA'].sum()} códigos")
    print(f"   ✓ PROMESSA: {df_tabulacoes['PROMESSA'].sum()} códigos")
    
    return df_tabulacoes

def enriquecer_discagens_expert(df_discagens_expert, df_acionamentos_tabulacaoRobo):
    """
    Enriquece o DataFrame de discagens_expert com classificações de tabulação.
    
    Args:
        df_discagens_expert (pd.DataFrame): DataFrame com discagens do expert,
            deve conter a coluna 'codtabulacao'
        df_acionamentos_tabulacaoRobo (pd.DataFrame): DataFrame com classificações,
            deve conter as colunas 'COD_TABULACAO', 'ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA'
    
    Returns:
        pd.DataFrame: DataFrame enriquecido com as colunas ACIONAMENTOS, CPC, CPCA e PROMESSA
    """
    
    print(f"📊 Merge com classificações de tabulação...")
    print(f"   Registros antes: {len(df_discagens_expert):,}")
    
    # Realizar o merge
    df_resultado = df_discagens_expert.merge(
        df_acionamentos_tabulacaoRobo[['COD_TABULACAO', 'ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']],
        left_on='codtabulacao',
        right_on='COD_TABULACAO',
        how='inner'
    ).drop(columns=['COD_TABULACAO'])  # Remove coluna duplicada após merge
    
    print(f"   ✓ Registros após merge: {len(df_resultado):,}")
    print(f"   ✓ Registros perdidos: {len(df_discagens_expert) - len(df_resultado):,}")
    
    # Estatísticas das classificações
    print(f"\n📈 Distribuição das classificações:")
    print(f"   ✓ ACIONAMENTOS: {df_resultado['ACIONAMENTOS'].sum():,} registros")
    print(f"   ✓ CPC: {df_resultado['CPC'].sum():,} registros")
    print(f"   ✓ CPCA: {df_resultado['CPCA'].sum():,} registros")
    print(f"   ✓ PROMESSA: {df_resultado['PROMESSA'].sum():,} registros")
    
    return df_resultado

def enriquecer_com_mailing_calendario(df_discagens, df_mailing_hist, df_dw_calendario):
    """
    Enriquece o DataFrame de discagens com dados de mailing_hist e calendário.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame com discagens enriquecidas,
            deve conter as colunas 'CONTRATO' e 'DATA'
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing,
            deve conter as colunas 'CONTRATO', 'DATA', 'FX_ATRASO'
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário,
            deve conter as colunas 'dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    
    Returns:
        tuple: (df_com_fx_atraso, df_sem_fx_atraso)
            - df_com_fx_atraso: DataFrame enriquecido com FX_ATRASO preenchido
            - df_sem_fx_atraso: DataFrame enriquecido sem FX_ATRASO (valores nulos)
    """
    
    df_resultado = df_discagens.copy()
    
    # Padronizar CONTRATO como maiúscula
    df_resultado['CONTRATO'] = df_resultado['CONTRATO'].astype(str).str.upper().str.strip()
    
    # ============================================
    # ENRIQUECER COM MAILING_HIST
    # ============================================
    df_mailing_temp = df_mailing_hist[['CONTRATO', 'DATA', 'FX_ATRASO', 'VALORPRIN_FIN']].copy()
    df_mailing_temp['CONTRATO'] = df_mailing_temp['CONTRATO'].astype(str).str.upper().str.strip()
    
    # Padronizar data antes do merge
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_mailing_temp['DATA'] = pd.to_datetime(df_mailing_temp['DATA']).dt.date
    
    print(f"📊 Merge com mailing_hist...")
    print(f"   Registros antes: {len(df_resultado):,}")
    
    df_resultado = df_resultado.merge(
        df_mailing_temp,
        on=['CONTRATO', 'DATA'],
        how='left'
    )
    
    print(f"   ✓ Registros após merge: {len(df_resultado):,}")
    registros_sem_fx = df_resultado['FX_ATRASO'].isna().sum()
    print(f"   ⚠️  Registros sem FX_ATRASO: {registros_sem_fx:,}")
    
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
    ).drop(columns=['dt_data'])  # Remove coluna duplicada após merge
    
    print(f"   ✓ Registros após merge: {len(df_resultado):,}")
    registros_sem_calendario = df_resultado['nr_dia_util'].isna().sum()
    print(f"   ⚠️  Registros sem dados de calendário: {registros_sem_calendario:,}")
    
    # ============================================
    # SEPARAR EM DOIS DATAFRAMES
    # ============================================
    df_com_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].notna()].copy()
    df_sem_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].isna()].copy()
    
    print(f"\n📦 Separação dos DataFrames:")
    print(f"   ✓ Registros COM FX_ATRASO: {len(df_com_fx_atraso):,}")
    print(f"   ✓ Registros SEM FX_ATRASO: {len(df_sem_fx_atraso):,}")
    
    return df_com_fx_atraso, df_sem_fx_atraso

# FUNIL
def acionamentos_fxAtraso_origem_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score) por FX_ATRASO e ORIGEM.
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame enriquecido com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_com_fx_atraso.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    df['ORIGEM'] = 'Robô'
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    print(f"📊 Processando acumulado mensal ÚNICO (melhor score por CPF) para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            print(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 3 +
            df_intervalo['CPCA'].astype(int) * 2 +
            df_intervalo['CPC'].astype(int) * 1
        )
        
        # 3. Ordenar por CPF e score (maior score primeiro)
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'FX_ATRASO', 'TABULACAO_SCORE'],
            ascending=[True, False]
        )
        
        # 4. Manter apenas o melhor score por CPF (unique)
        df_unique = df_intervalo.drop_duplicates(
            subset=['CPF'],
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
        
        agrupado['DATA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    print(f"\n📅 Merge com dw_calendario...")
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
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    print(f"   ✓ Registros finais: {len(df_final):,}")
    print(f"\n📈 Totais acumulados ÚNICOS (última data):")
    print(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    print(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    print(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    print(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")

    return df_final

def acionamentos_unique_fxAtraso_origem_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score) por FX_ATRASO e ORIGEM.
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame enriquecido com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_com_fx_atraso.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    df['ORIGEM'] = 'Robô'
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    print(f"📊 Processando acumulado mensal ÚNICO (melhor score por CPF) para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            print(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 3 +
            df_intervalo['CPCA'].astype(int) * 2 +
            df_intervalo['CPC'].astype(int) * 1
        )
        
        # 3. Ordenar por CPF e score (maior score primeiro)
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'TABULACAO_SCORE'],
            ascending=[True, False]
        )
        
        # 4. Manter apenas o melhor score por CPF (unique)
        df_unique = df_intervalo.drop_duplicates(
            subset=['CPF'],
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
        
        agrupado['DATA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    print(f"\n📅 Merge com dw_calendario...")
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
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    print(f"   ✓ Registros finais: {len(df_final):,}")
    print(f"\n📈 Totais acumulados ÚNICOS (última data):")
    print(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    print(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    print(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    print(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")

    return df_final

def acionamentos_esforco_fxAtraso_origem_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos por FX_ATRASO e ORIGEM.
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame enriquecido com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_com_fx_atraso.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    df['ORIGEM'] = 'Robô'
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    print(f"📊 Processando acumulado mensal para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            print(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
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
        
        agrupado['DATA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    print(f"\n📅 Merge com dw_calendario...")
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
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    print(f"   ✓ Registros finais: {len(df_final):,}")
    print(f"\n📈 Totais acumulados:")
    print(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    print(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    print(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    print(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")

    return df_final