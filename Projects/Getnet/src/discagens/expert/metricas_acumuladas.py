"""
Módulo de Métricas Acumuladas - Discagens Expert

Responsável por calcular:
- Acumulado mensal de acionamentos (Esforço)
- Acumulado mensal de acionamentos únicos por CPF (Unique)
- Acumulado mensal de acionamentos únicos por CPF + FX_ATRASO (fxAtraso_origem)
"""

import pandas as pd
from utils.utils import salvar_log, registrar_tempo
from ...config import LOG_DISCAGENS

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


@registrar_tempo("Funil ESFORÇO - Expert", arquivo_log=LOG_DISCAGENS)
def acionamentos_esforco_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos por FX_ATRASO e ORIGEM (sem deduplicação).
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais
    """
    df = df_com_fx_atraso.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
    
    salvar_log(f"📊 Processando ESFORÇO (todas as discagens) para {len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
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
    
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Merge com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])
    
    # Preencher NaN com 0
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
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
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    df_final['FX_ATRASO'] = 'Esforço'
    return df_final


@registrar_tempo("Funil UNIQUE - Expert", arquivo_log=LOG_DISCAGENS)
def acionamentos_unique_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score).
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas
    """
    df = df_com_fx_atraso.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
    
    salvar_log(f"📊 Processando UNIQUE (melhor score por CPF) para {len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 4 +
            df_intervalo['CPCA'].astype(int) * 3 +
            df_intervalo['CPC'].astype(int) * 2 +
            df_intervalo['ACIONAMENTOS'].astype(int) * 1
        )
        
        # Ordenar e manter melhor score por CPF
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'TABULACAO_SCORE'],
            ascending=[True, False]
        )
        df_unique = df_intervalo.drop_duplicates(subset=['CPF'], keep='first').copy()
        
        # Agrupar por FX_ATRASO e ORIGEM
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
    
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Merge com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])
    
    # Preencher NaN com 0
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
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
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    df_final['FX_ATRASO'] = 'Unique'
    return df_final


@registrar_tempo("Funil fxAtraso_origem - Expert", arquivo_log=LOG_DISCAGENS)
def acionamentos_fxAtraso_origem_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF + FX_ATRASO (melhor score).
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por CPF + FX_ATRASO
    """
    df = df_com_fx_atraso.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
    
    salvar_log(f"📊 Processando fxAtraso_origem (melhor score por CPF+FX_ATRASO) para {len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # Calcular score
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 4 +
            df_intervalo['CPCA'].astype(int) * 3 +
            df_intervalo['CPC'].astype(int) * 2 +
            df_intervalo['ACIONAMENTOS'].astype(int) * 1
        )
        
        # Ordenar e manter melhor score por CPF + FX_ATRASO
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'FX_ATRASO', 'TABULACAO_SCORE'],
            ascending=[True, True, False]
        )
        df_unique = df_intervalo.drop_duplicates(subset=['CPF', 'FX_ATRASO'], keep='first').copy()
        
        # Agrupar
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
    
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Merge com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])
    
    # Preencher NaN com 0
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
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
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    return df_final


__all__ = [
    'acionamentos_esforco_expert',
    'acionamentos_unique_expert',
    'acionamentos_fxAtraso_origem_expert'
]
