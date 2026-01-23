"""
Módulo de Métricas Acumuladas - Discagens Trestto

Responsável por calcular:
- Acumulado mensal de acionamentos (Esforço)
- Acumulado mensal de acionamentos únicos por CPF (Unique)
- Acumulado mensal de acionamentos únicos por CPF + FX_ATRASO (fxAtraso_origem)
"""

import pandas as pd
from datetime import datetime, timedelta
import warnings

from Projects.utils.utils import salvar_log, registrar_tempo
from ...config import LOG_DISCAGENS

warnings.filterwarnings("ignore", category=FutureWarning)

@registrar_tempo("Funil ESFORÇO - Trestto", arquivo_log=LOG_DISCAGENS)
def acionamentos_esforco_trestto(df_discagens, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos (sem deduplicação).
    Preenche dias úteis faltantes até D-1 com acumulado até aquela data.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame com dados de discagens enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso
    """
    df = df_discagens.copy()
    df['ORIGEM'] = 'ROBÔ'
    df['DATA'] = pd.to_datetime(df['DATA'])
    
    # Preparar calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    # Identificar range
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Processando ESFORÇO (todas as discagens)...", arquivo_log=LOG_DISCAGENS)
    
    primeira_data_origem = df['DATA'].min()
    ultima_data_origem = df['DATA'].max()
    data_d_menos_1 = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
    data_final_desejada = max(ultima_data_origem, data_d_menos_1)
    
    salvar_log(f"\n📅 Range de processamento:", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Primeira data com dados: {primeira_data_origem.strftime('%Y-%m-%d')}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Última data com dados: {ultima_data_origem.strftime('%Y-%m-%d')}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Data D-1 (ontem): {data_d_menos_1.strftime('%Y-%m-%d')}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Data final do processamento: {data_final_desejada.strftime('%Y-%m-%d')}", arquivo_log=LOG_DISCAGENS)
    
    # Obter todos os dias úteis
    todos_dias_uteis = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= primeira_data_origem) &
        (df_dw_calendario_temp['dt_data'] <= data_final_desejada)
    ]['dt_data'].sort_values().unique()
    
    datas_com_dados = set(df['DATA'].unique())
    datas_faltantes = sorted([d for d in todos_dias_uteis if d not in datas_com_dados])
    
    salvar_log(f"\n📊 Análise de completude:", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Total de dias úteis no range: {len(todos_dias_uteis)}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Dias com dados: {len(datas_com_dados)}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Dias sem dados (a calcular): {len(datas_faltantes)}", arquivo_log=LOG_DISCAGENS)
    
    # Processar
    datas_para_processar = sorted(todos_dias_uteis)
    resultados = []
    
    salvar_log(f"\n🔄 Processando acumulados para {len(datas_para_processar)} dias úteis...", arquivo_log=LOG_DISCAGENS)
    
    for i, data in enumerate(datas_para_processar, 1):
        if i % 10 == 0 or i == len(datas_para_processar):
            salvar_log(f"    Processando {i}/{len(datas_para_processar)} datas...", arquivo_log=LOG_DISCAGENS)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        if len(df_intervalo) == 0:
            agrupado = pd.DataFrame({
                'FX_ATRASO': [],
                'ORIGEM': [],
                'TRABALHADO': [],
                'VALORPRIN_FIN_TRABALHADO': [],
                'ACIONAMENTOS': [],
                'VALORPRIN_FIN_ACIONAMENTOS': [],
                'CPC': [],
                'VALORPRIN_FIN_CPC': [],
                'CPCA': [],
                'VALORPRIN_FIN_CPCA': [],
                'PROMESSA': [],
                'VALORPRIN_FIN_PROMESSA': []
            })
            agrupado['DATA'] = data
            resultados.append(agrupado)
            continue
        
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
    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='left'
    ).drop(columns=['dt_data'])
    
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas].sort_values('DATA').reset_index(drop=True)
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    df_final['FX_ATRASO'] = 'Esforço'
    return df_final


@registrar_tempo("Funil UNIQUE - Trestto", arquivo_log=LOG_DISCAGENS)
def acionamentos_unique_trestto(df_discagens, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score).
    Preenche dias úteis faltantes até D-1.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame com dados de discagens enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas
    """
    df = df_discagens.copy()
    df['ORIGEM'] = 'ROBÔ'
    df['DATA'] = pd.to_datetime(df['DATA'])
    
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Processando UNIQUE (melhor score por CPF)...", arquivo_log=LOG_DISCAGENS)
    
    primeira_data_origem = df['DATA'].min()
    ultima_data_origem = df['DATA'].max()
    data_d_menos_1 = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
    data_final_desejada = max(ultima_data_origem, data_d_menos_1)
    
    todos_dias_uteis = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= primeira_data_origem) &
        (df_dw_calendario_temp['dt_data'] <= data_final_desejada)
    ]['dt_data'].sort_values().unique()
    
    datas_para_processar = sorted(todos_dias_uteis)
    resultados = []
    
    salvar_log(f"\n🔄 Processando acumulados para {len(datas_para_processar)} dias úteis...", arquivo_log=LOG_DISCAGENS)
    
    for i, data in enumerate(datas_para_processar, 1):
        if i % 10 == 0 or i == len(datas_para_processar):
            salvar_log(f"    Processando {i}/{len(datas_para_processar)} datas...", arquivo_log=LOG_DISCAGENS)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        if len(df_intervalo) == 0:
            agrupado = pd.DataFrame({
                'FX_ATRASO': [],
                'ORIGEM': [],
                'TRABALHADO': [],
                'VALORPRIN_FIN_TRABALHADO': [],
                'ACIONAMENTOS': [],
                'VALORPRIN_FIN_ACIONAMENTOS': [],
                'CPC': [],
                'VALORPRIN_FIN_CPC': [],
                'CPCA': [],
                'VALORPRIN_FIN_CPCA': [],
                'PROMESSA': [],
                'VALORPRIN_FIN_PROMESSA': []
            })
            agrupado['DATA'] = data
            resultados.append(agrupado)
            continue
        
        # Calcular score
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'] * 1000 +
            df_intervalo['CPCA'] * 100 +
            df_intervalo['CPC'] * 10 +
            df_intervalo['ACIONAMENTOS'] * 1 +
            df_intervalo['TRABALHADO']
        )
        
        # Manter melhor score por CPF
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'TABULACAO_SCORE'],
            ascending=[True, False]
        )
        df_unique = df_intervalo.drop_duplicates(subset=['CPF'], keep='first').copy()
        
        # Converter para binário e agrupar
        df_unique['TRABALHADO_BIN'] = (df_unique['TRABALHADO'] >= 1).astype(int)
        df_unique['ACIONAMENTOS_BIN'] = (df_unique['ACIONAMENTOS'] >= 1).astype(int)
        df_unique['CPC_BIN'] = (df_unique['CPC'] >= 1).astype(int)
        df_unique['CPCA_BIN'] = (df_unique['CPCA'] >= 1).astype(int)
        df_unique['PROMESSA_BIN'] = (df_unique['PROMESSA'] >= 1).astype(int)
        
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
    
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Merge com calendário
    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='left'
    ).drop(columns=['dt_data'])
    
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas].sort_values('DATA').reset_index(drop=True)
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    df_final['FX_ATRASO'] = 'Unique'
    return df_final


@registrar_tempo("Funil fxAtraso_origem - Trestto", arquivo_log=LOG_DISCAGENS)
def acionamentos_fxAtraso_origem_trestto(df_discagens, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF + FX_ATRASO (melhor score).
    Preenche dias úteis faltantes até D-1 com valores da última data disponível.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame com dados de discagens enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por CPF + FX_ATRASO
    """
    df = df_discagens.copy()
    df['ORIGEM'] = 'ROBÔ'
    
    # Converter métricas para binário
    df['TRABALHADO'] = (df['TRABALHADO'] >= 1).astype(int)
    df['ACIONAMENTOS'] = (df['ACIONAMENTOS'] >= 1).astype(int)
    df['CPC'] = (df['CPC'] >= 1).astype(int)
    df['CPCA'] = (df['CPCA'] >= 1).astype(int)
    df['PROMESSA'] = (df['PROMESSA'] >= 1).astype(int)
    
    df['DATA'] = pd.to_datetime(df['DATA'])
    
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
    
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
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
            df_intervalo['ACIONAMENTOS'].astype(int) * 1 +
            df_intervalo['TRABALHADO'].astype(int)
        )
        
        # Manter melhor score por CPF + FX_ATRASO
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
    
    # Preencher buracos
    salvar_log(f"\n🔄 Verificando buracos no range e dias úteis faltantes...", arquivo_log=LOG_DISCAGENS)
    
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    primeira_data_origem = df_final['DATA'].min()
    ultima_data_origem = df_final['DATA'].max()
    data_d_menos_1 = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
    data_final_desejada = max(ultima_data_origem, data_d_menos_1)
    
    todos_dias_uteis = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= primeira_data_origem) & 
        (df_dw_calendario_temp['dt_data'] <= data_final_desejada)
    ]['dt_data'].sort_values().unique()
    
    datas_existentes = set(df_final['DATA'].unique())
    datas_faltantes = sorted([d for d in todos_dias_uteis if d not in datas_existentes])
    
    if len(datas_faltantes) > 0:
        salvar_log(f"   ⚠️  Encontrados {len(datas_faltantes)} dias úteis faltantes", arquivo_log=LOG_DISCAGENS)
        
        dfs_duplicados = []
        for data_faltante in datas_faltantes:
            datas_anteriores = [d for d in datas_existentes if d < data_faltante]
            if len(datas_anteriores) > 0:
                ultima_data_anterior = max(datas_anteriores)
                df_duplicado = df_final[df_final['DATA'] == ultima_data_anterior].copy()
                df_duplicado['DATA'] = data_faltante
                dfs_duplicados.append(df_duplicado)
                datas_existentes.add(data_faltante)
        
        if len(dfs_duplicados) > 0:
            df_final = pd.concat([df_final] + dfs_duplicados, ignore_index=True)
            salvar_log(f"   ✓ Preenchimento de {len(dfs_duplicados)} dias concluído!", arquivo_log=LOG_DISCAGENS)
    
    # Merge com calendário
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])
    
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    
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
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
    return df_final


__all__ = [
    'acionamentos_esforco_trestto',
    'acionamentos_unique_trestto',
    'acionamentos_fxAtraso_origem_trestto'
]
