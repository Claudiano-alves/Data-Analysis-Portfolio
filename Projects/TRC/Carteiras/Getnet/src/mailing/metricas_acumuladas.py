"""
Módulo de Métricas Acumuladas de Mailing
Contém funções para gerar métricas acumuladas (mensais) de mailing.
"""

import pandas as pd
from utils.utils import registrar_tempo, unir_dataframes, salvar_log
from ..config import LOG_MAILING


@registrar_tempo("Acumulado mailing hist por faixa de atraso", arquivo_log=LOG_MAILING)
def gerar_acumulado_maling_hist_fxAtraso(df_maling_hist, df_dw_calendario):
    """
    Gera DataFrame com acumulado de contratos e CPFs do mailing por dia útil,
    agrupado por faixa de atraso.
    
    Args:
        df_maling_hist (pd.DataFrame): DataFrame de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com acumulado por faixa de atraso
    """
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log("INÍCIO DO PROCESSAMENTO DE MALING_HIST", arquivo_log=LOG_MAILING)
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log(f"Total de registros em df_maling_hist: {len(df_maling_hist)}", arquivo_log=LOG_MAILING)
    
    df_reduzido = df_maling_hist[['DATA', 'CONTRATO', 'CPF', 'FX_ATRASO', 'VALOR']].copy()
    salvar_log(f"Memória reduzida - trabalhando com {len(df_reduzido.columns)} colunas", arquivo_log=LOG_MAILING)
    
    df_reduzido['DATA'] = pd.to_datetime(df_reduzido['DATA'])
    
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]
    
    df_reduzido = df_reduzido.merge(
        df_calendario_reduzido,
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])
    
    salvar_log(f"Registros após merge com calendário: {len(df_reduzido)}", arquivo_log=LOG_MAILING)
    
    resultados = []
    datas_unicas = sorted(df_reduzido['DATA'].unique())
    
    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=LOG_MAILING)
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_MAILING)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        df_intervalo = df_reduzido[
            (df_reduzido['DATA'] >= inicio_mes) & 
            (df_reduzido['DATA'] <= data)
        ].copy()
        
        info_data = df_reduzido[df_reduzido['DATA'] == data][
            ['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        ].drop_duplicates()
        
        if len(info_data) == 0:
            continue
        
        info_data = info_data.iloc[0]
        
        # Contratos únicos por faixa
        df_contratos_unicos = df_intervalo.sort_values('DATA').groupby(['CONTRATO', 'FX_ATRASO']).tail(1)
        agrupado_contratos = df_contratos_unicos.groupby('FX_ATRASO').agg({
            'CONTRATO': 'nunique',
            'VALOR': 'sum'
        }).reset_index()
        
        agrupado_contratos['DATA'] = data
        agrupado_contratos['Indicador'] = 'Contrato'
        agrupado_contratos['MesAbreviado'] = info_data['mes_abreviado']
        agrupado_contratos['nr_dia_util'] = info_data['nr_dia_util']
        agrupado_contratos['quartil'] = info_data['quartil']
        agrupado_contratos['dt_mes'] = info_data['dt_mes']
        agrupado_contratos = agrupado_contratos.rename(columns={'CONTRATO': 'qte'})
        
        # CPFs únicos por faixa
        df_cpfs_unicos = df_intervalo.sort_values('DATA').groupby(['CPF', 'FX_ATRASO']).tail(1)
        agrupado_cpfs = df_cpfs_unicos.groupby('FX_ATRASO').agg({
            'CPF': 'nunique',
            'VALOR': 'sum'
        }).reset_index()
        
        agrupado_cpfs['DATA'] = data
        agrupado_cpfs['Indicador'] = 'Carteira (CPFs)'
        agrupado_cpfs['MesAbreviado'] = info_data['mes_abreviado']
        agrupado_cpfs['nr_dia_util'] = info_data['nr_dia_util']
        agrupado_cpfs['quartil'] = info_data['quartil']
        agrupado_cpfs['dt_mes'] = info_data['dt_mes']
        agrupado_cpfs = agrupado_cpfs.rename(columns={'CPF': 'qte'})
        
        resultados.append(agrupado_contratos)
        resultados.append(agrupado_cpfs)
    
    df_acumulado = pd.concat(resultados, ignore_index=True)
    
    df_acumulado = df_acumulado[[
        'DATA', 'Indicador', 'qte', 'FX_ATRASO', 'MesAbreviado',
        'nr_dia_util', 'quartil', 'dt_mes', 'VALOR'
    ]]
    
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log("RESUMO FINAL MALING_HIST", arquivo_log=LOG_MAILING)
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}", arquivo_log=LOG_MAILING)
    salvar_log(f"Registros de Contratos: {len(df_acumulado[df_acumulado['Indicador'] == 'Contrato'])}", arquivo_log=LOG_MAILING)
    salvar_log(f"Registros de CPFs: {len(df_acumulado[df_acumulado['Indicador'] == 'Carteira (CPFs)'])}", arquivo_log=LOG_MAILING)
    salvar_log(f"Valor total final: R$ {df_acumulado['VALOR'].sum():,.2f}", arquivo_log=LOG_MAILING)
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    
    return df_acumulado


@registrar_tempo("Acumulado mailing hist unique", arquivo_log=LOG_MAILING)
def gerar_acumulado_maling_hist_unique(df_maling_hist, df_dw_calendario):
    """
    Gera DataFrame com acumulado ÚNICO de contratos e CPFs do mailing por dia útil.
    Sem agrupamento por faixa de atraso.
    
    Args:
        df_maling_hist (pd.DataFrame): DataFrame de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com acumulado único
    """
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log("INÍCIO DO PROCESSAMENTO DE MALING_HIST", arquivo_log=LOG_MAILING)
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log(f"Total de registros em df_maling_hist: {len(df_maling_hist)}", arquivo_log=LOG_MAILING)
    
    df_reduzido = df_maling_hist[['DATA', 'CONTRATO', 'CPF', 'VALOR']].copy()
    salvar_log(f"Memória reduzida - trabalhando com {len(df_reduzido.columns)} colunas", arquivo_log=LOG_MAILING)
    
    df_reduzido['DATA'] = pd.to_datetime(df_reduzido['DATA'])
    
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]
    
    df_reduzido = df_reduzido.merge(
        df_calendario_reduzido,
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])
    
    salvar_log(f"Registros após merge com calendário: {len(df_reduzido)}", arquivo_log=LOG_MAILING)
    
    resultados = []
    datas_unicas = sorted(df_reduzido['DATA'].unique())
    
    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=LOG_MAILING)
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_MAILING)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        df_intervalo = df_reduzido[
            (df_reduzido['DATA'] >= inicio_mes) & 
            (df_reduzido['DATA'] <= data)
        ].copy()
        
        info_data = df_reduzido[df_reduzido['DATA'] == data][
            ['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        ].drop_duplicates()
        
        if len(info_data) == 0:
            continue
        
        info_data = info_data.iloc[0]
        
        # Contratos únicos
        df_contratos_unicos = df_intervalo.sort_values('DATA').groupby('CONTRATO').tail(1)
        total_contratos = df_contratos_unicos['CONTRATO'].nunique()
        valor_contratos = df_contratos_unicos['VALOR'].sum()
        
        resultado_contratos = {
            'DATA': data,
            'Indicador': 'Contratos',
            'qte': total_contratos,
            'FX_ATRASO': 'Unique',
            'MesAbreviado': info_data['mes_abreviado'],
            'nr_dia_util': info_data['nr_dia_util'],
            'quartil': info_data['quartil'],
            'dt_mes': info_data['dt_mes'],
            'VALOR': valor_contratos
        }
        
        # CPFs únicos
        df_cpfs_unicos = df_intervalo.sort_values('DATA').groupby('CPF').tail(1)
        total_cpfs = df_cpfs_unicos['CPF'].nunique()
        valor_cpfs = df_cpfs_unicos['VALOR'].sum()
        
        resultado_cpfs = {
            'DATA': data,
            'Indicador': 'Carteira (CPFs)',
            'qte': total_cpfs,
            'FX_ATRASO': 'Unique',
            'MesAbreviado': info_data['mes_abreviado'],
            'nr_dia_util': info_data['nr_dia_util'],
            'quartil': info_data['quartil'],
            'dt_mes': info_data['dt_mes'],
            'VALOR': valor_cpfs
        }
        
        resultados.append(resultado_contratos)
        resultados.append(resultado_cpfs)
    
    df_acumulado = pd.DataFrame(resultados)
    
    df_acumulado = df_acumulado[[
        'DATA', 'Indicador', 'qte', 'FX_ATRASO', 'MesAbreviado',
        'nr_dia_util', 'quartil', 'dt_mes', 'VALOR'
    ]]
    
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log("RESUMO FINAL MALING_HIST", arquivo_log=LOG_MAILING)
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}", arquivo_log=LOG_MAILING)
    salvar_log(f"Registros de Contratos: {len(df_acumulado[df_acumulado['Indicador'] == 'Contratos'])}", arquivo_log=LOG_MAILING)
    salvar_log(f"Registros de CPFs: {len(df_acumulado[df_acumulado['Indicador'] == 'Carteira (CPFs)'])}", arquivo_log=LOG_MAILING)
    salvar_log(f"Valor total final: R$ {df_acumulado['VALOR'].sum():,.2f}", arquivo_log=LOG_MAILING)
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    
    return df_acumulado