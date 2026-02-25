"""
Módulo de Métricas Acumuladas de Pagamentos
Contém funções para gerar métricas acumuladas (mensais) de pagamentos.
"""

from typing import List, Optional, Tuple
import pandas as pd
from utils.utils import registrar_tempo, unir_dataframes, salvar_log
from ..config import LOG_PAGAMENTOS

@registrar_tempo("Gerando acumulado de pagamentos", arquivo_log=LOG_PAGAMENTOS)
def gerar_acumulado_por_dia_util(
    df_agrupado: pd.DataFrame,
    dimensoes_segmentacao: Optional[List[str]] = None,
    log_file: str = LOG_PAGAMENTOS
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Gera DataFrame com acumulado de pagamentos do início do mês até cada dia útil.
    
    Args:
        df_agrupado: DataFrame de pagamentos agrupados
        dimensoes_segmentacao: Lista de dimensões para agrupamento (ex: ['ORIGEM'])
                              Se None, auto-detecta dimensões disponíveis
        log_file: Arquivo de log
    
    Returns:
        tuple: (df_acumulado, df_esforco, df_unique)
    
    Examples:
        # Com segmentação por ORIGEM
        df_acum, df_esf, df_unq = gerar_acumulado_por_dia_util(
            df_agrupado,
            dimensoes_segmentacao=['ORIGEM']
        )
        
        # Sem segmentação
        df_acum, df_esf, df_unq = gerar_acumulado_por_dia_util(
            df_agrupado,
            dimensoes_segmentacao=[]
        )
        
        # Auto-detectar
        df_acum, df_esf, df_unq = gerar_acumulado_por_dia_util(df_agrupado)
    """
    
    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log("GERANDO ACUMULADO POR DIA ÚTIL", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)
    
    # ============================================
    # VALIDAÇÃO INICIAL
    # ============================================
    if df_agrupado.empty:
        salvar_log("AVISO: DataFrame de pagamentos está vazio. Retornando DataFrames vazios.", arquivo_log=log_file)
        salvar_log("=" * 60, arquivo_log=log_file)
        return _criar_dataframes_vazios_pagamentos(dimensoes_segmentacao)
    
    # ============================================
    # DETECTAR DIMENSÕES
    # ============================================
    if dimensoes_segmentacao is None:
        # Auto-detectar dimensões além de FX_ATRASO
        dimensoes_possiveis = ['ORIGEM', 'CANAL', 'REGIAO', 'PARCEIRO']
        dimensoes_segmentacao = [
            dim for dim in dimensoes_possiveis 
            if dim in df_agrupado.columns
        ]
    
    # Validar dimensões solicitadas existem no DataFrame
    dimensoes_validas = [
        dim for dim in dimensoes_segmentacao 
        if dim in df_agrupado.columns
    ]
    
    if dimensoes_validas:
        salvar_log(f"📊 Dimensões de segmentação: {dimensoes_validas}", arquivo_log=log_file)
    else:
        salvar_log("📊 Sem segmentação (apenas FX_ATRASO)", arquivo_log=log_file)
    
    # ============================================
    # PROCESSAMENTO ACUMULADO
    # ============================================
    resultados = []
    datas_unicas = sorted(df_agrupado['DATA_PAGTO'].unique())
    
    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=log_file)
    
    # Definir colunas para agrupamento
    colunas_agrupamento = ['FX_ATRASO'] + dimensoes_validas
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=log_file)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1).date()
        
        df_intervalo = df_agrupado[
            (df_agrupado['DATA_PAGTO'] >= inicio_mes) & 
            (df_agrupado['DATA_PAGTO'] <= data)
        ].copy()
        
        if df_intervalo.empty:
            continue
        
        # Agrupar dinamicamente
        agrupado = df_intervalo.groupby(colunas_agrupamento).agg({
            'qte': 'sum',
            'VALOR_PARC': 'sum'
        }).reset_index()
        
        # Informações da data
        info_data = df_agrupado[df_agrupado['DATA_PAGTO'] == data][
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        ].drop_duplicates().iloc[0]
        
        agrupado['DATA_PAGTO'] = data
        agrupado['Indicador'] = 'Pagamentos'
        agrupado['MesAbreviado'] = info_data['MesAbreviado']
        agrupado['nr_dia_util'] = info_data['nr_dia_util']
        agrupado['quartil'] = info_data['quartil']
        agrupado['dt_mes'] = info_data['dt_mes']
        
        resultados.append(agrupado)
    
    # ============================================
    # VALIDAÇÃO DE RESULTADOS
    # ============================================
    if not resultados:
        salvar_log("AVISO: Nenhum resultado gerado após processamento. Retornando DataFrames vazios.", arquivo_log=log_file)
        salvar_log("=" * 60, arquivo_log=log_file)
        return _criar_dataframes_vazios_pagamentos(dimensoes_validas)
    
    df_acumulado = pd.concat(resultados, ignore_index=True)
    
    # ============================================
    # REORDENAR COLUNAS DINAMICAMENTE
    # ============================================
    colunas_finais = (
        ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] +
        dimensoes_validas +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
    )
    
    df_acumulado = df_acumulado[colunas_finais]
    
    salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}", arquivo_log=log_file)
    salvar_log(f"Quantidade total final: {df_acumulado['qte'].sum()}", arquivo_log=log_file)
    salvar_log(f"Valor total final: R$ {df_acumulado['VALOR_PARC'].sum():,.2f}", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)

    # Remover linhas com qte zero
    df_acumulado = df_acumulado[df_acumulado['qte'] > 0]
    
    # ============================================
    # CRIAR VERSÕES UNIQUE E ESFORÇO
    # ============================================
    df_unique = df_acumulado.copy()
    df_unique['FX_ATRASO'] = 'Unique'
    
    df_esforco = df_acumulado.copy()
    df_esforco['FX_ATRASO'] = 'Esforço'
    
    return df_acumulado, df_esforco, df_unique


def _criar_dataframes_vazios_pagamentos(
    dimensoes_segmentacao: Optional[List[str]] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Cria DataFrames vazios com estrutura correta para pagamentos.
    
    Args:
        dimensoes_segmentacao: Dimensões adicionais para incluir nas colunas
    
    Returns:
        tuple: (df_vazio, df_vazio, df_vazio)
    """
    if dimensoes_segmentacao is None:
        dimensoes_segmentacao = []
    
    colunas = (
        ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] +
        dimensoes_segmentacao +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
    )
    
    df_vazio = pd.DataFrame(columns=colunas)
    return df_vazio.copy(), df_vazio.copy(), df_vazio.copy()

def gerar_acumulado_por_dia_util_(df_agrupado):
    """
    Gera DataFrame com acumulado de pagamentos do início do mês até cada dia útil.
    
    Args:
        df_agrupado (pd.DataFrame): DataFrame de pagamentos agrupados
    
    Returns:
        tuple: (df_acumulado, df_esforco, df_unique)
    """
    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)
    salvar_log("GERANDO ACUMULADO POR DIA ÚTIL", arquivo_log=LOG_PAGAMENTOS)
    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)
    
    if df_agrupado.empty:
        salvar_log("AVISO: DataFrame de pagamentos está vazio. Retornando DataFrames vazios.", arquivo_log=LOG_PAGAMENTOS)
        salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)

        colunas = [
            'DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO', 'ORIGEM',
            'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC'
        ]
        df_vazio = pd.DataFrame(columns=colunas)
        return df_vazio.copy(), df_vazio.copy(), df_vazio.copy()
    
    resultados = []
    datas_unicas = sorted(df_agrupado['DATA_PAGTO'].unique())
    
    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=LOG_PAGAMENTOS)
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_PAGAMENTOS)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1).date()
        
        df_intervalo = df_agrupado[
            (df_agrupado['DATA_PAGTO'] >= inicio_mes) & 
            (df_agrupado['DATA_PAGTO'] <= data)
        ].copy()
        
        if df_intervalo.empty:
            continue
        
        # Agrupar por FX_ATRASO e ORIGEM
        agrupado = df_intervalo.groupby(['FX_ATRASO', 'ORIGEM']).agg({
            'qte': 'sum',
            'VALOR_PARC': 'sum'
        }).reset_index()
        
        info_data = df_agrupado[df_agrupado['DATA_PAGTO'] == data][
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        ].drop_duplicates().iloc[0]
        
        agrupado['DATA_PAGTO'] = data
        agrupado['Indicador'] = 'Pagamentos'
        agrupado['MesAbreviado'] = info_data['MesAbreviado']
        agrupado['nr_dia_util'] = info_data['nr_dia_util']
        agrupado['quartil'] = info_data['quartil']
        agrupado['dt_mes'] = info_data['dt_mes']
        
        resultados.append(agrupado)
    
    if not resultados:
        salvar_log("AVISO: Nenhum resultado gerado após processamento. Retornando DataFrames vazios.", arquivo_log=LOG_PAGAMENTOS)
        salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)

        colunas = [
            'DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO', 'ORIGEM',
            'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC'
        ]
        df_vazio = pd.DataFrame(columns=colunas)
        return df_vazio.copy(), df_vazio.copy(), df_vazio.copy()
    
    df_acumulado = pd.concat(resultados, ignore_index=True)
    
    df_acumulado = df_acumulado[[
        'DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO', 'ORIGEM',
        'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC'
    ]]
    
    salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Quantidade total final: {df_acumulado['qte'].sum()}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Valor total final: R$ {df_acumulado['VALOR_PARC'].sum():,.2f}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)

    df_acumulado = df_acumulado[df_acumulado['qte'] > 0]
    
    df_unique = df_acumulado.copy()
    df_unique['FX_ATRASO'] = 'Unique'
    
    df_esforco = df_acumulado.copy()
    df_esforco['FX_ATRASO'] = 'Esforço'
    
    return df_acumulado, df_esforco, df_unique
