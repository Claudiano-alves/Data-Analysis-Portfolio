"""
Módulo de Tratamento de Mailing History
Contém funções genéricas para limpeza, validação e enriquecimento de dados de mailing.
"""

import pandas as pd
from utils.utils import salvar_log

def adicionar_faixa_atraso(df, bins, labels, coluna_atraso='ATRASO', log_path=None):
    """
    Adiciona a coluna FX_ATRASO ao DataFrame categorizado em faixas fornecidas.
    
    Args:
        df (pd.DataFrame): DataFrame com coluna de atraso
        bins (list): Lista de limites para as faixas (ex: [0, 30, 60...])
        labels (list): Lista de rótulos para as faixas
        coluna_atraso (str): Nome da coluna que contém o valor de atraso. Default: 'ATRASO'
        log_path (str, optional): Caminho do arquivo de log.
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'FX_ATRASO'
    """
    if log_path:
        salvar_log(f"Categorizando faixas de atraso para {len(df)} registros...", arquivo_log=log_path)
    
    df = df.copy()
    df['FX_ATRASO'] = pd.cut(
        df[coluna_atraso], 
        bins=bins, 
        labels=labels, 
        right=True
    )
    return df

def adicionar_valor_principal(df_mailing_hist, df_cad_devf, col_contrato_mailing='CONTRATO', col_contrato_devf='CONTRATO_FIN', log_path=None):
    """
    Adiciona a coluna VALORPRIN_FIN ao DataFrame de mailing através de join com CAD_DEVF.
    
    Args:
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist
        df_cad_devf (pd.DataFrame): DataFrame de CAD_DEVF
        col_contrato_mailing (str): Nome da coluna de contrato no mailing
        col_contrato_devf (str): Nome da coluna de contrato na base de valores
        log_path (str): Caminho para log
    
    Returns:
        pd.DataFrame: DataFrame de mailing_hist com nova coluna VALORPRIN_FIN
    """
    df_resultado = df_mailing_hist.copy()
    
    # Garantir tipos compatíveis para merge
    df_resultado[col_contrato_mailing] = df_resultado[col_contrato_mailing].astype(str)
    
    df_cad_devf_temp = df_cad_devf[[col_contrato_devf, 'VALORPRIN_FIN']].copy()
    df_cad_devf_temp[col_contrato_devf] = df_cad_devf_temp[col_contrato_devf].astype(str)
    
    if log_path:
        salvar_log(f"📊 Antes do join - Mailing: {len(df_resultado):,} | CAD_DEVF: {len(df_cad_devf_temp):,}", arquivo_log=log_path)
    
    df_resultado = df_resultado.merge(
        df_cad_devf_temp,
        left_on=col_contrato_mailing,
        right_on=col_contrato_devf,
        how='left'
    )
    
    # Remove a coluna duplicada do merge se os nomes forem diferentes
    if col_contrato_mailing != col_contrato_devf:
        df_resultado = df_resultado.drop(columns=[col_contrato_devf])
    
    if log_path:
        salvar_log(f"📊 Após join: {len(df_resultado):,}", arquivo_log=log_path)
        salvar_log(f"📊 Contratos com valor: {df_resultado['VALORPRIN_FIN'].notna().sum():,}", arquivo_log=log_path)
    
    return df_resultado

