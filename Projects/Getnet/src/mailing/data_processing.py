"""
Módulo de Tratamento de Mailing History
Contém funções para limpeza, validação e enriquecimento de dados de mailing.
"""

import numpy as np
import pandas as pd
from Projects.utils.utils import registrar_tempo, salvar_log
from ..config import LOG_MAILING


FAIXAS_ATRASO_BINS = [float('-inf'), 0, 30, 60, 90, 120, 150, 180, 360, 720, float('inf')]
FAIXAS_ATRASO_LABELS = [
    'Preventivo',
    '0-30',
    '31-60',
    '61-90',
    '91-120',
    '121-150',
    '151-180',
    '181-360',
    '361-720',
    'Maior 720'
]


def adicionar_produto(df):
    """
    Adiciona a coluna PRODUTO ao DataFrame de mailing_hist baseado em COD_CLI e COD_CAR.
    
    Args:
        df (pd.DataFrame): DataFrame com colunas 'COD_CLI' e 'COD_CAR'
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'PRODUTO'
    """
    conditions = [
        (df['COD_CLI'] == 228) & (df['COD_CAR'] == 2),
        (df['COD_CLI'] == 198) & (df['COD_CAR'].isin([1, 2, 3])),
        (df['COD_CLI'] == 196) & (df['COD_CAR'].isin([1, 3, 4]))
    ]
    
    choices = [
        'API',
        'Agenda Negativa',
        'Equipamentos'
    ]
    
    df['PRODUTO'] = np.select(conditions, choices, default='Outros')
    return df


def adicionar_faixa_atraso(df, coluna_atraso='ATRASO'):
    """
    Adiciona a coluna FX_ATRASO ao DataFrame categorizado em faixas padrão.
    
    Args:
        df (pd.DataFrame): DataFrame com coluna de atraso
        coluna_atraso (str): Nome da coluna que contém o valor de atraso
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'FX_ATRASO'
    """
    df['FX_ATRASO'] = pd.cut(
        df[coluna_atraso], 
        bins=FAIXAS_ATRASO_BINS, 
        labels=FAIXAS_ATRASO_LABELS, 
        right=True
    )
    return df


def adicionar_valor_principal(df_mailing_hist, df_cad_devf):
    """
    Adiciona a coluna VALORPRIN_FIN ao DataFrame de mailing através de join com CAD_DEVF.
    
    Args:
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist (com coluna CONTRATO)
        df_cad_devf (pd.DataFrame): DataFrame de CAD_DEVF (com colunas CONTRATO_FIN e VALORPRIN_FIN)
    
    Returns:
        pd.DataFrame: DataFrame de mailing_hist com nova coluna VALORPRIN_FIN
    """
    df_resultado = df_mailing_hist.copy()
    
    df_resultado['CONTRATO'] = df_resultado['CONTRATO'].astype(str)
    df_cad_devf_temp = df_cad_devf[['CONTRATO_FIN', 'VALORPRIN_FIN']].copy()
    df_cad_devf_temp['CONTRATO_FIN'] = df_cad_devf_temp['CONTRATO_FIN'].astype(str)
    
    salvar_log(f"📊 Antes do join - Mailing: {len(df_resultado):,} | CAD_DEVF: {len(df_cad_devf_temp):,}", arquivo_log=LOG_MAILING)
    
    df_resultado = df_resultado.merge(
        df_cad_devf_temp,
        left_on='CONTRATO',
        right_on='CONTRATO_FIN',
        how='left'
    )
    
    df_resultado = df_resultado.drop(columns=['CONTRATO_FIN'])
    
    salvar_log(f"📊 Após join: {len(df_resultado):,}", arquivo_log=LOG_MAILING)
    salvar_log(f"📊 Contratos com valor: {df_resultado['VALORPRIN_FIN'].notna().sum():,}", arquivo_log=LOG_MAILING)
    salvar_log(f"📊 Contratos sem valor: {df_resultado['VALORPRIN_FIN'].isna().sum():,}", arquivo_log=LOG_MAILING)
    
    return df_resultado


def tratar_base_mailing_hist(df):
    """
    Aplica todos os tratamentos padrão para base de mailing_hist.
    
    Args:
        df (pd.DataFrame): DataFrame de mailing_hist
    
    Returns:
        pd.DataFrame: DataFrame tratado com PRODUTO e FX_ATRASO
    """
    df = adicionar_produto(df)
    df = adicionar_faixa_atraso(df)
    return df


def criar_faixa_customizada(df, coluna, bins, labels, nome_nova_coluna=None):
    """
    Cria uma coluna de faixa customizada.
    
    Args:
        df (pd.DataFrame): DataFrame
        coluna (str): Nome da coluna para categorizar
        bins (list): Lista de bins para pd.cut
        labels (list): Lista de labels para as faixas
        nome_nova_coluna (str, optional): Nome da nova coluna. Default: 'FX_{coluna}'
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna
    """
    if nome_nova_coluna is None:
        nome_nova_coluna = f'FX_{coluna}'
    
    df[nome_nova_coluna] = pd.cut(
        df[coluna], 
        bins=bins, 
        labels=labels, 
        right=True
    )
    return df
