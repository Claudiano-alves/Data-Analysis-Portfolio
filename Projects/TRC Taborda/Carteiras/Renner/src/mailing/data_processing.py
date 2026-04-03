"""
Módulo de Tratamento de Mailing History
Contém funções para limpeza, validação e enriquecimento de dados de mailing.
"""

import numpy as np
import pandas as pd
from utils.utils import registrar_tempo, salvar_log
from ..config import LOG_MAILING

def add_faixa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona coluna FAIXA com base na coluna PRODUTO (RENEG/CDC) e ATRASO.
    
    Args:
        df (pd.DataFrame): DataFrame com colunas PRODUTO e ATRASO
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna FAIXA
    """
    df = df.copy()

    is_reneg = df['PRODUTO'] == 'RENEG'

    df['FAIXA'] = 'OUTROS'

    df.loc[is_reneg & df['ATRASO'].between(11, 180), 'FAIXA'] = 'R1'
    df.loc[is_reneg & df['ATRASO'].between(181, 360), 'FAIXA'] = 'R2'
    df.loc[~is_reneg & df['ATRASO'].between(30, 180), 'FAIXA'] = 'C1'
    df.loc[~is_reneg & df['ATRASO'].between(181, 360), 'FAIXA'] = 'C2'
    df.loc[df['ATRASO'].between(361, 540), 'FAIXA'] = 'PRE_WO'
    df.loc[df['ATRASO'] >= 541, 'FAIXA'] = 'WO'

    return df

def merge_mailing_com_base_aux(df_mailing_hist: pd.DataFrame, df_base_auxiliar_renner: pd.DataFrame) -> pd.DataFrame:
    """
    Cruza df_mailing_hist com df_base_auxiliar_renner, incrementando CONTRATO_ORIGINAL e PRODUTO.
    
    Args:
        df_mailing_hist (pd.DataFrame): DataFrame com colunas DATA, CONTRATO, CPF, ATRASO, COD_CLI, COD_CAR, VALOR
        df_base_auxiliar_renner (pd.DataFrame): DataFrame com colunas CONTRATO_FIN, CONTRATO_ORIGINAL, PRODUTO
    
    Returns:
        pd.DataFrame: Todas as colunas do mailing + CONTRATO_ORIGINAL + PRODUTO
    """
    df_aux = df_base_auxiliar_renner[['CONTRATO_FIN', 'CONTRATO_ORIGINAL', 'PRODUTO']]

    df_resultado = df_mailing_hist.merge(
        df_aux,
        left_on='CONTRATO',
        right_on='CONTRATO_FIN',
        how='left'
    ).drop(columns='CONTRATO_FIN')

    return df_resultado
