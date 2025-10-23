import numpy as np
import pandas as pd

FAIXAS_ATRASO_BINS = [float('-inf'), 0, 30, 60, 90, 120, 150, 180, 360, 720, float('inf')]
FAIXAS_ATRASO_LABELS = [
    'Menor 0',
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

# ============================================
# FUNÇÕES DE TRATAMENTO - MAILING_HIST
# ============================================

def adicionar_produto(df):
    """
    Adiciona a coluna PRODUTO ao DataFrame de mailing_hist
    
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
    Adiciona a coluna FX_ATRASO ao DataFrame
    
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


def tratar_base_mailing_hist(df):
    """
    Aplica todos os tratamentos padrão para base de mailing_hist
    
    Args:
        df (pd.DataFrame): DataFrame de mailing_hist
    
    Returns:
        pd.DataFrame: DataFrame tratado
    """
    df = adicionar_produto(df)
    df = adicionar_faixa_atraso(df)
    return df


# ============================================
# FUNÇÃO GENÉRICA
# ============================================

def criar_faixa_customizada(df, coluna, bins, labels, nome_nova_coluna=None):
    """
    Cria uma coluna de faixa customizada
    
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