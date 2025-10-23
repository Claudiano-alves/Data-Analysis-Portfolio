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
    return df