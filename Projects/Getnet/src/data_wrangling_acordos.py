import pandas as pd

def adicionar_tipo_negociacao(df_acordos):
    """
    Adiciona coluna identificando se a negociação foi feita por ROBÔ ou HUMANO.
    
    Args:
        df_acordos: DataFrame com coluna RECUP_ACO
        
    Returns:
        DataFrame com nova coluna TIPO_NEGOCIACAO
    """
    codigos_robo = [1626, 1, 11003]
    
    df_acordos['TIPO'] = df_acordos['RECUP_ACO'].apply(
        lambda x: 'ROBÔ' if x in codigos_robo else 'HUMANO'
    )
    
    return df_acordos