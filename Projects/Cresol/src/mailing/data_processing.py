import pandas as pd


def add_pf_pj(df: pd.DataFrame, coluna_cpf: str = 'CPF') -> pd.DataFrame:
    """
    Adiciona coluna PF_PJ ao DataFrame com base no CPF.
    
    Lógica: se os 3 dígitos centrais dos últimos 6 do CPF forem '000' → PJ, senão → PF.
    Equivalente SQL:
        CASE 
            WHEN LEFT(RIGHT(LTRIM(RTRIM(CPF)),6),3) = '000' THEN 'PJ' 
            ELSE 'PF'
        END AS PF_PJ

    Args:
        df: DataFrame de entrada
        coluna_cpf: Nome da coluna CPF (padrão: 'CPF')

    Returns:
        DataFrame com coluna PF_PJ adicionada
    """
    df = df.copy()
    df['PF_PJ'] = df[coluna_cpf].astype(str).str.strip().str[-6:].str[:3].apply(
        lambda x: 'PJ' if x == '000' else 'PF'
    )
    return df


def add_pa(df: pd.DataFrame, coluna_cod_cli: str = 'COD_CLI') -> pd.DataFrame:
    """
    Adiciona coluna PA ao DataFrame com base no COD_CLI.

    Equivalente SQL:
        CASE 
            WHEN COD_CLI = 181 THEN 'CRESOL BASER'
            WHEN COD_CLI = 230 THEN 'CRESOL SICOPER'
            WHEN COD_CLI = 231 THEN 'CRESOL SICOPER CALLCENTER'
            WHEN COD_CLI = 220 THEN 'ASCOOB CRESOL'
            WHEN COD_CLI = 254 THEN 'CENTRAL BRASIL EXTRA'
        END AS PA

    Args:
        df: DataFrame de entrada
        coluna_cod_cli: Nome da coluna COD_CLI (padrão: 'COD_CLI')

    Returns:
        DataFrame com coluna PA adicionada
    """
    PA_MAP = {
        181: 'CRESOL BASER',
        230: 'CRESOL SICOPER',
        231: 'CRESOL SICOPER CALLCENTER',
        220: 'ASCOOB CRESOL',
        254: 'CENTRAL BRASIL EXTRA',
    }

    df = df.copy()
    df['PA'] = df[coluna_cod_cli].map(PA_MAP)
    return df