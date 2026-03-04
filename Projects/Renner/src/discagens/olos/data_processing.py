"""
Módulo de Tratamentos - Discagens OLOS (Renner)

Responsável por:
- Enriquecimento com mailing_hist e calendário
"""

import pandas as pd
from utils.utils import salvar_log, registrar_tempo
from utils.config import LOG_DISCAGENS


@registrar_tempo("Enriquecimento OLOS com mailing e calendário", arquivo_log=LOG_DISCAGENS)
def enriquecer_olos_com_mailing_calendario(df_olos, df_mailing_hist, df_dw_calendario):
    """
    Enriquece o DataFrame de discagens OLOS com dados de mailing_hist e calendário.
    Retorna dois DataFrames: um com FX_ATRASO e outro sem.

    Args:
        df_olos (pd.DataFrame): DataFrame com discagens OLOS (DATA, CONTRATO, CAMPANHA, ROUTE)
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing tratado —
                                        todas as colunas presentes serão trazidas,
                                        incluindo segmentações específicas (ex: FAIXA)
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário

    Returns:
        tuple: (df_com_fx_atraso, df_sem_fx_atraso)
    """
    df_resultado = df_olos.copy()

    df_resultado['CONTRATO'] = df_resultado['CONTRATO'].astype(str).str.upper().str.strip()

    df_mailing_temp = df_mailing_hist.copy()
    df_mailing_temp['CONTRATO'] = df_mailing_temp['CONTRATO'].astype(str).str.upper().str.strip()

    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_mailing_temp['DATA'] = pd.to_datetime(df_mailing_temp['DATA']).dt.date

    salvar_log(f"📊 Merge OLOS com mailing_hist... ({len(df_resultado):,} registros)", arquivo_log=LOG_DISCAGENS)
    df_resultado = df_resultado.merge(df_mailing_temp, on=['CONTRATO', 'DATA'], how='left')
    registros_sem_fx = df_resultado['FX_ATRASO'].isna().sum()
    salvar_log(f"   ⚠️  Registros sem FX_ATRASO: {registros_sem_fx:,}", arquivo_log=LOG_DISCAGENS)

    # Resolver CPF duplicado — CPF_x = olos (via mailing), CPF_y = mailing
    # Para quem cruzou: usa CPF do mailing (CPF_y)
    # Para quem não cruzou: usa CPF das discagens (CPF_x)
    if 'CPF_x' in df_resultado.columns and 'CPF_y' in df_resultado.columns:
        df_resultado['CPF'] = df_resultado['CPF_y'].fillna(df_resultado['CPF_x'])
        df_resultado = df_resultado.drop(columns=['CPF_x', 'CPF_y'])

    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date

    salvar_log(f"📅 Merge OLOS com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    df_resultado = df_resultado.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='left'
    ).drop(columns=['dt_data'])

    df_com_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].notna()].copy()
    df_sem_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].isna()].copy()

    salvar_log(f"📦 COM FX_ATRASO: {len(df_com_fx_atraso):,} | SEM FX_ATRASO: {len(df_sem_fx_atraso):,}", arquivo_log=LOG_DISCAGENS)
    return df_com_fx_atraso, df_sem_fx_atraso

def unir_discagens_expert_olos(df_expert, df_olos):
    """
    Une os DataFrames de discagens Expert e OLOS em um único analítico.
    
    - CAMPANHA (OLOS) é renomeada para GrupoPrincipal
    - ROUTE (OLOS) é descartada
    - Coluna CANAL identifica a origem dos dados ('EXPERT' ou 'OLOS')
    - Colunas específicas da Expert ficam nulas nos registros OLOS
    
    Args:
        df_expert (pd.DataFrame): DataFrame de discagens Expert tratado
        df_olos (pd.DataFrame): DataFrame de discagens OLOS enriquecido
    
    Returns:
        pd.DataFrame: DataFrame unificado
    """
    # ============================================
    # EXPERT
    # ============================================
    df_expert_temp = df_expert.copy()
    df_expert_temp['CANAL'] = 'EXPERT'

    # ============================================
    # OLOS
    # ============================================
    df_olos_temp = df_olos.copy()
    df_olos_temp = df_olos_temp.rename(columns={'CAMPANHA': 'GrupoPrincipal'})
    df_olos_temp = df_olos_temp.drop(columns=['ROUTE'], errors='ignore')
    df_olos_temp['CANAL'] = 'OLOS'

    # ============================================
    # UNIÃO
    # ============================================
    df_unido = pd.concat([df_expert_temp, df_olos_temp], ignore_index=True)

    # ============================================
    # REORDENAR COLUNAS
    # ============================================
    colunas_prioritarias = [
        'DATA', 'CONTRATO', 'CPF', 'GrupoPrincipal', 'CANAL',
        'ATRASO', 'COD_CLI', 'COD_CAR', 'VALOR',
        'FX_ATRASO', 'CONTRATO_ORIGINAL', 'PRODUTO', 'FAIXA',
        'OPERACAO', 'ESTADO',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]

    # Garante apenas colunas que existem + restante não listado ao final
    colunas_existentes = [c for c in colunas_prioritarias if c in df_unido.columns]
    colunas_restantes = [c for c in df_unido.columns if c not in colunas_existentes]
    df_unido = df_unido[colunas_existentes + colunas_restantes]

    return df_unido

__all__ = [
    'enriquecer_olos_com_mailing_calendario'
]