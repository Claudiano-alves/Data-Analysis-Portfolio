"""
Módulo de Pipeline de Mailing
Orquestra as funções de tratamento e geração de métricas de mailing.
"""

from utils.utils import unir_dataframes
from .data_processing import tratar_base_mailing_hist
from .metricas_acumuladas import (
    gerar_acumulado_maling_hist_fxAtraso,
    gerar_acumulado_maling_hist_unique
)


def processar_mailing_completo(df_mailing_hist, df_dw_calendario):
    """
    Pipeline completo de tratamento e geração de métricas de mailing.
    
    Orquestra:
    1. Tratamento de mailing (PRODUTO, FX_ATRASO)
    2. Geração de métricas acumuladas (por faixa e unique)
    3. União de todas as métricas
    
    Args:
        df_mailing_hist (pd.DataFrame): DataFrame bruto de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com métricas consolidadas de mailing
    """
    # ============================================
    # ETAPA 1: TRATAMENTO
    # ============================================
    df_mailing_tratado = tratar_base_mailing_hist(df_mailing_hist)
    
    # ============================================
    # ETAPA 2: MÉTRICAS ACUMULADAS
    # ============================================
    df_mailing_fxAtraso = gerar_acumulado_maling_hist_fxAtraso(
        df_mailing_tratado,
        df_dw_calendario
    )
    df_mailing_unique = gerar_acumulado_maling_hist_unique(
        df_mailing_tratado,
        df_dw_calendario
    )
    
    # ============================================
    # ETAPA 3: CONSOLIDAÇÃO
    # ============================================
    df_mailing_final = unir_dataframes(df_mailing_fxAtraso, df_mailing_unique)
    
    return df_mailing_tratado, df_mailing_final


def gerar_acumulado_mailing_hist(df_mailing_hist, df_dw_calendario):
    """
    Função wrapper - mantém compatibilidade com código legado.
    Gera acumulado completo de mailing (fxAtraso + unique).
    """
    return processar_mailing_completo(df_mailing_hist, df_dw_calendario)
