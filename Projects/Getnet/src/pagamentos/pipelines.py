"""
Módulo de Pipeline de Pagamentos
Orquestra as funções de tratamento e geração de métricas de pagamentos.
"""

from ..utils import unir_dataframes
from .data_processing import data_pagamentos
from .metricas_acumuladas import gerar_acumulado_por_dia_util


def processar_pagamentos_completo(df_pagamentos, df_acordos, df_mailing_hist, df_dw_calendario):
    """
    Pipeline completo de tratamento e geração de métricas de pagamentos.
    
    Orquestra:
    1. Tratamento de pagamentos (merge com acordos, mailing)
    2. Geração de métricas acumuladas (acumulado, esforço, unique)
    3. União de todas as métricas
    
    Args:
        df_pagamentos (pd.DataFrame): DataFrame bruto de pagamentos
        df_acordos (pd.DataFrame): DataFrame de acordos
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        tuple: (df_pagamentos_final, df_sem_fx_atraso, df_pagamentos_analitico)
    """
    # ============================================
    # ETAPA 1: TRATAMENTO
    # ============================================
    df_pagamentos_tratado, df_pagamentos_sem_fx_atraso, df_pagamentos_analitico = data_pagamentos(
        df_pagamentos, df_acordos, df_mailing_hist, df_dw_calendario
    )
    
    # ============================================
    # ETAPA 2: MÉTRICAS ACUMULADAS
    # ============================================
    df_pagamentos_acum, df_esforco, df_unique = gerar_acumulado_por_dia_util(df_pagamentos_tratado)
    
    # ============================================
    # ETAPA 3: CONSOLIDAÇÃO
    # ============================================
    if df_pagamentos_acum.empty and df_esforco.empty and df_unique.empty:
        df_pagamentos_final = df_pagamentos_acum.copy()
    else:
        df_pagamentos_final = unir_dataframes(df_pagamentos_acum, df_esforco, df_unique)
    
    # ============================================
    # ETAPA 5: SALVAR ANALÍTICOS
    # ============================================
    from Projects.utils.utils import salvar_dataframes_csv
    from ..config import PROCESS_PATHS
    
    salvar_dataframes_csv(
        caminho_destino=PROCESS_PATHS["pagamentos"],
        df_pagamentos_analitico=df_pagamentos_analitico,
        df_pagamentos_sem_fx_atraso=df_pagamentos_sem_fx_atraso
    )

    return df_pagamentos_final, df_pagamentos_sem_fx_atraso, df_pagamentos_analitico


def tratar_pagamentos(df_pagamentos, df_acordos, df_mailing_hist, df_dw_calendario):
    """
    Função wrapper - mantém compatibilidade com código legado.
    Processa pagamentos com todas as etapas.
    """
    return processar_pagamentos_completo(df_pagamentos, df_acordos, df_mailing_hist, df_dw_calendario)
