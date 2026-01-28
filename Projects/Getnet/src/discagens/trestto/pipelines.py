"""
Módulo de Pipelines - Discagens Trestto

Responsável pela orquestração completa do processamento de discagens trestto:
- Consolidação por DATA + CPF
- Enriquecimento com mailing e calendário
- Cálculo de métricas
"""

import pandas as pd
from utils.utils import unir_dataframes, salvar_log, registrar_tempo
from ...config import LOG_DISCAGENS
from .tratamentos import enriquecer_discagens_trestto
from .metricas_acumuladas import (
    acionamentos_esforco_trestto,
    acionamentos_unique_trestto,
    acionamentos_fxAtraso_origem_trestto
)


@registrar_tempo("Pipeline completo DISCAGENS TRESTTO", arquivo_log=LOG_DISCAGENS)
def processar_discagens_trestto_completo(
    df_discagens_trestto,
    df_mailing_hist,
    df_dw_calendario
):
    """
    Executa o pipeline completo de discagens trestto:
    1. Consolidação por DATA + CPF
    2. Enriquecimento com mailing e calendário
    3. Cálculo de métricas (esforço, unique, fxAtraso_origem)
    4. União de resultados
    
    Args:
        df_discagens_trestto (pd.DataFrame): DataFrame bruto de discagens trestto
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing
        df_dw_calendario (pd.DataFrame): DataFrame com calendário
    
    Returns:
        tuple: (df_acionamentos_trestto, df_analitico_trestto, df_sem_fx_atraso)
    """
    
    # 1. Enriquecimento com mailing e calendário
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
    salvar_log("📊 INICIANDO PIPELINE DE DISCAGENS TRESTTO", arquivo_log=LOG_DISCAGENS)
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
    
    df_com_fx_atraso, df_discagens_trestto_sem_fx_atraso = enriquecer_discagens_trestto(
        df_discagens_trestto, df_mailing_hist, df_dw_calendario
    )
    
    # 2. Calcular métricas
    salvar_log("\n📈 Calculando métricas acumuladas...", arquivo_log=LOG_DISCAGENS)
    df_esforco = acionamentos_esforco_trestto(df_com_fx_atraso, df_dw_calendario)
    df_unique = acionamentos_unique_trestto(df_com_fx_atraso, df_dw_calendario)
    df_fxAtraso_origem = acionamentos_fxAtraso_origem_trestto(df_com_fx_atraso, df_dw_calendario)
    
    # 3. Unir resultados
    salvar_log("\n📦 Consolidando resultados...", arquivo_log=LOG_DISCAGENS)
    df_acionamentos_trestto = unir_dataframes(
        df_fxAtraso_origem, df_unique, df_esforco
    )
    
    # Analitico é o dataframe enriquecido
    df_analitico_trestto = df_com_fx_atraso.copy()
    
    salvar_log("\n✅ PIPELINE DE DISCAGENS TRESTTO CONCLUÍDO!", arquivo_log=LOG_DISCAGENS)
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
    
    # ============================================
    # ETAPA 5: SALVAR ANALÍTICOS
    # ============================================
    from Projects.utils.utils import salvar_dataframes_csv
    from ...config import PROCESS_PATHS
    
    salvar_dataframes_csv(
        processo="discagens",
        df_analitico_trestto=df_analitico_trestto,
        df_discagens_trestto_sem_fx_atraso=df_discagens_trestto_sem_fx_atraso
    )

    return df_acionamentos_trestto, df_analitico_trestto, df_discagens_trestto_sem_fx_atraso


__all__ = [
    'processar_discagens_trestto_completo'
]
