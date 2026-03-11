"""
Módulo de Pipelines - Discagens Expert

Responsável pela orquestração completa do processamento de discagens expert:
- Enriquecimento com tabulações
- Tratamento base
- Enriquecimento com mailing e calendário
- Segmentação
- Cálculo de métricas
"""

import pandas as pd
from utils.utils import unir_dataframes, salvar_log, registrar_tempo
from ...config import LOG_DISCAGENS
from .data_processing import (
    aplicar_transformacoes_discagens,
    enriquecer_com_mailing_calendario,
    segmentar_discagens_expert
)
from .metricas_acumuladas import (
    discagens_esforco_funil,
    discagens_fxAtraso_funil,
    discagens_unique_funil,
    processar_acumulados_discagens,
    discagens_fxAtraso_daily,
    discagens_unique_daily,
    discagens_esforco_daily
)


@registrar_tempo("Pipeline completo DISCAGENS EXPERT", arquivo_log=LOG_DISCAGENS)
def processar_discagens_expert_completo(
    df_discagens_expert,
    df_mailing_hist,
    df_dw_calendario
):
    """
    Executa o pipeline completo de discagens expert:
    1. Tratamento base (operação, estado, origem)
    2. Enriquecimento com tabulações robô
    3. Enriquecimento com mailing e calendário
    4. Segmentação
    5. Cálculo de métricas (esforço, unique, fxAtraso_origem)
    6. União de resultados
    
    Args:
        df_discagens_expert (pd.DataFrame): DataFrame bruto de discagens expert
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing
        df_dw_calendario (pd.DataFrame): DataFrame com calendário
    
    Returns:
        tuple: (df_acionamentos_expert, df_analitico_expert, df_sem_fx_atraso, 
                df_humano_tabulados, df_operacao_outros)
    """
    
    # 1. Tratamento base
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
    salvar_log("📊 INICIANDO PIPELINE DE DISCAGENS EXPERT", arquivo_log=LOG_DISCAGENS)
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)

    df_tratado = aplicar_transformacoes_discagens(df_discagens_expert)
    
    # 3. Enriquecer com mailing e calendário
    df_com_fx_atraso, df_discagens_sem_fx_atraso = enriquecer_com_mailing_calendario(
        df_tratado, df_mailing_hist, df_dw_calendario
    )
    
    # 4. Segmentar
    df_enriquecido_discagens_expert_limpo, df_humano_tabulados_como_robo, df_dicagens_operacaoOutros = segmentar_discagens_expert(
        df_com_fx_atraso
    )
    
    # 5. Calcular métricas
    salvar_log("\n📈 Calculando métricas acumuladas...", arquivo_log=LOG_DISCAGENS)
    df_esforco = discagens_esforco(df_enriquecido_discagens_expert_limpo, df_dw_calendario)
    df_unique = discagens_unique(df_enriquecido_discagens_expert_limpo, df_dw_calendario)
    df_fxAtraso_origem = acionamentos_fxAtraso_origem_expert(df_enriquecido_discagens_expert_limpo, df_dw_calendario)
    
    # 6. Unir resultados
    salvar_log("\n📦 Consolidando resultados...", arquivo_log=LOG_DISCAGENS)
    df_acionamentos_expert = unir_dataframes(
        df_fxAtraso_origem, df_unique, df_esforco
    )

    # Analitico é o dataframe segmentado
    df_analitico_expert = df_enriquecido_discagens_expert_limpo.copy()
    
    salvar_log("\n✅ PIPELINE DE DISCAGENS EXPERT CONCLUÍDO!", arquivo_log=LOG_DISCAGENS)
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)

    # ============================================
    # ETAPA 5: SALVAR ANALÍTICOS
    # ============================================
    from Projects.utils.utils import salvar_dataframes_csv
    from ...config import PROCESS_PATHS
    
    salvar_dataframes_csv(
        processo="discagens",
        df_analitico_expert=df_analitico_expert,
        df_discagens_sem_fx_atraso=df_discagens_sem_fx_atraso,
        df_humano_tabulados_como_robo=df_humano_tabulados_como_robo,
        df_dicagens_operacaoOutros=df_dicagens_operacaoOutros
    )
    
    return df_acionamentos_expert, df_analitico_expert, df_discagens_sem_fx_atraso, df_humano_tabulados_como_robo, df_dicagens_operacaoOutros


__all__ = [
    'processar_discagens_expert_completo',

    # Metricas
    'discagens_esforco_funil',
    'discagens_fxAtraso_funil',
    'discagens_unique_funil',
    'processar_acumulados_discagens',
    'discagens_fxAtraso_daily',
    'discagens_unique_daily',
    'discagens_esforco_daily'
]
