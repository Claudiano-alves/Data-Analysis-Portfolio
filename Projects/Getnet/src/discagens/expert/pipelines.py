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
from ...utils import unir_dataframes, salvar_log, registrar_tempo
from .tratamentos import (
    tratar_base_discagens_expert,
    criar_df_tabulacoes_robo,
    enriquecer_com_tabulacoes_robo,
    enriquecer_com_mailing_calendario,
    segmentar_discagens_expert
)
from .metricas_acumuladas import (
    acionamentos_esforco_expert,
    acionamentos_unique_expert,
    acionamentos_fxAtraso_origem_expert
)


@registrar_tempo("Pipeline completo DISCAGENS EXPERT")
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
    salvar_log("="*80)
    salvar_log("📊 INICIANDO PIPELINE DE DISCAGENS EXPERT")
    salvar_log("="*80)
    
    df_tratado = tratar_base_discagens_expert(df_discagens_expert)
    
    # 2. Enriquecer com tabulações robô
    df_tabulacoes_robo = criar_df_tabulacoes_robo()
    df_enriquecido = enriquecer_com_tabulacoes_robo(df_tratado, df_tabulacoes_robo)
    
    # 3. Enriquecer com mailing e calendário
    df_com_fx_atraso, df_sem_fx_atraso = enriquecer_com_mailing_calendario(
        df_enriquecido, df_mailing_hist, df_dw_calendario
    )
    
    # 4. Segmentar
    df_restante, df_humano_tabulados, df_operacao_outros = segmentar_discagens_expert(
        df_com_fx_atraso
    )
    
    # 5. Calcular métricas
    salvar_log("\n📈 Calculando métricas acumuladas...")
    df_esforco = acionamentos_esforco_expert(df_restante, df_dw_calendario)
    df_unique = acionamentos_unique_expert(df_restante, df_dw_calendario)
    df_fxAtraso_origem = acionamentos_fxAtraso_origem_expert(df_restante, df_dw_calendario)
    
    # 6. Unir resultados
    salvar_log("\n📦 Consolidando resultados...")
    df_acionamentos_expert = unir_dataframes(
        df_fxAtraso_origem, df_unique, df_esforco
    )
    
    # Analitico é o dataframe segmentado
    df_analitico_expert = df_restante.copy()
    
    salvar_log("\n✅ PIPELINE DE DISCAGENS EXPERT CONCLUÍDO!")
    salvar_log("="*80)
    
    return df_acionamentos_expert, df_analitico_expert, df_sem_fx_atraso, df_humano_tabulados, df_operacao_outros


__all__ = [
    'processar_discagens_expert_completo'
]
