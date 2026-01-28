"""
Módulo de Pipeline de Acionamentos
Orquestra as funções de tratamento, métricas acumuladas e diárias.
"""

from utils.utils import unir_dataframes
from .data_processing import (
    tratar_acionamentos_tabulacao,
    confere_tabulacao_acionamentos,
    enriquecer_acionamentos
)
from .metricas_acumuladas import (
    acionamentos_fxAtraso_origem_humano,
    acionamentos_unique_humano,
    acionamentos_esforco_humano
)


def acionamentos_humano(df_tab_acionamentos, df_tabulacao_aciona, df_dw_calendario, df_maling_hist):
    """
    Pipeline completo de tratamento e geração de métricas de acionamentos HUMANO.
    
    Orquestra:
    1. Tratamento de tabulações
    2. Conferência de tabulações aos acionamentos
    3. Enriquecimento com mailing_hist e calendário
    4. Geração de métricas acumuladas (fxAtraso + origem, unique, esforço)
    5. União de todas as métricas em um único DataFrame
    
    Args:
        df_tab_acionamentos (pd.DataFrame): DataFrame com acionamentos
        df_tabulacao_aciona (pd.DataFrame): DataFrame com tabulações
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        df_maling_hist (pd.DataFrame): DataFrame com histórico de mailing
    
    Returns:
        tuple: (
            df_acionamentos_humano: DataFrame com métricas consolidadas,
            df_analitico_acionamentos_humano: DataFrame enriquecido para análise detalhada,
            df_acion_semFaixa_humano: Acionamentos sem FX_ATRASO,
            df_acion_semDescricao_humano: Acionamentos sem DESC_ACIONA,
            df_acion_semOrigem_humano: Acionamentos sem ORIGEM
        )
    """
    # ============================================
    # ETAPA 1: TRATAMENTO
    # ============================================
    df_tabulacao_aciona = tratar_acionamentos_tabulacao(df_tabulacao_aciona)
    df_acionamentos_tabulados = confere_tabulacao_acionamentos(df_tab_acionamentos, df_tabulacao_aciona)
    
    # ============================================
    # ETAPA 2: ENRIQUECIMENTO
    # ============================================
    df_acionamentos_enriquecido_limpo, df_acion_semFaixa_humano, df_acion_semDescricao_humano, df_acion_semOrigem_humano = enriquecer_acionamentos(
        df_acionamentos_tabulados, 
        df_maling_hist, 
        df_dw_calendario
    )

    # ============================================
    # ETAPA 3: MÉTRICAS ACUMULADAS
    # ============================================
    df_acionamentos_fxAtraso_origem_humano = acionamentos_fxAtraso_origem_humano(
        df_acionamentos_enriquecido_limpo, 
        df_dw_calendario
    )
    df_acionamentos_unique_humano = acionamentos_unique_humano(
        df_acionamentos_enriquecido_limpo, 
        df_dw_calendario
    )
    df_acionamentos_esforco_humano = acionamentos_esforco_humano(
        df_acionamentos_enriquecido_limpo, 
        df_dw_calendario
    )

    # ============================================
    # ETAPA 4: CONSOLIDAÇÃO
    # ============================================
    df_acionamentos_humano = unir_dataframes(
        df_acionamentos_fxAtraso_origem_humano, 
        df_acionamentos_unique_humano, 
        df_acionamentos_esforco_humano
    )

    df_analitico_acionamentos_humano = df_acionamentos_enriquecido_limpo.copy()
    
    # ============================================
    # ETAPA 5: SALVAR ANALÍTICOS
    # ============================================
    from Projects.utils.utils import salvar_dataframes_csv
    from ..config import PROCESS_PATHS
    
    salvar_dataframes_csv(
        processo="acionamentos",
        df_enriquecido=df_acionamentos_enriquecido_limpo,
        df_sem_faixa=df_acion_semFaixa_humano,
        df_sem_descricao=df_acion_semDescricao_humano,
        df_sem_origem=df_acion_semOrigem_humano
    )

    return (
        df_acionamentos_humano, 
        df_analitico_acionamentos_humano, 
        df_acion_semFaixa_humano, 
        df_acion_semDescricao_humano, 
        df_acion_semOrigem_humano
    )
