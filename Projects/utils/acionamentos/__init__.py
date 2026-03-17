"""
Acionamentos - Módulo de tratamento, processamento e análise de acionamentos

Estrutura de módulos:
- tratamentos: Limpeza, validação e enriquecimento de dados
- metricas_acumuladas: Métricas mensais consolidadas
- metricas_diarias: Métricas diárias (dia a dia)
- pipelines: Orquestração completa
"""

from .data_processing import (
    tratar_acionamentos_tabulacao,
    confere_tabulacao_acionamentos,
    enriquecer_acionamentos,
    separar_inconsistencias
)

from .metricas_acumuladas import (
    acionamentos_segmentacoes_funil
    ,acionamentos_unique_funil
    ,acionamentos_esforco_funil

    ,acionamentos_segmentacoes_daily
    ,acionamentos_unique_daily
    ,acionamentos_esforco_daily
    ,processar_acumulados_acionamentos
)

from .metricas_diarias import (
    acionamentos_unique_origem_fxAtraso,
    acionamentos_unique_fxAtraso,
    acionamentos_esforco_origem_fxAtraso
)

from .pipelines import acionamentos_humano

__all__ = [
    # Tratamentos
    'tratar_acionamentos_tabulacao',
    'confere_tabulacao_acionamentos',
    'enriquecer_acionamentos',
    'separar_inconsistencias',
    
    # Métricas acumuladas
    'acionamentos_segmentacoes_funil',
    'acionamentos_unique_funil',
    'acionamentos_esforco_funil',

    'acionamentos_segmentacoes_daily',
    'acionamentos_unique_daily',
    'acionamentos_esforco_daily',

    'processar_acumulados_acionamentos'
    
    # Pipeline
    'acionamentos_humano'
]
