"""
Acionamentos - Módulo de tratamento, processamento e análise de acionamentos

Estrutura de módulos:
- tratamentos: Limpeza, validação e enriquecimento de dados
- metricas_acumuladas: Métricas mensais consolidadas
- metricas_diarias: Métricas diárias (dia a dia)
- pipelines: Orquestração completa
"""

from .tratamentos import (
    tratar_acionamentos_tabulacao,
    confere_tabulacao_acionamentos,
    enriquecer_acionamentos,
    separar_inconsistencias,
    acionamentos_duplicados
)

from .metricas_acumuladas import (
    acionamentos_fxAtraso_origem_humano,
    acionamentos_unique_humano,
    acionamentos_esforco_humano
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
    'acionamentos_duplicados',
    
    # Métricas acumuladas
    'acionamentos_fxAtraso_origem_humano',
    'acionamentos_unique_humano',
    'acionamentos_esforco_humano',
    
    # Métricas diárias
    'acionamentos_unique_origem_fxAtraso',
    'acionamentos_unique_fxAtraso',
    'acionamentos_esforco_origem_fxAtraso',
    
    # Pipeline
    'acionamentos_humano'
]
