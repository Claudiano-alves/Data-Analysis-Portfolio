"""
Consolidacao - Módulo de orquestração e consolidação do pipeline

Este módulo coordena a execução de todos os submódulos respeitando
as dependências entre eles.

Estrutura:
- pipelines: Orquestração completa e consolidação
"""

from .pipelines import (
    executar_pipeline_funil_completo,
    consolidar_dataframes_funil
)

__all__ = [
    'executar_pipeline_funil_completo',
    'consolidar_dataframes_funil'
]
