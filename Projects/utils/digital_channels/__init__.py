
"""
Pagamentos - Módulo de tratamento, processamento e análise de pagamentos

Estrutura de módulos:
- tratamentos: Limpeza, validação e enriquecimento de dados
- metricas_acumuladas: Métricas mensais consolidadas
- pipelines: Orquestração completa
"""

from .data_processing import data_channels
from .metricas_acumuladas import (
    processar_acumulados_massivos

    # Metricas
    ,massivos_segmentacoes_funil
    ,massivos_unique_funil
    ,massivos_esforco_funil
    ,massivos_segmentacoes_daily
    ,massivos_unique_daily
    ,massivos_esforco_daily
)

# from .metricas_acumuladas import gerar_acumulado_por_dia_util

# from .pipelines import (
#     processar_pagamentos_completo,
#     tratar_pagamentos
# )

__all__ = [
    # Tratamentos
    'data_channels',
    
    
    'processar_acumulados_massivos',

    # Metricas
    'massivos_segmentacoes_funil',
    'massivos_unique_funil',
    'massivos_esforco_funil',
    'massivos_segmentacoes_daily',
    'massivos_unique_daily',
    'massivos_esforco_daily'
]
