
"""
Pagamentos - Módulo de tratamento, processamento e análise de pagamentos

Estrutura de módulos:
- tratamentos: Limpeza, validação e enriquecimento de dados
- metricas_acumuladas: Métricas mensais consolidadas
- pipelines: Orquestração completa
"""

from .data_processing import data_channels
from .metricas_acumuladas import acumulado_unique, acumulado_por_faixa_atraso, acumulado_esforco

# from .metricas_acumuladas import gerar_acumulado_por_dia_util

# from .pipelines import (
#     processar_pagamentos_completo,
#     tratar_pagamentos
# )

__all__ = [
    # Tratamentos
    'data_channels',
    'acumulado_unique', 
    'acumulado_por_faixa_atraso', 
    'acumulado_esforco'
]
