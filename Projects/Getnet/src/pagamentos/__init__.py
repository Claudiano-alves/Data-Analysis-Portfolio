"""
Pagamentos - Módulo de tratamento, processamento e análise de pagamentos

Estrutura de módulos:
- tratamentos: Limpeza, validação e enriquecimento de dados
- metricas_acumuladas: Métricas mensais consolidadas
- pipelines: Orquestração completa
"""

from .tratamentos import data_pagamentos

from .metricas_acumuladas import gerar_acumulado_por_dia_util

from .pipelines import (
    processar_pagamentos_completo,
    tratar_pagamentos
)

__all__ = [
    # Tratamentos
    'data_pagamentos',
    
    # Métricas acumuladas
    'gerar_acumulado_por_dia_util',
    
    # Pipeline
    'processar_pagamentos_completo',
    'tratar_pagamentos'
]
