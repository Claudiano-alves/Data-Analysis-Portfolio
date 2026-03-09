"""
Pagamentos - Módulo de tratamento, processamento e análise de pagamentos

Estrutura de módulos:
- tratamentos: Limpeza, validação e enriquecimento de dados
- metricas_acumuladas: Métricas mensais consolidadas
- pipelines: Orquestração completa
"""

from .data_processing import data_pagamentos

from .metricas_acumuladas import (
    pagamentos_funil, 
    total_pagamentos_funil,
    pagamentos_daily,
    total_pagamentos_daily,
    processar_acumulados_pagamentos
)

from .pipelines import (
    processar_pagamentos_completo,
    tratar_pagamentos
)

__all__ = [
    # Tratamentos
    'data_pagamentos',
    
    # Métricas acumuladas
    'pagamentos_funil', 
    'total_pagamentos_funil',
    'pagamentos_daily',
    'total_pagamentos_daily',
    'processar_acumulados_pagamentos',
    
    # Pipeline
    'processar_pagamentos_completo',
    'tratar_pagamentos'
]
