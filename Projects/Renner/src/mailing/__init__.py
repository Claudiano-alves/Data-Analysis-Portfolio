"""
Mailing - Módulo de tratamento, processamento e análise de mailing history

Estrutura de módulos:
- tratamentos: Limpeza, validação e enriquecimento de dados
- metricas_acumuladas: Métricas mensais consolidadas
- pipelines: Orquestração completa
"""

from .data_processing import (
    add_faixa,
    merge_mailing_com_base_aux
)

from .metricas_acumuladas import (
    gerar_acumulado_mailing_unique_por_faixa
)

from .pipelines import (
    processar_mailing_completo,
    gerar_acumulado_mailing_hist
)

__all__ = [
    # Tratamentos
    'add_faixa',
    'merge_mailing_com_base_aux',
    
    # Métricas acumuladas
    'gerar_acumulado_mailing_unique_por_faixa',
    
    # Pipeline
    'processar_mailing_completo',
    'gerar_acumulado_mailing_hist'
]
