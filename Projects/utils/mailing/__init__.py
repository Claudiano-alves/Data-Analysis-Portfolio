"""
Mailing - Módulo de tratamento, processamento e análise de mailing history

Estrutura de módulos:
- tratamentos: Limpeza, validação e enriquecimento de dados
- metricas_acumuladas: Métricas mensais consolidadas
- pipelines: Orquestração completa
"""

from .data_processing import (
    adicionar_faixa_atraso,
    adicionar_valor_principal
)

from .metricas_acumuladas import (
    gerar_acumulado_mailing_hist_segmentacoes,
    gerar_acumulado_mailing_hist_unique
)

from .pipelines import (
    processar_mailing_completo
)

__all__ = [
    # Tratamentos
    'adicionar_faixa_atraso',
    'adicionar_valor_principal',
    
    # Métricas acumuladas
    'gerar_acumulado_mailing_hist_segmentacoes',
    'gerar_acumulado_mailing_hist_unique',
    
    # Pipeline
    'processar_mailing_completo'
]
