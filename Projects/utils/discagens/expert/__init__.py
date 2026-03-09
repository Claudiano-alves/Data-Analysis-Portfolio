"""
__init__.py - Módulo Expert de Discagens

Exports principais para importação simplificada
"""

from .data_processing import (
    enriquecer_com_mailing_calendario,
    segmentar_discagens_expert,
    aplicar_transformacoes_discagens
)

from .metricas_acumuladas import (
    discagens_esforco_funil,
    discagens_fxAtraso_funil,
    discagens_unique_funil,
    processar_acumulados_discagens,
    discagens_fxAtraso_daily,
    discagens_unique_daily,
    discagens_esforco_daily,
    processar_acumulados_discagens
)

from .pipelines import (
    processar_discagens_expert_completo
)

__all__ = [
    'criar_df_tabulacoes_robo',
    'enriquecer_com_mailing_calendario',
    'segmentar_discagens_expert',
    'aplicar_transformacoes_discagens',
    'discagens_esforco_funil',
    'discagens_fxAtraso_funil',
    'discagens_unique_funil',
    'processar_acumulados_discagens',
    'discagens_fxAtraso_daily',
    'discagens_unique_daily',
    'discagens_esforco_daily',
    'processar_acumulados_discagens',
    'processar_discagens_expert_completo'
]
