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
    discagens_esforco_funil
    ,discagens_fxAtraso_funil
    ,discagens_unique_funil
    ,processar_acumulados_discagens
    ,discagens_fxAtraso_daily
    ,discagens_unique_daily
    ,discagens_esforco_daily
    ,discagens_operacao_segmentacoes_funil
    ,discagens_operacao_unique_funil
    ,discagens_operacao_esforco_funil
    ,discagens_operacao_segmentacoes_daily
    ,discagens_operacao_unique_daily
    ,discagens_operacao_esforco_daily
)

from .pipelines import (
    processar_discagens_expert_completo
)

__all__ = [

    # data_processing
    'criar_df_tabulacoes_robo',
    'enriquecer_com_mailing_calendario',
    'segmentar_discagens_expert',
    'aplicar_transformacoes_discagens',

    # metricas_acumuladas
    'discagens_esforco_funil',
    'discagens_fxAtraso_funil',
    'discagens_unique_funil',
    'processar_acumulados_discagens',
    'discagens_fxAtraso_daily',
    'discagens_unique_daily',
    'discagens_esforco_daily',
    'discagens_operacao_segmentacoes_funil',
    'discagens_operacao_unique_funil',
    'discagens_operacao_esforco_funil',
    'discagens_operacao_segmentacoes_daily',
    'discagens_operacao_unique_daily',
    'discagens_operacao_esforco_daily',
    'processar_acumulados_discagens_operacao',
    'processar_acumulados_discagens_completo',

    # pipelines
    'processar_discagens_expert_completo'
]
