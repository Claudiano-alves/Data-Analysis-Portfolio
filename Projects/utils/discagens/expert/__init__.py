"""
__init__.py - Módulo Expert de Discagens

Exports principais para importação simplificada
"""

from .data_processing import (
    tratar_base_discagens_expert,
    criar_df_tabulacoes_robo,
    enriquecer_com_tabulacoes_robo,
    enriquecer_com_mailing_calendario,
    segmentar_discagens_expert,
    aplicar_transformacoes_discagens
)

from .metricas_acumuladas import (
    acionamentos_esforco_expert,
    acionamentos_unique_expert,
    acionamentos_fxAtraso_origem_expert,
    acionamentos_fxAtraso_dinamico
)

from .pipelines import (
    processar_discagens_expert_completo
)

__all__ = [
    'tratar_base_discagens_expert',
    'criar_df_tabulacoes_robo',
    'enriquecer_com_tabulacoes_robo',
    'enriquecer_com_mailing_calendario',
    'segmentar_discagens_expert',
    'aplicar_transformacoes_discagens',
    'acionamentos_esforco_expert',
    'acionamentos_unique_expert',
    'acionamentos_fxAtraso_origem_expert',
    'acionamentos_fxAtraso_dinamico',
    'processar_discagens_expert_completo'
]
