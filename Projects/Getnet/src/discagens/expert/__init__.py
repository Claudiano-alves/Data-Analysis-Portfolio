"""
__init__.py - Módulo Expert de Discagens

Exports principais para importação simplificada
"""

from .tratamentos import (
    tratar_base_discagens_expert,
    criar_df_tabulacoes_robo,
    enriquecer_com_tabulacoes_robo,
    enriquecer_com_mailing_calendario,
    segmentar_discagens_expert
)

from .metricas_acumuladas import (
    acionamentos_esforco_expert,
    acionamentos_unique_expert,
    acionamentos_fxAtraso_origem_expert
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
    'acionamentos_esforco_expert',
    'acionamentos_unique_expert',
    'acionamentos_fxAtraso_origem_expert',
    'processar_discagens_expert_completo'
]
