"""
__init__.py - Módulo Trestto de Discagens

Exports principais para importação simplificada
"""

from .tratamentos import enriquecer_discagens_trestto
from .metricas_acumuladas import (
    acionamentos_esforco_trestto,
    acionamentos_unique_trestto,
    acionamentos_fxAtraso_origem_trestto
)
from .pipelines import processar_discagens_trestto_completo

__all__ = [
    'enriquecer_discagens_trestto',
    'acionamentos_esforco_trestto',
    'acionamentos_unique_trestto',
    'acionamentos_fxAtraso_origem_trestto',
    'processar_discagens_trestto_completo'
]
