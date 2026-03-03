"""
__init__.py - Módulo Discagens

Exports principais para importação simplificada dos módulos expert e trestto
"""

from .expert import (
    processar_discagens_expert_completo
)

from .olos import (
    enriquecer_olos_com_mailing_calendario
)

__all__ = [
    'processar_discagens_expert_completo',
    'enriquecer_olos_com_mailing_calendario'
]
