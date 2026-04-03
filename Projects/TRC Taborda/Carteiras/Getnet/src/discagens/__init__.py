"""
__init__.py - Módulo Discagens

Exports principais para importação simplificada dos módulos expert e trestto
"""

from .expert import (
    processar_discagens_expert_completo
)

from .trestto import (
    processar_discagens_trestto_completo
)

__all__ = [
    'processar_discagens_expert_completo',
    'processar_discagens_trestto_completo'
]
