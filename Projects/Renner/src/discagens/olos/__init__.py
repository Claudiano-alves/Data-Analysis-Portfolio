"""
__init__.py - Módulo Olos de Discagens

Exports principais para importação simplificada
"""

from .data_processing import (
    enriquecer_olos_com_mailing_calendario
)

__all__ = [
    'enriquecer_olos_com_mailing_calendario',
    'unir_discagens_expert_olos'
]
