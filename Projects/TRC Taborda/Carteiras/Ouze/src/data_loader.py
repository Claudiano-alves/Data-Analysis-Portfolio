import pandas as pd
from typing import Dict
from utils.data_loader import data_loader
from utils.db_connection import get_db_connections
from Ouze.src.config import DATASETS_TO_LOAD, WHERE_CLAUSES


def load_data_ouze(
    data_inicio: str,
    data_fim: str
) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados específicos para a carteira Ouze.
    
    Args:
        data_inicio: Data inicial do período (formato 'YYYY-MM-DD')
        data_fim: Data final do período (formato 'YYYY-MM-DD')
    
    Returns:
        Dict com DataFrames carregados
    
    Note:
        Não carrega: Discagens Trestto
        Filtros SQL definidos em config.WHERE_CLAUSES
    """

    # 🔌 Conexões gerenciadas pelo context manager
    with get_db_connections() as (conn_trc, conn_bd2, conn_src):

        # 📋 Datasets necessários para Ouze
        datasets_ouze = [
            nome for nome, ativo in DATASETS_TO_LOAD.items() if ativo
        ]

        # 🔄 Carregamento efetivo usando configurações centralizadas
        return data_loader(
            conn_trc=conn_trc,
            conn_bd2=conn_bd2,
            conn_src=conn_src,
            data_inicio=data_inicio,
            data_fim=data_fim,
            where_campanhas=WHERE_CLAUSES['campanhas'],
            where_clientes_mailing=WHERE_CLAUSES['clientes_mailing'],
            where_acionamentos=WHERE_CLAUSES['acionamentos'],
            where_tabulacao=WHERE_CLAUSES['tabulacao'],
            where_clientes_pagamentos=WHERE_CLAUSES['clientes_pagamentos'],
            where_clientes_acordos=WHERE_CLAUSES['clientes_acordos'],
            where_massivos=WHERE_CLAUSES['massivos'],
            where_telefones=WHERE_CLAUSES['telefones'],
            datasets_to_load=datasets_ouze
        )