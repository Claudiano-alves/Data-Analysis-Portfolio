import pandas as pd
from typing import Dict
from utils.data_loader import data_loader
from utils.db_connection import get_db_connections
from Estacio.process_digital_channels.src.config import WHERE_CLAUSES, DATASETS_TO_LOAD, COLUMNS_MASSIVOS


def load_data_estacio(
    data_inicio: str,
    data_fim: str,
    columns: Dict[str, str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados específicos para a carteira Estácio.
    
    Args:
        data_inicio: Data inicial do período (formato 'YYYY-MM-DD')
        data_fim: Data final do período (formato 'YYYY-MM-DD')
        columns: Dicionário com colunas customizadas para cada dataset.
                 Se None, usa COLUMNS_MASSIVOS padrão da config.
    
    Returns:
        Dict com DataFrames carregados
    
    Note:
        Carrega apenas: SMS, RCS, Email, WhatsApp
        Filtros SQL: COD_CLI = 252, ID_CAR = 95
        Colunas definidas em config.COLUMNS_MASSIVOS ou via parâmetro
    """
    
    # Usar colunas padrão se não fornecidas
    if columns is None:
        columns = COLUMNS_MASSIVOS

    # 🔌 Conexões gerenciadas pelo context manager
    with get_db_connections() as (conn_trc, conn_bd2, conn_src):

        # 📋 Datasets necessários para Estácio (apenas massivos)
        datasets_estacio = [
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
            datasets_to_load=datasets_estacio,
            columns=columns
        )