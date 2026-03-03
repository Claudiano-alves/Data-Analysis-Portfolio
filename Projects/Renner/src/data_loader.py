import pandas as pd
from typing import Dict
from utils.data_loader import data_loader
from utils.db_connection import get_db_connections
from Renner.src.config import WHERE_CLAUSES, DATASETS_TO_LOAD, COLUMNS_MASSIVOS, DATASETS_EXCLUSIVOS_RENNER

def load_data_renner(
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
        Filtros SQL: COD_CLI = 247, ID_CAR = 100
        Colunas definidas em config.COLUMNS_MASSIVOS ou via parâmetro
        Datasets exclusivos da Estácio definidos em DATASETS_EXCLUSIVOS_RENNER
    """

    if columns is None:
        columns = COLUMNS_MASSIVOS

    with get_db_connections() as (conn_trc, conn_bd2, conn_src):

        # Exclui datasets exclusivos da Estácio — o data_loader não os reconhece
        datasets_renner = [
            nome for nome, ativo in DATASETS_TO_LOAD.items()
            if ativo and nome not in DATASETS_EXCLUSIVOS_RENNER
        ]

        # 🔄 Carregamento padrão via função utilitária
        dados = data_loader(
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
            datasets_to_load=datasets_renner,
            columns=columns
        )

        # 🔄 Carregamentos exclusivos da Renner
        for dataset, get_query in DATASETS_EXCLUSIVOS_RENNER.items():
            print(f"🔍 Carregando {dataset} (Renner)...")
            
            # Se for a query de discagens_olos, passamos as datas
            if dataset == 'discagens_olos':
                query_sql = get_query(data_inicio, data_fim)
            else:
                # Para base_auxiliar, se não precisar de data, mantém assim
                # Ou passe as datas também se a função get_query_base_aux_renner aceitar
                query_sql = get_query() 

            dados[dataset] = pd.read_sql(query_sql, conn_src)
            print(f"✅ {dataset} carregado — {len(dados[dataset]):,} linhas")

    return dados