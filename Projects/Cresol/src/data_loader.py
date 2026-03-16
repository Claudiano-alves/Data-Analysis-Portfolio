import pandas as pd
from typing import Dict
from utils.data_loader import data_loader
from utils.db_connection import get_db_connections
from Cresol.src.config import WHERE_CLAUSES, DATASETS_TO_LOAD, COLUMNS
from utils.config import DEFAULT_COLUMNS

def load_data_cresol(
    data_inicio: str,
    data_fim: str,
) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados específicos para a carteira Cresol.
 
    DATASETS_TO_LOAD é a única fonte de verdade sobre o que carregar.
    Cada entry tem:
        active: bool  → se False, o dataset é ignorado
        query:        → None = fluxo padrão via data_loader
                        função = dataset exclusivo, carregado manualmente
 
    Resolução de colunas por dataset (três camadas):
        1. COLUMNS[dataset]         → colunas específicas da Cresol (config da carteira)
        2. DEFAULT_COLUMNS[dataset] → colunas padrão (utils/config.py)
        3. None                     → SELECT * na query
 
    Args:
        data_inicio: Data inicial do período (formato 'YYYY-MM-DD')
        data_fim: Data final do período (formato 'YYYY-MM-DD')
 
    Returns:
        Dict com DataFrames carregados
    """
    # Separa ativos em: padrão (data_loader) vs exclusivos (query própria)
    datasets_standard   = [d for d, cfg in DATASETS_TO_LOAD.items() if cfg['active'] and cfg['query'] is None]
    datasets_exclusivos = {d: cfg['query'] for d, cfg in DATASETS_TO_LOAD.items() if cfg['active'] and cfg['query'] is not None}
 
    # Resolve colunas (três camadas): carteira → default → None (SELECT *)
    resolved_columns = {
        dataset: COLUMNS.get(dataset) or DEFAULT_COLUMNS.get(dataset)
        for dataset in datasets_standard
    }
 
    with get_db_connections() as (conn_trc, conn_bd2, conn_src):
 
        # Carregamento padrão via data_loader
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
            datasets_to_load=datasets_standard,
            columns=resolved_columns,
        )
 
        # Carregamento dos datasets exclusivos da carteira
        for dataset, get_query in datasets_exclusivos.items():
            col = COLUMNS.get(dataset) or DEFAULT_COLUMNS.get(dataset)
 
            print(f"🔍 Carregando {dataset} (exclusivo)...")
            query_sql = get_query(data_inicio, data_fim, col)
            dados[dataset] = pd.read_sql(query_sql, conn_src)
            print(f"✅ {dataset} carregado — {len(dados[dataset]):,} linhas")
 
    return dados