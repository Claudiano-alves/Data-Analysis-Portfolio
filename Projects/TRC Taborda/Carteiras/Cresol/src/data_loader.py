import pandas as pd
from typing import Dict, List, Optional
from utils.data_loader import data_loader
from utils.db_connection import get_db_connections
from Cresol.src.config import WHERE_CLAUSES, DATASETS_TO_LOAD, COLUMNS
from utils.config import DEFAULT_COLUMNS

# Cresol/src/data_loader.py — adiciona conn opcional
def load_data_cresol(
    data_inicio: str,
    data_fim: str,
    datasets_to_load: Optional[List[str]] = None,
    conn_trc=None,
    conn_bd2=None,
    conn_src=None,
) -> Dict[str, pd.DataFrame]:

    print(f"DEBUG load_data_cresol: conn_trc={conn_trc is not None}, conn_bd2={conn_bd2 is not None}, conn_src={conn_src is not None}")

    datasets_ativos = {
        d: cfg for d, cfg in DATASETS_TO_LOAD.items()
        if cfg['active'] and (datasets_to_load is None or d in datasets_to_load)
    }

    datasets_standard   = [d for d, cfg in datasets_ativos.items() if cfg['query'] is None]
    datasets_exclusivos = {d: cfg['query'] for d, cfg in datasets_ativos.items() if cfg['query'] is not None}

    resolved_columns = {
        dataset: COLUMNS.get(dataset) or DEFAULT_COLUMNS.get(dataset)
        for dataset in datasets_standard
    }

    print(f"DEBUG: usando conexões {'externas' if conn_trc and conn_bd2 and conn_src else 'novas'}")

    if conn_trc and conn_bd2 and conn_src:
        return _carregar(conn_trc, conn_bd2, conn_src, data_inicio, data_fim,
                         datasets_standard, datasets_exclusivos, resolved_columns)

    with get_db_connections() as (conn_trc, conn_bd2, conn_src):
        return _carregar(conn_trc, conn_bd2, conn_src, data_inicio, data_fim,
                         datasets_standard, datasets_exclusivos, resolved_columns)


def _carregar(conn_trc, conn_bd2, conn_src, data_inicio, data_fim,
              datasets_standard, datasets_exclusivos, resolved_columns):
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

    for dataset, get_query in datasets_exclusivos.items():
        col = COLUMNS.get(dataset) or DEFAULT_COLUMNS.get(dataset)
        print(f"🔍 Carregando {dataset} (exclusivo)...")
        query_sql = get_query(data_inicio, data_fim, col)
        dados[dataset] = pd.read_sql(query_sql, conn_src)
        print(f"✅ {dataset} carregado — {len(dados[dataset]):,} linhas")

    return dados
