# import sys 
# sys.path.append("..")

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import Tuple, Optional, List, Dict
from utils.data_loader import data_loader
import pandas as pd
from utils.db_connection import get_db_connections
from Ouze.src.dataset_config import OUZE_DATASETS

def load_data_ouze(
    data_inicio: str,
    data_fim: str
) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados específicos para a carteira Ouze.

    Não carrega: Discagens Trestto
    """

    # 🔌 Conexões gerenciadas pelo context manager
    with get_db_connections() as (conn_trc, conn_bd2, conn_src):

        # WHEREs específicos da Ouze
        where_campanhas = "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA = 141)"
        # MAILING_HIST
        where_clientes_mailing = "COD_CLI = 253"
        # ACIONAMENTOS
        where_acionamentos = """
              WHERE C.COD_CLI = 253
              AND C.COD_CAR = 1
              AND A.COD_RECUP NOT IN (15721)
              AND B.CLASSIFICACAO_ACIONAMENTO = 1
        """
        where_tabulacao_aciona = "WHERE COD_CLI = 253"
        # PAGAMENTOS
        where_clientes_pagamentos = "WHERE B.COD_CLI = 253"
        # ACORDOS
        where_clientes_acordos = "WHERE B.COD_CLI = 253 AND B.COD_CAR = 1"
        # MASSIVOS SMS, RCS E EMAIL
        where_massivos = "WHERE ID_CAR = 100"
        # TELEFONES
        where_telefones = "WHERE COD_CLI = 253"
        # Datasets necessários para Ouze
        datasets_ouze = [
            nome for nome, ativo in OUZE_DATASETS.items() if ativo
        ]

        # 🔄 Carregamento efetivo
        return data_loader(
            conn_trc=conn_trc,
            conn_bd2=conn_bd2,
            conn_src=conn_src,
            data_inicio=data_inicio,
            data_fim=data_fim,
            where_campanhas=where_campanhas,
            where_clientes_mailing=where_clientes_mailing,
            where_acionamentos=where_acionamentos,
            where_tabulacao=where_tabulacao_aciona,
            where_clientes_pagamentos=where_clientes_pagamentos,
            where_clientes_acordos=where_clientes_acordos,
            where_massivos=where_massivos,
            where_telefones=where_telefones,
            datasets_to_load=datasets_ouze
        )