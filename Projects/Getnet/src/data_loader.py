import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import Tuple, Optional, List, Dict
from utils.data_loader import data_loader
from utils.db_connection import get_db_connections
from Getnet.src.dataset_config import GETNET_DATASETS

def load_data_getnet( 
    data_inicio: str, 
    data_fim: str
) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados específicos para a carteira GetNet.
    
    Não carrega: SMS, RCS, Email
    """
    # 🔌 Conexões gerenciadas pelo context manager
    with get_db_connections() as (conn_trc, conn_bd2, conn_src):

        # WHEREs específicos da GetNet
        where_campanhas = "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA IN (19, 30))"
        where_clientes = "WHERE COD_CLI IN(196,198,228)"
        where_acionamentos = "WHERE ((C.COD_CLI = 198 AND C.COD_CAR IN (1, 2, 3)) OR (C.COD_CLI = 196 AND C.COD_CAR IN (1, 3, 4)) OR (C.COD_CLI = 228 AND C.COD_CAR = 2))"

        # Datasets necessários para GetNet
        datasets_getnet = [
            nome for nome, ativo in GETNET_DATASETS.items() if ativo
        ]
        
        return data_loader(
            conn_trc=conn_trc,
            conn_bd2=conn_bd2,
            conn_src=conn_src,
            data_inicio=data_inicio,
            data_fim=data_fim,
            where_campanhas=where_campanhas,
            where_clientes_mailing=where_clientes,
            where_acionamentos=where_acionamentos,
            where_tabulacao=where_clientes,
            where_clientes_pagamentos=where_clientes,
            where_clientes_acordos=where_clientes,
            #where_telefones=f"CPF_DEV IN (SELECT CPF_DEV FROM CAD_DEVF WHERE {where_clientes})",
            datasets_to_load=datasets_getnet
        )