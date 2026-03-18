# Cresol/src/database.py

from utils.utils import salvar_log
from utils._database.insert import inserir_dataframe
from utils._database.check import ja_inserido_hoje
from Cresol.src.config import LOGS, LOG_PIPELINE, TABELA_SINTETICO, COLUNAS_SINTETICO, TIPOS_SINTETICO

def inserir_acumulado(df, conn, arquivo_log=LOG_PIPELINE):
    if ja_inserido_hoje(TABELA_SINTETICO, conn):
        msg = f"[{TABELA_SINTETICO}] ⚠️  Já existem dados de hoje — insert abortado"
        print(msg)
        salvar_log(msg, arquivo_log)
        return False

    return inserir_dataframe(
        df=df,
        tabela=TABELA_SINTETICO,
        conn=conn,
        colunas=COLUNAS_SINTETICO,
        tipos=TIPOS_SINTETICO,
        arquivo_log=arquivo_log,
    )