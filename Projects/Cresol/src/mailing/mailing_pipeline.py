# Cresol/src/mailing/pipeline.py

from utils.mailing.pipelines import processar_mailing_completo
from utils.mailing.metricas_acumuladas import processar_acumulados_mailing
from Cresol.src.mailing.data_processing import add_pf_pj, add_pa
from Cresol.src.config import LOG_MAILING, segmentacoes_extras


def executar_analitico(df_mailing_hist, df_dw_calendario):
    """
    Processa e retorna o analítico de mailing.
    Usado na etapa de persistência — sem cálculo de acumulado.

    Retorna dict com:
        - analitico : DataFrame analítico de mailing
    """
    df_mailing_tratado = df_mailing_hist.copy()

    for tratamento in [add_pf_pj, add_pa]:
        df_mailing_tratado = tratamento(df_mailing_tratado)

    return {
        'analitico': df_mailing_tratado,
    }


def executar_acumulado(df_mailing_analitico, df_dw_calendario):
    """
    Calcula os acumulados a partir do analítico já processado.
    Usado na etapa de acumulados — recebe df do banco ou da memória.

    Retorna dict com:
        - acumulado : DataFrame acumulado de mailing
    """
    df_acumulado = processar_acumulados_mailing(
        df_mailing_analitico,
        df_dw_calendario,
        segmentacoes=segmentacoes_extras,
        arquivo_log=LOG_MAILING,
    )

    return {
        'acumulado': df_acumulado,
    }