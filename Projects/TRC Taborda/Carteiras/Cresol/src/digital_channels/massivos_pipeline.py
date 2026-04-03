# Cresol/src/digital_channels/pipeline.py

from utils.digital_channels.data_processing import processar_massivos
from utils.digital_channels.metricas_acumuladas import processar_acumulados_massivos
from Cresol.src.config import LOG_CHANNELS, segmentacoes_extras


def executar_analitico(df_sms, df_rcs, df_email, df_whats, df_mailing_analitico, df_dw_calendario):
    """
    Processa e retorna o analítico de massivos.
    Usado na etapa de persistência — sem cálculo de acumulado.

    Retorna dict com:
        - analitico          : DataFrame analítico de massivos
        - sem_relacionamento : DataFrame de massivos sem relacionamento com mailing
    """
    df_analitico, df_sem_relacionamento = processar_massivos(
        df_sms=df_sms,
        df_rcs=df_rcs,
        df_email=df_email,
        df_whats=df_whats,
        df_mailing_hist=df_mailing_analitico,
        df_dw_calendario=df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras,
        arquivo_log=LOG_CHANNELS,
    )

    return {
        'analitico':          df_analitico,
        'sem_relacionamento': df_sem_relacionamento,
    }


def executar_acumulado(df_massivos_analitico):
    """
    Calcula os acumulados a partir do analítico já processado.
    Usado na etapa de acumulados — recebe df do banco ou da memória.

    Retorna dict com:
        - acumulado : DataFrame acumulado de massivos
    """
    df_acumulado = processar_acumulados_massivos(
        df_massivos=df_massivos_analitico,
        segmentacoes=segmentacoes_extras,
        arquivo_log=LOG_CHANNELS,
    )

    return {
        'acumulado': df_acumulado,
    }