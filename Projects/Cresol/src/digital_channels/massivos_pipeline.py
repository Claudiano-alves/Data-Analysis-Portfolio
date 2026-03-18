from utils.digital_channels.data_processing import processar_massivos
from utils.digital_channels.metricas_acumuladas import processar_acumulados_massivos
from Cresol.src.config import LOG_CHANNELS, segmentacoes_extras

def executar(df_sms, df_rcs, df_email, df_whats, df_mailing_analitico, df_dw_calendario):
    """
    Retorna dict com:
        - analitico          : DataFrame analítico de massivos
        - acumulado          : DataFrame acumulado de massivos
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

    df_acumulado = processar_acumulados_massivos(
        df_massivos=df_analitico,
        segmentacoes=segmentacoes_extras,
        arquivo_log=LOG_CHANNELS,
    )

    return {
        'analitico':          df_analitico,
        'acumulado':          df_acumulado,
        'sem_relacionamento': df_sem_relacionamento,
    }