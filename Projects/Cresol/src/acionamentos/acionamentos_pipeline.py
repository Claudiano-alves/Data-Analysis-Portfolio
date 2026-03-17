from utils.acionamentos.data_processing import processar_acionamentos
from utils.acionamentos.metricas_acumuladas import processar_acumulados_acionamentos
from Cresol.src.config import LOG_ACIONAMENTOS, segmentacoes_extras


def executar(df_tab_acionamentos, df_tabulacao_aciona, df_mailing_analitico, df_dw_calendario):
    """
    Retorna (df_acionamentos_analitico, df_acionamentos_acumulados, df_sem_relacionamento, df_sem_descricao)
    """
    df_analitico, df_sem_relacionamento, df_sem_descricao = processar_acionamentos(
        df_tab_acionamentos=df_tab_acionamentos,
        df_tabulacao_aciona=df_tabulacao_aciona,
        df_mailing_hist=df_mailing_analitico,
        df_dw_calendario=df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras,
        arquivo_log=LOG_ACIONAMENTOS,
    )

    df_acumulados = processar_acumulados_acionamentos(
        df_acionamentos=df_analitico,
        segmentacoes=segmentacoes_extras,
        retorno='consolidado',
        arquivo_log=LOG_ACIONAMENTOS,
    )

    return df_analitico, df_acumulados, df_sem_relacionamento, df_sem_descricao