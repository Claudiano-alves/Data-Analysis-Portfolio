# Cresol/src/acionamentos/pipeline.py

from utils.acionamentos.data_processing import processar_acionamentos
from utils.acionamentos.metricas_acumuladas import processar_acumulados_acionamentos
from Cresol.src.config import LOG_ACIONAMENTOS, segmentacoes_extras

def executar(df_tab_acionamentos, df_tabulacao_aciona, df_mailing_analitico, df_dw_calendario):
    """
    Retorna dict com:
        - analitico          : DataFrame analítico de acionamentos
        - acumulado          : DataFrame acumulado de acionamentos
        - sem_relacionamento : DataFrame de acionamentos sem relacionamento com mailing
        - sem_descricao      : DataFrame de acionamentos sem descrição
    """
    df_analitico, df_sem_relacionamento, df_sem_descricao = processar_acionamentos(
        df_tab_acionamentos=df_tab_acionamentos,
        df_tabulacao_aciona=df_tabulacao_aciona,
        df_mailing_hist=df_mailing_analitico,
        df_dw_calendario=df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras,
        arquivo_log=LOG_ACIONAMENTOS,
    )

    df_acumulado = processar_acumulados_acionamentos(
        df_acionamentos=df_analitico,
        segmentacoes=segmentacoes_extras,
        retorno='consolidado',
        arquivo_log=LOG_ACIONAMENTOS,
    )

    return {
        'analitico':          df_analitico,
        'acumulado':          df_acumulado,
        'sem_relacionamento': df_sem_relacionamento,
        'sem_descricao':      df_sem_descricao,
    }