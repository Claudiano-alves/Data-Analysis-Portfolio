# Cresol/src/acionamentos/pipeline.py

from utils.acionamentos.data_processing import processar_acionamentos
from utils.acionamentos.metricas_acumuladas import processar_acumulados_acionamentos
from Cresol.src.config import LOG_ACIONAMENTOS, segmentacoes_extras

def executar_analitico(df_tab_acionamentos, df_tabulacao_aciona, df_mailing_analitico, df_dw_calendario):
    """
    Processa e retorna o analítico de acionamentos.
    Usado na etapa de persistência — sem cálculo de acumulado.

    Retorna dict com:
        - analitico          : DataFrame analítico de acionamentos
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

    return {
        'analitico':          df_analitico,
        'sem_relacionamento': df_sem_relacionamento,
        'sem_descricao':      df_sem_descricao,
    }


def executar_acumulado(df_acionamentos_analitico):
    """
    Calcula os acumulados a partir do analítico já processado.
    Usado na etapa de acumulados — recebe df do banco ou da memória.

    Retorna dict com:
        - acumulado : DataFrame acumulado de acionamentos
    """
    df_acumulado = processar_acumulados_acionamentos(
        df_acionamentos=df_acionamentos_analitico,
        segmentacoes=segmentacoes_extras,
        retorno='consolidado',
        arquivo_log=LOG_ACIONAMENTOS,
    )

    return {
        'acumulado': df_acumulado,
    }