# Cresol/src/discagens/pipeline.py

from utils.discagens.expert.data_processing import aplicar_transformacoes_discagens
from utils.discagens.expert.metricas_acumuladas import processar_acumulados_discagens_completo
from utils.discagens.expert.Ringing.metricas_acumuladas import processar_acumulados_ringing
from Cresol.src.config import TRANSFORMACOES_DISCAGENS, LOG_DISCAGENS, segmentacoes_extras


def executar(df_discagens_expert, df_mailing_analitico, df_dw_calendario):
    """
    Retorna dict com:
        - analitico           : DataFrame analítico de discagens
        - acumulado           : DataFrame acumulado de discagens
        - ringing             : DataFrame acumulado de ringing
        - sem_relacionamento  : DataFrame de discagens sem relacionamento com mailing
    """
    df_analitico, df_sem_relacionamento = aplicar_transformacoes_discagens(
        df=df_discagens_expert,
        config=TRANSFORMACOES_DISCAGENS,
        df_mailing=df_mailing_analitico,
        df_calendario=df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras,
        arquivo_log=LOG_DISCAGENS,
    )

    df_acumulado = processar_acumulados_discagens_completo(
        df_discagens=df_analitico,
        segmentacoes_extras=segmentacoes_extras,
        retorno='consolidado',
        arquivo_log=LOG_DISCAGENS,
    )

    df_ringing = processar_acumulados_ringing(
        df_discagens=df_analitico,
        segmentacoes=segmentacoes_extras,
        consolidado=True,
        arquivo_log=LOG_DISCAGENS,
    )

    return {
        'analitico':          df_analitico,
        'acumulado':          df_acumulado,
        'ringing':            df_ringing,
        'sem_relacionamento': df_sem_relacionamento,
    }