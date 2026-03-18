from utils.mailing.pipelines import processar_mailing_completo
from Cresol.src.mailing.data_processing import add_pf_pj, add_pa
from Cresol.src.config import LOG_MAILING, segmentacoes_extras

def executar(df_mailing_hist, df_dw_calendario):
    """
    Retorna dict com:
        - analitico : DataFrame analítico de mailing
        - acumulado : DataFrame acumulado de mailing
    """
    df_analitico, df_acumulado = processar_mailing_completo(
        df_mailing_hist,
        df_dw_calendario,
        segmentacoes=segmentacoes_extras,
        tratamentos_extras=[add_pf_pj, add_pa],
        arquivo_log=LOG_MAILING,
    )

    return {
        'analitico': df_analitico,
        'acumulado': df_acumulado,
    }