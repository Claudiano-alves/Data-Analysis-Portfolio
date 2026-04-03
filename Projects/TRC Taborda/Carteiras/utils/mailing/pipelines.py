"""
Módulo de Pipeline de Mailing
Orquestra as funções de tratamento e geração de métricas de mailing.
"""

from utils.utils import unir_dataframes
from .metricas_acumuladas import processar_acumulados_mailing

def processar_mailing_completo(
    df_mailing_hist,
    df_dw_calendario,
    segmentacoes,
    tratamentos_extras=None,
    acumulados_extras=None,
    arquivo_log=None,
):
    """
    Pipeline completo de tratamento e geração de métricas de mailing.
 
    Não há tratamento padrão fixo — todas as transformações são responsabilidade
    da carteira via tratamentos_extras, incluindo adicionar_faixa_atraso quando necessário.
 
    Exemplos:
        Cresol:
            segmentacoes=['PF_PJ', 'PA']
            tratamentos_extras=[add_pf_pj, add_pa]
 
        Renner/Ouze (com FX_ATRASO):
            segmentacoes=['FX_ATRASO', 'FAIXA']
            tratamentos_extras=[partial(adicionar_faixa_atraso, bins=BINS, labels=LABELS), add_faixa]
 
    Args:
        df_mailing_hist (pd.DataFrame): DataFrame bruto de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes (list): Colunas de segmentação dinâmicas da carteira.
                             Ex: ['PF_PJ', 'PA'] ou ['FX_ATRASO', 'FAIXA']
        tratamentos_extras (list, optional): Funções de tratamento da carteira.
                                             Cada função recebe um DataFrame e retorna um DataFrame.
        acumulados_extras (list, optional): Funções de acumulado da carteira.
                                            Cada função recebe (df_mailing_tratado, df_dw_calendario)
                                            e retorna um DataFrame unido ao resultado final.
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MAILING
 
    Returns:
        tuple: (df_mailing_tratado, df_mailing_final)
    """
    # ============================================
    # ETAPA 1: TRATAMENTOS (por carteira)
    # ============================================
    df_mailing_tratado = df_mailing_hist.copy()
 
    if tratamentos_extras:
        for tratamento in tratamentos_extras:
            df_mailing_tratado = tratamento(df_mailing_tratado)
 
    # ============================================
    # ETAPA 2: MÉTRICAS ACUMULADAS
    # ============================================
    df_acumulado_mailing = processar_acumulados_mailing(
        df_mailing_tratado,
        df_dw_calendario,
        segmentacoes=segmentacoes,
        arquivo_log=arquivo_log,
    )
 
    return df_mailing_tratado, df_acumulado_mailing