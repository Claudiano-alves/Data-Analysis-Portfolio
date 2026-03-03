"""
Módulo de Pipeline de Mailing
Orquestra as funções de tratamento e geração de métricas de mailing.
"""

from utils.utils import unir_dataframes
from .data_processing import adicionar_faixa_atraso
from .metricas_acumuladas import (
    gerar_acumulado_maling_hist_fxAtraso,
    gerar_acumulado_maling_hist_unique
)

def processar_mailing_completo(df_mailing_hist, df_dw_calendario, bins, labels, tratamentos_extras=None, segmentacoes_extras=None, acumulados_extras=None):
    """
    Pipeline completo de tratamento e geração de métricas de mailing.
    
    Args:
        df_mailing_hist (pd.DataFrame): DataFrame bruto de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        bins (list): Limites das faixas de atraso
        labels (list): Rótulos das faixas de atraso
        tratamentos_extras (list, optional): Lista de funções de tratamento específicas da carteira.
                                             Cada função recebe um DataFrame e retorna um DataFrame.
        segmentacoes_extras (list, optional): Lista de colunas adicionais para segmentação.
                                              Ex: ['FAIXA'] para a carteira Renner
        acumulados_extras (list, optional): Lista de funções de acumulado específicas da carteira.
                                            Cada função recebe (df_mailing_tratado, df_dw_calendario)
                                            e retorna um DataFrame que será unido ao resultado final.
                                            Ex: [gerar_acumulado_unique_por_faixa]
    
    Returns:
        tuple: (df_mailing_tratado, df_mailing_final)
    """
    # ============================================
    # ETAPA 1: TRATAMENTO PADRÃO
    # ============================================
    df_mailing_tratado = adicionar_faixa_atraso(df_mailing_hist, bins=bins, labels=labels)

    # ============================================
    # ETAPA 2: TRATAMENTOS EXTRAS (por carteira)
    # ============================================
    if tratamentos_extras:
        for tratamento in tratamentos_extras:
            df_mailing_tratado = tratamento(df_mailing_tratado)

    # ============================================
    # ETAPA 3: MÉTRICAS ACUMULADAS
    # ============================================
    df_mailing_fxAtraso = gerar_acumulado_maling_hist_fxAtraso(
        df_mailing_tratado,
        df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras
    )
    df_mailing_unique = gerar_acumulado_maling_hist_unique(
        df_mailing_tratado,
        df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras
    )

    # ============================================
    # ETAPA 4: ACUMULADOS EXTRAS (por carteira)
    # ============================================
    dfs_acumulados = [df_mailing_fxAtraso, df_mailing_unique]

    if acumulados_extras:
        for gerar_acumulado in acumulados_extras:
            df_extra = gerar_acumulado(df_mailing_tratado, df_dw_calendario)
            dfs_acumulados.append(df_extra)

    # ============================================
    # ETAPA 5: CONSOLIDAÇÃO
    # ============================================
    df_mailing_final = unir_dataframes(*dfs_acumulados)

    return df_mailing_tratado, df_mailing_final


def gerar_acumulado_mailing_hist(df_mailing_hist, df_dw_calendario, bins, labels, tratamentos_extras=None, segmentacoes_extras=None, acumulados_extras=None):
    """
    Função wrapper - mantém compatibilidade com código legado.
    """
    return processar_mailing_completo(df_mailing_hist, df_dw_calendario, bins, labels, tratamentos_extras, segmentacoes_extras, acumulados_extras)

def processar_mailing_completo_(df_mailing_hist, df_dw_calendario, bins, labels, tratamentos_extras=None, segmentacoes_extras=None):
    """
    Pipeline completo de tratamento e geração de métricas de mailing.
    
    Args:
        df_mailing_hist (pd.DataFrame): DataFrame bruto de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        bins (list): Limites das faixas de atraso
        labels (list): Rótulos das faixas de atraso
        tratamentos_extras (list, optional): Lista de funções de tratamento específicas da carteira
        segmentacoes_extras (list, optional): Lista de colunas adicionais para segmentação
                                              Ex: ['FAIXA'] para a carteira Renner
    
    Returns:
        tuple: (df_mailing_tratado, df_mailing_final)
    """
    # ============================================
    # ETAPA 1: TRATAMENTO PADRÃO
    # ============================================
    df_mailing_tratado = adicionar_faixa_atraso(df_mailing_hist, bins=bins, labels=labels)

    # ============================================
    # ETAPA 2: TRATAMENTOS EXTRAS (por carteira)
    # ============================================
    if tratamentos_extras:
        for tratamento in tratamentos_extras:
            df_mailing_tratado = tratamento(df_mailing_tratado)

    # ============================================
    # ETAPA 3: MÉTRICAS ACUMULADAS
    # ============================================
    df_mailing_fxAtraso = gerar_acumulado_maling_hist_fxAtraso(
        df_mailing_tratado,
        df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras
    )
    df_mailing_unique = gerar_acumulado_maling_hist_unique(
        df_mailing_tratado,
        df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras
    )

    # ============================================
    # ETAPA 4: CONSOLIDAÇÃO
    # ============================================
    df_mailing_final = unir_dataframes(df_mailing_fxAtraso, df_mailing_unique)

    return df_mailing_tratado, df_mailing_final


def gerar_acumulado_mailing_hist_(df_mailing_hist, df_dw_calendario, bins, labels, tratamentos_extras=None, segmentacoes_extras=None):
    """
    Função wrapper - mantém compatibilidade com código legado.
    """
    return processar_mailing_completo(df_mailing_hist, df_dw_calendario, bins, labels, tratamentos_extras, segmentacoes_extras)
