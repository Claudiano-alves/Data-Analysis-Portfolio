"""
Módulo de Métricas Diárias de Acionamentos
Contém funções para gerar métricas diárias (dia a dia) de acionamentos.
"""

import pandas as pd
from functools import reduce
from utils.utils import registrar_tempo, salvar_log
from ..config import LOG_ACIONAMENTOS


def acionamentos_unique_origem_fxAtraso(df_acionamentos_enriquecido, df_dw_calendario):
    """
    Gera contagem DIÁRIA de CPFs únicos por ORIGEM e FX_ATRASO (melhor score).
    
    Args:
        df_acionamentos_enriquecido (pd.DataFrame): DataFrame enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: Métricas diárias de CPFs únicos por origem e faixa de atraso
    """
    df = df_acionamentos_enriquecido.copy()
    df['TABULACAO_SCORE'] = (
        df['PROMESSA'].astype(int) * 3 +
        df['CPCA'].astype(int) * 2 +
        df['CPC'].astype(int) * 1
    )

    df_ordenado = df.sort_values(
        ['CPF_DEV', 'DATA_ACIONA', 'TABULACAO_SCORE'],
        ascending=[True, True, False]
    )

    df_unique_filtrado = df_ordenado.drop_duplicates(
        subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM'],
        keep='first'
    ).copy()

    df_unique_filtrado.loc[:, 'ACIONAMENTOS'] = df_unique_filtrado['ACIONAMENTOS'].astype(int)
    df_unique_filtrado.loc[:, 'CPC'] = df_unique_filtrado['CPC'].astype(int)
    df_unique_filtrado.loc[:, 'CPCA'] = df_unique_filtrado['CPCA'].astype(int)
    df_unique_filtrado.loc[:, 'PROMESSA'] = df_unique_filtrado['PROMESSA'].astype(int)

    df_aciona = df_unique_filtrado[df_unique_filtrado['ACIONAMENTOS'] >= 1].drop_duplicates(subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM']).copy()
    df_cpc = df_unique_filtrado[df_unique_filtrado['CPC'] >= 1].drop_duplicates(subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM']).copy()
    df_cpca = df_unique_filtrado[df_unique_filtrado['CPCA'] >= 1].drop_duplicates(subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM']).copy()
    df_promessa = df_unique_filtrado[df_unique_filtrado['PROMESSA'] >= 1].drop_duplicates(subset=['CPF_DEV', 'DATA_ACIONA', 'ORIGEM']).copy()

    def contar_cpfs(df_filtrado, nome_coluna):
        return (
            df_filtrado.groupby(
                ['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
                observed=True
            )
            .agg({ 
                'CPF_DEV': 'nunique',
                'VALOR': 'sum'
            })
            .rename(columns={
                'CPF_DEV': nome_coluna,
                'VALOR': f'VALORPRIN_FIN_{nome_coluna}'
            })
            .reset_index()
        )

    df_contagem_aciona = contar_cpfs(df_aciona, 'ACIONAMENTOS')
    df_contagem_cpc = contar_cpfs(df_cpc, 'CPC')
    df_contagem_cpca = contar_cpfs(df_cpca, 'CPCA')
    df_contagem_promessa = contar_cpfs(df_promessa, 'PROMESSA')

    dfs = [df_contagem_aciona, df_contagem_cpc, df_contagem_cpca, df_contagem_promessa]

    df_final = reduce(lambda left, right: pd.merge(
        left, right,
        on=['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
        how='outer'
    ), dfs)

    colunas_metricas = ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']
    colunas_valor = ['VALORPRIN_FIN_ACIONAMENTOS', 'VALORPRIN_FIN_CPC', 'VALORPRIN_FIN_CPCA', 'VALORPRIN_FIN_PROMESSA']
    
    df_final[colunas_metricas] = df_final[colunas_metricas].fillna(0).astype(int)
    df_final[colunas_valor] = df_final[colunas_valor].fillna(0)

    df_final = df_final.merge(
        df_dw_calendario[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']].drop_duplicates(),
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='left'
    ).drop('dt_data', axis=1)

    return df_final


def acionamentos_unique_fxAtraso(df_acionamentos_com_calendario, df_dw_calendario):
    """
    Gera contagem DIÁRIA de CPFs únicos por FX_ATRASO (melhor score global).
    
    Args:
        df_acionamentos_com_calendario (pd.DataFrame): DataFrame enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: Métricas diárias de CPFs únicos por faixa de atraso
    """
    df = df_acionamentos_com_calendario.copy()
    
    df['TABULACAO_SCORE'] = (
        df['PROMESSA'].astype(int) * 3 +
        df['CPCA'].astype(int) * 2 +
        df['CPC'].astype(int) * 1
    )
    
    df_ordenado = df.sort_values(
        ['CPF_DEV', 'DATA_ACIONA', 'TABULACAO_SCORE'],
        ascending=[True, True, False]
    )
    
    df_unique_filtrado = df_ordenado.drop_duplicates(
        subset=['CPF_DEV', 'DATA_ACIONA'],
        keep='first'
    ).copy()
    
    df_unique_filtrado.loc[:, 'ACIONAMENTOS'] = df_unique_filtrado['ACIONAMENTOS'].astype(int)
    df_unique_filtrado.loc[:, 'CPC'] = df_unique_filtrado['CPC'].astype(int)
    df_unique_filtrado.loc[:, 'CPCA'] = df_unique_filtrado['CPCA'].astype(int)
    df_unique_filtrado.loc[:, 'PROMESSA'] = df_unique_filtrado['PROMESSA'].astype(int)
    
    df_aciona = df_unique_filtrado[df_unique_filtrado['ACIONAMENTOS'] >= 1].copy()
    df_cpc = df_unique_filtrado[df_unique_filtrado['CPC'] >= 1].copy()
    df_cpca = df_unique_filtrado[df_unique_filtrado['CPCA'] >= 1].copy()
    df_promessa = df_unique_filtrado[df_unique_filtrado['PROMESSA'] >= 1].copy()
    
    def contar_cpfs(df_filtrado, nome_coluna):
        df_contagem = (
            df_filtrado.groupby(
                ['DATA_ACIONA', 'FX_ATRASO'],
                observed=True
            )
            .agg({ 
                'CPF_DEV': 'nunique',
                'VALOR': 'sum'
            })
            .rename(columns={
                'CPF_DEV': nome_coluna,
                'VALOR': f'VALORPRIN_FIN_{nome_coluna}'
            })
            .reset_index()
        )
        df_contagem.insert(2, 'ORIGEM', 'UNIQUE')
        return df_contagem
    
    df_contagem_aciona = contar_cpfs(df_aciona, 'ACIONAMENTOS')
    df_contagem_cpc = contar_cpfs(df_cpc, 'CPC')
    df_contagem_cpca = contar_cpfs(df_cpca, 'CPCA')
    df_contagem_promessa = contar_cpfs(df_promessa, 'PROMESSA')
    
    dfs = [df_contagem_aciona, df_contagem_cpc, df_contagem_cpca, df_contagem_promessa]
    
    df_final_unique = reduce(lambda left, right: pd.merge(
        left, right,
        on=['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
        how='outer'
    ), dfs)
    
    colunas_metricas = ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']
    colunas_valor = ['VALORPRIN_FIN_ACIONAMENTOS', 'VALORPRIN_FIN_CPC', 'VALORPRIN_FIN_CPCA', 'VALORPRIN_FIN_PROMESSA']
    
    df_final_unique[colunas_metricas] = df_final_unique[colunas_metricas].fillna(0).astype(int)
    df_final_unique[colunas_valor] = df_final_unique[colunas_valor].fillna(0)
    
    df_final_unique = df_final_unique.merge(
        df_dw_calendario[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']].drop_duplicates(),
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='left'
    ).drop('dt_data', axis=1)
    
    return df_final_unique


def acionamentos_esforco_origem_fxAtraso(df_acionamentos_enriquecido, df_dw_calendario):
    """
    Gera contagem DIÁRIA de esforço (total) de acionamentos por ORIGEM e FX_ATRASO.
    
    Args:
        df_acionamentos_enriquecido (pd.DataFrame): DataFrame enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: Métricas diárias de esforço por origem e faixa de atraso
    """
    df_esforco = df_acionamentos_enriquecido.copy()

    df_esforco['TABULACAO_SCORE'] = (
        df_esforco['PROMESSA'].astype(int) * 3 +
        df_esforco['CPCA'].astype(int) * 2 +
        df_esforco['CPC'].astype(int) * 1
    )
    
    df_ordenado = df_esforco.sort_values(
        ['CPF_DEV', 'DATA_ACIONA', 'TABULACAO_SCORE'],
        ascending=[True, True, False]
    )
    
    df_ordenado = df_ordenado.drop_duplicates(
        subset=['CPF_DEV', 'DATA_ACIONA', 'TABULACAO_SCORE'],
        keep='first'
    ).copy()
    
    df_esforco.loc[:, 'ACIONAMENTOS'] = df_esforco['ACIONAMENTOS'].astype(int)
    df_esforco.loc[:, 'CPC'] = df_esforco['CPC'].astype(int)
    df_esforco.loc[:, 'CPCA'] = df_esforco['CPCA'].astype(int)
    df_esforco.loc[:, 'PROMESSA'] = df_esforco['PROMESSA'].astype(int)

    df_aciona = df_esforco[df_esforco['ACIONAMENTOS'] >= 1].copy()
    df_cpc = df_esforco[df_esforco['CPC'] >= 1].copy()
    df_cpca = df_esforco[df_esforco['CPCA'] >= 1].copy()
    df_promessa = df_esforco[df_esforco['PROMESSA'] >= 1].copy()

    def contar_esforco(df_filtrado, nome_coluna):
        return (
            df_filtrado.groupby(
                ['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
                observed=True
            )
            .agg({ 
                nome_coluna: 'sum',
                'VALOR': 'sum'
            })
            .rename(columns={
                'VALOR': f'VALORPRIN_FIN_{nome_coluna}'
            })
            .reset_index()
        )

    df_contagem_aciona = contar_esforco(df_aciona, 'ACIONAMENTOS')
    df_contagem_cpc = contar_esforco(df_cpc, 'CPC')
    df_contagem_cpca = contar_esforco(df_cpca, 'CPCA')
    df_contagem_promessa = contar_esforco(df_promessa, 'PROMESSA')

    dfs = [df_contagem_aciona, df_contagem_cpc, df_contagem_cpca, df_contagem_promessa]

    df_final = reduce(lambda left, right: pd.merge(
        left, right,
        on=['DATA_ACIONA', 'FX_ATRASO', 'ORIGEM'],
        how='outer'
    ), dfs)

    colunas_metricas = ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']
    colunas_valor = ['VALORPRIN_FIN_ACIONAMENTOS', 'VALORPRIN_FIN_CPC', 'VALORPRIN_FIN_CPCA', 'VALORPRIN_FIN_PROMESSA']
    
    df_final[colunas_metricas] = df_final[colunas_metricas].fillna(0).astype(int)
    df_final[colunas_valor] = df_final[colunas_valor].fillna(0)

    df_final = df_final.merge(
        df_dw_calendario[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']].drop_duplicates(),
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='left'
    ).drop('dt_data', axis=1)
    
    df_final['FX_ATRASO'] = 'Esforço'

    return df_final
