"""
Módulo de Métricas Acumuladas de Mailing
Contém funções para gerar métricas acumuladas (mensais) de mailing.
"""

import pandas as pd
from utils.utils import registrar_tempo, unir_dataframes, salvar_log
from ..config import LOG_MAILING

@registrar_tempo("Acumulado mailing hist unique", arquivo_log=LOG_MAILING)
def gerar_acumulado_mailing_unique_por_faixa(df_maling_hist, df_dw_calendario, segmentacoes_extras=None):
    """
    Gera DataFrame com acumulado ÚNICO de contratos e CPFs do mailing por dia útil.
    Sem agrupamento por faixa de atraso.
    
    Args:
        df_maling_hist (pd.DataFrame): DataFrame de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Lista de colunas adicionais para segmentação
                                              Ex: ['FAIXA'] para a carteira Renner
    
    Returns:
        pd.DataFrame: DataFrame com acumulado único
    """
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log("INÍCIO DO PROCESSAMENTO DE MALING_HIST", arquivo_log=LOG_MAILING)
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log(f"Total de registros em df_maling_hist: {len(df_maling_hist)}", arquivo_log=LOG_MAILING)

    segmentacoes_extras = segmentacoes_extras or []

    colunas_reduzidas = ['DATA', 'CONTRATO', 'CPF', 'VALOR'] + segmentacoes_extras
    df_reduzido = df_maling_hist[colunas_reduzidas].copy()
    salvar_log(f"Memória reduzida - trabalhando com {len(df_reduzido.columns)} colunas", arquivo_log=LOG_MAILING)

    df_reduzido['DATA'] = pd.to_datetime(df_reduzido['DATA'])

    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]

    df_reduzido = df_reduzido.merge(
        df_calendario_reduzido,
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    salvar_log(f"Registros após merge com calendário: {len(df_reduzido)}", arquivo_log=LOG_MAILING)

    resultados = []
    datas_unicas = sorted(df_reduzido['DATA'].unique())

    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=LOG_MAILING)

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_MAILING)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)

        df_intervalo = df_reduzido[
            (df_reduzido['DATA'] >= inicio_mes) &
            (df_reduzido['DATA'] <= data)
        ].copy()

        info_data = df_reduzido[df_reduzido['DATA'] == data][
            ['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        ].drop_duplicates()

        if len(info_data) == 0:
            continue

        info_data = info_data.iloc[0]

        if segmentacoes_extras:
            # Com segmentação: agrupa por segmentação e gera uma linha por combinação
            df_contratos_unicos = df_intervalo.sort_values('DATA').groupby(['CONTRATO'] + segmentacoes_extras).tail(1)
            agrupado_contratos = df_contratos_unicos.groupby(segmentacoes_extras).agg({
                'CONTRATO': 'nunique',
                'VALOR': 'sum'
            }).reset_index()
            agrupado_contratos['Indicador'] = 'Contratos'
            agrupado_contratos = agrupado_contratos.rename(columns={'CONTRATO': 'qte'})

            df_cpfs_unicos = df_intervalo.sort_values('DATA').groupby(['CPF'] + segmentacoes_extras).tail(1)
            agrupado_cpfs = df_cpfs_unicos.groupby(segmentacoes_extras).agg({
                'CPF': 'nunique',
                'VALOR': 'sum'
            }).reset_index()
            agrupado_cpfs['Indicador'] = 'Carteira (CPFs)'
            agrupado_cpfs = agrupado_cpfs.rename(columns={'CPF': 'qte'})

            for agrupado in [agrupado_contratos, agrupado_cpfs]:
                agrupado['DATA'] = data
                agrupado['FX_ATRASO'] = 'Unique'
                agrupado['MesAbreviado'] = info_data['mes_abreviado']
                agrupado['nr_dia_util'] = info_data['nr_dia_util']
                agrupado['quartil'] = info_data['quartil']
                agrupado['dt_mes'] = info_data['dt_mes']

            resultados.append(agrupado_contratos)
            resultados.append(agrupado_cpfs)

        else:
            # Sem segmentação: comportamento original
            df_contratos_unicos = df_intervalo.sort_values('DATA').groupby('CONTRATO').tail(1)
            df_cpfs_unicos = df_intervalo.sort_values('DATA').groupby('CPF').tail(1)

            resultados.append({
                'DATA': data,
                'Indicador': 'Contratos',
                'qte': df_contratos_unicos['CONTRATO'].nunique(),
                'FX_ATRASO': 'Unique',
                'MesAbreviado': info_data['mes_abreviado'],
                'nr_dia_util': info_data['nr_dia_util'],
                'quartil': info_data['quartil'],
                'dt_mes': info_data['dt_mes'],
                'VALOR': df_contratos_unicos['VALOR'].sum()
            })
            resultados.append({
                'DATA': data,
                'Indicador': 'Carteira (CPFs)',
                'qte': df_cpfs_unicos['CPF'].nunique(),
                'FX_ATRASO': 'Unique',
                'MesAbreviado': info_data['mes_abreviado'],
                'nr_dia_util': info_data['nr_dia_util'],
                'quartil': info_data['quartil'],
                'dt_mes': info_data['dt_mes'],
                'VALOR': df_cpfs_unicos['VALOR'].sum()
            })

    df_acumulado = pd.DataFrame(resultados) if not segmentacoes_extras else pd.concat(resultados, ignore_index=True)

    colunas_finais = ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    df_acumulado = df_acumulado[colunas_finais]

    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log("RESUMO FINAL MALING_HIST", arquivo_log=LOG_MAILING)
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)
    salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}", arquivo_log=LOG_MAILING)
    salvar_log(f"Registros de Contratos: {len(df_acumulado[df_acumulado['Indicador'] == 'Contratos'])}", arquivo_log=LOG_MAILING)
    salvar_log(f"Registros de CPFs: {len(df_acumulado[df_acumulado['Indicador'] == 'Carteira (CPFs)'])}", arquivo_log=LOG_MAILING)
    salvar_log(f"Valor total final: R$ {df_acumulado['VALOR'].sum():,.2f}", arquivo_log=LOG_MAILING)
    salvar_log("=" * 60, arquivo_log=LOG_MAILING)

    return df_acumulado