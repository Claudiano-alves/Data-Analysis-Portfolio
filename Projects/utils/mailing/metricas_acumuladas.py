"""
Módulo de Métricas Acumuladas de Mailing
Contém funções para gerar métricas acumuladas (mensais) de mailing.
"""

import pandas as pd
from utils.utils import registrar_tempo, unir_dataframes, salvar_log

def gerar_acumulado_mailing_hist_segmentacoes(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=None):
    """
    Gera DataFrame com acumulado de contratos e CPFs do mailing por dia útil,
    agrupado pelas segmentações dinâmicas da carteira.

    Substitui gerar_acumulado_maling_hist_fxAtraso — agora sem FX_ATRASO fixo.
    A segmentação principal é definida pela carteira via parâmetro.

    Exemplos:
        Cresol:  segmentacoes=['PF_PJ', 'PA']
        Renner:  segmentacoes=['FX_ATRASO', 'FAIXA']
        Ouze:    segmentacoes=['FX_ATRASO']

    Args:
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MAILING

    Returns:
        pd.DataFrame: DataFrame com acumulado por segmentações
    """
    @registrar_tempo("Acumulado mailing hist segmentações", arquivo_log=arquivo_log)
    def _executar():
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log("INÍCIO DO PROCESSAMENTO DE MAILING_HIST", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log(f"Total de registros em df_mailing_hist: {len(df_mailing_hist)}", arquivo_log=arquivo_log)

        colunas_reduzidas = ['DATA', 'CONTRATO', 'CPF', 'VALOR'] + segmentacoes
        df_reduzido = df_mailing_hist[colunas_reduzidas].copy()
        salvar_log(f"Memória reduzida - trabalhando com {len(df_reduzido.columns)} colunas", arquivo_log=arquivo_log)

        df_reduzido['DATA'] = pd.to_datetime(df_reduzido['DATA'])

        df_calendario_temp = df_dw_calendario.copy()
        df_calendario_temp['dt_data'] = pd.to_datetime(df_calendario_temp['dt_data'])
        df_calendario_reduzido = df_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]

        df_reduzido = df_reduzido.merge(
            df_calendario_reduzido,
            left_on='DATA',
            right_on='dt_data',
            how='inner'
        ).drop(columns=['dt_data'])

        salvar_log(f"Registros após merge com calendário: {len(df_reduzido)}", arquivo_log=arquivo_log)

        resultados = []
        datas_unicas = sorted(df_reduzido['DATA'].unique())
        salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=arquivo_log)

        for i, data in enumerate(datas_unicas, 1):
            if i % 10 == 0 or i == len(datas_unicas):
                salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=arquivo_log)

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
            campos_calendario = {
                'DATA': data,
                'MesAbreviado': info_data['mes_abreviado'],
                'nr_dia_util': info_data['nr_dia_util'],
                'quartil': info_data['quartil'],
                'dt_mes': info_data['dt_mes'],
            }

            # Contratos únicos por segmentação
            df_contratos_unicos = df_intervalo.sort_values('DATA').groupby(['CONTRATO'] + segmentacoes).tail(1)
            agrupado_contratos = df_contratos_unicos.groupby(segmentacoes).agg(
                qte=('CONTRATO', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()
            agrupado_contratos['Indicador'] = 'Contratos'
            for col, val in campos_calendario.items():
                agrupado_contratos[col] = val

            # CPFs únicos por segmentação
            df_cpfs_unicos = df_intervalo.sort_values('DATA').groupby(['CPF'] + segmentacoes).tail(1)
            agrupado_cpfs = df_cpfs_unicos.groupby(segmentacoes).agg(
                qte=('CPF', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()
            agrupado_cpfs['Indicador'] = 'Carteira (CPFs)'
            for col, val in campos_calendario.items():
                agrupado_cpfs[col] = val

            resultados.append(agrupado_contratos)
            resultados.append(agrupado_cpfs)

        df_acumulado = pd.concat(resultados, ignore_index=True)

        colunas_finais = ['DATA', 'Indicador', 'qte'] + segmentacoes + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        df_acumulado = df_acumulado[colunas_finais]

        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log("RESUMO FINAL MAILING_HIST", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}", arquivo_log=arquivo_log)
        salvar_log(f"Registros de Contratos: {len(df_acumulado[df_acumulado['Indicador'] == 'Contratos'])}", arquivo_log=arquivo_log)
        salvar_log(f"Registros de CPFs: {len(df_acumulado[df_acumulado['Indicador'] == 'Carteira (CPFs)'])}", arquivo_log=arquivo_log)
        salvar_log(f"Valor total final: R$ {df_acumulado['VALOR'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)

        return df_acumulado

    return _executar()

def gerar_acumulado_mailing_hist_unique(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=None):
    """
    Gera DataFrame com acumulado ÚNICO de contratos e CPFs do mailing por dia útil.
    Todas as colunas de segmentacoes recebem label 'Unique' no output.

    Args:
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MAILING

    Returns:
        pd.DataFrame: DataFrame com acumulado único, segmentacoes = 'Unique'
    """
    @registrar_tempo("Acumulado mailing hist unique", arquivo_log=arquivo_log)
    def _executar():
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log("INÍCIO DO PROCESSAMENTO DE MAILING_HIST UNIQUE", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log(f"Total de registros em df_mailing_hist: {len(df_mailing_hist)}", arquivo_log=arquivo_log)

        df_reduzido = df_mailing_hist[['DATA', 'CONTRATO', 'CPF', 'VALOR']].copy()
        salvar_log(f"Memória reduzida - trabalhando com {len(df_reduzido.columns)} colunas", arquivo_log=arquivo_log)

        df_reduzido['DATA'] = pd.to_datetime(df_reduzido['DATA'])

        df_calendario_temp = df_dw_calendario.copy()
        df_calendario_temp['dt_data'] = pd.to_datetime(df_calendario_temp['dt_data'])
        df_calendario_reduzido = df_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]

        df_reduzido = df_reduzido.merge(
            df_calendario_reduzido,
            left_on='DATA',
            right_on='dt_data',
            how='inner'
        ).drop(columns=['dt_data'])

        salvar_log(f"Registros após merge com calendário: {len(df_reduzido)}", arquivo_log=arquivo_log)

        resultados = []
        datas_unicas = sorted(df_reduzido['DATA'].unique())
        salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=arquivo_log)

        for i, data in enumerate(datas_unicas, 1):
            if i % 10 == 0 or i == len(datas_unicas):
                salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=arquivo_log)

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

            campos_base = {
                'DATA': data,
                'MesAbreviado': info_data['mes_abreviado'],
                'nr_dia_util': info_data['nr_dia_util'],
                'quartil': info_data['quartil'],
                'dt_mes': info_data['dt_mes'],
                **{col: 'Unique' for col in segmentacoes},
            }

            # Contratos únicos
            df_contratos_unicos = df_intervalo.sort_values('DATA').groupby('CONTRATO').tail(1)
            resultados.append({
                **campos_base,
                'Indicador': 'Contratos',
                'qte': df_contratos_unicos['CONTRATO'].nunique(),
                'VALOR': df_contratos_unicos['VALOR'].sum(),
            })

            # CPFs únicos
            df_cpfs_unicos = df_intervalo.sort_values('DATA').groupby('CPF').tail(1)
            resultados.append({
                **campos_base,
                'Indicador': 'Carteira (CPFs)',
                'qte': df_cpfs_unicos['CPF'].nunique(),
                'VALOR': df_cpfs_unicos['VALOR'].sum(),
            })

        df_acumulado = pd.DataFrame(resultados)

        colunas_finais = ['DATA', 'Indicador', 'qte'] + segmentacoes + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        df_acumulado = df_acumulado[colunas_finais]

        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log("RESUMO FINAL MAILING_HIST UNIQUE", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}", arquivo_log=arquivo_log)
        salvar_log(f"Registros de Contratos: {len(df_acumulado[df_acumulado['Indicador'] == 'Contratos'])}", arquivo_log=arquivo_log)
        salvar_log(f"Registros de CPFs: {len(df_acumulado[df_acumulado['Indicador'] == 'Carteira (CPFs)'])}", arquivo_log=arquivo_log)
        salvar_log(f"Valor total final: R$ {df_acumulado['VALOR'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)

        return df_acumulado

    return _executar()