"""
Módulo de Métricas Acumuladas de Mailing
Contém funções para gerar métricas acumuladas (mensais) de mailing.
"""

import pandas as pd
from utils.utils import registrar_tempo, unir_dataframes, salvar_log

def gerar_acumulado_funil_mailing_hist_segmentacoes(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=None):
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

def gerar_acumulado_funil_mailing_hist_unique(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=None):
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

def gerar_acumulado_daily_mailing_hist_segmentacoes(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=None):
    @registrar_tempo("Daily mailing hist segmentações", arquivo_log=arquivo_log)
    def _executar():
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log("INÍCIO DO PROCESSAMENTO DE MAILING_HIST DAILY", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log(f"Total de registros em df_mailing_hist: {len(df_mailing_hist)}", arquivo_log=arquivo_log)

        colunas_reduzidas = ['DATA', 'CONTRATO', 'CPF', 'VALOR'] + segmentacoes
        df_reduzido = df_mailing_hist[colunas_reduzidas].copy()
        df_reduzido['DATA'] = pd.to_datetime(df_reduzido['DATA'])

        df_calendario_temp = df_dw_calendario.copy()
        df_calendario_temp['dt_data'] = pd.to_datetime(df_calendario_temp['dt_data'])
        df_calendario_reduzido = df_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]

        df_reduzido = df_reduzido.merge(
            df_calendario_reduzido,
            left_on='DATA',
            right_on='dt_data',
            how='left'
        ).drop(columns=['dt_data'])

        df_reduzido['nr_dia_util']   = pd.to_numeric(df_reduzido['nr_dia_util'], errors='coerce').fillna(0).astype(int)
        df_reduzido['quartil']       = df_reduzido['quartil'].fillna('N/A').astype(str)
        df_reduzido['dt_mes']        = pd.to_numeric(df_reduzido['dt_mes'], errors='coerce').fillna(0).astype(int)
        df_reduzido['mes_abreviado'] = df_reduzido['mes_abreviado'].fillna('N/A').astype(str)

        salvar_log(f"Registros após merge com calendário: {len(df_reduzido)}", arquivo_log=arquivo_log)

        resultados = []
        datas_unicas = sorted(df_reduzido['DATA'].unique())
        salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=arquivo_log)

        for i, data in enumerate(datas_unicas, 1):
            if i % 10 == 0 or i == len(datas_unicas):
                salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=arquivo_log)

            df_dia = df_reduzido[df_reduzido['DATA'] == data].copy()

            info_data = df_dia[['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']].drop_duplicates()
            if len(info_data) == 0:
                continue
            info_data = info_data.iloc[0]

            campos_calendario = {
                'DATA':         data,
                'MesAbreviado': info_data['mes_abreviado'],
                'nr_dia_util':  info_data['nr_dia_util'],
                'quartil':      info_data['quartil'],
                'dt_mes':       info_data['dt_mes'],
            }

            agrupado_contratos = df_dia.drop_duplicates(subset=['CONTRATO'] + segmentacoes).groupby(segmentacoes).agg(
                qte=('CONTRATO', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()
            agrupado_contratos['Indicador'] = 'Contratos'
            for col, val in campos_calendario.items():
                agrupado_contratos[col] = val

            agrupado_cpfs = df_dia.drop_duplicates(subset=['CPF'] + segmentacoes).groupby(segmentacoes).agg(
                qte=('CPF', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()
            agrupado_cpfs['Indicador'] = 'Carteira (CPFs)'
            for col, val in campos_calendario.items():
                agrupado_cpfs[col] = val

            resultados.append(agrupado_contratos)
            resultados.append(agrupado_cpfs)

        df_final = pd.concat(resultados, ignore_index=True)
        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_finais = ['DATA', 'Indicador', 'qte'] + segmentacoes + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        df_final = df_final[colunas_finais]

        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log(f"✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def gerar_acumulado_daily_mailing_hist_unique(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=None):
    @registrar_tempo("Daily mailing hist unique", arquivo_log=arquivo_log)
    def _executar():
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log("INÍCIO DO PROCESSAMENTO DE MAILING_HIST DAILY UNIQUE", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log(f"Total de registros em df_mailing_hist: {len(df_mailing_hist)}", arquivo_log=arquivo_log)

        df_reduzido = df_mailing_hist[['DATA', 'CONTRATO', 'CPF', 'VALOR']].copy()
        df_reduzido['DATA'] = pd.to_datetime(df_reduzido['DATA'])

        df_calendario_temp = df_dw_calendario.copy()
        df_calendario_temp['dt_data'] = pd.to_datetime(df_calendario_temp['dt_data'])
        df_calendario_reduzido = df_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]

        df_reduzido = df_reduzido.merge(
            df_calendario_reduzido,
            left_on='DATA',
            right_on='dt_data',
            how='left'
        ).drop(columns=['dt_data'])

        df_reduzido['nr_dia_util']   = pd.to_numeric(df_reduzido['nr_dia_util'], errors='coerce').fillna(0).astype(int)
        df_reduzido['quartil']       = df_reduzido['quartil'].fillna('N/A').astype(str)
        df_reduzido['dt_mes']        = pd.to_numeric(df_reduzido['dt_mes'], errors='coerce').fillna(0).astype(int)
        df_reduzido['mes_abreviado'] = df_reduzido['mes_abreviado'].fillna('N/A').astype(str)

        salvar_log(f"Registros após merge com calendário: {len(df_reduzido)}", arquivo_log=arquivo_log)

        resultados = []
        datas_unicas = sorted(df_reduzido['DATA'].unique())
        salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=arquivo_log)

        for i, data in enumerate(datas_unicas, 1):
            if i % 10 == 0 or i == len(datas_unicas):
                salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=arquivo_log)

            df_dia = df_reduzido[df_reduzido['DATA'] == data].copy()

            info_data = df_dia[['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']].drop_duplicates()
            if len(info_data) == 0:
                continue
            info_data = info_data.iloc[0]

            campos_base = {
                'DATA':         data,
                'MesAbreviado': info_data['mes_abreviado'],
                'nr_dia_util':  info_data['nr_dia_util'],
                'quartil':      info_data['quartil'],
                'dt_mes':       info_data['dt_mes'],
                **{col: 'Unique' for col in segmentacoes},
            }

            resultados.append({
                **campos_base,
                'Indicador': 'Contratos',
                'qte':       df_dia['CONTRATO'].nunique(),
                'VALOR':     df_dia.drop_duplicates(subset=['CONTRATO'])['VALOR'].sum(),
            })

            resultados.append({
                **campos_base,
                'Indicador': 'Carteira (CPFs)',
                'qte':       df_dia['CPF'].nunique(),
                'VALOR':     df_dia.drop_duplicates(subset=['CPF'])['VALOR'].sum(),
            })

        df_final = pd.DataFrame(resultados)

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_finais = ['DATA', 'Indicador', 'qte'] + segmentacoes + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        df_final = df_final[colunas_finais]

        salvar_log("=" * 60, arquivo_log=arquivo_log)
        salvar_log(f"✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 60, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def processar_acumulados_mailing(
    df_mailing_hist,
    df_dw_calendario,
    segmentacoes,
    calcular_funil=True,
    calcular_daily=True,
    retorno='consolidado',
    arquivo_log=None
):
    """
    Orquestra a geração e união de métricas acumuladas de mailing.

    Args:
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist.
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        calcular_funil (bool): Se True, calcula os acumulados de funil. Default: True
        calcular_daily (bool): Se True, calcula os acumulados diários. Default: True
        retorno (str): 'separado'    → retorna (df_funil, df_daily)
                       'consolidado' → retorna um único df com coluna 'TIPO' identificando funil/daily
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MAILING

    Returns:
        Se retorno='separado':
            tuple: (df_funil, df_daily) — None para os não calculados
        Se retorno='consolidado':
            pd.DataFrame: DataFrame único com coluna TIPO = 'Funil' ou 'Daily'
    """
    @registrar_tempo("Pipeline acumulados mailing", arquivo_log=arquivo_log)
    def _executar():
        if not calcular_funil and not calcular_daily:
            raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True")

        # ============================================
        # ETAPA 1: FUNIL
        # ============================================
        df_funil = None
        if calcular_funil:
            df_seg    = gerar_acumulado_funil_mailing_hist_segmentacoes(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=arquivo_log)
            df_unique = gerar_acumulado_funil_mailing_hist_unique(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=arquivo_log)
            df_funil  = unir_dataframes(df_seg, df_unique)

        # ============================================
        # ETAPA 2: DAILY
        # ============================================
        df_daily = None
        if calcular_daily:
            df_seg_d    = gerar_acumulado_daily_mailing_hist_segmentacoes(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=arquivo_log)
            df_unique_d = gerar_acumulado_daily_mailing_hist_unique(df_mailing_hist, df_dw_calendario, segmentacoes, arquivo_log=arquivo_log)
            df_daily    = unir_dataframes(df_seg_d, df_unique_d)

        # ============================================
        # ETAPA 3: RETORNO
        # ============================================
        if retorno == 'consolidado':
            dfs = []
            if df_funil is not None:
                df_funil['TIPO'] = 'Funil'
                dfs.append(df_funil)
            if df_daily is not None:
                df_daily['TIPO'] = 'Daily'
                dfs.append(df_daily)
            return unir_dataframes(*dfs)

        return df_funil, df_daily

    return _executar()