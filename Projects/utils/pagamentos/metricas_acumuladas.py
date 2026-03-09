"""
Módulo de Métricas Acumuladas de Pagamentos
Contém funções para gerar métricas acumuladas (mensais) de pagamentos.
"""

from typing import List, Optional, Tuple
import pandas as pd
from utils.utils import registrar_tempo, unir_dataframes, salvar_log, normalizar_tipos_df
from ..config import LOG_PAGAMENTOS

@registrar_tempo("Gerando acumulado de pagamentos")
def pagamentos_funil(
    df_pagamentos_tratado: pd.DataFrame,
    df_dw_calendario: pd.DataFrame,
    dimensoes_segmentacao: Optional[List[str]] = None,
    log_file: str = LOG_PAGAMENTOS
):
    """
    Gera acumulado de pagamentos do início do mês até cada dia útil.

    - Considera apenas pagamentos com FX_ATRASO (correlação na mailing)
    - Itera por todas as datas do calendário no período
    - Dias sem pagamento replicam o último valor do mês
    - Join com calendário via inner no final → exclui dias não úteis
    - df_esforco e df_unique agregam todas as segmentações em uma linha por data

    Args:
        df_pagamentos_tratado: DataFrame retornado por data_pagamentos
        df_dw_calendario: DataFrame de calendário
        dimensoes_segmentacao: Colunas extras para segmentar (ex: ['FAIXA'])
        log_file: Arquivo de log

    Returns:
        tuple: (df_acumulado, df_esforco, df_unique)
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    # ============================================
    # PREPARAÇÃO
    # ============================================
    dimensoes_segmentacao = dimensoes_segmentacao or []
    dimensoes_validas = [
        dim for dim in dimensoes_segmentacao
        if dim in df_pagamentos_tratado.columns
    ]
    colunas_segmentacao = ['FX_ATRASO'] + dimensoes_validas

    df = df_pagamentos_tratado[
        df_pagamentos_tratado['FX_ATRASO'].notna() &
        df_pagamentos_tratado['DATA_ACORDO'].notna()
    ].copy()
    df['DATA_PAGTO'] = pd.to_datetime(df['DATA_PAGTO'])

    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])

    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log("GERANDO ACUMULADO POR DIA ÚTIL", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log(f"Total de pagamentos com FX_ATRASO: {len(df)}", arquivo_log=log_file)
    salvar_log(f"Total sem FX_ATRASO (ignorados): {len(df_pagamentos_tratado) - len(df)}", arquivo_log=log_file)

    if dimensoes_validas:
        salvar_log(f"Dimensões de segmentação: {dimensoes_validas}", arquivo_log=log_file)
    else:
        salvar_log("Sem segmentação adicional", arquivo_log=log_file)

    if df.empty:
        salvar_log("AVISO: Nenhum pagamento com FX_ATRASO. Retornando DataFrames vazios.", arquivo_log=log_file)
        colunas = ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] + dimensoes_validas + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
        df_vazio = pd.DataFrame(columns=colunas)
        return df_vazio.copy(), df_vazio.copy(), df_vazio.copy()

    # ============================================
    # PERÍODO DO CALENDÁRIO
    # ============================================
    data_min = df['DATA_PAGTO'].min()
    data_max = df['DATA_PAGTO'].max()

    df_calendario_periodo = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= data_min) &
        (df_dw_calendario_temp['dt_data'] <= data_max)
    ].sort_values('dt_data').copy()

    datas_calendario = df_calendario_periodo['dt_data'].tolist()
    salvar_log(f"Total de datas no calendário a processar: {len(datas_calendario)}", arquivo_log=log_file)

    # ============================================
    # ACUMULADO POR DATA
    # ============================================
    resultados = []
    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"Processando {i}/{len(datas_calendario)} datas...", arquivo_log=log_file)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)
        tem_dados = (df['DATA_PAGTO'] == data).any()

        if tem_dados:
            df_intervalo = df[
                (df['DATA_PAGTO'] >= inicio_mes) &
                (df['DATA_PAGTO'] <= data)
            ].copy()

            agrupado = df_intervalo.groupby(colunas_segmentacao).agg(
                qte=('CPF_DEV', 'count'),
                VALOR_PARC=('VALOR_PARC', 'sum')
            ).reset_index()

            agrupado['DATA_PAGTO'] = data
            agrupado['Indicador']  = 'Pagamentos'

            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA_PAGTO'] = data
                resultados.append(agrupado_replicado)
            else:
                combinacoes = df[colunas_segmentacao].drop_duplicates()
                if len(combinacoes) > 0:
                    agrupado_zero = combinacoes.copy()
                    agrupado_zero['DATA_PAGTO'] = data
                    agrupado_zero['Indicador']  = 'Pagamentos'
                    agrupado_zero['qte']        = 0
                    agrupado_zero['VALOR_PARC'] = 0.0
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)

        # Limpar mês anterior da memória
        if i > 1 and data.month != datas_calendario[i - 2].month:
            mes_anterior = (datas_calendario[i - 2].year, datas_calendario[i - 2].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    # ============================================
    # JOIN COM CALENDÁRIO (inner → remove não úteis)
    # ============================================
    df_final = pd.concat(resultados, ignore_index=True)

    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_PAGTO',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    # ============================================
    # ORDENAR COLUNAS FINAIS
    # ============================================
    colunas_finais = (
        ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] +
        dimensoes_validas +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
    )
    df_final = df_final[colunas_finais]
    df_final = df_final[df_final['qte'] > 0]

    salvar_log(f"Total de registros acumulados: {len(df_final)}", arquivo_log=log_file)
    salvar_log(f"Quantidade total: {df_final['qte'].sum()}", arquivo_log=log_file)
    salvar_log(f"Valor total: R$ {df_final['VALOR_PARC'].sum():,.2f}", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)

    # ============================================
    # ESFORÇO E UNIQUE
    # Agrega todas as segmentações → uma linha por data
    # ============================================
    colunas_agrupamento_totais = ['DATA_PAGTO', 'Indicador', 'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']

    df_esforco = df_final.groupby(colunas_agrupamento_totais).agg(
        qte=('qte', 'sum'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()
    df_esforco['FX_ATRASO'] = 'Esforço'
    for dim in dimensoes_validas:
        df_esforco[dim] = 'Esforço'

    df_unique = df_final.groupby(colunas_agrupamento_totais).agg(
        qte=('qte', 'sum'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()
    df_unique['FX_ATRASO'] = 'Unique'
    for dim in dimensoes_validas:
        df_unique[dim] = 'Unique'

    # Reordenar colunas nos dfs de esforço e unique
    df_esforco = df_esforco[colunas_finais]
    df_unique  = df_unique[colunas_finais]

    return df_final, df_esforco, df_unique

def total_pagamentos_funil(
    df_pagamentos_tratado: pd.DataFrame,
    df_dw_calendario: pd.DataFrame,
    dimensoes_segmentacao: Optional[List[str]] = None,
    log_file: str = LOG_PAGAMENTOS
):
    """
    Gera acumulado de pagamentos do início do mês até cada dia útil.

    - Considera apenas pagamentos com FX_ATRASO (correlação na mailing)
    - Itera por todas as datas do calendário no período
    - Dias sem pagamento replicam o último valor do mês
    - Join com calendário via inner no final → exclui dias não úteis
    - df_esforco e df_unique agregam todas as segmentações em uma linha por data

    Args:
        df_pagamentos_tratado: DataFrame retornado por data_pagamentos
        df_dw_calendario: DataFrame de calendário
        dimensoes_segmentacao: Colunas extras para segmentar (ex: ['FAIXA'])
        log_file: Arquivo de log

    Returns:
        tuple: (df_acumulado, df_esforco, df_unique)
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    # ============================================
    # PREPARAÇÃO
    # ============================================
    dimensoes_segmentacao = dimensoes_segmentacao or []
    dimensoes_validas = [
        dim for dim in dimensoes_segmentacao
        if dim in df_pagamentos_tratado.columns
    ]
    colunas_segmentacao = ['FX_ATRASO'] + dimensoes_validas

    df = df_pagamentos_tratado[df_pagamentos_tratado['FX_ATRASO'].notna()].copy()
    df['DATA_PAGTO'] = pd.to_datetime(df['DATA_PAGTO'])

    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])

    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log("GERANDO ACUMULADO POR DIA ÚTIL", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log(f"Total de pagamentos com FX_ATRASO: {len(df)}", arquivo_log=log_file)
    salvar_log(f"Total sem FX_ATRASO (ignorados): {len(df_pagamentos_tratado) - len(df)}", arquivo_log=log_file)

    if dimensoes_validas:
        salvar_log(f"Dimensões de segmentação: {dimensoes_validas}", arquivo_log=log_file)
    else:
        salvar_log("Sem segmentação adicional", arquivo_log=log_file)

    if df.empty:
        salvar_log("AVISO: Nenhum pagamento com FX_ATRASO. Retornando DataFrames vazios.", arquivo_log=log_file)
        colunas = ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] + dimensoes_validas + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
        df_vazio = pd.DataFrame(columns=colunas)
        return df_vazio.copy(), df_vazio.copy(), df_vazio.copy()

    # ============================================
    # PERÍODO DO CALENDÁRIO
    # ============================================
    data_min = df['DATA_PAGTO'].min()
    data_max = df['DATA_PAGTO'].max()

    df_calendario_periodo = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= data_min) &
        (df_dw_calendario_temp['dt_data'] <= data_max)
    ].sort_values('dt_data').copy()

    datas_calendario = df_calendario_periodo['dt_data'].tolist()
    salvar_log(f"Total de datas no calendário a processar: {len(datas_calendario)}", arquivo_log=log_file)

    # ============================================
    # ACUMULADO POR DATA
    # ============================================
    resultados = []
    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"Processando {i}/{len(datas_calendario)} datas...", arquivo_log=log_file)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)
        tem_dados = (df['DATA_PAGTO'] == data).any()

        if tem_dados:
            df_intervalo = df[
                (df['DATA_PAGTO'] >= inicio_mes) &
                (df['DATA_PAGTO'] <= data)
            ].copy()

            agrupado = df_intervalo.groupby(colunas_segmentacao).agg(
                qte=('CPF_DEV', 'count'),
                VALOR_PARC=('VALOR_PARC', 'sum')
            ).reset_index()

            agrupado['DATA_PAGTO'] = data
            agrupado['Indicador']  = 'Total pagamentos'

            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA_PAGTO'] = data
                resultados.append(agrupado_replicado)
            else:
                combinacoes = df[colunas_segmentacao].drop_duplicates()
                if len(combinacoes) > 0:
                    agrupado_zero = combinacoes.copy()
                    agrupado_zero['DATA_PAGTO'] = data
                    agrupado_zero['Indicador']  = 'Total pagamentos'
                    agrupado_zero['qte']        = 0
                    agrupado_zero['VALOR_PARC'] = 0.0
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)

        # Limpar mês anterior da memória
        if i > 1 and data.month != datas_calendario[i - 2].month:
            mes_anterior = (datas_calendario[i - 2].year, datas_calendario[i - 2].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    # ============================================
    # JOIN COM CALENDÁRIO (inner → remove não úteis)
    # ============================================
    df_final = pd.concat(resultados, ignore_index=True)

    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_PAGTO',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    # ============================================
    # ORDENAR COLUNAS FINAIS
    # ============================================
    colunas_finais = (
        ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] +
        dimensoes_validas +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
    )
    df_final = df_final[colunas_finais]
    df_final = df_final[df_final['qte'] > 0]

    salvar_log(f"Total de registros acumulados: {len(df_final)}", arquivo_log=log_file)
    salvar_log(f"Quantidade total: {df_final['qte'].sum()}", arquivo_log=log_file)
    salvar_log(f"Valor total: R$ {df_final['VALOR_PARC'].sum():,.2f}", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)

    # ============================================
    # ESFORÇO E UNIQUE
    # Agrega todas as segmentações → uma linha por data
    # ============================================
    colunas_agrupamento_totais = ['DATA_PAGTO', 'Indicador', 'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']

    df_esforco = df_final.groupby(colunas_agrupamento_totais).agg(
        qte=('qte', 'sum'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()
    df_esforco['FX_ATRASO'] = 'Esforço'
    for dim in dimensoes_validas:
        df_esforco[dim] = 'Esforço'

    df_unique = df_final.groupby(colunas_agrupamento_totais).agg(
        qte=('qte', 'sum'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()
    df_unique['FX_ATRASO'] = 'Unique'
    for dim in dimensoes_validas:
        df_unique[dim] = 'Unique'

    # Reordenar colunas nos dfs de esforço e unique
    df_esforco = df_esforco[colunas_finais]
    df_unique  = df_unique[colunas_finais]

    return df_final, df_esforco, df_unique

@registrar_tempo("Daily pagamentos", arquivo_log=LOG_PAGAMENTOS)
def pagamentos_daily(
    df_pagamentos_tratado: pd.DataFrame,
    dimensoes_segmentacao: Optional[List[str]] = None,
    log_file: str = LOG_PAGAMENTOS
):
    """
    Gera contagem de pagamentos por dia (não acumulado).

    - Considera apenas pagamentos com FX_ATRASO e DATA_ACORDO
    - Retorna todos os dias, incluindo não úteis
    - df_esforco e df_unique agregam todas as segmentações em uma linha por data

    Args:
        df_pagamentos_tratado: DataFrame retornado por data_pagamentos
        dimensoes_segmentacao: Colunas extras para segmentar (ex: ['FAIXA'])
        log_file: Arquivo de log

    Returns:
        tuple: (df_final, df_esforco, df_unique)
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    # ============================================
    # PREPARAÇÃO
    # ============================================
    dimensoes_segmentacao = dimensoes_segmentacao or []
    dimensoes_validas = [
        dim for dim in dimensoes_segmentacao
        if dim in df_pagamentos_tratado.columns
    ]
    colunas_segmentacao = ['FX_ATRASO'] + dimensoes_validas

    # Filtrar pagamentos com FX_ATRASO e DATA_ACORDO
    df = df_pagamentos_tratado[
        df_pagamentos_tratado['FX_ATRASO'].notna() &
        df_pagamentos_tratado['DATA_ACORDO'].notna()
    ].copy()
    df['DATA_PAGTO'] = pd.to_datetime(df['DATA_PAGTO'])

    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log("GERANDO DAILY DE PAGAMENTOS", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log(f"Total de pagamentos com FX_ATRASO e DATA_ACORDO: {len(df)}", arquivo_log=log_file)
    salvar_log(f"Total ignorados: {len(df_pagamentos_tratado) - len(df)}", arquivo_log=log_file)

    if dimensoes_validas:
        salvar_log(f"Dimensões de segmentação: {dimensoes_validas}", arquivo_log=log_file)
    else:
        salvar_log("Sem segmentação adicional", arquivo_log=log_file)

    colunas_finais = (
        ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] +
        dimensoes_validas +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
    )

    if df.empty:
        salvar_log("AVISO: Nenhum pagamento encontrado. Retornando DataFrames vazios.", arquivo_log=log_file)
        df_vazio = pd.DataFrame(columns=colunas_finais)
        return df_vazio.copy(), df_vazio.copy(), df_vazio.copy()

    # ============================================
    # COLUNAS DO CALENDÁRIO JÁ PRESENTES NO DF
    # ============================================
    df['nr_dia_util']   = pd.to_numeric(df['nr_dia_util'], errors='coerce').fillna(0).astype(int)
    df['quartil']       = df['quartil'].fillna('N/A').astype(str)
    df['dt_mes']        = pd.to_numeric(df['dt_mes'], errors='coerce').fillna(0).astype(int)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A').astype(str)

    colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

    # ============================================
    # AGRUPAMENTO DIÁRIO
    # ============================================
    datas_unicas = sorted(df['DATA_PAGTO'].unique())
    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=log_file)

    resultados = []

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"Processando {i}/{len(datas_unicas)} datas...", arquivo_log=log_file)

        df_dia = df[df['DATA_PAGTO'] == data].copy()

        agrupado = df_dia.groupby(
            colunas_segmentacao + colunas_calendario,
            dropna=False
        ).agg(
            qte=('CPF_DEV', 'count'),
            VALOR_PARC=('VALOR_PARC', 'sum')
        ).reset_index()

        agrupado['DATA_PAGTO'] = data
        agrupado['Indicador']  = 'Pagamentos'
        resultados.append(agrupado)

    # ============================================
    # CONSOLIDAR E ORDENAR COLUNAS
    # ============================================
    df_final = pd.concat(resultados, ignore_index=True)
    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})
    df_final = df_final[colunas_finais]
    df_final = df_final[df_final['qte'] > 0]

    salvar_log(f"Total de registros diários: {len(df_final)}", arquivo_log=log_file)
    salvar_log(f"Quantidade total: {df_final['qte'].sum()}", arquivo_log=log_file)
    salvar_log(f"Valor total: R$ {df_final['VALOR_PARC'].sum():,.2f}", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)

    # ============================================
    # ESFORÇO E UNIQUE
    # Agrega todas as segmentações → uma linha por data
    # ============================================
    colunas_agrupamento_totais = ['DATA_PAGTO', 'Indicador', 'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']

    df_esforco = df_final.groupby(colunas_agrupamento_totais).agg(
        qte=('qte', 'sum'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()
    df_esforco['FX_ATRASO'] = 'Esforço'
    for dim in dimensoes_validas:
        df_esforco[dim] = 'Esforço'
    df_esforco = df_esforco[colunas_finais]

    df_unique = df_final.groupby(colunas_agrupamento_totais).agg(
        qte=('qte', 'sum'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()
    df_unique['FX_ATRASO'] = 'Unique'
    for dim in dimensoes_validas:
        df_unique[dim] = 'Unique'
    df_unique = df_unique[colunas_finais]

    return df_final, df_esforco, df_unique

@registrar_tempo("Daily pagamentos", arquivo_log=LOG_PAGAMENTOS)
def total_pagamentos_daily(
    df_pagamentos_tratado: pd.DataFrame,
    dimensoes_segmentacao: Optional[List[str]] = None,
    log_file: str = LOG_PAGAMENTOS
):
    """
    Gera contagem de pagamentos por dia (não acumulado).

    - Considera apenas pagamentos com FX_ATRASO e DATA_ACORDO
    - Retorna todos os dias, incluindo não úteis
    - df_esforco e df_unique agregam todas as segmentações em uma linha por data

    Args:
        df_pagamentos_tratado: DataFrame retornado por data_pagamentos
        dimensoes_segmentacao: Colunas extras para segmentar (ex: ['FAIXA'])
        log_file: Arquivo de log

    Returns:
        tuple: (df_final, df_esforco, df_unique)
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    # ============================================
    # PREPARAÇÃO
    # ============================================
    dimensoes_segmentacao = dimensoes_segmentacao or []
    dimensoes_validas = [
        dim for dim in dimensoes_segmentacao
        if dim in df_pagamentos_tratado.columns
    ]
    colunas_segmentacao = ['FX_ATRASO'] + dimensoes_validas

    # Filtrar pagamentos com FX_ATRASO e DATA_ACORDO
    df = df_pagamentos_tratado[df_pagamentos_tratado['FX_ATRASO'].notna()].copy()
    df['DATA_PAGTO'] = pd.to_datetime(df['DATA_PAGTO'])

    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log("GERANDO DAILY DE PAGAMENTOS", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log(f"Total de pagamentos com FX_ATRASO e DATA_ACORDO: {len(df)}", arquivo_log=log_file)
    salvar_log(f"Total ignorados: {len(df_pagamentos_tratado) - len(df)}", arquivo_log=log_file)

    if dimensoes_validas:
        salvar_log(f"Dimensões de segmentação: {dimensoes_validas}", arquivo_log=log_file)
    else:
        salvar_log("Sem segmentação adicional", arquivo_log=log_file)

    colunas_finais = (
        ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] +
        dimensoes_validas +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
    )

    if df.empty:
        salvar_log("AVISO: Nenhum pagamento encontrado. Retornando DataFrames vazios.", arquivo_log=log_file)
        df_vazio = pd.DataFrame(columns=colunas_finais)
        return df_vazio.copy(), df_vazio.copy(), df_vazio.copy()

    # ============================================
    # COLUNAS DO CALENDÁRIO JÁ PRESENTES NO DF
    # ============================================
    df['nr_dia_util']   = pd.to_numeric(df['nr_dia_util'], errors='coerce').fillna(0).astype(int)
    df['quartil']       = df['quartil'].fillna('N/A').astype(str)
    df['dt_mes']        = pd.to_numeric(df['dt_mes'], errors='coerce').fillna(0).astype(int)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A').astype(str)

    colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

    # ============================================
    # AGRUPAMENTO DIÁRIO
    # ============================================
    datas_unicas = sorted(df['DATA_PAGTO'].unique())
    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}", arquivo_log=log_file)

    resultados = []

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"Processando {i}/{len(datas_unicas)} datas...", arquivo_log=log_file)

        df_dia = df[df['DATA_PAGTO'] == data].copy()

        agrupado = df_dia.groupby(
            colunas_segmentacao + colunas_calendario,
            dropna=False
        ).agg(
            qte=('CPF_DEV', 'count'),
            VALOR_PARC=('VALOR_PARC', 'sum')
        ).reset_index()

        agrupado['DATA_PAGTO'] = data
        agrupado['Indicador']  = 'Pagamentos'
        resultados.append(agrupado)

    # ============================================
    # CONSOLIDAR E ORDENAR COLUNAS
    # ============================================
    df_final = pd.concat(resultados, ignore_index=True)
    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})
    df_final = df_final[colunas_finais]
    df_final = df_final[df_final['qte'] > 0]

    salvar_log(f"Total de registros diários: {len(df_final)}", arquivo_log=log_file)
    salvar_log(f"Quantidade total: {df_final['qte'].sum()}", arquivo_log=log_file)
    salvar_log(f"Valor total: R$ {df_final['VALOR_PARC'].sum():,.2f}", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)

    # ============================================
    # ESFORÇO E UNIQUE
    # Agrega todas as segmentações → uma linha por data
    # ============================================
    colunas_agrupamento_totais = ['DATA_PAGTO', 'Indicador', 'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']

    df_esforco = df_final.groupby(colunas_agrupamento_totais).agg(
        qte=('qte', 'sum'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()
    df_esforco['FX_ATRASO'] = 'Esforço'
    for dim in dimensoes_validas:
        df_esforco[dim] = 'Esforço'
    df_esforco = df_esforco[colunas_finais]

    df_unique = df_final.groupby(colunas_agrupamento_totais).agg(
        qte=('qte', 'sum'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()
    df_unique['FX_ATRASO'] = 'Unique'
    for dim in dimensoes_validas:
        df_unique[dim] = 'Unique'
    df_unique = df_unique[colunas_finais]

    return df_final, df_esforco, df_unique

@registrar_tempo("Pipeline acumulados pagamentos", arquivo_log=LOG_PAGAMENTOS)
def processar_acumulados_pagamentos(
    df_pagamentos_tratado: pd.DataFrame,
    df_dw_calendario: pd.DataFrame,
    dimensoes_segmentacao: Optional[List[str]] = None,
    calcular_funil: bool = True,
    calcular_daily: bool = True,
    retorno: str = 'separado'
):
    """
    Orquestra a geração, transformação e união de métricas de pagamentos.

    Args:
        df_pagamentos_tratado: DataFrame retornado por data_pagamentos
        df_dw_calendario: DataFrame com dados de calendário
        dimensoes_segmentacao: Colunas adicionais para segmentação (ex: ['FAIXA'])
        calcular_funil: Se True, calcula acumulado mensal. Default: True
        calcular_daily: Se True, calcula contagem diária. Default: True
        retorno: 'separado' → retorna (df_funil, df_daily)
                 'consolidado' → retorna um único df com coluna 'TIPO' = 'Funil' ou 'Daily'

    Returns:
        Se retorno='separado':
            tuple: (df_funil, df_daily) — None para os não calculados
        Se retorno='consolidado':
            pd.DataFrame: DataFrame único com coluna TIPO
    """
    if not calcular_funil and not calcular_daily:
        raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True")

    # ============================================
    # ETAPA 1: FUNIL (acumulado mensal)
    # ============================================
    df_funil = None
    if calcular_funil:
        df_acumulado, df_esforco_funil, df_unique_funil = pagamentos_funil(
            df_pagamentos_tratado=df_pagamentos_tratado,
            df_dw_calendario=df_dw_calendario,
            dimensoes_segmentacao=dimensoes_segmentacao
        )

        df_total_funil, df_total_esforco_funil, df_total_unique_funil = total_pagamentos_funil(
            df_pagamentos_tratado=df_pagamentos_tratado,
            df_dw_calendario=df_dw_calendario,
            dimensoes_segmentacao=dimensoes_segmentacao
        )

        df_funil = unir_dataframes(
            normalizar_tipos_df(df_acumulado),
            normalizar_tipos_df(df_esforco_funil),
            normalizar_tipos_df(df_unique_funil),
            normalizar_tipos_df(df_total_funil),
            normalizar_tipos_df(df_total_esforco_funil),
            normalizar_tipos_df(df_total_unique_funil)
        )

    # ============================================
    # ETAPA 2: DAILY (contagem diária)
    # ============================================
    df_daily = None
    if calcular_daily:
        df_dia, df_esforco_daily, df_unique_daily = pagamentos_daily(
            df_pagamentos_tratado=df_pagamentos_tratado,
            dimensoes_segmentacao=dimensoes_segmentacao
        )

        df_total_dia, df_total_esforco_daily, df_total_unique_daily = total_pagamentos_daily(
            df_pagamentos_tratado=df_pagamentos_tratado,
            dimensoes_segmentacao=dimensoes_segmentacao
        )

        df_daily = unir_dataframes(
            normalizar_tipos_df(df_dia),
            normalizar_tipos_df(df_esforco_daily),
            normalizar_tipos_df(df_unique_daily),
            normalizar_tipos_df(df_total_dia),
            normalizar_tipos_df(df_total_esforco_daily),
            normalizar_tipos_df(df_total_unique_daily)
        )

    # ============================================
    # ETAPA 3: RETORNO
    # ============================================
    if retorno == 'consolidado':
        dfs = []
        if df_funil is not None:
            df_funil['TIPO'] = 'Funil'
            dfs.append(normalizar_tipos_df(df_funil))
        if df_daily is not None:
            df_daily['TIPO'] = 'Daily'
            dfs.append(normalizar_tipos_df(df_daily))
        return unir_dataframes(*dfs)

    return df_funil, df_daily

def _criar_dataframes_vazios_pagamentos(
    dimensoes_segmentacao: Optional[List[str]] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Cria DataFrames vazios com estrutura correta para pagamentos.
    
    Args:
        dimensoes_segmentacao: Dimensões adicionais para incluir nas colunas
    
    Returns:
        tuple: (df_vazio, df_vazio, df_vazio)
    """
    if dimensoes_segmentacao is None:
        dimensoes_segmentacao = []
    
    colunas = (
        ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] +
        dimensoes_segmentacao +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
    )
    
    df_vazio = pd.DataFrame(columns=colunas)
    return df_vazio.copy(), df_vazio.copy(), df_vazio.copy()