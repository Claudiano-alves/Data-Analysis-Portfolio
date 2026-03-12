"""
Módulo de Métricas Acumuladas - Discagens Expert

Responsável por calcular:
- Acumulado mensal de acionamentos (Esforço)
- Acumulado mensal de acionamentos únicos por CPF (Unique)
- Acumulado mensal de acionamentos únicos por CPF + FX_ATRASO (fxAtraso_origem)
"""

from typing import List, Optional
import pandas as pd
from utils.utils import salvar_log, registrar_tempo, unir_dataframes
from ...config import LOG_DISCAGENS

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


def discagens_operacao_fxAtraso_funil(df_discagens, segmentacoes_extras=None):
    """
    Contagem acumulada mensal de CPFs únicos por OPERACAO + FX_ATRASO + segmentacoes_extras.
 
    - Indicador recebe o valor de OPERACAO.
    - A deduplicação por CPF é feita dentro de cada operação separadamente.
    - Um CPF é contado uma única vez por combinação OPERACAO + FX_ATRASO + segmentacoes_extras.
 
    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
            Colunas obrigatórias: CPF, DATA, OPERACAO, VALOR, FX_ATRASO,
                                  nr_dia_util, quartil, dt_mes, mes_abreviado
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.
 
    Returns:
        pd.DataFrame:
            DATA | Indicador | qte | FX_ATRASO | [segmentacoes_extras] |
            MesAbreviado | nr_dia_util | quartil | dt_mes | VALOR
    """
    warnings.filterwarnings("ignore", category=FutureWarning)
 
    segmentacoes_extras = segmentacoes_extras or []
    colunas_agrupamento = ['OPERACAO', 'FX_ATRASO'] + segmentacoes_extras
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
 
    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
 
    datas_calendario = sorted(df['DATA'].unique())
    resultados = []
    ultimo_valor_por_mes = {}
 
    for i, data in enumerate(datas_calendario, 1):
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes  = (data.year, data.month)
 
        tem_dados = (df['DATA'] == data).any()
 
        if tem_dados:
            df_intervalo = df[
                (df['DATA'] >= inicio_mes) &
                (df['DATA'] <= data)
            ].copy()
 
            df_unico = df_intervalo.drop_duplicates(
                subset=['CPF'] + colunas_agrupamento,
                keep='first'
            )
 
            cal = (
                df[df['DATA'] == data][colunas_calendario]
                .iloc[0]
                .to_dict()
            )
 
            agrupado = df_unico.groupby(colunas_agrupamento).agg(
                qte=('CPF', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()
 
            for col, val in cal.items():
                agrupado[col] = val
            agrupado['DATA'] = data
 
            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)
 
        else:
            if chave_mes in ultimo_valor_por_mes:
                rep = ultimo_valor_por_mes[chave_mes].copy()
                rep['DATA'] = data
                resultados.append(rep)
            else:
                combinacoes = df[colunas_agrupamento].drop_duplicates().copy()
                if len(combinacoes) > 0:
                    combinacoes['DATA'] = data
                    combinacoes['qte'] = 0
                    combinacoes['VALOR'] = 0.0
                    for col in colunas_calendario:
                        combinacoes[col] = None
                    ultimo_valor_por_mes[chave_mes] = combinacoes
                    resultados.append(combinacoes)
 
        if i < len(datas_calendario):
            proxima = datas_calendario[i]
            if proxima.month != data.month or proxima.year != data.year:
                ultimo_valor_por_mes.pop(chave_mes, None)
 
    df_final = pd.concat(resultados, ignore_index=True)
 
    df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})
 
    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)
 
    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    return df_final[colunas_ordenadas]
  
def discagens_operacao_unique_funil(df_discagens, segmentacoes_extras=None):
    """
    Contagem acumulada mensal de CPFs únicos pela maior FX_ATRASO, por operação.
 
    - Indicador recebe o valor de OPERACAO.
    - Dentro de cada operação, para cada CPF acumulado no mês, considera apenas
      o registro de maior FX_ATRASO.
    - FX_ATRASO e segmentacoes_extras recebem label 'Unique' no output.
 
    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.
 
    Returns:
        pd.DataFrame: FX_ATRASO = 'Unique', Indicador = OPERACAO.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)
 
    segmentacoes_extras = segmentacoes_extras or []
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
 
    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    df = df.sort_values('FX_ATRASO', ascending=False)
 
    datas_calendario = sorted(df['DATA'].unique())
    resultados = []
    ultimo_valor_por_mes = {}
 
    for i, data in enumerate(datas_calendario, 1):
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes  = (data.year, data.month)
 
        tem_dados = (df['DATA'] == data).any()
 
        if tem_dados:
            df_intervalo = df[
                (df['DATA'] >= inicio_mes) &
                (df['DATA'] <= data)
            ].copy()
 
            # Maior FX_ATRASO por CPF dentro de cada operação (df já ordenado desc)
            df_unique = df_intervalo.drop_duplicates(subset=['CPF', 'OPERACAO'], keep='first')
 
            cal = (
                df[df['DATA'] == data][colunas_calendario]
                .iloc[0]
                .to_dict()
            )
 
            agrupado = df_unique.groupby(['OPERACAO']).agg(
                qte=('CPF', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()
 
            for col, val in cal.items():
                agrupado[col] = val
            agrupado['DATA'] = data
 
            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)
 
        else:
            if chave_mes in ultimo_valor_por_mes:
                rep = ultimo_valor_por_mes[chave_mes].copy()
                rep['DATA'] = data
                resultados.append(rep)
            else:
                combinacoes = df[['OPERACAO']].drop_duplicates().copy()
                if len(combinacoes) > 0:
                    combinacoes['DATA'] = data
                    combinacoes['qte'] = 0
                    combinacoes['VALOR'] = 0.0
                    for col in colunas_calendario:
                        combinacoes[col] = None
                    ultimo_valor_por_mes[chave_mes] = combinacoes
                    resultados.append(combinacoes)
 
        if i < len(datas_calendario):
            proxima = datas_calendario[i]
            if proxima.month != data.month or proxima.year != data.year:
                ultimo_valor_por_mes.pop(chave_mes, None)
 
    df_final = pd.concat(resultados, ignore_index=True)
 
    df_final['FX_ATRASO'] = 'Unique'
    for col in segmentacoes_extras:
        df_final[col] = 'Unique'
 
    df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})
 
    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)
 
    colunas_grupo = (
        ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    )
    df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
        qte=('qte', 'sum'),
        VALOR=('VALOR', 'sum')
    )
 
    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    return df_final[colunas_ordenadas]
  
def discagens_operacao_esforco_funil(df_discagens, segmentacoes_extras=None):
    """
    Contagem acumulada mensal do total de discagens por operação + FX_ATRASO (sem deduplicação).
 
    - Indicador recebe o valor de OPERACAO.
    - Se um CPF foi discado 10 vezes na operação no período, todas as 10 são contadas.
    - FX_ATRASO e segmentacoes_extras recebem label 'Esforço' no output.
 
    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.
 
    Returns:
        pd.DataFrame: FX_ATRASO = 'Esforço', Indicador = OPERACAO.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)
 
    segmentacoes_extras = segmentacoes_extras or []
    colunas_agrupamento = ['OPERACAO', 'FX_ATRASO'] + segmentacoes_extras
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
 
    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
 
    datas_calendario = sorted(df['DATA'].unique())
    resultados = []
    ultimo_valor_por_mes = {}
 
    for i, data in enumerate(datas_calendario, 1):
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes  = (data.year, data.month)
 
        tem_dados = (df['DATA'] == data).any()
 
        if tem_dados:
            df_intervalo = df[
                (df['DATA'] >= inicio_mes) &
                (df['DATA'] <= data)
            ].copy()
 
            cal = (
                df[df['DATA'] == data][colunas_calendario]
                .iloc[0]
                .to_dict()
            )
 
            agrupado = df_intervalo.groupby(colunas_agrupamento).agg(
                qte=('CPF', 'count'),
                VALOR=('VALOR', 'sum')
            ).reset_index()
 
            for col, val in cal.items():
                agrupado[col] = val
            agrupado['DATA'] = data
 
            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)
 
        else:
            if chave_mes in ultimo_valor_por_mes:
                rep = ultimo_valor_por_mes[chave_mes].copy()
                rep['DATA'] = data
                resultados.append(rep)
            else:
                combinacoes = df[colunas_agrupamento].drop_duplicates().copy()
                if len(combinacoes) > 0:
                    combinacoes['DATA'] = data
                    combinacoes['qte'] = 0
                    combinacoes['VALOR'] = 0.0
                    for col in colunas_calendario:
                        combinacoes[col] = None
                    ultimo_valor_por_mes[chave_mes] = combinacoes
                    resultados.append(combinacoes)
 
        if i < len(datas_calendario):
            proxima = datas_calendario[i]
            if proxima.month != data.month or proxima.year != data.year:
                ultimo_valor_por_mes.pop(chave_mes, None)
 
    df_final = pd.concat(resultados, ignore_index=True)
 
    df_final['FX_ATRASO'] = 'Esforço'
    for col in segmentacoes_extras:
        df_final[col] = 'Esforço'
 
    df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})
 
    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)
 
    colunas_grupo = (
        ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    )
    df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
        qte=('qte', 'sum'),
        VALOR=('VALOR', 'sum')
    )
 
    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    return df_final[colunas_ordenadas]
 
def discagens_operacao_fxAtraso_daily(df_discagens, segmentacoes_extras=None):
    """
    CPFs únicos por dia por OPERACAO + FX_ATRASO + segmentacoes_extras.
 
    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.
 
    Returns:
        pd.DataFrame: Contagem diária sem acumulado. Indicador = OPERACAO.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)
 
    segmentacoes_extras = segmentacoes_extras or []
    colunas_agrupamento = ['OPERACAO', 'FX_ATRASO'] + segmentacoes_extras
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
 
    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
 
    for col in colunas_calendario:
        if col not in df.columns:
            df[col] = None
    df['nr_dia_util']   = df['nr_dia_util'].fillna(0)
    df['quartil']       = df['quartil'].fillna(0)
    df['dt_mes']        = df['dt_mes'].fillna(pd.NaT)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')
 
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
 
    for data in datas_unicas:
        df_dia = df[df['DATA'] == data].copy()
 
        df_unico = df_dia.drop_duplicates(
            subset=['CPF'] + colunas_agrupamento,
            keep='first'
        )
 
        agrupado = df_unico.groupby(
            colunas_agrupamento + colunas_calendario, dropna=False
        ).agg(
            qte=('CPF', 'nunique'),
            VALOR=('VALOR', 'sum')
        ).reset_index()
 
        agrupado['DATA'] = data
        resultados.append(agrupado)
 
    df_final = pd.concat(resultados, ignore_index=True)
 
    df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})
 
    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)
 
    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    return df_final[colunas_ordenadas]
 
def discagens_operacao_unique_daily(df_discagens, segmentacoes_extras=None):
    """
    CPFs únicos por dia e por operação (maior FX_ATRASO por CPF dentro do mesmo dia e operação).
 
    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.
 
    Returns:
        pd.DataFrame: FX_ATRASO = 'Unique', Indicador = OPERACAO.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)
 
    segmentacoes_extras = segmentacoes_extras or []
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
 
    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    df = df.sort_values('FX_ATRASO', ascending=False)
 
    for col in colunas_calendario:
        if col not in df.columns:
            df[col] = None
    df['nr_dia_util']   = df['nr_dia_util'].fillna(0)
    df['quartil']       = df['quartil'].fillna(0)
    df['dt_mes']        = df['dt_mes'].fillna(pd.NaT)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')
 
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
 
    for data in datas_unicas:
        df_dia = df[df['DATA'] == data].copy()
 
        # Maior FX_ATRASO por CPF dentro da operação (df já ordenado desc)
        df_unique = df_dia.drop_duplicates(subset=['CPF', 'OPERACAO'], keep='first')
 
        agrupado = df_unique.groupby(
            ['OPERACAO'] + colunas_calendario, dropna=False
        ).agg(
            qte=('CPF', 'nunique'),
            VALOR=('VALOR', 'sum')
        ).reset_index()
 
        agrupado['DATA'] = data
        resultados.append(agrupado)
 
    df_final = pd.concat(resultados, ignore_index=True)
 
    df_final['FX_ATRASO'] = 'Unique'
    for col in segmentacoes_extras:
        df_final[col] = 'Unique'
 
    df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})
 
    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)
 
    colunas_grupo = (
        ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    )
    df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
        qte=('qte', 'sum'),
        VALOR=('VALOR', 'sum')
    )
 
    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    return df_final[colunas_ordenadas]
  
def discagens_operacao_esforco_daily(df_discagens, segmentacoes_extras=None):
    """
    Total de discagens por dia e por operação (sem deduplicação).
 
    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.
 
    Returns:
        pd.DataFrame: FX_ATRASO = 'Esforço', Indicador = OPERACAO.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)
 
    segmentacoes_extras = segmentacoes_extras or []
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
 
    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
 
    for col in colunas_calendario:
        if col not in df.columns:
            df[col] = None
    df['nr_dia_util']   = df['nr_dia_util'].fillna(0)
    df['quartil']       = df['quartil'].fillna(0)
    df['dt_mes']        = df['dt_mes'].fillna(pd.NaT)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')
 
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
 
    for data in datas_unicas:
        df_dia = df[df['DATA'] == data].copy()
        colunas_agrupamento = ['OPERACAO', 'FX_ATRASO'] + segmentacoes_extras
 
        agrupado = df_dia.groupby(
            colunas_agrupamento + colunas_calendario, dropna=False
        ).agg(
            qte=('CPF', 'count'),
            VALOR=('VALOR', 'sum')
        ).reset_index()
 
        agrupado['DATA'] = data
        resultados.append(agrupado)
 
    df_final = pd.concat(resultados, ignore_index=True)
 
    df_final['FX_ATRASO'] = 'Esforço'
    for col in segmentacoes_extras:
        df_final[col] = 'Esforço'
 
    df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})
 
    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)
 
    colunas_grupo = (
        ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    )
    df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
        qte=('qte', 'sum'),
        VALOR=('VALOR', 'sum')
    )
 
    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    return df_final[colunas_ordenadas]

@registrar_tempo("Funil fxAtraso - Discagens", arquivo_log=LOG_DISCAGENS)
def discagens_fxAtraso_funil(df_discagens, df_dw_calendario, segmentacoes_extras=None):
    """
    Gera contagem acumulada mensal de CPFs únicos por combinação de FX_ATRASO + segmentacoes_extras.
    
    Para cada combinação de FX_ATRASO + segmentacoes_extras, um CPF é contado uma única vez.
    Se o mesmo CPF aparecer em combinações diferentes, é contado em cada uma delas.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame de discagens unificado (expert + olos)
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas adicionais para segmentação.
                                              Ex: ['FAIXA'] para a carteira Renner
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso
        Colunas: DATA | Indicador | qte | FX_ATRASO | [segmentacoes_extras] | MesAbreviado | nr_dia_util | quartil | dt_mes | VALOR
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    colunas_agrupamento = ['FX_ATRASO'] + segmentacoes_extras

    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])

    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])

    data_min = df['DATA'].min()
    data_max = df['DATA'].max()

    df_calendario_periodo = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= data_min) &
        (df_dw_calendario_temp['dt_data'] <= data_max)
    ].sort_values('dt_data').copy()

    datas_calendario = df_calendario_periodo['dt_data'].tolist()
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Processando fxAtraso (CPF único por combinação {' + '.join(colunas_agrupamento)}) para {len(datas_calendario)} datas...", arquivo_log=LOG_DISCAGENS)

    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_DISCAGENS)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)

        tem_dados = (df['DATA'] == data).any()

        if tem_dados:
            df_intervalo = df[
                (df['DATA'] >= inicio_mes) &
                (df['DATA'] <= data)
            ].copy()

            # Unicidade por CPF + combinação de segmentação
            df_unico = df_intervalo.drop_duplicates(
                subset=['CPF'] + colunas_agrupamento,
                keep='first'
            ).copy()

            agrupado = df_unico.groupby(colunas_agrupamento).agg(
                qte=('CPF', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()

            agrupado['DATA'] = data
            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA'] = data
                resultados.append(agrupado_replicado)
            else:
                combinacoes = df[colunas_agrupamento].drop_duplicates()
                if len(combinacoes) > 0:
                    agrupado_zero = combinacoes.copy()
                    agrupado_zero['DATA'] = data
                    agrupado_zero['qte'] = 0
                    agrupado_zero['VALOR'] = 0.0
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)

    df_final['Indicador'] = 'Trabalhado'

    salvar_log(f"\\n📅 Merge com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ Total CPFs (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=LOG_DISCAGENS)
    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)

    return df_final

@registrar_tempo("Funil UNIQUE - Discagens", arquivo_log=LOG_DISCAGENS)
def discagens_unique_funil(df_discagens, df_dw_calendario, segmentacoes_extras=None):
    """
    Gera contagem acumulada mensal de CPFs únicos por maior faixa de atraso.
    
    Para cada CPF no período acumulado do mês, considera apenas o registro
    de maior FX_ATRASO. Em caso de empate na faixa, mantém apenas um registro.
    O VALOR que compõe a soma é o do registro selecionado.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame de discagens unificado (expert + olos)
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas adicionais para segmentação.
                                              Ex: ['FAIXA'] para a carteira Renner
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas
        Colunas: DATA | Indicador | qte | FX_ATRASO | [segmentacoes_extras] | MesAbreviado | nr_dia_util | quartil | dt_mes | VALOR
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []

    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Pré-ordenar o df uma única vez fora do loop
    df = df.sort_values('FX_ATRASO', ascending=False)

    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])

    data_min = df['DATA'].min()
    data_max = df['DATA'].max()

    df_calendario_periodo = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= data_min) &
        (df_dw_calendario_temp['dt_data'] <= data_max)
    ].sort_values('dt_data').copy()

    datas_calendario = df_calendario_periodo['dt_data'].tolist()
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Processando UNIQUE (maior FX_ATRASO por CPF) para {len(datas_calendario)} datas...", arquivo_log=LOG_DISCAGENS)

    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_DISCAGENS)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)

        tem_dados = (df['DATA'] == data).any()

        if tem_dados:
            df_intervalo = df[
                (df['DATA'] >= inicio_mes) &
                (df['DATA'] <= data)
            ].copy()

            # df já está ordenado por FX_ATRASO desc — drop_duplicates pega maior faixa por CPF
            df_unique = df_intervalo.drop_duplicates(subset=['CPF'], keep='first')

            colunas_agrupamento = ['FX_ATRASO'] + segmentacoes_extras

            agrupado = df_unique.groupby(colunas_agrupamento).agg(
                qte=('CPF', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()

            agrupado['DATA'] = data
            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA'] = data
                resultados.append(agrupado_replicado)
            else:
                combinacoes = df[['FX_ATRASO'] + segmentacoes_extras].drop_duplicates()
                if len(combinacoes) > 0:
                    agrupado_zero = combinacoes.copy()
                    agrupado_zero['DATA'] = data
                    agrupado_zero['qte'] = 0
                    agrupado_zero['VALOR'] = 0.0
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)

    df_final['FX_ATRASO'] = 'Unique'
    for col in segmentacoes_extras:
        df_final[col] = 'Unique'

    df_final['Indicador'] = 'Trabalhado'

    salvar_log(f"\\n📅 Merge com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_grupo = ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
        qte=('qte', 'sum'),
        VALOR=('VALOR', 'sum')
    )

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ CPFs únicos (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=LOG_DISCAGENS)
    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)

    return df_final

@registrar_tempo("Funil ESFORÇO - Discagens", arquivo_log=LOG_DISCAGENS)
def discagens_esforco_funil(df_discagens, df_dw_calendario, segmentacoes_extras=None):
    """
    Gera contagem acumulada mensal do total de discagens por FX_ATRASO (sem deduplicação).
    Se um CPF apareceu 10 vezes no período, todas as 10 discagens são contadas.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame de discagens unificado (expert + olos)
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas adicionais para segmentação.
                                              Ex: ['FAIXA'] para a carteira Renner
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais de esforço
        Colunas: DATA | Indicador | qte | FX_ATRASO | [segmentacoes_extras] | MesAbreviado | nr_dia_util | quartil | dt_mes | VALOR
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []

    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])

    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])

    data_min = df['DATA'].min()
    data_max = df['DATA'].max()

    df_calendario_periodo = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= data_min) &
        (df_dw_calendario_temp['dt_data'] <= data_max)
    ].sort_values('dt_data').copy()

    datas_calendario = df_calendario_periodo['dt_data'].tolist()
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Processando ESFORÇO (total de discagens) para {len(datas_calendario)} datas...", arquivo_log=LOG_DISCAGENS)

    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_DISCAGENS)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)

        tem_dados = (df['DATA'] == data).any()

        if tem_dados:
            df_intervalo = df[
                (df['DATA'] >= inicio_mes) &
                (df['DATA'] <= data)
            ].copy()

            colunas_agrupamento = ['FX_ATRASO'] + segmentacoes_extras

            agrupado = df_intervalo.groupby(colunas_agrupamento).agg(
                qte=('CPF', 'count'),
                VALOR=('VALOR', 'sum')
            ).reset_index()

            agrupado['DATA'] = data
            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA'] = data
                resultados.append(agrupado_replicado)
            else:
                combinacoes = df[['FX_ATRASO'] + segmentacoes_extras].drop_duplicates()
                if len(combinacoes) > 0:
                    agrupado_zero = combinacoes.copy()
                    agrupado_zero['DATA'] = data
                    agrupado_zero['qte'] = 0
                    agrupado_zero['VALOR'] = 0.0
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)

    df_final['FX_ATRASO'] = 'Esforço'
    for col in segmentacoes_extras:
        df_final[col] = 'Esforço'

    df_final['Indicador'] = 'Trabalhado'

    salvar_log(f"\\n📅 Merge com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    # Consolidar linhas duplicadas após sobrescrita de FX_ATRASO e segmentacoes_extras
    colunas_grupo = ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
        qte=('qte', 'sum'),
        VALOR=('VALOR', 'sum')
    )

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ Total discagens (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=LOG_DISCAGENS)
    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)

    return df_final

@registrar_tempo("Daily UNIQUE - Discagens", arquivo_log=LOG_DISCAGENS)
def discagens_unique_daily(df_discagens, df_dw_calendario, segmentacoes_extras=None):
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []

    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    df = df.sort_values('FX_ATRASO', ascending=False)

    # Preencher colunas de calendário nulas (sábados/domingos)
    df['nr_dia_util'] = df['nr_dia_util'].fillna(0)
    df['quartil'] = df['quartil'].fillna(0)
    df['dt_mes'] = df['dt_mes'].fillna(pd.NaT)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')

    datas_unicas = sorted(df['DATA'].unique())
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Processando UNIQUE DAILY (maior FX_ATRASO por CPF) para {len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)

        df_dia = df[df['DATA'] == data].copy()
        df_unique = df_dia.drop_duplicates(subset=['CPF'], keep='first')

        colunas_agrupamento = ['FX_ATRASO'] + segmentacoes_extras
        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        agrupado = df_unique.groupby(colunas_agrupamento + colunas_calendario, dropna=False).agg(
            qte=('CPF', 'nunique'),
            VALOR=('VALOR', 'sum')
        ).reset_index()

        agrupado['DATA'] = data
        resultados.append(agrupado)

    df_final = pd.concat(resultados, ignore_index=True)

    df_final['FX_ATRASO'] = 'Unique'
    for col in segmentacoes_extras:
        df_final[col] = 'Unique'
    df_final['Indicador'] = 'Trabalhado'

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_grupo = ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
        qte=('qte', 'sum'),
        VALOR=('VALOR', 'sum')
    )

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)

    return df_final

@registrar_tempo("Daily ESFORÇO - Discagens", arquivo_log=LOG_DISCAGENS)
def discagens_esforco_daily(df_discagens, df_dw_calendario, segmentacoes_extras=None):
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []

    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Preencher colunas de calendário nulas (sábados/domingos)
    df['nr_dia_util'] = df['nr_dia_util'].fillna(0)
    df['quartil'] = df['quartil'].fillna(0)
    df['dt_mes'] = df['dt_mes'].fillna(pd.NaT)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')

    datas_unicas = sorted(df['DATA'].unique())
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Processando ESFORÇO DAILY (total de discagens) para {len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)

        df_dia = df[df['DATA'] == data].copy()

        colunas_agrupamento = ['FX_ATRASO'] + segmentacoes_extras
        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        agrupado = df_dia.groupby(colunas_agrupamento + colunas_calendario, dropna=False).agg(
            qte=('CPF', 'count'),
            VALOR=('VALOR', 'sum')
        ).reset_index()

        agrupado['DATA'] = data
        resultados.append(agrupado)

    df_final = pd.concat(resultados, ignore_index=True)

    df_final['FX_ATRASO'] = 'Esforço'
    for col in segmentacoes_extras:
        df_final[col] = 'Esforço'
    df_final['Indicador'] = 'Trabalhado'

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_grupo = ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
        qte=('qte', 'sum'),
        VALOR=('VALOR', 'sum')
    )

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)

    return df_final

@registrar_tempo("Daily fxAtraso - Discagens", arquivo_log=LOG_DISCAGENS)
def discagens_fxAtraso_daily(df_discagens, df_dw_calendario, segmentacoes_extras=None):
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    colunas_agrupamento = ['FX_ATRASO'] + segmentacoes_extras

    df = df_discagens.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Preencher colunas de calendário nulas (sábados/domingos)
    df['nr_dia_util'] = df['nr_dia_util'].fillna(0)
    df['quartil'] = df['quartil'].fillna(0)
    df['dt_mes'] = df['dt_mes'].fillna(pd.NaT)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')

    datas_unicas = sorted(df['DATA'].unique())
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Processando fxAtraso DAILY (CPF único por combinação {' + '.join(colunas_agrupamento)}) para {len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)

        df_dia = df[df['DATA'] == data].copy()

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        df_unico = df_dia.drop_duplicates(
            subset=['CPF'] + colunas_agrupamento,
            keep='first'
        )

        agrupado = df_unico.groupby(colunas_agrupamento + colunas_calendario, dropna=False).agg(
            qte=('CPF', 'nunique'),
            VALOR=('VALOR', 'sum')
        ).reset_index()

        agrupado['DATA'] = data
        resultados.append(agrupado)

    df_final = pd.concat(resultados, ignore_index=True)

    df_final['Indicador'] = 'Trabalhado'

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)

    return df_final

@registrar_tempo("Pipeline acumulados discagens", arquivo_log=LOG_DISCAGENS)
def processar_acumulados_discagens(
    df_discagens,
    df_dw_calendario,
    segmentacoes_extras=None,
    calcular_funil=True,
    calcular_daily=True,
    retorno=None
):
    """
    Orquestra a geração e união de métricas acumuladas de discagens.

    Args:
        df_discagens (pd.DataFrame): DataFrame de discagens unificado (expert + olos)
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas adicionais para segmentação.
                                              Ex: ['FAIXA'] para a carteira Renner
        calcular_funil (bool): Se True, calcula os acumulados de funil. Default: True
        calcular_daily (bool): Se True, calcula os acumulados diários. Default: True
        retorno (str): 'separado' → retorna (df_funil, df_daily)
                       'consolidado' → retorna um único df com coluna 'TIPO' identificando funil/daily

    Returns:
        Se retorno='separado':
            tuple: (df_funil, df_daily) — None para os não calculados
        Se retorno='consolidado':
            pd.DataFrame: DataFrame único com coluna TIPO = 'Funil' ou 'Daily'
    """
    if not calcular_funil and not calcular_daily:
        raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True")

    # ============================================
    # ETAPA 1: FUNIL
    # ============================================
    df_funil = None
    if calcular_funil:
        df_fxAtraso = discagens_fxAtraso_funil(
            df_discagens=df_discagens,
            df_dw_calendario=df_dw_calendario,
            segmentacoes_extras=segmentacoes_extras
        )
        df_unique = discagens_unique_funil(
            df_discagens=df_discagens,
            df_dw_calendario=df_dw_calendario,
            segmentacoes_extras=segmentacoes_extras
        )
        df_esforco = discagens_esforco_funil(
            df_discagens=df_discagens,
            df_dw_calendario=df_dw_calendario,
            segmentacoes_extras=segmentacoes_extras
        )
        df_funil = unir_dataframes(df_fxAtraso, df_unique, df_esforco)

    # ============================================
    # ETAPA 2: DAILY
    # ============================================
    df_daily = None
    if calcular_daily:
        df_fxAtraso_daily = discagens_fxAtraso_daily(
            df_discagens=df_discagens,
            df_dw_calendario=df_dw_calendario,
            segmentacoes_extras=segmentacoes_extras
        )
        df_unique_daily = discagens_unique_daily(
            df_discagens=df_discagens,
            df_dw_calendario=df_dw_calendario,
            segmentacoes_extras=segmentacoes_extras
        )
        df_esforco_daily = discagens_esforco_daily(
            df_discagens=df_discagens,
            df_dw_calendario=df_dw_calendario,
            segmentacoes_extras=segmentacoes_extras
        )
        df_daily = unir_dataframes(df_fxAtraso_daily, df_unique_daily, df_esforco_daily)

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

def processar_acumulados_discagens_operacao(
    df_discagens,
    segmentacoes_extras=None,
    calcular_funil=True,
    calcular_daily=True,
    retorno=None
):
    """
    Orquestra a geração e união de métricas acumuladas de discagens por operação.

    O df_discagens deve conter as colunas:
        CPF, DATA, OPERACAO, VALOR, FX_ATRASO,
        nr_dia_util, quartil, dt_mes, mes_abreviado
        + colunas em segmentacoes_extras (ex: ['FAIXA'])

    Depende da função unir_dataframes() disponível no escopo.

    O campo Indicador no output recebe o valor de OPERACAO.
    Todos os cálculos (fxAtraso, unique, esforço) são realizados dentro de cada operação
    separadamente — um CPF pode ser contado em operações diferentes de forma independente.

    Args:
        df_discagens (pd.DataFrame): DataFrame de discagens já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.
        calcular_funil (bool): Calcula acumulados mensais (funil). Default: True.
        calcular_daily (bool): Calcula granularidade diária. Default: True.
        retorno (str):
            'separado'    → retorna (df_funil, df_daily) — None para os não calculados
            'consolidado' → retorna df único com coluna TIPO = 'Funil' ou 'Daily'

    Returns:
        tuple | pd.DataFrame

    Exemplo de uso:
        df_funil, df_daily = processar_acumulados_discagens_operacao(
            df_discagens=df,
            segmentacoes_extras=['FAIXA']
        )

        df_tudo = processar_acumulados_discagens_operacao(
            df_discagens=df,
            segmentacoes_extras=['FAIXA'],
            retorno='consolidado'
        )
    """
    if not calcular_funil and not calcular_daily:
        raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True.")

    # ---- FUNIL ---------------------------------------------------------------
    df_funil = None
    if calcular_funil:
        df_fxAtraso = discagens_operacao_fxAtraso_funil(df_discagens, segmentacoes_extras)
        df_unique   = discagens_operacao_unique_funil(df_discagens, segmentacoes_extras)
        df_esforco  = discagens_operacao_esforco_funil(df_discagens, segmentacoes_extras)
        df_funil    = unir_dataframes(df_fxAtraso, df_unique, df_esforco)

    # ---- DAILY ---------------------------------------------------------------
    df_daily = None
    if calcular_daily:
        df_fxAtraso_d = discagens_operacao_fxAtraso_daily(df_discagens, segmentacoes_extras)
        df_unique_d   = discagens_operacao_unique_daily(df_discagens, segmentacoes_extras)
        df_esforco_d  = discagens_operacao_esforco_daily(df_discagens, segmentacoes_extras)
        df_daily      = unir_dataframes(df_fxAtraso_d, df_unique_d, df_esforco_d)

    # ---- RETORNO -------------------------------------------------------------
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

def processar_acumulados_discagens_completo(
    df_discagens,
    df_dw_calendario,
    segmentacoes_extras=None,
    calcular_funil=True,
    calcular_daily=True,
    retorno=None,
):
    """
    Orquestra o cálculo de acumulados de discagens em duas visões e retorna
    um único DataFrame consolidado:

        1. Trabalhado — Indicador = 'Trabalhado' (via processar_acumulados_discagens)
        2. Por Operação — Indicador = OPERACAO    (via processar_acumulados_discagens_operacao)

    Args:
        df_discagens (pd.DataFrame): DataFrame de discagens já com colunas de calendário.
            Colunas obrigatórias: CPF, DATA, OPERACAO, VALOR, FX_ATRASO,
                                  nr_dia_util, quartil, dt_mes, mes_abreviado
                                  + colunas em segmentacoes_extras (ex: ['FAIXA'])
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.
        calcular_funil (bool): Calcula acumulados mensais (funil). Default: True.
        calcular_daily (bool): Calcula granularidade diária. Default: True.

    Returns:
        pd.DataFrame: DataFrame único com as duas visões empilhadas.
            Colunas: DATA | Indicador | qte | FX_ATRASO | [segmentacoes_extras] |
                     MesAbreviado | nr_dia_util | quartil | dt_mes | VALORPRIN_FIN | TIPO

    Exemplo de uso:
        df_final = processar_acumulados_discagens_completo(
            df_discagens=df,
            segmentacoes_extras=['FAIXA']
        )
    """
    if not calcular_funil and not calcular_daily:
        raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True.")

    # ---- VISÃO 1: TRABALHADO -------------------------------------------------
    df_trabalhado = processar_acumulados_discagens(
        df_discagens=df_discagens,
        df_dw_calendario=df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras,
        calcular_funil=calcular_funil,
        calcular_daily=calcular_daily,
        retorno=retorno
    )

    # ---- VISÃO 2: POR OPERAÇÃO -----------------------------------------------
    df_operacao = processar_acumulados_discagens_operacao(
        df_discagens=df_discagens,
        segmentacoes_extras=segmentacoes_extras,
        calcular_funil=calcular_funil,
        calcular_daily=calcular_daily,
        retorno=retorno
    )

    # ---- RETORNO -------------------------------------------------------------
    if retorno == 'consolidado':
        return unir_dataframes(df_trabalhado, df_operacao)
 
    # retorno != 'consolidado': cada função retornou (df_funil, df_daily)
    df_trabalhado_funil, df_trabalhado_daily = df_trabalhado
    df_operacao_funil,   df_operacao_daily   = df_operacao
 
    return df_trabalhado_funil, df_trabalhado_daily, df_operacao_funil, df_operacao_daily

__all__ = [
    'discagens_esforco_funil',
    'discagens_fxAtraso_funil',
    'discagens_unique_funil',
    'processar_acumulados_discagens',
    'discagens_fxAtraso_daily',
    'discagens_unique_daily',
    'discagens_esforco_daily',
    'discagens_operacao_fxAtraso_funil',
    'discagens_operacao_unique_funil',
    'discagens_operacao_esforco_funil',
    'discagens_operacao_fxAtraso_daily',
    'discagens_operacao_unique_daily',
    'discagens_operacao_esforco_daily',
    'processar_acumulados_discagens_operacao',
    'processar_acumulados_discagens_completo'
]
