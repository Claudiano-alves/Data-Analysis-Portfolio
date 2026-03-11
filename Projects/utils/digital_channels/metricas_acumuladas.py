import pandas as pd
import warnings
from utils.utils import registrar_tempo, salvar_log, unir_dataframes
from ..config import LOG_CHANNELS

import pandas as pd
import warnings


# =============================================================================
# FUNIL — acumulado mensal (do dia 1 do mês até cada data)
# =============================================================================
def massivos_fxAtraso_funil(df_massivos, segmentacoes_extras=None):
    """
    Contagem acumulada mensal de CPFs únicos por CANAL + FX_ATRASO + segmentacoes_extras.

    - Indicador recebe o valor do CANAL (SMS, WHATS, EMAIL, RCS, etc.)
    - A deduplicação por CPF é feita dentro de cada canal separadamente.
    - Um CPF é contado uma única vez por combinação CANAL + FX_ATRASO + segmentacoes_extras.

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
            Colunas obrigatórias: CPF, DATA, CANAL, VALOR, FX_ATRASO,
                                  nr_dia_util, quartil, dt_mes, mes_abreviado
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.

    Returns:
        pd.DataFrame:
            DATA | Indicador | qte | FX_ATRASO | [segmentacoes_extras] |
            MesAbreviado | nr_dia_util | quartil | dt_mes | VALORPRIN_FIN
    """
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    # CANAL entra no agrupamento para separar os cálculos por canal
    colunas_agrupamento = ['CANAL', 'FX_ATRASO'] + segmentacoes_extras
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

    df = df_massivos.copy()
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

            # Unicidade por CPF dentro de cada CANAL + FX_ATRASO + segmentacoes_extras
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
                VALORPRIN_FIN=('VALOR', 'sum')
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
                    combinacoes['VALORPRIN_FIN'] = 0.0
                    for col in colunas_calendario:
                        combinacoes[col] = None
                    ultimo_valor_por_mes[chave_mes] = combinacoes
                    resultados.append(combinacoes)

        if i < len(datas_calendario):
            proxima = datas_calendario[i]
            if proxima.month != data.month or proxima.year != data.year:
                ultimo_valor_por_mes.pop(chave_mes, None)

    df_final = pd.concat(resultados, ignore_index=True)

    # CANAL vira Indicador
    df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    return df_final[colunas_ordenadas]

def massivos_unique_funil(df_massivos, segmentacoes_extras=None):
    """
    Contagem acumulada mensal de CPFs únicos pela maior FX_ATRASO, por canal.

    - Indicador recebe o valor do CANAL.
    - Dentro de cada canal, para cada CPF acumulado no mês, considera apenas
      o registro de maior FX_ATRASO.
    - FX_ATRASO e segmentacoes_extras recebem label 'Unique' no output.

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.

    Returns:
        pd.DataFrame: FX_ATRASO = 'Unique', Indicador = CANAL.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

    df = df_massivos.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    # Ordena desc para drop_duplicates pegar maior FX_ATRASO por CPF dentro do canal
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

            # Maior FX_ATRASO por CPF DENTRO de cada canal (df já ordenado desc)
            df_unique = df_intervalo.drop_duplicates(subset=['CPF', 'CANAL'], keep='first')

            cal = (
                df[df['DATA'] == data][colunas_calendario]
                .iloc[0]
                .to_dict()
            )

            agrupado = df_unique.groupby(['CANAL']).agg(
                qte=('CPF', 'nunique'),
                VALORPRIN_FIN=('VALOR', 'sum')
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
                combinacoes = df[['CANAL']].drop_duplicates().copy()
                if len(combinacoes) > 0:
                    combinacoes['DATA'] = data
                    combinacoes['qte'] = 0
                    combinacoes['VALORPRIN_FIN'] = 0.0
                    for col in colunas_calendario:
                        combinacoes[col] = None
                    ultimo_valor_por_mes[chave_mes] = combinacoes
                    resultados.append(combinacoes)

        if i < len(datas_calendario):
            proxima = datas_calendario[i]
            if proxima.month != data.month or proxima.year != data.year:
                ultimo_valor_por_mes.pop(chave_mes, None)

    df_final = pd.concat(resultados, ignore_index=True)

    # Labels fixos para FX_ATRASO e segmentacoes_extras
    df_final['FX_ATRASO'] = 'Unique'
    for col in segmentacoes_extras:
        df_final[col] = 'Unique'

    # CANAL vira Indicador
    df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)

    # Consolida após sobrescrita dos labels
    colunas_grupo = (
        ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    )
    df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
        qte=('qte', 'sum'),
        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    )

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    return df_final[colunas_ordenadas]

def massivos_esforco_funil(df_massivos, segmentacoes_extras=None):
    """
    Contagem acumulada mensal do total de acionamentos por canal + FX_ATRASO (sem deduplicação).

    - Indicador recebe o valor do CANAL.
    - Se um CPF apareceu 10 vezes no canal no período, todas as 10 ocorrências são contadas.
    - FX_ATRASO e segmentacoes_extras recebem label 'Esforço' no output.

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.

    Returns:
        pd.DataFrame: FX_ATRASO = 'Esforço', Indicador = CANAL.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    colunas_agrupamento = ['CANAL', 'FX_ATRASO'] + segmentacoes_extras
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

    df = df_massivos.copy()
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

            # Sem deduplicação — conta todas as ocorrências
            agrupado = df_intervalo.groupby(colunas_agrupamento).agg(
                qte=('CPF', 'count'),
                VALORPRIN_FIN=('VALOR', 'sum')
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
                    combinacoes['VALORPRIN_FIN'] = 0.0
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

    # CANAL vira Indicador
    df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)

    colunas_grupo = (
        ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    )
    df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
        qte=('qte', 'sum'),
        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    )

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    return df_final[colunas_ordenadas]

# =============================================================================
# DAILY — granularidade diária (apenas o dia, sem acumulado)
# =============================================================================
def massivos_fxAtraso_daily(df_massivos, segmentacoes_extras=None):
    """
    CPFs únicos por dia por CANAL + FX_ATRASO + segmentacoes_extras.

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.

    Returns:
        pd.DataFrame: Contagem diária sem acumulado. Indicador = CANAL.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    colunas_agrupamento = ['CANAL', 'FX_ATRASO'] + segmentacoes_extras
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

    df = df_massivos.copy()
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
            VALORPRIN_FIN=('VALOR', 'sum')
        ).reset_index()

        agrupado['DATA'] = data
        resultados.append(agrupado)

    df_final = pd.concat(resultados, ignore_index=True)

    df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    return df_final[colunas_ordenadas]

def massivos_unique_daily(df_massivos, segmentacoes_extras=None):
    """
    CPFs únicos por dia e por canal (maior FX_ATRASO por CPF dentro do mesmo dia e canal).

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.

    Returns:
        pd.DataFrame: FX_ATRASO = 'Unique', Indicador = CANAL.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

    df = df_massivos.copy()
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

        # Maior FX_ATRASO por CPF dentro do canal (df já ordenado desc)
        df_unique = df_dia.drop_duplicates(subset=['CPF', 'CANAL'], keep='first')

        agrupado = df_unique.groupby(
            ['CANAL'] + colunas_calendario, dropna=False
        ).agg(
            qte=('CPF', 'nunique'),
            VALORPRIN_FIN=('VALOR', 'sum')
        ).reset_index()

        agrupado['DATA'] = data
        resultados.append(agrupado)

    df_final = pd.concat(resultados, ignore_index=True)

    df_final['FX_ATRASO'] = 'Unique'
    for col in segmentacoes_extras:
        df_final[col] = 'Unique'

    df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)

    colunas_grupo = (
        ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    )
    df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
        qte=('qte', 'sum'),
        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    )

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    return df_final[colunas_ordenadas]

def massivos_esforco_daily(df_massivos, segmentacoes_extras=None):
    """
    Total de acionamentos por dia e por canal (sem deduplicação).

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.

    Returns:
        pd.DataFrame: FX_ATRASO = 'Esforço', Indicador = CANAL.
    """
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

    df = df_massivos.copy()
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
        colunas_agrupamento = ['CANAL', 'FX_ATRASO'] + segmentacoes_extras

        agrupado = df_dia.groupby(
            colunas_agrupamento + colunas_calendario, dropna=False
        ).agg(
            qte=('CPF', 'count'),
            VALORPRIN_FIN=('VALOR', 'sum')
        ).reset_index()

        agrupado['DATA'] = data
        resultados.append(agrupado)

    df_final = pd.concat(resultados, ignore_index=True)

    df_final['FX_ATRASO'] = 'Esforço'
    for col in segmentacoes_extras:
        df_final[col] = 'Esforço'

    df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

    colunas_num = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_num] = df_final[colunas_num].fillna(0)

    colunas_grupo = (
        ['DATA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    )
    df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
        qte=('qte', 'sum'),
        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    )

    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    return df_final[colunas_ordenadas]

# =============================================================================
# PIPELINE ORQUESTRADOR
# =============================================================================
def processar_acumulados_massivos(
    df_massivos,
    segmentacoes_extras=None,
    calcular_funil=True,
    calcular_daily=True,
    retorno='separado'
):
    """
    Orquestra a geração e união de métricas acumuladas de massivos por canal.

    O df_massivos deve conter as colunas:
        CPF, DATA, CANAL, VALOR, FX_ATRASO,
        nr_dia_util, quartil, dt_mes, mes_abreviado
        + colunas em segmentacoes_extras (ex: ['FAIXA'])

    Depende da função unir_dataframes() disponível no escopo.

    O campo Indicador no output recebe o valor do CANAL (SMS, WHATS, EMAIL, RCS, etc.).
    Todos os cálculos (fxAtraso, unique, esforço) são realizados dentro de cada canal
    separadamente — um CPF pode ser contado em SMS e em WHATS de forma independente.

    Args:
        df_massivos (pd.DataFrame): DataFrame de massivos já com colunas de calendário.
        segmentacoes_extras (list, optional): Ex: ['FAIXA']. Default: None.
        calcular_funil (bool): Calcula acumulados mensais (funil). Default: True.
        calcular_daily (bool): Calcula granularidade diária. Default: True.
        retorno (str):
            'separado'    → retorna (df_funil, df_daily) — None para os não calculados
            'consolidado' → retorna df único com coluna TIPO = 'Funil' ou 'Daily'

    Returns:
        tuple | pd.DataFrame

    Exemplo de uso:
        # Retorno separado com FAIXA como segmentação
        df_funil, df_daily = processar_acumulados_massivos(
            df_massivos=df,
            segmentacoes_extras=['FAIXA']
        )

        # Retorno consolidado
        df_tudo = processar_acumulados_massivos(
            df_massivos=df,
            segmentacoes_extras=['FAIXA'],
            retorno='consolidado'
        )

        # Apenas funil
        df_funil, _ = processar_acumulados_massivos(
            df_massivos=df,
            segmentacoes_extras=['FAIXA'],
            calcular_daily=False
        )
    """
    if not calcular_funil and not calcular_daily:
        raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True.")

    # ---- FUNIL ---------------------------------------------------------------
    df_funil = None
    if calcular_funil:
        df_fxAtraso = massivos_fxAtraso_funil(df_massivos, segmentacoes_extras)
        df_unique   = massivos_unique_funil(df_massivos, segmentacoes_extras)
        df_esforco  = massivos_esforco_funil(df_massivos, segmentacoes_extras)
        df_funil    = unir_dataframes(df_fxAtraso, df_unique, df_esforco)

    # ---- DAILY ---------------------------------------------------------------
    df_daily = None
    if calcular_daily:
        df_fxAtraso_d = massivos_fxAtraso_daily(df_massivos, segmentacoes_extras)
        df_unique_d   = massivos_unique_daily(df_massivos, segmentacoes_extras)
        df_esforco_d  = massivos_esforco_daily(df_massivos, segmentacoes_extras)
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

@registrar_tempo("Acumulado por faixa de atraso (multicanal)", arquivo_log=LOG_CHANNELS)
def acumulado_por_faixa_atraso(df_dw_calendario, df_sms=None, df_email=None, df_rcs=None, df_whats=None):
    """
    Gera contagem acumulada mensal de CPFs únicos por FX_ATRASO para múltiplos canais.
    Quando um CPF aparece em múltiplas faixas no período, considera a MAIOR faixa de atraso.
    Garante que dias sem dados novos repliquem os valores do dia anterior.
    
    Args:
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        df_sms (pd.DataFrame, optional): DataFrame SMS com colunas: DATA, CPF, FX_ATRASO, COD_CLI
        df_email (pd.DataFrame, optional): DataFrame Email com colunas: DATA, CPF, FX_ATRASO, COD_CLI
        df_rcs (pd.DataFrame, optional): DataFrame RCS com colunas: DATA, CPF, FX_ATRASO, COD_CLI
        df_whats (pd.DataFrame, optional): DataFrame WhatsApp com colunas: DATA, CPF, FX_ATRASO, COD_CLI
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso e canal
    """
    warnings.filterwarnings("ignore", category=FutureWarning)

    # Dicionário com os DataFrames e seus respectivos nomes de canal
    canais = {
        'SMS': df_sms,
        'Email': df_email,
        'RCS': df_rcs,
        'WhatsApp': df_whats
    }
    
    # Filtrar apenas os canais que foram fornecidos (não None e não vazios)
    canais_ativos = {
        nome: df for nome, df in canais.items() 
        if df is not None and not df.empty
    }
    
    if not canais_ativos:
        salvar_log("⚠️ Nenhum DataFrame de canal foi fornecido!", arquivo_log=LOG_CHANNELS)
        return pd.DataFrame()
    
    salvar_log("=" * 80, arquivo_log=LOG_CHANNELS)
    salvar_log(f"📊 Canais ativos detectados: {', '.join(canais_ativos.keys())}", arquivo_log=LOG_CHANNELS)
    
    resultados_finais = []
    
    # Processar cada canal separadamente
    for nome_canal, df_canal in canais_ativos.items():
        salvar_log(f"\n📡 Processando canal: {nome_canal}", arquivo_log=LOG_CHANNELS)
        
        df = df_canal.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])

        # Preparar calendário
        df_dw_calendario_temp = df_dw_calendario.copy()
        df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
        
        # Obter todas as datas do calendário que estão no período dos dados
        data_min = df['DATA'].min()
        data_max = df['DATA'].max()
        
        df_calendario_periodo = df_dw_calendario_temp[
            (df_dw_calendario_temp['dt_data'] >= data_min) & 
            (df_dw_calendario_temp['dt_data'] <= data_max)
        ].sort_values('dt_data').copy()
        
        datas_calendario = df_calendario_periodo['dt_data'].tolist()
        resultados = []

        salvar_log(f"   Processando acumulado mensal por FX_ATRASO (maior prioridade) para {len(datas_calendario)} datas...", arquivo_log=LOG_CHANNELS)

        ultimo_valor_por_mes = {}

        for i, data in enumerate(datas_calendario, 1):
            if i % 10 == 0 or i == len(datas_calendario):
                salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_CHANNELS)
            
            inicio_mes = pd.Timestamp(data.year, data.month, 1)
            chave_mes = (data.year, data.month)
            
            tem_dados = (df['DATA'] == data).any()
            
            if tem_dados:
                # Filtrar dados do início do mês até a data atual
                df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
                
                # Ordenar por CPF e FX_ATRASO (descendente para pegar a maior faixa)
                df_intervalo = df_intervalo.sort_values(
                    ['CPF', 'FX_ATRASO'],
                    ascending=[True, False]
                )
                
                # Manter apenas o registro com maior FX_ATRASO para cada CPF
                df_filtrado = df_intervalo.drop_duplicates(
                    subset=['CPF'],
                    keep='first'
                ).copy()
                
                # Agrupar por FX_ATRASO e contar
                agrupado = df_filtrado.groupby('FX_ATRASO').agg(
                    QTD_CPF=('CPF', 'count')
                ).reset_index()
                
                agrupado['DATA'] = data
                agrupado['CANAL'] = nome_canal
                ultimo_valor_por_mes[chave_mes] = agrupado
                resultados.append(agrupado)
            
            else:
                # Replicar valores do último dia com dados no mês
                if chave_mes in ultimo_valor_por_mes:
                    agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                    agrupado_replicado['DATA'] = data
                    resultados.append(agrupado_replicado)
                else:
                    # Se não há dados anteriores no mês, criar registro com zeros
                    faixas_unicas = df['FX_ATRASO'].unique()
                    
                    if len(faixas_unicas) > 0:
                        agrupado_zero = pd.DataFrame({
                            'FX_ATRASO': faixas_unicas,
                            'DATA': data,
                            'CANAL': nome_canal,
                            'QTD_CPF': 0
                        })
                        
                        ultimo_valor_por_mes[chave_mes] = agrupado_zero
                        resultados.append(agrupado_zero)
            
            # Limpar cache do mês anterior quando mudar de mês
            if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
                mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
                if mes_anterior in ultimo_valor_por_mes:
                    del ultimo_valor_por_mes[mes_anterior]

        # Concatenar resultados do canal
        df_canal_final = pd.concat(resultados, ignore_index=True)
        resultados_finais.append(df_canal_final)
        
        salvar_log(f"   ✓ {nome_canal}: {len(df_canal_final):,} registros processados", arquivo_log=LOG_CHANNELS)

    # Concatenar todos os canais
    df_final = pd.concat(resultados_finais, ignore_index=True)

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_CHANNELS)
    # Preparar calendário novamente para o merge
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    # Fazer merge com calendário
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    # Preencher valores nulos com zero
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Ordenar colunas
    colunas_ordenadas = [
        'DATA', 'CANAL', 'FX_ATRASO', 'QTD_CPF',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"\n✓ Total de registros finais: {len(df_final):,}", arquivo_log=LOG_CHANNELS)
    salvar_log(f"\n📈 Totais acumulados por CANAL (última data):", arquivo_log=LOG_CHANNELS)
    
    ultima_data = df_final['DATA'].max()
    df_ultima_data = df_final[df_final['DATA'] == ultima_data]
    
    for canal in canais_ativos.keys():
        df_canal_stats = df_ultima_data[df_ultima_data['CANAL'] == canal]
        if not df_canal_stats.empty:
            salvar_log(f"   📡 {canal}:", arquivo_log=LOG_CHANNELS)
            salvar_log(f"      ✓ CPFs únicos: {df_canal_stats['QTD_CPF'].sum():,}", arquivo_log=LOG_CHANNELS)
    
    salvar_log("=" * 80, arquivo_log=LOG_CHANNELS)

    return df_final

@registrar_tempo("Acumulado unique (multicanal)", arquivo_log=LOG_CHANNELS)
def acumulado_unique(df_dw_calendario, df_sms=None, df_email=None, df_rcs=None, df_whats=None):
    """
    Gera contagem acumulada mensal de CPFs únicos (independente da faixa de atraso) para múltiplos canais.
    Cada CPF é contado apenas uma vez no período, independente de quantas vezes aparecer.
    Garante que dias sem dados novos repliquem os valores do dia anterior.
    
    Args:
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        df_sms (pd.DataFrame, optional): DataFrame SMS com colunas: DATA, CPF, FX_ATRASO, COD_CLI
        df_email (pd.DataFrame, optional): DataFrame Email com colunas: DATA, CPF, FX_ATRASO, COD_CLI
        df_rcs (pd.DataFrame, optional): DataFrame RCS com colunas: DATA, CPF, FX_ATRASO, COD_CLI
        df_whats (pd.DataFrame, optional): DataFrame WhatsApp com colunas: DATA, CPF, FX_ATRASO, COD_CLI
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por canal
    """
    warnings.filterwarnings("ignore", category=FutureWarning)

    # Dicionário com os DataFrames e seus respectivos nomes de canal
    canais = {
        'SMS': df_sms,
        'Email': df_email,
        'RCS': df_rcs,
        'WhatsApp': df_whats
    }
    
    # Filtrar apenas os canais que foram fornecidos (não None e não vazios)
    canais_ativos = {
        nome: df for nome, df in canais.items() 
        if df is not None and not df.empty
    }
    
    if not canais_ativos:
        salvar_log("⚠️ Nenhum DataFrame de canal foi fornecido!", arquivo_log=LOG_CHANNELS)
        return pd.DataFrame()
    
    salvar_log("=" * 80, arquivo_log=LOG_CHANNELS)
    salvar_log(f"📊 Canais ativos detectados: {', '.join(canais_ativos.keys())}", arquivo_log=LOG_CHANNELS)
    
    resultados_finais = []
    
    # Processar cada canal separadamente
    for nome_canal, df_canal in canais_ativos.items():
        salvar_log(f"\n📡 Processando canal: {nome_canal}", arquivo_log=LOG_CHANNELS)
        
        df = df_canal.copy()
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

        salvar_log(f"   Processando acumulado mensal ÚNICO (CPF único no período) para {len(datas_calendario)} datas...", arquivo_log=LOG_CHANNELS)

        ultimo_valor_por_mes = {}

        for i, data in enumerate(datas_calendario, 1):
            if i % 10 == 0 or i == len(datas_calendario):
                salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_CHANNELS)
            
            inicio_mes = pd.Timestamp(data.year, data.month, 1)
            chave_mes = (data.year, data.month)
            
            tem_dados = (df['DATA'] == data).any()
            
            if tem_dados:
                # Filtrar dados do início do mês até a data atual
                df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
                
                # Manter apenas um registro por CPF (primeiro que aparecer)
                df_unique = df_intervalo.drop_duplicates(
                    subset=['CPF'],
                    keep='first'
                ).copy()
                
                # Contar totais (sem agrupar por faixa)
                agrupado = pd.DataFrame({
                    'FX_ATRASO': ['Unique'],
                    'DATA': [data],
                    'CANAL': [nome_canal],
                    'QTD_CPF': [df_unique['CPF'].nunique()]
                })
                
                ultimo_valor_por_mes[chave_mes] = agrupado
                resultados.append(agrupado)
            
            else:
                # Replicar valores do último dia com dados no mês
                if chave_mes in ultimo_valor_por_mes:
                    agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                    agrupado_replicado['DATA'] = data
                    resultados.append(agrupado_replicado)
                else:
                    # Se não há dados anteriores no mês, criar registro com zeros
                    agrupado_zero = pd.DataFrame({
                        'FX_ATRASO': ['Unique'],
                        'DATA': [data],
                        'CANAL': [nome_canal],
                        'QTD_CPF': [0]
                    })
                    
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)
            
            # Limpar cache do mês anterior quando mudar de mês
            if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
                mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
                if mes_anterior in ultimo_valor_por_mes:
                    del ultimo_valor_por_mes[mes_anterior]

        # Concatenar resultados do canal
        df_canal_final = pd.concat(resultados, ignore_index=True)
        resultados_finais.append(df_canal_final)
        
        salvar_log(f"   ✓ {nome_canal}: {len(df_canal_final):,} registros processados", arquivo_log=LOG_CHANNELS)

    # Concatenar todos os canais
    df_final = pd.concat(resultados_finais, ignore_index=True)

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_CHANNELS)
    # Preparar calendário novamente para o merge
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    # Fazer merge com calendário
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    # Preencher valores nulos com zero
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Ordenar colunas
    colunas_ordenadas = [
        'DATA', 'CANAL', 'FX_ATRASO', 'QTD_CPF',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"\n✓ Total de registros finais: {len(df_final):,}", arquivo_log=LOG_CHANNELS)
    salvar_log(f"\n📈 Totais acumulados ÚNICOS por CANAL (última data):", arquivo_log=LOG_CHANNELS)
    
    ultima_data = df_final['DATA'].max()
    df_ultima_data = df_final[df_final['DATA'] == ultima_data]
    
    for canal in canais_ativos.keys():
        df_canal_stats = df_ultima_data[df_ultima_data['CANAL'] == canal]
        if not df_canal_stats.empty:
            salvar_log(f"   📡 {canal}:", arquivo_log=LOG_CHANNELS)
            salvar_log(f"      ✓ CPFs únicos: {df_canal_stats['QTD_CPF'].sum():,}", arquivo_log=LOG_CHANNELS)
    
    salvar_log("=" * 80, arquivo_log=LOG_CHANNELS)

    return df_final

@registrar_tempo("Acumulado esforço (multicanal)", arquivo_log=LOG_CHANNELS)
def acumulado_esforco(df_dw_calendario, df_sms=None, df_email=None, df_rcs=None, df_whats=None):
    """
    Gera contagem acumulada mensal de TODOS os registros (esforço total) para múltiplos canais.
    Considera CPFs duplicados - conta todas as ocorrências no período.
    Garante que dias sem dados novos repliquem os valores do dia anterior.
    
    Args:
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        df_sms (pd.DataFrame, optional): DataFrame SMS com colunas: DATA, CPF, FX_ATRASO, COD_CLI
        df_email (pd.DataFrame, optional): DataFrame Email com colunas: DATA, CPF, FX_ATRASO, COD_CLI
        df_rcs (pd.DataFrame, optional): DataFrame RCS com colunas: DATA, CPF, FX_ATRASO, COD_CLI
        df_whats (pd.DataFrame, optional): DataFrame WhatsApp com colunas: DATA, CPF, FX_ATRASO, COD_CLI
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais de esforço por canal
    """
    warnings.filterwarnings("ignore", category=FutureWarning)
    
    # Dicionário com os DataFrames e seus respectivos nomes de canal
    canais = {
        'SMS': df_sms,
        'Email': df_email,
        'RCS': df_rcs,
        'WhatsApp': df_whats
    }
    
    # Filtrar apenas os canais que foram fornecidos (não None e não vazios)
    canais_ativos = {
        nome: df for nome, df in canais.items() 
        if df is not None and not df.empty
    }
    
    if not canais_ativos:
        salvar_log("⚠️ Nenhum DataFrame de canal foi fornecido!", arquivo_log=LOG_CHANNELS)
        return pd.DataFrame()
    
    salvar_log("=" * 80, arquivo_log=LOG_CHANNELS)
    salvar_log(f"📊 Canais ativos detectados: {', '.join(canais_ativos.keys())}", arquivo_log=LOG_CHANNELS)
    
    resultados_finais = []
    
    # Processar cada canal separadamente
    for nome_canal, df_canal in canais_ativos.items():
        salvar_log(f"\n📡 Processando canal: {nome_canal}", arquivo_log=LOG_CHANNELS)
        
        df = df_canal.copy()
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
        
        salvar_log(f"   Processando acumulado mensal de ESFORÇO (todos os registros) para {len(datas_calendario)} datas...", arquivo_log=LOG_CHANNELS)
        
        ultimo_valor_por_mes = {}
        
        for i, data in enumerate(datas_calendario, 1):
            if i % 10 == 0 or i == len(datas_calendario):
                salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_CHANNELS)
            
            inicio_mes = pd.Timestamp(data.year, data.month, 1)
            chave_mes = (data.year, data.month)
            
            tem_dados = (df['DATA'] == data).any()
            
            if tem_dados:
                # Filtrar dados do início do mês até a data atual
                df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
                
                # Contar TODOS os registros (sem deduplicate)
                agrupado = pd.DataFrame({
                    'FX_ATRASO': ['Esforço'],
                    'DATA': [data],
                    'CANAL': [nome_canal],
                    'QTD_CPF': [len(df_intervalo)]  # Total de registros
                })
                
                ultimo_valor_por_mes[chave_mes] = agrupado
                resultados.append(agrupado)
            
            else:
                # Replicar valores do último dia com dados no mês
                if chave_mes in ultimo_valor_por_mes:
                    agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                    agrupado_replicado['DATA'] = data
                    resultados.append(agrupado_replicado)
                else:
                    # Se não há dados anteriores no mês, criar registro com zeros
                    agrupado_zero = pd.DataFrame({
                        'FX_ATRASO': ['Esforço'],
                        'DATA': [data],
                        'CANAL': [nome_canal],
                        'QTD_CPF': [0]
                    })
                    
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)
            
            # Limpar cache do mês anterior quando mudar de mês
            if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
                mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
                if mes_anterior in ultimo_valor_por_mes:
                    del ultimo_valor_por_mes[mes_anterior]
        
        # Concatenar resultados do canal
        df_canal_final = pd.concat(resultados, ignore_index=True)
        resultados_finais.append(df_canal_final)
        
        salvar_log(f"   ✓ {nome_canal}: {len(df_canal_final):,} registros processados", arquivo_log=LOG_CHANNELS)

    # Concatenar todos os canais
    df_final = pd.concat(resultados_finais, ignore_index=True)

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_CHANNELS)
    # Preparar calendário novamente para o merge
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    # Fazer merge com calendário
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])
    
    # Preencher valores nulos com zero
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    
    # Ordenar colunas
    colunas_ordenadas = [
        'DATA', 'CANAL', 'FX_ATRASO', 'QTD_CPF',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]
    
    salvar_log(f"\n✓ Total de registros finais: {len(df_final):,}", arquivo_log=LOG_CHANNELS)
    salvar_log(f"\n📈 Totais acumulados de ESFORÇO por CANAL (última data):", arquivo_log=LOG_CHANNELS)
    
    ultima_data = df_final['DATA'].max()
    df_ultima_data = df_final[df_final['DATA'] == ultima_data]
    
    for canal in canais_ativos.keys():
        df_canal_stats = df_ultima_data[df_ultima_data['CANAL'] == canal]
        if not df_canal_stats.empty:
            salvar_log(f"   📡 {canal}:", arquivo_log=LOG_CHANNELS)
            salvar_log(f"      ✓ Total de registros: {df_canal_stats['QTD_CPF'].sum():,}", arquivo_log=LOG_CHANNELS)
    
    salvar_log("=" * 80, arquivo_log=LOG_CHANNELS)

    return df_final