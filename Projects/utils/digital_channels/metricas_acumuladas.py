import pandas as pd
import warnings
from utils.utils import registrar_tempo, salvar_log, unir_dataframes

def massivos_segmentacoes_funil(df_massivos, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal de CPFs únicos por CANAL + segmentacoes.

    Função dinâmica — não assume nenhuma segmentação padrão.
    As segmentações são definidas pela carteira na chamada da função.

    Exemplos:
        Com faixa de atraso:  segmentacoes=['FX_ATRASO', 'FAIXA']
        Sem faixa de atraso:  segmentacoes=['FX_ATRASO']

    - Indicador recebe o valor do CANAL (SMS, WHATS, EMAIL, RCS, etc.)
    - A deduplicação por CPF é feita dentro de cada canal separadamente.
    - Um CPF é contado uma única vez por combinação CANAL + segmentacoes.
    - Apenas dias úteis (nr_dia_util > 0) geram registros no funil.
      Dados de fins de semana são incorporados no próximo dia útil.

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
            Colunas obrigatórias: CPF, DATA, CANAL, VALOR,
                                  nr_dia_util, quartil, dt_mes, mes_abreviado
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MASSIVOS

    Returns:
        pd.DataFrame:
            DATA | Indicador | qte | [segmentacoes] |
            MesAbreviado | nr_dia_util | quartil | dt_mes | VALORPRIN_FIN
    """
    @registrar_tempo("Funil Segmentações - Massivos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_agrupamento = ['CANAL'] + segmentacoes
        colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        df = df_massivos.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])

        # Apenas dias úteis definem os pontos de corte do funil
        datas_calendario = sorted(
            df.loc[df['nr_dia_util'] > 0, 'DATA'].unique()
        )
        resultados = []
        ultimo_valor_por_mes = {}

        for i, data in enumerate(datas_calendario, 1):
            inicio_mes = pd.Timestamp(data.year, data.month, 1)
            chave_mes  = (data.year, data.month)

            # Acumula do início do mês até a data atual (inclui fins de semana nos dados)
            df_intervalo = df[
                (df['DATA'] >= inicio_mes) &
                (df['DATA'] <= data)
            ].copy()

            df_unico = df_intervalo.drop_duplicates(
                subset=['CPF'] + colunas_agrupamento,
                keep='first'
            )

            cal = df.loc[df['DATA'] == data, colunas_calendario].iloc[0].to_dict()

            agrupado = df_unico.groupby(colunas_agrupamento).agg(
                qte=('CPF', 'nunique'),
                VALORPRIN_FIN=('VALOR', 'sum')
            ).reset_index()

            for col, val in cal.items():
                agrupado[col] = val
            agrupado['DATA'] = data

            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

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
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total qte (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALORPRIN_FIN total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALORPRIN_FIN'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def massivos_unique_funil(df_massivos, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal de CPFs únicos pelo maior ATRASO, por canal.

    - Indicador recebe o valor do CANAL.
    - Dentro de cada canal, para cada CPF acumulado no mês, considera apenas
      o registro de maior ATRASO.
    - Todas as colunas de segmentacoes recebem label 'Unique' no output.
    - Apenas dias úteis (nr_dia_util > 0) geram registros no funil.
      Dados de fins de semana são incorporados no próximo dia útil.

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MASSIVOS

    Returns:
        pd.DataFrame: segmentacoes = 'Unique', Indicador = CANAL.
    """
    @registrar_tempo("Funil Unique - Massivos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        df = df_massivos.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])
        # Ordena desc por ATRASO para drop_duplicates pegar o registro de maior atraso
        # por CPF dentro do canal
        df = df.sort_values('ATRASO', ascending=False)

        # Apenas dias úteis definem os pontos de corte do funil
        datas_calendario = sorted(
            df.loc[df['nr_dia_util'] > 0, 'DATA'].unique()
        )
        resultados = []
        ultimo_valor_por_mes = {}

        for i, data in enumerate(datas_calendario, 1):
            inicio_mes = pd.Timestamp(data.year, data.month, 1)
            chave_mes  = (data.year, data.month)

            # Acumula do início do mês até a data atual (inclui fins de semana nos dados)
            df_intervalo = df[
                (df['DATA'] >= inicio_mes) &
                (df['DATA'] <= data)
            ].copy()

            # Maior ATRASO por CPF DENTRO de cada canal (df já ordenado desc)
            df_unique = df_intervalo.drop_duplicates(subset=['CPF', 'CANAL'], keep='first')

            cal = df.loc[df['DATA'] == data, colunas_calendario].iloc[0].to_dict()

            agrupado = df_unique.groupby(['CANAL']).agg(
                qte=('CPF', 'nunique'),
                VALORPRIN_FIN=('VALOR', 'sum')
            ).reset_index()

            for col, val in cal.items():
                agrupado[col] = val
            agrupado['DATA'] = data

            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

            if i < len(datas_calendario):
                proxima = datas_calendario[i]
                if proxima.month != data.month or proxima.year != data.year:
                    ultimo_valor_por_mes.pop(chave_mes, None)

        df_final = pd.concat(resultados, ignore_index=True)

        # Labels fixos para segmentacoes
        for col in segmentacoes:
            df_final[col] = 'Unique'

        # CANAL vira Indicador
        df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        # Consolida após sobrescrita dos labels
        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
            qte=('qte', 'sum'),
            VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total qte (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALORPRIN_FIN total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALORPRIN_FIN'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def massivos_esforco_funil(df_massivos, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal do total de acionamentos por canal + segmentacoes (sem deduplicação).

    - Indicador recebe o valor do CANAL.
    - Se um CPF apareceu 10 vezes no canal no período, todas as 10 ocorrências são contadas.
    - Todas as colunas de segmentacoes recebem label 'Esforço' no output.
    - Apenas dias úteis (nr_dia_util > 0) geram registros no funil.
      Dados de fins de semana são incorporados no próximo dia útil.

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MASSIVOS

    Returns:
        pd.DataFrame: segmentacoes = 'Esforço', Indicador = CANAL.
    """
    @registrar_tempo("Funil Esforço - Massivos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_agrupamento = ['CANAL'] + segmentacoes
        colunas_calendario  = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        df = df_massivos.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])

        # Apenas dias úteis definem os pontos de corte do funil
        datas_calendario = sorted(
            df.loc[df['nr_dia_util'] > 0, 'DATA'].unique()
        )
        resultados = []
        ultimo_valor_por_mes = {}

        for i, data in enumerate(datas_calendario, 1):
            inicio_mes = pd.Timestamp(data.year, data.month, 1)
            chave_mes  = (data.year, data.month)

            # Acumula do início do mês até a data atual (inclui fins de semana nos dados)
            df_intervalo = df[
                (df['DATA'] >= inicio_mes) &
                (df['DATA'] <= data)
            ].copy()

            cal = df.loc[df['DATA'] == data, colunas_calendario].iloc[0].to_dict()

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

            if i < len(datas_calendario):
                proxima = datas_calendario[i]
                if proxima.month != data.month or proxima.year != data.year:
                    ultimo_valor_por_mes.pop(chave_mes, None)

        df_final = pd.concat(resultados, ignore_index=True)

        for col in segmentacoes:
            df_final[col] = 'Esforço'

        # CANAL vira Indicador
        df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
            qte=('qte', 'sum'),
            VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total qte (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALORPRIN_FIN total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALORPRIN_FIN'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def massivos_segmentacoes_daily(df_massivos, segmentacoes, arquivo_log=None):
    """
    CPFs únicos por dia por CANAL + segmentacoes.

    Função dinâmica — não assume nenhuma segmentação padrão.
    As segmentações são definidas pela carteira na chamada da função.

    Exemplos:
        Com faixa de atraso:  segmentacoes=['FX_ATRASO', 'FAIXA']
        Sem faixa de atraso:  segmentacoes=['FX_ATRASO']

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MASSIVOS

    Returns:
        pd.DataFrame: Contagem diária sem acumulado. Indicador = CANAL.
    """
    @registrar_tempo("Daily Segmentações - Massivos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_agrupamento = ['CANAL'] + segmentacoes
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
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def massivos_unique_daily(df_massivos, segmentacoes, arquivo_log=None):
    """
    CPFs únicos por dia e por canal (maior ATRASO por CPF dentro do mesmo dia e canal).

    - Todas as colunas de segmentacoes recebem label 'Unique' no output.

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MASSIVOS

    Returns:
        pd.DataFrame: segmentacoes = 'Unique', Indicador = CANAL.
    """
    @registrar_tempo("Daily Unique - Massivos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        df = df_massivos.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])
        # Ordena desc por ATRASO para drop_duplicates pegar o registro de maior atraso
        # por CPF dentro do canal
        df = df.sort_values('ATRASO', ascending=False)

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

            # Maior ATRASO por CPF dentro do canal (df já ordenado desc)
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

        for col in segmentacoes:
            df_final[col] = 'Unique'

        df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
            qte=('qte', 'sum'),
            VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def massivos_esforco_daily(df_massivos, segmentacoes, arquivo_log=None):
    """
    Total de acionamentos por dia e por canal (sem deduplicação).

    - Todas as colunas de segmentacoes recebem label 'Esforço' no output.

    Args:
        df_massivos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MASSIVOS

    Returns:
        pd.DataFrame: segmentacoes = 'Esforço', Indicador = CANAL.
    """
    @registrar_tempo("Daily Esforço - Massivos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

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
            colunas_agrupamento = ['CANAL'] + segmentacoes

            agrupado = df_dia.groupby(
                colunas_agrupamento + colunas_calendario, dropna=False
            ).agg(
                qte=('CPF', 'count'),
                VALORPRIN_FIN=('VALOR', 'sum')
            ).reset_index()

            agrupado['DATA'] = data
            resultados.append(agrupado)

        df_final = pd.concat(resultados, ignore_index=True)

        for col in segmentacoes:
            df_final[col] = 'Esforço'

        df_final = df_final.rename(columns={'CANAL': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
            qte=('qte', 'sum'),
            VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def processar_acumulados_massivos(
    df_massivos,
    segmentacoes,
    calcular_funil=True,
    calcular_daily=True,
    consolidado=True,
    arquivo_log=None
):
    """
    Orquestra a geração e união de métricas acumuladas de massivos por canal.

    O df_massivos deve conter as colunas:
        CPF, DATA, CANAL, VALOR,
        nr_dia_util, quartil, dt_mes, mes_abreviado
        + colunas em segmentacoes (ex: ['FX_ATRASO', 'FAIXA'])

    Depende da função unir_dataframes() disponível no escopo.

    O campo Indicador no output recebe o valor do CANAL (SMS, WHATS, EMAIL, RCS, etc.).
    Todos os cálculos (fxAtraso, unique, esforço) são realizados dentro de cada canal
    separadamente — um CPF pode ser contado em SMS e em WHATS de forma independente.

    Args:
        df_massivos (pd.DataFrame): DataFrame de massivos já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        calcular_funil (bool): Calcula acumulados mensais (funil). Default: True.
        calcular_daily (bool): Calcula granularidade diária. Default: True.
        consolidado (bool):
            True  → retorna df único com coluna TIPO = 'Funil' ou 'Daily'
            False → retorna (df_funil, df_daily) — None para os não calculados
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_MASSIVOS

    Returns:
        pd.DataFrame | tuple:
            consolidado=True  → pd.DataFrame com coluna TIPO
            consolidado=False → (df_funil, df_daily)

    Exemplo de uso:
        # Retorno consolidado (padrão)
        df_tudo = processar_acumulados_massivos(
            df_massivos=df,
            segmentacoes=['FX_ATRASO', 'FAIXA'],
            arquivo_log=LOG_MASSIVOS
        )

        # Retorno separado
        df_funil, df_daily = processar_acumulados_massivos(
            df_massivos=df,
            segmentacoes=['FX_ATRASO'],
            consolidado=False,
            arquivo_log=LOG_MASSIVOS
        )

        # Apenas funil consolidado
        df_funil = processar_acumulados_massivos(
            df_massivos=df,
            segmentacoes=['FX_ATRASO'],
            calcular_daily=False,
            arquivo_log=LOG_MASSIVOS
        )
    """
    if not calcular_funil and not calcular_daily:
        raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True.")

    # ---- FUNIL ---------------------------------------------------------------
    df_funil = None
    if calcular_funil:
        df_fxAtraso = massivos_segmentacoes_funil(df_massivos, segmentacoes, arquivo_log)
        df_unique   = massivos_unique_funil(df_massivos, segmentacoes, arquivo_log)
        df_esforco  = massivos_esforco_funil(df_massivos, segmentacoes, arquivo_log)
        df_funil    = unir_dataframes(df_fxAtraso, df_unique, df_esforco)

    # ---- DAILY ---------------------------------------------------------------
    df_daily = None
    if calcular_daily:
        df_fxAtraso_d = massivos_segmentacoes_daily(df_massivos, segmentacoes, arquivo_log)
        df_unique_d   = massivos_unique_daily(df_massivos, segmentacoes, arquivo_log)
        df_esforco_d  = massivos_esforco_daily(df_massivos, segmentacoes, arquivo_log)
        df_daily      = unir_dataframes(df_fxAtraso_d, df_unique_d, df_esforco_d)

    # ---- RETORNO -------------------------------------------------------------
    if consolidado:
        dfs = []
        if df_funil is not None:
            df_funil['TIPO'] = 'Funil'
            dfs.append(df_funil)
        if df_daily is not None:
            df_daily['TIPO'] = 'Daily'
            dfs.append(df_daily)
        return unir_dataframes(*dfs)

    return df_funil, df_daily

__all__ = [
    'processar_acumulados_massivos',

    # Metricas
    'massivos_unique_funil',
    'massivos_fxAtraso_funil',
    'massivos_unique_funil',
    'massivos_fxAtraso_daily',
    'massivos_unique_daily',
    'massivos_esforco_daily'
]