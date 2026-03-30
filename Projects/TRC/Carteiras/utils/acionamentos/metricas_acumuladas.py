"""
Módulo de Métricas Acumuladas de Acionamentos
Contém funções para gerar métricas acumuladas (mensais) de acionamentos.
"""

import pandas as pd
from functools import reduce
from utils.utils import registrar_tempo, salvar_log, transformar_funil_formato_long, unir_dataframes, normalizar_tipos_df
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

def acionamentos_segmentacoes_funil(df_acionamentos, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal de acionamentos por segmentacoes.

    Função dinâmica — não assume nenhuma segmentação padrão.
    As segmentações são definidas pela carteira na chamada da função.

    Exemplos:
        Renner:  segmentacoes=['FX_ATRASO', 'FAIXA']
        Ouze:    segmentacoes=['FX_ATRASO']

    - Para cada CPF acumulado no mês, mantém o registro de maior TABULACAO_SCORE
      por combinação de segmentacoes.
    - Indicadores: Acionamentos, CPC, CPCA, Promessa.
    - Apenas dias úteis (nr_dia_util > 0) geram registros no funil.
      Dados de fins de semana são incorporados no próximo dia útil.

    Args:
        df_acionamentos (pd.DataFrame): DataFrame já com colunas de calendário.
            Colunas obrigatórias: CPF_DEV, DATA_ACIONA, VALORPRIN_FIN,
                                  ACIONAMENTOS, CPC, CPCA, PROMESSA,
                                  nr_dia_util, quartil, dt_mes, mes_abreviado
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        pd.DataFrame:
            DATA_ACIONA | Indicador | qte | [segmentacoes] |
            MesAbreviado | nr_dia_util | quartil | dt_mes | VALORPRIN_FIN
    """
    @registrar_tempo("Funil Segmentações - Acionamentos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        indicadores = [
            ('Acionamentos', 'ACIONAMENTOS'),
            ('CPC',          'CPC'),
            ('CPCA',         'CPCA'),
            ('Promessa',     'PROMESSA'),
        ]

        df = df_acionamentos.copy()
        df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

        # Apenas dias úteis definem os pontos de corte do funil
        datas_calendario = sorted(
            df.loc[df['nr_dia_util'] > 0, 'DATA_ACIONA'].unique()
        )
        resultados = []
        ultimo_valor_por_mes = {}

        for i, data in enumerate(datas_calendario, 1):
            inicio_mes = pd.Timestamp(data.year, data.month, 1)
            chave_mes  = (data.year, data.month)

            # Acumula do início do mês até a data atual (inclui fins de semana nos dados)
            df_intervalo = df[
                (df['DATA_ACIONA'] >= inicio_mes) &
                (df['DATA_ACIONA'] <= data)
            ].copy()

            df_intervalo['TABULACAO_SCORE'] = (
                df_intervalo['PROMESSA'].astype(int) * 3 +
                df_intervalo['CPCA'].astype(int)     * 2 +
                df_intervalo['CPC'].astype(int)      * 1
            )

            df_intervalo = df_intervalo.sort_values(
                ['CPF_DEV'] + segmentacoes + ['TABULACAO_SCORE'],
                ascending=[True] * (len(segmentacoes) + 1) + [False]
            )

            df_filtrado = df_intervalo.drop_duplicates(
                subset=['CPF_DEV'] + segmentacoes, keep='first'
            ).copy()

            cal = df.loc[df['DATA_ACIONA'] == data, colunas_calendario].iloc[0].to_dict()

            agrupados_data = []
            for indicador, col_flag in indicadores:
                df_flag = df_filtrado[df_filtrado[col_flag] == 1]

                agrupado = df_filtrado.groupby(segmentacoes).agg(
                    qte=(col_flag, 'sum')
                ).reset_index()

                agrupado_valor = (
                    df_flag.groupby(segmentacoes).agg(
                        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
                    ).reset_index()
                    if len(df_flag) > 0
                    else pd.DataFrame(columns=segmentacoes + ['VALORPRIN_FIN'])
                )

                agrupado = agrupado.merge(agrupado_valor, on=segmentacoes, how='left')
                agrupado['VALORPRIN_FIN'] = agrupado['VALORPRIN_FIN'].fillna(0)
                agrupado['Indicador']     = indicador
                agrupado['DATA_ACIONA']   = data
                for col, val in cal.items():
                    agrupado[col] = val
                agrupados_data.append(agrupado)

            agrupado_concat = pd.concat(agrupados_data, ignore_index=True)
            ultimo_valor_por_mes[chave_mes] = agrupado_concat
            resultados.append(agrupado_concat)

            if i < len(datas_calendario):
                proxima = datas_calendario[i]
                if proxima.month != data.month or proxima.year != data.year:
                    ultimo_valor_por_mes.pop(chave_mes, None)

        df_final = pd.concat(resultados, ignore_index=True)
        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_ordenadas = (
            ['DATA_ACIONA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total qte (última data): {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALORPRIN_FIN total (última data): R$ {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['VALORPRIN_FIN'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def acionamentos_unique_funil(df_acionamentos, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal de acionamentos por maior TABULACAO_SCORE por CPF.

    - Para cada CPF acumulado no mês, considera apenas um registro
      (maior TABULACAO_SCORE, sem segmentação detalhada).
    - Todas as colunas de segmentacoes recebem label 'Unique' no output.
    - Apenas dias úteis (nr_dia_util > 0) geram registros no funil.
      Dados de fins de semana são incorporados no próximo dia útil.

    Args:
        df_acionamentos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        pd.DataFrame: segmentacoes = 'Unique'.
    """
    @registrar_tempo("Funil Unique - Acionamentos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        indicadores = [
            ('Acionamentos', 'ACIONAMENTOS'),
            ('CPC',          'CPC'),
            ('CPCA',         'CPCA'),
            ('Promessa',     'PROMESSA'),
        ]

        df = df_acionamentos.copy()
        df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

        # Apenas dias úteis definem os pontos de corte do funil
        datas_calendario = sorted(
            df.loc[df['nr_dia_util'] > 0, 'DATA_ACIONA'].unique()
        )
        resultados = []
        ultimo_valor_por_mes = {}

        for i, data in enumerate(datas_calendario, 1):
            inicio_mes = pd.Timestamp(data.year, data.month, 1)
            chave_mes  = (data.year, data.month)

            # Acumula do início do mês até a data atual (inclui fins de semana nos dados)
            df_intervalo = df[
                (df['DATA_ACIONA'] >= inicio_mes) &
                (df['DATA_ACIONA'] <= data)
            ].copy()

            df_intervalo['TABULACAO_SCORE'] = (
                df_intervalo['PROMESSA'].astype(int) * 3 +
                df_intervalo['CPCA'].astype(int)     * 2 +
                df_intervalo['CPC'].astype(int)      * 1
            )

            df_intervalo = df_intervalo.sort_values(
                ['CPF_DEV', 'TABULACAO_SCORE'], ascending=[True, False]
            )

            df_unique = df_intervalo.drop_duplicates(subset=['CPF_DEV'], keep='first').copy()

            cal = df.loc[df['DATA_ACIONA'] == data, colunas_calendario].iloc[0].to_dict()

            agrupados_data = []
            for indicador, col_flag in indicadores:
                df_flag = df_unique[df_unique[col_flag] == 1]

                agrupado = df_unique.groupby(segmentacoes).agg(
                    qte=(col_flag, 'sum')
                ).reset_index()

                agrupado_valor = (
                    df_flag.groupby(segmentacoes).agg(
                        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
                    ).reset_index()
                    if len(df_flag) > 0
                    else pd.DataFrame(columns=segmentacoes + ['VALORPRIN_FIN'])
                )

                agrupado = agrupado.merge(agrupado_valor, on=segmentacoes, how='left')
                agrupado['VALORPRIN_FIN'] = agrupado['VALORPRIN_FIN'].fillna(0)
                for col in segmentacoes:
                    agrupado[col] = 'Unique'
                agrupado['Indicador']   = indicador
                agrupado['DATA_ACIONA'] = data
                for col, val in cal.items():
                    agrupado[col] = val
                agrupados_data.append(agrupado)

            agrupado_concat = pd.concat(agrupados_data, ignore_index=True)
            ultimo_valor_por_mes[chave_mes] = agrupado_concat
            resultados.append(agrupado_concat)

            if i < len(datas_calendario):
                proxima = datas_calendario[i]
                if proxima.month != data.month or proxima.year != data.year:
                    ultimo_valor_por_mes.pop(chave_mes, None)

        df_final = pd.concat(resultados, ignore_index=True)
        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA_ACIONA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
            qte=('qte', 'sum'),
            VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
        )

        colunas_ordenadas = (
            ['DATA_ACIONA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total qte (última data): {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALORPRIN_FIN total (última data): R$ {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['VALORPRIN_FIN'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def acionamentos_esforco_funil(df_acionamentos, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal de acionamentos sem deduplicação por CPF.

    - Todas as ocorrências do período são somadas, sem nenhum critério de unicidade.
    - Todas as colunas de segmentacoes recebem label 'Esforço' no output.
    - Apenas dias úteis (nr_dia_util > 0) geram registros no funil.
      Dados de fins de semana são incorporados no próximo dia útil.

    Args:
        df_acionamentos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        pd.DataFrame: segmentacoes = 'Esforço'.
    """
    @registrar_tempo("Funil Esforço - Acionamentos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        indicadores = [
            ('Acionamentos', 'ACIONAMENTOS'),
            ('CPC',          'CPC'),
            ('CPCA',         'CPCA'),
            ('Promessa',     'PROMESSA'),
        ]

        df = df_acionamentos.copy()
        df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

        # Apenas dias úteis definem os pontos de corte do funil
        datas_calendario = sorted(
            df.loc[df['nr_dia_util'] > 0, 'DATA_ACIONA'].unique()
        )
        resultados = []
        ultimo_valor_por_mes = {}

        for i, data in enumerate(datas_calendario, 1):
            inicio_mes = pd.Timestamp(data.year, data.month, 1)
            chave_mes  = (data.year, data.month)

            # Acumula do início do mês até a data atual (inclui fins de semana nos dados)
            df_intervalo = df[
                (df['DATA_ACIONA'] >= inicio_mes) &
                (df['DATA_ACIONA'] <= data)
            ].copy()

            cal = df.loc[df['DATA_ACIONA'] == data, colunas_calendario].iloc[0].to_dict()

            agrupados_data = []
            for indicador, col_flag in indicadores:
                df_flag = df_intervalo[df_intervalo[col_flag] == 1]

                agrupado = df_intervalo.groupby(segmentacoes).agg(
                    qte=(col_flag, 'sum')
                ).reset_index()

                agrupado_valor = (
                    df_flag.groupby(segmentacoes).agg(
                        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
                    ).reset_index()
                    if len(df_flag) > 0
                    else pd.DataFrame(columns=segmentacoes + ['VALORPRIN_FIN'])
                )

                agrupado = agrupado.merge(agrupado_valor, on=segmentacoes, how='left')
                agrupado['VALORPRIN_FIN'] = agrupado['VALORPRIN_FIN'].fillna(0)
                for col in segmentacoes:
                    agrupado[col] = 'Esforço'
                agrupado['Indicador']   = indicador
                agrupado['DATA_ACIONA'] = data
                for col, val in cal.items():
                    agrupado[col] = val
                agrupados_data.append(agrupado)

            agrupado_concat = pd.concat(agrupados_data, ignore_index=True)
            ultimo_valor_por_mes[chave_mes] = agrupado_concat
            resultados.append(agrupado_concat)

            if i < len(datas_calendario):
                proxima = datas_calendario[i]
                if proxima.month != data.month or proxima.year != data.year:
                    ultimo_valor_por_mes.pop(chave_mes, None)

        df_final = pd.concat(resultados, ignore_index=True)
        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA_ACIONA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
            qte=('qte', 'sum'),
            VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
        )

        colunas_ordenadas = (
            ['DATA_ACIONA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total qte (última data): {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALORPRIN_FIN total (última data): R$ {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['VALORPRIN_FIN'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def acionamentos_segmentacoes_daily(df_acionamentos, segmentacoes, arquivo_log=None):
    """
    Contagem diária de acionamentos por segmentacoes (sem acumulado).

    Função dinâmica — não assume nenhuma segmentação padrão.
    As segmentações são definidas pela carteira na chamada da função.

    Exemplos:
        Renner:  segmentacoes=['FX_ATRASO', 'FAIXA']
        Ouze:    segmentacoes=['FX_ATRASO']

    - Para cada CPF no dia, mantém o registro de maior TABULACAO_SCORE
      por combinação de segmentacoes.
    - Indicadores: Acionamentos, CPC, CPCA, Promessa.

    Args:
        df_acionamentos (pd.DataFrame): DataFrame já com colunas de calendário.
            Colunas obrigatórias: CPF_DEV, DATA_ACIONA, VALORPRIN_FIN,
                                  ACIONAMENTOS, CPC, CPCA, PROMESSA,
                                  nr_dia_util, quartil, dt_mes, mes_abreviado
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        pd.DataFrame:
            DATA_ACIONA | Indicador | qte | [segmentacoes] |
            MesAbreviado | nr_dia_util | quartil | dt_mes | VALORPRIN_FIN
    """
    @registrar_tempo("Daily Segmentações - Acionamentos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        indicadores = [
            ('Acionamentos', 'ACIONAMENTOS'),
            ('CPC',          'CPC'),
            ('CPCA',         'CPCA'),
            ('Promessa',     'PROMESSA'),
        ]

        df = df_acionamentos.copy()
        df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

        for col in colunas_calendario:
            if col not in df.columns:
                df[col] = None
        df['nr_dia_util']   = pd.to_numeric(df['nr_dia_util'], errors='coerce').fillna(0).astype(int)
        df['quartil']       = df['quartil'].fillna('N/A').astype(str)
        df['dt_mes']        = pd.to_numeric(df['dt_mes'], errors='coerce').fillna(0).astype(int)
        df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A').astype(str)

        datas_unicas = sorted(df['DATA_ACIONA'].unique())
        resultados = []

        for data in datas_unicas:
            df_dia = df[df['DATA_ACIONA'] == data].copy()

            df_dia['TABULACAO_SCORE'] = (
                df_dia['PROMESSA'].astype(int) * 3 +
                df_dia['CPCA'].astype(int)     * 2 +
                df_dia['CPC'].astype(int)      * 1
            )

            df_dia = df_dia.sort_values(
                ['CPF_DEV'] + segmentacoes + ['TABULACAO_SCORE'],
                ascending=[True] * (len(segmentacoes) + 1) + [False]
            )

            df_filtrado = df_dia.drop_duplicates(
                subset=['CPF_DEV'] + segmentacoes, keep='first'
            ).reset_index(drop=True)

            for indicador, col_flag in indicadores:
                df_flag = df_filtrado[df_filtrado[col_flag] == 1]

                agrupado = df_filtrado.groupby(
                    segmentacoes + colunas_calendario, dropna=False
                ).agg(
                    qte=(col_flag, 'sum')
                ).reset_index()

                agrupado_valor = (
                    df_flag.groupby(segmentacoes + colunas_calendario, dropna=False).agg(
                        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
                    ).reset_index()
                    if len(df_flag) > 0
                    else pd.DataFrame(columns=segmentacoes + colunas_calendario + ['VALORPRIN_FIN'])
                )

                agrupado = agrupado.merge(
                    agrupado_valor, on=segmentacoes + colunas_calendario, how='left'
                ).reset_index(drop=True)
                agrupado['VALORPRIN_FIN'] = agrupado['VALORPRIN_FIN'].fillna(0)
                agrupado['Indicador']     = indicador
                agrupado['DATA_ACIONA']   = data
                resultados.append(agrupado)

        df_final = pd.concat(resultados, ignore_index=True)
        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_ordenadas = (
            ['DATA_ACIONA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def acionamentos_unique_daily(df_acionamentos, segmentacoes, arquivo_log=None):
    """
    Contagem diária de acionamentos por maior TABULACAO_SCORE por CPF (sem acumulado).

    - Para cada CPF no dia, considera apenas um registro (maior TABULACAO_SCORE).
    - Todas as colunas de segmentacoes recebem label 'Unique' no output.

    Args:
        df_acionamentos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        pd.DataFrame: segmentacoes = 'Unique'.
    """
    @registrar_tempo("Daily Unique - Acionamentos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        indicadores = [
            ('Acionamentos', 'ACIONAMENTOS'),
            ('CPC',          'CPC'),
            ('CPCA',         'CPCA'),
            ('Promessa',     'PROMESSA'),
        ]

        df = df_acionamentos.copy()
        df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

        for col in colunas_calendario:
            if col not in df.columns:
                df[col] = None
        df['nr_dia_util']   = df['nr_dia_util'].fillna(0)
        df['quartil']       = df['quartil'].fillna(0)
        df['dt_mes']        = df['dt_mes'].fillna(pd.NaT)
        df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')

        datas_unicas = sorted(df['DATA_ACIONA'].unique())
        resultados = []

        for data in datas_unicas:
            df_dia = df[df['DATA_ACIONA'] == data].copy()

            df_dia['TABULACAO_SCORE'] = (
                df_dia['PROMESSA'].astype(int) * 3 +
                df_dia['CPCA'].astype(int)     * 2 +
                df_dia['CPC'].astype(int)      * 1
            )

            df_dia    = df_dia.sort_values(['CPF_DEV', 'TABULACAO_SCORE'], ascending=[True, False])
            df_unique = df_dia.drop_duplicates(subset=['CPF_DEV'], keep='first')

            for indicador, col_flag in indicadores:
                agrupado = df_unique.groupby(
                    segmentacoes + colunas_calendario, dropna=False
                ).apply(
                    lambda g, f=col_flag: pd.Series({
                        'qte':           g[f].sum(),
                        'VALORPRIN_FIN': g.loc[g[f] == 1, 'VALORPRIN_FIN'].sum()
                    }), include_groups=False
                ).reset_index()

                for col in segmentacoes:
                    agrupado[col] = 'Unique'

                agrupado['Indicador']   = indicador
                agrupado['DATA_ACIONA'] = data
                resultados.append(agrupado)

        df_final = pd.concat(resultados, ignore_index=True)

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_grupo = (
            ['DATA_ACIONA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
            qte=('qte', 'sum'),
            VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
        )

        colunas_ordenadas = (
            ['DATA_ACIONA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def acionamentos_esforco_daily(df_acionamentos, segmentacoes, arquivo_log=None):
    """
    Contagem diária de acionamentos sem deduplicação por CPF (sem acumulado).

    - Todas as ocorrências do dia são somadas, sem nenhum critério de unicidade.
    - Todas as colunas de segmentacoes recebem label 'Esforço' no output.

    Args:
        df_acionamentos (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        pd.DataFrame: segmentacoes = 'Esforço'.
    """
    @registrar_tempo("Daily Esforço - Acionamentos", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        indicadores = [
            ('Acionamentos', 'ACIONAMENTOS'),
            ('CPC',          'CPC'),
            ('CPCA',         'CPCA'),
            ('Promessa',     'PROMESSA'),
        ]

        df = df_acionamentos.copy()
        df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

        for col in colunas_calendario:
            if col not in df.columns:
                df[col] = None
        df['nr_dia_util']   = df['nr_dia_util'].fillna(0)
        df['quartil']       = df['quartil'].fillna(0)
        df['dt_mes']        = df['dt_mes'].fillna(pd.NaT)
        df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')

        datas_unicas = sorted(df['DATA_ACIONA'].unique())
        resultados = []

        for data in datas_unicas:
            df_dia = df[df['DATA_ACIONA'] == data].copy()

            for indicador, col_flag in indicadores:
                agrupado = df_dia.groupby(
                    segmentacoes + colunas_calendario, dropna=False
                ).apply(
                    lambda g, f=col_flag: pd.Series({
                        'qte':           g[f].sum(),
                        'VALORPRIN_FIN': g.loc[g[f] == 1, 'VALORPRIN_FIN'].sum()
                    }), include_groups=False
                ).reset_index()

                for col in segmentacoes:
                    agrupado[col] = 'Esforço'

                agrupado['Indicador']   = indicador
                agrupado['DATA_ACIONA'] = data
                resultados.append(agrupado)

        df_final = pd.concat(resultados, ignore_index=True)

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_grupo = (
            ['DATA_ACIONA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
            qte=('qte', 'sum'),
            VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
        )

        colunas_ordenadas = (
            ['DATA_ACIONA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def processar_acumulados_acionamentos(
    df_acionamentos,
    segmentacoes,
    calcular_funil=True,
    calcular_daily=True,
    retorno=None,
    arquivo_log=None
):
    """
    Orquestra a geração, transformação e união de métricas acumuladas de acionamentos.

    Args:
        df_acionamentos (pd.DataFrame): DataFrame de acionamentos já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        calcular_funil (bool): Se True, calcula os acumulados de funil. Default: True
        calcular_daily (bool): Se True, calcula os acumulados diários. Default: True
        retorno (str): 'separado' → retorna (df_funil, df_daily)
                       'consolidado' → retorna um único df com coluna 'TIPO' identificando funil/daily
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        Se retorno='separado':
            tuple: (df_funil, df_daily) — None para os não calculados
        Se retorno='consolidado':
            pd.DataFrame: DataFrame único com coluna TIPO = 'Funil' ou 'Daily'
    """
    @registrar_tempo("Pipeline acumulados acionamentos", arquivo_log=arquivo_log)
    def _executar():
        if not calcular_funil and not calcular_daily:
            raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True")

        # ============================================
        # ETAPA 1: FUNIL
        # ============================================
        df_funil = None
        if calcular_funil:
            df_funil = unir_dataframes(
                normalizar_tipos_df(acionamentos_segmentacoes_funil(df_acionamentos, segmentacoes, arquivo_log=arquivo_log)),
                normalizar_tipos_df(acionamentos_unique_funil(df_acionamentos, segmentacoes, arquivo_log=arquivo_log)),
                normalizar_tipos_df(acionamentos_esforco_funil(df_acionamentos, segmentacoes, arquivo_log=arquivo_log))
            )

        # ============================================
        # ETAPA 2: DAILY
        # ============================================
        df_daily = None
        if calcular_daily:
            df_daily = unir_dataframes(
                normalizar_tipos_df(acionamentos_segmentacoes_daily(df_acionamentos, segmentacoes, arquivo_log=arquivo_log)),
                normalizar_tipos_df(acionamentos_unique_daily(df_acionamentos, segmentacoes, arquivo_log=arquivo_log)),
                normalizar_tipos_df(acionamentos_esforco_daily(df_acionamentos, segmentacoes, arquivo_log=arquivo_log))
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

    return _executar()

__all__ = [
    'acionamentos_segmentacoes_funil',
    'acionamentos_unique_funil',
    'acionamentos_esforco_funil',

    'acionamentos_segmentacoes_daily',
    'acionamentos_unique_daily',
    'acionamentos_esforco_daily',

    'processar_acumulados_acionamentos'
]