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
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

def discagens_operacao_segmentacoes_funil(df_discagens, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal de CPFs únicos por OPERACAO + segmentacoes.
 
    Função dinâmica — não assume nenhuma segmentação padrão.
    As segmentações são definidas pela carteira na chamada da função.
 
    Exemplos:
        Cresol:  segmentacoes=['PF_PJ', 'PA']
        Renner:  segmentacoes=['FX_ATRASO', 'FAIXA']
        Ouze:    segmentacoes=['FX_ATRASO']
 
    - Indicador recebe o valor de OPERACAO.
    - Um CPF é contado uma única vez por combinação OPERACAO + segmentacoes.
 
    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
            Colunas obrigatórias: CPF, DATA, OPERACAO, VALOR,
                                  nr_dia_util, quartil, dt_mes, mes_abreviado
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS
 
    Returns:
        pd.DataFrame:
            DATA | Indicador | qte | [segmentacoes] |
            MesAbreviado | nr_dia_util | quartil | dt_mes | VALOR
    """
    @registrar_tempo("Funil Segmentações - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)
 
        colunas_agrupamento = ['OPERACAO'] + segmentacoes
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
 
                cal = df[df['DATA'] == data][colunas_calendario].iloc[0].to_dict()
 
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
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )
        df_final = df_final[colunas_ordenadas]
 
        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total CPFs (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)
 
        return df_final
 
    return _executar()
 
def discagens_operacao_unique_funil(df_discagens, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal de CPFs únicos por operação, sem segmentação detalhada.
 
    - Para cada CPF acumulado no mês, considera apenas um registro por OPERACAO
      (ordenação descendente pela primeira coluna de segmentacoes).
    - Todas as colunas de segmentacoes recebem label 'Unique' no output.
 
    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS
 
    Returns:
        pd.DataFrame: segmentacoes = 'Unique', Indicador = OPERACAO.
    """
    @registrar_tempo("Funil Unique - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)
 
        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
 
        df = df_discagens.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])
        df = df.sort_values(segmentacoes[0], ascending=False)
 
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
 
                df_unique = df_intervalo.drop_duplicates(subset=['CPF', 'OPERACAO'], keep='first')
 
                cal = df[df['DATA'] == data][colunas_calendario].iloc[0].to_dict()
 
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
 
        for col in segmentacoes:
            df_final[col] = 'Unique'
 
        df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})
 
        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)
 
        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
            qte=('qte', 'sum'),
            VALOR=('VALOR', 'sum')
        )
 
        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )
        df_final = df_final[colunas_ordenadas]
 
        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total CPFs (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)
 
        return df_final
 
    return _executar()
 
def discagens_operacao_esforco_funil(df_discagens, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal do total de discagens por operação (sem deduplicação).
 
    - Indicador recebe o valor de OPERACAO.
    - Todas as colunas de segmentacoes recebem label 'Esforço' no output.
 
    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS
 
    Returns:
        pd.DataFrame: segmentacoes = 'Esforço', Indicador = OPERACAO.
    """
    @registrar_tempo("Funil Esforço - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)
 
        colunas_agrupamento = ['OPERACAO'] + segmentacoes
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
 
                cal = df[df['DATA'] == data][colunas_calendario].iloc[0].to_dict()
 
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
 
        for col in segmentacoes:
            df_final[col] = 'Esforço'
 
        df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})
 
        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)
 
        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
            qte=('qte', 'sum'),
            VALOR=('VALOR', 'sum')
        )
 
        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )
        df_final = df_final[colunas_ordenadas]
 
        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total CPFs (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)
 
        return df_final
 
    return _executar()

def discagens_operacao_segmentacoes_daily(df_discagens, segmentacoes, arquivo_log=None):
    """
    CPFs únicos por dia por OPERACAO + segmentacoes.

    Função dinâmica — não assume nenhuma segmentação padrão.

    Exemplos:
        Cresol:  segmentacoes=['PF_PJ', 'PA']
        Renner:  segmentacoes=['FX_ATRASO', 'FAIXA']
        Ouze:    segmentacoes=['FX_ATRASO']

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        pd.DataFrame: Contagem diária sem acumulado. Indicador = OPERACAO.
    """
    @registrar_tempo("Daily Segmentações - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_agrupamento = ['OPERACAO'] + segmentacoes
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
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final[colunas_ordenadas]

    return _executar()

def discagens_operacao_unique_daily(df_discagens, segmentacoes, arquivo_log=None):
    """
    CPFs únicos por dia e por operação.

    Para cada CPF, considera apenas um registro por OPERACAO no dia
    (ordenação descendente pela primeira coluna de segmentacoes).
    Todas as colunas de segmentacoes recebem label 'Unique' no output.

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        pd.DataFrame: segmentacoes = 'Unique', Indicador = OPERACAO.
    """
    @registrar_tempo("Daily Unique - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        df = df_discagens.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])
        df = df.sort_values(segmentacoes[0], ascending=False)

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

        for col in segmentacoes:
            df_final[col] = 'Unique'

        df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
            qte=('qte', 'sum'),
            VALOR=('VALOR', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final[colunas_ordenadas]

    return _executar()

def discagens_operacao_esforco_daily(df_discagens, segmentacoes, arquivo_log=None):
    """
    Total de discagens por dia e por operação (sem deduplicação).

    Todas as colunas de segmentacoes recebem label 'Esforço' no output.

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        pd.DataFrame: segmentacoes = 'Esforço', Indicador = OPERACAO.
    """
    @registrar_tempo("Daily Esforço - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_agrupamento = ['OPERACAO'] + segmentacoes
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

            agrupado = df_dia.groupby(
                colunas_agrupamento + colunas_calendario, dropna=False
            ).agg(
                qte=('CPF', 'count'),
                VALOR=('VALOR', 'sum')
            ).reset_index()

            agrupado['DATA'] = data
            resultados.append(agrupado)

        df_final = pd.concat(resultados, ignore_index=True)

        for col in segmentacoes:
            df_final[col] = 'Esforço'

        df_final = df_final.rename(columns={'OPERACAO': 'Indicador', 'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
            qte=('qte', 'sum'),
            VALOR=('VALOR', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final[colunas_ordenadas]

    return _executar()

def discagens_segmentacoes_funil(df_discagens, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal de CPFs únicos por segmentacoes.

    Função dinâmica — não assume nenhuma segmentação padrão.
    As segmentações são definidas pela carteira na chamada da função.

    Exemplos:
        Renner:  segmentacoes=['FX_ATRASO', 'FAIXA']
        Ouze:    segmentacoes=['FX_ATRASO']

    - Indicador recebe o valor 'Trabalhado'.
    - Um CPF é contado uma única vez por combinação de segmentacoes.

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
            Colunas obrigatórias: CPF, DATA, VALOR,
                                  nr_dia_util, quartil, dt_mes, mes_abreviado
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        pd.DataFrame:
            DATA | Indicador | qte | [segmentacoes] |
            MesAbreviado | nr_dia_util | quartil | dt_mes | VALOR
    """
    @registrar_tempo("Funil Segmentações - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

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
                    subset=['CPF'] + segmentacoes,
                    keep='first'
                )

                cal = df[df['DATA'] == data][colunas_calendario].iloc[0].to_dict()

                agrupado = df_unico.groupby(segmentacoes).agg(
                    qte=('CPF', 'nunique'),
                    VALOR=('VALOR', 'sum')
                ).reset_index()

                for col, val in cal.items():
                    agrupado[col] = val
                agrupado['DATA']      = data
                agrupado['Indicador'] = 'Trabalhado'

                ultimo_valor_por_mes[chave_mes] = agrupado
                resultados.append(agrupado)

            else:
                if chave_mes in ultimo_valor_por_mes:
                    rep = ultimo_valor_por_mes[chave_mes].copy()
                    rep['DATA'] = data
                    resultados.append(rep)
                else:
                    combinacoes = df[segmentacoes].drop_duplicates().copy()
                    if len(combinacoes) > 0:
                        combinacoes['DATA']      = data
                        combinacoes['Indicador'] = 'Trabalhado'
                        combinacoes['qte']       = 0
                        combinacoes['VALOR']     = 0.0
                        for col in colunas_calendario:
                            combinacoes[col] = None
                        ultimo_valor_por_mes[chave_mes] = combinacoes
                        resultados.append(combinacoes)

            if i < len(datas_calendario):
                proxima = datas_calendario[i]
                if proxima.month != data.month or proxima.year != data.year:
                    ultimo_valor_por_mes.pop(chave_mes, None)

        df_final = pd.concat(resultados, ignore_index=True)
        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total CPFs (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def discagens_unique_funil(df_discagens, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal de CPFs únicos por maior valor da primeira segmentação.

    - Para cada CPF acumulado no mês, considera apenas um registro
      (ordenação descendente pela primeira coluna de segmentacoes).
    - Todas as colunas de segmentacoes recebem label 'Unique' no output.

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        pd.DataFrame: segmentacoes = 'Unique', Indicador = 'Trabalhado'.
    """
    @registrar_tempo("Funil Unique - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        df = df_discagens.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])
        df = df.sort_values(segmentacoes[0], ascending=False)

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

                df_unique = df_intervalo.drop_duplicates(subset=['CPF'], keep='first')

                cal = df[df['DATA'] == data][colunas_calendario].iloc[0].to_dict()

                agrupado = df_unique.groupby(segmentacoes).agg(
                    qte=('CPF', 'nunique'),
                    VALOR=('VALOR', 'sum')
                ).reset_index()

                for col, val in cal.items():
                    agrupado[col] = val
                agrupado['DATA']      = data
                agrupado['Indicador'] = 'Trabalhado'

                ultimo_valor_por_mes[chave_mes] = agrupado
                resultados.append(agrupado)

            else:
                if chave_mes in ultimo_valor_por_mes:
                    rep = ultimo_valor_por_mes[chave_mes].copy()
                    rep['DATA'] = data
                    resultados.append(rep)
                else:
                    combinacoes = df[segmentacoes].drop_duplicates().copy()
                    if len(combinacoes) > 0:
                        combinacoes['DATA']      = data
                        combinacoes['Indicador'] = 'Trabalhado'
                        combinacoes['qte']       = 0
                        combinacoes['VALOR']     = 0.0
                        for col in colunas_calendario:
                            combinacoes[col] = None
                        ultimo_valor_por_mes[chave_mes] = combinacoes
                        resultados.append(combinacoes)

            if i < len(datas_calendario):
                proxima = datas_calendario[i]
                if proxima.month != data.month or proxima.year != data.year:
                    ultimo_valor_por_mes.pop(chave_mes, None)

        df_final = pd.concat(resultados, ignore_index=True)

        for col in segmentacoes:
            df_final[col] = 'Unique'

        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
            qte=('qte', 'sum'),
            VALOR=('VALOR', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total CPFs (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def discagens_esforco_funil(df_discagens, segmentacoes, arquivo_log=None):
    """
    Contagem acumulada mensal do total de discagens por segmentacoes (sem deduplicação).

    - Todas as colunas de segmentacoes recebem label 'Esforço' no output.

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        pd.DataFrame: segmentacoes = 'Esforço', Indicador = 'Trabalhado'.
    """
    @registrar_tempo("Funil Esforço - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

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

                cal = df[df['DATA'] == data][colunas_calendario].iloc[0].to_dict()

                agrupado = df_intervalo.groupby(segmentacoes).agg(
                    qte=('CPF', 'count'),
                    VALOR=('VALOR', 'sum')
                ).reset_index()

                for col, val in cal.items():
                    agrupado[col] = val
                agrupado['DATA']      = data
                agrupado['Indicador'] = 'Trabalhado'

                ultimo_valor_por_mes[chave_mes] = agrupado
                resultados.append(agrupado)

            else:
                if chave_mes in ultimo_valor_por_mes:
                    rep = ultimo_valor_por_mes[chave_mes].copy()
                    rep['DATA'] = data
                    resultados.append(rep)
                else:
                    combinacoes = df[segmentacoes].drop_duplicates().copy()
                    if len(combinacoes) > 0:
                        combinacoes['DATA']      = data
                        combinacoes['Indicador'] = 'Trabalhado'
                        combinacoes['qte']       = 0
                        combinacoes['VALOR']     = 0.0
                        for col in colunas_calendario:
                            combinacoes[col] = None
                        ultimo_valor_por_mes[chave_mes] = combinacoes
                        resultados.append(combinacoes)

            if i < len(datas_calendario):
                proxima = datas_calendario[i]
                if proxima.month != data.month or proxima.year != data.year:
                    ultimo_valor_por_mes.pop(chave_mes, None)

        df_final = pd.concat(resultados, ignore_index=True)

        for col in segmentacoes:
            df_final[col] = 'Esforço'

        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
            qte=('qte', 'sum'),
            VALOR=('VALOR', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )
        df_final = df_final[colunas_ordenadas]

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Total discagens (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['qte'].sum():,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final

    return _executar()

def discagens_segmentacoes_daily(df_discagens, segmentacoes, arquivo_log=None):
    """
    CPFs únicos por dia por segmentacoes.

    Função dinâmica — não assume nenhuma segmentação padrão.

    Exemplos:
        Renner:  segmentacoes=['FX_ATRASO', 'FAIXA']
        Ouze:    segmentacoes=['FX_ATRASO']

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        pd.DataFrame: Contagem diária sem acumulado. Indicador = 'Trabalhado'.
    """
    @registrar_tempo("Daily Segmentações - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

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
                subset=['CPF'] + segmentacoes,
                keep='first'
            )

            agrupado = df_unico.groupby(
                segmentacoes + colunas_calendario, dropna=False
            ).agg(
                qte=('CPF', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()

            agrupado['DATA']      = data
            agrupado['Indicador'] = 'Trabalhado'
            resultados.append(agrupado)

        df_final = pd.concat(resultados, ignore_index=True)
        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final[colunas_ordenadas]

    return _executar()

def discagens_unique_daily(df_discagens, segmentacoes, arquivo_log=None):
    """
    CPFs únicos por dia por maior valor da primeira segmentação.

    Para cada CPF, considera apenas um registro no dia
    (ordenação descendente pela primeira coluna de segmentacoes).
    Todas as colunas de segmentacoes recebem label 'Unique' no output.

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        pd.DataFrame: segmentacoes = 'Unique', Indicador = 'Trabalhado'.
    """
    @registrar_tempo("Daily Unique - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

        df = df_discagens.copy()
        df['DATA'] = pd.to_datetime(df['DATA'])
        df = df.sort_values(segmentacoes[0], ascending=False)

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

            df_unique = df_dia.drop_duplicates(subset=['CPF'], keep='first')

            agrupado = df_unique.groupby(
                segmentacoes + colunas_calendario, dropna=False
            ).agg(
                qte=('CPF', 'nunique'),
                VALOR=('VALOR', 'sum')
            ).reset_index()

            agrupado['DATA']      = data
            agrupado['Indicador'] = 'Trabalhado'
            resultados.append(agrupado)

        df_final = pd.concat(resultados, ignore_index=True)

        for col in segmentacoes:
            df_final[col] = 'Unique'

        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
            qte=('qte', 'sum'),
            VALOR=('VALOR', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final[colunas_ordenadas]

    return _executar()

def discagens_esforco_daily(df_discagens, segmentacoes, arquivo_log=None):
    """
    Total de discagens por dia por segmentacoes (sem deduplicação).

    Todas as colunas de segmentacoes recebem label 'Esforço' no output.

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        pd.DataFrame: segmentacoes = 'Esforço', Indicador = 'Trabalhado'.
    """
    @registrar_tempo("Daily Esforço - Discagens", arquivo_log=arquivo_log)
    def _executar():
        warnings.filterwarnings("ignore", category=FutureWarning)

        colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']

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

            agrupado = df_dia.groupby(
                segmentacoes + colunas_calendario, dropna=False
            ).agg(
                qte=('CPF', 'count'),
                VALOR=('VALOR', 'sum')
            ).reset_index()

            agrupado['DATA']      = data
            agrupado['Indicador'] = 'Trabalhado'
            resultados.append(agrupado)

        df_final = pd.concat(resultados, ignore_index=True)

        for col in segmentacoes:
            df_final[col] = 'Esforço'

        df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

        colunas_num = df_final.select_dtypes(include=['number']).columns
        df_final[colunas_num] = df_final[colunas_num].fillna(0)

        colunas_grupo = (
            ['DATA', 'Indicador'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        )
        df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
            qte=('qte', 'sum'),
            VALOR=('VALOR', 'sum')
        )

        colunas_ordenadas = (
            ['DATA', 'Indicador', 'qte'] + segmentacoes +
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        )

        salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_final[colunas_ordenadas]

    return _executar()

def processar_acumulados_discagens(
    df_discagens,
    segmentacoes,
    calcular_funil=True,
    calcular_daily=True,
    retorno=None,
    arquivo_log=None
):
    """
    Orquestra a geração e união de métricas acumuladas de discagens.

    Args:
        df_discagens (pd.DataFrame): DataFrame de discagens já com colunas de calendário.
        segmentacoes (list): Colunas de segmentação. Ex: ['FX_ATRASO'] ou ['FX_ATRASO', 'FAIXA']
        calcular_funil (bool): Se True, calcula os acumulados de funil. Default: True
        calcular_daily (bool): Se True, calcula os acumulados diários. Default: True
        retorno (str): 'separado' → retorna (df_funil, df_daily)
                       'consolidado' → retorna um único df com coluna 'TIPO' identificando funil/daily
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        Se retorno='separado':
            tuple: (df_funil, df_daily) — None para os não calculados
        Se retorno='consolidado':
            pd.DataFrame: DataFrame único com coluna TIPO = 'Funil' ou 'Daily'
    """
    @registrar_tempo("Pipeline acumulados discagens", arquivo_log=arquivo_log)
    def _executar():
        if not calcular_funil and not calcular_daily:
            raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True")

        # ============================================
        # ETAPA 1: FUNIL
        # ============================================
        df_funil = None
        if calcular_funil:
            df_fxAtraso = discagens_segmentacoes_funil(df_discagens, segmentacoes, arquivo_log=arquivo_log)
            df_unique   = discagens_unique_funil(df_discagens, segmentacoes, arquivo_log=arquivo_log)
            df_esforco  = discagens_esforco_funil(df_discagens, segmentacoes, arquivo_log=arquivo_log)
            df_funil    = unir_dataframes(df_fxAtraso, df_unique, df_esforco)

        # ============================================
        # ETAPA 2: DAILY
        # ============================================
        df_daily = None
        if calcular_daily:
            df_fxAtraso_d = discagens_segmentacoes_daily(df_discagens, segmentacoes, arquivo_log=arquivo_log)
            df_unique_d   = discagens_unique_daily(df_discagens, segmentacoes, arquivo_log=arquivo_log)
            df_esforco_d  = discagens_esforco_daily(df_discagens, segmentacoes, arquivo_log=arquivo_log)
            df_daily      = unir_dataframes(df_fxAtraso_d, df_unique_d, df_esforco_d)

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

def processar_acumulados_discagens_operacao(
    df_discagens,
    segmentacoes_extras=None,
    calcular_funil=True,
    calcular_daily=True,
    retorno=None,
    arquivo_log=None          
):
    """
    Orquestra a geração e união de métricas acumuladas de discagens por operação.
    ...
    Args:
        ...
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS
    """
    if not calcular_funil and not calcular_daily:
        raise ValueError("Ao menos um dos parâmetros calcular_funil ou calcular_daily deve ser True.")

    # ---- FUNIL ---------------------------------------------------------------
    df_funil = None
    if calcular_funil:
        df_fxAtraso = discagens_operacao_segmentacoes_funil(df_discagens, segmentacoes_extras, arquivo_log=arquivo_log)
        df_unique   = discagens_operacao_unique_funil(df_discagens, segmentacoes_extras, arquivo_log=arquivo_log)
        df_esforco  = discagens_operacao_esforco_funil(df_discagens, segmentacoes_extras, arquivo_log=arquivo_log)
        df_funil    = unir_dataframes(df_fxAtraso, df_unique, df_esforco)

    # ---- DAILY ---------------------------------------------------------------
    df_daily = None
    if calcular_daily:
        df_fxAtraso_d = discagens_operacao_segmentacoes_daily(df_discagens, segmentacoes_extras, arquivo_log=arquivo_log)
        df_unique_d   = discagens_operacao_unique_daily(df_discagens, segmentacoes_extras, arquivo_log=arquivo_log)
        df_esforco_d  = discagens_operacao_esforco_daily(df_discagens, segmentacoes_extras, arquivo_log=arquivo_log)
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
    segmentacoes_extras=None,
    calcular_funil=True,
    calcular_daily=True,
    retorno=None,
    arquivo_log=None 
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
        segmentacoes=segmentacoes_extras,
        calcular_funil=calcular_funil,
        calcular_daily=calcular_daily,
        retorno=retorno,
        arquivo_log=arquivo_log
    )

    # ---- VISÃO 2: POR OPERAÇÃO -----------------------------------------------
    df_operacao = processar_acumulados_discagens_operacao(
        df_discagens=df_discagens,
        segmentacoes_extras=segmentacoes_extras,
        calcular_funil=calcular_funil,
        calcular_daily=calcular_daily,
        retorno=retorno,
        arquivo_log=arquivo_log
    )

    # ---- RETORNO -------------------------------------------------------------
    if retorno == 'consolidado':
        return unir_dataframes(df_trabalhado, df_operacao)
 
    # retorno != 'consolidado': cada função retornou (df_funil, df_daily)
    df_trabalhado_funil, df_trabalhado_daily = df_trabalhado
    df_operacao_funil,   df_operacao_daily   = df_operacao
 
    return df_trabalhado_funil, df_trabalhado_daily, df_operacao_funil, df_operacao_daily

__all__ = [
    'discagens_segmentacoes_funil',
    'discagens_esforco_funil',
    'discagens_unique_funil',

    'discagens_segmentacoes_daily',
    'discagens_unique_daily',
    'discagens_esforco_daily',
    
    'discagens_operacao_segmentacoes_funil',
    'discagens_operacao_unique_funil',
    'discagens_operacao_esforco_funil',

    'discagens_operacao_segmentacoes_daily',
    'discagens_operacao_unique_daily',
    'discagens_operacao_esforco_daily',

    'processar_acumulados_discagens',
    'processar_acumulados_discagens_operacao',
    'processar_acumulados_discagens_completo'
]
