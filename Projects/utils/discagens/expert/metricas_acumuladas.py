"""
Módulo de Métricas Acumuladas - Discagens Expert

Responsável por calcular:
- Acumulado mensal de acionamentos (Esforço)
- Acumulado mensal de acionamentos únicos por CPF (Unique)
- Acumulado mensal de acionamentos únicos por CPF + FX_ATRASO (fxAtraso_origem)
"""

from typing import List, Optional
import pandas as pd
from utils.utils import salvar_log, registrar_tempo
from ...config import LOG_DISCAGENS

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

@registrar_tempo("Funil ESFORÇO - Expert", arquivo_log=LOG_DISCAGENS)
def acionamentos_esforco_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos por FX_ATRASO e ORIGEM (sem deduplicação).
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais
    """
    df = df_com_fx_atraso.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
    
    salvar_log(f"📊 Processando ESFORÇO (todas as discagens) para {len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        agrupado = df_intervalo.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'TRABALHADO': g['TRABALHADO'].sum(),
            'VALORPRIN_FIN_TRABALHADO': g.loc[g['TRABALHADO'] == 1, 'VALOR'].sum(),
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALOR'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALOR'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALOR'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALOR'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        resultados.append(agrupado)
    
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Merge com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])
    
    # Preencher NaN com 0
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    
    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    df_final['FX_ATRASO'] = 'Esforço'
    return df_final

@registrar_tempo("Funil UNIQUE - Expert", arquivo_log=LOG_DISCAGENS)
def acionamentos_unique_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score).
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas
    """
    df = df_com_fx_atraso.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
    
    salvar_log(f"📊 Processando UNIQUE (melhor score por CPF) para {len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_DISCAGENS)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 4 +
            df_intervalo['CPCA'].astype(int) * 3 +
            df_intervalo['CPC'].astype(int) * 2 +
            df_intervalo['ACIONAMENTOS'].astype(int) * 1
        )
        
        # Ordenar e manter melhor score por CPF
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'TABULACAO_SCORE'],
            ascending=[True, False]
        )
        df_unique = df_intervalo.drop_duplicates(subset=['CPF'], keep='first').copy()
        
        # Agrupar por FX_ATRASO e ORIGEM
        agrupado = df_unique.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'TRABALHADO': g['TRABALHADO'].sum(),
            'VALORPRIN_FIN_TRABALHADO': g.loc[g['TRABALHADO'] == 1, 'VALOR'].sum(),
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALOR'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALOR'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALOR'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALOR'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        resultados.append(agrupado)
    
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Merge com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])
    
    # Preencher NaN com 0
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    
    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    df_final['FX_ATRASO'] = 'Unique'
    return df_final

def acionamentos_fxAtraso_dinamico(
    df_com_fx_atraso: pd.DataFrame,
    df_dw_calendario: pd.DataFrame,
    dimensoes_agrupamento: Optional[List[str]] = None,
    log_file: str = LOG_DISCAGENS
) -> pd.DataFrame:
    """
    Motor genérico de cálculo de métricas acumuladas por faixa de atraso.
    
    Args:
        df_com_fx_atraso: DataFrame com FX_ATRASO e métricas de tabulação
        df_dw_calendario: DataFrame com dados de calendário
        dimensoes_agrupamento: Lista de dimensões para agrupamento.
                               Default: ['FX_ATRASO']
                               Exemplos:
                               - ['FX_ATRASO'] → agregação simples
                               - ['FX_ATRASO', 'ORIGEM'] → expert com robô
                               - ['FX_ATRASO', 'CANAL'] → futura segmentação
        log_file: Arquivo de log
    
    Returns:
        DataFrame com contagens acumuladas mensais
        
    Note:
        Dimensões inexistentes no DataFrame são ignoradas automaticamente.
        A função se adapta ao modelo de dados disponível.
    """
    df = df_com_fx_atraso.copy()
    df['DATA'] = pd.to_datetime(df['DATA'])
    
    # ============================================
    # VALIDAÇÃO DE DIMENSÕES
    # ============================================
    if dimensoes_agrupamento is None:
        dimensoes_agrupamento = ['FX_ATRASO']
    
    # Manter apenas dimensões que existem no DataFrame
    dimensoes_validas = [
        dim for dim in dimensoes_agrupamento 
        if dim in df.columns
    ]
    
    if 'FX_ATRASO' not in dimensoes_validas:
        raise ValueError("FX_ATRASO é obrigatório e não foi encontrado no DataFrame")
    
    # Log das dimensões efetivamente usadas
    salvar_log(
        f"📊 Dimensões de agrupamento: {dimensoes_validas}",
        arquivo_log=log_file
    )
    
    # ============================================
    # PROCESSAMENTO ACUMULADO
    # ============================================
    datas_unicas = sorted(df['DATA'].unique())
    resultados = []
    
    salvar_log(
        f"📊 Processando acumulado (melhor score por CPF+FX_ATRASO) para {len(datas_unicas)} datas...",
        arquivo_log=log_file
    )
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=log_file)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 4 +
            df_intervalo['CPCA'].astype(int) * 3 +
            df_intervalo['CPC'].astype(int) * 2 +
            df_intervalo['ACIONAMENTOS'].astype(int) * 1
        )
        
        # Ordenar e manter melhor score por CPF + FX_ATRASO
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'FX_ATRASO', 'TABULACAO_SCORE'],
            ascending=[True, True, False]
        )
        df_unique = df_intervalo.drop_duplicates(
            subset=['CPF', 'FX_ATRASO'], 
            keep='first'
        ).copy()
        
        # ============================================
        # AGRUPAMENTO DINÂMICO
        # ============================================
        agrupado = df_unique.groupby(dimensoes_validas).apply(lambda g: pd.Series({
            'TRABALHADO': g['TRABALHADO'].sum(),
            'VALORPRIN_FIN_TRABALHADO': g.loc[g['TRABALHADO'] == 1, 'VALOR'].sum(),
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALOR'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALOR'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALOR'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALOR'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        resultados.append(agrupado)
    
    df_final = pd.concat(resultados, ignore_index=True)
    
    # ============================================
    # MERGE COM CALENDÁRIO
    # ============================================
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])
    
    # Preencher NaN com 0
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    
    # ============================================
    # REORDENAÇÃO DINÂMICA DE COLUNAS
    # ============================================
    colunas_ordenadas = (
        ['DATA'] +
        dimensoes_validas +
        [
            'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
            'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
            'CPC', 'VALORPRIN_FIN_CPC',
            'CPCA', 'VALORPRIN_FIN_CPCA',
            'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
            'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
        ]
    )
    
    df_final = df_final[colunas_ordenadas]
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=log_file)
    return df_final

@registrar_tempo("Funil UNIQUE - Discagens", arquivo_log=LOG_DISCAGENS)
def discagens_unique(df_discagens, df_dw_calendario, segmentacoes_extras=None):
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

            # Ordenar por FX_ATRASO decrescente — maior faixa primeiro
            # Ordem alfabética decrescente funciona com as labels definidas (0-9, Preventivo)
            df_intervalo = df_intervalo.sort_values(
                ['CPF', 'FX_ATRASO'],
                ascending=[True, False]
            )

            # Manter apenas o registro de maior faixa por CPF
            df_unique = df_intervalo.drop_duplicates(subset=['CPF'], keep='first').copy()

            # Agrupar por segmentações
            colunas_agrupamento = ['FX_ATRASO'] + segmentacoes_extras

            agrupado = df_unique.groupby(colunas_agrupamento).agg(
                CPFs=('CPF', 'nunique'),
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
                    agrupado_zero['CPFs'] = 0
                    agrupado_zero['VALOR'] = 0.0
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)

    # FX_ATRASO = 'Unique' após agrupamento
    df_final['FX_ATRASO'] = 'Unique'

    # Segmentações extras recebem 'Unique'
    for col in segmentacoes_extras:
        df_final[col] = 'Unique'

    salvar_log(f"\\n📅 Merge com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    colunas_ordenadas = (
        ['DATA', 'FX_ATRASO'] + segmentacoes_extras +
        ['CPFs', 'VALOR', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ CPFs únicos (última data): {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPFs'].sum():,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ VALOR total (última data): R$ {df_final[df_final['DATA'] == df_final['DATA'].max()]['VALOR'].sum():,.2f}", arquivo_log=LOG_DISCAGENS)
    salvar_log("=" * 80, arquivo_log=LOG_DISCAGENS)

    return df_final

__all__ = [
    'acionamentos_esforco_expert',
    'acionamentos_unique_expert',
    'acionamentos_fxAtraso_dinamico'
]
