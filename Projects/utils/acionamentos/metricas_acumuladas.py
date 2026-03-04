"""
Módulo de Métricas Acumuladas de Acionamentos
Contém funções para gerar métricas acumuladas (mensais) de acionamentos.
"""

import pandas as pd
from functools import reduce
from utils.utils import registrar_tempo, salvar_log, transformar_funil_formato_long, unir_dataframes
from ..config import LOG_ACIONAMENTOS

@registrar_tempo("Funil de acionamentos fxAtraso", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_fxAtraso(df_acionamentos_enriquecido_limpo, df_dw_calendario, segmentacoes_extras=None):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF + FX_ATRASO (melhor score).
    Garante que dias sem dados novos repliquem os valores do dia anterior.
    
    Args:
        df_acionamentos_enriquecido_limpo (pd.DataFrame): DataFrame enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas adicionais para segmentação.
                                              Ex: ['FAIXA'] para a carteira Renner
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    colunas_segmentacao = ['FX_ATRASO'] + segmentacoes_extras

    df = df_acionamentos_enriquecido_limpo.copy()
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])

    data_min = df['DATA_ACIONA'].min()
    data_max = df['DATA_ACIONA'].max()

    df_calendario_periodo = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= data_min) &
        (df_dw_calendario_temp['dt_data'] <= data_max)
    ].sort_values('dt_data').copy()

    datas_calendario = df_calendario_periodo['dt_data'].tolist()
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando acumulado mensal por {' + '.join(colunas_segmentacao)} para {len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)

        tem_dados = (df['DATA_ACIONA'] == data).any()

        if tem_dados:
            df_intervalo = df[(df['DATA_ACIONA'] >= inicio_mes) & (df['DATA_ACIONA'] <= data)].copy()

            df_intervalo['TABULACAO_SCORE'] = (
                df_intervalo['PROMESSA'].astype(int) * 3 +
                df_intervalo['CPCA'].astype(int) * 2 +
                df_intervalo['CPC'].astype(int) * 1
            )

            df_intervalo = df_intervalo.sort_values(
                ['CPF_DEV'] + colunas_segmentacao + ['TABULACAO_SCORE'],
                ascending=[True] * (len(colunas_segmentacao) + 1) + [False]
            )

            df_filtrado = df_intervalo.drop_duplicates(
                subset=['CPF_DEV'] + colunas_segmentacao,
                keep='first'
            ).copy()

            agrupado = df_filtrado.groupby(colunas_segmentacao).apply(lambda g: pd.Series({
                'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
                'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
                'CPC': g['CPC'].sum(),
                'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
                'CPCA': g['CPCA'].sum(),
                'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
                'PROMESSA': g['PROMESSA'].sum(),
                'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
            }), include_groups=False).reset_index()

            agrupado['DATA_ACIONA'] = data
            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA_ACIONA'] = data
                resultados.append(agrupado_replicado)
            else:
                combinacoes = df[colunas_segmentacao].drop_duplicates()
                if len(combinacoes) > 0:
                    agrupado_zero = combinacoes.copy()
                    agrupado_zero['DATA_ACIONA'] = data
                    agrupado_zero['ACIONAMENTOS'] = 0
                    agrupado_zero['VALORPRIN_FIN_ACIONAMENTOS'] = 0.0
                    agrupado_zero['CPC'] = 0
                    agrupado_zero['VALORPRIN_FIN_CPC'] = 0.0
                    agrupado_zero['CPCA'] = 0
                    agrupado_zero['VALORPRIN_FIN_CPCA'] = 0.0
                    agrupado_zero['PROMESSA'] = 0
                    agrupado_zero['VALORPRIN_FIN_PROMESSA'] = 0.0
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)
    df_final['TRABALHADO'] = 0
    df_final['VALORPRIN_FIN_TRABALHADO'] = 0.0

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_ACIONAMENTOS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    colunas_ordenadas = (
        ['DATA_ACIONA'] + colunas_segmentacao +
        ['TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
         'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
         'CPC', 'VALORPRIN_FIN_CPC',
         'CPCA', 'VALORPRIN_FIN_CPCA',
         'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
         'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"\n📈 Totais acumulados por {' + '.join(colunas_segmentacao)} (última data):", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['ACIONAMENTOS'].sum():,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPC'].sum():,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPCA'].sum():,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['PROMESSA'].sum():,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_final


@registrar_tempo("Funil de acionamentos unique", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_unique(df_acionamentos_enriquecido_limpo, df_dw_calendario, segmentacoes_extras=None):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score).
    Colunas de segmentação extras recebem valor 'Unique' no resultado.
    
    Args:
        df_acionamentos_enriquecido_limpo (pd.DataFrame): DataFrame enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas adicionais para segmentação.
                                              Ex: ['FAIXA'] para a carteira Renner
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []

    df = df_acionamentos_enriquecido_limpo.copy()
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])

    data_min = df['DATA_ACIONA'].min()
    data_max = df['DATA_ACIONA'].max()

    df_calendario_periodo = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= data_min) &
        (df_dw_calendario_temp['dt_data'] <= data_max)
    ].sort_values('dt_data').copy()

    datas_calendario = df_calendario_periodo['dt_data'].tolist()
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando acumulado mensal ÚNICO para {len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)

        tem_dados = (df['DATA_ACIONA'] == data).any()

        if tem_dados:
            df_intervalo = df[(df['DATA_ACIONA'] >= inicio_mes) & (df['DATA_ACIONA'] <= data)].copy()

            df_intervalo['TABULACAO_SCORE'] = (
                df_intervalo['PROMESSA'].astype(int) * 3 +
                df_intervalo['CPCA'].astype(int) * 2 +
                df_intervalo['CPC'].astype(int) * 1
            )

            df_intervalo = df_intervalo.sort_values(
                ['CPF_DEV', 'TABULACAO_SCORE'],
                ascending=[True, False]
            )

            df_unique = df_intervalo.drop_duplicates(subset=['CPF_DEV'], keep='first').copy()

            agrupado = df_unique.groupby(['FX_ATRASO']).apply(lambda g: pd.Series({
                'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
                'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
                'CPC': g['CPC'].sum(),
                'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
                'CPCA': g['CPCA'].sum(),
                'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
                'PROMESSA': g['PROMESSA'].sum(),
                'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
            }), include_groups=False).reset_index()

            # Segmentações extras recebem 'Unique'
            for col in segmentacoes_extras:
                agrupado[col] = 'Unique'

            agrupado['DATA_ACIONA'] = data
            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA_ACIONA'] = data
                resultados.append(agrupado_replicado)
            else:
                combinacoes = df[['FX_ATRASO']].drop_duplicates()
                if len(combinacoes) > 0:
                    agrupado_zero = combinacoes.copy()
                    for col in segmentacoes_extras:
                        agrupado_zero[col] = 'Unique'
                    agrupado_zero['DATA_ACIONA'] = data
                    agrupado_zero['ACIONAMENTOS'] = 0
                    agrupado_zero['VALORPRIN_FIN_ACIONAMENTOS'] = 0.0
                    agrupado_zero['CPC'] = 0
                    agrupado_zero['VALORPRIN_FIN_CPC'] = 0.0
                    agrupado_zero['CPCA'] = 0
                    agrupado_zero['VALORPRIN_FIN_CPCA'] = 0.0
                    agrupado_zero['PROMESSA'] = 0
                    agrupado_zero['VALORPRIN_FIN_PROMESSA'] = 0.0
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)
    df_final['FX_ATRASO'] = 'Unique'
    df_final['TRABALHADO'] = 0
    df_final['VALORPRIN_FIN_TRABALHADO'] = 0.0

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_ACIONAMENTOS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    colunas_ordenadas = (
        ['DATA_ACIONA', 'FX_ATRASO'] + segmentacoes_extras +
        ['TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
         'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
         'CPC', 'VALORPRIN_FIN_CPC',
         'CPCA', 'VALORPRIN_FIN_CPCA',
         'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
         'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_final


@registrar_tempo("Funil de acionamentos esforço", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_esforco(df_acionamentos_enriquecido_limpo, df_dw_calendario, segmentacoes_extras=None):
    """
    Gera contagem acumulada mensal de acionamentos por FX_ATRASO.
    Colunas de segmentação extras recebem valor 'Esforço' no resultado.
    
    Args:
        df_acionamentos_enriquecido_limpo (pd.DataFrame): DataFrame enriquecido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas adicionais para segmentação.
                                              Ex: ['FAIXA'] para a carteira Renner
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais de esforço
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []

    df = df_acionamentos_enriquecido_limpo.copy()
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])

    data_min = df['DATA_ACIONA'].min()
    data_max = df['DATA_ACIONA'].max()

    df_calendario_periodo = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= data_min) &
        (df_dw_calendario_temp['dt_data'] <= data_max)
    ].sort_values('dt_data').copy()

    datas_calendario = df_calendario_periodo['dt_data'].tolist()
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando acumulado mensal ESFORÇO para {len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)

        tem_dados = (df['DATA_ACIONA'] == data).any()

        if tem_dados:
            df_intervalo = df[(df['DATA_ACIONA'] >= inicio_mes) & (df['DATA_ACIONA'] <= data)].copy()

            agrupado = df_intervalo.groupby(['FX_ATRASO']).apply(lambda g: pd.Series({
                'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
                'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
                'CPC': g['CPC'].sum(),
                'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
                'CPCA': g['CPCA'].sum(),
                'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
                'PROMESSA': g['PROMESSA'].sum(),
                'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
            }), include_groups=False).reset_index()

            # Segmentações extras recebem 'Esforço'
            for col in segmentacoes_extras:
                agrupado[col] = 'Esforço'

            agrupado['DATA_ACIONA'] = data
            ultimo_valor_por_mes[chave_mes] = agrupado
            resultados.append(agrupado)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA_ACIONA'] = data
                resultados.append(agrupado_replicado)
            else:
                combinacoes = df[['FX_ATRASO']].drop_duplicates()
                if len(combinacoes) > 0:
                    agrupado_zero = combinacoes.copy()
                    for col in segmentacoes_extras:
                        agrupado_zero[col] = 'Esforço'
                    agrupado_zero['DATA_ACIONA'] = data
                    agrupado_zero['ACIONAMENTOS'] = 0
                    agrupado_zero['VALORPRIN_FIN_ACIONAMENTOS'] = 0.0
                    agrupado_zero['CPC'] = 0
                    agrupado_zero['VALORPRIN_FIN_CPC'] = 0.0
                    agrupado_zero['CPCA'] = 0
                    agrupado_zero['VALORPRIN_FIN_CPCA'] = 0.0
                    agrupado_zero['PROMESSA'] = 0
                    agrupado_zero['VALORPRIN_FIN_PROMESSA'] = 0.0
                    ultimo_valor_por_mes[chave_mes] = agrupado_zero
                    resultados.append(agrupado_zero)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)
    df_final['FX_ATRASO'] = 'Esforço'
    df_final['TRABALHADO'] = 0
    df_final['VALORPRIN_FIN_TRABALHADO'] = 0.0

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_ACIONAMENTOS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    colunas_ordenadas = (
        ['DATA_ACIONA', 'FX_ATRASO'] + segmentacoes_extras +
        ['TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
         'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
         'CPC', 'VALORPRIN_FIN_CPC',
         'CPCA', 'VALORPRIN_FIN_CPCA',
         'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
         'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_final

@registrar_tempo("Pipeline acumulados acionamentos", arquivo_log=LOG_ACIONAMENTOS)
def processar_acumulados_acionamentos(df_acionamentos_limpo, df_dw_calendario, segmentacoes_extras=None):
    """
    Orquestra a geração, transformação e união de métricas acumuladas de acionamentos.

    Args:
        df_acionamentos_limpo (pd.DataFrame): DataFrame de acionamentos enriquecido e limpo
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas adicionais para segmentação.
                                              Ex: ['FAIXA'] para a carteira Renner

    Returns:
        pd.DataFrame: DataFrame consolidado com todos os acumulados em formato long
    """
    # ============================================
    # ETAPA 1: ACUMULADOS
    # ============================================
    df_acumulado_fxAtraso = acionamentos_fxAtraso(
        df_acionamentos_enriquecido_limpo=df_acionamentos_limpo,
        df_dw_calendario=df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras
    )
    df_acumulado_unique = acionamentos_unique(
        df_acionamentos_enriquecido_limpo=df_acionamentos_limpo,
        df_dw_calendario=df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras
    )
    df_acumulado_esforco = acionamentos_esforco(
        df_acionamentos_enriquecido_limpo=df_acionamentos_limpo,
        df_dw_calendario=df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras
    )

    # ============================================
    # ETAPA 2: TRANSFORMAR PARA FORMATO LONG
    # ============================================
    df_fxAtraso_long = transformar_funil_formato_long(df_acumulado_fxAtraso)
    df_unique_long = transformar_funil_formato_long(df_acumulado_unique)
    df_esforco_long = transformar_funil_formato_long(df_acumulado_esforco)

    # ============================================
    # ETAPA 3: UNIÃO
    # ============================================
    return unir_dataframes(df_fxAtraso_long, df_unique_long, df_esforco_long)