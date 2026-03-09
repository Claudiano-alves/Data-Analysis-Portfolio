"""
Módulo de Métricas Acumuladas de Acionamentos
Contém funções para gerar métricas acumuladas (mensais) de acionamentos.
"""

import pandas as pd
from functools import reduce
from utils.utils import registrar_tempo, salvar_log, transformar_funil_formato_long, unir_dataframes, normalizar_tipos_df
from ..config import LOG_ACIONAMENTOS

@registrar_tempo("Funil de acionamentos fxAtraso", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_fxAtraso_funil(df_acionamentos_enriquecido_limpo, df_dw_calendario, segmentacoes_extras=None):
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
    ultimo_valor_por_mes = {}

    indicadores = [
        ('Acionamentos', 'ACIONAMENTOS'),
        ('CPC', 'CPC'),
        ('CPCA', 'CPCA'),
        ('Promessa', 'PROMESSA'),
    ]

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando acumulado mensal por {' + '.join(colunas_segmentacao)} para {len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

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
                subset=['CPF_DEV'] + colunas_segmentacao, keep='first'
            ).copy()

            agrupados_data = []
            for indicador, col_flag in indicadores:
                df_flag = df_filtrado[df_filtrado[col_flag] == 1]

                agrupado = df_filtrado.groupby(colunas_segmentacao).agg(
                    qte=(col_flag, 'sum')
                ).reset_index()

                agrupado_valor = df_flag.groupby(colunas_segmentacao).agg(
                    VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
                ).reset_index() if len(df_flag) > 0 else pd.DataFrame(columns=colunas_segmentacao + ['VALORPRIN_FIN'])

                agrupado = agrupado.merge(agrupado_valor, on=colunas_segmentacao, how='left')
                agrupado['VALORPRIN_FIN'] = agrupado['VALORPRIN_FIN'].fillna(0)
                agrupado['Indicador'] = indicador
                agrupado['DATA_ACIONA'] = data
                agrupados_data.append(agrupado)

            agrupado_concat = pd.concat(agrupados_data, ignore_index=True)
            ultimo_valor_por_mes[chave_mes] = agrupado_concat
            resultados.append(agrupado_concat)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA_ACIONA'] = data
                resultados.append(agrupado_replicado)
            else:
                combinacoes = df[colunas_segmentacao].drop_duplicates()
                if len(combinacoes) > 0:
                    agrupados_zero = []
                    for indicador, _ in indicadores:
                        agrupado_zero = combinacoes.copy()
                        agrupado_zero['DATA_ACIONA'] = data
                        agrupado_zero['Indicador'] = indicador
                        agrupado_zero['qte'] = 0
                        agrupado_zero['VALORPRIN_FIN'] = 0.0
                        agrupados_zero.append(agrupado_zero)
                    agrupado_concat = pd.concat(agrupados_zero, ignore_index=True)
                    ultimo_valor_por_mes[chave_mes] = agrupado_concat
                    resultados.append(agrupado_concat)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)
    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_ACIONAMENTOS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])

    for col in df_final.select_dtypes(include=['number']).columns:
        df_final[col] = df_final[col].fillna(0)
    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    df_final = pd.concat(resultados, ignore_index=True)

    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data']).reset_index(drop=True)

    df_final = df_final.copy().reset_index(drop=True)

    for col in df_final.select_dtypes(include=['number']).columns:
        df_final[col] = df_final[col].fillna(0)

    colunas_grupo = ['DATA_ACIONA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']

    # df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
    #     qte=('qte', 'sum'),
    #     VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    # )
    
    # df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
    #     qte=('qte', 'sum'),
    #     VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    # )

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_ordenadas = (
        ['DATA_ACIONA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    df_final = df_final[colunas_ordenadas]
    
    return df_final

@registrar_tempo("Funil de acionamentos unique", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_unique_funil(df_acionamentos_enriquecido_limpo, df_dw_calendario, segmentacoes_extras=None):
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
    ultimo_valor_por_mes = {}

    indicadores = [
        ('Acionamentos', 'ACIONAMENTOS'),
        ('CPC', 'CPC'),
        ('CPCA', 'CPCA'),
        ('Promessa', 'PROMESSA'),
    ]

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando acumulado mensal ÚNICO para {len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

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
                ['CPF_DEV', 'TABULACAO_SCORE'], ascending=[True, False]
            )

            df_unique = df_intervalo.drop_duplicates(subset=['CPF_DEV'], keep='first').copy()

            agrupados_data = []
            for indicador, col_flag in indicadores:
                df_flag = df_unique[df_unique[col_flag] == 1]

                agrupado = df_unique.groupby(['FX_ATRASO']).agg(
                    qte=(col_flag, 'sum')
                ).reset_index()

                agrupado_valor = df_flag.groupby(['FX_ATRASO']).agg(
                    VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
                ).reset_index() if len(df_flag) > 0 else pd.DataFrame(columns=['FX_ATRASO', 'VALORPRIN_FIN'])

                agrupado = agrupado.merge(agrupado_valor, on='FX_ATRASO', how='left')
                agrupado['VALORPRIN_FIN'] = agrupado['VALORPRIN_FIN'].fillna(0)
                agrupado['FX_ATRASO'] = 'Unique'
                for col in segmentacoes_extras:
                    agrupado[col] = 'Unique'
                agrupado['Indicador'] = indicador
                agrupado['DATA_ACIONA'] = data
                agrupados_data.append(agrupado)

            agrupado_concat = pd.concat(agrupados_data, ignore_index=True)
            ultimo_valor_por_mes[chave_mes] = agrupado_concat
            resultados.append(agrupado_concat)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA_ACIONA'] = data
                resultados.append(agrupado_replicado)
            else:
                agrupados_zero = []
                for indicador, _ in indicadores:
                    agrupado_zero = pd.DataFrame([{
                        'FX_ATRASO': 'Unique',
                        **{col: 'Unique' for col in segmentacoes_extras},
                        'DATA_ACIONA': data,
                        'Indicador': indicador,
                        'qte': 0,
                        'VALORPRIN_FIN': 0.0
                    }])
                    agrupados_zero.append(agrupado_zero)
                agrupado_concat = pd.concat(agrupados_zero, ignore_index=True)
                ultimo_valor_por_mes[chave_mes] = agrupado_concat
                resultados.append(agrupado_concat)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_ACIONAMENTOS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_grupo = ['DATA_ACIONA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
        qte=('qte', 'sum'),
        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    )

    colunas_ordenadas = (
        ['DATA_ACIONA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_final

@registrar_tempo("Funil de acionamentos esforço", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_esforco_funil(df_acionamentos_enriquecido_limpo, df_dw_calendario, segmentacoes_extras=None):
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
    ultimo_valor_por_mes = {}

    indicadores = [
        ('Acionamentos', 'ACIONAMENTOS'),
        ('CPC', 'CPC'),
        ('CPCA', 'CPCA'),
        ('Promessa', 'PROMESSA'),
    ]

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando acumulado mensal ESFOR\u00c7O para {len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo_log=LOG_ACIONAMENTOS)

        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)
        tem_dados = (df['DATA_ACIONA'] == data).any()

        if tem_dados:
            df_intervalo = df[(df['DATA_ACIONA'] >= inicio_mes) & (df['DATA_ACIONA'] <= data)].copy()

            agrupados_data = []
            for indicador, col_flag in indicadores:
                df_flag = df_intervalo[df_intervalo[col_flag] == 1]

                agrupado = df_intervalo.groupby(['FX_ATRASO']).agg(
                    qte=(col_flag, 'sum')
                ).reset_index()

                agrupado_valor = df_flag.groupby(['FX_ATRASO']).agg(
                    VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
                ).reset_index() if len(df_flag) > 0 else pd.DataFrame(columns=['FX_ATRASO', 'VALORPRIN_FIN'])

                agrupado = agrupado.merge(agrupado_valor, on='FX_ATRASO', how='left')
                agrupado['VALORPRIN_FIN'] = agrupado['VALORPRIN_FIN'].fillna(0)
                agrupado['FX_ATRASO'] = 'Esforço'
                for col in segmentacoes_extras:
                    agrupado[col] = 'Esforço'
                agrupado['Indicador'] = indicador
                agrupado['DATA_ACIONA'] = data
                agrupados_data.append(agrupado)

            agrupado_concat = pd.concat(agrupados_data, ignore_index=True)
            ultimo_valor_por_mes[chave_mes] = agrupado_concat
            resultados.append(agrupado_concat)

        else:
            if chave_mes in ultimo_valor_por_mes:
                agrupado_replicado = ultimo_valor_por_mes[chave_mes].copy()
                agrupado_replicado['DATA_ACIONA'] = data
                resultados.append(agrupado_replicado)
            else:
                agrupados_zero = []
                for indicador, _ in indicadores:
                    agrupado_zero = pd.DataFrame([{
                        'FX_ATRASO': 'Esforço',
                        **{col: 'Esforço' for col in segmentacoes_extras},
                        'DATA_ACIONA': data,
                        'Indicador': indicador,
                        'qte': 0,
                        'VALORPRIN_FIN': 0.0
                    }])
                    agrupados_zero.append(agrupado_zero)
                agrupado_concat = pd.concat(agrupados_zero, ignore_index=True)
                ultimo_valor_por_mes[chave_mes] = agrupado_concat
                resultados.append(agrupado_concat)

        if i > 0 and inicio_mes.month != datas_calendario[i-1].month:
            mes_anterior = (datas_calendario[i-1].year, datas_calendario[i-1].month)
            if mes_anterior in ultimo_valor_por_mes:
                del ultimo_valor_por_mes[mes_anterior]

    df_final = pd.concat(resultados, ignore_index=True)

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_ACIONAMENTOS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA', right_on='dt_data', how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_grupo = ['DATA_ACIONA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    df_final = df_final.groupby(colunas_grupo, as_index=False).agg(
        qte=('qte', 'sum'),
        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    )

    colunas_ordenadas = (
        ['DATA_ACIONA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_final

@registrar_tempo("Daily fxAtraso - Acionamentos", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_fxAtraso_daily(df_acionamentos_enriquecido_limpo, df_dw_calendario, segmentacoes_extras=None):
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []
    colunas_segmentacao = ['FX_ATRASO'] + segmentacoes_extras

    df = df_acionamentos_enriquecido_limpo.copy()
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

    df['nr_dia_util'] = pd.to_numeric(df['nr_dia_util'], errors='coerce').fillna(0).astype(int)
    df['quartil'] = df['quartil'].fillna('N/A').astype(str)
    df['dt_mes'] = pd.to_numeric(df['dt_mes'], errors='coerce').fillna(0).astype(int)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A').astype(str)

    datas_unicas = sorted(df['DATA_ACIONA'].unique())
    colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando fxAtraso DAILY por {' + '.join(colunas_segmentacao)} para {len(datas_unicas)} datas...", arquivo_log=LOG_ACIONAMENTOS)

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_ACIONAMENTOS)

        df_dia = df[df['DATA_ACIONA'] == data].copy()

        df_dia['TABULACAO_SCORE'] = (
            df_dia['PROMESSA'].astype(int) * 3 +
            df_dia['CPCA'].astype(int) * 2 +
            df_dia['CPC'].astype(int) * 1
        )

        df_dia = df_dia.sort_values(
            ['CPF_DEV'] + colunas_segmentacao + ['TABULACAO_SCORE'],
            ascending=[True] * (len(colunas_segmentacao) + 1) + [False]
        )

        df_filtrado = df_dia.drop_duplicates(
            subset=['CPF_DEV'] + colunas_segmentacao, keep='first'
        ).reset_index(drop=True)

        for indicador, col_flag in [
            ('Acionamentos', 'ACIONAMENTOS'),
            ('CPC', 'CPC'),
            ('CPCA', 'CPCA'),
            ('Promessa', 'PROMESSA'),
        ]:
            df_flag = df_filtrado[df_filtrado[col_flag] == 1]

            agrupado = df_filtrado.groupby(colunas_segmentacao + colunas_calendario, dropna=False).agg(
                qte=(col_flag, 'sum')
            ).reset_index()

            agrupado_valor = df_flag.groupby(colunas_segmentacao + colunas_calendario, dropna=False).agg(
                VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
            ).reset_index() if len(df_flag) > 0 else pd.DataFrame(columns=colunas_segmentacao + colunas_calendario + ['VALORPRIN_FIN'])

            agrupado = agrupado.merge(agrupado_valor, on=colunas_segmentacao + colunas_calendario, how='left').reset_index(drop=True)
            agrupado['VALORPRIN_FIN'] = agrupado['VALORPRIN_FIN'].fillna(0)
            agrupado['Indicador'] = indicador
            agrupado['DATA_ACIONA'] = data
            resultados.append(agrupado)

    df_final = pd.concat(resultados, ignore_index=True)

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_grupo = ['DATA_ACIONA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    # df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
    #     qte=('qte', 'sum'),
    #     VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    # )

    colunas_ordenadas = (
        ['DATA_ACIONA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_final

@registrar_tempo("Daily unique - Acionamentos", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_unique_daily(df_acionamentos_enriquecido_limpo, df_dw_calendario, segmentacoes_extras=None):
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []

    df = df_acionamentos_enriquecido_limpo.copy()
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

    df['nr_dia_util'] = df['nr_dia_util'].fillna(0)
    df['quartil'] = df['quartil'].fillna(0)
    df['dt_mes'] = df['dt_mes'].fillna(pd.NaT)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')

    datas_unicas = sorted(df['DATA_ACIONA'].unique())
    colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando UNIQUE DAILY para {len(datas_unicas)} datas...", arquivo_log=LOG_ACIONAMENTOS)

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_ACIONAMENTOS)

        df_dia = df[df['DATA_ACIONA'] == data].copy()

        df_dia['TABULACAO_SCORE'] = (
            df_dia['PROMESSA'].astype(int) * 3 +
            df_dia['CPCA'].astype(int) * 2 +
            df_dia['CPC'].astype(int) * 1
        )

        df_dia = df_dia.sort_values(['CPF_DEV', 'TABULACAO_SCORE'], ascending=[True, False])
        df_unique = df_dia.drop_duplicates(subset=['CPF_DEV'], keep='first')

        for indicador, col_flag in [
            ('Acionamentos', 'ACIONAMENTOS'),
            ('CPC', 'CPC'),
            ('CPCA', 'CPCA'),
            ('Promessa', 'PROMESSA'),
        ]:
            agrupado = df_unique.groupby(['FX_ATRASO'] + colunas_calendario, dropna=False).apply(
                lambda g, f=col_flag: pd.Series({
                    'qte': g[f].sum(),
                    'VALORPRIN_FIN': g.loc[g[f] == 1, 'VALORPRIN_FIN'].sum()
                }), include_groups=False
            ).reset_index()

            agrupado['FX_ATRASO'] = 'Unique'
            for col in segmentacoes_extras:
                agrupado[col] = 'Unique'

            agrupado['Indicador'] = indicador
            agrupado['DATA_ACIONA'] = data
            resultados.append(agrupado)

    df_final = pd.concat(resultados, ignore_index=True)

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_grupo = ['DATA_ACIONA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
        qte=('qte', 'sum'),
        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    )

    colunas_ordenadas = (
        ['DATA_ACIONA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_final

@registrar_tempo("Daily esforço - Acionamentos", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_esforco_daily(df_acionamentos_enriquecido_limpo, df_dw_calendario, segmentacoes_extras=None):
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    segmentacoes_extras = segmentacoes_extras or []

    df = df_acionamentos_enriquecido_limpo.copy()
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

    df['nr_dia_util'] = df['nr_dia_util'].fillna(0)
    df['quartil'] = df['quartil'].fillna(0)
    df['dt_mes'] = df['dt_mes'].fillna(pd.NaT)
    df['mes_abreviado'] = df['mes_abreviado'].fillna('N/A')

    datas_unicas = sorted(df['DATA_ACIONA'].unique())
    colunas_calendario = ['nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']
    resultados = []

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando ESFORÇO DAILY para {len(datas_unicas)} datas...", arquivo_log=LOG_ACIONAMENTOS)

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...", arquivo_log=LOG_ACIONAMENTOS)

        df_dia = df[df['DATA_ACIONA'] == data].copy()

        for indicador, col_flag in [
            ('Acionamentos', 'ACIONAMENTOS'),
            ('CPC', 'CPC'),
            ('CPCA', 'CPCA'),
            ('Promessa', 'PROMESSA'),
        ]:
            agrupado = df_dia.groupby(['FX_ATRASO'] + colunas_calendario, dropna=False).apply(
                lambda g, f=col_flag: pd.Series({
                    'qte': g[f].sum(),
                    'VALORPRIN_FIN': g.loc[g[f] == 1, 'VALORPRIN_FIN'].sum()
                }), include_groups=False
            ).reset_index()

            agrupado['FX_ATRASO'] = 'Esforço'
            for col in segmentacoes_extras:
                agrupado[col] = 'Esforço'

            agrupado['Indicador'] = indicador
            agrupado['DATA_ACIONA'] = data
            resultados.append(agrupado)

    df_final = pd.concat(resultados, ignore_index=True)

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    df_final = df_final.rename(columns={'mes_abreviado': 'MesAbreviado'})

    colunas_grupo = ['DATA_ACIONA', 'Indicador', 'FX_ATRASO'] + segmentacoes_extras + ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    df_final = df_final.groupby(colunas_grupo, as_index=False, dropna=False).agg(
        qte=('qte', 'sum'),
        VALORPRIN_FIN=('VALORPRIN_FIN', 'sum')
    )

    colunas_ordenadas = (
        ['DATA_ACIONA', 'Indicador', 'qte', 'FX_ATRASO'] + segmentacoes_extras +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_final

@registrar_tempo("Pipeline acumulados acionamentos", arquivo_log=LOG_ACIONAMENTOS)
def processar_acumulados_acionamentos(
    df_acionamentos_limpo,
    df_dw_calendario,
    segmentacoes_extras=None,
    calcular_funil=True,
    calcular_daily=True,
    retorno='separado'
):
    """
    Orquestra a geração, transformação e união de métricas acumuladas de acionamentos.

    Args:
        df_acionamentos_limpo (pd.DataFrame): DataFrame de acionamentos enriquecido e limpo
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas adicionais para segmentação.
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
        df_funil = unir_dataframes(
            normalizar_tipos_df(acionamentos_fxAtraso_funil(
                df_acionamentos_enriquecido_limpo=df_acionamentos_limpo,
                df_dw_calendario=df_dw_calendario,
                segmentacoes_extras=segmentacoes_extras
            )),
            normalizar_tipos_df(acionamentos_unique_funil(
                df_acionamentos_enriquecido_limpo=df_acionamentos_limpo,
                df_dw_calendario=df_dw_calendario,
                segmentacoes_extras=segmentacoes_extras
            )),
            normalizar_tipos_df(acionamentos_esforco_funil(
                df_acionamentos_enriquecido_limpo=df_acionamentos_limpo,
                df_dw_calendario=df_dw_calendario,
                segmentacoes_extras=segmentacoes_extras
            ))
        )

    # ============================================
    # ETAPA 2: DAILY
    # ============================================
    df_daily = None
    if calcular_daily:
        df_daily = unir_dataframes(
            normalizar_tipos_df(acionamentos_fxAtraso_daily(
                df_acionamentos_enriquecido_limpo=df_acionamentos_limpo,
                df_dw_calendario=df_dw_calendario,
                segmentacoes_extras=segmentacoes_extras
            )),
            normalizar_tipos_df(acionamentos_unique_daily(
                df_acionamentos_enriquecido_limpo=df_acionamentos_limpo,
                df_dw_calendario=df_dw_calendario,
                segmentacoes_extras=segmentacoes_extras
            )),
            normalizar_tipos_df(acionamentos_esforco_daily(
                df_acionamentos_enriquecido_limpo=df_acionamentos_limpo,
                df_dw_calendario=df_dw_calendario,
                segmentacoes_extras=segmentacoes_extras
            ))
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

__all__ = [
    'acionamentos_fxAtraso_funil',
    'acionamentos_unique_funil',
    'acionamentos_esforco_funil',
    'acionamentos_fxAtraso_daily',
    'acionamentos_unique_daily',
    'acionamentos_esforco_daily',
    'processar_acumulados_acionamentos'
]