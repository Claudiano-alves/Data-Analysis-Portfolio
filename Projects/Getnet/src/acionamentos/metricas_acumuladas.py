"""
Módulo de Métricas Acumuladas de Acionamentos
Contém funções para gerar métricas acumuladas (mensais) de acionamentos.
"""

import pandas as pd
from functools import reduce
from utils.utils import registrar_tempo, salvar_log
from ..config import LOG_ACIONAMENTOS


@registrar_tempo("Funil de acionamentos fxAtraso e origem humano", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_fxAtraso_origem_humano(df_acionamentos_enriquecido_limpo, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF + FX_ATRASO + ORIGEM (melhor score).
    Garante que dias sem dados novos repliquem os valores do dia anterior.
    
    Args:
        df_acionamentos_enriquecido_limpo (pd.DataFrame): DataFrame enriquecido expert
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_acionamentos_enriquecido_limpo.copy()
    df['DATA_ACIONA'] = pd.to_datetime(df['DATA_ACIONA'])

    # Preparar calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    # Obter todas as datas do calendário que estão no período dos dados
    data_min = df['DATA_ACIONA'].min()
    data_max = df['DATA_ACIONA'].max()
    
    df_calendario_periodo = df_dw_calendario_temp[
        (df_dw_calendario_temp['dt_data'] >= data_min) & 
        (df_dw_calendario_temp['dt_data'] <= data_max)
    ].sort_values('dt_data').copy()
    
    datas_calendario = df_calendario_periodo['dt_data'].tolist()
    resultados = []

    salvar_log("=" * 80, arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando acumulado mensal por FX_ATRASO + ORIGEM para {len(datas_calendario)} datas...", arquivo=LOG_ACIONAMENTOS)

    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo=LOG_ACIONAMENTOS)
        
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
                ['CPF_DEV', 'FX_ATRASO', 'ORIGEM', 'TABULACAO_SCORE'],
                ascending=[True, True, True, False]
            )
            
            df_filtrado = df_intervalo.drop_duplicates(
                subset=['CPF_DEV', 'FX_ATRASO', 'ORIGEM'],
                keep='first'
            ).copy()
            
            agrupado = df_filtrado.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
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
                combinacoes = df[['FX_ATRASO', 'ORIGEM']].drop_duplicates()
                
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

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo=LOG_ACIONAMENTOS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    colunas_ordenadas = [
        'DATA_ACIONA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"\n📈 Totais acumulados por FX_ATRASO + ORIGEM (última data):", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['ACIONAMENTOS'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPC'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPCA'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['PROMESSA'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log("=" * 80)

    return df_final


@registrar_tempo("Funil de acionamentos unique humano", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_unique_humano(df_acionamentos_enriquecido_limpo, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score) por FX_ATRASO e ORIGEM.
    Versão para df_acionamentos_enriquecido_limpo (expert).
    Garante que dias sem dados novos repliquem os valores do dia anterior.
    
    Args:
        df_acionamentos_enriquecido_limpo (pd.DataFrame): DataFrame enriquecido expert
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

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

    salvar_log("=" * 80, arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando acumulado mensal ÚNICO (melhor score por CPF) para {len(datas_calendario)} datas...", arquivo=LOG_ACIONAMENTOS)

    ultimo_valor_por_mes = {}

    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo=LOG_ACIONAMENTOS)
        
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
            
            df_unique = df_intervalo.drop_duplicates(
                subset=['CPF_DEV'],
                keep='first'
            ).copy()
            
            agrupado = df_unique.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
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
                combinacoes = df[['FX_ATRASO', 'ORIGEM']].drop_duplicates()
                
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

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo=LOG_ACIONAMENTOS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    colunas_ordenadas = [
        'DATA_ACIONA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"\n📈 Totais acumulados ÚNICOS (última data):", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['ACIONAMENTOS'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPC'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPCA'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['PROMESSA'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo=LOG_ACIONAMENTOS)

    df_final['FX_ATRASO'] = 'Unique'
    return df_final


@registrar_tempo("Funil de acionamentos esforço humano", arquivo_log=LOG_ACIONAMENTOS)
def acionamentos_esforco_humano(df_acionamentos_enriquecido_limpo, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos por FX_ATRASO e ORIGEM.
    Versão para df_acionamentos_enriquecido_limpo (expert).
    Garante que dias sem dados novos repliquem os valores do dia anterior.
    
    Args:
        df_acionamentos_enriquecido_limpo (pd.DataFrame): DataFrame enriquecido expert
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)
    
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
    
    salvar_log("=" * 80, arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Processando acumulado mensal para {len(datas_calendario)} datas...", arquivo=LOG_ACIONAMENTOS)
    
    ultimo_valor_por_mes = {}
    
    for i, data in enumerate(datas_calendario, 1):
        if i % 10 == 0 or i == len(datas_calendario):
            salvar_log(f"   Processando {i}/{len(datas_calendario)} datas...", arquivo=LOG_ACIONAMENTOS)
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        chave_mes = (data.year, data.month)
        
        tem_dados = (df['DATA_ACIONA'] == data).any()
        
        if tem_dados:
            df_intervalo = df[(df['DATA_ACIONA'] >= inicio_mes) & (df['DATA_ACIONA'] <= data)].copy()
            
            agrupado = df_intervalo.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
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
                combinacoes = df[['FX_ATRASO', 'ORIGEM']].drop_duplicates()
                
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

    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo=LOG_ACIONAMENTOS)
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])
    
    colunas_numericas = df_final.select_dtypes(include=['number']).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)
    
    colunas_ordenadas = [
        'DATA_ACIONA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]
    
    salvar_log(f"   ✓ Registros finais: {len(df_final):,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"\n📈 Totais acumulados:", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['ACIONAMENTOS'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPC'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['CPCA'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA_ACIONA'] == df_final['DATA_ACIONA'].max()]['PROMESSA'].sum():,}", arquivo=LOG_ACIONAMENTOS)
    salvar_log("=" * 80)

    df_final['FX_ATRASO'] = 'Esforço'
    return df_final
