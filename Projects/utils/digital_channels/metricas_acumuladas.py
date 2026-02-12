import pandas as pd
import warnings
from utils.utils import registrar_tempo, salvar_log
from ..config import LOG_CHANNELS


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