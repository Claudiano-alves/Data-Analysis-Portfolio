def discagens_operacao_fxAtraso_funil_(df_discagens, segmentacoes_extras=None):
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
  
def discagens_operacao_unique_funil_(df_discagens, segmentacoes_extras=None):
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
  
def discagens_operacao_esforco_funil_(df_discagens, segmentacoes_extras=None):
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
def discagens_unique_funil_(df_discagens, df_dw_calendario, segmentacoes_extras=None):
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
def discagens_esforco_funil_(df_discagens, df_dw_calendario, segmentacoes_extras=None):
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

@registrar_tempo("Segmentação de discagens expert", arquivo_log=LOG_DISCAGENS)
def segmentar_discagens_expert(df):
    """
    Separa o DataFrame em 3 grupos:
    1. ORIGEM = 'Humano' E ACIONAMENTOS = 1
    2. OPERACAO = 'Outros'
    3. Restante
    
    Args:
        df (pd.DataFrame): DataFrame com discagens enriquecidas
    
    Returns:
        tuple: (df_restante, df_humano_primeiro, df_operacao_outros)
    """
    df_trabalho = df.copy()
    
    condicao_humano = (df_trabalho['ORIGEM'] == 'Humano') & (df_trabalho['ACIONAMENTOS'] == 1)
    df_humano_primeiro = df_trabalho[condicao_humano].copy()
    df_trabalho = df_trabalho[~condicao_humano].copy()
    
    condicao_outros = df_trabalho['OPERACAO'] == 'Outros'
    df_operacao_outros = df_trabalho[condicao_outros].copy()
    df_restante = df_trabalho[~condicao_outros].copy()
    
    salvar_log(f"📊 Segmentação de discagens expert:", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Humano + Primeiro Acionamento: {len(df_humano_primeiro):,} ({len(df_humano_primeiro)/len(df)*100:.1f}%)", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Operação 'Outros': {len(df_operacao_outros):,} ({len(df_operacao_outros)/len(df)*100:.1f}%)", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   • Restante: {len(df_restante):,} ({len(df_restante)/len(df)*100:.1f}%)", arquivo_log=LOG_DISCAGENS)

    return df_restante, df_humano_primeiro, df_operacao_outros

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

@registrar_tempo("Enriquecendo acionamentos mailing hist e calendário", arquivo_log=LOG_ACIONAMENTOS)
def enriquecer_acionamentos_(df_acionamentos, df_mailing_hist, df_dw_calendario,
                            separar_inconsistencias_flag=True):
    """
    Enriquece a base de acionamentos com informações de mailing_hist e calendário.
    Todas as colunas do mailing_hist serão trazidas para os acionamentos,
    incluindo segmentações específicas da carteira (ex: FAIXA, PRODUTO).
    
    Args:
        df_acionamentos (pd.DataFrame): DataFrame de acionamentos tabulados
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist tratado
        df_dw_calendario (pd.DataFrame): DataFrame de calendário
        separar_inconsistencias_flag (bool): Se True, separa inconsistências
    
    Returns:
        Se separar_inconsistencias_flag=True:
            tuple: (df_limpo, df_sem_fx_atraso, df_sem_descricao)
        Se separar_inconsistencias_flag=False:
            pd.DataFrame: DataFrame enriquecido completo
    """
    df_resultado = df_acionamentos.copy()
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    # ============================================
    # ENRIQUECER COM MAILING_HIST
    # ============================================
    df_mailing_temp = df_mailing_hist.copy()
    df_mailing_temp = df_mailing_temp.rename(columns={
        'CONTRATO': 'CONTRATO_FIN',
        'DATA': 'DATA_ACIONA'
    })

    df_resultado['DATA_ACIONA'] = pd.to_datetime(df_resultado['DATA_ACIONA']).dt.date
    df_mailing_temp['DATA_ACIONA'] = pd.to_datetime(df_mailing_temp['DATA_ACIONA']).dt.date

    salvar_log(f"📊 Merge com mailing_hist...", arquivo_log=LOG_ACIONAMENTOS)
    df_resultado = df_resultado.merge(
        df_mailing_temp,
        on=['CONTRATO_FIN', 'DATA_ACIONA'],
        how='left'
    )

    # Resolver CPF duplicado se vier do mailing
    if 'CPF_x' in df_resultado.columns and 'CPF_y' in df_resultado.columns:
        df_resultado['CPF'] = df_resultado['CPF_y'].fillna(df_resultado['CPF_x'])
        df_resultado = df_resultado.drop(columns=['CPF_x', 'CPF_y'])

    salvar_log(f"   ✓ Registros: {len(df_resultado):,}", arquivo_log=LOG_ACIONAMENTOS)

    # ============================================
    # REMOVER DUPLICATAS GERADAS PELO MERGE
    # ============================================
    registros_antes = len(df_resultado)
    df_resultado = df_resultado.drop_duplicates(keep='first')
    duplicatas_removidas = registros_antes - len(df_resultado)
    if duplicatas_removidas > 0:
        salvar_log(f"   🔧 Removidas {duplicatas_removidas:,} duplicatas do merge", arquivo_log=LOG_ACIONAMENTOS)
        salvar_log(f"   ✓ Registros únicos: {len(df_resultado):,}", arquivo_log=LOG_ACIONAMENTOS)

    # ============================================
    # ENRIQUECER COM DW_CALENDARIO
    # ============================================
    df_resultado['DATA_ACIONA'] = pd.to_datetime(df_resultado['DATA_ACIONA']).dt.date
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date

    df_resultado = df_resultado.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='left'
    ).drop(columns='dt_data')

    salvar_log(f"   ✓ Registros finais: {len(df_resultado):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ Com FX_ATRASO: {df_resultado['FX_ATRASO'].notna().sum():,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    if separar_inconsistencias_flag:
        return separar_inconsistencias(df_resultado)
    else:
        return df_resultado

@registrar_tempo("Enriquecendo acionamentos mailing hist e calendário", arquivo_log=LOG_ACIONAMENTOS)
def enriquecer_acionamentos(df_acionamentos, df_mailing_hist, df_dw_calendario,
                            separar_inconsistencias_flag=True):
    """
    Enriquece a base de acionamentos com informações de mailing_hist e calendário.
    Todas as colunas do mailing_hist serão trazidas para os acionamentos,
    incluindo segmentações específicas da carteira (ex: FAIXA, PRODUTO).
    
    Args:
        df_acionamentos (pd.DataFrame): DataFrame de acionamentos tabulados
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist tratado
        df_dw_calendario (pd.DataFrame): DataFrame de calendário
        separar_inconsistencias_flag (bool): Se True, separa inconsistências
    
    Returns:
        Se separar_inconsistencias_flag=True:
            tuple: (df_limpo, df_sem_fx_atraso, df_sem_descricao)
        Se separar_inconsistencias_flag=False:
            pd.DataFrame: DataFrame enriquecido completo
    """
    df_resultado = df_acionamentos.copy()
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    # ============================================
    # ENRIQUECER COM MAILING_HIST
    # ============================================
    df_mailing_temp = df_mailing_hist.copy()
    df_mailing_temp = df_mailing_temp.rename(columns={
        'CONTRATO': 'CONTRATO_FIN',
        'DATA': 'DATA_ACIONA'
    })

    df_resultado['DATA_ACIONA'] = pd.to_datetime(df_resultado['DATA_ACIONA']).dt.date
    df_mailing_temp['DATA_ACIONA'] = pd.to_datetime(df_mailing_temp['DATA_ACIONA']).dt.date

    salvar_log(f"📊 Merge com mailing_hist...", arquivo_log=LOG_ACIONAMENTOS)
    df_resultado = df_resultado.merge(
        df_mailing_temp,
        on=['CONTRATO_FIN', 'DATA_ACIONA'],
        how='left'
    )

    # ============================================
    # RESOLVER TODAS AS COLUNAS DUPLICADAS (_x / _y)
    # geradas pelo merge com mailing_hist
    # ============================================
    colunas_x = [c for c in df_resultado.columns if c.endswith('_x')]
    for col_x in colunas_x:
        col_base = col_x[:-2]          # nome sem sufixo
        col_y = col_base + '_y'
        if col_y in df_resultado.columns:
            # Prioriza o valor do mailing (_y); se nulo, mantém o original (_x)
            df_resultado[col_base] = df_resultado[col_y].fillna(df_resultado[col_x])
            df_resultado = df_resultado.drop(columns=[col_x, col_y])
            salvar_log(f"   🔧 Coluna duplicada resolvida: {col_base} ({col_x} + {col_y})", arquivo_log=LOG_ACIONAMENTOS)

    salvar_log(f"   ✓ Registros: {len(df_resultado):,}", arquivo_log=LOG_ACIONAMENTOS)

    # ============================================
    # REMOVER DUPLICATAS GERADAS PELO MERGE
    # ============================================
    registros_antes = len(df_resultado)
    df_resultado = df_resultado.drop_duplicates(keep='first')
    duplicatas_removidas = registros_antes - len(df_resultado)
    if duplicatas_removidas > 0:
        salvar_log(f"   🔧 Removidas {duplicatas_removidas:,} duplicatas do merge", arquivo_log=LOG_ACIONAMENTOS)
        salvar_log(f"   ✓ Registros únicos: {len(df_resultado):,}", arquivo_log=LOG_ACIONAMENTOS)

    # ============================================
    # ENRIQUECER COM DW_CALENDARIO
    # ============================================
    df_resultado['DATA_ACIONA'] = pd.to_datetime(df_resultado['DATA_ACIONA']).dt.date
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date

    df_resultado = df_resultado.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA_ACIONA',
        right_on='dt_data',
        how='left'
    ).drop(columns='dt_data')

    salvar_log(f"   ✓ Registros finais: {len(df_resultado):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ Com FX_ATRASO: {df_resultado['FX_ATRASO'].notna().sum():,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    if separar_inconsistencias_flag:
        return separar_inconsistencias(df_resultado)
    else:
        return df_resultado

def acionamentos_duplicados(df_acionamentos_enriquecido_limpo):
    """
    Identifica acionamentos com conflitos de score por CPF + DATA
    
    Args:
        df_acionamentos_enriquecido_limpo (pd.DataFrame): DataFrame enriquecido
    
    Returns:
        pd.DataFrame: Registros com conflitos de tabulação
    """
    df_acionamentos_score = df_acionamentos_enriquecido_limpo.copy()

    # Criar coluna de score
    df_acionamentos_score['TABULACAO_SCORE'] = (
        df_acionamentos_score['PROMESSA'].astype(int) * 3 +
        df_acionamentos_score['CPCA'].astype(int) * 2 +
        df_acionamentos_score['CPC'].astype(int) * 1
    )

    # Identificar CPF + DATA com mais de um score
    cpf_data_score_counts = df_acionamentos_score.groupby(['CONTRATO_FIN', 'DATA_ACIONA'])['TABULACAO_SCORE'].nunique()
    cpf_data_com_conflito = cpf_data_score_counts[cpf_data_score_counts > 1].reset_index()
    cpf_data_com_conflito = cpf_data_com_conflito.drop(columns='TABULACAO_SCORE')

    # Filtrar registros completos com esses casos
    df_conflitos_score = df_acionamentos_score.merge(cpf_data_com_conflito, on=['CONTRATO_FIN', 'DATA_ACIONA'], how='inner')
    return df_conflitos_score


@registrar_tempo("Substituindo tabulações por 0 e 1", arquivo_log=LOG_ACIONAMENTOS)
def tratar_acionamentos_tabulacao(df_tabualacao_aciona):
    """
    Converte colunas de tabulação para formato binário (0 e 1).
    
    Args:
        df_tabualacao_aciona (pd.DataFrame): DataFrame com colunas CPC, CPCA, PROMESSA
    
    Returns:
        pd.DataFrame: DataFrame com colunas binárias convertidas para int
    """
    colunas_binarias = ['CPC', 'CPCA', 'PROMESSA']
    df_tabualacao_aciona[colunas_binarias] = df_tabualacao_aciona[colunas_binarias].astype(int)
    salvar_log("Finalizado!", arquivo_log=LOG_ACIONAMENTOS)
    return df_tabualacao_aciona

@registrar_tempo("Conferindo tabulações aos acionamentos", arquivo_log=LOG_ACIONAMENTOS)
def confere_tabulacao_acionamentos(df_tab_acionamentos, df_tabulacao_aciona):
    """
    Faz o merge entre tab_acionamentos e tabulacao_aciona e adiciona flags de CPC, CPCA e PROMESSA
    
    Args:
        df_tab_acionamentos (pd.DataFrame): DataFrame com acionamentos (coluna COD_ACIONA)
        df_tabulacao_aciona (pd.DataFrame): DataFrame com tabulações (colunas IDTABCRM, DESCR, CPC, CPCA, PROMESSA)
    
    Returns:
        pd.DataFrame: DataFrame merged com flags de tabulação e coluna ACIONAMENTOS
    """
    df_resultado = df_tab_acionamentos.copy()
    df_tabula = df_tabulacao_aciona.copy()

    df_resultado['COD_ACIONA'] = df_resultado['COD_ACIONA'].astype(str)
    df_tabula['IDTABCRM'] = df_tabula['IDTABCRM'].astype(str)

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Antes do merge - Acionamentos: {len(df_resultado):,} | Tabulações: {len(df_tabula):,}", arquivo_log=LOG_ACIONAMENTOS)

    df_resultado = df_resultado.merge(
        df_tabula[['IDTABCRM', 'DESCR', 'CPC', 'CPCA', 'PROMESSA']],
        left_on='COD_ACIONA',
        right_on='IDTABCRM',
        how='left'
    ).drop(columns='IDTABCRM')

    df_resultado[['CPC', 'CPCA', 'PROMESSA']] = df_resultado[['CPC', 'CPCA', 'PROMESSA']].fillna(0).astype(int)

    df_resultado['ACIONAMENTOS'] = 1

    # Reordenar: colunas originais + ACIONAMENTOS + colunas do cruzamento
    colunas_originais = df_tab_acionamentos.columns.tolist()
    df_resultado = df_resultado[colunas_originais + ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA', 'DESCR']]

    salvar_log(f"📊 Após merge: {len(df_resultado):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"📊 Acionamentos com DESCR: {df_resultado['DESCR'].notna().sum():,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_resultado

@registrar_tempo("Separando inconsistências", arquivo_log=LOG_ACIONAMENTOS)
def separar_inconsistencias(df_acionamentos):
    """
    Separa acionamentos com inconsistências em DataFrames específicos.
    
    Args:
        df_acionamentos (pd.DataFrame): DataFrame de acionamentos enriquecido
    
    Returns:
        tuple: (df_limpo, df_sem_fx_atraso, df_sem_descricao)
    """
    sem_fx_atraso = df_acionamentos['FX_ATRASO'].isna()
    sem_descricao = df_acionamentos['DESCR'].isna()

    df_sem_fx_atraso = df_acionamentos[sem_fx_atraso].copy()
    df_sem_descricao = df_acionamentos[sem_descricao].copy()
    df_limpo = df_acionamentos[~(sem_fx_atraso | sem_descricao)].copy()

    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"\n📋 ANÁLISE DE INCONSISTÊNCIAS:", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   ✓ Registros limpos: {len(df_limpo):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   ⚠️  Sem FX_ATRASO (fora da mailing): {len(df_sem_fx_atraso):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   ⚠️  Sem DESCR (erro de tabulação): {len(df_sem_descricao):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log(f"   📊 Total de registros: {len(df_acionamentos):,}", arquivo_log=LOG_ACIONAMENTOS)
    salvar_log("=" * 80, arquivo_log=LOG_ACIONAMENTOS)

    return df_limpo, df_sem_fx_atraso, df_sem_descricao


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

