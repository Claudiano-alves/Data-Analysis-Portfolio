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
