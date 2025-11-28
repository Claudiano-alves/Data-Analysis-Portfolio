import pandas as pd
from utils import salvar_log, registrar_tempo, unir_dataframes

@registrar_tempo("Dados de pagamentos")
def data_pagamentos(df_pagamentos, df_acordos, df_maling_hist, df_dw_calendario):
    df_pagamentos['CONTRATO_FIN'] = df_pagamentos['CONTRATO_FIN'].str.strip()
    df_acordos['CONTRATO_FIN'] = df_acordos['CONTRATO_FIN'].str.strip()
    df_maling_hist['CONTRATO'] = df_maling_hist['CONTRATO'].str.strip()

    df_maling_hist['DATA'] = pd.to_datetime(df_maling_hist['DATA'])
    df_maling_hist_unique = df_maling_hist[['CONTRATO', 'FX_ATRASO', 'DATA']].drop_duplicates()

    salvar_log("="*60)
    salvar_log("INÍCIO DO PROCESSAMENTO DE PAGAMENTOS")
    salvar_log("="*60)
    salvar_log(f"Total de registros em df_pagamentos: {len(df_pagamentos)}")
    salvar_log(f"Total de registros em df_acordos: {len(df_acordos)}")
    salvar_log(f"Total de registros únicos em df_maling_hist: {len(df_maling_hist_unique)}")

    df_acordos_validos = (
        df_acordos[df_acordos['CANC_ACORDO'].isna()]
        .drop_duplicates(subset=['CONTRATO_FIN', 'NACORDO_ACO', 'TIPO'])
        [['CONTRATO_FIN', 'NACORDO_ACO', 'TIPO', 'DATA_ACORDO']]
    )

    df_acordos_validos['DATA_ACORDO'] = pd.to_datetime(df_acordos_validos['DATA_ACORDO'])
    
    salvar_log(f"Total de acordos válidos (não cancelados): {len(df_acordos_validos)}")

    # Cruzamento com acordos
    df_resultado = df_pagamentos.merge(
        df_acordos_validos,
        on=['CONTRATO_FIN', 'NACORDO_ACO'],
        how='inner'
    )

    salvar_log(f"Total de registros após cruzamento com acordos: {len(df_resultado)}")

    df_resultado['DATA_PAGTO'] = pd.to_datetime(df_resultado['DATA_PAGTO']).dt.date
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date

    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]

    df_resultado = df_resultado.merge(
        df_calendario_reduzido,
        left_on='DATA_PAGTO',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])

    salvar_log(f"Total de registros após cruzamento com calendário: {len(df_resultado)}")

    # Cruzamento com maling_hist e remoção das colunas duplicadas
    df_pagamentos_tratado = df_resultado.merge(
        df_maling_hist_unique,
        left_on=['CONTRATO_FIN', 'DATA_ACORDO'],
        right_on=['CONTRATO', 'DATA'],
        how='left'
    ).drop(columns=['CONTRATO', 'DATA'])

    salvar_log(f"Total de registros após cruzamento com histórico: {len(df_pagamentos_tratado)}")

    df_com_fx_atraso = df_pagamentos_tratado[df_pagamentos_tratado['FX_ATRASO'].notna()]
    df_sem_fx_atraso = df_pagamentos_tratado[df_pagamentos_tratado['FX_ATRASO'].isna()]

    salvar_log("="*60)
    salvar_log("SEPARAÇÃO DE DADOS POR FX_ATRASO")
    salvar_log("="*60)
    salvar_log(f"Pagamentos COM FX_ATRASO: {len(df_com_fx_atraso)}")
    salvar_log(f"Pagamentos SEM FX_ATRASO: {len(df_sem_fx_atraso)}")
    salvar_log(f"Percentual COM FX_ATRASO: {(len(df_com_fx_atraso)/len(df_pagamentos_tratado)*100):.2f}%")
    salvar_log(f"Percentual SEM FX_ATRASO: {(len(df_sem_fx_atraso)/len(df_pagamentos_tratado)*100):.2f}%")

    df_agrupado = df_com_fx_atraso.groupby(
        ['DATA_PAGTO', 'FX_ATRASO', 'TIPO', 'mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    ).agg(
        qte=('CPF_DEV', 'count'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()

    salvar_log(f"Total de linhas agrupadas (antes de remover qte=0): {len(df_agrupado)}")

    # Adicionar coluna Indicador
    df_agrupado['Indicador'] = 'Pagamentos'

    # Renomear mes_abreviado para MesAbreviado
    df_agrupado = df_agrupado.rename(columns={'mes_abreviado': 'MesAbreviado'})

    # Reordenar colunas conforme solicitado
    df_agrupado = df_agrupado[[
        'DATA_PAGTO', 
        'Indicador', 
        'qte', 
        'FX_ATRASO', 
        'TIPO', 
        'MesAbreviado', 
        'nr_dia_util', 
        'quartil', 
        'dt_mes',
        'VALOR_PARC'
    ]]

    # Remover linhas onde qte é 0
    df_agrupado_antes = len(df_agrupado)
    df_agrupado = df_agrupado[df_agrupado['qte'] > 0]
    linhas_removidas = df_agrupado_antes - len(df_agrupado)

    salvar_log(f"Linhas removidas com qte=0: {linhas_removidas}")
    salvar_log(f"Total de linhas finais no DataFrame agrupado: {len(df_agrupado)}")

    salvar_log("="*60)
    salvar_log("RESUMO FINAL")
    salvar_log("="*60)
    salvar_log(f"Valor total de parcelas: R$ {df_agrupado['VALOR_PARC'].sum():,.2f}")
    salvar_log(f"Quantidade total de pagamentos: {df_agrupado['qte'].sum()}")
    salvar_log("="*60)
    df_pagamento = df_agrupado.copy()
    df_pagamentos_analitico = df_pagamentos_tratado.copy()

    return df_pagamento, df_sem_fx_atraso, df_pagamentos_analitico

@registrar_tempo("Gerando acumulado de pagamentos")
def gerar_acumulado_por_dia_util(df_agrupado):
    """
    Gera DataFrame com acumulado de pagamentos do início do mês até cada dia útil.
    """
    salvar_log("="*60)
    salvar_log("GERANDO ACUMULADO POR DIA ÚTIL")
    salvar_log("="*60)
    
    resultados = []
    
    # Obter datas únicas ordenadas
    datas_unicas = sorted(df_agrupado['DATA_PAGTO'].unique())
    
    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}")
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        # Obter o início do mês da data atual
        inicio_mes = pd.Timestamp(data.year, data.month, 1).date()
        
        # Filtrar do início do mês até a data atual
        df_intervalo = df_agrupado[
            (df_agrupado['DATA_PAGTO'] >= inicio_mes) & 
            (df_agrupado['DATA_PAGTO'] <= data)
        ].copy()
        
        # Agrupar por FX_ATRASO e TIPO (acumulado do mês até a data)
        agrupado = df_intervalo.groupby(['FX_ATRASO', 'TIPO']).agg({
            'qte': 'sum',
            'VALOR_PARC': 'sum'
        }).reset_index()
        
        # Adicionar informações da data
        # Pegar informações do calendário da data atual
        info_data = df_agrupado[df_agrupado['DATA_PAGTO'] == data][
            ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        ].drop_duplicates().iloc[0]
        
        agrupado['DATA_PAGTO'] = data
        agrupado['Indicador'] = 'Pagamentos'
        agrupado['MesAbreviado'] = info_data['MesAbreviado']
        agrupado['nr_dia_util'] = info_data['nr_dia_util']
        agrupado['quartil'] = info_data['quartil']
        agrupado['dt_mes'] = info_data['dt_mes']
        
        resultados.append(agrupado)
    
    # Concatenar todos os resultados
    df_acumulado = pd.concat(resultados, ignore_index=True)
    
    # Reordenar colunas
    df_acumulado = df_acumulado[[
        'DATA_PAGTO',
        'Indicador',
        'qte',
        'FX_ATRASO',
        'TIPO',
        'MesAbreviado',
        'nr_dia_util',
        'quartil',
        'dt_mes',
        'VALOR_PARC'
    ]]
    
    salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}")
    salvar_log(f"Quantidade total final: {df_acumulado['qte'].sum()}")
    salvar_log(f"Valor total final: R$ {df_acumulado['VALOR_PARC'].sum():,.2f}")
    salvar_log("="*60)
    
    df_acumulado = df_acumulado[df_acumulado['qte'] > 0]
    df_unique = df_acumulado.copy()
    df_unique['FX_ATRASO'] = 'Unique'
    df_esforco = df_acumulado.copy()
    df_esforco['FX_ATRASO'] = 'Esforço'
    return df_acumulado, df_esforco, df_unique

def tratar_pagamentos(df_pagamentos, df_acordos, df_maling_hist, df_dw_calendario):
    df_pagamentos, df_sem_fx_atraso, df_pagamento_analitico = data_pagamentos(df_pagamentos, df_acordos, df_maling_hist, df_dw_calendario)
    df_pagamentos_funil, df_esforco, df_unique = gerar_acumulado_por_dia_util(df_pagamentos)

    df_pagamentos_funil = unir_dataframes(df_pagamentos_funil, df_esforco, df_unique)

    return df_pagamentos_funil, df_sem_fx_atraso, df_pagamento_analitico