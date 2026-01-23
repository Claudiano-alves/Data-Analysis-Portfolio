"""
Módulo de Tratamento de Pagamentos
Contém funções para limpeza, validação e enriquecimento de dados de pagamentos.
"""

import pandas as pd
from Projects.utils.utils import registrar_tempo, salvar_log
from ..config import LOG_PAGAMENTOS


@registrar_tempo("Dados de pagamentos", arquivo_log=LOG_PAGAMENTOS)
def data_pagamentos(df_pagamentos, df_acordos, df_mailing_hist, df_dw_calendario):
    """
    Processa dados de pagamentos com validações e enriquecimentos.
    
    Args:
        df_pagamentos (pd.DataFrame): DataFrame de pagamentos
        df_acordos (pd.DataFrame): DataFrame de acordos
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist (para FX_ATRASO)
        df_dw_calendario (pd.DataFrame): DataFrame de calendário
    
    Returns:
        tuple: (df_pagamento_tratado, df_sem_fx_atraso, df_pagamentos_analitico)
    """
    df_pagamentos['CONTRATO_FIN'] = df_pagamentos['CONTRATO_FIN'].str.strip()
    df_acordos['CONTRATO_FIN'] = df_acordos['CONTRATO_FIN'].str.strip()
    df_mailing_hist['CONTRATO'] = df_mailing_hist['CONTRATO'].str.strip()

    df_mailing_hist['DATA'] = pd.to_datetime(df_mailing_hist['DATA'])
    df_mailing_hist_unique = df_mailing_hist[['CONTRATO', 'FX_ATRASO', 'DATA']].drop_duplicates()

    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)
    salvar_log("INÍCIO DO PROCESSAMENTO DE PAGAMENTOS", arquivo_log=LOG_PAGAMENTOS)
    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Total de registros em df_pagamentos: {len(df_pagamentos)}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Total de registros em df_acordos: {len(df_acordos)}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Total de registros únicos em df_mailing_hist: {len(df_mailing_hist_unique)}", arquivo_log=LOG_PAGAMENTOS)

    # Filtrar acordos válidos
    df_acordos_validos = (
        df_acordos[df_acordos['CANC_ACORDO'].isna()]
        .drop_duplicates(subset=['CONTRATO_FIN', 'NACORDO_ACO', 'TIPO'])
        [['CONTRATO_FIN', 'NACORDO_ACO', 'TIPO', 'DATA_ACORDO']]
    )

    df_acordos_validos['DATA_ACORDO'] = pd.to_datetime(df_acordos_validos['DATA_ACORDO'])
    salvar_log(f"Total de acordos válidos (não cancelados): {len(df_acordos_validos)}", arquivo_log=LOG_PAGAMENTOS)

    # Merge com acordos
    df_resultado = df_pagamentos.merge(
        df_acordos_validos,
        on=['CONTRATO_FIN', 'NACORDO_ACO'],
        how='inner'
    )

    salvar_log(f"Total de registros após cruzamento com acordos: {len(df_resultado)}", arquivo_log=LOG_PAGAMENTOS)

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

    salvar_log(f"Total de registros após cruzamento com calendário: {len(df_resultado)}", arquivo_log=LOG_PAGAMENTOS)

    # Merge com mailing_hist
    df_pagamentos_tratado = df_resultado.merge(
        df_mailing_hist_unique,
        left_on=['CONTRATO_FIN', 'DATA_ACORDO'],
        right_on=['CONTRATO', 'DATA'],
        how='left'
    ).drop(columns=['CONTRATO', 'DATA'])

    salvar_log(f"Total de registros após cruzamento com histórico: {len(df_pagamentos_tratado)}", arquivo_log=LOG_PAGAMENTOS)

    df_com_fx_atraso = df_pagamentos_tratado[df_pagamentos_tratado['FX_ATRASO'].notna()]
    df_sem_fx_atraso = df_pagamentos_tratado[df_pagamentos_tratado['FX_ATRASO'].isna()]

    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)
    salvar_log("SEPARAÇÃO DE DADOS POR FX_ATRASO", arquivo_log=LOG_PAGAMENTOS)
    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Pagamentos COM FX_ATRASO: {len(df_com_fx_atraso)}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Pagamentos SEM FX_ATRASO: {len(df_sem_fx_atraso)}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)
    
    # Agrupar por faixa e tipo
    df_agrupado = df_com_fx_atraso.groupby(
        ['DATA_PAGTO', 'FX_ATRASO', 'TIPO', 'mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    ).agg(
        qte=('CPF_DEV', 'count'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()

    salvar_log(f"Total de linhas agrupadas (antes de remover qte=0): {len(df_agrupado)}", arquivo_log=LOG_PAGAMENTOS)

    df_agrupado['Indicador'] = 'Pagamentos'
    df_agrupado = df_agrupado.rename(columns={'mes_abreviado': 'MesAbreviado'})

    df_agrupado = df_agrupado[[
        'DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO', 'TIPO',
        'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC'
    ]]

    # Remover linhas onde qte é 0
    df_agrupado_antes = len(df_agrupado)
    df_agrupado = df_agrupado[df_agrupado['qte'] > 0]
    linhas_removidas = df_agrupado_antes - len(df_agrupado)

    salvar_log(f"Linhas removidas com qte=0: {linhas_removidas}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Total de linhas finais no DataFrame agrupado: {len(df_agrupado)}", arquivo_log=LOG_PAGAMENTOS)

    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)
    salvar_log("RESUMO FINAL", arquivo_log=LOG_PAGAMENTOS)
    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Valor total de parcelas: R$ {df_agrupado['VALOR_PARC'].sum():,.2f}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log(f"Quantidade total de pagamentos: {df_agrupado['qte'].sum()}", arquivo_log=LOG_PAGAMENTOS)
    salvar_log("=" * 60, arquivo_log=LOG_PAGAMENTOS)

    df_pagamentos_analitico = df_pagamentos_tratado.copy()

    return df_agrupado, df_sem_fx_atraso, df_pagamentos_analitico
