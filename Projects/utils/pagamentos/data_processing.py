"""
Módulo de Tratamento de Pagamentos
Contém funções para limpeza, validação e enriquecimento de dados de pagamentos.
"""

import pandas as pd
from utils.utils import registrar_tempo, salvar_log
from ..config import LOG_PAGAMENTOS
from typing import List, Optional, Tuple

@registrar_tempo("Dados de pagamentos", arquivo_log=LOG_PAGAMENTOS)
def data_pagamentos(
    df_pagamentos: pd.DataFrame,
    df_acordos: pd.DataFrame,
    df_mailing_hist: pd.DataFrame,
    df_dw_calendario: pd.DataFrame,
    dimensoes_segmentacao: Optional[List[str]] = None,
    log_file: str = LOG_PAGAMENTOS
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Processa dados de pagamentos com validações e enriquecimentos.
    
    Args:
        df_pagamentos: DataFrame de pagamentos
        df_acordos: DataFrame de acordos
        df_mailing_hist: DataFrame de mailing_hist (para FX_ATRASO)
        df_dw_calendario: DataFrame de calendário
        dimensoes_segmentacao: Lista de dimensões para segmentação (ex: ['ORIGEM'])
                              Se None, não segmenta (só FX_ATRASO)
        log_file: Arquivo de log
    
    Returns:
        tuple: (df_pagamento_agrupado, df_sem_fx_atraso, df_pagamentos_analitico)
    
    Examples:
        # Carteira com segmentação por ORIGEM
        df_pag, df_sem_fx, df_analitico = data_pagamentos(
            df_pagamentos, df_acordos, df_mailing, df_calendario,
            dimensoes_segmentacao=['ORIGEM']
        )
        
        # Carteira sem segmentação
        df_pag, df_sem_fx, df_analitico = data_pagamentos(
            df_pagamentos, df_acordos, df_mailing, df_calendario,
            dimensoes_segmentacao=[]
        )
    """
    
    # ============================================
    # VALIDAR E PREPARAR DIMENSÕES
    # ============================================
    if dimensoes_segmentacao is None:
        dimensoes_segmentacao = []
    
    # Filtrar apenas dimensões que existem em df_acordos
    dimensoes_validas_acordos = [
        dim for dim in dimensoes_segmentacao 
        if dim in df_acordos.columns
    ]
    
    # ============================================
    # PREPARAÇÃO INICIAL
    # ============================================
    df_pagamentos['CONTRATO_FIN'] = df_pagamentos['CONTRATO_FIN'].str.strip()
    df_acordos['CONTRATO_FIN'] = df_acordos['CONTRATO_FIN'].str.strip()
    df_mailing_hist['CONTRATO'] = df_mailing_hist['CONTRATO'].str.strip()

    df_mailing_hist['DATA'] = pd.to_datetime(df_mailing_hist['DATA'])
    df_mailing_hist_unique = df_mailing_hist[['CONTRATO', 'FX_ATRASO', 'DATA']].drop_duplicates()

    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log("INÍCIO DO PROCESSAMENTO DE PAGAMENTOS", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log(f"Total de registros em df_pagamentos: {len(df_pagamentos)}", arquivo_log=log_file)
    salvar_log(f"Total de registros em df_acordos: {len(df_acordos)}", arquivo_log=log_file)
    salvar_log(f"Total de registros únicos em df_mailing_hist: {len(df_mailing_hist_unique)}", arquivo_log=log_file)
    
    if dimensoes_validas_acordos:
        salvar_log(f"📊 Dimensões de segmentação: {dimensoes_validas_acordos}", arquivo_log=log_file)
    else:
        salvar_log("📊 Sem segmentação (apenas FX_ATRASO)", arquivo_log=log_file)

    # ============================================
    # FILTRAR ACORDOS VÁLIDOS (DINÂMICO)
    # ============================================
    # Colunas base sempre necessárias
    colunas_acordos = ['CONTRATO_FIN', 'NACORDO_ACO', 'DATA_ACORDO']
    
    # Adicionar dimensões de segmentação se existirem
    colunas_acordos_completas = colunas_acordos + dimensoes_validas_acordos
    
    # Definir subset para drop_duplicates (sem DATA_ACORDO)
    subset_duplicates = ['CONTRATO_FIN', 'NACORDO_ACO'] + dimensoes_validas_acordos
    
    df_acordos_validos = (
        df_acordos[df_acordos['CANC_ACORDO'].isna()]
        .drop_duplicates(subset=subset_duplicates)
        [colunas_acordos_completas]
    )

    df_acordos_validos['DATA_ACORDO'] = pd.to_datetime(df_acordos_validos['DATA_ACORDO'])
    salvar_log(f"Total de acordos válidos (não cancelados): {len(df_acordos_validos)}", arquivo_log=log_file)

    # ============================================
    # MERGE COM ACORDOS
    # ============================================
    df_resultado = df_pagamentos.merge(
        df_acordos_validos,
        on=['CONTRATO_FIN', 'NACORDO_ACO'],
        how='inner'
    )

    salvar_log(f"Total de registros após cruzamento com acordos: {len(df_resultado)}", arquivo_log=log_file)

    # ============================================
    # MERGE COM CALENDÁRIO
    # ============================================
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

    salvar_log(f"Total de registros após cruzamento com calendário: {len(df_resultado)}", arquivo_log=log_file)

    # ============================================
    # MERGE COM MAILING_HIST
    # ============================================
    df_pagamentos_tratado = df_resultado.merge(
        df_mailing_hist_unique,
        left_on=['CONTRATO_FIN', 'DATA_ACORDO'],
        right_on=['CONTRATO', 'DATA'],
        how='left'
    ).drop(columns=['CONTRATO', 'DATA'])

    salvar_log(f"Total de registros após cruzamento com histórico: {len(df_pagamentos_tratado)}", arquivo_log=log_file)

    # ============================================
    # SEPARAR POR FX_ATRASO
    # ============================================
    df_com_fx_atraso = df_pagamentos_tratado[df_pagamentos_tratado['FX_ATRASO'].notna()]
    df_sem_fx_atraso = df_pagamentos_tratado[df_pagamentos_tratado['FX_ATRASO'].isna()]

    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log("SEPARAÇÃO DE DADOS POR FX_ATRASO", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log(f"Pagamentos COM FX_ATRASO: {len(df_com_fx_atraso)}", arquivo_log=log_file)
    salvar_log(f"Pagamentos SEM FX_ATRASO: {len(df_sem_fx_atraso)}", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)
    
    # ============================================
    # AGRUPAMENTO DINÂMICO
    # ============================================
    # Colunas base do agrupamento
    colunas_agrupamento = ['DATA_PAGTO', 'FX_ATRASO']
    
    # Adicionar dimensões de segmentação válidas
    # Filtrar novamente para garantir que existem em df_com_fx_atraso
    dimensoes_validas_agrupamento = [
        dim for dim in dimensoes_validas_acordos
        if dim in df_com_fx_atraso.columns
    ]
    
    colunas_agrupamento_completas = (
        colunas_agrupamento + 
        dimensoes_validas_agrupamento +
        ['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    )
    
    df_agrupado = df_com_fx_atraso.groupby(
        colunas_agrupamento_completas
    ).agg(
        qte=('CPF_DEV', 'count'),
        VALOR_PARC=('VALOR_PARC', 'sum')
    ).reset_index()

    salvar_log(f"Total de linhas agrupadas (antes de remover qte=0): {len(df_agrupado)}", arquivo_log=log_file)

    # ============================================
    # FORMATAÇÃO FINAL
    # ============================================
    df_agrupado['Indicador'] = 'Pagamentos'
    df_agrupado = df_agrupado.rename(columns={'mes_abreviado': 'MesAbreviado'})

    # Reordenar colunas dinamicamente
    colunas_finais = (
        ['DATA_PAGTO', 'Indicador', 'qte', 'FX_ATRASO'] +
        dimensoes_validas_agrupamento +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR_PARC']
    )
    
    df_agrupado = df_agrupado[colunas_finais]

    # Remover linhas onde qte é 0
    df_agrupado_antes = len(df_agrupado)
    df_agrupado = df_agrupado[df_agrupado['qte'] > 0]
    linhas_removidas = df_agrupado_antes - len(df_agrupado)

    salvar_log(f"Linhas removidas com qte=0: {linhas_removidas}", arquivo_log=log_file)
    salvar_log(f"Total de linhas finais no DataFrame agrupado: {len(df_agrupado)}", arquivo_log=log_file)

    # ============================================
    # RESUMO FINAL
    # ============================================
    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log("RESUMO FINAL", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)
    salvar_log(f"Valor total de parcelas: R$ {df_agrupado['VALOR_PARC'].sum():,.2f}", arquivo_log=log_file)
    salvar_log(f"Quantidade total de pagamentos: {df_agrupado['qte'].sum()}", arquivo_log=log_file)
    salvar_log("=" * 60, arquivo_log=log_file)

    df_pagamentos_analitico = df_pagamentos_tratado.copy()

    return df_agrupado, df_sem_fx_atraso, df_pagamentos_analitico