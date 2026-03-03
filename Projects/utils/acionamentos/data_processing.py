"""
Módulo de Tratamento de Acionamentos
Contém funções para limpeza, validação e enriquecimento de dados de acionamentos.
"""

import pandas as pd
from utils.utils import registrar_tempo, salvar_log
from ..config import LOG_ACIONAMENTOS


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
