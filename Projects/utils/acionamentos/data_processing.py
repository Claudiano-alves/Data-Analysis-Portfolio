"""
Módulo de Tratamento de Acionamentos
Contém funções para limpeza, validação e enriquecimento de dados de acionamentos.
"""

import pandas as pd
from utils.utils import registrar_tempo, salvar_log
from typing import Any, Dict, Optional, Union, Tuple, List

def tratar_acionamentos_tabulacao(df_tabulacao_aciona, arquivo_log=None):
    """
    Converte colunas de tabulação para formato binário (0 e 1).

    Args:
        df_tabulacao_aciona (pd.DataFrame): DataFrame com colunas CPC, CPCA, PROMESSA
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        pd.DataFrame: DataFrame com colunas binárias convertidas para int
    """
    @registrar_tempo("Substituindo tabulações por 0 e 1", arquivo_log=arquivo_log)
    def _executar():
        colunas_binarias = ['CPC', 'CPCA', 'PROMESSA']
        df_tabulacao_aciona[colunas_binarias] = df_tabulacao_aciona[colunas_binarias].astype(int)
        salvar_log("Finalizado!", arquivo_log=arquivo_log)
        return df_tabulacao_aciona

    return _executar()

def confere_tabulacao_acionamentos(df_tab_acionamentos, df_tabulacao_aciona, arquivo_log=None):
    """
    Faz o merge entre tab_acionamentos e tabulacao_aciona e adiciona flags de CPC, CPCA e PROMESSA.

    Args:
        df_tab_acionamentos (pd.DataFrame): DataFrame com acionamentos (coluna COD_ACIONA)
        df_tabulacao_aciona (pd.DataFrame): DataFrame com tabulações (colunas IDTABCRM, DESCR, CPC, CPCA, PROMESSA)
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        pd.DataFrame: DataFrame merged com flags de tabulação e coluna ACIONAMENTOS
    """
    @registrar_tempo("Conferindo tabulações aos acionamentos", arquivo_log=arquivo_log)
    def _executar():
        df_resultado = df_tab_acionamentos.copy()
        df_tabula    = df_tabulacao_aciona.copy()

        df_resultado['COD_ACIONA'] = df_resultado['COD_ACIONA'].astype(str)
        df_tabula['IDTABCRM']      = df_tabula['IDTABCRM'].astype(str)

        salvar_log("=" * 80, arquivo_log=arquivo_log)
        salvar_log(f"📊 Antes do merge - Acionamentos: {len(df_resultado):,} | Tabulações: {len(df_tabula):,}", arquivo_log=arquivo_log)

        df_resultado = df_resultado.merge(
            df_tabula[['IDTABCRM', 'DESCR', 'CPC', 'CPCA', 'PROMESSA']],
            left_on='COD_ACIONA',
            right_on='IDTABCRM',
            how='left'
        ).drop(columns='IDTABCRM')

        df_resultado[['CPC', 'CPCA', 'PROMESSA']] = df_resultado[['CPC', 'CPCA', 'PROMESSA']].fillna(0).astype(int)
        df_resultado['ACIONAMENTOS'] = 1

        colunas_originais = df_tab_acionamentos.columns.tolist()
        df_resultado = df_resultado[colunas_originais + ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA', 'DESCR']]

        salvar_log(f"📊 Após merge: {len(df_resultado):,}", arquivo_log=arquivo_log)
        salvar_log(f"📊 Acionamentos com DESCR: {df_resultado['DESCR'].notna().sum():,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_resultado

    return _executar()

def separar_inconsistencias(df_acionamentos, segmentacoes_extras=None, arquivo_log=None):
    """
    Separa acionamentos com inconsistências em DataFrames específicos.

    A verificação de relacionamento com mailing é feita dinamicamente:
    usa a primeira coluna de segmentacoes_extras se fornecida,
    caso contrário usa 'VALOR' (sempre presente no mailing).
    Se nenhuma segmentacao_extras for passada e DESCR for a única
    inconsistência relevante, apenas df_sem_descricao é separado.

    Args:
        df_acionamentos (pd.DataFrame): DataFrame de acionamentos enriquecido
        segmentacoes_extras (list, optional): Colunas de segmentação vindas do mailing.
                                              Ex: ['PF_PJ', 'PA'] ou ['FX_ATRASO']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        tuple: (df_limpo, df_sem_relacionamento, df_sem_descricao)
    """
    @registrar_tempo("Separando inconsistências", arquivo_log=arquivo_log)
    def _executar():
        coluna_referencia = segmentacoes_extras[0] if segmentacoes_extras else 'VALOR'

        # Verifica se a coluna de referência existe no DataFrame
        if coluna_referencia in df_acionamentos.columns:
            sem_relacionamento = df_acionamentos[coluna_referencia].isna()
        else:
            sem_relacionamento = pd.Series(False, index=df_acionamentos.index)

        sem_descricao = df_acionamentos['DESC_ACIONAMENTO'].isna()

        df_sem_relacionamento = df_acionamentos[sem_relacionamento].copy()
        df_sem_descricao      = df_acionamentos[sem_descricao].copy()
        df_limpo              = df_acionamentos[~(sem_relacionamento | sem_descricao)].copy()

        salvar_log("=" * 80, arquivo_log=arquivo_log)
        salvar_log(f"\n📋 ANÁLISE DE INCONSISTÊNCIAS:", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Registros limpos: {len(df_limpo):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ⚠️  Sem relacionamento ({coluna_referencia}): {len(df_sem_relacionamento):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ⚠️  Sem DESCR (erro de tabulação): {len(df_sem_descricao):,}", arquivo_log=arquivo_log)
        salvar_log(f"   📊 Total de registros: {len(df_acionamentos):,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        return df_limpo, df_sem_relacionamento, df_sem_descricao

    return _executar()

def enriquecer_acionamentos(
    df_acionamentos,
    df_mailing_hist,
    df_dw_calendario,
    segmentacoes_extras=None,
    separar_inconsistencias_flag=True,
    arquivo_log=None,
):
    """
    Enriquece a base de acionamentos com informações de mailing_hist e calendário.

    A separação entre registros que cruzaram e não cruzaram com o mailing
    é feita dinamicamente: usa a primeira coluna de segmentacoes_extras se
    fornecida, caso contrário usa 'VALOR' (sempre presente no mailing).

    Args:
        df_acionamentos (pd.DataFrame): DataFrame de acionamentos tabulados
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist tratado
        df_dw_calendario (pd.DataFrame): DataFrame de calendário
        segmentacoes_extras (list, optional): Colunas de segmentação vindas do mailing.
                                              Ex: ['PF_PJ', 'PA'] ou ['FX_ATRASO', 'FAIXA']
        separar_inconsistencias_flag (bool): Se True, separa inconsistências
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS

    Returns:
        Se separar_inconsistencias_flag=True:
            tuple: (df_limpo, df_sem_relacionamento, df_sem_descricao)
        Se separar_inconsistencias_flag=False:
            pd.DataFrame: DataFrame enriquecido completo
    """
    @registrar_tempo("Enriquecendo acionamentos mailing hist e calendário", arquivo_log=arquivo_log)
    def _executar():
        # Coluna de referência para detectar se cruzou com mailing
        coluna_referencia = segmentacoes_extras[0] if segmentacoes_extras else 'VALOR'

        df_resultado = df_acionamentos.copy()
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        # ============================================
        # ENRIQUECER COM MAILING_HIST
        # ============================================
        df_mailing_temp = df_mailing_hist.copy()
        df_mailing_temp = df_mailing_temp.rename(columns={
            'CONTRATO': 'CONTRATO_FIN',
            'DATA': 'DATA_ACIONA'
        })

        df_resultado['DATA_ACIONA']  = pd.to_datetime(df_resultado['DATA_ACIONA']).dt.date
        df_mailing_temp['DATA_ACIONA'] = pd.to_datetime(df_mailing_temp['DATA_ACIONA']).dt.date

        salvar_log(f"📊 Merge com mailing_hist... ({len(df_resultado):,} registros)", arquivo_log=arquivo_log)
        df_resultado = df_resultado.merge(
            df_mailing_temp,
            on=['CONTRATO_FIN', 'DATA_ACIONA'],
            how='left'
        )

        registros_sem_relacionamento = df_resultado[coluna_referencia].isna().sum()
        salvar_log(f"   ⚠️  Registros sem relacionamento ({coluna_referencia}): {registros_sem_relacionamento:,}", arquivo_log=arquivo_log)

        # ============================================
        # RESOLVER TODAS AS COLUNAS DUPLICADAS (_x / _y)
        # ============================================
        colunas_x = [c for c in df_resultado.columns if c.endswith('_x')]
        for col_x in colunas_x:
            col_base = col_x[:-2]
            col_y = col_base + '_y'
            if col_y in df_resultado.columns:
                # Prioriza o valor do mailing (_y); se nulo, mantém o original (_x)
                df_resultado[col_base] = df_resultado[col_y].fillna(df_resultado[col_x])
                df_resultado = df_resultado.drop(columns=[col_x, col_y])
                salvar_log(f"   🔧 Coluna duplicada resolvida: {col_base} ({col_x} + {col_y})", arquivo_log=arquivo_log)

        salvar_log(f"   ✓ Registros após merge: {len(df_resultado):,}", arquivo_log=arquivo_log)

        # ============================================
        # REMOVER DUPLICATAS GERADAS PELO MERGE
        # ============================================
        registros_antes = len(df_resultado)
        df_resultado = df_resultado.drop_duplicates(keep='first')
        duplicatas_removidas = registros_antes - len(df_resultado)
        if duplicatas_removidas > 0:
            salvar_log(f"   🔧 Removidas {duplicatas_removidas:,} duplicatas do merge", arquivo_log=arquivo_log)
            salvar_log(f"   ✓ Registros únicos: {len(df_resultado):,}", arquivo_log=arquivo_log)

        # ============================================
        # ENRIQUECER COM DW_CALENDARIO
        # ============================================
        df_resultado['DATA_ACIONA'] = pd.to_datetime(df_resultado['DATA_ACIONA']).dt.date
        df_dw_calendario_temp = df_dw_calendario.copy()
        df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date

        salvar_log(f"📅 Merge com dw_calendario...", arquivo_log=arquivo_log)
        df_resultado = df_resultado.merge(
            df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
            left_on='DATA_ACIONA', right_on='dt_data', how='left'
        ).drop(columns='dt_data')

        salvar_log(f"   ✓ Registros finais: {len(df_resultado):,}", arquivo_log=arquivo_log)
        salvar_log(f"   ✓ Com relacionamento: {df_resultado[coluna_referencia].notna().sum():,}", arquivo_log=arquivo_log)
        salvar_log("=" * 80, arquivo_log=arquivo_log)

        if separar_inconsistencias_flag:
            return separar_inconsistencias(
                df_resultado, 
                segmentacoes_extras=segmentacoes_extras, 
                arquivo_log=arquivo_log
            )
        else:
            return df_resultado

    return _executar()

def processar_acionamentos(
    df_tab_acionamentos: pd.DataFrame,
    df_tabulacao_aciona: pd.DataFrame,
    df_mailing_hist: pd.DataFrame,
    df_dw_calendario: pd.DataFrame,
    segmentacoes_extras: Optional[List[str]] = None,
    separar_inconsistencias_flag: bool = True,
    arquivo_log: Optional[str] = None,
):
    """
    Pipeline completo de processamento de acionamentos.
 
    Etapas:
        1. confere_tabulacao_acionamentos → merge com tabulações (CPC, CPCA, PROMESSA, DESCR)
        2. enriquecer_acionamentos        → merge com mailing e calendário + separação de inconsistências
 
    Args:
        df_tab_acionamentos (pd.DataFrame): DataFrame de acionamentos
        df_tabulacao_aciona (pd.DataFrame): DataFrame de tabulações
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist tratado
        df_dw_calendario (pd.DataFrame): DataFrame de calendário
        segmentacoes_extras (list, optional): Colunas de segmentação vindas do mailing.
                                              Ex: ['PF_PJ', 'PA'] ou ['FX_ATRASO']
        separar_inconsistencias_flag (bool): Se True, separa inconsistências. Default: True
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_ACIONAMENTOS
 
    Returns:
        Se separar_inconsistencias_flag=True:
            tuple: (df_limpo, df_sem_relacionamento, df_sem_descricao)
        Se separar_inconsistencias_flag=False:
            pd.DataFrame: DataFrame enriquecido completo
    """
    # ETAPA 1: Conferir tabulações
    df_acionamentos = confere_tabulacao_acionamentos(
        df_tab_acionamentos=df_tab_acionamentos,
        df_tabulacao_aciona=df_tabulacao_aciona,
        arquivo_log=arquivo_log,
    )
 
    # ETAPA 2: Enriquecer com mailing e calendário
    return enriquecer_acionamentos(
        df_acionamentos=df_acionamentos,
        df_mailing_hist=df_mailing_hist,
        df_dw_calendario=df_dw_calendario,
        segmentacoes_extras=segmentacoes_extras,
        separar_inconsistencias_flag=separar_inconsistencias_flag,
        arquivo_log=arquivo_log,
    )