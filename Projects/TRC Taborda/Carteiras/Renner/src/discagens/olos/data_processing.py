"""
Módulo de Tratamentos - Discagens OLOS (Renner)

Responsável por:
- Enriquecimento com mailing_hist e calendário
"""

import pandas as pd
from utils.utils import salvar_log, registrar_tempo
from utils.config import LOG_DISCAGENS

@registrar_tempo("Enriquecimento OLOS com mailing e calendário", arquivo_log=LOG_DISCAGENS)
def enriquecer_olos_com_mailing_calendario(df_olos, df_mailing_hist, df_dw_calendario):
    """
    Enriquece o DataFrame de discagens OLOS com dados de mailing_hist e calendário.
    Retorna dois DataFrames: um com FX_ATRASO e outro sem.

    Args:
        df_olos (pd.DataFrame): DataFrame com discagens OLOS (DATA, CONTRATO, CAMPANHA, ROUTE)
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing tratado
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário

    Returns:
        tuple: (df_com_fx_atraso, df_sem_fx_atraso)
    """
    df_resultado = df_olos.copy()
    df_resultado['CONTRATO'] = df_resultado['CONTRATO'].astype(str).str.upper().str.strip()
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date

    # ============================================
    # PREPARAR MAILING
    # filtrar pelo contratos únicos do OLOS antes de qualquer operação
    # ============================================
    contratos_olos = set(df_resultado['CONTRATO'].unique())
    salvar_log(f"📊 Contratos únicos OLOS: {len(contratos_olos):,}", arquivo_log=LOG_DISCAGENS)

    df_mailing_temp = df_mailing_hist[
        df_mailing_hist['CONTRATO'].astype(str).str.upper().str.strip().isin(contratos_olos)
    ].copy().reset_index(drop=True)
    df_mailing_temp['CONTRATO'] = df_mailing_temp['CONTRATO'].astype(str).str.upper().str.strip()
    df_mailing_temp['DATA'] = pd.to_datetime(df_mailing_temp['DATA']).dt.date
    salvar_log(f"📊 Mailing filtrado por contratos OLOS: {len(df_mailing_temp):,} registros", arquivo_log=LOG_DISCAGENS)

    # Pré-deduplica por CONTRATO+DATA — mantém maior ATRASO
    df_mailing_temp = (
        df_mailing_temp
        .sort_values('ATRASO', ascending=False)
        .drop_duplicates(subset=['CONTRATO', 'DATA'], keep='first')
        .reset_index(drop=True)
    )
    salvar_log(f"📊 Mailing deduplicado: {len(df_mailing_temp):,} registros", arquivo_log=LOG_DISCAGENS)

    # ============================================
    # MERGE OLOS + MAILING
    # resultado: DATA, CONTRATO, CAMPANHA, ROUTE + colunas mailing
    # ============================================
    salvar_log(f"📊 Merge OLOS com mailing_hist... ({len(df_resultado):,} registros)", arquivo_log=LOG_DISCAGENS)
    df_resultado = df_resultado.merge(df_mailing_temp, on=['CONTRATO', 'DATA'], how='left')
    registros_sem_fx = df_resultado['FX_ATRASO'].isna().sum()
    salvar_log(f"   ⚠️  Registros sem FX_ATRASO: {registros_sem_fx:,}", arquivo_log=LOG_DISCAGENS)

    # ============================================
    # MERGE COM CALENDÁRIO
    # ============================================
    df_dw_calendario_temp = df_dw_calendario[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']].copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date

    salvar_log(f"📅 Merge OLOS com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    df_resultado = df_resultado.merge(
        df_dw_calendario_temp,
        left_on='DATA', right_on='dt_data', how='left'
    ).drop(columns=['dt_data'])

    # ============================================
    # SEPARAR COM E SEM FX_ATRASO
    # ============================================
    df_com_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].notna()].copy()
    df_sem_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].isna()].copy()

    salvar_log(f"📦 COM FX_ATRASO: {len(df_com_fx_atraso):,} | SEM FX_ATRASO: {len(df_sem_fx_atraso):,}", arquivo_log=LOG_DISCAGENS)
    return df_com_fx_atraso, df_sem_fx_atraso

def unir_discagens_expert_olos(df_expert, df_olos):
    """
    Une os DataFrames de discagens Expert e OLOS em um único analítico.

    - CAMPANHA (OLOS) é renomeada para GrupoPrincipal
    - ROUTE (OLOS) é descartada
    - Coluna CANAL identifica a origem dos dados ('EXPERT' ou 'OLOS')
    - Colunas específicas da Expert ficam nulas nos registros OLOS

    Args:
        df_expert (pd.DataFrame): DataFrame de discagens Expert tratado
        df_olos (pd.DataFrame): DataFrame de discagens OLOS enriquecido

    Returns:
        pd.DataFrame: DataFrame unificado Expert + OLOS
    """
    df_expert_temp = df_expert.copy()
    df_expert_temp['CANAL'] = 'EXPERT'

    df_olos_temp = df_olos.copy()
    df_olos_temp = df_olos_temp.rename(columns={'CAMPANHA': 'GrupoPrincipal'})
    df_olos_temp = df_olos_temp.drop(columns=['ROUTE'], errors='ignore')
    df_olos_temp['CANAL'] = 'OLOS'

    df_unido = pd.concat([df_expert_temp, df_olos_temp], ignore_index=True)

    colunas_prioritarias = [
        'DATA', 'CONTRATO', 'CPF', 'GrupoPrincipal', 'CANAL',
        'ATRASO', 'COD_CLI', 'COD_CAR', 'VALOR',
        'FX_ATRASO', 'CONTRATO_ORIGINAL', 'PRODUTO', 'FAIXA',
        'OPERACAO', 'ESTADO',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]

    colunas_existentes = [c for c in colunas_prioritarias if c in df_unido.columns]
    colunas_restantes = [c for c in df_unido.columns if c not in colunas_existentes]
    return df_unido[colunas_existentes + colunas_restantes]

def unir_discagens_expert_olos_massivos(df_expert_olos, df_massivos):
    """
    Adiciona os massivos ao DataFrame unificado Expert + OLOS.

    Args:
        df_expert_olos (pd.DataFrame): DataFrame unificado Expert + OLOS
        df_massivos (pd.DataFrame): DataFrame de massivos enriquecido com mailing e calendário

    Returns:
        pd.DataFrame: DataFrame unificado Expert + OLOS + Massivos
    """
    df_unido = pd.concat([df_expert_olos, df_massivos], ignore_index=True)

    colunas_prioritarias = [
        'DATA', 'CONTRATO', 'CPF', 'GrupoPrincipal', 'CANAL',
        'ATRASO', 'COD_CLI', 'COD_CAR', 'VALOR',
        'FX_ATRASO', 'CONTRATO_ORIGINAL', 'PRODUTO', 'FAIXA',
        'OPERACAO', 'ESTADO',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]

    colunas_existentes = [c for c in colunas_prioritarias if c in df_unido.columns]
    colunas_restantes = [c for c in df_unido.columns if c not in colunas_existentes]
    return df_unido[colunas_existentes + colunas_restantes]

def preparar_massivos(df_sms, df_rcs, df_email, df_whats):
    """
    Une os DataFrames de canais massivos, mantendo apenas CPF e DATA únicos.
    Para CPFs iguais na mesma data, mantém apenas uma ocorrência.
    A coluna CANAL identifica a origem do registro (SMS, RCS, EMAIL, WHATS).

    Args:
        df_sms, df_rcs, df_email, df_whats (pd.DataFrame): DataFrames dos canais massivos
            Observação: df_email usa 'DATA' como coluna de data,
                        os demais usam 'DATA_DISPARO'

    Returns:
        pd.DataFrame: DataFrame com colunas CPF, DATA e CANAL únicos
    """
    dfs = []
    for df, col_data, canal in [
        (df_sms,   'DATA_DISPARO', 'SMS'),
        (df_rcs,   'DATA_DISPARO', 'RCS'),
        (df_email, 'DATA',         'EMAIL'),
        (df_whats, 'DATA_DISPARO', 'WHATS'),
    ]:
        if df is not None and len(df) > 0:
            df_temp = df[['CPF', col_data]].copy()
            df_temp = df_temp.rename(columns={col_data: 'DATA'})
            df_temp['CANAL'] = canal
            dfs.append(df_temp)

    df_massivos = pd.concat(dfs, ignore_index=True)
    df_massivos['DATA'] = pd.to_datetime(df_massivos['DATA']).dt.date
    df_massivos['CPF'] = df_massivos['CPF'].astype(str).str.strip()

    salvar_log(f"📊 Massivos unificados: {len(df_massivos):,} combinações CPF+DATA únicas", arquivo_log=LOG_DISCAGENS)
    return df_massivos

def processar_massivos(df_sms, df_rcs, df_email, df_whats, df_mailing_hist, df_dw_calendario):

    # ============================================
    # ETAPA 1: UNIR MASSIVOS
    # ============================================
    df_massivos = preparar_massivos(df_sms, df_rcs, df_email, df_whats)
    print(f"Massivos unificados: {len(df_massivos):,} registros")

    # ============================================
    # ETAPA 2: PRÉ-DEDUPLICAR MAILING
    # um contrato por CPF+DATA — maior ATRASO
    # ============================================
    df_mailing_temp = df_mailing_hist.copy()
    df_mailing_temp['DATA'] = pd.to_datetime(df_mailing_temp['DATA']).dt.date
    df_mailing_temp['CPF'] = df_mailing_temp['CPF'].astype(str).str.strip()
    df_mailing_temp = (
        df_mailing_temp
        .sort_values('ATRASO', ascending=False)
        .drop_duplicates(subset=['CPF', 'DATA'], keep='first')
        .reset_index(drop=True)
    )
    salvar_log(f"📊 Mailing pré-deduplicado: {len(df_mailing_temp):,} registros", arquivo_log=LOG_DISCAGENS)

    # ============================================
    # ETAPA 3: ENRIQUECER COM MAILING
    # ============================================
    df_resultado = df_massivos.merge(
        df_mailing_temp,
        on=['CPF', 'DATA'],
        how='left'
    )

    # ============================================
    # ETAPA 4: ENRIQUECER COM CALENDÁRIO
    # ============================================
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date

    df_resultado = df_resultado.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='left'
    ).drop(columns=['dt_data'])

    # ============================================
    # ETAPA 5: SEPARAR COM E SEM MAILING
    # ============================================
    df_com_mailing = df_resultado[df_resultado['FX_ATRASO'].notna()].reset_index(drop=True)
    df_sem_mailing = df_resultado[df_resultado['FX_ATRASO'].isna()].reset_index(drop=True)

    salvar_log(f"📦 COM mailing: {len(df_com_mailing):,} | SEM mailing: {len(df_sem_mailing):,}", arquivo_log=LOG_DISCAGENS)
    return df_com_mailing, df_sem_mailing

def processar_trabalhado(df_expert, df_olos, df_sms, df_rcs, df_email, df_whats, df_mailing_hist, df_dw_calendario):
    """
    Orquestra o enriquecimento do OLOS, processamento dos massivos
    e a união com Expert em um único DataFrame.

    Args:
        df_expert (pd.DataFrame): DataFrame de discagens Expert tratado
        df_olos (pd.DataFrame): DataFrame de discagens OLOS bruto
        df_sms, df_rcs, df_email, df_whats (pd.DataFrame): DataFrames dos canais massivos
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing tratado
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário

    Returns:
        tuple:
            df_expert_olos_massivos (pd.DataFrame): DataFrame unificado Expert + OLOS + Massivos
            df_massivos_com_mailing (pd.DataFrame): DataFrame apenas dos massivos com mailing,
                                                    usado para cálculo de acumulado por canal
    """
    # ============================================
    # ETAPA 1: ENRIQUECER OLOS
    # ============================================
    df_olos_com_fx, _ = enriquecer_olos_com_mailing_calendario(
        df_olos=df_olos,
        df_mailing_hist=df_mailing_hist,
        df_dw_calendario=df_dw_calendario
    )

    # ============================================
    # ETAPA 2: UNIR EXPERT + OLOS
    # ============================================
    df_expert_olos = unir_discagens_expert_olos(
        df_expert=df_expert,
        df_olos=df_olos_com_fx
    )

    # ============================================
    # ETAPA 3: PROCESSAR MASSIVOS
    # sem filtro de discagens — contagem separada por canal
    # ============================================
    df_massivos_com_mailing, _ = processar_massivos(
        df_sms=df_sms,
        df_rcs=df_rcs,
        df_email=df_email,
        df_whats=df_whats,
        df_mailing_hist=df_mailing_hist,
        df_dw_calendario=df_dw_calendario
    )

    # ============================================
    # ETAPA 4: UNIR EXPERT + OLOS + MASSIVOS
    # ============================================
    df_expert_olos_massivos = unir_discagens_expert_olos_massivos(
        df_expert_olos=df_expert_olos,
        df_massivos=df_massivos_com_mailing
    )

    return df_expert_olos_massivos, df_massivos_com_mailing

__all__ = [
    'enriquecer_olos_com_mailing_calendario',
    'unir_discagens_expert_olos',
    'unir_discagens_expert_olos_massivos',
    'preparar_massivos',
    'processar_massivos',
    'processar_trabalhado'
]