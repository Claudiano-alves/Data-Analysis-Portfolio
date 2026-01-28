"""
Módulo de Tratamentos - Discagens Trestto

Responsável por:
- Consolidação de discagens por DATA + CPF
- Merge com mailing e calendário
- Enriquecimento com dados externos
"""

import pandas as pd
from utils.utils import unir_dataframes, salvar_log, registrar_tempo
from ...config import LOG_DISCAGENS

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


@registrar_tempo("Enriquecimento base discagens trestto", arquivo_log=LOG_DISCAGENS)
def enriquecer_discagens_trestto(df_discagens_trestto, df_mailing_hist, df_dw_calendario):
    """
    Enriquece o DataFrame de discagens Trestto com dados de mailing_hist e calendário.
    Consolidação por DATA + CPF antes do merge.
    
    Args:
        df_discagens_trestto (pd.DataFrame): DataFrame com discagens Trestto,
            deve conter 'CPF', 'DATA', 'DISCAGEM', 'ALO', 'CPC', 'CPCA', 'PROMESSA'
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        tuple: (df_com_fx_atraso, df_sem_fx_atraso)
    """
    
    df = df_discagens_trestto.copy()
    df_mailing = df_mailing_hist.copy()
    
    # Converter DATA para o mesmo tipo
    df['DATA'] = pd.to_datetime(df['DATA']).dt.date
    df_mailing['DATA'] = pd.to_datetime(df_mailing['DATA']).dt.date
    
    # Garantir que CPF está como string
    df['CPF'] = df['CPF'].astype(str).str.strip()
    df_mailing['CPF'] = df_mailing['CPF'].astype(str).str.strip()
    
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Antes da consolidação - Trestto: {len(df):,}", arquivo_log=LOG_DISCAGENS)
    
    # Consolidar Trestto por DATA + CPF
    colunas_metricas = ['DISCAGEM', 'ALO', 'CPC', 'CPCA', 'PROMESSA']
    df_consolidado = df.groupby(['DATA', 'CPF'], as_index=False)[colunas_metricas].sum()
    df_consolidado = df_consolidado.rename(columns={'ALO': 'ACIONAMENTOS'})
    
    salvar_log(f"📊 Após consolidação - Trestto: {len(df_consolidado):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"📊 Mailing: {len(df_mailing):,}", arquivo_log=LOG_DISCAGENS)
    
    # Enriquecer com mailing_hist
    df_mailing_temp = df_mailing[['CPF', 'DATA', 'PRODUTO', 'FX_ATRASO', 'VALORPRIN_FIN']].drop_duplicates()
    
    salvar_log(f"\n📊 Merge com mailing_hist...", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   Registros antes: {len(df_consolidado):,}", arquivo_log=LOG_DISCAGENS)
    
    df_resultado = df_consolidado.merge(
        df_mailing_temp,
        on=['CPF', 'DATA'],
        how='left'
    )
    
    salvar_log(f"   ✓ Registros após merge: {len(df_resultado):,}", arquivo_log=LOG_DISCAGENS)
    registros_sem_fx = df_resultado['FX_ATRASO'].isna().sum()
    salvar_log(f"   ⚠️  Registros sem FX_ATRASO: {registros_sem_fx:,}", arquivo_log=LOG_DISCAGENS)
    
    # Enriquecer com calendário
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date
    
    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]
    
    salvar_log(f"\n📅 Merge com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   Registros antes: {len(df_resultado):,}", arquivo_log=LOG_DISCAGENS)
    
    df_resultado = df_resultado.merge(
        df_calendario_reduzido,
        left_on='DATA',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])
    
    salvar_log(f"   ✓ Registros após merge: {len(df_resultado):,}", arquivo_log=LOG_DISCAGENS)
    
    # Renomear DISCAGEM para TRABALHADO
    df_resultado.rename(columns={'DISCAGEM': 'TRABALHADO'}, inplace=True)
    
    # Separar em dois DataFrames
    df_com_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].notna()].copy()
    df_sem_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].isna()].copy()
    
    salvar_log(f"\n📦 Separação dos DataFrames:", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ Registros COM FX_ATRASO: {len(df_com_fx_atraso):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ Registros SEM FX_ATRASO: {len(df_sem_fx_atraso):,}", arquivo_log=LOG_DISCAGENS)
    salvar_log(f"   ✓ CPFs únicos SEM FX_ATRASO: {df_sem_fx_atraso['CPF'].nunique():,}", arquivo_log=LOG_DISCAGENS)
    salvar_log("="*80, arquivo_log=LOG_DISCAGENS)
    
    return df_com_fx_atraso, df_sem_fx_atraso


__all__ = [
    'enriquecer_discagens_trestto'
]
