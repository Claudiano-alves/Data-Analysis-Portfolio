"""
Módulo de Tratamentos - Discagens Expert

Responsável por:
- Enriquecimento com operação, estado e origem
- Merge com tabulações de robô
- Merge com mailing e calendário
- Segmentação de dados
"""

import numpy as np
import pandas as pd
from utils.utils import salvar_log, registrar_tempo
from ...config import LOG_DISCAGENS
# ============================================
# CONSTANTES
# ============================================

DDD_ESTADO = {
    '11': 'SP', '12': 'SP', '13': 'SP', '14': 'SP', '15': 'SP', '16': 'SP', '17': 'SP', '18': 'SP', '19': 'SP',
    '21': 'RJ', '22': 'RJ', '24': 'RJ',
    '27': 'ES', '28': 'ES',
    '31': 'MG', '32': 'MG', '33': 'MG', '34': 'MG', '35': 'MG', '37': 'MG', '38': 'MG',
    '41': 'PR', '42': 'PR', '43': 'PR', '44': 'PR', '45': 'PR', '46': 'PR',
    '47': 'SC', '48': 'SC', '49': 'SC',
    '51': 'RS', '53': 'RS', '54': 'RS', '55': 'RS',
    '61': 'DF', '62': 'GO', '63': 'TO', '64': 'GO', '65': 'MT', '66': 'MT', '67': 'MS',
    '68': 'AC', '69': 'RO',
    '71': 'BA', '73': 'BA', '74': 'BA', '75': 'BA', '77': 'BA',
    '79': 'SE',
    '81': 'PE', '82': 'AL', '83': 'PB', '84': 'RN', '85': 'CE', '86': 'PI', '87': 'PE', '88': 'CE', '89': 'PI',
    '91': 'PA', '92': 'AM', '93': 'PA', '94': 'PA', '95': 'RR', '96': 'AP', '97': 'AM', '98': 'MA', '99': 'MA'
}


# ============================================
# FUNÇÕES DE TRATAMENTO - DISCAGENS EXPERT
# ============================================

def adicionar_operacao(df):
    """
    Adiciona a coluna OPERACAO ao DataFrame de discagens expert
    
    Args:
        df (pd.DataFrame): DataFrame com coluna 'GrupoPrincipal'
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'OPERACAO'
    """
    conditions = [
        df['GrupoPrincipal'] == 4118,
        df['GrupoPrincipal'] == 4022,
        df['GrupoPrincipal'] == 4017,
        df['GrupoPrincipal'].isin([4047, 4679, 4681, 4683, 4671]),
        df['GrupoPrincipal'].isin([4433, 4504]),
        df['GrupoPrincipal'].isin([4326, 4636, 4637, 4649])
    ]
    
    choices = [
        'ATIVO',
        'MANUAL',
        'RECEPTIVO',
        'URA CPC',
        'PREVENTIVO',
        'AGV NEGOCIADORA'
    ]
    
    df['OPERACAO'] = np.select(conditions, choices, default='Outros')
    return df

def adicionar_estado_por_ddd(df, coluna_ddd='ddd'):
    """
    Adiciona a coluna ESTADO baseada no DDD
    
    Args:
        df (pd.DataFrame): DataFrame com coluna de DDD
        coluna_ddd (str): Nome da coluna que contém o DDD
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'ESTADO'
    """
    df['ESTADO'] = df[coluna_ddd].astype(str).str.zfill(2).map(DDD_ESTADO)
    return df

def adicionar_origem(df_discagens_expert):
    """
    Adiciona a coluna ORIGEM diferenciando Robô (AGV NEGOCIADORA) de HUMANO
    
    Args:
        df_discagens_expert (pd.DataFrame): DataFrame com coluna 'OPERACAO'
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'ORIGEM'
    """
    df_discagens_expert = df_discagens_expert.copy()
    df_discagens_expert['ORIGEM'] = df_discagens_expert['OPERACAO'].apply(
        lambda x: 'Robô' if x == 'AGV NEGOCIADORA' else 'Humano'
    )
    return df_discagens_expert

@registrar_tempo("Tratamento base discagens expert", arquivo_log=LOG_DISCAGENS)
def tratar_base_discagens_expert(df):
    """
    Aplica todos os tratamentos padrão para base de discagens expert:
    - Adiciona OPERACAO
    - Adiciona ESTADO por DDD
    - Adiciona ORIGEM
    
    Args:
        df (pd.DataFrame): DataFrame de discagens expert
    
    Returns:
        pd.DataFrame: DataFrame tratado
    """
    df = adicionar_operacao(df)
    df = adicionar_estado_por_ddd(df)
    df = adicionar_origem(df)
    salvar_log(f"✅ Tratamento de discagens expert concluído", arquivo_log=LOG_DISCAGENS)
    return df

@registrar_tempo("Criação DF tabulações robô", arquivo_log=LOG_DISCAGENS)
def criar_df_tabulacoes_robo():
    """
    Cria um DataFrame com classificação de códigos de tabulação.
    
    Returns:
        pd.DataFrame: DataFrame com colunas:
            - COD_TABULACAO: código de tabulação
            - ACIONAMENTOS, CPC, CPCA, PROMESSA: 1 se classificado, 0 caso contrário
    """
    codigos_alo = [
        '10','13','15','20','21','23','24','4','45','6','16','120','152','159','161',
        '19','28','31','32','37','38','48','51','7','30','42','160','27','36','39',
        '47','1','12','14','2','5','8','83','84','85','86','88','89','9','99','113',
        '136','50','101','103','114','115','25','43','46','52','87'
    ]
    codigos_cpc = [
        '10','13','15','20','21','23','24','4','45','6','16','120','152','159','161',
        '19','28','31','32','37','38','48','51','7','30','42','160','27','36','39',
        '47','86','25','43','46','87'
    ]
    codigos_cpca = [
        '10','13','15','20','21','23','24','4','45','6','16','120','152','159','161',
        '19','28','31','32','37','38','48','51','7','42','160','25','43','46'
    ]
    codigos_promessa = [
        '10','13','15','20','21','23','24','4','45','6','160','25','43','46'
    ]
    
    todos_codigos = sorted(set(codigos_alo + codigos_cpc + codigos_cpca + codigos_promessa))
    
    df_tabulacoes = pd.DataFrame({'COD_TABULACAO': todos_codigos})
    df_tabulacoes['ACIONAMENTOS'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_alo).astype(int)
    df_tabulacoes['CPC'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_cpc).astype(int)
    df_tabulacoes['CPCA'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_cpca).astype(int)
    df_tabulacoes['PROMESSA'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_promessa).astype(int)
    
    salvar_log(f"✅ DataFrame de classificação de tabulações criado! ({len(df_tabulacoes)} códigos)", arquivo_log=LOG_DISCAGENS)
    return df_tabulacoes

@registrar_tempo("Enriquecimento com tabulações robô", arquivo_log=LOG_DISCAGENS)
def enriquecer_com_tabulacoes_robo(df_discagens_expert, df_tabulacoes_robo):
    """
    Enriquece o DataFrame de discagens_expert com classificações de tabulação.
    
    Args:
        df_discagens_expert (pd.DataFrame): DataFrame com discagens, coluna 'codtabulacao'
        df_tabulacoes_robo (pd.DataFrame): DataFrame com classificações de tabulação
    
    Returns:
        pd.DataFrame: DataFrame enriquecido com colunas TRABALHADO, ACIONAMENTOS, CPC, CPCA, PROMESSA
    """
    salvar_log(f"📊 Merge com classificações de tabulação... ({len(df_discagens_expert):,} registros)", arquivo_log=LOG_DISCAGENS)
    
    df_resultado = df_discagens_expert.merge(
        df_tabulacoes_robo[['COD_TABULACAO', 'ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']],
        left_on='codtabulacao',
        right_on='COD_TABULACAO',
        how='left'
    ).drop(columns=['COD_TABULACAO'])
    
    col_idx = df_resultado.columns.get_loc('ACIONAMENTOS')
    df_resultado.insert(col_idx, 'TRABALHADO', 1)
    
    colunas_indicadores = ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']
    df_resultado[colunas_indicadores] = df_resultado[colunas_indicadores].fillna(0)
    
    salvar_log(f"✓ Enriquecimento concluído! ({len(df_resultado):,} registros)", arquivo_log=LOG_DISCAGENS)
    return df_resultado

@registrar_tempo("Enriquecimento com mailing e calendário", arquivo_log=LOG_DISCAGENS)
def enriquecer_com_mailing_calendario(df_discagens, df_mailing_hist, df_dw_calendario):
    """
    Enriquece o DataFrame de discagens com dados de mailing_hist e calendário.
    Retorna dois DataFrames: um com FX_ATRASO e outro sem.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame com discagens enriquecidas
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        tuple: (df_com_fx_atraso, df_sem_fx_atraso)
    """
    df_resultado = df_discagens.copy()
    
    # Padronizar CONTRATO
    df_resultado['CONTRATO'] = df_resultado['CONTRATO'].astype(str).str.upper().str.strip()
    
    # Enriquecer com mailing_hist
    df_mailing_temp = df_mailing_hist[['CONTRATO', 'DATA', 'FX_ATRASO', 'VALOR']].copy()
    df_mailing_temp['CONTRATO'] = df_mailing_temp['CONTRATO'].astype(str).str.upper().str.strip()
    
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_mailing_temp['DATA'] = pd.to_datetime(df_mailing_temp['DATA']).dt.date
    
    salvar_log(f"📊 Merge com mailing_hist... ({len(df_resultado):,} registros)", arquivo_log=LOG_DISCAGENS)
    df_resultado = df_resultado.merge(df_mailing_temp, on=['CONTRATO', 'DATA'], how='left')
    registros_sem_fx = df_resultado['FX_ATRASO'].isna().sum()
    salvar_log(f"   ⚠️  Registros sem FX_ATRASO: {registros_sem_fx:,}", arquivo_log=LOG_DISCAGENS)
    
    # Enriquecer com calendário
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date
    
    salvar_log(f"📅 Merge com dw_calendario...", arquivo_log=LOG_DISCAGENS)
    df_resultado = df_resultado.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA', right_on='dt_data', how='left'
    ).drop(columns=['dt_data'])
    
    # Separar em dois DataFrames
    df_com_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].notna()].copy()
    df_sem_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].isna()].copy()
    
    salvar_log(f"📦 COM FX_ATRASO: {len(df_com_fx_atraso):,} | SEM FX_ATRASO: {len(df_sem_fx_atraso):,}", arquivo_log=LOG_DISCAGENS)
    return df_com_fx_atraso, df_sem_fx_atraso

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

__all__ = [
    'adicionar_operacao',
    'adicionar_estado_por_ddd',
    'adicionar_origem',
    'tratar_base_discagens_expert',
    'criar_df_tabulacoes_robo',
    'enriquecer_com_tabulacoes_robo',
    'enriquecer_com_mailing_calendario',
    'segmentar_discagens_expert',
    'DDD_ESTADO'
]
