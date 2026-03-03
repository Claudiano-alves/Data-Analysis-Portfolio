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
from utils.config import LOG_DISCAGENS, DDD_ESTADO
from typing import Any, Dict, Optional, Union, Tuple

# ============================================
# FUNÇÕES DE TRATAMENTO - DISCAGENS EXPERT
# ============================================

def adicionar_operacao(
    df: pd.DataFrame,
    grupo_map: Dict[Union[int, Tuple[int, ...]], str],
    coluna_grupo: str = 'GrupoPrincipal',
    coluna_destino: str = 'OPERACAO',
    default_value: str = 'Outros'
) -> pd.DataFrame:
    """
    Adiciona coluna de operação ao DataFrame baseado em mapeamento de grupos.
    
    Args:
        df: DataFrame com coluna de grupo principal
        grupo_map: Dicionário mapeando grupos/tuplas de grupos para operações
                   Ex: {4118: "ATIVO", (4047, 4679): "URA CPC"}
        coluna_grupo: Nome da coluna contendo o ID do grupo (default: 'GrupoPrincipal')
        coluna_destino: Nome da coluna a ser criada (default: 'OPERACAO')
        default_value: Valor padrão para grupos não mapeados (default: 'Outros')
    
    Returns:
        DataFrame com nova coluna de operação
    
    Example:
        >>> grupo_map = {
        ...     4118: "ATIVO",
        ...     (4047, 4679, 4681): "URA CPC"
        ... }
        >>> df = adicionar_operacao(df, grupo_map)
    """
    conditions = []
    choices = []
    
    for grupo_ids, operacao in grupo_map.items():
        if isinstance(grupo_ids, tuple):
            # Múltiplos IDs para mesma operação
            conditions.append(df[coluna_grupo].isin(grupo_ids))
        else:
            # ID único
            conditions.append(df[coluna_grupo] == grupo_ids)
        
        choices.append(operacao)
    
    df[coluna_destino] = np.select(conditions, choices, default=default_value)
    
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

@registrar_tempo("Tratamento base discagens expert", arquivo_log=LOG_DISCAGENS)
# utils/transformations.py
def aplicar_transformacoes_discagens(
    df: pd.DataFrame,
    config: Dict[str, Any],
    df_mailing: Optional[pd.DataFrame] = None,
    df_calendario: Optional[pd.DataFrame] = None,
    log_file: str = LOG_DISCAGENS
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Aplica transformações baseadas em configuração.
    
    Args:
        df: DataFrame de discagens
        config: Dicionário de configuração
        df_tabulacoes: DataFrame de tabulações robô (obrigatório se config solicitar)
        df_mailing: DataFrame de mailing (obrigatório se config solicitar)
        df_calendario: DataFrame de calendário (obrigatório se config solicitar)
        log_file: Arquivo de log
    
    Returns:
        Tuple[DataFrame transformado, DataFrame sem FX_ATRASO (se aplicável)]
    """
    # Validar dependências
    #validar_dependencias_config(config, df_tabulacoes, df_mailing, df_calendario)
    
    df = df.copy()
    df_sem_fx_atraso = None
    
    # ============================================
    # TRANSFORMAÇÕES SIMPLES
    # ============================================
    
    # OPERACAO
    if config.get('grupo_map') is not None:
        salvar_log("   🔄 Adicionando OPERACAO...", arquivo_log=log_file)
        df = adicionar_operacao(df=df, grupo_map=config['grupo_map'])
    
    # ESTADO
    if config.get('adicionar_estado', False):
        salvar_log("   🔄 Adicionando ESTADO...", arquivo_log=log_file)
        df = adicionar_estado_por_ddd(df=df)
    
    # ============================================
    # ENRIQUECIMENTOS
    # ============================================
    
    # MAILING + CALENDÁRIO
    if config.get('enriquecer_mailing_calendario', False):
        salvar_log("   🔄 Enriquecendo com mailing e calendário...", arquivo_log=log_file)
        df, df_sem_fx_atraso = enriquecer_com_mailing_calendario(
            df, df_mailing, df_calendario
        )
    
    # ============================================
    # GARANTIR COLUNAS OBRIGATÓRIAS COM VALORES ESPECÍFICOS
    # ============================================
    colunas_obrigatorias = config.get('colunas_obrigatorias', {})
    
    # Suportar tanto dicionário quanto lista (retrocompatibilidade)
    if isinstance(colunas_obrigatorias, dict):
        # Formato novo: {'TRABALHADO': 1, 'CPC': 0}
        for coluna, valor_padrao in colunas_obrigatorias.items():
            if coluna not in df.columns:
                salvar_log(
                    f"   ➕ Criando coluna obrigatória '{coluna}' = {valor_padrao}",
                    arquivo_log=log_file
                )
                df[coluna] = valor_padrao
    else:
        # Formato antigo: ['TRABALHADO', 'CPC'] → valor padrão = 0
        for coluna in colunas_obrigatorias:
            if coluna not in df.columns:
                salvar_log(
                    f"   ➕ Criando coluna obrigatória '{coluna}' = 0",
                    arquivo_log=log_file
                )
                df[coluna] = 0
    
    salvar_log("   ✅ Transformações concluídas", arquivo_log=log_file)
    return df, df_sem_fx_atraso

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
    'enriquecer_com_mailing_calendario',
    'segmentar_discagens_expert',
    'aplicar_transformacoes_discagens'
]
