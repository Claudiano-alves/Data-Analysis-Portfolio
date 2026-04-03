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
from typing import Any, Dict, Optional, Union, Tuple, List

# ============================================
# FUNÇÕES DE TRATAMENTO - DISCAGENS EXPERT
# ============================================

def adicionar_operacao(
    df: pd.DataFrame,
    grupo_map: Dict[Union[int, Tuple[int, ...]], str],
    coluna_grupo: str = 'CAMPANHA',
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

def adicionar_estado_por_ddd(df, coluna_ddd='DDD'):
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

def enriquecer_com_mailing_calendario(
    df_discagens,
    df_mailing_hist,
    df_dw_calendario,
    segmentacoes_extras=None,
    arquivo_log=None,
):
    """
    Enriquece o DataFrame de discagens com dados de mailing_hist e calendário.

    A separação entre registros que cruzaram e não cruzaram com o mailing
    é feita dinamicamente: usa a primeira coluna de segmentacoes_extras se
    fornecida, caso contrário usa 'VALOR' (sempre presente no mailing).

    Args:
        df_discagens (pd.DataFrame): DataFrame com discagens
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
        segmentacoes_extras (list, optional): Colunas de segmentação vindas do mailing.
                                              Ex: ['PF_PJ', 'PA'] ou ['FX_ATRASO', 'FAIXA']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        tuple: (df_com_relacionamento, df_sem_relacionamento)
    """
    @registrar_tempo("Enriquecimento com mailing e calendário", arquivo_log=arquivo_log)
    def _executar():
        # Coluna de referência para detectar se cruzou com mailing
        coluna_referencia = segmentacoes_extras[0] if segmentacoes_extras else 'VALOR'

        df_resultado = df_discagens.copy()

        df_resultado['CONTRATO'] = df_resultado['CONTRATO'].astype(str).str.upper().str.strip()

        df_mailing_temp = df_mailing_hist.copy()
        df_mailing_temp['CONTRATO'] = df_mailing_temp['CONTRATO'].astype(str).str.upper().str.strip()

        df_resultado['DATA']     = pd.to_datetime(df_resultado['DATA']).dt.date
        df_mailing_temp['DATA']  = pd.to_datetime(df_mailing_temp['DATA']).dt.date

        salvar_log(f"📊 Merge com mailing_hist... ({len(df_resultado):,} registros)", arquivo_log=arquivo_log)
        df_resultado = df_resultado.merge(df_mailing_temp, on=['CONTRATO', 'DATA'], how='left')

        registros_sem_relacionamento = df_resultado[coluna_referencia].isna().sum()
        salvar_log(f"   ⚠️  Registros sem relacionamento ({coluna_referencia}): {registros_sem_relacionamento:,}", arquivo_log=arquivo_log)

        # CPF_x = discagens, CPF_y = mailing
        # Para quem cruzou: usa CPF do mailing (CPF_y)
        # Para quem não cruzou: usa CPF das discagens (CPF_x)
        if 'CPF_x' in df_resultado.columns and 'CPF_y' in df_resultado.columns:
            df_resultado['CPF'] = df_resultado['CPF_y'].fillna(df_resultado['CPF_x'])
            df_resultado = df_resultado.drop(columns=['CPF_x', 'CPF_y'])

        df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
        df_dw_calendario_temp = df_dw_calendario.copy()
        df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date

        salvar_log(f"📅 Merge com dw_calendario...", arquivo_log=arquivo_log)
        df_resultado = df_resultado.merge(
            df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
            left_on='DATA', right_on='dt_data', how='left'
        ).drop(columns=['dt_data'])

        df_com_relacionamento  = df_resultado[df_resultado[coluna_referencia].notna()].copy()
        df_sem_relacionamento  = df_resultado[df_resultado[coluna_referencia].isna()].copy()

        salvar_log(f"📦 COM relacionamento: {len(df_com_relacionamento):,} | SEM relacionamento: {len(df_sem_relacionamento):,}", arquivo_log=arquivo_log)

        return df_com_relacionamento, df_sem_relacionamento

    return _executar()

def aplicar_transformacoes_discagens(
    df: pd.DataFrame,
    config: Dict[str, Any],
    df_mailing: Optional[pd.DataFrame] = None,
    df_calendario: Optional[pd.DataFrame] = None,
    segmentacoes_extras: Optional[List[str]] = None,
    arquivo_log: Optional[str] = None,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Aplica transformações baseadas em configuração.
 
    Args:
        df: DataFrame de discagens
        config: Dicionário de configuração. Chaves suportadas:
                - grupo_map (dict): Mapeamento para coluna OPERACAO
                - adicionar_estado (bool): Adiciona coluna ESTADO por DDD
                - enriquecer_mailing_calendario (bool): Faz merge com mailing e calendário
        df_mailing: DataFrame de mailing (obrigatório se config solicitar)
        df_calendario: DataFrame de calendário (obrigatório se config solicitar)
        segmentacoes_extras: Colunas de segmentação vindas do mailing.
                             Ex: ['PF_PJ', 'PA'] ou ['FX_ATRASO']
        arquivo_log: Caminho do arquivo de log. Ex: LOG_DISCAGENS
 
    Returns:
        Tuple[DataFrame transformado, DataFrame sem relacionamento (se aplicável)]
    """
    @registrar_tempo("Tratamento base discagens expert", arquivo_log=arquivo_log)
    def _executar():
        df_local = df.copy()
        df_sem_relacionamento = None
 
        # OPERACAO
        if config.get('grupo_map') is not None:
            salvar_log("   🔄 Adicionando OPERACAO...", arquivo_log=arquivo_log)
            df_local = adicionar_operacao(df=df_local, grupo_map=config['grupo_map'])
 
        # ESTADO
        if config.get('adicionar_estado', False):
            salvar_log("   🔄 Adicionando ESTADO...", arquivo_log=arquivo_log)
            df_local = adicionar_estado_por_ddd(df=df_local)
 
        # MAILING + CALENDÁRIO
        if config.get('enriquecer_mailing_calendario', False):
            salvar_log("   🔄 Enriquecendo com mailing e calendário...", arquivo_log=arquivo_log)
            df_local, df_sem_relacionamento = enriquecer_com_mailing_calendario(
                df_discagens=df_local,
                df_mailing_hist=df_mailing,
                df_dw_calendario=df_calendario,
                segmentacoes_extras=segmentacoes_extras,
                arquivo_log=arquivo_log,
            )
 
        salvar_log("   ✅ Transformações concluídas", arquivo_log=arquivo_log)
        return df_local, df_sem_relacionamento
 
    return _executar()

__all__ = [
    'adicionar_operacao',
    'adicionar_estado_por_ddd',
    'enriquecer_com_mailing_calendario',
    'aplicar_transformacoes_discagens'
]
