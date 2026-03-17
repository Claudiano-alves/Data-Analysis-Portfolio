import pandas as pd
from typing import List, Optional

from utils.utils import salvar_log, registrar_tempo
from utils.discagens.expert.metricas_acumuladas import (
    discagens_segmentacoes_funil,
    discagens_unique_funil,
    discagens_esforco_funil,
    discagens_segmentacoes_daily,
    discagens_unique_daily,
    discagens_esforco_daily,
)

DATA_ENCERRAMENTO_INVALIDA = '1899-12-30'
COD_SIP_RINGING = '200'

def _filtrar_ringing(df: pd.DataFrame, arquivo_log: Optional[str] = None) -> pd.DataFrame:
    """
    Aplica os filtros de Ringing no DataFrame de discagens.

    Filtros:
        - DATA_ENCERRAMENTO != '1899-12-30'
        - COD_SIP = 200

    Args:
        df: DataFrame de discagens
        arquivo_log: Caminho do arquivo de log

    Returns:
        DataFrame filtrado
    """
    df_filtrado = df[
        (df['DATA_ENCERRAMENTO'].dt.normalize() != pd.Timestamp('1899-12-30')) &
        (df['COD_SIP'].astype(str) == '200')
    ].copy()

    salvar_log(f"🔍 Filtro Ringing aplicado: {len(df):,} → {len(df_filtrado):,} registros", arquivo_log=arquivo_log)
    return df_filtrado

def _renomear_indicador(df: pd.DataFrame, indicador: str) -> pd.DataFrame:
    """Substitui o valor da coluna Indicador pelo nome definido."""
    df = df.copy()
    df['Indicador'] = indicador
    return df

def processar_acumulados_ringing(
    df_discagens: pd.DataFrame,
    segmentacoes: List[str],
    consolidado: bool = True,
    arquivo_log: Optional[str] = None,
):
    """
    Orquestra o cálculo de todas as métricas de Ringing.

    Aplica o filtro uma única vez e calcula funil e daily para
    segmentacoes, unique e esforço.

    Args:
        df_discagens (pd.DataFrame): DataFrame já com colunas de calendário
        segmentacoes (list): Colunas de segmentação. Ex: ['PF_PJ', 'PA']
        consolidado (bool): Se True, retorna todos os DataFrames concatenados em um só.
                            Se False, retorna um dicionário com cada métrica separada.
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_DISCAGENS

    Returns:
        Se consolidado=True:
            pd.DataFrame: Todos os cálculos concatenados
        Se consolidado=False:
            dict com chaves:
                'funil_seg', 'funil_unique', 'funil_esforco',
                'daily_seg', 'daily_unique', 'daily_esforco'
    """
    # Filtro aplicado uma única vez
    df_filtrado = _filtrar_ringing(df_discagens, arquivo_log=arquivo_log)

    salvar_log("📊 Calculando métricas Ringing...", arquivo_log=arquivo_log)

    resultados = {
        'funil_seg':     _renomear_indicador(discagens_segmentacoes_funil(df_filtrado, segmentacoes=segmentacoes, arquivo_log=arquivo_log), 'Ringing'),
        'funil_unique':  _renomear_indicador(discagens_unique_funil(df_filtrado,       segmentacoes=segmentacoes, arquivo_log=arquivo_log), 'Ringing'),
        'funil_esforco': _renomear_indicador(discagens_esforco_funil(df_filtrado,      segmentacoes=segmentacoes, arquivo_log=arquivo_log), 'Ringing'),
        'daily_seg':     _renomear_indicador(discagens_segmentacoes_daily(df_filtrado, segmentacoes=segmentacoes, arquivo_log=arquivo_log), 'Ringing'),
        'daily_unique':  _renomear_indicador(discagens_unique_daily(df_filtrado,       segmentacoes=segmentacoes, arquivo_log=arquivo_log), 'Ringing'),
        'daily_esforco': _renomear_indicador(discagens_esforco_daily(df_filtrado,      segmentacoes=segmentacoes, arquivo_log=arquivo_log), 'Ringing'),
    }

    if consolidado:
        return pd.concat(list(resultados.values()), ignore_index=True)
    return resultados