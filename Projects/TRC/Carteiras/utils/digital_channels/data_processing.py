import pandas as pd
from utils.utils import salvar_log, salvar_dataframes_digital
from ..config import LOG_CHANNELS
from typing import List, Optional, Tuple

def preparar_massivos(
    df_sms,
    df_rcs,
    df_email,
    df_whats,
    arquivo_log: Optional[str] = None,
) -> pd.DataFrame:
    """
    Une os DataFrames de canais massivos, mantendo apenas CPF e DATA únicos.
    Para CPFs iguais na mesma data, mantém apenas uma ocorrência.
    A coluna CANAL identifica a origem do registro (SMS, RCS, EMAIL, WHATS).

    Args:
        df_sms, df_rcs, df_email, df_whats (pd.DataFrame): DataFrames dos canais massivos.
            Observação: df_email usa 'DATA' como coluna de data,
                        os demais usam 'DATA_DISPARO'
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_CHANNELS

    Returns:
        pd.DataFrame: DataFrame com colunas CPF, DATA e CANAL
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
    df_massivos['CPF']  = df_massivos['CPF'].astype(str).str.strip()

    salvar_log(f"📊 Massivos unificados: {len(df_massivos):,} combinações CPF+DATA", arquivo_log=arquivo_log)
    return df_massivos

def processar_massivos(
    df_sms,
    df_rcs,
    df_email,
    df_whats,
    df_mailing_hist: pd.DataFrame,
    df_dw_calendario: pd.DataFrame,
    segmentacoes_extras: Optional[List[str]] = None,
    arquivo_log: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pipeline completo de processamento dos canais massivos.

    A separação entre registros com e sem relacionamento com mailing
    é feita dinamicamente: usa a primeira coluna de segmentacoes_extras
    se fornecida, caso contrário usa 'VALOR' (sempre presente no mailing).

    Etapas:
        1. Unir massivos (SMS, RCS, EMAIL, WHATS)
        2. Pré-deduplicar mailing (um contrato por CPF+DATA, maior ATRASO)
        3. Enriquecer com mailing (remove colunas duplicadas dos massivos antes)
        4. Enriquecer com calendário
        5. Separar com e sem relacionamento

    Args:
        df_sms, df_rcs, df_email, df_whats: DataFrames dos canais massivos
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist
        df_dw_calendario (pd.DataFrame): DataFrame de calendário
        segmentacoes_extras (list, optional): Colunas de segmentação vindas do mailing.
                                              Ex: ['PF_PJ', 'PA'] ou ['FX_ATRASO']
        arquivo_log (str): Caminho do arquivo de log. Ex: LOG_CHANNELS

    Returns:
        tuple: (df_com_relacionamento, df_sem_relacionamento)
    """
    coluna_referencia = segmentacoes_extras[0] if segmentacoes_extras else 'VALOR'

    # ============================================
    # ETAPA 1: UNIR MASSIVOS
    # ============================================
    df_massivos = preparar_massivos(df_sms, df_rcs, df_email, df_whats, arquivo_log=arquivo_log)
    salvar_log(f"📊 Massivos unificados: {len(df_massivos):,} registros", arquivo_log=arquivo_log)

    # ============================================
    # ETAPA 2: PRÉ-DEDUPLICAR MAILING
    # um contrato por CPF+DATA — maior ATRASO
    # ============================================
    df_mailing_temp = df_mailing_hist.copy()
    df_mailing_temp['DATA'] = pd.to_datetime(df_mailing_temp['DATA']).dt.date
    df_mailing_temp['CPF']  = df_mailing_temp['CPF'].astype(str).str.strip()
    df_mailing_temp = (
        df_mailing_temp
        .sort_values('ATRASO', ascending=False)
        .drop_duplicates(subset=['CPF', 'DATA'], keep='first')
        .reset_index(drop=True)
    )
    salvar_log(f"📊 Mailing pré-deduplicado: {len(df_mailing_temp):,} registros", arquivo_log=arquivo_log)

    # ============================================
    # ETAPA 3: ENRIQUECER COM MAILING
    # Remove colunas de segmentação dos massivos antes do merge
    # para evitar conflitos _x/_y — elas virão do mailing
    # ============================================
    if segmentacoes_extras:
        colunas_para_dropar = [col for col in segmentacoes_extras if col in df_massivos.columns]
        if colunas_para_dropar:
            df_massivos = df_massivos.drop(columns=colunas_para_dropar)
            salvar_log(f"   🔧 Colunas removidas dos massivos antes do merge: {colunas_para_dropar}", arquivo_log=arquivo_log)

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
    # ETAPA 5: SEPARAR COM E SEM RELACIONAMENTO
    # ============================================
    df_com_relacionamento = df_resultado[df_resultado[coluna_referencia].notna()].reset_index(drop=True)
    df_sem_relacionamento = df_resultado[df_resultado[coluna_referencia].isna()].reset_index(drop=True)

    salvar_log(f"📦 COM relacionamento: {len(df_com_relacionamento):,} | SEM relacionamento: {len(df_sem_relacionamento):,}", arquivo_log=arquivo_log)

    return df_com_relacionamento, df_sem_relacionamento

def data_channels(
    df_mailing_hist: pd.DataFrame,
    df_dw_calendario: pd.DataFrame,
    df_sms: pd.DataFrame = None,
    df_email: pd.DataFrame = None,
    df_rcs: pd.DataFrame = None,
    output_path: str = None,
    log_file: str = LOG_CHANNELS
):
    """
    Processa o cruzamento entre os dataframes para múltiplos canais de comunicação.
    
    Passos para cada canal:
    1. Remove CPFs duplicados na mesma data do df_mailing_hist, 
       mantendo o registro com maior valor
    2. Cruza canal com df_mailing_hist por CPF e DATA,
       adicionando as colunas FX_ATRASO e COD_CLI
    3. Cruza o resultado com df_dw_calendario pela DATA (dt_data),
       adicionando as colunas nr_dia_util, quartil, dt_mes e mes_abreviado
    4. Separa registros sem faixa de atraso (trabalhados fora da base mailing)
    5. Salva os dataframes processados (se output_path fornecido)
    
    Parâmetros:
    -----------
    df_mailing_hist : pd.DataFrame
        DataFrame com colunas: DATA, CONTRATO, CPF, ATRASO, COD_CLI, COD_CAR, VALOR, FX_ATRASO
    df_dw_calendario : pd.DataFrame
        DataFrame com colunas: dt_data, dt_ano, dt_mes, dt_dia, nr_dia_semana, 
        nr_dia_ano, fl_dia_util, nr_dia_util, quartil, mes_abreviado
    df_sms : pd.DataFrame, optional
        DataFrame com colunas: DATA, CPF
    df_email : pd.DataFrame, optional
        DataFrame com colunas: DATA, CPF
    df_rcs : pd.DataFrame, optional
        DataFrame com colunas: DATA, CPF
    output_path : str, optional
        Caminho do diretório onde os dataframes serão salvos
    log_file : str, optional
        Caminho do arquivo de log (padrão: LOG_CHANNELS)
    
    Retorna:
    --------
    dict
        Dicionário com os resultados de cada canal e paths dos arquivos salvos:
        {
            'sms': (df_sms_enriquecido, df_sms_sem_faixa) ou (None, None),
            'email': (df_email_enriquecido, df_email_sem_faixa) ou (None, None),
            'rcs': (df_rcs_enriquecido, df_rcs_sem_faixa) ou (None, None),
            'saved_files': {...} (se output_path fornecido)
        }
    """
    
    def processar_canal(df_canal: pd.DataFrame, nome_canal: str, df_mailing_dedup: pd.DataFrame):
        """
        Processa um único canal de comunicação.
        
        Parâmetros:
        -----------
        df_canal : pd.DataFrame
            DataFrame do canal com colunas: DATA, CPF
        nome_canal : str
            Nome do canal (para logs)
        df_mailing_dedup : pd.DataFrame
            DataFrame de mailing já deduplicado
            
        Retorna:
        --------
        tuple[pd.DataFrame, pd.DataFrame]
            (df_enriquecido, df_sem_faixa)
        """
        if df_canal is None or df_canal.empty:
            salvar_log(f"⚠️ Canal {nome_canal.upper()} não fornecido ou vazio, pulando...", arquivo_log=log_file)
            return None, None
        
        salvar_log(f"\n{'='*60}", arquivo_log=log_file)
        salvar_log(f"📊 Processando canal: {nome_canal.upper()}", arquivo_log=log_file)
        salvar_log(f"{'='*60}", arquivo_log=log_file)
        
        # Criar cópia e garantir que a coluna DATA do canal esteja no formato datetime
        df_canal_copy = df_canal.copy()
        if df_canal_copy['DATA'].dtype == 'object':
            df_canal_copy['DATA'] = pd.to_datetime(df_canal_copy['DATA'])
        
        # Passo 2: Cruzar canal com df_mailing_hist
        salvar_log(f"🔗 Passo 2: Cruzando {nome_canal} com df_mailing_hist por CPF e DATA...", arquivo_log=log_file)
        
        df_resultado = df_canal_copy.merge(
            df_mailing_dedup[['DATA', 'CPF', 'FX_ATRASO', 'COD_CLI']],
            on=['DATA', 'CPF'],
            how='left'
        )
        
        registros_enriquecidos = df_resultado['FX_ATRASO'].notna().sum()
        salvar_log(f"  📥 Registros de {nome_canal}: {len(df_canal):,}", arquivo_log=log_file)
        salvar_log(f"  ✅ Registros enriquecidos com FX_ATRASO e COD_CLI: {registros_enriquecidos:,}", arquivo_log=log_file)
        salvar_log(f"  ⚠️ Registros sem match: {len(df_resultado) - registros_enriquecidos:,}", arquivo_log=log_file)
        
        # Passo 3: Cruzar com df_dw_calendario
        salvar_log(f"📅 Passo 3: Cruzando com df_dw_calendario pela DATA...", arquivo_log=log_file)
        
        # Garantir que ambas as colunas de data estejam no mesmo formato
        df_resultado_copy = df_resultado.copy()
        df_calendario_copy = df_dw_calendario.copy()
        
        # Converter para datetime se necessário
        if df_resultado_copy['DATA'].dtype == 'object':
            df_resultado_copy['DATA'] = pd.to_datetime(df_resultado_copy['DATA'])
        if df_calendario_copy['dt_data'].dtype == 'object':
            df_calendario_copy['dt_data'] = pd.to_datetime(df_calendario_copy['dt_data'])
        
        df_final = df_resultado_copy.merge(
            df_calendario_copy[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
            left_on='DATA',
            right_on='dt_data',
            how='left'
        )
        
        # Remover coluna dt_data duplicada (já temos DATA)
        df_final = df_final.drop(columns=['dt_data'])
        
        registros_com_calendario = df_final['nr_dia_util'].notna().sum()
        salvar_log(f"  ✅ Registros enriquecidos com dados do calendário: {registros_com_calendario:,}", arquivo_log=log_file)
        salvar_log(f"  ⚠️ Registros sem match no calendário: {len(df_final) - registros_com_calendario:,}", arquivo_log=log_file)
        
        # Passo 4: Separar registros sem faixa de atraso
        salvar_log(f"🔍 Passo 4: Separando {nome_canal} sem faixa de atraso...", arquivo_log=log_file)
        
        df_enriquecido = df_final[df_final['FX_ATRASO'].notna()].copy()
        df_sem_faixa = df_final[df_final['FX_ATRASO'].isna()].copy()
        
        salvar_log(f"  ✅ {nome_canal.upper()} com faixa de atraso (base mailing): {len(df_enriquecido):,}", arquivo_log=log_file)
        salvar_log(f"  ⚠️ {nome_canal.upper()} sem faixa de atraso (fora da base mailing): {len(df_sem_faixa):,}", arquivo_log=log_file)
        
        # Resumo do canal
        salvar_log(f"\n📋 RESUMO - {nome_canal.upper()}", arquivo_log=log_file)
        salvar_log(f"📊 Total de registros processados: {len(df_final):,}", arquivo_log=log_file)
        
        if len(df_final) > 0:
            salvar_log(f"  └─ Na base mailing: {len(df_enriquecido):,} ({len(df_enriquecido)/len(df_final)*100:.2f}%)", arquivo_log=log_file)
            salvar_log(f"  └─ Fora da base mailing: {len(df_sem_faixa):,} ({len(df_sem_faixa)/len(df_final)*100:.2f}%)", arquivo_log=log_file)
        
        salvar_log(f"✅ Canal {nome_canal.upper()} processado com sucesso!", arquivo_log=log_file)
        
        return df_enriquecido, df_sem_faixa
    
    # ========== INÍCIO DO PROCESSAMENTO PRINCIPAL ==========
    
    salvar_log(f"\n{'#'*60}", arquivo_log=log_file)
    salvar_log(f"📊 INICIANDO PROCESSAMENTO DE CANAIS", arquivo_log=log_file)
    salvar_log(f"{'#'*60}", arquivo_log=log_file)
    
    # Passo 1: Remover CPFs duplicados na mesma data (comum para todos os canais)
    salvar_log(f"\n🔍 Passo 1: Removendo CPFs duplicados por DATA (mantendo maior VALOR)...", arquivo_log=log_file)
    
    df_mailing_dedup = (df_mailing_hist
                        .sort_values('VALOR', ascending=False)
                        .drop_duplicates(subset=['DATA', 'CPF'], keep='first')
                        .copy())
    
    # Garantir que a coluna DATA do mailing esteja no formato datetime
    if df_mailing_dedup['DATA'].dtype == 'object':
        df_mailing_dedup['DATA'] = pd.to_datetime(df_mailing_dedup['DATA'])
    
    registros_removidos = len(df_mailing_hist) - len(df_mailing_dedup)
    salvar_log(f"  ✂️ Registros removidos: {registros_removidos:,}", arquivo_log=log_file)
    salvar_log(f"  ✅ Registros restantes: {len(df_mailing_dedup):,}", arquivo_log=log_file)
    
    # Processar cada canal
    resultados = {}
    
    # Processar SMS
    if df_sms is not None:
        sms_enriquecido, sms_sem_faixa = processar_canal(df_sms, 'sms', df_mailing_dedup)
        resultados['sms'] = (sms_enriquecido, sms_sem_faixa)
    else:
        resultados['sms'] = (None, None)
    
    # Processar Email
    if df_email is not None:
        email_enriquecido, email_sem_faixa = processar_canal(df_email, 'email', df_mailing_dedup)
        resultados['email'] = (email_enriquecido, email_sem_faixa)
    else:
        resultados['email'] = (None, None)
    
    # Processar RCS
    if df_rcs is not None:
        rcs_enriquecido, rcs_sem_faixa = processar_canal(df_rcs, 'rcs', df_mailing_dedup)
        resultados['rcs'] = (rcs_enriquecido, rcs_sem_faixa)
    else:
        resultados['rcs'] = (None, None)
    
    # Resumo final consolidado
    salvar_log(f"\n{'#'*60}", arquivo_log=log_file)
    salvar_log(f"📋 RESUMO FINAL CONSOLIDADO", arquivo_log=log_file)
    salvar_log(f"{'#'*60}", arquivo_log=log_file)
    
    for canal, (df_enriquecido, df_sem_faixa) in resultados.items():
        if df_enriquecido is not None:
            total = len(df_enriquecido) + len(df_sem_faixa)
            salvar_log(f"\n{canal.upper()}:", arquivo_log=log_file)
            salvar_log(f"  Total: {total:,}", arquivo_log=log_file)
            salvar_log(f"  ├─ Enriquecidos: {len(df_enriquecido):,}", arquivo_log=log_file)
            salvar_log(f"  └─ Sem faixa: {len(df_sem_faixa):,}", arquivo_log=log_file)
        else:
            salvar_log(f"\n{canal.upper()}: Não processado", arquivo_log=log_file)
    
    salvar_log(f"\n✅ PROCESSAMENTO COMPLETO!", arquivo_log=log_file)
    
    # ========== SALVAR DATAFRAMES (SE output_path FORNECIDO) ==========
    
    if output_path is not None:
        salvar_log(f"\n{'#'*60}", arquivo_log=log_file)
        salvar_log(f"💾 SALVANDO DATAFRAMES", arquivo_log=log_file)
        salvar_log(f"{'#'*60}", arquivo_log=log_file)
        salvar_log(f"📁 Diretório de saída: {output_path}", arquivo_log=log_file)
        
        # Desempacotar resultados
        sms_enr, sms_sf = resultados['sms']
        email_enr, email_sf = resultados['email']
        rcs_enr, rcs_sf = resultados['rcs']
        
        # Salvar usando a função utilitária
        saved_files = salvar_dataframes_digital(
            output_path,
            sms_enr=sms_enr,
            sms_sf=sms_sf,
            email_enr=email_enr,
            email_sf=email_sf,
            rcs_enr=rcs_enr,
            rcs_sf=rcs_sf
        )
        
        resultados['saved_files'] = saved_files
        salvar_log(f"✅ Dataframes salvos com sucesso!", arquivo_log=log_file)
    else:
        salvar_log(f"\n⚠️ output_path não fornecido - dataframes não foram salvos", arquivo_log=log_file)
    
    salvar_log(f"{'#'*60}\n", arquivo_log=log_file)
    
    return resultados

# Função auxiliar para desempacotar resultados
def unpack_results(resultados: dict):
    """
    Desempacota o dicionário de resultados em variáveis individuais.
    
    Parâmetros:
    -----------
    resultados : dict
        Dicionário retornado pela função data_channels
    
    Retorna:
    --------
    tuple
        (df_sms_enriquecido, df_sms_sem_faixa,
         df_email_enriquecido, df_email_sem_faixa,
         df_rcs_enriquecido, df_rcs_sem_faixa)
    
    Exemplo:
    --------
    >>> resultados = data_channels(df_mailing, df_calendario, df_sms=df_sms, df_email=df_email)
    >>> sms_enr, sms_sf, email_enr, email_sf, rcs_enr, rcs_sf = unpack_results(resultados)
    """
    return (
        *resultados['sms'],
        *resultados['email'],
        *resultados['rcs']
    )