import pandas as pd
from utils.utils import salvar_log
from ..config import LOG_CHANNELS


def data_channels(
    df_mailing_hist: pd.DataFrame,
    df_dw_calendario: pd.DataFrame,
    df_sms: pd.DataFrame = None,
    df_email: pd.DataFrame = None,
    df_rcs: pd.DataFrame = None,
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
    log_file : str, optional
        Caminho do arquivo de log (padrão: LOG_CHANNELS)
    
    Retorna:
    --------
    dict
        Dicionário com os resultados de cada canal:
        {
            'sms': (df_sms_enriquecido, df_sms_sem_faixa) ou (None, None),
            'email': (df_email_enriquecido, df_email_sem_faixa) ou (None, None),
            'rcs': (df_rcs_enriquecido, df_rcs_sem_faixa) ou (None, None)
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