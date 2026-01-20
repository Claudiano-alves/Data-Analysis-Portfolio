from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

def get_query_discagens(dt_ini, dt_fim):
    """
    Retorna a query SQL para buscar discagens com período parametrizado
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
    
    Returns:
        str: Query SQL formatada com as datas e tabela dinâmica
    """
    # Converter a data inicial para extrair ano e mês
    data_obj = datetime.strptime(dt_ini, '%Y-%m-%d')
    ano = data_obj.year
    mes = data_obj.strftime('%m')  # Formato com zero à esquerda (01, 02, etc.)
    
    # Gerar nome da tabela dinamicamente
    tabela = f"totalinfo_{ano}_{mes}"
    
    query = f"""
    SELECT 
        * 
    FROM OPENQUERY (EXPERT,'
    SELECT
        DATE(A.instante) DATA,
        A.id,
        A.chave1 AS CONTRATO,
        A.Chave3 AS CPF,
        A.ddd,
        A.fone,
        A.GrupoPrincipal,
        A.UltCodSigRecPublica,
        A.ResultadoClassificacao,
        A.MotivoEncerramentoBilhete,
        A.Instante200OKPub,
        A.Agente,
        A.tempoconversacao_ms,
        c.codtabulacao
    FROM {tabela} A
    LEFT JOIN tabulacaooper			c ON a.CallID = c.callid AND a.GrupoPrincipal = c.codgrupo
    WHERE A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA = 141)
    ')
    """
    return query

def get_query_mailing_hist(dt_ini, dt_fim):
    """
    Retorna a query SQL para buscar mailing_hist com período parametrizado
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
    
    Returns:
        str: Query SQL formatada com as datas
    """
    query = f"""
    SELECT 
        DATA,
        UPPER(LTRIM(RTRIM(CONTRATO))) AS CONTRATO,
        LTRIM(RTRIM(CPF)) AS CPF,
        ATRASO,
        COD_CLI,
        COD_CAR,
        VALOR
    FROM MAILING_HIST 
    WHERE DATA BETWEEN '{dt_ini}' AND '{dt_fim}'
    AND COD_CLI = 253
    """
    return query

def get_query_base_acionamentos(dt_ini, dt_fim):
    query = f"""
        SELECT 
            CAST(A.DATA_ACIONA AS DATE) AS DATA_ACIONA,
            CAST(A.DATA_ACIONA AS TIME) AS HORA,
            UPPER(LTRIM(RTRIM(A.CONTRATO_FIN))) AS CONTRATO_FIN,
            LTRIM(RTRIM(C.CPF_DEV)) AS CPF_DEV,
            LTRIM(RTRIM(A.COD_ACIONAMENTO)) AS COD_ACIONA,
            LTRIM(RTRIM(B.DESC_ACIONAMENTO)) AS DESC_ACIONAMENTO,
            LTRIM(RTRIM(REC.COD_RECUP)) AS COD_RECUP,
            LTRIM(RTRIM(REC.NOME_RECUP)) AS NOME_RECUP,
            LTRIM(RTRIM(REC.LOGIN_RECUP)) AS LOGIN_RECUP,
            LTRIM(RTRIM(REC.ULTGRUPO_RECUP)) AS ULTGRUPO_RECUP,
            LTRIM(RTRIM(C.COD_CLI)) AS COD_CLI,
            C.VALORPRIN_FIN AS VALORPRIN_FIN,
            LTRIM(RTRIM(C.STATCONT_FIN)) AS STATCONT_FIN,
            C.DTDEVOL_FIN AS DTDEVOL_FIN,
            C.DTENTRADA_FIN AS DTENTRADA_FIN,
            LTRIM(RTRIM(B.CLASSIFICACAO_ACIONAMENTO)) AS CLASSIFICACAO_ACIONAMENTO
        FROM ACIONA A 
        LEFT JOIN CAD_ACIONAMENTO B ON A.COD_ACIONAMENTO = B.COD_ACIONAMENTO
        LEFT JOIN CAD_RECUP REC ON REC.COD_RECUP = A.COD_RECUP
        INNER JOIN CAD_DEVF       C ON A.CONTRATO_FIN = C.CONTRATO_FIN
        WHERE C.COD_CLI = 253
        AND CAST(A.DATA_ACIONA AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'
    """
    return query

def get_query_dw_calendario(dt_ini, dt_fim):
    query = f"""
        SELECT 
            *
        FROM DW_CALENDARIO 
        WHERE FL_DIA_UTIL = 1
        AND DT_DATA BETWEEN '{dt_ini}' AND '{dt_fim}'
    """
    return query

def get_query_pagamentos(dt_ini, dt_fim):
    query = f"""
        SELECT 
            CAST(R.DATA_PAGTO AS DATE) AS DATA_PAGTO,
            R.VALOR_PARC,
            R.NACORDO_ACO,
            R.CONTRATO_FIN,
            B.CPF_DEV,
            B.COD_CLI,
            B.VALORPRIN_FIN,
            B.VALOR_FIN,
            B.STATCONT_FIN,
            B.DTDEVOL_FIN
        FROM RECIBOCREDOR R
        INNER JOIN CAD_DEVF		B WITH (NOLOCK) ON B.CONTRATO_FIN = R.CONTRATO_FIN
        WHERE CAST(R.DATA_PAGTO AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'
        AND B.COD_CLI = 253
    """
    return query

def get_query_acordos(dt_ini, dt_fim):
    query = f"""
        SELECT 
            CAST(A.DTACORDOHORA_ACO AS DATE) DATA_ACORDO,       
            CAST(A.DTACORDOHORA_ACO AS TIME) HORA_ACORDO,       
            B.CPF_DEV,
            A.CONTRATO_FIN,
            CAST(A.DTCANCELAMENTOACO_ACO AS DATE) CANC_ACORDO, 
            A.NACORDO_ACO,                                         
            A.VLRACORDO_ACO, 
            B.VALORPRIN_FIN,
            B.DTDEVOL_FIN,
            A.RECUP_ACO,
            Q.NOME_RECUP AS RECUPERADOR,
        FROM CAD_ACO A
        INNER JOIN CAD_DEVF		B WITH (NOLOCK) ON B.CONTRATO_FIN = A.CONTRATO_FIN
        INNER JOIN CAD_ACOP		P ON P.CONTRATO_FIN = A.CONTRATO_FIN AND A.NACORDO_ACO = P.NACORDO_ACO AND P.PARCELA_ACOP = 1
		INNER JOIN CAD_RECUP    Q ON A.RECUP_ACO = Q.COD_RECUP
        WHERE CAST(A.DTACORDOHORA_ACO AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'
        AND B.COD_CLI = 253
    """
    return query

def get_query_email(dt_ini, dt_fim):
    query = f"""
        SELECT 
            DATA,
            CPF
        FROM EMAIL
        WHERE ID_CAR = 100 AND DATA_ENVIO BETWEEN '{dt_ini}' AND '{dt_fim}'
    """
    return query

def get_query_sms(dt_ini, dt_fim):
    query = f"""
        SELECT 
            DATA,
            CPF
        FROM SMS
        WHERE ID_CAR = 100 AND DATA_ENVIO BETWEEN '{dt_ini}' AND '{dt_fim}'
    """
    return query

def get_query_rsc(dt_ini, dt_fim):
    query = f"""
        SELECT 
            DATA,
            CPF
        FROM RSC
        WHERE ID_CAR = 100 AND DATA_ENVIO BETWEEN '{dt_ini}' AND '{dt_fim}'
    """
    return query