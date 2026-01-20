from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

def get_query_discagens(dt_ini, dt_fim, where_clause=""):
    """
    Retorna a query SQL para buscar discagens com período parametrizado e filtros customizados
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE customizada (opcional)
                           Ex: "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA IN (19, 30))"
    
    Returns:
        str: Query SQL formatada com as datas, filtros e tabela dinâmica
    """
    # Converter a data inicial para extrair ano e mês
    data_obj = datetime.strptime(dt_ini, '%Y-%m-%d')
    ano = data_obj.year
    mes = data_obj.strftime('%m')  # Formato com zero à esquerda (01, 02, etc.)
    
    # Gerar nome da tabela dinamicamente
    tabela = f"totalinfo_{ano}_{mes}"
    
    # Adicionar WHERE se houver cláusula
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    
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
    LEFT JOIN tabulacaooper c ON a.CallID = c.callid AND a.GrupoPrincipal = c.codgrupo
    {where_sql}
    ')
    """
    return query

def get_query_mailing_hist(dt_ini, dt_fim, where_clause=""):
    """
    Retorna a query SQL para buscar mailing_hist com período parametrizado e filtros customizados
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                           Ex: "COD_CLI IN(196,198,228)"
                           Nota: O filtro de DATA BETWEEN é sempre aplicado
    
    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    # Filtro de data é obrigatório
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    
    # Adicionar filtro customizado se houver
    if where_clause:
        where_completo = f"{where_clause} AND {where_data}"
    else:
        where_completo = where_data
    
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
    {where_completo}
    """
    return query

def get_query_base_acionamentos(dt_ini, dt_fim, where_clause=""):
    """
    Retorna a query SQL para buscar acionamentos com período parametrizado e filtros customizados
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                           Ex: "((C.COD_CLI = 198 AND C.COD_CAR IN (1, 2, 3)) OR (C.COD_CLI = 196 AND C.COD_CAR IN (1, 3, 4)))"
                           Nota: O filtro de DATA_ACIONA BETWEEN é sempre aplicado
    
    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    # Filtro de data é obrigatório
    where_data = f"CAST(A.DATA_ACIONA AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'"
    
    # Adicionar filtro customizado se houver
    if where_clause:
        where_completo = f"{where_clause} AND {where_data}"
    else:
        where_completo = where_data
    
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
        INNER JOIN CAD_DEVF C ON A.CONTRATO_FIN = C.CONTRATO_FIN
        {where_completo}
    """
    return query

def get_query_tabulacao_aciona(where_completo):
    query = f"""
        SELECT 
            COD_ACIONA,
            DESC_ACIONA,
            CPC,
            CPCA,
            PROMESSA
        FROM ACIONAMENTO_CARTEIRA
        {where_completo}
    """
    return query

def get_query_discagens_trestto(dt_ini, dt_fim):
    """
    Retorna a query SQL para buscar discagens do Trestto (Robô)
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
    
    Returns:
        str: Query SQL formatada com as datas
    """
    query = f"""
    SELECT  
        DATA, 
        CPF, 
        SUBSTATUSURA, 
        'ROBÔ' TIPO, 
        DISCAGEM, 
        ALO, 
        CPC, 
        CPCA, 
        PROMESSA  
    FROM DISCAGENS_TRESTTO 
    WHERE DATA BETWEEN '{dt_ini}' AND '{dt_fim}'
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

def get_query_pagamentos(dt_ini, dt_fim, where_clause=""):
    """
    Retorna a query SQL para buscar pagamentos com período parametrizado e filtros customizados
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                           Ex: "B.COD_CLI IN(198, 196, 228)"
                           Nota: O filtro de DATA_PAGTO BETWEEN é sempre aplicado
    
    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    # Filtro de data é obrigatório
    where_data = f"CAST(R.DATA_PAGTO AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'"
    
    # Adicionar filtro customizado se houver
    if where_clause:
        where_completo = f"{where_clause} AND {where_data}"
    else:
        where_completo = where_data
    
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
        INNER JOIN CAD_DEVF B WITH (NOLOCK) ON B.CONTRATO_FIN = R.CONTRATO_FIN
        {where_completo}
    """
    return query

def get_query_acordos(dt_ini, dt_fim, where_clause=""):
    """
    Retorna a query SQL para buscar acordos com período parametrizado e filtros customizados
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                           Ex: "B.COD_CLI IN(198, 196, 228)"
                           Ex: "B.COD_CLI = 198 AND B.COD_CAR IN (1, 2, 3)"
                           Nota: O filtro de DTACORDOHORA_ACO BETWEEN é sempre aplicado
    
    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    # Filtro de data é obrigatório
    where_data = f"CAST(A.DTACORDOHORA_ACO AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'"
    
    # Adicionar filtro customizado se houver
    if where_clause:
        where_completo = f"{where_clause} AND {where_data}"
    else:
        where_completo = where_data
    
    query = f"""
        SELECT DISTINCT
            B.CONTRATO_FIN,
            B.CPF_DEV,
            CAST(A.DTACORDOHORA_ACO AS DATE) DATA_ACORDO,
            CAST(P.VENC_ACOP AS DATE) VENC_PARCELA,
            CAST(A.DTCANCELAMENTOACO_ACO AS DATE) CANC_ACORDO,
            A.NACORDO_ACO,
            A.VLRACORDO_ACO,
            P.VALOR_ACOP,
            A.RECUP_ACO,
            P.PARCELA_ACOP,
            A.COD_STAC,
            Q.NOME_RECUP AS RECUPERADOR,
            B.COD_CLI,
            B.COD_CAR,
            B.ATRASO_FIN
        FROM CAD_ACO A WITH (NOLOCK)
        INNER JOIN CAD_DEVF B WITH (NOLOCK) ON B.CONTRATO_FIN = A.CONTRATO_FIN 
        INNER JOIN CAD_ACOP P WITH (NOLOCK) ON P.CONTRATO_FIN = A.CONTRATO_FIN AND A.NACORDO_ACO = P.NACORDO_ACO AND P.PARCELA_ACOP = 1
        INNER JOIN CAD_RECUP Q WITH (NOLOCK) ON A.RECUP_ACO = Q.COD_RECUP
        {where_completo}
    """
    return query

def get_query_sms(dt_ini, dt_fim, where_clause=""):
    """
    Retorna a query SQL para buscar massivos_sms com período parametrizado e filtros customizados
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                           Ex: "COD_CLI IN(198, 196, 228)"
                           Nota: O filtro de DATA_ENVIO BETWEEN é sempre aplicado
    
    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    # Filtro de data é obrigatório
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    
    # Adicionar filtro customizado se houver
    if where_clause:
        where_completo = f"{where_clause} AND {where_data}"
    else:
        where_completo = where_data
    
    query = f"""
        SELECT 
            DATA,
            CPF
        FROM SMS
        {where_completo}
    """
    return query

def get_query_rcs(dt_ini, dt_fim, where_clause=""):
    """
    Retorna a query SQL para buscar massivos_rcs com período parametrizado e filtros customizados
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                           Ex: "COD_CLI IN(198, 196, 228)"
                           Nota: O filtro de DATA_ENVIO BETWEEN é sempre aplicado
    
    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    # Filtro de data é obrigatório
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    
    # Adicionar filtro customizado se houver
    if where_clause:
        where_completo = f"{where_clause} AND {where_data}"
    else:
        where_completo = where_data
    
    query = f"""
        SELECT 
            DATA,
            CPF
        FROM SMS
        {where_completo}
    """
    return query

def get_query_email(dt_ini, dt_fim, where_clause=""):
    """
    Retorna a query SQL para buscar massivos_email com período parametrizado e filtros customizados
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                           Ex: "COD_CLI IN(198, 196, 228)"
                           Nota: O filtro de DATA_ENVIO BETWEEN é sempre aplicado
    
    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    # Filtro de data é obrigatório
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    
    # Adicionar filtro customizado se houver
    if where_clause:
        where_completo = f"{where_clause} AND {where_data}"
    else:
        where_completo = where_data
    
    query = f"""
        SELECT 
            DATA,
            CPF
        FROM EMAIL
        {where_completo}
    """
    return query

def get_query_telefone(where_clause=""):
    """
    Retorna a query SQL para buscar telefones com filtros customizados
    
    Args:
        where_clause (str): Cláusula WHERE customizada (opcional)
                           Ex: "COD_CLI IN(198, 196, 228)"
                           Ex: "CPF_DEV IN ('12345678900', '98765432100')"
    
    Returns:
        str: Query SQL formatada com os filtros
    """
    # Adicionar WHERE se houver cláusula
    where_sql = f"{where_clause}" if where_clause else ""
    
    query = f"""
        SELECT 
            CPF_DEV,
            DDD_TEL,
            TEL_TEL,
            PERC_TEL,
            COD_TIPO,
            POSSUIWHATSAPP_TEL
        FROM CAD_DEVT WITH (NOLOCK)
        {where_sql}
    """
    return query

def get_query_blacklist_expert(where_clause=""):
    """
    Retorna a query SQL para buscar telefones da blacklist com filtros customizados
    
    Args:
        where_clause (str): Cláusula WHERE customizada (opcional)
                           Ex: "DDD = '16'"
                           Ex: "DDD IN ('11', '16', '19')"
    
    Returns:
        str: Query SQL formatada com os filtros
    """
    # Adicionar WHERE se houver cláusula
    # where_sql = f"WHERE {where_clause}" if where_clause else ""
    
    query = f"""
        SELECT 
            DDD, 
            TELEFONE
        FROM OPENQUERY(EXPERT, '
            SELECT 
                A.DDD, 
                A.TELEFONE
            FROM blacklist A
        ')
    """
    return query