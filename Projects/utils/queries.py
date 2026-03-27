from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from datetime import datetime

def get_query_discagens_(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar discagens com período parametrizado e filtros customizados.

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE customizada (opcional)
                            Ex: "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA IN (19, 30))"
        columns (str): Colunas do SELECT (opcional). Se None, usa SELECT *.

    Returns:
        str: Query SQL formatada com as datas, filtros e tabela dinâmica
    """
    data_obj = datetime.strptime(dt_ini, '%Y-%m-%d')
    ano = data_obj.year
    mes = data_obj.strftime('%m')
    tabela = f"totalinfo_{ano}_{mes}"

    where_sql = f"WHERE {where_clause}" if where_clause else ""

    return f"""
    SELECT * FROM OPENQUERY (EXPERT,'
    SELECT
        {columns or '*'}
    FROM {tabela} A
    {where_sql}
    ')
    """

def get_query_discagens(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar discagens.

    Lógica de filtro por período:
        - range <= 15 dias → tabela dinâmica + filtro de instante
        - range > 15 dias  → tabela dinâmica apenas (mês inteiro)

    Args:
        dt_ini (str)       : Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str)       : Data final no formato 'YYYY-MM-DD'
        where_clause (str) : Cláusula WHERE customizada (opcional)
        columns (str)      : Colunas do SELECT (opcional). Se None, usa SELECT *
    """
    data_ini = datetime.strptime(dt_ini, '%Y-%m-%d')
    data_fim_obj = datetime.strptime(dt_fim, '%Y-%m-%d')
    range_dias = (data_fim_obj - data_ini).days + 1

    ano = data_ini.year
    mes = data_ini.strftime('%m')
    tabela = f"totalinfo_{ano}_{mes}"

    # monta cláusula WHERE
    partes_where = []

    if where_clause:
        partes_where.append(where_clause)

    if range_dias <= 15:
        dt_ini_fmt = f"{dt_ini} 00:00:00"
        dt_fim_fmt = f"{dt_fim} 23:59:59"
        partes_where.append(
            f"A.instante BETWEEN ''{dt_ini_fmt}'' AND ''{dt_fim_fmt}''"
        )

    where_sql = f"WHERE {' AND '.join(partes_where)}" if partes_where else ""

    return f"""
    SELECT * FROM OPENQUERY (EXPERT,'
    SELECT
        {columns or '*'}
    FROM {tabela} A
    {where_sql}
    ')
    """

def get_query_mailing_hist(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar mailing_hist com período parametrizado e filtros customizados.

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                            Ex: "COD_CLI IN(196,198,228)"
                            Nota: O filtro de DATA BETWEEN é sempre aplicado
        columns (str): Colunas do SELECT (opcional). Se None, usa SELECT *.

    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    where_completo = f"{where_clause} AND {where_data}" if where_clause else where_data

    return f"""
    SELECT 
        {columns or '*'}
    FROM MAILING_HIST 
    {where_completo}
    """

def get_query_base_acionamentos(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar acionamentos com período parametrizado e filtros customizados.

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                            Ex: "((C.COD_CLI = 198 AND C.COD_CAR IN (1, 2, 3)) OR (C.COD_CLI = 196 AND C.COD_CAR IN (1, 3, 4)))"
                            Nota: O filtro de DATA_ACIONA BETWEEN é sempre aplicado
        columns (str): Colunas do SELECT (opcional). Se None, usa SELECT *.

    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    where_data = f"CAST(A.DATA_ACIONA AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'"
    where_completo = f"{where_clause} AND {where_data}" if where_clause else where_data

    return f"""
        SELECT 
            {columns or '*'}
        FROM ACIONA A 
        LEFT JOIN CAD_ACIONAMENTO B ON A.COD_ACIONAMENTO = B.COD_ACIONAMENTO
        LEFT JOIN CAD_RECUP REC ON REC.COD_RECUP = A.COD_RECUP
        INNER JOIN CAD_DEVF C ON A.CONTRATO_FIN = C.CONTRATO_FIN
        {where_completo}
    """

def get_query_pagamentos(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar pagamentos com período parametrizado e filtros customizados.

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                            Ex: "B.COD_CLI IN(198, 196, 228)"
                            Nota: O filtro de DATA_PAGTO BETWEEN é sempre aplicado
        columns (str): Colunas do SELECT (opcional). Se None, usa SELECT *.

    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    where_data = f"CAST(R.DATA_PAGTO AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'"
    where_completo = f"{where_clause} AND {where_data}" if where_clause else where_data

    return f"""
        SELECT 
            {columns or '*'}
        FROM RECIBOCREDOR R
        INNER JOIN CAD_DEVF B WITH (NOLOCK) ON B.CONTRATO_FIN = R.CONTRATO_FIN
        {where_completo}
    """

def get_query_acordos(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar acordos com período parametrizado e filtros customizados.

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                            Ex: "B.COD_CLI IN(198, 196, 228)"
                            Nota: O filtro de DTACORDOHORA_ACO BETWEEN é sempre aplicado
        columns (str): Colunas do SELECT (opcional). Se None, usa SELECT *.

    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    where_data = f"CAST(A.DTACORDOHORA_ACO AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'"
    where_completo = f"{where_clause} AND {where_data}" if where_clause else where_data

    return f"""
        SELECT DISTINCT
            {columns or '*'}
        FROM CAD_ACO A WITH (NOLOCK)
        INNER JOIN CAD_DEVF B WITH (NOLOCK) ON B.CONTRATO_FIN = A.CONTRATO_FIN 
        INNER JOIN CAD_ACOP P WITH (NOLOCK) ON P.CONTRATO_FIN = A.CONTRATO_FIN AND A.NACORDO_ACO = P.NACORDO_ACO AND P.PARCELA_ACOP = 1
        INNER JOIN CAD_RECUP Q WITH (NOLOCK) ON A.RECUP_ACO = Q.COD_RECUP
        {where_completo}
    """

def get_query_sms(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar massivos_sms com período parametrizado e filtros customizados.

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
        columns (str): Colunas do SELECT (opcional). Se None, usa SELECT *.

    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    where_completo = f"{where_clause} AND {where_data}" if where_clause else f"WHERE {where_data}"

    return f"""
        SELECT 
            {columns or '*'}
        FROM SMS
        {where_completo}
    """

def get_query_rcs(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar massivos_rcs com período parametrizado e filtros customizados.

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
        columns (str): Colunas do SELECT (opcional). Se None, usa SELECT *.

    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    where_completo = f"{where_clause} AND {where_data}" if where_clause else f"WHERE {where_data}"

    return f"""
        SELECT 
            {columns or '*'}
        FROM RCS
        {where_completo}
    """

def get_query_email(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar massivos_email com período parametrizado e filtros customizados.

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
        columns (str): Colunas do SELECT (opcional). Se None, usa SELECT *.

    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    where_completo = f"{where_clause} AND {where_data}" if where_clause else f"WHERE {where_data}"

    return f"""
        SELECT 
            {columns or '*'}
        FROM EMAIL
        {where_completo}
    """

def get_query_whats(dt_ini, dt_fim, where_clause="", columns=None):
    """
    Retorna a query SQL para buscar massivos_whatsapp com período parametrizado e filtros customizados.

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
        columns (str): Colunas do SELECT (opcional). Se None, usa SELECT *.

    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    where_completo = f"{where_clause} AND {where_data}" if where_clause else f"WHERE {where_data}"

    return f"""
        SELECT 
            {columns or '*'}
        FROM WHATS
        {where_completo}
    """


# ============================================================
# Queries fixas — sem suporte a colunas por parâmetro
# ============================================================

def get_query_tabulacao_aciona(where_completo):
    return f"""
        SELECT 
            IDTABCRM,
            DESCR,
            CPC,
            CPCA,
            PROMESSA
        FROM ACIONAMENTOSREGRA
        {where_completo}
    """

def get_query_discagens_trestto(dt_ini, dt_fim):
    """
    Retorna a query SQL para buscar discagens do Trestto (Robô).

    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'

    Returns:
        str: Query SQL formatada com as datas
    """
    return f"""
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

def get_query_dw_calendario(dt_ini, dt_fim):
    return f"""
        SELECT 
            *
        FROM DW_CALENDARIO 
        WHERE FL_DIA_UTIL = 1
        AND DT_DATA BETWEEN '{dt_ini}' AND '{dt_fim}'
    """
