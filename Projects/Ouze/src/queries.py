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
    WHERE A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA IN (19, 30))
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
        COD_CAR
    FROM MAILING_HIST 
    WHERE DATA BETWEEN '{dt_ini}' AND '{dt_fim}'
    AND COD_CLI IN(196,198,228)
    """
    return query

def get_query_cad_devf():
    """
    Retorna a query SQL para buscar dados do cadastro de devolução financeira
    
    Esta query não necessita de parâmetros de data pois busca o cadastro atual
    
    Returns:
        str: Query SQL
    """
    query = """
    SELECT 
        LTRIM(RTRIM(D.CPF_DEV)) AS CPF_DEV,
        UPPER(LTRIM(RTRIM(D.CONTRATO_FIN))) AS CONTRATO_FIN,
        D.VALORPRIN_FIN,
        D.VALOR_FIN,
        D.DTDEVOL_FIN,
        D.ATRASO_FIN,
        D.COD_CLI,
        D.COD_CAR,
        D.STATCONT_FIN,
        C.DESC_CAR
    FROM CAD_DEVF D
    INNER JOIN CAD_CAR C WITH (NOLOCK) ON D.COD_CLI = C.COD_CLI AND D.COD_CAR = C.COD_CAR
    WHERE (D.COD_CLI = 198 AND D.COD_CAR IN (1,2,3)) 
       OR (D.COD_CLI = 196 AND D.COD_CAR IN (1,3,4)) 
       OR (D.COD_CLI = 228 AND D.COD_CAR = 2)
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

def get_query_tabulacao_aciona():
    query = f"""
        SELECT 
            COD_ACIONA,
            DESC_ACIONA,
            CPC,
            CPCA,
            PROMESSA
        FROM ACIONAMENTO_CARTEIRA
        WHERE COD_CLI = 196
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
        WHERE ((C.COD_CLI = 198 AND C.COD_CAR IN (1, 2, 3)) 
            OR (C.COD_CLI = 196 AND C.COD_CAR IN (1, 3, 4)) 
            OR (C.COD_CLI = 228 AND C.COD_CAR = 2))
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
        AND B.COD_CLI IN(198, 196, 228)
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
            CASE
                WHEN A.RECUP_ACO IN(1626, 1, 11003) THEN 'ROBÔ'
                ELSE 'HUMANO'
            END TIPO
        FROM CAD_ACO A
        INNER JOIN CAD_DEVF		B WITH (NOLOCK) ON B.CONTRATO_FIN = A.CONTRATO_FIN
        WHERE CAST(A.DTACORDOHORA_ACO AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'
        AND B.COD_CLI IN(198, 196, 228)
    """
    return query

def get_query_tempos(dt_ini, dt_fim):
    query = f"""
        SELECT * 
        FROM OPENQUERY(EXPERT, '
        SELECT
            rah.nome AS ''NOME_AGENTE'',
            rah.agente AS ''LOGIN_AGENTE'',
            rah.dia AS ''DATA_LOGIN'',
            MIN(rah.login) AS ''PRIMEIRO_LOGIN'',
            rah.dia AS ''DATA_LOGOUT'',
            MAX(rah.logout) AS ''ULTIMO_LOGOUT'',
            TIMEDIFF(MAX(rah.logout), MIN(rah.login)) AS ''TP_LOGADO'',
            rah.tempo_falado_hora_grupo AS ''Produtivo'',

            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
            FROM tb_relatorio_pausa tp
            WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
            AND tp.DSC_AGENTE = rah.agente
            AND tp.DSC_PAUSA IN ( 
                ''10 minutos - primeira'',
                ''pausa 10 primeira'',
                ''1? pausa 10 min'',
                ''1ª pausa 10 min | expert'',
                ''1ª pausa/descanso(10min)'',
                ''descanso''
            ))  AS ''1a Pausa 10 minutos'',
                    
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (  
                ''1? pausa 10 min'',
                ''2? pausa 10 min'',
                ''pausa 10 segunda'',
                ''2ª pausa 10 min | expert'',
                ''10 minutos - segunda'',
                ''10 minutos - primeira'',
                ''pausa 10 primeira'',
                ''10 minutos - segunda'',
                ''2ª pausa 10 min | expert''
            )) AS ''2a Pausa 10 minutos'',
            
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (    
                ''almoÇo'',
                ''almoÇo'',
                ''almoço'',
                ''lanche 1hr''
            ))  AS ''Almoço'',

            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (    
                ''erro/sistema'',
                ''outros | expert''
            ))  AS ''Banheiro'',
            
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                    AND tp.DSC_AGENTE = rah.agente
                    AND tp.DSC_PAUSA IN (    
                ''treinamento'',
                ''feedback'',
                ''suporte'',
                ''reunião'',
                ''reuniao/trein'',
                ''pausasuper'',
                ''supervisao'',
                ''treinamento expert'',
                ''reuniao / treinamento von'',
                ''reuniao / treinamento''
            ))  AS ''FEEDBACK'',
            
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (    
                ''pausa 20 | expert'',
                ''pausa 20'',
                ''pausa lanche'',
                ''pausa lanche 20 min vonix''
            ))  AS ''Interlavo 20 minutos'',
            
            (SELECT SEC_TO_TIME(SUM(tp.SEC_TEMPO_PAUSA))
                FROM tb_relatorio_pausa tp
                WHERE tp.DAT_OCORRENCIA BETWEEN CONCAT(rah.dia, '' 00:00:00'') AND CONCAT(rah.dia, '' 23:59:59'')
                AND tp.DSC_AGENTE = rah.agente
                AND tp.DSC_PAUSA IN (    
                ''selecionando pausa'',
                ''whatsapp'',
                ''administrativa'',
                ''erro/sistema'',
                ''pausa - discagem'',
                ''reunião''
            )) AS ''Outras pausas''
        FROM relatorio_agentes_hora rah
            JOIN grupo g ON rah.grupoprincipal = g.id_grupo
        WHERE
            rah.dia BETWEEN ''{dt_ini}'' AND ''{dt_fim}''
            AND rah.grupoprincipal IN (4643, 4651, 4652, 4653, 4658, 4660, 4662, 4699)
        GROUP BY
            rah.dia,
            rah.agente
        ORDER BY
            rah.dia,
            rah.agente
        ')
        """
    return query