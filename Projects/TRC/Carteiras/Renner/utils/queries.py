
def get_query_base_aux_renner():
    """
    Retorna a query SQL para buscar os indicadores pivotados por contrato/produto/cliente.

    Returns:
        str: Query SQL formatada.
    """
    query = """
        SELECT 
            UPPER(LTRIM(RTRIM(AUX.CONTRATO_FIN))) AS CONTRATO_FIN,
            AUX.CONTRATO_ORIGINAL,
            CASE 
                WHEN CHARINDEX('_', AUX.CONTRATO_ORIGINAL) > 0 THEN 'RENEG'
                ELSE 'CDC'
            END AS PRODUTO
        FROM AUX_DEVF AUX
        INNER JOIN CAD_DEVF DVF ON DVF.CONTRATO_FIN = AUX.CONTRATO_FIN
        WHERE DVF.COD_CLI = 247
    """
    return query

def get_query_discagens_olos(dt_ini, dt_fim):
    """
    Retorna a query SQL para buscar discagens OLOS com período parametrizado
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
    
    Returns:
        str: Query SQL formatada com as datas
    """
    query = f"""
    SELECT
        CAST(STARTDATE AS DATE) DATA,
        CUSTOMERID AS CONTRATO,
        CAMPAIGNID AS CAMPANHA,
        ROUTE AS ROUTE
    FROM [TRC-DC-BD2].OLOS.STAGE.ATTEMPTSRAWDATA
    WHERE CAST(STARTDATE AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'
    AND CAMPAIGNID IN (14,15,16,17,18,19,20,21,40,43)
    """
    return query