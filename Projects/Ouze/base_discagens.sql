DECLARE @DT_INI AS DATE = '2025-09-01' -- PRIMEIRO DIA DO MÊS
DECLARE @DT_FIM AS DATE = '2025-09-30' -- ÚLTIMO DIA DO MÊS
DECLARE @DT		AS DATE = '2025-09-01' -- PRIMEIRO DIA DESEJADO
DECLARE @DT2	AS DATE = '2025-09-30' -- ÚLTIMO DIA DESEJADO

-- CONFIGURAÇÃO FIXA ====================================================== 
DECLARE @TableName AS VARCHAR(50) = 'totalinfo_' + FORMAT(@DT_INI, 'yyyy_MM');
DECLARE @SQL NVARCHAR(MAX);

--select DISTINCT DATA from #DISCAGENS WHERE GrupoPrincipal IN (4326, 4636) ORDER BY DATA

-->> #DISCAGENS 
SELECT 
    * -- DROP TABLE #DISCAGENS / SELECT DISTINCT GrupoPrincipal FROM #DISCAGENS where operacao = 'outros'
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
    A.tempoconversacao_ms
FROM totalinfo_2025_09 A
WHERE A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA IN (19, 30))
')
WHERE DATA BETWEEN @DT_INI AND @DT2;