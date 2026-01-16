-- BASE GERAL
Use Planejamento
drop table if exists #basePay
drop table if exists #ACIONAMENTOS
drop table if exists #basePaySint
drop table if exists #acionamento2
drop table if exists #DISCAGEM_temp

declare @dataIni  date = cast(getdate() -1 as date);

SELECT
	Assigned_Portfolio
,	DPD_BUCKET FAIXA
INTO #basePay
fROM OPENQUERY([TRC-DC-BDM\BD],
'
	SELECT 
		ltrim(rtrim(contrato_fin)) Assigned_Portfolio
		,	DPD_BUCKET
		,	cast(CALENDAR_DATE as date) dt_base 
	FROM AUX_PAYJOY_REMESSA 
	WHERE CALENDAR_DATE = (SELECT MAX(CALENDAR_DATE) FROM AUX_PAYJOY_REMESSA)
'
)

CREATE TABLE #ACIONAMENTOS (
	DATA DATE
,	CONTRATO VARCHAR(20) 
,	FAIXA  VARCHAR(20)
,	Total_RPCs_Right_Party_Contacts INT
,	Unique_RPCs INT
,	Promises_to_Pay_PTP INT
,	Payment_Already_Made INT
,	Dont_Know_on_the_Phone INT
,	Debt_Not_Recognized INT
)


-- ACIONAMENTO CONTRATO
--drop table if exists #ACIONAMENTOS

declare @sqlAciona  varchar(max);
set @sqlAciona = '
SELECT 
	DATA
,	A.CONTRATO
,	FAIXA
,	SUM(CPC) Total_RPCs_Right_Party_Contacts
,	IIF(SUM(CPC) >= 1,1,0) Unique_RPCs
,	SUM(PROMESSA) Promises_to_Pay_PTP
,	SUM(AlegaPagamento) Payment_Already_Made
,	SUM(DesconheceCliente) Dont_Know_on_the_Phone
,	SUM(DesconheceDivida) Debt_Not_Recognized

FROM OPENQUERY([TRC-DC-BDM\BD],
''
	SELECT 
		CAST(DATA_ACIONA AS DATE) AS DATA,
		LTRIM(RTRIM(A.CONTRATO_FIN)) AS CONTRATO,
		CPC = CASE WHEN A.COD_ACIONAMENTO IN (
						11316,	11322,	11324,	11326,	11331,	11349,	11350,	11355,	11356,
						11359,	11361,	11363,	11366,	11370,	11371,	11373,	11318,	
						11321,	11323,	11327,	11332,	11337,	11339,	11346,	11358,	
						11360,	11362,	11365,	11367,	11369,	11372,	11374,	11375,	11375,	11377
					)
					THEN 1 ELSE 0 END,
		PROMESSA = CASE WHEN A.COD_ACIONAMENTO IN (11350,	11355,	11356,	11360,	11362,	11365)
						THEN 1 ELSE 0 END,
		AlegaPagamento     = CASE WHEN A.COD_ACIONAMENTO = 11086 THEN 1 ELSE 0 END,
		DesconheceCliente  = CASE WHEN A.COD_ACIONAMENTO = 11102 THEN 1 ELSE 0 END,
		DesconheceDivida   = CASE WHEN A.COD_ACIONAMENTO = 11099 THEN 1 ELSE 0 END
	FROM ACIONA A
	INNER JOIN CAD_DEVF B 
		ON A.CONTRATO_FIN = B.CONTRATO_FIN 
		AND B.COD_CLI = 124 
		AND B.COD_CAR = 13
	WHERE DATA_ACIONA >= '''''+ CONVERT(VARCHAR(10),@dataIni, 120) +'''''
''
) A
inner join #basePay B on A.contrato = b.Assigned_Portfolio
GROUP BY DATA , A.CONTRATO,FAIXA
';

INSERT INTO #ACIONAMENTOS (
	DATA
,	CONTRATO 
,	FAIXA  
,	Total_RPCs_Right_Party_Contacts 
,	Unique_RPCs 
,	Promises_to_Pay_PTP 
,	Payment_Already_Made 
,	Dont_Know_on_the_Phone 
,	Debt_Not_Recognized 
)
exec(@sqlAciona);





-- BASE SINTETICA
SELECT
	FAIXA
,	COUNT(*) Assigned_Portfolio
INTO #basePaySint
FROM #basePay
GROUP BY FAIXA

-- ACIONAMENTO SINTETICO

SELECT
	DATA
,	FAIXA
,	SUM(Total_RPCs_Right_Party_Contacts) Total_RPCs_Right_Party_Contacts
,	SUM(Unique_RPCs) Unique_RPCs
,	SUM(Promises_to_Pay_PTP) Promises_to_Pay_PTP
,	SUM(Payment_Already_Made) Payment_Already_Made
,	SUM(Dont_Know_on_the_Phone) Dont_Know_on_the_Phone
,	SUM(Debt_Not_Recognized) Debt_Not_Recognized
into #acionamento2
FROM #ACIONAMENTOs
GROUP BY DATA, FAIXA


SELECT
	DATA
,	FAIXA
,	4 Active_Agents
,	SUM(Total_Call_Attempts) Total_Call_Attempts
,	SUM(Total_Answered_Calls) Total_Answered_Calls
,	SUM(Total_Answered_Calls_Excl_Short_Calls) Total_Answered_Calls_Excl_Short_Calls
,	SUM(Contacted_Portfolio) Contacted_Portfolio
,	SUM(Unique_Customers_Reached) Unique_Customers_Reached
,	SUM(Unique_Customers_Reached_Excl_Short_Calls) Unique_Customers_Reached_Excl_Short_Calls
INTO #DISCAGEM_temp
FROM(
SELECT
	CAST(DATA AS DATE) DATA
,	upper(CONTRATO) CONTRATO
,	FAIXA
,	COUNT(DISTINCT LOGIN) Active_Agents
,	COUNT(DISTINCT CALLID) Total_Call_Attempts
,	SUM(CASE WHEN LOGIN <> '' THEN 1 ELSE 0 END) Total_Answered_Calls
,	SUM(CASE WHEN LOGIN <> '' AND TEMPO_FALADO >= 15 THEN 1 ELSE 0 END) Total_Answered_Calls_Excl_Short_Calls
,	IIF(COUNT(DISTINCT CALLID)>=1,1,0) Contacted_Portfolio
,	IIF(SUM(CASE WHEN LOGIN <> '' THEN 1 ELSE 0 END) >= 1,1,0) Unique_Customers_Reached
,	IIF(SUM(CASE WHEN LOGIN <> '' AND TEMPO_FALADO >= 15 THEN 1 ELSE 0 END)>=1,1,0) Unique_Customers_Reached_Excl_Short_Calls
FROM DISCAGEM A
INNER JOIN #basePay B ON upper(LTRIM(RTRIM(A.CONTRATO))) COLLATE Latin1_General_CI_AS = B.Assigned_Portfolio 
WHERE CAMPANHA = 4881 and DATA >= @dataIni
GROUP BY
	CAST(DATA AS DATE)
,	CONTRATO 
,	FAIXA
)a GROUP BY DATA, FAIXA

SELECT
    A.DATA
,   A.FAIXA
,	C.Assigned_Portfolio
,	Reachable_Portfolio = 0
,	A.Contacted_Portfolio
,	A.Active_Agents
,	A.Total_Call_Attempts
,	A.Total_Answered_Calls
,	B.Dont_Know_on_the_Phone
,	A.Total_Answered_Calls_Excl_Short_Calls
,	A.Unique_Customers_Reached
,	A.Unique_Customers_Reached_Excl_Short_Calls
,	B.Total_RPCs_Right_Party_Contacts
,	B.Unique_RPCs
,	B.Promises_to_Pay_PTP
,	B.Debt_Not_Recognized
,	B.Payment_Already_Made
,	Device_Reported_Stolen = 0
,	Device_Warranty_Acess_issue = 0
,	Number_Of_SMS_Sent = 0
,	Number_Of_WA_Sent = 0
,	Number_Of_Email_sent = 0

FROM #DISCAGEM_temp A
LEFT JOIN #acionamento2 B 
    ON A.DATA = B.DATA AND A.FAIXA = B.FAIXA collate SQL_Latin1_General_CP1_CI_AS
LEFT JOIN #basePaySint C 
    ON A.FAIXA = C.FAIXA collate SQL_Latin1_General_CP1_CI_AS

ORDER BY 1



