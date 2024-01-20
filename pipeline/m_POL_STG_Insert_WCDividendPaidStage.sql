WITH
SQ_VWDividendPaid AS (
	SELECT 
	VWDividendPaid.AgencyCode,
	VWDividendPaid.PolicyKey,
	VWDividendPaid.DividendPaidAmount,
	VWDividendPaid.DividendPaidDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.VWDividendPaid
	WHERE VWDividendPaid.PolicyKey<>' ' AND VWDividendPaid.DividendPaidAmount<>0.0
	AND CONVERT(datetime,VWDividendPaid.DividendPaidDate,101)>'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_DataCollect AS (
	SELECT
	AgencyCode,
	DividendPaidAmount,
	DividendPaidDate,
	PolicyKey,
	-- *INF*: IIF(IS_DATE(DividendPaidDate,'YYYY-MM-DD'),TO_DATE(DividendPaidDate,'YYYY-MM-DD'))
	IFF(IS_DATE(DividendPaidDate, 'YYYY-MM-DD'), TO_TIMESTAMP(DividendPaidDate, 'YYYY-MM-DD')) AS O_DividendPaidDate,
	-- *INF*: SUBSTR(PolicyKey,1,3)
	SUBSTR(PolicyKey, 1, 3) AS PolicySymbol,
	-- *INF*: SUBSTR(PolicyKey,4,7)
	SUBSTR(PolicyKey, 4, 7) AS PolicyNumber,
	-- *INF*: SUBSTR(PolicyKey,11,2)
	SUBSTR(PolicyKey, 11, 2) AS PolicyModule,
	CURRENT_TIMESTAMP AS ExtractDate,
	'EAS' AS SourceSystemId
	FROM SQ_VWDividendPaid
),
SEQ_GenerateKey AS (
	CREATE SEQUENCE SEQ_GenerateKey
	START = 1
	INCREMENT = 1;
),
WCDividendPaidStage AS (
	TRUNCATE TABLE WCDividendPaidStage;
	INSERT INTO WCDividendPaidStage
	(WCDividendStageRecID, DividendPaidDate, DividendPaidAmt, AgencyCode, PolicySymbol, PolicyNumber, PolicyModule, ExtractDate, SourceSystemId)
	SELECT 
	SEQ_GenerateKey.NEXTVAL AS WCDIVIDENDSTAGERECID, 
	O_DividendPaidDate AS DIVIDENDPAIDDATE, 
	DividendPaidAmount AS DIVIDENDPAIDAMT, 
	AGENCYCODE, 
	POLICYSYMBOL, 
	POLICYNUMBER, 
	POLICYMODULE, 
	EXTRACTDATE, 
	SOURCESYSTEMID
	FROM EXP_DataCollect
),