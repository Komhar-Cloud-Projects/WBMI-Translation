WITH
SQ_WB_BIL_AccountActivity AS (
	SELECT
		AccountActivityId,
		ModifiedUserId,
		ModifiedDate,
		AccountId,
		ActivitySource,
		ProcessedStatusCode,
		ErrorDescription,
		BankingSystemCode
	FROM WB_BIL_AccountActivity
	WHERE 1=2
),
WB_BIL_AccountActivity1 AS (
	INSERT INTO WB_BIL_AccountActivity
	(ProcessedStatusCode)
	SELECT 
	PROCESSEDSTATUSCODE
	FROM SQ_WB_BIL_AccountActivity
),