WITH
SQ_WorkIn2vateEPLI AS (
	SELECT
		WorkIn2vateEPLIId,
		ClassCode,
		InsuranceLineCode,
		PolStatusCode,
		PolStatusDescription,
		CustomerNumber,
		PolicyKey,
		Name,
		DoingBusinessAs,
		PremiumTransactionEffectiveDate,
		PremiumTransactionExpirationDate,
		ExtractDate,
		SourceSystemId,
		AuditID
	FROM WorkIn2vateEPLI
	WHERE @{pipeline().parameters.WHERECLAUSE}
),
LKP_WorkIn2vateEPLI AS (
	SELECT
	CustomerNumber
	FROM (
		SELECT 
			CustomerNumber
		FROM WorkIn2vateEPLI
		WHERE (PolStatusDescription = 'Inforce' ) and AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CustomerNumber ORDER BY CustomerNumber) = 1
),
EXP_GetValues AS (
	SELECT
	SQ_WorkIn2vateEPLI.PolStatusDescription,
	SQ_WorkIn2vateEPLI.CustomerNumber AS i_CustomerNumber,
	SQ_WorkIn2vateEPLI.PolicyKey AS i_PolicyKey,
	SQ_WorkIn2vateEPLI.Name AS i_Name,
	SQ_WorkIn2vateEPLI.PremiumTransactionExpirationDate AS i_PremiumTransactionExpirationDate,
	SQ_WorkIn2vateEPLI.ExtractDate AS i_ExtractDate,
	LKP_WorkIn2vateEPLI.CustomerNumber AS LKP_CustomerNumber,
	-- *INF*: IIF(ISNULL(LKP_CustomerNumber),0,1)
	IFF(LKP_CustomerNumber IS NULL, 0, 1) AS FLAG,
	-- *INF*: RPAD(SUBSTR(LTRIM(RTRIM(i_CustomerNumber)),1,10),10,' ')
	RPAD(SUBSTR(LTRIM(RTRIM(i_CustomerNumber)), 1, 10), 10, ' ') AS v_CustomerNumber,
	-- *INF*: RPAD(SUBSTR(LTRIM(RTRIM(i_PolicyKey)),1,12),12,' ')
	RPAD(SUBSTR(LTRIM(RTRIM(i_PolicyKey)), 1, 12), 12, ' ') AS v_PolicyKey,
	-- *INF*: RPAD(SUBSTR(LTRIM(RTRIM(i_Name)),1,30),30,' ')
	RPAD(SUBSTR(LTRIM(RTRIM(i_Name)), 1, 30), 30, ' ') AS v_Name,
	-- *INF*: TO_CHAR(i_PremiumTransactionExpirationDate,'MM/DD/YYYY')
	TO_CHAR(i_PremiumTransactionExpirationDate, 'MM/DD/YYYY') AS v_PremiumTransactionExpirationDate,
	-- *INF*: TO_CHAR(i_ExtractDate,'YYYYMMDD')
	TO_CHAR(i_ExtractDate, 'YYYYMMDD') AS o_ExtractDate,
	v_CustomerNumber || '~'
 || v_PolicyKey || '~'
 || v_Name || '~'
 || v_PremiumTransactionExpirationDate AS o_Record
	FROM SQ_WorkIn2vateEPLI
	LEFT JOIN LKP_WorkIn2vateEPLI
	ON LKP_WorkIn2vateEPLI.CustomerNumber = SQ_WorkIn2vateEPLI.CustomerNumber
),
RTR_EPLI AS (
	SELECT
	PolStatusDescription,
	o_ExtractDate,
	o_Record,
	FLAG
	FROM EXP_GetValues
),
RTR_EPLI_Inforce AS (SELECT * FROM RTR_EPLI WHERE LTRIM(RTRIM(PolStatusDescription))='Inforce'  AND FLAG = '1'),
RTR_EPLI_CancelledAndNotInforce AS (SELECT * FROM RTR_EPLI WHERE IN(LTRIM(RTRIM(PolStatusDescription)),'Cancelled','Not Inforce')  AND FLAG = '0'),
EXP_EPLI_Inforce AS (
	SELECT
	o_ExtractDate AS i_ExtractDate,
	o_Record AS Record,
	@{pipeline().parameters.FILENAME_INFORCE} || '_' || i_ExtractDate AS o_FileName
	FROM RTR_EPLI_Inforce
),
TGT_EPLIFlatFile_Inforce AS (
	INSERT INTO EPLIFlatFile
	(Record, FileName)
	SELECT 
	RECORD, 
	o_FileName AS FILENAME
	FROM EXP_EPLI_Inforce
),
EXP_EPLI_CancelledAndNotInforce AS (
	SELECT
	o_ExtractDate AS i_ExtractDate,
	o_Record AS Record,
	@{pipeline().parameters.FILENAME_CANCELLEDANDNOTINFORCE} || '_' || i_ExtractDate AS o_FileName
	FROM RTR_EPLI_CancelledAndNotInforce
),
TGT_EPLIFlatFile_CancelledAndNotInforce AS (
	INSERT INTO EPLIFlatFile
	(Record, FileName)
	SELECT 
	RECORD, 
	o_FileName AS FILENAME
	FROM EXP_EPLI_CancelledAndNotInforce
),