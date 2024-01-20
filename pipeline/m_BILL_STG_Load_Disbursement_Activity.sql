WITH
SQ_DisbursementsFile AS (

-- TODO Manual --

),
EXP_Load_Disbursement_Activity AS (
	SELECT
	DisbursementId AS in_DisbursementId,
	-- *INF*: TO_BIGINT(RTRIM(in_DisbursementId))
	CAST(RTRIM(in_DisbursementId) AS BIGINT) AS out_DisbursementId,
	DisbursementStatusCode AS in_DisbursementStatusCode,
	-- *INF*: IIF(in_DisbursementStatusCode = 'C', 'D',
	-- IIF(in_DisbursementStatusCode = 'V', 'S',
	-- in_DisbursementStatusCode))
	IFF(
	    in_DisbursementStatusCode = 'C', 'D',
	    IFF(
	        in_DisbursementStatusCode = 'V', 'S', in_DisbursementStatusCode
	    )
	) AS out_DisbursementStatusCode,
	TransactionDate AS in_TransactionDate,
	-- *INF*: TO_DATE(in_TransactionDate,'YYYYMMDD')
	TO_TIMESTAMP(in_TransactionDate, 'YYYYMMDD') AS out_TransactionDate,
	CheckNumber,
	'infmatca' AS out_ModifiedUserId,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS out_ModifiedDate,
	'N' AS out_ProcessedStatusCode
	FROM SQ_DisbursementsFile
),
LKP_WB_BIL_DisbursementActivity AS (
	SELECT
	DisbursementActivityId,
	DisbursementId,
	CheckNumber,
	TransactionDate,
	DisbursementStatusCode
	FROM (
		SELECT 
			DisbursementActivityId,
			DisbursementId,
			CheckNumber,
			TransactionDate,
			DisbursementStatusCode
		FROM WB_BIL_DisbursementActivity
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DisbursementId,CheckNumber,TransactionDate,DisbursementStatusCode ORDER BY DisbursementActivityId) = 1
),
FIL_Null_DisbursementActivityId AS (
	SELECT
	LKP_WB_BIL_DisbursementActivity.DisbursementActivityId AS in_DisbursementActivityId, 
	EXP_Load_Disbursement_Activity.out_DisbursementId AS DisbursementId, 
	EXP_Load_Disbursement_Activity.out_DisbursementStatusCode AS DisbursementStatusCode, 
	EXP_Load_Disbursement_Activity.out_TransactionDate AS TransactionDate, 
	EXP_Load_Disbursement_Activity.CheckNumber, 
	EXP_Load_Disbursement_Activity.out_ModifiedUserId AS ModifiedUserId, 
	EXP_Load_Disbursement_Activity.out_ModifiedDate AS ModifiedDate, 
	EXP_Load_Disbursement_Activity.out_ProcessedStatusCode AS ProcessedStatusCode
	FROM EXP_Load_Disbursement_Activity
	LEFT JOIN LKP_WB_BIL_DisbursementActivity
	ON LKP_WB_BIL_DisbursementActivity.DisbursementId = EXP_Load_Disbursement_Activity.out_DisbursementId AND LKP_WB_BIL_DisbursementActivity.CheckNumber = EXP_Load_Disbursement_Activity.CheckNumber AND LKP_WB_BIL_DisbursementActivity.TransactionDate = EXP_Load_Disbursement_Activity.out_TransactionDate AND LKP_WB_BIL_DisbursementActivity.DisbursementStatusCode = EXP_Load_Disbursement_Activity.out_DisbursementStatusCode
	WHERE ISNULL(in_DisbursementActivityId)
),
WB_BIL_DisbursementActivity AS (
	INSERT INTO WB_BIL_DisbursementActivity
	(ModifiedUserId, ModifiedDate, DisbursementId, CheckNumber, TransactionDate, DisbursementStatusCode, ProcessedStatusCode)
	SELECT 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	DISBURSEMENTID, 
	CHECKNUMBER, 
	TRANSACTIONDATE, 
	DISBURSEMENTSTATUSCODE, 
	PROCESSEDSTATUSCODE
	FROM FIL_Null_DisbursementActivityId
),