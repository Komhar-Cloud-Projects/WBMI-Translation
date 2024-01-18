WITH
SQ_Conv_Sweep_FIS_Payments AS (

-- TODO Manual --

),
EXPTRANS AS (
	SELECT
	DCAccountNumber,
	Mod_UserID,
	Mod_Date,
	Remit_SRC,
	Reconcil_Date,
	Pol_EFF_Date,
	Pol_Cleared_Date,
	Pol_Init_Date,
	Status,
	ApprovalCode,
	Auth_By,
	PaidAmount,
	TransactionID,
	TransactionData,
	-- *INF*: TO_DATE(LTRIM(RTRIM(Pol_EFF_Date)),'YYYYMMDD')
	-- 
	TO_TIMESTAMP(LTRIM(RTRIM(Pol_EFF_Date)), 'YYYYMMDD') AS o_pol_eff_date,
	-- *INF*: TO_DATE(LTRIM(RTRIM(Reconcil_Date)),'YYYYMMDD') 
	TO_TIMESTAMP(LTRIM(RTRIM(Reconcil_Date)), 'YYYYMMDD') AS o_Reconcil_date,
	-- *INF*: TO_DATE(LTRIM(RTRIM(Mod_Date)),'YYYYMMDDHH24MISS')
	TO_TIMESTAMP(LTRIM(RTRIM(Mod_Date)), 'YYYYMMDDHH24MISS') AS o_Mod_date,
	-- *INF*: TO_DATE(LTRIM(RTRIM(Pol_Cleared_Date)),'YYYYMMDDHH24MISS')
	TO_TIMESTAMP(LTRIM(RTRIM(Pol_Cleared_Date)), 'YYYYMMDDHH24MISS') AS o_Pol_Cleared_date,
	-- *INF*: TO_DATE(LTRIM(RTRIM(Pol_Init_Date)),'YYYYMMDDHH24MISS')
	TO_TIMESTAMP(LTRIM(RTRIM(Pol_Init_Date)), 'YYYYMMDDHH24MISS') AS o_Pol_init_Date,
	'Payment' AS o_transactionType,
	'N' AS o_ProcessStatueCode,
	'AACH' AS o_paymentMode,
	-- *INF*: SUBSTR(DCAccountNumber,9,1)
	SUBSTR(DCAccountNumber, 9, 1) AS v_ACCT_9,
	-- *INF*: DECODE(true,IS_NUMBER(v_ACCT_9)
	--     ,SUBSTR(DCAccountNumber,1,12),
	-- null)
	DECODE(
	    true,
	    REGEXP_LIKE(v_ACCT_9, '^[0-9]+$'), SUBSTR(DCAccountNumber, 1, 12),
	    null
	) AS o_Account,
	-- *INF*: DECODE(true,
	-- v_ACCT_9 = 'H' ,SUBSTR(DCAccountNumber,1,12),
	-- v_ACCT_9 = 'D' ,SUBSTR(DCAccountNumber,1,12),
	-- null)
	DECODE(
	    true,
	    v_ACCT_9 = 'H', SUBSTR(DCAccountNumber, 1, 12),
	    v_ACCT_9 = 'D', SUBSTR(DCAccountNumber, 1, 12),
	    null
	) AS o_policy
	FROM SQ_Conv_Sweep_FIS_Payments
),
WB_BIL_RemittanceStage AS (
	INSERT INTO WB_BIL_RemittanceStage
	(ModifiedUserId, ModifiedDate, TransactionId, TransactionType, TransactionData, RemittanceSource, ProcessedStatusCode, ReconciliationDate, VendorAccountNumber, EffectiveDate, DateCleared, DateInitiated, PaidAmount, PaymentMode, TransactionStatus, ApprovalCode, AuthorizedBy, PolicyNumber)
	SELECT 
	Mod_UserID AS MODIFIEDUSERID, 
	o_Mod_date AS MODIFIEDDATE, 
	TransactionID AS TRANSACTIONID, 
	o_transactionType AS TRANSACTIONTYPE, 
	TRANSACTIONDATA, 
	Remit_SRC AS REMITTANCESOURCE, 
	o_ProcessStatueCode AS PROCESSEDSTATUSCODE, 
	o_Reconcil_date AS RECONCILIATIONDATE, 
	o_Account AS VENDORACCOUNTNUMBER, 
	o_pol_eff_date AS EFFECTIVEDATE, 
	o_Pol_Cleared_date AS DATECLEARED, 
	o_Pol_init_Date AS DATEINITIATED, 
	PAIDAMOUNT, 
	o_paymentMode AS PAYMENTMODE, 
	Status AS TRANSACTIONSTATUS, 
	APPROVALCODE, 
	Auth_By AS AUTHORIZEDBY, 
	o_policy AS POLICYNUMBER
	FROM EXPTRANS
),