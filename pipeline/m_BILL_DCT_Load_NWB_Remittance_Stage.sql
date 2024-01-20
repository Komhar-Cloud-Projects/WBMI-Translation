WITH
SQ_NWBPayments_source AS (

-- TODO Manual --

),
EXPTRANS AS (
	SELECT
	Location,
	TransactionDate AS Transaction_Date,
	filler1 AS Filler1,
	TransactionSeq AS Transaction_Seq,
	filler2 AS Filler2,
	AccountNum AS Acct_Number,
	filler3 AS Filler3,
	PaidAmt AS Paid_Amt,
	filler4 AS Filler4,
	'N' AS out_RemittanceSource,
	'InformN' AS out_ModifiedUserId,
	'Processed' AS out_TransactionStatus,
	'AACH' AS out_PaymentMode,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS out_ReconcilationDate,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS out_ModifyDate,
	-- *INF*: (TO_DECIMAL(Paid_Amt)  *  .01)
	(CAST(Paid_Amt AS FLOAT) * .01) AS out_PaidAmt,
	'Payment' AS out_TransactionType,
	'N' AS out_ProcessedStatus,
	Location||Transaction_Date||Filler1||Transaction_Seq||Filler2||Filler3||Paid_Amt||Filler4 AS out_TransactionData,
	'USER' AS out_AuthorizedBy,
	Transaction_Date||'NWB'||Transaction_Seq AS out_TransactionId
	FROM SQ_NWBPayments_source
),
WB_BIL_RemittanceStage AS (
	INSERT INTO WB_BIL_RemittanceStage
	(ModifiedUserId, ModifiedDate, TransactionId, TransactionType, TransactionData, RemittanceSource, ProcessedStatusCode, ReconciliationDate, PaidAmount, PaymentMode, AuthorizedBy, PolicyNumber)
	SELECT 
	out_ModifiedUserId AS MODIFIEDUSERID, 
	out_ModifyDate AS MODIFIEDDATE, 
	out_TransactionId AS TRANSACTIONID, 
	out_TransactionType AS TRANSACTIONTYPE, 
	out_TransactionData AS TRANSACTIONDATA, 
	out_RemittanceSource AS REMITTANCESOURCE, 
	out_ProcessedStatus AS PROCESSEDSTATUSCODE, 
	out_ReconcilationDate AS RECONCILIATIONDATE, 
	out_PaidAmt AS PAIDAMOUNT, 
	out_PaymentMode AS PAYMENTMODE, 
	out_AuthorizedBy AS AUTHORIZEDBY, 
	Acct_Number AS POLICYNUMBER
	FROM EXPTRANS
),