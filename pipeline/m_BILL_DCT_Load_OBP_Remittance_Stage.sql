WITH
SQ_OBPfile_source AS (

-- TODO Manual --

),
FILTRANS AS (
	SELECT
	Record_type, 
	tran_Code, 
	Recv_ID, 
	Chk_Digit, 
	Acct_Num AS AccT_Num, 
	Paid_Amt, 
	Payor_Name, 
	Payor_ID_Num, 
	Discr_Data, 
	Addeda_Rec_Ind AS Addeda_Rec_IND, 
	Reference_id
	FROM SQ_OBPfile_source
	WHERE Record_type = '6'
),
EXPTRANS AS (
	SELECT
	Record_type,
	tran_Code,
	Recv_ID,
	Chk_Digit,
	AccT_Num,
	Paid_Amt,
	Payor_Name,
	Payor_ID_Num,
	Discr_Data,
	Addeda_Rec_IND,
	Reference_id,
	'O' AS out_RemittanceSource,
	'InformO' AS out_ModifiedUserId,
	'Processed' AS out_Transaction_status,
	'OBP' AS out_Payment_mode,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS out_reconcilation_date,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS out_ModifyDate,
	-- *INF*: (TO_DECIMAL(Paid_Amt)  *  .01)
	(CAST(Paid_Amt AS FLOAT) * .01) AS out_PaidAmt,
	'Payment' AS out_Transaction_Type,
	'N' AS out_ProcessedStatus,
	Record_type || tran_Code || Recv_ID  || Chk_Digit || AccT_Num || Paid_Amt  || Payor_Name || Payor_ID_Num || Discr_Data || Addeda_Rec_IND || Reference_id AS out_transactionData,
	'USER' AS out_AuthorizedBy
	FROM FILTRANS
),
WB_BIL_RemittanceStage AS (
	INSERT INTO WB_BIL_RemittanceStage
	(ModifiedUserId, ModifiedDate, TransactionId, TransactionType, TransactionData, RemittanceSource, ProcessedStatusCode, ReconciliationDate, VendorAccountNumber, PaidAmount, PaymentMode, TransactionStatus, AuthorizedBy)
	SELECT 
	out_ModifiedUserId AS MODIFIEDUSERID, 
	out_ModifyDate AS MODIFIEDDATE, 
	Reference_id AS TRANSACTIONID, 
	out_Transaction_Type AS TRANSACTIONTYPE, 
	out_transactionData AS TRANSACTIONDATA, 
	out_RemittanceSource AS REMITTANCESOURCE, 
	out_ProcessedStatus AS PROCESSEDSTATUSCODE, 
	out_reconcilation_date AS RECONCILIATIONDATE, 
	Payor_ID_Num AS VENDORACCOUNTNUMBER, 
	out_PaidAmt AS PAIDAMOUNT, 
	out_Payment_mode AS PAYMENTMODE, 
	out_Transaction_status AS TRANSACTIONSTATUS, 
	out_AuthorizedBy AS AUTHORIZEDBY
	FROM EXPTRANS
),