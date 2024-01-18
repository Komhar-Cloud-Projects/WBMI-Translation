WITH
XMLDSQ_DirectBillerRemittanceFile AS (
),
EXP_Load_Remittance_Stage AS (
	SELECT
	number,
	reconciliation_date AS in_reconciliation_date,
	-- *INF*: TO_DATE(in_reconciliation_date,'MM/DD/YYYY')
	TO_TIMESTAMP(in_reconciliation_date, 'MM/DD/YYYY') AS out_reconciliation_date,
	transaction_id,
	account AS in_VendorAccountNumber,
	invoice_number,
	effective_date AS in_effective_date,
	-- *INF*: TO_DATE(in_effective_date, 'MM/DD/YYYY')
	TO_TIMESTAMP(in_effective_date, 'MM/DD/YYYY') AS out_effective_date,
	date_cleared AS in_date_cleared,
	-- *INF*: TO_DATE(in_date_cleared,'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP(in_date_cleared, 'MM/DD/YYYY HH24:MI:SS') AS out_date_cleared,
	date_initiated AS in_date_initiated,
	-- *INF*: sysdate
	-- --TO_DATE(in_date_initiated,'MM/DD/YYYY HH24:MI:SS')
	CURRENT_TIMESTAMP AS out_date_initiated,
	bill_amount,
	paid_amount,
	fee_amount,
	payment_mode,
	status,
	approval_code,
	authorized_by,
	number  || ',' || in_reconciliation_date || ',' || transaction_id || ',' || in_VendorAccountNumber || ',' || invoice_number || ',' || customer_name || ',' || in_effective_date || ',' || in_date_cleared || ',' || in_date_initiated || ',' || bill_amount || ',' || paid_amount || ',' || fee_amount || ',' || payment_mode || ',' || status || ',' || approval_code || ',' || authorized_by  || ',' || originalbillamount || ',' || minimumpayment || ',' || DCAccountNumber || ',' || stoppaperinvoices AS out_TransactionData,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS out_ModifyDate,
	'B' AS out_RemittanceSource,
	'infmatca' AS out_ModifierUserID,
	-- *INF*: IIF(paid_amount  >  0,  'Payment', DECODE(payment_mode,
	-- 'Checking', 'Failed',
	-- 'Business Checking', 'Failed' ,
	-- 'Savings', 'Failed' ,
	-- 'Visa', 'Chargeback',
	-- 'MasterCard', 'Chargeback',
	-- 'American Express','Chargeback',
	-- 'Discover', 'Chargeback',
	--  'Reversal'))
	IFF(
	    paid_amount > 0, 'Payment',
	    DECODE(
	        payment_mode,
	        'Checking', 'Failed',
	        'Business Checking', 'Failed',
	        'Savings', 'Failed',
	        'Visa', 'Chargeback',
	        'MasterCard', 'Chargeback',
	        'American Express', 'Chargeback',
	        'Discover', 'Chargeback',
	        'Reversal'
	    )
	) AS out_TransactionType,
	-- *INF*: DECODE(payment_mode,
	-- 'Checking', 'WPA',
	-- 'Business Checking', 'WPA' ,
	-- 'Savings', 'WPA' ,
	-- 'Visa', 'CCP',
	-- 'MasterCard', 'CCP',
	-- 'American Express',  'CCP',
	-- 'Discover',  'CCP',
	-- payment_mode)
	DECODE(
	    payment_mode,
	    'Checking', 'WPA',
	    'Business Checking', 'WPA',
	    'Savings', 'WPA',
	    'Visa', 'CCP',
	    'MasterCard', 'CCP',
	    'American Express', 'CCP',
	    'Discover', 'CCP',
	    payment_mode
	) AS out_PaymentMode,
	-- *INF*: IIF(paid_amount < 0  AND (payment_mode = 'Visa' 
	--  OR payment_mode = 'MasterCard'
	--  OR  payment_mode = 'American Express'
	-- OR  payment_mode =
	-- 'Discover'),'S', 'N')
	IFF(
	    paid_amount < 0
	    and (payment_mode = 'Visa'
	    or payment_mode = 'MasterCard'
	    or payment_mode = 'American Express'
	    or payment_mode = 'Discover'),
	    'S',
	    'N'
	) AS out_ProcessFlag,
	originalbillamount,
	minimumpayment,
	accountkey AS DCAccountNumber,
	stoppaperinvoices,
	'Vendor Account Reference number missing Transaction ID =  '  || transaction_id AS var_vendor_AccountMissing_Message,
	-- *INF*: IIF(ISNULL(in_VendorAccountNumber), ERROR(var_vendor_AccountMissing_Message), in_VendorAccountNumber)
	IFF(
	    in_VendorAccountNumber IS NULL, ERROR(var_vendor_AccountMissing_Message),
	    in_VendorAccountNumber
	) AS out_Vendor_Account
	FROM XMLDSQ_DirectBillerRemittanceFile
),
LKP_WB_Bill_Remittance_stage AS (
	SELECT
	TransactionId,
	IN_transaction_id,
	IN_RemittanceSource,
	IN_TransactionType,
	RemittanceSource,
	TransactionType
	FROM (
		SELECT 
			TransactionId,
			IN_transaction_id,
			IN_RemittanceSource,
			IN_TransactionType,
			RemittanceSource,
			TransactionType
		FROM WB_BIL_RemittanceStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TransactionId,RemittanceSource,TransactionType ORDER BY TransactionId) = 1
),
FIL_TransactionID AS (
	SELECT
	LKP_WB_Bill_Remittance_stage.TransactionId AS Transaction_Id_Lookup, 
	LKP_WB_Bill_Remittance_stage.IN_transaction_id, 
	EXP_Load_Remittance_Stage.out_TransactionData AS SQ_TransactionData, 
	EXP_Load_Remittance_Stage.out_ModifyDate AS SQ_ModifyDate, 
	EXP_Load_Remittance_Stage.out_ProcessFlag AS SQ_ProcessFlag, 
	EXP_Load_Remittance_Stage.out_ModifierUserID AS SQ_ModifierUserID, 
	LKP_WB_Bill_Remittance_stage.IN_RemittanceSource, 
	LKP_WB_Bill_Remittance_stage.IN_TransactionType, 
	EXP_Load_Remittance_Stage.number, 
	EXP_Load_Remittance_Stage.out_reconciliation_date AS reconciliation_date, 
	EXP_Load_Remittance_Stage.transaction_id, 
	EXP_Load_Remittance_Stage.out_Vendor_Account AS VendorAccountNumber, 
	EXP_Load_Remittance_Stage.invoice_number, 
	EXP_Load_Remittance_Stage.out_effective_date AS effective_date, 
	EXP_Load_Remittance_Stage.out_date_cleared AS date_cleared, 
	EXP_Load_Remittance_Stage.out_date_initiated AS date_initiated, 
	EXP_Load_Remittance_Stage.bill_amount, 
	EXP_Load_Remittance_Stage.paid_amount, 
	EXP_Load_Remittance_Stage.fee_amount, 
	EXP_Load_Remittance_Stage.out_PaymentMode AS payment_mode, 
	EXP_Load_Remittance_Stage.status, 
	EXP_Load_Remittance_Stage.approval_code, 
	EXP_Load_Remittance_Stage.authorized_by, 
	EXP_Load_Remittance_Stage.originalbillamount, 
	EXP_Load_Remittance_Stage.minimumpayment, 
	EXP_Load_Remittance_Stage.DCAccountNumber, 
	EXP_Load_Remittance_Stage.stoppaperinvoices
	FROM EXP_Load_Remittance_Stage
	LEFT JOIN LKP_WB_Bill_Remittance_stage
	ON LKP_WB_Bill_Remittance_stage.TransactionId = EXP_Load_Remittance_Stage.transaction_id AND LKP_WB_Bill_Remittance_stage.RemittanceSource = EXP_Load_Remittance_Stage.out_RemittanceSource AND LKP_WB_Bill_Remittance_stage.TransactionType = EXP_Load_Remittance_Stage.out_TransactionType
	WHERE ISNULL(Transaction_Id_Lookup)
),
WB_BIL_RemittanceStage AS (
	INSERT INTO WB_BIL_RemittanceStage
	(ModifiedUserId, ModifiedDate, TransactionId, TransactionType, TransactionData, RemittanceSource, ProcessedStatusCode, ReconciliationDate, VendorAccountNumber, InvoiceNumber, EffectiveDate, DateCleared, DateInitiated, BillAmount, PaidAmount, FeeAmount, PaymentMode, TransactionStatus, ApprovalCode, AuthorizedBy, OriginalBillAmount, MinimumPayment, DCAccountNumber, PaperProcessingCode)
	SELECT 
	SQ_ModifierUserID AS MODIFIEDUSERID, 
	SQ_ModifyDate AS MODIFIEDDATE, 
	transaction_id AS TRANSACTIONID, 
	IN_TransactionType AS TRANSACTIONTYPE, 
	SQ_TransactionData AS TRANSACTIONDATA, 
	IN_RemittanceSource AS REMITTANCESOURCE, 
	SQ_ProcessFlag AS PROCESSEDSTATUSCODE, 
	reconciliation_date AS RECONCILIATIONDATE, 
	VENDORACCOUNTNUMBER, 
	invoice_number AS INVOICENUMBER, 
	effective_date AS EFFECTIVEDATE, 
	date_cleared AS DATECLEARED, 
	date_initiated AS DATEINITIATED, 
	bill_amount AS BILLAMOUNT, 
	paid_amount AS PAIDAMOUNT, 
	fee_amount AS FEEAMOUNT, 
	payment_mode AS PAYMENTMODE, 
	status AS TRANSACTIONSTATUS, 
	approval_code AS APPROVALCODE, 
	authorized_by AS AUTHORIZEDBY, 
	originalbillamount AS ORIGINALBILLAMOUNT, 
	minimumpayment AS MINIMUMPAYMENT, 
	DCACCOUNTNUMBER, 
	stoppaperinvoices AS PAPERPROCESSINGCODE
	FROM FIL_TransactionID
),