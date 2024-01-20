WITH
SQ_FIS_CL_MerchantActivityFile_Source_FlatFile AS (

-- TODO Manual --

),
FIL_ValidTransactions AS (
	SELECT
	LineNumber, 
	Count, 
	ReconciliationDate, 
	TransactionId, 
	DCAccountNumber, 
	InvoiceNumber, 
	CustomerName, 
	EffectiveDate, 
	DateCleared, 
	DateInitiated, 
	BillAmount, 
	PaidAmount, 
	FeeAmount, 
	PaymentMode, 
	Status, 
	ApprovalCode, 
	AccountKey, 
	PaymentType, 
	AuthorizedBy, 
	Designator, 
	Division, 
	StateOrProductDescription, 
	PolicyTermEffectiveDate
	FROM SQ_FIS_CL_MerchantActivityFile_Source_FlatFile
	WHERE IS_NUMBER(LineNumber)
),
EXP_Load_Remittance_Stage AS (
	SELECT
	LineNumber AS number,
	ReconciliationDate AS in_reconciliation_date,
	-- *INF*: TO_DATE(in_reconciliation_date,'MM/DD/YY')
	TO_TIMESTAMP(in_reconciliation_date, 'MM/DD/YY') AS out_reconciliation_date,
	TransactionId AS transaction_id,
	DCAccountNumber AS in_VendorAccountNumber,
	InvoiceNumber AS invoice_number,
	CustomerName AS customer_name,
	-- *INF*: DECODE(TRUE,
	-- SUBSTR(in_VendorAccountNumber,1,3) = 'ADA',
	-- invoice_number,
	-- customer_name)
	-- -- For ADA type files, pick name from field 6 for Invoice number else use designated name
	DECODE(
	    TRUE,
	    SUBSTR(in_VendorAccountNumber, 1, 3) = 'ADA', invoice_number,
	    customer_name
	) AS out_customer_name,
	-- *INF*: DECODE(TRUE,
	-- SUBSTR(in_VendorAccountNumber,1,3) = 'ADA',
	-- NULL,
	-- invoice_number)
	-- -- For ADA type files, clear name from field 6 for Invoice number else use designated invoice number
	DECODE(
	    TRUE,
	    SUBSTR(in_VendorAccountNumber, 1, 3) = 'ADA', NULL,
	    invoice_number
	) AS out_invoice_number,
	EffectiveDate AS in_effective_date,
	-- *INF*: TO_DATE(in_effective_date,'MM-DD-YY HH24:MI:SS')
	-- --TO_DATE(substr(in_effective_date,1,(LENGTH(in_effective_date) - 3)),'MM/DD/YY HH24:MI:SS')
	TO_TIMESTAMP(in_effective_date, 'MM-DD-YY HH24:MI:SS') AS out_effective_date,
	DateCleared AS in_date_cleared,
	-- *INF*: TO_DATE(in_date_cleared,'MM/DD/YYYY HH24:MI')
	-- --TO_DATE(in_date_cleared,'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP(in_date_cleared, 'MM/DD/YYYY HH24:MI') AS out_date_cleared,
	DateInitiated AS in_date_initiated,
	-- *INF*: SYSDATE
	-- --TO_DATE(in_date_initiated,'MM/DD/YYYY HH24:MI:SS')
	CURRENT_TIMESTAMP AS out_date_initiated,
	BillAmount AS bill_amount,
	PaidAmount AS paid_amount,
	FeeAmount AS fee_amount,
	PaymentMode AS payment_mode,
	Status AS status,
	ApprovalCode AS approval_code,
	AuthorizedBy AS i_authorized_by,
	-- *INF*: IIF(ISNULL(i_authorized_by),'USER',i_authorized_by)
	IFF(i_authorized_by IS NULL, 'USER', i_authorized_by) AS o_authorized_by,
	Division,
	StateOrProductDescription,
	PolicyTermEffectiveDate,
	number  || ',' || in_reconciliation_date || ',' || transaction_id || ',' || in_VendorAccountNumber || ',' || invoice_number || ',' || customer_name || ',' || in_effective_date || ',' || in_date_cleared || ',' || in_date_initiated || ',' || bill_amount || ',' || paid_amount || ',' || fee_amount || ',' || payment_mode || ',' || status || ',' || approval_code || ',' || i_authorized_by  || ',' || originalbillamount || ',' || minimumpayment || ',' || DCAccountNumber || ',' || stoppaperinvoices || ',' || Division || ','  || StateOrProductDescription || ',' || PolicyTermEffectiveDate AS out_TransactionData,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS ModifiedDate,
	'F' AS out_RemittanceSource,
	'infmatca' AS out_ModifierUserID,
	-- *INF*: IIF(paid_amount  >  0,  'Payment', DECODE(payment_mode,
	-- 'Checking', 'Failed',
	-- 'Business Checking', 'Failed' ,
	-- 'Savings', 'Failed' ,
	-- 'ES', 'Failed' ,
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
	        'ES', 'Failed',
	        'Visa', 'Chargeback',
	        'MasterCard', 'Chargeback',
	        'American Express', 'Chargeback',
	        'Discover', 'Chargeback',
	        'Reversal'
	    )
	) AS v_TransactionType,
	-- *INF*: v_TransactionType
	-- 
	-- 
	-- 
	-- --IIF(paid_amount  >  0,  'Payment', DECODE(payment_mode,
	-- --'Checking', 'Failed',
	-- --'Business Checking', 'Failed' ,
	-- --'Savings', 'Failed' ,
	-- --'ES', 'Failed' ,
	-- --'Visa', 'Chargeback',
	-- --'MasterCard', 'Chargeback',
	-- --'American Express','Chargeback',
	-- --'Discover', 'Chargeback',
	--  --'Reversal'))
	v_TransactionType AS out_TransactionType,
	-- *INF*: DECODE(payment_mode,
	-- 'Checking', 'WPA',
	-- 'Business Checking', 'WPA' ,
	-- 'Savings', 'WPA' ,
	-- 'ES', 'WPA' ,
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
	    'ES', 'WPA',
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
	AccountKey AS DCAccountNumber,
	'Vendor Account Reference number missing Transaction ID =  '  || transaction_id AS var_vendor_AccountMissing_Message,
	-- *INF*: IIF(ISNULL(in_VendorAccountNumber), ERROR(var_vendor_AccountMissing_Message), in_VendorAccountNumber)
	IFF(
	    in_VendorAccountNumber IS NULL, ERROR(var_vendor_AccountMissing_Message),
	    in_VendorAccountNumber
	) AS out_Vendor_Account,
	-- *INF*: IIF(v_TransactionType  <>  'Payment', Division, NULL)
	IFF(v_TransactionType <> 'Payment', Division, NULL) AS out_ReturnCode
	FROM FIL_ValidTransactions
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
	EXP_Load_Remittance_Stage.number, 
	EXP_Load_Remittance_Stage.out_ModifierUserID AS ModifierUserID, 
	EXP_Load_Remittance_Stage.ModifiedDate, 
	EXP_Load_Remittance_Stage.transaction_id, 
	LKP_WB_Bill_Remittance_stage.IN_TransactionType, 
	EXP_Load_Remittance_Stage.out_TransactionData AS TransactionData, 
	LKP_WB_Bill_Remittance_stage.IN_RemittanceSource, 
	EXP_Load_Remittance_Stage.out_ProcessFlag AS ProcessFlag, 
	EXP_Load_Remittance_Stage.out_reconciliation_date AS reconciliation_date, 
	EXP_Load_Remittance_Stage.out_Vendor_Account AS VendorAccountNumber, 
	EXP_Load_Remittance_Stage.out_invoice_number AS invoice_number, 
	EXP_Load_Remittance_Stage.out_customer_name AS customer_name, 
	EXP_Load_Remittance_Stage.out_effective_date AS effective_date, 
	EXP_Load_Remittance_Stage.out_date_cleared AS date_cleared, 
	EXP_Load_Remittance_Stage.out_date_initiated AS date_initiated, 
	EXP_Load_Remittance_Stage.bill_amount, 
	EXP_Load_Remittance_Stage.paid_amount, 
	EXP_Load_Remittance_Stage.fee_amount, 
	EXP_Load_Remittance_Stage.out_PaymentMode AS payment_mode, 
	EXP_Load_Remittance_Stage.status, 
	EXP_Load_Remittance_Stage.approval_code, 
	EXP_Load_Remittance_Stage.o_authorized_by AS authorized_by, 
	EXP_Load_Remittance_Stage.originalbillamount, 
	EXP_Load_Remittance_Stage.minimumpayment, 
	EXP_Load_Remittance_Stage.DCAccountNumber, 
	EXP_Load_Remittance_Stage.stoppaperinvoices, 
	EXP_Load_Remittance_Stage.out_ReturnCode AS ReturnCode
	FROM EXP_Load_Remittance_Stage
	LEFT JOIN LKP_WB_Bill_Remittance_stage
	ON LKP_WB_Bill_Remittance_stage.TransactionId = EXP_Load_Remittance_Stage.transaction_id AND LKP_WB_Bill_Remittance_stage.RemittanceSource = EXP_Load_Remittance_Stage.out_RemittanceSource AND LKP_WB_Bill_Remittance_stage.TransactionType = EXP_Load_Remittance_Stage.out_TransactionType
	WHERE ISNULL(Transaction_Id_Lookup)
),
WB_BIL_RemittanceStage AS (
	INSERT INTO WB_BIL_RemittanceStage
	(ModifiedUserId, ModifiedDate, TransactionId, TransactionType, TransactionData, RemittanceSource, ProcessedStatusCode, ReconciliationDate, VendorAccountNumber, InvoiceNumber, CustomerName, EffectiveDate, DateCleared, DateInitiated, BillAmount, PaidAmount, FeeAmount, PaymentMode, TransactionStatus, ApprovalCode, AuthorizedBy, OriginalBillAmount, MinimumPayment, DCAccountNumber, PaperProcessingCode, ReturnCode)
	SELECT 
	ModifierUserID AS MODIFIEDUSERID, 
	MODIFIEDDATE, 
	transaction_id AS TRANSACTIONID, 
	IN_TransactionType AS TRANSACTIONTYPE, 
	TRANSACTIONDATA, 
	IN_RemittanceSource AS REMITTANCESOURCE, 
	ProcessFlag AS PROCESSEDSTATUSCODE, 
	reconciliation_date AS RECONCILIATIONDATE, 
	VENDORACCOUNTNUMBER, 
	invoice_number AS INVOICENUMBER, 
	customer_name AS CUSTOMERNAME, 
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
	stoppaperinvoices AS PAPERPROCESSINGCODE, 
	RETURNCODE
	FROM FIL_TransactionID
),