WITH
SQ_WB_BIL_RemittanceStage AS (
	SELECT
		RemittanceStageId,
		ModifiedUserId,
		ModifiedDate,
		TransactionId,
		TransactionType,
		TransactionData,
		RemittanceSource,
		ProcessedStatusCode,
		ErrorDescription,
		PaymentId,
		ReconciliationDate,
		VendorAccountNumber,
		InvoiceNumber,
		CustomerName,
		EffectiveDate,
		DateCleared,
		DateInitiated,
		BillAmount,
		PaidAmount,
		FeeAmount,
		PaymentMode,
		TransactionStatus,
		ApprovalCode,
		AuthorizedBy,
		OriginalBillAmount,
		MinimumPayment,
		DCAccountNumber,
		PaperProcessingCode,
		ReturnCode
	FROM WB_BIL_RemittanceStage
	WHERE WB_BIL_RemittanceStage.ProcessedStatusCode = 'N' 
	AND
	(WB_BIL_RemittanceStage.TransactionType  IN ('Chargeback', 'Failed') OR WB_BIL_RemittanceStage.ReturnCode is not null) 
	AND
	WB_BIL_RemittanceStage.RemittanceSource in (@{pipeline().parameters.SOURCELIST})
),
exp_Prep_4_Stage_2_Service_Processing AS (
	SELECT
	RemittanceStageId,
	ModifiedDate,
	TransactionId,
	TransactionType,
	VendorAccountNumber AS Account,
	InvoiceNumber,
	CustomerName,
	EffectiveDate,
	DateCleared,
	DateInitiated,
	PaidAmount,
	PaymentMode,
	AuthorizedBy,
	OriginalBillAmount,
	MinimumPayment,
	DCAccountNumber,
	PaperProcessingCode,
	ReturnCode,
	RemittanceSource
	FROM SQ_WB_BIL_RemittanceStage
),
rtr_TransactionProcessing AS (
	SELECT
	RemittanceStageId,
	Account,
	InvoiceNumber,
	CustomerName,
	EffectiveDate,
	DateCleared,
	DateInitiated,
	PaidAmount,
	PaymentMode,
	AuthorizedBy,
	TransactionId,
	ModifiedDate,
	OriginalBillAmount,
	MinimumPayment,
	DCAccountNumber,
	PaperProcessingCode,
	TransactionType,
	ReturnCode,
	RemittanceSource
	FROM exp_Prep_4_Stage_2_Service_Processing
),
rtr_TransactionProcessing_ProcessPayments AS (SELECT * FROM rtr_TransactionProcessing WHERE TransactionType = 'Payment'),
rtr_TransactionProcessing_ProcessAdverseActions AS (SELECT * FROM rtr_TransactionProcessing WHERE TransactionType = 'Reversal'  OR  TransactionType = 'Failed' OR  NOT ISNULL(ReturnCode)),
exp_ProcessPayment AS (
	SELECT
	RemittanceStageId,
	Account,
	InvoiceNumber,
	CustomerName,
	EffectiveDate,
	DateCleared,
	DateInitiated,
	PaidAmount,
	PaymentMode,
	AuthorizedBy,
	TransactionId,
	ModifiedDate,
	'P' AS Out_Alloc_Class_code,
	-- *INF*: SYSTIMESTAMP()  ||  'batchNumber'  ||  TO_CHAR( v_batch_number)  
	CURRENT_TIMESTAMP() || 'batchNumber' || TO_CHAR(v_batch_number) AS Out_Paymentbatch,
	-- *INF*: --iif(IVR and CC, PPCC, PPA) or (Internet and CC, CCP, WPA)
	'' AS Out_PaymentMethod,
	-- *INF*: IIF(v_SeqNumber = 0,@{pipeline().parameters.BATCHSIZE},v_SeqNumber + 1)
	IFF(v_SeqNumber = 0, @{pipeline().parameters.BATCHSIZE}, v_SeqNumber + 1) AS v_SeqNumber,
	-- *INF*: TRUNC(v_SeqNumber / @{pipeline().parameters.BATCHSIZE},0)
	TRUNC(v_SeqNumber / @{pipeline().parameters.BATCHSIZE},0) AS v_batch_number,
	v_SeqNumber AS Out_SeqNumber,
	v_batch_number AS Out_batchNumber,
	OriginalBillAmount AS OriginalBillAmount1,
	MinimumPayment AS MinimumPayment1,
	DCAccountNumber AS DCAccountNumber1,
	PaperProcessingCode AS PaperProcessingCode1,
	RemittanceSource AS RemittanceSource1
	FROM rtr_TransactionProcessing_ProcessPayments
),
agg_ProcessPayment AS (
	SELECT
	Out_batchNumber AS Out_batch_number,
	Out_Paymentbatch,
	Out_SeqNumber
	FROM exp_ProcessPayment
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Out_batch_number ORDER BY NULL) = 1
),
ProcessPayment AS (-- ProcessPayment

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
fil_Payment_errors AS (
	SELECT
	tns2_ErrorMessage
	FROM ProcessPayment
	WHERE NOT ISNULL(tns2_ErrorMessage)
),
Payment_Error_file AS (
	INSERT INTO Payment_Error_file
	(Error_Message)
	SELECT 
	tns2_ErrorMessage AS ERROR_MESSAGE
	FROM fil_Payment_errors
),
exp_Process_adverseActions AS (
	SELECT
	RemittanceStageId AS RemittanceStageId3,
	ReturnCode AS ReturnCode3,
	RemittanceSource AS RemittanceSource3,
	-- *INF*: CONCAT('Adv Action' , TO_CHAR( v_advAct_batch) ) 
	CONCAT('Adv Action', TO_CHAR(v_advAct_batch)) AS Out_AdvAct_batch,
	v_AdvAct_seq AS Out_AdvAct_sequence,
	v_advAct_batch AS Out_AdvAct_BatchNumber,
	-- *INF*: IIF(v_AdvAct_seq= 0,@{pipeline().parameters.BATCHSIZE},v_AdvAct_seq + 1)
	IFF(v_AdvAct_seq = 0, @{pipeline().parameters.BATCHSIZE}, v_AdvAct_seq + 1) AS v_AdvAct_seq,
	-- *INF*: TRUNC(v_AdvAct_seq  / @{pipeline().parameters.BATCHSIZE},0)
	TRUNC(v_AdvAct_seq / @{pipeline().parameters.BATCHSIZE},0) AS v_advAct_batch,
	-- *INF*: DECODE(TRUE,
	-- RemittanceSource3 = 'B','NF',
	-- ReturnCode3)
	-- -- Directbiller defaults to 'NF' while FIS sends unique return codes for ACH returns
	DECODE(
	    TRUE,
	    RemittanceSource3 = 'B', 'NF',
	    ReturnCode3
	) AS Out_ReasonCode
	FROM rtr_TransactionProcessing_ProcessAdverseActions
),
agg_ProcessAdverseAction AS (
	SELECT
	Out_AdvAct_batch,
	Out_AdvAct_sequence,
	Out_AdvAct_BatchNumber
	FROM exp_Process_adverseActions
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Out_AdvAct_BatchNumber ORDER BY NULL) = 1
),
web_Process_AdverseAction1 AS (-- web_Process_AdverseAction1

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
fil_ErrorMessage AS (
	SELECT
	tns2_ErrorMessage
	FROM web_Process_AdverseAction1
	WHERE NOT ISNULL(tns2_ErrorMessage)
),
AdverseAction_Error_File AS (
	INSERT INTO AdverseAction_Error_File
	(Error_Message)
	SELECT 
	tns2_ErrorMessage AS ERROR_MESSAGE
	FROM fil_ErrorMessage
),
exp_error_handling AS (
	SELECT
	tns2_ErrorMessage AS tns2_ErrorMessage1,
	-- *INF*: ERROR(tns2_ErrorMessage1)
	ERROR(tns2_ErrorMessage1) AS v_Workflow_Error
	FROM fil_ErrorMessage
),
AdverseAction_Error_Handling_File AS (
	INSERT INTO AdverseAction_Error_Handling_File
	(Error_message)
	SELECT 
	tns2_ErrorMessage1 AS ERROR_MESSAGE
	FROM exp_error_handling
),
exp_Payment_Error_Handling AS (
	SELECT
	tns2_ErrorMessage,
	-- *INF*: ERROR(tns2_ErrorMessage ) 
	ERROR(tns2_ErrorMessage) AS v_Error_handling_process
	FROM fil_Payment_errors
),
Payment_Error_Handling_file AS (
	INSERT INTO Payment_Error_Handling_file
	(Error_Message)
	SELECT 
	tns2_ErrorMessage AS ERROR_MESSAGE
	FROM exp_Payment_Error_Handling
),