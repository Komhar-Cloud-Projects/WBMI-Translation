WITH
SQ_SupPaymentWorkflow AS (
	select [SupPaymentWorkflowId]
	      ,[CreatedDate]
	      ,[CreatedUserId]
	      ,[ModifiedDate]
	      ,[ModifiedUserId]
	      ,[PaymentWorkFlow]
	from [dbo].[SupPaymentWorkflow]
	where (CreatedDate  >= '@{pipeline().parameters.SELECTION_START_TS}' 
	    or ModifiedDate  >= '@{pipeline().parameters.SELECTION_START_TS}')
),
EXPTRANS AS (
	SELECT
	SupPaymentWorkflowId,
	CreatedDate,
	CreatedUserId,
	ModifiedDate,
	ModifiedUserId,
	PaymentWorkFlow,
	CURRENT_TIMESTAMP AS ExtractDate,
	'InsurPay' AS SourceSystemId
	FROM SQ_SupPaymentWorkflow
),
SupPaymentWorkflowStage AS (
	TRUNCATE TABLE SupPaymentWorkflowStage;
	INSERT INTO SupPaymentWorkflowStage
	(ExtractDate, SourceSystemId, SupPaymentWorkflowId, CreatedDate, CreatedUserId, ModifiedDate, ModifiedUserId, PaymentWorkflow)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	SUPPAYMENTWORKFLOWID, 
	CREATEDDATE, 
	CREATEDUSERID, 
	MODIFIEDDATE, 
	MODIFIEDUSERID, 
	PaymentWorkFlow AS PAYMENTWORKFLOW
	FROM EXPTRANS
),