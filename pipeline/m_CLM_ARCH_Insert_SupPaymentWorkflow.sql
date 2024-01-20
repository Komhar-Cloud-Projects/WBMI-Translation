WITH
SQ_SupPaymentWorkflowStage AS (
	SELECT
		SupPaymentWorkflowStageId,
		ExtractDate,
		SourceSystemId,
		SupPaymentWorkflowId,
		CreatedDate,
		CreatedUserId,
		ModifiedDate,
		ModifiedUserId,
		PaymentWorkflow
	FROM SupPaymentWorkflowStage
),
EXPTRANS AS (
	SELECT
	SupPaymentWorkflowStageId,
	ExtractDate,
	SourceSystemId,
	SupPaymentWorkflowId,
	CreatedDate,
	CreatedUserId,
	ModifiedDate,
	ModifiedUserId,
	PaymentWorkflow,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_SupPaymentWorkflowStage
),
ArchSupPaymentWorkflowStage AS (
	INSERT INTO ArchSupPaymentWorkflowStage
	(AuditId, SupPaymentWorkflowStageId, ExtractDate, SourceSystemId, SupPaymentWorkflowId, CreatedDate, CreatedUserId, ModifiedDate, ModifiedUserId, PaymentWorkflow)
	SELECT 
	AUDITID, 
	SUPPAYMENTWORKFLOWSTAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	SUPPAYMENTWORKFLOWID, 
	CREATEDDATE, 
	CREATEDUSERID, 
	MODIFIEDDATE, 
	MODIFIEDUSERID, 
	PAYMENTWORKFLOW
	FROM EXPTRANS
),