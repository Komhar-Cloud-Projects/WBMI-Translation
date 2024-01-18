WITH
SQ_InsurPayPaymentApprovalStage AS (
	SELECT
		InsurPayPaymentApprovalStageId,
		ExtractDate,
		SourceSystemId,
		InsurPayBatchId,
		ApprovalStatus,
		ApprovalByUserId,
		ApprovalDate,
		DenialReason,
		CreatedUserId,
		CreatedDate,
		ModifiedUserId,
		ModifiedDate
	FROM InsurPayPaymentApprovalStage
),
EXPTRANS AS (
	SELECT
	InsurPayPaymentApprovalStageId,
	ExtractDate,
	SourceSystemId,
	InsurPayBatchId,
	ApprovalStatus,
	ApprovalByUserId,
	ApprovalDate,
	DenialReason,
	CreatedUserId,
	CreatedDate,
	ModifiedUserId,
	ModifiedDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_InsurPayPaymentApprovalStage
),
ArchInsurPayPaymentApprovalStage AS (
	INSERT INTO ArchInsurPayPaymentApprovalStage
	(AuditId, InsurPayPaymentApprovalStageId, ExtractDate, SourceSystemId, InsurPayBatchId, ApprovalStatus, ApprovalByUserId, ApprovalDate, DenialReason, CreatedUserId, CreatedDate, ModifiedUserId, ModifiedDate)
	SELECT 
	AUDITID, 
	INSURPAYPAYMENTAPPROVALSTAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	INSURPAYBATCHID, 
	APPROVALSTATUS, 
	APPROVALBYUSERID, 
	APPROVALDATE, 
	DENIALREASON, 
	CREATEDUSERID, 
	CREATEDDATE, 
	MODIFIEDUSERID, 
	MODIFIEDDATE
	FROM EXPTRANS
),