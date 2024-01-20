WITH
SQ_InsurPayPaymentApproval AS (
	select InsurPayBatchId,
		ApprovalStatus,
		ApprovalByUserId,
		ApprovalDate,
		DenialReason,
		CreatedUserId,
		CreatedDate,
		ModifiedUserId,
		ModifiedDate
	from dbo.InsurPayPaymentApproval
	where (CreatedDate  >= '@{pipeline().parameters.SELECTION_START_TS}'
	    or ModifiedDate  >= '@{pipeline().parameters.SELECTION_START_TS}')
),
EXP_Source AS (
	SELECT
	InsurPayBatchId,
	ApprovalStatus,
	ApprovalByUserId,
	ApprovalDate,
	DenialReason,
	CreatedUserId,
	CreatedDate,
	ModifiedUserId,
	ModifiedDate,
	CURRENT_TIMESTAMP AS ExtractDate,
	'InsurPay' AS SourceSystemId
	FROM SQ_InsurPayPaymentApproval
),
InsurPayPaymentApprovalStage AS (
	TRUNCATE TABLE InsurPayPaymentApprovalStage;
	INSERT INTO InsurPayPaymentApprovalStage
	(ExtractDate, SourceSystemId, InsurPayBatchId, ApprovalStatus, ApprovalByUserId, ApprovalDate, DenialReason, CreatedUserId, CreatedDate, ModifiedUserId, ModifiedDate)
	SELECT 
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
	FROM EXP_Source
),