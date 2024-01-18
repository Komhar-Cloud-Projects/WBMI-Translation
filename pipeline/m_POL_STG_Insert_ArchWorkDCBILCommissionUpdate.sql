WITH
SQ_WorkDCBILCommissionUpdate AS (
	SELECT
		WorkDCBILCommissionUpdateId,
		ExtractDate,
		SourceSystemId,
		PolicyReference,
		AuthorizationDate,
		AuthorizedAmount,
		TierAmount,
		UpdateType
	FROM WorkDCBILCommissionUpdate
),
EXp_Default AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	WorkDCBILCommissionUpdateId,
	ExtractDate,
	SourceSystemId,
	PolicyReference,
	AuthorizationDate,
	AuthorizedAmount,
	TierAmount,
	UpdateType
	FROM SQ_WorkDCBILCommissionUpdate
),
ArchWorkDCBILCommissionUpdate AS (
	INSERT INTO ArchWorkDCBILCommissionUpdate
	(ExtractDate, SourceSystemId, AuditId, WorkDCBILCommissionUpdateId, PolicyReference, AuthorizationDate, AuthorizedAmount, TierAmount, UpdateType)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	audit_id AS AUDITID, 
	WORKDCBILCOMMISSIONUPDATEID, 
	POLICYREFERENCE, 
	AUTHORIZATIONDATE, 
	AUTHORIZEDAMOUNT, 
	TIERAMOUNT, 
	UPDATETYPE
	FROM EXp_Default
),