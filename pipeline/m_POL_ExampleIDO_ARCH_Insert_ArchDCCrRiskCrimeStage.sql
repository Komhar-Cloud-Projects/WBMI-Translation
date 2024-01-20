WITH
SQ_DcCrRiskCrimeStage AS (
	SELECT
		DcCrRiskCrimeStageId,
		CrRiskId,
		CrRiskCrimeId,
		SessionId,
		Id,
		Type,
		ExtractDate,
		SourceSystemId
	FROM DcCrRiskCrimeStage
),
EXP_Metadata AS (
	SELECT
	DcCrRiskCrimeStageId,
	CrRiskId,
	CrRiskCrimeId,
	SessionId,
	Id,
	Type,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DcCrRiskCrimeStage
),
ArchDCCrRiskCrimeStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCrRiskCrimeStage
	(DcCrRiskCrimeStageId, CrRiskId, CrRiskCrimeId, SessionId, Id, Type, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCCRRISKCRIMESTAGEID, 
	CRRISKID, 
	CRRISKCRIMEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),