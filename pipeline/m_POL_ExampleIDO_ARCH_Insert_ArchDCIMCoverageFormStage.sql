WITH
SQ_DCIMCoverageFormStage AS (
	SELECT
		DCIMCoverageFormStageId,
		ExtractDate,
		SourceSystemid,
		LineId,
		IM_CoverageFormId,
		SessionId,
		Id,
		Type,
		Description,
		Deleted
	FROM DCIMCoverageFormStage
),
EXP_Metadata AS (
	SELECT
	DCIMCoverageFormStageId,
	ExtractDate,
	SourceSystemid,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LineId,
	IM_CoverageFormId,
	SessionId,
	Id,
	Deleted,
	Type,
	Description
	FROM SQ_DCIMCoverageFormStage
),
ArchDCIMCoverageFormStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCIMCoverageFormStage
	(ExtractDate, SourceSystemid, AuditId, LineId, IM_CoverageFormId, SessionId, Id, Deleted, Type, Description)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	LINEID, 
	IM_COVERAGEFORMID, 
	SESSIONID, 
	ID, 
	DELETED, 
	TYPE, 
	DESCRIPTION
	FROM EXP_Metadata
),