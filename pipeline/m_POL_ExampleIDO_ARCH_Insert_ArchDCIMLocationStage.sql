WITH
SQ_DCIMLocationStage AS (
	SELECT
		DCIMLocationStageId,
		IMLocationId,
		SessionId,
		Id,
		CTGovernmentAgencies,
		ExtractDate,
		SourceSystemId,
		Description,
		Number
	FROM DCIMLocationStage
),
EXP_Metadata AS (
	SELECT
	DCIMLocationStageId,
	IMLocationId,
	SessionId,
	Id,
	CTGovernmentAgencies,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	Description,
	Number
	FROM SQ_DCIMLocationStage
),
ArchDCIMLocationStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCIMLocationStage
	(DCIMLocationStageId, IMLocationId, SessionId, Id, CTGovernmentAgencies, ExtractDate, SourceSystemId, AuditId, Description, Number)
	SELECT 
	DCIMLOCATIONSTAGEID, 
	IMLOCATIONID, 
	SESSIONID, 
	ID, 
	CTGOVERNMENTAGENCIES, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DESCRIPTION, 
	NUMBER
	FROM EXP_Metadata
),