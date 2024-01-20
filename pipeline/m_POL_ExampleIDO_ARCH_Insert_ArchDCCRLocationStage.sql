WITH
SQ_DCCRLocationStage AS (
	SELECT
		DCCRLocationStageId,
		CR_LocationId,
		SessionId,
		Id,
		Number,
		Description,
		TerritoryGroup,
		RatableEmployees,
		ExtractDate,
		SourceSystemId
	FROM DCCRLocationStage
),
EXP_Metadata AS (
	SELECT
	DCCRLocationStageId,
	CR_LocationId,
	SessionId,
	Id,
	Number,
	Description,
	TerritoryGroup,
	RatableEmployees,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCRLocationStage
),
ArchDCCRLocationStage AS (
	INSERT INTO ArchDCCRLocationStage
	(DCCRLocationStageId, CR_LocationId, SessionId, Id, Number, Description, TerritoryGroup, RatableEmployees, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCCRLOCATIONSTAGEID, 
	CR_LOCATIONID, 
	SESSIONID, 
	ID, 
	NUMBER, 
	DESCRIPTION, 
	TERRITORYGROUP, 
	RATABLEEMPLOYEES, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),