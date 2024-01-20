WITH
SQ_DCCROccupancyStage AS (
	SELECT
		DCCROccupancyStageId,
		CR_OccupancyId,
		SessionId,
		Id,
		CrimeClass,
		OccupancyTypeMonoline,
		Description,
		ShortDescription,
		RateGroup,
		RateGroupOverride,
		ExtractDate,
		SourceSystemId
	FROM DCCROccupancyStage
),
EXP_Metadata AS (
	SELECT
	DCCROccupancyStageId,
	CR_OccupancyId,
	SessionId,
	Id,
	CrimeClass,
	OccupancyTypeMonoline,
	Description,
	ShortDescription,
	RateGroup,
	RateGroupOverride,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCROccupancyStage
),
ArchDCCROccupancyStage AS (
	INSERT INTO ArchDCCROccupancyStage
	(DCCROccupancyStageId, CR_OccupancyId, SessionId, Id, CrimeClass, OccupancyTypeMonoline, Description, ShortDescription, RateGroup, RateGroupOverride, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCCROCCUPANCYSTAGEID, 
	CR_OCCUPANCYID, 
	SESSIONID, 
	ID, 
	CRIMECLASS, 
	OCCUPANCYTYPEMONOLINE, 
	DESCRIPTION, 
	SHORTDESCRIPTION, 
	RATEGROUP, 
	RATEGROUPOVERRIDE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),