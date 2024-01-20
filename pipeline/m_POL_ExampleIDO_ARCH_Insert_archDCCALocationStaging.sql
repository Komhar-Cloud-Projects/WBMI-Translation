WITH
SQ_DCCALocationStaging AS (
	SELECT
		DCCALocationStagingId,
		ExtractDate,
		SourceSystemId,
		CA_LocationId,
		SessionId,
		Id,
		Description,
		EstimatedAnnualRenumeration,
		Territory,
		Number
	FROM DCCALocationStaging
),
EXP_Metadata AS (
	SELECT
	DCCALocationStagingId,
	ExtractDate,
	SourceSystemId,
	CA_LocationId,
	SessionId,
	Id,
	Description,
	EstimatedAnnualRenumeration,
	Territory,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	Number
	FROM SQ_DCCALocationStaging
),
ArchDCCALocationStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCALocationStaging
	(ExtractDate, SourceSystemId, AuditId, CA_LocationId, SessionId, Id, Description, EstimatedAnnualRenumeration, Territory, Number)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	CA_LOCATIONID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	ESTIMATEDANNUALRENUMERATION, 
	TERRITORY, 
	NUMBER
	FROM EXP_Metadata
),