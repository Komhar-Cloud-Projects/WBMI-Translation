WITH
SQ_AgencySilverCircleTierStaging AS (
	SELECT
		AgencySilverCircleTierStageId,
		AgencyCode,
		SilverCircleYear,
		SilverCircleLevelDescription,
		HashKey,
		ModifiedUserId,
		ModifiedDate,
		ExtractDate,
		SourceSystemId
	FROM AgencySilverCircleTierStaging
),
EXP_Input_Output AS (
	SELECT
	AgencySilverCircleTierStageId AS SilverCircleAgencyStageId,
	AgencyCode,
	SilverCircleYear,
	SilverCircleLevelDescription,
	HashKey,
	ModifiedUserId,
	ModifiedDate,
	ExtractDate,
	@{pipeline().parameters.SOURCESYSTEMID} AS SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_AgencySilverCircleTierStaging
),
archAgencySilverCircleTierStaging AS (
	INSERT INTO archAgencySilverCircleTierStaging
	(AgencySilverCircleTierStageId, AgencyCode, SilverCircleYear, SilverCircleLevelDescription, HashKey, ModifiedUserId, ModifiedDate, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	SilverCircleAgencyStageId AS AGENCYSILVERCIRCLETIERSTAGEID, 
	AGENCYCODE, 
	SILVERCIRCLEYEAR, 
	SILVERCIRCLELEVELDESCRIPTION, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXP_Input_Output
),