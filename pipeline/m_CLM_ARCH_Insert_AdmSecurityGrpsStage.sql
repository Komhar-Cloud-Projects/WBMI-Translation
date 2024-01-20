WITH
SQ_AdmSecurityGrpsStage AS (
	SELECT
		AdmSecurityGrpsStageId,
		GroupSecurityId,
		GroupType,
		CreatedDate,
		GroupName,
		ModifiedDate,
		ModifiedUserId,
		ExtractDate,
		SourceSystemId
	FROM AdmSecurityGrpsStage
),
EXPTRANS AS (
	SELECT
	AdmSecurityGrpsStageId,
	GroupSecurityId,
	GroupType,
	CreatedDate,
	GroupName,
	ModifiedDate,
	ModifiedUserId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_AdmSecurityGrpsStage
),
ArchAdmSecurityGrpsStage AS (
	INSERT INTO ArchAdmSecurityGrpsStage
	(AdmSecurityGrpsStageId, GroupSecurityId, GroupType, CreatedDate, GroupName, ModifiedDate, ModifiedUserId, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	ADMSECURITYGRPSSTAGEID, 
	GROUPSECURITYID, 
	GROUPTYPE, 
	CREATEDDATE, 
	GROUPNAME, 
	MODIFIEDDATE, 
	MODIFIEDUSERID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXPTRANS
),