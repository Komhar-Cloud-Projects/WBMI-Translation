WITH
SQ_AdmMbrSecurityStage AS (
	SELECT
		AdmMbrSecurityStageId,
		MemberId,
		GroupSecurityId,
		ModifiedDate,
		ModifiedUserId,
		ExtractDate,
		SourceSystemId
	FROM AdmMbrSecurityStage
),
EXPTRANS AS (
	SELECT
	AdmMbrSecurityStageId,
	MemberId,
	GroupSecurityId,
	ModifiedDate,
	ModifiedUserId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_AdmMbrSecurityStage
),
ArchAdmMbrSecurityStage AS (
	INSERT INTO ArchAdmMbrSecurityStage
	(AdmMbrSecurityStageId, MemberId, GroupSecurityId, ModifiedDate, ModifiedUserId, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	ADMMBRSECURITYSTAGEID, 
	MEMBERID, 
	GROUPSECURITYID, 
	MODIFIEDDATE, 
	MODIFIEDUSERID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXPTRANS
),