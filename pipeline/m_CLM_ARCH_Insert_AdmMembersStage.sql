WITH
SQ_AdmMembersStage AS (
	SELECT
		AdmMembersStageId,
		MemberId,
		UserID,
		DateLastLogin,
		CreatedDate,
		ModifiedDate,
		ModifiedUserId,
		ExtractDate,
		SourceSystemId
	FROM AdmMembersStage
),
EXPTRANS AS (
	SELECT
	AdmMembersStageId,
	MemberId,
	UserID,
	DateLastLogin,
	CreatedDate,
	ModifiedDate,
	ModifiedUserId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_AdmMembersStage
),
ArchAdmMembersStage AS (
	INSERT INTO ArchAdmMembersStage
	(AdmMembersStageId, MemberId, UserID, DateLastLogin, CreatedDate, ModifiedDate, ModifiedUserId, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	ADMMEMBERSSTAGEID, 
	MEMBERID, 
	USERID, 
	DATELASTLOGIN, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	MODIFIEDUSERID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXPTRANS
),