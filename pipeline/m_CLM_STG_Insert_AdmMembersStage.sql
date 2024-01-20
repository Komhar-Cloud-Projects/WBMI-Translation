WITH
SQ_adm_members AS (
	SELECT
		member_id,
		UserID,
		date_last_login,
		created_date,
		modified_date,
		modified_user_id
	FROM adm_members
),
EXPTRANS AS (
	SELECT
	member_id,
	UserID,
	date_last_login,
	created_date,
	modified_date,
	modified_user_id,
	SYSDATE AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId
	FROM SQ_adm_members
),
AdmMembersStage AS (
	TRUNCATE TABLE AdmMembersStage;
	INSERT INTO AdmMembersStage
	(MemberId, UserID, DateLastLogin, CreatedDate, ModifiedDate, ModifiedUserId, ExtractDate, SourceSystemId)
	SELECT 
	member_id AS MEMBERID, 
	USERID, 
	date_last_login AS DATELASTLOGIN, 
	created_date AS CREATEDDATE, 
	modified_date AS MODIFIEDDATE, 
	modified_user_id AS MODIFIEDUSERID, 
	EXTRACTDATE, 
	SOURCESYSTEMID
	FROM EXPTRANS
),