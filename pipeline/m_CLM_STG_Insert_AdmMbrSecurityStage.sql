WITH
SQ_adm_mbr_security AS (
	SELECT
		member_id,
		group_security_id,
		modified_date,
		modified_user_id
	FROM adm_mbr_security
),
EXPTRANS AS (
	SELECT
	member_id,
	group_security_id,
	modified_date,
	modified_user_id,
	SYSDATE AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId
	FROM SQ_adm_mbr_security
),
AdmMbrSecurityStage AS (
	TRUNCATE TABLE AdmMbrSecurityStage;
	INSERT INTO AdmMbrSecurityStage
	(MemberId, GroupSecurityId, ModifiedDate, ModifiedUserId, ExtractDate, SourceSystemId)
	SELECT 
	member_id AS MEMBERID, 
	group_security_id AS GROUPSECURITYID, 
	modified_date AS MODIFIEDDATE, 
	modified_user_id AS MODIFIEDUSERID, 
	EXTRACTDATE, 
	SOURCESYSTEMID
	FROM EXPTRANS
),