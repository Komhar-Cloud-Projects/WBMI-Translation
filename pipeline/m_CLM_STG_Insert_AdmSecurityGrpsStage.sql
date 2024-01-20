WITH
SQ_adm_security_grps AS (
	SELECT
		group_security_id,
		group_type,
		created_date,
		group_name,
		modified_date,
		modified_user_id
	FROM adm_security_grps
),
EXPTRANS AS (
	SELECT
	group_security_id,
	group_type,
	created_date,
	group_name,
	modified_date,
	modified_user_id,
	SYSDATE AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId
	FROM SQ_adm_security_grps
),
AdmSecurityGrpsStage AS (
	TRUNCATE TABLE AdmSecurityGrpsStage;
	INSERT INTO AdmSecurityGrpsStage
	(GroupSecurityId, GroupType, CreatedDate, GroupName, ModifiedDate, ModifiedUserId, ExtractDate, SourceSystemId)
	SELECT 
	group_security_id AS GROUPSECURITYID, 
	group_type AS GROUPTYPE, 
	created_date AS CREATEDDATE, 
	group_name AS GROUPNAME, 
	modified_date AS MODIFIEDDATE, 
	modified_user_id AS MODIFIEDUSERID, 
	EXTRACTDATE, 
	SOURCESYSTEMID
	FROM EXPTRANS
),