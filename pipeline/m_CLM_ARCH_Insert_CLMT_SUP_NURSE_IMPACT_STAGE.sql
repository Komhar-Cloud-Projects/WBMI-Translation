WITH
SQ_sup_nurse_impact_stage AS (
	SELECT 
	S.SupNurseImpactStageId, 
	S.impact_type, 
	S.impact_category, 
	S.description, 
	S.created_date, 
	S.created_user_id, 
	S.modified_date, 
	S.modified_user_id, 
	S.expiration_date, 
	S.ExtractDate, 
	S.SourceSystemId
	 
	FROM
	 sup_nurse_impact_stage S
	
	--Where
	--sup_nurse_impact_stage.created_date >= --'@{pipeline().parameters.SELECTION_START_TS}'
	--OR
	--sup_nurse_impact_stage.modified_date >= --'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_sup_nurse_impact_stage AS (
	SELECT
	SupNurseImpactStageId,
	impact_type,
	impact_category,
	description,
	created_date,
	created_user_id,
	modified_date,
	modified_user_id,
	expiration_date,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_sup_nurse_impact_stage
),
arch_sup_nurse_impact_stage AS (
	INSERT INTO arch_sup_nurse_impact_stage
	(SupNurseImpactStageId, impact_type, impact_category, description, created_date, created_user_id, modified_date, modified_user_id, expiration_date, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	SUPNURSEIMPACTSTAGEID, 
	IMPACT_TYPE, 
	IMPACT_CATEGORY, 
	DESCRIPTION, 
	CREATED_DATE, 
	CREATED_USER_ID, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXPIRATION_DATE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_sup_nurse_impact_stage
),