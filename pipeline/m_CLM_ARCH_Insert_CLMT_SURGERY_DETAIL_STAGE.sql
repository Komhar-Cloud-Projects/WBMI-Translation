WITH
SQ_clmt_surgery_detail_stage AS (
	SELECT 
	C.ClmtSurgeryDetailStageId, 
	C.clmt_surgery_detail_id, 
	C.surgery_type_cd, 
	C.source_system_id, 
	C.created_ts, 
	C.created_user_id, 
	C.modified_ts, 
	C.modified_user_id, 
	C.ExtractDate, 
	C.SourceSystemId
	 
	FROM
	 clmt_surgery_detail_stage C
	
	--Where
	--clmt_surgery_detail_stage.created_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
	--OR
	--clmt_surgery_detail_stage.modified_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_clmt_surgery_detail_stage AS (
	SELECT
	ClmtSurgeryDetailStageId,
	clmt_surgery_detail_id,
	surgery_type_cd,
	source_system_id,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_clmt_surgery_detail_stage
),
arch_clmt_surgery_detail_stage AS (
	INSERT INTO arch_clmt_surgery_detail_stage
	(ClmtSurgeryDetailStageId, clmt_surgery_detail_id, surgery_type_cd, source_system_id, created_ts, created_user_id, modified_ts, modified_user_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CLMTSURGERYDETAILSTAGEID, 
	CLMT_SURGERY_DETAIL_ID, 
	SURGERY_TYPE_CD, 
	SOURCE_SYSTEM_ID, 
	CREATED_TS, 
	CREATED_USER_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_clmt_surgery_detail_stage
),