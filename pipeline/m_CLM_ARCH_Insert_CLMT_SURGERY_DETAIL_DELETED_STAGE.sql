WITH
SQ_clmt_surgery_detail_deleted AS (
	SELECT 
	C.ClmtSurgeryDetailDeletedStageId, 
	C.clmt_surgery_detail_id, 
	C.surgery_type_cd, 
	C.ExtractDate, 
	C.SourceSystemId 
	
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.clmt_surgery_detail_deleted_stage C
),
EXP_arch_clmt_surgery_detail_deleted_stage AS (
	SELECT
	ClmtSurgeryDetailDeletedStageId,
	clmt_surgery_detail_id,
	surgery_type_cd,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_clmt_surgery_detail_deleted
),
arch_clmt_surgery_detail_deleted_stage AS (
	INSERT INTO arch_clmt_surgery_detail_deleted_stage
	(ClmtSurgeryDetailDeletedStageId, clmt_surgery_detail_id, surgery_type_cd, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CLMTSURGERYDETAILDELETEDSTAGEID, 
	CLMT_SURGERY_DETAIL_ID, 
	SURGERY_TYPE_CD, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_clmt_surgery_detail_deleted_stage
),