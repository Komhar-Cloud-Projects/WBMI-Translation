WITH
SQ_exceed_clmt_surgery_relation_stage AS (
	SELECT 
	E.ExceedClmtSurgeryRelationStageId, 
	E.tch_claim_nbr, 
	E.tch_client_id, 
	E.clmt_surgery_detail_id, 
	E.modified_ts, 
	E.modified_user_id, 
	E.ExtractDate, 
	E.SourceSystemId
	 
	FROM
	 exceed_clmt_surgery_relation_stage E
	
	--Where
	--exceed_clmt_surgery_relation_stage.modified_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_exceed_clmt_surgery_relation_stage AS (
	SELECT
	ExceedClmtSurgeryRelationStageId,
	tch_claim_nbr,
	tch_client_id,
	clmt_surgery_detail_id,
	modified_ts,
	modified_user_id,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_exceed_clmt_surgery_relation_stage
),
arch_exceed_clmt_surgery_relation_stage AS (
	INSERT INTO arch_exceed_clmt_surgery_relation_stage
	(ExceedClmtSurgeryRelationStageId, tch_claim_nbr, tch_client_id, clmt_surgery_detail_id, modified_ts, modified_user_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	EXCEEDCLMTSURGERYRELATIONSTAGEID, 
	TCH_CLAIM_NBR, 
	TCH_CLIENT_ID, 
	CLMT_SURGERY_DETAIL_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_exceed_clmt_surgery_relation_stage
),