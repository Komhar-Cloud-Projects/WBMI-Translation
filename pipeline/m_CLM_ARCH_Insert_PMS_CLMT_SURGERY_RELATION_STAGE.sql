WITH
SQ_pms_clmt_surgery_relation_stage AS (
	SELECT 
	P.PmsClaimantSurgeryRelationStageId, 
	P.pms_policy_sym, 
	P.pms_policy_num, 
	P.pms_policy_mod, 
	P.pms_date_of_loss, 
	P.pms_loss_occurence, 
	P.pms_loss_claimant, 
	P.clmt_surgery_detail_id, 
	P.modified_ts, 
	P.modified_user_id, 
	P.ExtractDate, 
	P.SourceSystemId 
	
	FROM
	 pms_clmt_surgery_relation_stage P
),
EXP_arch_pms_clmt_surgery_relation_stage AS (
	SELECT
	PmsClaimantSurgeryRelationStageId,
	pms_policy_sym,
	pms_policy_num,
	pms_policy_mod,
	pms_date_of_loss,
	pms_loss_occurence,
	pms_loss_claimant,
	clmt_surgery_detail_id,
	modified_ts,
	modified_user_id,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_pms_clmt_surgery_relation_stage
),
arch_pms_clmt_surgery_relation_stage AS (
	INSERT INTO arch_pms_clmt_surgery_relation_stage
	(PmsClaimantSurgeryRelationStageId, pms_policy_sym, pms_policy_num, pms_policy_mod, pms_date_of_loss, pms_loss_occurence, pms_loss_claimant, clmt_surgery_detail_id, modified_ts, modified_user_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	PMSCLAIMANTSURGERYRELATIONSTAGEID, 
	PMS_POLICY_SYM, 
	PMS_POLICY_NUM, 
	PMS_POLICY_MOD, 
	PMS_DATE_OF_LOSS, 
	PMS_LOSS_OCCURENCE, 
	PMS_LOSS_CLAIMANT, 
	CLMT_SURGERY_DETAIL_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_pms_clmt_surgery_relation_stage
),