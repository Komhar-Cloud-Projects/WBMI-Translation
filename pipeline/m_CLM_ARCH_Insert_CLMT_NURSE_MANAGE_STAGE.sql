WITH
SQ_clmt_nurse_manage_stage AS (
	SELECT 
	C.ClmtNurseManageStageId, 
	C.clmt_nurse_manage_id, 
	C.tch_claim_nbr, 
	C.tch_client_id, 
	C.pms_policy_sym, 
	C.pms_policy_num, 
	C.pms_policy_mod, 
	C.pms_date_of_loss, 
	C.pms_loss_occurence, 
	C.pms_loss_claimant, 
	C.source_system_id, 
	C.estimated_savings_amount, 
	C.created_ts, 
	C.created_user_id, 
	C.modified_ts, 
	C.modified_user_id, 
	C.ExtractDate, 
	C.SourceSystemId 
	
	FROM
	 clmt_nurse_manage_stage C
	
	--WHERE
	--clmt_nurse_manage_stage.created_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
	--OR
	--clmt_nurse_manage_stage.modified_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_clmt_nurse_manage_stage AS (
	SELECT
	ClmtNurseManageStageId,
	clmt_nurse_manage_id,
	tch_claim_nbr,
	tch_client_id,
	pms_policy_sym,
	pms_policy_num,
	pms_policy_mod,
	pms_date_of_loss,
	pms_loss_occurence,
	pms_loss_claimant,
	source_system_id,
	estimated_savings_amount,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_clmt_nurse_manage_stage
),
arch_clmt_nurse_manage_stage AS (
	INSERT INTO arch_clmt_nurse_manage_stage
	(ClmtNurseManageStageId, clmt_nurse_manage_id, tch_claim_nbr, tch_client_id, pms_policy_sym, pms_policy_num, pms_policy_mod, pms_date_of_loss, pms_loss_occurence, pms_loss_claimant, source_system_id, estimated_savings_amount, created_date, created_user_id, modified_date, modified_user_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CLMTNURSEMANAGESTAGEID, 
	CLMT_NURSE_MANAGE_ID, 
	TCH_CLAIM_NBR, 
	TCH_CLIENT_ID, 
	PMS_POLICY_SYM, 
	PMS_POLICY_NUM, 
	PMS_POLICY_MOD, 
	PMS_DATE_OF_LOSS, 
	PMS_LOSS_OCCURENCE, 
	PMS_LOSS_CLAIMANT, 
	SOURCE_SYSTEM_ID, 
	ESTIMATED_SAVINGS_AMOUNT, 
	created_ts AS CREATED_DATE, 
	CREATED_USER_ID, 
	modified_ts AS MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_clmt_nurse_manage_stage
),