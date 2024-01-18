WITH
SQ_pms_clmt_summary_stage AS (
	SELECT 
	P.PmsClmtSummaryStageId, 
	P.pms_policy_sym, 
	P.pms_policy_num, 
	P.pms_policy_mod, 
	P.pms_date_of_loss, 
	P.pms_loss_occurence, 
	P.pms_loss_claimant, 
	P.ttd_rate, 
	P.daily_ttd_rate, 
	P.created_ts, 
	P.created_user_id, 
	P.modified_ts, 
	P.modified_user_id, 
	P.ExtractDate, 
	P.SourceSystemId 
	
	FROM
	 pms_clmt_summary_stage P
	
	--Where
	--pms_clmt_summary_stage.created_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
	--OR
	--pms_clmt_summary_stage.modified_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_pms_clmt_summary_stage AS (
	SELECT
	PmsClmtSummaryStageId,
	pms_policy_sym,
	pms_policy_num,
	pms_policy_mod,
	pms_date_of_loss,
	pms_loss_occurence,
	pms_loss_claimant,
	ttd_rate,
	daily_ttd_rate,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_pms_clmt_summary_stage
),
arch_pms_clmt_summary_stage AS (
	INSERT INTO arch_pms_clmt_summary_stage
	(PmsClmtSummaryStageId, pms_policy_sym, pms_policy_num, pms_policy_mod, pms_date_of_loss, pms_loss_occurence, pms_loss_claimant, ttd_rate, daily_ttd_rate, created_ts, created_user_id, modified_ts, modified_user_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	PMSCLMTSUMMARYSTAGEID, 
	PMS_POLICY_SYM, 
	PMS_POLICY_NUM, 
	PMS_POLICY_MOD, 
	PMS_DATE_OF_LOSS, 
	PMS_LOSS_OCCURENCE, 
	PMS_LOSS_CLAIMANT, 
	TTD_RATE, 
	DAILY_TTD_RATE, 
	CREATED_TS, 
	CREATED_USER_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_pms_clmt_summary_stage
),