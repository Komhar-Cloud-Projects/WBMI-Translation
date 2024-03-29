WITH
SQ_CLM_OFFSET_ONSET_STAGE AS (
	SELECT
		clm_offset_onset_id,
		tch_claim_nbr,
		off_onset_ts,
		off_policy_sym,
		off_policy_nbr,
		off_policy_mod,
		off_date_loss,
		off_occ_nbr,
		on_policy_sym,
		on_policy_num,
		on_policy_mod,
		on_date_loss,
		on_occ_nbr,
		process_sta_ind,
		entry_opr_id,
		entry_timestamp,
		update_opr_id,
		update_timestamp,
		off_line_business,
		on_line_business,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM CLM_OFFSET_ONSET_STAGE
	WHERE clm_offset_onset_stage.off_onset_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_CLM_OFFSET_ONSET_STAGE AS (
	SELECT
	clm_offset_onset_id AS CLM_OFFSET_ONSET_ID,
	tch_claim_nbr AS TCH_CLAIM_NBR,
	off_onset_ts AS OFF_ONSET_TS,
	off_policy_sym AS OFF_POLICY_SYM,
	off_policy_nbr AS OFF_POLICY_NBR,
	off_policy_mod AS OFF_POLICY_MOD,
	off_date_loss AS OFF_DATE_LOSS,
	off_occ_nbr AS OFF_OCC_NBR,
	on_policy_sym AS ON_POLICY_SYM,
	on_policy_num AS ON_POLICY_NUM,
	on_policy_mod AS ON_POLICY_MOD,
	on_date_loss AS ON_DATE_LOSS,
	on_occ_nbr AS ON_OCC_NBR,
	process_sta_ind AS PROCESS_STA_IND,
	entry_opr_id AS ENTRY_OPR_ID,
	entry_timestamp AS ENTRY_TIMESTAMP,
	update_opr_id AS UPDATE_OPR_ID,
	update_timestamp AS UPDATE_TIMESTAMP,
	off_line_business AS OFF_LINE_BUSINESS,
	on_line_business AS ON_LINE_BUSINESS,
	extract_date AS EXTRACT_DATE,
	as_of_date AS AS_OF_DATE,
	record_count AS RECORD_COUNT,
	source_system_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_CLM_OFFSET_ONSET_STAGE
),
ARCH_CLM_OFFSET_ONSET_STAGE AS (
	INSERT INTO ARCH_CLM_OFFSET_ONSET_STAGE
	(CLM_OFFSET_ONSET_ID, TCH_CLAIM_NBR, OFF_ONSET_TS, OFF_POLICY_SYM, OFF_POLICY_NBR, OFF_POLICY_MOD, OFF_DATE_LOSS, OFF_OCC_NBR, ON_POLICY_SYM, ON_POLICY_NUM, ON_POLICY_MOD, ON_DATE_LOSS, ON_OCC_NBR, PROCESS_STA_IND, ENTRY_OPR_ID, ENTRY_TIMESTAMP, UPDATE_OPR_ID, UPDATE_TIMESTAMP, OFF_LINE_BUSINESS, ON_LINE_BUSINESS, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, AUDIT_ID)
	SELECT 
	CLM_OFFSET_ONSET_ID, 
	TCH_CLAIM_NBR, 
	OFF_ONSET_TS, 
	OFF_POLICY_SYM, 
	OFF_POLICY_NBR, 
	OFF_POLICY_MOD, 
	OFF_DATE_LOSS, 
	OFF_OCC_NBR, 
	ON_POLICY_SYM, 
	ON_POLICY_NUM, 
	ON_POLICY_MOD, 
	ON_DATE_LOSS, 
	ON_OCC_NBR, 
	PROCESS_STA_IND, 
	ENTRY_OPR_ID, 
	ENTRY_TIMESTAMP, 
	UPDATE_OPR_ID, 
	UPDATE_TIMESTAMP, 
	OFF_LINE_BUSINESS, 
	ON_LINE_BUSINESS, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_CLM_OFFSET_ONSET_STAGE
),