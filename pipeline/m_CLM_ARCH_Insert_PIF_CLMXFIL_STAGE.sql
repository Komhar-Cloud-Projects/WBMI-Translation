WITH
SQ_pif_clmxfil_stage AS (
	SELECT
		pif_clmxfil_stage_id,
		clmxfil_type,
		clmxfil_policy_loc,
		clmxfil_policy_sym,
		clmxfil_policy_number,
		clmxfil_policy_mod,
		clmxfil_master_co,
		clmxfil_year_of_loss,
		clmxfil_month_of_loss,
		clmxfil_day_of_loss,
		clmxfil_occurence_number,
		clmxfil_claimant_number,
		clmxfil_insur_line,
		clmxfil_location,
		clmxfil_sub_location,
		clmxfil_risk_unit_group,
		clmxfil_class_code_grp,
		clmxfil_class_code_mem,
		clmxfil_loss_unit,
		clmxfil_seq_risk_unit,
		clmxfil_type_exposure,
		clmxfil_coverage,
		clmxfil_major_peril_seq,
		clmxfil_member,
		clmxfil_loss_disability,
		clmxfil_res_category,
		clmxfil_rein_layer,
		clmxfil_rein_id,
		clmxfil_rein_co_num,
		clmxfil_rein_broker,
		clmxfil_onset_only,
		clmxfil_d_policy_loc,
		clmxfil_d_policy_sym,
		clmxfil_d_policy_number,
		clmxfil_d_policy_mod,
		clmxfil_d_master_co,
		clmxfil_d_year_of_loss,
		clmxfil_d_month_of_loss,
		clmxfil_d_day_of_loss,
		clmxfil_d_occurence_number,
		clmxfil_d_claimant_number,
		clmxfil_d_insur_line,
		clmxfil_d_location,
		clmxfil_d_sub_location,
		clmxfil_d_risk_unit_group,
		clmxfil_d_class_code_grp,
		clmxfil_d_class_code_mem,
		clmxfil_d_loss_unit,
		clmxfil_d_seq_risk_unit,
		clmxfil_d_type_exposure,
		clmxfil_d_coverage,
		clmxfil_d_major_peril_seq,
		clmxfil_d_member,
		clmxfil_d_loss_disability,
		clmxfil_d_res_category,
		clmxfil_d_rein_layer,
		clmxfil_d_rein_id,
		clmxfil_d_rein_co_num,
		clmxfil_d_rein_broker,
		clmxfil_d_cat_code,
		clmxfil_d_loss_state,
		clmxfil_d_cause_loss,
		clmxfil_d_seg_id_from_stat,
		clmxfil_d_cov_eff_date,
		clmxfil_d_loc_num,
		clmxfil_d_lob,
		clmxfil_d_claim_office,
		clmxfil_d_claim_number,
		clmxfil_d_pass_fail_ind,
		clmxfil_activity_date,
		clmxfil_d_claim_payee,
		logical_flag,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM pif_clmxfil_stage
),
EXP_arch_PIF_4578_stage AS (
	SELECT
	pif_clmxfil_stage_id,
	clmxfil_type,
	clmxfil_policy_loc,
	clmxfil_policy_sym,
	clmxfil_policy_number,
	clmxfil_policy_mod,
	clmxfil_master_co,
	clmxfil_year_of_loss,
	clmxfil_month_of_loss,
	clmxfil_day_of_loss,
	clmxfil_occurence_number,
	clmxfil_claimant_number,
	clmxfil_insur_line,
	clmxfil_location,
	clmxfil_sub_location,
	clmxfil_risk_unit_group,
	clmxfil_class_code_grp,
	clmxfil_class_code_mem,
	clmxfil_loss_unit,
	clmxfil_seq_risk_unit,
	clmxfil_type_exposure,
	clmxfil_coverage,
	clmxfil_major_peril_seq,
	clmxfil_member,
	clmxfil_loss_disability,
	clmxfil_res_category,
	clmxfil_rein_layer,
	clmxfil_rein_id,
	clmxfil_rein_co_num,
	clmxfil_rein_broker,
	clmxfil_onset_only,
	clmxfil_d_policy_loc,
	clmxfil_d_policy_sym,
	clmxfil_d_policy_number,
	clmxfil_d_policy_mod,
	clmxfil_d_master_co,
	clmxfil_d_year_of_loss,
	clmxfil_d_month_of_loss,
	clmxfil_d_day_of_loss,
	clmxfil_d_occurence_number,
	clmxfil_d_claimant_number,
	clmxfil_d_insur_line,
	clmxfil_d_location,
	clmxfil_d_sub_location,
	clmxfil_d_risk_unit_group,
	clmxfil_d_class_code_grp,
	clmxfil_d_class_code_mem,
	clmxfil_d_loss_unit,
	clmxfil_d_seq_risk_unit,
	clmxfil_d_type_exposure,
	clmxfil_d_coverage,
	clmxfil_d_major_peril_seq,
	clmxfil_d_member,
	clmxfil_d_loss_disability,
	clmxfil_d_res_category,
	clmxfil_d_rein_layer,
	clmxfil_d_rein_id,
	clmxfil_d_rein_co_num,
	clmxfil_d_rein_broker,
	clmxfil_d_cat_code,
	clmxfil_d_loss_state,
	clmxfil_d_cause_loss,
	clmxfil_d_seg_id_from_stat,
	clmxfil_d_cov_eff_date,
	clmxfil_d_loc_num,
	clmxfil_d_lob,
	clmxfil_d_claim_office,
	clmxfil_d_claim_number,
	clmxfil_d_pass_fail_ind,
	clmxfil_activity_date,
	clmxfil_d_claim_payee,
	logical_flag,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_pif_clmxfil_stage
),
arch_pif_clmxfil_stage AS (
	INSERT INTO arch_pif_clmxfil_stage
	(pif_clmxfil_stage_id, clmxfil_type, clmxfil_policy_loc, clmxfil_policy_sym, clmxfil_policy_number, clmxfil_policy_mod, clmxfil_master_co, clmxfil_year_of_loss, clmxfil_month_of_loss, clmxfil_day_of_loss, clmxfil_occurence_number, clmxfil_claimant_number, clmxfil_insur_line, clmxfil_location, clmxfil_sub_location, clmxfil_risk_unit_group, clmxfil_class_code_grp, clmxfil_class_code_mem, clmxfil_loss_unit, clmxfil_seq_risk_unit, clmxfil_type_exposure, clmxfil_coverage, clmxfil_major_peril_seq, clmxfil_member, clmxfil_loss_disability, clmxfil_res_category, clmxfil_rein_layer, clmxfil_rein_id, clmxfil_rein_co_num, clmxfil_rein_broker, clmxfil_onset_only, clmxfil_d_policy_loc, clmxfil_d_policy_sym, clmxfil_d_policy_number, clmxfil_d_policy_mod, clmxfil_d_master_co, clmxfil_d_year_of_loss, clmxfil_d_month_of_loss, clmxfil_d_day_of_loss, clmxfil_d_occurence_number, clmxfil_d_claimant_number, clmxfil_d_insur_line, clmxfil_d_location, clmxfil_d_sub_location, clmxfil_d_risk_unit_group, clmxfil_d_class_code_grp, clmxfil_d_class_code_mem, clmxfil_d_loss_unit, clmxfil_d_seq_risk_unit, clmxfil_d_type_exposure, clmxfil_d_coverage, clmxfil_d_major_peril_seq, clmxfil_d_member, clmxfil_d_loss_disability, clmxfil_d_res_category, clmxfil_d_rein_layer, clmxfil_d_rein_id, clmxfil_d_rein_co_num, clmxfil_d_rein_broker, clmxfil_d_cat_code, clmxfil_d_loss_state, clmxfil_d_cause_loss, clmxfil_d_seg_id_from_stat, clmxfil_d_cov_eff_date, clmxfil_d_loc_num, clmxfil_d_lob, clmxfil_d_claim_office, clmxfil_d_claim_number, clmxfil_d_pass_fail_ind, clmxfil_activity_date, clmxfil_d_claim_payee, logical_flag, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	PIF_CLMXFIL_STAGE_ID, 
	CLMXFIL_TYPE, 
	CLMXFIL_POLICY_LOC, 
	CLMXFIL_POLICY_SYM, 
	CLMXFIL_POLICY_NUMBER, 
	CLMXFIL_POLICY_MOD, 
	CLMXFIL_MASTER_CO, 
	CLMXFIL_YEAR_OF_LOSS, 
	CLMXFIL_MONTH_OF_LOSS, 
	CLMXFIL_DAY_OF_LOSS, 
	CLMXFIL_OCCURENCE_NUMBER, 
	CLMXFIL_CLAIMANT_NUMBER, 
	CLMXFIL_INSUR_LINE, 
	CLMXFIL_LOCATION, 
	CLMXFIL_SUB_LOCATION, 
	CLMXFIL_RISK_UNIT_GROUP, 
	CLMXFIL_CLASS_CODE_GRP, 
	CLMXFIL_CLASS_CODE_MEM, 
	CLMXFIL_LOSS_UNIT, 
	CLMXFIL_SEQ_RISK_UNIT, 
	CLMXFIL_TYPE_EXPOSURE, 
	CLMXFIL_COVERAGE, 
	CLMXFIL_MAJOR_PERIL_SEQ, 
	CLMXFIL_MEMBER, 
	CLMXFIL_LOSS_DISABILITY, 
	CLMXFIL_RES_CATEGORY, 
	CLMXFIL_REIN_LAYER, 
	CLMXFIL_REIN_ID, 
	CLMXFIL_REIN_CO_NUM, 
	CLMXFIL_REIN_BROKER, 
	CLMXFIL_ONSET_ONLY, 
	CLMXFIL_D_POLICY_LOC, 
	CLMXFIL_D_POLICY_SYM, 
	CLMXFIL_D_POLICY_NUMBER, 
	CLMXFIL_D_POLICY_MOD, 
	CLMXFIL_D_MASTER_CO, 
	CLMXFIL_D_YEAR_OF_LOSS, 
	CLMXFIL_D_MONTH_OF_LOSS, 
	CLMXFIL_D_DAY_OF_LOSS, 
	CLMXFIL_D_OCCURENCE_NUMBER, 
	CLMXFIL_D_CLAIMANT_NUMBER, 
	CLMXFIL_D_INSUR_LINE, 
	CLMXFIL_D_LOCATION, 
	CLMXFIL_D_SUB_LOCATION, 
	CLMXFIL_D_RISK_UNIT_GROUP, 
	CLMXFIL_D_CLASS_CODE_GRP, 
	CLMXFIL_D_CLASS_CODE_MEM, 
	CLMXFIL_D_LOSS_UNIT, 
	CLMXFIL_D_SEQ_RISK_UNIT, 
	CLMXFIL_D_TYPE_EXPOSURE, 
	CLMXFIL_D_COVERAGE, 
	CLMXFIL_D_MAJOR_PERIL_SEQ, 
	CLMXFIL_D_MEMBER, 
	CLMXFIL_D_LOSS_DISABILITY, 
	CLMXFIL_D_RES_CATEGORY, 
	CLMXFIL_D_REIN_LAYER, 
	CLMXFIL_D_REIN_ID, 
	CLMXFIL_D_REIN_CO_NUM, 
	CLMXFIL_D_REIN_BROKER, 
	CLMXFIL_D_CAT_CODE, 
	CLMXFIL_D_LOSS_STATE, 
	CLMXFIL_D_CAUSE_LOSS, 
	CLMXFIL_D_SEG_ID_FROM_STAT, 
	CLMXFIL_D_COV_EFF_DATE, 
	CLMXFIL_D_LOC_NUM, 
	CLMXFIL_D_LOB, 
	CLMXFIL_D_CLAIM_OFFICE, 
	CLMXFIL_D_CLAIM_NUMBER, 
	CLMXFIL_D_PASS_FAIL_IND, 
	CLMXFIL_ACTIVITY_DATE, 
	CLMXFIL_D_CLAIM_PAYEE, 
	LOGICAL_FLAG, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_PIF_4578_stage
),