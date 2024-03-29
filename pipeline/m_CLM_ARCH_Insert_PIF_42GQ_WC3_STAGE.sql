WITH
SQ_PIF_42GQ_WC3_stage AS (
	SELECT
		pif_42gq_wc3_stage_id AS PIF_42GQ_WC3_stage_id,
		pif_symbol AS PIF_SYMBOL,
		pif_policy_number AS PIF_POLICY_NUMBER,
		pif_module AS PIF_MODULE,
		ipfcgq_rec_length AS IPFCGQ_REC_LENGTH,
		ipfcgq_action_code AS IPFCGQ_ACTION_CODE,
		ipfcgq_file_id AS IPFCGQ_FILE_ID,
		ipfcgq_segment_id AS IPFCGQ_SEGMENT_ID,
		ipfcgq_segment_level_code AS IPFCGQ_SEGMENT_LEVEL_CODE,
		ipfcgq_segment_part_code AS IPFCGQ_SEGMENT_PART_CODE,
		ipfcgq_sub_part_code AS IPFCGQ_SUB_PART_CODE,
		ipfcgq_year_of_loss AS IPFCGQ_YEAR_OF_LOSS,
		ipfcgq_month_of_loss AS IPFCGQ_MONTH_OF_LOSS,
		ipfcgq_day_of_loss AS IPFCGQ_DAY_OF_LOSS,
		ipfcgq_loss_occurence AS IPFCGQ_LOSS_OCCURENCE,
		ipfcgq_loss_claimant AS IPFCGQ_LOSS_CLAIMANT,
		ipfcgq_claimant_use_code AS IPFCGQ_CLAIMANT_USE_CODE,
		ipfcgq_claimant_use_seq AS IPFCGQ_CLAIMANT_USE_SEQ,
		ipfcgq_year_process AS IPFCGQ_YEAR_PROCESS,
		ipfcgq_month_process AS IPFCGQ_MONTH_PROCESS,
		ipfcgq_day_process AS IPFCGQ_DAY_PROCESS,
		ipfcgq_year_change_entry AS IPFCGQ_YEAR_CHANGE_ENTRY,
		ipfcgq_month_change_entry AS IPFCGQ_MONTH_CHANGE_ENTRY,
		ipfcgq_day_change_entry AS IPFCGQ_DAY_CHANGE_ENTRY,
		ipfcgq_sequence_change_entry AS IPFCGQ_SEQUENCE_CHANGE_ENTRY,
		ipfcgq_segment_status AS IPFCGQ_SEGMENT_STATUS,
		ipfcgq_entry_operator AS IPFCGQ_ENTRY_OPERATOR,
		ipfcgq_benefit_name_1 AS IPFCGQ_BENEFIT_NAME_1,
		ipfcgq_benefit_type_1 AS IPFCGQ_BENEFIT_TYPE_1,
		ipfcgq_depend_status_1 AS IPFCGQ_DEPEND_STATUS_1,
		ipfcgq_benefit_birth_date_1 AS IPFCGQ_BENEFIT_BIRTH_DATE_1,
		ipfcgq_benefit_term_date_1 AS IPFCGQ_BENEFIT_TERM_DATE_1,
		ipfcgq_benefit_term_reason_1 AS IPFCGQ_BENEFIT_TERM_REASON_1,
		ipfcgq_benefit_name_2 AS IPFCGQ_BENEFIT_NAME_2,
		ipfcgq_benefit_type_2 AS IPFCGQ_BENEFIT_TYPE_2,
		ipfcgq_depend_status_2 AS IPFCGQ_DEPEND_STATUS_2,
		ipfcgq_benefit_birth_date_2 AS IPFCGQ_BENEFIT_BIRTH_DATE_2,
		ipfcgq_benefit_term_date_2 AS IPFCGQ_BENEFIT_TERM_DATE_2,
		ipfcgq_benefit_term_reason_2 AS IPFCGQ_BENEFIT_TERM_REASON_2,
		ipfcgq_benefit_name_3 AS IPFCGQ_BENEFIT_NAME_3,
		ipfcgq_benefit_type_3 AS IPFCGQ_BENEFIT_TYPE_3,
		ipfcgq_depend_status_3 AS IPFCGQ_DEPEND_STATUS_3,
		ipfcgq_benefit_birth_date_3 AS IPFCGQ_BENEFIT_BIRTH_DATE_3,
		ipfcgq_benefit_term_date_3 AS IPFCGQ_BENEFIT_TERM_DATE_3,
		ipfcgq_benefit_term_reason_3 AS IPFCGQ_BENEFIT_TERM_REASON_3,
		ipfcgq_benefit_name_4 AS IPFCGQ_BENEFIT_NAME_4,
		ipfcgq_benefit_type_4 AS IPFCGQ_BENEFIT_TYPE_4,
		ipfcgq_depend_status_4 AS IPFCGQ_DEPEND_STATUS_4,
		ipfcgq_benefit_birth_date_4 AS IPFCGQ_BENEFIT_BIRTH_DATE_4,
		ipfcgq_benefit_term_date_4 AS IPFCGQ_BENEFIT_TERM_DATE_4,
		ipfcgq_benefit_term_reason_4 AS IPFCGQ_BENEFIT_TERM_REASON_4,
		ipfcgq_fraud_claim_ind AS IPFCGQ_FRAUD_CLAIM_IND,
		ipfcgq_lump_sum_ind AS IPFCGQ_LUMP_SUM_IND,
		ipfcgq_mangd_care_org_ind AS IPFCGQ_MANGD_CARE_ORG_IND,
		ipfcgq_deduction_ind AS IPFCGQ_DEDUCTION_IND,
		ipfcgq_clmt_fees AS IPFCGQ_CLMT_FEES,
		ipfcgq_empl_fees AS IPFCGQ_EMPL_FEES,
		ipfcgq_voc_reh_ind AS IPFCGQ_VOC_REH_IND,
		ipfcgq_type_covg AS IPFCGQ_TYPE_COVG,
		ipfcgq_type_setl AS IPFCGQ_TYPE_SETL,
		ipfcgq_number_of_part78 AS IPFCGQ_NUMBER_OF_PART78,
		ipfcgq_offset_onset_ind AS IPFCGQ_OFFSET_ONSET_IND,
		ipfcgq_date_hire AS IPFCGQ_DATE_HIRE,
		ipfcgq_pms_future_use_gq AS IPFCGQ_PMS_FUTURE_USE_GQ,
		ipfcgq_direct_reporting AS IPFCGQ_DIRECT_REPORTING,
		ipfcgq_cust_spl_use_gq AS IPFCGQ_CUST_SPL_USE_GQ,
		ipfcgq_yr2000_cust_use AS IPFCGQ_YR2000_CUST_USE,
		inf_action,
		inf_timestamp,
		logical_flag,
		extract_date AS EXTRACT_DATE,
		as_of_date AS AS_OF_DATE,
		record_count AS RECORD_COUNT,
		source_system_id AS SOURCE_SYSTEM_ID
	FROM PIF_42GQ_WC3_stage
),
EXP_arch_PIF_42GQ_WC3_stage AS (
	SELECT
	PIF_42GQ_WC3_stage_id,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCGQ_REC_LENGTH,
	IPFCGQ_ACTION_CODE,
	IPFCGQ_FILE_ID,
	IPFCGQ_SEGMENT_ID,
	IPFCGQ_SEGMENT_LEVEL_CODE,
	IPFCGQ_SEGMENT_PART_CODE,
	IPFCGQ_SUB_PART_CODE,
	IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT,
	IPFCGQ_CLAIMANT_USE_CODE,
	IPFCGQ_CLAIMANT_USE_SEQ,
	IPFCGQ_YEAR_PROCESS,
	IPFCGQ_MONTH_PROCESS,
	IPFCGQ_DAY_PROCESS,
	IPFCGQ_YEAR_CHANGE_ENTRY,
	IPFCGQ_MONTH_CHANGE_ENTRY,
	IPFCGQ_DAY_CHANGE_ENTRY,
	IPFCGQ_SEQUENCE_CHANGE_ENTRY,
	IPFCGQ_SEGMENT_STATUS,
	IPFCGQ_ENTRY_OPERATOR,
	IPFCGQ_BENEFIT_NAME_1,
	IPFCGQ_BENEFIT_TYPE_1,
	IPFCGQ_DEPEND_STATUS_1,
	IPFCGQ_BENEFIT_BIRTH_DATE_1,
	IPFCGQ_BENEFIT_TERM_DATE_1,
	IPFCGQ_BENEFIT_TERM_REASON_1,
	IPFCGQ_BENEFIT_NAME_2,
	IPFCGQ_BENEFIT_TYPE_2,
	IPFCGQ_DEPEND_STATUS_2,
	IPFCGQ_BENEFIT_BIRTH_DATE_2,
	IPFCGQ_BENEFIT_TERM_DATE_2,
	IPFCGQ_BENEFIT_TERM_REASON_2,
	IPFCGQ_BENEFIT_NAME_3,
	IPFCGQ_BENEFIT_TYPE_3,
	IPFCGQ_DEPEND_STATUS_3,
	IPFCGQ_BENEFIT_BIRTH_DATE_3,
	IPFCGQ_BENEFIT_TERM_DATE_3,
	IPFCGQ_BENEFIT_TERM_REASON_3,
	IPFCGQ_BENEFIT_NAME_4,
	IPFCGQ_BENEFIT_TYPE_4,
	IPFCGQ_DEPEND_STATUS_4,
	IPFCGQ_BENEFIT_BIRTH_DATE_4,
	IPFCGQ_BENEFIT_TERM_DATE_4,
	IPFCGQ_BENEFIT_TERM_REASON_4,
	IPFCGQ_FRAUD_CLAIM_IND,
	IPFCGQ_LUMP_SUM_IND,
	IPFCGQ_MANGD_CARE_ORG_IND,
	IPFCGQ_DEDUCTION_IND,
	IPFCGQ_CLMT_FEES,
	IPFCGQ_EMPL_FEES,
	IPFCGQ_VOC_REH_IND,
	IPFCGQ_TYPE_COVG,
	IPFCGQ_TYPE_SETL,
	IPFCGQ_NUMBER_OF_PART78,
	IPFCGQ_OFFSET_ONSET_IND,
	IPFCGQ_DATE_HIRE,
	IPFCGQ_PMS_FUTURE_USE_GQ,
	IPFCGQ_DIRECT_REPORTING,
	IPFCGQ_CUST_SPL_USE_GQ,
	IPFCGQ_YR2000_CUST_USE,
	inf_action,
	inf_timestamp,
	logical_flag,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_PIF_42GQ_WC3_stage
),
arch_PIF_42GQ_WC3_stage AS (
	INSERT INTO arch_PIF_42GQ_WC3_stage
	(pif_42gq_wc3_stage_id, pif_symbol, pif_policy_number, pif_module, ipfcgq_rec_length, ipfcgq_action_code, ipfcgq_file_id, ipfcgq_segment_id, ipfcgq_segment_level_code, ipfcgq_segment_part_code, ipfcgq_sub_part_code, ipfcgq_year_of_loss, ipfcgq_month_of_loss, ipfcgq_day_of_loss, ipfcgq_loss_occurence, ipfcgq_loss_claimant, ipfcgq_claimant_use_code, ipfcgq_claimant_use_seq, ipfcgq_year_process, ipfcgq_month_process, ipfcgq_day_process, ipfcgq_year_change_entry, ipfcgq_month_change_entry, ipfcgq_day_change_entry, ipfcgq_sequence_change_entry, ipfcgq_segment_status, ipfcgq_entry_operator, ipfcgq_benefit_name_1, ipfcgq_benefit_type_1, ipfcgq_depend_status_1, ipfcgq_benefit_birth_date_1, ipfcgq_benefit_term_date_1, ipfcgq_benefit_term_reason_1, ipfcgq_benefit_name_2, ipfcgq_benefit_type_2, ipfcgq_depend_status_2, ipfcgq_benefit_birth_date_2, ipfcgq_benefit_term_date_2, ipfcgq_benefit_term_reason_2, ipfcgq_benefit_name_3, ipfcgq_benefit_type_3, ipfcgq_depend_status_3, ipfcgq_benefit_birth_date_3, ipfcgq_benefit_term_date_3, ipfcgq_benefit_term_reason_3, ipfcgq_benefit_name_4, ipfcgq_benefit_type_4, ipfcgq_depend_status_4, ipfcgq_benefit_birth_date_4, ipfcgq_benefit_term_date_4, ipfcgq_benefit_term_reason_4, ipfcgq_fraud_claim_ind, ipfcgq_lump_sum_ind, ipfcgq_mangd_care_org_ind, ipfcgq_deduction_ind, ipfcgq_clmt_fees, ipfcgq_empl_fees, ipfcgq_voc_reh_ind, ipfcgq_type_covg, ipfcgq_type_setl, ipfcgq_number_of_part78, ipfcgq_offset_onset_ind, ipfcgq_date_hire, ipfcgq_pms_future_use_gq, ipfcgq_direct_reporting, ipfcgq_cust_spl_use_gq, ipfcgq_yr2000_cust_use, inf_action, inf_timestamp, logical_flag, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	PIF_42GQ_WC3_stage_id AS PIF_42GQ_WC3_STAGE_ID, 
	PIF_SYMBOL AS PIF_SYMBOL, 
	PIF_POLICY_NUMBER AS PIF_POLICY_NUMBER, 
	PIF_MODULE AS PIF_MODULE, 
	IPFCGQ_REC_LENGTH AS IPFCGQ_REC_LENGTH, 
	IPFCGQ_ACTION_CODE AS IPFCGQ_ACTION_CODE, 
	IPFCGQ_FILE_ID AS IPFCGQ_FILE_ID, 
	IPFCGQ_SEGMENT_ID AS IPFCGQ_SEGMENT_ID, 
	IPFCGQ_SEGMENT_LEVEL_CODE AS IPFCGQ_SEGMENT_LEVEL_CODE, 
	IPFCGQ_SEGMENT_PART_CODE AS IPFCGQ_SEGMENT_PART_CODE, 
	IPFCGQ_SUB_PART_CODE AS IPFCGQ_SUB_PART_CODE, 
	IPFCGQ_YEAR_OF_LOSS AS IPFCGQ_YEAR_OF_LOSS, 
	IPFCGQ_MONTH_OF_LOSS AS IPFCGQ_MONTH_OF_LOSS, 
	IPFCGQ_DAY_OF_LOSS AS IPFCGQ_DAY_OF_LOSS, 
	IPFCGQ_LOSS_OCCURENCE AS IPFCGQ_LOSS_OCCURENCE, 
	IPFCGQ_LOSS_CLAIMANT AS IPFCGQ_LOSS_CLAIMANT, 
	IPFCGQ_CLAIMANT_USE_CODE AS IPFCGQ_CLAIMANT_USE_CODE, 
	IPFCGQ_CLAIMANT_USE_SEQ AS IPFCGQ_CLAIMANT_USE_SEQ, 
	IPFCGQ_YEAR_PROCESS AS IPFCGQ_YEAR_PROCESS, 
	IPFCGQ_MONTH_PROCESS AS IPFCGQ_MONTH_PROCESS, 
	IPFCGQ_DAY_PROCESS AS IPFCGQ_DAY_PROCESS, 
	IPFCGQ_YEAR_CHANGE_ENTRY AS IPFCGQ_YEAR_CHANGE_ENTRY, 
	IPFCGQ_MONTH_CHANGE_ENTRY AS IPFCGQ_MONTH_CHANGE_ENTRY, 
	IPFCGQ_DAY_CHANGE_ENTRY AS IPFCGQ_DAY_CHANGE_ENTRY, 
	IPFCGQ_SEQUENCE_CHANGE_ENTRY AS IPFCGQ_SEQUENCE_CHANGE_ENTRY, 
	IPFCGQ_SEGMENT_STATUS AS IPFCGQ_SEGMENT_STATUS, 
	IPFCGQ_ENTRY_OPERATOR AS IPFCGQ_ENTRY_OPERATOR, 
	IPFCGQ_BENEFIT_NAME_1 AS IPFCGQ_BENEFIT_NAME_1, 
	IPFCGQ_BENEFIT_TYPE_1 AS IPFCGQ_BENEFIT_TYPE_1, 
	IPFCGQ_DEPEND_STATUS_1 AS IPFCGQ_DEPEND_STATUS_1, 
	IPFCGQ_BENEFIT_BIRTH_DATE_1 AS IPFCGQ_BENEFIT_BIRTH_DATE_1, 
	IPFCGQ_BENEFIT_TERM_DATE_1 AS IPFCGQ_BENEFIT_TERM_DATE_1, 
	IPFCGQ_BENEFIT_TERM_REASON_1 AS IPFCGQ_BENEFIT_TERM_REASON_1, 
	IPFCGQ_BENEFIT_NAME_2 AS IPFCGQ_BENEFIT_NAME_2, 
	IPFCGQ_BENEFIT_TYPE_2 AS IPFCGQ_BENEFIT_TYPE_2, 
	IPFCGQ_DEPEND_STATUS_2 AS IPFCGQ_DEPEND_STATUS_2, 
	IPFCGQ_BENEFIT_BIRTH_DATE_2 AS IPFCGQ_BENEFIT_BIRTH_DATE_2, 
	IPFCGQ_BENEFIT_TERM_DATE_2 AS IPFCGQ_BENEFIT_TERM_DATE_2, 
	IPFCGQ_BENEFIT_TERM_REASON_2 AS IPFCGQ_BENEFIT_TERM_REASON_2, 
	IPFCGQ_BENEFIT_NAME_3 AS IPFCGQ_BENEFIT_NAME_3, 
	IPFCGQ_BENEFIT_TYPE_3 AS IPFCGQ_BENEFIT_TYPE_3, 
	IPFCGQ_DEPEND_STATUS_3 AS IPFCGQ_DEPEND_STATUS_3, 
	IPFCGQ_BENEFIT_BIRTH_DATE_3 AS IPFCGQ_BENEFIT_BIRTH_DATE_3, 
	IPFCGQ_BENEFIT_TERM_DATE_3 AS IPFCGQ_BENEFIT_TERM_DATE_3, 
	IPFCGQ_BENEFIT_TERM_REASON_3 AS IPFCGQ_BENEFIT_TERM_REASON_3, 
	IPFCGQ_BENEFIT_NAME_4 AS IPFCGQ_BENEFIT_NAME_4, 
	IPFCGQ_BENEFIT_TYPE_4 AS IPFCGQ_BENEFIT_TYPE_4, 
	IPFCGQ_DEPEND_STATUS_4 AS IPFCGQ_DEPEND_STATUS_4, 
	IPFCGQ_BENEFIT_BIRTH_DATE_4 AS IPFCGQ_BENEFIT_BIRTH_DATE_4, 
	IPFCGQ_BENEFIT_TERM_DATE_4 AS IPFCGQ_BENEFIT_TERM_DATE_4, 
	IPFCGQ_BENEFIT_TERM_REASON_4 AS IPFCGQ_BENEFIT_TERM_REASON_4, 
	IPFCGQ_FRAUD_CLAIM_IND AS IPFCGQ_FRAUD_CLAIM_IND, 
	IPFCGQ_LUMP_SUM_IND AS IPFCGQ_LUMP_SUM_IND, 
	IPFCGQ_MANGD_CARE_ORG_IND AS IPFCGQ_MANGD_CARE_ORG_IND, 
	IPFCGQ_DEDUCTION_IND AS IPFCGQ_DEDUCTION_IND, 
	IPFCGQ_CLMT_FEES AS IPFCGQ_CLMT_FEES, 
	IPFCGQ_EMPL_FEES AS IPFCGQ_EMPL_FEES, 
	IPFCGQ_VOC_REH_IND AS IPFCGQ_VOC_REH_IND, 
	IPFCGQ_TYPE_COVG AS IPFCGQ_TYPE_COVG, 
	IPFCGQ_TYPE_SETL AS IPFCGQ_TYPE_SETL, 
	IPFCGQ_NUMBER_OF_PART78 AS IPFCGQ_NUMBER_OF_PART78, 
	IPFCGQ_OFFSET_ONSET_IND AS IPFCGQ_OFFSET_ONSET_IND, 
	IPFCGQ_DATE_HIRE AS IPFCGQ_DATE_HIRE, 
	IPFCGQ_PMS_FUTURE_USE_GQ AS IPFCGQ_PMS_FUTURE_USE_GQ, 
	IPFCGQ_DIRECT_REPORTING AS IPFCGQ_DIRECT_REPORTING, 
	IPFCGQ_CUST_SPL_USE_GQ AS IPFCGQ_CUST_SPL_USE_GQ, 
	IPFCGQ_YR2000_CUST_USE AS IPFCGQ_YR2000_CUST_USE, 
	INF_ACTION, 
	INF_TIMESTAMP, 
	LOGICAL_FLAG, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_PIF_42GQ_WC3_stage
),