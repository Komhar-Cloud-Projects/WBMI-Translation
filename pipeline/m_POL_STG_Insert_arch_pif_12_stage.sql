WITH
SQ_pif_12_stage AS (
	SELECT
		pif_12_stage_id,
		pif_symbol AS PIF_SYMBOL,
		pif_policy_number AS PIF_POLICY_NUMBER,
		pif_module AS PIF_MODULE,
		description_rec_length AS DESCRIPTION_REC_LENGTH,
		description_action_code AS DESCRIPTION_ACTION_CODE,
		description_file_id AS DESCRIPTION_FILE_ID,
		description_id AS DESCRIPTION_ID,
		use_code AS USE_CODE,
		use_location AS USE_LOCATION,
		description_sequence AS DESCRIPTION_SEQUENCE,
		description_sort_name AS DESCRIPTION_SORT_NAME,
		description_time AS DESCRIPTION_TIME,
		description_line_1 AS DESCRIPTION_LINE_1,
		description_line_2 AS DESCRIPTION_LINE_2,
		description_line_3 AS DESCRIPTION_LINE_3,
		description_line_4 AS DESCRIPTION_LINE_4,
		description_name_code AS DESCRIPTION_NAME_CODE,
		desc_uk_postal_code AS DESC_UK_POSTAL_CODE,
		desc_misc_adj_zip_code AS DESC_MISC_ADJ_ZIP_CODE,
		desc_misc_adj_tax_id AS DESC_MISC_ADJ_TAX_ID,
		desc_ars_pay_plan AS DESC_ARS_PAY_PLAN,
		desc_ars_dun_plan AS DESC_ARS_DUN_PLAN,
		desc_ars_sundry_plan AS DESC_ARS_SUNDRY_PLAN,
		desc_ars_transfer_ind AS DESC_ARS_TRANSFER_IND,
		desc_rn_ars_pay_plan AS DESC_RN_ARS_PAY_PLAN,
		desc_rn_ars_dun_plan AS DESC_RN_ARS_DUN_PLAN,
		desc_rn_ars_sundry_plan AS DESC_RN_ARS_SUNDRY_PLAN,
		desc_rn_ars_account_number AS DESC_RN_ARS_ACCOUNT_NUMBER,
		desc_rn_ars_sub_acct_num AS DESC_RN_ARS_SUB_ACCT_NUM,
		desc_rn_ars_billing_entity AS DESC_RN_ARS_BILLING_ENTITY,
		desc_rn_ars_billing_type AS DESC_RN_ARS_BILLING_TYPE,
		desc_rn_ars_additional_id AS DESC_RN_ARS_ADDITIONAL_ID,
		desc_rn_ars_billing_class AS DESC_RN_ARS_BILLING_CLASS,
		desc_ars_prior_can_amt AS DESC_ARS_PRIOR_CAN_AMT,
		desc_ars_prior_exp_can_amt AS DESC_ARS_PRIOR_EXP_CAN_AMT,
		desc_rn_ars_future_use AS DESC_RN_ARS_FUTURE_USE,
		description_loan_number AS DESCRIPTION_LOAN_NUMBER,
		desc_lp_id_number AS DESC_LP_ID_NUMBER,
		desc_zip_ind AS DESC_ZIP_IND,
		desc_endorsement_date AS DESC_ENDORSEMENT_DATE,
		desc_date_stamp AS DESC_DATE_STAMP,
		desc_time_stamp AS DESC_TIME_STAMP,
		desc_ais_indicator AS DESC_AIS_INDICATOR,
		desc_lgl_addr_change_id,
		desc_mortgagee_change_sw,
		desc_cust_future_use AS DESC_CUST_FUTURE_USE,
		desc_watercraft_seq,
		desc_yr2000_cust_use AS DESC_YR2000_CUST_USE,
		desc_dup_key_seq_num AS DESC_DUP_KEY_SEQ_NUM,
		logical_flag,
		extract_date AS EXTRACT_DATE,
		as_of_date AS AS_OF_DATE,
		record_count AS RECORD_COUNT,
		source_system_id AS SOURCE_SYSTEM_ID,
		inf_action,
		inf_timestamp
	FROM pif_12_stage
),
EXP_arch_pif_12_stage AS (
	SELECT
	pif_12_stage_id,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	DESCRIPTION_REC_LENGTH,
	DESCRIPTION_ACTION_CODE,
	DESCRIPTION_FILE_ID,
	DESCRIPTION_ID,
	USE_CODE,
	USE_LOCATION,
	DESCRIPTION_SEQUENCE,
	DESCRIPTION_SORT_NAME,
	DESCRIPTION_TIME,
	DESCRIPTION_LINE_1,
	DESCRIPTION_LINE_2,
	DESCRIPTION_LINE_3,
	DESCRIPTION_LINE_4,
	DESCRIPTION_NAME_CODE,
	DESC_UK_POSTAL_CODE,
	DESC_MISC_ADJ_ZIP_CODE,
	DESC_MISC_ADJ_TAX_ID,
	DESC_ARS_PAY_PLAN,
	DESC_ARS_DUN_PLAN,
	DESC_ARS_SUNDRY_PLAN,
	DESC_ARS_TRANSFER_IND,
	DESC_RN_ARS_PAY_PLAN,
	DESC_RN_ARS_DUN_PLAN,
	DESC_RN_ARS_SUNDRY_PLAN,
	DESC_RN_ARS_ACCOUNT_NUMBER,
	DESC_RN_ARS_SUB_ACCT_NUM,
	DESC_RN_ARS_BILLING_ENTITY,
	DESC_RN_ARS_BILLING_TYPE,
	DESC_RN_ARS_ADDITIONAL_ID,
	DESC_RN_ARS_BILLING_CLASS,
	DESC_ARS_PRIOR_CAN_AMT,
	DESC_ARS_PRIOR_EXP_CAN_AMT,
	DESC_RN_ARS_FUTURE_USE,
	DESCRIPTION_LOAN_NUMBER,
	DESC_LP_ID_NUMBER,
	DESC_ZIP_IND,
	DESC_ENDORSEMENT_DATE,
	DESC_DATE_STAMP,
	DESC_TIME_STAMP,
	DESC_AIS_INDICATOR,
	desc_lgl_addr_change_id,
	desc_mortgagee_change_sw,
	DESC_CUST_FUTURE_USE,
	desc_watercraft_seq,
	DESC_YR2000_CUST_USE,
	DESC_DUP_KEY_SEQ_NUM,
	logical_flag,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	inf_action,
	inf_timestamp
	FROM SQ_pif_12_stage
),
arch_pif_12_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_pif_12_stage
	(pif_12_stage_id, pif_symbol, pif_policy_number, pif_module, description_rec_length, description_action_code, description_file_id, description_id, use_code, use_location, description_sequence, description_sort_name, description_time, description_line_1, description_line_2, description_line_3, description_line_4, description_name_code, desc_uk_postal_code, desc_misc_adj_zip_code, desc_misc_adj_tax_id, desc_ars_pay_plan, desc_ars_dun_plan, desc_ars_sundry_plan, desc_ars_transfer_ind, desc_rn_ars_pay_plan, desc_rn_ars_dun_plan, desc_rn_ars_sundry_plan, desc_rn_ars_account_number, desc_rn_ars_sub_acct_num, desc_rn_ars_billing_entity, desc_rn_ars_billing_type, desc_rn_ars_additional_id, desc_rn_ars_billing_class, desc_ars_prior_can_amt, desc_ars_prior_exp_can_amt, desc_rn_ars_future_use, description_loan_number, desc_lp_id_number, desc_zip_ind, desc_endorsement_date, desc_date_stamp, desc_time_stamp, desc_ais_indicator, desc_lgl_addr_change_id, desc_mortgagee_change_sw, desc_cust_future_use, desc_watercraft_seq, desc_yr2000_cust_use, desc_dup_key_seq_num, logical_flag, extract_date, as_of_date, record_count, source_system_id, audit_id, inf_action, inf_timestamp)
	SELECT 
	PIF_12_STAGE_ID, 
	PIF_SYMBOL AS PIF_SYMBOL, 
	PIF_POLICY_NUMBER AS PIF_POLICY_NUMBER, 
	PIF_MODULE AS PIF_MODULE, 
	DESCRIPTION_REC_LENGTH AS DESCRIPTION_REC_LENGTH, 
	DESCRIPTION_ACTION_CODE AS DESCRIPTION_ACTION_CODE, 
	DESCRIPTION_FILE_ID AS DESCRIPTION_FILE_ID, 
	DESCRIPTION_ID AS DESCRIPTION_ID, 
	USE_CODE AS USE_CODE, 
	USE_LOCATION AS USE_LOCATION, 
	DESCRIPTION_SEQUENCE AS DESCRIPTION_SEQUENCE, 
	DESCRIPTION_SORT_NAME AS DESCRIPTION_SORT_NAME, 
	DESCRIPTION_TIME AS DESCRIPTION_TIME, 
	DESCRIPTION_LINE_1 AS DESCRIPTION_LINE_1, 
	DESCRIPTION_LINE_2 AS DESCRIPTION_LINE_2, 
	DESCRIPTION_LINE_3 AS DESCRIPTION_LINE_3, 
	DESCRIPTION_LINE_4 AS DESCRIPTION_LINE_4, 
	DESCRIPTION_NAME_CODE AS DESCRIPTION_NAME_CODE, 
	DESC_UK_POSTAL_CODE AS DESC_UK_POSTAL_CODE, 
	DESC_MISC_ADJ_ZIP_CODE AS DESC_MISC_ADJ_ZIP_CODE, 
	DESC_MISC_ADJ_TAX_ID AS DESC_MISC_ADJ_TAX_ID, 
	DESC_ARS_PAY_PLAN AS DESC_ARS_PAY_PLAN, 
	DESC_ARS_DUN_PLAN AS DESC_ARS_DUN_PLAN, 
	DESC_ARS_SUNDRY_PLAN AS DESC_ARS_SUNDRY_PLAN, 
	DESC_ARS_TRANSFER_IND AS DESC_ARS_TRANSFER_IND, 
	DESC_RN_ARS_PAY_PLAN AS DESC_RN_ARS_PAY_PLAN, 
	DESC_RN_ARS_DUN_PLAN AS DESC_RN_ARS_DUN_PLAN, 
	DESC_RN_ARS_SUNDRY_PLAN AS DESC_RN_ARS_SUNDRY_PLAN, 
	DESC_RN_ARS_ACCOUNT_NUMBER AS DESC_RN_ARS_ACCOUNT_NUMBER, 
	DESC_RN_ARS_SUB_ACCT_NUM AS DESC_RN_ARS_SUB_ACCT_NUM, 
	DESC_RN_ARS_BILLING_ENTITY AS DESC_RN_ARS_BILLING_ENTITY, 
	DESC_RN_ARS_BILLING_TYPE AS DESC_RN_ARS_BILLING_TYPE, 
	DESC_RN_ARS_ADDITIONAL_ID AS DESC_RN_ARS_ADDITIONAL_ID, 
	DESC_RN_ARS_BILLING_CLASS AS DESC_RN_ARS_BILLING_CLASS, 
	DESC_ARS_PRIOR_CAN_AMT AS DESC_ARS_PRIOR_CAN_AMT, 
	DESC_ARS_PRIOR_EXP_CAN_AMT AS DESC_ARS_PRIOR_EXP_CAN_AMT, 
	DESC_RN_ARS_FUTURE_USE AS DESC_RN_ARS_FUTURE_USE, 
	DESCRIPTION_LOAN_NUMBER AS DESCRIPTION_LOAN_NUMBER, 
	DESC_LP_ID_NUMBER AS DESC_LP_ID_NUMBER, 
	DESC_ZIP_IND AS DESC_ZIP_IND, 
	DESC_ENDORSEMENT_DATE AS DESC_ENDORSEMENT_DATE, 
	DESC_DATE_STAMP AS DESC_DATE_STAMP, 
	DESC_TIME_STAMP AS DESC_TIME_STAMP, 
	DESC_AIS_INDICATOR AS DESC_AIS_INDICATOR, 
	DESC_LGL_ADDR_CHANGE_ID, 
	DESC_MORTGAGEE_CHANGE_SW, 
	DESC_CUST_FUTURE_USE AS DESC_CUST_FUTURE_USE, 
	DESC_WATERCRAFT_SEQ, 
	DESC_YR2000_CUST_USE AS DESC_YR2000_CUST_USE, 
	DESC_DUP_KEY_SEQ_NUM AS DESC_DUP_KEY_SEQ_NUM, 
	LOGICAL_FLAG, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID, 
	INF_ACTION, 
	INF_TIMESTAMP
	FROM EXP_arch_pif_12_stage
),