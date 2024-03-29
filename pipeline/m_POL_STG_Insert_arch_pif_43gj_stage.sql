WITH
SQ_pif_43gj_stage AS (
	SELECT
		pif_43gj_stage_id,
		pif_symbol AS PIF_SYMBOL,
		pif_policy_number AS PIF_POLICY_NUMBER,
		pif_module AS PIF_MODULE,
		pmd4j_rec_length AS PMD4J_REC_LENGTH,
		pmd4j_action_code AS PMD4J_ACTION_CODE,
		pmd4j_file_id AS PMD4J_FILE_ID,
		pmd4j_segment_id AS PMD4J_SEGMENT_ID,
		pmd4j_segment_status AS PMD4J_SEGMENT_STATUS,
		pmd4j_year_transaction AS PMD4J_YEAR_TRANSACTION,
		pmd4j_month_transaction AS PMD4J_MONTH_TRANSACTION,
		pmd4j_day_transaction AS PMD4J_DAY_TRANSACTION,
		pmd4j_segment_level_code AS PMD4J_SEGMENT_LEVEL_CODE,
		pmd4j_segment_part_code AS PMD4J_SEGMENT_PART_CODE,
		pmd4j_sub_part_code AS PMD4J_SUB_PART_CODE,
		pmd4j_insurance_line AS PMD4J_INSURANCE_LINE,
		pmd4j_location_number AS PMD4J_LOCATION_NUMBER,
		pmd4j_sub_location_number AS PMD4J_SUB_LOCATION_NUMBER,
		pmd4j_risk_unit_group AS PMD4J_RISK_UNIT_GROUP,
		pmd4j_seq_rsk_unt_grp AS PMD4J_SEQ_RSK_UNT_GRP,
		pmd4j_risk_unit AS PMD4J_RISK_UNIT,
		pmd4j_risk_sequence AS PMD4J_RISK_SEQUENCE,
		pmd4j_risk_type_ind AS PMD4J_RISK_TYPE_IND,
		pmd4j_year_item_effective AS PMD4J_YEAR_ITEM_EFFECTIVE,
		pmd4j_month_item_effective AS PMD4J_MONTH_ITEM_EFFECTIVE,
		pmd4j_day_item_effective AS PMD4J_DAY_ITEM_EFFECTIVE,
		pmd4j_use_code AS PMD4J_USE_CODE,
		pmd4j_sequence_use_code AS PMD4J_SEQUENCE_USE_CODE,
		pmd4j_year_process AS PMD4J_YEAR_PROCESS,
		pmd4j_month_process AS PMD4J_MONTH_PROCESS,
		pmd4j_day_process AS PMD4J_DAY_PROCESS,
		pmd4j_use_code_data AS PMD4J_USE_CODE_DATA,
		pmd4j_sort_name AS PMD4J_SORT_NAME,
		pmd4j_tax_loc AS PMD4J_TAX_LOC,
		pmd4j_name_type_ind AS PMD4J_NAME_TYPE_IND,
		pmd4j_address_line_1 AS PMD4J_ADDRESS_LINE_1,
		pmd4j_addr_lin_2_pos_1 AS PMD4J_ADDR_LIN_2_POS_1,
		pmd4j_addr_lin_2_pos_2_30 AS PMD4J_ADDR_LIN_2_POS_2_30,
		pmd4j_address_line_3 AS PMD4J_ADDRESS_LINE_3,
		pmd4j_address_line_4 AS PMD4J_ADDRESS_LINE_4,
		pmd4j_id_number_9 AS PMD4J_ID_NUMBER_9,
		pmd4j_postal_code_1 AS PMD4J_POSTAL_CODE_1,
		pmd4j_phone_area AS PMD4J_PHONE_AREA,
		pmd4j_phone_exchange AS PMD4J_PHONE_EXCHANGE,
		pmd4j_phone_number AS PMD4J_PHONE_NUMBER,
		pmd4j_phone_extension AS PMD4J_PHONE_EXTENSION,
		pmd4j_interest_item AS PMD4J_INTEREST_ITEM,
		pmd4j_loc_line_exclusion AS PMD4J_LOC_LINE_EXCLUSION,
		pmd4j_surcharge AS PMD4J_SURCHARGE,
		pmd4j_municipal_tax AS PMD4J_MUNICIPAL_TAX,
		pmd4j_02_audit_code AS PMD4J_02_AUDIT_CODE,
		pmd4j_02_legal_entity AS PMD4J_02_LEGAL_ENTITY,
		pmd4j_dec_change_flag AS PMD4J_DEC_CHANGE_FLAG,
		pmd4j_blkt_misc_loc_exclusion AS PMD4J_BLKT_MISC_LOC_EXCLUSION,
		pmd4j_location_state AS PMD4J_LOCATION_STATE,
		pmd4j_matching_loc_sw AS PMD4J_MATCHING_LOC_SW,
		pmd4j_item_expire_year_cc AS PMD4J_ITEM_EXPIRE_YEAR_CC,
		pmd4j_item_expire_year_yy AS PMD4J_ITEM_EXPIRE_YEAR_YY,
		pmd4j_item_expire_month AS PMD4J_ITEM_EXPIRE_MONTH,
		pmd4j_item_expire_day AS PMD4J_ITEM_EXPIRE_DAY,
		pmd4j_class AS PMD4J_CLASS,
		pmd4j_name_ext_ind AS PMD4J_NAME_EXT_IND,
		pmd4j_pms_future_use AS PMD4J_PMS_FUTURE_USE,
		pmd4j_wbm_action_ind AS PMD4J_WBM_ACTION_IND,
		pmd4j_wbm_form AS PMD4J_WBM_FORM,
		pmd4j_wbm_cdays AS PMD4J_WBM_CDAYS,
		pmd4j_wbm_cprnt AS PMD4J_WBM_CPRNT,
		pmd4j_dist_area AS PMD4J_DIST_AREA,
		pmd4j_risk_city AS PMD4J_RISK_CITY,
		pmd4j_survey_status AS PMD4J_SURVEY_STATUS,
		pmd4j_survey_date AS PMD4J_SURVEY_DATE,
		pmd4j_pc_indicator AS PMD4J_PC_INDICATOR,
		pmd4j_yr2000_cust_use AS PMD4J_YR2000_CUST_USE,
		logical_flag,
		extract_date AS EXTRACT_DATE,
		as_of_date AS AS_OF_DATE,
		record_count AS RECORD_COUNT,
		source_system_id AS SOURCE_SYSTEM_ID,
		inf_action,
		inf_timestamp
	FROM pif_43gj_stage
),
EXP_PIF_43GJ_STAGE AS (
	SELECT
	pif_43gj_stage_id,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	PMD4J_REC_LENGTH,
	PMD4J_ACTION_CODE,
	PMD4J_FILE_ID,
	PMD4J_SEGMENT_ID,
	PMD4J_SEGMENT_STATUS,
	PMD4J_YEAR_TRANSACTION,
	PMD4J_MONTH_TRANSACTION,
	PMD4J_DAY_TRANSACTION,
	PMD4J_SEGMENT_LEVEL_CODE,
	PMD4J_SEGMENT_PART_CODE,
	PMD4J_SUB_PART_CODE,
	PMD4J_INSURANCE_LINE,
	PMD4J_LOCATION_NUMBER,
	PMD4J_SUB_LOCATION_NUMBER,
	PMD4J_RISK_UNIT_GROUP,
	PMD4J_SEQ_RSK_UNT_GRP,
	PMD4J_RISK_UNIT,
	PMD4J_RISK_SEQUENCE,
	PMD4J_RISK_TYPE_IND,
	PMD4J_YEAR_ITEM_EFFECTIVE,
	PMD4J_MONTH_ITEM_EFFECTIVE,
	PMD4J_DAY_ITEM_EFFECTIVE,
	PMD4J_USE_CODE,
	PMD4J_SEQUENCE_USE_CODE,
	PMD4J_YEAR_PROCESS,
	PMD4J_MONTH_PROCESS,
	PMD4J_DAY_PROCESS,
	PMD4J_USE_CODE_DATA,
	PMD4J_SORT_NAME,
	PMD4J_TAX_LOC,
	PMD4J_NAME_TYPE_IND,
	PMD4J_ADDRESS_LINE_1,
	PMD4J_ADDR_LIN_2_POS_1,
	PMD4J_ADDR_LIN_2_POS_2_30,
	PMD4J_ADDRESS_LINE_3,
	PMD4J_ADDRESS_LINE_4,
	PMD4J_ID_NUMBER_9,
	PMD4J_POSTAL_CODE_1,
	PMD4J_PHONE_AREA,
	PMD4J_PHONE_EXCHANGE,
	PMD4J_PHONE_NUMBER,
	PMD4J_PHONE_EXTENSION,
	PMD4J_INTEREST_ITEM,
	PMD4J_LOC_LINE_EXCLUSION,
	PMD4J_SURCHARGE,
	PMD4J_MUNICIPAL_TAX,
	PMD4J_02_AUDIT_CODE,
	PMD4J_02_LEGAL_ENTITY,
	PMD4J_DEC_CHANGE_FLAG,
	PMD4J_BLKT_MISC_LOC_EXCLUSION,
	PMD4J_LOCATION_STATE,
	PMD4J_MATCHING_LOC_SW,
	PMD4J_ITEM_EXPIRE_YEAR_CC,
	PMD4J_ITEM_EXPIRE_YEAR_YY,
	PMD4J_ITEM_EXPIRE_MONTH,
	PMD4J_ITEM_EXPIRE_DAY,
	PMD4J_CLASS,
	PMD4J_NAME_EXT_IND,
	PMD4J_PMS_FUTURE_USE,
	PMD4J_WBM_ACTION_IND,
	PMD4J_WBM_FORM,
	PMD4J_WBM_CDAYS,
	PMD4J_WBM_CPRNT,
	PMD4J_DIST_AREA,
	PMD4J_RISK_CITY,
	PMD4J_SURVEY_STATUS,
	PMD4J_SURVEY_DATE,
	PMD4J_PC_INDICATOR,
	PMD4J_YR2000_CUST_USE,
	logical_flag,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	inf_action,
	inf_timestamp,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_audit_id
	FROM SQ_pif_43gj_stage
),
arch_pif_43gj_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_pif_43gj_stage
	(pif_43gj_stage_id, pif_symbol, pif_policy_number, pif_module, pmd4j_rec_length, pmd4j_action_code, pmd4j_file_id, pmd4j_segment_id, pmd4j_segment_status, pmd4j_year_transaction, pmd4j_month_transaction, pmd4j_day_transaction, pmd4j_segment_level_code, pmd4j_segment_part_code, pmd4j_sub_part_code, pmd4j_insurance_line, pmd4j_location_number, pmd4j_sub_location_number, pmd4j_risk_unit_group, pmd4j_seq_rsk_unt_grp, pmd4j_risk_unit, pmd4j_risk_sequence, pmd4j_risk_type_ind, pmd4j_year_item_effective, pmd4j_month_item_effective, pmd4j_day_item_effective, pmd4j_use_code, pmd4j_sequence_use_code, pmd4j_year_process, pmd4j_month_process, pmd4j_day_process, pmd4j_use_code_data, pmd4j_sort_name, pmd4j_tax_loc, pmd4j_name_type_ind, pmd4j_address_line_1, pmd4j_addr_lin_2_pos_1, pmd4j_addr_lin_2_pos_2_30, pmd4j_address_line_3, pmd4j_address_line_4, pmd4j_id_number_9, pmd4j_postal_code_1, pmd4j_phone_area, pmd4j_phone_exchange, pmd4j_phone_number, pmd4j_phone_extension, pmd4j_interest_item, pmd4j_loc_line_exclusion, pmd4j_surcharge, pmd4j_municipal_tax, pmd4j_02_audit_code, pmd4j_02_legal_entity, pmd4j_dec_change_flag, pmd4j_blkt_misc_loc_exclusion, pmd4j_location_state, pmd4j_matching_loc_sw, pmd4j_item_expire_year_cc, pmd4j_item_expire_year_yy, pmd4j_item_expire_month, pmd4j_item_expire_day, pmd4j_class, pmd4j_name_ext_ind, pmd4j_pms_future_use, pmd4j_wbm_action_ind, pmd4j_wbm_form, pmd4j_wbm_cdays, pmd4j_wbm_cprnt, pmd4j_dist_area, pmd4j_risk_city, pmd4j_survey_status, pmd4j_survey_date, pmd4j_pc_indicator, pmd4j_yr2000_cust_use, logical_flag, extract_date, as_of_date, record_count, source_system_id, audit_id, inf_action, inf_timestamp)
	SELECT 
	PIF_43GJ_STAGE_ID, 
	PIF_SYMBOL AS PIF_SYMBOL, 
	PIF_POLICY_NUMBER AS PIF_POLICY_NUMBER, 
	PIF_MODULE AS PIF_MODULE, 
	PMD4J_REC_LENGTH AS PMD4J_REC_LENGTH, 
	PMD4J_ACTION_CODE AS PMD4J_ACTION_CODE, 
	PMD4J_FILE_ID AS PMD4J_FILE_ID, 
	PMD4J_SEGMENT_ID AS PMD4J_SEGMENT_ID, 
	PMD4J_SEGMENT_STATUS AS PMD4J_SEGMENT_STATUS, 
	PMD4J_YEAR_TRANSACTION AS PMD4J_YEAR_TRANSACTION, 
	PMD4J_MONTH_TRANSACTION AS PMD4J_MONTH_TRANSACTION, 
	PMD4J_DAY_TRANSACTION AS PMD4J_DAY_TRANSACTION, 
	PMD4J_SEGMENT_LEVEL_CODE AS PMD4J_SEGMENT_LEVEL_CODE, 
	PMD4J_SEGMENT_PART_CODE AS PMD4J_SEGMENT_PART_CODE, 
	PMD4J_SUB_PART_CODE AS PMD4J_SUB_PART_CODE, 
	PMD4J_INSURANCE_LINE AS PMD4J_INSURANCE_LINE, 
	PMD4J_LOCATION_NUMBER AS PMD4J_LOCATION_NUMBER, 
	PMD4J_SUB_LOCATION_NUMBER AS PMD4J_SUB_LOCATION_NUMBER, 
	PMD4J_RISK_UNIT_GROUP AS PMD4J_RISK_UNIT_GROUP, 
	PMD4J_SEQ_RSK_UNT_GRP AS PMD4J_SEQ_RSK_UNT_GRP, 
	PMD4J_RISK_UNIT AS PMD4J_RISK_UNIT, 
	PMD4J_RISK_SEQUENCE AS PMD4J_RISK_SEQUENCE, 
	PMD4J_RISK_TYPE_IND AS PMD4J_RISK_TYPE_IND, 
	PMD4J_YEAR_ITEM_EFFECTIVE AS PMD4J_YEAR_ITEM_EFFECTIVE, 
	PMD4J_MONTH_ITEM_EFFECTIVE AS PMD4J_MONTH_ITEM_EFFECTIVE, 
	PMD4J_DAY_ITEM_EFFECTIVE AS PMD4J_DAY_ITEM_EFFECTIVE, 
	PMD4J_USE_CODE AS PMD4J_USE_CODE, 
	PMD4J_SEQUENCE_USE_CODE AS PMD4J_SEQUENCE_USE_CODE, 
	PMD4J_YEAR_PROCESS AS PMD4J_YEAR_PROCESS, 
	PMD4J_MONTH_PROCESS AS PMD4J_MONTH_PROCESS, 
	PMD4J_DAY_PROCESS AS PMD4J_DAY_PROCESS, 
	PMD4J_USE_CODE_DATA AS PMD4J_USE_CODE_DATA, 
	PMD4J_SORT_NAME AS PMD4J_SORT_NAME, 
	PMD4J_TAX_LOC AS PMD4J_TAX_LOC, 
	PMD4J_NAME_TYPE_IND AS PMD4J_NAME_TYPE_IND, 
	PMD4J_ADDRESS_LINE_1 AS PMD4J_ADDRESS_LINE_1, 
	PMD4J_ADDR_LIN_2_POS_1 AS PMD4J_ADDR_LIN_2_POS_1, 
	PMD4J_ADDR_LIN_2_POS_2_30 AS PMD4J_ADDR_LIN_2_POS_2_30, 
	PMD4J_ADDRESS_LINE_3 AS PMD4J_ADDRESS_LINE_3, 
	PMD4J_ADDRESS_LINE_4 AS PMD4J_ADDRESS_LINE_4, 
	PMD4J_ID_NUMBER_9 AS PMD4J_ID_NUMBER_9, 
	PMD4J_POSTAL_CODE_1 AS PMD4J_POSTAL_CODE_1, 
	PMD4J_PHONE_AREA AS PMD4J_PHONE_AREA, 
	PMD4J_PHONE_EXCHANGE AS PMD4J_PHONE_EXCHANGE, 
	PMD4J_PHONE_NUMBER AS PMD4J_PHONE_NUMBER, 
	PMD4J_PHONE_EXTENSION AS PMD4J_PHONE_EXTENSION, 
	PMD4J_INTEREST_ITEM AS PMD4J_INTEREST_ITEM, 
	PMD4J_LOC_LINE_EXCLUSION AS PMD4J_LOC_LINE_EXCLUSION, 
	PMD4J_SURCHARGE AS PMD4J_SURCHARGE, 
	PMD4J_MUNICIPAL_TAX AS PMD4J_MUNICIPAL_TAX, 
	PMD4J_02_AUDIT_CODE AS PMD4J_02_AUDIT_CODE, 
	PMD4J_02_LEGAL_ENTITY AS PMD4J_02_LEGAL_ENTITY, 
	PMD4J_DEC_CHANGE_FLAG AS PMD4J_DEC_CHANGE_FLAG, 
	PMD4J_BLKT_MISC_LOC_EXCLUSION AS PMD4J_BLKT_MISC_LOC_EXCLUSION, 
	PMD4J_LOCATION_STATE AS PMD4J_LOCATION_STATE, 
	PMD4J_MATCHING_LOC_SW AS PMD4J_MATCHING_LOC_SW, 
	PMD4J_ITEM_EXPIRE_YEAR_CC AS PMD4J_ITEM_EXPIRE_YEAR_CC, 
	PMD4J_ITEM_EXPIRE_YEAR_YY AS PMD4J_ITEM_EXPIRE_YEAR_YY, 
	PMD4J_ITEM_EXPIRE_MONTH AS PMD4J_ITEM_EXPIRE_MONTH, 
	PMD4J_ITEM_EXPIRE_DAY AS PMD4J_ITEM_EXPIRE_DAY, 
	PMD4J_CLASS AS PMD4J_CLASS, 
	PMD4J_NAME_EXT_IND AS PMD4J_NAME_EXT_IND, 
	PMD4J_PMS_FUTURE_USE AS PMD4J_PMS_FUTURE_USE, 
	PMD4J_WBM_ACTION_IND AS PMD4J_WBM_ACTION_IND, 
	PMD4J_WBM_FORM AS PMD4J_WBM_FORM, 
	PMD4J_WBM_CDAYS AS PMD4J_WBM_CDAYS, 
	PMD4J_WBM_CPRNT AS PMD4J_WBM_CPRNT, 
	PMD4J_DIST_AREA AS PMD4J_DIST_AREA, 
	PMD4J_RISK_CITY AS PMD4J_RISK_CITY, 
	PMD4J_SURVEY_STATUS AS PMD4J_SURVEY_STATUS, 
	PMD4J_SURVEY_DATE AS PMD4J_SURVEY_DATE, 
	PMD4J_PC_INDICATOR AS PMD4J_PC_INDICATOR, 
	PMD4J_YR2000_CUST_USE AS PMD4J_YR2000_CUST_USE, 
	LOGICAL_FLAG, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	o_audit_id AS AUDIT_ID, 
	INF_ACTION, 
	INF_TIMESTAMP
	FROM EXP_PIF_43GJ_STAGE
),