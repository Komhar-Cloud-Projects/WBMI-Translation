WITH
SQ_PIF_42GJ_stage AS (
	SELECT
		pif_42gj_stage_id AS PIF_42GJ_stage_id,
		pif_symbol AS PIF_SYMBOL,
		pif_policy_number AS PIF_POLICY_NUMBER,
		pif_module AS PIF_MODULE,
		ipfc4j_rec_length_red AS IPFC4J_REC_LENGTH_RED,
		ipfc4j_action_code AS IPFC4J_ACTION_CODE,
		ipfc4j_file_id AS IPFC4J_FILE_ID,
		ipfc4j_segment_id AS IPFC4J_SEGMENT_ID,
		ipfc4j_segment_level_code AS IPFC4J_SEGMENT_LEVEL_CODE,
		ipfc4j_segment_part_code AS IPFC4J_SEGMENT_PART_CODE,
		ipfc4j_sub_part_code AS IPFC4J_SUB_PART_CODE,
		ipfc4j_insurance_line AS IPFC4J_INSURANCE_LINE,
		ipfc4j_location_number AS IPFC4J_LOCATION_NUMBER,
		ipfc4j_sub_location_number AS IPFC4J_SUB_LOCATION_NUMBER,
		ipfc4j_risk_unit_group AS IPFC4J_RISK_UNIT_GROUP,
		ipfc4j_class_code_group AS IPFC4J_CLASS_CODE_GROUP,
		ipfc4j_class_code_member AS IPFC4J_CLASS_CODE_MEMBER,
		ipfc4j_risk_unit AS IPFC4J_RISK_UNIT,
		ipfc4j_risk_sequence AS IPFC4J_RISK_SEQUENCE,
		ipfc4j_risk_type_ind AS IPFC4J_RISK_TYPE_IND,
		ipfc4j_year_item_effective AS IPFC4J_YEAR_ITEM_EFFECTIVE,
		ipfc4j_month_item_effective AS IPFC4J_MONTH_ITEM_EFFECTIVE,
		ipfc4j_day_item_effective AS IPFC4J_DAY_ITEM_EFFECTIVE,
		ipfc4j_loss_year AS IPFC4J_LOSS_YEAR,
		ipfc4j_loss_month AS IPFC4J_LOSS_MONTH,
		ipfc4j_loss_day AS IPFC4J_LOSS_DAY,
		ipfc4j_loss_occurence AS IPFC4J_LOSS_OCCURENCE,
		ipfc4j_loss_claimant AS IPFC4J_LOSS_CLAIMANT,
		ipfc4j_use_code AS IPFC4J_USE_CODE,
		ipfc4j_sequence_use_code AS IPFC4J_SEQUENCE_USE_CODE,
		ipfc4j_ai_seq_use_code AS IPFC4J_AI_SEQ_USE_CODE,
		ipfc4j_year_process AS IPFC4J_YEAR_PROCESS,
		ipfc4j_month_process AS IPFC4J_MONTH_PROCESS,
		ipfc4j_day_process AS IPFC4J_DAY_PROCESS,
		ipfc4j_year_change_entry AS IPFC4J_YEAR_CHANGE_ENTRY,
		ipfc4j_month_change_entry AS IPFC4J_MONTH_CHANGE_ENTRY,
		ipfc4j_day_change_entry AS IPFC4J_DAY_CHANGE_ENTRY,
		ipfc4j_sequence_change_entry AS IPFC4J_SEQUENCE_CHANGE_ENTRY,
		ipfc4j_segment_status AS IPFC4J_SEGMENT_STATUS,
		ipfc4j_entry_operator AS IPFC4J_ENTRY_OPERATOR,
		ipfc4j_use_code_data AS IPFC4J_USE_CODE_DATA,
		ipfc4j_sort_name AS IPFC4J_SORT_NAME,
		ipfc4j_name_type_ind AS IPFC4J_NAME_TYPE_IND,
		ipfc4j_address_line_1 AS IPFC4J_ADDRESS_LINE_1,
		ipfc4j_addr_lin_2_pos_1 AS IPFC4J_ADDR_LIN_2_POS_1,
		ipfc4j_addr_lin_2_pos_2_30 AS IPFC4J_ADDR_LIN_2_POS_2_30,
		ipfc4j_address_line_3 AS IPFC4J_ADDRESS_LINE_3,
		ipfc4j_address_line_4 AS IPFC4J_ADDRESS_LINE_4,
		ipfc4j_id_number AS IPFC4J_ID_NUMBER,
		ipfc4j_zip_basic AS IPFC4J_ZIP_BASIC,
		ipfc4j_zip_expanded AS IPFC4J_ZIP_EXPANDED,
		ipfc4j_phone_area AS IPFC4J_PHONE_AREA,
		ipfc4j_phone_exchange AS IPFC4J_PHONE_EXCHANGE,
		ipfc4j_phone_number AS IPFC4J_PHONE_NUMBER,
		ipfc4j_phone_extension AS IPFC4J_PHONE_EXTENSION,
		ipfc4j_interest_item AS IPFC4J_INTEREST_ITEM,
		ipfc4j_location_state AS IPFC4J_LOCATION_STATE,
		ipfc4j_offset_onset_ind AS IPFC4J_OFFSET_ONSET_IND,
		ipfc4j_pms_future_use_4j_1 AS IPFC4J_PMS_FUTURE_USE_4J_1,
		ipfc4j_cust_spl_use_4j_1 AS IPFC4J_CUST_SPL_USE_4J_1,
		ipfc4j_pms_future_use AS IPFC4J_PMS_FUTURE_USE,
		ipfc4j_yr2000_cust_use AS IPFC4J_YR2000_CUST_USE,
		inf_action,
		inf_timestamp,
		logical_flag,
		extract_date AS EXTRACT_DATE,
		as_of_date AS AS_OF_DATE,
		record_count AS RECORD_COUNT,
		source_system_id AS SOURCE_SYSTEM_ID
	FROM PIF_42GJ_stage
),
EXP_arch_PIF_42GJ_stage AS (
	SELECT
	PIF_42GJ_stage_id,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFC4J_REC_LENGTH_RED,
	IPFC4J_ACTION_CODE,
	IPFC4J_FILE_ID,
	IPFC4J_SEGMENT_ID,
	IPFC4J_SEGMENT_LEVEL_CODE,
	IPFC4J_SEGMENT_PART_CODE,
	IPFC4J_SUB_PART_CODE,
	IPFC4J_INSURANCE_LINE,
	IPFC4J_LOCATION_NUMBER,
	IPFC4J_SUB_LOCATION_NUMBER,
	IPFC4J_RISK_UNIT_GROUP,
	IPFC4J_CLASS_CODE_GROUP,
	IPFC4J_CLASS_CODE_MEMBER,
	IPFC4J_RISK_UNIT,
	IPFC4J_RISK_SEQUENCE,
	IPFC4J_RISK_TYPE_IND,
	IPFC4J_YEAR_ITEM_EFFECTIVE,
	IPFC4J_MONTH_ITEM_EFFECTIVE,
	IPFC4J_DAY_ITEM_EFFECTIVE,
	IPFC4J_LOSS_YEAR,
	IPFC4J_LOSS_MONTH,
	IPFC4J_LOSS_DAY,
	IPFC4J_LOSS_OCCURENCE,
	IPFC4J_LOSS_CLAIMANT,
	IPFC4J_USE_CODE,
	IPFC4J_SEQUENCE_USE_CODE,
	IPFC4J_AI_SEQ_USE_CODE,
	IPFC4J_YEAR_PROCESS,
	IPFC4J_MONTH_PROCESS,
	IPFC4J_DAY_PROCESS,
	IPFC4J_YEAR_CHANGE_ENTRY,
	IPFC4J_MONTH_CHANGE_ENTRY,
	IPFC4J_DAY_CHANGE_ENTRY,
	IPFC4J_SEQUENCE_CHANGE_ENTRY,
	IPFC4J_SEGMENT_STATUS,
	IPFC4J_ENTRY_OPERATOR,
	IPFC4J_USE_CODE_DATA,
	IPFC4J_SORT_NAME,
	IPFC4J_NAME_TYPE_IND,
	IPFC4J_ADDRESS_LINE_1,
	IPFC4J_ADDR_LIN_2_POS_1,
	IPFC4J_ADDR_LIN_2_POS_2_30,
	IPFC4J_ADDRESS_LINE_3,
	IPFC4J_ADDRESS_LINE_4,
	IPFC4J_ID_NUMBER,
	IPFC4J_ZIP_BASIC,
	IPFC4J_ZIP_EXPANDED,
	IPFC4J_PHONE_AREA,
	IPFC4J_PHONE_EXCHANGE,
	IPFC4J_PHONE_NUMBER,
	IPFC4J_PHONE_EXTENSION,
	IPFC4J_INTEREST_ITEM,
	IPFC4J_LOCATION_STATE,
	IPFC4J_OFFSET_ONSET_IND,
	IPFC4J_PMS_FUTURE_USE_4J_1,
	IPFC4J_CUST_SPL_USE_4J_1,
	IPFC4J_PMS_FUTURE_USE,
	IPFC4J_YR2000_CUST_USE,
	inf_action,
	inf_timestamp,
	logical_flag,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_PIF_42GJ_stage
),
arch_PIF_42GJ_stage AS (
	INSERT INTO arch_PIF_42GJ_stage
	(pif_42gj_stage_id, pif_symbol, pif_policy_number, pif_module, ipfc4j_rec_length_red, ipfc4j_action_code, ipfc4j_file_id, ipfc4j_segment_id, ipfc4j_segment_level_code, ipfc4j_segment_part_code, ipfc4j_sub_part_code, ipfc4j_insurance_line, ipfc4j_location_number, ipfc4j_sub_location_number, ipfc4j_risk_unit_group, ipfc4j_class_code_group, ipfc4j_class_code_member, ipfc4j_risk_unit, ipfc4j_risk_sequence, ipfc4j_risk_type_ind, ipfc4j_year_item_effective, ipfc4j_month_item_effective, ipfc4j_day_item_effective, ipfc4j_loss_year, ipfc4j_loss_month, ipfc4j_loss_day, ipfc4j_loss_occurence, ipfc4j_loss_claimant, ipfc4j_use_code, ipfc4j_sequence_use_code, ipfc4j_ai_seq_use_code, ipfc4j_year_process, ipfc4j_month_process, ipfc4j_day_process, ipfc4j_year_change_entry, ipfc4j_month_change_entry, ipfc4j_day_change_entry, ipfc4j_sequence_change_entry, ipfc4j_segment_status, ipfc4j_entry_operator, ipfc4j_use_code_data, ipfc4j_sort_name, ipfc4j_name_type_ind, ipfc4j_address_line_1, ipfc4j_addr_lin_2_pos_1, ipfc4j_addr_lin_2_pos_2_30, ipfc4j_address_line_3, ipfc4j_address_line_4, ipfc4j_id_number, ipfc4j_zip_basic, ipfc4j_zip_expanded, ipfc4j_phone_area, ipfc4j_phone_exchange, ipfc4j_phone_number, ipfc4j_phone_extension, ipfc4j_interest_item, ipfc4j_location_state, ipfc4j_offset_onset_ind, ipfc4j_pms_future_use_4j_1, ipfc4j_cust_spl_use_4j_1, ipfc4j_pms_future_use, ipfc4j_yr2000_cust_use, inf_action, inf_timestamp, logical_flag, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	PIF_42GJ_stage_id AS PIF_42GJ_STAGE_ID, 
	PIF_SYMBOL AS PIF_SYMBOL, 
	PIF_POLICY_NUMBER AS PIF_POLICY_NUMBER, 
	PIF_MODULE AS PIF_MODULE, 
	IPFC4J_REC_LENGTH_RED AS IPFC4J_REC_LENGTH_RED, 
	IPFC4J_ACTION_CODE AS IPFC4J_ACTION_CODE, 
	IPFC4J_FILE_ID AS IPFC4J_FILE_ID, 
	IPFC4J_SEGMENT_ID AS IPFC4J_SEGMENT_ID, 
	IPFC4J_SEGMENT_LEVEL_CODE AS IPFC4J_SEGMENT_LEVEL_CODE, 
	IPFC4J_SEGMENT_PART_CODE AS IPFC4J_SEGMENT_PART_CODE, 
	IPFC4J_SUB_PART_CODE AS IPFC4J_SUB_PART_CODE, 
	IPFC4J_INSURANCE_LINE AS IPFC4J_INSURANCE_LINE, 
	IPFC4J_LOCATION_NUMBER AS IPFC4J_LOCATION_NUMBER, 
	IPFC4J_SUB_LOCATION_NUMBER AS IPFC4J_SUB_LOCATION_NUMBER, 
	IPFC4J_RISK_UNIT_GROUP AS IPFC4J_RISK_UNIT_GROUP, 
	IPFC4J_CLASS_CODE_GROUP AS IPFC4J_CLASS_CODE_GROUP, 
	IPFC4J_CLASS_CODE_MEMBER AS IPFC4J_CLASS_CODE_MEMBER, 
	IPFC4J_RISK_UNIT AS IPFC4J_RISK_UNIT, 
	IPFC4J_RISK_SEQUENCE AS IPFC4J_RISK_SEQUENCE, 
	IPFC4J_RISK_TYPE_IND AS IPFC4J_RISK_TYPE_IND, 
	IPFC4J_YEAR_ITEM_EFFECTIVE AS IPFC4J_YEAR_ITEM_EFFECTIVE, 
	IPFC4J_MONTH_ITEM_EFFECTIVE AS IPFC4J_MONTH_ITEM_EFFECTIVE, 
	IPFC4J_DAY_ITEM_EFFECTIVE AS IPFC4J_DAY_ITEM_EFFECTIVE, 
	IPFC4J_LOSS_YEAR AS IPFC4J_LOSS_YEAR, 
	IPFC4J_LOSS_MONTH AS IPFC4J_LOSS_MONTH, 
	IPFC4J_LOSS_DAY AS IPFC4J_LOSS_DAY, 
	IPFC4J_LOSS_OCCURENCE AS IPFC4J_LOSS_OCCURENCE, 
	IPFC4J_LOSS_CLAIMANT AS IPFC4J_LOSS_CLAIMANT, 
	IPFC4J_USE_CODE AS IPFC4J_USE_CODE, 
	IPFC4J_SEQUENCE_USE_CODE AS IPFC4J_SEQUENCE_USE_CODE, 
	IPFC4J_AI_SEQ_USE_CODE AS IPFC4J_AI_SEQ_USE_CODE, 
	IPFC4J_YEAR_PROCESS AS IPFC4J_YEAR_PROCESS, 
	IPFC4J_MONTH_PROCESS AS IPFC4J_MONTH_PROCESS, 
	IPFC4J_DAY_PROCESS AS IPFC4J_DAY_PROCESS, 
	IPFC4J_YEAR_CHANGE_ENTRY AS IPFC4J_YEAR_CHANGE_ENTRY, 
	IPFC4J_MONTH_CHANGE_ENTRY AS IPFC4J_MONTH_CHANGE_ENTRY, 
	IPFC4J_DAY_CHANGE_ENTRY AS IPFC4J_DAY_CHANGE_ENTRY, 
	IPFC4J_SEQUENCE_CHANGE_ENTRY AS IPFC4J_SEQUENCE_CHANGE_ENTRY, 
	IPFC4J_SEGMENT_STATUS AS IPFC4J_SEGMENT_STATUS, 
	IPFC4J_ENTRY_OPERATOR AS IPFC4J_ENTRY_OPERATOR, 
	IPFC4J_USE_CODE_DATA AS IPFC4J_USE_CODE_DATA, 
	IPFC4J_SORT_NAME AS IPFC4J_SORT_NAME, 
	IPFC4J_NAME_TYPE_IND AS IPFC4J_NAME_TYPE_IND, 
	IPFC4J_ADDRESS_LINE_1 AS IPFC4J_ADDRESS_LINE_1, 
	IPFC4J_ADDR_LIN_2_POS_1 AS IPFC4J_ADDR_LIN_2_POS_1, 
	IPFC4J_ADDR_LIN_2_POS_2_30 AS IPFC4J_ADDR_LIN_2_POS_2_30, 
	IPFC4J_ADDRESS_LINE_3 AS IPFC4J_ADDRESS_LINE_3, 
	IPFC4J_ADDRESS_LINE_4 AS IPFC4J_ADDRESS_LINE_4, 
	IPFC4J_ID_NUMBER AS IPFC4J_ID_NUMBER, 
	IPFC4J_ZIP_BASIC AS IPFC4J_ZIP_BASIC, 
	IPFC4J_ZIP_EXPANDED AS IPFC4J_ZIP_EXPANDED, 
	IPFC4J_PHONE_AREA AS IPFC4J_PHONE_AREA, 
	IPFC4J_PHONE_EXCHANGE AS IPFC4J_PHONE_EXCHANGE, 
	IPFC4J_PHONE_NUMBER AS IPFC4J_PHONE_NUMBER, 
	IPFC4J_PHONE_EXTENSION AS IPFC4J_PHONE_EXTENSION, 
	IPFC4J_INTEREST_ITEM AS IPFC4J_INTEREST_ITEM, 
	IPFC4J_LOCATION_STATE AS IPFC4J_LOCATION_STATE, 
	IPFC4J_OFFSET_ONSET_IND AS IPFC4J_OFFSET_ONSET_IND, 
	IPFC4J_PMS_FUTURE_USE_4J_1 AS IPFC4J_PMS_FUTURE_USE_4J_1, 
	IPFC4J_CUST_SPL_USE_4J_1 AS IPFC4J_CUST_SPL_USE_4J_1, 
	IPFC4J_PMS_FUTURE_USE AS IPFC4J_PMS_FUTURE_USE, 
	IPFC4J_YR2000_CUST_USE AS IPFC4J_YR2000_CUST_USE, 
	INF_ACTION, 
	INF_TIMESTAMP, 
	LOGICAL_FLAG, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_PIF_42GJ_stage
),