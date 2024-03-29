WITH
LKP_pif_42gp_stage AS (
	SELECT
	logical_flag,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgp_year_of_loss,
	ipfcgp_month_of_loss,
	ipfcgp_day_of_loss,
	ipfcgp_loss_occurence
	FROM (
		SELECT 
			logical_flag,
			pif_symbol,
			pif_policy_number,
			pif_module,
			ipfcgp_year_of_loss,
			ipfcgp_month_of_loss,
			ipfcgp_day_of_loss,
			ipfcgp_loss_occurence
		FROM pif_42gp_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgp_year_of_loss,ipfcgp_month_of_loss,ipfcgp_day_of_loss,ipfcgp_loss_occurence ORDER BY logical_flag) = 1
),
SQ_pifmstr_PIF_42GJ AS (

-- TODO Manual --

),
EXP_INPUTS AS (
	SELECT
	IPFC4J_ID_NUMBER AS IPFC4J_ID_NUMBER1,
	-- *INF*: LTRIM(RTRIM(IPFC4J_ID_NUMBER1))
	LTRIM(RTRIM(IPFC4J_ID_NUMBER1)) AS IPFC4J_ID_NUMBER
	FROM SQ_pifmstr_PIF_42GJ
),
LKP_pif_42gj_stage_history AS (
),
EXP_Evaluate AS (
	SELECT
	EXP_INPUTS.IPFC4J_ID_NUMBER,
	LKP_pif_42gj_stage_history.lkp_Ipfc4J_Id_Number,
	LKP_pif_42gj_stage_history.Ipfc4J_Id_Number_Tokens,
	-- *INF*: IIF(ISNULL(REPLACECHR(0,lkp_Ipfc4J_Id_Number,CHR(13),'')),IPFC4J_ID_NUMBER,Ipfc4J_Id_Number_Tokens)
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    REGEXP_REPLACE(lkp_Ipfc4J_Id_Number,CHR(13),'','i') IS NULL, IPFC4J_ID_NUMBER,
	    Ipfc4J_Id_Number_Tokens
	) AS v_Ipfc4J_Id_Number1,
	v_Ipfc4J_Id_Number1 AS o_Ipfc4J_Id_Number
	FROM EXP_INPUTS
	LEFT JOIN LKP_pif_42gj_stage_history
	ON LKP_pif_42gj_stage_history.lkp_Ipfc4J_Id_Number = EXP_INPUTS.IPFC4J_ID_NUMBER
),
FIL_REMV_NULL_SEG_ID AS (
	SELECT
	SQ_pifmstr_PIF_42GJ.PIF_SYMBOL, 
	SQ_pifmstr_PIF_42GJ.PIF_POLICY_NUMBER, 
	SQ_pifmstr_PIF_42GJ.PIF_MODULE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_REC_LENGTH_RED, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_ACTION_CODE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_FILE_ID, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_SEGMENT_ID, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_SEGMENT_LEVEL_CODE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_SEGMENT_PART_CODE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_SUB_PART_CODE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_INSURANCE_LINE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_LOCATION_NUMBER, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_SUB_LOCATION_NUMBER, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_RISK_UNIT_GROUP, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_CLASS_CODE_GROUP, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_CLASS_CODE_MEMBER, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_RISK_UNIT, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_RISK_SEQUENCE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_RISK_TYPE_IND, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_YEAR_ITEM_EFFECTIVE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_MONTH_ITEM_EFFECTIVE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_DAY_ITEM_EFFECTIVE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_LOSS_YEAR, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_LOSS_MONTH, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_LOSS_DAY, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_LOSS_OCCURENCE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_LOSS_CLAIMANT, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_USE_CODE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_SEQUENCE_USE_CODE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_AI_SEQ_USE_CODE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_YEAR_PROCESS, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_MONTH_PROCESS, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_DAY_PROCESS, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_YEAR_CHANGE_ENTRY, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_MONTH_CHANGE_ENTRY, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_DAY_CHANGE_ENTRY, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_SEQUENCE_CHANGE_ENTRY, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_SEGMENT_STATUS, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_ENTRY_OPERATOR, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_USE_CODE_DATA, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_SORT_NAME, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_NAME_TYPE_IND, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_ADDRESS_LINE_1, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_ADDR_LIN_2_POS_1, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_ADDR_LIN_2_POS_2_30, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_ADDRESS_LINE_3, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_ADDRESS_LINE_4, 
	EXP_Evaluate.o_Ipfc4J_Id_Number AS IPFC4J_ID_NUMBER, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_ZIP_BASIC, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_ZIP_EXPANDED, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_PHONE_AREA, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_PHONE_EXCHANGE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_PHONE_NUMBER, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_PHONE_EXTENSION, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_INTEREST_ITEM, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_LOCATION_STATE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_OFFSET_ONSET_IND, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_PMS_FUTURE_USE_4J_1, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_CUST_SPL_USE_4J_1, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_PMS_FUTURE_USE, 
	SQ_pifmstr_PIF_42GJ.IPFC4J_YR2000_CUST_USE
	FROM EXP_Evaluate
	 -- Manually join with SQ_pifmstr_PIF_42GJ
	WHERE NOT ISNULL(IPFC4J_SEGMENT_ID)
),
SRT_pifmstr_PIF_42GJ_stage AS (
	SELECT
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
	IPFC4J_LOSS_YEAR, 
	IPFC4J_LOSS_MONTH, 
	IPFC4J_LOSS_DAY, 
	IPFC4J_LOSS_OCCURENCE, 
	IPFC4J_LOSS_CLAIMANT, 
	IPFC4J_USE_CODE, 
	IPFC4J_YEAR_ITEM_EFFECTIVE, 
	IPFC4J_MONTH_ITEM_EFFECTIVE, 
	IPFC4J_DAY_ITEM_EFFECTIVE, 
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
	IPFC4J_YR2000_CUST_USE
	FROM FIL_REMV_NULL_SEG_ID
	ORDER BY PIF_SYMBOL ASC, PIF_POLICY_NUMBER ASC, PIF_MODULE ASC, IPFC4J_LOSS_YEAR ASC, IPFC4J_LOSS_MONTH ASC, IPFC4J_LOSS_DAY ASC, IPFC4J_LOSS_OCCURENCE ASC, IPFC4J_LOSS_CLAIMANT ASC, IPFC4J_USE_CODE ASC, IPFC4J_YEAR_ITEM_EFFECTIVE ASC, IPFC4J_MONTH_ITEM_EFFECTIVE ASC, IPFC4J_DAY_ITEM_EFFECTIVE ASC, IPFC4J_ENTRY_OPERATOR ASC
),
AGG_pifmstr_PIF_42GJ_stage AS (
	SELECT
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
	IPFC4J_YR2000_CUST_USE
	FROM SRT_pifmstr_PIF_42GJ_stage
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PIF_SYMBOL, PIF_POLICY_NUMBER, PIF_MODULE, IPFC4J_LOSS_YEAR, IPFC4J_LOSS_MONTH, IPFC4J_LOSS_DAY, IPFC4J_LOSS_OCCURENCE, IPFC4J_LOSS_CLAIMANT, IPFC4J_USE_CODE ORDER BY NULL) = 1
),
EXP_pifmstr_PIF_42GJ_stage AS (
	SELECT
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
	-- *INF*: :LKP.LKP_PIF_42GP_STAGE(PIF_SYMBOL, PIF_POLICY_NUMBER, PIF_MODULE, IPFC4J_LOSS_YEAR, IPFC4J_LOSS_MONTH, IPFC4J_LOSS_DAY, IPFC4J_LOSS_OCCURENCE)
	LKP_PIF_42GP_STAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFC4J_LOSS_YEAR_IPFC4J_LOSS_MONTH_IPFC4J_LOSS_DAY_IPFC4J_LOSS_OCCURENCE.logical_flag AS LOGICAL_FLAG_OP,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT_OP,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM AGG_pifmstr_PIF_42GJ_stage
	LEFT JOIN LKP_PIF_42GP_STAGE LKP_PIF_42GP_STAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFC4J_LOSS_YEAR_IPFC4J_LOSS_MONTH_IPFC4J_LOSS_DAY_IPFC4J_LOSS_OCCURENCE
	ON LKP_PIF_42GP_STAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFC4J_LOSS_YEAR_IPFC4J_LOSS_MONTH_IPFC4J_LOSS_DAY_IPFC4J_LOSS_OCCURENCE.pif_symbol = PIF_SYMBOL
	AND LKP_PIF_42GP_STAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFC4J_LOSS_YEAR_IPFC4J_LOSS_MONTH_IPFC4J_LOSS_DAY_IPFC4J_LOSS_OCCURENCE.pif_policy_number = PIF_POLICY_NUMBER
	AND LKP_PIF_42GP_STAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFC4J_LOSS_YEAR_IPFC4J_LOSS_MONTH_IPFC4J_LOSS_DAY_IPFC4J_LOSS_OCCURENCE.pif_module = PIF_MODULE
	AND LKP_PIF_42GP_STAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFC4J_LOSS_YEAR_IPFC4J_LOSS_MONTH_IPFC4J_LOSS_DAY_IPFC4J_LOSS_OCCURENCE.ipfcgp_year_of_loss = IPFC4J_LOSS_YEAR
	AND LKP_PIF_42GP_STAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFC4J_LOSS_YEAR_IPFC4J_LOSS_MONTH_IPFC4J_LOSS_DAY_IPFC4J_LOSS_OCCURENCE.ipfcgp_month_of_loss = IPFC4J_LOSS_MONTH
	AND LKP_PIF_42GP_STAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFC4J_LOSS_YEAR_IPFC4J_LOSS_MONTH_IPFC4J_LOSS_DAY_IPFC4J_LOSS_OCCURENCE.ipfcgp_day_of_loss = IPFC4J_LOSS_DAY
	AND LKP_PIF_42GP_STAGE_PIF_SYMBOL_PIF_POLICY_NUMBER_PIF_MODULE_IPFC4J_LOSS_YEAR_IPFC4J_LOSS_MONTH_IPFC4J_LOSS_DAY_IPFC4J_LOSS_OCCURENCE.ipfcgp_loss_occurence = IPFC4J_LOSS_OCCURENCE

),
pifmstr_PIF_42GJ_stage AS (
	TRUNCATE TABLE pif_42gj_stage;
	INSERT INTO pif_42gj_stage
	(pif_symbol, pif_policy_number, pif_module, ipfc4j_rec_length_red, ipfc4j_action_code, ipfc4j_file_id, ipfc4j_segment_id, ipfc4j_segment_level_code, ipfc4j_segment_part_code, ipfc4j_sub_part_code, ipfc4j_insurance_line, ipfc4j_location_number, ipfc4j_sub_location_number, ipfc4j_risk_unit_group, ipfc4j_class_code_group, ipfc4j_class_code_member, ipfc4j_risk_unit, ipfc4j_risk_sequence, ipfc4j_risk_type_ind, ipfc4j_year_item_effective, ipfc4j_month_item_effective, ipfc4j_day_item_effective, ipfc4j_loss_year, ipfc4j_loss_month, ipfc4j_loss_day, ipfc4j_loss_occurence, ipfc4j_loss_claimant, ipfc4j_use_code, ipfc4j_sequence_use_code, ipfc4j_ai_seq_use_code, ipfc4j_year_process, ipfc4j_month_process, ipfc4j_day_process, ipfc4j_year_change_entry, ipfc4j_month_change_entry, ipfc4j_day_change_entry, ipfc4j_sequence_change_entry, ipfc4j_segment_status, ipfc4j_entry_operator, ipfc4j_use_code_data, ipfc4j_sort_name, ipfc4j_name_type_ind, ipfc4j_address_line_1, ipfc4j_addr_lin_2_pos_1, ipfc4j_addr_lin_2_pos_2_30, ipfc4j_address_line_3, ipfc4j_address_line_4, ipfc4j_id_number, ipfc4j_zip_basic, ipfc4j_zip_expanded, ipfc4j_phone_area, ipfc4j_phone_exchange, ipfc4j_phone_number, ipfc4j_phone_extension, ipfc4j_interest_item, ipfc4j_location_state, ipfc4j_offset_onset_ind, ipfc4j_pms_future_use_4j_1, ipfc4j_cust_spl_use_4j_1, ipfc4j_pms_future_use, ipfc4j_yr2000_cust_use, logical_flag, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
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
	LOGICAL_FLAG_OP AS LOGICAL_FLAG, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT_OP AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_pifmstr_PIF_42GJ_stage
),