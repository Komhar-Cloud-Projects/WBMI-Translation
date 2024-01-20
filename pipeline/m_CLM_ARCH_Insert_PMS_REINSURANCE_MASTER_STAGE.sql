WITH
SQ_pms_reinsurance_master_stage AS (
	SELECT
		reinsurance_master_stage_id,
		rcm_location_code,
		rcm_id,
		rcm_affiliate_code,
		rcm_reins_co_number,
		rcm_reins_type,
		rcm_company_name,
		rcm_address_part,
		rcm_city_st_part,
		rcm_zip_code,
		rcm_master_reins_co,
		rcm_fac_comm_rate,
		rcm_telephone_area_code,
		rcm_telephone_first_3,
		rcm_telephone_last_4,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM pms_reinsurance_master_stage
),
EXP_CLAIM_TAB_STAGE AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	reinsurance_master_stage_id,
	rcm_location_code,
	rcm_id,
	rcm_affiliate_code,
	rcm_reins_co_number,
	rcm_reins_type,
	rcm_company_name,
	rcm_address_part,
	rcm_city_st_part,
	rcm_zip_code,
	rcm_master_reins_co,
	rcm_fac_comm_rate,
	rcm_telephone_area_code,
	rcm_telephone_first_3,
	rcm_telephone_last_4,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_pms_reinsurance_master_stage
),
arch_pms_reinsurance_master_stage AS (
	INSERT INTO arch_pms_reinsurance_master_stage
	(reinsurance_master_stage_id, rcm_location_code, rcm_id, rcm_affiliate_code, rcm_reins_co_number, rcm_reins_type, rcm_company_name, rcm_address_part, rcm_city_st_part, rcm_zip_code, rcm_master_reins_co, rcm_fac_comm_rate, rcm_telephone_area_code, rcm_telephone_first_3, rcm_telephone_last_4, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	REINSURANCE_MASTER_STAGE_ID, 
	RCM_LOCATION_CODE, 
	RCM_ID, 
	RCM_AFFILIATE_CODE, 
	RCM_REINS_CO_NUMBER, 
	RCM_REINS_TYPE, 
	RCM_COMPANY_NAME, 
	RCM_ADDRESS_PART, 
	RCM_CITY_ST_PART, 
	RCM_ZIP_CODE, 
	RCM_MASTER_REINS_CO, 
	RCM_FAC_COMM_RATE, 
	RCM_TELEPHONE_AREA_CODE, 
	RCM_TELEPHONE_FIRST_3, 
	RCM_TELEPHONE_LAST_4, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_CLAIM_TAB_STAGE
),