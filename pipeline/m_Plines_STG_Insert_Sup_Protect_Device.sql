WITH
SQ_sup_protect_device AS (
	SELECT
		state_type_code,
		code,
		ho_type_code,
		eff_date,
		exp_date,
		pmsc_code,
		descript,
		modified_date,
		modified_user_id,
		discount_descript,
		sort_order,
		device_group
	FROM sup_protect_device
),
EXPTRANS AS (
	SELECT
	state_type_code,
	code,
	ho_type_code,
	eff_date,
	exp_date,
	pmsc_code,
	descript,
	modified_date,
	modified_user_id,
	discount_descript,
	sort_order,
	device_group,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_sup_protect_device
),
sup_protect_device_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_protect_device_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_protect_device_stage
	(state_type_code, code, ho_type_code, eff_date, exp_date, pmsc_code, descript, modified_date, modified_user_id, discount_descript, sort_order, device_group, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	STATE_TYPE_CODE, 
	CODE, 
	HO_TYPE_CODE, 
	EFF_DATE, 
	EXP_DATE, 
	PMSC_CODE, 
	DESCRIPT, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	DISCOUNT_DESCRIPT, 
	SORT_ORDER, 
	DEVICE_GROUP, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXPTRANS
),