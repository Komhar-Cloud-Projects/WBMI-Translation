WITH
SQ_offering AS (
	SELECT
		offering_id,
		bus_dvsn_prdct_id,
		strtgc_bus_unit_dvsn_prog_id,
		offering_descript,
		pol_pco,
		prog_code,
		created_user_id,
		created_date,
		modified_user_id,
		modified_date
	FROM offering
),
EXP_OFFERING_HCTR AS (
	SELECT
	offering_id,
	bus_dvsn_prdct_id,
	strtgc_bus_unit_dvsn_prog_id,
	offering_descript,
	pol_pco,
	prog_code,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	'' AS RECORD_COUNT_OP
	FROM SQ_offering
),
offering_hctr_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.offering_hctr_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.offering_hctr_stage
	(offering_id, bus_dvsn_prdct_id, strtgc_bus_unit_dvsn_prog_id, offering_descript, pol_pco, prog_code, created_user_id, created_date, modified_user_id, modified_date, extract_date, source_system_id)
	SELECT 
	OFFERING_ID, 
	BUS_DVSN_PRDCT_ID, 
	STRTGC_BUS_UNIT_DVSN_PROG_ID, 
	OFFERING_DESCRIPT, 
	POL_PCO, 
	PROG_CODE, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_OFFERING_HCTR
),