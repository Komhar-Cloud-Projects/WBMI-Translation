WITH
med_provider_stage AS (
	SELECT 
	rtrim(med_provider_stage.med_bill_id), 
	case rtrim(med_provider_stage.bus_name) when '' then null else rtrim(med_provider_stage.bus_name) end, 
	case rtrim(med_provider_stage.last_name)  when '' then null else rtrim(med_provider_stage.last_name)  end, 
	case rtrim(med_provider_stage.first_name) when '' then null else rtrim(med_provider_stage.first_name)  end, 
	case rtrim(med_provider_stage.prfx) when '' then null else rtrim(med_provider_stage.prfx)  end, 
	case rtrim(med_provider_stage.sfx) when '' then null else rtrim(med_provider_stage.sfx)  end, 
	case rtrim(med_provider_stage.title) when '' then null else rtrim(med_provider_stage.title) end, 
	case rtrim(med_provider_stage.spty_code) when '' then null else rtrim(med_provider_stage.spty_code) end, 
	case rtrim(med_provider_stage.addr) when '' then null else rtrim(med_provider_stage.addr) end, 
	case rtrim(med_provider_stage.city) when '' then null else rtrim(med_provider_stage.city) end, 
	case rtrim(med_provider_stage.state) when '' then null else rtrim(med_provider_stage.state) end, 
	case rtrim(med_provider_stage.zip) when '' then null else rtrim(med_provider_stage.zip) end, 
	case rtrim(med_provider_stage.tax_id) when '' then null else rtrim(med_provider_stage.tax_id)  end
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.med_provider_stage med_provider_stage
	WHERE 1=1 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_PROVIDER_TYPE AS (
	SELECT
	med_bill_id,
	bus_name,
	last_name,
	-- *INF*: iif (bus_name = 'N/A','INDIV','GROUP')
	IFF(bus_name = 'N/A', 'INDIV', 'GROUP') AS provider_type_code,
	first_name,
	'N/A' AS mid_name,
	prfx,
	sfx,
	title,
	spty_code,
	addr,
	city,
	state,
	zip,
	tax_id,
	1 AS current_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM med_provider_stage
),
LKP_MED_BILL_KEY AS (
	SELECT
	med_bill_ak_id,
	med_bill_key,
	TCH_BILL_NBR
	FROM (
		SELECT 
		medical_bill.med_bill_ak_id as med_bill_ak_id, 
		RTRIM(medical_bill.med_bill_key) as med_bill_key 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill medical_bill
		WHERE
		medical_bill.CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_key ORDER BY med_bill_ak_id) = 1
),
EXP_default_values AS (
	SELECT
	EXP_PROVIDER_TYPE.bus_name,
	EXP_PROVIDER_TYPE.last_name,
	EXP_PROVIDER_TYPE.provider_type_code,
	EXP_PROVIDER_TYPE.first_name,
	EXP_PROVIDER_TYPE.mid_name,
	EXP_PROVIDER_TYPE.prfx,
	EXP_PROVIDER_TYPE.sfx,
	EXP_PROVIDER_TYPE.title,
	EXP_PROVIDER_TYPE.spty_code,
	EXP_PROVIDER_TYPE.addr,
	EXP_PROVIDER_TYPE.city,
	EXP_PROVIDER_TYPE.state,
	EXP_PROVIDER_TYPE.zip,
	EXP_PROVIDER_TYPE.tax_id,
	EXP_PROVIDER_TYPE.current_snpsht_flag,
	EXP_PROVIDER_TYPE.audit_id,
	LKP_MED_BILL_KEY.med_bill_ak_id
	FROM EXP_PROVIDER_TYPE
	LEFT JOIN LKP_MED_BILL_KEY
	ON LKP_MED_BILL_KEY.med_bill_key = EXP_PROVIDER_TYPE.med_bill_id
),
LKP_medical_provider AS (
	SELECT
	med_provider_ak_id,
	med_bill_ak_id,
	provider_type_code,
	bus_name,
	last_name,
	first_name,
	mid_name,
	prfx,
	sfx,
	title,
	specialty_code,
	addr,
	city,
	state,
	zip,
	tax_id,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	in_med_bill_ak_id
	FROM (
		SELECT 
		medical_provider.med_provider_ak_id as med_provider_ak_id, 
		medical_provider.med_bill_ak_id as med_bill_ak_id 
		rtrim(medical_provider.provider_type_code) as provider_type_code, 
		rtrim(medical_provider.bus_name) as bus_name, 
		rtrim(medical_provider.last_name) as last_name, 
		rtrim(medical_provider.first_name) as first_name, 
		rtrim(medical_provider.mid_name) as mid_name, 
		rtrim(medical_provider.prfx) as prfx, 
		rtrim(medical_provider.sfx) as sfx, 
		rtrim(medical_provider.title) as title, 
		rtrim(medical_provider.specialty_code) as specialty_code, 
		rtrim(medical_provider.addr) as addr, 
		rtrim(medical_provider.city) as city, 
		rtrim(medical_provider.state) as state, 
		rtrim(medical_provider.zip) as zip, 
		rtrim(medical_provider.tax_id) as tax_id
		FROM 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_provider medical_provider
		WHERE
		medical_provider.CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_ak_id ORDER BY med_provider_ak_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_medical_provider.med_provider_ak_id AS lkp_med_provider_ak_id,
	LKP_medical_provider.med_bill_ak_id AS lkp_med_bill_ak_id,
	LKP_medical_provider.provider_type_code AS lkp_provider_type_code,
	LKP_medical_provider.bus_name AS lkp_bus_name,
	LKP_medical_provider.last_name AS lkp_last_name,
	LKP_medical_provider.first_name AS lkp_first_name,
	LKP_medical_provider.mid_name AS lkp_mid_name,
	LKP_medical_provider.prfx AS lkp_prfx,
	LKP_medical_provider.sfx AS lkp_sfx,
	LKP_medical_provider.title AS lkp_title,
	LKP_medical_provider.specialty_code AS lkp_specialty_code,
	LKP_medical_provider.addr AS lkp_addr,
	LKP_medical_provider.city AS lkp_city,
	LKP_medical_provider.state AS lkp_state,
	LKP_medical_provider.zip AS lkp_zip,
	LKP_medical_provider.tax_id AS lkp_tax_id,
	EXP_default_values.bus_name,
	EXP_default_values.last_name,
	EXP_default_values.provider_type_code,
	EXP_default_values.first_name,
	EXP_default_values.mid_name,
	EXP_default_values.prfx,
	EXP_default_values.sfx,
	EXP_default_values.title,
	EXP_default_values.spty_code,
	EXP_default_values.addr,
	EXP_default_values.city,
	EXP_default_values.state,
	EXP_default_values.zip,
	EXP_default_values.tax_id,
	EXP_default_values.current_snpsht_flag,
	EXP_default_values.audit_id,
	EXP_default_values.med_bill_ak_id,
	-- *INF*: IIF(ISnull(lkp_med_provider_ak_id),'NEW',
	-- iif (
	-- lkp_provider_type_code <> provider_type_code OR
	-- lkp_bus_name <> bus_name OR
	-- lkp_last_name <> last_name OR
	-- lkp_first_name <> first_name OR
	-- lkp_mid_name <> mid_name OR
	-- lkp_prfx <> prfx OR
	-- lkp_sfx <> sfx OR
	-- lkp_title <> title OR
	-- lkp_specialty_code <> spty_code OR
	-- lkp_addr <> addr OR
	-- lkp_city <> city OR
	-- state <> lkp_state OR
	-- lkp_zip <> zip OR
	-- lkp_tax_id <> tax_id 
	-- , 'UPDATE','NOCHANGE'))
	-- 
	IFF(lkp_med_provider_ak_id IS NULL, 'NEW', IFF(lkp_provider_type_code <> provider_type_code OR lkp_bus_name <> bus_name OR lkp_last_name <> last_name OR lkp_first_name <> first_name OR lkp_mid_name <> mid_name OR lkp_prfx <> prfx OR lkp_sfx <> sfx OR lkp_title <> title OR lkp_specialty_code <> spty_code OR lkp_addr <> addr OR lkp_city <> city OR state <> lkp_state OR lkp_zip <> zip OR lkp_tax_id <> tax_id, 'UPDATE', 'NOCHANGE')) AS v_change_flag,
	v_change_flag AS Change_Flag
	FROM EXP_default_values
	LEFT JOIN LKP_medical_provider
	ON LKP_medical_provider.med_bill_ak_id = EXP_default_values.med_bill_ak_id
),
Filter_Insert AS (
	SELECT
	lkp_med_provider_ak_id, 
	bus_name, 
	last_name, 
	provider_type_code, 
	first_name, 
	mid_name, 
	prfx, 
	sfx, 
	title, 
	spty_code, 
	addr, 
	city, 
	state, 
	zip, 
	tax_id, 
	current_snpsht_flag, 
	audit_id, 
	med_bill_ak_id, 
	Change_Flag
	FROM EXP_detect_changes
	WHERE Change_Flag = 'NEW' or Change_Flag = 'UPDATE'
),
SEQ_medical_provider_ak_id AS (
	CREATE SEQUENCE SEQ_medical_provider_ak_id
	START = 2000000
	INCREMENT = 1;
),
EXP_AUDIT_FIELDS_INSERT AS (
	SELECT
	-- *INF*: IIF(Change_Flag = 'NEW',NEXTVAL,lkp_med_provider_ak_id)
	IFF(Change_Flag = 'NEW', NEXTVAL, lkp_med_provider_ak_id) AS med_provider_ak_id,
	med_bill_ak_id,
	provider_type_code,
	bus_name,
	-- *INF*: iif (v_full_name ='','N/A',v_full_name)
	IFF(v_full_name = '', 'N/A', v_full_name) AS full_name,
	last_name,
	first_name,
	mid_name,
	-- *INF*: rtrim(REPLACESTR(TRUE,first_name  || ' ' ||  mid_name || ' ' || last_name,'N/A',''))
	rtrim(REPLACESTR(TRUE, first_name || ' ' || mid_name || ' ' || last_name, 'N/A', '')) AS v_full_name,
	prfx,
	sfx,
	title,
	spty_code,
	addr,
	city,
	state,
	zip,
	tax_id,
	current_snpsht_flag,
	audit_id,
	-- *INF*: iif(Change_Flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(Change_Flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS CREATE_MOD_DATE,
	SEQ_medical_provider_ak_id.NEXTVAL,
	lkp_med_provider_ak_id,
	Change_Flag
	FROM Filter_Insert
),
medical_provider_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_provider
	(med_provider_ak_id, med_bill_ak_id, provider_type_code, bus_name, full_name, last_name, first_name, mid_name, prfx, sfx, title, specialty_code, addr, city, state, zip, tax_id, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	MED_PROVIDER_AK_ID, 
	MED_BILL_AK_ID, 
	PROVIDER_TYPE_CODE, 
	BUS_NAME, 
	FULL_NAME, 
	LAST_NAME, 
	FIRST_NAME, 
	MID_NAME, 
	PRFX, 
	SFX, 
	TITLE, 
	spty_code AS SPECIALTY_CODE, 
	ADDR, 
	CITY, 
	STATE, 
	ZIP, 
	TAX_ID, 
	current_snpsht_flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATE_MOD_DATE AS CREATED_DATE, 
	CREATE_MOD_DATE AS MODIFIED_DATE
	FROM EXP_AUDIT_FIELDS_INSERT
),
medical_provider AS (
	SELECT 
	medical_provider.med_provider_id, 
	medical_provider.med_provider_ak_id, 
	medical_provider.eff_from_date, 
	medical_provider.eff_to_date 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_provider
	WHERE
	Medical_provider.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and exists
	(
	select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_provider medical_provider2
	where source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  and crrnt_snpsht_flag = 1 and
	medical_provider2.med_provider_ak_id = medical_provider2.med_provider_ak_id 
	group by medical_provider2.med_provider_ak_id having count(*) > 1
	)
	order by medical_provider.med_provider_ak_id, medical_provider.eff_from_date  desc
),
EXP_Lag_eff_from_date11 AS (
	SELECT
	med_provider_id,
	med_provider_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	med_provider_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		med_provider_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	med_provider_ak_id AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM medical_provider
),
FIL_First_Row_in_AK_Group AS (
	SELECT
	med_provider_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date11
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_MED_PROVIDER AS (
	SELECT
	med_provider_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_First_Row_in_AK_Group
),
medical_provider_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_provider AS T
	USING UPD_MED_PROVIDER AS S
	ON T.med_provider_id = S.med_provider_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),