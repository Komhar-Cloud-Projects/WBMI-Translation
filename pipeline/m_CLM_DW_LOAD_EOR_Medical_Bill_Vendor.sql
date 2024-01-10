WITH
SQ_sup_eor_vendor_stage AS (
	SELECT	case rtrim(vendor_code) when ''  then 'N/A'   else rtrim(vendor_code) end	AS vendor_code 
	,		case rtrim(vendor_name) when ''  then 'N/A'    else rtrim(vendor_name) end	AS vendor_name 
	,		case rtrim(vendor_addr) when ''  then 'N/A'   else rtrim(vendor_addr) end	AS vendor_addr 
	,		case rtrim(vendor_city) when ''  then 'N/A'    else rtrim(vendor_city) end	AS vendor_city 
	,		case rtrim(vendor_state) when ''  then 'N/A'    else rtrim(vendor_state) end	AS vendor_state
	,		case rtrim(vendor_zip) when ''  then 'N/A'    else rtrim(vendor_zip) end		AS vendor_zip
	,		case rtrim(vendor_ph) when ''  then 'N/A'    else rtrim(vendor_ph) end		AS vendor_ph 
	,		case rtrim(vendor_fax) when ''  then 'N/A'   else rtrim(vendor_fax) end		AS vendor_fax
	FROM	@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_eor_vendor_stage
),
EXP_PROVIDER_TYPE AS (
	SELECT
	vendor_code,
	vendor_name,
	vendor_addr,
	vendor_city,
	vendor_state,
	vendor_zip,
	vendor_ph,
	vendor_fax,
	1 AS current_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_sup_eor_vendor_stage
),
LKP_medical_bill_vendor AS (
	SELECT
	NewLookupRow,
	med_bill_vendor_ak_id,
	in_vendor_code,
	vendor_code,
	in_vendor_name,
	vendor_name,
	in_vendor_addr,
	vendor_addr,
	in_vendor_city,
	vendor_city,
	in_vendor_state,
	vendor_state,
	in_vendor_zip,
	vendor_zip,
	in_vendor_ph,
	vendor_ph,
	in_vendor_fax,
	vendor_fax,
	current_snpsht_flag,
	audit_id
	FROM (
		SELECT	med_bill_vendor_ak_id	AS med_bill_vendor_ak_id
		,		rtrim(vendor_code)		AS vendor_code
		,		rtrim(vendor_name)		AS vendor_name
		,		rtrim(vendor_addr)		AS vendor_addr
		,		rtrim(vendor_city)		AS vendor_city
		,		rtrim(vendor_state)		AS vendor_state
		,		rtrim(vendor_zip)		AS vendor_zip
		,		rtrim(vendor_ph)		AS vendor_ph
		,		rtrim(vendor_fax)		AS vendor_fax
		FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_vendor
		WHERE	crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_code ORDER BY NewLookupRow) = 1
),
FIL_NEW_CHANGED_ROWS AS (
	SELECT
	NewLookupRow, 
	current_snpsht_flag, 
	audit_id, 
	med_bill_vendor_ak_id, 
	vendor_code, 
	vendor_name, 
	vendor_addr, 
	vendor_city, 
	vendor_state, 
	vendor_zip, 
	vendor_ph, 
	vendor_fax
	FROM LKP_medical_bill_vendor
	WHERE NewLookupRow = 1 OR 
NewLookupRow = 2
),
EXP_AUDIT_FIELDS AS (
	SELECT
	current_snpsht_flag,
	audit_id,
	NewLookupRow,
	-- *INF*: iif(NewLookupRow=1,
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(NewLookupRow = 1, to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS CREATE_MOD_DATE,
	med_bill_vendor_ak_id,
	vendor_code,
	vendor_name,
	vendor_addr,
	vendor_city,
	vendor_state,
	vendor_zip,
	vendor_ph,
	vendor_fax
	FROM FIL_NEW_CHANGED_ROWS
),
medical_bill_vendor AS (
	INSERT INTO medical_bill_vendor
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, med_bill_vendor_ak_id, vendor_code, vendor_name, vendor_addr, vendor_city, vendor_state, vendor_zip, vendor_ph, vendor_fax)
	SELECT 
	current_snpsht_flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATE_MOD_DATE AS CREATED_DATE, 
	CREATE_MOD_DATE AS MODIFIED_DATE, 
	MED_BILL_VENDOR_AK_ID, 
	VENDOR_CODE, 
	VENDOR_NAME, 
	VENDOR_ADDR, 
	VENDOR_CITY, 
	VENDOR_STATE, 
	VENDOR_ZIP, 
	VENDOR_PH, 
	VENDOR_FAX
	FROM EXP_AUDIT_FIELDS
),
SQ_medical_bill_vendor AS (
	SELECT	med_bill_vendor_id
	,		eff_from_date
	,		eff_to_date 
	,		med_bill_vendor_ak_id
	FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_vendor MBV
	WHERE	source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND		EXISTS
			(select 1
			FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_vendor MBV2
			WHERE	source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			AND		crrnt_snpsht_flag = 1
			AND		MBV2.med_bill_vendor_ak_id = MBV.med_bill_vendor_ak_id 
			GROUP	BY	MBV2.med_bill_vendor_ak_id
			HAVING	count(*) > 1
	)
	ORDER	BY med_bill_vendor_ak_id
	,		eff_from_date  desc
),
EXP_Lag_eff_from_date11 AS (
	SELECT
	med_bill_vendor_id,
	med_bill_vendor_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	med_bill_vendor_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		med_bill_vendor_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	med_bill_vendor_ak_id AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_medical_bill_vendor
),
FIL_First_Row_in_AK_Group AS (
	SELECT
	med_bill_vendor_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date11
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_MED_Vendor AS (
	SELECT
	med_bill_vendor_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_First_Row_in_AK_Group
),
medical_bill_vendor1 AS (
	MERGE INTO medical_bill_vendor AS T
	USING UPD_MED_Vendor AS S
	ON T.med_bill_vendor_id = S.med_bill_vendor_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),