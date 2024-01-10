WITH
SQ_master_1099_list_stage AS (
	SELECT
		master_1099_list_stage_id,
		tax_id,
		reportable_ind,
		tax_levy_ind,
		irs_name,
		address_line_1,
		address_line_2,
		city,
		state_code,
		zip_code,
		country_code,
		modified_ts,
		modified_user_id,
		notes,
		search_tax_id,
		tax_id_type,
		phone,
		last_modified_dt,
		irs_1099_type,
		vendor_type_cd,
		extract_date,
		as_of_date,
		record_count,
		source_system_id,
		is_valid
	FROM master_1099_list_stage
	WHERE master_1099_list_stage.is_valid in ('Y','N')
),
EXP_Validate_Data AS (
	SELECT
	master_1099_list_stage_id AS IN_master_1099_list_stage_id,
	tax_id AS IN_tax_id,
	-- *INF*: LTRIM(RTRIM(IN_tax_id))
	LTRIM(RTRIM(IN_tax_id)) AS v_tax_id_1,
	-- *INF*: iif(isnull(IN_tax_id)  OR length(IN_tax_id)= 0 OR IS_SPACES(IN_tax_id), '000000000',ltrim(rtrim(IN_tax_id)) )
	IFF(IN_tax_id IS NULL OR length(IN_tax_id) = 0 OR IS_SPACES(IN_tax_id), '000000000', ltrim(rtrim(IN_tax_id))) AS v_tax_id,
	v_tax_id AS out_tax_id,
	reportable_ind AS IN_reportable_ind,
	-- *INF*: iif(isnull(IN_reportable_ind)  OR length(IN_reportable_ind)= 0 OR IS_SPACES(IN_reportable_ind), 'N/A',ltrim(rtrim(IN_reportable_ind)) )
	IFF(IN_reportable_ind IS NULL OR length(IN_reportable_ind) = 0 OR IS_SPACES(IN_reportable_ind), 'N/A', ltrim(rtrim(IN_reportable_ind))) AS v_reportable_ind,
	v_reportable_ind AS out_reportable_id,
	tax_levy_ind AS IN_tax_levy_ind,
	-- *INF*: iif(isnull(IN_tax_levy_ind)  OR length(IN_tax_levy_ind)= 0 OR IS_SPACES(IN_tax_levy_ind), 'N/A',ltrim(rtrim(IN_tax_levy_ind)) )
	IFF(IN_tax_levy_ind IS NULL OR length(IN_tax_levy_ind) = 0 OR IS_SPACES(IN_tax_levy_ind), 'N/A', ltrim(rtrim(IN_tax_levy_ind))) AS v_tax_levy_ind,
	v_tax_levy_ind AS out_v_tax_levy_ind,
	irs_name AS IN_irs_name,
	-- *INF*: iif(isnull(IN_irs_name)  OR length(IN_irs_name)= 0 OR IS_SPACES(IN_irs_name), 'N/A',ltrim(rtrim(IN_irs_name)) )
	IFF(IN_irs_name IS NULL OR length(IN_irs_name) = 0 OR IS_SPACES(IN_irs_name), 'N/A', ltrim(rtrim(IN_irs_name))) AS v_irs_name,
	v_irs_name AS out_irs_name,
	address_line_1 AS IN_address_line_1,
	-- *INF*: iif(isnull(IN_address_line_1)  OR length(IN_address_line_1)= 0 OR IS_SPACES(IN_address_line_1), 'N/A',ltrim(rtrim(IN_address_line_1)) )
	IFF(IN_address_line_1 IS NULL OR length(IN_address_line_1) = 0 OR IS_SPACES(IN_address_line_1), 'N/A', ltrim(rtrim(IN_address_line_1))) AS v_address_line_1,
	v_address_line_1 AS out_address_line_1,
	address_line_2 AS IN_address_line_2,
	-- *INF*: iif(isnull(IN_address_line_2)  OR length(IN_address_line_2)= 0 OR IS_SPACES(IN_address_line_2), 'N/A',ltrim(rtrim(IN_address_line_2)) )
	IFF(IN_address_line_2 IS NULL OR length(IN_address_line_2) = 0 OR IS_SPACES(IN_address_line_2), 'N/A', ltrim(rtrim(IN_address_line_2))) AS v_address_line_2,
	v_address_line_2 AS out_address_line_2,
	city AS IN_city,
	-- *INF*: iif(isnull(IN_city)  OR length(IN_city)= 0 OR IS_SPACES(IN_city), 'N/A',ltrim(rtrim(IN_city)) )
	IFF(IN_city IS NULL OR length(IN_city) = 0 OR IS_SPACES(IN_city), 'N/A', ltrim(rtrim(IN_city))) AS v_city,
	v_city AS out_city,
	state_code AS IN_state_code,
	-- *INF*: iif(isnull(IN_state_code)  OR length(IN_state_code)= 0 OR IS_SPACES(IN_state_code), 'N/A',ltrim(rtrim(IN_state_code)) )
	IFF(IN_state_code IS NULL OR length(IN_state_code) = 0 OR IS_SPACES(IN_state_code), 'N/A', ltrim(rtrim(IN_state_code))) AS v_state_code,
	v_state_code AS out_state_code,
	zip_code AS IN_zip_code,
	-- *INF*: iif(isnull(IN_zip_code)  OR length(IN_zip_code)= 0 OR IS_SPACES(IN_zip_code), 'N/A',ltrim(rtrim(IN_zip_code)) )
	IFF(IN_zip_code IS NULL OR length(IN_zip_code) = 0 OR IS_SPACES(IN_zip_code), 'N/A', ltrim(rtrim(IN_zip_code))) AS v_zip_code,
	v_zip_code AS out_zip_code,
	country_code AS IN_country_code,
	-- *INF*: iif(isnull(IN_country_code)  OR length(IN_country_code)= 0 OR IS_SPACES(IN_country_code), 'N/A',ltrim(rtrim(IN_country_code)) )
	IFF(IN_country_code IS NULL OR length(IN_country_code) = 0 OR IS_SPACES(IN_country_code), 'N/A', ltrim(rtrim(IN_country_code))) AS v_country_code,
	v_country_code AS out_country_code,
	modified_ts AS IN_modified_ts,
	modified_user_id AS IN_modified_user_id,
	notes AS IN_notes,
	search_tax_id AS IN_search_tax_id,
	tax_id_type AS in_tax_id_type,
	-- *INF*: iif(isnull(in_tax_id_type)  OR length(ltrim(ltrim(in_tax_id_type)))= 0 OR IS_SPACES(in_tax_id_type), '',ltrim(rtrim(in_tax_id_type)) )
	IFF(in_tax_id_type IS NULL OR length(ltrim(ltrim(in_tax_id_type))) = 0 OR IS_SPACES(in_tax_id_type), '', ltrim(rtrim(in_tax_id_type))) AS tax_id_type,
	phone AS IN_phone,
	-- *INF*: iif(isnull(IN_phone)  OR length(IN_phone)= 0 OR IS_SPACES(IN_phone), 'N/A',ltrim(rtrim(IN_phone)) )
	IFF(IN_phone IS NULL OR length(IN_phone) = 0 OR IS_SPACES(IN_phone), 'N/A', ltrim(rtrim(IN_phone))) AS v_phone,
	v_phone AS out_phone,
	-- *INF*: iif(SUBSTR(v_tax_id_1,3,1)='-' and INSTR(v_tax_id_1,' ')=0 and LENGTH(v_tax_id_1)=10,0,IIF(LENGTH(v_tax_id_1)=11,0,
	-- -1))
	-- 
	-- 
	-- 
	IFF(SUBSTR(v_tax_id_1, 3, 1) = '-' AND INSTR(v_tax_id_1, ' ') = 0 AND LENGTH(v_tax_id_1) = 10, 0, IFF(LENGTH(v_tax_id_1) = 11, 0, - 1)) AS v_err_flag,
	v_err_flag AS out_err_flag,
	last_modified_dt AS IN_last_modified_dt,
	irs_1099_type AS in_irs_1099_type,
	-- *INF*: iif(isnull(in_irs_1099_type)  OR length(in_irs_1099_type)= 0 OR IS_SPACES(in_irs_1099_type), 'M',ltrim(rtrim(in_irs_1099_type)) )
	IFF(in_irs_1099_type IS NULL OR length(in_irs_1099_type) = 0 OR IS_SPACES(in_irs_1099_type), 'M', ltrim(rtrim(in_irs_1099_type))) AS v_irs_1099_type,
	v_irs_1099_type AS irs_1099_type,
	vendor_type_cd AS in_vendor_type_cd,
	-- *INF*: iif(isnull(in_vendor_type_cd)  OR length(in_vendor_type_cd)= 0 OR IS_SPACES(in_vendor_type_cd), 'N/A',ltrim(rtrim(in_vendor_type_cd)) )
	IFF(in_vendor_type_cd IS NULL OR length(in_vendor_type_cd) = 0 OR IS_SPACES(in_vendor_type_cd), 'N/A', ltrim(rtrim(in_vendor_type_cd))) AS v_vendor_type_cd,
	v_vendor_type_cd AS vendor_type_cd,
	extract_date AS IN_extract_date,
	as_of_date AS IN_as_of_date,
	record_count AS IN_record_count,
	source_system_id AS IN_source_system_id,
	-- *INF*: iif(isnull(IN_modified_ts),TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),IN_modified_ts)
	-- 
	-- 
	IFF(IN_modified_ts IS NULL, TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), IN_modified_ts) AS modified_ts,
	-- *INF*: iif(isnull(IN_modified_user_id)  OR length(IN_modified_user_id)= 0 OR IS_SPACES(IN_modified_user_id), 'N/A',ltrim(rtrim(IN_modified_user_id)) )
	IFF(IN_modified_user_id IS NULL OR length(IN_modified_user_id) = 0 OR IS_SPACES(IN_modified_user_id), 'N/A', ltrim(rtrim(IN_modified_user_id))) AS modified_user_id,
	-- *INF*: iif(isnull(IN_notes)  OR length(IN_notes)= 0 OR IS_SPACES(IN_notes), 'N/A',ltrim(rtrim(IN_notes)) )
	IFF(IN_notes IS NULL OR length(IN_notes) = 0 OR IS_SPACES(IN_notes), 'N/A', ltrim(rtrim(IN_notes))) AS notes,
	-- *INF*: iif(isnull(IN_search_tax_id)  OR length(IN_search_tax_id)= 0 OR IS_SPACES(IN_search_tax_id), 'N/A',ltrim(rtrim(IN_search_tax_id)) )
	IFF(IN_search_tax_id IS NULL OR length(IN_search_tax_id) = 0 OR IS_SPACES(IN_search_tax_id), 'N/A', ltrim(rtrim(IN_search_tax_id))) AS v_search_tax_id,
	v_search_tax_id AS search_tax_id,
	is_valid AS in_is_valid,
	-- *INF*: IIF(isnull(in_is_valid) or length(rtrim(ltrim(in_is_valid)))=0 or IS_SPACES(in_is_valid), 'N',in_is_valid) 
	IFF(in_is_valid IS NULL OR length(rtrim(ltrim(in_is_valid))) = 0 OR IS_SPACES(in_is_valid), 'N', in_is_valid) AS is_valid
	FROM SQ_master_1099_list_stage
),
FIL_keep_conditions AS (
	SELECT
	out_tax_id AS tax_id, 
	out_reportable_id AS reportable_id, 
	out_v_tax_levy_ind AS tax_levy_ind, 
	out_irs_name AS irs_name, 
	out_address_line_1 AS address_line_1, 
	out_address_line_2 AS address_line_2, 
	out_city AS city, 
	out_state_code AS state_code, 
	out_zip_code AS zip_code, 
	out_country_code AS country_code, 
	tax_id_type, 
	out_phone AS phone, 
	out_err_flag AS err_flag, 
	irs_1099_type, 
	vendor_type_cd, 
	search_tax_id, 
	is_valid, 
	IN_modified_ts AS modified_ts
	FROM EXP_Validate_Data
	WHERE err_flag=0 and is_valid !='' and tax_id_type!='' and search_tax_id !='N/A'
),
LKP_claim_master_1099_list AS (
	SELECT
	claim_master_1099_list_id,
	claim_master_1099_list_ak_id,
	tax_id,
	irs_tax_id,
	tax_id_type,
	reportable_ind,
	tax_levy_ind,
	irs_name,
	address_line_1,
	address_line_2,
	city,
	state_code,
	zip_code,
	country_code,
	phone,
	irs_1099_type,
	vendor_type_code,
	err_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_system_id,
	created_date,
	modified_date,
	in_tax_id_type
	FROM (
		SELECT 
			claim_master_1099_list_id,
			claim_master_1099_list_ak_id,
			tax_id,
			irs_tax_id,
			tax_id_type,
			reportable_ind,
			tax_levy_ind,
			irs_name,
			address_line_1,
			address_line_2,
			city,
			state_code,
			zip_code,
			country_code,
			phone,
			irs_1099_type,
			vendor_type_code,
			err_flag,
			crrnt_snpsht_flag,
			audit_id,
			eff_from_date,
			eff_to_date,
			source_system_id,
			created_date,
			modified_date,
			in_tax_id_type
		FROM claim_master_1099_list
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY irs_tax_id,tax_id_type ORDER BY claim_master_1099_list_id DESC) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_claim_master_1099_list.claim_master_1099_list_id AS LKP_claim_master_1099_list_id,
	LKP_claim_master_1099_list.claim_master_1099_list_ak_id AS LKP_claim_master_1099_list_ak_id,
	LKP_claim_master_1099_list.tax_id AS LKP_tax_id,
	LKP_claim_master_1099_list.irs_tax_id AS LKP_irs_tax_id,
	LKP_claim_master_1099_list.tax_id_type AS LKP_tax_id_type,
	LKP_claim_master_1099_list.reportable_ind AS LKP_reportable_ind,
	LKP_claim_master_1099_list.tax_levy_ind AS LKP_tax_levy_ind,
	LKP_claim_master_1099_list.irs_name AS LKP_irs_name,
	LKP_claim_master_1099_list.address_line_1 AS LKP_address_line_1,
	LKP_claim_master_1099_list.address_line_2 AS LKP_address_line_2,
	LKP_claim_master_1099_list.city AS LKP_city,
	LKP_claim_master_1099_list.state_code AS LKP_state_code,
	LKP_claim_master_1099_list.zip_code AS LKP_zip_code,
	LKP_claim_master_1099_list.country_code AS LKP_country_code,
	LKP_claim_master_1099_list.phone AS LKP_phone,
	LKP_claim_master_1099_list.irs_1099_type AS LKP_irs_1099_type1,
	LKP_claim_master_1099_list.vendor_type_code AS LKP_vendor_type_code,
	LKP_claim_master_1099_list.err_flag AS LKP_err_flag,
	LKP_claim_master_1099_list.crrnt_snpsht_flag AS LKP_crrnt_snpsht_flag1,
	-- *INF*: DECODE(true,
	-- LKP_crrnt_snpsht_flag1= 'T', 1,
	-- LKP_crrnt_snpsht_flag1='1',1,
	-- LKP_crrnt_snpsht_flag1='Y',1,
	-- 0)
	-- 
	-- -- bit types seem to be randomly converted to either integers or strings in informatica so allowances have to be made when trying to do comparisons.
	DECODE(true,
		LKP_crrnt_snpsht_flag1 = 'T', 1,
		LKP_crrnt_snpsht_flag1 = '1', 1,
		LKP_crrnt_snpsht_flag1 = 'Y', 1,
		0) AS v_LKP_crrnt_snpsht_flag1,
	LKP_claim_master_1099_list.audit_id AS LKP_audit_id1,
	LKP_claim_master_1099_list.eff_from_date AS LKP_eff_from_date1,
	LKP_claim_master_1099_list.eff_to_date AS LKP_eff_to_date1,
	LKP_claim_master_1099_list.source_system_id AS LKP_source_system_id1,
	LKP_claim_master_1099_list.created_date AS LKP_created_date1,
	LKP_claim_master_1099_list.modified_date AS LKP_modified_date1,
	FIL_keep_conditions.tax_id AS out_tax_id,
	FIL_keep_conditions.reportable_id AS out_reportable_id,
	FIL_keep_conditions.tax_levy_ind AS out_v_tax_levy_ind,
	FIL_keep_conditions.irs_name AS out_irs_name,
	FIL_keep_conditions.address_line_1 AS out_address_line_1,
	FIL_keep_conditions.address_line_2 AS out_address_line_2,
	FIL_keep_conditions.city AS out_city,
	FIL_keep_conditions.state_code AS out_state_code,
	FIL_keep_conditions.zip_code AS out_zip_code,
	FIL_keep_conditions.country_code AS out_country_code,
	FIL_keep_conditions.tax_id_type AS in_tax_id_type,
	FIL_keep_conditions.phone AS out_phone,
	FIL_keep_conditions.err_flag AS out_err_flag,
	FIL_keep_conditions.irs_1099_type,
	FIL_keep_conditions.vendor_type_cd,
	FIL_keep_conditions.is_valid,
	-- *INF*: IIF(is_valid='Y',1,0)
	IFF(is_valid = 'Y', 1, 0) AS v_crrnt_snpsht_flag,
	v_crrnt_snpsht_flag AS out_crrnt_snpsht_flag,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(LKP_claim_master_1099_list_id),'NEW',
	-- rtrim(ltrim(LKP_tax_id)) <> rtrim(ltrim(out_tax_id)),'UPDATE',
	-- rtrim(ltrim(LKP_tax_id_type)) <> rtrim(ltrim(in_tax_id_type)),'UPDATE',
	-- rtrim(ltrim(LKP_reportable_ind)) <> rtrim(ltrim(out_reportable_id)),'UPDATE',
	-- rtrim(ltrim(LKP_tax_levy_ind)) <> rtrim(ltrim(out_v_tax_levy_ind)),'UPDATE',
	-- rtrim(ltrim(LKP_irs_name)) <> rtrim(ltrim(out_irs_name)),'UPDATE',
	-- rtrim(ltrim(LKP_address_line_1)) <> rtrim(ltrim(out_address_line_1)),'UPDATE',
	-- rtrim(ltrim(LKP_address_line_2)) <> rtrim(ltrim(out_address_line_2)),'UPDATE',
	-- rtrim(ltrim(LKP_city)) <> rtrim(ltrim(out_city)),'UPDATE',
	-- rtrim(ltrim(LKP_state_code)) <> rtrim(ltrim(out_state_code)),'UPDATE',
	-- rtrim(ltrim(LKP_zip_code)) <> rtrim(ltrim(out_zip_code)),'UPDATE',
	-- rtrim(ltrim(LKP_country_code)) <> rtrim(ltrim(out_country_code)),'UPDATE',
	-- rtrim(ltrim(LKP_phone)) <> rtrim(ltrim(out_phone)),'UPDATE',
	-- rtrim(ltrim(LKP_irs_1099_type1)) <> rtrim(ltrim(irs_1099_type)),'UPDATE',
	-- rtrim(ltrim(LKP_vendor_type_code)) <> rtrim(ltrim(vendor_type_cd)),'UPDATE',
	-- rtrim(ltrim(LKP_irs_tax_id)) <> rtrim(ltrim(search_tax_id)),'UPDATE',
	-- v_LKP_crrnt_snpsht_flag1 <> v_crrnt_snpsht_flag, 'UPDATE',
	-- 'NOCHANGE')
	DECODE(TRUE,
		LKP_claim_master_1099_list_id IS NULL, 'NEW',
		rtrim(ltrim(LKP_tax_id)) <> rtrim(ltrim(out_tax_id)), 'UPDATE',
		rtrim(ltrim(LKP_tax_id_type)) <> rtrim(ltrim(in_tax_id_type)), 'UPDATE',
		rtrim(ltrim(LKP_reportable_ind)) <> rtrim(ltrim(out_reportable_id)), 'UPDATE',
		rtrim(ltrim(LKP_tax_levy_ind)) <> rtrim(ltrim(out_v_tax_levy_ind)), 'UPDATE',
		rtrim(ltrim(LKP_irs_name)) <> rtrim(ltrim(out_irs_name)), 'UPDATE',
		rtrim(ltrim(LKP_address_line_1)) <> rtrim(ltrim(out_address_line_1)), 'UPDATE',
		rtrim(ltrim(LKP_address_line_2)) <> rtrim(ltrim(out_address_line_2)), 'UPDATE',
		rtrim(ltrim(LKP_city)) <> rtrim(ltrim(out_city)), 'UPDATE',
		rtrim(ltrim(LKP_state_code)) <> rtrim(ltrim(out_state_code)), 'UPDATE',
		rtrim(ltrim(LKP_zip_code)) <> rtrim(ltrim(out_zip_code)), 'UPDATE',
		rtrim(ltrim(LKP_country_code)) <> rtrim(ltrim(out_country_code)), 'UPDATE',
		rtrim(ltrim(LKP_phone)) <> rtrim(ltrim(out_phone)), 'UPDATE',
		rtrim(ltrim(LKP_irs_1099_type1)) <> rtrim(ltrim(irs_1099_type)), 'UPDATE',
		rtrim(ltrim(LKP_vendor_type_code)) <> rtrim(ltrim(vendor_type_cd)), 'UPDATE',
		rtrim(ltrim(LKP_irs_tax_id)) <> rtrim(ltrim(search_tax_id)), 'UPDATE',
		v_LKP_crrnt_snpsht_flag1 <> v_crrnt_snpsht_flag, 'UPDATE',
		'NOCHANGE') AS v_change_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_change_flag='NEW',TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	-- --TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	IFF(v_change_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	-- *INF*: iif(ISNULL(LKP_claim_master_1099_list_id),1,0)
	IFF(LKP_claim_master_1099_list_id IS NULL, 1, 0) AS v_check_master_list_id,
	v_check_master_list_id AS out_check_master_list_id,
	FIL_keep_conditions.search_tax_id,
	v_change_flag AS change_flag,
	FIL_keep_conditions.modified_ts
	FROM FIL_keep_conditions
	LEFT JOIN LKP_claim_master_1099_list
	ON LKP_claim_master_1099_list.irs_tax_id = FIL_keep_conditions.search_tax_id AND LKP_claim_master_1099_list.tax_id_type = FIL_keep_conditions.tax_id_type
),
RTR_claim_master_1099_list AS (
	SELECT
	LKP_claim_master_1099_list_id,
	LKP_claim_master_1099_list_ak_id,
	out_tax_id,
	out_reportable_id,
	out_v_tax_levy_ind,
	out_irs_name,
	out_address_line_1,
	out_address_line_2,
	out_city,
	out_state_code,
	out_zip_code,
	out_country_code,
	in_tax_id_type,
	out_phone,
	out_err_flag,
	irs_1099_type,
	vendor_type_cd,
	out_crrnt_snpsht_flag AS crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_system_id,
	created_date,
	modified_date,
	out_check_master_list_id AS check_master_list_id,
	search_tax_id,
	change_flag
	FROM EXP_detect_changes
),
RTR_claim_master_1099_list_INSERT AS (SELECT * FROM RTR_claim_master_1099_list WHERE IN(change_flag,'NEW')),
RTR_claim_master_1099_list_UPDATE AS (SELECT * FROM RTR_claim_master_1099_list WHERE change_flag='UPDATE'),
SEQ_claim_master_1099_list_ak_id AS (
	CREATE SEQUENCE SEQ_claim_master_1099_list_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_Insert_Update_Out AS (
	SELECT
	LKP_claim_master_1099_list_id AS LKP_claim_master_1099_list_id1,
	LKP_claim_master_1099_list_ak_id AS LKP_claim_master_1099_list_ak_id1,
	-- *INF*: IIF(change_flag1='NEW',NEXTVAL,LKP_claim_master_1099_list_ak_id1)
	IFF(change_flag1 = 'NEW', NEXTVAL, LKP_claim_master_1099_list_ak_id1) AS claim_master_1099_list_ak_id,
	out_tax_id AS out_tax_id1,
	out_reportable_id AS out_reportable_id1,
	out_v_tax_levy_ind AS out_v_tax_levy_ind1,
	out_irs_name AS out_irs_name1,
	out_address_line_ AS out_address_line_11,
	out_address_line_2 AS out_address_line_21,
	out_city AS out_city1,
	out_state_code AS out_state_code1,
	out_zip_code AS out_zip_code1,
	out_country_code AS out_country_code1,
	in_tax_id_type AS in_tax_id_type1,
	out_phone AS out_phone1,
	out_err_flag AS out_err_flag1,
	irs_1099_type AS irs_1099_type1,
	vendor_type_cd AS vendor_type_cd1,
	crrnt_snpsht_flag AS crrnt_snpsht_flag1,
	audit_id AS audit_id1,
	eff_from_date AS eff_from_date1,
	eff_to_date AS eff_to_date1,
	source_system_id AS source_system_id1,
	created_date AS created_date1,
	modified_date AS modified_date1,
	check_master_list_id AS check_master_list_id1,
	search_tax_id AS search_tax_id1,
	expire_eff_to_date AS expire_eff_to_date1,
	expire_crrnt_snpsht_flag AS expire_crrnt_snpsht_flag1,
	change_flag AS change_flag1,
	SEQ_claim_master_1099_list_ak_id.NEXTVAL
	FROM RTR_claim_master_1099_list_INSERT
),
UPD_Claim_Master_1099_List_Insert AS (
	SELECT
	claim_master_1099_list_ak_id, 
	out_tax_id1, 
	out_reportable_id1, 
	out_v_tax_levy_ind1, 
	out_irs_name1, 
	out_address_line_11, 
	out_address_line_21, 
	out_city1, 
	out_state_code1, 
	out_zip_code1, 
	out_country_code1, 
	in_tax_id_type1, 
	out_phone1, 
	out_err_flag1, 
	irs_1099_type1, 
	vendor_type_cd1, 
	crrnt_snpsht_flag1, 
	audit_id1, 
	eff_from_date1, 
	eff_to_date1, 
	source_system_id1, 
	created_date1, 
	modified_date1, 
	check_master_list_id1, 
	search_tax_id1
	FROM EXP_Insert_Update_Out
),
TGT_claim_master_1099_list_INSERT AS (
	INSERT INTO claim_master_1099_list
	(claim_master_1099_list_ak_id, tax_id, irs_tax_id, tax_id_type, reportable_ind, tax_levy_ind, irs_name, address_line_1, address_line_2, city, state_code, zip_code, country_code, phone, irs_1099_type, vendor_type_code, err_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_system_id, created_date, modified_date)
	SELECT 
	CLAIM_MASTER_1099_LIST_AK_ID, 
	out_tax_id1 AS TAX_ID, 
	search_tax_id1 AS IRS_TAX_ID, 
	in_tax_id_type1 AS TAX_ID_TYPE, 
	out_reportable_id1 AS REPORTABLE_IND, 
	out_v_tax_levy_ind1 AS TAX_LEVY_IND, 
	out_irs_name1 AS IRS_NAME, 
	out_address_line_11 AS ADDRESS_LINE_1, 
	out_address_line_21 AS ADDRESS_LINE_2, 
	out_city1 AS CITY, 
	out_state_code1 AS STATE_CODE, 
	out_zip_code1 AS ZIP_CODE, 
	out_country_code1 AS COUNTRY_CODE, 
	out_phone1 AS PHONE, 
	irs_1099_type1 AS IRS_1099_TYPE, 
	vendor_type_cd1 AS VENDOR_TYPE_CODE, 
	out_err_flag1 AS ERR_FLAG, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	source_system_id1 AS SOURCE_SYSTEM_ID, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE
	FROM UPD_Claim_Master_1099_List_Insert
),
UPD_cliam_master_1099_list_UPDATE AS (
	SELECT
	LKP_claim_master_1099_list_id AS LKP_claim_master_1099_list_id2, 
	out_tax_id AS out_tax_id2, 
	out_reportable_id AS out_reportable_id2, 
	out_v_tax_levy_ind AS out_v_tax_levy_ind2, 
	out_irs_name AS out_irs_name2, 
	out_address_line_1 AS out_address_line_12, 
	out_address_line_2 AS out_address_line_22, 
	out_city AS out_city2, 
	out_state_code AS out_state_code2, 
	out_zip_code AS out_zip_code2, 
	out_country_code AS out_country_code2, 
	in_tax_id_type AS in_tax_id_type2, 
	out_phone AS out_phone2, 
	irs_1099_type AS irs_1099_type2, 
	vendor_type_cd AS vendor_type_cd2, 
	audit_id AS audit_id2, 
	modified_date AS modified_date2, 
	search_tax_id AS search_tax_id2, 
	expire_eff_to_date AS expire_eff_to_date3, 
	expire_crrnt_snpsht_flag AS expire_crrnt_snpsht_flag3, 
	eff_from_date AS eff_from_date3, 
	out_err_flag AS out_err_flag3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3
	FROM RTR_claim_master_1099_list_UPDATE
),
TGT_claim_master_1099_list_UPDATE AS (
	MERGE INTO claim_master_1099_list AS T
	USING UPD_cliam_master_1099_list_UPDATE AS S
	ON T.claim_master_1099_list_id = S.LKP_claim_master_1099_list_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.tax_id = S.out_tax_id2, T.irs_tax_id = S.search_tax_id2, T.tax_id_type = S.in_tax_id_type2, T.reportable_ind = S.out_reportable_id2, T.tax_levy_ind = S.out_v_tax_levy_ind2, T.irs_name = S.out_irs_name2, T.address_line_1 = S.out_address_line_12, T.address_line_2 = S.out_address_line_22, T.city = S.out_city2, T.state_code = S.out_state_code2, T.zip_code = S.out_zip_code2, T.country_code = S.out_country_code2, T.phone = S.out_phone2, T.irs_1099_type = S.irs_1099_type2, T.vendor_type_code = S.vendor_type_cd2, T.err_flag = S.out_err_flag3, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag3, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date3, T.modified_date = S.modified_date2
),