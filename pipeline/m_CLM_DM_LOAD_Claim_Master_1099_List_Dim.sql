WITH
SQ_claim_master_1099_list AS (
	SELECT 
	claim_master_1099_list.claim_master_1099_list_id, 
	claim_master_1099_list.claim_master_1099_list_ak_id, 
	claim_master_1099_list.tax_id, 
	claim_master_1099_list.irs_tax_id, 
	claim_master_1099_list.tax_id_type, 
	claim_master_1099_list.reportable_ind, 
	claim_master_1099_list.tax_levy_ind, 
	claim_master_1099_list.irs_name, 
	claim_master_1099_list.address_line_1, 
	claim_master_1099_list.address_line_2,
	 claim_master_1099_list.city, 
	claim_master_1099_list.state_code, 
	claim_master_1099_list.zip_code, 
	claim_master_1099_list.country_code, 
	claim_master_1099_list.phone, 
	claim_master_1099_list.irs_1099_type, 
	claim_master_1099_list.vendor_type_code, 
	claim_master_1099_list.err_flag,
	claim_master_1099_list.crrnt_snpsht_flag,
	claim_master_1099_list.eff_from_date,
	claim_master_1099_list.eff_to_date,
	 claim_master_1099_list.created_date, 
	claim_master_1099_list.modified_date 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_master_1099_list
	where claim_master_1099_list.modified_date >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_claim_master_1099_list_dim AS (
	SELECT
	claim_master_1099_list_id,
	claim_master_1099_list_ak_id,
	tax_id AS in_tax_id,
	-- *INF*: LTRIM(RTRIM(in_tax_id))
	LTRIM(RTRIM(in_tax_id)) AS v_tax_id,
	v_tax_id AS out_tax_id,
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
	vendor_type_code AS in_vendor_type_code,
	-- *INF*: ltrim(rtrim(in_vendor_type_code))
	ltrim(rtrim(in_vendor_type_code)) AS v_vendor_code_type,
	v_vendor_code_type AS vendor_code_type,
	err_flag,
	crrnt_snpsht_flag,
	-- *INF*: DECODE(crrnt_snpsht_flag,
	-- 'T',1,
	-- 'F',0,
	-- '1',1,
	-- '0',0,
	-- 0)
	DECODE(
	    crrnt_snpsht_flag,
	    'T', 1,
	    'F', 0,
	    '1', 1,
	    '0', 0,
	    0
	) AS crrnt_snpsht_flag_out,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date
	FROM SQ_claim_master_1099_list
),
LKP_claim_master_1099_list_dim AS (
	SELECT
	claim_master_1099_list_dim_id,
	edw_claim_master_1099_list_pk_id,
	edw_claim_master_1099_list_ak_id
	FROM (
		SELECT 
			claim_master_1099_list_dim_id,
			edw_claim_master_1099_list_pk_id,
			edw_claim_master_1099_list_ak_id
		FROM claim_master_1099_list_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_master_1099_list_pk_id,edw_claim_master_1099_list_ak_id ORDER BY claim_master_1099_list_dim_id) = 1
),
LKP_sup_claim_vendor_1099_type AS (
	SELECT
	vendor_type_code_descript,
	vendor_type_code
	FROM (
		SELECT 
		sup_claim_vendor_1099_type.vendor_type_code_descript as vendor_type_code_descript, 
		ltrim(rtrim(sup_claim_vendor_1099_type.vendor_type_code)) as vendor_type_code 
		FROM 
		sup_claim_vendor_1099_type
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_type_code ORDER BY vendor_type_code_descript) = 1
),
EXP_tocheck_source AS (
	SELECT
	LKP_claim_master_1099_list_dim.claim_master_1099_list_dim_id AS LKP_claim_master_1099_list_dim_id,
	EXP_claim_master_1099_list_dim.claim_master_1099_list_id,
	EXP_claim_master_1099_list_dim.claim_master_1099_list_ak_id,
	EXP_claim_master_1099_list_dim.out_tax_id AS tax_id,
	EXP_claim_master_1099_list_dim.tax_id_type,
	EXP_claim_master_1099_list_dim.reportable_ind,
	EXP_claim_master_1099_list_dim.tax_levy_ind,
	EXP_claim_master_1099_list_dim.irs_name,
	EXP_claim_master_1099_list_dim.address_line_1,
	EXP_claim_master_1099_list_dim.address_line_2,
	EXP_claim_master_1099_list_dim.city,
	EXP_claim_master_1099_list_dim.state_code,
	EXP_claim_master_1099_list_dim.zip_code,
	EXP_claim_master_1099_list_dim.country_code,
	EXP_claim_master_1099_list_dim.phone,
	EXP_claim_master_1099_list_dim.irs_1099_type,
	EXP_claim_master_1099_list_dim.vendor_code_type,
	LKP_sup_claim_vendor_1099_type.vendor_type_code_descript AS in_vendor_type_code_descript,
	-- *INF*: iif(isnull(in_vendor_type_code_descript) or IS_SPACES(in_vendor_type_code_descript) or LENGTH(in_vendor_type_code_descript)=0,'N/A',in_vendor_type_code_descript)
	IFF(
	    in_vendor_type_code_descript IS NULL
	    or LENGTH(in_vendor_type_code_descript)>0
	    and TRIM(in_vendor_type_code_descript)=''
	    or LENGTH(in_vendor_type_code_descript) = 0,
	    'N/A',
	    in_vendor_type_code_descript
	) AS v_vendor_type_code_descript,
	v_vendor_type_code_descript AS vendor_type_code_descript,
	EXP_claim_master_1099_list_dim.err_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	-- *INF*: IIF(ISNULL(LKP_claim_master_1099_list_dim_id),1,0)
	IFF(LKP_claim_master_1099_list_dim_id IS NULL, 1, 0) AS V_LKP_claim_master_1099_list_dim_id,
	V_LKP_claim_master_1099_list_dim_id AS OUT_LKP_claim_master_1099_list_dim_id,
	EXP_claim_master_1099_list_dim.irs_tax_id,
	EXP_claim_master_1099_list_dim.crrnt_snpsht_flag_out AS edw_crrnt_snpsht_flag,
	EXP_claim_master_1099_list_dim.eff_from_date AS edw_eff_from_date,
	EXP_claim_master_1099_list_dim.eff_to_date AS edw_eff_to_date,
	EXP_claim_master_1099_list_dim.created_date AS created_date1,
	EXP_claim_master_1099_list_dim.modified_date AS modified_date1
	FROM EXP_claim_master_1099_list_dim
	LEFT JOIN LKP_claim_master_1099_list_dim
	ON LKP_claim_master_1099_list_dim.edw_claim_master_1099_list_pk_id = EXP_claim_master_1099_list_dim.claim_master_1099_list_id AND LKP_claim_master_1099_list_dim.edw_claim_master_1099_list_ak_id = EXP_claim_master_1099_list_dim.claim_master_1099_list_ak_id
	LEFT JOIN LKP_sup_claim_vendor_1099_type
	ON LKP_sup_claim_vendor_1099_type.vendor_type_code = EXP_claim_master_1099_list_dim.vendor_code_type
),
RTR_claim_master_1099_list_dim AS (
	SELECT
	LKP_claim_master_1099_list_dim_id,
	claim_master_1099_list_id,
	claim_master_1099_list_ak_id,
	tax_id,
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
	vendor_code_type,
	vendor_type_code_descript,
	err_flag,
	edw_crrnt_snpsht_flag AS crrnt_snpsht_flag,
	audit_id,
	edw_eff_from_date AS eff_from_date,
	edw_eff_to_date AS eff_to_date,
	created_date1 AS created_date,
	modified_date1 AS modified_date,
	OUT_LKP_claim_master_1099_list_dim_id,
	irs_tax_id
	FROM EXP_tocheck_source
),
RTR_claim_master_1099_list_dim_INSERT AS (SELECT * FROM RTR_claim_master_1099_list_dim WHERE OUT_LKP_claim_master_1099_list_dim_id=1),
RTR_claim_master_1099_list_dim_DEFAULT1 AS (SELECT * FROM RTR_claim_master_1099_list_dim WHERE NOT ( (OUT_LKP_claim_master_1099_list_dim_id=1) )),
TGT_claim_master_1099_list_dim_INSERT AS (
	INSERT INTO claim_master_1099_list_dim
	(edw_claim_master_1099_list_pk_id, edw_claim_master_1099_list_ak_id, tax_id, irs_tax_id, tax_id_type, reportable_ind, tax_levy_ind, irs_name, address_line_1, address_line_2, city, state_code, zip_code, country_code, phone, irs_1099_type, vendor_type_code, vendor_type_code_descript, err_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date)
	SELECT 
	claim_master_1099_list_id AS EDW_CLAIM_MASTER_1099_LIST_PK_ID, 
	claim_master_1099_list_ak_id AS EDW_CLAIM_MASTER_1099_LIST_AK_ID, 
	TAX_ID, 
	IRS_TAX_ID, 
	TAX_ID_TYPE, 
	REPORTABLE_IND, 
	TAX_LEVY_IND, 
	IRS_NAME, 
	address_line_ AS ADDRESS_LINE_1, 
	ADDRESS_LINE_2, 
	CITY, 
	STATE_CODE, 
	ZIP_CODE, 
	COUNTRY_CODE, 
	PHONE, 
	IRS_1099_TYPE, 
	vendor_code_type AS VENDOR_TYPE_CODE, 
	VENDOR_TYPE_CODE_DESCRIPT, 
	ERR_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM RTR_claim_master_1099_list_dim_INSERT
),
UPD_claim_master_1099_list_dim AS (
	SELECT
	LKP_claim_master_1099_list_dim_id AS LKP_claim_master_1099_list_dim_id2, 
	claim_master_1099_list_id AS claim_master_1099_list_id2, 
	claim_master_1099_list_ak_id AS claim_master_1099_list_ak_id2, 
	tax_id AS tax_id2, 
	tax_id_type AS tax_id_type2, 
	reportable_ind AS reportable_ind2, 
	tax_levy_ind AS tax_levy_ind2, 
	irs_name AS irs_name2, 
	address_line_1 AS address_line_12, 
	address_line_ AS address_line_22, 
	city AS city2, 
	state_code AS state_code2, 
	zip_code AS zip_code2, 
	country_code AS country_code2, 
	phone AS phone2, 
	irs_1099_type AS irs_1099_type2, 
	vendor_code_type AS vendor_code_type2, 
	vendor_type_code_descript AS vendor_type_code_descript2, 
	err_flag AS err_flag2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	created_date AS created_date2, 
	modified_date AS modified_date2, 
	irs_tax_id AS irs_tax_id2
	FROM RTR_claim_master_1099_list_dim_DEFAULT1
),
TGT_claim_master_1099_list_dim_UPDATE AS (
	MERGE INTO claim_master_1099_list_dim AS T
	USING UPD_claim_master_1099_list_dim AS S
	ON T.claim_master_1099_list_dim_id = S.LKP_claim_master_1099_list_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.tax_id = S.tax_id2, T.irs_tax_id = S.irs_tax_id2, T.tax_id_type = S.tax_id_type2, T.reportable_ind = S.reportable_ind2, T.tax_levy_ind = S.tax_levy_ind2, T.irs_name = S.irs_name2, T.address_line_1 = S.address_line_12, T.address_line_2 = S.address_line_22, T.city = S.city2, T.state_code = S.state_code2, T.zip_code = S.zip_code2, T.country_code = S.country_code2, T.phone = S.phone2, T.irs_1099_type = S.irs_1099_type2, T.vendor_type_code = S.vendor_code_type2, T.vendor_type_code_descript = S.vendor_type_code_descript2, T.err_flag = S.err_flag2, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.created_date = S.created_date2, T.modified_date = S.modified_date2
),