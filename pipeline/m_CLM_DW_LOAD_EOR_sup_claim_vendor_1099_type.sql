WITH
SQ_vendor_1099_type_stage AS (
	SELECT 
	vendor_1099_type_stage.vendor_1099_type_id, 
	vendor_1099_type_stage.vendor_type_code, 
	vendor_1099_type_stage.vendor_type_desc 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.vendor_1099_type_stage
),
EXP_vendor_type AS (
	SELECT
	vendor_1099_type_id,
	vendor_type_code,
	vendor_type_desc
	FROM SQ_vendor_1099_type_stage
),
LKP_sup_claim_vendor_type_desc AS (
	SELECT
	sup_claim_vendor_1099_type_id,
	vendor_type_code,
	vendor_type_code_descript
	FROM (
		SELECT 
		sup_claim_vendor_1099_type.sup_claim_vendor_1099_type_id as sup_claim_vendor_1099_type_id, sup_claim_vendor_1099_type.vendor_type_code_descript as vendor_type_code_descript,
		ltrim(rtrim(sup_claim_vendor_1099_type.vendor_type_code)) as vendor_type_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_vendor_1099_type
		where 
		sup_claim_vendor_1099_type.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_type_code ORDER BY sup_claim_vendor_1099_type_id) = 1
),
EXP_vendor_change AS (
	SELECT
	LKP_sup_claim_vendor_type_desc.sup_claim_vendor_1099_type_id AS in_LKP_sup_claim_vendor_1099_type_id,
	LKP_sup_claim_vendor_type_desc.vendor_type_code AS in_LKP_vendor_type_code,
	LKP_sup_claim_vendor_type_desc.vendor_type_code_descript AS in_LKP_vendor_type_code_descript,
	EXP_vendor_type.vendor_1099_type_id,
	EXP_vendor_type.vendor_type_code AS in_vendor_type_code,
	-- *INF*: iif(isnull(in_vendor_type_code) or IS_SPACES(in_vendor_type_code) or LENGTH(in_vendor_type_code)=0,'N/A',in_vendor_type_code)
	IFF(in_vendor_type_code IS NULL 
		OR LENGTH(in_vendor_type_code)>0 AND TRIM(in_vendor_type_code)='' 
		OR LENGTH(in_vendor_type_code
		) = 0,
		'N/A',
		in_vendor_type_code
	) AS v_vendor_type_code,
	v_vendor_type_code AS vendor_type_code,
	EXP_vendor_type.vendor_type_desc AS in_vendor_type_desc,
	-- *INF*: iif(isnull(in_vendor_type_desc) or IS_SPACES(in_vendor_type_desc) or LENGTH(in_vendor_type_desc)=0,'N/A',ltrim(rtrim(in_vendor_type_desc)))
	IFF(in_vendor_type_desc IS NULL 
		OR LENGTH(in_vendor_type_desc)>0 AND TRIM(in_vendor_type_desc)='' 
		OR LENGTH(in_vendor_type_desc
		) = 0,
		'N/A',
		ltrim(rtrim(in_vendor_type_desc
			)
		)
	) AS v_vendor_type_desc,
	v_vendor_type_desc AS vendor_type_desc,
	-- *INF*: IIF(ISNULL(in_LKP_sup_claim_vendor_1099_type_id), 'NEW', 
	-- IIF(ltrim(rtrim(in_LKP_vendor_type_code_descript)) != ltrim(rtrim(v_vendor_type_desc)),
	-- 'UPDATE', 'NOCHANGE'))
	IFF(in_LKP_sup_claim_vendor_1099_type_id IS NULL,
		'NEW',
		IFF(ltrim(rtrim(in_LKP_vendor_type_code_descript
				)
			) != ltrim(rtrim(v_vendor_type_desc
				)
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS v_eff_from_date,
	v_eff_from_date AS eff_from_date
	FROM EXP_vendor_type
	LEFT JOIN LKP_sup_claim_vendor_type_desc
	ON LKP_sup_claim_vendor_type_desc.vendor_type_code = EXP_vendor_type.vendor_type_code
),
FIL_vendor_1099_type AS (
	SELECT
	vendor_1099_type_id, 
	vendor_type_code, 
	vendor_type_desc, 
	changed_flag, 
	eff_from_date
	FROM EXP_vendor_change
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
EXP_default_values AS (
	SELECT
	vendor_1099_type_id,
	vendor_type_code,
	vendor_type_desc,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM FIL_vendor_1099_type
),
TGT_sup_claim_vendor_1099_type_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_vendor_1099_type
	(vendor_type_code, vendor_type_code_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	VENDOR_TYPE_CODE, 
	vendor_type_desc AS VENDOR_TYPE_CODE_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_default_values
),
SQ_sup_claim_vendor_1099_type_UPDATE AS (
	SELECT 
	a.sup_claim_vendor_1099_type_id, 
	a.vendor_type_code, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_vendor_1099_type a
	WHERE EXISTS(SELECT 1			
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_vendor_1099_type b
	WHERE b.crrnt_snpsht_flag = 1 
	AND 
	a.vendor_type_code=b.vendor_type_code
	GROUP BY b.vendor_type_code
	HAVING COUNT(*) > 1)
	ORDER BY a.vendor_type_code, a.eff_from_date  DESC
),
EXP_vendor_1099_upadte AS (
	SELECT
	sup_claim_vendor_1099_type_id,
	vendor_type_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- vendor_type_code=v_prev_vendor_type_code,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	-- 
	DECODE(TRUE,
		vendor_type_code = v_prev_vendor_type_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	vendor_type_code AS v_prev_vendor_type_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snp_sht_flag,
	SYSDATE AS modified_dt
	FROM SQ_sup_claim_vendor_1099_type_UPDATE
),
FIL_vendor_1099_type_update AS (
	SELECT
	sup_claim_vendor_1099_type_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snp_sht_flag, 
	modified_dt
	FROM EXP_vendor_1099_upadte
	WHERE orig_eff_to_date != eff_to_date
),
UPD_vendor_1099_type AS (
	SELECT
	sup_claim_vendor_1099_type_id, 
	eff_to_date, 
	crrnt_snp_sht_flag, 
	modified_dt
	FROM FIL_vendor_1099_type_update
),
TGT_sup_claim_vendor_1099_type_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_vendor_1099_type AS T
	USING UPD_vendor_1099_type AS S
	ON T.sup_claim_vendor_1099_type_id = S.sup_claim_vendor_1099_type_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snp_sht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_dt
),