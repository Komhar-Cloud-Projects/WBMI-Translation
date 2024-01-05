WITH
SQ_sup_eor_excl_rsn_stage AS (
	SELECT 
	sup_eor_excl_rsn_stage.autopay_excl_rsn_code,
	sup_eor_excl_rsn_stage.description,
	sup_eor_excl_rsn_stage.exclude_from_manualpay
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_eor_excl_rsn_stage
),
EXP_sup_eor_autopay_exclude_reason1 AS (
	SELECT
	autopay_excl_rsn_code,
	description,
	exclude_from_manualpay,
	-- *INF*: IIF(ISNULL(description), 'N/A',description)
	IFF(description IS NULL, 'N/A', description) AS description_out,
	-- *INF*: IIF(ISNULL( exclude_from_manualpay), 'N/A',exclude_from_manualpay)
	--  
	IFF(exclude_from_manualpay IS NULL, 'N/A', exclude_from_manualpay) AS exclude_from_manualpay_out
	FROM SQ_sup_eor_excl_rsn_stage
),
LKP_sup_eor_autopay_exclude_reason AS (
	SELECT
	autopay_excl_rsn_code,
	autopay_excl_rsn_descript,
	excl_from_manualpay
	FROM (
		SELECT  
		   C.autopay_excl_rsn_code         as autopay_excl_rsn_code 
		,  C.autopay_excl_rsn_descript as autopay_excl_rsn_descript
		,  C.excl_from_manualpay  as excl_from_manualpay
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_eor_autopay_exclude_reason C
		WHERE C.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY autopay_excl_rsn_code ORDER BY autopay_excl_rsn_code DESC) = 1
),
EXP_sup_eor_autopay_exclude_reason2 AS (
	SELECT
	LKP_sup_eor_autopay_exclude_reason.autopay_excl_rsn_code AS IN_LKP_autopay_excl_rsn_code,
	LKP_sup_eor_autopay_exclude_reason.autopay_excl_rsn_descript AS IN_LKP_autopay_excl_rsn_descript,
	LKP_sup_eor_autopay_exclude_reason.excl_from_manualpay AS IN_LKP_excl_from_manualpay,
	EXP_sup_eor_autopay_exclude_reason1.autopay_excl_rsn_code AS IN_autopay_excl_rsn_code,
	EXP_sup_eor_autopay_exclude_reason1.description_out AS IN_description,
	EXP_sup_eor_autopay_exclude_reason1.exclude_from_manualpay_out AS IN_exclude_from_manualpay,
	-- *INF*: IIF(ISNULL(IN_LKP_autopay_excl_rsn_code), 'NEW', 
	-- IIF(ltrim(rtrim(IN_LKP_excl_from_manualpay)) != ltrim(rtrim( IN_exclude_from_manualpay)  ) OR
	-- ltrim(rtrim(IN_LKP_autopay_excl_rsn_descript)) != ltrim(rtrim( IN_description)  ) ,
	-- 'UPDATE', 'NOCHANGE'))
	IFF(IN_LKP_autopay_excl_rsn_code IS NULL, 'NEW', IFF(ltrim(rtrim(IN_LKP_excl_from_manualpay)) != ltrim(rtrim(IN_exclude_from_manualpay)) OR ltrim(rtrim(IN_LKP_autopay_excl_rsn_descript)) != ltrim(rtrim(IN_description)), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS v_eff_from_date,
	v_eff_from_date AS eff_from_date
	FROM EXP_sup_eor_autopay_exclude_reason1
	LEFT JOIN LKP_sup_eor_autopay_exclude_reason
	ON LKP_sup_eor_autopay_exclude_reason.autopay_excl_rsn_code = EXP_sup_eor_autopay_exclude_reason1.autopay_excl_rsn_code
),
FIL_sup_eor_autopay_exclude_reason3 AS (
	SELECT
	IN_autopay_excl_rsn_code AS autopay_excl_rsn_code, 
	IN_description AS description, 
	IN_exclude_from_manualpay AS exclude_from_manualpay, 
	changed_flag, 
	eff_from_date
	FROM EXP_sup_eor_autopay_exclude_reason2
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
EXP_default_values1 AS (
	SELECT
	autopay_excl_rsn_code,
	description,
	exclude_from_manualpay,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM FIL_sup_eor_autopay_exclude_reason3
),
TGT_sup_eor_autopay_exclude_reason AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_eor_autopay_exclude_reason
	(autopay_excl_rsn_code, autopay_excl_rsn_descript, excl_from_manualpay, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	AUTOPAY_EXCL_RSN_CODE, 
	description AS AUTOPAY_EXCL_RSN_DESCRIPT, 
	exclude_from_manualpay AS EXCL_FROM_MANUALPAY, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_default_values1
),
SQ_sup_eor_autopay_exclude_reason AS (
	SELECT sup_eor_autopay_excl_rsn_id
	,       autopay_excl_rsn_code
	,		eff_from_date
	,		eff_to_date 
	FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_eor_autopay_exclude_reason MBV
	WHERE	source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND		EXISTS
	(select 1
			FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_eor_autopay_exclude_reason MBV2
			WHERE	source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			AND		crrnt_snpsht_flag = 1
			AND		MBV2.autopay_excl_rsn_code = MBV.autopay_excl_rsn_code
			GROUP	BY	MBV2.autopay_excl_rsn_code
			HAVING	count(*) > 1
	)
	ORDER BY MBV.autopay_excl_rsn_code   , MBV.eff_from_date DESC
),
EXP_sup_eor_autopay_exclude_reason AS (
	SELECT
	sup_eor_autopay_excl_rsn_id,
	autopay_excl_rsn_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- autopay_excl_rsn_code=v_prev_autopay_excl_rsn_code,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	-- 
	DECODE(TRUE,
	autopay_excl_rsn_code = v_prev_autopay_excl_rsn_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	autopay_excl_rsn_code AS v_prev_autopay_excl_rsn_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snp_sht_flag,
	SYSDATE AS modified_dt
	FROM SQ_sup_eor_autopay_exclude_reason
),
FIL_sup_eor_autopay_exclude_reason AS (
	SELECT
	sup_eor_autopay_excl_rsn_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snp_sht_flag, 
	modified_dt
	FROM EXP_sup_eor_autopay_exclude_reason
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_eor_autopay_exclude_reason AS (
	SELECT
	sup_eor_autopay_excl_rsn_id, 
	eff_to_date, 
	crrnt_snp_sht_flag, 
	modified_dt
	FROM FIL_sup_eor_autopay_exclude_reason
),
UPD_sup_eor_autopay_exclude_reason AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_eor_autopay_exclude_reason AS T
	USING UPD_sup_eor_autopay_exclude_reason AS S
	ON T.sup_eor_autopay_excl_rsn_id = S.sup_eor_autopay_excl_rsn_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snp_sht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_dt
),