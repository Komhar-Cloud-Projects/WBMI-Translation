WITH
SQ_gtam_wbmtmrkt_stage AS (
	SELECT
		gtam_wbmtmrkt_stage_id,
		abbreviation_of_target_mkt,
		date_field1,
		date_field2,
		description_of_target_mkt,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbmtmrkt_stage
),
EXP_values AS (
	SELECT
	gtam_wbmtmrkt_stage_id,
	abbreviation_of_target_mkt AS in_abbreviation_of_target_mkt,
	-- *INF*: decode(TRUE,
	-- ISNULL(in_abbreviation_of_target_mkt),'N/A',
	-- IS_SPACES(in_abbreviation_of_target_mkt),'N/A',
	-- LENGTH(in_abbreviation_of_target_mkt)=0,'N/A',
	-- LTRIM(RTRIM(in_abbreviation_of_target_mkt)))
	decode(TRUE,
		in_abbreviation_of_target_mkt IS NULL, 'N/A',
		LENGTH(in_abbreviation_of_target_mkt)>0 AND TRIM(in_abbreviation_of_target_mkt)='', 'N/A',
		LENGTH(in_abbreviation_of_target_mkt
		) = 0, 'N/A',
		LTRIM(RTRIM(in_abbreviation_of_target_mkt
			)
		)
	) AS target_market_code,
	description_of_target_mkt AS in_description_of_target_mkt,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_description_of_target_mkt)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --decode(TRUE,
	-- --ISNULL(in_description_of_target_mkt),'N/A',
	-- --IS_SPACES(in_description_of_target_mkt),'N/A',
	-- --LENGTH(in_description_of_target_mkt)=0,'N/A',
	-- --LTRIM(RTRIM(in_description_of_target_mkt)))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_description_of_target_mkt
	) AS target_mrkt_code_descript
	FROM SQ_gtam_wbmtmrkt_stage
),
LKP_sup_target_marget_code AS (
	SELECT
	sup_target_mrkt_code_id,
	target_mrkt_code_descript,
	target_mrkt_code
	FROM (
		SELECT 
			sup_target_market_code.sup_target_mrkt_code_id as sup_target_mrkt_code_id, 	sup_target_market_code.target_mrkt_code_descript as target_mrkt_code_descript,
			ltrim(rtrim(sup_target_market_code.target_mrkt_code)) as target_mrkt_code 
		FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_target_market_code
		where sup_target_market_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY target_mrkt_code ORDER BY sup_target_mrkt_code_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_sup_target_marget_code.sup_target_mrkt_code_id AS lkp_sup_target_mrkt_code_id,
	LKP_sup_target_marget_code.target_mrkt_code_descript AS lkp_target_mrkt_code_descript,
	EXP_values.target_market_code,
	EXP_values.target_mrkt_code_descript,
	-- *INF*: iif(isnull(lkp_sup_target_mrkt_code_id),'NEW',IIF(
	-- LTRIM(RTRIM(lkp_target_mrkt_code_descript)) != LTRIM(RTRIM(target_mrkt_code_descript)),'UPDATE','NOCHANGE'))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(lkp_sup_target_mrkt_code_id IS NULL,
		'NEW',
		IFF(LTRIM(RTRIM(lkp_target_mrkt_code_descript
				)
			) != LTRIM(RTRIM(target_mrkt_code_descript
				)
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_values
	LEFT JOIN LKP_sup_target_marget_code
	ON LKP_sup_target_marget_code.target_mrkt_code = EXP_values.target_market_code
),
FIL_insert AS (
	SELECT
	target_market_code, 
	target_mrkt_code_descript, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
TGT_sup_target_market_code_INSERT AS (
	INSERT INTO sup_target_market_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, target_mrkt_code, target_mrkt_code_descript)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	target_market_code AS TARGET_MRKT_CODE, 
	TARGET_MRKT_CODE_DESCRIPT
	FROM FIL_insert
),
SQ_sup_target_market_code AS (
	SELECT 
			a.sup_target_mrkt_code_id, 
			a.eff_from_date, 
			a.eff_to_date, 
			a.target_mrkt_code
	FROM
			 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_target_market_code a
	WHERE 
			a.target_mrkt_code  IN 
			(SELECT target_mrkt_code FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_target_market_code
			WHERE crrnt_snpsht_flag = 1 GROUP BY target_mrkt_code  HAVING count(*) > 1)
	ORDER BY 
			a.target_mrkt_code, a.eff_from_date  DESC
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	sup_target_mrkt_code_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	target_mrkt_code,
	-- *INF*: DECODE(TRUE,
	-- target_mrkt_code = v_prev_target_mrkt_code,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		target_mrkt_code = v_prev_target_mrkt_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	target_mrkt_code AS v_prev_target_mrkt_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_dt
	FROM SQ_sup_target_market_code
),
FIL_FirstRowInAKGrouptRowInAKGroup AS (
	SELECT
	sup_target_mrkt_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_dt
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_assoc_program_code_id AS (
	SELECT
	sup_target_mrkt_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_dt
	FROM FIL_FirstRowInAKGrouptRowInAKGroup
),
TGT_sup_target_market_code_UPDATE AS (
	MERGE INTO sup_target_market_code AS T
	USING UPD_sup_assoc_program_code_id AS S
	ON T.sup_target_mrkt_code_id = S.sup_target_mrkt_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_dt
),