WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT 
	ltrim(rtrim(a.CS01_CODE))
	, ltrim(rtrim(a.CS01_CODE_DES))
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE a
	where a.CS01_TABLE_ID = 'CADJ'
	AND CS01_CODE LIKE 'X%'
),
FIL_VALID_CODE AS (
	SELECT
	CS01_CODE, 
	CS01_CODE_DES
	FROM SQ_CLAIM_SUPPORT_01_STAGE
	WHERE LENGTH(CS01_CODE) = 3
),
EXP_Source AS (
	SELECT
	CS01_CODE,
	CS01_CODE_DES,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	sysdate AS created_date
	FROM FIL_VALID_CODE
),
LKP_sup_claim_adjuster AS (
	SELECT
	sup_claim_adjuster_id,
	wbconnect_user_id,
	adjuster_code
	FROM (
		SELECT 
		ltrim(rtrim(a.sup_claim_adjuster_id)) as sup_claim_adjuster_id
		, ltrim(rtrim(a.wbconnect_user_id)) as wbconnect_user_id
		, ltrim(rtrim(a.adjuster_code)) as adjuster_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_adjuster a
		WHERE
		source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adjuster_code ORDER BY sup_claim_adjuster_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_sup_claim_adjuster.sup_claim_adjuster_id,
	LKP_sup_claim_adjuster.wbconnect_user_id AS old_wbconnect_user_id,
	EXP_Source.CS01_CODE,
	EXP_Source.CS01_CODE_DES,
	EXP_Source.AUDIT_ID,
	EXP_Source.crrnt_snpsht_flag,
	EXP_Source.SOURCE_SYSTEM_ID,
	EXP_Source.created_date,
	-- *INF*: iif(isnull(sup_claim_adjuster_id),
	-- 'NEW'
	-- ,iif(CS01_CODE_DES != old_wbconnect_user_id
	-- ,'UPDATE'
	-- ,'NO CHANGE')
	-- )
	IFF(sup_claim_adjuster_id IS NULL,
		'NEW',
		IFF(CS01_CODE_DES != old_wbconnect_user_id,
			'UPDATE',
			'NO CHANGE'
		)
	) AS v_Changed_Flag,
	v_Changed_Flag AS changed_flag,
	-- *INF*: iif(v_Changed_Flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_Changed_Flag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date
	FROM EXP_Source
	LEFT JOIN LKP_sup_claim_adjuster
	ON LKP_sup_claim_adjuster.adjuster_code = EXP_Source.CS01_CODE
),
FIL_Insert AS (
	SELECT
	old_wbconnect_user_id, 
	CS01_CODE, 
	CS01_CODE_DES, 
	AUDIT_ID, 
	crrnt_snpsht_flag, 
	SOURCE_SYSTEM_ID, 
	created_date, 
	changed_flag, 
	eff_from_date, 
	eff_to_date
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW' or changed_flag='UPDATE'
),
sup_claim_adjuster_insert AS (
	INSERT INTO sup_claim_adjuster
	(adjuster_code, wbconnect_user_id, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	CS01_CODE AS ADJUSTER_CODE, 
	CS01_CODE_DES AS WBCONNECT_USER_ID, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	created_date AS MODIFIED_DATE
	FROM FIL_Insert
),
SQ_sup_claim_adjuster AS (
	SELECT 
	a.sup_claim_adjuster_id
	, a.adjuster_code
	, a.wbconnect_user_id
	, a.eff_from_date
	, a.eff_to_date
	, a.source_sys_id 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_adjuster a
	where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS (SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_adjuster b
		WHERE b.crrnt_snpsht_flag = 1
	and a.adjuster_code = b.adjuster_code
	and a.source_sys_id = b.source_sys_id
		GROUP BY b.adjuster_code,b.source_sys_id
		HAVING COUNT(*) > 1)
	order by a.adjuster_code,a.source_sys_id, a.eff_from_date desc
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Expire_Rows AS (
	SELECT
	sup_claim_adjuster_id,
	adjuster_code,
	wbconnect_user_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: decode(true,
	-- adjuster_code = v_PREV_ROW_ADJUSTER_CODE
	-- , ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1)
	-- ,orig_eff_to_date)
	decode(true,
		adjuster_code = v_PREV_ROW_ADJUSTER_CODE, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	adjuster_code AS v_PREV_ROW_ADJUSTER_CODE,
	source_sys_id AS v_PREV_ROW_source_sys_id,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date
	FROM SQ_sup_claim_adjuster
),
FIL_Claimant_Coverage_Detail AS (
	SELECT
	sup_claim_adjuster_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Update_Target AS (
	SELECT
	sup_claim_adjuster_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_Claimant_Coverage_Detail
),
sup_claim_adjuster_update AS (
	MERGE INTO sup_claim_adjuster AS T
	USING UPD_Update_Target AS S
	ON T.sup_claim_adjuster_id = S.sup_claim_adjuster_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),