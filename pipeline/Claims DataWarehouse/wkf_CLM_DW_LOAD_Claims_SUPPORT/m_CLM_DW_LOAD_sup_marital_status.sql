WITH
SQ_Client_UCT_Stage AS (
	SELECT client_uct_stage.cicu_uct_cd, client_uct_stage.cicu_uct_des 
	FROM
	 client_uct_stage
	where CICU_VIEW_NM = 'MARITAL_STAT_COD_V'
),
EXP_Default_Values AS (
	SELECT
	cicu_uct_cd,
	-- *INF*: IIF(ISNULL(cicu_uct_cd), 'N/A', ltrim(rtrim(cicu_uct_cd)))
	IFF(cicu_uct_cd IS NULL, 'N/A', ltrim(rtrim(cicu_uct_cd))) AS cicu_uct_cd_out,
	cicu_uct_des,
	-- *INF*: IIF(ISNULL(cicu_uct_des), 'N/A', cicu_uct_des)
	IFF(cicu_uct_des IS NULL, 'N/A', cicu_uct_des) AS cicu_uct_des_out
	FROM SQ_Client_UCT_Stage
),
LKP_sup_marital_status AS (
	SELECT
	sup_marital_status_id,
	marital_status_descript,
	marital_status_code
	FROM (
		SELECT sup_marital_status.sup_marital_status_id as sup_marital_status_id, sup_marital_status.marital_status_descript as marital_status_descript, 
		ltrim(rtrim(sup_marital_status.marital_status_code)) as marital_status_code FROM sup_marital_status
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY marital_status_code ORDER BY sup_marital_status_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_sup_marital_status.sup_marital_status_id AS lkp_sup_marital_status_id,
	LKP_sup_marital_status.marital_status_descript AS lkp_marital_status_descript,
	EXP_Default_Values.cicu_uct_cd_out AS cicu_uct_cd,
	EXP_Default_Values.cicu_uct_des_out AS cicu_uct_des,
	-- *INF*: IIF(ISNULL(lkp_sup_marital_status_id), 'NEW', IIF(LTRIM(RTRIM(lkp_marital_status_descript)) != (LTRIM(RTRIM(cicu_uct_des))), 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(lkp_sup_marital_status_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(lkp_marital_status_descript)) != ( LTRIM(RTRIM(cicu_uct_des)) ), 'UPDATE', 'NOCHANGE')) AS v_CHANGED_FLAG,
	v_CHANGED_FLAG AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_CHANGED_FLAG='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_CHANGED_FLAG = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_sup_marital_status
	ON LKP_sup_marital_status.marital_status_code = EXP_Default_Values.cicu_uct_cd_out
),
FIL_sup_marital_status_insert AS (
	SELECT
	cicu_uct_cd, 
	cicu_uct_des, 
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	source_sys_id
	FROM EXP_detect_changes
	WHERE CHANGED_FLAG = 'NEW' or CHANGED_FLAG = 'UPDATE'
),
TGT_sup_marital_status_INSERT AS (
	INSERT INTO sup_marital_status
	(marital_status_code, marital_status_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	cicu_uct_cd AS MARITAL_STATUS_CODE, 
	cicu_uct_des AS MARITAL_STATUS_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_marital_status_insert
),
SQ_sup_marital_status AS (
	SELECT a.sup_marital_status_id,
	a.marital_status_code, 
	a.eff_from_date,
	a.eff_to_date 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_marital_status a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_marital_status b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.marital_status_code = b.marital_status_code
			GROUP BY marital_status_code
			HAVING COUNT(*) > 1)
	ORDER BY marital_status_code, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_marital_status_id,
	marital_status_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	marital_status_code= v_Prev_row_occuptn_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
	marital_status_code = v_Prev_row_occuptn_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	marital_status_code AS v_Prev_row_occuptn_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_marital_status
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_marital_status_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_workers_comp_wage_period AS (
	SELECT
	sup_marital_status_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
TGT_sup_marital_status_UPDATE AS (
	MERGE INTO sup_marital_status AS T
	USING UPD_sup_workers_comp_wage_period AS S
	ON T.sup_marital_status_id = S.sup_marital_status_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),