WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT 
	CS01_TABLE_SEQ_NBR, 
	CS01_CODE, 
	CS01_CODE_DES 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE
	WHERE
	CS01_TABLE_ID = 'C005'
),
EXP_Values AS (
	SELECT
	CS01_TABLE_SEQ_NBR,
	CS01_CODE,
	CS01_CODE_DES
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
EXP_Lkp_Values AS (
	SELECT
	CS01_TABLE_SEQ_NBR AS in_CS01_TABLE_SEQ_NBR,
	-- *INF*: IIF(ISNULL(in_CS01_TABLE_SEQ_NBR), 
	-- 0,
	-- in_CS01_TABLE_SEQ_NBR)
	IFF(in_CS01_TABLE_SEQ_NBR IS NULL, 0, in_CS01_TABLE_SEQ_NBR) AS CS01_TABLE_SEQ_NBR,
	CS01_CODE AS in_CS01_CODE,
	-- *INF*: IIF(ISNULL(in_CS01_CODE), 
	-- 'N/A',
	-- ltrim(rtrim(in_CS01_CODE)))
	IFF(in_CS01_CODE IS NULL, 'N/A', ltrim(rtrim(in_CS01_CODE))) AS CS01_CODE,
	CS01_CODE_DES AS in_CS01_CODE_DES,
	-- *INF*: IIF(ISNULL(in_CS01_CODE_DES),
	-- 'N/A', 
	-- in_CS01_CODE_DES)
	IFF(in_CS01_CODE_DES IS NULL, 'N/A', in_CS01_CODE_DES) AS CS01_CODE_DES
	FROM EXP_Values
),
LKP_Sup_State AS (
	SELECT
	sup_state_id,
	state_abbrev,
	state_descript,
	state_code
	FROM (
		SELECT 
		a.sup_state_id as sup_state_id, 
		a.state_abbrev as state_abbrev, 
		a.state_descript as state_descript, 
		ltrim(rtrim(a.state_code)) as state_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state a
		WHERE 
		source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
		ORDER BY state_code --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY sup_state_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lkp_Values.CS01_TABLE_SEQ_NBR,
	-- *INF*: TO_CHAR(CS01_TABLE_SEQ_NBR)
	TO_CHAR(CS01_TABLE_SEQ_NBR) AS out_CS01_TABLE_SEQ_NBR,
	EXP_Lkp_Values.CS01_CODE,
	EXP_Lkp_Values.CS01_CODE_DES,
	LKP_Sup_State.sup_state_id AS lkp_sup_state_id,
	LKP_Sup_State.state_abbrev AS lkp_state_abbrev,
	LKP_Sup_State.state_descript AS lkp_state_descript,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: IIF(ISNULL(lkp_sup_state_id),'NEW',
	-- 	IIF(
	-- 	(to_char(CS01_TABLE_SEQ_NBR) <> lkp_state_abbrev or  
	-- 	ltrim(rtrim(CS01_CODE_DES)) <> ltrim(rtrim(lkp_state_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(
	    lkp_sup_state_id IS NULL, 'NEW',
	    IFF(
	        (to_char(CS01_TABLE_SEQ_NBR) <> lkp_state_abbrev
	        or ltrim(rtrim(CS01_CODE_DES)) <> ltrim(rtrim(lkp_state_descript))),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(
	    v_Changed_Flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP, 'MM/DD/YYYY HH24:MI:SS'), 'MM/DD/YYYY HH24:MI:SS')
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_System_Id,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_Sup_State
	ON LKP_Sup_State.state_code = EXP_Lkp_Values.CS01_CODE
),
FIL_Insert AS (
	SELECT
	out_CS01_TABLE_SEQ_NBR AS CS01_TABLE_SEQ_NBR, 
	CS01_CODE, 
	CS01_CODE_DES, 
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_Id, 
	Created_Date, 
	Modified_Date
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
sup_state_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
	(state_code, state_abbrev, state_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	CS01_CODE AS STATE_CODE, 
	CS01_TABLE_SEQ_NBR AS STATE_ABBREV, 
	CS01_CODE_DES AS STATE_DESCRIPT, 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_Id AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE
	FROM FIL_Insert
),
SQ_sup_state AS (
	SELECT 
	a.sup_state_id, 
	a.state_code, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.state_code = b.state_code
			GROUP BY state_code
			HAVING COUNT(*) > 1)
	ORDER BY state_code, eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	sup_state_id,
	state_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	state_code = v_PREV_ROW_state_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    state_code = v_PREV_ROW_state_code, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	state_code AS v_PREV_ROW_state_code,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_sup_state
),
FIL_FirstRowInAKGroup AS (
	SELECT
	sup_state_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_Sup_State AS (
	SELECT
	sup_state_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
sup_state_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state AS T
	USING UPD_Sup_State AS S
	ON T.sup_state_id = S.sup_state_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),