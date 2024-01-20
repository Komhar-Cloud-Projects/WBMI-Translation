WITH
LKP_Agency AS (
	SELECT
	agency_ak_id,
	agency_key
	FROM (
		SELECT a.agency_ak_id as agency_ak_id, a.agency_key as agency_key FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency a
		where crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key ORDER BY agency_ak_id) = 1
),
SQ_Producer_code_stage AS (
	SELECT
		producer_code_stage_id,
		STATE_CODE,
		AGENCY_NUM,
		PRODUCER_CODE,
		EMP_ID,
		PRODUCER_DESCRIPT,
		AGENCY_CODE,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM Producer_code_stage
),
EXP_EDW_Convert_Agency_Key_Id_for_Producer_code AS (
	SELECT
	producer_code_stage_id AS producer_code_Stage_id,
	STATE_CODE AS IN_STATE_CODE,
	AGENCY_NUM AS IN_AGENCY_NUM,
	PRODUCER_CODE AS IN_PRODUCER_CODE,
	EMP_ID AS IN_EMP_ID,
	PRODUCER_DESCRIPT AS IN_PRODUCER_DESCRIPT,
	AGENCY_CODE AS IN_AGENCY_CODE,
	-- *INF*: iif(isnull(IN_STATE_CODE),'N/A',iif(IS_SPACES(IN_STATE_CODE),'N/A',IN_STATE_CODE))
	IFF(
	    IN_STATE_CODE IS NULL, 'N/A',
	    IFF(
	        LENGTH(IN_STATE_CODE)>0 AND TRIM(IN_STATE_CODE)='', 'N/A', IN_STATE_CODE
	    )
	) AS STATE_CODE,
	-- *INF*: iif(isnull(IN_AGENCY_NUM),'N/A',iif(IS_SPACES(IN_AGENCY_NUM),'N/A',IN_AGENCY_NUM))
	IFF(
	    IN_AGENCY_NUM IS NULL, 'N/A',
	    IFF(
	        LENGTH(IN_AGENCY_NUM)>0 AND TRIM(IN_AGENCY_NUM)='', 'N/A', IN_AGENCY_NUM
	    )
	) AS AGENCY_NUM,
	-- *INF*: TO_CHAR(IN_PRODUCER_CODE)
	TO_CHAR(IN_PRODUCER_CODE) AS v_producer_code,
	-- *INF*: rpad(v_producer_code,3)
	rpad(v_producer_code, 3) AS v_producer_code_pad,
	-- *INF*: iif(isnull(v_producer_code),'N/A',iif(IS_SPACES(v_producer_code),'N/A',v_producer_code))
	IFF(
	    v_producer_code IS NULL, 'N/A',
	    IFF(
	        LENGTH(v_producer_code)>0 AND TRIM(v_producer_code)='', 'N/A', v_producer_code
	    )
	) AS PRODUCER_CODE,
	v_producer_code_pad AS producer_code_out,
	-- *INF*: iif(isnull(IN_PRODUCER_DESCRIPT),'Not Available',iif(IS_SPACES(IN_PRODUCER_DESCRIPT),'Not Available',
	-- iif(length(IN_PRODUCER_DESCRIPT)=0,'Not Available',ltrim(rtrim(IN_PRODUCER_DESCRIPT)))))
	IFF(
	    IN_PRODUCER_DESCRIPT IS NULL, 'Not Available',
	    IFF(
	        LENGTH(IN_PRODUCER_DESCRIPT)>0
	    and TRIM(IN_PRODUCER_DESCRIPT)='', 'Not Available',
	        IFF(
	            length(IN_PRODUCER_DESCRIPT) = 0, 'Not Available',
	            ltrim(rtrim(IN_PRODUCER_DESCRIPT))
	        )
	    )
	) AS PRODUCER_DESCRIPT,
	-- *INF*: iif(isnull(IN_AGENCY_CODE),'N/A',iif(IS_SPACES(IN_AGENCY_CODE),'N/A',IN_AGENCY_CODE))
	IFF(
	    IN_AGENCY_CODE IS NULL, 'N/A',
	    IFF(
	        LENGTH(IN_AGENCY_CODE)>0 AND TRIM(IN_AGENCY_CODE)='', 'N/A', IN_AGENCY_CODE
	    )
	) AS AGENCY_CODE,
	IN_STATE_CODE || IN_AGENCY_NUM AS v_agency_key,
	STATE_CODE || AGENCY_NUM AS OUT_Agency_Key,
	-- *INF*: :LKP.LKP_AGENCY(v_agency_key)
	LKP_AGENCY_v_agency_key.agency_ak_id AS v_AGENCY_ak_ID,
	v_AGENCY_ak_ID AS OUT_AGENCY_ak_ID,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID
	FROM SQ_Producer_code_stage
	LEFT JOIN LKP_AGENCY LKP_AGENCY_v_agency_key
	ON LKP_AGENCY_v_agency_key.agency_key = v_agency_key

),
LKP_Producer_code AS (
	SELECT
	producer_code_id,
	prdcr_code_ak_id,
	producer_descript,
	agency_ak_id,
	producer_code,
	agency_key,
	emp_id
	FROM (
		SELECT a.producer_code_id as producer_code_id, 
		a.prdcr_code_ak_id as prdcr_code_ak_id,
		a.producer_descript as producer_descript, 
		a.agency_ak_id as agency_ak_id, 
		a.producer_code as producer_code, 
		a.agency_key as agency_key, 
		a.emp_id as emp_id 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.producer_code a
		WHERE a.producer_code_id in (SELECT MAX(b.producer_code_id)
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.producer_code b
			WHERE crrnt_snpsht_flag=1
			GROUP BY b.producer_code,b.agency_key,b.emp_id )
			ORDER BY a.producer_code,a.agency_key,a.emp_id
		
		--IN Subquery exists so that we only pick the MAX value of the PK for each AK Group
		--WHERE clause is always eff_to_date = '12/31/2100'
		--GROUP BY clause is always the AK
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer generate an ORDER BY statemen
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY producer_code,agency_key,emp_id ORDER BY producer_code_id) = 1
),
SEQ_Producer_code AS (
	CREATE SEQUENCE SEQ_Producer_code
	START = 0
	INCREMENT = 1;
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Producer_code.producer_code_id AS old_producer_code_id,
	LKP_Producer_code.prdcr_code_ak_id,
	LKP_Producer_code.producer_descript AS OLD_PRODUCER_DESCRIPT,
	LKP_Producer_code.agency_ak_id AS OLD_OUT_AGENCY_ak_ID,
	EXP_EDW_Convert_Agency_Key_Id_for_Producer_code.PRODUCER_DESCRIPT,
	EXP_EDW_Convert_Agency_Key_Id_for_Producer_code.OUT_AGENCY_ak_ID,
	-- *INF*: iif(isnull(old_producer_code_id),'NEW',
	-- 	iif(	(PRODUCER_DESCRIPT <> OLD_PRODUCER_DESCRIPT) or
	-- 	( OUT_AGENCY_ak_ID<> OLD_OUT_AGENCY_ak_ID),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	-- 
	-- 
	IFF(
	    old_producer_code_id IS NULL, 'NEW',
	    IFF(
	        (PRODUCER_DESCRIPT <> OLD_PRODUCER_DESCRIPT)
	    or (OUT_AGENCY_ak_ID <> OLD_OUT_AGENCY_ak_ID),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	1 AS Crrnt_SnapSht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	-- 
	-- --sysdate normally has a time value.  We don't want the time value as our effectivity runs from day to day starting at midnight
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS Eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_to_date,
	EXP_EDW_Convert_Agency_Key_Id_for_Producer_code.SOURCE_SYSTEM_ID,
	sysdate AS Created_date,
	sysdate AS Modified_date,
	v_changed_flag AS changed_flag,
	SEQ_Producer_code.NEXTVAL,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- NEXTVAL,
	-- prdcr_code_ak_id)
	IFF(v_changed_flag = 'NEW', NEXTVAL, prdcr_code_ak_id) AS out_prdcr_code_ak_id
	FROM EXP_EDW_Convert_Agency_Key_Id_for_Producer_code
	LEFT JOIN LKP_Producer_code
	ON LKP_Producer_code.producer_code = EXP_EDW_Convert_Agency_Key_Id_for_Producer_code.producer_code_out AND LKP_Producer_code.agency_key = EXP_EDW_Convert_Agency_Key_Id_for_Producer_code.OUT_Agency_Key AND LKP_Producer_code.emp_id = EXP_EDW_Convert_Agency_Key_Id_for_Producer_code.IN_EMP_ID
),
FIL_existing_Prdcr_Codes AS (
	SELECT
	EXP_Detect_Changes.out_prdcr_code_ak_id, 
	EXP_Detect_Changes.OUT_AGENCY_ak_ID AS OUT_AGENCY_AK_ID, 
	EXP_EDW_Convert_Agency_Key_Id_for_Producer_code.PRODUCER_CODE, 
	EXP_Detect_Changes.PRODUCER_DESCRIPT, 
	EXP_EDW_Convert_Agency_Key_Id_for_Producer_code.OUT_Agency_Key, 
	EXP_Detect_Changes.Crrnt_SnapSht_Flag, 
	EXP_Detect_Changes.Audit_id, 
	EXP_Detect_Changes.Eff_from_date, 
	EXP_Detect_Changes.Eff_to_date, 
	EXP_Detect_Changes.SOURCE_SYSTEM_ID, 
	EXP_Detect_Changes.Created_date, 
	EXP_Detect_Changes.Modified_date, 
	EXP_Detect_Changes.changed_flag, 
	EXP_EDW_Convert_Agency_Key_Id_for_Producer_code.IN_EMP_ID
	FROM EXP_Detect_Changes
	 -- Manually join with EXP_EDW_Convert_Agency_Key_Id_for_Producer_code
	WHERE changed_flag='NEW' or changed_flag = 'UPDATE'
),
Producer_code_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.producer_code
	(prdcr_code_ak_id, agency_ak_id, producer_code, producer_descript, agency_key, emp_id, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	out_prdcr_code_ak_id AS PRDCR_CODE_AK_ID, 
	OUT_AGENCY_AK_ID AS AGENCY_AK_ID, 
	PRODUCER_CODE AS PRODUCER_CODE, 
	PRODUCER_DESCRIPT AS PRODUCER_DESCRIPT, 
	OUT_Agency_Key AS AGENCY_KEY, 
	IN_EMP_ID AS EMP_ID, 
	Crrnt_SnapSht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_id AS AUDIT_ID, 
	Eff_from_date AS EFF_FROM_DATE, 
	Eff_to_date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_date AS CREATED_DATE, 
	Modified_date AS MODIFIED_DATE
	FROM FIL_existing_Prdcr_Codes
),
SQ_Producer_code AS (
	SELECT a.producer_code_id,
	a.producer_code,
	a.agency_key, 
	a.emp_id,
	a.eff_from_date,
	a.eff_to_date 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.producer_code a
	WHERE EXISTS(SELECT 1			
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Producer_Code b
		WHERE eff_to_date = '12/31/2100 23:59:59'
		AND a.producer_code = b.producer_code
		AND a.agency_key = b.agency_key
		AND a.emp_id = b.emp_id
		GROUP BY producer_code,agency_key,emp_id
		HAVING COUNT(*) > 1)
	ORDER BY producer_code,agency_key,emp_id,eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of eff_to_date='12/31/2100' and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the
	--eff_to_date by utilizing a local variable to keep track
),
EXP_UPD_Producer_Code AS (
	SELECT
	producer_code_id,
	producer_code,
	agency_key,
	emp_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE, producer_code = v_PREV_ROW_producer_code and agency_key = v_PREV_ROW_agency_key and emp_id = v_prev_row_emp_id  , ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    producer_code = v_PREV_ROW_producer_code and agency_key = v_PREV_ROW_agency_key and emp_id = v_prev_row_emp_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	producer_code AS v_PREV_ROW_producer_code,
	agency_key AS v_PREV_ROW_agency_key,
	emp_id AS v_prev_row_emp_id,
	0 AS Crrnt_snpsht_flag,
	sysdate AS modified_date
	FROM SQ_Producer_code
),
FIL_AGY_Prdcr_code_updates AS (
	SELECT
	producer_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	Crrnt_snpsht_flag, 
	modified_date
	FROM EXP_UPD_Producer_Code
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_Producer_Code AS (
	SELECT
	producer_code_id, 
	eff_to_date, 
	Crrnt_snpsht_flag, 
	modified_date
	FROM FIL_AGY_Prdcr_code_updates
),
producer_code_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.producer_code AS T
	USING UPD_Producer_Code AS S
	ON T.producer_code_id = S.producer_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.Crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),