WITH
SQ_gtam_wbsiccod_stage AS (
	SELECT
		gtam_wbsiccod_stage_id,
		sic_code_number,
		sic_code_description,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbsiccod_stage
	WHERE 1=0
),
EXP_values AS (
	SELECT
	sic_code_number AS in_sic_code_number,
	-- *INF*: IIF(ISNULL(in_sic_code_number) OR IS_SPACES(in_sic_code_number) OR LENGTH(in_sic_code_number)=0
	-- ,'N/A'
	-- ,ltrim(rtrim(in_sic_code_number)))
	IFF(in_sic_code_number IS NULL OR IS_SPACES(in_sic_code_number) OR LENGTH(in_sic_code_number) = 0, 'N/A', ltrim(rtrim(in_sic_code_number))) AS sic_code_number,
	sic_code_description AS in_sic_code_description,
	-- *INF*: IIF(ISNULL(in_sic_code_description) OR IS_SPACES(in_sic_code_description) OR
	-- LENGTH(in_sic_code_description)=0,'N/A',UPPER(ltrim(rtrim(in_sic_code_description))))
	IFF(in_sic_code_description IS NULL OR IS_SPACES(in_sic_code_description) OR LENGTH(in_sic_code_description) = 0, 'N/A', UPPER(ltrim(rtrim(in_sic_code_description)))) AS sic_code_description
	FROM SQ_gtam_wbsiccod_stage
),
LKP_sup_sic_code AS (
	SELECT
	sup_sic_code_id,
	sic_code_descript,
	sic_code
	FROM (
		SELECT 
			sup_sic_code.sup_sic_code_id as sup_sic_code_id, 
			sup_sic_code.sic_code_descript as sic_code_descript, 
			LTRIM(RTRIM(sup_sic_code.sic_code)) as sic_code 
		FROM  
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_sic_code
		WHERE 
			sup_sic_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sic_code ORDER BY sup_sic_code_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_values.sic_code_number,
	EXP_values.sic_code_description,
	LKP_sup_sic_code.sup_sic_code_id AS lkp_sup_sic_code_id,
	LKP_sup_sic_code.sic_code_descript AS lkp_sic_code_description,
	1 AS crrnt_snapshot_flag,
	-- *INF*: IIF(ISNULL(lkp_sup_sic_code_id),'NEW',
	-- 	IIF(
	-- 	(ltrim(rtrim(sic_code_description)) <> UPPER(ltrim(rtrim(lkp_sic_code_description)))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(lkp_sup_sic_code_id IS NULL, 'NEW', IFF(( ltrim(rtrim(sic_code_description)) <> UPPER(ltrim(rtrim(lkp_sic_code_description))) ), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(v_Changed_Flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'), 'MM/DD/YYYY HH24:MI:SS')) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_System_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date
	FROM EXP_values
	LEFT JOIN LKP_sup_sic_code
	ON LKP_sup_sic_code.sic_code = EXP_values.sic_code_number
),
FIL_insert AS (
	SELECT
	sic_code_number, 
	sic_code_description, 
	crrnt_snapshot_flag, 
	Audit_ID, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_ID, 
	Created_Date, 
	Modified_Date
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
TGT_sup_sic_code_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_sic_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, sic_code, sic_code_descript)
	SELECT 
	crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	sic_code_number AS SIC_CODE, 
	sic_code_description AS SIC_CODE_DESCRIPT
	FROM FIL_insert
),
SQ_DCPolicyStaging AS (
	SELECT DISTINCT A.SICCode, 
	A.SICCodeDesc 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging A
	WHERE A.SICCode IS NOT NULL AND LTRIM(RTRIM(A.SICCode))<>'' AND A.SICCodeDesc IS NOT NULL AND LTRIM(RTRIM(A.SICCodeDesc))<>''
	AND LEN(LTRIM(RTRIM(A.SICCode)))<=4
	AND  A.SessionId = (SELECT MAX(B.SessionId) FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging B where RIGHT('0000'+LTRIM(RTRIM(B.SICCode)), 4) = RIGHT('0000'+LTRIM(RTRIM(A.SICCode)), 4))
),
EXP_GetValues_DCT AS (
	SELECT
	SICCode AS i_SICCode,
	SICCodeDesc AS i_SICCodeDesc,
	-- *INF*: LTRIM(RTRIM(i_SICCode))
	LTRIM(RTRIM(i_SICCode)) AS v_SICCode,
	-- *INF*: UPPER(LTRIM(RTRIM(i_SICCodeDesc)))
	UPPER(LTRIM(RTRIM(i_SICCodeDesc))) AS v_SICCodeDesc,
	'[' || v_SICCode || ']' AS v_SICCode2,
	-- *INF*: LPAD(v_SICCode, 4, '0')
	LPAD(v_SICCode, 4, '0') AS sic_code_number,
	-- *INF*: IIF(INSTR(v_SICCodeDesc, v_SICCode2)=0, v_SICCodeDesc, LTRIM(RTRIM(REPLACESTR(0, v_SICCodeDesc, v_SICCode2, ''))))
	IFF(INSTR(v_SICCodeDesc, v_SICCode2) = 0, v_SICCodeDesc, LTRIM(RTRIM(REPLACESTR(0, v_SICCodeDesc, v_SICCode2, '')))) AS sic_code_description
	FROM SQ_DCPolicyStaging
),
LKP_sup_sic_code_DCT_new AS (
	SELECT
	sup_sic_code_id,
	sic_code_descript,
	sic_code
	FROM (
		SELECT 
			sup_sic_code_id,
			sic_code_descript,
			sic_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_sic_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sic_code ORDER BY sup_sic_code_id) = 1
),
EXP_Detect_Changes_DCT AS (
	SELECT
	LKP_sup_sic_code_DCT_new.sic_code_descript AS lkp_sic_code_description,
	LKP_sup_sic_code_DCT_new.sup_sic_code_id AS lkp_sup_sic_code_id,
	EXP_GetValues_DCT.sic_code_number,
	EXP_GetValues_DCT.sic_code_description,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_sic_code_description),
	-- 'NEW',
	-- UPPER(LTRIM(RTRIM(lkp_sic_code_description))) <> sic_code_description,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
	lkp_sic_code_description IS NULL, 'NEW',
	UPPER(LTRIM(RTRIM(lkp_sic_code_description))) <> sic_code_description, 'UPDATE',
	'NOCHANGE') AS v_Changed_Flag,
	1 AS o_crrnt_snapshot_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_Audit_ID,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_Changed_Flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS o_Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_Eff_To_Date,
	'DCT' AS o_Source_System_ID,
	SYSDATE AS o_Created_Date,
	SYSDATE AS o_Modified_Date,
	v_Changed_Flag AS o_Changed_Flag
	FROM EXP_GetValues_DCT
	LEFT JOIN LKP_sup_sic_code_DCT_new
	ON LKP_sup_sic_code_DCT_new.sic_code = EXP_GetValues_DCT.sic_code_number
),
RTRTRANS AS (
	SELECT
	lkp_sup_sic_code_id,
	sic_code_number,
	sic_code_description,
	o_crrnt_snapshot_flag,
	o_Audit_ID,
	o_Eff_From_Date,
	o_Eff_To_Date,
	o_Source_System_ID,
	o_Created_Date,
	o_Modified_Date,
	o_Changed_Flag
	FROM EXP_Detect_Changes_DCT
),
RTRTRANS_INSERT AS (SELECT * FROM RTRTRANS WHERE o_Changed_Flag= 'NEW'),
RTRTRANS_UPDATE AS (SELECT * FROM RTRTRANS WHERE o_Changed_Flag='UPDATE'),
UPD_sup_sic_code_desc AS (
	SELECT
	lkp_sup_sic_code_id AS lkp_sup_sic_code_id3, 
	o_Modified_Date AS o_Modified_Date3, 
	sic_code_number AS sic_code_number3, 
	sic_code_description AS sic_code_description3
	FROM RTRTRANS_UPDATE
),
TGT_sup_sic_code_UPD_DCT AS (
	MERGE INTO sup_sic_code AS T
	USING UPD_sup_sic_code_desc AS S
	ON T.sup_sic_code_id = S.lkp_sup_sic_code_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.modified_date = S.o_Modified_Date3, T.sic_code = S.sic_code_number3, T.sic_code_descript = S.sic_code_description3
),
TGT_sup_sic_code_INSERT_DCT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_sic_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, sic_code, sic_code_descript)
	SELECT 
	o_crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	o_Audit_ID AS AUDIT_ID, 
	o_Eff_From_Date AS EFF_FROM_DATE, 
	o_Eff_To_Date AS EFF_TO_DATE, 
	o_Source_System_ID AS SOURCE_SYS_ID, 
	o_Created_Date AS CREATED_DATE, 
	o_Modified_Date AS MODIFIED_DATE, 
	sic_code_number AS SIC_CODE, 
	sic_code_description AS SIC_CODE_DESCRIPT
	FROM RTRTRANS_INSERT
),
SQ_sup_sic_code AS (
	SELECT 
		sup_sic_code_id, 
		eff_from_date, 
		eff_to_date, 
		sic_code 
	FROM
	 	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_sic_code
	WHERE sic_code IN 
		   (SELECT sic_code FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_sic_code
	           WHERE crrnt_snpsht_flag = 1 GROUP BY sic_code HAVING count(*) > 1)
	ORDER BY  sic_code, eff_from_date  DESC
	
	
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_Eff_dates AS (
	SELECT
	sup_sic_code_id,
	sic_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	sic_code = v_PREV_ROW_sic_code_number, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
	sic_code = v_PREV_ROW_sic_code_number, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	sic_code AS v_PREV_ROW_sic_code_number,
	SYSDATE AS modified_date,
	0 AS crrnt_snapshot_flag
	FROM SQ_sup_sic_code
),
FIL_FirstRowInAKGroup AS (
	SELECT
	sup_sic_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snapshot_flag
	FROM EXP_Lag_Eff_dates
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_Sup_Sic_Code AS (
	SELECT
	sup_sic_code_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snapshot_flag
	FROM FIL_FirstRowInAKGroup
),
TGT_sup_sic_code_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_sic_code AS T
	USING UPD_Sup_Sic_Code AS S
	ON T.sup_sic_code_id = S.sup_sic_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snapshot_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),