WITH
SQ_gtam_tl14_stage AS (
	SELECT reason_amended_code,
	       alpha_descr_of_reason_amended
	FROM gtam_tl14_stage
	
	---- Certain values('CZ','Q') that are filtered out because the 
	---- descriptions are NULL in the GTAMs and we have 
	---- static values for them
),
EXP_values_tl14 AS (
	SELECT
	reason_amended_code AS IN_reason_amended_code,
	-- *INF*: IIF(ISNULL(IN_reason_amended_code) OR IS_SPACES(IN_reason_amended_code) OR LENGTH(IN_reason_amended_code)=0
	-- ,'N/A'
	-- ,ltrim(rtrim(IN_reason_amended_code)))
	IFF(IN_reason_amended_code IS NULL OR IS_SPACES(IN_reason_amended_code) OR LENGTH(IN_reason_amended_code) = 0, 'N/A', ltrim(rtrim(IN_reason_amended_code))) AS reason_amended_code,
	alpha_descr_of_reason_amended AS IN_alpha_descr_of_reason_amended,
	-- *INF*: DECODE(TRUE,
	-- ltrim(rtrim(IN_reason_amended_code))='O',
	-- 'Other',
	-- ISNULL(IN_alpha_descr_of_reason_amended) OR IS_SPACES(IN_alpha_descr_of_reason_amended) OR
	-- LENGTH(IN_alpha_descr_of_reason_amended)=0,
	-- 'N/A',
	-- ltrim(rtrim(IN_alpha_descr_of_reason_amended))
	-- )
	DECODE(TRUE,
	ltrim(rtrim(IN_reason_amended_code)) = 'O', 'Other',
	IN_alpha_descr_of_reason_amended IS NULL OR IS_SPACES(IN_alpha_descr_of_reason_amended) OR LENGTH(IN_alpha_descr_of_reason_amended) = 0, 'N/A',
	ltrim(rtrim(IN_alpha_descr_of_reason_amended))) AS alpha_descr_of_reason_amended
	FROM SQ_gtam_tl14_stage
),
LKP_sup_reason_amended_code_tl14 AS (
	SELECT
	sup_rsn_amended_code_id,
	rsn_amended_code_descript,
	rsn_amended_code
	FROM (
		SELECT 
			tl.sup_rsn_amended_code_id as sup_rsn_amended_code_id, 
		LTRIM(RTRIM(tl.rsn_amended_code_descript))  as rsn_amended_code_descript,
		LTRIM(RTRIM(tl.rsn_amended_code)) as rsn_amended_code	
		FROM  
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code tl
		WHERE 
			tl.crrnt_snpsht_flag=1 and tl.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code ORDER BY sup_rsn_amended_code_id DESC) = 1
),
EXP_Detect_Changes_tl14 AS (
	SELECT
	EXP_values_tl14.reason_amended_code,
	EXP_values_tl14.alpha_descr_of_reason_amended,
	LKP_sup_reason_amended_code_tl14.sup_rsn_amended_code_id AS LKP_sup_rsn_amended_code_id,
	LKP_sup_reason_amended_code_tl14.rsn_amended_code_descript AS LKP_rsn_amended_code_descript,
	1 AS crrnt_snapshot_flag,
	-- *INF*: IIF(ISNULL(LKP_sup_rsn_amended_code_id),'NEW',
	-- 	IIF(
	-- 	(ltrim(rtrim(alpha_descr_of_reason_amended)) <> ltrim(rtrim(LKP_rsn_amended_code_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(LKP_sup_rsn_amended_code_id IS NULL, 'NEW', IFF(( ltrim(rtrim(alpha_descr_of_reason_amended)) <> ltrim(rtrim(LKP_rsn_amended_code_descript)) ), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
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
	FROM EXP_values_tl14
	LEFT JOIN LKP_sup_reason_amended_code_tl14
	ON LKP_sup_reason_amended_code_tl14.rsn_amended_code = EXP_values_tl14.reason_amended_code
),
FIL_insert_tl14 AS (
	SELECT
	reason_amended_code AS rsn_amended_code, 
	alpha_descr_of_reason_amended AS rsn_amended_code_descript, 
	crrnt_snapshot_flag, 
	Audit_ID, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_ID, 
	Created_Date, 
	Modified_Date
	FROM EXP_Detect_Changes_tl14
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
INS_sup_reason_amended_code_tl14 AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, rsn_amended_code, rsn_amended_code_descript, StandardReasonAmendedCode, StandardReasonAmendedDescription)
	SELECT 
	crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	RSN_AMENDED_CODE, 
	RSN_AMENDED_CODE_DESCRIPT, 
	rsn_amended_code AS STANDARDREASONAMENDEDCODE, 
	rsn_amended_code_descript AS STANDARDREASONAMENDEDDESCRIPTION
	FROM FIL_insert_tl14
),
SQ_gtam_wbrsnca_stage AS (
	SELECT cancellation_reason_code,
	       cancellation_reason_descript
	FROM gtam_wbrsnca_stage
	WHERE cancellation_reason_code NOT IN ('CZ','Q')
	
	
	
	---- Certain values('CZ','Q') that are filtered out because the 
	---- descriptions are NULL in the GTAMs and we have 
	---- static values for them
),
EXP_values_wbrsnca AS (
	SELECT
	cancellation_reason_code AS IN_cancellation_reason_code,
	-- *INF*: IIF(ISNULL(IN_cancellation_reason_code) OR IS_SPACES(IN_cancellation_reason_code) OR LENGTH(IN_cancellation_reason_code)=0
	-- ,'N/A'
	-- ,ltrim(rtrim(IN_cancellation_reason_code)))
	IFF(IN_cancellation_reason_code IS NULL OR IS_SPACES(IN_cancellation_reason_code) OR LENGTH(IN_cancellation_reason_code) = 0, 'N/A', ltrim(rtrim(IN_cancellation_reason_code))) AS cancellation_reason_code_code,
	cancellation_reason_descript AS IN_cancellation_reason_descript,
	-- *INF*: DECODE(TRUE,
	-- ltrim(rtrim(IN_cancellation_reason_code))='O',
	-- 'Other',
	-- ISNULL(IN_cancellation_reason_descript) OR IS_SPACES(IN_cancellation_reason_descript) OR
	-- LENGTH(IN_cancellation_reason_descript)=0,
	-- 'N/A',
	-- ltrim(rtrim(IN_cancellation_reason_descript))
	-- )
	DECODE(TRUE,
	ltrim(rtrim(IN_cancellation_reason_code)) = 'O', 'Other',
	IN_cancellation_reason_descript IS NULL OR IS_SPACES(IN_cancellation_reason_descript) OR LENGTH(IN_cancellation_reason_descript) = 0, 'N/A',
	ltrim(rtrim(IN_cancellation_reason_descript))) AS cancellation_reason_descript
	FROM SQ_gtam_wbrsnca_stage
),
LKP_sup_reason_amended_code_wbrsnca AS (
	SELECT
	sup_rsn_amended_code_id,
	rsn_amended_code_descript,
	rsn_amended_code
	FROM (
		SELECT 
			tl.sup_rsn_amended_code_id as sup_rsn_amended_code_id, 
		LTRIM(RTRIM(tl.rsn_amended_code_descript))  as rsn_amended_code_descript,
		LTRIM(RTRIM(tl.rsn_amended_code)) as rsn_amended_code
		FROM  
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code tl
		WHERE 
			tl.crrnt_snpsht_flag=1 and tl.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code ORDER BY sup_rsn_amended_code_id DESC) = 1
),
EXP_Detect_Changes_wbrsnca AS (
	SELECT
	EXP_values_wbrsnca.cancellation_reason_code_code AS reason_amended_code,
	EXP_values_wbrsnca.cancellation_reason_descript AS alpha_descr_of_reason_amended,
	LKP_sup_reason_amended_code_wbrsnca.sup_rsn_amended_code_id AS LKP_sup_rsn_amended_code_id,
	LKP_sup_reason_amended_code_wbrsnca.rsn_amended_code_descript AS LKP_rsn_amended_code_descript,
	1 AS crrnt_snapshot_flag,
	-- *INF*: IIF(ISNULL(LKP_sup_rsn_amended_code_id),'NEW',
	-- 	IIF(
	-- 	(ltrim(rtrim(alpha_descr_of_reason_amended)) <> ltrim(rtrim(LKP_rsn_amended_code_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(LKP_sup_rsn_amended_code_id IS NULL, 'NEW', IFF(( ltrim(rtrim(alpha_descr_of_reason_amended)) <> ltrim(rtrim(LKP_rsn_amended_code_descript)) ), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
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
	FROM EXP_values_wbrsnca
	LEFT JOIN LKP_sup_reason_amended_code_wbrsnca
	ON LKP_sup_reason_amended_code_wbrsnca.rsn_amended_code = EXP_values_wbrsnca.cancellation_reason_code_code
),
FIL_insert_wbrsnca AS (
	SELECT
	reason_amended_code AS rsn_amended_code, 
	alpha_descr_of_reason_amended AS rsn_amended_code_descript, 
	crrnt_snapshot_flag, 
	Audit_ID, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_ID, 
	Created_Date, 
	Modified_Date
	FROM EXP_Detect_Changes_wbrsnca
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
INS_sup_reason_amended_code_wbrsnca AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, rsn_amended_code, rsn_amended_code_descript, StandardReasonAmendedCode, StandardReasonAmendedDescription)
	SELECT 
	crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	RSN_AMENDED_CODE, 
	RSN_AMENDED_CODE_DESCRIPT, 
	rsn_amended_code AS STANDARDREASONAMENDEDCODE, 
	rsn_amended_code_descript AS STANDARDREASONAMENDEDDESCRIPTION
	FROM FIL_insert_wbrsnca
),
SQ_sup_reason_amended_code1 AS (
	SELECT 
		sup_rsn_amended_code_id, 
		eff_from_date, 
		eff_to_date, 
		rsn_amended_code 
	FROM
	 	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code
	WHERE rsn_amended_code IN 
		   (SELECT rsn_amended_code FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code
	           WHERE crrnt_snpsht_flag = 1 GROUP BY rsn_amended_code,source_sys_id HAVING count(*) > 1)
	ORDER BY  rsn_amended_code, eff_from_date  DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	sup_rsn_amended_code_id,
	rsn_amended_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	rsn_amended_code = v_PREV_ROW_rsn_amended_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
	rsn_amended_code = v_PREV_ROW_rsn_amended_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	rsn_amended_code AS v_PREV_ROW_rsn_amended_code,
	SYSDATE AS modified_date,
	0 AS crrnt_snapshot_flag
	FROM SQ_sup_reason_amended_code1
),
FIL_FirstRowInAKGroup AS (
	SELECT
	sup_rsn_amended_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snapshot_flag
	FROM EXP_Lag_Eff_dates
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_Sup_Reason_Amended_Code AS (
	SELECT
	sup_rsn_amended_code_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snapshot_flag
	FROM FIL_FirstRowInAKGroup
),
UPD_sup_reason_amended_code AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code AS T
	USING UPD_Sup_Reason_Amended_Code AS S
	ON T.sup_rsn_amended_code_id = S.sup_rsn_amended_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snapshot_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),