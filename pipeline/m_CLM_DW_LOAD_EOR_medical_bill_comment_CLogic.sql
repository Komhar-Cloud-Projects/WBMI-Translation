WITH
SQ_med_bill_comment_stage AS (
	SELECT 
	RTRIM(med_bill_comment_stage.med_bill_id), 
	med_bill_comment_stage.comment_seq_num,
	CASE RTRIM(med_bill_comment_stage.comment_type) 
		WHEN '' THEN 'N/A' 
		ELSE RTRIM(med_bill_comment_stage.comment_type) END, 
	CASE RTRIM(med_bill_comment_stage.comment) 
		WHEN '' THEN 'N/A' 
		ELSE RTRIM(med_bill_comment_stage.comment) END 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.med_bill_comment_stage
	-- JIRA-PROD-3730 NPTolan Changed CASE statements for comment_type and comment columns to return 'N/A' instead of NULL
),
LKP_MED_BILL_KEY AS (
	SELECT
	med_bill_ak_id,
	med_bill_key,
	TCH_BILL_NBR
	FROM (
		SELECT 
		medical_bill.med_bill_ak_id as med_bill_ak_id, 
		RTRIM(medical_bill.med_bill_key) as med_bill_key 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill medical_bill
		WHERE
		medical_bill.CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_key ORDER BY med_bill_ak_id) = 1
),
LKP_MEDICAL_BILL_COMMENT AS (
	SELECT
	NewLookupRow,
	med_bill_comment_ak_id,
	in_med_bill_ak_id,
	med_bill_ak_id,
	in_comment_seq_num,
	comment_seq_num,
	comment_type,
	in_comment_type,
	comment,
	in_comment
	FROM (
		SELECT 
		medical_bill_comment.med_bill_comment_ak_id as med_bill_comment_ak_id, RTRIM(medical_bill_comment.comment_type) as comment_type, RTRIM(medical_bill_comment.comment) as comment, medical_bill_comment.med_bill_ak_id as med_bill_ak_id, medical_bill_comment.comment_seq_num as comment_seq_num 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_comment medical_bill_comment
		WHERE
		medical_bill_comment.CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_ak_id,comment_seq_num ORDER BY NewLookupRow) = 1
),
FIL_NEW_UNCHANGED_ROWS AS (
	SELECT
	NewLookupRow, 
	med_bill_ak_id, 
	comment_seq_num, 
	comment_type, 
	comment, 
	med_bill_comment_ak_id
	FROM LKP_MEDICAL_BILL_COMMENT
	WHERE NewLookupRow = 1 OR
NewLookupRow = 2
),
EXP_AUDIT_FIELDS AS (
	SELECT
	NewLookupRow,
	med_bill_ak_id,
	comment_seq_num,
	comment_type,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(NewLookupRow=1,
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(NewLookupRow = 1, to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	comment,
	med_bill_comment_ak_id
	FROM FIL_NEW_UNCHANGED_ROWS
),
medical_bill_comment_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_comment
	(med_bill_comment_ak_id, med_bill_ak_id, comment_seq_num, comment_type, comment, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	MED_BILL_COMMENT_AK_ID, 
	MED_BILL_AK_ID, 
	COMMENT_SEQ_NUM, 
	COMMENT_TYPE, 
	COMMENT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	created_date AS MODIFIED_DATE
	FROM EXP_AUDIT_FIELDS
),
SQ_medical_bill_comment AS (
	SELECT 
	medical_bill_comment.med_bill_comment_id, medical_bill_comment.med_bill_comment_ak_id, medical_bill_comment.eff_from_date, 
	medical_bill_comment.eff_to_date 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_comment AS medical_bill_comment
	WHERE
	medical_bill_comment.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND EXISTS
	(
	SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_comment AS medical_bill_comment2
	WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  and crrnt_snpsht_flag = 1 and
	medical_bill_comment2.med_bill_comment_ak_id = medical_bill_comment.med_bill_comment_ak_id 
	GROUP BY medical_bill_comment2.med_bill_comment_ak_id HAVING COUNT(*) > 1
	)
	order by medical_bill_comment.med_bill_comment_ak_id, medical_bill_comment.eff_from_date  desc
),
EXP_Lag_eff_from_date111 AS (
	SELECT
	med_bill_comment_id,
	med_bill_comment_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	med_bill_comment_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		med_bill_comment_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	med_bill_comment_ak_id AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_medical_bill_comment
),
FIL_First_Row_in_AK_Group AS (
	SELECT
	med_bill_comment_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date111
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_TO_DATE AS (
	SELECT
	med_bill_comment_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_First_Row_in_AK_Group
),
medical_bill_comment_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_comment AS T
	USING UPD_TO_DATE AS S
	ON T.med_bill_comment_id = S.med_bill_comment_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),