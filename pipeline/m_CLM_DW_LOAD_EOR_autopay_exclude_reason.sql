WITH
SQ_eor_excl_reason_stage AS (
	SELECT c.med_bill_id
	                  ,c.autopay_excl_rsn_code
	  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.eor_excl_reason_stage c
),
EXP_LKP AS (
	SELECT
	med_bill_id,
	autopay_excl_rsn_code,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(autopay_excl_rsn_code))),'N/A',autopay_excl_rsn_code)
	IFF(RTRIM(LTRIM(autopay_excl_rsn_code)) IS NULL, 'N/A', autopay_excl_rsn_code) AS reason_code_out
	FROM SQ_eor_excl_reason_stage
),
LKP_MEDICAL_BILL AS (
	SELECT
	med_bill_ak_id,
	med_bill_key
	FROM (
		SELECT  med_bill_key as  med_bill_key,
		 med_bill_ak_id 	AS med_bill_ak_id
		FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill
		WHERE	crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_key ORDER BY med_bill_ak_id DESC) = 1
),
EXP_LKP2 AS (
	SELECT
	LKP_MEDICAL_BILL.med_bill_ak_id,
	EXP_LKP.reason_code_out AS autopay_reason_code_out
	FROM EXP_LKP
	LEFT JOIN LKP_MEDICAL_BILL
	ON LKP_MEDICAL_BILL.med_bill_key = EXP_LKP.med_bill_id
),
LKP_EOR_AUTOPAY_REASON_TGT AS (
	SELECT
	eor_autopay_excl_rsn_ak_id,
	autopay_excl_rsn_code,
	med_bill_ak_id
	FROM (
		SELECT	eor_autopay_excl_rsn_ak_id	AS eor_autopay_excl_rsn_ak_id
		,		      med_bill_ak_id as  med_bill_ak_id
		,                 autopay_excl_rsn_code as  autopay_excl_rsn_code
		FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason
		WHERE	crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY autopay_excl_rsn_code,med_bill_ak_id ORDER BY eor_autopay_excl_rsn_ak_id DESC) = 1
),
EXP_AUDIT_FIELDS AS (
	SELECT
	LKP_EOR_AUTOPAY_REASON_TGT.eor_autopay_excl_rsn_ak_id AS LKP_eor_autopay_excl_rsn_ak_id,
	EXP_LKP2.med_bill_ak_id,
	EXP_LKP2.autopay_reason_code_out AS autopay_excl_rsn_code,
	1 AS current_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif( ISNULL(LKP_eor_autopay_excl_rsn_ak_id), 'INSERT', 'NOINSERT')
	-- 	 
	IFF(LKP_eor_autopay_excl_rsn_ak_id IS NULL, 'INSERT', 'NOINSERT') AS CHANGE_FLAG,
	-- *INF*: to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS CREATE_MOD_DATE
	FROM EXP_LKP2
	LEFT JOIN LKP_EOR_AUTOPAY_REASON_TGT
	ON LKP_EOR_AUTOPAY_REASON_TGT.autopay_excl_rsn_code = EXP_LKP2.autopay_reason_code_out AND LKP_EOR_AUTOPAY_REASON_TGT.med_bill_ak_id = EXP_LKP2.med_bill_ak_id
),
FIL_NEW_CHANGED_ROWS1 AS (
	SELECT
	med_bill_ak_id, 
	autopay_excl_rsn_code, 
	current_snpsht_flag, 
	audit_id, 
	CHANGE_FLAG, 
	eff_from_date, 
	eff_to_date, 
	SOURCE_SYSTEM_ID, 
	CREATE_MOD_DATE
	FROM EXP_AUDIT_FIELDS
	WHERE CHANGE_FLAG = 'INSERT'
),
SEQ_EOR_Excl_Rsn_AK AS (
	CREATE SEQUENCE SEQ_EOR_Excl_Rsn_AK
	START = 0
	INCREMENT = 1;
),
eor_autopay_exclude_reason_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason
	(eor_autopay_excl_rsn_ak_id, med_bill_ak_id, autopay_excl_rsn_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_EOR_Excl_Rsn_AK.NEXTVAL AS EOR_AUTOPAY_EXCL_RSN_AK_ID, 
	MED_BILL_AK_ID, 
	AUTOPAY_EXCL_RSN_CODE, 
	current_snpsht_flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATE_MOD_DATE AS CREATED_DATE, 
	CREATE_MOD_DATE AS MODIFIED_DATE
	FROM FIL_NEW_CHANGED_ROWS1
),
SQ_eor_autopay_exclude_reason AS (
	SELECT eor_autopay_excl_rsn_id
	,  eor_autopay_excl_rsn_ak_id
	,		eff_from_date
	,		eff_to_date 
	 
	FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason MBV
	WHERE	source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND		EXISTS
			(select 1
			FROM	@{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason MBV2
			WHERE	source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			AND		crrnt_snpsht_flag = 1
			AND		MBV2.eor_autopay_excl_rsn_ak_id = MBV.eor_autopay_excl_rsn_ak_id 
			GROUP	BY	MBV2.eor_autopay_excl_rsn_ak_id
			HAVING	count(*) > 1
	)
	ORDER	BY eor_autopay_excl_rsn_ak_id
	,		eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	eor_autopay_excl_rsn_id,
	eor_autopay_excl_rsn_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	eor_autopay_excl_rsn_ak_id = v_PREV_ROW_eor_autopay_excl_rsn_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    eor_autopay_excl_rsn_ak_id = v_PREV_ROW_eor_autopay_excl_rsn_ak_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	eor_autopay_excl_rsn_ak_id AS v_PREV_ROW_eor_autopay_excl_rsn_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_eor_autopay_exclude_reason
),
FIL_First_Row_in_AK_Group AS (
	SELECT
	eor_autopay_excl_rsn_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_EOR_autopay_exclude_reason AS (
	SELECT
	eor_autopay_excl_rsn_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_First_Row_in_AK_Group
),
eor_autopay_exclude_reason_UPD AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason AS T
	USING UPD_EOR_autopay_exclude_reason AS S
	ON T.eor_autopay_excl_rsn_id = S.eor_autopay_excl_rsn_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),