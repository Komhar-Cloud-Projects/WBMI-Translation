WITH
SQ_eor_autopay_exclude_reason AS (
	SELECT 
	CPO.eor_autopay_excl_rsn_id, 
	CPO.eor_autopay_excl_rsn_ak_id, 
	CPO.med_bill_ak_id, 
	CPO.autopay_excl_rsn_code,
	CPO.eff_from_date 
	FROM  
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.eor_autopay_exclude_reason CPO 
	WHERE created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_default AS (
	SELECT
	med_bill_ak_id,
	autopay_excl_rsn_code,
	-- *INF*: LTRIM(RTRIM(autopay_excl_rsn_code))
	-- 
	-- 
	LTRIM(RTRIM(autopay_excl_rsn_code)) AS v_autopay_excl_rsn_code,
	v_autopay_excl_rsn_code AS autopay_excl_rsn_code_out,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	eor_autopay_excl_rsn_id,
	eor_autopay_excl_rsn_ak_id
	FROM SQ_eor_autopay_exclude_reason
),
LKP_Med_Bill_Dim_Id AS (
	SELECT
	med_bill_dim_id,
	edw_med_bill_ak_id
	FROM (
		SELECT A.med_bill_dim_id as med_bill_dim_id, A.edw_med_bill_ak_id as edw_med_bill_ak_id 
		 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_dim A
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_med_bill_ak_id ORDER BY med_bill_dim_id DESC) = 1
),
LKP_sup_autopay_exclude_reason AS (
	SELECT
	autopay_excl_rsn_code,
	autopay_excl_rsn_descript,
	excl_from_manualpay
	FROM (
		SELECT  B.autopay_excl_rsn_code AS autopay_excl_rsn_code,
		                     B.autopay_excl_rsn_descript AS autopay_excl_rsn_descript ,
		                   B.excl_from_manualpay  AS excl_from_manualpay
		                   
		FROM (
		SELECT  A.autopay_excl_rsn_descript AS autopay_excl_rsn_descript,  
		                   A. excl_from_manualpay   AS excl_from_manualpay,  
		                   LTRIM(RTRIM(A.autopay_excl_rsn_code) )AS autopay_excl_rsn_code 
		 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_eor_autopay_exclude_reason A
		WHERE crrnt_snpsht_flag =1  
		 --- ORDER BY autopay_excl_rsn_code,autopay_excl_rsn_descript,excl_from_manualpay in transform LKP_sup_autopay_exclude_reason
		) B
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY autopay_excl_rsn_code ORDER BY autopay_excl_rsn_code DESC) = 1
),
EXP_LKP AS (
	SELECT
	EXP_default.crrnt_snpsht_flag,
	EXP_default.audit_id,
	EXP_default.eff_from_date,
	EXP_default.eff_to_date,
	EXP_default.created_date,
	EXP_default.modified_date,
	EXP_default.eor_autopay_excl_rsn_id AS edw_eor_autopay_excl_rsn_id,
	EXP_default.eor_autopay_excl_rsn_ak_id AS edw_eor_autopay_excl_rsn_ak_id,
	EXP_default.med_bill_ak_id AS edw_med_bill_ak_id,
	LKP_sup_autopay_exclude_reason.autopay_excl_rsn_code,
	LKP_sup_autopay_exclude_reason.autopay_excl_rsn_descript,
	LKP_sup_autopay_exclude_reason.excl_from_manualpay,
	LKP_Med_Bill_Dim_Id.med_bill_dim_id
	FROM EXP_default
	LEFT JOIN LKP_Med_Bill_Dim_Id
	ON LKP_Med_Bill_Dim_Id.edw_med_bill_ak_id = EXP_default.med_bill_ak_id
	LEFT JOIN LKP_sup_autopay_exclude_reason
	ON LKP_sup_autopay_exclude_reason.autopay_excl_rsn_code = EXP_default.autopay_excl_rsn_code_out
),
LKP_eor_autopay_exclude_reason_dim AS (
	SELECT
	eor_autopay_excl_rsn_dim_id,
	edw_eor_autopay_excl_rsn_ak_id
	FROM (
		SELECT 
		A.eor_autopay_excl_rsn_dim_id as eor_autopay_excl_rsn_dim_id, 
		A.edw_eor_autopay_excl_rsn_ak_id as edw_eor_autopay_excl_rsn_ak_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason_dim A
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_eor_autopay_excl_rsn_ak_id ORDER BY eor_autopay_excl_rsn_dim_id DESC) = 1
),
RTR_INSERT_UPDATE AS (
	SELECT
	LKP_eor_autopay_exclude_reason_dim.eor_autopay_excl_rsn_dim_id,
	EXP_LKP.crrnt_snpsht_flag,
	EXP_LKP.audit_id,
	EXP_LKP.eff_from_date,
	EXP_LKP.eff_to_date,
	EXP_LKP.created_date,
	EXP_LKP.modified_date,
	EXP_LKP.edw_eor_autopay_excl_rsn_id AS eor_autopay_excl_rsn_pk_id,
	EXP_LKP.edw_eor_autopay_excl_rsn_ak_id,
	LKP_Med_Bill_Dim_Id.med_bill_dim_id,
	EXP_LKP.autopay_excl_rsn_code,
	EXP_LKP.autopay_excl_rsn_descript,
	EXP_LKP.excl_from_manualpay
	FROM EXP_LKP
	LEFT JOIN LKP_Med_Bill_Dim_Id
	ON LKP_Med_Bill_Dim_Id.edw_med_bill_ak_id = EXP_default.med_bill_ak_id
	LEFT JOIN LKP_eor_autopay_exclude_reason_dim
	ON LKP_eor_autopay_exclude_reason_dim.edw_eor_autopay_excl_rsn_ak_id = EXP_LKP.edw_eor_autopay_excl_rsn_ak_id
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE IIF(ISNULL(eor_autopay_excl_rsn_dim_id),TRUE,FALSE)),
RTR_INSERT_UPDATE_DEFAULT1 AS (SELECT * FROM RTR_INSERT_UPDATE WHERE NOT ( (IIF(ISNULL(eor_autopay_excl_rsn_dim_id),TRUE,FALSE)) )),
UPD_update AS (
	SELECT
	eor_autopay_excl_rsn_dim_id AS eor_autopay_excl_rsn_dim_id2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	created_date AS created_date2, 
	modified_date AS modified_date2, 
	eor_autopay_excl_rsn_pk_id AS eor_autopay_excl_rsn_pk_id2, 
	edw_eor_autopay_excl_rsn_ak_id AS edw_eor_autopay_excl_rsn_ak_id2, 
	med_bill_dim_id AS med_bill_dim_id2, 
	autopay_excl_rsn_code AS autopay_excl_rsn_code2, 
	autopay_excl_rsn_descript AS autopay_excl_rsn_descript2, 
	excl_from_manualpay AS excl_from_manualpay2
	FROM RTR_INSERT_UPDATE_DEFAULT1
),
eor_autopay_exclude_reason_dim_UPD2 AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason_dim AS T
	USING UPD_update AS S
	ON T.eor_autopay_excl_rsn_dim_id = S.eor_autopay_excl_rsn_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.created_date = S.created_date2, T.modified_date = S.modified_date2, T.edw_eor_autopay_excl_rsn_pk_id = S.eor_autopay_excl_rsn_pk_id2, T.edw_eor_autopay_excl_rsn_ak_id = S.edw_eor_autopay_excl_rsn_ak_id2, T.med_bill_dim_id = S.med_bill_dim_id2, T.autopay_excl_rsn_code = S.autopay_excl_rsn_code2, T.autopay_excl_rsn_descript = S.autopay_excl_rsn_descript2, T.excl_from_manualpay = S.excl_from_manualpay2
),
UPD_Insert AS (
	SELECT
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	eor_autopay_excl_rsn_pk_id AS eor_autopay_excl_rsn_pk_id1, 
	edw_eor_autopay_excl_rsn_ak_id AS edw_eor_autopay_excl_rsn_ak_id1, 
	med_bill_dim_id AS med_bill_dim_id1, 
	autopay_excl_rsn_code AS autopay_excl_rsn_code1, 
	autopay_excl_rsn_descript AS autopay_excl_rsn_descript1, 
	excl_from_manualpay AS excl_from_manualpay1
	FROM RTR_INSERT_UPDATE_INSERT
),
eor_autopay_exclude_reason_dim_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, edw_eor_autopay_excl_rsn_pk_id, edw_eor_autopay_excl_rsn_ak_id, med_bill_dim_id, autopay_excl_rsn_code, autopay_excl_rsn_descript, excl_from_manualpay)
	SELECT 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	eor_autopay_excl_rsn_pk_id1 AS EDW_EOR_AUTOPAY_EXCL_RSN_PK_ID, 
	edw_eor_autopay_excl_rsn_ak_id1 AS EDW_EOR_AUTOPAY_EXCL_RSN_AK_ID, 
	med_bill_dim_id1 AS MED_BILL_DIM_ID, 
	autopay_excl_rsn_code1 AS AUTOPAY_EXCL_RSN_CODE, 
	autopay_excl_rsn_descript1 AS AUTOPAY_EXCL_RSN_DESCRIPT, 
	excl_from_manualpay1 AS EXCL_FROM_MANUALPAY
	FROM UPD_Insert
),
SQ_eor_autopay_exclude_reason_dim AS (
	SELECT A.eor_autopay_excl_rsn_dim_id, 
	                          A.eff_from_date, 
	               A.eff_to_date ,
	        A.edw_eor_autopay_excl_rsn_ak_id
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason_dim A  
	WHERE 
	EXISTS
	(
	SELECT  1 FROM 
	@{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason_dim B 
	WHERE CRRNT_SNPSHT_FLAG = 1 AND  
	A.edw_eor_autopay_excl_rsn_ak_id = B.edw_eor_autopay_excl_rsn_ak_id
	GROUP BY   B.edw_eor_autopay_excl_rsn_ak_id
	HAVING COUNT(*) > 1
	)
	ORDER BY   A.edw_eor_autopay_excl_rsn_ak_id,
	  A.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	eor_autopay_excl_rsn_dim_id,
	edw_eor_autopay_excl_rsn_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,	
	--       edw_eor_autopay_excl_rsn_ak_id = v_PREV_ROW_edw_eor_autopay_excl_rsn_ak_id    
	--        , ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
	edw_eor_autopay_excl_rsn_ak_id = v_PREV_ROW_edw_eor_autopay_excl_rsn_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_eor_autopay_excl_rsn_ak_id AS v_PREV_ROW_edw_eor_autopay_excl_rsn_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_eor_autopay_exclude_reason_dim
),
FILTRANS AS (
	SELECT
	eor_autopay_excl_rsn_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_EFF_TO_DATE AS (
	SELECT
	eor_autopay_excl_rsn_dim_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FILTRANS
),
eor_autopay_exclude_reason_dim_UPD1 AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.eor_autopay_exclude_reason_dim AS T
	USING UPD_EFF_TO_DATE AS S
	ON T.eor_autopay_excl_rsn_dim_id = S.eor_autopay_excl_rsn_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),