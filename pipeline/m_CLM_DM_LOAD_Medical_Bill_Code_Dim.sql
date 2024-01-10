WITH
SQ_medical_bill_code AS (
	SELECT 
	medical_bill_code.med_bill_code_id, 
	medical_bill_code.med_bill_code_ak_id, 
	medical_bill_code.med_bill_ak_id, 
	medical_bill_code.med_bill_code_type, 
	medical_bill_code.med_bill_code, 
	medical_bill_code.med_bill_code_descript, 
	medical_bill_code.med_bill_date, 
	medical_bill.med_bill_id 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.medical_bill_code medical_bill_code, 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.medical_bill medical_bill
	WHERE
	medical_bill_code.med_bill_ak_id = medical_bill.med_bill_ak_id and
	medical_bill_code.created_date >= '@{pipeline().parameters.SELECTION_START_TS}' and
	medical_bill.crrnt_snpsht_flag = 1 and medical_bill_code.med_bill_serv_ak_id = 0
),
LKP_MED_BILL_DIM AS (
	SELECT
	med_bill_dim_id,
	edw_med_bill_ak_id,
	med_bill_ak_id
	FROM (
		SELECT 
			med_bill_dim_id,
			edw_med_bill_ak_id,
			med_bill_ak_id
		FROM medical_bill_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_med_bill_ak_id ORDER BY med_bill_dim_id) = 1
),
EXPTRANS AS (
	SELECT
	SQ_medical_bill_code.med_bill_id,
	SQ_medical_bill_code.med_bill_code_ak_id,
	LKP_MED_BILL_DIM.med_bill_dim_id,
	SQ_medical_bill_code.med_bill_code_type,
	SQ_medical_bill_code.med_bill_code,
	SQ_medical_bill_code.med_bill_code_descript,
	SQ_medical_bill_code.med_bill_date,
	1 AS crrnt_snpsht_flag,
	-- *INF*: to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	SQ_medical_bill_code.med_bill_code_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_medical_bill_code
	LEFT JOIN LKP_MED_BILL_DIM
	ON LKP_MED_BILL_DIM.edw_med_bill_ak_id = SQ_medical_bill_code.med_bill_ak_id
),
LKP_MED_BILL_CODE_DIM_Exists AS (
	SELECT
	med_bill_code_dim_id,
	edw_med_bill_code_pk_id,
	edw_med_bill_code_ak_id,
	med_bill_dim_id,
	med_bill_code_type,
	med_bill_code,
	med_bill_code_descript,
	med_bill_date,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	med_bill_code_ak_id,
	med_bill_code_type1,
	med_bill_code1,
	med_bill_date1,
	med_bill_code_descript1
	FROM (
		SELECT 
			med_bill_code_dim_id,
			edw_med_bill_code_pk_id,
			edw_med_bill_code_ak_id,
			med_bill_dim_id,
			med_bill_code_type,
			med_bill_code,
			med_bill_code_descript,
			med_bill_date,
			crrnt_snpsht_flag,
			audit_id,
			eff_from_date,
			eff_to_date,
			created_date,
			modified_date,
			med_bill_code_ak_id,
			med_bill_code_type1,
			med_bill_code1,
			med_bill_date1,
			med_bill_code_descript1
		FROM medical_bill_code_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_med_bill_code_ak_id,med_bill_code_type,med_bill_code,med_bill_date,med_bill_code_descript ORDER BY med_bill_code_dim_id) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_MED_BILL_CODE_DIM_Exists.med_bill_code_dim_id,
	EXPTRANS.med_bill_id,
	EXPTRANS.med_bill_code_ak_id,
	EXPTRANS.med_bill_dim_id,
	EXPTRANS.med_bill_code_type,
	EXPTRANS.med_bill_code,
	EXPTRANS.med_bill_code_descript,
	EXPTRANS.med_bill_date,
	EXPTRANS.crrnt_snpsht_flag,
	EXPTRANS.eff_from_date,
	EXPTRANS.eff_to_date,
	EXPTRANS.created_date,
	EXPTRANS.modified_date,
	EXPTRANS.med_bill_code_id,
	EXPTRANS.audit_id
	FROM EXPTRANS
	LEFT JOIN LKP_MED_BILL_CODE_DIM_Exists
	ON LKP_MED_BILL_CODE_DIM_Exists.edw_med_bill_code_ak_id = EXPTRANS.med_bill_code_ak_id AND LKP_MED_BILL_CODE_DIM_Exists.med_bill_code_type = EXPTRANS.med_bill_code_type AND LKP_MED_BILL_CODE_DIM_Exists.med_bill_code = EXPTRANS.med_bill_code AND LKP_MED_BILL_CODE_DIM_Exists.med_bill_date = EXPTRANS.med_bill_date AND LKP_MED_BILL_CODE_DIM_Exists.med_bill_code_descript = EXPTRANS.med_bill_code_descript
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(med_bill_code_dim_id)),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (ISNULL(med_bill_code_dim_id)) )),
UPD_MED_BILL_CODE_DIM AS (
	SELECT
	med_bill_code_dim_id AS med_bill_code_dim_id2, 
	med_bill_code_id AS med_bill_code_id2, 
	med_bill_code_ak_id AS med_bill_code_ak_id2, 
	med_bill_dim_id AS med_bill_dim_id2, 
	med_bill_code_type AS med_bill_code_type2, 
	med_bill_code AS med_bill_code2, 
	med_bill_code_descript AS med_bill_code_descript2, 
	med_bill_date AS med_bill_date2, 
	audit_id AS audit_id2, 
	modified_date AS modified_date2
	FROM RTR_Insert_Update_DEFAULT1
),
medical_bill_code_dim_upd AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_code_dim AS T
	USING UPD_MED_BILL_CODE_DIM AS S
	ON T.med_bill_code_dim_id = S.med_bill_code_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_med_bill_code_pk_id = S.med_bill_code_id2, T.edw_med_bill_code_ak_id = S.med_bill_code_ak_id2, T.med_bill_dim_id = S.med_bill_dim_id2, T.med_bill_code_type = S.med_bill_code_type2, T.med_bill_code = S.med_bill_code2, T.med_bill_code_descript = S.med_bill_code_descript2, T.med_bill_date = S.med_bill_date2, T.audit_id = S.audit_id2, T.modified_date = S.modified_date2
),
medical_bill_code_dim_ins AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_code_dim
	(edw_med_bill_code_pk_id, edw_med_bill_code_ak_id, med_bill_dim_id, med_bill_code_type, med_bill_code, med_bill_code_descript, med_bill_date, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date)
	SELECT 
	med_bill_code_id AS EDW_MED_BILL_CODE_PK_ID, 
	med_bill_code_ak_id AS EDW_MED_BILL_CODE_AK_ID, 
	MED_BILL_DIM_ID, 
	MED_BILL_CODE_TYPE, 
	MED_BILL_CODE, 
	MED_BILL_CODE_DESCRIPT, 
	MED_BILL_DATE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM RTR_Insert_Update_INSERT
),