WITH
SQ_medical_bill_code AS (
	SELECT 
	medical_bill_code.med_bill_code_id, 
	medical_bill_code.med_bill_code_ak_id, 
	medical_bill_code.med_bill_serv_ak_id, 
	rtrim(medical_bill_code.med_bill_code_type), 
	rtrim(medical_bill_code.med_bill_code), 
	rtrim(medical_bill_code.med_bill_code_descript), 
	medical_bill_code.med_bill_date, 
	medical_bill.med_bill_id 
	
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.medical_bill_code medical_bill_code, 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.medical_bill medical_bill,
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.medical_bill_service medical_bill_service
	
	WHERE
	medical_bill_code.med_bill_ak_id = medical_bill.med_bill_ak_id and
	medical_bill.crrnt_snpsht_flag = 1 and 
	medical_bill_code.created_date >= '@{pipeline().parameters.SELECTION_START_TS}' and
	medical_bill_code.med_bill_serv_ak_id <> 0 and
	medical_bill_code.med_bill_serv_ak_id = medical_bill_service.med_bill_serv_ak_id
),
LKP_Med_Bill_Serv_Dim_Id AS (
	SELECT
	med_bill_serv_dim_id,
	edw_med_bill_serv_ak_id,
	edw_med_bill_serv_ak_id1
	FROM (
		SELECT 
			med_bill_serv_dim_id,
			edw_med_bill_serv_ak_id,
			edw_med_bill_serv_ak_id1
		FROM medical_bill_service_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_med_bill_serv_ak_id ORDER BY med_bill_serv_dim_id) = 1
),
EXP_Audit AS (
	SELECT
	SQ_medical_bill_code.med_bill_id,
	SQ_medical_bill_code.med_bill_code_ak_id,
	SQ_medical_bill_code.med_bill_code_type,
	SQ_medical_bill_code.med_bill_code,
	SQ_medical_bill_code.med_bill_code_descript,
	SQ_medical_bill_code.med_bill_date,
	1 AS crrnt_snpsht_flag,
	-- *INF*: to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	SQ_medical_bill_code.med_bill_code_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	LKP_Med_Bill_Serv_Dim_Id.med_bill_serv_dim_id
	FROM SQ_medical_bill_code
	LEFT JOIN LKP_Med_Bill_Serv_Dim_Id
	ON LKP_Med_Bill_Serv_Dim_Id.edw_med_bill_serv_ak_id = SQ_medical_bill_code.med_bill_serv_ak_id
),
LKP_Med_Bill_Service_Code_Exists AS (
	SELECT
	med_bill_service_code_dim_id,
	edw_med_bill_code_pk_id,
	edw_med_bill_code_ak_id,
	med_bill_serv_dim_id,
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
	med_bill_code_id,
	med_bill_code_ak_id,
	med_bill_serv_dim_id1,
	med_bill_code_type1,
	med_bill_code1,
	med_bill_code_descript1,
	med_bill_date1
	FROM (
		SELECT 
		medical_bill_service_code_dim.med_bill_serv_code_dim_id as med_bill_service_code_dim_id, medical_bill_service_code_dim.crrnt_snpsht_flag as crrnt_snpsht_flag, medical_bill_service_code_dim.audit_id as audit_id, medical_bill_service_code_dim.eff_from_date as eff_from_date, medical_bill_service_code_dim.eff_to_date as eff_to_date, medical_bill_service_code_dim.created_date as created_date, medical_bill_service_code_dim.modified_date as modified_date, medical_bill_service_code_dim.edw_med_bill_code_pk_id as edw_med_bill_code_pk_id, medical_bill_service_code_dim.edw_med_bill_code_ak_id as edw_med_bill_code_ak_id, medical_bill_service_code_dim.med_bill_serv_dim_id as med_bill_serv_dim_id, RTRIM(medical_bill_service_code_dim.med_bill_serv_code_type) as med_bill_code_type, rtrim(medical_bill_service_code_dim.med_bill_serv_code) as med_bill_code, rtrim(medical_bill_service_code_dim.med_bill_serv_code_descript) as med_bill_code_descript, medical_bill_service_code_dim.med_bill_serv_date as med_bill_date 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_service_code_dim medical_bill_service_code_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_med_bill_code_pk_id,edw_med_bill_code_ak_id,med_bill_serv_dim_id,med_bill_code_type,med_bill_code,med_bill_code_descript,med_bill_date ORDER BY med_bill_service_code_dim_id) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_Med_Bill_Service_Code_Exists.med_bill_service_code_dim_id,
	EXP_Audit.med_bill_id,
	EXP_Audit.med_bill_code_ak_id,
	EXP_Audit.med_bill_code_type,
	EXP_Audit.med_bill_code,
	EXP_Audit.med_bill_code_descript,
	EXP_Audit.med_bill_date,
	EXP_Audit.crrnt_snpsht_flag,
	EXP_Audit.eff_from_date,
	EXP_Audit.eff_to_date,
	EXP_Audit.created_date,
	EXP_Audit.modified_date,
	EXP_Audit.med_bill_code_id,
	EXP_Audit.audit_id,
	EXP_Audit.med_bill_serv_dim_id
	FROM EXP_Audit
	LEFT JOIN LKP_Med_Bill_Service_Code_Exists
	ON LKP_Med_Bill_Service_Code_Exists.edw_med_bill_code_pk_id = EXP_Audit.med_bill_code_id AND LKP_Med_Bill_Service_Code_Exists.edw_med_bill_code_ak_id = EXP_Audit.med_bill_code_ak_id AND LKP_Med_Bill_Service_Code_Exists.med_bill_serv_dim_id = EXP_Audit.med_bill_serv_dim_id AND LKP_Med_Bill_Service_Code_Exists.med_bill_code_type = EXP_Audit.med_bill_code_type AND LKP_Med_Bill_Service_Code_Exists.med_bill_code = EXP_Audit.med_bill_code AND LKP_Med_Bill_Service_Code_Exists.med_bill_code_descript = EXP_Audit.med_bill_code_descript AND LKP_Med_Bill_Service_Code_Exists.med_bill_date = EXP_Audit.med_bill_date
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(med_bill_service_code_dim_id)),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (ISNULL(med_bill_service_code_dim_id)) )),
medical_bill_service_code_dim_ins AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_service_code_dim
	(edw_med_bill_code_pk_id, edw_med_bill_code_ak_id, med_bill_serv_dim_id, med_bill_serv_code_type, med_bill_serv_code, med_bill_serv_code_descript, med_bill_serv_date, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date)
	SELECT 
	med_bill_code_id AS EDW_MED_BILL_CODE_PK_ID, 
	med_bill_code_ak_id AS EDW_MED_BILL_CODE_AK_ID, 
	MED_BILL_SERV_DIM_ID, 
	med_bill_code_type AS MED_BILL_SERV_CODE_TYPE, 
	med_bill_code AS MED_BILL_SERV_CODE, 
	med_bill_code_descript AS MED_BILL_SERV_CODE_DESCRIPT, 
	med_bill_date AS MED_BILL_SERV_DATE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM RTR_Insert_Update_INSERT
),
UPD_MED_BILL_CODE_DIM AS (
	SELECT
	med_bill_service_code_dim_id AS med_bill_code_dim_id2, 
	med_bill_code_id AS med_bill_code_id2, 
	med_bill_code_ak_id AS med_bill_code_ak_id2, 
	med_bill_code_type AS med_bill_code_type2, 
	med_bill_code AS med_bill_code2, 
	med_bill_code_descript AS med_bill_code_descript2, 
	med_bill_date AS med_bill_date2, 
	audit_id AS audit_id2, 
	modified_date AS modified_date2, 
	med_bill_serv_dim_id AS med_bill_serv_dim_id2
	FROM RTR_Insert_Update_DEFAULT1
),
medical_bill_service_code_dim_upd AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_service_code_dim AS T
	USING UPD_MED_BILL_CODE_DIM AS S
	ON T.med_bill_serv_code_dim_id = S.med_bill_code_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_med_bill_code_pk_id = S.med_bill_code_id2, T.edw_med_bill_code_ak_id = S.med_bill_code_ak_id2, T.med_bill_serv_dim_id = S.med_bill_serv_dim_id2, T.med_bill_serv_code_type = S.med_bill_code_type2, T.med_bill_serv_code = S.med_bill_code2, T.med_bill_serv_code_descript = S.med_bill_code_descript2, T.med_bill_serv_date = S.med_bill_date2, T.audit_id = S.audit_id2, T.modified_date = S.modified_date2
),