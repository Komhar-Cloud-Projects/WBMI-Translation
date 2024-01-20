WITH
SEQ_medical_bill_code_ak_id AS (
	CREATE SEQUENCE SEQ_medical_bill_code_ak_id
	START = 10000000
	INCREMENT = 1;
),
SQ_med_bill_code_stage AS (
	SELECT 
	RTRIM(med_bill_code_stage.med_bill_id), 
	CASE RTRIM(med_bill_code_stage.code_type) WHEN '' THEN NULL ELSE RTRIM(med_bill_code_stage.code_type) END,  
	CASE RTRIM(med_bill_code_stage.code) WHEN '' THEN NULL ELSE RTRIM(med_bill_code_stage.code) END , 
	CASE RTRIM(med_bill_code_stage.descript) WHEN '' THEN NULL ELSE  Substring(RTRIM(med_bill_code_stage.descript),1,11)  END , 
	med_bill_service_stage.serv_seq_num 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.med_bill_code_stage med_bill_code_stage left outer join  @{pipeline().parameters.SOURCE_TABLE_OWNER}.med_bill_service_stage med_bill_service_stage
	on 
	med_bill_code_stage.med_bill_id =  med_bill_service_stage.med_bill_id and
	med_bill_code_stage.med_bill_serv_id = med_bill_service_stage.med_bill_serv_id
	WHERE 1=1
	@{pipeline().parameters.WHERE_CLAUSE}
	order by 
	med_bill_code_stage.med_bill_id, 
	med_bill_service_stage.serv_seq_num
),
LKP_MED_BILL_KEY AS (
	SELECT
	med_bill_ak_id,
	med_bill_key
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
LKP_MEDICAL_BILL_SERVICE AS (
	SELECT
	med_bill_serv_ak_id,
	med_bill_ak_id,
	serv_seq_num
	FROM (
		SELECT 
		medical_bill_service.med_bill_serv_ak_id as med_bill_serv_ak_id,
		medical_bill_service.med_bill_ak_id as med_bill_ak_id,
		medical_bill_service.serv_seq_num as serv_seq_num
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_service
		order by medical_bill_service.med_bill_serv_ak_id,medical_bill_service.serv_seq_num , created_date desc --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_ak_id,serv_seq_num ORDER BY med_bill_serv_ak_id) = 1
),
EXP_CODE_TYPE AS (
	SELECT
	LKP_MED_BILL_KEY.med_bill_ak_id,
	LKP_MEDICAL_BILL_SERVICE.med_bill_serv_ak_id,
	SQ_med_bill_code_stage.code_type,
	SQ_med_bill_code_stage.code,
	SQ_med_bill_code_stage.descript
	FROM SQ_med_bill_code_stage
	LEFT JOIN LKP_MEDICAL_BILL_SERVICE
	ON LKP_MEDICAL_BILL_SERVICE.med_bill_ak_id = LKP_MED_BILL_KEY.med_bill_ak_id AND LKP_MEDICAL_BILL_SERVICE.serv_seq_num = SQ_med_bill_code_stage.serv_seq_num
	LEFT JOIN LKP_MED_BILL_KEY
	ON LKP_MED_BILL_KEY.med_bill_key = SQ_med_bill_code_stage.med_bill_id
),
LKP_MEDICAL_BILL_CODE1 AS (
	SELECT
	med_bill_code_ak_id,
	med_bill_ak_id,
	med_bill_serv_ak_id,
	med_bill_code,
	med_bill_code_type,
	med_bill_code_descript
	FROM (
		SELECT 
		med_bill_code_id as med_bill_code_id,
		medical_bill_code.med_bill_code_ak_id as med_bill_code_ak_id, 
		medical_bill_code.med_bill_ak_id as med_bill_ak_id, 
		medical_bill_code.med_bill_serv_ak_id as med_bill_serv_ak_id, 
		rtrim(medical_bill_code.med_bill_code) as med_bill_code, 
		rtrim(medical_bill_code.med_bill_code_descript) as med_bill_code_descript, 
		rtrim(medical_bill_code.med_bill_code_type) as med_bill_code_type
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_code medical_bill_code
		where medical_bill_code.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_ak_id,med_bill_serv_ak_id,med_bill_code,med_bill_code_type,med_bill_code_descript ORDER BY med_bill_code_ak_id) = 1
),
EXPTRANS AS (
	SELECT
	EXP_CODE_TYPE.med_bill_ak_id AS in_med_bill_ak_id,
	EXP_CODE_TYPE.med_bill_serv_ak_id AS in_med_bill_serv_ak_id,
	EXP_CODE_TYPE.code_type AS in_code_type,
	EXP_CODE_TYPE.code AS in_code,
	EXP_CODE_TYPE.descript AS in_descript,
	-- *INF*: TO_DATE('01/01/1800','MM/DD/YYYY')
	TO_TIMESTAMP('01/01/1800', 'MM/DD/YYYY') AS out_med_bill_date,
	LKP_MEDICAL_BILL_CODE1.med_bill_code_ak_id,
	-- *INF*: IIF(ISnull(med_bill_code_ak_id),'NEW'
	-- ,'NOCHANGE')
	IFF(med_bill_code_ak_id IS NULL, 'NEW', 'NOCHANGE') AS V_ChangeFlag,
	V_ChangeFlag AS ChangeFlag
	FROM EXP_CODE_TYPE
	LEFT JOIN LKP_MEDICAL_BILL_CODE1
	ON LKP_MEDICAL_BILL_CODE1.med_bill_ak_id = EXP_CODE_TYPE.med_bill_ak_id AND LKP_MEDICAL_BILL_CODE1.med_bill_serv_ak_id = EXP_CODE_TYPE.med_bill_serv_ak_id AND LKP_MEDICAL_BILL_CODE1.med_bill_code = EXP_CODE_TYPE.code AND LKP_MEDICAL_BILL_CODE1.med_bill_code_type = EXP_CODE_TYPE.code_type AND LKP_MEDICAL_BILL_CODE1.med_bill_code_descript = EXP_CODE_TYPE.descript
),
router_update_insert AS (
	SELECT
	med_bill_code_ak_id,
	in_med_bill_ak_id AS med_bill_ak_id,
	in_med_bill_serv_ak_id AS med_bill_serv_ak_id,
	in_code_type AS med_bill_code_type,
	in_code AS med_bill_code,
	in_descript AS med_bill_code_descript,
	out_med_bill_date AS med_bill_date,
	ChangeFlag
	FROM EXPTRANS
),
router_update_insert_NEW_ROW AS (SELECT * FROM router_update_insert WHERE ChangeFlag='NEW'),
EXP_AUDIT_FIELDS AS (
	SELECT
	-- *INF*:   IIF(ChangeFlag='NEW', NEXTVAL, med_bill_code_ak_id)
	IFF(ChangeFlag = 'NEW', NEXTVAL, med_bill_code_ak_id) AS med_bill_code_ak_id_out,
	med_bill_code_ak_id,
	med_bill_ak_id,
	med_bill_serv_ak_id,
	med_bill_code_type,
	med_bill_code,
	med_bill_code_descript,
	med_bill_date,
	ChangeFlag,
	SEQ_medical_bill_code_ak_id.NEXTVAL,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(ChangeFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    ChangeFlag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	sysdate AS created_date
	FROM router_update_insert_NEW_ROW
),
medical_bill_code_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_code
	(med_bill_code_ak_id, med_bill_ak_id, med_bill_serv_ak_id, med_bill_code_type, med_bill_code, med_bill_code_descript, med_bill_date, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	med_bill_code_ak_id_out AS MED_BILL_CODE_AK_ID, 
	MED_BILL_AK_ID, 
	MED_BILL_SERV_AK_ID, 
	MED_BILL_CODE_TYPE, 
	MED_BILL_CODE, 
	MED_BILL_CODE_DESCRIPT, 
	MED_BILL_DATE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	created_date AS MODIFIED_DATE
	FROM EXP_AUDIT_FIELDS
),