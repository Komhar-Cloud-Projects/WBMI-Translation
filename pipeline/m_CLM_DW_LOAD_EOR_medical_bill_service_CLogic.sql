WITH
SQ_med_bill_service_stage AS (
	SELECT 
	RTRIM(med_bill_service_stage.med_bill_id) ,
	CASE med_bill_service_stage.serv_seq_num WHEN '' THEN NULL ELSE med_bill_service_stage.serv_seq_num END,
	CASE med_bill_service_stage.serv_from_date WHEN '' THEN NULL ELSE med_bill_service_stage.serv_from_date  END,
	CASE med_bill_service_stage.serv_to_date WHEN '' THEN NULL ELSE med_bill_service_stage.serv_to_date END,
	CASE RTRIM(med_bill_service_stage.serv_place_code) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.serv_place_code) END,
	CASE RTRIM(med_bill_service_stage.serv_type_code) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.serv_type_code) END,
	CASE RTRIM(med_bill_service_stage.adjusted_code1) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.adjusted_code1) END,
	CASE RTRIM(med_bill_service_stage.adjusted_code2) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.adjusted_code2) END,
	CASE RTRIM(med_bill_service_stage.mod_proc_code1) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.mod_proc_code1) END,
	CASE RTRIM(med_bill_service_stage.mod_proc_descript1) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.mod_proc_descript1) END,
	CASE RTRIM(med_bill_service_stage.mod_proc_code2) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.mod_proc_code2) END,
	CASE RTRIM(med_bill_service_stage.mod_proc_descript2) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.mod_proc_descript2) END,
	med_bill_service_stage.serv_minutes ,
	med_bill_service_stage.serv_units,
	med_bill_service_stage.drug_qty_dispensed ,
	med_bill_service_stage.drug_qty_allowed,
	med_bill_service_stage.drug_awp,
	CASE RTRIM(med_bill_service_stage.proc_drug_rev_ind) WHEN '' THEN NULL ELSE  RTRIM(med_bill_service_stage.proc_drug_rev_ind) END,
	CASE RTRIM(med_bill_service_stage.proc_drug_rev_code) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.proc_drug_rev_code) END,
	CASE RTRIM(med_bill_service_stage.proc_drug_rev_des) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.proc_drug_rev_des) END,
	med_bill_service_stage.serv_charge ,
	med_bill_service_stage.serv_red ,
	med_bill_service_stage.serv_network_red ,
	med_bill_service_stage.serv_recom_pay ,
	CASE RTRIM(med_bill_service_stage.serv_review_cmnt) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.serv_review_cmnt)  END,
	CASE RTRIM(med_bill_service_stage.diagnose_cross_ref) WHEN '' THEN NULL ELSE RTRIM(med_bill_service_stage.diagnose_cross_ref)  END
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.med_bill_service_stage med_bill_service_stage
	where 1=1 
	@{pipeline().parameters.WHERE_CLAUSE}
),
LKP_MED_BILL_KEY AS (
	SELECT
	med_bill_ak_id,
	med_bill_key,
	med_bill_id
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
EXP_ASSIGN_DEFAULTS1 AS (
	SELECT
	LKP_MED_BILL_KEY.med_bill_ak_id,
	SQ_med_bill_service_stage.serv_seq_num,
	SQ_med_bill_service_stage.serv_from_date AS in_serv_from_date,
	-- *INF*: IIF(ISNULL(in_serv_from_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_serv_from_date)
	IFF(in_serv_from_date IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), in_serv_from_date) AS serv_from_date,
	SQ_med_bill_service_stage.serv_to_date AS in_serv_to_date,
	-- *INF*: IIF(ISNULL(in_serv_to_date),TO_DATE('12/31/2100','MM/DD/YYYY'),in_serv_to_date)
	IFF(in_serv_to_date IS NULL, TO_TIMESTAMP('12/31/2100', 'MM/DD/YYYY'), in_serv_to_date) AS serv_to_date,
	SQ_med_bill_service_stage.serv_place_code AS in_serv_place_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_serv_place_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_serv_place_code) AS serv_place_code,
	SQ_med_bill_service_stage.serv_type_code AS in_serv_type_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_serv_type_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_serv_type_code) AS serv_type_code,
	SQ_med_bill_service_stage.adjusted_code1 AS in_adjusted_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_adjusted_code1)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_adjusted_code1) AS adjusted_code1,
	SQ_med_bill_service_stage.adjusted_code2 AS in_adjusted_code2,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_adjusted_code2)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_adjusted_code2) AS adjusted_code2,
	SQ_med_bill_service_stage.mod_proc_code1 AS in_mod_proc_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_mod_proc_code1)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_mod_proc_code1) AS mod_proc_code1,
	SQ_med_bill_service_stage.mod_proc_descript1 AS in_mod_proc_descript1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_mod_proc_descript1)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_mod_proc_descript1) AS mod_proc_descript1,
	SQ_med_bill_service_stage.mod_proc_code2 AS in_mod_proc_code2,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_mod_proc_code2)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_mod_proc_code2) AS mod_proc_code2,
	SQ_med_bill_service_stage.mod_proc_descript2 AS in_mod_proc_descript2,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_mod_proc_descript2)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_mod_proc_descript2) AS mod_proc_descript2,
	SQ_med_bill_service_stage.serv_minutes,
	SQ_med_bill_service_stage.serv_units,
	SQ_med_bill_service_stage.drug_qty_dispensed,
	SQ_med_bill_service_stage.drug_qty_allowed,
	SQ_med_bill_service_stage.drug_awp,
	SQ_med_bill_service_stage.proc_drug_rev_ind AS in_proc_drug_rev_ind,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_proc_drug_rev_ind)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_proc_drug_rev_ind) AS proc_drug_rev_ind,
	SQ_med_bill_service_stage.proc_drug_rev_code AS in_proc_drug_rev_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_proc_drug_rev_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_proc_drug_rev_code) AS proc_drug_rev_code,
	SQ_med_bill_service_stage.proc_drug_rev_des AS in_proc_drug_rev_des,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_proc_drug_rev_des)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_proc_drug_rev_des) AS proc_drug_rev_des,
	SQ_med_bill_service_stage.serv_charge,
	SQ_med_bill_service_stage.serv_red,
	SQ_med_bill_service_stage.serv_network_red,
	SQ_med_bill_service_stage.serv_recom_pay,
	SQ_med_bill_service_stage.serv_review_cmnt AS in_serv_review_cmnt,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_serv_review_cmnt)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_serv_review_cmnt) AS serv_review_cmnt,
	SQ_med_bill_service_stage.diagnose_cross_ref AS in_diagnose_cross_ref,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_diagnose_cross_ref)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_diagnose_cross_ref) AS diagnose_cross_ref
	FROM SQ_med_bill_service_stage
	LEFT JOIN LKP_MED_BILL_KEY
	ON LKP_MED_BILL_KEY.med_bill_key = SQ_med_bill_service_stage.med_bill_id
),
LKP_medical_bill_service1 AS (
	SELECT
	med_bill_serv_ak_id,
	serv_seq_num,
	serv_from_date,
	serv_to_date,
	serv_place_code,
	serv_type_code,
	bill_review_adjusted_code1,
	bill_review_adjusted_code2,
	modified_proc_code1,
	modified_proc_descript,
	modified_proc_code2,
	modified_proc_descript2,
	serv_minutes,
	serv_units,
	drug_qty_dispensed,
	drug_qty_allowed,
	drug_actual_wholesale_price,
	proc_drug_revenue_type_ind,
	proc_drug_revenue_code,
	proc_drug_revenue_descript,
	serv_charge,
	serv_bill_review_red,
	serv_network_red,
	serv_recommend_pay,
	serv_review_comment,
	diagnose_cross_ref,
	in_med_bill_ak_id,
	in_serv_seq_num,
	in_serv_units,
	in_proc_drug_rev_des,
	in_serv_charge,
	in_serv_red,
	in_serv_network_red,
	in_serv_recom_pay,
	in_serv_review_cmnt,
	med_bill_ak_id
	FROM (
		SELECT 
		  MBS.med_bill_serv_ak_id as med_bill_serv_ak_id, 
		  MBS.med_bill_ak_id as med_bill_ak_id, 
		  MBS.serv_seq_num as serv_seq_num, 
		  MBS.serv_from_date as serv_from_date, 
		  MBS.serv_to_date as serv_to_date,
		  MBS.serv_place_code as serv_place_code,
		  MBS.serv_type_code as serv_type_code,
		  MBS.bill_review_adjusted_code1 as bill_review_adjusted_code1,
		  MBS.bill_review_adjusted_code2 as bill_review_adjusted_code2,
		  MBS.modified_proc_code1 as modified_proc_code1,
		  MBS.modified_proc_descript as modified_proc_descript,
		  MBS.modified_proc_code2 as modified_proc_code2,
		  MBS.modified_proc_descript2 as modified_proc_descript2,
		  MBS.serv_minutes as serv_minutes,
		  MBS.serv_units as serv_units,
		  MBS.drug_qty_dispensed as drug_qty_dispensed,
		  MBS.drug_qty_allowed as drug_qty_allowed,
		  MBS.drug_actual_wholesale_price as drug_actual_wholesale_price,
		  MBS.proc_drug_revenue_type_ind as proc_drug_revenue_type_ind,
		  MBS.proc_drug_revenue_code as proc_drug_revenue_code,
		  MBS.proc_drug_revenue_descript as proc_drug_revenue_descript,
		  MBS.serv_charge as serv_charge,
		  MBS.serv_bill_review_red as serv_bill_review_red,
		  MBS.serv_network_red as serv_network_red,
		  MBS.serv_recommend_pay as serv_recommend_pay,
		  MBS.serv_review_comment as serv_review_comment,
		  MBS.diagnose_cross_ref as diagnose_cross_ref 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.medical_bill_service MBS
		WHERE MBS.crrnt_snpsht_flag = 1 AND MBS.source_sys_id = 'DCT'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_bill_ak_id,serv_seq_num,serv_units,serv_charge,proc_drug_revenue_descript,serv_bill_review_red,serv_network_red,serv_recommend_pay,serv_review_comment ORDER BY med_bill_serv_ak_id) = 1
),
SEQ_medical_bill_service_ak_id AS (
	CREATE SEQUENCE SEQ_medical_bill_service_ak_id
	START = 7000000
	INCREMENT = 1;
),
EXP_Detect_Changes AS (
	SELECT
	EXP_ASSIGN_DEFAULTS1.med_bill_ak_id,
	EXP_ASSIGN_DEFAULTS1.serv_seq_num,
	EXP_ASSIGN_DEFAULTS1.serv_from_date,
	EXP_ASSIGN_DEFAULTS1.serv_to_date,
	EXP_ASSIGN_DEFAULTS1.serv_place_code,
	EXP_ASSIGN_DEFAULTS1.serv_type_code,
	EXP_ASSIGN_DEFAULTS1.adjusted_code1,
	EXP_ASSIGN_DEFAULTS1.adjusted_code2,
	EXP_ASSIGN_DEFAULTS1.mod_proc_code1,
	EXP_ASSIGN_DEFAULTS1.mod_proc_descript1,
	EXP_ASSIGN_DEFAULTS1.mod_proc_code2,
	EXP_ASSIGN_DEFAULTS1.mod_proc_descript2,
	EXP_ASSIGN_DEFAULTS1.serv_minutes,
	EXP_ASSIGN_DEFAULTS1.serv_units,
	EXP_ASSIGN_DEFAULTS1.drug_qty_dispensed,
	EXP_ASSIGN_DEFAULTS1.drug_qty_allowed,
	EXP_ASSIGN_DEFAULTS1.drug_awp,
	EXP_ASSIGN_DEFAULTS1.proc_drug_rev_ind,
	EXP_ASSIGN_DEFAULTS1.proc_drug_rev_code,
	EXP_ASSIGN_DEFAULTS1.proc_drug_rev_des,
	EXP_ASSIGN_DEFAULTS1.serv_charge,
	EXP_ASSIGN_DEFAULTS1.serv_red,
	EXP_ASSIGN_DEFAULTS1.serv_network_red,
	EXP_ASSIGN_DEFAULTS1.serv_recom_pay,
	EXP_ASSIGN_DEFAULTS1.serv_review_cmnt,
	EXP_ASSIGN_DEFAULTS1.diagnose_cross_ref,
	SEQ_medical_bill_service_ak_id.NEXTVAL,
	LKP_medical_bill_service1.med_bill_serv_ak_id AS LKP_med_bill_serv_ak_id,
	-- *INF*: IIF(ISNULL(LKP_med_bill_serv_ak_id)
	--    , NEXTVAL
	-- -- else --
	--    , LKP_med_bill_serv_ak_id
	-- )
	-- 
	IFF(LKP_med_bill_serv_ak_id IS NULL, NEXTVAL, LKP_med_bill_serv_ak_id) AS out_med_bill_serv_ak_id,
	LKP_medical_bill_service1.serv_from_date AS LKP_serv_from_date,
	LKP_medical_bill_service1.serv_to_date AS LKP_serv_to_date,
	LKP_medical_bill_service1.serv_place_code AS LKP_serv_place_code,
	LKP_medical_bill_service1.serv_type_code AS LKP_serv_type_code,
	LKP_medical_bill_service1.bill_review_adjusted_code1 AS LKP_bill_review_adjusted_code1,
	LKP_medical_bill_service1.bill_review_adjusted_code2 AS LKP_bill_review_adjusted_code2,
	LKP_medical_bill_service1.modified_proc_code1 AS LKP_modified_proc_code1,
	LKP_medical_bill_service1.modified_proc_descript AS LKP_modified_proc_descript1,
	LKP_medical_bill_service1.modified_proc_code2 AS LKP_modified_proc_code2,
	LKP_medical_bill_service1.modified_proc_descript2 AS LKP_modified_proc_descript2,
	LKP_medical_bill_service1.serv_minutes AS LKP_serv_minutes,
	LKP_medical_bill_service1.serv_units AS LKP_serv_units,
	LKP_medical_bill_service1.drug_qty_dispensed AS LKP_drug_qty_dispensed,
	LKP_medical_bill_service1.drug_qty_allowed AS LKP_drug_qty_allowed,
	LKP_medical_bill_service1.drug_actual_wholesale_price AS LKP_drug_actual_wholesale_price,
	LKP_medical_bill_service1.proc_drug_revenue_type_ind AS LKP_proc_drug_revenue_type_ind,
	LKP_medical_bill_service1.proc_drug_revenue_code AS LKP_proc_drug_revenue_code,
	LKP_medical_bill_service1.proc_drug_revenue_descript AS LKP_proc_drug_revenue_descript,
	LKP_medical_bill_service1.serv_charge AS LKP_serv_charge,
	LKP_medical_bill_service1.serv_bill_review_red AS LKP_serv_bill_review_red,
	LKP_medical_bill_service1.serv_network_red AS LKP_serv_network_red,
	LKP_medical_bill_service1.serv_recommend_pay AS LKP_serv_recommend_pay,
	LKP_medical_bill_service1.serv_review_comment AS LKP_serv_review_comment,
	LKP_medical_bill_service1.diagnose_cross_ref AS LKP_diagnose_cross_ref,
	-- *INF*: iif(isnull(LKP_med_bill_serv_ak_id)
	--    , 'NEW'
	-- -- else --
	--    , iif(
	-- 	LKP_serv_from_date  != serv_from_date OR 
	-- 	LKP_serv_to_date != serv_to_date OR 
	--      ltrim(rtrim(LKP_serv_place_code)) != ltrim(rtrim(serv_place_code))  OR
	--      ltrim(rtrim(LKP_serv_type_code)) != ltrim(rtrim(serv_type_code))  OR
	--      ltrim(rtrim(LKP_bill_review_adjusted_code1)) != ltrim(rtrim(adjusted_code1))  OR
	--      ltrim(rtrim(LKP_bill_review_adjusted_code2)) != ltrim(rtrim(adjusted_code2))  OR
	--      ltrim(rtrim(LKP_modified_proc_code1)) != ltrim(rtrim(mod_proc_code1))  OR
	--      ltrim(rtrim(LKP_modified_proc_descript1)) != ltrim(rtrim(mod_proc_descript1))  OR
	--      ltrim(rtrim(LKP_modified_proc_code2)) != ltrim(rtrim(mod_proc_code2))  OR
	--      ltrim(rtrim(LKP_modified_proc_descript2)) != ltrim(rtrim(mod_proc_descript2))  OR 
	-- 	LKP_serv_minutes != serv_minutes OR 
	-- 	LKP_serv_units != serv_units OR 
	-- 	LKP_drug_qty_dispensed != drug_qty_dispensed OR 
	-- 	LKP_drug_qty_allowed != drug_qty_allowed OR 
	-- 	LKP_drug_actual_wholesale_price != drug_awp OR 
	--      ltrim(rtrim(LKP_proc_drug_revenue_type_ind)) != ltrim(rtrim(proc_drug_rev_ind))  OR
	--      ltrim(rtrim(LKP_proc_drug_revenue_code)) != ltrim(rtrim(proc_drug_rev_code))  OR
	--      ltrim(rtrim(LKP_proc_drug_revenue_descript)) != ltrim(rtrim(proc_drug_rev_des))  OR
	-- 	LKP_serv_charge != serv_charge OR 
	-- 	LKP_serv_bill_review_red != serv_red OR 
	-- 	LKP_serv_network_red != serv_network_red OR 
	-- 	LKP_serv_recommend_pay != serv_recom_pay OR 
	-- 	ltrim(rtrim(LKP_serv_review_comment)) != ltrim(rtrim(serv_review_cmnt))  OR
	--      ltrim(rtrim(LKP_diagnose_cross_ref)) != ltrim(rtrim(diagnose_cross_ref))
	--         , 'UPDATE'
	--    -- else --
	--         , 'NOCHANGE'
	--    )
	-- )
	IFF(
	    LKP_med_bill_serv_ak_id IS NULL, 'NEW',
	    IFF(
	        LKP_serv_from_date != serv_from_date
	        or LKP_serv_to_date != serv_to_date
	        or ltrim(rtrim(LKP_serv_place_code)) != ltrim(rtrim(serv_place_code))
	        or ltrim(rtrim(LKP_serv_type_code)) != ltrim(rtrim(serv_type_code))
	        or ltrim(rtrim(LKP_bill_review_adjusted_code1)) != ltrim(rtrim(adjusted_code1))
	        or ltrim(rtrim(LKP_bill_review_adjusted_code2)) != ltrim(rtrim(adjusted_code2))
	        or ltrim(rtrim(LKP_modified_proc_code1)) != ltrim(rtrim(mod_proc_code1))
	        or ltrim(rtrim(LKP_modified_proc_descript1)) != ltrim(rtrim(mod_proc_descript1))
	        or ltrim(rtrim(LKP_modified_proc_code2)) != ltrim(rtrim(mod_proc_code2))
	        or ltrim(rtrim(LKP_modified_proc_descript2)) != ltrim(rtrim(mod_proc_descript2))
	        or LKP_serv_minutes != serv_minutes
	        or LKP_serv_units != serv_units
	        or LKP_drug_qty_dispensed != drug_qty_dispensed
	        or LKP_drug_qty_allowed != drug_qty_allowed
	        or LKP_drug_actual_wholesale_price != drug_awp
	        or ltrim(rtrim(LKP_proc_drug_revenue_type_ind)) != ltrim(rtrim(proc_drug_rev_ind))
	        or ltrim(rtrim(LKP_proc_drug_revenue_code)) != ltrim(rtrim(proc_drug_rev_code))
	        or ltrim(rtrim(LKP_proc_drug_revenue_descript)) != ltrim(rtrim(proc_drug_rev_des))
	        or LKP_serv_charge != serv_charge
	        or LKP_serv_bill_review_red != serv_red
	        or LKP_serv_network_red != serv_network_red
	        or LKP_serv_recommend_pay != serv_recom_pay
	        or ltrim(rtrim(LKP_serv_review_comment)) != ltrim(rtrim(serv_review_cmnt))
	        or ltrim(rtrim(LKP_diagnose_cross_ref)) != ltrim(rtrim(diagnose_cross_ref)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	'DCT' AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_ASSIGN_DEFAULTS1
	LEFT JOIN LKP_medical_bill_service1
	ON LKP_medical_bill_service1.med_bill_ak_id = EXP_ASSIGN_DEFAULTS1.med_bill_ak_id AND LKP_medical_bill_service1.serv_seq_num = EXP_ASSIGN_DEFAULTS1.serv_seq_num AND LKP_medical_bill_service1.serv_units = EXP_ASSIGN_DEFAULTS1.serv_units AND LKP_medical_bill_service1.serv_charge = EXP_ASSIGN_DEFAULTS1.serv_charge AND LKP_medical_bill_service1.proc_drug_revenue_descript = EXP_ASSIGN_DEFAULTS1.proc_drug_rev_des AND LKP_medical_bill_service1.serv_bill_review_red = EXP_ASSIGN_DEFAULTS1.serv_red AND LKP_medical_bill_service1.serv_network_red = EXP_ASSIGN_DEFAULTS1.serv_network_red AND LKP_medical_bill_service1.serv_recommend_pay = EXP_ASSIGN_DEFAULTS1.serv_recom_pay AND LKP_medical_bill_service1.serv_review_comment = EXP_ASSIGN_DEFAULTS1.serv_review_cmnt
),
FIL_Insert AS (
	SELECT
	med_bill_ak_id, 
	serv_seq_num, 
	serv_from_date, 
	serv_to_date, 
	serv_place_code, 
	serv_type_code, 
	adjusted_code1, 
	adjusted_code2, 
	mod_proc_code1, 
	mod_proc_descript1, 
	mod_proc_code2, 
	mod_proc_descript2, 
	serv_minutes, 
	serv_units, 
	drug_qty_dispensed, 
	drug_qty_allowed, 
	drug_awp, 
	proc_drug_rev_ind, 
	proc_drug_rev_code, 
	proc_drug_rev_des, 
	serv_charge, 
	serv_red, 
	serv_network_red, 
	serv_recom_pay, 
	serv_review_cmnt, 
	diagnose_cross_ref, 
	out_med_bill_serv_ak_id AS LKP_med_bill_serv_ak_id, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_Detect_Changes
	WHERE changed_flag = 'NEW' OR changed_flag = 'UPDATE'
),
medical_bill_service_insert AS (
	INSERT INTO medical_bill_service
	(med_bill_serv_ak_id, med_bill_ak_id, serv_seq_num, serv_from_date, serv_to_date, serv_place_code, serv_type_code, bill_review_adjusted_code1, bill_review_adjusted_code2, modified_proc_code1, modified_proc_descript, modified_proc_code2, modified_proc_descript2, serv_minutes, serv_units, drug_qty_dispensed, drug_qty_allowed, drug_actual_wholesale_price, proc_drug_revenue_type_ind, proc_drug_revenue_code, proc_drug_revenue_descript, serv_charge, serv_bill_review_red, serv_network_red, serv_recommend_pay, serv_review_comment, diagnose_cross_ref, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	LKP_med_bill_serv_ak_id AS MED_BILL_SERV_AK_ID, 
	MED_BILL_AK_ID, 
	SERV_SEQ_NUM, 
	SERV_FROM_DATE, 
	SERV_TO_DATE, 
	SERV_PLACE_CODE, 
	SERV_TYPE_CODE, 
	adjusted_code1 AS BILL_REVIEW_ADJUSTED_CODE1, 
	adjusted_code2 AS BILL_REVIEW_ADJUSTED_CODE2, 
	mod_proc_code1 AS MODIFIED_PROC_CODE1, 
	mod_proc_descript1 AS MODIFIED_PROC_DESCRIPT, 
	mod_proc_code2 AS MODIFIED_PROC_CODE2, 
	mod_proc_descript2 AS MODIFIED_PROC_DESCRIPT2, 
	SERV_MINUTES, 
	SERV_UNITS, 
	DRUG_QTY_DISPENSED, 
	DRUG_QTY_ALLOWED, 
	drug_awp AS DRUG_ACTUAL_WHOLESALE_PRICE, 
	proc_drug_rev_ind AS PROC_DRUG_REVENUE_TYPE_IND, 
	proc_drug_rev_code AS PROC_DRUG_REVENUE_CODE, 
	proc_drug_rev_des AS PROC_DRUG_REVENUE_DESCRIPT, 
	SERV_CHARGE, 
	serv_red AS SERV_BILL_REVIEW_RED, 
	SERV_NETWORK_RED, 
	serv_recom_pay AS SERV_RECOMMEND_PAY, 
	serv_review_cmnt AS SERV_REVIEW_COMMENT, 
	DIAGNOSE_CROSS_REF, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_Insert
),