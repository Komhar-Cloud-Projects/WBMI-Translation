WITH
SQ_clm_med_plan_type_stage AS (
	SELECT
		clm_med_plan_type_stage_id,
		cms_doc_cntl_num,
		injured_party_id,
		wbm_plan_ins_type,
		state_venue,
		med_oblig_to_clmt,
		orm_terminate_dt,
		no_fault_ins_limit,
		exhaust_limit_dt,
		tpoc_date1,
		tpoc_amount1,
		tpoc_fund_dlay_dt1,
		tpoc_date2,
		tpoc_amount2,
		tpoc_fund_dlay_dt2,
		tpoc_date3,
		tpoc_amount3,
		tpoc_fund_dlay_dt3,
		tpoc_date4,
		tpoc_amount4,
		tpoc_fund_dlay_dt4,
		tpoc_date5,
		tpoc_amount5,
		tpoc_fund_dlay_dt5,
		extract_date,
		as_of_date,
		record_count,
		source_system_id,
		plan_type_deleted
	FROM clm_med_plan_type_stage
),
LKP_Claim_Medical_Ak_Id AS (
	SELECT
	claim_med_ak_id,
	injured_party_id
	FROM (
		SELECT claim_medical.claim_med_ak_id as claim_med_ak_id, claim_medical.injured_party_id as injured_party_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY injured_party_id ORDER BY claim_med_ak_id) = 1
),
EXP_Values AS (
	SELECT
	SQ_clm_med_plan_type_stage.cms_doc_cntl_num,
	SQ_clm_med_plan_type_stage.wbm_plan_ins_type,
	SQ_clm_med_plan_type_stage.state_venue,
	SQ_clm_med_plan_type_stage.med_oblig_to_clmt,
	SQ_clm_med_plan_type_stage.orm_terminate_dt,
	SQ_clm_med_plan_type_stage.no_fault_ins_limit,
	SQ_clm_med_plan_type_stage.exhaust_limit_dt,
	SQ_clm_med_plan_type_stage.plan_type_deleted,
	LKP_Claim_Medical_Ak_Id.claim_med_ak_id,
	cms_doc_cntl_num AS cms_doc_cntl_num1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(wbm_plan_ins_type)
	-- 
	UDF_DEFAULT_VALUE_FOR_STRINGS(wbm_plan_ins_type) AS wbm_plan_ins_type1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(state_venue)
	UDF_DEFAULT_VALUE_FOR_STRINGS(state_venue) AS state_venue1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(med_oblig_to_clmt)
	UDF_DEFAULT_VALUE_FOR_STRINGS(med_oblig_to_clmt) AS med_oblig_to_clmt1,
	-- *INF*: IIF(ISNULL(orm_terminate_dt), TO_DATE('1/1/1800','MM/DD/YYYY'), orm_terminate_dt)
	-- 
	IFF(orm_terminate_dt IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), orm_terminate_dt) AS orm_terminate_dt1,
	-- *INF*: IIF(ISNULL(no_fault_ins_limit),0.00,no_fault_ins_limit)
	IFF(no_fault_ins_limit IS NULL, 0.00, no_fault_ins_limit) AS no_fault_ins_limit1,
	-- *INF*: IIF(ISNULL(exhaust_limit_dt), TO_DATE('1/1/1800','MM/DD/YYYY'), exhaust_limit_dt)
	-- 
	IFF(exhaust_limit_dt IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), exhaust_limit_dt) AS exhaust_limit_dt1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(plan_type_deleted)
	UDF_DEFAULT_VALUE_FOR_STRINGS(plan_type_deleted) AS plan_type_deleted1
	FROM SQ_clm_med_plan_type_stage
	LEFT JOIN LKP_Claim_Medical_Ak_Id
	ON LKP_Claim_Medical_Ak_Id.injured_party_id = SQ_clm_med_plan_type_stage.injured_party_id
),
LKP_Target AS (
	SELECT
	claim_med_plan_ak_id,
	state_venue,
	med_obligation_to_claimant,
	orm_termination_date,
	no_fault_ins_lmt,
	exhaust_lmt_date,
	plan_type_deleted,
	claim_med_ak_id,
	wbmi_plan_ins_type
	FROM (
		SELECT claim_medical_plan.claim_med_plan_ak_id as claim_med_plan_ak_id, claim_medical_plan.state_venue as state_venue, claim_medical_plan.med_obligation_to_claimant as med_obligation_to_claimant, claim_medical_plan.orm_termination_date as orm_termination_date, claim_medical_plan.no_fault_ins_lmt as no_fault_ins_lmt, claim_medical_plan.exhaust_lmt_date as exhaust_lmt_date, claim_medical_plan.plan_type_deleted as plan_type_deleted, claim_medical_plan.claim_med_ak_id as claim_med_ak_id, claim_medical_plan.wbmi_plan_ins_type as wbmi_plan_ins_type 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_med_ak_id,wbmi_plan_ins_type ORDER BY claim_med_plan_ak_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Target.claim_med_plan_ak_id AS lkp_claim_med_plan_ak_id,
	LKP_Target.state_venue AS lkp_state_venue,
	LKP_Target.med_obligation_to_claimant AS lkp_med_obligation_to_claimant,
	LKP_Target.orm_termination_date AS lkp_orm_termination_date,
	LKP_Target.no_fault_ins_lmt AS lkp_no_fault_ins_lmt,
	LKP_Target.exhaust_lmt_date AS lkp_exhaust_lmt_date,
	LKP_Target.plan_type_deleted AS lkp_plan_type_deleted,
	EXP_Values.claim_med_ak_id,
	EXP_Values.cms_doc_cntl_num1,
	EXP_Values.wbm_plan_ins_type1,
	EXP_Values.state_venue1,
	EXP_Values.med_oblig_to_clmt1,
	EXP_Values.orm_terminate_dt1,
	EXP_Values.no_fault_ins_limit1,
	EXP_Values.exhaust_limit_dt1,
	EXP_Values.plan_type_deleted1,
	-- *INF*: iif(isnull(lkp_claim_med_plan_ak_id),'NEW',	
	-- 	iif (
	-- (lkp_state_venue != state_venue1 OR
	-- lkp_med_obligation_to_claimant != med_oblig_to_clmt1 OR
	-- lkp_orm_termination_date != orm_terminate_dt1 OR
	-- lkp_no_fault_ins_lmt != no_fault_ins_limit1 OR
	-- lkp_exhaust_lmt_date != exhaust_limit_dt1 OR
	-- lkp_plan_type_deleted != plan_type_deleted1),
	--  'UPDATE','NOCHANGE'))
	IFF(
	    lkp_claim_med_plan_ak_id IS NULL, 'NEW',
	    IFF(
	        (lkp_state_venue != state_venue1
	        or lkp_med_obligation_to_claimant != med_oblig_to_clmt1
	        or lkp_orm_termination_date !=
	        orm_terminate_dt1
	        or lkp_no_fault_ins_lmt != no_fault_ins_limit1
	        or lkp_exhaust_lmt_date != exhaust_limit_dt1
	        or lkp_plan_type_deleted != plan_type_deleted1),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	1 AS Crrnt_Snpsht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(
	    v_Changed_Flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date
	FROM EXP_Values
	LEFT JOIN LKP_Target
	ON LKP_Target.claim_med_ak_id = EXP_Values.claim_med_ak_id AND LKP_Target.wbmi_plan_ins_type = EXP_Values.wbm_plan_ins_type1
),
FIL_Insert AS (
	SELECT
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date, 
	lkp_claim_med_plan_ak_id, 
	claim_med_ak_id, 
	cms_doc_cntl_num1, 
	wbm_plan_ins_type1, 
	state_venue1, 
	med_oblig_to_clmt1, 
	orm_terminate_dt1, 
	no_fault_ins_limit1, 
	exhaust_limit_dt1, 
	plan_type_deleted1
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_medical_plan AS (
	CREATE SEQUENCE SEQ_claim_medical_plan
	START = 0
	INCREMENT = 1;
),
EXP_Insert AS (
	SELECT
	lkp_claim_med_plan_ak_id,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	Changed_Flag,
	SOURCE_SYSTEM_ID,
	Created_Date,
	Modified_Date,
	SEQ_claim_medical_plan.NEXTVAL,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, lkp_claim_med_plan_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, lkp_claim_med_plan_ak_id) AS claim_med_plan_ak_id_out,
	claim_med_ak_id,
	cms_doc_cntl_num1,
	wbm_plan_ins_type1,
	state_venue1,
	med_oblig_to_clmt1,
	orm_terminate_dt1,
	no_fault_ins_limit1,
	exhaust_limit_dt1,
	plan_type_deleted1
	FROM FIL_Insert
),
claim_medical_plan_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_med_plan_ak_id, claim_med_ak_id, cms_document_cntl_num, wbmi_plan_ins_type, state_venue, med_obligation_to_claimant, orm_termination_date, no_fault_ins_lmt, exhaust_lmt_date, plan_type_deleted)
	SELECT 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	claim_med_plan_ak_id_out AS CLAIM_MED_PLAN_AK_ID, 
	CLAIM_MED_AK_ID, 
	cms_doc_cntl_num1 AS CMS_DOCUMENT_CNTL_NUM, 
	wbm_plan_ins_type1 AS WBMI_PLAN_INS_TYPE, 
	state_venue1 AS STATE_VENUE, 
	med_oblig_to_clmt1 AS MED_OBLIGATION_TO_CLAIMANT, 
	orm_terminate_dt1 AS ORM_TERMINATION_DATE, 
	no_fault_ins_limit1 AS NO_FAULT_INS_LMT, 
	exhaust_limit_dt1 AS EXHAUST_LMT_DATE, 
	plan_type_deleted1 AS PLAN_TYPE_DELETED
	FROM EXP_Insert
),
SQ_claim_medical_plan AS (
	SELECT 
	a.claim_med_plan_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.claim_med_plan_ak_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND  
	 EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_med_plan_ak_id  = b.claim_med_plan_ak_id 
			GROUP BY claim_med_plan_ak_id 
			HAVING COUNT(*) > 1)
	ORDER BY claim_med_plan_ak_id, eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_med_plan_id,
	claim_med_plan_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_med_plan_ak_id = v_PREV_ROW_claim_med_plan_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    claim_med_plan_ak_id = v_PREV_ROW_claim_med_plan_ak_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_med_plan_ak_id AS v_PREV_ROW_claim_med_plan_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_medical_plan
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_med_plan_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_Claim_Occurrence AS (
	SELECT
	claim_med_plan_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
claim_medical_plan_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan AS T
	USING UPD_Claim_Occurrence AS S
	ON T.claim_med_plan_id = S.claim_med_plan_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),