WITH
SQ_pifmstr_PIF_4578_stage AS (
	SELECT DISTINCT A.pif_symbol, A.pif_policy_number, A.pif_module, A.loss_reins_co_no
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4578_stage A
	WHERE loss_part= 8 
	AND LTRIM(RTRIM(logical_flag)) in ('0','-1')
),
EXP_Default AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol  ||  pif_policy_number  || pif_module AS Pol_Key,
	loss_reins_co_no AS in_loss_reins_co_no,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_loss_reins_co_no)
	UDF_DEFAULT_VALUE_FOR_STRINGS(in_loss_reins_co_no) AS loss_reins_co_no
	FROM SQ_pifmstr_PIF_4578_stage
),
LKP_Policy_1 AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag = 1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
LKP_SupReinsuranceMaster_1 AS (
	SELECT
	ReinsuranceMasterReinsuranceType,
	ReinsuranceMasterReinsuranceCompanyName,
	ReinsuranceMasterReinsuranceCompanyNumber
	FROM (
		SELECT 
			ReinsuranceMasterReinsuranceType,
			ReinsuranceMasterReinsuranceCompanyName,
			ReinsuranceMasterReinsuranceCompanyNumber
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupReinsuranceMaster
		WHERE CurrentSnapshotFlag='1' and SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ReinsuranceMasterReinsuranceCompanyNumber ORDER BY ReinsuranceMasterReinsuranceType) = 1
),
EXP_Default_Values1 AS (
	SELECT
	LKP_Policy_1.pol_ak_id AS pol_key_ak_id,
	EXP_Default.loss_reins_co_no,
	LKP_SupReinsuranceMaster_1.ReinsuranceMasterReinsuranceType AS rcm_reins_type,
	-- *INF*: --IIF(ISNULL(rcm_reins_type),'N/A',rcm_reins_type)
	-- --change for April Release ,  we will not use supReinsuranceMaster table , just hardcode it as 'N/A'
	-- 'N/A'
	'N/A' AS rcm_reins_type_out,
	-1 AS SupReinsuranceMasterId_out,
	LKP_SupReinsuranceMaster_1.ReinsuranceMasterReinsuranceCompanyName AS rcm_company_name,
	-- *INF*: IIF(ISNULL(rcm_company_name),'N/A',rcm_company_name)
	IFF(rcm_company_name IS NULL, 'N/A', rcm_company_name) AS rcm_company_name_out
	FROM EXP_Default
	LEFT JOIN LKP_Policy_1
	ON LKP_Policy_1.pol_key = EXP_Default.Pol_Key
	LEFT JOIN LKP_SupReinsuranceMaster_1
	ON LKP_SupReinsuranceMaster_1.ReinsuranceMasterReinsuranceCompanyNumber = EXP_Default.loss_reins_co_no
),
LKP_reinsurance_coverage_tgt AS (
	SELECT
	reins_cov_id,
	reins_cov_ak_id,
	reins_co_name,
	reins_type,
	pol_ak_id,
	reins_co_num
	FROM (
		SELECT RC.reins_cov_id    AS reins_cov_id,
		       RC.reins_cov_ak_id AS reins_cov_ak_id,
		       RC.reins_co_name   AS reins_co_name,
		       RTRIM(RC.reins_type)      AS reins_type,
		       RC.pol_ak_id       AS pol_ak_id,
		       RTRIM(RC.reins_co_num)    AS reins_co_num
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage RC
		WHERE  crrnt_snpsht_flag = 1 and reins_section_code = 'N/A' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,reins_co_num ORDER BY reins_cov_id DESC) = 1
),
EXP_Detect_Existing_Row AS (
	SELECT
	LKP_reinsurance_coverage_tgt.reins_cov_ak_id AS lkp_reins_cov_ak_id,
	LKP_reinsurance_coverage_tgt.reins_cov_id AS lkp_reins_cov_id,
	LKP_reinsurance_coverage_tgt.reins_co_name AS lkp_reins_co_name,
	LKP_reinsurance_coverage_tgt.reins_type AS lkp_reins_type,
	EXP_Default_Values1.pol_key_ak_id,
	EXP_Default_Values1.loss_reins_co_no,
	-- *INF*: iif(isnull(lkp_reins_cov_id),'NEW',
	-- 	iif (
	-- 	ltrim(rtrim(lkp_reins_co_name)) <> ltrim(rtrim(rcm_company_name)) or
	-- 	(ltrim(rtrim(lkp_reins_type))  <> ltrim(rtrim(rcm_reins_type)))
	--   	,'UPDATE'
	-- 	,'NOCHANGE'))
	IFF(
	    lkp_reins_cov_id IS NULL, 'NEW',
	    IFF(
	        ltrim(rtrim(lkp_reins_co_name)) <> ltrim(rtrim(rcm_company_name))
	        or (ltrim(rtrim(lkp_reins_type)) <> ltrim(rtrim(rcm_reins_type))),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(
	    v_Changed_Flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	-- *INF*: 1
	-- ---- Logical Flag of '1' is used to differentiate that this is from PIF_4578 Part 8
	1 AS logical_flag,
	'N/A' AS Default_NA,
	-1 AS Default_sup_id,
	loss_reins_co_no AS reins_co_num,
	rcm_company_name AS reins_co_name,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS reins_eff_date,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS reins_exp_date,
	SYSDATE AS reins_enter_date,
	-- *INF*: IIF(ISNULL(rcm_reins_type),'N/A',TO_CHAR(rcm_reins_type))
	IFF(rcm_reins_type IS NULL, 'N/A', TO_CHAR(rcm_reins_type)) AS reins_type,
	0 AS reins_prcnt_prem_ceded,
	0 AS reins_prcnt_loss_ceded,
	0 AS reins_prcnt_facultative_commssn,
	0 AS reins_excess_amt,
	0 AS reins_occurrence_lmt,
	0 AS reins_agg_lmt,
	-1 AS sup_reins_company_name_id,
	EXP_Default_Values1.rcm_reins_type_out AS rcm_reins_type,
	EXP_Default_Values1.SupReinsuranceMasterId_out AS SupReinsuranceMasterId,
	EXP_Default_Values1.rcm_company_name_out AS rcm_company_name
	FROM EXP_Default_Values1
	LEFT JOIN LKP_reinsurance_coverage_tgt
	ON LKP_reinsurance_coverage_tgt.pol_ak_id = EXP_Default_Values1.pol_key_ak_id AND LKP_reinsurance_coverage_tgt.reins_co_num = EXP_Default_Values1.loss_reins_co_no
),
FIL_Existing_Row AS (
	SELECT
	lkp_reins_cov_ak_id, 
	Changed_Flag, 
	pol_key_ak_id, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	logical_flag, 
	Default_NA, 
	Default_sup_id, 
	reins_co_num, 
	reins_co_name, 
	reins_eff_date, 
	reins_exp_date, 
	reins_enter_date, 
	reins_type, 
	reins_prcnt_prem_ceded, 
	reins_prcnt_loss_ceded, 
	reins_prcnt_facultative_commssn, 
	reins_excess_amt, 
	reins_occurrence_lmt, 
	reins_agg_lmt, 
	sup_reins_company_name_id, 
	SupReinsuranceMasterId
	FROM EXP_Detect_Existing_Row
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_Reins_Cov_AK_ID AS (
	CREATE SEQUENCE SEQ_Reins_Cov_AK_ID
	START = 0
	INCREMENT = 1;
),
EXP_AK_id AS (
	SELECT
	lkp_reins_cov_ak_id,
	Changed_Flag,
	SEQ_Reins_Cov_AK_ID.NEXTVAL,
	-- *INF*: IIF(Changed_Flag='NEW',
	-- NEXTVAL,
	-- lkp_reins_cov_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, lkp_reins_cov_ak_id) AS reins_cov_ak_id,
	pol_key_ak_id,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	logical_flag,
	Default_NA,
	Default_sup_id,
	reins_co_num,
	reins_co_name,
	reins_eff_date,
	reins_exp_date,
	reins_enter_date,
	reins_type,
	reins_prcnt_prem_ceded,
	reins_prcnt_loss_ceded,
	reins_prcnt_facultative_commssn,
	reins_excess_amt,
	reins_occurrence_lmt,
	reins_agg_lmt,
	sup_reins_company_name_id,
	SupReinsuranceMasterId,
	'N/A' AS ReinsuranceMethod
	FROM FIL_Existing_Row
),
reinsurance_coverage_Part_8_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, logical_flag, reins_cov_ak_id, pol_ak_id, reins_ins_line, reins_loc_unit_num, reins_sub_loc_unit_num, reins_risk_unit_grp, reins_risk_unit_grp_seq_num, reins_risk_unit, reins_risk_unit_seq_num, reins_section_code, reins_co_num, reins_co_name, reins_eff_date, reins_exp_date, reins_enter_date, reins_type, reins_prcnt_prem_ceded, reins_prcnt_loss_ceded, reins_prcnt_facultative_commssn, reins_excess_amt, reins_occurrence_lmt, reins_agg_lmt, SupInsuranceLineId, SupRiskUnitId, SupRiskUnitGroupId, ReinsuranceMethod)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	LOGICAL_FLAG, 
	REINS_COV_AK_ID, 
	pol_key_ak_id AS POL_AK_ID, 
	Default_NA AS REINS_INS_LINE, 
	Default_NA AS REINS_LOC_UNIT_NUM, 
	Default_NA AS REINS_SUB_LOC_UNIT_NUM, 
	Default_NA AS REINS_RISK_UNIT_GRP, 
	Default_NA AS REINS_RISK_UNIT_GRP_SEQ_NUM, 
	Default_NA AS REINS_RISK_UNIT, 
	Default_NA AS REINS_RISK_UNIT_SEQ_NUM, 
	Default_NA AS REINS_SECTION_CODE, 
	REINS_CO_NUM, 
	REINS_CO_NAME, 
	REINS_EFF_DATE, 
	REINS_EXP_DATE, 
	REINS_ENTER_DATE, 
	REINS_TYPE, 
	REINS_PRCNT_PREM_CEDED, 
	REINS_PRCNT_LOSS_CEDED, 
	REINS_PRCNT_FACULTATIVE_COMMSSN, 
	REINS_EXCESS_AMT, 
	REINS_OCCURRENCE_LMT, 
	REINS_AGG_LMT, 
	Default_sup_id AS SUPINSURANCELINEID, 
	Default_sup_id AS SUPRISKUNITID, 
	Default_sup_id AS SUPRISKUNITGROUPID, 
	REINSURANCEMETHOD
	FROM EXP_AK_id
),
SQ_reinsurance_coverage AS (
	SELECT 
		 A.reins_cov_id,
	       A.eff_from_date,
	       A.eff_to_date,
	       A.source_sys_id,
	       A.pol_ak_id,
	       A.reins_ins_line,
	       A.reins_loc_unit_num,
	       A.reins_sub_loc_unit_num,
	       A.reins_risk_unit_grp,
	       A.reins_risk_unit_grp_seq_num,
	       A.reins_risk_unit,
	       A.reins_risk_unit_seq_num,
	       A.reins_section_code,
	       A.reins_co_num,
	       A.reins_eff_date,
	       A.reins_enter_date
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage a
	where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS (SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage b
		WHERE b.crrnt_snpsht_flag = 1
	      AND a.reins_cov_ak_id = b.reins_cov_ak_id
	      AND a.source_sys_id = b.source_sys_id
		GROUP BY b.reins_cov_ak_id
		HAVING COUNT(*) > 1)
	order by a.reins_cov_ak_id, a.eff_from_date desc
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Expire_Rows AS (
	SELECT
	reins_cov_id,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	reins_risk_unit_grp,
	reins_risk_unit_grp_seq_num,
	reins_risk_unit,
	reins_risk_unit_seq_num,
	reins_section_code,
	reins_co_num,
	reins_eff_date,
	reins_enter_date,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE (TRUE, 
	-- pol_ak_id = v_PREV_ROW_pol_ak_id1 AND
	-- reins_ins_line = v_PREV_ROW_reins_ins_line1 AND
	-- reins_loc_unit_num = v_PREV_ROW_reins_loc_unit_num1 AND
	-- reins_sub_loc_unit_num = v_PREV_ROW_reins_sub_loc_unit_num1 AND
	-- reins_risk_unit_grp = v_PREV_ROW_reins_risk_unit_grp1 AND
	-- reins_risk_unit_grp_seq_num = v_PREV_ROW_reins_risk_unit_grp_seq_num1 AND
	-- reins_risk_unit = v_PREV_ROW_reins_risk_unit1 AND
	-- reins_risk_unit_seq_num = v_PREV_ROW_reins_risk_unit_seq_num1 AND
	-- reins_section_code = v_PREV_ROW_reins_section_code1 AND
	-- reins_co_num = v_PREV_ROW_reins_co_num1 AND
	-- reins_eff_date = v_PREV_ROW_reins_eff_date1 AND
	-- IIF(reins_section_code='N/A', 1=1, reins_enter_date = v_PREV_ROW_reins_enter_date1) AND
	-- source_sys_id = v_PREV_ROW_source_sys_id1
	-- , ADD_TO_DATE(v_PREV_ROW_eff_from_date1,'SS',-1)
	-- ,orig_eff_to_date)
	DECODE(
	    TRUE,
	    pol_ak_id = v_PREV_ROW_pol_ak_id1 AND reins_ins_line = v_PREV_ROW_reins_ins_line1 AND reins_loc_unit_num = v_PREV_ROW_reins_loc_unit_num1 AND reins_sub_loc_unit_num = v_PREV_ROW_reins_sub_loc_unit_num1 AND reins_risk_unit_grp = v_PREV_ROW_reins_risk_unit_grp1 AND reins_risk_unit_grp_seq_num = v_PREV_ROW_reins_risk_unit_grp_seq_num1 AND reins_risk_unit = v_PREV_ROW_reins_risk_unit1 AND reins_risk_unit_seq_num = v_PREV_ROW_reins_risk_unit_seq_num1 AND reins_section_code = v_PREV_ROW_reins_section_code1 AND reins_co_num = v_PREV_ROW_reins_co_num1 AND reins_eff_date = v_PREV_ROW_reins_eff_date1 AND 
	    IFF(
	        reins_section_code = 'N/A', 1 = 1, reins_enter_date = v_PREV_ROW_reins_enter_date1
	    ) AND source_sys_id = v_PREV_ROW_source_sys_id1, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date1),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	pol_ak_id AS v_PREV_ROW_pol_ak_id1,
	reins_ins_line AS v_PREV_ROW_reins_ins_line1,
	reins_loc_unit_num AS v_PREV_ROW_reins_loc_unit_num1,
	reins_sub_loc_unit_num AS v_PREV_ROW_reins_sub_loc_unit_num1,
	reins_risk_unit_grp AS v_PREV_ROW_reins_risk_unit_grp1,
	reins_risk_unit_grp_seq_num AS v_PREV_ROW_reins_risk_unit_grp_seq_num1,
	reins_risk_unit AS v_PREV_ROW_reins_risk_unit1,
	reins_risk_unit_seq_num AS v_PREV_ROW_reins_risk_unit_seq_num1,
	reins_section_code AS v_PREV_ROW_reins_section_code1,
	reins_co_num AS v_PREV_ROW_reins_co_num1,
	reins_eff_date AS v_PREV_ROW_reins_eff_date1,
	reins_enter_date AS v_PREV_ROW_reins_enter_date1,
	source_sys_id AS v_PREV_ROW_source_sys_id1,
	eff_from_date AS v_PREV_ROW_eff_from_date1,
	0 AS crrnt_snapshot_flag,
	sysdate AS modified_date
	FROM SQ_reinsurance_coverage
),
FIL_reinsurance_coverage AS (
	SELECT
	reins_cov_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snapshot_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Update_Target AS (
	SELECT
	reins_cov_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snapshot_flag, 
	modified_date
	FROM FIL_reinsurance_coverage
),
reinsurance_coverage_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage AS T
	USING UPD_Update_Target AS S
	ON T.reins_cov_id = S.reins_cov_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snapshot_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),