WITH
LKP_CLAIM_REP AS (
	SELECT
	claim_rep_id,
	claim_rep_wbconnect_user_id
	FROM (
		SELECT claim_representative.claim_rep_id as claim_rep_id, claim_representative.claim_rep_wbconnect_user_id as claim_rep_wbconnect_user_id, claim_representative.source_sys_id as source_sys_id FROM claim_representative
		where claim_representative.crrnt_snpsht_flag='1'
		AND claim_representative.source_sys_id='EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_wbconnect_user_id ORDER BY claim_rep_id) = 1
),
SQ_claim_representative AS (
	SELECT claim_representative.claim_rep_id, claim_representative.claim_rep_ak_id, claim_representative.claim_rep_key, claim_representative.claim_rep_full_name, claim_representative.claim_rep_first_name, claim_representative.claim_rep_last_name, claim_representative.claim_rep_mid_name, claim_representative.claim_rep_name_prfx, claim_representative.claim_rep_name_sfx, claim_representative.co_descript, claim_representative.dvsn_code, claim_representative.dvsn_descript, claim_representative.dvsn_mgr, claim_representative.dept_descript, claim_representative.dept_name, claim_representative.dept_mgr, claim_representative.handling_office_code, claim_representative.handling_office_descript, claim_representative.handling_office_mgr, claim_representative.claim_rep_wbconnect_user_id, claim_representative.crrnt_snpsht_flag, claim_representative.audit_id, claim_representative.eff_from_date, claim_representative.eff_to_date, claim_representative.source_sys_id, claim_representative.created_date, claim_representative.modified_date, claim_representative.claim_rep_email, claim_representative.handling_office_mgr_email, claim_representative.claim_rep_direct_automatic_pay_lmt, claim_representative.claim_rep_direct_automatic_reserve_lmt, claim_representative.handling_office_mgr_direct_automatic_pay_lmt, claim_representative.handling_office_mgr_direct_automatic_reserve_lmt,
	claim_representative.cost_center,
	claim_representative.claim_rep_branch_num,
	claim_representative.claim_rep_num,
	claim_representative.ExceedAuthorityFlag,
	claim_representative.ClaimsDesktopAuthorityType
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_representative
	WHERE
	CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_claim_rep_DM AS (
	SELECT
	claim_rep_id,
	claim_rep_ak_id,
	claim_rep_key,
	claim_rep_full_name,
	claim_rep_first_name,
	claim_rep_last_name,
	claim_rep_mid_name,
	claim_rep_name_prfx,
	claim_rep_name_sfx,
	co_descript,
	dvsn_code,
	dvsn_descript,
	dvsn_mgr,
	dept_descript,
	dept_name,
	dept_mgr,
	handling_office_code,
	handling_office_descript,
	handling_office_mgr,
	claim_rep_wbconnect_user_id AS old_claim_rep_wbconnect_user_id,
	-- *INF*: IIF(old_claim_rep_wbconnect_user_id='N/A',claim_rep_key,old_claim_rep_wbconnect_user_id)
	-- 
	-- -- For certain PMS records, the wbconnect_user_id is N/A. For these records, we use rep_key which is the three letter adjuster code
	IFF(old_claim_rep_wbconnect_user_id = 'N/A',
		claim_rep_key,
		old_claim_rep_wbconnect_user_id
	) AS claim_rep_wbconnect_user_id_op,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	claim_rep_email,
	handling_office_mgr_email,
	claim_rep_direct_automatic_pay_lmt,
	claim_rep_direct_automatic_reserve_lmt,
	handling_office_mgr_direct_automatic_pay_lmt,
	handling_office_mgr_direct_automatic_reserve_lmt,
	cost_center,
	claim_rep_branch_num,
	claim_rep_num,
	ExceedAuthorityFlag,
	-- *INF*: DECODE (true,
	-- RTRIM(LTRIM(ExceedAuthorityFlag)) ='T','Yes',
	-- RTRIM(LTRIM(ExceedAuthorityFlag)) ='Y','Yes',
	-- TO_INTEGER(ExceedAuthorityFlag)=1, 'Yes',
	-- 'No'
	-- )
	-- 
	-- -- Bit types are inconsistantly handled in Informatica as integers and strings.  I'm forcing an integer check to  just in case the type needs to change..
	DECODE(true,
		RTRIM(LTRIM(ExceedAuthorityFlag
			)
		) = 'T', 'Yes',
		RTRIM(LTRIM(ExceedAuthorityFlag
			)
		) = 'Y', 'Yes',
		CAST(ExceedAuthorityFlag AS INTEGER) = 1, 'Yes',
		'No'
	) AS ExceedAuthorityFlag_out,
	ClaimsDesktopAuthorityType
	FROM SQ_claim_representative
),
FIL_Claim_rep_DM AS (
	SELECT
	claim_rep_id, 
	claim_rep_ak_id, 
	claim_rep_key, 
	claim_rep_full_name, 
	claim_rep_first_name, 
	claim_rep_last_name, 
	claim_rep_mid_name, 
	claim_rep_name_prfx, 
	claim_rep_name_sfx, 
	co_descript, 
	dvsn_code, 
	dvsn_descript, 
	dvsn_mgr, 
	dept_descript, 
	dept_name, 
	dept_mgr, 
	handling_office_code, 
	handling_office_descript, 
	handling_office_mgr, 
	old_claim_rep_wbconnect_user_id, 
	claim_rep_wbconnect_user_id_op AS claim_rep_wbconnect_user_id, 
	source_sys_id, 
	eff_from_date, 
	claim_rep_email, 
	handling_office_mgr_email, 
	claim_rep_direct_automatic_pay_lmt, 
	claim_rep_direct_automatic_reserve_lmt, 
	handling_office_mgr_direct_automatic_pay_lmt, 
	handling_office_mgr_direct_automatic_reserve_lmt, 
	cost_center, 
	claim_rep_branch_num, 
	claim_rep_num, 
	ExceedAuthorityFlag_out AS ExceedAuthorityFlag, 
	ClaimsDesktopAuthorityType
	FROM EXP_claim_rep_DM
	WHERE IIF(source_sys_id='EXCEED' OR old_claim_rep_wbconnect_user_id='N/A' , 1, IIF(NOT ISNULL(:LKP.LKP_CLAIM_REP(old_claim_rep_wbconnect_user_id)),0,1))

-- This has been done because we could have the same person come in from Exceed as well PMS. In that case, EXCEED records always take precedence. The lookup is getting us those PMS rep that also exist as EXCEED reps in claim_representative table. All these reps would be filtered out.
),
LKP_CLAIM_REP_DIM AS (
	SELECT
	claim_rep_dim_id,
	eff_from_date,
	in_eff_from_date,
	edw_claim_rep_ak_id,
	in_edw_claim_rep_ak_id
	FROM (
		SELECT 
			claim_rep_dim_id,
			eff_from_date,
			in_eff_from_date,
			edw_claim_rep_ak_id,
			in_edw_claim_rep_ak_id
		FROM claim_representative_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_rep_ak_id,eff_from_date ORDER BY claim_rep_dim_id) = 1
),
EXP_claim_representative_dim_detect_changes AS (
	SELECT
	FIL_Claim_rep_DM.claim_rep_id,
	FIL_Claim_rep_DM.claim_rep_ak_id,
	FIL_Claim_rep_DM.claim_rep_key,
	FIL_Claim_rep_DM.claim_rep_full_name,
	FIL_Claim_rep_DM.claim_rep_first_name,
	FIL_Claim_rep_DM.claim_rep_last_name,
	FIL_Claim_rep_DM.claim_rep_mid_name,
	FIL_Claim_rep_DM.claim_rep_name_prfx,
	FIL_Claim_rep_DM.claim_rep_name_sfx,
	FIL_Claim_rep_DM.co_descript,
	FIL_Claim_rep_DM.dvsn_code,
	FIL_Claim_rep_DM.dvsn_descript,
	FIL_Claim_rep_DM.dvsn_mgr,
	FIL_Claim_rep_DM.dept_descript,
	FIL_Claim_rep_DM.dept_name,
	FIL_Claim_rep_DM.dept_mgr,
	FIL_Claim_rep_DM.handling_office_code,
	FIL_Claim_rep_DM.handling_office_descript,
	FIL_Claim_rep_DM.handling_office_mgr,
	FIL_Claim_rep_DM.claim_rep_wbconnect_user_id,
	'1' AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	FIL_Claim_rep_DM.eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	FIL_Claim_rep_DM.source_sys_id,
	sysdate AS created_date,
	sysdate AS modified_date,
	LKP_CLAIM_REP_DIM.claim_rep_dim_id,
	FIL_Claim_rep_DM.claim_rep_email,
	FIL_Claim_rep_DM.handling_office_mgr_email,
	FIL_Claim_rep_DM.claim_rep_direct_automatic_pay_lmt,
	FIL_Claim_rep_DM.claim_rep_direct_automatic_reserve_lmt,
	FIL_Claim_rep_DM.handling_office_mgr_direct_automatic_pay_lmt,
	FIL_Claim_rep_DM.handling_office_mgr_direct_automatic_reserve_lmt,
	FIL_Claim_rep_DM.cost_center,
	FIL_Claim_rep_DM.claim_rep_branch_num,
	FIL_Claim_rep_DM.claim_rep_num,
	FIL_Claim_rep_DM.ExceedAuthorityFlag,
	FIL_Claim_rep_DM.ClaimsDesktopAuthorityType
	FROM FIL_Claim_rep_DM
	LEFT JOIN LKP_CLAIM_REP_DIM
	ON LKP_CLAIM_REP_DIM.edw_claim_rep_ak_id = FIL_Claim_rep_DM.claim_rep_ak_id AND LKP_CLAIM_REP_DIM.eff_from_date = FIL_Claim_rep_DM.eff_from_date
),
RTR_INS_UPD AS (
	SELECT
	claim_rep_dim_id,
	claim_rep_id,
	claim_rep_ak_id,
	claim_rep_key,
	claim_rep_full_name,
	claim_rep_first_name,
	claim_rep_last_name,
	claim_rep_mid_name,
	claim_rep_name_prfx,
	claim_rep_name_sfx,
	co_descript,
	dvsn_code,
	dvsn_descript,
	dvsn_mgr,
	dept_descript,
	dept_name,
	dept_mgr,
	handling_office_code,
	handling_office_descript,
	handling_office_mgr,
	claim_rep_wbconnect_user_id,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	claim_rep_email,
	handling_office_mgr_email,
	claim_rep_direct_automatic_pay_lmt,
	claim_rep_direct_automatic_reserve_lmt,
	handling_office_mgr_direct_automatic_pay_lmt,
	handling_office_mgr_direct_automatic_reserve_lmt,
	cost_center,
	claim_rep_branch_num,
	claim_rep_num,
	ExceedAuthorityFlag,
	ClaimsDesktopAuthorityType
	FROM EXP_claim_representative_dim_detect_changes
),
RTR_INS_UPD_INSERT AS (SELECT * FROM RTR_INS_UPD WHERE ISNULL(claim_rep_dim_id)),
RTR_INS_UPD_DEFAULT1 AS (SELECT * FROM RTR_INS_UPD WHERE NOT ( (ISNULL(claim_rep_dim_id)) )),
UPD_CLAIM_REP AS (
	SELECT
	claim_rep_dim_id AS claim_rep_dim_id2, 
	claim_rep_id AS claim_rep_id2, 
	claim_rep_ak_id AS claim_rep_ak_id2, 
	claim_rep_key AS claim_rep_key2, 
	claim_rep_full_name AS claim_rep_full_name2, 
	claim_rep_first_name AS claim_rep_first_name2, 
	claim_rep_last_name AS claim_rep_last_name2, 
	claim_rep_mid_name AS claim_rep_mid_name2, 
	claim_rep_name_prfx AS claim_rep_name_prfx2, 
	claim_rep_name_sfx AS claim_rep_name_sfx2, 
	co_descript AS co_descript2, 
	dvsn_code AS dvsn_code2, 
	dvsn_descript AS dvsn_descript2, 
	dvsn_mgr AS dvsn_mgr2, 
	dept_descript AS dept_descript2, 
	dept_name AS dept_name2, 
	dept_mgr AS dept_mgr2, 
	handling_office_code AS handling_office_code2, 
	handling_office_descript AS handling_office_descript2, 
	handling_office_mgr AS handling_office_mgr2, 
	claim_rep_wbconnect_user_id AS claim_rep_wbconnect_user_id2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	source_sys_id AS source_sys_id2, 
	modified_date AS modified_date2, 
	claim_rep_email AS claim_rep_email2, 
	handling_office_mgr_email AS handling_office_mgr_email2, 
	claim_rep_direct_automatic_pay_lmt AS claim_rep_direct_automatic_pay_lmt2, 
	claim_rep_direct_automatic_reserve_lmt AS claim_rep_direct_automatic_reserve_lmt2, 
	handling_office_mgr_direct_automatic_pay_lmt AS handling_office_mgr_direct_automatic_pay_lmt2, 
	handling_office_mgr_direct_automatic_reserve_lmt AS handling_office_mgr_direct_automatic_reserve_lmt2, 
	cost_center AS cost_center2, 
	claim_rep_branch_num AS claim_rep_branch_num2, 
	claim_rep_num AS claim_rep_num2, 
	ExceedAuthorityFlag AS ExceedAuthorityFlag2, 
	ClaimsDesktopAuthorityType AS ClaimsDesktopAuthorityType2
	FROM RTR_INS_UPD_DEFAULT1
),
claim_representative_dim_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_dim AS T
	USING UPD_CLAIM_REP AS S
	ON T.claim_rep_dim_id = S.claim_rep_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_claim_rep_pk_id = S.claim_rep_id2, T.edw_claim_rep_ak_id = S.claim_rep_ak_id2, T.co_descript = S.co_descript2, T.dvsn_code = S.dvsn_code2, T.dvsn_descript = S.dvsn_descript2, T.dvsn_mgr = S.dvsn_mgr2, T.dept_descript = S.dept_descript2, T.dept_name = S.dept_name2, T.dept_mgr = S.dept_mgr2, T.handling_office_code = S.handling_office_code2, T.handling_office_descript = S.handling_office_descript2, T.handling_office_mgr = S.handling_office_mgr2, T.claim_rep_key = S.claim_rep_key2, T.claim_rep_full_name = S.claim_rep_full_name2, T.claim_rep_first_name = S.claim_rep_first_name2, T.claim_rep_last_name = S.claim_rep_last_name2, T.claim_rep_mid_name = S.claim_rep_mid_name2, T.claim_rep_name_prfx = S.claim_rep_name_prfx2, T.claim_rep_name_sfx = S.claim_rep_name_sfx2, T.claim_rep_wbconnect_user_id = S.claim_rep_wbconnect_user_id2, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.modified_date = S.modified_date2, T.claim_rep_email = S.claim_rep_email2, T.claim_rep_direct_automatic_pay_lmt = S.claim_rep_direct_automatic_pay_lmt2, T.claim_rep_direct_automatic_reserve_lmt = S.claim_rep_direct_automatic_reserve_lmt2, T.handling_office_mgr_email = S.handling_office_mgr_email2, T.handling_office_mgr_direct_automatic_pay_lmt = S.handling_office_mgr_direct_automatic_pay_lmt2, T.handling_office_mgr_direct_automatic_reserve_lmt = S.handling_office_mgr_direct_automatic_reserve_lmt2, T.claim_rep_branch_num = S.claim_rep_branch_num2, T.cost_center = S.cost_center2, T.claim_rep_num = S.claim_rep_num2, T.ExceedAuthorityFlag = S.ExceedAuthorityFlag2, T.ClaimsDesktopAuthorityType = S.ClaimsDesktopAuthorityType2
),
claim_representative_dim_insert AS (
	INSERT INTO claim_representative_dim
	(edw_claim_rep_pk_id, edw_claim_rep_ak_id, co_descript, dvsn_code, dvsn_descript, dvsn_mgr, dept_descript, dept_name, dept_mgr, handling_office_code, handling_office_descript, handling_office_mgr, claim_rep_key, claim_rep_full_name, claim_rep_first_name, claim_rep_last_name, claim_rep_mid_name, claim_rep_name_prfx, claim_rep_name_sfx, claim_rep_wbconnect_user_id, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, claim_rep_email, claim_rep_direct_automatic_pay_lmt, claim_rep_direct_automatic_reserve_lmt, handling_office_mgr_email, handling_office_mgr_direct_automatic_pay_lmt, handling_office_mgr_direct_automatic_reserve_lmt, claim_rep_branch_num, cost_center, claim_rep_num, ExceedAuthorityFlag, ClaimsDesktopAuthorityType)
	SELECT 
	claim_rep_id AS EDW_CLAIM_REP_PK_ID, 
	claim_rep_ak_id AS EDW_CLAIM_REP_AK_ID, 
	CO_DESCRIPT, 
	DVSN_CODE, 
	DVSN_DESCRIPT, 
	DVSN_MGR, 
	DEPT_DESCRIPT, 
	DEPT_NAME, 
	DEPT_MGR, 
	HANDLING_OFFICE_CODE, 
	HANDLING_OFFICE_DESCRIPT, 
	HANDLING_OFFICE_MGR, 
	CLAIM_REP_KEY, 
	CLAIM_REP_FULL_NAME, 
	CLAIM_REP_FIRST_NAME, 
	CLAIM_REP_LAST_NAME, 
	CLAIM_REP_MID_NAME, 
	CLAIM_REP_NAME_PRFX, 
	CLAIM_REP_NAME_SFX, 
	CLAIM_REP_WBCONNECT_USER_ID, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	CLAIM_REP_EMAIL, 
	CLAIM_REP_DIRECT_AUTOMATIC_PAY_LMT, 
	CLAIM_REP_DIRECT_AUTOMATIC_RESERVE_LMT, 
	HANDLING_OFFICE_MGR_EMAIL, 
	HANDLING_OFFICE_MGR_DIRECT_AUTOMATIC_PAY_LMT, 
	HANDLING_OFFICE_MGR_DIRECT_AUTOMATIC_RESERVE_LMT, 
	CLAIM_REP_BRANCH_NUM, 
	COST_CENTER, 
	CLAIM_REP_NUM, 
	EXCEEDAUTHORITYFLAG, 
	CLAIMSDESKTOPAUTHORITYTYPE
	FROM RTR_INS_UPD_INSERT
),
SQ_claim_representative_dim AS (
	SELECT 
	CLAIM_REPRESENTATIVE_DIM.CLAIM_REP_DIM_ID, 
	CLAIM_REPRESENTATIVE_DIM.EFF_FROM_DATE, 
	CLAIM_REPRESENTATIVE_DIM.EFF_TO_DATE, 
	CLAIM_REPRESENTATIVE_DIM.EDW_CLAIM_REP_AK_ID 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIM_REPRESENTATIVE_DIM CLAIM_REPRESENTATIVE_DIM
	WHERE EXISTS
	(
	SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIM_REPRESENTATIVE_DIM CLAIM_REPRESENTATIVE_DIM2
	WHERE CRRNT_SNPSHT_FLAG = 1 AND CLAIM_REPRESENTATIVE_DIM.EDW_CLAIM_REP_AK_ID = CLAIM_REPRESENTATIVE_DIM2.EDW_CLAIM_REP_AK_ID
	GROUP BY CLAIM_REPRESENTATIVE_DIM2.EDW_CLAIM_REP_AK_ID HAVING COUNT(*) > 1
	)
	ORDER BY CLAIM_REPRESENTATIVE_DIM.EDW_CLAIM_REP_AK_ID , CLAIM_REPRESENTATIVE_DIM.EFF_FROM_DATE DESC
),
EXP_Lag_eff_from_date111 AS (
	SELECT
	claim_rep_dim_id,
	edw_claim_rep_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claim_rep_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		edw_claim_rep_ak_id = v_PREV_ROW_occurrence_key, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claim_rep_ak_id AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_representative_dim
),
FIL_claim_representative_dim_update AS (
	SELECT
	claim_rep_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date111
	WHERE orig_eff_to_date != eff_to_date
),
UPD_claim_representative_dim AS (
	SELECT
	claim_rep_dim_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_claim_representative_dim_update
),
claim_representative_dim_expire AS (
	MERGE INTO claim_representative_dim AS T
	USING UPD_claim_representative_dim AS S
	ON T.claim_rep_dim_id = S.claim_rep_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),