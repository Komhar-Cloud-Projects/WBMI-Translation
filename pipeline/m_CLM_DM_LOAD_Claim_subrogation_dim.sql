WITH
SQ_claim_representative_subrogation AS (
	SELECT CRS_S.claim_rep_subrogation_id as claim_rep_subrogation_id_subro_rep,	  
	       CRS_S.claim_rep_subrogation_ak_id as claim_rep_subrogation_ak_id_subro_rep,
	       CRS_S.claim_rep_ak_id as claim_rep_ak_id_subro_rep,
	       CRS_S.claim_rep_role_code  as claim_rep_role_code_subro_rep,
		   
		   CRS_R.claim_rep_subrogation_id as claim_rep_subrogation_id_ref,	  
	       CRS_R.claim_rep_subrogation_ak_id as claim_rep_subrogation_ak_id_ref,
	       CRS_R.claim_rep_ak_id as claim_rep_ak_id_ref,
	       CRS_R.claim_rep_role_code  as claim_rep_role_code_ref,
		   CS.claim_subrogation_ak_id ,
	       CS.claim_subrogation_id ,
	       CS.claimant_cov_det_ak_id ,
	       CS.insd_ded_amt ,
	       CS.referred_to_subrogation_date ,
	       CS.subrogation_comment ,
	       CS.installment_reached_ind ,
	       CS.agreement_amt ,
	       CS.monthly_installment_amt ,
	       CS.pay_start_date ,
	       CS.file_status_code ,
	       CS.ded_status_code ,
	       CS.closure_date,
		distinct_eff_From_dates.eff_from_date 
	FROM   
	(
	SELECT claim_subrogation_ak_id,eff_from_date FROM dbo.claim_subrogation
	WHERE created_Date>= '@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT claim_subrogation_ak_id,eff_from_date FROM dbo.claim_representative_subrogation
	WHERE created_Date>= '@{pipeline().parameters.SELECTION_START_TS}'
	) AS distinct_eff_From_Dates
	
	left outer join claim_subrogation CS ON 
	distinct_eff_From_Dates.claim_subrogation_ak_id = CS.claim_subrogation_ak_id
	AND distinct_eff_From_Dates.eff_from_date between CS.eff_from_date AND CS.eff_to_date
	
	left outer join claim_representative_subrogation CRS_S ON 
	distinct_eff_From_Dates.claim_subrogation_ak_id = CRS_S.claim_subrogation_ak_id
	AND distinct_eff_From_Dates.eff_from_date between CRS_S.eff_from_date AND CRS_S.eff_to_date
	and CRS_S.claim_rep_role_code = 'S'
	
	left outer join claim_representative_subrogation CRS_R ON 
	distinct_eff_From_Dates.claim_subrogation_ak_id = CRS_R.claim_subrogation_ak_id
	AND distinct_eff_From_Dates.eff_from_date between CRS_R.eff_from_date AND CRS_R.eff_to_date
	and CRS_R.claim_rep_role_code = 'R'
),
LKP_claim_representative_ref_rep AS (
	SELECT
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
	claim_rep_ak_id
	FROM (
		SELECT claim_representative.claim_rep_key               AS claim_rep_key,
		       claim_representative.claim_rep_full_name         AS claim_rep_full_name,
		       claim_representative.claim_rep_first_name        AS claim_rep_first_name,
		       claim_representative.claim_rep_last_name         AS claim_rep_last_name,
		       claim_representative.claim_rep_mid_name          AS claim_rep_mid_name,
		       claim_representative.claim_rep_name_prfx         AS claim_rep_name_prfx,
		       claim_representative.claim_rep_name_sfx          AS claim_rep_name_sfx,
		       claim_representative.co_descript                 AS co_descript,
		       claim_representative.dvsn_code                   AS dvsn_code,
		       claim_representative.dvsn_descript               AS dvsn_descript,
		       claim_representative.dvsn_mgr                    AS dvsn_mgr,
		       claim_representative.dept_descript               AS dept_descript,
		       claim_representative.dept_name                   AS dept_name,
		       claim_representative.dept_mgr                    AS dept_mgr,
		       claim_representative.handling_office_code        AS handling_office_code,
		       claim_representative.handling_office_descript    AS handling_office_descript,
		       claim_representative.handling_office_mgr         AS handling_office_mgr,
		       claim_representative.claim_rep_wbconnect_user_id AS claim_rep_wbconnect_user_id,
		       claim_representative.claim_rep_ak_id             AS claim_rep_ak_id
		FROM   claim_representative
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id ORDER BY claim_rep_key) = 1
),
LKP_claim_representative_subro_rep AS (
	SELECT
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
	claim_rep_ak_id
	FROM (
		SELECT claim_representative.claim_rep_key               AS claim_rep_key,
		       claim_representative.claim_rep_full_name         AS claim_rep_full_name,
		       claim_representative.claim_rep_first_name        AS claim_rep_first_name,
		       claim_representative.claim_rep_last_name         AS claim_rep_last_name,
		       claim_representative.claim_rep_mid_name          AS claim_rep_mid_name,
		       claim_representative.claim_rep_name_prfx         AS claim_rep_name_prfx,
		       claim_representative.claim_rep_name_sfx          AS claim_rep_name_sfx,
		       claim_representative.co_descript                 AS co_descript,
		       claim_representative.dvsn_code                   AS dvsn_code,
		       claim_representative.dvsn_descript               AS dvsn_descript,
		       claim_representative.dvsn_mgr                    AS dvsn_mgr,
		       claim_representative.dept_descript               AS dept_descript,
		       claim_representative.dept_name                   AS dept_name,
		       claim_representative.dept_mgr                    AS dept_mgr,
		       claim_representative.handling_office_code        AS handling_office_code,
		       claim_representative.handling_office_descript    AS handling_office_descript,
		       claim_representative.handling_office_mgr         AS handling_office_mgr,
		       claim_representative.claim_rep_wbconnect_user_id AS claim_rep_wbconnect_user_id,
		       claim_representative.claim_rep_ak_id             AS claim_rep_ak_id
		FROM   claim_representative
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id ORDER BY claim_rep_key) = 1
),
LKP_sup_claim_subrogation_deductible_status_code AS (
	SELECT
	ded_status_code_descript,
	ded_status_code
	FROM (
		SELECT sup_claim_subrogation_deductible_status_code.ded_status_code_descript as ded_status_code_descript, sup_claim_subrogation_deductible_status_code.ded_status_code as ded_status_code 
		FROM sup_claim_subrogation_deductible_status_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ded_status_code ORDER BY ded_status_code_descript) = 1
),
LKP_sup_claim_subrogation_file_status_code AS (
	SELECT
	file_status_code_descript,
	file_status_code
	FROM (
		SELECT 
			file_status_code_descript,
			file_status_code
		FROM sup_claim_subrogation_file_status_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY file_status_code ORDER BY file_status_code_descript) = 1
),
EXPTRANS AS (
	SELECT
	SQ_claim_representative_subrogation.claim_rep_subrogation_id_subro_rep,
	SQ_claim_representative_subrogation.claim_rep_subrogation_ak_id_subro_rep,
	SQ_claim_representative_subrogation.claim_rep_ak_id_subro_rep,
	SQ_claim_representative_subrogation.claim_rep_role_code_subro_rep,
	SQ_claim_representative_subrogation.claim_rep_subrogation_id_ref,
	SQ_claim_representative_subrogation.claim_rep_subrogation_ak_id_ref,
	SQ_claim_representative_subrogation.claim_rep_ak_id_ref,
	SQ_claim_representative_subrogation.claim_rep_role_code_ref,
	SQ_claim_representative_subrogation.claim_subrogation_ak_id,
	SQ_claim_representative_subrogation.claim_subrogation_id,
	SQ_claim_representative_subrogation.claimant_cov_det_ak_id,
	SQ_claim_representative_subrogation.insd_ded_amt,
	SQ_claim_representative_subrogation.referred_to_subrogation_date,
	SQ_claim_representative_subrogation.subrogation_comment,
	SQ_claim_representative_subrogation.installment_reached_ind,
	SQ_claim_representative_subrogation.agreement_amt,
	SQ_claim_representative_subrogation.monthly_installment_amt,
	SQ_claim_representative_subrogation.pay_start_date,
	SQ_claim_representative_subrogation.file_status_code,
	SQ_claim_representative_subrogation.ded_status_code,
	SQ_claim_representative_subrogation.closure_date,
	SQ_claim_representative_subrogation.eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_modified_Date,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	LKP_sup_claim_subrogation_deductible_status_code.ded_status_code_descript,
	-- *INF*: IIF(ISNULL(ded_status_code_descript), 'N/A', ded_status_code_descript)
	IFF(ded_status_code_descript IS NULL, 'N/A', ded_status_code_descript) AS ded_status_code_descript_out,
	LKP_sup_claim_subrogation_file_status_code.file_status_code_descript,
	-- *INF*: IIF(ISNULL(file_status_code_descript), 'N/A', file_status_code_descript)
	IFF(file_status_code_descript IS NULL, 'N/A', file_status_code_descript) AS file_status_code_descript_out,
	LKP_claim_representative_subro_rep.claim_rep_key AS claim_rep_key_subro_rep,
	LKP_claim_representative_subro_rep.claim_rep_full_name AS claim_rep_full_name_subro_rep,
	LKP_claim_representative_subro_rep.claim_rep_first_name AS claim_rep_first_name_subro_rep,
	LKP_claim_representative_subro_rep.claim_rep_last_name AS claim_rep_last_name_subro_rep,
	LKP_claim_representative_subro_rep.claim_rep_mid_name AS claim_rep_mid_name_subro_rep,
	LKP_claim_representative_subro_rep.claim_rep_name_prfx AS claim_rep_name_prfx_subro_rep,
	LKP_claim_representative_subro_rep.claim_rep_name_sfx AS claim_rep_name_sfx_subro_rep,
	LKP_claim_representative_subro_rep.co_descript AS co_descript_subro_rep,
	LKP_claim_representative_subro_rep.dvsn_code AS dvsn_code_subro_rep,
	LKP_claim_representative_subro_rep.dvsn_descript AS dvsn_descript_subro_rep,
	LKP_claim_representative_subro_rep.dvsn_mgr AS dvsn_mgr_subro_rep,
	LKP_claim_representative_subro_rep.dept_descript AS dept_descript_subro_rep,
	LKP_claim_representative_subro_rep.dept_name AS dept_name_subro_rep,
	LKP_claim_representative_subro_rep.dept_mgr AS dept_mgr_subro_rep,
	LKP_claim_representative_subro_rep.handling_office_code AS handling_office_code_subro_rep,
	LKP_claim_representative_subro_rep.handling_office_descript AS handling_office_descript_subro_rep,
	LKP_claim_representative_subro_rep.handling_office_mgr AS handling_office_mgr_subro_rep,
	LKP_claim_representative_subro_rep.claim_rep_wbconnect_user_id AS claim_rep_wbconnect_user_id_subro_rep,
	LKP_claim_representative_ref_rep.claim_rep_key AS claim_rep_key_ref_rep,
	LKP_claim_representative_ref_rep.claim_rep_full_name AS claim_rep_full_name_ref_rep,
	LKP_claim_representative_ref_rep.claim_rep_first_name AS claim_rep_first_name_ref_rep,
	LKP_claim_representative_ref_rep.claim_rep_last_name AS claim_rep_last_name_ref_rep,
	LKP_claim_representative_ref_rep.claim_rep_mid_name AS claim_rep_mid_name_ref_rep,
	LKP_claim_representative_ref_rep.claim_rep_name_prfx AS claim_rep_name_prfx_ref_rep,
	LKP_claim_representative_ref_rep.claim_rep_name_sfx AS claim_rep_name_sfx_ref_rep,
	LKP_claim_representative_ref_rep.co_descript AS co_descript_ref_rep,
	LKP_claim_representative_ref_rep.dvsn_code AS dvsn_code_ref_rep,
	LKP_claim_representative_ref_rep.dvsn_descript AS dvsn_descript_ref_rep,
	LKP_claim_representative_ref_rep.dvsn_mgr AS dvsn_mgr_ref_rep,
	LKP_claim_representative_ref_rep.dept_descript AS dept_descript_ref_rep,
	LKP_claim_representative_ref_rep.dept_name AS dept_name_ref_rep,
	LKP_claim_representative_ref_rep.dept_mgr AS dept_mgr_ref_rep,
	LKP_claim_representative_ref_rep.handling_office_code AS handling_office_code_ref_rep,
	LKP_claim_representative_ref_rep.handling_office_descript AS handling_office_descript_ref_rep,
	LKP_claim_representative_ref_rep.handling_office_mgr AS handling_office_mgr_ref_rep,
	LKP_claim_representative_ref_rep.claim_rep_wbconnect_user_id AS claim_rep_wbconnect_user_id_ref_rep
	FROM SQ_claim_representative_subrogation
	LEFT JOIN LKP_claim_representative_ref_rep
	ON LKP_claim_representative_ref_rep.claim_rep_ak_id = SQ_claim_representative_subrogation.claim_rep_ak_id_ref
	LEFT JOIN LKP_claim_representative_subro_rep
	ON LKP_claim_representative_subro_rep.claim_rep_ak_id = SQ_claim_representative_subrogation.claim_rep_ak_id_subro_rep
	LEFT JOIN LKP_sup_claim_subrogation_deductible_status_code
	ON LKP_sup_claim_subrogation_deductible_status_code.ded_status_code = SQ_claim_representative_subrogation.ded_status_code
	LEFT JOIN LKP_sup_claim_subrogation_file_status_code
	ON LKP_sup_claim_subrogation_file_status_code.file_status_code = SQ_claim_representative_subrogation.file_status_code
),
LKP_claim_subrogation_dim AS (
	SELECT
	claim_subrogation_dim_id,
	edw_claim_subrogation_pk_id,
	edw_claim_rep_subrogation_pk_id_subrogation_rep,
	edw_claim_rep_subrogation_pk_id_referring_adjuster
	FROM (
		SELECT 
			claim_subrogation_dim_id,
			edw_claim_subrogation_pk_id,
			edw_claim_rep_subrogation_pk_id_subrogation_rep,
			edw_claim_rep_subrogation_pk_id_referring_adjuster
		FROM claim_subrogation_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_subrogation_pk_id,edw_claim_rep_subrogation_pk_id_subrogation_rep,edw_claim_rep_subrogation_pk_id_referring_adjuster ORDER BY claim_subrogation_dim_id) = 1
),
RTRTRANS AS (
	SELECT
	LKP_claim_subrogation_dim.claim_subrogation_dim_id,
	EXPTRANS.claim_rep_subrogation_id_subro_rep,
	EXPTRANS.claim_rep_subrogation_ak_id_subro_rep,
	EXPTRANS.claim_rep_ak_id_subro_rep,
	EXPTRANS.claim_rep_role_code_subro_rep,
	EXPTRANS.claim_rep_subrogation_id_ref,
	EXPTRANS.claim_rep_subrogation_ak_id_ref,
	EXPTRANS.claim_rep_ak_id_ref,
	EXPTRANS.claim_rep_role_code_ref,
	EXPTRANS.claim_subrogation_ak_id,
	EXPTRANS.claim_subrogation_id,
	EXPTRANS.claimant_cov_det_ak_id,
	EXPTRANS.insd_ded_amt,
	EXPTRANS.referred_to_subrogation_date,
	EXPTRANS.subrogation_comment,
	EXPTRANS.installment_reached_ind,
	EXPTRANS.agreement_amt,
	EXPTRANS.monthly_installment_amt,
	EXPTRANS.pay_start_date,
	EXPTRANS.file_status_code,
	EXPTRANS.ded_status_code,
	EXPTRANS.closure_date,
	EXPTRANS.eff_from_date,
	EXPTRANS.eff_to_date,
	EXPTRANS.created_modified_Date,
	EXPTRANS.crrnt_snpsht_flag,
	EXPTRANS.audit_id,
	EXPTRANS.ded_status_code_descript_out AS ded_status_code_descript,
	EXPTRANS.file_status_code_descript_out AS file_status_code_descript,
	EXPTRANS.claim_rep_key_subro_rep,
	EXPTRANS.claim_rep_full_name_subro_rep,
	EXPTRANS.claim_rep_first_name_subro_rep,
	EXPTRANS.claim_rep_last_name_subro_rep,
	EXPTRANS.claim_rep_mid_name_subro_rep,
	EXPTRANS.claim_rep_name_prfx_subro_rep,
	EXPTRANS.claim_rep_name_sfx_subro_rep,
	EXPTRANS.co_descript_subro_rep,
	EXPTRANS.dvsn_code_subro_rep,
	EXPTRANS.dvsn_descript_subro_rep,
	EXPTRANS.dvsn_mgr_subro_rep,
	EXPTRANS.dept_descript_subro_rep,
	EXPTRANS.dept_name_subro_rep,
	EXPTRANS.dept_mgr_subro_rep,
	EXPTRANS.handling_office_code_subro_rep,
	EXPTRANS.handling_office_descript_subro_rep,
	EXPTRANS.handling_office_mgr_subro_rep,
	EXPTRANS.claim_rep_wbconnect_user_id_subro_rep,
	EXPTRANS.claim_rep_key_ref_rep,
	EXPTRANS.claim_rep_full_name_ref_rep,
	EXPTRANS.claim_rep_first_name_ref_rep,
	EXPTRANS.claim_rep_last_name_ref_rep,
	EXPTRANS.claim_rep_mid_name_ref_rep,
	EXPTRANS.claim_rep_name_prfx_ref_rep,
	EXPTRANS.claim_rep_name_sfx_ref_rep,
	EXPTRANS.co_descript_ref_rep,
	EXPTRANS.dvsn_code_ref_rep,
	EXPTRANS.dvsn_descript_ref_rep,
	EXPTRANS.dvsn_mgr_ref_rep,
	EXPTRANS.dept_descript_ref_rep,
	EXPTRANS.dept_name_ref_rep,
	EXPTRANS.dept_mgr_ref_rep,
	EXPTRANS.handling_office_code_ref_rep,
	EXPTRANS.handling_office_descript_ref_rep,
	EXPTRANS.handling_office_mgr_ref_rep,
	EXPTRANS.claim_rep_wbconnect_user_id_ref_rep
	FROM EXPTRANS
	LEFT JOIN LKP_claim_subrogation_dim
	ON LKP_claim_subrogation_dim.edw_claim_subrogation_pk_id = EXPTRANS.claim_subrogation_id AND LKP_claim_subrogation_dim.edw_claim_rep_subrogation_pk_id_subrogation_rep = EXPTRANS.claim_rep_subrogation_id_subro_rep AND LKP_claim_subrogation_dim.edw_claim_rep_subrogation_pk_id_referring_adjuster = EXPTRANS.claim_rep_subrogation_id_ref
),
RTRTRANS_Insert AS (SELECT * FROM RTRTRANS WHERE ISNULL(claim_subrogation_dim_id)),
RTRTRANS_DEFAULT1 AS (SELECT * FROM RTRTRANS WHERE NOT ( (ISNULL(claim_subrogation_dim_id)) )),
claim_subrogation_dim AS (
	INSERT INTO claim_subrogation_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, edw_claim_subrogation_pk_id, edw_claim_rep_subrogation_pk_id_subrogation_rep, edw_claim_rep_subrogation_pk_id_referring_adjuster, edw_claim_subrogation_ak_id, edw_claimant_cov_det_ak_id, edw_claim_rep_subrogation_ak_id_subrogation_rep, edw_claim_rep_subrogation_ak_id_referring_adjuster, insd_ded_amt, referred_to_subrogation_date, subrogation_comment, installment_reached_ind, agreement_amt, monthly_installment_amt, pay_start_date, file_status_code, file_status_code_descript, ded_status_code, ded_status_code_descript, closure_date, subrogation_rep_co_descript, subrogation_rep_dvsn_code, subrogation_rep_dvsn_descript, subrogation_rep_dvsn_mgr, subrogation_rep_dept_descript, subrogation_rep_dept_name, subrogation_rep_dept_mgr, subrogation_rep_handling_office_code, subrogation_rep_handling_office_descript, subrogation_rep_handling_office_mgr, subrogation_rep_key, subrogation_rep_full_name, subrogation_rep_first_name, subrogation_rep_last_name, subrogation_rep_mid_name, subrogation_rep_name_prfx, subrogation_rep_name_sfx, subrogation_rep_wbconnect_user_id, referring_adjuster_co_descript, referring_adjuster_dvsn_code, referring_adjuster_dvsn_descript, referring_adjuster_dvsn_mgr, referring_adjuster_dept_descript, referring_adjuster_dept_name, referring_adjuster_dept_mgr, referring_adjuster_handling_office_code, referring_adjuster_handling_office_descript, referring_adjuster_handling_office_mgr, referring_adjuster_key, referring_adjuster_full_name, referring_adjuster_first_name, referring_adjuster_last_name, referring_adjuster_mid_name, referring_adjuster_name_prfx, referring_adjuster_name_sfx, referring_adjuster_wbconnect_user_id)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	created_modified_Date AS CREATED_DATE, 
	created_modified_Date AS MODIFIED_DATE, 
	claim_subrogation_id AS EDW_CLAIM_SUBROGATION_PK_ID, 
	claim_rep_subrogation_id_subro_rep AS EDW_CLAIM_REP_SUBROGATION_PK_ID_SUBROGATION_REP, 
	claim_rep_subrogation_id_ref AS EDW_CLAIM_REP_SUBROGATION_PK_ID_REFERRING_ADJUSTER, 
	claim_subrogation_ak_id AS EDW_CLAIM_SUBROGATION_AK_ID, 
	claimant_cov_det_ak_id AS EDW_CLAIMANT_COV_DET_AK_ID, 
	claim_rep_subrogation_ak_id_subro_rep AS EDW_CLAIM_REP_SUBROGATION_AK_ID_SUBROGATION_REP, 
	claim_rep_subrogation_ak_id_ref AS EDW_CLAIM_REP_SUBROGATION_AK_ID_REFERRING_ADJUSTER, 
	INSD_DED_AMT, 
	REFERRED_TO_SUBROGATION_DATE, 
	SUBROGATION_COMMENT, 
	INSTALLMENT_REACHED_IND, 
	AGREEMENT_AMT, 
	MONTHLY_INSTALLMENT_AMT, 
	PAY_START_DATE, 
	FILE_STATUS_CODE, 
	FILE_STATUS_CODE_DESCRIPT, 
	DED_STATUS_CODE, 
	DED_STATUS_CODE_DESCRIPT, 
	CLOSURE_DATE, 
	co_descript_subro_rep AS SUBROGATION_REP_CO_DESCRIPT, 
	dvsn_code_subro_rep AS SUBROGATION_REP_DVSN_CODE, 
	dvsn_descript_subro_rep AS SUBROGATION_REP_DVSN_DESCRIPT, 
	dvsn_mgr_subro_rep AS SUBROGATION_REP_DVSN_MGR, 
	dept_descript_subro_rep AS SUBROGATION_REP_DEPT_DESCRIPT, 
	dept_name_subro_rep AS SUBROGATION_REP_DEPT_NAME, 
	dept_mgr_subro_rep AS SUBROGATION_REP_DEPT_MGR, 
	handling_office_code_subro_rep AS SUBROGATION_REP_HANDLING_OFFICE_CODE, 
	handling_office_descript_subro_rep AS SUBROGATION_REP_HANDLING_OFFICE_DESCRIPT, 
	handling_office_mgr_subro_rep AS SUBROGATION_REP_HANDLING_OFFICE_MGR, 
	claim_rep_key_subro_rep AS SUBROGATION_REP_KEY, 
	claim_rep_full_name_subro_rep AS SUBROGATION_REP_FULL_NAME, 
	claim_rep_first_name_subro_rep AS SUBROGATION_REP_FIRST_NAME, 
	claim_rep_last_name_subro_rep AS SUBROGATION_REP_LAST_NAME, 
	claim_rep_mid_name_subro_rep AS SUBROGATION_REP_MID_NAME, 
	claim_rep_name_prfx_subro_rep AS SUBROGATION_REP_NAME_PRFX, 
	claim_rep_name_sfx_subro_rep AS SUBROGATION_REP_NAME_SFX, 
	claim_rep_wbconnect_user_id_subro_rep AS SUBROGATION_REP_WBCONNECT_USER_ID, 
	co_descript_ref_rep AS REFERRING_ADJUSTER_CO_DESCRIPT, 
	dvsn_code_ref_rep AS REFERRING_ADJUSTER_DVSN_CODE, 
	dvsn_descript_ref_rep AS REFERRING_ADJUSTER_DVSN_DESCRIPT, 
	dvsn_mgr_ref_rep AS REFERRING_ADJUSTER_DVSN_MGR, 
	dept_descript_ref_rep AS REFERRING_ADJUSTER_DEPT_DESCRIPT, 
	dept_name_ref_rep AS REFERRING_ADJUSTER_DEPT_NAME, 
	dept_mgr_ref_rep AS REFERRING_ADJUSTER_DEPT_MGR, 
	handling_office_code_ref_rep AS REFERRING_ADJUSTER_HANDLING_OFFICE_CODE, 
	handling_office_descript_ref_rep AS REFERRING_ADJUSTER_HANDLING_OFFICE_DESCRIPT, 
	handling_office_mgr_ref_rep AS REFERRING_ADJUSTER_HANDLING_OFFICE_MGR, 
	claim_rep_key_ref_rep AS REFERRING_ADJUSTER_KEY, 
	claim_rep_full_name_ref_rep AS REFERRING_ADJUSTER_FULL_NAME, 
	claim_rep_first_name_ref_rep AS REFERRING_ADJUSTER_FIRST_NAME, 
	claim_rep_last_name_ref_rep AS REFERRING_ADJUSTER_LAST_NAME, 
	claim_rep_mid_name_ref_rep AS REFERRING_ADJUSTER_MID_NAME, 
	claim_rep_name_prfx_ref_rep AS REFERRING_ADJUSTER_NAME_PRFX, 
	claim_rep_name_sfx_ref_rep AS REFERRING_ADJUSTER_NAME_SFX, 
	claim_rep_wbconnect_user_id_ref_rep AS REFERRING_ADJUSTER_WBCONNECT_USER_ID
	FROM RTRTRANS_Insert
),
UPD_existing_records AS (
	SELECT
	claim_subrogation_dim_id AS claim_subrogation_dim_id2, 
	claim_rep_subrogation_id_subro_rep AS claim_rep_subrogation_id_subro_rep2, 
	claim_rep_subrogation_ak_id_subro_rep AS claim_rep_subrogation_ak_id_subro_rep2, 
	claim_rep_ak_id_subro_rep AS claim_rep_ak_id_subro_rep2, 
	claim_rep_role_code_subro_rep AS claim_rep_role_code_subro_rep2, 
	claim_rep_subrogation_id_ref AS claim_rep_subrogation_id_ref2, 
	claim_rep_subrogation_ak_id_ref AS claim_rep_subrogation_ak_id_ref2, 
	claim_rep_ak_id_ref AS claim_rep_ak_id_ref2, 
	claim_rep_role_code_ref AS claim_rep_role_code_ref2, 
	claim_subrogation_ak_id AS claim_subrogation_ak_id2, 
	claim_subrogation_id AS claim_subrogation_id2, 
	claimant_cov_det_ak_id AS claimant_cov_det_ak_id2, 
	insd_ded_amt AS insd_ded_amt2, 
	referred_to_subrogation_date AS referred_to_subrogation_date2, 
	subrogation_comment AS subrogation_comment2, 
	installment_reached_ind AS installment_reached_ind2, 
	agreement_amt AS agreement_amt2, 
	monthly_installment_amt AS monthly_installment_amt2, 
	pay_start_date AS pay_start_date2, 
	file_status_code AS file_status_code2, 
	ded_status_code AS ded_status_code2, 
	closure_date AS closure_date2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	created_modified_Date AS created_modified_Date2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	ded_status_code_descript AS ded_status_code_descript2, 
	file_status_code_descript AS file_status_code_descript2, 
	claim_rep_key_subro_rep AS claim_rep_key_subro_rep2, 
	claim_rep_full_name_subro_rep AS claim_rep_full_name_subro_rep2, 
	claim_rep_first_name_subro_rep AS claim_rep_first_name_subro_rep2, 
	claim_rep_last_name_subro_rep AS claim_rep_last_name_subro_rep2, 
	claim_rep_mid_name_subro_rep AS claim_rep_mid_name_subro_rep2, 
	claim_rep_name_prfx_subro_rep AS claim_rep_name_prfx_subro_rep2, 
	claim_rep_name_sfx_subro_rep AS claim_rep_name_sfx_subro_rep2, 
	co_descript_subro_rep AS co_descript_subro_rep2, 
	dvsn_code_subro_rep AS dvsn_code_subro_rep2, 
	dvsn_descript_subro_rep AS dvsn_descript_subro_rep2, 
	dvsn_mgr_subro_rep AS dvsn_mgr_subro_rep2, 
	dept_descript_subro_rep AS dept_descript_subro_rep2, 
	dept_name_subro_rep AS dept_name_subro_rep2, 
	dept_mgr_subro_rep AS dept_mgr_subro_rep2, 
	handling_office_code_subro_rep AS handling_office_code_subro_rep2, 
	handling_office_descript_subro_rep AS handling_office_descript_subro_rep2, 
	handling_office_mgr_subro_rep AS handling_office_mgr_subro_rep2, 
	claim_rep_wbconnect_user_id_subro_rep AS claim_rep_wbconnect_user_id_subro_rep2, 
	claim_rep_key_ref_rep AS claim_rep_key_ref_rep2, 
	claim_rep_full_name_ref_rep AS claim_rep_full_name_ref_rep2, 
	claim_rep_first_name_ref_rep AS claim_rep_first_name_ref_rep2, 
	claim_rep_last_name_ref_rep AS claim_rep_last_name_ref_rep2, 
	claim_rep_mid_name_ref_rep AS claim_rep_mid_name_ref_rep2, 
	claim_rep_name_prfx_ref_rep AS claim_rep_name_prfx_ref_rep2, 
	claim_rep_name_sfx_ref_rep AS claim_rep_name_sfx_ref_rep2, 
	co_descript_ref_rep AS co_descript_ref_rep2, 
	dvsn_code_ref_rep AS dvsn_code_ref_rep2, 
	dvsn_descript_ref_rep AS dvsn_descript_ref_rep2, 
	dvsn_mgr_ref_rep AS dvsn_mgr_ref_rep2, 
	dept_descript_ref_rep AS dept_descript_ref_rep2, 
	dept_name_ref_rep AS dept_name_ref_rep2, 
	dept_mgr_ref_rep AS dept_mgr_ref_rep2, 
	handling_office_code_ref_rep AS handling_office_code_ref_rep2, 
	handling_office_descript_ref_rep AS handling_office_descript_ref_rep2, 
	handling_office_mgr_ref_rep AS handling_office_mgr_ref_rep2, 
	claim_rep_wbconnect_user_id_ref_rep AS claim_rep_wbconnect_user_id_ref_rep2
	FROM RTRTRANS_DEFAULT1
),
claim_subrogation_dim_update AS (
	MERGE INTO claim_subrogation_dim AS T
	USING UPD_existing_records AS S
	ON T.claim_subrogation_dim_id = S.claim_subrogation_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.modified_date = S.created_modified_Date2, T.edw_claim_subrogation_pk_id = S.claim_subrogation_id2, T.edw_claim_rep_subrogation_pk_id_subrogation_rep = S.claim_rep_subrogation_id_subro_rep2, T.edw_claim_rep_subrogation_pk_id_referring_adjuster = S.claim_rep_subrogation_id_ref2, T.edw_claim_subrogation_ak_id = S.claim_subrogation_ak_id2, T.edw_claimant_cov_det_ak_id = S.claimant_cov_det_ak_id2, T.edw_claim_rep_subrogation_ak_id_subrogation_rep = S.claim_rep_subrogation_ak_id_subro_rep2, T.edw_claim_rep_subrogation_ak_id_referring_adjuster = S.claim_rep_subrogation_ak_id_ref2, T.insd_ded_amt = S.insd_ded_amt2, T.referred_to_subrogation_date = S.referred_to_subrogation_date2, T.subrogation_comment = S.subrogation_comment2, T.installment_reached_ind = S.installment_reached_ind2, T.agreement_amt = S.agreement_amt2, T.monthly_installment_amt = S.monthly_installment_amt2, T.pay_start_date = S.pay_start_date2, T.file_status_code = S.file_status_code2, T.file_status_code_descript = S.file_status_code_descript2, T.ded_status_code = S.ded_status_code2, T.ded_status_code_descript = S.ded_status_code_descript2, T.closure_date = S.closure_date2, T.subrogation_rep_co_descript = S.co_descript_subro_rep2, T.subrogation_rep_dvsn_code = S.dvsn_code_subro_rep2, T.subrogation_rep_dvsn_descript = S.dvsn_descript_subro_rep2, T.subrogation_rep_dvsn_mgr = S.dvsn_mgr_subro_rep2, T.subrogation_rep_dept_descript = S.dept_descript_subro_rep2, T.subrogation_rep_dept_name = S.dept_name_subro_rep2, T.subrogation_rep_dept_mgr = S.dept_mgr_subro_rep2, T.subrogation_rep_handling_office_code = S.handling_office_code_subro_rep2, T.subrogation_rep_handling_office_descript = S.handling_office_descript_subro_rep2, T.subrogation_rep_handling_office_mgr = S.handling_office_mgr_subro_rep2, T.subrogation_rep_key = S.claim_rep_key_subro_rep2, T.subrogation_rep_full_name = S.claim_rep_full_name_subro_rep2, T.subrogation_rep_first_name = S.claim_rep_first_name_subro_rep2, T.subrogation_rep_last_name = S.claim_rep_last_name_subro_rep2, T.subrogation_rep_mid_name = S.claim_rep_mid_name_subro_rep2, T.subrogation_rep_name_prfx = S.claim_rep_name_prfx_subro_rep2, T.subrogation_rep_name_sfx = S.claim_rep_name_sfx_subro_rep2, T.subrogation_rep_wbconnect_user_id = S.claim_rep_wbconnect_user_id_subro_rep2, T.referring_adjuster_co_descript = S.co_descript_ref_rep2, T.referring_adjuster_dvsn_code = S.dvsn_code_ref_rep2, T.referring_adjuster_dvsn_descript = S.dvsn_descript_ref_rep2, T.referring_adjuster_dvsn_mgr = S.dvsn_mgr_ref_rep2, T.referring_adjuster_dept_descript = S.dept_descript_ref_rep2, T.referring_adjuster_dept_name = S.dept_name_ref_rep2, T.referring_adjuster_dept_mgr = S.dept_mgr_ref_rep2, T.referring_adjuster_handling_office_code = S.handling_office_code_ref_rep2, T.referring_adjuster_handling_office_descript = S.handling_office_descript_ref_rep2, T.referring_adjuster_handling_office_mgr = S.handling_office_mgr_ref_rep2, T.referring_adjuster_key = S.claim_rep_key_ref_rep2, T.referring_adjuster_full_name = S.claim_rep_full_name_ref_rep2, T.referring_adjuster_first_name = S.claim_rep_first_name_ref_rep2, T.referring_adjuster_last_name = S.claim_rep_last_name_ref_rep2, T.referring_adjuster_mid_name = S.claim_rep_mid_name_ref_rep2, T.referring_adjuster_name_prfx = S.claim_rep_name_prfx_ref_rep2, T.referring_adjuster_name_sfx = S.claim_rep_name_sfx_ref_rep2, T.referring_adjuster_wbconnect_user_id = S.claim_rep_wbconnect_user_id_ref_rep2
),
SQ_claim_subrogation_dim AS (
	SELECT a.claim_subrogation_dim_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.edw_claim_subrogation_ak_id 
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_subrogation_dim a
	WHERE EXISTS
	(SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_subrogation_dim b
	WHERE b.crrnt_snpsht_flag = 1 AND
	a.edw_claim_subrogation_ak_id = b.edw_claim_subrogation_ak_id
	GROUP BY b.edw_claim_subrogation_ak_id HAVING count(*) > 1)
	ORDER BY a.edw_claim_subrogation_ak_id, a.eff_from_date DESC
),
EXP_Source AS (
	SELECT
	claim_subrogation_dim_id,
	edw_claim_subrogation_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claim_subrogation_ak_id=v_PREV_ROW_edw_claim_subrogation_ak_id , 	ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1)
	--        ,orig_eff_to_date)
	DECODE(
	    TRUE,
	    edw_claim_subrogation_ak_id = v_PREV_ROW_edw_claim_subrogation_ak_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS o_eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claim_subrogation_ak_id AS v_PREV_ROW_edw_claim_subrogation_ak_id,
	sysdate AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_subrogation_dim
),
FLT_Source_Rows AS (
	SELECT
	claim_subrogation_dim_id, 
	orig_eff_to_date, 
	o_eff_to_date AS eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Source
	WHERE orig_eff_to_date <> eff_to_date
),
Upd_Update_Eff_Dates AS (
	SELECT
	claim_subrogation_dim_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FLT_Source_Rows
),
claim_subrogation_dim_expire_rows AS (
	MERGE INTO claim_subrogation_dim AS T
	USING Upd_Update_Eff_Dates AS S
	ON T.claim_subrogation_dim_id = S.claim_subrogation_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),