WITH
LKP_Claim_Rep_AK_id AS (
	SELECT
	claim_rep_ak_id,
	claim_rep_key
	FROM (
		SELECT 
		a.claim_rep_ak_id as claim_rep_ak_id
		, ltrim(rtrim(a.claim_rep_key)) as claim_rep_key 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative a
		where a.crrnt_snpsht_flag = 1
		and a.source_sys_id	 = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key ORDER BY claim_rep_ak_id) = 1
),
SQ_clm_subrogation_stage AS (
	SELECT
		clm_subrogation_stage_id,
		tch_claim_nbr,
		tch_claimant_id,
		object_type_cd,
		object_seq_nbr,
		cov_type_cd,
		cov_seq_nbr,
		bur_cause_loss,
		insd_deduct,
		ref_subro_dt,
		general_comment,
		install_reached,
		amt_of_agreement,
		install_amt_month,
		start_pmt_dt,
		update_ts,
		update_user_id,
		create_ts,
		create_user_id,
		closure_date,
		subro_rep_clt_id,
		subro_mgr_clt_id,
		referring_adj_id,
		file_status,
		deduct_has_been,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM clm_subrogation_stage
),
EXP_source_anchor AS (
	SELECT
	clm_subrogation_stage_id,
	tch_claim_nbr,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_claim_nbr))) OR LENGTH(LTRIM(RTRIM(tch_claim_nbr))) = 0, 'N/A', LTRIM(RTRIM(tch_claim_nbr)))
	IFF(
	    LTRIM(RTRIM(tch_claim_nbr)) IS NULL OR LENGTH(LTRIM(RTRIM(tch_claim_nbr))) = 0, 'N/A',
	    LTRIM(RTRIM(tch_claim_nbr))
	) AS tch_claim_nbr_out,
	tch_claimant_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_claimant_id))) OR LENGTH(LTRIM(RTRIM(tch_claimant_id))) = 0, 'N/A', LTRIM(RTRIM(tch_claimant_id)))
	IFF(
	    LTRIM(RTRIM(tch_claimant_id)) IS NULL OR LENGTH(LTRIM(RTRIM(tch_claimant_id))) = 0, 'N/A',
	    LTRIM(RTRIM(tch_claimant_id))
	) AS tch_claimant_id_out,
	object_type_cd,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(object_type_cd))) OR LENGTH(LTRIM(RTRIM(object_type_cd))) = 0, 'N/A', LTRIM(RTRIM(object_type_cd)))
	IFF(
	    LTRIM(RTRIM(object_type_cd)) IS NULL OR LENGTH(LTRIM(RTRIM(object_type_cd))) = 0, 'N/A',
	    LTRIM(RTRIM(object_type_cd))
	) AS object_type_cd_out,
	object_seq_nbr,
	cov_type_cd,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(cov_type_cd))) OR LENGTH(LTRIM(RTRIM(cov_type_cd))) = 0, 'N/A', LTRIM(RTRIM(cov_type_cd)))
	IFF(
	    LTRIM(RTRIM(cov_type_cd)) IS NULL OR LENGTH(LTRIM(RTRIM(cov_type_cd))) = 0, 'N/A',
	    LTRIM(RTRIM(cov_type_cd))
	) AS cov_type_cd_out,
	cov_seq_nbr,
	bur_cause_loss,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(bur_cause_loss))) OR LENGTH(LTRIM(RTRIM(bur_cause_loss))) = 0, 'N/A',  SUBSTR(bur_cause_loss, 1,2))
	IFF(
	    LTRIM(RTRIM(bur_cause_loss)) IS NULL OR LENGTH(LTRIM(RTRIM(bur_cause_loss))) = 0, 'N/A',
	    SUBSTR(bur_cause_loss, 1, 2)
	) AS bur_cause_loss_out,
	-- *INF*: IIF(ISNULL(bur_cause_loss) OR IS_SPACES(bur_cause_loss), 'N/A', SUBSTR(bur_cause_loss, 3,1))
	IFF(
	    bur_cause_loss IS NULL OR LENGTH(bur_cause_loss)>0 AND TRIM(bur_cause_loss)='', 'N/A',
	    SUBSTR(bur_cause_loss, 3, 1)
	) AS reserve_category_out,
	subro_rep_clt_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(subro_rep_clt_id))) OR LENGTH(LTRIM(RTRIM(subro_rep_clt_id))) = 0, 'N/A', LTRIM(RTRIM(subro_rep_clt_id)))
	IFF(
	    LTRIM(RTRIM(subro_rep_clt_id)) IS NULL OR LENGTH(LTRIM(RTRIM(subro_rep_clt_id))) = 0, 'N/A',
	    LTRIM(RTRIM(subro_rep_clt_id))
	) AS subro_rep_clt_id_out,
	subro_mgr_clt_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(subro_mgr_clt_id))) OR LENGTH(LTRIM(RTRIM(subro_mgr_clt_id)))  = 0 , 'N/A', LTRIM(RTRIM(subro_mgr_clt_id)))
	IFF(
	    LTRIM(RTRIM(subro_mgr_clt_id)) IS NULL OR LENGTH(LTRIM(RTRIM(subro_mgr_clt_id))) = 0, 'N/A',
	    LTRIM(RTRIM(subro_mgr_clt_id))
	) AS subro_mgr_clt_id_out,
	referring_adj_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(referring_adj_id))) OR LENGTH(LTRIM(RTRIM(referring_adj_id))) = 0, 'N/A', LTRIM(RTRIM(referring_adj_id)))
	IFF(
	    LTRIM(RTRIM(referring_adj_id)) IS NULL OR LENGTH(LTRIM(RTRIM(referring_adj_id))) = 0, 'N/A',
	    LTRIM(RTRIM(referring_adj_id))
	) AS referring_adj_id_out
	FROM SQ_clm_subrogation_stage
),
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	claim_party_occurrence_ak_id,
	offset_onset_ind,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		CO.claim_occurrence_type_code as offset_onset_ind,
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CP.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.claim_party_role_code = 'CLMT'
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_claimant_coverage_detail AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_party_occurrence_ak_id,
	s3p_object_type_code,
	s3p_object_seq_num,
	s3p_pkg_seq_num,
	s3p_ins_line_code,
	s3p_unit_type_code,
	s3p_wc_class_descript,
	loc_unit_num,
	sub_loc_unit_num,
	ins_line,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	major_peril_code,
	major_peril_seq,
	reserve_ctgry,
	cause_of_loss,
	claimant_cov_eff_date,
	claimant_cov_exp_date,
	risk_type_ind,
	s3p_unit_descript,
	spec_pers_prop_use_code,
	pkg_ded_amt,
	pkg_lmt_amt,
	manual_entry_ind,
	unit_veh_registration_state_code,
	unit_veh_stated_amt,
	unit_dam_descript,
	unit_veh_yr,
	unit_veh_make,
	unit_vin_num,
	CoverageGUID,
	pms_type_bureau_code,
	IN_claim_party_occurrence_ak_id,
	IN_COB_OBJECT_TYPE_CD,
	IN_COB_OBJECT_SEQ_NBR,
	IN_MAJOR_PERIL_CODE1,
	IN_s3p_PKG_SEQ_NUM1,
	IN_cause_of_loss_out,
	IN_reserve_ctgry_out
	FROM (
		SELECT a.claimant_cov_det_ak_id as claimant_cov_det_ak_id,
		LTRIM(RTRIM(a.s3p_ins_line_code)) as s3p_ins_line_code, 
		LTRIM(RTRIM(a.s3p_unit_type_code)) as s3p_unit_type_code, 
		LTRIM(RTRIM(a.s3p_wc_class_descript)) as s3p_wc_class_descript, 
		LTRIM(RTRIM(a.loc_unit_num)) as loc_unit_num, 
		LTRIM(RTRIM(a.sub_loc_unit_num)) as sub_loc_unit_num, 
		LTRIM(RTRIM(a.ins_line)) as ins_line, 
		LTRIM(RTRIM(a.risk_unit_grp)) as risk_unit_grp, 
		LTRIM(RTRIM(a.risk_unit_grp_seq_num)) as risk_unit_grp_seq_num, 
		LTRIM(RTRIM(a.risk_unit)) as risk_unit, 
		LTRIM(RTRIM(a.risk_unit_seq_num)) as risk_unit_seq_num, 
		LTRIM(RTRIM(a.major_peril_seq)) as major_peril_seq, 
		a.claimant_cov_eff_date as claimant_cov_eff_date, 
		a.claimant_cov_exp_date as claimant_cov_exp_date, 
		LTRIM(RTRIM(a.risk_type_ind)) as risk_type_ind, 
		LTRIM(RTRIM(a.s3p_unit_descript)) as s3p_unit_descript, 
		LTRIM(RTRIM(a.spec_pers_prop_use_code)) as spec_pers_prop_use_code, 
		a.pkg_ded_amt as pkg_ded_amt, 
		a.pkg_lmt_amt as pkg_lmt_amt, 
		LTRIM(RTRIM(a.manual_entry_ind)) as manual_entry_ind,
		LTRIM(RTRIM(a.unit_veh_registration_state_code)) as unit_veh_registration_state_code, 
		a.unit_veh_stated_amt as unit_veh_stated_amt, 
		LTRIM(RTRIM(a.unit_dam_descript)) as unit_dam_descript, 
		a.unit_veh_yr as unit_veh_yr, 
		LTRIM(RTRIM(a.unit_veh_make)) as unit_veh_make, 
		LTRIM(RTRIM(a.unit_vin_num)) as unit_vin_num, 
		a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		LTRIM(RTRIM(a.s3p_object_type_code)) as s3p_object_type_code, 
		a.s3p_object_seq_num as s3p_object_seq_num, 
		LTRIM(RTRIM(a.major_peril_code)) as major_peril_code, 
		a.s3p_pkg_seq_num as s3p_pkg_seq_num, 
		a.cause_of_loss as cause_of_loss, 
		a.reserve_ctgry as reserve_ctgry,
		a.CoverageGUID as CoverageGUID, 
		LTRIM(RTRIM(a.pms_type_bureau_code)) as pms_type_bureau_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail a
		where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,s3p_object_type_code,s3p_object_seq_num,major_peril_code,s3p_pkg_seq_num,cause_of_loss,reserve_ctgry ORDER BY claimant_cov_det_ak_id) = 1
),
LKP_claim_subrogation AS (
	SELECT
	claim_subrogation_ak_id,
	claimant_cov_det_ak_id
	FROM (
		SELECT a.claim_subrogation_ak_id as claim_subrogation_ak_id, 
		a.claimant_cov_det_ak_id as claimant_cov_det_ak_id 
		FROM claim_subrogation a
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY claim_subrogation_ak_id) = 1
),
EXP_gather_values AS (
	SELECT
	LKP_claimant_coverage_detail.claimant_cov_det_ak_id,
	EXP_source_anchor.subro_rep_clt_id_out,
	EXP_source_anchor.subro_mgr_clt_id_out,
	EXP_source_anchor.referring_adj_id_out,
	LKP_claim_subrogation.claim_subrogation_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_REP_AK_ID(subro_rep_clt_id_out)
	LKP_CLAIM_REP_AK_ID_subro_rep_clt_id_out.claim_rep_ak_id AS v_subro_rep_ak_id,
	-- *INF*: IIF(ISNULL(v_subro_rep_ak_id), '-2', v_subro_rep_ak_id)
	IFF(v_subro_rep_ak_id IS NULL, '-2', v_subro_rep_ak_id) AS subro_rep_ak_id_out,
	'S' AS subro_rep_role_code,
	-- *INF*: :LKP.LKP_CLAIM_REP_AK_ID(referring_adj_id_out)
	LKP_CLAIM_REP_AK_ID_referring_adj_id_out.claim_rep_ak_id AS v_referring_rep_ak_id,
	-- *INF*: IIF(ISNULL(v_referring_rep_ak_id), '-2', v_referring_rep_ak_id)
	IFF(v_referring_rep_ak_id IS NULL, '-2', v_referring_rep_ak_id) AS referring_rep_ak_id_out,
	'R' AS referring_rep_role_code,
	-- *INF*: :LKP.LKP_CLAIM_REP_AK_ID(subro_mgr_clt_id_out)
	LKP_CLAIM_REP_AK_ID_subro_mgr_clt_id_out.claim_rep_ak_id AS v_subro_mgr_ak_id,
	-- *INF*: IIF(ISNULL(v_subro_mgr_ak_id), '-2', v_subro_mgr_ak_id)
	IFF(v_subro_mgr_ak_id IS NULL, '-2', v_subro_mgr_ak_id) AS subro_mgr_ak_id_out,
	'M' AS manager_role_code
	FROM EXP_source_anchor
	LEFT JOIN LKP_claim_subrogation
	ON LKP_claim_subrogation.claimant_cov_det_ak_id = LKP_claimant_coverage_detail.claimant_cov_det_ak_id
	LEFT JOIN LKP_claimant_coverage_detail
	ON LKP_claimant_coverage_detail.claim_party_occurrence_ak_id = LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id AND LKP_claimant_coverage_detail.s3p_object_type_code = EXP_source_anchor.object_type_cd_out AND LKP_claimant_coverage_detail.s3p_object_seq_num = EXP_source_anchor.object_seq_nbr AND LKP_claimant_coverage_detail.major_peril_code = EXP_source_anchor.cov_type_cd_out AND LKP_claimant_coverage_detail.s3p_pkg_seq_num = EXP_source_anchor.cov_seq_nbr AND LKP_claimant_coverage_detail.cause_of_loss = EXP_source_anchor.bur_cause_loss_out AND LKP_claimant_coverage_detail.reserve_ctgry = EXP_source_anchor.reserve_category_out
	LEFT JOIN LKP_CLAIM_REP_AK_ID LKP_CLAIM_REP_AK_ID_subro_rep_clt_id_out
	ON LKP_CLAIM_REP_AK_ID_subro_rep_clt_id_out.claim_rep_key = subro_rep_clt_id_out

	LEFT JOIN LKP_CLAIM_REP_AK_ID LKP_CLAIM_REP_AK_ID_referring_adj_id_out
	ON LKP_CLAIM_REP_AK_ID_referring_adj_id_out.claim_rep_key = referring_adj_id_out

	LEFT JOIN LKP_CLAIM_REP_AK_ID LKP_CLAIM_REP_AK_ID_subro_mgr_clt_id_out
	ON LKP_CLAIM_REP_AK_ID_subro_mgr_clt_id_out.claim_rep_key = subro_mgr_clt_id_out

),
Union_split_rows AS (
	SELECT claim_subrogation_ak_id, subro_rep_ak_id_out AS rep_ak_id, subro_rep_role_code AS rep_role_code
	FROM 
	UNION
	SELECT claim_subrogation_ak_id, subro_mgr_ak_id_out AS rep_ak_id, manager_role_code AS rep_role_code
	FROM 
	UNION
	SELECT claim_subrogation_ak_id, referring_rep_ak_id_out AS rep_ak_id, referring_rep_role_code AS rep_role_code
	FROM 
),
EXPTRANS AS (
	SELECT
	claim_subrogation_ak_id,
	rep_ak_id,
	rep_role_code
	FROM Union_split_rows
),
LKP_claim_representative_subrogation AS (
	SELECT
	claim_rep_subrogation_ak_id,
	claim_rep_ak_id,
	claim_subrogation_ak_id,
	claim_rep_role_code
	FROM (
		SELECT 
		A.claim_rep_subrogation_id AS claim_rep_subrogation_id,
		A.eff_from_date AS eff_from_date,
		A.eff_to_date AS eff_to_date,
		A.claim_subrogation_ak_id AS claim_subrogation_ak_id,
		A.claim_rep_ak_id AS claim_rep_ak_id,
		A.claim_rep_subrogation_ak_id AS claim_rep_subrogation_ak_id,
		A.claim_rep_role_code AS claim_rep_role_code
		FROM   claim_representative_subrogation A
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_subrogation_ak_id,claim_rep_role_code ORDER BY claim_rep_subrogation_ak_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_claim_representative_subrogation.claim_rep_subrogation_ak_id,
	LKP_claim_representative_subrogation.claim_rep_ak_id AS old_claim_rep_ak_id,
	EXPTRANS.claim_subrogation_ak_id,
	EXPTRANS.rep_ak_id,
	EXPTRANS.rep_role_code,
	-- *INF*: IIF(ISNULL(claim_rep_subrogation_ak_id), 'NEW',
	-- IIF(rep_ak_id != old_claim_rep_ak_id, 'UPDATE', 'NOCHANGE'))
	IFF(
	    claim_rep_subrogation_ak_id IS NULL, 'NEW',
	    IFF(
	        rep_ak_id != old_claim_rep_ak_id, 'UPDATE', 'NOCHANGE'
	    )
	) AS changed_flag
	FROM EXPTRANS
	LEFT JOIN LKP_claim_representative_subrogation
	ON LKP_claim_representative_subrogation.claim_subrogation_ak_id = EXPTRANS.claim_subrogation_ak_id AND LKP_claim_representative_subrogation.claim_rep_role_code = EXPTRANS.rep_role_code
),
FIL_inserts AS (
	SELECT
	claim_rep_subrogation_ak_id, 
	claim_subrogation_ak_id, 
	rep_ak_id, 
	rep_role_code, 
	changed_flag
	FROM EXP_detect_changes
	WHERE changed_flag <> 'NOCHANGE'
),
SEQ_claim_rep_subrogation_ak_id AS (
	CREATE SEQUENCE SEQ_claim_rep_subrogation_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_set_vlaues AS (
	SELECT
	SEQ_claim_rep_subrogation_ak_id.NEXTVAL,
	claim_rep_subrogation_ak_id,
	claim_subrogation_ak_id,
	rep_ak_id,
	rep_role_code,
	changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	sysdate AS created_modified_Date,
	-- *INF*: IIF(ISNULL(claim_rep_subrogation_ak_id), NEXTVAL, claim_rep_subrogation_ak_id)
	IFF(claim_rep_subrogation_ak_id IS NULL, NEXTVAL, claim_rep_subrogation_ak_id) AS claim_rep_subrogation_ak_id1
	FROM FIL_inserts
),
claim_representative_subrogation_insert AS (
	INSERT INTO claim_representative_subrogation
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_rep_subrogation_ak_id, claim_subrogation_ak_id, claim_rep_ak_id, claim_rep_role_code)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	created_modified_Date AS CREATED_DATE, 
	created_modified_Date AS MODIFIED_DATE, 
	claim_rep_subrogation_ak_id1 AS CLAIM_REP_SUBROGATION_AK_ID, 
	CLAIM_SUBROGATION_AK_ID, 
	rep_ak_id AS CLAIM_REP_AK_ID, 
	rep_role_code AS CLAIM_REP_ROLE_CODE
	FROM EXP_set_vlaues
),
SQ_claim_representative_subrogation AS (
	SELECT   a.claim_rep_subrogation_id,
	a.eff_from_date,
	a.eff_to_date,
	a.claim_rep_subrogation_ak_id,
	a.claim_subrogation_ak_id,
	a.claim_rep_ak_id,
	a.claim_rep_role_code
	FROM     @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_subrogation A
	WHERE    a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND      EXISTS
	(SELECT  1
	FROM     @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_subrogation B
	WHERE    B.source_sys_id  = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND B.crrnt_snpsht_flag  = '1'
	AND A.claim_rep_subrogation_ak_id = B.claim_rep_subrogation_ak_id
	AND A.claim_subrogation_ak_id  = B.claim_subrogation_ak_id
	AND  A.claim_rep_role_code  = B.claim_rep_role_code
	GROUP BY claim_rep_subrogation_ak_id, claim_subrogation_ak_id, claim_rep_role_code
	HAVING   COUNT(*) > 1)
	ORDER BY claim_rep_subrogation_ak_id, claim_subrogation_ak_id,   claim_rep_role_code,  eff_from_date DESC
),
EXPTRANS1 AS (
	SELECT
	claim_rep_subrogation_id,
	eff_from_date,
	eff_to_date,
	claim_rep_subrogation_ak_id,
	claim_subrogation_ak_id,
	claim_rep_ak_id,
	claim_rep_role_code,
	-- *INF*: DECODE(TRUE,
	-- 	claim_rep_subrogation_ak_id = v_prev_row_claim_rep_subrogation_ak_id AND claim_rep_role_code = v_prev_row_claim_rep_role_code, ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),
	-- 	eff_to_date)
	DECODE(
	    TRUE,
	    claim_rep_subrogation_ak_id = v_prev_row_claim_rep_subrogation_ak_id AND claim_rep_role_code = v_prev_row_claim_rep_role_code, DATEADD(SECOND,- 1,v_prev_row_eff_from_date),
	    eff_to_date
	) AS v_new_eff_to_date,
	v_new_eff_to_date AS new_eff_to_date,
	0 AS new_crrnt_snpsht_flag,
	sysdate AS new_modified_date,
	claim_rep_subrogation_ak_id AS v_prev_row_claim_rep_subrogation_ak_id,
	eff_from_date AS v_prev_row_eff_from_date,
	claim_rep_role_code AS v_prev_row_claim_rep_role_code
	FROM SQ_claim_representative_subrogation
),
FILTRANS AS (
	SELECT
	claim_rep_subrogation_id, 
	eff_to_date, 
	claim_rep_subrogation_ak_id, 
	claim_subrogation_ak_id, 
	claim_rep_ak_id, 
	claim_rep_role_code, 
	new_eff_to_date, 
	new_crrnt_snpsht_flag, 
	new_modified_date
	FROM EXPTRANS1
	WHERE eff_to_date <> new_eff_to_date
),
UPDTRANS AS (
	SELECT
	claim_rep_subrogation_id, 
	eff_to_date, 
	claim_rep_subrogation_ak_id, 
	claim_subrogation_ak_id, 
	claim_rep_ak_id, 
	claim_rep_role_code, 
	new_eff_to_date, 
	new_crrnt_snpsht_flag, 
	new_modified_date
	FROM FILTRANS
),
claim_representative_subrogation_update AS (
	MERGE INTO claim_representative_subrogation AS T
	USING UPDTRANS AS S
	ON T.claim_rep_subrogation_id = S.claim_rep_subrogation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.new_crrnt_snpsht_flag, T.eff_to_date = S.new_eff_to_date, T.modified_date = S.new_modified_date
),