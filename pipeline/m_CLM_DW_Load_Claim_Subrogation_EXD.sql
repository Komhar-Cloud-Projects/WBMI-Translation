WITH
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
	IFF(LTRIM(RTRIM(tch_claim_nbr)) IS NULL OR LENGTH(LTRIM(RTRIM(tch_claim_nbr))) = 0, 'N/A', LTRIM(RTRIM(tch_claim_nbr))) AS tch_claim_nbr_out,
	tch_claimant_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_claimant_id))) OR LENGTH(LTRIM(RTRIM(tch_claimant_id))) = 0, 'N/A', LTRIM(RTRIM(tch_claimant_id)))
	IFF(LTRIM(RTRIM(tch_claimant_id)) IS NULL OR LENGTH(LTRIM(RTRIM(tch_claimant_id))) = 0, 'N/A', LTRIM(RTRIM(tch_claimant_id))) AS tch_claimant_id_out,
	object_type_cd,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(object_type_cd))) OR LENGTH(LTRIM(RTRIM(object_type_cd))) = 0, 'N/A', LTRIM(RTRIM(object_type_cd)))
	IFF(LTRIM(RTRIM(object_type_cd)) IS NULL OR LENGTH(LTRIM(RTRIM(object_type_cd))) = 0, 'N/A', LTRIM(RTRIM(object_type_cd))) AS object_type_cd_out,
	object_seq_nbr,
	cov_type_cd,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(cov_type_cd))) OR LENGTH(LTRIM(RTRIM(cov_type_cd))) = 0, 'N/A', LTRIM(RTRIM(cov_type_cd)))
	IFF(LTRIM(RTRIM(cov_type_cd)) IS NULL OR LENGTH(LTRIM(RTRIM(cov_type_cd))) = 0, 'N/A', LTRIM(RTRIM(cov_type_cd))) AS cov_type_cd_out,
	cov_seq_nbr,
	bur_cause_loss,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(bur_cause_loss))) OR LENGTH(LTRIM(RTRIM(bur_cause_loss))) = 0, 'N/A',  SUBSTR(bur_cause_loss, 1,2))
	IFF(LTRIM(RTRIM(bur_cause_loss)) IS NULL OR LENGTH(LTRIM(RTRIM(bur_cause_loss))) = 0, 'N/A', SUBSTR(bur_cause_loss, 1, 2)) AS bur_cause_loss_out,
	-- *INF*: IIF(ISNULL(bur_cause_loss) OR IS_SPACES(bur_cause_loss), 'N/A', SUBSTR(bur_cause_loss, 3,1))
	IFF(bur_cause_loss IS NULL OR IS_SPACES(bur_cause_loss), 'N/A', SUBSTR(bur_cause_loss, 3, 1)) AS reserve_category_out,
	insd_deduct,
	-- *INF*: IIF (ISNULL(insd_deduct), 0 , insd_deduct)
	IFF(insd_deduct IS NULL, 0, insd_deduct) AS insd_deduct_out,
	ref_subro_dt,
	-- *INF*: IIF(ISNULL(ref_subro_dt), to_date('01/01/1800', 'MM/DD/YYYY'), ref_subro_dt)
	IFF(ref_subro_dt IS NULL, to_date('01/01/1800', 'MM/DD/YYYY'), ref_subro_dt) AS ref_subro_dt_out,
	general_comment,
	install_reached,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(install_reached))) OR LENGTH(LTRIM(RTRIM(install_reached))) = 0, 'N/A', LTRIM(RTRIM(install_reached)))
	IFF(LTRIM(RTRIM(install_reached)) IS NULL OR LENGTH(LTRIM(RTRIM(install_reached))) = 0, 'N/A', LTRIM(RTRIM(install_reached))) AS install_reached_out,
	amt_of_agreement,
	-- *INF*: IIF(ISNULL(amt_of_agreement), 0, amt_of_agreement)
	IFF(amt_of_agreement IS NULL, 0, amt_of_agreement) AS amt_of_agreement_out,
	install_amt_month,
	-- *INF*: IIF(ISNULL(install_amt_month), 0,install_amt_month) 
	IFF(install_amt_month IS NULL, 0, install_amt_month) AS install_amt_month_out,
	start_pmt_dt,
	-- *INF*: IIF(ISNULL(start_pmt_dt), to_date('01/01/1800', 'MM/DD/YYYY'), start_pmt_dt)
	IFF(start_pmt_dt IS NULL, to_date('01/01/1800', 'MM/DD/YYYY'), start_pmt_dt) AS start_pmt_dt_out,
	update_ts,
	update_user_id,
	create_ts,
	create_user_id,
	closure_date,
	-- *INF*: IIF(ISNULL(closure_date), to_date('01/01/1800', 'MM/DD/YYYY'), closure_date)
	IFF(closure_date IS NULL, to_date('01/01/1800', 'MM/DD/YYYY'), closure_date) AS closure_date_out,
	subro_rep_clt_id,
	subro_mgr_clt_id,
	referring_adj_id,
	file_status,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(file_status))) OR LENGTH(LTRIM(RTRIM(file_status))) = 0, 'N/A', LTRIM(RTRIM(file_status)))
	IFF(LTRIM(RTRIM(file_status)) IS NULL OR LENGTH(LTRIM(RTRIM(file_status))) = 0, 'N/A', LTRIM(RTRIM(file_status))) AS file_status_out,
	deduct_has_been,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(deduct_has_been))) OR LENGTH(LTRIM(RTRIM(deduct_has_been))) = 0, 'N/A', LTRIM(RTRIM(deduct_has_been)))
	IFF(LTRIM(RTRIM(deduct_has_been)) IS NULL OR LENGTH(LTRIM(RTRIM(deduct_has_been))) = 0, 'N/A', LTRIM(RTRIM(deduct_has_been))) AS deduct_has_been_out,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
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
EXP_gather_values AS (
	SELECT
	LKP_claimant_coverage_detail.claimant_cov_det_ak_id,
	EXP_source_anchor.insd_deduct_out AS insd_deduct,
	EXP_source_anchor.ref_subro_dt_out AS ref_subro_dt,
	EXP_source_anchor.general_comment,
	EXP_source_anchor.install_reached_out,
	EXP_source_anchor.amt_of_agreement_out AS amt_of_agreement,
	EXP_source_anchor.install_amt_month_out AS install_amt_month,
	EXP_source_anchor.start_pmt_dt_out AS start_pmt_dt,
	EXP_source_anchor.file_status_out AS file_status,
	EXP_source_anchor.deduct_has_been_out AS deduct_has_been,
	EXP_source_anchor.closure_date_out AS closure_date,
	EXP_source_anchor.tch_claim_nbr_out
	FROM EXP_source_anchor
	LEFT JOIN LKP_claimant_coverage_detail
	ON LKP_claimant_coverage_detail.claim_party_occurrence_ak_id = LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id AND LKP_claimant_coverage_detail.s3p_object_type_code = EXP_source_anchor.object_type_cd_out AND LKP_claimant_coverage_detail.s3p_object_seq_num = EXP_source_anchor.object_seq_nbr AND LKP_claimant_coverage_detail.major_peril_code = EXP_source_anchor.cov_type_cd_out AND LKP_claimant_coverage_detail.s3p_pkg_seq_num = EXP_source_anchor.cov_seq_nbr AND LKP_claimant_coverage_detail.cause_of_loss = EXP_source_anchor.bur_cause_loss_out AND LKP_claimant_coverage_detail.reserve_ctgry = EXP_source_anchor.reserve_category_out
),
LKP_CLM_COMMENTS_STAGE AS (
	SELECT
	TCC_COMMENT_TXT,
	FOLDER_KEY,
	COMMENT_ITEM_NBR
	FROM (
		SELECT comment.TCC_COMMENT_TXT as TCC_COMMENT_TXT, 
		comment.FOLDER_KEY as FOLDER_KEY, 
		comment.COMMENT_ITEM_NBR as COMMENT_ITEM_NBR 
		FROM CLM_COMMENTS_STAGE comment
		inner join  clm_subrogation_stage subro
		on subro.tch_claim_nbr = comment.FOLDER_KEY
		and subro.general_comment = comment.COMMENT_ITEM_NBR
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FOLDER_KEY,COMMENT_ITEM_NBR ORDER BY TCC_COMMENT_TXT) = 1
),
LKP_claim_subrogation AS (
	SELECT
	claim_subrogation_id,
	claim_subrogation_ak_id,
	claimant_cov_det_ak_id,
	insd_ded_amt,
	referred_to_subrogation_date,
	subrogation_comment,
	installment_reached_ind,
	agreement_amt,
	monthly_installment_amt,
	pay_start_date,
	file_status_code,
	ded_status_code,
	closure_date
	FROM (
		SELECT A.claim_subrogation_id as claim_subrogation_id, 
		A.claim_subrogation_ak_id as claim_subrogation_ak_id, 
		A.insd_ded_amt as insd_ded_amt,
		A.referred_to_subrogation_date as referred_to_subrogation_date, 
		A.subrogation_comment as subrogation_comment, 
		A.installment_reached_ind as installment_reached_ind, 
		A.agreement_amt as agreement_amt, 
		A.monthly_installment_amt as monthly_installment_amt, 
		A.pay_start_date as pay_start_date, 
		A.file_status_code as file_status_code, 
		A.ded_status_code as ded_status_code, 
		A.closure_date as closure_date, 
		A.claimant_cov_det_ak_id as claimant_cov_det_ak_id 
		FROM claim_subrogation A
		WHERE A.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY claim_subrogation_id) = 1
),
EXP_gather_old_values AS (
	SELECT
	LKP_claim_subrogation.claim_subrogation_id,
	LKP_claim_subrogation.claim_subrogation_ak_id,
	LKP_claim_subrogation.claimant_cov_det_ak_id AS old_claimant_cov_det_ak_id,
	LKP_claim_subrogation.insd_ded_amt AS old_insd_ded_amt,
	LKP_claim_subrogation.referred_to_subrogation_date AS old_referred_to_subrogation_date,
	LKP_claim_subrogation.subrogation_comment AS old_subrogation_comment,
	LKP_claim_subrogation.installment_reached_ind AS old_installment_reached_ind,
	LKP_claim_subrogation.agreement_amt AS old_agreement_amt,
	LKP_claim_subrogation.monthly_installment_amt AS old_monthly_installment_amt,
	LKP_claim_subrogation.pay_start_date AS old_pay_start_date,
	LKP_claim_subrogation.file_status_code AS old_file_status_code,
	LKP_claim_subrogation.ded_status_code AS old_ded_status_code,
	LKP_claim_subrogation.closure_date AS old_closure_date,
	EXP_gather_values.insd_deduct,
	EXP_gather_values.ref_subro_dt,
	EXP_gather_values.general_comment,
	EXP_gather_values.install_reached_out,
	EXP_gather_values.amt_of_agreement,
	EXP_gather_values.install_amt_month,
	EXP_gather_values.start_pmt_dt,
	EXP_gather_values.file_status,
	EXP_gather_values.deduct_has_been,
	EXP_gather_values.closure_date,
	LKP_CLM_COMMENTS_STAGE.TCC_COMMENT_TXT,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(TCC_COMMENT_TXT))) OR LENGTH(LTRIM(RTRIM(TCC_COMMENT_TXT))) = 0, 'N/A', LTRIM(RTRIM(TCC_COMMENT_TXT)))
	IFF(LTRIM(RTRIM(TCC_COMMENT_TXT)) IS NULL OR LENGTH(LTRIM(RTRIM(TCC_COMMENT_TXT))) = 0, 'N/A', LTRIM(RTRIM(TCC_COMMENT_TXT))) AS v_TCC_COMMENT_TXT,
	v_TCC_COMMENT_TXT AS TCC_COMMENT_TXT_out,
	-- *INF*: iif(isnull(old_claimant_cov_det_ak_id)
	-- , 'NEW'
	-- , IIF(old_insd_ded_amt != insd_deduct OR
	-- old_referred_to_subrogation_date != ref_subro_dt OR
	-- old_subrogation_comment != v_TCC_COMMENT_TXT OR
	-- old_installment_reached_ind != install_reached_out OR
	-- old_agreement_amt != amt_of_agreement OR
	-- old_monthly_installment_amt != install_amt_month OR
	-- old_pay_start_date != start_pmt_dt OR
	-- old_file_status_code != file_status OR
	-- old_ded_status_code != deduct_has_been OR
	-- old_closure_date != closure_date
	-- ,'UPDATE'
	-- ,'NOCHANGE')
	-- )
	-- 
	IFF(old_claimant_cov_det_ak_id IS NULL, 'NEW', IFF(old_insd_ded_amt != insd_deduct OR old_referred_to_subrogation_date != ref_subro_dt OR old_subrogation_comment != v_TCC_COMMENT_TXT OR old_installment_reached_ind != install_reached_out OR old_agreement_amt != amt_of_agreement OR old_monthly_installment_amt != install_amt_month OR old_pay_start_date != start_pmt_dt OR old_file_status_code != file_status OR old_ded_status_code != deduct_has_been OR old_closure_date != closure_date, 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	EXP_gather_values.claimant_cov_det_ak_id
	FROM EXP_gather_values
	LEFT JOIN LKP_CLM_COMMENTS_STAGE
	ON LKP_CLM_COMMENTS_STAGE.FOLDER_KEY = EXP_gather_values.tch_claim_nbr_out AND LKP_CLM_COMMENTS_STAGE.COMMENT_ITEM_NBR = EXP_gather_values.general_comment
	LEFT JOIN LKP_claim_subrogation
	ON LKP_claim_subrogation.claimant_cov_det_ak_id = EXP_gather_values.claimant_cov_det_ak_id
),
FIL_insert_rows AS (
	SELECT
	claim_subrogation_id, 
	claim_subrogation_ak_id, 
	claimant_cov_det_ak_id, 
	insd_deduct, 
	ref_subro_dt, 
	install_reached_out, 
	amt_of_agreement, 
	install_amt_month, 
	start_pmt_dt, 
	file_status, 
	deduct_has_been, 
	closure_date, 
	TCC_COMMENT_TXT_out AS TCC_COMMENT_TXT, 
	changed_flag, 
	old_claimant_cov_det_ak_id
	FROM EXP_gather_old_values
	WHERE changed_flag = 'NEW' OR changed_flag = 'UPDATE'
),
SEQ_claim_subrogation_ak_id AS (
	CREATE SEQUENCE SEQ_claim_subrogation_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_determine_AK AS (
	SELECT
	claim_subrogation_id,
	claim_subrogation_ak_id,
	claimant_cov_det_ak_id,
	insd_deduct,
	ref_subro_dt,
	install_reached_out,
	amt_of_agreement,
	install_amt_month,
	start_pmt_dt,
	file_status,
	deduct_has_been,
	closure_date,
	TCC_COMMENT_TXT,
	changed_flag,
	SEQ_claim_subrogation_ak_id.NEXTVAL,
	-- *INF*: IIF(ISNULL(claim_subrogation_ak_id), NEXTVAL, claim_subrogation_ak_id)
	IFF(claim_subrogation_ak_id IS NULL, NEXTVAL, claim_subrogation_ak_id) AS claim_subrogation_ak_id_out,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	SYSDATE AS created_modified_date
	FROM FIL_insert_rows
),
claim_subrogation_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_subrogation
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_subrogation_ak_id, claimant_cov_det_ak_id, insd_ded_amt, referred_to_subrogation_date, subrogation_comment, installment_reached_ind, agreement_amt, monthly_installment_amt, pay_start_date, file_status_code, ded_status_code, closure_date)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	source_system_id AS SOURCE_SYS_ID, 
	created_modified_date AS CREATED_DATE, 
	created_modified_date AS MODIFIED_DATE, 
	claim_subrogation_ak_id_out AS CLAIM_SUBROGATION_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	insd_deduct AS INSD_DED_AMT, 
	ref_subro_dt AS REFERRED_TO_SUBROGATION_DATE, 
	TCC_COMMENT_TXT AS SUBROGATION_COMMENT, 
	install_reached_out AS INSTALLMENT_REACHED_IND, 
	amt_of_agreement AS AGREEMENT_AMT, 
	install_amt_month AS MONTHLY_INSTALLMENT_AMT, 
	start_pmt_dt AS PAY_START_DATE, 
	file_status AS FILE_STATUS_CODE, 
	deduct_has_been AS DED_STATUS_CODE, 
	CLOSURE_DATE
	FROM EXP_determine_AK
),
SQ_claim_subrogation AS (
	SELECT a.claim_subrogation_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.source_sys_id, 
	a.claim_subrogation_ak_id, 
	a.claimant_cov_det_ak_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_subrogation a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND
	EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_subrogation b
			WHERE b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND 
	b.crrnt_snpsht_flag = '1'
			AND a.claim_subrogation_ak_id =  b.claim_subrogation_ak_id
			AND a.claimant_cov_det_ak_id = b.claimant_cov_det_ak_id
			GROUP BY claim_subrogation_ak_id, claimant_cov_det_ak_id 
			HAVING COUNT(*) > 1) 
	ORDER BY claim_subrogation_ak_id, claimant_cov_det_ak_id , eff_from_date  DESC
),
EXP_expire_rows AS (
	SELECT
	claim_subrogation_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	claim_subrogation_ak_id,
	claimant_cov_det_ak_id,
	-- *INF*: DECODE(TRUE,
	-- 	claim_subrogation_ak_id = v_prev_row_claim_subrogation_ak_id AND claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id, ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),
	-- 	eff_to_date)
	DECODE(TRUE,
	claim_subrogation_ak_id = v_prev_row_claim_subrogation_ak_id AND claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id, ADD_TO_DATE(v_prev_row_eff_from_date, 'SS', - 1),
	eff_to_date) AS v_new_eff_to_date,
	v_new_eff_to_date AS new_eff_to_date,
	0 AS new_crrnt_snpsht_flag,
	claim_subrogation_ak_id AS v_prev_row_claim_subrogation_ak_id,
	claimant_cov_det_ak_id AS v_prev_row_claimant_cov_det_ak_id,
	eff_from_date AS v_prev_row_eff_from_date,
	sysdate AS new_modified_Date
	FROM SQ_claim_subrogation
),
FIL_first_row_in_ak_group AS (
	SELECT
	claim_subrogation_id, 
	eff_to_date, 
	claim_subrogation_ak_id, 
	claimant_cov_det_ak_id, 
	new_eff_to_date, 
	new_crrnt_snpsht_flag, 
	new_modified_Date
	FROM EXP_expire_rows
	WHERE eff_to_date <> new_eff_to_date
),
UPD_claim_subrogation AS (
	SELECT
	claim_subrogation_id, 
	new_eff_to_date, 
	new_crrnt_snpsht_flag, 
	new_modified_Date
	FROM FIL_first_row_in_ak_group
),
claim_subrogation_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_subrogation AS T
	USING UPD_claim_subrogation AS S
	ON T.claim_subrogation_id = S.claim_subrogation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.new_crrnt_snpsht_flag, T.eff_to_date = S.new_eff_to_date, T.modified_date = S.new_modified_Date
),