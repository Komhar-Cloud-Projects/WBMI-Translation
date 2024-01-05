WITH
SQ_clm_total_loss_stage AS (
	SELECT
		clm_total_loss_stage_id,
		tch_claim_nbr,
		tch_client_id,
		object_type_cd,
		object_seq_nbr,
		cov_type_cd,
		cov_seq_nbr,
		bur_cause_loss,
		seq_nbr,
		add_status,
		add_status_ts,
		add_upload_ts,
		add_uuid,
		vehicle_vin,
		vehicle_year,
		vehicle_make,
		vehicle_model,
		loss_date,
		loss_owner,
		new_owner,
		owner_retained,
		payment_retained,
		loss_acv,
		sales_tax,
		title_fees,
		registration,
		deductible,
		salvage_amount,
		create_ts,
		create_user_id,
		update_ts,
		update_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM clm_total_loss_stage
),
EXP_source_anchor AS (
	SELECT
	clm_total_loss_stage_id,
	tch_claim_nbr,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_claim_nbr))) OR LENGTH(LTRIM(RTRIM(tch_claim_nbr))) = 0, 'N/A', LTRIM(RTRIM(tch_claim_nbr)))
	IFF(LTRIM(RTRIM(tch_claim_nbr)) IS NULL OR LENGTH(LTRIM(RTRIM(tch_claim_nbr))) = 0, 'N/A', LTRIM(RTRIM(tch_claim_nbr))) AS tch_claim_nbr_out,
	tch_client_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_client_id))) OR LENGTH(LTRIM(RTRIM(tch_client_id))) = 0, 'N/A', LTRIM(RTRIM(tch_client_id)))
	IFF(LTRIM(RTRIM(tch_client_id)) IS NULL OR LENGTH(LTRIM(RTRIM(tch_client_id))) = 0, 'N/A', LTRIM(RTRIM(tch_client_id))) AS tch_client_id_out,
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
	seq_nbr,
	vehicle_vin,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(vehicle_vin))) OR LENGTH(LTRIM(RTRIM(vehicle_vin))) = 0, 'N/A', LTRIM(RTRIM(vehicle_vin)))
	IFF(LTRIM(RTRIM(vehicle_vin)) IS NULL OR LENGTH(LTRIM(RTRIM(vehicle_vin))) = 0, 'N/A', LTRIM(RTRIM(vehicle_vin))) AS vehicle_vin_out,
	vehicle_year,
	vehicle_year AS vehicle_year_out,
	vehicle_make,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(vehicle_make))) OR LENGTH(LTRIM(RTRIM(vehicle_make))) = 0, 'N/A', LTRIM(RTRIM(vehicle_make)))
	IFF(LTRIM(RTRIM(vehicle_make)) IS NULL OR LENGTH(LTRIM(RTRIM(vehicle_make))) = 0, 'N/A', LTRIM(RTRIM(vehicle_make))) AS vehicle_make_out,
	vehicle_model,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(vehicle_model))) OR LENGTH(LTRIM(RTRIM(vehicle_model))) = 0, 'N/A', LTRIM(RTRIM(vehicle_model)))
	IFF(LTRIM(RTRIM(vehicle_model)) IS NULL OR LENGTH(LTRIM(RTRIM(vehicle_model))) = 0, 'N/A', LTRIM(RTRIM(vehicle_model))) AS vehicle_model_out,
	loss_date,
	-- *INF*: IIF(ISNULL(loss_date), to_date('01/01/1800', 'MM/DD/YYYY'), loss_date)
	IFF(loss_date IS NULL, to_date('01/01/1800', 'MM/DD/YYYY'), loss_date) AS loss_date_out,
	loss_owner,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(loss_owner))) OR LENGTH(LTRIM(RTRIM(loss_owner))) = 0, 'N/A', LTRIM(RTRIM(loss_owner)))
	IFF(LTRIM(RTRIM(loss_owner)) IS NULL OR LENGTH(LTRIM(RTRIM(loss_owner))) = 0, 'N/A', LTRIM(RTRIM(loss_owner))) AS loss_owner_out,
	new_owner,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(new_owner))) OR LENGTH(LTRIM(RTRIM(new_owner))) = 0, 'N/A', LTRIM(RTRIM(new_owner)))
	IFF(LTRIM(RTRIM(new_owner)) IS NULL OR LENGTH(LTRIM(RTRIM(new_owner))) = 0, 'N/A', LTRIM(RTRIM(new_owner))) AS new_owner_out,
	owner_retained,
	-- *INF*: IIF(ISNULL(owner_retained), 'N/A', to_char(owner_retained))
	IFF(owner_retained IS NULL, 'N/A', to_char(owner_retained)) AS owner_retained_out,
	payment_retained,
	-- *INF*: IIF(ISNULL(payment_retained), 0, payment_retained)
	IFF(payment_retained IS NULL, 0, payment_retained) AS payment_retained_out,
	loss_acv,
	-- *INF*: IIF(ISNULL(loss_acv), 0, loss_acv)
	IFF(loss_acv IS NULL, 0, loss_acv) AS loss_acv_out,
	sales_tax,
	-- *INF*: IIF(ISNULL(sales_tax), 0, sales_tax)
	IFF(sales_tax IS NULL, 0, sales_tax) AS sales_tax_out,
	title_fees,
	-- *INF*: IIF(ISNULL(title_fees), 0, title_fees)
	IFF(title_fees IS NULL, 0, title_fees) AS title_fees_out,
	registration,
	-- *INF*: IIF(ISNULL(registration), 0, registration)
	IFF(registration IS NULL, 0, registration) AS registration_out,
	deductible,
	-- *INF*: IIF(ISNULL(deductible), 0, deductible)
	IFF(deductible IS NULL, 0, deductible) AS deductible_out,
	salvage_amount,
	-- *INF*: IIF(ISNULL(salvage_amount), 0, salvage_amount)
	IFF(salvage_amount IS NULL, 0, salvage_amount) AS salvage_amount_out,
	create_ts,
	create_user_id,
	update_ts,
	update_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_clm_total_loss_stage
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
	EXP_source_anchor.tch_claim_nbr_out,
	EXP_source_anchor.seq_nbr,
	EXP_source_anchor.vehicle_vin_out,
	EXP_source_anchor.vehicle_year_out,
	EXP_source_anchor.vehicle_make_out,
	EXP_source_anchor.vehicle_model_out,
	EXP_source_anchor.loss_date_out,
	EXP_source_anchor.loss_owner_out,
	EXP_source_anchor.new_owner_out,
	EXP_source_anchor.owner_retained_out,
	EXP_source_anchor.payment_retained_out,
	EXP_source_anchor.loss_acv_out,
	EXP_source_anchor.sales_tax_out,
	EXP_source_anchor.title_fees_out,
	EXP_source_anchor.registration_out,
	EXP_source_anchor.deductible_out,
	EXP_source_anchor.salvage_amount_out
	FROM EXP_source_anchor
	LEFT JOIN LKP_claimant_coverage_detail
	ON LKP_claimant_coverage_detail.claim_party_occurrence_ak_id = LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id AND LKP_claimant_coverage_detail.s3p_object_type_code = EXP_source_anchor.object_type_cd_out AND LKP_claimant_coverage_detail.s3p_object_seq_num = EXP_source_anchor.object_seq_nbr AND LKP_claimant_coverage_detail.major_peril_code = EXP_source_anchor.cov_type_cd_out AND LKP_claimant_coverage_detail.s3p_pkg_seq_num = EXP_source_anchor.cov_seq_nbr AND LKP_claimant_coverage_detail.cause_of_loss = EXP_source_anchor.bur_cause_loss_out AND LKP_claimant_coverage_detail.reserve_ctgry = EXP_source_anchor.reserve_category_out
),
LKP_claim_total_loss AS (
	SELECT
	claim_total_loss_ak_id,
	claimant_cov_det_ak_id,
	claim_total_loss_seq_num,
	vin_num,
	veh_yr,
	veh_make,
	veh_model,
	total_loss_date,
	loss_owner,
	new_owner,
	owner_retained_ind,
	pay_retained_amt,
	loss_acv,
	sale_tax,
	title_fee,
	registration_fee,
	salvage_ded,
	salvage_amt
	FROM (
		SELECT a.claim_total_loss_ak_id as claim_total_loss_ak_id, 
		a.vin_num as vin_num, 
		a.veh_yr as veh_yr, 
		a.veh_make as veh_make, 
		a.veh_model as veh_model, 
		a.total_loss_date as total_loss_date, 
		a.loss_owner as loss_owner, 
		a.new_owner as new_owner, 
		a.owner_retained_ind as owner_retained_ind, 
		a.pay_retained_amt as pay_retained_amt, 
		a.loss_acv as loss_acv, 
		a.sale_tax as sale_tax, 
		a.title_fee as title_fee, 
		a.registration_fee as registration_fee, 
		a.salvage_ded as salvage_ded, 
		a.salvage_amt as salvage_amt, 
		a.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
		a.claim_total_loss_seq_num as claim_total_loss_seq_num 
		FROM claim_total_loss a
		WHERE a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,claim_total_loss_seq_num ORDER BY claim_total_loss_ak_id) = 1
),
EXP_gather_old_values AS (
	SELECT
	LKP_claim_total_loss.claim_total_loss_ak_id AS old_claim_total_loss_ak_id,
	LKP_claim_total_loss.claimant_cov_det_ak_id AS old_claimant_cov_det_ak_id,
	LKP_claim_total_loss.claim_total_loss_seq_num AS old_claim_total_loss_seq_num,
	LKP_claim_total_loss.vin_num AS old_vin_num,
	LKP_claim_total_loss.veh_yr AS old_veh_yr,
	LKP_claim_total_loss.veh_make AS old_veh_make,
	LKP_claim_total_loss.veh_model AS old_veh_model,
	LKP_claim_total_loss.total_loss_date AS old_total_loss_date,
	LKP_claim_total_loss.loss_owner AS old_loss_owner,
	LKP_claim_total_loss.new_owner AS old_new_owner,
	LKP_claim_total_loss.owner_retained_ind AS old_owner_retained_ind,
	LKP_claim_total_loss.pay_retained_amt AS old_pay_retained_amt,
	LKP_claim_total_loss.loss_acv AS old_loss_acv,
	LKP_claim_total_loss.sale_tax AS old_sale_tax,
	LKP_claim_total_loss.title_fee AS old_title_fee,
	LKP_claim_total_loss.registration_fee AS old_registration_fee,
	LKP_claim_total_loss.salvage_ded AS old_salvage_ded,
	LKP_claim_total_loss.salvage_amt AS old_salvage_amt,
	EXP_gather_values.vehicle_vin_out,
	EXP_gather_values.vehicle_year_out,
	EXP_gather_values.vehicle_make_out,
	EXP_gather_values.vehicle_model_out,
	EXP_gather_values.loss_date_out,
	EXP_gather_values.loss_owner_out,
	EXP_gather_values.new_owner_out,
	EXP_gather_values.owner_retained_out,
	EXP_gather_values.payment_retained_out,
	EXP_gather_values.loss_acv_out,
	EXP_gather_values.sales_tax_out,
	EXP_gather_values.title_fees_out,
	EXP_gather_values.registration_out,
	EXP_gather_values.deductible_out,
	EXP_gather_values.salvage_amount_out,
	-- *INF*: iif(isnull(old_claimant_cov_det_ak_id)
	-- , 'NEW'
	-- , IIF(old_vin_num != vehicle_vin_out
	-- OR old_veh_yr != vehicle_year_out
	-- OR old_veh_make != vehicle_make_out
	-- OR old_veh_model != vehicle_model_out
	-- OR old_total_loss_date != loss_date_out
	-- OR old_loss_owner != loss_owner_out
	-- OR old_new_owner != new_owner_out
	-- OR old_owner_retained_ind != owner_retained_out
	-- OR old_pay_retained_amt != payment_retained_out
	-- OR old_loss_acv != loss_acv_out
	-- OR old_sale_tax != sales_tax_out
	-- OR old_title_fee != title_fees_out
	-- OR old_registration_fee != registration_out
	-- OR old_salvage_ded != deductible_out
	-- OR old_salvage_amt != salvage_amount_out
	-- ,'UPDATE'
	-- ,'NOCHANGE')
	-- )
	-- 
	IFF(old_claimant_cov_det_ak_id IS NULL, 'NEW', IFF(old_vin_num != vehicle_vin_out OR old_veh_yr != vehicle_year_out OR old_veh_make != vehicle_make_out OR old_veh_model != vehicle_model_out OR old_total_loss_date != loss_date_out OR old_loss_owner != loss_owner_out OR old_new_owner != new_owner_out OR old_owner_retained_ind != owner_retained_out OR old_pay_retained_amt != payment_retained_out OR old_loss_acv != loss_acv_out OR old_sale_tax != sales_tax_out OR old_title_fee != title_fees_out OR old_registration_fee != registration_out OR old_salvage_ded != deductible_out OR old_salvage_amt != salvage_amount_out, 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	EXP_gather_values.claimant_cov_det_ak_id,
	EXP_gather_values.seq_nbr
	FROM EXP_gather_values
	LEFT JOIN LKP_claim_total_loss
	ON LKP_claim_total_loss.claimant_cov_det_ak_id = EXP_gather_values.claimant_cov_det_ak_id AND LKP_claim_total_loss.claim_total_loss_seq_num = EXP_gather_values.seq_nbr
),
FIL_insert_rows AS (
	SELECT
	old_claim_total_loss_ak_id, 
	claimant_cov_det_ak_id, 
	seq_nbr, 
	changed_flag, 
	vehicle_vin_out, 
	vehicle_year_out, 
	vehicle_make_out, 
	vehicle_model_out, 
	loss_date_out, 
	loss_owner_out, 
	new_owner_out, 
	owner_retained_out, 
	payment_retained_out, 
	loss_acv_out, 
	sales_tax_out, 
	title_fees_out, 
	registration_out, 
	deductible_out, 
	salvage_amount_out
	FROM EXP_gather_old_values
	WHERE changed_flag = 'NEW' OR changed_flag = 'UPDATE'
),
SEQ_claim_total_loss_ak_id AS (
	CREATE SEQUENCE SEQ_claim_total_loss_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_determine_AK AS (
	SELECT
	old_claim_total_loss_ak_id,
	claimant_cov_det_ak_id,
	seq_nbr,
	changed_flag,
	SEQ_claim_total_loss_ak_id.NEXTVAL,
	-- *INF*: IIF(ISNULL(old_claim_total_loss_ak_id), NEXTVAL, old_claim_total_loss_ak_id)
	IFF(old_claim_total_loss_ak_id IS NULL, NEXTVAL, old_claim_total_loss_ak_id) AS claim_total_loss_ak_id_out,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	SYSDATE AS created_modified_date,
	vehicle_vin_out,
	vehicle_year_out,
	vehicle_make_out,
	vehicle_model_out,
	loss_date_out,
	loss_owner_out,
	new_owner_out,
	owner_retained_out,
	payment_retained_out,
	loss_acv_out,
	sales_tax_out,
	title_fees_out,
	registration_out,
	deductible_out,
	salvage_amount_out
	FROM FIL_insert_rows
),
claim_total_loss AS (
	INSERT INTO claim_total_loss
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_total_loss_ak_id, claimant_cov_det_ak_id, claim_total_loss_seq_num, vin_num, veh_yr, veh_make, veh_model, total_loss_date, loss_owner, new_owner, owner_retained_ind, pay_retained_amt, loss_acv, sale_tax, title_fee, registration_fee, salvage_ded, salvage_amt)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	source_system_id AS SOURCE_SYS_ID, 
	created_modified_date AS CREATED_DATE, 
	created_modified_date AS MODIFIED_DATE, 
	claim_total_loss_ak_id_out AS CLAIM_TOTAL_LOSS_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	seq_nbr AS CLAIM_TOTAL_LOSS_SEQ_NUM, 
	vehicle_vin_out AS VIN_NUM, 
	vehicle_year_out AS VEH_YR, 
	vehicle_make_out AS VEH_MAKE, 
	vehicle_model_out AS VEH_MODEL, 
	loss_date_out AS TOTAL_LOSS_DATE, 
	loss_owner_out AS LOSS_OWNER, 
	new_owner_out AS NEW_OWNER, 
	owner_retained_out AS OWNER_RETAINED_IND, 
	payment_retained_out AS PAY_RETAINED_AMT, 
	loss_acv_out AS LOSS_ACV, 
	sales_tax_out AS SALE_TAX, 
	title_fees_out AS TITLE_FEE, 
	registration_out AS REGISTRATION_FEE, 
	deductible_out AS SALVAGE_DED, 
	salvage_amount_out AS SALVAGE_AMT
	FROM EXP_determine_AK
),
SQ_claim_total_loss AS (
	SELECT a.claim_total_loss_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.source_sys_id, 
	a.claim_total_loss_ak_id, 
	a.claimant_cov_det_ak_id, 
	a.claim_total_loss_seq_num 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_total_loss a
	WHERE
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND
	EXISTS(SELECT 1	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_total_loss b
		WHERE b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND 
		b.crrnt_snpsht_flag = '1'
		AND a.claim_total_loss_ak_id = b.claim_total_loss_ak_id
		AND a.claimant_cov_det_ak_id = b.claimant_cov_det_ak_id
		AND a.claim_total_loss_seq_num = b.claim_total_loss_seq_num 
		GROUP BY claim_total_loss_ak_id, claimant_cov_det_ak_id, claim_total_loss_seq_num
		HAVING COUNT(*) > 1)
	ORDER BY claim_total_loss_ak_id, claimant_cov_det_ak_id, claim_total_loss_seq_num, eff_from_date  DESC
),
EXP_expire_rows AS (
	SELECT
	claim_total_loss_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	claim_total_loss_ak_id,
	claimant_cov_det_ak_id,
	claim_total_loss_seq_num,
	-- *INF*: DECODE(TRUE,
	-- 	claim_total_loss_ak_id = v_prev_row_claim_total_loss_ak_id AND claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id, ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),
	-- 	eff_to_date)
	DECODE(TRUE,
	claim_total_loss_ak_id = v_prev_row_claim_total_loss_ak_id AND claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id, ADD_TO_DATE(v_prev_row_eff_from_date, 'SS', - 1),
	eff_to_date) AS v_new_eff_to_date,
	v_new_eff_to_date AS new_eff_to_date,
	0 AS new_crrnt_snpsht_flag,
	claim_total_loss_ak_id AS v_prev_row_claim_total_loss_ak_id,
	claimant_cov_det_ak_id AS v_prev_row_claimant_cov_det_ak_id,
	eff_from_date AS v_prev_row_eff_from_date,
	sysdate AS new_modified_Date
	FROM SQ_claim_total_loss
),
FIL_first_row_in_ak_group AS (
	SELECT
	claim_total_loss_id, 
	eff_to_date, 
	claim_total_loss_ak_id, 
	claimant_cov_det_ak_id, 
	new_eff_to_date, 
	new_crrnt_snpsht_flag, 
	new_modified_Date
	FROM EXP_expire_rows
	WHERE eff_to_date <> new_eff_to_date
),
UPD_claim_total_loss AS (
	SELECT
	claim_total_loss_id, 
	new_eff_to_date, 
	new_crrnt_snpsht_flag, 
	new_modified_Date
	FROM FIL_first_row_in_ak_group
),
claim_total_loss_update AS (
	MERGE INTO claim_total_loss AS T
	USING UPD_claim_total_loss AS S
	ON T.claim_total_loss_id = S.claim_total_loss_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.new_crrnt_snpsht_flag, T.eff_to_date = S.new_eff_to_date, T.modified_date = S.new_modified_Date
),