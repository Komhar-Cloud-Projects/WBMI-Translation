WITH
SQ_claimant_coverage_detail AS (
	SELECT CCD.claimant_cov_det_id,
	       CCD.claimant_cov_det_ak_id,
	       CCD.s3p_unit_type_code,
	       CCD.loc_unit_num,
	       CCD.sub_loc_unit_num,
	       CCD.ins_line,
	       CCD.risk_unit_grp,
	       CCD.risk_unit_grp_seq_num,
	       CCD.risk_unit,
	       CCD.risk_unit_seq_num,
	       CCD.major_peril_code,
	       CCD.major_peril_seq,
	       CCD.reserve_ctgry,
	       CCD.cause_of_loss,
	       CCD.pms_type_bureau_code,
	       CCD.claimant_cov_eff_date,
	       CCD.risk_type_ind,
	       CCD.spec_pers_prop_use_code,
	       CCD.pkg_ded_amt,
	       CCD.pkg_lmt_amt,
	       CCD.unit_veh_registration_state_code,
	       CCD.unit_veh_stated_amt,
	       CCD.unit_veh_yr,
	       CCD.unit_veh_make,
	       CCD.unit_vin_num,
	       CCD.audit_id,
	       CCD.eff_from_date
	FROM   claimant_coverage_detail CCD
	WHERE  CCD.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_get_values AS (
	SELECT
	claimant_cov_det_id,
	ins_line,
	-- *INF*: LTRIM(RTRIM(ins_line))
	LTRIM(RTRIM(ins_line
		)
	) AS ins_line_OUT,
	major_peril_code,
	-- *INF*: LTRIM(RTRIM(major_peril_code))
	LTRIM(RTRIM(major_peril_code
		)
	) AS major_peril_code_OUT,
	reserve_ctgry,
	-- *INF*: LTRIM(RTRIM(reserve_ctgry))
	LTRIM(RTRIM(reserve_ctgry
		)
	) AS reserve_ctgry_OUT,
	cause_of_loss,
	-- *INF*: LTRIM(RTRIM(cause_of_loss))
	LTRIM(RTRIM(cause_of_loss
		)
	) AS cause_of_loss_OUT,
	major_peril_seq,
	risk_unit_grp,
	-- *INF*: LTRIM(RTRIM(risk_unit_grp))
	LTRIM(RTRIM(risk_unit_grp
		)
	) AS risk_unit_grp_OUT,
	risk_unit_grp_seq_num,
	risk_unit,
	-- *INF*: LTRIM(RTRIM(risk_unit))
	LTRIM(RTRIM(risk_unit
		)
	) AS risk_unit_OUT,
	risk_unit_seq_num,
	unit_veh_registration_state_code,
	unit_vin_num,
	unit_veh_stated_amt,
	unit_veh_yr,
	unit_veh_make,
	s3p_unit_type_code,
	loc_unit_num,
	sub_loc_unit_num,
	risk_type_ind,
	-- *INF*: LTRIM(RTRIM(risk_type_ind))
	LTRIM(RTRIM(risk_type_ind
		)
	) AS risk_type_ind_OUT,
	spec_pers_prop_use_code,
	pkg_ded_amt,
	pkg_lmt_amt,
	pms_type_bureau_code,
	claimant_cov_det_ak_id,
	eff_from_date,
	claimant_cov_eff_date,
	audit_id
	FROM SQ_claimant_coverage_detail
),
LKP_sup_claim_reserve_category AS (
	SELECT
	IN_reserve_ctgry,
	sup_claim_reserve_ctgry_id,
	reserve_ctgry_code,
	reserve_ctgry_descript,
	crrnt_snpsht_flag
	FROM (
		SELECT sup_claim_reserve_category.sup_claim_reserve_ctgry_id as sup_claim_reserve_ctgry_id,
		 LTRIM(RTRIM(sup_claim_reserve_category.reserve_ctgry_descript)) as reserve_ctgry_descript,
		sup_claim_reserve_category.crrnt_snpsht_flag as crrnt_snpsht_flag, 
		 LTRIM(RTRIM(sup_claim_reserve_category.reserve_ctgry_code)) as reserve_ctgry_code
		 FROM sup_claim_reserve_category where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY reserve_ctgry_code ORDER BY IN_reserve_ctgry) = 1
),
LKP_sup_major_peril AS (
	SELECT
	IN_major_peril_code,
	major_peril_code,
	major_peril_descript,
	crrnt_snpsht_flag
	FROM (
		SELECT sup_major_peril.major_peril_descript as major_peril_descript, sup_major_peril.crrnt_snpsht_flag as crrnt_snpsht_flag, sup_major_peril.major_peril_code as major_peril_code FROM sup_major_peril WHERE sup_major_peril.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril_code ORDER BY IN_major_peril_code) = 1
),
LKP_sup_risk_unit AS (
	SELECT
	IN_risk_unit,
	IN_ins_line,
	risk_unit_code,
	risk_unit_descript,
	ins_line
	FROM (
		SELECT sup_risk_unit.risk_unit_descript as risk_unit_descript,
		LTRIM(RTRIM( sup_risk_unit.risk_unit_code)) as risk_unit_code,
		 LTRIM(RTRIM( sup_risk_unit.ins_line)) as ins_line 
		FROM sup_risk_unit
		where sup_risk_unit.crrnt_snpsht_flag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_code,ins_line ORDER BY IN_risk_unit) = 1
),
LKP_sup_risk_unit_group AS (
	SELECT
	IN_risk_unit_grp,
	IN_ins_line,
	IN_risk_type_ind,
	risk_unit_grp_code,
	risk_unit_grp_descript,
	prdct_type_code,
	lob,
	ins_line,
	crrnt_snpsht_flag
	FROM (
		SELECT  LTRIM(RTRIM(sup_risk_unit_group.risk_unit_grp_descript)) as risk_unit_grp_descript, 
		sup_risk_unit_group.lob as lob, 
		sup_risk_unit_group.crrnt_snpsht_flag as crrnt_snpsht_flag, 
		LTRIM(RTRIM(sup_risk_unit_group.risk_unit_grp_code)) as risk_unit_grp_code, 
		LTRIM(RTRIM(sup_risk_unit_group.ins_line)) as ins_line,
		 LTRIM(RTRIM(sup_risk_unit_group.prdct_type_code)) as prdct_type_code
		 FROM sup_risk_unit_group 
		where sup_risk_unit_group.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_grp_code,ins_line,prdct_type_code ORDER BY IN_risk_unit_grp) = 1
),
lkp_sup_insurance_line AS (
	SELECT
	sup_ins_line_id,
	ins_line_code,
	ins_line_descript,
	IN_ins_line_code
	FROM (
		SELECT sup_insurance_line.sup_ins_line_id as sup_ins_line_id, sup_insurance_line.ins_line_descript as ins_line_descript, sup_insurance_line.ins_line_code as ins_line_code FROM sup_insurance_line where crrnt_snpsht_flag = '1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_code ORDER BY sup_ins_line_id) = 1
),
EXP_DEFAULT_VALUES AS (
	SELECT
	EXP_get_values.claimant_cov_det_id,
	EXP_get_values.ins_line_OUT AS ins_line,
	lkp_sup_insurance_line.ins_line_descript,
	-- *INF*: IIF(ISNULL(ins_line_descript), 'N/A', ins_line_descript)
	IFF(ins_line_descript IS NULL,
		'N/A',
		ins_line_descript
	) AS ins_line_descript_out,
	EXP_get_values.major_peril_code_OUT AS major_peril_code,
	LKP_sup_major_peril.major_peril_descript,
	-- *INF*: IIF(ISNULL(major_peril_descript), 'N/A', major_peril_descript )
	IFF(major_peril_descript IS NULL,
		'N/A',
		major_peril_descript
	) AS major_peril_descript_out,
	EXP_get_values.major_peril_seq,
	EXP_get_values.cause_of_loss_OUT AS cause_of_loss,
	-- *INF*: iif(isnull(cause_of_loss_short_descript),'N/A',cause_of_loss_short_descript)
	IFF(cause_of_loss_short_descript IS NULL,
		'N/A',
		cause_of_loss_short_descript
	) AS cause_of_loss_short_descript_out,
	-- *INF*: iif(isnull(cause_of_loss_long_descript),'N/A',cause_of_loss_long_descript)
	IFF(cause_of_loss_long_descript IS NULL,
		'N/A',
		cause_of_loss_long_descript
	) AS cause_of_loss_long_descript_out,
	EXP_get_values.reserve_ctgry_OUT AS reserve_ctgry,
	LKP_sup_claim_reserve_category.reserve_ctgry_descript,
	-- *INF*: iif(isnull(reserve_ctgry_descript),'N/A',reserve_ctgry_descript)
	IFF(reserve_ctgry_descript IS NULL,
		'N/A',
		reserve_ctgry_descript
	) AS reserve_ctgry_descript_out,
	EXP_get_values.risk_unit_grp_OUT AS risk_unit_grp,
	LKP_sup_risk_unit_group.risk_unit_grp_descript,
	-- *INF*: IIF(ISNULL(risk_unit_grp_descript), 'N/A', risk_unit_grp_descript)
	IFF(risk_unit_grp_descript IS NULL,
		'N/A',
		risk_unit_grp_descript
	) AS risk_unit_grp_descript_out,
	EXP_get_values.risk_unit_grp_seq_num,
	EXP_get_values.risk_unit_OUT AS risk_unit,
	LKP_sup_risk_unit.risk_unit_descript,
	-- *INF*: IIF(ISNULL(risk_unit_descript), 'N/A', risk_unit_descript)
	IFF(risk_unit_descript IS NULL,
		'N/A',
		risk_unit_descript
	) AS risk_unit_descript_out,
	EXP_get_values.risk_unit_seq_num,
	EXP_get_values.unit_veh_registration_state_code,
	EXP_get_values.unit_vin_num,
	EXP_get_values.unit_veh_stated_amt,
	EXP_get_values.unit_veh_yr,
	EXP_get_values.unit_veh_make,
	EXP_get_values.s3p_unit_type_code,
	EXP_get_values.pms_type_bureau_code,
	EXP_get_values.loc_unit_num,
	EXP_get_values.spec_pers_prop_use_code,
	EXP_get_values.pkg_ded_amt,
	EXP_get_values.pkg_lmt_amt,
	1 AS crrnt_snpsht_flag,
	-- *INF*: IIF(source_audit_id < 0 , source_audit_id , @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID})
	-- 
	-- --- We want to identify the loss master data inserted into coverage_dim based on the audit_id. In EDW we insert data into claimant_coverage_detail for loss_master_historical_data.
	IFF(source_audit_id < 0,
		source_audit_id,
		@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	) AS audit_id,
	EXP_get_values.eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	EXP_get_values.claimant_cov_det_ak_id,
	EXP_get_values.sub_loc_unit_num,
	EXP_get_values.claimant_cov_eff_date,
	EXP_get_values.audit_id AS source_audit_id
	FROM EXP_get_values
	LEFT JOIN LKP_sup_claim_reserve_category
	ON LKP_sup_claim_reserve_category.reserve_ctgry_code = EXP_get_values.reserve_ctgry_OUT
	LEFT JOIN LKP_sup_major_peril
	ON LKP_sup_major_peril.major_peril_code = EXP_get_values.major_peril_code_OUT
	LEFT JOIN LKP_sup_risk_unit
	ON LKP_sup_risk_unit.risk_unit_code = EXP_get_values.risk_unit_OUT AND LKP_sup_risk_unit.ins_line = EXP_get_values.ins_line_OUT
	LEFT JOIN LKP_sup_risk_unit_group
	ON LKP_sup_risk_unit_group.risk_unit_grp_code = EXP_get_values.risk_unit_grp_OUT AND LKP_sup_risk_unit_group.ins_line = EXP_get_values.ins_line_OUT AND LKP_sup_risk_unit_group.prdct_type_code = EXP_get_values.risk_type_ind_OUT
	LEFT JOIN lkp_sup_insurance_line
	ON lkp_sup_insurance_line.ins_line_code = EXP_get_values.ins_line_OUT
),
LKP_COVERAGE_DIM AS (
	SELECT
	cov_dim_id,
	eff_from_date,
	in_eff_from_date,
	edw_claimant_cov_det_ak_id,
	in_edw_claimant_cov_det_ak_id
	FROM (
		SELECT 
			cov_dim_id,
			eff_from_date,
			in_eff_from_date,
			edw_claimant_cov_det_ak_id,
			in_edw_claimant_cov_det_ak_id
		FROM coverage_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id,eff_from_date ORDER BY cov_dim_id) = 1
),
RTR_claim_coverage_dim AS (
	SELECT
	LKP_COVERAGE_DIM.cov_dim_id AS claim_cov_dim_id,
	EXP_DEFAULT_VALUES.claimant_cov_det_id,
	EXP_DEFAULT_VALUES.ins_line,
	EXP_DEFAULT_VALUES.ins_line_descript_out,
	EXP_DEFAULT_VALUES.major_peril_code,
	EXP_DEFAULT_VALUES.major_peril_descript_out,
	EXP_DEFAULT_VALUES.major_peril_seq,
	EXP_DEFAULT_VALUES.cause_of_loss,
	EXP_DEFAULT_VALUES.cause_of_loss_short_descript_out,
	EXP_DEFAULT_VALUES.cause_of_loss_long_descript_out,
	EXP_DEFAULT_VALUES.reserve_ctgry,
	EXP_DEFAULT_VALUES.reserve_ctgry_descript_out,
	EXP_DEFAULT_VALUES.risk_unit_grp,
	EXP_DEFAULT_VALUES.risk_unit_grp_descript_out,
	EXP_DEFAULT_VALUES.risk_unit_grp_seq_num,
	EXP_DEFAULT_VALUES.risk_unit,
	EXP_DEFAULT_VALUES.risk_unit_descript_out,
	EXP_DEFAULT_VALUES.risk_unit_seq_num,
	EXP_DEFAULT_VALUES.unit_veh_registration_state_code,
	EXP_DEFAULT_VALUES.unit_vin_num,
	EXP_DEFAULT_VALUES.unit_veh_stated_amt,
	EXP_DEFAULT_VALUES.unit_veh_yr,
	EXP_DEFAULT_VALUES.unit_veh_make,
	EXP_DEFAULT_VALUES.s3p_unit_type_code,
	EXP_DEFAULT_VALUES.pms_type_bureau_code,
	EXP_DEFAULT_VALUES.loc_unit_num,
	EXP_DEFAULT_VALUES.spec_pers_prop_use_code,
	EXP_DEFAULT_VALUES.pkg_ded_amt,
	EXP_DEFAULT_VALUES.pkg_lmt_amt,
	EXP_DEFAULT_VALUES.crrnt_snpsht_flag,
	EXP_DEFAULT_VALUES.audit_id,
	EXP_DEFAULT_VALUES.eff_from_date,
	EXP_DEFAULT_VALUES.eff_to_date,
	EXP_DEFAULT_VALUES.source_sys_id,
	EXP_DEFAULT_VALUES.created_date,
	EXP_DEFAULT_VALUES.modified_date,
	EXP_DEFAULT_VALUES.claimant_cov_det_ak_id,
	EXP_DEFAULT_VALUES.claimant_cov_eff_date,
	EXP_DEFAULT_VALUES.sub_loc_unit_num
	FROM EXP_DEFAULT_VALUES
	LEFT JOIN LKP_COVERAGE_DIM
	ON LKP_COVERAGE_DIM.edw_claimant_cov_det_ak_id = EXP_DEFAULT_VALUES.claimant_cov_det_ak_id AND LKP_COVERAGE_DIM.eff_from_date = EXP_DEFAULT_VALUES.eff_from_date
),
RTR_claim_coverage_dim_INSERT AS (SELECT * FROM RTR_claim_coverage_dim WHERE ISNULL(claim_cov_dim_id)),
RTR_claim_coverage_dim_DEFAULT1 AS (SELECT * FROM RTR_claim_coverage_dim WHERE NOT ( (ISNULL(claim_cov_dim_id)) )),
UPD_claim_coverage_dim_update AS (
	SELECT
	claim_cov_dim_id AS claim_cov_dim_id2, 
	claimant_cov_det_id AS claimant_cov_det_id2, 
	ins_line AS ins_line2, 
	ins_line_descript_out AS ins_line_descript_out2, 
	major_peril_code AS major_peril_code2, 
	major_peril_descript_out AS major_peril_descript_out2, 
	major_peril_seq AS major_peril_seq2, 
	cause_of_loss AS cause_of_loss2, 
	cause_of_loss_short_descript_out AS cause_of_loss_short_descript_out2, 
	cause_of_loss_long_descript_out AS cause_of_loss_long_descript_out2, 
	reserve_ctgry AS reserve_ctgry2, 
	reserve_ctgry_descript_out AS reserve_ctgry_descript_out2, 
	pms_type_bureau_code AS pms_type_bureau_code2, 
	risk_unit_grp AS risk_unit_grp2, 
	risk_unit_grp_descript_out AS risk_unit_grp_descript_out2, 
	risk_unit_grp_seq_num AS risk_unit_grp_seq_num2, 
	risk_unit AS risk_unit2, 
	risk_unit_descript_out AS risk_unit_descript_out2, 
	risk_unit_seq_num AS risk_unit_seq_num2, 
	unit_veh_registration_state_code AS unit_veh_registration_state_code2, 
	unit_vin_num AS unit_vin_num2, 
	unit_veh_stated_amt AS unit_veh_stated_amt2, 
	unit_veh_yr AS unit_veh_yr2, 
	unit_veh_make AS unit_veh_make2, 
	s3p_unit_type_code AS s3p_unit_type_code2, 
	loc_unit_num AS loc_unit_num2, 
	spec_pers_prop_use_code AS spec_pers_prop_use_code2, 
	pkg_ded_amt AS pkg_ded_amt2, 
	pkg_lmt_amt AS pkg_lmt_amt2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	source_sys_id AS source_sys_id2, 
	modified_date AS modified_date2, 
	claimant_cov_det_ak_id AS claimant_cov_det_ak_id2, 
	claimant_cov_eff_date AS claimant_cov_eff_date2, 
	sub_loc_unit_num AS sub_loc_unit_num2
	FROM RTR_claim_coverage_dim_DEFAULT1
),
coverage_dim_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage_dim AS T
	USING UPD_claim_coverage_dim_update AS S
	ON T.cov_dim_id = S.claim_cov_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_claimant_cov_det_pk_id = S.claimant_cov_det_id2, T.edw_claimant_cov_det_ak_id = S.claimant_cov_det_ak_id2, T.ins_line = S.ins_line2, T.ins_line_descript = S.ins_line_descript_out2, T.major_peril_code = S.major_peril_code2, T.major_peril_descript = S.major_peril_descript_out2, T.major_peril_seq_num = S.major_peril_seq2, T.type_bureau_code = S.pms_type_bureau_code2, T.risk_unit_grp = S.risk_unit_grp2, T.risk_unit_grp_descript = S.risk_unit_grp_descript_out2, T.risk_unit_grp_seq_num = S.risk_unit_grp_seq_num2, T.risk_unit = S.risk_unit2, T.risk_unit_descript = S.risk_unit_descript_out2, T.risk_unit_seq_num = S.risk_unit_seq_num2, T.unit_veh_registration_state_code = S.unit_veh_registration_state_code2, T.unit_vin_num = S.unit_vin_num2, T.unit_veh_state_amt = S.unit_veh_stated_amt2, T.unit_veh_yr = S.unit_veh_yr2, T.unit_veh_make = S.unit_veh_make2, T.loc_unit_num = S.loc_unit_num2, T.spec_pers_prop_use_code = S.spec_pers_prop_use_code2, T.pkg_deduction_amt = S.pkg_ded_amt2, T.pkg_lmt_amt = S.pkg_lmt_amt2, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.modified_date = S.modified_date2, T.cov_eff_date = S.claimant_cov_eff_date2, T.sub_loc_unit_num = S.sub_loc_unit_num2
),
UPD_claim_coverage_dim_insert AS (
	SELECT
	claimant_cov_det_id AS claimant_cov_det_id1, 
	ins_line AS ins_line1, 
	ins_line_descript_out AS ins_line_descript_out1, 
	major_peril_code AS major_peril_code1, 
	major_peril_descript_out AS major_peril_descript_out1, 
	major_peril_seq AS major_peril_seq1, 
	cause_of_loss AS cause_of_loss1, 
	cause_of_loss_short_descript_out AS cause_of_loss_short_descript_out1, 
	cause_of_loss_long_descript_out AS cause_of_loss_long_descript_out1, 
	reserve_ctgry AS reserve_ctgry1, 
	reserve_ctgry_descript_out AS reserve_ctgry_descript_out1, 
	risk_unit_grp AS risk_unit_grp1, 
	risk_unit_grp_descript_out AS risk_unit_grp_descript_out1, 
	risk_unit_grp_seq_num AS risk_unit_grp_seq_num1, 
	risk_unit AS risk_unit1, 
	risk_unit_descript_out AS risk_unit_descript_out1, 
	risk_unit_seq_num AS risk_unit_seq_num1, 
	unit_veh_registration_state_code AS unit_veh_registration_state_code1, 
	unit_vin_num AS unit_vin_num1, 
	unit_veh_stated_amt AS unit_veh_stated_amt1, 
	unit_veh_yr AS unit_veh_yr1, 
	unit_veh_make AS unit_veh_make1, 
	s3p_unit_type_code AS s3p_unit_type_code1, 
	pms_type_bureau_code AS pms_type_bureau_code1, 
	loc_unit_num AS loc_unit_num1, 
	spec_pers_prop_use_code AS spec_pers_prop_use_code1, 
	pkg_ded_amt AS pkg_ded_amt1, 
	pkg_lmt_amt AS pkg_lmt_amt1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	source_sys_id AS source_sys_id1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	claimant_cov_det_ak_id AS claimant_cov_det_ak_id1, 
	claimant_cov_eff_date AS claimant_cov_eff_date1, 
	sub_loc_unit_num AS sub_loc_unit_num1
	FROM RTR_claim_coverage_dim_INSERT
),
coverage_dim_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage_dim
	(edw_claimant_cov_det_pk_id, edw_claimant_cov_det_ak_id, ins_line, ins_line_descript, major_peril_code, major_peril_descript, major_peril_seq_num, type_bureau_code, risk_unit_grp, risk_unit_grp_descript, risk_unit_grp_seq_num, risk_unit, risk_unit_descript, risk_unit_seq_num, unit_veh_registration_state_code, unit_vin_num, unit_veh_state_amt, unit_veh_yr, unit_veh_make, loc_unit_num, spec_pers_prop_use_code, pkg_deduction_amt, pkg_lmt_amt, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, cov_eff_date, sub_loc_unit_num)
	SELECT 
	claimant_cov_det_id1 AS EDW_CLAIMANT_COV_DET_PK_ID, 
	claimant_cov_det_ak_id1 AS EDW_CLAIMANT_COV_DET_AK_ID, 
	ins_line1 AS INS_LINE, 
	ins_line_descript_out1 AS INS_LINE_DESCRIPT, 
	major_peril_code1 AS MAJOR_PERIL_CODE, 
	major_peril_descript_out1 AS MAJOR_PERIL_DESCRIPT, 
	major_peril_seq1 AS MAJOR_PERIL_SEQ_NUM, 
	pms_type_bureau_code1 AS TYPE_BUREAU_CODE, 
	risk_unit_grp1 AS RISK_UNIT_GRP, 
	risk_unit_grp_descript_out1 AS RISK_UNIT_GRP_DESCRIPT, 
	risk_unit_grp_seq_num1 AS RISK_UNIT_GRP_SEQ_NUM, 
	risk_unit1 AS RISK_UNIT, 
	risk_unit_descript_out1 AS RISK_UNIT_DESCRIPT, 
	risk_unit_seq_num1 AS RISK_UNIT_SEQ_NUM, 
	unit_veh_registration_state_code1 AS UNIT_VEH_REGISTRATION_STATE_CODE, 
	unit_vin_num1 AS UNIT_VIN_NUM, 
	unit_veh_stated_amt1 AS UNIT_VEH_STATE_AMT, 
	unit_veh_yr1 AS UNIT_VEH_YR, 
	unit_veh_make1 AS UNIT_VEH_MAKE, 
	loc_unit_num1 AS LOC_UNIT_NUM, 
	spec_pers_prop_use_code1 AS SPEC_PERS_PROP_USE_CODE, 
	pkg_ded_amt1 AS PKG_DEDUCTION_AMT, 
	pkg_lmt_amt1 AS PKG_LMT_AMT, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	claimant_cov_eff_date1 AS COV_EFF_DATE, 
	sub_loc_unit_num1 AS SUB_LOC_UNIT_NUM
	FROM UPD_claim_coverage_dim_insert
),
SQ_coverage_dim AS (
	SELECT 
	COVERAGE_DIM.COV_DIM_ID, 
	COVERAGE_DIM.EFF_FROM_DATE, 
	COVERAGE_DIM.EFF_TO_DATE, 
	COVERAGE_DIM.EDW_CLAIMANT_COV_DET_AK_ID 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.COVERAGE_DIM COVERAGE_DIM
	WHERE EXISTS 
	(
	SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.COVERAGE_DIM COVERAGE_DIM2
	WHERE CRRNT_SNPSHT_FLAG = 1 AND COVERAGE_DIM.EDW_CLAIMANT_COV_DET_AK_ID = COVERAGE_DIM2.EDW_CLAIMANT_COV_DET_AK_ID 
	GROUP BY COVERAGE_DIM2.EDW_CLAIMANT_COV_DET_AK_ID HAVING COUNT(*) > 1
	)
	ORDER BY COVERAGE_DIM.EDW_CLAIMANT_COV_DET_AK_ID , COVERAGE_DIM.EFF_FROM_DATE DESC
),
EXP_Lag_eff_from_date111 AS (
	SELECT
	cov_dim_id,
	edw_claimant_cov_det_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claimant_cov_det_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		edw_claimant_cov_det_ak_id = v_PREV_ROW_occurrence_key, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claimant_cov_det_ak_id AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_coverage_dim
),
FIL_claim_representative_dim_update AS (
	SELECT
	cov_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date111
	WHERE orig_eff_to_date != eff_to_date
),
UPD_claim_representative_dim AS (
	SELECT
	cov_dim_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_claim_representative_dim_update
),
coverage_dim_expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage_dim AS T
	USING UPD_claim_representative_dim AS S
	ON T.cov_dim_id = S.cov_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),