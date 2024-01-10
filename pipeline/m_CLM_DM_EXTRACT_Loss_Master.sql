WITH
LKP_sup_state AS (
	SELECT
	state_abbrev,
	state_code
	FROM (
		SELECT 
			state_abbrev,
			state_code
		FROM sup_state
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_abbrev DESC) = 1
),
LKP_calendar_dim AS (
	SELECT
	clndr_date,
	clndr_id
	FROM (
		SELECT 
			clndr_date,
			clndr_id
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_id ORDER BY clndr_date DESC) = 1
),
SQ_loss_master_fact AS (
	SELECT
	       loss_master_fact. loss_master_fact_id,
	       loss_master_fact.claim_trans_date_id,
	       loss_master_fact.incptn_date_id,
	       loss_master_fact.new_claim_count,
	       loss_master_fact.outstanding_amt,
	       loss_master_fact.paid_loss_amt,
	       loss_master_fact.paid_exp_amt,
	       loss_master_fact.eom_unpaid_loss_adjust_exp,
	       loss_master_fact.orig_reserve,
	       claim_occurrence_dim.source_claim_rpted_date,
	       claim_occurrence_dim.claim_loss_date,
	       claim_occurrence_dim.claim_cat_code,
	       claim_occurrence_dim.claim_num,
	       claim_occurrence_dim.claim_occurrence_num,
	       claim_occurrence_dim.claim_loss_descript,
	       claim_occurrence_dim.loss_loc_state,
	       policy_dim.pol_sym,
	       policy_dim.pol_num,
	       policy_dim.pol_mod,
	       policy_dim.mco,
	       policy_dim.producer_code,
	       policy_dim.pol_co_num,
	       policy_dim.pol_eff_date,
	       policy_dim.pol_exp_date,
	       policy_dim.pms_pol_lob_code,
	       policy_dim.pol_co_line_code,
	       policy_dim.ClassOfBusinessCode,
	       agency_dim.agency_state_code,
	       '60'  AS  terr_code,
	       agency_dim.agency_num,
	       claim_case_dim.suit_status_code,
	       claim_transaction_type_dim.pms_trans_code,
	       claim_transaction_type_dim.trans_ctgry_code,
	       claim_transaction_type_dim.trans_kind_code,
	       claim_transaction_type_dim.type_disability,
	       contract_customer_dim.cust_num,
	       contract_customer_dim.name,
	       contract_customer_dim.sort_name,
	       contract_customer_dim.mailing_addr_line_1,
	       contract_customer_dim.mailing_city_name,
	       contract_customer_dim.mailing_state_prov_code,
	       coverage_dim.ins_line,
	       coverage_dim.major_peril_code,
	       coverage_dim.major_peril_seq_num,
	       coverage_dim.type_bureau_code,
	       coverage_dim.risk_unit_grp,
	       coverage_dim.risk_unit_grp_seq_num,
	       coverage_dim.risk_unit,
	       coverage_dim.risk_unit_seq_num,
	       coverage_dim.loc_unit_num,
	       coverage_dim.sub_loc_unit_num,
	       loss_master_dim.risk_state_prov_code,
	       loss_master_dim.risk_zip_code,
	       loss_master_dim.terr_code,
	       loss_master_dim.tax_loc,
	       loss_master_dim.class_code,
	       loss_master_dim.exposure,
	       loss_master_dim.sub_line_code,
	       loss_master_dim.source_sar_asl,
	       loss_master_dim.source_sar_prdct_line,
	       loss_master_dim.source_sar_sp_use_code,
	       loss_master_dim.source_statistical_line,
	       loss_master_dim.variation_code,
	       loss_master_dim.pol_type,
	       loss_master_dim.auto_reins_facility,
	       loss_master_dim.statistical_brkdwn_line,
	       loss_master_dim.statistical_code1,
	       loss_master_dim.statistical_code2,
	       loss_master_dim.statistical_code3,
	       loss_master_dim.loss_master_cov_code,
	       claim_representative_dim_H.handling_office_code,
	       claim_representative_dim_H.claim_rep_branch_num,
	       claim_representative_dim_H.cost_center,
	       claim_representative_dim_H.claim_rep_num,
	       claim_representative_dim_E.claim_rep_num as claim_rep_num_E,
	       claim_payment_dim.micro_ecd_draft_num,
	       claim_payment_dim.bank_acct_num,
	       claimant_dim.claimant_full_name,
	       claimant_dim.claimant_num,
	       claimant_dim.body_part_code,
	       claimant_dim.nature_inj_code,
	       claimant_dim.cause_inj_code,
	       reinsurance_coverage_dim.reins_co_num,
	       reinsurance_coverage_dim.reins_type,
	       reinsurance_coverage_dim.reins_prcnt_loss_ceded,
	       claimant_coverage_dim.cause_of_loss,
	       claimant_coverage_dim.reserve_ctgry,
	       calendar_dim.clndr_date
	FROM   loss_master_dim,
	       claim_representative_dim claim_representative_dim_H,
	       claim_representative_dim claim_representative_dim_E,
	       claim_payment_dim,
	       claimant_dim,
	       reinsurance_coverage_dim,
	       claimant_coverage_dim,
	       calendar_dim,
	       coverage_dim,
	       loss_master_fact,
	       claim_occurrence_dim,
	       policy_dim,
	       V2.agency_dim agency_dim,
	       claim_case_dim,
	       claim_transaction_type_dim,
	       contract_customer_dim 
	WHERE  loss_master_fact.claim_occurrence_dim_id = claim_occurrence_dim.claim_occurrence_dim_id
	       AND loss_master_fact.pol_dim_id = policy_dim.pol_dim_id
	       AND loss_master_fact.agency_dim_id = agency_dim.agency_dim_id
	       AND loss_master_fact.claim_pay_dim_id = claim_payment_dim.claim_pay_dim_id
	       AND loss_master_fact.claim_trans_type_dim_id = claim_transaction_type_dim.claim_trans_type_dim_id
	       AND loss_master_fact.claimant_dim_id = claimant_dim.claimant_dim_id
	       AND loss_master_fact.cov_dim_id = coverage_dim.cov_dim_id
	       AND loss_master_fact.loss_master_dim_id = loss_master_dim.loss_master_dim_id
	       AND loss_master_fact.loss_master_run_date_id = calendar_dim.clndr_id
	       AND loss_master_fact.claimant_cov_dim_id = claimant_coverage_dim.claimant_cov_dim_id
	       AND loss_master_fact.claim_rep_dim_prim_claim_rep_id = claim_representative_dim_H.claim_rep_dim_id
	       AND loss_master_fact.claim_rep_dim_examiner_id = claim_representative_dim_E.claim_rep_dim_id
	       AND loss_master_fact.reins_cov_dim_id = reinsurance_coverage_dim.reins_cov_dim_id
	       AND loss_master_fact.contract_cust_dim_id = contract_customer_dim.contract_cust_dim_id
	       AND loss_master_fact.claim_case_dim_id = claim_case_dim.claim_case_dim_id
	       AND loss_master_fact.audit_id = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	
	--- 2/14/2012  Defaulted the value of Agency_Dim.terr_code to '60' as we dont have pay_code value of agency stored 
	--- at EDW  or datamart level. 
	
	--AND loss_master_fact.audit_id <> -50
	--AND loss_master_fact.loss_master_fact_id in (6301268,6328323)
),
EXP_Source AS (
	SELECT
	loss_master_fact_id,
	pms_trans_code,
	trans_ctgry_code,
	trans_kind_code,
	type_disability,
	pol_sym,
	pol_num,
	pol_mod,
	mco,
	producer_code,
	pol_co_num,
	pol_eff_date,
	pol_exp_date,
	pms_pol_lob_code,
	pol_co_line_code,
	ClassOfBusinessCode,
	agency_state_code,
	agency_terr_code,
	agency_num,
	clndr_date,
	ins_line,
	major_peril_code,
	major_peril_seq_num,
	type_bureau_code,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	loc_unit_num,
	sub_loc_unit_num,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code,
	cause_of_loss,
	reserve_ctgry,
	source_claim_rpted_date,
	claim_loss_date,
	claim_cat_code,
	claim_num,
	claim_occurrence_num,
	claim_loss_descript,
	handling_office_code,
	claim_rep_branch_num,
	cost_center,
	claim_rep_num AS claim_rep_num_H,
	claim_rep_num_E,
	reins_co_num,
	reins_type,
	reins_prcnt_loss_ceded,
	micro_ecd_draft_num,
	bank_acct_num,
	claim_trans_date_id,
	new_claim_count,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	eom_unpaid_loss_adjust_exp,
	orig_reserve,
	claimant_full_name,
	claimant_num,
	body_part_code,
	nature_inj_code,
	cause_inj_code,
	cust_num,
	name,
	sort_name,
	suit_status_code,
	incptn_date_id,
	loss_loc_state,
	mailing_addr_line_1,
	mailing_city_name,
	mailing_state_prov_code
	FROM SQ_loss_master_fact
),
LKP_Calendar_Dim_Trans_Date AS (
	SELECT
	clndr_id,
	clndr_date,
	clndr_yr,
	clndr_month
	FROM (
		SELECT
		DATEPART(day,calendar_dim.clndr_date) as clndr_date, 
		calendar_dim.clndr_yr as clndr_yr, 
		calendar_dim.clndr_month as clndr_month, 
		calendar_dim.clndr_id as clndr_id 
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_id ORDER BY clndr_id) = 1
),
EXP_Default_Value AS (
	SELECT
	EXP_Source.loss_master_fact_id,
	EXP_Source.pms_trans_code,
	EXP_Source.trans_ctgry_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(trans_ctgry_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(trans_ctgry_code
	) AS trans_ctgry_code_out,
	EXP_Source.trans_kind_code,
	EXP_Source.type_disability,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(type_disability)
	:UDF.DEFAULT_VALUE_TO_BLANKS(type_disability
	) AS type_disability_Out,
	EXP_Source.pol_sym,
	EXP_Source.pol_num,
	EXP_Source.pol_mod,
	EXP_Source.mco,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(mco)
	:UDF.DEFAULT_VALUE_TO_BLANKS(mco
	) AS mco_Out,
	EXP_Source.producer_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(producer_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(producer_code
	) AS producer_code_Out,
	EXP_Source.pol_co_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(pol_co_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(pol_co_num
	) AS pol_co_num_Out,
	EXP_Source.pol_eff_date,
	-- *INF*: TO_CHAR(pol_eff_date,'YYYYMMDD')
	TO_CHAR(pol_eff_date, 'YYYYMMDD'
	) AS pol_eff_date_Out,
	EXP_Source.pol_exp_date,
	-- *INF*: TO_CHAR(pol_exp_date,'YYYYMMDD')
	TO_CHAR(pol_exp_date, 'YYYYMMDD'
	) AS pol_exp_date_Out,
	EXP_Source.pms_pol_lob_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(pms_pol_lob_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(pms_pol_lob_code
	) AS pms_pol_lob_code_Out,
	EXP_Source.pol_co_line_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(pol_co_line_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(pol_co_line_code
	) AS pol_co_line_code_Out,
	EXP_Source.ClassOfBusinessCode,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(ClassOfBusinessCode)
	:UDF.DEFAULT_VALUE_TO_BLANKS(ClassOfBusinessCode
	) AS ClassOfBusinessCode_Out,
	EXP_Source.agency_state_code,
	-- *INF*: rtrim(agency_state_code)
	rtrim(agency_state_code
	) AS agency_state_code_out,
	EXP_Source.agency_terr_code,
	EXP_Source.agency_num,
	-- *INF*: rtrim(agency_state_code) || rtrim(agency_terr_code) || rtrim(agency_num)
	rtrim(agency_state_code
	) || rtrim(agency_terr_code
	) || rtrim(agency_num
	) AS agency_number_LM,
	-- *INF*: rtrim(agency_state_code) || '00' || rtrim(agency_num)
	rtrim(agency_state_code
	) || '00' || rtrim(agency_num
	) AS account_number_LM,
	EXP_Source.clndr_date,
	-- *INF*: SUBSTR(TO_CHAR(clndr_date,'YYYYMMDD'),1,6)
	SUBSTR(TO_CHAR(clndr_date, 'YYYYMMDD'
		), 1, 6
	) AS Loss_Master_Account_Date,
	EXP_Source.ins_line,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(ins_line)
	:UDF.DEFAULT_VALUE_TO_BLANKS(ins_line
	) AS ins_line_Out,
	EXP_Source.major_peril_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(major_peril_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(major_peril_code
	) AS major_peril_code_Out,
	EXP_Source.major_peril_seq_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(major_peril_seq_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(major_peril_seq_num
	) AS major_peril_seq_num_out,
	EXP_Source.type_bureau_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(type_bureau_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(type_bureau_code
	) AS type_bureau_code_Out,
	EXP_Source.risk_unit_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(risk_unit_grp)
	:UDF.DEFAULT_VALUE_TO_BLANKS(risk_unit_grp
	) AS risk_unit_grp_Out,
	EXP_Source.risk_unit_grp_seq_num,
	-- *INF*: IIF(risk_unit_grp_seq_num='N/A','0',
	-- SUBSTR(:UDF.DEFAULT_VALUE_TO_BLANKS(risk_unit_grp_seq_num),3,1))
	IFF(risk_unit_grp_seq_num = 'N/A',
		'0',
		SUBSTR(:UDF.DEFAULT_VALUE_TO_BLANKS(risk_unit_grp_seq_num
			), 3, 1
		)
	) AS risk_unit_grp_seq_num_pos_3,
	-- *INF*: IIF(risk_unit_grp_seq_num='N/A','0',
	-- SUBSTR(:UDF.DEFAULT_VALUE_TO_BLANKS(risk_unit_grp_seq_num),2,1))
	IFF(risk_unit_grp_seq_num = 'N/A',
		'0',
		SUBSTR(:UDF.DEFAULT_VALUE_TO_BLANKS(risk_unit_grp_seq_num
			), 2, 1
		)
	) AS risk_unit_grp_seq_num_pos_2,
	EXP_Source.risk_unit,
	-- *INF*: SUBSTR(:UDF.DEFAULT_VALUE_TO_BLANKS(risk_unit),1,3)
	SUBSTR(:UDF.DEFAULT_VALUE_TO_BLANKS(risk_unit
		), 1, 3
	) AS risk_unit_Out,
	EXP_Source.risk_unit_seq_num,
	-- *INF*: IIF(risk_unit_seq_num='N/A','0',risk_unit_seq_num)
	IFF(risk_unit_seq_num = 'N/A',
		'0',
		risk_unit_seq_num
	) AS risk_unit_seq_num_out,
	EXP_Source.loc_unit_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(loc_unit_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(loc_unit_num
	) AS loc_unit_num_Out,
	EXP_Source.sub_loc_unit_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(sub_loc_unit_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(sub_loc_unit_num
	) AS sub_loc_unit_num_Out,
	EXP_Source.risk_state_prov_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(risk_state_prov_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(risk_state_prov_code
	) AS risk_state_prov_code_Out,
	EXP_Source.risk_zip_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(risk_zip_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(risk_zip_code
	) AS risk_zip_code_Out,
	EXP_Source.terr_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(terr_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(terr_code
	) AS terr_code_Out,
	EXP_Source.tax_loc,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(tax_loc)
	:UDF.DEFAULT_VALUE_TO_BLANKS(tax_loc
	) AS tax_loc_Out,
	EXP_Source.class_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(class_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(class_code
	) AS class_code_Out,
	EXP_Source.exposure,
	EXP_Source.sub_line_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(sub_line_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(sub_line_code
	) AS sub_line_code_Out,
	EXP_Source.source_sar_asl,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(source_sar_asl)
	:UDF.DEFAULT_VALUE_TO_BLANKS(source_sar_asl
	) AS source_sar_asl_Out,
	EXP_Source.source_sar_prdct_line,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(source_sar_prdct_line)
	:UDF.DEFAULT_VALUE_TO_BLANKS(source_sar_prdct_line
	) AS source_sar_prdct_line_Out,
	EXP_Source.source_sar_sp_use_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(source_sar_sp_use_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(source_sar_sp_use_code
	) AS source_sar_sp_use_code_Out,
	EXP_Source.source_statistical_line,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(source_statistical_line)
	:UDF.DEFAULT_VALUE_TO_BLANKS(source_statistical_line
	) AS source_statistical_line_Out,
	EXP_Source.variation_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(variation_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(variation_code
	) AS variation_code_Out,
	EXP_Source.pol_type,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(pol_type)
	:UDF.DEFAULT_VALUE_TO_BLANKS(pol_type
	) AS pol_type_Out,
	EXP_Source.auto_reins_facility,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(auto_reins_facility)
	:UDF.DEFAULT_VALUE_TO_BLANKS(auto_reins_facility
	) AS auto_reins_facility_Out,
	EXP_Source.statistical_brkdwn_line,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(statistical_brkdwn_line)
	:UDF.DEFAULT_VALUE_TO_BLANKS(statistical_brkdwn_line
	) AS statistical_brkdwn_line_Out,
	EXP_Source.statistical_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(statistical_code1)
	:UDF.DEFAULT_VALUE_TO_BLANKS(statistical_code1
	) AS statistical_code1_Out,
	EXP_Source.statistical_code2,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(statistical_code2)
	:UDF.DEFAULT_VALUE_TO_BLANKS(statistical_code2
	) AS statistical_code2_Out,
	EXP_Source.statistical_code3,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(statistical_code3)
	:UDF.DEFAULT_VALUE_TO_BLANKS(statistical_code3
	) AS statistical_code3_Out,
	EXP_Source.loss_master_cov_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(loss_master_cov_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(loss_master_cov_code
	) AS loss_master_cov_code_Out,
	EXP_Source.cause_of_loss,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(cause_of_loss)
	:UDF.DEFAULT_VALUE_TO_BLANKS(cause_of_loss
	) AS cause_of_loss_Out,
	EXP_Source.reserve_ctgry,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(reserve_ctgry)
	:UDF.DEFAULT_VALUE_TO_BLANKS(reserve_ctgry
	) AS reserve_ctgry_Out,
	EXP_Source.source_claim_rpted_date,
	-- *INF*: TO_CHAR(source_claim_rpted_date,'YYYYMMDD')
	TO_CHAR(source_claim_rpted_date, 'YYYYMMDD'
	) AS source_claim_rpted_date_out,
	EXP_Source.claim_loss_date,
	-- *INF*: TO_CHAR(claim_loss_date,'YYYYMMDD')
	TO_CHAR(claim_loss_date, 'YYYYMMDD'
	) AS claim_loss_date_Out,
	EXP_Source.claim_cat_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_cat_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(claim_cat_code
	) AS claim_cat_code_Out,
	EXP_Source.claim_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(claim_num
	) AS claim_num_Out,
	EXP_Source.claim_occurrence_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_occurrence_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(claim_occurrence_num
	) AS claim_occurrence_num_Out,
	EXP_Source.claim_loss_descript,
	-- *INF*: SUBSTR(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_loss_descript),1,38)
	SUBSTR(:UDF.DEFAULT_VALUE_TO_BLANKS(claim_loss_descript
		), 1, 38
	) AS claim_loss_descript_Out,
	EXP_Source.handling_office_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(handling_office_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(handling_office_code
	) AS handling_office_code_Out,
	EXP_Source.claim_rep_branch_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_rep_branch_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(claim_rep_branch_num
	) AS claim_rep_branch_num_Out,
	EXP_Source.cost_center,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(cost_center)
	:UDF.DEFAULT_VALUE_TO_BLANKS(cost_center
	) AS cost_center_Out,
	EXP_Source.claim_rep_num_H,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claim_rep_num_H)
	:UDF.DEFAULT_VALUE_TO_BLANKS(claim_rep_num_H
	) AS claim_rep_num_Out,
	EXP_Source.reins_co_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(reins_co_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(reins_co_num
	) AS reins_co_num_Out,
	EXP_Source.reins_type,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(reins_type)
	:UDF.DEFAULT_VALUE_TO_BLANKS(reins_type
	) AS reins_type_Out,
	EXP_Source.reins_prcnt_loss_ceded,
	EXP_Source.micro_ecd_draft_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(micro_ecd_draft_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(micro_ecd_draft_num
	) AS micro_ecd_draft_num_Out,
	EXP_Source.bank_acct_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(bank_acct_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(bank_acct_num
	) AS bank_acct_num_Out,
	LKP_Calendar_Dim_Trans_Date.clndr_month AS trans_month,
	-- *INF*: lpad(to_char(trans_month),2,'0')
	lpad(to_char(trans_month
		), 2, '0'
	) AS trans_month_out,
	LKP_Calendar_Dim_Trans_Date.clndr_date AS trans_day,
	-- *INF*: lpad(to_char(trans_day),2,'0')
	lpad(to_char(trans_day
		), 2, '0'
	) AS trans_day_out,
	LKP_Calendar_Dim_Trans_Date.clndr_yr AS trans_year,
	EXP_Source.new_claim_count,
	EXP_Source.outstanding_amt,
	EXP_Source.paid_loss_amt,
	EXP_Source.paid_exp_amt,
	EXP_Source.eom_unpaid_loss_adjust_exp,
	EXP_Source.orig_reserve,
	EXP_Source.claimant_full_name,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_full_name)
	:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_full_name
	) AS claimant_full_name_Out,
	EXP_Source.claimant_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(claimant_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(claimant_num
	) AS claimant_num_Out,
	EXP_Source.body_part_code,
	EXP_Source.nature_inj_code,
	EXP_Source.cause_inj_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(body_part_code)
	--  || 
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(nature_inj_code)
	--  || 
	-- :UDF.DEFAULT_VALUE_TO_BLANKS(cause_inj_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(body_part_code
	) || :UDF.DEFAULT_VALUE_TO_BLANKS(nature_inj_code
	) || :UDF.DEFAULT_VALUE_TO_BLANKS(cause_inj_code
	) AS accident_injury_anal_code,
	EXP_Source.cust_num,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(cust_num)
	:UDF.DEFAULT_VALUE_TO_BLANKS(cust_num
	) AS cust_num_Out,
	EXP_Source.name,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(name)
	:UDF.DEFAULT_VALUE_TO_BLANKS(name
	) AS name_Out,
	EXP_Source.sort_name,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(sort_name)
	:UDF.DEFAULT_VALUE_TO_BLANKS(sort_name
	) AS sort_name_Out,
	EXP_Source.suit_status_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(suit_status_code)
	:UDF.DEFAULT_VALUE_TO_BLANKS(suit_status_code
	) AS suit_status_code_Out,
	EXP_Source.incptn_date_id,
	-- *INF*: TO_CHAR(:LKP.LKP_calendar_dim(incptn_date_id),'YYYYMM')
	TO_CHAR(LKP_CALENDAR_DIM_incptn_date_id.clndr_date, 'YYYYMM'
	) AS incptn_date,
	EXP_Source.loss_loc_state,
	-- *INF*: IIF(loss_loc_state='N/A'
	-- ,''
	-- ,:LKP.LKP_sup_state(:UDF.DEFAULT_VALUE_TO_BLANKS(loss_loc_state)))
	-- 
	-- 
	IFF(loss_loc_state = 'N/A',
		'',
		LKP_SUP_STATE__UDF_DEFAULT_VALUE_TO_BLANKS_loss_loc_state.state_abbrev
	) AS loss_loc_state_out,
	-- *INF*: IIF(trans_kind_code='D',
	-- '0',
	-- pol_sym || pol_num)
	IFF(trans_kind_code = 'D',
		'0',
		pol_sym || pol_num
	) AS reinsurance_cession_number,
	EXP_Source.claim_rep_num_E,
	EXP_Source.mailing_addr_line_1,
	EXP_Source.mailing_city_name,
	EXP_Source.mailing_state_prov_code,
	mailing_city_name || ', ' || mailing_state_prov_code AS mailing_city_address,
	'' AS Default_Spaces,
	'0' AS Default_0,
	'00' AS Default_00
	FROM EXP_Source
	LEFT JOIN LKP_Calendar_Dim_Trans_Date
	ON LKP_Calendar_Dim_Trans_Date.clndr_id = EXP_Source.claim_trans_date_id
	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_incptn_date_id
	ON LKP_CALENDAR_DIM_incptn_date_id.clndr_id = incptn_date_id

	LEFT JOIN LKP_SUP_STATE LKP_SUP_STATE__UDF_DEFAULT_VALUE_TO_BLANKS_loss_loc_state
	ON LKP_SUP_STATE__UDF_DEFAULT_VALUE_TO_BLANKS_loss_loc_state.state_code = :UDF.DEFAULT_VALUE_TO_BLANKS(loss_loc_state
	)

),
UPD_loss_master_file AS (
	SELECT
	loss_master_fact_id, 
	pms_trans_code, 
	mco_Out, 
	pol_co_num_Out, 
	pol_sym, 
	pol_num, 
	source_sar_sp_use_code_Out, 
	pol_eff_date_Out, 
	pol_exp_date_Out, 
	account_number_LM, 
	Loss_Master_Account_Date, 
	major_peril_seq_num_out, 
	risk_unit_Out, 
	agency_number_LM, 
	agency_state_code_out AS agency_state_code, 
	statistical_brkdwn_line_Out, 
	pol_co_line_code_Out, 
	source_sar_asl_Out, 
	claim_cat_code_Out, 
	cause_of_loss_Out, 
	new_claim_count, 
	claim_occurrence_num_Out, 
	source_claim_rpted_date_out, 
	claim_loss_date_Out, 
	claim_rep_branch_num_Out, 
	claim_rep_num_Out, 
	trans_kind_code, 
	reins_type_Out, 
	reins_co_num_Out, 
	reinsurance_cession_number, 
	reins_prcnt_loss_ceded, 
	bank_acct_num_Out, 
	micro_ecd_draft_num_Out, 
	outstanding_amt, 
	paid_loss_amt, 
	paid_exp_amt, 
	eom_unpaid_loss_adjust_exp, 
	loss_master_cov_code_Out, 
	loss_loc_state_out, 
	risk_state_prov_code_Out, 
	tax_loc_Out, 
	risk_zip_code_Out, 
	type_bureau_code_Out, 
	major_peril_code_Out, 
	class_code_Out, 
	exposure, 
	sub_line_code_Out, 
	statistical_code1_Out, 
	incptn_date, 
	pms_pol_lob_code_Out, 
	handling_office_code_Out, 
	source_sar_prdct_line_Out, 
	source_statistical_line_Out, 
	claim_num_Out, 
	claimant_num_Out, 
	claimant_full_name_Out, 
	name_Out, 
	orig_reserve, 
	type_disability_Out, 
	accident_injury_anal_code, 
	cust_num_Out, 
	producer_code_Out, 
	auto_reins_facility_Out, 
	variation_code_Out, 
	sort_name_Out, 
	claim_rep_num_E, 
	claim_loss_descript_Out, 
	ClassOfBusinessCode_Out AS pif_clb_Out, 
	pol_type_Out, 
	suit_status_code_Out, 
	trans_month_out AS trans_month, 
	trans_day_out AS trans_day, 
	trans_year, 
	cost_center_Out, 
	statistical_code2_Out, 
	statistical_code3_Out, 
	mailing_addr_line_1, 
	mailing_city_address, 
	pol_mod, 
	terr_code_Out, 
	ins_line_Out, 
	loc_unit_num_Out, 
	sub_loc_unit_num_Out, 
	risk_unit_grp_Out, 
	risk_unit_grp_seq_num_pos_3, 
	risk_unit_grp_seq_num_pos_2, 
	risk_unit_seq_num_out, 
	reserve_ctgry_Out, 
	trans_ctgry_code_out, 
	Default_Spaces, 
	Default_0, 
	Default_00
	FROM EXP_Default_Value
),
EDW_LOSS_MASTER AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.EDW_LOSS_MASTER
	(LM_TRANSACTION_CODE, LM_PIF_LOCATION, LM_MASTER_COMPANY_NUMBER, LM_POLICY_COMPANY_NUMBER, LM_POLICY_SYMBOL, LM_POLICY_NUMBER, LM_CERTIFICATE_NUMBER, LM_POLICY_ANNIV_EFF_DATE, LM_POLICY_ANNIV_EXP_DATE, LM_ACCOUNT_NUMBER, LM_ACCOUNT_ENTERED_DATE, LM_LOSS_ID_OR_MAJ_PER_SEQ, LM_MAJ_PER_OR_UNIT_NO, LM_AGENCY_NUMBER, LM_AGENCY_STATE_PROVINCE, LM_AGENCY_DEPT_CODE, LM_STAT_BREAKDOWN_LINE, LM_COMPANY_LINE, LM_ANNUAL_STATEMENT_LINE, LM_CATASTROPHE_NUMBER, LM_LOSS_CAUSE, LM_NEW_CLAIM_COUNT, LM_LOSS_OCCURRENCE_NUMBER, LM_DATE_REPORTED, LM_DATE_OF_LOSS, LM_BRANCH_NUMBER, LM_ADJUSTOR_BRANCH_NUMBER, LM_ADJUSTING_COMPANY_NUMBER, LM_KIND_CODE, LM_TYPE_REINSURANCE, LM_REINSURANCE_COMPANY_NUMBER, LM_REINSURANCE_CESSION_NUMBER, LM_REINSURANCE_RATIO, LM_BANK_NUMBER, LM_DRAFT_NUMBER, LM_AMOUNT_OUTSTANDING, LM_AMOUNT_PAID_LOSSES, LM_AMOUNT_PAID_EXPENSES, LM_EOM_UNPAID_LOSS_ADJ_EXP, LM_AUTO_AND_GEN_LIAB_COV_CODE, LM_ACCIDENT_STATE_PROVINCE, LM_RISK_STATE_PROVINCE, LM_REGION_CODE, LM_TAX_LOCATION, LM_RISK_ZIP_CODE_POSTAL_ZONE, LM_TYPE_BUREAU, LM_MAJOR_PERIL, LM_CLASS, LM_EXPOSURE, LM_CSP_SUB_LINE, LM_STATISTICAL_CODES, LM_CSP_INCEPTION_DATE, LM_LINE_OF_BUSINESS, LM_CLAIMS_OFFICE, LM_PRODUCT_LINE, LM_STATISTICAL_LINE, LM_CLAIM_CONVERSION_NUMBER, LM_CLAIMANT_NUMBER, LM_CLAIMANT_NAME, LM_INSUREDS_NAME, LM_AVERAGE_RESERVE_CODE, LM_ORIGINAL_RESERVE, LM_TYPE_DISABILITY, LM_ACCIDENT_INJURY_ANAL_CODE, LM_CUSTOMER_NUMBER, LM_PRODUCER_CODE, LM_AUTO_REINSURANCE_FACILITY, LM_VARIATION_CODE, LM_SORT_NAME, LM_EXAMINER_CODE, LM_ACCIDENT_DESCRIPTION, LM_SPECIAL_USE_SAR, LM_CLASS_OF_BUSINESS, LM_POLICY_TYPE, LM_SUIT_INDICATION, LM_DRAFT_MONTH, LM_DRAFT_DAY, LM_DRAFT_YEAR, LM_COST_CENTER, LM_COST_CONTAINMENT, LM_VARIABLE_STAT_AREA2, LM_VARIABLE_STAT_AREA3, LM_RISK_STREET_ADDRESS, LM_RISK_CITY_STATE_ADDRESS, LM_MODULE_NUMBER, LM_ZONE_TERRITORY, LM_SAR_INSURANCE_LINE, LM_SAR_LOCATION_NUMBER, LM_SAR_SUB_LOCATION_NUMBER, LM_SAR_RISK_UNIT_GROUP, LM_SAR_CLASS_CODE_GROUP, LM_SAR_CLASS_CODE_MEMBER, LM_SAR_SEQUENCE_RISK_UNIT_N, LM_SAR_SEQ_ADDIT_INT_TYP, LM_ORIGINAL_RESERVE_PART_1, LM_RESERVE_CATEGORY, LM_TRANSACTION_CATEGORY)
	SELECT 
	pms_trans_code AS LM_TRANSACTION_CODE, 
	Default_00 AS LM_PIF_LOCATION, 
	mco_Out AS LM_MASTER_COMPANY_NUMBER, 
	pol_co_num_Out AS LM_POLICY_COMPANY_NUMBER, 
	pol_sym AS LM_POLICY_SYMBOL, 
	pol_num AS LM_POLICY_NUMBER, 
	source_sar_sp_use_code_Out AS LM_CERTIFICATE_NUMBER, 
	pol_eff_date_Out AS LM_POLICY_ANNIV_EFF_DATE, 
	pol_exp_date_Out AS LM_POLICY_ANNIV_EXP_DATE, 
	account_number_LM AS LM_ACCOUNT_NUMBER, 
	Loss_Master_Account_Date AS LM_ACCOUNT_ENTERED_DATE, 
	major_peril_seq_num_out AS LM_LOSS_ID_OR_MAJ_PER_SEQ, 
	risk_unit_Out AS LM_MAJ_PER_OR_UNIT_NO, 
	agency_number_LM AS LM_AGENCY_NUMBER, 
	agency_state_code AS LM_AGENCY_STATE_PROVINCE, 
	Default_00 AS LM_AGENCY_DEPT_CODE, 
	statistical_brkdwn_line_Out AS LM_STAT_BREAKDOWN_LINE, 
	pol_co_line_code_Out AS LM_COMPANY_LINE, 
	source_sar_asl_Out AS LM_ANNUAL_STATEMENT_LINE, 
	claim_cat_code_Out AS LM_CATASTROPHE_NUMBER, 
	cause_of_loss_Out AS LM_LOSS_CAUSE, 
	new_claim_count AS LM_NEW_CLAIM_COUNT, 
	claim_occurrence_num_Out AS LM_LOSS_OCCURRENCE_NUMBER, 
	source_claim_rpted_date_out AS LM_DATE_REPORTED, 
	claim_loss_date_Out AS LM_DATE_OF_LOSS, 
	Default_00 AS LM_BRANCH_NUMBER, 
	claim_rep_branch_num_Out AS LM_ADJUSTOR_BRANCH_NUMBER, 
	claim_rep_num_Out AS LM_ADJUSTING_COMPANY_NUMBER, 
	trans_kind_code AS LM_KIND_CODE, 
	reins_type_Out AS LM_TYPE_REINSURANCE, 
	reins_co_num_Out AS LM_REINSURANCE_COMPANY_NUMBER, 
	reinsurance_cession_number AS LM_REINSURANCE_CESSION_NUMBER, 
	reins_prcnt_loss_ceded AS LM_REINSURANCE_RATIO, 
	bank_acct_num_Out AS LM_BANK_NUMBER, 
	micro_ecd_draft_num_Out AS LM_DRAFT_NUMBER, 
	outstanding_amt AS LM_AMOUNT_OUTSTANDING, 
	paid_loss_amt AS LM_AMOUNT_PAID_LOSSES, 
	paid_exp_amt AS LM_AMOUNT_PAID_EXPENSES, 
	eom_unpaid_loss_adjust_exp AS LM_EOM_UNPAID_LOSS_ADJ_EXP, 
	loss_master_cov_code_Out AS LM_AUTO_AND_GEN_LIAB_COV_CODE, 
	loss_loc_state_out AS LM_ACCIDENT_STATE_PROVINCE, 
	risk_state_prov_code_Out AS LM_RISK_STATE_PROVINCE, 
	Default_0 AS LM_REGION_CODE, 
	tax_loc_Out AS LM_TAX_LOCATION, 
	risk_zip_code_Out AS LM_RISK_ZIP_CODE_POSTAL_ZONE, 
	type_bureau_code_Out AS LM_TYPE_BUREAU, 
	major_peril_code_Out AS LM_MAJOR_PERIL, 
	class_code_Out AS LM_CLASS, 
	exposure AS LM_EXPOSURE, 
	sub_line_code_Out AS LM_CSP_SUB_LINE, 
	statistical_code1_Out AS LM_STATISTICAL_CODES, 
	incptn_date AS LM_CSP_INCEPTION_DATE, 
	pms_pol_lob_code_Out AS LM_LINE_OF_BUSINESS, 
	handling_office_code_Out AS LM_CLAIMS_OFFICE, 
	source_sar_prdct_line_Out AS LM_PRODUCT_LINE, 
	source_statistical_line_Out AS LM_STATISTICAL_LINE, 
	claim_num_Out AS LM_CLAIM_CONVERSION_NUMBER, 
	claimant_num_Out AS LM_CLAIMANT_NUMBER, 
	claimant_full_name_Out AS LM_CLAIMANT_NAME, 
	name_Out AS LM_INSUREDS_NAME, 
	Default_0 AS LM_AVERAGE_RESERVE_CODE, 
	orig_reserve AS LM_ORIGINAL_RESERVE, 
	type_disability_Out AS LM_TYPE_DISABILITY, 
	accident_injury_anal_code AS LM_ACCIDENT_INJURY_ANAL_CODE, 
	cust_num_Out AS LM_CUSTOMER_NUMBER, 
	producer_code_Out AS LM_PRODUCER_CODE, 
	auto_reins_facility_Out AS LM_AUTO_REINSURANCE_FACILITY, 
	variation_code_Out AS LM_VARIATION_CODE, 
	sort_name_Out AS LM_SORT_NAME, 
	claim_rep_num_E AS LM_EXAMINER_CODE, 
	claim_loss_descript_Out AS LM_ACCIDENT_DESCRIPTION, 
	source_sar_sp_use_code_Out AS LM_SPECIAL_USE_SAR, 
	pif_clb_Out AS LM_CLASS_OF_BUSINESS, 
	pol_type_Out AS LM_POLICY_TYPE, 
	suit_status_code_Out AS LM_SUIT_INDICATION, 
	trans_month AS LM_DRAFT_MONTH, 
	trans_day AS LM_DRAFT_DAY, 
	trans_year AS LM_DRAFT_YEAR, 
	cost_center_Out AS LM_COST_CENTER, 
	Default_00 AS LM_COST_CONTAINMENT, 
	statistical_code2_Out AS LM_VARIABLE_STAT_AREA2, 
	statistical_code3_Out AS LM_VARIABLE_STAT_AREA3, 
	mailing_addr_line_1 AS LM_RISK_STREET_ADDRESS, 
	mailing_city_address AS LM_RISK_CITY_STATE_ADDRESS, 
	pol_mod AS LM_MODULE_NUMBER, 
	terr_code_Out AS LM_ZONE_TERRITORY, 
	ins_line_Out AS LM_SAR_INSURANCE_LINE, 
	loc_unit_num_Out AS LM_SAR_LOCATION_NUMBER, 
	sub_loc_unit_num_Out AS LM_SAR_SUB_LOCATION_NUMBER, 
	risk_unit_grp_Out AS LM_SAR_RISK_UNIT_GROUP, 
	risk_unit_grp_seq_num_pos_3 AS LM_SAR_CLASS_CODE_GROUP, 
	risk_unit_grp_seq_num_pos_2 AS LM_SAR_CLASS_CODE_MEMBER, 
	risk_unit_seq_num_out AS LM_SAR_SEQUENCE_RISK_UNIT_N, 
	Default_0 AS LM_SAR_SEQ_ADDIT_INT_TYP, 
	Default_0 AS LM_ORIGINAL_RESERVE_PART_1, 
	reserve_ctgry_Out AS LM_RESERVE_CATEGORY, 
	trans_ctgry_code_out AS LM_TRANSACTION_CATEGORY
	FROM UPD_loss_master_file
),