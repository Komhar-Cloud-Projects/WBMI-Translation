WITH
SQ_work_claim_ncci_report_extract AS (
	SELECT ncci_extract_tab.work_claim_ncci_rpt_extract_id,
	       ncci_extract_tab.edw_claim_occurrence_ak_id,
	       ncci_extract_tab.edw_claim_party_occurrence_ak_id,
	       ncci_extract_tab.edw_pol_ak_id,
	       ncci_extract_tab.created_date,
	       ncci_extract_tab.transm_status,
	       ncci_extract_tab.rcrd_type_code,
	       ncci_extract_tab.carrier_code,
	       ncci_extract_tab.future_use_1,
	       ncci_extract_tab.pol_num_id,
	       ncci_extract_tab.pol_eff_date,
	       ncci_extract_tab.valuation_lvl_code,
	       ncci_extract_tab.repl_rpt_code,
	       ncci_extract_tab.claim_num_id,
	       ncci_extract_tab.future_use_2,
	       ncci_extract_tab.jurisdiction_state,
	       ncci_extract_tab.acc_state,
	       ncci_extract_tab.acc_date,
	       ncci_extract_tab.reported_to_insr_date,
	       ncci_extract_tab.class_code,
	       ncci_extract_tab.type_of_loss,
	       ncci_extract_tab.type_of_recovery,
	       ncci_extract_tab.type_of_claim,
	       ncci_extract_tab.claimant_gndr_code,
	       ncci_extract_tab.birth_yr,
	       ncci_extract_tab.hire_yr,
	       ncci_extract_tab.future_use_3,
	       ncci_extract_tab.preinjury_avg_weeky_wage_amt,
	       ncci_extract_tab.method_of_determining_preinjury_avg_wkly_wage_code,
	       ncci_extract_tab.part_of_body_code,
	       ncci_extract_tab.nature_of_inj,
	       ncci_extract_tab.cause_of_inj,
	       ncci_extract_tab.claim_status_code,
	       ncci_extract_tab.closing_date,
	       ncci_extract_tab.incurred_indemnity_amt_total,
	       ncci_extract_tab.bnft_type_code_1,
	       ncci_extract_tab.bnft_amt_paid_1,
	       ncci_extract_tab.wkly_bnft_1,
	       ncci_extract_tab.bnft_type_code_2,
	       ncci_extract_tab.bnft_amt_paid_2,
	       ncci_extract_tab.wkly_bnft_2,
	       ncci_extract_tab.bnft_type_code_3,
	       ncci_extract_tab.bnft_amt_paid_3,
	       ncci_extract_tab.wkly_bnft_3,
	       ncci_extract_tab.bnft_type_code_4,
	       ncci_extract_tab.bnft_amt_paid_4,
	       ncci_extract_tab.wkly_bnft_4,
	       ncci_extract_tab.bnft_type_code_5,
	       ncci_extract_tab.bnft_amt_paid_5,
	       ncci_extract_tab.wkly_bnft_5,
	       ncci_extract_tab.future_use_4,
	       ncci_extract_tab.vocational_rehabilitation_evaluation_exp_amt_paid,
	       ncci_extract_tab.vocational_rehabilitation_maint_bnft_amt_paid,
	       ncci_extract_tab.vocational_rehabilitation_education_exp_amt_paid,
	       ncci_extract_tab.vocational_rehabilitation_other_amt_paid,
	       ncci_extract_tab.incurred_med_amt_total,
	       ncci_extract_tab.total_paid_med_amt,
	       ncci_extract_tab.post_inj_wkly_wage_amt,
	       ncci_extract_tab.impairment_disability_percentage,
	       ncci_extract_tab.impairment_percentage_basis_code,
	       ncci_extract_tab.max_med_improvement_date,
	       ncci_extract_tab.attorney_or_au_rep_ind,
	       ncci_extract_tab.controverted_disputed_case_ind,
	       ncci_extract_tab.claimant_lgl_amt_paid,
	       ncci_extract_tab.emplyr_lgl_amt_paid,
	       ncci_extract_tab.bnft_cvrd_by_lump_sum_settlement_code_1,
	       ncci_extract_tab.lump_sum_settlement_amt_paid_1,
	       ncci_extract_tab.bnft_cvrd_by_lump_sum_settlement_code_2,
	       ncci_extract_tab.lump_sum_settlement_amt_paid_2,
	       ncci_extract_tab.bnft_cvrd_by_lump_sum_settlement_code_3,
	       ncci_extract_tab.lump_sum_settlement_amt_paid_3,
	       ncci_extract_tab.bnft_cvrd_by_lump_sum_settlement_code_4,
	       ncci_extract_tab.lump_sum_settlement_amt_paid_4,
	       ncci_extract_tab.bnft_cvrd_by_lump_sum_settlement_code_5,
	       ncci_extract_tab.lump_sum_settlement_amt_paid_5,
	       ncci_extract_tab.bnft_cvrd_by_lump_sum_settlement_code_6,
	       ncci_extract_tab.lump_sum_settlement_amt_paid_6,
	       ncci_extract_tab.med_extinguishment_ind,
	       ncci_extract_tab.return_to_work_date,
	       ncci_extract_tab.return_to_work_rate_of_pay_ind,
	       ncci_extract_tab.extraordinary_loss_event_claim_ind,
	       ncci_extract_tab.future_use_5,
	       ncci_extract_tab.prv_carrier_code,
	       ncci_extract_tab.future_use_6,
	       ncci_extract_tab.prv_pol_num_id,
	       ncci_extract_tab.prv_pol_eff_date,
	       ncci_extract_tab.prv_reported_to_insr_date,
	       ncci_extract_tab.prv_claim_num_id,
	       ncci_extract_tab.recovery_reimb_amt,
	       ncci_extract_tab.future_use_7,
	       ncci_extract_tab.future_use_8
	FROM   work_claim_ncci_report_extract ncci_extract_tab WITH (NOLOCK)
	WHERE  (( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -( @{pipeline().parameters.NUMBER_OF_MONTHS} + 1 ), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	          AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -( @{pipeline().parameters.NUMBER_OF_MONTHS} ), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) ))
),
EXP_Calculate_Filter_Condition AS (
	SELECT
	work_claim_ncci_rpt_extract_id,
	edw_claim_occurrence_ak_id,
	edw_claim_party_occurrence_ak_id,
	edw_pol_ak_id,
	created_date,
	transm_status,
	rcrd_type_code,
	carrier_code,
	future_use_1,
	pol_num_id,
	pol_eff_date,
	valuation_lvl_code,
	repl_rpt_code,
	claim_num_id,
	future_use_2,
	jurisdiction_state,
	acc_state,
	acc_date,
	reported_to_insr_date,
	class_code,
	type_of_loss,
	type_of_recovery,
	type_of_claim,
	claimant_gndr_code,
	birth_yr,
	hire_yr,
	future_use_3,
	preinjury_avg_weeky_wage_amt,
	method_of_determining_preinjury_avg_wkly_wage_code,
	part_of_body_code,
	nature_of_inj,
	cause_of_inj,
	claim_status_code,
	closing_date,
	incurred_indemnity_amt_total,
	bnft_type_code_1,
	bnft_amt_paid_1,
	wkly_bnft_1,
	bnft_type_code_2,
	bnft_amt_paid_2,
	wkly_bnft_2,
	bnft_type_code_3,
	bnft_amt_paid_3,
	wkly_bnft_3,
	bnft_type_code_4,
	bnft_amt_paid_4,
	wkly_bnft_4,
	bnft_type_code_5,
	bnft_amt_paid_5,
	wkly_bnft_5,
	future_use_4,
	vocational_rehabilitation_evaluation_exp_amt_paid,
	vocational_rehabilitation_maint_bnft_amt_paid,
	vocational_rehabilitation_education_exp_amt_paid,
	vocational_rehabilitation_other_amt_paid,
	incurred_med_amt_total,
	total_paid_med_amt,
	post_inj_wkly_wage_amt,
	impairment_disability_percentage,
	impairment_percentage_basis_code,
	max_med_improvement_date,
	attorney_or_au_rep_ind,
	controverted_disputed_case_ind,
	claimant_lgl_amt_paid,
	emplyr_lgl_amt_paid,
	bnft_cvrd_by_lump_sum_settlement_code_1,
	lump_sum_settlement_amt_paid_1,
	bnft_cvrd_by_lump_sum_settlement_code_2,
	lump_sum_settlement_amt_paid_2,
	bnft_cvrd_by_lump_sum_settlement_code_3,
	lump_sum_settlement_amt_paid_3,
	bnft_cvrd_by_lump_sum_settlement_code_4,
	lump_sum_settlement_amt_paid_4,
	bnft_cvrd_by_lump_sum_settlement_code_5,
	lump_sum_settlement_amt_paid_5,
	bnft_cvrd_by_lump_sum_settlement_code_6,
	lump_sum_settlement_amt_paid_6,
	med_extinguishment_ind,
	return_to_work_date,
	return_to_work_rate_of_pay_ind,
	extraordinary_loss_event_claim_ind,
	future_use_5,
	prv_carrier_code,
	future_use_6,
	prv_pol_num_id,
	prv_pol_eff_date,
	prv_reported_to_insr_date,
	prv_claim_num_id,
	recovery_reimb_amt,
	future_use_7,
	future_use_8,
	@{pipeline().parameters.FILTER_CONDITION} AS filter_Condition
	FROM SQ_work_claim_ncci_report_extract
),
Filter_All_OR_No_Records AS (
	SELECT
	work_claim_ncci_rpt_extract_id, 
	edw_claim_occurrence_ak_id, 
	edw_claim_party_occurrence_ak_id, 
	edw_pol_ak_id, 
	created_date, 
	transm_status, 
	rcrd_type_code, 
	carrier_code, 
	future_use_1, 
	pol_num_id, 
	pol_eff_date, 
	valuation_lvl_code, 
	repl_rpt_code, 
	claim_num_id, 
	future_use_2, 
	jurisdiction_state, 
	acc_state, 
	acc_date, 
	reported_to_insr_date, 
	class_code, 
	type_of_loss, 
	type_of_recovery, 
	type_of_claim, 
	claimant_gndr_code, 
	birth_yr, 
	hire_yr, 
	future_use_3, 
	preinjury_avg_weeky_wage_amt, 
	method_of_determining_preinjury_avg_wkly_wage_code, 
	part_of_body_code, 
	nature_of_inj, 
	cause_of_inj, 
	claim_status_code, 
	closing_date, 
	incurred_indemnity_amt_total, 
	bnft_type_code_1, 
	bnft_amt_paid_1, 
	wkly_bnft_1, 
	bnft_type_code_2, 
	bnft_amt_paid_2, 
	wkly_bnft_2, 
	bnft_type_code_3, 
	bnft_amt_paid_3, 
	wkly_bnft_3, 
	bnft_type_code_4, 
	bnft_amt_paid_4, 
	wkly_bnft_4, 
	bnft_type_code_5, 
	bnft_amt_paid_5, 
	wkly_bnft_5, 
	future_use_4, 
	vocational_rehabilitation_evaluation_exp_amt_paid, 
	vocational_rehabilitation_maint_bnft_amt_paid, 
	vocational_rehabilitation_education_exp_amt_paid, 
	vocational_rehabilitation_other_amt_paid, 
	incurred_med_amt_total, 
	total_paid_med_amt, 
	post_inj_wkly_wage_amt, 
	impairment_disability_percentage, 
	impairment_percentage_basis_code, 
	max_med_improvement_date, 
	attorney_or_au_rep_ind, 
	controverted_disputed_case_ind, 
	claimant_lgl_amt_paid, 
	emplyr_lgl_amt_paid, 
	bnft_cvrd_by_lump_sum_settlement_code_1, 
	lump_sum_settlement_amt_paid_1, 
	bnft_cvrd_by_lump_sum_settlement_code_2, 
	lump_sum_settlement_amt_paid_2, 
	bnft_cvrd_by_lump_sum_settlement_code_3, 
	lump_sum_settlement_amt_paid_3, 
	bnft_cvrd_by_lump_sum_settlement_code_4, 
	lump_sum_settlement_amt_paid_4, 
	bnft_cvrd_by_lump_sum_settlement_code_5, 
	lump_sum_settlement_amt_paid_5, 
	bnft_cvrd_by_lump_sum_settlement_code_6, 
	lump_sum_settlement_amt_paid_6, 
	med_extinguishment_ind, 
	return_to_work_date, 
	return_to_work_rate_of_pay_ind, 
	extraordinary_loss_event_claim_ind, 
	future_use_5, 
	prv_carrier_code, 
	future_use_6, 
	prv_pol_num_id, 
	prv_pol_eff_date, 
	prv_reported_to_insr_date, 
	prv_claim_num_id, 
	recovery_reimb_amt, 
	future_use_7, 
	future_use_8, 
	filter_Condition
	FROM EXP_Calculate_Filter_Condition
	WHERE iif(upper(ltrim(rtrim(filter_Condition))) = 'TRUE',
true,false)
),
FF_NCCI_WC_CLAIMS AS (
	INSERT INTO FF_NCCI_WC_CLAIMS
	(rcrd_type_code, carrier_code, future_use_1, pol_num_id, pol_eff_date, valuation_lvl_code, repl_rpt_code, claim_num_id, future_use_2, jurisdiction_state, acc_state, acc_date, reported_to_insr_date, class_code, type_of_loss, type_of_recovery, type_of_claim, claimant_gndr_code, birth_yr, hire_yr, future_use_3, preinjury_avg_weeky_wage_amt, method_of_determining_preinjury_avg_wkly_wage_code, part_of_body_code, nature_of_inj, cause_of_inj, claim_status_code, closing_date, incurred_indemnity_amt_total, bnft_type_code_1, bnft_amt_paid_1, wkly_bnft_1, bnft_type_code_2, bnft_amt_paid_2, wkly_bnft_2, bnft_type_code_3, bnft_amt_paid_3, wkly_bnft_3, bnft_type_code_4, bnft_amt_paid_4, wkly_bnft_4, bnft_type_code_5, bnft_amt_paid_5, wkly_bnft_5, future_use_4, vocational_rehabilitation_evaluation_exp_amt_paid, vocational_rehabilitation_maint_bnft_amt_paid, vocational_rehabilitation_education_exp_amt_paid, vocational_rehabilitation_other_amt_paid, incurred_med_amt_total, total_paid_med_amt, post_inj_wkly_wage_amt, impairment_disability_percentage, impairment_percentage_basis_code, max_med_improvement_date, attorney_or_au_rep_ind, controverted_disputed_case_ind, claimant_lgl_amt_paid, emplyr_lgl_amt_paid, bnft_cvrd_by_lump_sum_settlement_code_1, lump_sum_settlement_amt_paid_1, bnft_cvrd_by_lump_sum_settlement_code_2, lump_sum_settlement_amt_paid_2, bnft_cvrd_by_lump_sum_settlement_code_3, lump_sum_settlement_amt_paid_3, bnft_cvrd_by_lump_sum_settlement_code_4, lump_sum_settlement_amt_paid_4, bnft_cvrd_by_lump_sum_settlement_code_5, lump_sum_settlement_amt_paid_5, bnft_cvrd_by_lump_sum_settlement_code_6, lump_sum_settlement_amt_paid_6, med_extinguishment_ind, return_to_work_date, return_to_work_rate_of_pay_ind, extraordinary_loss_event_claim_ind, future_use_5, prv_carrier_code, future_use_6, prv_pol_num_id, prv_pol_eff_date, prv_reported_to_insr_date, prv_claim_num_id, recovery_reimb_amt, future_use_7, future_use_8)
	SELECT 
	RCRD_TYPE_CODE, 
	CARRIER_CODE, 
	FUTURE_USE_1, 
	POL_NUM_ID, 
	POL_EFF_DATE, 
	VALUATION_LVL_CODE, 
	REPL_RPT_CODE, 
	CLAIM_NUM_ID, 
	FUTURE_USE_2, 
	JURISDICTION_STATE, 
	ACC_STATE, 
	ACC_DATE, 
	REPORTED_TO_INSR_DATE, 
	CLASS_CODE, 
	TYPE_OF_LOSS, 
	TYPE_OF_RECOVERY, 
	TYPE_OF_CLAIM, 
	CLAIMANT_GNDR_CODE, 
	BIRTH_YR, 
	HIRE_YR, 
	FUTURE_USE_3, 
	PREINJURY_AVG_WEEKY_WAGE_AMT, 
	METHOD_OF_DETERMINING_PREINJURY_AVG_WKLY_WAGE_CODE, 
	PART_OF_BODY_CODE, 
	NATURE_OF_INJ, 
	CAUSE_OF_INJ, 
	CLAIM_STATUS_CODE, 
	CLOSING_DATE, 
	INCURRED_INDEMNITY_AMT_TOTAL, 
	BNFT_TYPE_CODE_1, 
	BNFT_AMT_PAID_1, 
	WKLY_BNFT_1, 
	BNFT_TYPE_CODE_2, 
	BNFT_AMT_PAID_2, 
	WKLY_BNFT_2, 
	BNFT_TYPE_CODE_3, 
	BNFT_AMT_PAID_3, 
	WKLY_BNFT_3, 
	BNFT_TYPE_CODE_4, 
	BNFT_AMT_PAID_4, 
	WKLY_BNFT_4, 
	BNFT_TYPE_CODE_5, 
	BNFT_AMT_PAID_5, 
	WKLY_BNFT_5, 
	FUTURE_USE_4, 
	VOCATIONAL_REHABILITATION_EVALUATION_EXP_AMT_PAID, 
	VOCATIONAL_REHABILITATION_MAINT_BNFT_AMT_PAID, 
	VOCATIONAL_REHABILITATION_EDUCATION_EXP_AMT_PAID, 
	VOCATIONAL_REHABILITATION_OTHER_AMT_PAID, 
	INCURRED_MED_AMT_TOTAL, 
	TOTAL_PAID_MED_AMT, 
	POST_INJ_WKLY_WAGE_AMT, 
	IMPAIRMENT_DISABILITY_PERCENTAGE, 
	IMPAIRMENT_PERCENTAGE_BASIS_CODE, 
	MAX_MED_IMPROVEMENT_DATE, 
	ATTORNEY_OR_AU_REP_IND, 
	CONTROVERTED_DISPUTED_CASE_IND, 
	CLAIMANT_LGL_AMT_PAID, 
	EMPLYR_LGL_AMT_PAID, 
	BNFT_CVRD_BY_LUMP_SUM_SETTLEMENT_CODE_1, 
	LUMP_SUM_SETTLEMENT_AMT_PAID_1, 
	BNFT_CVRD_BY_LUMP_SUM_SETTLEMENT_CODE_2, 
	LUMP_SUM_SETTLEMENT_AMT_PAID_2, 
	BNFT_CVRD_BY_LUMP_SUM_SETTLEMENT_CODE_3, 
	LUMP_SUM_SETTLEMENT_AMT_PAID_3, 
	BNFT_CVRD_BY_LUMP_SUM_SETTLEMENT_CODE_4, 
	LUMP_SUM_SETTLEMENT_AMT_PAID_4, 
	BNFT_CVRD_BY_LUMP_SUM_SETTLEMENT_CODE_5, 
	LUMP_SUM_SETTLEMENT_AMT_PAID_5, 
	BNFT_CVRD_BY_LUMP_SUM_SETTLEMENT_CODE_6, 
	LUMP_SUM_SETTLEMENT_AMT_PAID_6, 
	MED_EXTINGUISHMENT_IND, 
	RETURN_TO_WORK_DATE, 
	RETURN_TO_WORK_RATE_OF_PAY_IND, 
	EXTRAORDINARY_LOSS_EVENT_CLAIM_IND, 
	FUTURE_USE_5, 
	PRV_CARRIER_CODE, 
	FUTURE_USE_6, 
	PRV_POL_NUM_ID, 
	PRV_POL_EFF_DATE, 
	PRV_REPORTED_TO_INSR_DATE, 
	PRV_CLAIM_NUM_ID, 
	RECOVERY_REIMB_AMT, 
	FUTURE_USE_7, 
	FUTURE_USE_8
	FROM Filter_All_OR_No_Records
),