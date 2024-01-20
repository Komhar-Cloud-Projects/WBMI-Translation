WITH
LKP_GetTotalPaidMedAmt AS (
	SELECT
	total_paid_med_amt,
	work_claim_ncci_rpt_extract_id
	FROM (
		SELECT 
			total_paid_med_amt,
			work_claim_ncci_rpt_extract_id
		FROM work_claim_ncci_report_extract
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY work_claim_ncci_rpt_extract_id ORDER BY total_paid_med_amt) = 1
),
SQ_work_claim_ncci_report_extract AS (
	SELECT ncci_extract_tab.work_claim_ncci_rpt_extract_id,
	       ncci_extract_tab.edw_claim_occurrence_ak_id,
	       ncci_extract_tab.edw_claim_party_occurrence_ak_id,
	       ncci_extract_tab.edw_pol_ak_id,
	       ncci_extract_tab.jurisdiction_state,
	       ncci_extract_tab.claim_status_code,
	       clmt.ttd_rate,
	       clmt.ppd_rate,
	       clmt.ptd_rate,
	       clmt.dtd_rate,
	       clmt_cov.cause_of_loss,
	       fin_dim.financial_type_code,
	       pay_dim.micro_ecd_draft_num,
	       ( CASE clmt_cov.cause_of_loss
	           WHEN '05' THEN SUM(loss_fact.direct_loss_paid_excluding_recoveries)
	           ELSE 0
	         END )                                                     AS indemnity_payment_amt,
	       SUM(loss_fact.direct_loss_outstanding_excluding_recoveries) AS open_reserve_amt,
	       SUM(loss_fact.direct_loss_paid_excluding_recoveries)        AS benefit_amt,
	       SUM(loss_fact.direct_alae_paid_excluding_recoveries)        AS emeployer_legal_amount_paid,
	       SUM(loss_fact.direct_subrogation_incurred)                  AS Subrogation_Recoveries,
	       ( CASE trans_dim.trans_ctgry_code
	           WHEN 'SI' THEN SUM(loss_fact.direct_other_recovery_loss_incurred)
	           ELSE 0
	         END )                                                     AS Other_Recoveries_For_Second_Injury_Fund,
	       ( CASE trans_dim.trans_ctgry_code
	           WHEN 'EX' THEN SUM(loss_fact.direct_other_recovery_alae_incurred)
	           ELSE 0
	         END )                                                     AS Recovery_Expenses
	FROM   work_claim_ncci_report_extract ncci_extract_tab WITH (NOLOCK)
	       INNER JOIN claimant_dim clmt WITH (NOLOCK)
	         ON ncci_extract_tab.edw_claim_party_occurrence_ak_id = clmt.edw_claim_party_occurrence_ak_id
	       INNER JOIN claim_occurrence_dim co WITH (NOLOCK)
	         ON ncci_extract_tab.edw_claim_occurrence_ak_id = co.edw_claim_occurrence_ak_id
	       INNER JOIN policy_dim pol WITH (NOLOCK)
	         ON ncci_extract_tab.edw_pol_ak_id = pol.edw_pol_ak_id
	       INNER JOIN claim_loss_transaction_fact loss_fact WITH (NOLOCK)
	         ON clmt.claimant_dim_id = loss_fact.claimant_dim_id
	            AND co.claim_occurrence_dim_id = loss_fact.claim_occurrence_dim_id
	            AND pol.pol_dim_id = loss_fact.pol_dim_id
	       INNER JOIN claimant_coverage_dim clmt_cov WITH (NOLOCK)
	         ON loss_fact.claimant_cov_dim_id = clmt_cov.claimant_cov_dim_id
	       INNER JOIN claim_financial_type_dim fin_dim WITH (NOLOCK)
	         ON loss_fact.claim_financial_type_dim_id = fin_dim.claim_financial_type_dim_id
	       LEFT OUTER JOIN claim_transaction_type_dim trans_dim
	         ON loss_fact.claim_trans_type_dim_id = trans_dim.claim_trans_type_dim_id
	       left outer join claim_payment_dim pay_dim
	       on loss_fact.claim_pay_dim_id = pay_dim.claim_pay_dim_id
	WHERE  co.crrnt_snpsht_flag = 1
	       AND pol.crrnt_snpsht_flag = 1
	       AND clmt.crrnt_snpsht_flag = 1
	       AND clmt_cov.crrnt_snpsht_flag = 1
	       AND ncci_extract_tab.transm_status IN ( 'O', 'Y' ) --only those records that are to be transmitted
	       AND ncci_extract_tab.valuation_lvl_code <> '999' --discard anything that is older than 10 years
	GROUP  BY ncci_extract_tab.work_claim_ncci_rpt_extract_id,
	          ncci_extract_tab.edw_claim_occurrence_ak_id,
	          ncci_extract_tab.edw_claim_party_occurrence_ak_id,
	          ncci_extract_tab.edw_pol_ak_id,
	       ncci_extract_tab.jurisdiction_state,
	          ncci_extract_tab.claim_status_code,
	          clmt.dtd_rate,
	          clmt.ptd_rate,
	          clmt.ttd_rate,
	          clmt.ppd_rate,
	          clmt_cov.cause_of_loss,
	          fin_dim.financial_type_code,
	          trans_dim.trans_ctgry_code,
	          pay_dim.micro_ecd_draft_num
),
Exp_Get_Source_Data AS (
	SELECT
	work_claim_ncci_rpt_extract_id,
	edw_claim_occurrence_ak_id,
	edw_claim_party_occurrence_ak_id1,
	edw_pol_ak_id,
	jurisdiction_state,
	claim_status_code,
	ttd_rate,
	ppd_rate,
	ptd_rate,
	dtd_rate,
	cause_of_loss,
	financial_type_code,
	micro_ecd_draft_num,
	direct_loss_paid_excluding_recoveries AS indemnity_payment_amt,
	direct_loss_outstanding_excluding_recoveries AS open_reserve_amt,
	direct_loss_incurred_excluding_recoveries AS benefit_amt,
	direct_alae_paid_excluding_recoveries AS employer_legal_amount_paid,
	direct_subrogation_incurred AS Subrogation_Recoveries,
	direct_other_recovery_loss_incurred AS Other_Recoveries_For_Second_Injury_Fund,
	direct_other_recovery_alae_incurred AS Recovery_Expenses,
	-- *INF*: iif(Recovery_Expenses > (Subrogation_Recoveries+Other_Recoveries_For_Second_Injury_Fund) or Subrogation_Recoveries = 0 or Other_Recoveries_For_Second_Injury_Fund = 0
	-- ,0
	-- ,Subrogation_Recoveries+Other_Recoveries_For_Second_Injury_Fund)
	IFF(
	    Recovery_Expenses > (Subrogation_Recoveries + Other_Recoveries_For_Second_Injury_Fund)
	    or Subrogation_Recoveries = 0
	    or Other_Recoveries_For_Second_Injury_Fund = 0,
	    0,
	    Subrogation_Recoveries + Other_Recoveries_For_Second_Injury_Fund
	) AS Recovery_Reimbursement_Amount
	FROM SQ_work_claim_ncci_report_extract
),
SQ_work_claim_ncci_report_extract1 AS (
	SELECT ncci_extract_tab.work_claim_ncci_rpt_extract_id,
		pay_dim.micro_ecd_draft_num,
		pay_dim.prim_payee_role_code,
	    ctgry_dim.claim_pay_ctgry_type,
	    ctgry_dim.claim_pay_ctgry_lump_sum_ind,
	    payfact.claim_pay_ctgry_amt,
	    (case claim_pay_ctgry_type
	    when 'PD' THEN payfact.claim_pay_ctgry_amt
	    ELSE 0 END) AS TPD_AMT,
	    (CASE claim_pay_ctgry_type
	    WHEN 'PD' THEN payfact.claim_pay_ctgry_end_date_id
	    ELSE 0 END) AS END_DATE,
	    (CASE claim_pay_ctgry_type
	    WHEN 'PD' THEN payfact.claim_pay_ctgry_start_date_id
	    ELSE 0 END) AS start_DATE
	FROM  work_claim_ncci_report_extract ncci_extract_tab WITH (NOLOCK)
	       INNER JOIN claimant_dim clmt WITH (NOLOCK)
	         ON ncci_extract_tab.edw_claim_party_occurrence_ak_id = clmt.edw_claim_party_occurrence_ak_id
	       INNER JOIN claim_occurrence_dim co WITH (NOLOCK)
	         ON ncci_extract_tab.edw_claim_occurrence_ak_id = co.edw_claim_occurrence_ak_id
	       INNER JOIN policy_dim pol WITH (NOLOCK)
	         ON ncci_extract_tab.edw_pol_ak_id = pol.edw_pol_ak_id
	       INNER JOIN claim_payment_category_fact payfact
	         ON co.claim_occurrence_dim_id = payfact.claim_occurrence_dim_id
	         and clmt.claimant_dim_id = payfact.claimant_dim_id
	         and pol.pol_dim_id = payfact.pol_dim_id
	       INNER JOIN claim_payment_category_type_dim ctgry_dim
	         ON payfact.claim_pay_ctgry_type_dim_id = ctgry_dim.claim_pay_ctgry_type_dim_id
	       INNER JOIN claim_payment_dim cp
	         ON payfact.claim_pay_dim_id = cp.claim_pay_dim_id
	       INNER JOIN claim_payment_dim pay_dim
	       on payfact.claim_pay_dim_id = pay_dim.claim_pay_dim_id
	WHERE  co.crrnt_snpsht_flag = 1
	and clmt.crrnt_snpsht_flag = 1
	and pol.crrnt_snpsht_flag = 1
	       AND ctgry_dim.crrnt_snpsht_flag = 1
	       AND cp.crrnt_snpsht_flag = 1
	       AND ncci_extract_tab.transm_status IN ( 'O', 'Y' ) --only those records that are to be transmitted
	       AND ncci_extract_tab.valuation_lvl_code <> '999' --discard anything that is older than 10 years
	ORDER BY ncci_extract_tab.work_claim_ncci_rpt_extract_id,END_DATE,START_DATE,payfact.claim_pay_ctgry_fact_id 
	
	--for benefit type code =11, we are interested in claim_pay_ctgry_type = 'PD' corresponding to the most recent payment date (payfact.claim_pay_ctgry_end_date_id). If end dates are same, then we go by Start_Date. later on this data is sorted and then sent to Aggregator.
),
Exp_Payment_Data AS (
	SELECT
	work_claim_ncci_rpt_extract_id,
	micro_ecd_draft_num,
	prim_payee_role_code,
	claim_pay_ctgry_type,
	claim_pay_ctgry_lump_sum_ind,
	-- *INF*: iif(claim_pay_ctgry_lump_sum_ind = 'N/A'
	-- ,'N'
	-- ,claim_pay_ctgry_lump_sum_ind)
	IFF(claim_pay_ctgry_lump_sum_ind = 'N/A', 'N', claim_pay_ctgry_lump_sum_ind) AS o_claim_pay_ctgry_lump_sum_ind,
	claim_pay_ctgry_amt,
	claim_pay_ctgry_earned_amt AS TPD_AMT,
	claim_pay_ctgry_start_date_id,
	claim_pay_ctgry_end_date_id
	FROM SQ_work_claim_ncci_report_extract1
),
JNR_Combine_Loss_Payment_Data AS (SELECT
	Exp_Get_Source_Data.work_claim_ncci_rpt_extract_id, 
	Exp_Get_Source_Data.edw_claim_occurrence_ak_id, 
	Exp_Get_Source_Data.edw_claim_party_occurrence_ak_id1, 
	Exp_Get_Source_Data.edw_pol_ak_id, 
	Exp_Get_Source_Data.jurisdiction_state, 
	Exp_Get_Source_Data.claim_status_code, 
	Exp_Get_Source_Data.ttd_rate, 
	Exp_Get_Source_Data.ppd_rate, 
	Exp_Get_Source_Data.ptd_rate, 
	Exp_Get_Source_Data.dtd_rate, 
	Exp_Get_Source_Data.cause_of_loss, 
	Exp_Get_Source_Data.financial_type_code, 
	Exp_Get_Source_Data.micro_ecd_draft_num AS micro_ecd_draft_num1, 
	Exp_Get_Source_Data.indemnity_payment_amt, 
	Exp_Get_Source_Data.open_reserve_amt, 
	Exp_Get_Source_Data.benefit_amt, 
	Exp_Get_Source_Data.employer_legal_amount_paid, 
	Exp_Get_Source_Data.Subrogation_Recoveries, 
	Exp_Get_Source_Data.Other_Recoveries_For_Second_Injury_Fund, 
	Exp_Get_Source_Data.Recovery_Expenses, 
	Exp_Get_Source_Data.Recovery_Reimbursement_Amount, 
	Exp_Payment_Data.work_claim_ncci_rpt_extract_id AS work_claim_ncci_rpt_extract_id1, 
	Exp_Payment_Data.micro_ecd_draft_num, 
	Exp_Payment_Data.prim_payee_role_code, 
	Exp_Payment_Data.claim_pay_ctgry_type, 
	Exp_Payment_Data.o_claim_pay_ctgry_lump_sum_ind AS claim_pay_ctgry_lump_sum_ind, 
	Exp_Payment_Data.claim_pay_ctgry_amt, 
	Exp_Payment_Data.TPD_AMT, 
	Exp_Payment_Data.claim_pay_ctgry_start_date_id, 
	Exp_Payment_Data.claim_pay_ctgry_end_date_id
	FROM Exp_Payment_Data
	RIGHT OUTER JOIN Exp_Get_Source_Data
	ON Exp_Get_Source_Data.work_claim_ncci_rpt_extract_id = Exp_Payment_Data.work_claim_ncci_rpt_extract_id AND Exp_Get_Source_Data.micro_ecd_draft_num = Exp_Payment_Data.micro_ecd_draft_num
),
SRT_Sort_Data AS (
	SELECT
	work_claim_ncci_rpt_extract_id, 
	edw_claim_occurrence_ak_id, 
	edw_claim_party_occurrence_ak_id1, 
	edw_pol_ak_id, 
	jurisdiction_state, 
	claim_status_code, 
	ttd_rate, 
	ppd_rate, 
	ptd_rate, 
	dtd_rate, 
	cause_of_loss, 
	financial_type_code, 
	indemnity_payment_amt, 
	open_reserve_amt, 
	benefit_amt, 
	employer_legal_amount_paid, 
	Subrogation_Recoveries, 
	Other_Recoveries_For_Second_Injury_Fund, 
	Recovery_Expenses, 
	Recovery_Reimbursement_Amount, 
	prim_payee_role_code, 
	claim_pay_ctgry_type, 
	claim_pay_ctgry_lump_sum_ind, 
	claim_pay_ctgry_amt, 
	TPD_AMT, 
	claim_pay_ctgry_end_date_id, 
	claim_pay_ctgry_start_date_id
	FROM JNR_Combine_Loss_Payment_Data
	ORDER BY work_claim_ncci_rpt_extract_id ASC, claim_pay_ctgry_end_date_id ASC, claim_pay_ctgry_start_date_id ASC
),
Agg_Calculate_Payments_And_Benefits AS (
	SELECT
	work_claim_ncci_rpt_extract_id,
	edw_claim_occurrence_ak_id,
	edw_claim_party_occurrence_ak_id1,
	edw_pol_ak_id,
	jurisdiction_state,
	claim_status_code,
	ttd_rate,
	ppd_rate,
	ptd_rate,
	dtd_rate,
	cause_of_loss,
	claim_pay_ctgry_type,
	claim_pay_ctgry_lump_sum_ind,
	prim_payee_role_code,
	financial_type_code,
	indemnity_payment_amt AS indemnity_payment_amt_loss_trans,
	-- *INF*: sum(indemnity_payment_amt_loss_trans)
	sum(indemnity_payment_amt_loss_trans) AS o_indemnity_payment_amt_loss_trans,
	claim_pay_ctgry_amt AS indemnity_payment_amt_pay_fact,
	-- *INF*: sum(indemnity_payment_amt_pay_fact)
	sum(indemnity_payment_amt_pay_fact) AS o_indemnity_payment_amt_pay_fact,
	open_reserve_amt,
	-- *INF*: sum(open_reserve_amt)
	sum(open_reserve_amt) AS o_open_reserve_amt,
	claim_pay_ctgry_amt AS benefit_amt,
	employer_legal_amount_paid,
	Subrogation_Recoveries,
	Other_Recoveries_For_Second_Injury_Fund,
	Recovery_Expenses,
	Recovery_Reimbursement_Amount AS Recovery_Reimbursement_Amount1,
	-- *INF*: iif(Recovery_Expenses > (Subrogation_Recoveries+Other_Recoveries_For_Second_Injury_Fund) or Subrogation_Recoveries = 0 or Other_Recoveries_For_Second_Injury_Fund = 0
	-- ,0
	-- ,Subrogation_Recoveries+Other_Recoveries_For_Second_Injury_Fund)
	IFF(
	    Recovery_Expenses > (Subrogation_Recoveries + Other_Recoveries_For_Second_Injury_Fund)
	    or Subrogation_Recoveries = 0
	    or Other_Recoveries_For_Second_Injury_Fund = 0,
	    0,
	    Subrogation_Recoveries + Other_Recoveries_For_Second_Injury_Fund
	) AS Recovery_Reimbursement_Amount,
	claim_pay_ctgry_amt AS claimant_legal_amount_paid,
	claim_pay_ctgry_end_date_id,
	claim_pay_ctgry_start_date_id,
	-- *INF*: sum(claimant_legal_amount_paid,(financial_type_code = 'D' and claim_pay_ctgry_type = 'CL'))
	sum(claimant_legal_amount_paid, (financial_type_code = 'D' and claim_pay_ctgry_type = 'CL')) AS o_claimanat_legal_amount_paid,
	-- *INF*: sum(employer_legal_amount_paid,(prim_payee_role_code = 'WDAT' and (claim_pay_ctgry_type = 'LF' OR claim_pay_ctgry_type = 'LS')))
	sum(employer_legal_amount_paid, (prim_payee_role_code = 'WDAT' and (claim_pay_ctgry_type = 'LF' OR claim_pay_ctgry_type = 'LS'))) AS o_employer_legal_amount_paid,
	-- *INF*: count(work_claim_ncci_rpt_extract_id,prim_payee_role_code = 'WPLA')
	count(work_claim_ncci_rpt_extract_id, prim_payee_role_code = 'WPLA') AS Attorny_Authorized_Representative_Ind,
	-- *INF*: sum(benefit_amt,((claim_pay_ctgry_type = 'DT' or claim_pay_ctgry_type = 'FN') and claim_pay_ctgry_lump_sum_ind = 'N'))
	sum(benefit_amt, ((claim_pay_ctgry_type = 'DT' or claim_pay_ctgry_type = 'FN') and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_DT_FN,
	-- *INF*: sum(benefit_amt,((claim_pay_ctgry_type = 'PT' or claim_pay_ctgry_type = 'PS') and claim_pay_ctgry_lump_sum_ind = 'N') )
	sum(benefit_amt, ((claim_pay_ctgry_type = 'PT' or claim_pay_ctgry_type = 'PS') and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'PP' and claim_pay_ctgry_lump_sum_ind = 'N') )
	sum(benefit_amt, (claim_pay_ctgry_type = 'PP' and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_PP,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'PB' and claim_pay_ctgry_lump_sum_ind = 'N') )
	sum(benefit_amt, (claim_pay_ctgry_type = 'PB' and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_PB,
	-- *INF*: sum(benefit_amt,((claim_pay_ctgry_type = 'TD' 
	-- or claim_pay_ctgry_type = 'PL'
	-- or claim_pay_ctgry_type = 'TC')
	-- --or claim_pay_ctgry_type = 'VC'
	-- --or claim_pay_ctgry_type = 'VD'
	-- --or claim_pay_ctgry_type = 'VE'
	-- --or claim_pay_ctgry_type = 'VR'
	-- --or claim_pay_ctgry_type = 'VT')
	-- and claim_pay_ctgry_lump_sum_ind = 'N'
	-- ))
	-- 
	-- --VC, VD,VE,VR,VT - all vocational codes taken out from the logic as per NCCI edit 0092-03.
	-- 
	-- 
	sum(benefit_amt, ((claim_pay_ctgry_type = 'TD' or claim_pay_ctgry_type = 'PL' or claim_pay_ctgry_type = 'TC') and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'MP' and claim_pay_ctgry_lump_sum_ind = 'N') )
	sum(benefit_amt, (claim_pay_ctgry_type = 'MP' and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_MP,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'DF' and claim_pay_ctgry_lump_sum_ind = 'N'))
	sum(benefit_amt, (claim_pay_ctgry_type = 'DF' and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_DF,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'PD' and claim_pay_ctgry_lump_sum_ind = 'N'))
	sum(benefit_amt, (claim_pay_ctgry_type = 'PD' and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_PD,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'EP' and claim_pay_ctgry_lump_sum_ind = 'N'))
	sum(benefit_amt, (claim_pay_ctgry_type = 'EP' and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_EP,
	-- *INF*: sum(benefit_amt,((claim_pay_ctgry_type = 'AL' OR claim_pay_ctgry_type = 'UI') and claim_pay_ctgry_lump_sum_ind = 'N'))
	sum(benefit_amt, ((claim_pay_ctgry_type = 'AL' OR claim_pay_ctgry_type = 'UI') and claim_pay_ctgry_lump_sum_ind = 'N')) AS Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI,
	-- *INF*: sum(benefit_amt,((claim_pay_ctgry_type = 'DT' or claim_pay_ctgry_type = 'FN') and claim_pay_ctgry_lump_sum_ind = 'Y'))
	-- 
	-- 
	sum(benefit_amt, ((claim_pay_ctgry_type = 'DT' or claim_pay_ctgry_type = 'FN') and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_DT_FN,
	-- *INF*: sum(benefit_amt,((claim_pay_ctgry_type = 'PT'  or claim_pay_ctgry_type = 'PS') and claim_pay_ctgry_lump_sum_ind = 'Y'))
	sum(benefit_amt, ((claim_pay_ctgry_type = 'PT' or claim_pay_ctgry_type = 'PS') and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_PT_PS,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'PP' and claim_pay_ctgry_lump_sum_ind = 'Y'))
	sum(benefit_amt, (claim_pay_ctgry_type = 'PP' and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_PP,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'PB' and claim_pay_ctgry_lump_sum_ind = 'Y'))
	sum(benefit_amt, (claim_pay_ctgry_type = 'PB' and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_PB,
	-- *INF*: sum(benefit_amt,((claim_pay_ctgry_type = 'TD' 
	-- or claim_pay_ctgry_type = 'PL'
	-- or claim_pay_ctgry_type = 'TC')
	-- --or claim_pay_ctgry_type = 'VC'
	-- --or claim_pay_ctgry_type = 'VD'
	-- --or claim_pay_ctgry_type = 'VE'
	-- --or claim_pay_ctgry_type = 'VR'
	-- --or claim_pay_ctgry_type = 'VT')
	-- and claim_pay_ctgry_lump_sum_ind = 'Y'))
	-- 
	-- 
	-- --Vocational amounts are excluded as part of DDRR-60
	sum(benefit_amt, ((claim_pay_ctgry_type = 'TD' or claim_pay_ctgry_type = 'PL' or claim_pay_ctgry_type = 'TC') and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_TD_PL_TC,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'MP' and claim_pay_ctgry_lump_sum_ind = 'Y'))
	sum(benefit_amt, (claim_pay_ctgry_type = 'MP' and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_MP,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'DF' and claim_pay_ctgry_lump_sum_ind = 'Y'))
	sum(benefit_amt, (claim_pay_ctgry_type = 'DF' and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_DF,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'PD' and claim_pay_ctgry_lump_sum_ind = 'Y'))
	sum(benefit_amt, (claim_pay_ctgry_type = 'PD' and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_PD,
	-- *INF*: sum(benefit_amt,(claim_pay_ctgry_type = 'EP' and claim_pay_ctgry_lump_sum_ind = 'Y'))
	sum(benefit_amt, (claim_pay_ctgry_type = 'EP' and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_EP,
	-- *INF*: sum(benefit_amt,((claim_pay_ctgry_type = 'AL' OR claim_pay_ctgry_type = 'UI') and claim_pay_ctgry_lump_sum_ind = 'Y'))
	sum(benefit_amt, ((claim_pay_ctgry_type = 'AL' OR claim_pay_ctgry_type = 'UI') and claim_pay_ctgry_lump_sum_ind = 'Y')) AS Benefit_Covered_By_Lump_Sump_AL_UI,
	-- *INF*: sum(indemnity_payment_amt_pay_fact,(cause_of_loss = '05' and claim_pay_ctgry_type = 'VE'))
	-- 
	-- 
	sum(indemnity_payment_amt_pay_fact, (cause_of_loss = '05' and claim_pay_ctgry_type = 'VE')) AS Vocational_Rehab_Evaluation_Expense_Amount_Paid,
	-- *INF*: sum(indemnity_payment_amt_pay_fact,(cause_of_loss = '05' and claim_pay_ctgry_type = 'VM'))
	-- 
	sum(indemnity_payment_amt_pay_fact, (cause_of_loss = '05' and claim_pay_ctgry_type = 'VM')) AS Vocational_Rehab_Maintenance_Benefit_Amount_Paid,
	-- *INF*: sum(indemnity_payment_amt_pay_fact,(cause_of_loss = '05' and claim_pay_ctgry_type = 'VD' ))
	-- 
	-- 
	sum(indemnity_payment_amt_pay_fact, (cause_of_loss = '05' and claim_pay_ctgry_type = 'VD')) AS Vocational_Rehab_Education_Expense_Amount_Paid,
	-- *INF*: sum(indemnity_payment_amt_pay_fact,(cause_of_loss = '05' and (claim_pay_ctgry_type = 'VC' or claim_pay_ctgry_type = 'VT' )))
	-- 
	-- 
	sum(indemnity_payment_amt_pay_fact, (cause_of_loss = '05' and (claim_pay_ctgry_type = 'VC' or claim_pay_ctgry_type = 'VT'))) AS Vocational_Rehab_Other_Amount_Paid,
	TPD_AMT
	FROM SRT_Sort_Data
	GROUP BY work_claim_ncci_rpt_extract_id, edw_claim_occurrence_ak_id, edw_claim_party_occurrence_ak_id1, edw_pol_ak_id
),
EXP_Calculate_Payments_And_Benefits AS (
	SELECT
	work_claim_ncci_rpt_extract_id,
	edw_claim_occurrence_ak_id,
	edw_claim_party_occurrence_ak_id1,
	ttd_rate,
	ppd_rate,
	ptd_rate,
	jurisdiction_state,
	dtd_rate,
	edw_pol_ak_id,
	claim_status_code,
	o_indemnity_payment_amt_loss_trans AS indemnity_payment_amt_loss_trans,
	o_indemnity_payment_amt_pay_fact AS indemnity_payment_amt_pay_fact,
	-- *INF*: lpad(to_char(to_integer(indemnity_payment_amt_pay_fact,false)),9,'0')
	lpad(to_char(CAST(indemnity_payment_amt_pay_fact AS INTEGER)), 9, '0') AS o_indemnity_payment_amt,
	o_open_reserve_amt AS open_reserve_amt,
	-- *INF*: lpad(to_char(to_integer(open_reserve_amt,false)),9,'0')
	lpad(to_char(CAST(open_reserve_amt AS INTEGER)), 9, '0') AS o_open_reserve_amt,
	Recovery_Reimbursement_Amount,
	-- *INF*: iif(isnull(Recovery_Reimbursement_Amount) or Recovery_Reimbursement_Amount = 0 ,'000000000' 
	-- ,lpad(to_char(to_integer(claimanat_legal_amount_paid,false)),9,'0'))
	IFF(
	    Recovery_Reimbursement_Amount IS NULL or Recovery_Reimbursement_Amount = 0, '000000000',
	    lpad(to_char(CAST(claimanat_legal_amount_paid AS INTEGER)), 9, '0')
	) AS o_Recovery_Reimbursement_Amount,
	o_claimanat_legal_amount_paid AS claimanat_legal_amount_paid,
	-- *INF*: iif(isnull(claimanat_legal_amount_paid)
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(claimanat_legal_amount_paid,false)),9,'0'))
	IFF(
	    claimanat_legal_amount_paid IS NULL, '000000000',
	    lpad(to_char(CAST(claimanat_legal_amount_paid AS INTEGER)), 9, '0')
	) AS o_claimant_legal_amount_paid,
	o_employer_legal_amount_paid AS employer_legal_amount_paid,
	-- *INF*: iif(isnull(employer_legal_amount_paid)
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(employer_legal_amount_paid,false)),9,'0')
	-- )
	IFF(
	    employer_legal_amount_paid IS NULL, '000000000',
	    lpad(to_char(CAST(employer_legal_amount_paid AS INTEGER)), 9, '0')
	) AS o_employer_legal_amount_paid,
	Attorny_Authorized_Representative_Ind,
	-- *INF*: iif(Attorny_Authorized_Representative_Ind >=1, 'Y','N')
	IFF(Attorny_Authorized_Representative_Ind >= 1, 'Y', 'N') AS o_Attorny_Authorized_Representative_Ind,
	Benefit_Amount_Paid_Excluding_Lump_Sump_DT_FN,
	Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS,
	Benefit_Amount_Paid_Excluding_Lump_Sump_PP,
	Benefit_Amount_Paid_Excluding_Lump_Sump_PB,
	Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC,
	Benefit_Amount_Paid_Excluding_Lump_Sump_MP,
	Benefit_Amount_Paid_Excluding_Lump_Sump_DF,
	Benefit_Amount_Paid_Excluding_Lump_Sump_PD,
	Benefit_Amount_Paid_Excluding_Lump_Sump_EP,
	Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI,
	-- *INF*: decode(true,
	-- claim_status_code = '5','00', --if it is a medical claim, then 00
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DT_FN>0  AND indemnity_payment_amt_pay_fact > 0,'01',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS>0 AND indemnity_payment_amt_pay_fact > 0,'02',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PP>0  AND  indemnity_payment_amt_pay_fact > 0,'03',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PB>0 AND  indemnity_payment_amt_pay_fact > 0,'04',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 AND  indemnity_payment_amt_pay_fact > 0 ,'05',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 AND  indemnity_payment_amt_pay_fact > 0 ,'06',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 AND  indemnity_payment_amt_pay_fact > 0 ,'09',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 AND  indemnity_payment_amt_pay_fact > 0,'11',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 AND  indemnity_payment_amt_pay_fact > 0,'12',
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 AND  indemnity_payment_amt > 0,'49', - removed from the logic as per meetings with Andrea and Mike Doyle
	-- (open_reserve_amt>0 and indemnity_payment_amt_loss_trans = 0),'05', --Anticipation of the benefit type
	-- '00')
	-- 
	decode(
	    true,
	    claim_status_code = '5', '00',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_DT_FN > 0 AND indemnity_payment_amt_pay_fact > 0, '01',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS > 0 AND indemnity_payment_amt_pay_fact > 0, '02',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_PP > 0 AND indemnity_payment_amt_pay_fact > 0, '03',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_PB > 0 AND indemnity_payment_amt_pay_fact > 0, '04',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0 AND indemnity_payment_amt_pay_fact > 0, '05',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0 AND indemnity_payment_amt_pay_fact > 0, '06',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0 AND indemnity_payment_amt_pay_fact > 0, '09',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0 AND indemnity_payment_amt_pay_fact > 0, '11',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0 AND indemnity_payment_amt_pay_fact > 0, '12',
	    (open_reserve_amt > 0 and indemnity_payment_amt_loss_trans = 0), '05',
	    '00'
	) AS v_Benefit_Type_Code1,
	v_Benefit_Type_Code1 AS o_Benefit_Type_Code1,
	-- *INF*: decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS>0 and v_Benefit_Type_Code1 < '02'  AND v_Benefit_Type_Code1 > '00' AND  indemnity_payment_amt_pay_fact > 0,'02',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PP>0 and v_Benefit_Type_Code1 < '03' AND v_Benefit_Type_Code1 > '00' AND  indemnity_payment_amt_pay_fact > 0 ,'03',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PB>0 and v_Benefit_Type_Code1 < '04' AND v_Benefit_Type_Code1 > '00' AND  indemnity_payment_amt_pay_fact > 0 ,'04',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code1 < '05'  AND v_Benefit_Type_Code1 > '00' AND  indemnity_payment_amt_pay_fact > 0 , '05',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 and v_Benefit_Type_Code1 < '06' AND v_Benefit_Type_Code1 > '00' AND  indemnity_payment_amt_pay_fact > 0 ,'06',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 and v_Benefit_Type_Code1 < '07' AND v_Benefit_Type_Code1 > '00' AND  indemnity_payment_amt_pay_fact > 0 ,'09',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 and v_Benefit_Type_Code1 < '11' AND v_Benefit_Type_Code1 > '00' AND  indemnity_payment_amt_pay_fact > 0 ,'11',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 and v_Benefit_Type_Code1 < '12' AND v_Benefit_Type_Code1 > '00' AND  indemnity_payment_amt_pay_fact > 0 , '12',
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code1 < '49' AND v_Benefit_Type_Code1 > '00' AND  indemnity_payment_amt > 0 ,'49', - removed from the logic as per meetings with Andrea and Mike Doyle
	-- '00')
	decode(
	    true,
	    claim_status_code = '5', '00',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS > 0 and v_Benefit_Type_Code1 < '02' AND v_Benefit_Type_Code1 > '00' AND indemnity_payment_amt_pay_fact > 0, '02',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_PP > 0 and v_Benefit_Type_Code1 < '03' AND v_Benefit_Type_Code1 > '00' AND indemnity_payment_amt_pay_fact > 0, '03',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_PB > 0 and v_Benefit_Type_Code1 < '04' AND v_Benefit_Type_Code1 > '00' AND indemnity_payment_amt_pay_fact > 0, '04',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code1 < '05' AND v_Benefit_Type_Code1 > '00' AND indemnity_payment_amt_pay_fact > 0, '05',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0 and v_Benefit_Type_Code1 < '06' AND v_Benefit_Type_Code1 > '00' AND indemnity_payment_amt_pay_fact > 0, '06',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0 and v_Benefit_Type_Code1 < '07' AND v_Benefit_Type_Code1 > '00' AND indemnity_payment_amt_pay_fact > 0, '09',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0 and v_Benefit_Type_Code1 < '11' AND v_Benefit_Type_Code1 > '00' AND indemnity_payment_amt_pay_fact > 0, '11',
	    Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0 and v_Benefit_Type_Code1 < '12' AND v_Benefit_Type_Code1 > '00' AND indemnity_payment_amt_pay_fact > 0, '12',
	    '00'
	) AS v_Benefit_Type_Code2,
	v_Benefit_Type_Code2 AS o_Benefit_Type_Code2,
	-- *INF*: iif(indemnity_payment_amt_pay_fact > 0,
	-- decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PP>0 and v_Benefit_Type_Code2 < '03' and v_Benefit_Type_Code2 > '00','03',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PB>0 and v_Benefit_Type_Code2 < '04' and v_Benefit_Type_Code2 > '00','04',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code2 < '05' and v_Benefit_Type_Code2 > '00', '05',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 and v_Benefit_Type_Code2 < '06' and v_Benefit_Type_Code2 > '00','06',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 and v_Benefit_Type_Code2 < '07' and v_Benefit_Type_Code2 > '00','09',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 and v_Benefit_Type_Code2 < '11' and v_Benefit_Type_Code2 > '00','11',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 and v_Benefit_Type_Code2 < '12' and v_Benefit_Type_Code2 > '00', '12',
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code2 < '49' and v_Benefit_Type_Code2 > '00','49', - removed from the logic as per meetings with Andrea and Mike Doyle
	-- '00')
	-- ,'00')
	IFF(
	    indemnity_payment_amt_pay_fact > 0,
	    decode(
	        true,
	        claim_status_code = '5', '00',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_PP > 0
	    and v_Benefit_Type_Code2 < '03'
	    and v_Benefit_Type_Code2 > '00', '03',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_PB > 0
	    and v_Benefit_Type_Code2 < '04'
	    and v_Benefit_Type_Code2 > '00', '04',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0
	    and v_Benefit_Type_Code2 < '05'
	    and v_Benefit_Type_Code2 > '00', '05',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0
	    and v_Benefit_Type_Code2 < '06'
	    and v_Benefit_Type_Code2 > '00', '06',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0
	    and v_Benefit_Type_Code2 < '07'
	    and v_Benefit_Type_Code2 > '00', '09',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0
	    and v_Benefit_Type_Code2 < '11'
	    and v_Benefit_Type_Code2 > '00', '11',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0
	    and v_Benefit_Type_Code2 < '12'
	    and v_Benefit_Type_Code2 > '00', '12',
	        '00'
	    ),
	    '00'
	) AS v_Benefit_Type_Code3,
	v_Benefit_Type_Code3 AS o_Benefit_Type_Code3,
	-- *INF*: iif(indemnity_payment_amt_pay_fact > 0,
	-- decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PB>0 and v_Benefit_Type_Code3 < '04' and v_Benefit_Type_Code3 > '00' ,'04',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code3 < '05' and v_Benefit_Type_Code3 > '00', '05',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 and v_Benefit_Type_Code3 < '06' and v_Benefit_Type_Code3 > '00','06',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 and v_Benefit_Type_Code3 < '07' and v_Benefit_Type_Code3 > '00','09',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 and v_Benefit_Type_Code3 < '11'  and v_Benefit_Type_Code3 > '00','11',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 and v_Benefit_Type_Code3 < '12' and v_Benefit_Type_Code3 > '00', '12',
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code3 < '49'  and v_Benefit_Type_Code3 > '00','49', - removed from the logic as per meetings with Andrea and Mike Doyle
	-- '00')
	-- ,'00')
	IFF(
	    indemnity_payment_amt_pay_fact > 0,
	    decode(
	        true,
	        claim_status_code = '5', '00',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_PB > 0
	    and v_Benefit_Type_Code3 < '04'
	    and v_Benefit_Type_Code3 > '00', '04',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0
	    and v_Benefit_Type_Code3 < '05'
	    and v_Benefit_Type_Code3 > '00', '05',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0
	    and v_Benefit_Type_Code3 < '06'
	    and v_Benefit_Type_Code3 > '00', '06',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0
	    and v_Benefit_Type_Code3 < '07'
	    and v_Benefit_Type_Code3 > '00', '09',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0
	    and v_Benefit_Type_Code3 < '11'
	    and v_Benefit_Type_Code3 > '00', '11',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0
	    and v_Benefit_Type_Code3 < '12'
	    and v_Benefit_Type_Code3 > '00', '12',
	        '00'
	    ),
	    '00'
	) AS v_Benefit_Type_Code4,
	v_Benefit_Type_Code4 AS o_Benefit_Type_Code4,
	-- *INF*: iif(indemnity_payment_amt_pay_fact > 0,
	-- decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code4 < '05' and v_Benefit_Type_Code4 > '00', '05',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 and v_Benefit_Type_Code4 < '06' and v_Benefit_Type_Code4 > '00','06',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 and v_Benefit_Type_Code4< '07' and v_Benefit_Type_Code4 > '00','09',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 and v_Benefit_Type_Code4 < '11'  and v_Benefit_Type_Code4 > '00','11',
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 and v_Benefit_Type_Code4 < '12' and v_Benefit_Type_Code4 > '00', '12',
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code4 < '49' and v_Benefit_Type_Code4 > '00','49', - removed from the logic as per meetings with Andrea and Mike Doyle
	-- '00')
	-- ,'00')
	IFF(
	    indemnity_payment_amt_pay_fact > 0,
	    decode(
	        true,
	        claim_status_code = '5', '00',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0
	    and v_Benefit_Type_Code4 < '05'
	    and v_Benefit_Type_Code4 > '00', '05',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0
	    and v_Benefit_Type_Code4 < '06'
	    and v_Benefit_Type_Code4 > '00', '06',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0
	    and v_Benefit_Type_Code4 < '07'
	    and v_Benefit_Type_Code4 > '00', '09',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0
	    and v_Benefit_Type_Code4 < '11'
	    and v_Benefit_Type_Code4 > '00', '11',
	        Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0
	    and v_Benefit_Type_Code4 < '12'
	    and v_Benefit_Type_Code4 > '00', '12',
	        '00'
	    ),
	    '00'
	) AS v_Benefit_Type_Code5,
	v_Benefit_Type_Code5 AS o_Benefit_Type_Code5,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DT_FN>0 AND indemnity_payment_amt_pay_fact > 0,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_DT_FN,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS>0 AND indemnity_payment_amt_pay_fact > 0,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PP>0 AND indemnity_payment_amt_pay_fact > 0,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PP,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PB>0 AND indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PB,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 AND indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 AND indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_MP,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 AND indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_DF,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 AND indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PD,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 AND indemnity_payment_amt_pay_fact > 0,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_EP,false),
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 AND indemnity_payment_amt > 0,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI,false), - removed from the logic as per meetings with Andrea and Mike Doyle
	-- 00000000)),9,'0')
	-- 
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            Benefit_Amount_Paid_Excluding_Lump_Sump_DT_FN > 0 AND indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_DT_FN AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS > 0 AND indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_PP > 0 AND indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PP AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_PB > 0 AND indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PB AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0 AND indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0 AND indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_MP AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0 AND indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_DF AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0 AND indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PD AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0 AND indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_EP AS INTEGER),
	            00000000
	        )), 9, '0') AS Benefit_Amount_Paid1,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS>0 and v_Benefit_Type_Code1 < '02' and v_Benefit_Type_Code1 > '00' and indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PP>0 and v_Benefit_Type_Code1 < '03' and v_Benefit_Type_Code1 > '00' and  indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PP,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PB>0 and v_Benefit_Type_Code1 < '04' and v_Benefit_Type_Code1 > '00' and  indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PB,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code1 < '05' and v_Benefit_Type_Code1 > '00'  and  indemnity_payment_amt_pay_fact > 0  ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 and v_Benefit_Type_Code1 < '06' and v_Benefit_Type_Code1 > '00'  and  indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_MP,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 and v_Benefit_Type_Code1 < '07' and v_Benefit_Type_Code1 > '00' and  indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_DF,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 and v_Benefit_Type_Code1 < '11' and v_Benefit_Type_Code1 > '00' and  indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PD,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 and v_Benefit_Type_Code1 < '12' and v_Benefit_Type_Code1 > '00' and  indemnity_payment_amt_pay_fact > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_EP,false),
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code1 < '49'  and v_Benefit_Type_Code1 > '00'  and  indemnity_payment_amt > 0 ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI,false), - removed from the logic as per meetings with Andrea and Mike Doyle
	-- 0)),9,'0')
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS > 0 and v_Benefit_Type_Code1 < '02' and v_Benefit_Type_Code1 > '00' and indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PT_PS AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_PP > 0 and v_Benefit_Type_Code1 < '03' and v_Benefit_Type_Code1 > '00' and indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PP AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_PB > 0 and v_Benefit_Type_Code1 < '04' and v_Benefit_Type_Code1 > '00' and indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PB AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code1 < '05' and v_Benefit_Type_Code1 > '00' and indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0 and v_Benefit_Type_Code1 < '06' and v_Benefit_Type_Code1 > '00' and indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_MP AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0 and v_Benefit_Type_Code1 < '07' and v_Benefit_Type_Code1 > '00' and indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_DF AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0 and v_Benefit_Type_Code1 < '11' and v_Benefit_Type_Code1 > '00' and indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PD AS INTEGER),
	            Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0 and v_Benefit_Type_Code1 < '12' and v_Benefit_Type_Code1 > '00' and indemnity_payment_amt_pay_fact > 0, CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_EP AS INTEGER),
	            0
	        )), 9, '0') AS Benefit_Amount_Paid2,
	-- *INF*: iif(indemnity_payment_amt_pay_fact > 0 ,
	-- lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PP>0 and v_Benefit_Type_Code2 < '03' and v_Benefit_Type_Code2 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PP,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PB>0 and v_Benefit_Type_Code2 < '04' and v_Benefit_Type_Code2 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PB,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code2 < '05' and v_Benefit_Type_Code2 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 and v_Benefit_Type_Code2 < '06' and v_Benefit_Type_Code2 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_MP,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 and v_Benefit_Type_Code2 < '07' and v_Benefit_Type_Code2 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_DF,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 and v_Benefit_Type_Code2 < '11' and v_Benefit_Type_Code2 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PD,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 and v_Benefit_Type_Code2 < '12' and v_Benefit_Type_Code2 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_EP,false),
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code2 < '49' and v_Benefit_Type_Code2 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI,false), - removed from the logic as per meetings with Andrea and Mike Doyle
	-- 0)),9,'0')
	-- ,'000000000')
	IFF(
	    indemnity_payment_amt_pay_fact > 0,
	    lpad(to_char(decode(
	                true,
	                claim_status_code = '5', 0,
	                Benefit_Amount_Paid_Excluding_Lump_Sump_PP > 0
	    and v_Benefit_Type_Code2 < '03'
	    and v_Benefit_Type_Code2 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PP AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_PB > 0
	    and v_Benefit_Type_Code2 < '04'
	    and v_Benefit_Type_Code2 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PB AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0
	    and v_Benefit_Type_Code2 < '05'
	    and v_Benefit_Type_Code2 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0
	    and v_Benefit_Type_Code2 < '06'
	    and v_Benefit_Type_Code2 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_MP AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0
	    and v_Benefit_Type_Code2 < '07'
	    and v_Benefit_Type_Code2 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_DF AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0
	    and v_Benefit_Type_Code2 < '11'
	    and v_Benefit_Type_Code2 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PD AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0
	    and v_Benefit_Type_Code2 < '12'
	    and v_Benefit_Type_Code2 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_EP AS INTEGER),
	                0
	            )), 9, '0'),
	    '000000000'
	) AS Benefit_Amount_Paid3,
	-- *INF*: iif(indemnity_payment_amt_pay_fact > 0 ,
	-- lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PB>0 and v_Benefit_Type_Code3 < '04' and v_Benefit_Type_Code3 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PB,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code3 < '05' and v_Benefit_Type_Code3 > '00' ,to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 and v_Benefit_Type_Code3 < '06' and v_Benefit_Type_Code3 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_MP,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 and v_Benefit_Type_Code3 < '07' and v_Benefit_Type_Code3 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_DF,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 and v_Benefit_Type_Code3 < '11' and v_Benefit_Type_Code3 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PD,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 and v_Benefit_Type_Code3 < '12' and v_Benefit_Type_Code3 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_EP,false),
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code3 < '49' and v_Benefit_Type_Code3 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI,false), - removed from the logic as per meetings with Andrea and Mike Doyle
	-- 0)),9,'0')
	-- ,'000000000')
	IFF(
	    indemnity_payment_amt_pay_fact > 0,
	    lpad(to_char(decode(
	                true,
	                claim_status_code = '5', 0,
	                Benefit_Amount_Paid_Excluding_Lump_Sump_PB > 0
	    and v_Benefit_Type_Code3 < '04'
	    and v_Benefit_Type_Code3 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PB AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0
	    and v_Benefit_Type_Code3 < '05'
	    and v_Benefit_Type_Code3 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0
	    and v_Benefit_Type_Code3 < '06'
	    and v_Benefit_Type_Code3 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_MP AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0
	    and v_Benefit_Type_Code3 < '07'
	    and v_Benefit_Type_Code3 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_DF AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0
	    and v_Benefit_Type_Code3 < '11'
	    and v_Benefit_Type_Code3 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PD AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0
	    and v_Benefit_Type_Code3 < '12'
	    and v_Benefit_Type_Code3 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_EP AS INTEGER),
	                0
	            )), 9, '0'),
	    '000000000'
	) AS Benefit_Amount_Paid4,
	-- *INF*: iif(indemnity_payment_amt_pay_fact > 0,
	-- lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code4 < '05' and v_Benefit_Type_Code4 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_MP>0 and v_Benefit_Type_Code4 < '06' and v_Benefit_Type_Code4 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_MP,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_DF>0 and v_Benefit_Type_Code4 < '07' and v_Benefit_Type_Code4 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_DF,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_PD>0 and v_Benefit_Type_Code4 < '11' and v_Benefit_Type_Code4 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_PD,false),
	-- Benefit_Amount_Paid_Excluding_Lump_Sump_EP>0 and v_Benefit_Type_Code4 < '12' and v_Benefit_Type_Code4 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_EP,false),
	-- --Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code4 < '49' and v_Benefit_Type_Code4 > '00',to_integer(Benefit_Amount_Paid_Excluding_Lump_Sump_AL_UI,false), - removed from the logic as per meetings with Andrea and Mike Doyle
	-- 0)),9,'0')
	-- ,'000000000')
	IFF(
	    indemnity_payment_amt_pay_fact > 0,
	    lpad(to_char(decode(
	                true,
	                claim_status_code = '5', 0,
	                Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC > 0
	    and v_Benefit_Type_Code4 < '05'
	    and v_Benefit_Type_Code4 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_TD_PL_TC AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_MP > 0
	    and v_Benefit_Type_Code4 < '06'
	    and v_Benefit_Type_Code4 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_MP AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_DF > 0
	    and v_Benefit_Type_Code4 < '07'
	    and v_Benefit_Type_Code4 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_DF AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_PD > 0
	    and v_Benefit_Type_Code4 < '11'
	    and v_Benefit_Type_Code4 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_PD AS INTEGER),
	                Benefit_Amount_Paid_Excluding_Lump_Sump_EP > 0
	    and v_Benefit_Type_Code4 < '12'
	    and v_Benefit_Type_Code4 > '00', CAST(Benefit_Amount_Paid_Excluding_Lump_Sump_EP AS INTEGER),
	                0
	            )), 9, '0'),
	    '000000000'
	) AS Benefit_Amount_Paid5,
	TPD_AMT,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- indemnity_payment_amt_loss_trans = 0 and open_reserve_amt >0,0,
	-- v_Benefit_Type_Code1 = '02',to_integer(ptd_rate,false),
	-- v_Benefit_Type_Code1 = '03' or v_Benefit_Type_Code1 = '04' ,to_integer(ppd_rate,false),
	-- v_Benefit_Type_Code1 = '05',to_integer(ttd_rate,false),
	-- v_Benefit_Type_Code1 = '09' ,to_integer(ppd_rate,false),
	-- v_Benefit_Type_Code1 = '11',to_integer(TPD_AMT,false),
	-- 0)),6,'0')
	-- 
	-- 
	-- 
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            indemnity_payment_amt_loss_trans = 0 and open_reserve_amt > 0, 0,
	            v_Benefit_Type_Code1 = '02', CAST(ptd_rate AS INTEGER),
	            v_Benefit_Type_Code1 = '03' or v_Benefit_Type_Code1 = '04', CAST(ppd_rate AS INTEGER),
	            v_Benefit_Type_Code1 = '05', CAST(ttd_rate AS INTEGER),
	            v_Benefit_Type_Code1 = '09', CAST(ppd_rate AS INTEGER),
	            v_Benefit_Type_Code1 = '11', CAST(TPD_AMT AS INTEGER),
	            0
	        )), 6, '0') AS Weekly_Benefit1,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- indemnity_payment_amt_loss_trans = 0 and open_reserve_amt >0,0,
	-- --v_Benefit_Type_Code2 = '01',to_integer(dtd_rate,false),
	-- v_Benefit_Type_Code2 = '02',to_integer(ptd_rate,false),
	-- v_Benefit_Type_Code2 = '03' or v_Benefit_Type_Code2 = '04' ,to_integer(ppd_rate,false),
	-- v_Benefit_Type_Code2 = '05' ,to_integer(ttd_rate,false),
	-- v_Benefit_Type_Code2 = '09',to_integer(ppd_rate,false),
	-- v_Benefit_Type_Code2 = '11',to_integer(TPD_AMT,false),
	-- 0)),6,'0')
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            indemnity_payment_amt_loss_trans = 0 and open_reserve_amt > 0, 0,
	            v_Benefit_Type_Code2 = '02', CAST(ptd_rate AS INTEGER),
	            v_Benefit_Type_Code2 = '03' or v_Benefit_Type_Code2 = '04', CAST(ppd_rate AS INTEGER),
	            v_Benefit_Type_Code2 = '05', CAST(ttd_rate AS INTEGER),
	            v_Benefit_Type_Code2 = '09', CAST(ppd_rate AS INTEGER),
	            v_Benefit_Type_Code2 = '11', CAST(TPD_AMT AS INTEGER),
	            0
	        )), 6, '0') AS Weekly_Benefit2,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- indemnity_payment_amt_loss_trans = 0 and open_reserve_amt >0,0,
	-- --v_Benefit_Type_Code3 = '01',to_integer(dtd_rate,false),
	-- v_Benefit_Type_Code3 = '02',to_integer(ptd_rate,false),
	-- v_Benefit_Type_Code3 = '03' or v_Benefit_Type_Code3 = '04' ,to_integer(ppd_rate,false),
	-- v_Benefit_Type_Code3 = '05' ,to_integer(ttd_rate,false),
	-- v_Benefit_Type_Code3 = '09',to_integer(ppd_rate,false),
	-- v_Benefit_Type_Code3 = '11',to_integer(TPD_AMT,false),
	-- 0)),6,'0')
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            indemnity_payment_amt_loss_trans = 0 and open_reserve_amt > 0, 0,
	            v_Benefit_Type_Code3 = '02', CAST(ptd_rate AS INTEGER),
	            v_Benefit_Type_Code3 = '03' or v_Benefit_Type_Code3 = '04', CAST(ppd_rate AS INTEGER),
	            v_Benefit_Type_Code3 = '05', CAST(ttd_rate AS INTEGER),
	            v_Benefit_Type_Code3 = '09', CAST(ppd_rate AS INTEGER),
	            v_Benefit_Type_Code3 = '11', CAST(TPD_AMT AS INTEGER),
	            0
	        )), 6, '0') AS Weekly_Benefit3,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- indemnity_payment_amt_loss_trans = 0 and open_reserve_amt >0,0,
	-- --v_Benefit_Type_Code4 = '01',to_integer(dtd_rate,false),
	-- v_Benefit_Type_Code4 = '02',to_integer(ptd_rate,false),
	-- v_Benefit_Type_Code4 = '03' or v_Benefit_Type_Code4 = '04' ,to_integer(ppd_rate,false),
	-- v_Benefit_Type_Code4 = '05',to_integer(ttd_rate,false),
	-- v_Benefit_Type_Code4 = '09',to_integer(ppd_rate,false),
	-- v_Benefit_Type_Code4 = '11',to_integer(TPD_AMT,false),
	-- 0)),6,'0')
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            indemnity_payment_amt_loss_trans = 0 and open_reserve_amt > 0, 0,
	            v_Benefit_Type_Code4 = '02', CAST(ptd_rate AS INTEGER),
	            v_Benefit_Type_Code4 = '03' or v_Benefit_Type_Code4 = '04', CAST(ppd_rate AS INTEGER),
	            v_Benefit_Type_Code4 = '05', CAST(ttd_rate AS INTEGER),
	            v_Benefit_Type_Code4 = '09', CAST(ppd_rate AS INTEGER),
	            v_Benefit_Type_Code4 = '11', CAST(TPD_AMT AS INTEGER),
	            0
	        )), 6, '0') AS Weekly_Benefit4,
	'000000' AS Weekly_Benefit5,
	Benefit_Covered_By_Lump_Sump_DT_FN,
	Benefit_Covered_By_Lump_Sump_PT_PS,
	Benefit_Covered_By_Lump_Sump_PP,
	Benefit_Covered_By_Lump_Sump_PB,
	Benefit_Covered_By_Lump_Sump_TD_PL_TC,
	Benefit_Covered_By_Lump_Sump_MP,
	Benefit_Covered_By_Lump_Sump_DF,
	Benefit_Covered_By_Lump_Sump_PD,
	Benefit_Covered_By_Lump_Sump_EP,
	Benefit_Covered_By_Lump_Sump_AL_UI,
	Vocational_Rehab_Evaluation_Expense_Amount_Paid,
	-- *INF*: iif(isnull(Vocational_Rehab_Evaluation_Expense_Amount_Paid) or claim_status_code = '5'
	-- ,0
	-- ,Vocational_Rehab_Evaluation_Expense_Amount_Paid)
	IFF(
	    Vocational_Rehab_Evaluation_Expense_Amount_Paid IS NULL or claim_status_code = '5', 0,
	    Vocational_Rehab_Evaluation_Expense_Amount_Paid
	) AS v_Vocational_Rehab_Evaluation_Expense_Amount_Paid,
	-- *INF*: iif(isnull(Vocational_Rehab_Evaluation_Expense_Amount_Paid) or claim_status_code = '5' or jurisdiction_state = '21'
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(Vocational_Rehab_Evaluation_Expense_Amount_Paid,false)),9,'0')
	-- )
	-- 
	-- --for MICHIGAN, state 21, we don't report vocational amounts
	IFF(
	    Vocational_Rehab_Evaluation_Expense_Amount_Paid IS NULL
	    or claim_status_code = '5'
	    or jurisdiction_state = '21',
	    '000000000',
	    lpad(to_char(CAST(Vocational_Rehab_Evaluation_Expense_Amount_Paid AS INTEGER)), 9, '0')
	) AS o_Vocational_Rehab_Evaluation_Expense_Amount_Paid,
	Vocational_Rehab_Maintenance_Benefit_Amount_Paid,
	-- *INF*: iif(isnull(Vocational_Rehab_Maintenance_Benefit_Amount_Paid) or claim_status_code = '5'
	-- ,0
	-- ,Vocational_Rehab_Maintenance_Benefit_Amount_Paid)
	IFF(
	    Vocational_Rehab_Maintenance_Benefit_Amount_Paid IS NULL or claim_status_code = '5', 0,
	    Vocational_Rehab_Maintenance_Benefit_Amount_Paid
	) AS v_Vocational_Rehab_Maintenance_Benefit_Amount_Paid,
	-- *INF*: iif(isnull(Vocational_Rehab_Maintenance_Benefit_Amount_Paid) or claim_status_code = '5'  or jurisdiction_state = '21'
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(Vocational_Rehab_Maintenance_Benefit_Amount_Paid,false)),9,'0'))
	-- 
	-- --for MICHIGAN, state 21, we don't report vocational amounts
	IFF(
	    Vocational_Rehab_Maintenance_Benefit_Amount_Paid IS NULL
	    or claim_status_code = '5'
	    or jurisdiction_state = '21',
	    '000000000',
	    lpad(to_char(CAST(Vocational_Rehab_Maintenance_Benefit_Amount_Paid AS INTEGER)), 9, '0')
	) AS o_Vocational_Rehab_Maintenance_Benefit_Amount_Paid,
	Vocational_Rehab_Education_Expense_Amount_Paid,
	-- *INF*: iif(isnull(Vocational_Rehab_Education_Expense_Amount_Paid) or claim_status_code = '5'
	-- ,0
	-- ,Vocational_Rehab_Education_Expense_Amount_Paid)
	IFF(
	    Vocational_Rehab_Education_Expense_Amount_Paid IS NULL or claim_status_code = '5', 0,
	    Vocational_Rehab_Education_Expense_Amount_Paid
	) AS v_Vocational_Rehab_Education_Expense_Amount_Paid,
	-- *INF*: iif(isnull(Vocational_Rehab_Education_Expense_Amount_Paid) or claim_status_code = '5' or jurisdiction_state = '21'
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(Vocational_Rehab_Education_Expense_Amount_Paid,false)),9,'0'))
	-- 
	-- --for MICHIGAN, state 21, we don't report vocational amounts
	IFF(
	    Vocational_Rehab_Education_Expense_Amount_Paid IS NULL
	    or claim_status_code = '5'
	    or jurisdiction_state = '21',
	    '000000000',
	    lpad(to_char(CAST(Vocational_Rehab_Education_Expense_Amount_Paid AS INTEGER)), 9, '0')
	) AS o_Vocational_Rehab_Education_Expense_Amount_Paid,
	Vocational_Rehab_Other_Amount_Paid,
	-- *INF*: iif(isnull(Vocational_Rehab_Other_Amount_Paid) or claim_status_code = '5'
	-- ,0
	-- ,Vocational_Rehab_Other_Amount_Paid)
	IFF(
	    Vocational_Rehab_Other_Amount_Paid IS NULL or claim_status_code = '5', 0,
	    Vocational_Rehab_Other_Amount_Paid
	) AS v_Vocational_Rehab_Other_Amount_Paid,
	-- *INF*: iif(isnull(Vocational_Rehab_Other_Amount_Paid) or claim_status_code = '5' or jurisdiction_state = '21'
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(Vocational_Rehab_Other_Amount_Paid,false)),9,'0')
	-- )
	-- 
	-- --for MICHIGAN, state 21, we don't report vocational amounts
	IFF(
	    Vocational_Rehab_Other_Amount_Paid IS NULL
	    or claim_status_code = '5'
	    or jurisdiction_state = '21',
	    '000000000',
	    lpad(to_char(CAST(Vocational_Rehab_Other_Amount_Paid AS INTEGER)), 9, '0')
	) AS o_Vocational_Rehab_Other_Amount_Paid,
	-- *INF*: decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Covered_By_Lump_Sump_DT_FN>0,'01',
	-- Benefit_Covered_By_Lump_Sump_PT_PS>0,'02',
	-- Benefit_Covered_By_Lump_Sump_PP>0,'03',
	-- Benefit_Covered_By_Lump_Sump_PB>0,'04',
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0,'05',
	-- Benefit_Covered_By_Lump_Sump_MP>0,'06',
	-- Benefit_Covered_By_Lump_Sump_DF>0,'09',
	-- Benefit_Covered_By_Lump_Sump_PD>0,'11',
	-- Benefit_Covered_By_Lump_Sump_EP>0,'12',
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0,'49',
	-- '00')
	decode(
	    true,
	    claim_status_code = '5', '00',
	    Benefit_Covered_By_Lump_Sump_DT_FN > 0, '01',
	    Benefit_Covered_By_Lump_Sump_PT_PS > 0, '02',
	    Benefit_Covered_By_Lump_Sump_PP > 0, '03',
	    Benefit_Covered_By_Lump_Sump_PB > 0, '04',
	    Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0, '05',
	    Benefit_Covered_By_Lump_Sump_MP > 0, '06',
	    Benefit_Covered_By_Lump_Sump_DF > 0, '09',
	    Benefit_Covered_By_Lump_Sump_PD > 0, '11',
	    Benefit_Covered_By_Lump_Sump_EP > 0, '12',
	    Benefit_Covered_By_Lump_Sump_AL_UI > 0, '49',
	    '00'
	) AS v_Benefit_Type_Code_Lump_Sump_Ind1,
	v_Benefit_Type_Code_Lump_Sump_Ind1 AS o_Benefit_Type_Code_Lump_Sump_Ind1,
	-- *INF*: decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Covered_By_Lump_Sump_PT_PS>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '02' and v_Benefit_Type_Code_Lump_Sump_Ind1 >'00' ,'02',
	-- Benefit_Covered_By_Lump_Sump_PP>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '03' and v_Benefit_Type_Code_Lump_Sump_Ind1 >'00' ,'03',
	-- Benefit_Covered_By_Lump_Sump_PB>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '04' and v_Benefit_Type_Code_Lump_Sump_Ind1 >'00' ,'04',
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '05'  and v_Benefit_Type_Code_Lump_Sump_Ind1 >'00', '05',
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind1 >'00' ,'06',
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '07'  and v_Benefit_Type_Code_Lump_Sump_Ind1 >'00','09',
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind1 >'00' ,'11',
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind1 >'00', '12',
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '49'  and v_Benefit_Type_Code_Lump_Sump_Ind1 >'00','49',
	-- '00')
	decode(
	    true,
	    claim_status_code = '5', '00',
	    Benefit_Covered_By_Lump_Sump_PT_PS > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '02' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', '02',
	    Benefit_Covered_By_Lump_Sump_PP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '03' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', '03',
	    Benefit_Covered_By_Lump_Sump_PB > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '04' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', '04',
	    Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', '05',
	    Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', '06',
	    Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', '09',
	    Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', '11',
	    Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', '12',
	    Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', '49',
	    '00'
	) AS v_Benefit_Type_Code_Lump_Sump_Ind2,
	v_Benefit_Type_Code_Lump_Sump_Ind2 AS o_Benefit_Type_Code_Lump_Sump_Ind2,
	-- *INF*: decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Covered_By_Lump_Sump_PP>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '03' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00' ,'03',
	-- Benefit_Covered_By_Lump_Sump_PB>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '04' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00' ,'04',
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '05',
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00','06',
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00','09',
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00' ,'11',
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '12',
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00','49',
	-- '00')
	decode(
	    true,
	    claim_status_code = '5', '00',
	    Benefit_Covered_By_Lump_Sump_PP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '03' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '03',
	    Benefit_Covered_By_Lump_Sump_PB > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '04' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '04',
	    Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '05',
	    Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '06',
	    Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '09',
	    Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '11',
	    Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '12',
	    Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', '49',
	    '00'
	) AS v_Benefit_Type_Code_Lump_Sump_Ind3,
	v_Benefit_Type_Code_Lump_Sump_Ind3 AS o_Benefit_Type_Code_Lump_Sump_Ind3,
	-- *INF*: decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Covered_By_Lump_Sump_PB>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '04' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00' ,'04',
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '05'  and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', '05',
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '06'  and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00','06',
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '07'  and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00' ,'09',
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '11'  and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00' ,'11',
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '12'  and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', '12',
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '49'  and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00','49',
	-- '00')
	decode(
	    true,
	    claim_status_code = '5', '00',
	    Benefit_Covered_By_Lump_Sump_PB > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '04' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', '04',
	    Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', '05',
	    Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', '06',
	    Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', '09',
	    Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', '11',
	    Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', '12',
	    Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', '49',
	    '00'
	) AS v_Benefit_Type_Code_Lump_Sump_Ind4,
	v_Benefit_Type_Code_Lump_Sump_Ind4 AS o_Benefit_Type_Code_Lump_Sump_Ind4,
	-- *INF*: decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '05'  and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', '05',
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '06'  and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00' ,'06',
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind4< '07'   and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00','09',
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '11'  and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00' ,'11',
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '12'  and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', '12',
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '49'  and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00' ,'49',
	-- '00')
	decode(
	    true,
	    claim_status_code = '5', '00',
	    Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', '05',
	    Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', '06',
	    Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', '09',
	    Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', '11',
	    Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', '12',
	    Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', '49',
	    '00'
	) AS v_Benefit_Type_Code_Lump_Sump_Ind5,
	v_Benefit_Type_Code_Lump_Sump_Ind5 AS o_Benefit_Type_Code_Lump_Sump_Ind5,
	-- *INF*: decode(true,
	-- claim_status_code = '5','00',
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '06'  and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00' ,'06',
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind5< '07' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00' ,'09',
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00' ,'11',
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', '12',
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '49'  and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00','49',
	-- '00')
	decode(
	    true,
	    claim_status_code = '5', '00',
	    Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', '06',
	    Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', '09',
	    Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', '11',
	    Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', '12',
	    Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', '49',
	    '00'
	) AS v_Benefit_Type_Code_Lump_Sump_Ind6,
	v_Benefit_Type_Code_Lump_Sump_Ind6 AS o_Benefit_Type_Code_Lump_Sump_Ind6,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- Benefit_Covered_By_Lump_Sump_DT_FN>0,to_integer(Benefit_Covered_By_Lump_Sump_DT_FN,false),
	-- Benefit_Covered_By_Lump_Sump_PT_PS>0,to_integer(Benefit_Covered_By_Lump_Sump_PT_PS,false),
	-- Benefit_Covered_By_Lump_Sump_PP>0,to_integer(Benefit_Covered_By_Lump_Sump_PP,false),
	-- Benefit_Covered_By_Lump_Sump_PB>0,to_integer(Benefit_Covered_By_Lump_Sump_PB,false),
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0,to_integer(Benefit_Covered_By_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Covered_By_Lump_Sump_MP>0 ,to_integer(Benefit_Covered_By_Lump_Sump_MP,false),
	-- Benefit_Covered_By_Lump_Sump_DF>0 ,to_integer(Benefit_Covered_By_Lump_Sump_DF,false),
	-- Benefit_Covered_By_Lump_Sump_PD>0 ,to_integer(Benefit_Covered_By_Lump_Sump_PD,false),
	-- Benefit_Covered_By_Lump_Sump_EP>0 ,to_integer(Benefit_Covered_By_Lump_Sump_EP,false),
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 ,to_integer(Benefit_Covered_By_Lump_Sump_AL_UI,false),
	-- 0)),9,'0')
	-- 
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            Benefit_Covered_By_Lump_Sump_DT_FN > 0, CAST(Benefit_Covered_By_Lump_Sump_DT_FN AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PT_PS > 0, CAST(Benefit_Covered_By_Lump_Sump_PT_PS AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PP > 0, CAST(Benefit_Covered_By_Lump_Sump_PP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PB > 0, CAST(Benefit_Covered_By_Lump_Sump_PB AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0, CAST(Benefit_Covered_By_Lump_Sump_TD_PL_TC AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_MP > 0, CAST(Benefit_Covered_By_Lump_Sump_MP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_DF > 0, CAST(Benefit_Covered_By_Lump_Sump_DF AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PD > 0, CAST(Benefit_Covered_By_Lump_Sump_PD AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_EP > 0, CAST(Benefit_Covered_By_Lump_Sump_EP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_AL_UI > 0, CAST(Benefit_Covered_By_Lump_Sump_AL_UI AS INTEGER),
	            0
	        )), 9, '0') AS Lump_Sump_Settlement_Amount_Paid1,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- Benefit_Covered_By_Lump_Sump_PT_PS>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '02'  and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00' ,to_integer(Benefit_Covered_By_Lump_Sump_PT_PS,false),
	-- Benefit_Covered_By_Lump_Sump_PP>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '03'  and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00',to_integer(Benefit_Covered_By_Lump_Sump_PP,false),
	-- Benefit_Covered_By_Lump_Sump_PB>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '04'  and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00' ,to_integer(Benefit_Covered_By_Lump_Sump_PB,false),
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '05'  and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00',to_integer(Benefit_Covered_By_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '06'  and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00',to_integer(Benefit_Covered_By_Lump_Sump_MP,false),
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '07'  and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00',to_integer(Benefit_Covered_By_Lump_Sump_DF,false),
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '11'  and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00',to_integer(Benefit_Covered_By_Lump_Sump_PD,false),
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '12'  and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00',to_integer(Benefit_Covered_By_Lump_Sump_EP,false),
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '49'   and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00',to_integer(Benefit_Covered_By_Lump_Sump_AL_UI,false),
	-- 0)),9,'0')
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            Benefit_Covered_By_Lump_Sump_PT_PS > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '02' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', CAST(Benefit_Covered_By_Lump_Sump_PT_PS AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '03' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', CAST(Benefit_Covered_By_Lump_Sump_PP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PB > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '04' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', CAST(Benefit_Covered_By_Lump_Sump_PB AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', CAST(Benefit_Covered_By_Lump_Sump_TD_PL_TC AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', CAST(Benefit_Covered_By_Lump_Sump_MP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', CAST(Benefit_Covered_By_Lump_Sump_DF AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', CAST(Benefit_Covered_By_Lump_Sump_PD AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', CAST(Benefit_Covered_By_Lump_Sump_EP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind1 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind1 > '00', CAST(Benefit_Covered_By_Lump_Sump_AL_UI AS INTEGER),
	            0
	        )), 9, '0') AS Lump_Sump_Settlement_Amount_Paid2,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- Benefit_Covered_By_Lump_Sump_PP>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '03'  and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00',to_integer(Benefit_Covered_By_Lump_Sump_PP,false),
	-- Benefit_Covered_By_Lump_Sump_PB>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '04'  and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00' ,to_integer(Benefit_Covered_By_Lump_Sump_PB,false),
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '05'  and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00',to_integer(Benefit_Covered_By_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '06'  and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00',to_integer(Benefit_Covered_By_Lump_Sump_MP,false),
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '07'  and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00',to_integer(Benefit_Covered_By_Lump_Sump_DF,false),
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '11'  and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00',to_integer(Benefit_Covered_By_Lump_Sump_PD,false),
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '12'  and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00',to_integer(Benefit_Covered_By_Lump_Sump_EP,false),
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '49'  and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00' ,to_integer(Benefit_Covered_By_Lump_Sump_AL_UI,false),
	-- 0)),9,'0')
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            Benefit_Covered_By_Lump_Sump_PP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '03' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', CAST(Benefit_Covered_By_Lump_Sump_PP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PB > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '04' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', CAST(Benefit_Covered_By_Lump_Sump_PB AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', CAST(Benefit_Covered_By_Lump_Sump_TD_PL_TC AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', CAST(Benefit_Covered_By_Lump_Sump_MP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', CAST(Benefit_Covered_By_Lump_Sump_DF AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', CAST(Benefit_Covered_By_Lump_Sump_PD AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', CAST(Benefit_Covered_By_Lump_Sump_EP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind2 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind2 > '00', CAST(Benefit_Covered_By_Lump_Sump_AL_UI AS INTEGER),
	            0
	        )), 9, '0') AS Lump_Sump_Settlement_Amount_Paid3,
	-- *INF*: lpad(to_char(decode(true,
	-- claim_status_code = '5',0,
	-- Benefit_Covered_By_Lump_Sump_PB>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '04'   and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00',to_integer(Benefit_Covered_By_Lump_Sump_PB,false),
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '05'   and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00',to_integer(Benefit_Covered_By_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '06'   and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00',to_integer(Benefit_Covered_By_Lump_Sump_MP,false),
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '07'   and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00',to_integer(Benefit_Covered_By_Lump_Sump_DF,false),
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '11'   and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00',to_integer(Benefit_Covered_By_Lump_Sump_PD,false),
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '12'   and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00',to_integer(Benefit_Covered_By_Lump_Sump_EP,false),
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '49'    and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00',to_integer(Benefit_Covered_By_Lump_Sump_AL_UI,false),
	-- 0)),9,'0')
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            Benefit_Covered_By_Lump_Sump_PB > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '04' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', CAST(Benefit_Covered_By_Lump_Sump_PB AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', CAST(Benefit_Covered_By_Lump_Sump_TD_PL_TC AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', CAST(Benefit_Covered_By_Lump_Sump_MP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', CAST(Benefit_Covered_By_Lump_Sump_DF AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', CAST(Benefit_Covered_By_Lump_Sump_PD AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', CAST(Benefit_Covered_By_Lump_Sump_EP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind3 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind3 > '00', CAST(Benefit_Covered_By_Lump_Sump_AL_UI AS INTEGER),
	            0
	        )), 9, '0') AS Lump_Sump_Settlement_Amount_Paid4,
	-- *INF*: lpad(to_char(decode(true,claim_status_code = '5',0,
	-- Benefit_Covered_By_Lump_Sump_TD_PL_TC>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00',to_integer(Benefit_Covered_By_Lump_Sump_TD_PL_TC,false),
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00',to_integer(Benefit_Covered_By_Lump_Sump_MP,false),
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00',to_integer(Benefit_Covered_By_Lump_Sump_DF,false),
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00',to_integer(Benefit_Covered_By_Lump_Sump_PD,false),
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00',to_integer(Benefit_Covered_By_Lump_Sump_EP,false),
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '49'  and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00',to_integer(Benefit_Covered_By_Lump_Sump_AL_UI,false),
	-- 0)),9,'0')
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            Benefit_Covered_By_Lump_Sump_TD_PL_TC > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '05' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', CAST(Benefit_Covered_By_Lump_Sump_TD_PL_TC AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', CAST(Benefit_Covered_By_Lump_Sump_MP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', CAST(Benefit_Covered_By_Lump_Sump_DF AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', CAST(Benefit_Covered_By_Lump_Sump_PD AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', CAST(Benefit_Covered_By_Lump_Sump_EP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind4 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind4 > '00', CAST(Benefit_Covered_By_Lump_Sump_AL_UI AS INTEGER),
	            0
	        )), 9, '0') AS Lump_Sump_Settlement_Amount_Paid5,
	-- *INF*: lpad(to_char(decode(true,claim_status_code = '5',0,
	-- Benefit_Covered_By_Lump_Sump_MP>0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00',to_integer(Benefit_Covered_By_Lump_Sump_MP,false),
	-- Benefit_Covered_By_Lump_Sump_DF>0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00',to_integer(Benefit_Covered_By_Lump_Sump_DF,false),
	-- Benefit_Covered_By_Lump_Sump_PD>0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00',to_integer(Benefit_Covered_By_Lump_Sump_PD,false),
	-- Benefit_Covered_By_Lump_Sump_EP>0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00',to_integer(Benefit_Covered_By_Lump_Sump_EP,false),
	-- Benefit_Covered_By_Lump_Sump_AL_UI>0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '49'  and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00',to_integer(Benefit_Covered_By_Lump_Sump_AL_UI,false),
	-- 0)),9,'0')
	lpad(to_char(decode(
	            true,
	            claim_status_code = '5', 0,
	            Benefit_Covered_By_Lump_Sump_MP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '06' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', CAST(Benefit_Covered_By_Lump_Sump_MP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_DF > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '07' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', CAST(Benefit_Covered_By_Lump_Sump_DF AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_PD > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '11' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', CAST(Benefit_Covered_By_Lump_Sump_PD AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_EP > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '12' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', CAST(Benefit_Covered_By_Lump_Sump_EP AS INTEGER),
	            Benefit_Covered_By_Lump_Sump_AL_UI > 0 and v_Benefit_Type_Code_Lump_Sump_Ind5 < '49' and v_Benefit_Type_Code_Lump_Sump_Ind5 > '00', CAST(Benefit_Covered_By_Lump_Sump_AL_UI AS INTEGER),
	            0
	        )), 9, '0') AS Lump_Sump_Settlement_Amount_Paid6,
	-- *INF*: to_integer((v_Vocational_Rehab_Evaluation_Expense_Amount_Paid + v_Vocational_Rehab_Maintenance_Benefit_Amount_Paid + 
	-- v_Vocational_Rehab_Education_Expense_Amount_Paid+v_Vocational_Rehab_Other_Amount_Paid),false)
	CAST((v_Vocational_Rehab_Evaluation_Expense_Amount_Paid + v_Vocational_Rehab_Maintenance_Benefit_Amount_Paid + v_Vocational_Rehab_Education_Expense_Amount_Paid + v_Vocational_Rehab_Other_Amount_Paid) AS INTEGER) AS v_Total_Vocational_Amounts,
	-- *INF*: to_integer(:LKP.LKP_GETTOTALPAIDMEDAMT(work_claim_ncci_rpt_extract_id))
	-- 
	CAST(LKP_GETTOTALPAIDMEDAMT_work_claim_ncci_rpt_extract_id.total_paid_med_amt AS INTEGER) AS v_Total_Paid_Med_Amount,
	-- *INF*: lpad(to_char(iif(jurisdiction_state = '21'
	-- ,v_Total_Vocational_Amounts+v_Total_Paid_Med_Amount
	-- ,v_Total_Paid_Med_Amount)),9,'0')
	lpad(to_char(
	        IFF(
	            jurisdiction_state = '21',
	            v_Total_Vocational_Amounts + v_Total_Paid_Med_Amount,
	            v_Total_Paid_Med_Amount
	        )), 9, '0') AS Upd_Total_Paid_Med_Amounts
	FROM Agg_Calculate_Payments_And_Benefits
	LEFT JOIN LKP_GETTOTALPAIDMEDAMT LKP_GETTOTALPAIDMEDAMT_work_claim_ncci_rpt_extract_id
	ON LKP_GETTOTALPAIDMEDAMT_work_claim_ncci_rpt_extract_id.work_claim_ncci_rpt_extract_id = work_claim_ncci_rpt_extract_id

),
Upd_Target AS (
	SELECT
	work_claim_ncci_rpt_extract_id, 
	o_Recovery_Reimbursement_Amount, 
	o_claimant_legal_amount_paid AS o_claimanat_legal_amount_paid, 
	o_employer_legal_amount_paid, 
	o_Attorny_Authorized_Representative_Ind, 
	o_Benefit_Type_Code1, 
	o_Benefit_Type_Code2, 
	o_Benefit_Type_Code3, 
	o_Benefit_Type_Code4, 
	o_Benefit_Type_Code5, 
	Benefit_Amount_Paid1, 
	Benefit_Amount_Paid2, 
	Benefit_Amount_Paid3, 
	Benefit_Amount_Paid4, 
	Benefit_Amount_Paid5, 
	Weekly_Benefit1, 
	Weekly_Benefit2, 
	Weekly_Benefit3, 
	Weekly_Benefit4, 
	Weekly_Benefit5, 
	o_Vocational_Rehab_Evaluation_Expense_Amount_Paid AS Vocational_Rehab_Evaluation_Expense_Amount_Paid, 
	o_Vocational_Rehab_Maintenance_Benefit_Amount_Paid AS Vocational_Rehab_Maintenance_Benefit_Amount_Paid, 
	o_Vocational_Rehab_Education_Expense_Amount_Paid AS Vocational_Rehab_Education_Expense_Amount_Paid, 
	o_Vocational_Rehab_Other_Amount_Paid AS Vocational_Rehab_Other_Amount_Paid, 
	o_Benefit_Type_Code_Lump_Sump_Ind1, 
	o_Benefit_Type_Code_Lump_Sump_Ind2, 
	o_Benefit_Type_Code_Lump_Sump_Ind3, 
	o_Benefit_Type_Code_Lump_Sump_Ind4, 
	o_Benefit_Type_Code_Lump_Sump_Ind5, 
	o_Benefit_Type_Code_Lump_Sump_Ind6, 
	Lump_Sump_Settlement_Amount_Paid1, 
	Lump_Sump_Settlement_Amount_Paid2, 
	Lump_Sump_Settlement_Amount_Paid3, 
	Lump_Sump_Settlement_Amount_Paid4, 
	Lump_Sump_Settlement_Amount_Paid5, 
	Lump_Sump_Settlement_Amount_Paid6, 
	Upd_Total_Paid_Med_Amounts
	FROM EXP_Calculate_Payments_And_Benefits
),
work_claim_ncci_report_extract AS (
	MERGE INTO work_claim_ncci_report_extract AS T
	USING Upd_Target AS S
	ON T.work_claim_ncci_rpt_extract_id = S.work_claim_ncci_rpt_extract_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.bnft_type_code_1 = S.o_Benefit_Type_Code1, T.bnft_amt_paid_1 = S.Benefit_Amount_Paid1, T.wkly_bnft_1 = S.Weekly_Benefit1, T.bnft_type_code_2 = S.o_Benefit_Type_Code2, T.bnft_amt_paid_2 = S.Benefit_Amount_Paid2, T.wkly_bnft_2 = S.Weekly_Benefit2, T.bnft_type_code_3 = S.o_Benefit_Type_Code3, T.bnft_amt_paid_3 = S.Benefit_Amount_Paid3, T.wkly_bnft_3 = S.Weekly_Benefit3, T.bnft_type_code_4 = S.o_Benefit_Type_Code4, T.bnft_amt_paid_4 = S.Benefit_Amount_Paid4, T.wkly_bnft_4 = S.Weekly_Benefit4, T.bnft_type_code_5 = S.o_Benefit_Type_Code5, T.bnft_amt_paid_5 = S.Benefit_Amount_Paid5, T.wkly_bnft_5 = S.Weekly_Benefit5, T.vocational_rehabilitation_evaluation_exp_amt_paid = S.Vocational_Rehab_Evaluation_Expense_Amount_Paid, T.vocational_rehabilitation_maint_bnft_amt_paid = S.Vocational_Rehab_Maintenance_Benefit_Amount_Paid, T.vocational_rehabilitation_education_exp_amt_paid = S.Vocational_Rehab_Education_Expense_Amount_Paid, T.vocational_rehabilitation_other_amt_paid = S.Vocational_Rehab_Other_Amount_Paid, T.total_paid_med_amt = S.Upd_Total_Paid_Med_Amounts, T.attorney_or_au_rep_ind = S.o_Attorny_Authorized_Representative_Ind, T.claimant_lgl_amt_paid = S.o_claimanat_legal_amount_paid, T.emplyr_lgl_amt_paid = S.o_employer_legal_amount_paid, T.bnft_cvrd_by_lump_sum_settlement_code_1 = S.o_Benefit_Type_Code_Lump_Sump_Ind1, T.lump_sum_settlement_amt_paid_1 = S.Lump_Sump_Settlement_Amount_Paid1, T.bnft_cvrd_by_lump_sum_settlement_code_2 = S.o_Benefit_Type_Code_Lump_Sump_Ind2, T.lump_sum_settlement_amt_paid_2 = S.Lump_Sump_Settlement_Amount_Paid2, T.bnft_cvrd_by_lump_sum_settlement_code_3 = S.o_Benefit_Type_Code_Lump_Sump_Ind3, T.lump_sum_settlement_amt_paid_3 = S.Lump_Sump_Settlement_Amount_Paid3, T.bnft_cvrd_by_lump_sum_settlement_code_4 = S.o_Benefit_Type_Code_Lump_Sump_Ind4, T.lump_sum_settlement_amt_paid_4 = S.Lump_Sump_Settlement_Amount_Paid4, T.bnft_cvrd_by_lump_sum_settlement_code_5 = S.o_Benefit_Type_Code_Lump_Sump_Ind5, T.lump_sum_settlement_amt_paid_5 = S.Lump_Sump_Settlement_Amount_Paid5, T.bnft_cvrd_by_lump_sum_settlement_code_6 = S.o_Benefit_Type_Code_Lump_Sump_Ind6, T.lump_sum_settlement_amt_paid_6 = S.Lump_Sump_Settlement_Amount_Paid6, T.recovery_reimb_amt = S.o_Recovery_Reimbursement_Amount
),