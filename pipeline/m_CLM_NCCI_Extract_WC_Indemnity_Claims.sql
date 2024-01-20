WITH
SQ_Get_WC_Medical_Data_NCCI AS (
	SELECT co.edw_claim_occurrence_ak_id,
	       pol.edw_pol_ak_id,
	       clmt.edw_claim_party_occurrence_ak_id,
	       SUM(loss_trans.direct_loss_paid_excluding_recoveries) as Total_Paid_Medical_Amount,
	       SUM(loss_trans.direct_loss_incurred_excluding_recoveries) as Incurred_Medical_Amount_Total
	FROM   claim_occurrence_dim co WITH (NOLOCK)
	      INNER JOIN claim_loss_transaction_fact loss_trans WITH (NOLOCK)
	         ON co.claim_occurrence_dim_id = loss_trans.claim_occurrence_dim_id
	       INNER JOIN claimant_dim clmt WITH (NOLOCK)
	         ON loss_trans.claimant_dim_id = clmt.claimant_dim_id
	       INNER JOIN policy_dim pol WITH (NOLOCK)
	         ON loss_trans.pol_dim_id = pol.pol_dim_id
	       INNER JOIN claimant_coverage_dim clmt_cov WITH (NOLOCK)
	         ON loss_trans.claimant_cov_dim_id = clmt_cov.claimant_cov_dim_id
	        INNER JOIN claim_payment_category_type_dim ctgry_type_dim WITH (NOLOCK)
	         ON loss_trans.claim_pay_ctgry_type_dim_id = ctgry_type_dim.claim_pay_ctgry_type_dim_id 
	WHERE  co.source_claim_rpted_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+1), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) --start selection date
	       AND co.source_claim_rpted_date < Dateadd(m, -@{pipeline().parameters.NUMBER_OF_MONTHS}, CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) --end selection date
	       AND ctgry_type_dim.claim_pay_ctgry_lump_sum_ind <> 'Y' 
	       AND clmt.crrnt_snpsht_flag = 1      
	       AND pol.crrnt_snpsht_flag = 1
	       AND clmt_cov.MajorPerilCode = '032' -- for WC claims
	       AND clmt_cov.cause_of_loss = '06' -- for medical claims
	       AND co.claim_num <> 'N/A' --for exceed_claims
	       AND clmt.jurisdiction_state_code not in ('CA', 'DE', 'ND', 'OH', 'WA', 'WY','N/A') --Non-DCI states
	GROUP  BY  co.edw_claim_occurrence_ak_id,
	       pol.edw_pol_ak_id,
	       clmt.edw_claim_party_occurrence_ak_id
),
EXP_Get_Medical_Source_Data AS (
	SELECT
	edw_claim_occurrence_ak_id,
	edw_pol_ak_id,
	edw_claim_party_occurrence_ak_id,
	direct_loss_paid_excluding_recoveries AS Total_Paid_Medical_Amount,
	-- *INF*: iif(isnull(Total_Paid_Medical_Amount)
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(Total_Paid_Medical_Amount,false)),9,'0')
	-- )
	IFF(
	    Total_Paid_Medical_Amount IS NULL, '000000000',
	    lpad(to_char(CAST(Total_Paid_Medical_Amount AS INTEGER)), 9, '0')
	) AS o_Total_Paid_Medical_Amount,
	direct_loss_incurred_excluding_recoveries AS Incurred_Medical_Amount_Total,
	-- *INF*: iif(isnull(Incurred_Medical_Amount_Total)
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(Incurred_Medical_Amount_Total,false)),9,'0')
	-- )
	IFF(
	    Incurred_Medical_Amount_Total IS NULL, '000000000',
	    lpad(to_char(CAST(Incurred_Medical_Amount_Total AS INTEGER)), 9, '0')
	) AS o_Incurred_Medical_Amount_Total
	FROM SQ_Get_WC_Medical_Data_NCCI
),
SQ_Get_WC_Indemnity_Data_NCCI AS (
	SELECT co.edw_claim_occurrence_ak_id,
	       ltrim(rtrim(co.source_claim_occurrence_status_code)),
	       co.source_claim_rpted_date,
	       ltrim(rtrim(co.loss_loc_state)),
	       co.claim_loss_date,
	       ltrim(rtrim(co.claim_num)),
	       ltrim(rtrim(co.wc_cat_code)),
	       pol.edw_pol_ak_id,
		   ltrim(rtrim(pol.pol_sym)) as pol_sym,
	       ltrim(rtrim(pol.pol_key)),
	       pol.pol_eff_date,
	       clmt.edw_claim_party_occurrence_ak_id,
	       ltrim(rtrim(clmt.claimant_status_type)),
	       clmt.claimant_close_date,
	       clmt.claimant_closed_after_reopen_date,
	       ltrim(rtrim(clmt.claimant_full_name)),
	       clmt.claimant_birthdate,
	       ltrim(rtrim(clmt.claimant_gndr)),
	       ltrim(rtrim(clmt.jurisdiction_state_code)),
	       clmt.hired_date,
	       clmt.avg_wkly_wage,
	       ltrim(rtrim(clmt.body_part_code)),
	       ltrim(rtrim(clmt.nature_inj_code)),
	       ltrim(rtrim(clmt.cause_inj_code)),
	       clmt.return_to_work_date,
	       ltrim(rtrim(clmt.controverted_case_code)),
	       ltrim(rtrim(clmt.wc_claimant_num)),
	       ltrim(rtrim(clmt.type_of_loss_code)),
	       ltrim(rtrim(clmt.pre_inj_avg_wkly_wage_code)),
	       clmt.post_inj_wkly_wage_amt,
	       clmt.impairment_disability_percentage,
	       ltrim(rtrim(clmt.impairment_disability_percentage_basis_code)),
	       clmt.max_med_improvement_date,
	       ltrim(rtrim(clmt.med_extinguishment_ind)),
	      ltrim(rtrim(clmt.crrnt_work_status)),
	       max(ltrim(rtrim(clm_trans_dim.trans_ctgry_code))),
	       SUM(loss_trans.direct_loss_incurred_excluding_recoveries) as Incurred_Indemnity_Amount_Total,
	       SUM(loss_trans.direct_subrogation_incurred) as direct_subrogation_incurred,
	       SUM(loss_trans.direct_other_recovery_loss_incurred) as direct_other_recovery_loss_incurred,
	       clmt_cov.cause_of_loss,
		   ltrim(rtrim(COVDET.ClassCode)) as classcode
	FROM   claim_occurrence_dim co WITH (NOLOCK)
	     inner join claim_loss_transaction_fact loss_trans WITH (NOLOCK)
	         ON co.claim_occurrence_dim_id = loss_trans.claim_occurrence_dim_id
	       INNER JOIN claimant_dim clmt WITH (NOLOCK)
	         ON loss_trans.claimant_dim_id = clmt.claimant_dim_id
	       INNER JOIN policy_dim pol WITH (NOLOCK)
	         ON loss_trans.pol_dim_id = pol.pol_dim_id
	       INNER JOIN CoverageDetailDim COVDET WITH (NOLOCK)
	         ON loss_trans.CoverageDetailDimId = COVDET.CoverageDetailDimId
	       INNER JOIN claimant_coverage_dim clmt_cov WITH (NOLOCK)
	         ON loss_trans.claimant_cov_dim_id = clmt_cov.claimant_cov_dim_id
	       left outer join (select * from claim_transaction_type_dim where trans_ctgry_code = 'SI') clm_trans_dim   
	        on loss_trans.claim_trans_type_dim_id = clm_trans_dim.claim_trans_type_dim_id 
	       left outer join work_claim_ncci_report_extract ncci_extract_tab 
	       on ncci_extract_tab.claim_num_id = clmt.wc_claimant_num
	WHERE  
	co.source_claim_rpted_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+1), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) --start selection date
	AND co.source_claim_rpted_date < Dateadd(m, -@{pipeline().parameters.NUMBER_OF_MONTHS}, CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) --end selection date
	AND 
		   clmt.crrnt_snpsht_flag = 1
	       AND pol.crrnt_snpsht_flag = 1
	       AND clmt_cov.MajorPerilCode = '032' -- for WC claims
	       AND clmt_cov.cause_of_loss in ('05','06')
	       AND co.claim_num <> 'N/A'  --for exceed_claims
	       AND co.source_claim_occurrence_status_code <> 'NO' -- for excluding Notice Only Claims, open and closed claims will be sampled and excess records will be filtered later on
	       and clmt.act_status_code <> 'FA' --federal act claims
	       and clmt.jurisdiction_state_code not in ('CA', 'DE', 'ND', 'OH', 'WA', 'WY','N/A') --Non-DCI states
	 and ncci_extract_tab.claim_num_id is null --we need this condition because if a death claim has already been picked up,we don't want to pick it up again as a death/ptd claim can be indemnity as well
	 GROUP  BY co.edw_claim_occurrence_ak_id,
	       ltrim(rtrim(co.source_claim_occurrence_status_code)),
	       co.source_claim_rpted_date,
	       ltrim(rtrim(co.loss_loc_state)),
	       co.claim_loss_date,
	       ltrim(rtrim(co.wc_cat_code)),
	       ltrim(rtrim(co.claim_num)),
	       pol.edw_pol_ak_id,
	       ltrim(rtrim(pol.pol_key)),
	       pol.pol_eff_date,
	       clmt.edw_claim_party_occurrence_ak_id,
	       ltrim(rtrim(clmt.claimant_status_type)),
	       clmt.claimant_close_date,
	       clmt.claimant_closed_after_reopen_date,
	       ltrim(rtrim(clmt.claimant_full_name)),
	       clmt.claimant_birthdate,
	       ltrim(rtrim(clmt.claimant_gndr)),
	       ltrim(rtrim(clmt.jurisdiction_state_code)),
	       clmt.hired_date,
	       clmt.avg_wkly_wage,
	       ltrim(rtrim(clmt.body_part_code)),
	       ltrim(rtrim(clmt.nature_inj_code)),
	       ltrim(rtrim(clmt.cause_inj_code)),
	       clmt.return_to_work_date,
	       ltrim(rtrim(clmt.controverted_case_code)),
	       ltrim(rtrim(clmt.wc_claimant_num)),
	       ltrim(rtrim(clmt.type_of_loss_code)),
	       ltrim(rtrim(clmt.pre_inj_avg_wkly_wage_code)),
	       clmt.post_inj_wkly_wage_amt,
	       clmt.impairment_disability_percentage,
	       ltrim(rtrim(clmt.impairment_disability_percentage_basis_code)),
	       ltrim(rtrim(clmt.med_extinguishment_ind)),
	      ltrim(rtrim(clmt.crrnt_work_status)),
	       clmt.max_med_improvement_date,
	       clmt_cov.cause_of_loss,
		   ltrim(rtrim(pol.pol_sym)),
		   ltrim(rtrim(COVDET.ClassCode))
),
mplt_CLM_NCCI_Extract_WC_Indemnity_Claims AS (WITH
	LKP_Sampling_Data_Open_Claims AS (
		SELECT
		open_claim_ratio,
		state
		FROM (
			SELECT 
			a.open_claim_ratio  as open_claim_ratio  
			, ltrim(rtrim(a.state))  as state
			FROM 
			work_claim_ncci_report_sampling a
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY state ORDER BY open_claim_ratio) = 1
	),
	LKP_Sampling_Data_Closed_Claims AS (
		SELECT
		close_claim_ratio,
		state
		FROM (
			SELECT 
			a.close_claim_ratio as close_claim_ratio
			, ltrim(rtrim(a.state))  as state
			FROM 
			work_claim_ncci_report_sampling a
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY state ORDER BY close_claim_ratio) = 1
	),
	LKP_Get_State_Code_NCCI AS (
		SELECT
		state_abbrev,
		state_code
		FROM (
			SELECT state_sup.state_abbrev as state_abbrev, ltrim(rtrim(state_sup.state_code)) as state_code FROM state_sup
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_abbrev) = 1
	),
	Mplt_Indemnity_Claims_In AS (
		
	),
	Agg_Source_Data AS (
		SELECT
		claim_occ_ak_id_OR_ncci_rpt_ext_id,
		source_claim_occurrence_status_code,
		source_claim_rpted_date,
		loss_loc_state,
		claim_loss_date,
		claim_num,
		wc_cat_code,
		class_code,
		edw_pol_ak_id,
		pol_sym,
		pol_key,
		pol_eff_date,
		edw_claim_party_occurrence_ak_id,
		claimant_status_type,
		claimant_close_date,
		claimant_closed_after_reopen_date,
		claimant_full_name,
		claimant_birthdate,
		claimant_gndr,
		jurisdiction_state_code,
		hired_date,
		avg_wkly_wage,
		body_part_code,
		nature_inj_code,
		cause_inj_code,
		return_to_work_date,
		controverted_case_code,
		wc_claimant_num,
		type_of_loss_code,
		pre_injury_avg_wkly_wage_code,
		post_inj_wkly_wage_amt,
		impairment_disability_percentage,
		impairment_disability_percentage_basis_code,
		med_extinguishment_ind,
		max_med_improvement_date,
		Incurred_Indemnity_Amount_Total,
		-- *INF*: sum(Incurred_Indemnity_Amount_Total,cause_of_loss = '05')
		sum(Incurred_Indemnity_Amount_Total, cause_of_loss = '05') AS o_Incurred_Indemnity_Amount_Total,
		cause_of_loss,
		-- *INF*: min(cause_of_loss)
		min(cause_of_loss) AS o_cause_of_loss,
		crrnt_work_status,
		trans_ctgry_code,
		-- *INF*: max(trans_ctgry_code)
		-- 
		-- --we are trying to find if there is a trans_ctgry_code = 'SI'
		max(trans_ctgry_code) AS o_trans_ctgry_code,
		direct_subrogation_incurred,
		-- *INF*: sum(direct_subrogation_incurred)
		sum(direct_subrogation_incurred) AS o_direct_subrogation_incurred,
		direct_other_recovery_loss_incurred,
		-- *INF*: sum(direct_other_recovery_loss_incurred)
		sum(direct_other_recovery_loss_incurred) AS o_direct_other_recovery_loss_incurred,
		-- *INF*: IIF(source_claim_occurrence_status_code = 'O' and (claimant_status_type = 'OPEN' or claimant_status_type = 'REOPEN' or claimant_status_type = 'N/A'),'O','C')
		IFF(
		    source_claim_occurrence_status_code = 'O'
		    and (claimant_status_type = 'OPEN'
		    or claimant_status_type = 'REOPEN'
		    or claimant_status_type = 'N/A'),
		    'O',
		    'C'
		) AS Open_Close_Flag
		FROM Mplt_Indemnity_Claims_In
		GROUP BY claim_occ_ak_id_OR_ncci_rpt_ext_id, edw_pol_ak_id, edw_claim_party_occurrence_ak_id
	),
	Fil_Indemnity_Records_Only AS (
		SELECT
		claim_occ_ak_id_OR_ncci_rpt_ext_id, 
		source_claim_occurrence_status_code, 
		source_claim_rpted_date, 
		loss_loc_state, 
		claim_loss_date, 
		claim_num, 
		wc_cat_code, 
		class_code, 
		edw_pol_ak_id, 
		pol_sym, 
		pol_key, 
		pol_eff_date, 
		edw_claim_party_occurrence_ak_id, 
		claimant_status_type, 
		claimant_close_date, 
		claimant_closed_after_reopen_date, 
		claimant_full_name, 
		claimant_birthdate, 
		claimant_gndr, 
		jurisdiction_state_code, 
		hired_date, 
		avg_wkly_wage, 
		body_part_code, 
		nature_inj_code, 
		cause_inj_code, 
		return_to_work_date, 
		controverted_case_code, 
		wc_claimant_num, 
		type_of_loss_code, 
		pre_injury_avg_wkly_wage_code, 
		post_inj_wkly_wage_amt, 
		impairment_disability_percentage, 
		impairment_disability_percentage_basis_code, 
		med_extinguishment_ind, 
		max_med_improvement_date, 
		o_Incurred_Indemnity_Amount_Total AS Incurred_Indemnity_Amount_Total, 
		o_cause_of_loss AS cause_of_loss, 
		crrnt_work_status, 
		o_trans_ctgry_code AS trans_ctgry_code, 
		o_direct_subrogation_incurred AS direct_subrogation_incurred, 
		o_direct_other_recovery_loss_incurred AS direct_other_recovery_loss_incurred, 
		Open_Close_Flag
		FROM Agg_Source_Data
		WHERE cause_of_loss = '05' and  Incurred_Indemnity_Amount_Total <> 0.00
	),
	EXP_Get_Indemnity_Source_Data AS (
		SELECT
		claim_occ_ak_id_OR_ncci_rpt_ext_id,
		source_claim_occurrence_status_code,
		source_claim_rpted_date,
		-- *INF*: to_char(source_claim_rpted_date,'YYYYMMDD')
		to_char(source_claim_rpted_date, 'YYYYMMDD') AS o_source_claim_rpted_date,
		loss_loc_state,
		-- *INF*: :LKP.LKP_Get_State_Code_NCCI(loss_loc_state)
		LKP_GET_STATE_CODE_NCCI_loss_loc_state.state_abbrev AS o_loss_loc_state,
		claim_loss_date,
		-- *INF*: to_char(claim_loss_date,'YYYYMMDD')
		to_char(claim_loss_date, 'YYYYMMDD') AS o_claim_loss_date,
		claim_num,
		wc_cat_code,
		-- *INF*: iif(wc_cat_code < '11' or wc_cat_code = 'N/A'
		-- ,'N'
		-- ,'Y')
		-- 
		-- --iif(wc_cat_code = 'N/A'
		-- --,'N'
		-- --,'Y')
		-- 
		-- --Changes made by Vikas Sood on 08/04/2011 for DDRR-71. Extraordinary loss indicator will be set to Y only when wc_cat_code >--=11
		IFF(wc_cat_code < '11' or wc_cat_code = 'N/A', 'N', 'Y') AS o_wc_cat_code,
		class_code AS risk_unit,
		edw_pol_ak_id,
		pol_sym,
		pol_key,
		pol_eff_date,
		-- *INF*: to_char(pol_eff_date,'yyyymmdd')
		to_char(pol_eff_date, 'yyyymmdd') AS o_pol_eff_date,
		edw_claim_party_occurrence_ak_id,
		claimant_status_type,
		claimant_close_date,
		claimant_closed_after_reopen_date,
		-- *INF*: iif(source_claim_occurrence_status_code = 'O'
		--      ,iif(claimant_status_type = 'OPEN' or claimant_status_type = 'REOPEN' or claimant_status_type = 'N/A'
		-- 		,'0'
		-- 		,'1')
		-- 	,'1')
		IFF(
		    source_claim_occurrence_status_code = 'O',
		    IFF(
		        claimant_status_type = 'OPEN'
		        or claimant_status_type = 'REOPEN'
		        or claimant_status_type = 'N/A',
		        '0',
		        '1'
		    ),
		    '1'
		) AS v_claim_status_code,
		-- *INF*: iif(v_claim_status_code = '1'
		--     ,iif(claimant_closed_after_reopen_date = to_date('18000101','YYYYMMDD')
		-- 		,to_char(claimant_close_date,'YYYYMMDD')
		--             ,to_char(claimant_closed_after_reopen_date,'YYYYMMDD')
		-- )
		--     ,'00000000')
		-- 
		-- --Report the most recent date as of loss valuation that claim was closed only if Claim Status Code is reported as 1 Closed.
		-- -- Zero-fill if the Claim Status Code  is 0Open. 
		-- 
		IFF(
		    v_claim_status_code = '1',
		    IFF(
		        claimant_closed_after_reopen_date = TO_TIMESTAMP('18000101', 'YYYYMMDD'),
		        to_char(claimant_close_date, 'YYYYMMDD'),
		        to_char(claimant_closed_after_reopen_date, 'YYYYMMDD')
		    ),
		    '00000000'
		) AS o_claimant_close_date,
		claimant_full_name,
		claimant_birthdate,
		claimant_gndr,
		jurisdiction_state_code,
		-- *INF*: :LKP.LKP_Get_State_Code_NCCI(jurisdiction_state_code)
		LKP_GET_STATE_CODE_NCCI_jurisdiction_state_code.state_abbrev AS jurisdiction_state_code_converted,
		hired_date,
		-- *INF*: iif(hired_date = to_date('01/01/1800','MM/DD/YYYY')
		-- ,'00000000'
		-- ,to_char(hired_date,'YYYYMMDD'))
		IFF(
		    hired_date = TO_TIMESTAMP('01/01/1800', 'MM/DD/YYYY'), '00000000',
		    to_char(hired_date, 'YYYYMMDD')
		) AS o_hired_date,
		avg_wkly_wage,
		-- *INF*: iif(isnull(avg_wkly_wage)
		--      ,'00000'
		--      ,iif(avg_wkly_wage > 99999
		--           ,'99999'
		--         ,lpad(to_char(to_integer(avg_wkly_wage)),5,'0')
		-- )
		-- )
		-- 
		IFF(
		    avg_wkly_wage IS NULL, '00000',
		    IFF(
		        avg_wkly_wage > 99999, '99999',
		        lpad(to_char(CAST(avg_wkly_wage AS INTEGER)), 5, '0')
		    )
		) AS o_avg_wkly_wage,
		body_part_code,
		nature_inj_code,
		cause_inj_code,
		return_to_work_date,
		-- *INF*: iif(upper(crrnt_work_status) = 'RESTRICTIONS WITHOUT WAGE LOSS' OR upper(crrnt_work_status) = 'FULL DUTY'
		-- 	,'Y'
		-- 	,iif(upper(crrnt_work_status) = 'RESTRICTIONS WITH WAGE LOSS' 
		-- 		,'N'
		-- 		,iif(upper(crrnt_work_status) = 'TOTALLY DISABLED'
		-- 			,' '
		--     		     )
		-- 	)
		-- )
		IFF(
		    upper(crrnt_work_status) = 'RESTRICTIONS WITHOUT WAGE LOSS'
		    or upper(crrnt_work_status) = 'FULL DUTY',
		    'Y',
		    IFF(
		        upper(crrnt_work_status) = 'RESTRICTIONS WITH WAGE LOSS', 'N',
		        IFF(
		            upper(crrnt_work_status) = 'TOTALLY DISABLED', ' '
		        )
		    )
		) AS v_ret_to_work_rate_of_pay_ind,
		-- *INF*: iif(to_char(return_to_work_date,'YYYYMMDD')  !=  '18000101' AND v_ret_to_work_rate_of_pay_ind='Y'
		-- ,to_char(return_to_work_date,'YYYYMMDD')
		-- ,'00000000')
		IFF(
		    to_char(return_to_work_date, 'YYYYMMDD') != '18000101' AND v_ret_to_work_rate_of_pay_ind = 'Y',
		    to_char(return_to_work_date, 'YYYYMMDD'),
		    '00000000'
		) AS v_return_to_work_date,
		-- *INF*: v_return_to_work_date
		-- 
		-- --iif(to_char(return_to_work_date,'YYYYMMDD')  !=  '18000101' AND (v_claim_status_code = '0' or  v_claim_status_code = '1')
		-- --,to_char(return_to_work_date,'YYYYMMDD')
		-- --,'00000000')
		-- 
		-- --iif(to_char(return_to_work_date,'YYYYMMDD')  !=  '18000101' AND (v_claim_status_code = '0' or  v_claim_status_code = '1' OR v_claim_status_code = '5')
		-- --,to_char(return_to_work_date,'YYYYMMDD')
		-- --,'00000000')
		-- 
		-- --changes made by Vikas Sood on 08/01/2011 to remove v_claim_status_code = '5' from IIF condition as if the claim is medical only, we don't report retrun to work date. DDRR-70 was created to address this
		v_return_to_work_date AS o_return_to_work_date,
		controverted_case_code,
		-- *INF*: iif(controverted_case_code = 'N/A'
		-- ,' '
		-- ,controverted_case_code)
		IFF(controverted_case_code = 'N/A', ' ', controverted_case_code) AS o_controverted_case_code,
		wc_claimant_num,
		type_of_loss_code,
		-- *INF*: iif(type_of_loss_code = 'N/A'
		-- ,'00'
		-- ,type_of_loss_code)
		IFF(type_of_loss_code = 'N/A', '00', type_of_loss_code) AS o_type_of_loss_code,
		pre_injury_avg_wkly_wage_code,
		-- *INF*: iif(pre_injury_avg_wkly_wage_code = 'N/A'
		-- ,'1'
		-- ,ltrim(pre_injury_avg_wkly_wage_code,'0'))
		IFF(pre_injury_avg_wkly_wage_code = 'N/A', '1', ltrim(pre_injury_avg_wkly_wage_code, '0')) AS o_pre_injury_avg_wkly_wage_code,
		post_inj_wkly_wage_amt,
		-- *INF*: iif(isnull(post_inj_wkly_wage_amt)
		-- ,'000000000'
		-- ,lpad(to_char(ceil(post_inj_wkly_wage_amt)),9,'0')
		-- )
		IFF(
		    post_inj_wkly_wage_amt IS NULL, '000000000',
		    lpad(to_char(ceil(post_inj_wkly_wage_amt)), 9, '0')
		) AS o_post_inj_wkly_wage_amt,
		impairment_disability_percentage,
		-- *INF*: iif(isnull(impairment_disability_percentage)
		-- ,'000'
		-- ,lpad(to_char(ceil(impairment_disability_percentage)),3,'0')
		-- )
		IFF(
		    impairment_disability_percentage IS NULL, '000',
		    lpad(to_char(ceil(impairment_disability_percentage)), 3, '0')
		) AS o_impairment_disability_percentage,
		impairment_disability_percentage_basis_code,
		max_med_improvement_date,
		med_extinguishment_ind,
		-- *INF*: iif(med_extinguishment_ind = 'N/A'
		-- ,' '
		-- ,med_extinguishment_ind)
		-- 
		IFF(med_extinguishment_ind = 'N/A', ' ', med_extinguishment_ind) AS o_med_extinguishment_ind,
		crrnt_work_status,
		v_ret_to_work_rate_of_pay_ind AS ret_to_work_rate_of_pay_ind,
		-- *INF*: IIF(ISNULL(max_med_improvement_date) OR to_char(max_med_improvement_date,'YYYYMMDD') = '18000101'
		-- ,'00000000'
		-- ,to_char(max_med_improvement_date,'YYYYMMDD')
		-- )
		IFF(
		    max_med_improvement_date IS NULL OR to_char(max_med_improvement_date, 'YYYYMMDD') = '18000101',
		    '00000000',
		    to_char(max_med_improvement_date, 'YYYYMMDD')
		) AS o_max_med_improvement_date,
		trans_ctgry_code,
		Incurred_Indemnity_Amount_Total,
		-- *INF*: iif(isnull(Incurred_Indemnity_Amount_Total)
		-- ,'000000000'
		-- ,lpad(to_char(to_integer(Incurred_Indemnity_Amount_Total)),9,'0')
		-- )
		-- 
		-- 
		IFF(
		    Incurred_Indemnity_Amount_Total IS NULL, '000000000',
		    lpad(to_char(CAST(Incurred_Indemnity_Amount_Total AS INTEGER)), 9, '0')
		) AS o_Incurred_Indemnity_Amount_Total,
		direct_subrogation_incurred,
		direct_other_recovery_loss_incurred,
		v_claim_status_code AS o_claim_status_code,
		-- *INF*: iif(direct_subrogation_incurred = 0
		--      ,iif(direct_other_recovery_loss_incurred = 0 
		--           ,'01'
		--           ,iif(direct_other_recovery_loss_incurred < 0 and trans_ctgry_code = 'SI'
		--                ,'02'
		--               ,iif(direct_other_recovery_loss_incurred < 0 and trans_ctgry_code <> 'SI'
		--                     ,'01'
		--                     ,'01'))
		--           )
		--     ,iif(direct_other_recovery_loss_incurred = 0 --and trans_ctgry_code = 'SI'
		--           ,'03'
		--         ,iif(direct_other_recovery_loss_incurred < 0 and trans_ctgry_code = 'SI'
		--           ,'04'
		--           ,'01')
		--         )
		-- )
		-- 
		-- 
		-- --if no recovery exists (i.e. no subrogation recovery and no other recovery for second injury fund, populate 01, if no subrogation recovery exists but SI recovery exists, populate 02,populate 03 when subrogation recovery exists but no other type of recovery exists (whether it is SI or not) and populate 04 when subrogation recovery exists as well SI recovery exists.
		IFF(
		    direct_subrogation_incurred = 0,
		    IFF(
		        direct_other_recovery_loss_incurred = 0, '01',
		        IFF(
		            direct_other_recovery_loss_incurred < 0
		        and trans_ctgry_code = 'SI', '02',
		            IFF(
		                direct_other_recovery_loss_incurred < 0
		            and trans_ctgry_code <> 'SI',
		                '01',
		                '01'
		            )
		        )
		    ),
		    IFF(
		        direct_other_recovery_loss_incurred = 0, '03',
		        IFF(
		            direct_other_recovery_loss_incurred < 0
		        and trans_ctgry_code = 'SI', '04',
		            '01'
		        )
		    )
		) AS Loss_Condition_Code,
		-- *INF*: iif(v_claim_status_code = '0'
		-- ,'Y'
		-- ,'O')
		-- 
		-- --status 'O' refers to one time transmission only. So any closed claims will be reported one time only
		IFF(v_claim_status_code = '0', 'Y', 'O') AS trans_status,
		cause_of_loss
		FROM Fil_Indemnity_Records_Only
		LEFT JOIN LKP_GET_STATE_CODE_NCCI LKP_GET_STATE_CODE_NCCI_loss_loc_state
		ON LKP_GET_STATE_CODE_NCCI_loss_loc_state.state_code = loss_loc_state
	
		LEFT JOIN LKP_GET_STATE_CODE_NCCI LKP_GET_STATE_CODE_NCCI_jurisdiction_state_code
		ON LKP_GET_STATE_CODE_NCCI_jurisdiction_state_code.state_code = jurisdiction_state_code
	
	),
	Agg_Claim_Count_Per_State AS (
		SELECT
		jurisdiction_state_code,
		Open_Close_Flag AS Open_Closed_Flag,
		-- *INF*: count(jurisdiction_state_code,
		-- Open_Closed_Flag='O'
		-- )
		count(jurisdiction_state_code, Open_Closed_Flag = 'O') AS Total_Claims_Open_Count_Per_State,
		-- *INF*: count(jurisdiction_state_code,
		-- Open_Closed_Flag='C'
		-- )
		count(jurisdiction_state_code, Open_Closed_Flag = 'C') AS Total_Claims_Closed_Count_Per_State
		FROM Fil_Indemnity_Records_Only
		GROUP BY jurisdiction_state_code
	),
	EXP_Calculate_Valuation_Levels AS (
		SELECT
		source_claim_rpted_date AS Input_Date,
		-- *INF*: to_integer(date_diff(sysdate,Input_Date,'mm'),true)
		-- 
		-- --date_diff function will calculate the differnce upto 2 decimal places. For the purpose of deriving valuation level codes, we don't want decimal places. Part of the fix for DDRR-65
		CAST(DATEDIFF(mm,CURRENT_TIMESTAMP,Input_Date) AS INTEGER) AS Difference_Months,
		-- *INF*: decode(true,
		-- Difference_Months <=19,'018',
		-- Difference_Months <=31,'030',
		-- Difference_Months <=43,'042',
		-- Difference_Months <=55,'054',
		-- Difference_Months <=67,'066',
		-- Difference_Months <=79,'078',
		-- Difference_Months <=91,'090',
		-- Difference_Months <=103,'102',
		-- Difference_Months <=115,'114',
		-- Difference_Months <=127,'126',
		-- Difference_Months <=139,'138',
		-- '999')
		-- 
		-- --'999' is dummy coz we don't need to send claims after 138 months (last valuation)
		decode(
		    true,
		    Difference_Months <= 19, '018',
		    Difference_Months <= 31, '030',
		    Difference_Months <= 43, '042',
		    Difference_Months <= 55, '054',
		    Difference_Months <= 67, '066',
		    Difference_Months <= 79, '078',
		    Difference_Months <= 91, '090',
		    Difference_Months <= 103, '102',
		    Difference_Months <= 115, '114',
		    Difference_Months <= 127, '126',
		    Difference_Months <= 139, '138',
		    '999'
		) AS Valuation_Level_Code
		FROM EXP_Get_Indemnity_Source_Data
	),
	EXP_Lookup_Sampling_Table AS (
		SELECT
		jurisdiction_state_code,
		Total_Claims_Open_Count_Per_State,
		Total_Claims_Closed_Count_Per_State,
		-- *INF*: :LKP.LKP_SAMPLING_DATA_OPEN_CLAIMS(jurisdiction_state_code)
		LKP_SAMPLING_DATA_OPEN_CLAIMS_jurisdiction_state_code.open_claim_ratio AS Sampling_Ratio_Open_Claims_Per_State,
		-- *INF*: :LKP.LKP_SAMPLING_DATA_CLOSED_CLAIMS(jurisdiction_state_code)
		LKP_SAMPLING_DATA_CLOSED_CLAIMS_jurisdiction_state_code.close_claim_ratio AS Sampling_Ratio_Closed_Claims_Per_State,
		-- *INF*: to_integer(((Sampling_Ratio_Open_Claims_Per_State/100)*Total_Claims_Open_Count_Per_State),false)
		CAST(((Sampling_Ratio_Open_Claims_Per_State / 100) * Total_Claims_Open_Count_Per_State) AS INTEGER) AS v_Open_Claims_Count_After_Sampling,
		-- *INF*: iif(v_Open_Claims_Count_After_Sampling < 1
		-- ,1
		-- ,v_Open_Claims_Count_After_Sampling)
		-- 
		IFF(v_Open_Claims_Count_After_Sampling < 1, 1, v_Open_Claims_Count_After_Sampling) AS Open_Claims_Count_After_Sampling,
		-- *INF*: to_integer(((Sampling_Ratio_Closed_Claims_Per_State/100)*Total_Claims_Closed_Count_Per_State),false)
		CAST(((Sampling_Ratio_Closed_Claims_Per_State / 100) * Total_Claims_Closed_Count_Per_State) AS INTEGER) AS v_Close_Claims_Count_After_Sampling,
		-- *INF*: iif(v_Close_Claims_Count_After_Sampling< 1 ,1 ,v_Close_Claims_Count_After_Sampling) 
		IFF(v_Close_Claims_Count_After_Sampling < 1, 1, v_Close_Claims_Count_After_Sampling) AS Close_Claims_Count_After_Sampling
		FROM Agg_Claim_Count_Per_State
		LEFT JOIN LKP_SAMPLING_DATA_OPEN_CLAIMS LKP_SAMPLING_DATA_OPEN_CLAIMS_jurisdiction_state_code
		ON LKP_SAMPLING_DATA_OPEN_CLAIMS_jurisdiction_state_code.state = jurisdiction_state_code
	
		LEFT JOIN LKP_SAMPLING_DATA_CLOSED_CLAIMS LKP_SAMPLING_DATA_CLOSED_CLAIMS_jurisdiction_state_code
		ON LKP_SAMPLING_DATA_CLOSED_CLAIMS_jurisdiction_state_code.state = jurisdiction_state_code
	
	),
	SRT_Sort_Indemnity_Data AS (
		SELECT
		EXP_Get_Indemnity_Source_Data.claim_occ_ak_id_OR_ncci_rpt_ext_id AS edw_claim_occurrence_ak_id, 
		EXP_Get_Indemnity_Source_Data.o_source_claim_rpted_date AS source_claim_rpted_date, 
		EXP_Get_Indemnity_Source_Data.o_loss_loc_state AS loss_loc_state, 
		EXP_Get_Indemnity_Source_Data.o_claim_loss_date AS claim_loss_date, 
		EXP_Get_Indemnity_Source_Data.claim_num, 
		EXP_Get_Indemnity_Source_Data.o_wc_cat_code AS wc_cat_code, 
		EXP_Get_Indemnity_Source_Data.risk_unit AS ClassCode, 
		EXP_Get_Indemnity_Source_Data.edw_pol_ak_id, 
		EXP_Get_Indemnity_Source_Data.pol_sym, 
		EXP_Get_Indemnity_Source_Data.pol_key, 
		EXP_Get_Indemnity_Source_Data.o_pol_eff_date AS pol_eff_date, 
		EXP_Get_Indemnity_Source_Data.edw_claim_party_occurrence_ak_id, 
		EXP_Get_Indemnity_Source_Data.o_claimant_close_date AS claimant_close_date, 
		EXP_Get_Indemnity_Source_Data.claimant_full_name, 
		EXP_Get_Indemnity_Source_Data.claimant_birthdate, 
		EXP_Get_Indemnity_Source_Data.claimant_gndr, 
		EXP_Get_Indemnity_Source_Data.jurisdiction_state_code AS jurisdiction_state_code1, 
		EXP_Get_Indemnity_Source_Data.jurisdiction_state_code_converted, 
		EXP_Get_Indemnity_Source_Data.o_hired_date AS hired_date, 
		EXP_Get_Indemnity_Source_Data.o_avg_wkly_wage AS avg_wkly_wage, 
		EXP_Get_Indemnity_Source_Data.body_part_code, 
		EXP_Get_Indemnity_Source_Data.nature_inj_code, 
		EXP_Get_Indemnity_Source_Data.cause_inj_code, 
		EXP_Get_Indemnity_Source_Data.o_return_to_work_date AS return_to_work_date, 
		EXP_Get_Indemnity_Source_Data.o_controverted_case_code AS controverted_case_code, 
		EXP_Get_Indemnity_Source_Data.wc_claimant_num, 
		EXP_Get_Indemnity_Source_Data.o_type_of_loss_code AS type_of_loss_code, 
		EXP_Get_Indemnity_Source_Data.o_pre_injury_avg_wkly_wage_code AS pre_injury_avg_wkly_wage_code, 
		EXP_Get_Indemnity_Source_Data.o_post_inj_wkly_wage_amt AS post_inj_wkly_wage_amt, 
		EXP_Get_Indemnity_Source_Data.o_impairment_disability_percentage AS impairment_disability_percentage, 
		EXP_Get_Indemnity_Source_Data.impairment_disability_percentage_basis_code, 
		EXP_Get_Indemnity_Source_Data.o_med_extinguishment_ind AS med_extinguishment_ind, 
		EXP_Get_Indemnity_Source_Data.ret_to_work_rate_of_pay_ind, 
		EXP_Get_Indemnity_Source_Data.o_max_med_improvement_date AS max_med_improvement_date, 
		EXP_Get_Indemnity_Source_Data.o_Incurred_Indemnity_Amount_Total AS Incurred_Indemnity_Amount_Total, 
		EXP_Get_Indemnity_Source_Data.o_claim_status_code AS claim_status_code, 
		EXP_Get_Indemnity_Source_Data.trans_status, 
		EXP_Get_Indemnity_Source_Data.Loss_Condition_Code, 
		EXP_Calculate_Valuation_Levels.Valuation_Level_Code AS valuation_level_code
		FROM EXP_Get_Indemnity_Source_Data
		ORDER BY jurisdiction_state_code1 ASC, claim_status_code ASC
	),
	SRT_Sort_States AS (
		SELECT
		jurisdiction_state_code, 
		Open_Claims_Count_After_Sampling, 
		Close_Claims_Count_After_Sampling
		FROM EXP_Lookup_Sampling_Table
		ORDER BY jurisdiction_state_code ASC
	),
	JNR_Join_Back_With_Indemnity_Claims AS (SELECT
		SRT_Sort_Indemnity_Data.edw_claim_occurrence_ak_id, 
		SRT_Sort_Indemnity_Data.source_claim_rpted_date, 
		SRT_Sort_Indemnity_Data.loss_loc_state, 
		SRT_Sort_Indemnity_Data.claim_loss_date, 
		SRT_Sort_Indemnity_Data.claim_num, 
		SRT_Sort_Indemnity_Data.wc_cat_code, 
		SRT_Sort_Indemnity_Data.ClassCode, 
		SRT_Sort_Indemnity_Data.edw_pol_ak_id, 
		SRT_Sort_Indemnity_Data.pol_sym, 
		SRT_Sort_Indemnity_Data.pol_key, 
		SRT_Sort_Indemnity_Data.pol_eff_date, 
		SRT_Sort_Indemnity_Data.edw_claim_party_occurrence_ak_id, 
		SRT_Sort_Indemnity_Data.claimant_close_date, 
		SRT_Sort_Indemnity_Data.claimant_full_name, 
		SRT_Sort_Indemnity_Data.claimant_birthdate, 
		SRT_Sort_Indemnity_Data.claimant_gndr, 
		SRT_Sort_Indemnity_Data.jurisdiction_state_code1 AS jurisdiction_state_code11, 
		SRT_Sort_Indemnity_Data.jurisdiction_state_code_converted, 
		SRT_Sort_Indemnity_Data.hired_date, 
		SRT_Sort_Indemnity_Data.avg_wkly_wage, 
		SRT_Sort_Indemnity_Data.body_part_code, 
		SRT_Sort_Indemnity_Data.nature_inj_code, 
		SRT_Sort_Indemnity_Data.cause_inj_code, 
		SRT_Sort_Indemnity_Data.return_to_work_date, 
		SRT_Sort_Indemnity_Data.controverted_case_code, 
		SRT_Sort_Indemnity_Data.wc_claimant_num, 
		SRT_Sort_Indemnity_Data.type_of_loss_code, 
		SRT_Sort_Indemnity_Data.pre_injury_avg_wkly_wage_code, 
		SRT_Sort_Indemnity_Data.post_inj_wkly_wage_amt, 
		SRT_Sort_Indemnity_Data.impairment_disability_percentage, 
		SRT_Sort_Indemnity_Data.impairment_disability_percentage_basis_code, 
		SRT_Sort_Indemnity_Data.med_extinguishment_ind, 
		SRT_Sort_Indemnity_Data.ret_to_work_rate_of_pay_ind, 
		SRT_Sort_Indemnity_Data.max_med_improvement_date, 
		SRT_Sort_Indemnity_Data.Incurred_Indemnity_Amount_Total, 
		SRT_Sort_Indemnity_Data.claim_status_code, 
		SRT_Sort_Indemnity_Data.trans_status, 
		SRT_Sort_Indemnity_Data.Loss_Condition_Code, 
		SRT_Sort_Indemnity_Data.valuation_level_code, 
		SRT_Sort_States.jurisdiction_state_code AS jurisdiction_state_code1, 
		SRT_Sort_States.Open_Claims_Count_After_Sampling, 
		SRT_Sort_States.Close_Claims_Count_After_Sampling
		FROM SRT_Sort_Indemnity_Data
		INNER JOIN SRT_Sort_States
		ON SRT_Sort_States.jurisdiction_state_code = SRT_Sort_Indemnity_Data.jurisdiction_state_code1
	),
	Exp_Apply_Sampling_Rules AS (
		SELECT
		edw_claim_occurrence_ak_id,
		source_claim_rpted_date,
		loss_loc_state,
		claim_loss_date,
		claim_num,
		wc_cat_code,
		ClassCode,
		edw_pol_ak_id,
		pol_sym,
		pol_key,
		pol_eff_date,
		edw_claim_party_occurrence_ak_id,
		claimant_close_date,
		claimant_full_name,
		claimant_birthdate,
		claimant_gndr,
		jurisdiction_state_code_converted,
		hired_date,
		avg_wkly_wage,
		body_part_code,
		nature_inj_code,
		cause_inj_code,
		return_to_work_date,
		controverted_case_code,
		wc_claimant_num,
		type_of_loss_code,
		pre_injury_avg_wkly_wage_code,
		post_inj_wkly_wage_amt,
		impairment_disability_percentage,
		impairment_disability_percentage_basis_code,
		med_extinguishment_ind,
		ret_to_work_rate_of_pay_ind,
		max_med_improvement_date,
		Incurred_Indemnity_Amount_Total,
		claim_status_code,
		trans_status,
		Loss_Condition_Code,
		-- *INF*: iif(claim_status_code = '1'
		-- ,0
		-- ,iif(jurisdiction_state_code_converted=v_state_code and claim_status_code = '0'
		-- 	    ,v_open_count +1
		--           ,1)
		-- )
		-- --for a particular state, keep a count on open claims
		IFF(
		    claim_status_code = '1', 0,
		    IFF(
		        jurisdiction_state_code_converted = v_state_code
		    and claim_status_code = '0',
		        v_open_count + 1,
		        1
		    )
		) AS v_open_count,
		-- *INF*: iif(claim_status_code = '0'
		-- ,0
		-- ,iif(jurisdiction_state_code_converted=v_state_code and claim_status_code = '1'
		-- 	    ,v_close_count +1
		--           ,1)
		-- )
		-- --for a particular state, keep a count on closed claims
		IFF(
		    claim_status_code = '0', 0,
		    IFF(
		        jurisdiction_state_code_converted = v_state_code
		    and claim_status_code = '1',
		        v_close_count + 1,
		        1
		    )
		) AS v_close_count,
		jurisdiction_state_code_converted AS v_state_code,
		claim_status_code AS v_claim_status,
		valuation_level_code,
		v_open_count AS o_open_count,
		v_close_count AS o_close_count,
		Open_Claims_Count_After_Sampling,
		Close_Claims_Count_After_Sampling
		FROM JNR_Join_Back_With_Indemnity_Claims
	),
	FIL_Apply_Sampling_Filter AS (
		SELECT
		edw_claim_occurrence_ak_id, 
		source_claim_rpted_date, 
		loss_loc_state, 
		claim_loss_date, 
		claim_num, 
		wc_cat_code, 
		ClassCode, 
		edw_pol_ak_id, 
		pol_sym, 
		pol_key, 
		pol_eff_date, 
		edw_claim_party_occurrence_ak_id, 
		claimant_close_date, 
		claimant_full_name, 
		claimant_birthdate, 
		claimant_gndr, 
		jurisdiction_state_code_converted, 
		hired_date, 
		avg_wkly_wage, 
		body_part_code, 
		nature_inj_code, 
		cause_inj_code, 
		return_to_work_date, 
		controverted_case_code, 
		wc_claimant_num, 
		type_of_loss_code, 
		pre_injury_avg_wkly_wage_code, 
		post_inj_wkly_wage_amt, 
		impairment_disability_percentage, 
		impairment_disability_percentage_basis_code, 
		med_extinguishment_ind, 
		ret_to_work_rate_of_pay_ind, 
		max_med_improvement_date, 
		Incurred_Indemnity_Amount_Total, 
		claim_status_code, 
		trans_status, 
		Loss_Condition_Code, 
		valuation_level_code, 
		o_open_count, 
		o_close_count, 
		Open_Claims_Count_After_Sampling, 
		Close_Claims_Count_After_Sampling
		FROM Exp_Apply_Sampling_Rules
		WHERE iif(@{pipeline().parameters.SAMPLING_FILTER} = 'Y'
	,Incurred_Indemnity_Amount_Total> '0' and (o_open_count<=Open_Claims_Count_After_Sampling
	and o_close_count<=Close_Claims_Count_After_Sampling)  
	,Incurred_Indemnity_Amount_Total> '0'  )
	
	--SAMPLING FILTER would be set to Y when we are picking up the indemnity claims the first time. However when we run the process the second time in the month, we don't want to filter it again so the SAMPLING filter at that time would be set to  a 'N'. There should be some open reserve and sampling rules are applied on open as well as closed claims to pick only a handful of claims. Sampling ratios are dictated by NCCI.
	),
	Mplt_Indemnity_Claims_Out AS (
		SELECT
		edw_claim_occurrence_ak_id AS claim_occ_ak_id_OR_ncci_rpt_ext_id, 
		source_claim_rpted_date, 
		loss_loc_state, 
		claim_loss_date, 
		claim_num, 
		wc_cat_code, 
		ClassCode, 
		edw_pol_ak_id, 
		pol_sym, 
		pol_key, 
		pol_eff_date, 
		edw_claim_party_occurrence_ak_id, 
		claimant_close_date, 
		claimant_full_name, 
		claimant_birthdate, 
		claimant_gndr, 
		jurisdiction_state_code_converted, 
		hired_date, 
		avg_wkly_wage, 
		body_part_code, 
		nature_inj_code, 
		cause_inj_code, 
		return_to_work_date, 
		controverted_case_code, 
		wc_claimant_num, 
		type_of_loss_code, 
		pre_injury_avg_wkly_wage_code, 
		post_inj_wkly_wage_amt, 
		impairment_disability_percentage, 
		impairment_disability_percentage_basis_code, 
		med_extinguishment_ind, 
		ret_to_work_rate_of_pay_ind, 
		max_med_improvement_date, 
		Incurred_Indemnity_Amount_Total, 
		claim_status_code, 
		trans_status, 
		Loss_Condition_Code, 
		valuation_level_code, 
		o_open_count, 
		o_close_count, 
		Open_Claims_Count_After_Sampling, 
		Close_Claims_Count_After_Sampling
		FROM FIL_Apply_Sampling_Filter
	),
),
JNR_Join_Medical_Data_With_Indemnity_Claims AS (SELECT
	EXP_Get_Medical_Source_Data.edw_claim_occurrence_ak_id AS edw_claim_occurrence_ak_id1, 
	EXP_Get_Medical_Source_Data.edw_pol_ak_id AS edw_pol_ak_id1, 
	EXP_Get_Medical_Source_Data.edw_claim_party_occurrence_ak_id AS edw_claim_party_occurrence_ak_id1, 
	EXP_Get_Medical_Source_Data.o_Total_Paid_Medical_Amount AS Total_Paid_Medical_Amount, 
	EXP_Get_Medical_Source_Data.o_Incurred_Medical_Amount_Total AS Incurred_Medical_Amount_Total, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.claim_occ_ak_id_OR_ncci_rpt_ext_id1 AS edw_claim_occurrence_ak_id, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.source_claim_rpted_date1 AS source_claim_rpted_date, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.loss_loc_state1 AS loss_loc_state, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.claim_loss_date1 AS claim_loss_date, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.claim_num1 AS claim_num, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.wc_cat_code1 AS wc_cat_code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.ClassCode, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.edw_pol_ak_id1 AS edw_pol_ak_id, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.pol_sym1 AS pol_sym, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.pol_key1 AS pol_key, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.pol_eff_date1 AS pol_eff_date, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.edw_claim_party_occurrence_ak_id1 AS edw_claim_party_occurrence_ak_id, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.claimant_close_date1 AS claimant_close_date, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.claimant_full_name1 AS claimant_full_name, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.claimant_birthdate1 AS claimant_birthdate, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.claimant_gndr1 AS claimant_gndr, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.jurisdiction_state_code_converted, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.hired_date1 AS hired_date, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.avg_wkly_wage1 AS avg_wkly_wage, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.body_part_code1 AS body_part_code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.nature_inj_code1 AS nature_inj_code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.cause_inj_code1 AS cause_inj_code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.return_to_work_date1 AS return_to_work_date, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.controverted_case_code1 AS controverted_case_code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.wc_claimant_num1 AS wc_claimant_num, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.type_of_loss_code1 AS type_of_loss_code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.pre_injury_avg_wkly_wage_code1 AS pre_injury_avg_wkly_wage_code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.post_inj_wkly_wage_amt1 AS post_inj_wkly_wage_amt, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.impairment_disability_percentage1 AS impairment_disability_percentage, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.impairment_disability_percentage_basis_code1 AS impairment_disability_percentage_basis_code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.med_extinguishment_ind1 AS med_extinguishment_ind, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.ret_to_work_rate_of_pay_ind, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.max_med_improvement_date1 AS max_med_improvement_date, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.Incurred_Indemnity_Amount_Total1 AS Incurred_Indemnity_Amount_Total, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.claim_status_code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.trans_status, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.Loss_Condition_Code, 
	mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.valuation_level_code
	FROM EXP_Get_Medical_Source_Data
	RIGHT OUTER JOIN mplt_CLM_NCCI_Extract_WC_Indemnity_Claims
	ON mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.claim_occ_ak_id_OR_ncci_rpt_ext_id1 = EXP_Get_Medical_Source_Data.edw_claim_occurrence_ak_id AND mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.edw_claim_party_occurrence_ak_id1 = EXP_Get_Medical_Source_Data.edw_claim_party_occurrence_ak_id AND mplt_CLM_NCCI_Extract_WC_Indemnity_Claims.edw_pol_ak_id1 = EXP_Get_Medical_Source_Data.edw_pol_ak_id
),
EXP_NCCI_Target AS (
	SELECT
	-- *INF*: '1'
	-- 
	-- --hardcoded as per specs
	'1' AS record_type_code,
	-- *INF*: '17124'
	-- --hardcoded as per specs
	'17124' AS carrier_code,
	-- *INF*: ' '
	-- --hardcoded as per specs
	' ' AS future_use3,
	valuation_level_code,
	' ' AS replacement_report_code,
	' ' AS future_use9,
	' ' AS future_use21,
	'01' AS type_of_claim,
	' ' AS future_use45,
	' ' AS future_use76,
	' ' AS future_use78,
	' ' AS future_use84,
	' ' AS Future_use,
	edw_claim_party_occurrence_ak_id,
	edw_pol_ak_id,
	edw_claim_occurrence_ak_id,
	pol_key,
	pol_eff_date,
	claim_num,
	jurisdiction_state_code_converted AS jurisdiction_state_code,
	loss_loc_state,
	claim_loss_date,
	source_claim_rpted_date,
	type_of_loss_code,
	claimant_gndr,
	-- *INF*: decode(true,
	-- claimant_gndr = 'M','1',
	-- claimant_gndr = 'F','2',
	-- '3')
	-- 
	decode(
	    true,
	    claimant_gndr = 'M', '1',
	    claimant_gndr = 'F', '2',
	    '3'
	) AS o_claimant_gndr,
	claimant_birthdate,
	-- *INF*: iif(TO_CHAR(GET_DATE_PART(claimant_birthdate,'YYYY')) = '9999'
	-- ,'0000'
	-- ,TO_CHAR(GET_DATE_PART(claimant_birthdate,'YYYY')))
	IFF(
	    TO_CHAR(DATE_PART(claimant_birthdate, 'YYYY')) = '9999', '0000',
	    TO_CHAR(DATE_PART(claimant_birthdate, 'YYYY'))
	) AS birth_yr,
	hired_date,
	-- *INF*: SUBSTR(hired_date,1,4)
	SUBSTR(hired_date, 1, 4) AS hired_yr,
	avg_wkly_wage,
	pre_injury_avg_wkly_wage_code,
	body_part_code,
	nature_inj_code,
	cause_inj_code,
	claim_status_code,
	claimant_close_date,
	Incurred_Indemnity_Amount_Total,
	Total_Paid_Medical_Amount,
	-- *INF*: iif(isnull(Total_Paid_Medical_Amount)
	-- ,'000000000'
	-- ,Total_Paid_Medical_Amount)
	IFF(Total_Paid_Medical_Amount IS NULL, '000000000', Total_Paid_Medical_Amount) AS o_Total_Paid_Medical_Amount,
	Incurred_Medical_Amount_Total,
	-- *INF*: iif(isnull(Incurred_Medical_Amount_Total)
	-- ,'000000000'
	-- ,Incurred_Medical_Amount_Total)
	IFF(Incurred_Medical_Amount_Total IS NULL, '000000000', Incurred_Medical_Amount_Total) AS o_Incurred_Medical_Amount_Total,
	wc_cat_code,
	return_to_work_date,
	-- *INF*: iif(claim_status_code = '5' ,'00000000' ,return_to_work_date)
	IFF(claim_status_code = '5', '00000000', return_to_work_date) AS v_return_to_work_date,
	v_return_to_work_date AS o_return_to_work_date,
	controverted_case_code,
	wc_claimant_num,
	post_inj_wkly_wage_amt,
	impairment_disability_percentage,
	impairment_disability_percentage_basis_code,
	-- *INF*: iif(impairment_disability_percentage_basis_code = 'N/A'
	-- ,'0'
	-- ,impairment_disability_percentage_basis_code)
	IFF(
	    impairment_disability_percentage_basis_code = 'N/A', '0',
	    impairment_disability_percentage_basis_code
	) AS o_impairment_disability_percentage_basis_code,
	med_extinguishment_ind,
	ret_to_work_rate_of_pay_ind,
	-- *INF*: iif(v_return_to_work_date = '00000000',
	-- ' ',ret_to_work_rate_of_pay_ind)
	-- 
	-- --changes made by VS on 08/22/2011
	IFF(v_return_to_work_date = '00000000', ' ', ret_to_work_rate_of_pay_ind) AS o_ret_to_work_rate_of_pay_ind,
	max_med_improvement_date,
	Loss_Condition_Code,
	sysdate AS create_date,
	trans_status,
	'00000' AS prev_carrier_code,
	'00000000' AS prev_policy_eff_date,
	'00000000' AS Prev_reported_to_insurer_date,
	pol_sym,
	ClassCode AS Classcode,
	-- *INF*: IIF(pol_sym = '000',Classcode,SUBSTR(LTRIM(RTRIM(Classcode)),1,4))
	IFF(pol_sym = '000', Classcode, SUBSTR(LTRIM(RTRIM(Classcode)), 1, 4)) AS V_classcode,
	V_classcode AS O_classcode
	FROM JNR_Join_Medical_Data_With_Indemnity_Claims
),
work_claim_ncci_report_extract AS (
	INSERT INTO work_claim_ncci_report_extract
	(edw_claim_occurrence_ak_id, edw_claim_party_occurrence_ak_id, edw_pol_ak_id, created_date, transm_status, rcrd_type_code, carrier_code, future_use_1, pol_num_id, pol_eff_date, valuation_lvl_code, repl_rpt_code, claim_num_id, future_use_2, jurisdiction_state, acc_state, acc_date, reported_to_insr_date, class_code, type_of_loss, type_of_recovery, type_of_claim, claimant_gndr_code, birth_yr, hire_yr, future_use_3, preinjury_avg_weeky_wage_amt, method_of_determining_preinjury_avg_wkly_wage_code, part_of_body_code, nature_of_inj, cause_of_inj, claim_status_code, closing_date, incurred_indemnity_amt_total, future_use_4, incurred_med_amt_total, total_paid_med_amt, post_inj_wkly_wage_amt, impairment_disability_percentage, impairment_percentage_basis_code, max_med_improvement_date, controverted_disputed_case_ind, med_extinguishment_ind, return_to_work_date, return_to_work_rate_of_pay_ind, extraordinary_loss_event_claim_ind, future_use_5, prv_carrier_code, future_use_6, prv_pol_eff_date, prv_reported_to_insr_date, future_use_7, future_use_8)
	SELECT 
	EDW_CLAIM_OCCURRENCE_AK_ID, 
	EDW_CLAIM_PARTY_OCCURRENCE_AK_ID, 
	EDW_POL_AK_ID, 
	create_date AS CREATED_DATE, 
	trans_status AS TRANSM_STATUS, 
	record_type_code AS RCRD_TYPE_CODE, 
	CARRIER_CODE, 
	future_use3 AS FUTURE_USE_1, 
	pol_key AS POL_NUM_ID, 
	POL_EFF_DATE, 
	valuation_level_code AS VALUATION_LVL_CODE, 
	replacement_report_code AS REPL_RPT_CODE, 
	wc_claimant_num AS CLAIM_NUM_ID, 
	future_use9 AS FUTURE_USE_2, 
	jurisdiction_state_code AS JURISDICTION_STATE, 
	loss_loc_state AS ACC_STATE, 
	claim_loss_date AS ACC_DATE, 
	source_claim_rpted_date AS REPORTED_TO_INSR_DATE, 
	O_classcode AS CLASS_CODE, 
	type_of_loss_code AS TYPE_OF_LOSS, 
	Loss_Condition_Code AS TYPE_OF_RECOVERY, 
	TYPE_OF_CLAIM, 
	o_claimant_gndr AS CLAIMANT_GNDR_CODE, 
	BIRTH_YR, 
	hired_yr AS HIRE_YR, 
	future_use21 AS FUTURE_USE_3, 
	avg_wkly_wage AS PREINJURY_AVG_WEEKY_WAGE_AMT, 
	pre_injury_avg_wkly_wage_code AS METHOD_OF_DETERMINING_PREINJURY_AVG_WKLY_WAGE_CODE, 
	body_part_code AS PART_OF_BODY_CODE, 
	nature_inj_code AS NATURE_OF_INJ, 
	cause_inj_code AS CAUSE_OF_INJ, 
	CLAIM_STATUS_CODE, 
	claimant_close_date AS CLOSING_DATE, 
	Incurred_Indemnity_Amount_Total AS INCURRED_INDEMNITY_AMT_TOTAL, 
	future_use45 AS FUTURE_USE_4, 
	o_Incurred_Medical_Amount_Total AS INCURRED_MED_AMT_TOTAL, 
	o_Total_Paid_Medical_Amount AS TOTAL_PAID_MED_AMT, 
	POST_INJ_WKLY_WAGE_AMT, 
	IMPAIRMENT_DISABILITY_PERCENTAGE, 
	o_impairment_disability_percentage_basis_code AS IMPAIRMENT_PERCENTAGE_BASIS_CODE, 
	MAX_MED_IMPROVEMENT_DATE, 
	controverted_case_code AS CONTROVERTED_DISPUTED_CASE_IND, 
	MED_EXTINGUISHMENT_IND, 
	o_return_to_work_date AS RETURN_TO_WORK_DATE, 
	o_ret_to_work_rate_of_pay_ind AS RETURN_TO_WORK_RATE_OF_PAY_IND, 
	wc_cat_code AS EXTRAORDINARY_LOSS_EVENT_CLAIM_IND, 
	future_use76 AS FUTURE_USE_5, 
	prev_carrier_code AS PRV_CARRIER_CODE, 
	future_use78 AS FUTURE_USE_6, 
	prev_policy_eff_date AS PRV_POL_EFF_DATE, 
	Prev_reported_to_insurer_date AS PRV_REPORTED_TO_INSR_DATE, 
	future_use84 AS FUTURE_USE_7, 
	Future_use AS FUTURE_USE_8
	FROM EXP_NCCI_Target
),