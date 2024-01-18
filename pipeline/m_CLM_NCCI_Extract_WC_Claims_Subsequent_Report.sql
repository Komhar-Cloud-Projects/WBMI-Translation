WITH
LKP_Get_State_Code_NCCI AS (
	SELECT
	state_abbrev,
	state_code
	FROM (
		SELECT state_sup.state_abbrev as state_abbrev, ltrim(rtrim(state_sup.state_code)) as state_code FROM state_sup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_abbrev) = 1
),
SQ_Get_WC_Get_Subsequent_Claims AS (
	SELECT ncci_extract_tab.work_claim_ncci_rpt_extract_id,
	       ncci_extract_tab.edw_claim_occurrence_ak_id,
	       ncci_extract_tab.edw_claim_party_occurrence_ak_id,
	       ncci_extract_tab.edw_pol_ak_id,
	       ncci_extract_tab.pol_num_id,
	       ncci_extract_tab.pol_eff_date,
	       ncci_extract_tab.jurisdiction_state,
	       ncci_extract_tab.reported_to_insr_date,
	       ncci_extract_tab.claim_status_code,
	       Ltrim(Rtrim(co.source_claim_occurrence_status_code)),
	       Ltrim(Rtrim(co.loss_loc_state)),
	       co.claim_loss_date,
	       Ltrim(Rtrim(co.claim_num)),
	       Ltrim(Rtrim(co.wc_cat_code)),
	       pol.pol_dim_id,
		   ltrim(rtrim(pol.pol_sym)) as pol_sym,
	       Ltrim(Rtrim(pol.pol_key)),
	       pol.pol_eff_date,
	       clmt.claimant_dim_id,
	       Ltrim(Rtrim(clmt.claimant_status_type)),
	       clmt.claimant_close_date,
	       clmt.claimant_closed_after_reopen_date,
	       Ltrim(Rtrim(clmt.claimant_full_name)),
	       clmt.claimant_birthdate,
	       Ltrim(Rtrim(clmt.claimant_gndr)),
	       Ltrim(Rtrim(clmt.jurisdiction_state_code)),
	       clmt.hired_date,
	       clmt.avg_wkly_wage,
	       Ltrim(Rtrim(clmt.body_part_code)),
	       Ltrim(Rtrim(clmt.nature_inj_code)),
	       Ltrim(Rtrim(clmt.cause_inj_code)),
	       clmt.return_to_work_date,
	       Ltrim(Rtrim(clmt.controverted_case_code)),
	       Ltrim(Rtrim(clmt.act_status_code)),
	       Ltrim(Rtrim(clmt.wc_claimant_num)),
	       Ltrim(Rtrim(clmt.type_of_loss_code)),
	       Ltrim(Rtrim(clmt.pre_inj_avg_wkly_wage_code)),
	       clmt.post_inj_wkly_wage_amt,
	       clmt.impairment_disability_percentage,
	       Ltrim(Rtrim(clmt.impairment_disability_percentage_basis_code)),
	       clmt.max_med_improvement_date,
	       Ltrim(Rtrim(clmt.med_extinguishment_ind)),
	       Ltrim(Rtrim(clmt.crrnt_work_status)),
	       max(ltrim(rtrim(clm_trans_dim.trans_ctgry_code))),
	       SUM(loss_trans.direct_loss_incurred_excluding_recoveries) AS Incurred_Indemnity_Amount_Total,
	       SUM(loss_trans.direct_subrogation_incurred)               AS direct_subrogation_incurred,
	       SUM(loss_trans.direct_other_recovery_loss_incurred)       AS direct_other_recovery_loss_incurred,
		   ltrim(rtrim(COVDET.ClassCode)) as classcode
	FROM   work_claim_ncci_report_extract ncci_extract_tab WITH (NOLOCK)
	       INNER JOIN (SELECT edw_claim_party_occurrence_ak_id,
	       MAX(work_claim_ncci_rpt_extract_id) AS max_id
	       FROM   dbo.work_claim_ncci_report_extract
	       GROUP  BY edw_claim_party_occurrence_ak_id) b
	       ON ncci_extract_tab.work_claim_ncci_rpt_extract_id = b.max_id
	       INNER JOIN claim_occurrence_dim co WITH (NOLOCK)
	       ON ncci_extract_tab.edw_claim_occurrence_ak_id = co.edw_claim_occurrence_ak_id
	       INNER JOIN claimant_dim clmt WITH (NOLOCK)
	       ON ncci_extract_tab.edw_claim_party_occurrence_ak_id = clmt.edw_claim_party_occurrence_ak_id
	       INNER JOIN policy_dim pol WITH (NOLOCK)
	       ON ncci_extract_tab.edw_pol_ak_id = pol.edw_pol_ak_id
	       INNER JOIN claim_loss_transaction_fact loss_trans WITH (NOLOCK)
	       ON co.claim_occurrence_dim_id = loss_trans.claim_occurrence_dim_id
	       AND clmt.claimant_dim_id = loss_trans.claimant_dim_id
	       AND pol.pol_dim_id = loss_trans.pol_dim_id
	       INNER JOIN CoverageDetailDim COVDET WITH (NOLOCK)
	       ON loss_trans.CoverageDetailDimId = COVDET.CoverageDetailDimId
	       INNER JOIN claimant_coverage_dim clmt_cov WITH (NOLOCK)
	       ON loss_trans.claimant_cov_dim_id = clmt_cov.claimant_cov_dim_id
	       INNER JOIN claim_payment_category_type_dim ctgry_dim WITH (NOLOCK)
	       ON loss_trans.claim_pay_ctgry_type_dim_id = ctgry_dim.claim_pay_ctgry_type_dim_id
	       LEFT OUTER JOIN (SELECT *
	FROM   claim_transaction_type_dim
	WHERE  trans_ctgry_code = 'SI') clm_trans_dim
	         ON loss_trans.claim_trans_type_dim_id = clm_trans_dim.claim_trans_type_dim_id
	WHERE  ( ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+13), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+12), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+25), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+24), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+37), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+36), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+49), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+48), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+61), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+60), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+73), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+72), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+85), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+84), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+97), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+96), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+109), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+108), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+121), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+120), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) ) )
	       AND clmt.crrnt_snpsht_flag = 1
	       AND co.crrnt_snpsht_flag = 1
	       AND pol.crrnt_snpsht_flag = 1
	       AND clmt_cov.crrnt_snpsht_flag = 1
	       AND clmt_cov.MajorPerilCode = '032' -- for WC claims
	       AND clmt_cov.cause_of_loss = '05'
	       AND co.claim_num <> 'N/A' --for exceed_claims
	GROUP  BY ncci_extract_tab.work_claim_ncci_rpt_extract_id,
	          ncci_extract_tab.edw_claim_occurrence_ak_id,
	          ncci_extract_tab.edw_claim_party_occurrence_ak_id,
	          ncci_extract_tab.edw_pol_ak_id,
	          ncci_extract_tab.pol_num_id,
	          ncci_extract_tab.pol_eff_date,
	          ncci_extract_tab.jurisdiction_state,
	          ncci_extract_tab.reported_to_insr_date,
	          ncci_extract_tab.claim_status_code,
	          Ltrim(Rtrim(co.source_claim_occurrence_status_code)),
	          Ltrim(Rtrim(co.loss_loc_state)),
	          co.claim_loss_date,
	          Ltrim(Rtrim(co.wc_cat_code)),
	          Ltrim(Rtrim(co.claim_num)),
	          pol.pol_dim_id,
			  ltrim(rtrim(pol.pol_sym)),
	          Ltrim(Rtrim(pol.pol_key)),
	          pol.pol_eff_date,
	          clmt.claimant_dim_id,
	          Ltrim(Rtrim(clmt.claimant_status_type)),
	          clmt.claimant_close_date,
	          clmt.claimant_closed_after_reopen_date,
	          Ltrim(Rtrim(clmt.claimant_full_name)),
	          clmt.claimant_birthdate,
	          Ltrim(Rtrim(clmt.claimant_gndr)),
	          Ltrim(Rtrim(clmt.jurisdiction_state_code)),
	          clmt.hired_date,
	          clmt.avg_wkly_wage,
	          Ltrim(Rtrim(clmt.body_part_code)),
	          Ltrim(Rtrim(clmt.nature_inj_code)),
	          Ltrim(Rtrim(clmt.cause_inj_code)),
	          clmt.return_to_work_date,
	          Ltrim(Rtrim(clmt.controverted_case_code)),
	          Ltrim(Rtrim(clmt.act_status_code)),
	          Ltrim(Rtrim(clmt.wc_claimant_num)),
	          Ltrim(Rtrim(clmt.type_of_loss_code)),
	          Ltrim(Rtrim(clmt.pre_inj_avg_wkly_wage_code)),
	          clmt.post_inj_wkly_wage_amt,
	          clmt.impairment_disability_percentage,
	          Ltrim(Rtrim(clmt.impairment_disability_percentage_basis_code)),
	          clmt.max_med_improvement_date,
	          Ltrim(Rtrim(clmt.med_extinguishment_ind)),
	          Ltrim(Rtrim(clmt.crrnt_work_status)),
			  ltrim(rtrim(COVDET.ClassCode))
),
EXP_Convert_Date_Format AS (
	SELECT
	reported_to_insr_date,
	-- *INF*: to_date(reported_to_insr_date,'YYYYMMDD')
	TO_TIMESTAMP(reported_to_insr_date, 'YYYYMMDD') AS o_reported_to_insr_date
	FROM SQ_Get_WC_Get_Subsequent_Claims
),
EXP_Calculate_Valuation_Levels AS (
	SELECT
	o_reported_to_insr_date AS Input_Date,
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
	FROM EXP_Convert_Date_Format
),
EXP_Get_Source_Data AS (
	SELECT
	SQ_Get_WC_Get_Subsequent_Claims.work_claim_ncci_rpt_extract_id,
	SQ_Get_WC_Get_Subsequent_Claims.edw_claim_occurrence_ak_id1 AS edw_claim_occurrence_ak_id,
	SQ_Get_WC_Get_Subsequent_Claims.edw_claim_party_occurrence_ak_id1,
	SQ_Get_WC_Get_Subsequent_Claims.edw_pol_ak_id1,
	SQ_Get_WC_Get_Subsequent_Claims.pol_num_id AS prev_pol_key,
	SQ_Get_WC_Get_Subsequent_Claims.pol_eff_date1 AS prev_pol_eff_date,
	EXP_Convert_Date_Format.reported_to_insr_date,
	SQ_Get_WC_Get_Subsequent_Claims.jurisdiction_state AS perv_jurisdiction_state,
	SQ_Get_WC_Get_Subsequent_Claims.claim_status_code AS prev_claim_status_code,
	SQ_Get_WC_Get_Subsequent_Claims.source_claim_occurrence_status_code,
	-- *INF*: to_char(source_claim_rpted_date,'YYYYMMDD')
	to_char(source_claim_rpted_date, 'YYYYMMDD') AS o_source_claim_rpted_date,
	SQ_Get_WC_Get_Subsequent_Claims.loss_loc_state,
	-- *INF*: :LKP.LKP_GET_STATE_CODE_NCCI(loss_loc_state) 
	LKP_GET_STATE_CODE_NCCI_loss_loc_state.state_abbrev AS o_loss_loc_state,
	SQ_Get_WC_Get_Subsequent_Claims.claim_loss_date,
	-- *INF*: to_char(claim_loss_date,'YYYYMMDD')
	to_char(claim_loss_date, 'YYYYMMDD') AS o_claim_loss_date,
	SQ_Get_WC_Get_Subsequent_Claims.claim_num,
	SQ_Get_WC_Get_Subsequent_Claims.wc_cat_code,
	-- *INF*: iif(wc_cat_code < '11' or wc_cat_code = 'N/A'
	-- ,'N'
	-- ,'Y')
	-- 
	-- --iif(wc_cat_code = 'N/A'
	-- --,'N'
	-- --,'Y')
	-- 
	-- --Changes made by Vikas Sood on 08/04/2011 for DDRR-71. Extraordinary loss indicator will be set to Y only when wc_cat_code >--=11
	-- 
	IFF(wc_cat_code < '11' or wc_cat_code = 'N/A', 'N', 'Y') AS o_wc_cat_code,
	SQ_Get_WC_Get_Subsequent_Claims.pol_dim_id,
	SQ_Get_WC_Get_Subsequent_Claims.pol_key,
	SQ_Get_WC_Get_Subsequent_Claims.pol_eff_date,
	-- *INF*: to_char(pol_eff_date,'yyyymmdd')
	to_char(pol_eff_date, 'yyyymmdd') AS v_pol_eff_date,
	v_pol_eff_date AS o_pol_eff_date,
	SQ_Get_WC_Get_Subsequent_Claims.claimant_dim_id,
	SQ_Get_WC_Get_Subsequent_Claims.claimant_status_type,
	SQ_Get_WC_Get_Subsequent_Claims.claimant_close_date,
	SQ_Get_WC_Get_Subsequent_Claims.claimant_closed_after_reopen_date,
	-- *INF*: iif(v_claim_status_code = '1'
	--     ,iif(claimant_closed_after_reopen_date = to_date('18000101','YYYYMMDD')
	-- 		,to_char(claimant_close_date,'YYYYMMDD')
	--             ,to_char(claimant_closed_after_reopen_date,'YYYYMMDD')
	-- )
	--     ,'00000000')
	-- 
	-- --Report the most recent date as of loss valuation that claim was closed only if Claim Status Code is reported as 1 Closed.
	-- -- Zero-fill if the Claim Status Code  is 0Open. 
	IFF(
	    v_claim_status_code = '1',
	    IFF(
	        claimant_closed_after_reopen_date = TO_TIMESTAMP('18000101', 'YYYYMMDD'),
	        to_char(claimant_close_date, 'YYYYMMDD'),
	        to_char(claimant_closed_after_reopen_date, 'YYYYMMDD')
	    ),
	    '00000000'
	) AS o_claimant_close_date,
	SQ_Get_WC_Get_Subsequent_Claims.claimant_full_name,
	SQ_Get_WC_Get_Subsequent_Claims.claimant_birthdate,
	SQ_Get_WC_Get_Subsequent_Claims.claimant_gndr,
	SQ_Get_WC_Get_Subsequent_Claims.jurisdiction_state_code,
	-- *INF*: :LKP.LKP_GET_STATE_CODE_NCCI(jurisdiction_state_code) 
	LKP_GET_STATE_CODE_NCCI_jurisdiction_state_code.state_abbrev AS v_jurisdiction_state_code,
	SQ_Get_WC_Get_Subsequent_Claims.hired_date,
	-- *INF*: iif(hired_date = to_date('01/01/1800','MM/DD/YYYY')
	-- ,'00000000'
	-- ,to_char(hired_date,'YYYYMMDD'))
	IFF(
	    hired_date = TO_TIMESTAMP('01/01/1800', 'MM/DD/YYYY'), '00000000',
	    to_char(hired_date, 'YYYYMMDD')
	) AS o_hired_date,
	SQ_Get_WC_Get_Subsequent_Claims.avg_wkly_wage,
	-- *INF*: iif(isnull(avg_wkly_wage)
	--      ,'00000'
	--      ,iif(avg_wkly_wage > 99999
	--           ,'99999'
	--         ,lpad(to_char(to_integer(avg_wkly_wage)),5,'0')
	-- )
	-- )
	IFF(
	    avg_wkly_wage IS NULL, '00000',
	    IFF(
	        avg_wkly_wage > 99999, '99999',
	        lpad(to_char(CAST(avg_wkly_wage AS INTEGER)), 5, '0')
	    )
	) AS o_avg_wkly_wage,
	SQ_Get_WC_Get_Subsequent_Claims.body_part_code,
	SQ_Get_WC_Get_Subsequent_Claims.nature_inj_code,
	SQ_Get_WC_Get_Subsequent_Claims.cause_inj_code,
	SQ_Get_WC_Get_Subsequent_Claims.return_to_work_date,
	-- *INF*: iif(to_char(return_to_work_date,'YYYYMMDD')  !=  '18000101' AND (v_claim_status_code = '0' or  v_claim_status_code = '1' ) ,to_char(return_to_work_date,'YYYYMMDD')
	-- ,'00000000')
	-- 
	IFF(
	    to_char(return_to_work_date, 'YYYYMMDD') != '18000101'
	    and (v_claim_status_code = '0'
	    or v_claim_status_code = '1'),
	    to_char(return_to_work_date, 'YYYYMMDD'),
	    '00000000'
	) AS o_return_to_work_date,
	SQ_Get_WC_Get_Subsequent_Claims.controverted_case_code,
	-- *INF*: iif(controverted_case_code = 'N/A'
	-- ,' '
	-- ,controverted_case_code)
	IFF(controverted_case_code = 'N/A', ' ', controverted_case_code) AS o_controverted_case_code,
	SQ_Get_WC_Get_Subsequent_Claims.act_status_code,
	-- *INF*: iif(act_status_code = 'FA' 
	-- ,'59'
	-- ,iif(in(jurisdiction_state_code,'CA', 'DE', 'ND', 'OH', 'WA', 'WY')
	-- ,' '
	-- ,v_jurisdiction_state_code)
	-- )
	IFF(
	    act_status_code = 'FA', '59',
	    IFF(
	        jurisdiction_state_code IN ('CA','DE','ND','OH','WA','WY'), ' ',
	        v_jurisdiction_state_code
	    )
	) AS o_jurisdiction_state_code,
	SQ_Get_WC_Get_Subsequent_Claims.wc_claimant_num,
	SQ_Get_WC_Get_Subsequent_Claims.type_of_loss_code,
	-- *INF*: iif(type_of_loss_code = 'N/A'
	-- ,'00'
	-- ,type_of_loss_code)
	IFF(type_of_loss_code = 'N/A', '00', type_of_loss_code) AS o_type_of_loss_code,
	SQ_Get_WC_Get_Subsequent_Claims.pre_inj_avg_wkly_wage_code AS pre_injury_avg_wkly_wage_code,
	-- *INF*: iif(pre_injury_avg_wkly_wage_code = 'N/A'
	-- ,'1'
	-- ,ltrim(pre_injury_avg_wkly_wage_code,'0'))
	IFF(pre_injury_avg_wkly_wage_code = 'N/A', '1', ltrim(pre_injury_avg_wkly_wage_code, '0')) AS o_pre_injury_avg_wkly_wage_code,
	SQ_Get_WC_Get_Subsequent_Claims.post_inj_wkly_wage_amt,
	-- *INF*: iif(isnull(post_inj_wkly_wage_amt)
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(post_inj_wkly_wage_amt,false)),9,'0')
	-- )
	IFF(
	    post_inj_wkly_wage_amt IS NULL, '000000000',
	    lpad(to_char(CAST(post_inj_wkly_wage_amt AS INTEGER)), 9, '0')
	) AS o_post_inj_wkly_wage_amt,
	SQ_Get_WC_Get_Subsequent_Claims.impairment_disability_percentage,
	-- *INF*: iif(isnull(impairment_disability_percentage)
	-- ,'000'
	-- ,lpad(to_char(to_integer(impairment_disability_percentage,false)),3,'0')
	-- )
	IFF(
	    impairment_disability_percentage IS NULL, '000',
	    lpad(to_char(CAST(impairment_disability_percentage AS INTEGER)), 3, '0')
	) AS o_impairment_disability_percentage,
	SQ_Get_WC_Get_Subsequent_Claims.impairment_disability_percentage_basis_code,
	SQ_Get_WC_Get_Subsequent_Claims.max_med_improvement_date,
	SQ_Get_WC_Get_Subsequent_Claims.med_extinguishment_ind,
	-- *INF*: iif(med_extinguishment_ind = 'N/A'
	-- ,' '
	-- ,med_extinguishment_ind)
	IFF(med_extinguishment_ind = 'N/A', ' ', med_extinguishment_ind) AS o_med_extinguishment_ind,
	SQ_Get_WC_Get_Subsequent_Claims.crrnt_work_status,
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
	) AS ret_to_work_rate_of_pay_ind,
	-- *INF*: IIF(ISNULL(max_med_improvement_date) OR to_char(max_med_improvement_date,'YYYYMMDD') = '18000101'
	-- ,'00000000'
	-- ,to_char(max_med_improvement_date,'YYYYMMDD')
	-- )
	IFF(
	    max_med_improvement_date IS NULL OR to_char(max_med_improvement_date, 'YYYYMMDD') = '18000101',
	    '00000000',
	    to_char(max_med_improvement_date, 'YYYYMMDD')
	) AS o_max_med_improvement_date,
	SQ_Get_WC_Get_Subsequent_Claims.trans_ctgry_code,
	SQ_Get_WC_Get_Subsequent_Claims.direct_loss_incurred_excluding_recoveries AS Incurred_Indemnity_Amount_Total,
	-- *INF*: iif(isnull(Incurred_Indemnity_Amount_Total)
	-- ,'000000000'
	-- ,lpad(to_char(to_integer(Incurred_Indemnity_Amount_Total)),9,'0')
	-- )
	-- 
	IFF(
	    Incurred_Indemnity_Amount_Total IS NULL, '000000000',
	    lpad(to_char(CAST(Incurred_Indemnity_Amount_Total AS INTEGER)), 9, '0')
	) AS o_Incurred_Indemnity_Amount_Total,
	SQ_Get_WC_Get_Subsequent_Claims.direct_subrogation_incurred,
	SQ_Get_WC_Get_Subsequent_Claims.direct_other_recovery_loss_incurred,
	-- *INF*: iif(source_claim_occurrence_status_code = 'O'
	--      ,iif(claimant_status_type = 'OPEN' or claimant_status_type = 'REOPEN'
	-- 		,'0'
	-- 		,'1')
	-- 	,'1')
	IFF(
	    source_claim_occurrence_status_code = 'O',
	    IFF(
	        claimant_status_type = 'OPEN' or claimant_status_type = 'REOPEN', '0', '1'
	    ),
	    '1'
	) AS v_claim_status_code,
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
	-- *INF*: decode(true,
	-- (prev_claim_status_code = '0' and v_claim_status_code = '0' and in(perv_jurisdiction_state ,' ','59') and (in(jurisdiction_state_code,'CA', 'DE', 'ND', 'OH', 'WA', 'WY') or act_status_code = 'FA')),'N',
	-- (prev_claim_status_code = '0' and v_claim_status_code = '0'), 'Y',
	-- (prev_claim_status_code = '0' and (v_claim_status_code = '1' or source_claim_occurrence_status_code = 'NO' or act_status_code = 'FA' )), 'O',
	-- (prev_claim_status_code = '1' and v_claim_status_code = '0'), 'Y',
	-- (prev_claim_status_code = '5' and v_claim_status_code = '0'), 'Y',
	-- (prev_claim_status_code = '1' and v_claim_status_code = '1'), 'N','N')
	-- 
	-- --if a previously reported open claim is still open but it was previously reported as either a NON-DCI claim represented by prev_jurisdiction_state = ' ' or it was previously reported as non-compensible reprenseted by prev_jurisdiction_state = '59' and it still is either Non-DCI or is still non-compensible then don't send it
	-- 
	-- --if a previously reported open claim stays open, then Y, 
	-- 
	-- --if a previously reported open claim is now closed or has became notice-only or is federal act claim, then send it one time reprenseted by 'O', 
	-- 
	-- --if a previously reported closed claim becomes open now, report 'Y',
	-- 
	-- --if a previously reported medical only claim (represented by 5) is still open, report 'Y',
	-- 
	-- -- if a previously reported closed claim is still closed, then 'N'. 
	-- -- For anything else, N
	decode(
	    true,
	    (prev_claim_status_code = '0' and v_claim_status_code = '0' and perv_jurisdiction_state IN (' ','59') and (jurisdiction_state_code IN ('CA','DE','ND','OH','WA','WY') or act_status_code = 'FA')), 'N',
	    (prev_claim_status_code = '0' and v_claim_status_code = '0'), 'Y',
	    (prev_claim_status_code = '0' and (v_claim_status_code = '1' or source_claim_occurrence_status_code = 'NO' or act_status_code = 'FA')), 'O',
	    (prev_claim_status_code = '1' and v_claim_status_code = '0'), 'Y',
	    (prev_claim_status_code = '5' and v_claim_status_code = '0'), 'Y',
	    (prev_claim_status_code = '1' and v_claim_status_code = '1'), 'N',
	    'N'
	) AS Trans_Status,
	EXP_Calculate_Valuation_Levels.Valuation_Level_Code AS Valuation_Level,
	-- *INF*: iif(prev_pol_key  != pol_key or prev_pol_eff_date != v_pol_eff_date
	-- ,'R'
	-- ,' ')
	IFF(prev_pol_key != pol_key or prev_pol_eff_date != v_pol_eff_date, 'R', ' ') AS Replacement_Report_Code,
	SQ_Get_WC_Get_Subsequent_Claims.pol_sym,
	SQ_Get_WC_Get_Subsequent_Claims.ClassCode
	FROM EXP_Calculate_Valuation_Levels
	 -- Manually join with EXP_Convert_Date_Format
	 -- Manually join with SQ_Get_WC_Get_Subsequent_Claims
	LEFT JOIN LKP_GET_STATE_CODE_NCCI LKP_GET_STATE_CODE_NCCI_loss_loc_state
	ON LKP_GET_STATE_CODE_NCCI_loss_loc_state.state_code = loss_loc_state

	LEFT JOIN LKP_GET_STATE_CODE_NCCI LKP_GET_STATE_CODE_NCCI_jurisdiction_state_code
	ON LKP_GET_STATE_CODE_NCCI_jurisdiction_state_code.state_code = jurisdiction_state_code

),
Rtr_Replacement_Record AS (
	SELECT
	work_claim_ncci_rpt_extract_id,
	edw_claim_occurrence_ak_id,
	edw_claim_party_occurrence_ak_id1,
	edw_pol_ak_id1,
	reported_to_insr_date,
	o_loss_loc_state AS loss_loc_state,
	o_claim_loss_date,
	claim_num,
	o_wc_cat_code,
	risk_unit,
	pol_dim_id,
	pol_key,
	o_pol_eff_date,
	claimant_dim_id,
	o_claimant_close_date,
	claimant_full_name,
	claimant_birthdate,
	claimant_gndr,
	o_hired_date AS hired_date,
	o_avg_wkly_wage,
	body_part_code,
	nature_inj_code,
	cause_inj_code,
	o_return_to_work_date,
	o_controverted_case_code AS controverted_case_code,
	o_jurisdiction_state_code,
	wc_claimant_num,
	o_type_of_loss_code,
	o_pre_injury_avg_wkly_wage_code AS pre_injury_avg_wkly_wage_code,
	o_post_inj_wkly_wage_amt,
	o_impairment_disability_percentage,
	impairment_disability_percentage_basis_code,
	o_med_extinguishment_ind AS med_extinguishment_ind,
	ret_to_work_rate_of_pay_ind,
	o_max_med_improvement_date,
	o_Incurred_Indemnity_Amount_Total,
	o_claim_status_code,
	Loss_Condition_Code,
	Trans_Status,
	Valuation_Level,
	Replacement_Report_Code,
	pol_sym,
	ClassCode
	FROM EXP_Get_Source_Data
),
Rtr_Replacement_Record_Replacement_Record AS (SELECT * FROM Rtr_Replacement_Record WHERE Replacement_Report_Code = 'R'),
Rtr_Replacement_Record_All AS (SELECT * FROM Rtr_Replacement_Record WHERE TRUE),
EXP_Update_Replacement_Records AS (
	SELECT
	pol_key AS pol_key1,
	o_pol_eff_date AS o_pol_eff_date1,
	wc_claimant_num AS wc_claimant_num1,
	Replacement_Report_Code AS Replacement_Report_Code1
	FROM Rtr_Replacement_Record_Replacement_Record
),
SQL_GetPrevious_Valuations_For_Replacement_Reports AS (-- SQL_GetPrevious_Valuations_For_Replacement_Reports

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_Error AS (
	SELECT
	SQLError,
	-- *INF*: iif (not isnull(SQLError),
	-- error('SQLError')
	-- )
	-- 
	IFF(SQLError IS NOT NULL, error('SQLError')) AS v_SQLError,
	work_claim_ncci_rpt_extract_id,
	'O' AS Transm_Status,
	Replacement_Report_Code2_output,
	pol_key1_output,
	o_pol_eff_date1_output,
	prev_pol_key,
	prev_pol_eff_date
	FROM SQL_GetPrevious_Valuations_For_Replacement_Reports
),
UPD_Replacement_Records AS (
	SELECT
	work_claim_ncci_rpt_extract_id, 
	Transm_Status, 
	Replacement_Report_Code2_output, 
	pol_key1_output, 
	o_pol_eff_date1_output, 
	prev_pol_key, 
	prev_pol_eff_date
	FROM EXP_Error
),
work_claim_ncci_report_extract2 AS (
	MERGE INTO work_claim_ncci_report_extract AS T
	USING UPD_Replacement_Records AS S
	ON T.work_claim_ncci_rpt_extract_id = S.work_claim_ncci_rpt_extract_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.transm_status = S.Transm_Status, T.pol_num_id = S.pol_key1_output, T.pol_eff_date = S.o_pol_eff_date1_output, T.repl_rpt_code = S.Replacement_Report_Code2_output, T.prv_pol_num_id = S.prev_pol_key, T.prv_pol_eff_date = S.prev_pol_eff_date
),
SQ_Get_WC_Subsequent_Claim_Medical_Data AS (
	SELECT ncci_extract_tab.work_claim_ncci_rpt_extract_id,
	       SUM(loss_trans.direct_loss_paid_excluding_recoveries)     AS Total_Paid_Medical_Amount,
	       SUM(loss_trans.direct_loss_incurred_excluding_recoveries) AS Incurred_Medical_Amount_Total
	FROM   work_claim_ncci_report_extract ncci_extract_tab WITH (NOLOCK)
	       INNER JOIN (SELECT edw_claim_party_occurrence_ak_id,
	                          MAX(work_claim_ncci_rpt_extract_id) AS max_id
	                   FROM   dbo.work_claim_ncci_report_extract
	                   GROUP  BY edw_claim_party_occurrence_ak_id) b
	         ON ncci_extract_tab.work_claim_ncci_rpt_extract_id = b.max_id
	       inner join claim_occurrence_dim co WITH (NOLOCK)
	       on ncci_extract_tab.edw_claim_occurrence_ak_id = co.edw_claim_occurrence_ak_id
	       INNER JOIN claimant_dim clmt WITH (NOLOCK)
	         ON ncci_extract_tab.edw_claim_party_occurrence_ak_id = clmt.edw_claim_party_occurrence_ak_id
	       INNER JOIN policy_dim pol WITH (NOLOCK)
	         ON ncci_extract_tab.edw_pol_ak_id = pol.edw_pol_ak_id
	       INNER JOIN claim_loss_transaction_fact loss_trans WITH (NOLOCK)
	         ON co.claim_occurrence_dim_id = loss_trans.claim_occurrence_dim_id
	            AND clmt.claimant_dim_id = loss_trans.claimant_dim_id
	            AND pol.pol_dim_id = loss_trans.pol_dim_id
	       INNER JOIN claimant_coverage_dim clmt_cov WITH (NOLOCK)
	         ON loss_trans.claimant_cov_dim_id = clmt_cov.claimant_cov_dim_id
	       INNER JOIN claim_payment_category_type_dim ctgry_dim WITH (NOLOCK)
	         ON loss_trans.claim_pay_ctgry_type_dim_id = ctgry_dim.claim_pay_ctgry_type_dim_id
	WHERE  ( ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+13), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+12), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+25), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+24), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+37), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+36), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+49), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+48), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+61), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+60), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+73), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+72), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+85), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+84), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+97), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+96), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+109), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+108), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) )
	          OR ( ncci_extract_tab.reported_to_insr_date >= Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+121), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate())))
	               AND ncci_extract_tab.reported_to_insr_date < Dateadd(m, -(@{pipeline().parameters.NUMBER_OF_MONTHS}+120), CONVERT(VARCHAR(4), Getdate(), 100) + CONVERT(VARCHAR(4), YEAR(Getdate()))) ) )       AND  clmt.crrnt_snpsht_flag = 1
	       AND co.crrnt_snpsht_flag = 1
	       AND pol.crrnt_snpsht_flag = 1
	      AND clmt_cov.crrnt_snpsht_flag = 1
	      and clmt_cov.majorperilcode = '032' -- for WC claims
	       AND clmt_cov.cause_of_loss = '06' -- for medical claims
	GROUP  BY ncci_extract_tab.work_claim_ncci_rpt_extract_id
),
EXP_Get_Source_Data2 AS (
	SELECT
	work_claim_ncci_rpt_extract_id,
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
	FROM SQ_Get_WC_Subsequent_Claim_Medical_Data
),
EXP_Pass_To_Jnr AS (
	SELECT
	work_claim_ncci_rpt_extract_id AS work_claim_ncci_rpt_extract_id3,
	edw_claim_occurrence_ak_id AS edw_claim_occurrence_ak_id3,
	edw_claim_party_occurrence_ak_id1 AS edw_claim_party_occurrence_ak_id13,
	edw_pol_ak_id1 AS edw_pol_ak_id13,
	reported_to_insr_date AS reported_to_insr_date3,
	loss_loc_state AS loss_loc_state3,
	o_claim_loss_date AS o_claim_loss_date3,
	claim_num AS claim_num3,
	o_wc_cat_code AS o_wc_cat_code3,
	risk_unit AS risk_unit3,
	pol_dim_id AS pol_dim_id3,
	pol_key AS pol_key3,
	o_pol_eff_date AS o_pol_eff_date3,
	claimant_dim_id AS claimant_dim_id3,
	o_claimant_close_date AS o_claimant_close_date3,
	claimant_full_name AS claimant_full_name3,
	claimant_birthdate AS claimant_birthdate3,
	claimant_gndr AS claimant_gndr3,
	hired_date AS hired_date3,
	o_avg_wkly_wage AS o_avg_wkly_wage3,
	body_part_code AS body_part_code3,
	nature_inj_code AS nature_inj_code3,
	cause_inj_code AS cause_inj_code3,
	o_return_to_work_date AS o_return_to_work_date3,
	controverted_case_code AS controverted_case_code3,
	o_jurisdiction_state_code AS o_jurisdiction_state_code3,
	wc_claimant_num AS wc_claimant_num3,
	o_type_of_loss_code AS o_type_of_loss_code3,
	pre_injury_avg_wkly_wage_code AS pre_injury_avg_wkly_wage_code3,
	o_post_inj_wkly_wage_amt AS o_post_inj_wkly_wage_amt3,
	o_impairment_disability_percentage AS o_impairment_disability_percentage3,
	impairment_disability_percentage_basis_code AS impairment_disability_percentage_basis_code3,
	med_extinguishment_ind AS med_extinguishment_ind3,
	ret_to_work_rate_of_pay_ind AS ret_to_work_rate_of_pay_ind3,
	o_max_med_improvement_date AS o_max_med_improvement_date3,
	o_Incurred_Indemnity_Amount_Total AS o_Incurred_Indemnity_Amount_Total3,
	o_claim_status_code AS o_claim_status_code3,
	Loss_Condition_Code AS Loss_Condition_Code3,
	Trans_Status AS Trans_Status3,
	Valuation_Level AS Valuation_Level3,
	pol_sym AS pol_sym3,
	ClassCode AS ClassCode3
	FROM Rtr_Replacement_Record_All
),
JNR_Join_Medical_Data_With_Indemnity_Claims AS (SELECT
	EXP_Get_Source_Data2.work_claim_ncci_rpt_extract_id, 
	EXP_Get_Source_Data2.o_Total_Paid_Medical_Amount AS Total_Paid_Medical_Amount, 
	EXP_Get_Source_Data2.o_Incurred_Medical_Amount_Total AS Incurred_Medical_Amount_Total, 
	EXP_Pass_To_Jnr.work_claim_ncci_rpt_extract_id3 AS work_claim_ncci_rpt_extract_id1, 
	EXP_Pass_To_Jnr.edw_claim_occurrence_ak_id3 AS edw_claim_occurrence_ak_id, 
	EXP_Pass_To_Jnr.edw_claim_party_occurrence_ak_id13 AS edw_claim_party_occurrence_ak_id1, 
	EXP_Pass_To_Jnr.edw_pol_ak_id13 AS edw_pol_ak_id1, 
	EXP_Pass_To_Jnr.reported_to_insr_date3 AS reported_to_insr_date, 
	EXP_Pass_To_Jnr.loss_loc_state3 AS loss_loc_state, 
	EXP_Pass_To_Jnr.o_claim_loss_date3 AS claim_loss_date, 
	EXP_Pass_To_Jnr.claim_num3 AS claim_num, 
	EXP_Pass_To_Jnr.o_wc_cat_code3 AS wc_cat_code, 
	EXP_Pass_To_Jnr.risk_unit3 AS risk_unit, 
	EXP_Pass_To_Jnr.pol_dim_id3 AS pol_dim_id1, 
	EXP_Pass_To_Jnr.pol_key3 AS pol_key, 
	EXP_Pass_To_Jnr.o_pol_eff_date3 AS pol_eff_date, 
	EXP_Pass_To_Jnr.claimant_dim_id3 AS claimant_dim_id1, 
	EXP_Pass_To_Jnr.o_claimant_close_date3 AS claimant_close_date, 
	EXP_Pass_To_Jnr.claimant_full_name3 AS claimant_full_name, 
	EXP_Pass_To_Jnr.claimant_birthdate3 AS claimant_birthdate, 
	EXP_Pass_To_Jnr.claimant_gndr3 AS claimant_gndr, 
	EXP_Pass_To_Jnr.o_jurisdiction_state_code3 AS jurisdiction_state_code, 
	EXP_Pass_To_Jnr.hired_date3 AS hired_date, 
	EXP_Pass_To_Jnr.o_avg_wkly_wage3 AS avg_wkly_wage, 
	EXP_Pass_To_Jnr.body_part_code3 AS body_part_code, 
	EXP_Pass_To_Jnr.nature_inj_code3 AS nature_inj_code, 
	EXP_Pass_To_Jnr.cause_inj_code3 AS cause_inj_code, 
	EXP_Pass_To_Jnr.o_return_to_work_date3 AS return_to_work_date, 
	EXP_Pass_To_Jnr.controverted_case_code3 AS controverted_case_code, 
	EXP_Pass_To_Jnr.wc_claimant_num3 AS wc_claimant_num, 
	EXP_Pass_To_Jnr.o_type_of_loss_code3 AS type_of_loss_code, 
	EXP_Pass_To_Jnr.pre_injury_avg_wkly_wage_code3 AS pre_injury_avg_wkly_wage_code, 
	EXP_Pass_To_Jnr.o_post_inj_wkly_wage_amt3 AS post_inj_wkly_wage_amt, 
	EXP_Pass_To_Jnr.o_impairment_disability_percentage3 AS impairment_disability_percentage, 
	EXP_Pass_To_Jnr.impairment_disability_percentage_basis_code3 AS impairment_disability_percentage_basis_code, 
	EXP_Pass_To_Jnr.med_extinguishment_ind3 AS med_extinguishment_ind, 
	EXP_Pass_To_Jnr.ret_to_work_rate_of_pay_ind3 AS ret_to_work_rate_of_pay_ind, 
	EXP_Pass_To_Jnr.o_max_med_improvement_date3 AS max_med_improvement_date, 
	EXP_Pass_To_Jnr.o_Incurred_Indemnity_Amount_Total3 AS Incurred_Indemnity_Amount_Total, 
	EXP_Pass_To_Jnr.o_claim_status_code3 AS claim_status_code, 
	EXP_Pass_To_Jnr.Loss_Condition_Code3 AS Loss_Condition_Code, 
	EXP_Pass_To_Jnr.Trans_Status3 AS Trans_Status, 
	EXP_Pass_To_Jnr.Valuation_Level3 AS Valuation_Level, 
	EXP_Pass_To_Jnr.pol_sym3, 
	EXP_Pass_To_Jnr.ClassCode3
	FROM EXP_Get_Source_Data2
	RIGHT OUTER JOIN EXP_Pass_To_Jnr
	ON EXP_Pass_To_Jnr.work_claim_ncci_rpt_extract_id3 = EXP_Get_Source_Data2.work_claim_ncci_rpt_extract_id
),
EXP_Calculations AS (
	SELECT
	Incurred_Indemnity_Amount_Total,
	claim_status_code,
	-- *INF*: iif(Incurred_Indemnity_Amount_Total='000000000'
	-- ,'5'
	-- ,claim_status_code)
	-- 
	-- --medical only claims will have Incurred_Indemnity_Amount_Total set to 0
	IFF(Incurred_Indemnity_Amount_Total = '000000000', '5', claim_status_code) AS o_claim_status_code,
	Trans_Status,
	-- *INF*: iif(Incurred_Indemnity_Amount_Total='000000000' and Trans_Status != 'N'
	-- ,'O'
	-- ,Trans_Status)
	-- 
	-- --If it is a med only claim, mark it for reporting one time only.
	-- 
	-- 
	IFF(
	    Incurred_Indemnity_Amount_Total = '000000000' and Trans_Status != 'N', 'O', Trans_Status
	) AS o_Trans_Status,
	work_claim_ncci_rpt_extract_id1,
	work_claim_ncci_rpt_extract_id
	FROM JNR_Join_Medical_Data_With_Indemnity_Claims
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
	JNR_Join_Medical_Data_With_Indemnity_Claims.Valuation_Level AS valuation_level_code,
	' ' AS replacement_report_code,
	' ' AS future_use9,
	' ' AS future_use21,
	'01' AS type_of_claim,
	' ' AS future_use45,
	' ' AS future_use76,
	' ' AS future_use78,
	' ' AS future_use84,
	' ' AS Future_use,
	JNR_Join_Medical_Data_With_Indemnity_Claims.work_claim_ncci_rpt_extract_id,
	JNR_Join_Medical_Data_With_Indemnity_Claims.edw_claim_party_occurrence_ak_id1 AS edw_claim_party_occurrence_ak_id,
	JNR_Join_Medical_Data_With_Indemnity_Claims.edw_pol_ak_id1 AS edw_pol_ak_id,
	JNR_Join_Medical_Data_With_Indemnity_Claims.edw_claim_occurrence_ak_id,
	JNR_Join_Medical_Data_With_Indemnity_Claims.pol_key,
	JNR_Join_Medical_Data_With_Indemnity_Claims.pol_eff_date,
	JNR_Join_Medical_Data_With_Indemnity_Claims.claim_num,
	JNR_Join_Medical_Data_With_Indemnity_Claims.jurisdiction_state_code,
	JNR_Join_Medical_Data_With_Indemnity_Claims.loss_loc_state,
	JNR_Join_Medical_Data_With_Indemnity_Claims.claim_loss_date,
	JNR_Join_Medical_Data_With_Indemnity_Claims.reported_to_insr_date AS source_claim_rpted_date,
	JNR_Join_Medical_Data_With_Indemnity_Claims.risk_unit,
	JNR_Join_Medical_Data_With_Indemnity_Claims.type_of_loss_code,
	JNR_Join_Medical_Data_With_Indemnity_Claims.claimant_gndr,
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
	JNR_Join_Medical_Data_With_Indemnity_Claims.claimant_birthdate,
	-- *INF*: iif(TO_CHAR(GET_DATE_PART(claimant_birthdate,'YYYY')) = '9999'
	-- ,'0000'
	-- ,TO_CHAR(GET_DATE_PART(claimant_birthdate,'YYYY')))
	IFF(
	    TO_CHAR(DATE_PART(claimant_birthdate, 'YYYY')) = '9999', '0000',
	    TO_CHAR(DATE_PART(claimant_birthdate, 'YYYY'))
	) AS birth_yr,
	JNR_Join_Medical_Data_With_Indemnity_Claims.hired_date,
	-- *INF*: SUBSTR(hired_date,1,4)
	SUBSTR(hired_date, 1, 4) AS hired_yr,
	JNR_Join_Medical_Data_With_Indemnity_Claims.avg_wkly_wage,
	JNR_Join_Medical_Data_With_Indemnity_Claims.pre_injury_avg_wkly_wage_code,
	JNR_Join_Medical_Data_With_Indemnity_Claims.body_part_code,
	JNR_Join_Medical_Data_With_Indemnity_Claims.nature_inj_code,
	JNR_Join_Medical_Data_With_Indemnity_Claims.cause_inj_code,
	EXP_Calculations.o_claim_status_code AS claim_status_code,
	JNR_Join_Medical_Data_With_Indemnity_Claims.claimant_close_date,
	JNR_Join_Medical_Data_With_Indemnity_Claims.Incurred_Indemnity_Amount_Total,
	JNR_Join_Medical_Data_With_Indemnity_Claims.Total_Paid_Medical_Amount,
	-- *INF*: iif(isnull(Total_Paid_Medical_Amount)
	-- ,'000000000'
	-- ,Total_Paid_Medical_Amount)
	IFF(Total_Paid_Medical_Amount IS NULL, '000000000', Total_Paid_Medical_Amount) AS o_Total_Paid_Medical_Amount,
	JNR_Join_Medical_Data_With_Indemnity_Claims.Incurred_Medical_Amount_Total,
	-- *INF*: iif(isnull(Incurred_Medical_Amount_Total)
	-- ,'000000000'
	-- ,Incurred_Medical_Amount_Total)
	IFF(Incurred_Medical_Amount_Total IS NULL, '000000000', Incurred_Medical_Amount_Total) AS o_Incurred_Medical_Amount_Total,
	JNR_Join_Medical_Data_With_Indemnity_Claims.wc_cat_code,
	JNR_Join_Medical_Data_With_Indemnity_Claims.return_to_work_date,
	-- *INF*: iif(claim_status_code = '5' ,'00000000' ,return_to_work_date)
	IFF(claim_status_code = '5', '00000000', return_to_work_date) AS v_return_to_work_date,
	v_return_to_work_date AS o_return_to_work_date,
	JNR_Join_Medical_Data_With_Indemnity_Claims.controverted_case_code,
	JNR_Join_Medical_Data_With_Indemnity_Claims.wc_claimant_num,
	JNR_Join_Medical_Data_With_Indemnity_Claims.post_inj_wkly_wage_amt,
	JNR_Join_Medical_Data_With_Indemnity_Claims.impairment_disability_percentage,
	JNR_Join_Medical_Data_With_Indemnity_Claims.impairment_disability_percentage_basis_code,
	-- *INF*: iif(impairment_disability_percentage_basis_code = 'N/A'
	-- ,'0'
	-- ,impairment_disability_percentage_basis_code)
	IFF(
	    impairment_disability_percentage_basis_code = 'N/A', '0',
	    impairment_disability_percentage_basis_code
	) AS o_impairment_disability_percentage_basis_code,
	JNR_Join_Medical_Data_With_Indemnity_Claims.med_extinguishment_ind,
	JNR_Join_Medical_Data_With_Indemnity_Claims.ret_to_work_rate_of_pay_ind,
	-- *INF*: iif(v_return_to_work_date = '00000000',
	-- ' ',ret_to_work_rate_of_pay_ind)
	-- 
	-- --changes made by VS on 08/22/2011
	IFF(v_return_to_work_date = '00000000', ' ', ret_to_work_rate_of_pay_ind) AS o_ret_to_work_rate_of_pay_ind,
	JNR_Join_Medical_Data_With_Indemnity_Claims.max_med_improvement_date,
	JNR_Join_Medical_Data_With_Indemnity_Claims.Loss_Condition_Code,
	sysdate AS create_date,
	EXP_Calculations.o_Trans_Status AS trans_status,
	'00000' AS prev_carrier_code,
	'00000000' AS prev_policy_eff_date,
	'00000000' AS Prev_reported_to_insurer_date,
	JNR_Join_Medical_Data_With_Indemnity_Claims.pol_sym3 AS pol_sym,
	JNR_Join_Medical_Data_With_Indemnity_Claims.ClassCode3 AS Classcode,
	-- *INF*: IIF(pol_sym = '000',Classcode,SUBSTR(LTRIM(RTRIM(Classcode)),1,4))
	IFF(pol_sym = '000', Classcode, SUBSTR(LTRIM(RTRIM(Classcode)), 1, 4)) AS V_classcode,
	V_classcode AS O_classcode
	FROM EXP_Calculations
	 -- Manually join with JNR_Join_Medical_Data_With_Indemnity_Claims
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