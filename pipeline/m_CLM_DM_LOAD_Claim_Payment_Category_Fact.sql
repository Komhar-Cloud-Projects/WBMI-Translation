WITH
LKP_calender_dim AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
),
SQ_claim_payment_category AS (
	SELECT A.claim_pay_ctgry_id,
	       A.claim_pay_ctgry_ak_id,
	       A.claim_pay_ak_id,
	       A.claim_pay_ctgry_type,
	       A.claim_pay_ctgry_seq_num,
	       A.claim_pay_ctgry_amt,
	       A.claim_pay_ctgry_earned_amt,
	       A.claim_pay_ctgry_billed_amt,
	       A.claim_pay_ctgry_start_date,
	       A.claim_pay_ctgry_end_date,
	       A.financial_type_code,
	       A.invc_num,
	       A.cost_containment_saving_amt,
	       A.cost_containment_red_amt,
	       A.cost_containment_ppo_amt,
	       A.attorney_fee_amt,
	       A.attorney_cost_amt,
	       A.attorney_file_num,
	       A.hourly_rate,
	       A.hours_worked,
	       A.num_of_days,
	       A.num_of_weeks,
	       A.tpd_rate,
	       A.tpd_rate_fac,
	       A.tpd_wage_loss,
	       A.tpd_wkly_wage,
	       A.claim_pay_ctgry_lump_sum_ind
	FROM   claim_payment_category A
	WHERE A.claim_pay_ctgry_id % 2= 1
),
LKP_claim_transaction AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_pay_ak_id
	FROM (
		SELECT A.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
		A.claim_pay_ak_id as claim_pay_ak_id 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction A
		WHERE A.claim_pay_ak_id <> -1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id ORDER BY claimant_cov_det_ak_id DESC) = 1
),
EXP_get_values AS (
	SELECT
	LKP_claim_transaction.claimant_cov_det_ak_id,
	SQ_claim_payment_category.claim_pay_ctgry_id,
	SQ_claim_payment_category.claim_pay_ctgry_ak_id,
	SQ_claim_payment_category.claim_pay_ak_id,
	SQ_claim_payment_category.claim_pay_ctgry_type,
	SQ_claim_payment_category.claim_pay_ctgry_seq_num,
	SQ_claim_payment_category.claim_pay_ctgry_amt,
	SQ_claim_payment_category.claim_pay_ctgry_earned_amt,
	SQ_claim_payment_category.claim_pay_ctgry_billed_amt,
	SQ_claim_payment_category.claim_pay_ctgry_start_date,
	SQ_claim_payment_category.claim_pay_ctgry_end_date,
	SQ_claim_payment_category.financial_type_code,
	SQ_claim_payment_category.invc_num,
	SQ_claim_payment_category.cost_containment_saving_amt,
	SQ_claim_payment_category.cost_containment_red_amt,
	SQ_claim_payment_category.cost_containment_ppo_amt,
	SQ_claim_payment_category.attorney_fee_amt,
	SQ_claim_payment_category.attorney_cost_amt,
	SQ_claim_payment_category.attorney_file_num,
	SQ_claim_payment_category.hourly_rate,
	SQ_claim_payment_category.hours_worked,
	SQ_claim_payment_category.num_of_days,
	SQ_claim_payment_category.num_of_weeks,
	SQ_claim_payment_category.tpd_rate,
	SQ_claim_payment_category.tpd_rate_fac,
	SQ_claim_payment_category.tpd_wage_loss,
	SQ_claim_payment_category.tpd_wkly_wage,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS trans_date,
	SQ_claim_payment_category.claim_pay_ctgry_lump_sum_ind
	FROM SQ_claim_payment_category
	LEFT JOIN LKP_claim_transaction
	ON LKP_claim_transaction.claim_pay_ak_id = SQ_claim_payment_category.claim_pay_ak_id
),
LKP_ClaimFeature AS (
	SELECT
	ClaimRepresentativeAkId,
	FeatureRepresentativeAssignedDate,
	claimant_cov_det_ak_id
	FROM (
		select CF.ClaimRepresentativeAkId as ClaimRepresentativeAkId, 
			CF.FeatureRepresentativeAssignedDate as FeatureRepresentativeAssignedDate, 
			CCD.claimant_cov_det_ak_id as claimant_cov_det_ak_id
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO 
			on CCD.claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id and CPO.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ClaimFeature CF 
			on CCD.claim_party_occurrence_ak_id = CF.ClaimPartyOccurrenceAKId and CF.CurrentSnapshotFlag = 1
		where CCD.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY ClaimRepresentativeAkId) = 1
),
LKP_Claim_Payment_Dim AS (
	SELECT
	IN_claim_pay_ak_id,
	claim_pay_dim_id,
	pay_issued_date,
	pay_cashed_date,
	pay_voided_date,
	pay_reposted_date,
	edw_claim_pay_ak_id
	FROM (
		SELECT claim_payment_dim.claim_pay_dim_id    AS claim_pay_dim_id,
		       claim_payment_dim.pay_issued_date     AS pay_issued_date,
		       claim_payment_dim.pay_cashed_date     AS pay_cashed_date,
		       claim_payment_dim.pay_voided_date     AS pay_voided_date,
		       claim_payment_dim.pay_reposted_date   AS pay_reposted_date,
		       claim_payment_dim.edw_claim_pay_ak_id AS edw_claim_pay_ak_id
		FROM   @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_payment_dim
		WHERE  edw_claim_pay_ak_id IN 
		(SELECT claim_pay_ak_id  FROM   @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_payment_category)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_pay_ak_id ORDER BY IN_claim_pay_ak_id DESC) = 1
),
LKP_claim_financial_type_dim AS (
	SELECT
	claim_financial_type_dim_id,
	financial_type_code
	FROM (
		SELECT 
			claim_financial_type_dim_id,
			financial_type_code
		FROM claim_financial_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY financial_type_code ORDER BY claim_financial_type_dim_id) = 1
),
LKP_claim_pay_ctgry_type_dim AS (
	SELECT
	claim_pay_ctgry_type_dim_id,
	claim_pay_ctgry_type,
	claim_pay_ctgry_lump_sum_ind
	FROM (
		SELECT 
			claim_pay_ctgry_type_dim_id,
			claim_pay_ctgry_type,
			claim_pay_ctgry_lump_sum_ind
		FROM claim_payment_category_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ctgry_type,claim_pay_ctgry_lump_sum_ind ORDER BY claim_pay_ctgry_type_dim_id) = 1
),
LKP_claim_rep_dim_id AS (
	SELECT
	claim_rep_dim_id,
	edw_claim_rep_ak_id
	FROM (
		SELECT 
			claim_rep_dim_id,
			edw_claim_rep_ak_id
		FROM claim_representative_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_rep_ak_id ORDER BY claim_rep_dim_id) = 1
),
mplt_claimant_coverage_dim_id AS (WITH
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_CLAIMANT_COV_DIM AS (
		SELECT
		claimant_cov_dim_id,
		edw_claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claimant_cov_dim_id,
				edw_claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM claimant_coverage_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claimant_cov_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		claimant_cov_dim_id
		FROM LKP_CLAIMANT_COV_DIM
	),
),
mplt_coverage_dim_id AS (WITH
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_coverage_dim AS (
		SELECT
		cov_dim_id,
		edw_claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				cov_dim_id,
				edw_claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM coverage_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY cov_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		cov_dim_id
		FROM LKP_coverage_dim
	),
),
EXP_Cov_Level_Dim_Ids AS (
	SELECT
	EXP_get_values.claimant_cov_det_ak_id,
	EXP_get_values.trans_date,
	mplt_coverage_dim_id.cov_dim_id,
	LKP_claim_pay_ctgry_type_dim.claim_pay_ctgry_type_dim_id,
	LKP_Claim_Payment_Dim.claim_pay_dim_id,
	LKP_Claim_Payment_Dim.pay_issued_date,
	LKP_Claim_Payment_Dim.pay_cashed_date,
	LKP_Claim_Payment_Dim.pay_voided_date,
	LKP_Claim_Payment_Dim.pay_reposted_date,
	LKP_claim_financial_type_dim.claim_financial_type_dim_id,
	mplt_claimant_coverage_dim_id.claimant_cov_dim_id,
	EXP_get_values.claim_pay_ctgry_id,
	EXP_get_values.claim_pay_ctgry_ak_id,
	EXP_get_values.claim_pay_ak_id,
	EXP_get_values.claim_pay_ctgry_type,
	EXP_get_values.claim_pay_ctgry_seq_num,
	EXP_get_values.claim_pay_ctgry_amt,
	EXP_get_values.claim_pay_ctgry_earned_amt,
	EXP_get_values.claim_pay_ctgry_billed_amt,
	EXP_get_values.claim_pay_ctgry_start_date,
	EXP_get_values.claim_pay_ctgry_end_date,
	EXP_get_values.financial_type_code,
	EXP_get_values.invc_num,
	EXP_get_values.cost_containment_saving_amt,
	EXP_get_values.cost_containment_red_amt,
	EXP_get_values.cost_containment_ppo_amt,
	EXP_get_values.attorney_fee_amt,
	EXP_get_values.attorney_cost_amt,
	EXP_get_values.attorney_file_num,
	EXP_get_values.hourly_rate,
	EXP_get_values.hours_worked,
	EXP_get_values.num_of_days,
	EXP_get_values.num_of_weeks,
	EXP_get_values.tpd_rate,
	EXP_get_values.tpd_rate_fac,
	EXP_get_values.tpd_wage_loss,
	EXP_get_values.tpd_wkly_wage,
	LKP_claim_rep_dim_id.claim_rep_dim_id AS FeatureRepresentativeDimId,
	LKP_ClaimFeature.FeatureRepresentativeAssignedDate
	FROM EXP_get_values
	 -- Manually join with mplt_claimant_coverage_dim_id
	 -- Manually join with mplt_coverage_dim_id
	LEFT JOIN LKP_ClaimFeature
	ON LKP_ClaimFeature.claimant_cov_det_ak_id = EXP_get_values.claimant_cov_det_ak_id
	LEFT JOIN LKP_Claim_Payment_Dim
	ON LKP_Claim_Payment_Dim.edw_claim_pay_ak_id = EXP_get_values.claim_pay_ak_id
	LEFT JOIN LKP_claim_financial_type_dim
	ON LKP_claim_financial_type_dim.financial_type_code = EXP_get_values.financial_type_code
	LEFT JOIN LKP_claim_pay_ctgry_type_dim
	ON LKP_claim_pay_ctgry_type_dim.claim_pay_ctgry_type = EXP_get_values.claim_pay_ctgry_type AND LKP_claim_pay_ctgry_type_dim.claim_pay_ctgry_lump_sum_ind = EXP_get_values.claim_pay_ctgry_lump_sum_ind
	LEFT JOIN LKP_claim_rep_dim_id
	ON LKP_claim_rep_dim_id.edw_claim_rep_ak_id = LKP_ClaimFeature.ClaimRepresentativeAkId
),
mplt_Claimant_dim_id AS (WITH
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_Claimant_coverage_detail AS (
		SELECT
		claim_party_occurrence_ak_id,
		claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_party_occurrence_ak_id,
				claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM claimant_coverage_detail
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_CLAIMANT_DIM AS (
		SELECT
		claimant_dim_id,
		edw_claim_party_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT claimant_dim.claimant_dim_id as claimant_dim_id, claimant_dim.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id, claimant_dim.eff_from_date as eff_from_date, claimant_dim.eff_to_date as eff_to_date 
			FROM claimant_dim
			WHERE edw_claim_party_occurrence_ak_id IN
			(select claim_party_occurrence_ak_id from @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_party_occurrence where claim_party_role_code in ('CMT','CLMT'))
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claimant_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		claimant_dim_id
		FROM LKP_CLAIMANT_DIM
	),
),
EXP_Claimant_Dim_Ids AS (
	SELECT
	EXP_Cov_Level_Dim_Ids.claimant_cov_det_ak_id,
	EXP_Cov_Level_Dim_Ids.trans_date,
	EXP_Cov_Level_Dim_Ids.cov_dim_id,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_type_dim_id,
	EXP_Cov_Level_Dim_Ids.claim_pay_dim_id,
	EXP_Cov_Level_Dim_Ids.pay_issued_date,
	EXP_Cov_Level_Dim_Ids.pay_cashed_date,
	EXP_Cov_Level_Dim_Ids.pay_voided_date,
	EXP_Cov_Level_Dim_Ids.pay_reposted_date,
	EXP_Cov_Level_Dim_Ids.claim_financial_type_dim_id,
	EXP_Cov_Level_Dim_Ids.claimant_cov_dim_id,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_id,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_ak_id,
	EXP_Cov_Level_Dim_Ids.claim_pay_ak_id,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_type,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_seq_num,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_amt,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_earned_amt,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_billed_amt,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_start_date,
	EXP_Cov_Level_Dim_Ids.claim_pay_ctgry_end_date,
	EXP_Cov_Level_Dim_Ids.financial_type_code,
	EXP_Cov_Level_Dim_Ids.invc_num,
	EXP_Cov_Level_Dim_Ids.cost_containment_saving_amt,
	EXP_Cov_Level_Dim_Ids.cost_containment_red_amt,
	EXP_Cov_Level_Dim_Ids.cost_containment_ppo_amt,
	EXP_Cov_Level_Dim_Ids.attorney_fee_amt,
	EXP_Cov_Level_Dim_Ids.attorney_cost_amt,
	EXP_Cov_Level_Dim_Ids.attorney_file_num,
	EXP_Cov_Level_Dim_Ids.hourly_rate,
	EXP_Cov_Level_Dim_Ids.hours_worked,
	EXP_Cov_Level_Dim_Ids.num_of_days,
	EXP_Cov_Level_Dim_Ids.num_of_weeks,
	EXP_Cov_Level_Dim_Ids.tpd_rate,
	EXP_Cov_Level_Dim_Ids.tpd_rate_fac,
	EXP_Cov_Level_Dim_Ids.tpd_wage_loss,
	EXP_Cov_Level_Dim_Ids.tpd_wkly_wage,
	mplt_Claimant_dim_id.claimant_dim_id,
	EXP_Cov_Level_Dim_Ids.FeatureRepresentativeDimId,
	EXP_Cov_Level_Dim_Ids.FeatureRepresentativeAssignedDate
	FROM EXP_Cov_Level_Dim_Ids
	 -- Manually join with mplt_Claimant_dim_id
),
mplt_Claim_occurence_dim_id AS (WITH
	LKP_claim_representative AS (
		SELECT
		claim_rep_wbconnect_user_id,
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			CASE claim_representative.claim_rep_wbconnect_user_id  WHEN 'N/A' THEN claim_representative.claim_rep_key 
			ELSE claim_representative.claim_rep_wbconnect_user_id END AS claim_rep_wbconnect_user_id, claim_representative.claim_rep_ak_id as claim_rep_ak_id, claim_representative.eff_from_date as eff_from_date, claim_representative.eff_to_date as eff_to_date FROM claim_representative
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_wbconnect_user_id DESC) = 1
	),
	LKP_claim_rep_dim AS (
		SELECT
		claim_rep_dim_id,
		claim_rep_wbconnect_user_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_rep_dim_id,
				claim_rep_wbconnect_user_id,
				eff_from_date,
				eff_to_date
			FROM claim_representative_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_wbconnect_user_id,eff_from_date,eff_to_date ORDER BY claim_rep_dim_id DESC) = 1
	),
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_Claimant_coverage_detail AS (
		SELECT
		claim_party_occurrence_ak_id,
		claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_party_occurrence_ak_id,
				claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM claimant_coverage_detail
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_Claim_Party_occurrence AS (
		SELECT
		claim_occurrence_ak_id,
		claim_case_ak_id,
		claim_party_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT claim_party_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_party_occurrence.claim_case_ak_id as claim_case_ak_id, claim_party_occurrence.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, claim_party_occurrence.eff_from_date as eff_from_date, claim_party_occurrence.eff_to_date as eff_to_date 
			FROM claim_party_occurrence
			WHERE
			claim_party_occurrence.claim_party_role_code in ('CMT','CLMT')
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_ak_id) = 1
	),
	LKP_Claim_Rep_Occurrence_PLH AS (
		SELECT
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'PLH'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Claim_Case AS (
		SELECT
		claim_case_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_case_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_case
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id,eff_from_date,eff_to_date ORDER BY claim_case_ak_id) = 1
	),
	EXP_get_reserve_calc_ids AS (
		SELECT
		LKP_Claim_Party_occurrence.claim_occurrence_ak_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_ak_id), -1, claim_occurrence_ak_id)
		IFF(claim_occurrence_ak_id IS NULL, - 1, claim_occurrence_ak_id) AS claim_occurrence_ak_id_out,
		EXP_get_values.IN_trans_date
		FROM EXP_get_values
		LEFT JOIN LKP_Claim_Party_occurrence
		ON LKP_Claim_Party_occurrence.claim_party_occurrence_ak_id = LKP_Claimant_coverage_detail.claim_party_occurrence_ak_id AND LKP_Claim_Party_occurrence.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Party_occurrence.eff_to_date >= EXP_get_values.IN_trans_date
	),
	LKP_claim_occurrence_Date AS (
		SELECT
		claim_occurrence_id,
		pol_key_ak_id,
		claim_loss_date,
		claim_discovery_date,
		claim_cat_start_date,
		claim_cat_end_date,
		claim_created_by_key,
		claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_id,
				pol_key_ak_id,
				claim_loss_date,
				claim_discovery_date,
				claim_cat_start_date,
				claim_cat_end_date,
				claim_created_by_key,
				claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_id DESC) = 1
	),
	LKP_Claim_Rep_Occurrence_Handler AS (
		SELECT
		claim_rep_ak_id,
		claim_assigned_date,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_assigned_date as claim_assigned_date, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'H'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Claim_Rep_Occurrence_Examiner AS (
		SELECT
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'E'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_claim_occurrence_dim AS (
		SELECT
		claim_occurrence_dim_id,
		source_claim_rpted_date,
		claim_rpted_date,
		claim_scripted_date,
		claim_open_date,
		claim_close_date,
		claim_reopen_date,
		claim_closed_after_reopen_date,
		claim_notice_only_date,
		edw_claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_dim_id,
				source_claim_rpted_date,
				claim_rpted_date,
				claim_scripted_date,
				claim_open_date,
				claim_close_date,
				claim_reopen_date,
				claim_closed_after_reopen_date,
				claim_notice_only_date,
				edw_claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_dim_id DESC) = 1
	),
	LKP_Claim_Created_by_rep_ak_id AS (
		SELECT
		claim_rep_ak_id,
		claim_rep_wbconnect_user_id,
		claim_rep_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_rep_ak_id,
				claim_rep_wbconnect_user_id,
				claim_rep_key,
				eff_from_date,
				eff_to_date
			FROM claim_representative
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id) = 1
	),
	LKP_Claim_Case_Dim AS (
		SELECT
		claim_case_dim_id,
		edw_claim_case_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_case_dim_id,
				edw_claim_case_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_case_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_case_ak_id,eff_from_date,eff_to_date ORDER BY claim_case_dim_id) = 1
	),
	LKP_V2_policy AS (
		SELECT
		pol_id,
		contract_cust_ak_id,
		agency_ak_id,
		AgencyAKID,
		StrategicProfitCenterAKId,
		InsuranceSegmentAKId,
		PolicyOfferingAKId,
		pol_eff_date,
		pol_exp_date,
		pol_sym,
		pol_num,
		pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT policy.pol_id as pol_id, policy.contract_cust_ak_id as contract_cust_ak_id, 
			policy.agency_ak_id as agency_ak_id,
			policy.AgencyAKID as AgencyAKID, 
			policy.StrategicProfitCenterAKId as StrategicProfitCenterAKId,
			policy.InsuranceSegmentAKId as InsuranceSegmentAKId,
			policy.PolicyOfferingAKId as PolicyOfferingAKId,
			policy.pol_eff_date as pol_eff_date, policy.pol_exp_date as pol_exp_date, policy.pol_sym as pol_sym, policy.pol_num as pol_num, policy.pol_ak_id as pol_ak_id, policy.eff_from_date as eff_from_date, policy.eff_to_date as eff_to_date 
			FROM 
			v2.policy policy
			WHERE 
			policy.pol_ak_id IN (select distinct pol_key_ak_id from claim_occurrence)
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_id DESC) = 1
	),
	LKP_contract_customer_dim AS (
		SELECT
		contract_cust_dim_id,
		edw_contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT contract_customer_dim.contract_cust_dim_id as contract_cust_dim_id, contract_customer_dim.edw_contract_cust_ak_id as edw_contract_cust_ak_id, contract_customer_dim.eff_from_date as eff_from_date, contract_customer_dim.eff_to_date as eff_to_date 
			FROM contract_customer_dim
			WHERE edw_contract_cust_ak_id IN
			(
			SELECT DISTINCT CC.contract_cust_ak_id 
			FROM @{pipeline().parameters.DB_NAME_EDW}.dbo.contract_customer CC, @{pipeline().parameters.DB_NAME_EDW}.V2.policy P, @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence CO
			WHERE CC.contract_cust_ak_id = P.contract_cust_ak_id
			AND CO.pol_key_ak_id = P.pol_ak_id
			AND P.crrnt_snpsht_flag = 1
			AND CC.crrnt_snpsht_flag = 1
			)
			
			--- 2/12/2014 : Modified the Lookup Query to join on AK ID values instead of Natural Key
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
	),
	LKP_AgencyDim AS (
		SELECT
		AgencyDimID,
		EDWAgencyAKID,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				AgencyDimID,
				EDWAgencyAKID,
				EffectiveDate,
				ExpirationDate
			FROM V3.AgencyDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID,EffectiveDate,ExpirationDate ORDER BY AgencyDimID DESC) = 1
	),
	LKP_V2_Agency AS (
		SELECT
		SalesTerritoryAKID,
		RegionalSalesManagerAKID,
		AgencyAKID,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				SalesTerritoryAKID,
				RegionalSalesManagerAKID,
				AgencyAKID,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID,EffectiveDate,ExpirationDate ORDER BY SalesTerritoryAKID) = 1
	),
	LKP_agency_dim AS (
		SELECT
		agency_dim_id,
		edw_agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				agency_dim_id,
				edw_agency_ak_id,
				eff_from_date,
				eff_to_date
			FROM V2.agency_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_Policy_Dim AS (
		SELECT
		pol_dim_id,
		edw_pol_pk_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			policy_dim.pol_dim_id as pol_dim_id, 
			policy_dim.edw_pol_pk_id as edw_pol_pk_id, 
			policy_dim.eff_from_date as eff_from_date, 
			policy_dim.eff_to_date as eff_to_date 
			FROM policy_dim
			WHERE edw_pol_pk_id IN 
			(SELECT policy.pol_id as pol_id FROM @{pipeline().parameters.DB_NAME_EDW}.v2.policy policy
			WHERE policy.pol_ak_id IN (select distinct pol_key_ak_id from @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence))
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_pk_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
	),
	EXP_Lkp_Dim_ids AS (
		SELECT
		EXP_get_reserve_calc_ids.IN_trans_date,
		LKP_Claim_Rep_Occurrence_Handler.claim_rep_ak_id AS claim_rep_primary_rep_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_primary_rep_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_primary_rep_wbconnect_user_id,
		LKP_Claim_Rep_Occurrence_Examiner.claim_rep_ak_id AS claim_rep_examiner_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_examiner_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_examiner_wbconnect_user_id,
		LKP_Claim_Rep_Occurrence_PLH.claim_rep_ak_id AS claim_rep_primary_lit_handler_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_primary_lit_handler_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_primary_lit_handler_wbconnect_user_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_primary_rep_ak_id, v_claim_rep_primary_rep_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_prim_claim_rep_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_examiner_ak_id, v_claim_rep_examiner_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_examiner_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_primary_lit_handler_ak_id, v_claim_rep_primary_lit_handler_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_prim_litigation_handler_id,
		LKP_Claim_Created_by_rep_ak_id.claim_rep_ak_id,
		LKP_Claim_Created_by_rep_ak_id.claim_rep_wbconnect_user_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_ak_id,claim_rep_wbconnect_user_id,IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_created_by_id
		FROM EXP_get_reserve_calc_ids
		LEFT JOIN LKP_Claim_Created_by_rep_ak_id
		ON LKP_Claim_Created_by_rep_ak_id.claim_rep_key = LKP_claim_occurrence_Date.claim_created_by_key AND LKP_Claim_Created_by_rep_ak_id.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Created_by_rep_ak_id.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Examiner
		ON LKP_Claim_Rep_Occurrence_Examiner.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Examiner.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Examiner.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_PLH
		ON LKP_Claim_Rep_Occurrence_PLH.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_PLH.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_PLH.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_primary_rep_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_examiner_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_primary_lit_handler_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_primary_rep_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_primary_rep_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_examiner_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_examiner_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_primary_lit_handler_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_primary_lit_handler_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.eff_from_date = claim_rep_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
	),
	LKP_RegionalSalesManager AS (
		SELECT
		SalesDirectorAKID,
		RegionalSalesManagerAKID,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				SalesDirectorAKID,
				RegionalSalesManagerAKID,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RegionalSalesManagerAKID,EffectiveDate,ExpirationDate ORDER BY SalesDirectorAKID) = 1
	),
	OUTPUT AS (
		SELECT
		LKP_claim_occurrence_dim.claim_occurrence_dim_id, 
		LKP_claim_occurrence_Date.claim_loss_date, 
		LKP_claim_occurrence_Date.claim_discovery_date, 
		LKP_claim_occurrence_dim.claim_scripted_date, 
		LKP_claim_occurrence_dim.source_claim_rpted_date, 
		LKP_claim_occurrence_dim.claim_rpted_date AS claim_occurrence_rpted_date, 
		LKP_claim_occurrence_dim.claim_open_date, 
		LKP_claim_occurrence_dim.claim_close_date, 
		LKP_claim_occurrence_dim.claim_reopen_date, 
		LKP_claim_occurrence_dim.claim_closed_after_reopen_date, 
		LKP_claim_occurrence_dim.claim_notice_only_date, 
		LKP_claim_occurrence_Date.claim_cat_start_date, 
		LKP_claim_occurrence_Date.claim_cat_end_date, 
		LKP_Claim_Rep_Occurrence_Handler.claim_assigned_date AS claim_rep_assigned_date, 
		LKP_Claim_Rep_Occurrence_Handler.eff_to_date AS claim_rep_unassigned_date, 
		EXP_Lkp_Dim_ids.claim_rep_dim_prim_claim_rep_id, 
		EXP_Lkp_Dim_ids.claim_rep_dim_examiner_id, 
		EXP_Lkp_Dim_ids.claim_rep_dim_prim_litigation_handler_id, 
		LKP_Policy_Dim.pol_dim_id AS pol_key_dim_id, 
		LKP_V2_policy.pol_eff_date, 
		LKP_V2_policy.pol_exp_date, 
		LKP_AgencyDim.AgencyDimID, 
		EXP_Lkp_Dim_ids.claim_created_by_id, 
		LKP_Claim_Case_Dim.claim_case_dim_id, 
		LKP_contract_customer_dim.contract_cust_dim_id, 
		LKP_V2_policy.pol_sym, 
		LKP_V2_policy.pol_num, 
		LKP_V2_policy.AgencyAKID, 
		LKP_V2_Agency.SalesTerritoryAKID, 
		LKP_V2_Agency.RegionalSalesManagerAKID, 
		LKP_RegionalSalesManager.SalesDirectorAKID, 
		LKP_V2_policy.StrategicProfitCenterAKId, 
		LKP_V2_policy.InsuranceSegmentAKId, 
		LKP_V2_policy.PolicyOfferingAKId, 
		LKP_agency_dim.agency_dim_id, 
		LKP_claim_occurrence_Date.pol_key_ak_id AS PolicyAkid
		FROM EXP_Lkp_Dim_ids
		LEFT JOIN LKP_AgencyDim
		ON LKP_AgencyDim.EDWAgencyAKID = LKP_V2_policy.AgencyAKID AND LKP_AgencyDim.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_AgencyDim.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Case_Dim
		ON LKP_Claim_Case_Dim.edw_claim_case_ak_id = LKP_Claim_Case.claim_case_ak_id AND LKP_Claim_Case_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Case_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Policy_Dim
		ON LKP_Policy_Dim.edw_pol_pk_id = LKP_V2_policy.pol_id AND LKP_Policy_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Policy_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_RegionalSalesManager
		ON LKP_RegionalSalesManager.RegionalSalesManagerAKID = LKP_V2_Agency.RegionalSalesManagerAKID AND LKP_RegionalSalesManager.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_RegionalSalesManager.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_V2_Agency
		ON LKP_V2_Agency.AgencyAKID = LKP_V2_policy.AgencyAKID AND LKP_V2_Agency.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_V2_Agency.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_V2_policy
		ON LKP_V2_policy.pol_ak_id = LKP_claim_occurrence_Date.pol_key_ak_id AND LKP_V2_policy.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_V2_policy.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_agency_dim
		ON LKP_agency_dim.edw_agency_ak_id = LKP_V2_policy.agency_ak_id AND LKP_agency_dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_agency_dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_Date
		ON LKP_claim_occurrence_Date.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_claim_occurrence_Date.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence_Date.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_dim
		ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = EXP_get_reserve_calc_ids.claim_occurrence_ak_id_out AND LKP_claim_occurrence_dim.eff_from_date <= EXP_get_reserve_calc_ids.IN_trans_date AND LKP_claim_occurrence_dim.eff_to_date >= EXP_get_reserve_calc_ids.IN_trans_date
		LEFT JOIN LKP_contract_customer_dim
		ON LKP_contract_customer_dim.edw_contract_cust_ak_id = LKP_V2_policy.contract_cust_ak_id AND LKP_contract_customer_dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_contract_customer_dim.eff_to_date >= EXP_get_values.IN_trans_date
	),
),
mplt_Strategic_Business_Division_Dim AS (WITH
	INPUT_Strategic_Business_Division AS (
		
	),
	EXP_inputs AS (
		SELECT
		policy_symbol,
		policy_number,
		policy_eff_date AS policy_eff_date_in,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_symbol)='N/A','N/A',substr(policy_symbol,1,1))
		IFF(
		    UDF_DEFAULT_VALUE_FOR_STRINGS(policy_symbol) = 'N/A', 'N/A', substr(policy_symbol, 1, 1)
		) AS policy_symbol_position_1,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_number)='N/A','N/A',substr(policy_number,1,1))
		IFF(
		    UDF_DEFAULT_VALUE_FOR_STRINGS(policy_number) = 'N/A', 'N/A', substr(policy_number, 1, 1)
		) AS policy_number_position_1,
		-- *INF*: IIF(isnull(policy_eff_date_in),SYSDATE,policy_eff_date_in)
		IFF(policy_eff_date_in IS NULL, CURRENT_TIMESTAMP, policy_eff_date_in) AS policy_eff_date
		FROM INPUT_Strategic_Business_Division
	),
	LKP_strategic_business_division_dim AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		created_date,
		modified_date,
		edw_strtgc_bus_dvsn_ak_id,
		pol_sym_1,
		pol_num_1,
		pol_eff_date,
		pol_exp_date,
		strtgc_bus_dvsn_code,
		strtgc_bus_dvsn_code_descript,
		policy_symbol_position_IN,
		policy_number_position_IN,
		policy_eff_date_IN
		FROM (
			SELECT 
				strtgc_bus_dvsn_dim_id,
				crrnt_snpsht_flag,
				audit_id,
				eff_from_date,
				eff_to_date,
				created_date,
				modified_date,
				edw_strtgc_bus_dvsn_ak_id,
				pol_sym_1,
				pol_num_1,
				pol_eff_date,
				pol_exp_date,
				strtgc_bus_dvsn_code,
				strtgc_bus_dvsn_code_descript,
				policy_symbol_position_IN,
				policy_number_position_IN,
				policy_eff_date_IN
			FROM strategic_business_division_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_sym_1,pol_num_1,pol_eff_date,pol_exp_date ORDER BY strtgc_bus_dvsn_dim_id) = 1
	),
	EXP_check_outputs AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		edw_strtgc_bus_dvsn_ak_id,
		strtgc_bus_dvsn_code,
		strtgc_bus_dvsn_code_descript,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_dim_id),-1,strtgc_bus_dvsn_dim_id)
		IFF(strtgc_bus_dvsn_dim_id IS NULL, - 1, strtgc_bus_dvsn_dim_id) AS strtgc_bus_dvsn_id_out,
		-- *INF*: IIF(isnull(edw_strtgc_bus_dvsn_ak_id),-1,edw_strtgc_bus_dvsn_ak_id)
		IFF(edw_strtgc_bus_dvsn_ak_id IS NULL, - 1, edw_strtgc_bus_dvsn_ak_id) AS edw_strtgc_bus_dvsn_ak_id_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code),'N/A',strtgc_bus_dvsn_code)
		IFF(strtgc_bus_dvsn_code IS NULL, 'N/A', strtgc_bus_dvsn_code) AS strtgc_bus_dvsn_code_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code_descript),'N/A',strtgc_bus_dvsn_code_descript)
		IFF(strtgc_bus_dvsn_code_descript IS NULL, 'N/A', strtgc_bus_dvsn_code_descript) AS strtgc_bus_dvsn_code_descript_out
		FROM LKP_strategic_business_division_dim
	),
	OUTPUT_return_Strategic_Business_Division AS (
		SELECT
		strtgc_bus_dvsn_id_out AS strtgc_bus_dvsn_dim_id, 
		edw_strtgc_bus_dvsn_ak_id_out AS edw_strtgc_bus_dvsn_ak_id, 
		strtgc_bus_dvsn_code_out AS strtgc_bus_dvsn_code, 
		strtgc_bus_dvsn_code_descript_out AS strtgc_bus_dvsn_code_descript
		FROM EXP_check_outputs
	),
),
EXP_get_dim_id AS (
	SELECT
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_id,
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_ak_id,
	EXP_Claimant_Dim_Ids.claim_pay_ak_id,
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_type,
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_seq_num,
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_amt,
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_earned_amt,
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_billed_amt,
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_start_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_pay_ctgry_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	-- 
	LKP_CALENDER_DIM_to_date_to_char_claim_pay_ctgry_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_pay_ctgry_start_date_id,
	-- *INF*: IIF(ISNULL(v_claim_pay_ctgry_start_date_id), -1, v_claim_pay_ctgry_start_date_id)
	IFF(v_claim_pay_ctgry_start_date_id IS NULL, - 1, v_claim_pay_ctgry_start_date_id) AS claim_pay_ctgry_start_date_id_out,
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_end_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_pay_ctgry_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_pay_ctgry_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_pay_ctgry_end_date_id,
	-- *INF*: IIF(ISNULL(v_claim_pay_ctgry_end_date_id), -1, v_claim_pay_ctgry_end_date_id)
	IFF(v_claim_pay_ctgry_end_date_id IS NULL, - 1, v_claim_pay_ctgry_end_date_id) AS claim_pay_ctgry_end_date_id_out,
	EXP_Claimant_Dim_Ids.claim_financial_type_dim_id,
	-- *INF*: IIF(ISNULL(claim_financial_type_dim_id), -1, claim_financial_type_dim_id)
	IFF(claim_financial_type_dim_id IS NULL, - 1, claim_financial_type_dim_id) AS claim_financial_type_dim_id_out,
	EXP_Claimant_Dim_Ids.financial_type_code,
	EXP_Claimant_Dim_Ids.invc_num,
	EXP_Claimant_Dim_Ids.cost_containment_saving_amt,
	EXP_Claimant_Dim_Ids.cost_containment_red_amt,
	EXP_Claimant_Dim_Ids.cost_containment_ppo_amt,
	EXP_Claimant_Dim_Ids.attorney_fee_amt,
	EXP_Claimant_Dim_Ids.attorney_cost_amt,
	EXP_Claimant_Dim_Ids.attorney_file_num,
	EXP_Claimant_Dim_Ids.hourly_rate,
	EXP_Claimant_Dim_Ids.hours_worked,
	EXP_Claimant_Dim_Ids.num_of_days,
	EXP_Claimant_Dim_Ids.num_of_weeks,
	EXP_Claimant_Dim_Ids.tpd_rate,
	EXP_Claimant_Dim_Ids.tpd_rate_fac,
	EXP_Claimant_Dim_Ids.tpd_wage_loss,
	EXP_Claimant_Dim_Ids.tpd_wkly_wage,
	mplt_Claim_occurence_dim_id.claim_occurrence_dim_id,
	-- *INF*: IIF(ISNULL(claim_occurrence_dim_id), -1, claim_occurrence_dim_id)
	IFF(claim_occurrence_dim_id IS NULL, - 1, claim_occurrence_dim_id) AS claim_occurrence_dim_id_out,
	EXP_Claimant_Dim_Ids.claimant_dim_id,
	-- *INF*: IIF(ISNULL(claimant_dim_id), -1, claimant_dim_id)
	IFF(claimant_dim_id IS NULL, - 1, claimant_dim_id) AS claimant_dim_id_out,
	EXP_Claimant_Dim_Ids.claimant_cov_dim_id,
	-- *INF*: IIF(ISNULL(claimant_cov_dim_id), -1, claimant_cov_dim_id)
	-- 
	IFF(claimant_cov_dim_id IS NULL, - 1, claimant_cov_dim_id) AS claimant_cov_dim_id_out,
	mplt_Claim_occurence_dim_id.pol_key_dim_id,
	-- *INF*: IIF(ISNULL(pol_key_dim_id), -1, pol_key_dim_id)
	IFF(pol_key_dim_id IS NULL, - 1, pol_key_dim_id) AS pol_key_dim_id_out,
	mplt_Claim_occurence_dim_id.agency_dim_id,
	-- *INF*: IIF(ISNULL(agency_dim_id), -1, agency_dim_id)
	IFF(agency_dim_id IS NULL, - 1, agency_dim_id) AS agency_dim_id_out,
	mplt_Claim_occurence_dim_id.claim_loss_date,
	mplt_Claim_occurence_dim_id.claim_discovery_date,
	mplt_Claim_occurence_dim_id.claim_occurrence_rpted_date,
	mplt_Claim_occurence_dim_id.claim_cat_start_date,
	mplt_Claim_occurence_dim_id.claim_cat_end_date,
	mplt_Claim_occurence_dim_id.claim_rep_assigned_date,
	mplt_Claim_occurence_dim_id.claim_rep_unassigned_date,
	mplt_Claim_occurence_dim_id.claim_scripted_date,
	mplt_Claim_occurence_dim_id.source_claim_rpted_date,
	mplt_Claim_occurence_dim_id.claim_open_date,
	mplt_Claim_occurence_dim_id.claim_close_date,
	mplt_Claim_occurence_dim_id.claim_reopen_date,
	mplt_Claim_occurence_dim_id.claim_closed_after_reopen_date,
	mplt_Claim_occurence_dim_id.claim_notice_only_date,
	mplt_Claim_occurence_dim_id.pol_eff_date,
	mplt_Claim_occurence_dim_id.pol_exp_date,
	EXP_Claimant_Dim_Ids.pay_issued_date,
	EXP_Claimant_Dim_Ids.pay_cashed_date,
	EXP_Claimant_Dim_Ids.pay_voided_date,
	EXP_Claimant_Dim_Ids.pay_reposted_date,
	mplt_Claim_occurence_dim_id.claim_rep_dim_prim_claim_rep_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_claim_rep_id), -1, claim_rep_dim_prim_claim_rep_id)
	-- 
	IFF(claim_rep_dim_prim_claim_rep_id IS NULL, - 1, claim_rep_dim_prim_claim_rep_id) AS claim_rep_dim_prim_claim_rep_id_out,
	EXP_Claimant_Dim_Ids.claim_pay_dim_id,
	-- *INF*: IIF(ISNULL(claim_pay_dim_id), -1, claim_pay_dim_id)
	IFF(claim_pay_dim_id IS NULL, - 1, claim_pay_dim_id) AS claim_pay_dim_id_out,
	EXP_Claimant_Dim_Ids.claim_pay_ctgry_type_dim_id,
	-- *INF*: IIF(ISNULL(claim_pay_ctgry_type_dim_id), -1, claim_pay_ctgry_type_dim_id)
	IFF(claim_pay_ctgry_type_dim_id IS NULL, - 1, claim_pay_ctgry_type_dim_id) AS claim_pay_ctgry_type_dim_id_out,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	EXP_Claimant_Dim_Ids.cov_dim_id,
	-- *INF*: IIF(ISNULL(cov_dim_id), -1, cov_dim_id)
	IFF(cov_dim_id IS NULL, - 1, cov_dim_id) AS cov_dim_id_out,
	mplt_Claim_occurence_dim_id.claim_rep_dim_examiner_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_examiner_id),-1,claim_rep_dim_examiner_id)
	IFF(claim_rep_dim_examiner_id IS NULL, - 1, claim_rep_dim_examiner_id) AS claim_rep_dim_examiner_id_out,
	mplt_Claim_occurence_dim_id.claim_rep_dim_prim_litigation_handler_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_litigation_handler_id),-1,claim_rep_dim_prim_litigation_handler_id)
	IFF(
	    claim_rep_dim_prim_litigation_handler_id IS NULL, - 1,
	    claim_rep_dim_prim_litigation_handler_id
	) AS claim_rep_dim_prim_litigation_handler_id_out,
	-1 AS default_dim_id,
	mplt_Claim_occurence_dim_id.AgencyDimID,
	mplt_Claim_occurence_dim_id.claim_created_by_id,
	-- *INF*: IIF(ISNULL(claim_created_by_id),-1,claim_created_by_id)
	IFF(claim_created_by_id IS NULL, - 1, claim_created_by_id) AS claim_created_by_dim_id_out,
	mplt_Claim_occurence_dim_id.claim_case_dim_id,
	-- *INF*: iif(isnull(claim_case_dim_id)
	-- ,-1
	-- ,claim_case_dim_id)
	IFF(claim_case_dim_id IS NULL, - 1, claim_case_dim_id) AS claim_case_dim_id_out,
	mplt_Claim_occurence_dim_id.contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(contract_cust_dim_id),-1,contract_cust_dim_id)
	IFF(contract_cust_dim_id IS NULL, - 1, contract_cust_dim_id) AS contract_cust_dim_id_out,
	mplt_Strategic_Business_Division_Dim.strtgc_bus_dvsn_dim_id,
	-- *INF*: IIF(isnull(strtgc_bus_dvsn_dim_id),-1,strtgc_bus_dvsn_dim_id)
	IFF(strtgc_bus_dvsn_dim_id IS NULL, - 1, strtgc_bus_dvsn_dim_id) AS strtgc_bus_dvsn_dim_id_out,
	mplt_Claim_occurence_dim_id.AgencyAKID,
	mplt_Claim_occurence_dim_id.SalesTerritoryAKID,
	mplt_Claim_occurence_dim_id.RegionalSalesManagerAKID,
	mplt_Claim_occurence_dim_id.SalesDirectorAKID,
	mplt_Claim_occurence_dim_id.StrategicProfitCenterAKId,
	mplt_Claim_occurence_dim_id.InsuranceSegmentAKId,
	mplt_Claim_occurence_dim_id.PolicyOfferingAKId,
	EXP_Claimant_Dim_Ids.FeatureRepresentativeDimId,
	-- *INF*: IIF(ISNULL(FeatureRepresentativeDimId),
	-- -1,
	-- FeatureRepresentativeDimId)
	IFF(FeatureRepresentativeDimId IS NULL, - 1, FeatureRepresentativeDimId) AS FeatureRepresentativeDimId_out,
	EXP_Claimant_Dim_Ids.FeatureRepresentativeAssignedDate
	FROM EXP_Claimant_Dim_Ids
	 -- Manually join with mplt_Claim_occurence_dim_id
	 -- Manually join with mplt_Strategic_Business_Division_Dim
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_pay_ctgry_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_pay_ctgry_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_pay_ctgry_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_pay_ctgry_end_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_pay_ctgry_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_pay_ctgry_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
EXP_Date_Ids AS (
	SELECT
	claim_pay_ctgry_id,
	claim_pay_ctgry_ak_id,
	claim_pay_ak_id,
	claim_pay_ctgry_type,
	claim_pay_ctgry_seq_num,
	claim_pay_ctgry_amt,
	claim_pay_ctgry_earned_amt,
	claim_pay_ctgry_billed_amt,
	claim_pay_ctgry_start_date_id_out,
	claim_pay_ctgry_end_date_id_out,
	claim_financial_type_dim_id_out,
	invc_num,
	cost_containment_saving_amt,
	cost_containment_red_amt,
	cost_containment_ppo_amt,
	attorney_fee_amt,
	attorney_cost_amt,
	attorney_file_num,
	hourly_rate,
	hours_worked,
	num_of_days,
	num_of_weeks,
	tpd_rate,
	tpd_rate_fac,
	tpd_wage_loss,
	tpd_wkly_wage,
	claim_occurrence_dim_id_out,
	claimant_dim_id_out,
	claimant_cov_dim_id_out,
	pol_key_dim_id_out,
	agency_dim_id_out,
	claim_rep_dim_prim_claim_rep_id_out,
	claim_pay_dim_id_out,
	claim_pay_ctgry_type_dim_id_out,
	audit_id,
	cov_dim_id_out,
	claim_rep_dim_examiner_id_out,
	claim_rep_dim_prim_litigation_handler_id_out,
	default_dim_id,
	claim_created_by_dim_id_out,
	claim_case_dim_id_out,
	contract_cust_dim_id_out,
	strtgc_bus_dvsn_dim_id_out,
	claim_loss_date AS IN_claim_loss_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_loss_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_loss_date_id), v_claim_loss_date_id, -1)
	IFF(v_claim_loss_date_id IS NOT NULL, v_claim_loss_date_id, - 1) AS claim_loss_date_id,
	claim_discovery_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_discovery_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_discovery_date_id), v_claim_discovery_date_id, -1)
	IFF(v_claim_discovery_date_id IS NOT NULL, v_claim_discovery_date_id, - 1) AS claim_discovery_date_id,
	claim_occurrence_rpted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_occurrence_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_occurrence_rpted_date_id), v_claim_occurrence_rpted_date_id, -1)
	IFF(v_claim_occurrence_rpted_date_id IS NOT NULL, v_claim_occurrence_rpted_date_id, - 1) AS claim_occurrence_rpted_date_id,
	claim_cat_start_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_start_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_start_date_id), v_claim_cat_start_date_id, -1)
	IFF(v_claim_cat_start_date_id IS NOT NULL, v_claim_cat_start_date_id, - 1) AS claim_cat_start_date_id,
	claim_cat_end_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_end_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_end_date_id), v_claim_cat_end_date_id, -1)
	IFF(v_claim_cat_end_date_id IS NOT NULL, v_claim_cat_end_date_id, - 1) AS claim_cat_end_date_id,
	claim_rep_assigned_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_assigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_assigned_date_id), v_claim_rep_assigned_date_id, -1)
	IFF(v_claim_rep_assigned_date_id IS NOT NULL, v_claim_rep_assigned_date_id, - 1) AS claim_rep_assigned_date_id,
	claim_rep_unassigned_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_unassigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_unassigned_date_id), v_claim_rep_unassigned_date_id, -1)
	IFF(v_claim_rep_unassigned_date_id IS NOT NULL, v_claim_rep_unassigned_date_id, - 1) AS claim_rep_unassigned_date_id,
	claim_scripted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_scripted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_scripted_date_id), v_claim_scripted_date_id, -1)
	IFF(v_claim_scripted_date_id IS NOT NULL, v_claim_scripted_date_id, - 1) AS claim_scripted_date_id,
	source_claim_rpted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_source_claim_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_source_claim_rpted_date_id), v_source_claim_rpted_date_id, -1)
	IFF(v_source_claim_rpted_date_id IS NOT NULL, v_source_claim_rpted_date_id, - 1) AS source_claim_rpted_date_id,
	claim_open_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_open_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_open_date_id), v_claim_open_date_id, -1)
	IFF(v_claim_open_date_id IS NOT NULL, v_claim_open_date_id, - 1) AS claim_open_date_id,
	claim_close_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_close_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_close_date_id), v_claim_close_date_id, -1)
	IFF(v_claim_close_date_id IS NOT NULL, v_claim_close_date_id, - 1) AS claim_close_date_id,
	claim_reopen_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_reopen_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_reopen_date_id), v_claim_reopen_date_id, -1)
	IFF(v_claim_reopen_date_id IS NOT NULL, v_claim_reopen_date_id, - 1) AS claim_reopen_date_id,
	claim_closed_after_reopen_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_closed_after_reopen_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_closed_after_reopen_date_id), v_claim_closed_after_reopen_date_id, -1)
	IFF(
	    v_claim_closed_after_reopen_date_id IS NOT NULL, v_claim_closed_after_reopen_date_id, - 1
	) AS claim_closed_after_reopen_date_id,
	claim_notice_only_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_notice_only_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_notice_only_date_id), v_claim_notice_only_date_id, -1)
	IFF(v_claim_notice_only_date_id IS NOT NULL, v_claim_notice_only_date_id, - 1) AS claim_notice_only_date_id,
	pol_eff_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_eff_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pol_eff_date_id), v_pol_eff_date_id, -1)
	IFF(v_pol_eff_date_id IS NOT NULL, v_pol_eff_date_id, - 1) AS pol_eff_date_id,
	pol_exp_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_exp_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pol_exp_date_id), v_pol_exp_date_id, -1)
	IFF(v_pol_exp_date_id IS NOT NULL, v_pol_exp_date_id, - 1) AS pol_exp_date_id,
	pay_issued_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pay_issued_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pay_issued_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_issued_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pay_issued_date_id), v_pay_issued_date_id, -1)
	IFF(v_pay_issued_date_id IS NOT NULL, v_pay_issued_date_id, - 1) AS pay_issued_date_id,
	pay_cashed_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pay_cashed_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pay_cashed_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_cashed_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pay_cashed_date_id), v_pay_cashed_date_id, -1)
	IFF(v_pay_cashed_date_id IS NOT NULL, v_pay_cashed_date_id, - 1) AS pay_cashed_date_id,
	pay_voided_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pay_voided_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pay_voided_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_voided_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pay_voided_date_id), v_pay_voided_date_id, -1)
	IFF(v_pay_voided_date_id IS NOT NULL, v_pay_voided_date_id, - 1) AS pay_voided_date_id,
	pay_reposted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pay_reposted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pay_reposted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_reposted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pay_reposted_date_id), v_pay_reposted_date_id, -1)
	IFF(v_pay_reposted_date_id IS NOT NULL, v_pay_reposted_date_id, - 1) AS pay_reposted_date_id,
	FeatureRepresentativeDimId_out AS FeatureRepresentativeDimId,
	FeatureRepresentativeAssignedDate,
	-- *INF*: :LKP.LKP_calender_dim(to_date(to_char(FeatureRepresentativeAssignedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_FeatureRepresentativeAssignedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_FeatureRepresentativeAssignedDate_id,
	-- *INF*: IIF(NOT ISNULL(v_FeatureRepresentativeAssignedDate_id),
	-- v_FeatureRepresentativeAssignedDate_id,
	-- -1)
	IFF(
	    v_FeatureRepresentativeAssignedDate_id IS NOT NULL, v_FeatureRepresentativeAssignedDate_id,
	    - 1
	) AS FeatureRepresentativeAssignedDate_id
	FROM EXP_get_dim_id
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(IN_claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pay_issued_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pay_issued_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pay_issued_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pay_cashed_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pay_cashed_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pay_cashed_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pay_voided_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pay_voided_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pay_voided_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pay_reposted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pay_reposted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pay_reposted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_FeatureRepresentativeAssignedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_FeatureRepresentativeAssignedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(FeatureRepresentativeAssignedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
LKP_Claim_Payment_Category_Fact AS (
	SELECT
	claim_pay_ctgry_fact_id,
	edw_claim_pay_ctgry_pk_id,
	edw_claim_pay_ctgry_ak_id,
	claim_occurrence_dim_id,
	claimant_dim_id,
	claimant_cov_dim_id,
	cov_dim_id,
	claim_rep_dim_prim_claim_rep_id,
	claim_rep_dim_examiner_id,
	claim_rep_dim_prim_litigation_handler_id,
	claim_rep_dim_trans_entry_oper_id,
	claim_rep_dim_claim_created_by_id,
	pol_dim_id,
	agency_dim_id,
	claim_pay_dim_id,
	claim_financial_type_dim_id,
	claim_pay_ctgry_type_dim_id,
	claim_pay_ctgry_amt,
	claim_pay_ctgry_earned_amt,
	claim_pay_ctgry_billed_amt,
	claim_pay_ctgry_start_date_id,
	claim_pay_ctgry_end_date_id,
	claim_loss_date_id,
	claim_discovery_date_id,
	claim_scripted_date_id,
	source_claim_rpted_date_id,
	claim_rpted_date_id,
	claim_open_date_id,
	claim_close_date_id,
	claim_reopen_date_id,
	claim_closed_after_reopen_date_id,
	claim_notice_only_date_id,
	claim_cat_start_date_id,
	claim_cat_end_date_id,
	claim_rep_assigned_date_id,
	claim_rep_unassigned_date_id,
	pol_eff_date_id,
	pol_exp_date_id,
	pay_issued_date_id,
	pay_cashed_date_id,
	pay_voided_date_id,
	pay_reposted_date_id,
	invc_num,
	cost_containment_saving_amt,
	cost_containment_red_amt,
	cost_containment_ppo_amt,
	attorney_fee_amt,
	attorney_cost_amt,
	attorney_file_num,
	hourly_rate,
	hours_worked,
	num_of_days,
	num_of_weeks,
	tpd_rate,
	tpd_rate_fac,
	tpd_wage_loss,
	tpd_wkly_wage,
	claim_case_dim_id,
	contract_cust_dim_id,
	strtgc_bus_dvsn_dim_id
	FROM (
		SELECT 
			claim_pay_ctgry_fact_id,
			edw_claim_pay_ctgry_pk_id,
			edw_claim_pay_ctgry_ak_id,
			claim_occurrence_dim_id,
			claimant_dim_id,
			claimant_cov_dim_id,
			cov_dim_id,
			claim_rep_dim_prim_claim_rep_id,
			claim_rep_dim_examiner_id,
			claim_rep_dim_prim_litigation_handler_id,
			claim_rep_dim_trans_entry_oper_id,
			claim_rep_dim_claim_created_by_id,
			pol_dim_id,
			agency_dim_id,
			claim_pay_dim_id,
			claim_financial_type_dim_id,
			claim_pay_ctgry_type_dim_id,
			claim_pay_ctgry_amt,
			claim_pay_ctgry_earned_amt,
			claim_pay_ctgry_billed_amt,
			claim_pay_ctgry_start_date_id,
			claim_pay_ctgry_end_date_id,
			claim_loss_date_id,
			claim_discovery_date_id,
			claim_scripted_date_id,
			source_claim_rpted_date_id,
			claim_rpted_date_id,
			claim_open_date_id,
			claim_close_date_id,
			claim_reopen_date_id,
			claim_closed_after_reopen_date_id,
			claim_notice_only_date_id,
			claim_cat_start_date_id,
			claim_cat_end_date_id,
			claim_rep_assigned_date_id,
			claim_rep_unassigned_date_id,
			pol_eff_date_id,
			pol_exp_date_id,
			pay_issued_date_id,
			pay_cashed_date_id,
			pay_voided_date_id,
			pay_reposted_date_id,
			invc_num,
			cost_containment_saving_amt,
			cost_containment_red_amt,
			cost_containment_ppo_amt,
			attorney_fee_amt,
			attorney_cost_amt,
			attorney_file_num,
			hourly_rate,
			hours_worked,
			num_of_days,
			num_of_weeks,
			tpd_rate,
			tpd_rate_fac,
			tpd_wage_loss,
			tpd_wkly_wage,
			claim_case_dim_id,
			contract_cust_dim_id,
			strtgc_bus_dvsn_dim_id
		FROM claim_payment_category_fact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_pay_ctgry_pk_id ORDER BY claim_pay_ctgry_fact_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Claim_Payment_Category_Fact.claim_pay_ctgry_fact_id AS lkp_claim_pay_ctgry_fact_id,
	LKP_Claim_Payment_Category_Fact.edw_claim_pay_ctgry_pk_id AS lkp_edw_claim_pay_ctgry_pk_id,
	LKP_Claim_Payment_Category_Fact.edw_claim_pay_ctgry_ak_id AS lkp_edw_claim_pay_ctgry_ak_id,
	LKP_Claim_Payment_Category_Fact.claim_occurrence_dim_id AS lkp_claim_occurrence_dim_id,
	LKP_Claim_Payment_Category_Fact.claimant_dim_id AS lkp_claimant_dim_id,
	LKP_Claim_Payment_Category_Fact.claimant_cov_dim_id AS lkp_claimant_cov_dim_id,
	LKP_Claim_Payment_Category_Fact.cov_dim_id AS lkp_cov_dim_id,
	LKP_Claim_Payment_Category_Fact.claim_rep_dim_prim_claim_rep_id AS lkp_claim_rep_dim_prim_claim_rep_id,
	LKP_Claim_Payment_Category_Fact.claim_rep_dim_examiner_id AS lkp_claim_rep_dim_examiner_id,
	LKP_Claim_Payment_Category_Fact.claim_rep_dim_prim_litigation_handler_id AS lkp_claim_rep_dim_prim_litigation_handler_id,
	LKP_Claim_Payment_Category_Fact.claim_rep_dim_trans_entry_oper_id AS lkp_claim_rep_dim_trans_entry_oper_id,
	LKP_Claim_Payment_Category_Fact.claim_rep_dim_claim_created_by_id AS lkp_claim_rep_dim_claim_created_by_id,
	LKP_Claim_Payment_Category_Fact.pol_dim_id AS lkp_pol_key_dim_id,
	LKP_Claim_Payment_Category_Fact.agency_dim_id AS lkp_agency_dim_id,
	LKP_Claim_Payment_Category_Fact.claim_pay_dim_id AS lkp_claim_pay_dim_id,
	LKP_Claim_Payment_Category_Fact.claim_financial_type_dim_id AS lkp_claim_financial_type_dim_id,
	LKP_Claim_Payment_Category_Fact.claim_pay_ctgry_type_dim_id AS lkp_claim_pay_ctgry_type_dim_id,
	LKP_Claim_Payment_Category_Fact.claim_pay_ctgry_amt AS lkp_claim_pay_ctgry_amt,
	LKP_Claim_Payment_Category_Fact.claim_pay_ctgry_earned_amt AS lkp_claim_pay_ctgry_earned_amt,
	LKP_Claim_Payment_Category_Fact.claim_pay_ctgry_billed_amt AS lkp_claim_pay_ctgry_billed_amt,
	LKP_Claim_Payment_Category_Fact.claim_pay_ctgry_start_date_id AS lkp_claim_pay_ctgry_start_date_id,
	LKP_Claim_Payment_Category_Fact.claim_pay_ctgry_end_date_id AS lkp_claim_pay_ctgry_end_date_id,
	LKP_Claim_Payment_Category_Fact.claim_loss_date_id AS lkp_claim_loss_date_id,
	LKP_Claim_Payment_Category_Fact.claim_discovery_date_id AS lkp_claim_discovery_date_id,
	LKP_Claim_Payment_Category_Fact.claim_scripted_date_id AS lkp_claim_scripted_date_id,
	LKP_Claim_Payment_Category_Fact.source_claim_rpted_date_id AS lkp_source_claim_rpted_date_id,
	LKP_Claim_Payment_Category_Fact.claim_rpted_date_id AS lkp_claim_rpted_date_id,
	LKP_Claim_Payment_Category_Fact.claim_open_date_id AS lkp_claim_open_date_id,
	LKP_Claim_Payment_Category_Fact.claim_close_date_id AS lkp_claim_close_date_id,
	LKP_Claim_Payment_Category_Fact.claim_reopen_date_id AS lkp_claim_reopen_date_id,
	LKP_Claim_Payment_Category_Fact.claim_closed_after_reopen_date_id AS lkp_claim_closed_after_reopen_date_id,
	LKP_Claim_Payment_Category_Fact.claim_notice_only_date_id AS lkp_claim_notice_only_date_id,
	LKP_Claim_Payment_Category_Fact.claim_cat_start_date_id AS lkp_claim_cat_start_date_id,
	LKP_Claim_Payment_Category_Fact.claim_cat_end_date_id AS lkp_claim_cat_end_date_id,
	LKP_Claim_Payment_Category_Fact.claim_rep_assigned_date_id AS lkp_claim_rep_assigned_date_id,
	LKP_Claim_Payment_Category_Fact.claim_rep_unassigned_date_id AS lkp_claim_rep_unassigned_date_id,
	LKP_Claim_Payment_Category_Fact.pol_eff_date_id AS lkp_pol_eff_date_id,
	LKP_Claim_Payment_Category_Fact.pol_exp_date_id AS lkp_pol_exp_date_id,
	LKP_Claim_Payment_Category_Fact.pay_issued_date_id AS lkp_pay_issued_date_id,
	LKP_Claim_Payment_Category_Fact.pay_cashed_date_id AS lkp_pay_cashed_date_id,
	LKP_Claim_Payment_Category_Fact.pay_voided_date_id AS lkp_pay_voided_date_id,
	LKP_Claim_Payment_Category_Fact.pay_reposted_date_id AS lkp_pay_reposted_date_id,
	LKP_Claim_Payment_Category_Fact.invc_num AS lkp_invc_num,
	LKP_Claim_Payment_Category_Fact.cost_containment_saving_amt AS lkp_cost_containment_saving_amt,
	LKP_Claim_Payment_Category_Fact.cost_containment_red_amt AS lkp_cost_containment_red_amt,
	LKP_Claim_Payment_Category_Fact.cost_containment_ppo_amt AS lkp_cost_containment_ppo_amt,
	LKP_Claim_Payment_Category_Fact.attorney_fee_amt AS lkp_attorney_fee_amt,
	LKP_Claim_Payment_Category_Fact.attorney_cost_amt AS lkp_attorney_cost_amt,
	LKP_Claim_Payment_Category_Fact.attorney_file_num AS lkp_attorney_file_num,
	LKP_Claim_Payment_Category_Fact.hourly_rate AS lkp_hourly_rate,
	LKP_Claim_Payment_Category_Fact.hours_worked AS lkp_hours_worked,
	LKP_Claim_Payment_Category_Fact.num_of_days AS lkp_num_of_days,
	LKP_Claim_Payment_Category_Fact.num_of_weeks AS lkp_num_of_weeks,
	LKP_Claim_Payment_Category_Fact.tpd_rate AS lkp_tpd_rate,
	LKP_Claim_Payment_Category_Fact.tpd_rate_fac AS lkp_tpd_rate_fac,
	LKP_Claim_Payment_Category_Fact.tpd_wage_loss AS lkp_tpd_wage_loss,
	LKP_Claim_Payment_Category_Fact.tpd_wkly_wage AS lkp_tpd_wkly_wage,
	LKP_Claim_Payment_Category_Fact.claim_case_dim_id AS lkp_claim_case_dim_id,
	LKP_Claim_Payment_Category_Fact.contract_cust_dim_id AS lkp_contract_cust_dim_id,
	LKP_Claim_Payment_Category_Fact.strtgc_bus_dvsn_dim_id AS lkp_strtgc_bus_dvsn_dim_id,
	EXP_Date_Ids.claim_pay_ctgry_id,
	EXP_Date_Ids.claim_pay_ctgry_ak_id,
	EXP_Date_Ids.claim_pay_ak_id,
	EXP_Date_Ids.claim_pay_ctgry_type,
	EXP_Date_Ids.claim_pay_ctgry_seq_num,
	EXP_Date_Ids.claim_pay_ctgry_amt,
	EXP_Date_Ids.claim_pay_ctgry_earned_amt,
	EXP_Date_Ids.claim_pay_ctgry_billed_amt,
	EXP_Date_Ids.claim_pay_ctgry_start_date_id_out,
	EXP_Date_Ids.claim_pay_ctgry_end_date_id_out,
	EXP_Date_Ids.claim_financial_type_dim_id_out,
	EXP_Date_Ids.invc_num,
	EXP_Date_Ids.cost_containment_saving_amt,
	EXP_Date_Ids.cost_containment_red_amt,
	EXP_Date_Ids.cost_containment_ppo_amt,
	EXP_Date_Ids.attorney_fee_amt,
	EXP_Date_Ids.attorney_cost_amt,
	EXP_Date_Ids.attorney_file_num,
	EXP_Date_Ids.hourly_rate,
	EXP_Date_Ids.hours_worked,
	EXP_Date_Ids.num_of_days,
	EXP_Date_Ids.num_of_weeks,
	EXP_Date_Ids.tpd_rate,
	EXP_Date_Ids.tpd_rate_fac,
	EXP_Date_Ids.tpd_wage_loss,
	EXP_Date_Ids.tpd_wkly_wage,
	EXP_Date_Ids.claim_occurrence_dim_id_out,
	EXP_Date_Ids.claimant_dim_id_out,
	EXP_Date_Ids.claimant_cov_dim_id_out,
	EXP_Date_Ids.pol_key_dim_id_out,
	EXP_Date_Ids.agency_dim_id_out,
	EXP_Date_Ids.claim_rep_dim_prim_claim_rep_id_out,
	EXP_Date_Ids.claim_pay_dim_id_out,
	EXP_Date_Ids.claim_pay_ctgry_type_dim_id_out,
	EXP_Date_Ids.audit_id,
	EXP_Date_Ids.cov_dim_id_out,
	EXP_Date_Ids.claim_rep_dim_examiner_id_out,
	EXP_Date_Ids.claim_rep_dim_prim_litigation_handler_id_out,
	EXP_Date_Ids.claim_created_by_dim_id_out,
	EXP_Date_Ids.default_dim_id,
	EXP_Date_Ids.claim_case_dim_id_out,
	EXP_Date_Ids.claim_loss_date_id,
	EXP_Date_Ids.claim_discovery_date_id,
	EXP_Date_Ids.claim_scripted_date_id,
	EXP_Date_Ids.source_claim_rpted_date_id AS source_claim_scripted_date_id,
	EXP_Date_Ids.claim_occurrence_rpted_date_id AS claim_rpted_date_id,
	EXP_Date_Ids.claim_open_date_id,
	EXP_Date_Ids.claim_close_date_id,
	EXP_Date_Ids.claim_reopen_date_id,
	EXP_Date_Ids.claim_closed_after_reopen_date_id,
	EXP_Date_Ids.claim_notice_only_date_id,
	EXP_Date_Ids.claim_cat_start_date_id,
	EXP_Date_Ids.claim_cat_end_date_id,
	EXP_Date_Ids.claim_rep_assigned_date_id,
	EXP_Date_Ids.claim_rep_unassigned_date_id,
	EXP_Date_Ids.pol_eff_date_id,
	EXP_Date_Ids.pol_exp_date_id,
	EXP_Date_Ids.pay_issued_date_id,
	EXP_Date_Ids.pay_cashed_date_id,
	EXP_Date_Ids.pay_voided_date_id,
	EXP_Date_Ids.pay_reposted_date_id,
	EXP_Date_Ids.contract_cust_dim_id_out AS contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(lkp_claim_pay_ctgry_fact_id) ,'NEW',
	-- IIF(
	-- lkp_edw_claim_pay_ctgry_ak_id <> claim_pay_ctgry_ak_id     OR
	-- lkp_claim_occurrence_dim_id <> claim_occurrence_dim_id_out   OR
	-- lkp_claimant_dim_id <> claimant_dim_id_out   OR
	-- lkp_claimant_cov_dim_id <> claimant_cov_dim_id_out    OR
	-- lkp_cov_dim_id <>cov_dim_id_out   OR
	-- lkp_claim_rep_dim_prim_claim_rep_id <> claim_rep_dim_prim_claim_rep_id_out   OR
	-- lkp_claim_rep_dim_examiner_id <> claim_rep_dim_examiner_id_out   OR
	-- lkp_claim_rep_dim_prim_litigation_handler_id <> claim_rep_dim_prim_litigation_handler_id_out   OR
	-- lkp_claim_rep_dim_trans_entry_oper_id <> default_dim_id   OR
	-- lkp_claim_rep_dim_claim_created_by_id <> claim_created_by_dim_id_out OR
	-- lkp_pol_key_dim_id <> pol_key_dim_id_out   OR
	-- lkp_agency_dim_id <> agency_dim_id_out   OR
	-- lkp_claim_pay_dim_id <> claim_pay_dim_id_out   OR
	-- lkp_claim_financial_type_dim_id <> claim_financial_type_dim_id_out   OR
	-- lkp_claim_pay_ctgry_type_dim_id <> claim_pay_ctgry_type_dim_id_out   OR
	-- ROUND(ABS(lkp_claim_pay_ctgry_amt - claim_pay_ctgry_amt)) >0.01   OR
	-- ROUND(ABS(lkp_claim_pay_ctgry_earned_amt - claim_pay_ctgry_earned_amt)) > 0.01   OR
	-- ROUND(ABS(lkp_claim_pay_ctgry_billed_amt - claim_pay_ctgry_billed_amt))>0.01   OR
	-- lkp_claim_pay_ctgry_start_date_id <> claim_pay_ctgry_start_date_id_out   OR
	-- lkp_claim_pay_ctgry_end_date_id <> claim_pay_ctgry_end_date_id_out   OR
	-- lkp_claim_loss_date_id <> claim_loss_date_id   OR
	-- lkp_claim_discovery_date_id <> claim_discovery_date_id   OR
	-- lkp_claim_scripted_date_id <> claim_scripted_date_id   OR
	-- lkp_source_claim_rpted_date_id <> source_claim_scripted_date_id   OR
	-- lkp_claim_rpted_date_id <> claim_rpted_date_id   OR
	-- lkp_claim_open_date_id <> claim_open_date_id   OR
	-- lkp_claim_close_date_id <> claim_close_date_id   OR
	-- lkp_claim_reopen_date_id <> claim_reopen_date_id   OR
	-- lkp_claim_closed_after_reopen_date_id <> claim_closed_after_reopen_date_id   OR
	-- lkp_claim_notice_only_date_id <> claim_notice_only_date_id   OR
	-- lkp_claim_cat_start_date_id <> claim_cat_start_date_id   OR
	-- lkp_claim_cat_end_date_id <> claim_cat_end_date_id   OR
	-- lkp_claim_rep_assigned_date_id <> claim_rep_assigned_date_id   OR
	-- lkp_claim_rep_unassigned_date_id <> claim_rep_unassigned_date_id   OR
	-- lkp_pol_eff_date_id <> pol_eff_date_id   OR
	-- lkp_pol_exp_date_id <> pol_exp_date_id   OR
	-- lkp_pay_issued_date_id <> pay_issued_date_id   OR
	-- lkp_pay_cashed_date_id <> pay_cashed_date_id   OR
	-- lkp_pay_voided_date_id <> pay_voided_date_id   OR
	-- lkp_pay_reposted_date_id <> pay_reposted_date_id   OR
	-- lkp_claim_case_dim_id <> claim_case_dim_id_out OR
	-- LTRIM(RTRIM(lkp_invc_num)) <> LTRIM(RTRIM(invc_num))   OR
	-- ROUND(ABS(lkp_cost_containment_saving_amt - cost_containment_saving_amt))>0.01   OR
	-- ROUND(ABS(lkp_cost_containment_red_amt - cost_containment_red_amt))>0.01   OR
	-- ROUND(ABS(lkp_cost_containment_ppo_amt - cost_containment_ppo_amt))>0.01  OR
	-- ROUND(ABS(lkp_attorney_fee_amt - attorney_fee_amt))>0.01   OR
	-- ROUND(ABS(lkp_attorney_cost_amt - attorney_cost_amt))>0.01  OR
	-- LTRIM(RTRIM(lkp_attorney_file_num)) <> LTRIM(RTRIM(attorney_file_num))   OR
	-- ROUND(ABS(lkp_hourly_rate -  hourly_rate))>0.01   OR
	-- ROUND(ABS(lkp_hours_worked - hours_worked))>0.01   OR
	-- lkp_num_of_days <> num_of_days   OR
	-- lkp_num_of_weeks <> num_of_weeks   OR
	-- ROUND(ABS(lkp_tpd_rate - tpd_rate))>0.01   OR
	-- ROUND(ABS(lkp_tpd_rate_fac - tpd_rate_fac))>0.01   OR
	-- ROUND(ABS(lkp_tpd_wage_loss - tpd_wage_loss))>0.01   OR
	-- ROUND(ABS(lkp_tpd_wkly_wage - tpd_wkly_wage))>0.01   OR
	-- ---LTRIM(RTRIM(lkp_claim_pay_ctgry_comment)) <> LTRIM(RTRIM(claim_pay_ctgry_comment)) OR
	-- lkp_contract_cust_dim_id <> contract_cust_dim_id  OR
	-- lkp_strtgc_bus_dvsn_dim_id <> strtgc_bus_dvsn_dim_id, 
	-- 'UPDATE','NOCHANGE'))
	-- 
	IFF(
	    lkp_claim_pay_ctgry_fact_id IS NULL, 'NEW',
	    IFF(
	        lkp_edw_claim_pay_ctgry_ak_id <> claim_pay_ctgry_ak_id
	        or lkp_claim_occurrence_dim_id <> claim_occurrence_dim_id_out
	        or lkp_claimant_dim_id <> claimant_dim_id_out
	        or lkp_claimant_cov_dim_id <> claimant_cov_dim_id_out
	        or lkp_cov_dim_id <> cov_dim_id_out
	        or lkp_claim_rep_dim_prim_claim_rep_id <> claim_rep_dim_prim_claim_rep_id_out
	        or lkp_claim_rep_dim_examiner_id <> claim_rep_dim_examiner_id_out
	        or lkp_claim_rep_dim_prim_litigation_handler_id <> claim_rep_dim_prim_litigation_handler_id_out
	        or lkp_claim_rep_dim_trans_entry_oper_id <> default_dim_id
	        or lkp_claim_rep_dim_claim_created_by_id <> claim_created_by_dim_id_out
	        or lkp_pol_key_dim_id <> pol_key_dim_id_out
	        or lkp_agency_dim_id <> agency_dim_id_out
	        or lkp_claim_pay_dim_id <> claim_pay_dim_id_out
	        or lkp_claim_financial_type_dim_id <> claim_financial_type_dim_id_out
	        or lkp_claim_pay_ctgry_type_dim_id <> claim_pay_ctgry_type_dim_id_out
	        or ROUND(ABS(lkp_claim_pay_ctgry_amt - claim_pay_ctgry_amt)) > 0.01
	        or ROUND(ABS(lkp_claim_pay_ctgry_earned_amt - claim_pay_ctgry_earned_amt)) > 0.01
	        or ROUND(ABS(lkp_claim_pay_ctgry_billed_amt - claim_pay_ctgry_billed_amt)) > 0.01
	        or lkp_claim_pay_ctgry_start_date_id <> claim_pay_ctgry_start_date_id_out
	        or lkp_claim_pay_ctgry_end_date_id <> claim_pay_ctgry_end_date_id_out
	        or lkp_claim_loss_date_id <> claim_loss_date_id
	        or lkp_claim_discovery_date_id <> claim_discovery_date_id
	        or lkp_claim_scripted_date_id <> claim_scripted_date_id
	        or lkp_source_claim_rpted_date_id <> source_claim_scripted_date_id
	        or lkp_claim_rpted_date_id <> claim_rpted_date_id
	        or lkp_claim_open_date_id <> claim_open_date_id
	        or lkp_claim_close_date_id <> claim_close_date_id
	        or lkp_claim_reopen_date_id <> claim_reopen_date_id
	        or lkp_claim_closed_after_reopen_date_id <> claim_closed_after_reopen_date_id
	        or lkp_claim_notice_only_date_id <> claim_notice_only_date_id
	        or lkp_claim_cat_start_date_id <> claim_cat_start_date_id
	        or lkp_claim_cat_end_date_id <> claim_cat_end_date_id
	        or lkp_claim_rep_assigned_date_id <> claim_rep_assigned_date_id
	        or lkp_claim_rep_unassigned_date_id <> claim_rep_unassigned_date_id
	        or lkp_pol_eff_date_id <> pol_eff_date_id
	        or lkp_pol_exp_date_id <> pol_exp_date_id
	        or lkp_pay_issued_date_id <> pay_issued_date_id
	        or lkp_pay_cashed_date_id <> pay_cashed_date_id
	        or lkp_pay_voided_date_id <> pay_voided_date_id
	        or lkp_pay_reposted_date_id <> pay_reposted_date_id
	        or lkp_claim_case_dim_id <> claim_case_dim_id_out
	        or LTRIM(RTRIM(lkp_invc_num)) <> LTRIM(RTRIM(invc_num))
	        or ROUND(ABS(lkp_cost_containment_saving_amt - cost_containment_saving_amt)) > 0.01
	        or ROUND(ABS(lkp_cost_containment_red_amt - cost_containment_red_amt)) > 0.01
	        or ROUND(ABS(lkp_cost_containment_ppo_amt - cost_containment_ppo_amt)) > 0.01
	        or ROUND(ABS(lkp_attorney_fee_amt - attorney_fee_amt)) > 0.01
	        or ROUND(ABS(lkp_attorney_cost_amt - attorney_cost_amt)) > 0.01
	        or LTRIM(RTRIM(lkp_attorney_file_num)) <> LTRIM(RTRIM(attorney_file_num))
	        or ROUND(ABS(lkp_hourly_rate - hourly_rate)) > 0.01
	        or ROUND(ABS(lkp_hours_worked - hours_worked)) > 0.01
	        or lkp_num_of_days <> num_of_days
	        or lkp_num_of_weeks <> num_of_weeks
	        or ROUND(ABS(lkp_tpd_rate - tpd_rate)) > 0.01
	        or ROUND(ABS(lkp_tpd_rate_fac - tpd_rate_fac)) > 0.01
	        or ROUND(ABS(lkp_tpd_wage_loss - tpd_wage_loss)) > 0.01
	        or ROUND(ABS(lkp_tpd_wkly_wage - tpd_wkly_wage)) > 0.01
	        or lkp_contract_cust_dim_id <> contract_cust_dim_id
	        or lkp_strtgc_bus_dvsn_dim_id <> strtgc_bus_dvsn_dim_id,
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	SYSDATE AS modified_date,
	EXP_Date_Ids.strtgc_bus_dvsn_dim_id_out AS strtgc_bus_dvsn_dim_id,
	EXP_Date_Ids.FeatureRepresentativeDimId,
	EXP_Date_Ids.FeatureRepresentativeAssignedDate_id
	FROM EXP_Date_Ids
	LEFT JOIN LKP_Claim_Payment_Category_Fact
	ON LKP_Claim_Payment_Category_Fact.edw_claim_pay_ctgry_pk_id = EXP_Date_Ids.claim_pay_ctgry_id
),
RTR_claim_payment_category_fact AS (
	SELECT
	lkp_claim_pay_ctgry_fact_id AS claim_pay_ctgry_fact_id,
	claim_pay_ctgry_id,
	claim_pay_ctgry_ak_id,
	claim_pay_ak_id,
	claim_pay_ctgry_type,
	claim_pay_ctgry_seq_num,
	claim_pay_ctgry_amt,
	claim_pay_ctgry_earned_amt,
	claim_pay_ctgry_billed_amt,
	claim_pay_ctgry_start_date_id_out AS claim_pay_ctgry_start_date_id,
	claim_pay_ctgry_end_date_id_out AS claim_pay_ctgry_end_date_id,
	claim_financial_type_dim_id_out AS claim_financial_type_dim_id,
	invc_num,
	cost_containment_saving_amt,
	cost_containment_red_amt,
	cost_containment_ppo_amt,
	attorney_fee_amt,
	attorney_cost_amt,
	attorney_file_num,
	hourly_rate,
	hours_worked,
	num_of_days,
	num_of_weeks,
	tpd_rate,
	tpd_rate_fac,
	tpd_wage_loss,
	tpd_wkly_wage,
	audit_id,
	claim_occurrence_dim_id_out AS claim_occurrence_dim_id,
	claimant_dim_id_out AS claimant_dim_id,
	claimant_cov_dim_id_out AS claimant_cov_dim_id,
	pol_key_dim_id_out AS pol_key_dim_id,
	agency_dim_id_out AS agency_dim_id,
	claim_pay_dim_id_out AS claim_pay_dim_id,
	claim_rep_dim_prim_claim_rep_id_out AS claim_rep_dim_prim_claim_rep_id,
	claim_rep_dim_examiner_id_out,
	claim_rep_dim_prim_litigation_handler_id_out,
	claim_created_by_dim_id_out,
	claim_pay_ctgry_type_dim_id_out AS claim_pay_ctgry_type_dim_id,
	cov_dim_id_out AS cov_dim_id,
	claim_case_dim_id_out,
	claim_loss_date_id,
	claim_discovery_date_id,
	claim_scripted_date_id,
	source_claim_scripted_date_id,
	claim_rpted_date_id,
	claim_open_date_id,
	claim_close_date_id,
	claim_reopen_date_id,
	claim_closed_after_reopen_date_id,
	claim_notice_only_date_id,
	claim_cat_start_date_id,
	claim_cat_end_date_id,
	claim_rep_assigned_date_id,
	claim_rep_unassigned_date_id,
	pol_eff_date_id,
	pol_exp_date_id,
	pay_issued_date_id,
	pay_cashed_date_id,
	pay_voided_date_id,
	pay_reposted_date_id,
	default_dim_id,
	changed_flag,
	modified_date,
	contract_cust_dim_id,
	strtgc_bus_dvsn_dim_id,
	FeatureRepresentativeDimId,
	FeatureRepresentativeAssignedDate_id
	FROM EXP_Detect_Changes
),
RTR_claim_payment_category_fact_INSERT AS (SELECT * FROM RTR_claim_payment_category_fact WHERE changed_flag = 'NEW'


--ISNULL(claim_pay_ctgry_fact_id)),
RTR_claim_payment_category_fact_UPDATE AS (SELECT * FROM RTR_claim_payment_category_fact WHERE changed_flag = 'UPDATE'

--NOT ISNULL(claim_pay_ctgry_fact_id)),
UPD_claim_payment_category_fact_update AS (
	SELECT
	claim_pay_ctgry_fact_id AS claim_pay_ctgry_fact_id3, 
	claim_pay_ctgry_id AS claim_pay_ctgry_id3, 
	claim_pay_ctgry_ak_id AS claim_pay_ctgry_ak_id3, 
	claim_pay_ak_id AS claim_pay_ak_id3, 
	claim_pay_ctgry_type AS claim_pay_ctgry_type3, 
	claim_pay_ctgry_seq_num AS claim_pay_ctgry_seq_num3, 
	claim_pay_ctgry_amt AS claim_pay_ctgry_amt3, 
	claim_pay_ctgry_earned_amt AS claim_pay_ctgry_earned_amt3, 
	claim_pay_ctgry_billed_amt AS claim_pay_ctgry_billed_amt3, 
	claim_pay_ctgry_start_date_id AS claim_pay_ctgry_start_date_id3, 
	claim_pay_ctgry_end_date_id AS claim_pay_ctgry_end_date_id3, 
	claim_financial_type_dim_id AS claim_financial_type_dim_id3, 
	invc_num AS invc_num3, 
	cost_containment_saving_amt AS cost_containment_saving_amt3, 
	cost_containment_red_amt AS cost_containment_red_amt3, 
	cost_containment_ppo_amt AS cost_containment_ppo_amt3, 
	attorney_fee_amt AS attorney_fee_amt3, 
	attorney_cost_amt AS attorney_cost_amt3, 
	attorney_file_num AS attorney_file_num3, 
	hourly_rate AS hourly_rate3, 
	hours_worked AS hours_worked3, 
	num_of_days AS num_of_days3, 
	num_of_weeks AS num_of_weeks3, 
	tpd_rate AS tpd_rate3, 
	tpd_rate_fac AS tpd_rate_fac3, 
	tpd_wage_loss AS tpd_wage_loss3, 
	tpd_wkly_wage AS tpd_wkly_wage3, 
	audit_id AS audit_id3, 
	claim_pay_dim_id AS claim_pay_dim_id3, 
	claim_rep_dim_prim_claim_rep_id AS claim_rep_dim_prim_claim_rep_id3, 
	claim_rep_dim_examiner_id_out AS claim_rep_dim_examiner_id_out1, 
	claim_rep_dim_prim_litigation_handler_id_out AS claim_rep_dim_prim_litigation_handler_id_out1, 
	claim_pay_ctgry_type_dim_id AS claim_pay_ctgry_type_dim_id3, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id3, 
	claimant_dim_id AS claimant_dim_id3, 
	claimant_cov_dim_id AS claimant_cov_dim_id3, 
	pol_key_dim_id AS pol_key_dim_id3, 
	agency_dim_id AS agency_dim_id3, 
	cov_dim_id AS cov_dim_id3, 
	claim_case_dim_id_out AS claim_case_dim_id_out3, 
	claim_loss_date_id, 
	claim_discovery_date_id, 
	claim_scripted_date_id, 
	source_claim_scripted_date_id, 
	claim_rpted_date_id, 
	claim_open_date_id, 
	claim_close_date_id, 
	claim_reopen_date_id, 
	claim_closed_after_reopen_date_id, 
	claim_notice_only_date_id, 
	claim_cat_start_date_id, 
	claim_cat_end_date_id, 
	claim_rep_assigned_date_id, 
	claim_rep_unassigned_date_id, 
	pol_eff_date_id, 
	pol_exp_date_id, 
	pay_issued_date_id, 
	pay_cashed_date_id, 
	pay_voided_date_id, 
	pay_reposted_date_id, 
	modified_date AS modified_date3, 
	claim_created_by_dim_id_out AS claim_created_by_dim_id_out3, 
	contract_cust_dim_id AS contract_cust_dim_id3, 
	strtgc_bus_dvsn_dim_id AS strtgc_bus_dvsn_dim_id3, 
	FeatureRepresentativeDimId AS FeatureRepresentativeDimId3, 
	FeatureRepresentativeAssignedDate_id AS FeatureRepresentativeAssignedDate_id3
	FROM RTR_claim_payment_category_fact_UPDATE
),
claim_payment_category_fact_update AS (
	MERGE INTO claim_payment_category_fact AS T
	USING UPD_claim_payment_category_fact_update AS S
	ON T.claim_pay_ctgry_fact_id = S.claim_pay_ctgry_fact_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_occurrence_dim_id = S.claim_occurrence_dim_id3, T.claimant_dim_id = S.claimant_dim_id3, T.claimant_cov_dim_id = S.claimant_cov_dim_id3, T.cov_dim_id = S.cov_dim_id3, T.claim_rep_dim_prim_claim_rep_id = S.claim_rep_dim_prim_claim_rep_id3, T.claim_rep_dim_examiner_id = S.claim_rep_dim_examiner_id_out1, T.claim_rep_dim_prim_litigation_handler_id = S.claim_rep_dim_prim_litigation_handler_id_out1, T.claim_rep_dim_claim_created_by_id = S.claim_created_by_dim_id_out3, T.pol_dim_id = S.pol_key_dim_id3, T.agency_dim_id = S.agency_dim_id3, T.claim_pay_dim_id = S.claim_pay_dim_id3, T.claim_financial_type_dim_id = S.claim_financial_type_dim_id3, T.claim_pay_ctgry_type_dim_id = S.claim_pay_ctgry_type_dim_id3, T.claim_pay_ctgry_amt = S.claim_pay_ctgry_amt3, T.claim_pay_ctgry_earned_amt = S.claim_pay_ctgry_earned_amt3, T.claim_pay_ctgry_billed_amt = S.claim_pay_ctgry_billed_amt3, T.claim_pay_ctgry_start_date_id = S.claim_pay_ctgry_start_date_id3, T.claim_pay_ctgry_end_date_id = S.claim_pay_ctgry_end_date_id3, T.claim_loss_date_id = S.claim_loss_date_id, T.claim_discovery_date_id = S.claim_discovery_date_id, T.claim_scripted_date_id = S.claim_scripted_date_id, T.source_claim_rpted_date_id = S.source_claim_scripted_date_id, T.claim_rpted_date_id = S.claim_rpted_date_id, T.claim_open_date_id = S.claim_open_date_id, T.claim_close_date_id = S.claim_close_date_id, T.claim_reopen_date_id = S.claim_reopen_date_id, T.claim_closed_after_reopen_date_id = S.claim_closed_after_reopen_date_id, T.claim_notice_only_date_id = S.claim_notice_only_date_id, T.claim_cat_start_date_id = S.claim_cat_start_date_id, T.claim_cat_end_date_id = S.claim_cat_end_date_id, T.claim_rep_assigned_date_id = S.claim_rep_assigned_date_id, T.claim_rep_unassigned_date_id = S.claim_rep_unassigned_date_id, T.pol_eff_date_id = S.pol_eff_date_id, T.pol_exp_date_id = S.pol_exp_date_id, T.pay_issued_date_id = S.pay_issued_date_id, T.pay_cashed_date_id = S.pay_cashed_date_id, T.pay_voided_date_id = S.pay_voided_date_id, T.pay_reposted_date_id = S.pay_reposted_date_id, T.invc_num = S.invc_num3, T.cost_containment_saving_amt = S.cost_containment_saving_amt3, T.cost_containment_red_amt = S.cost_containment_red_amt3, T.cost_containment_ppo_amt = S.cost_containment_ppo_amt3, T.attorney_fee_amt = S.attorney_fee_amt3, T.attorney_cost_amt = S.attorney_cost_amt3, T.attorney_file_num = S.attorney_file_num3, T.hourly_rate = S.hourly_rate3, T.hours_worked = S.hours_worked3, T.num_of_days = S.num_of_days3, T.num_of_weeks = S.num_of_weeks3, T.tpd_rate = S.tpd_rate3, T.tpd_rate_fac = S.tpd_rate_fac3, T.tpd_wage_loss = S.tpd_wage_loss3, T.tpd_wkly_wage = S.tpd_wkly_wage3, T.claim_case_dim_id = S.claim_case_dim_id_out3, T.contract_cust_dim_id = S.contract_cust_dim_id3, T.strtgc_bus_dvsn_dim_id = S.strtgc_bus_dvsn_dim_id3, T.ClaimRepresentativeDimFeatureClaimRepresentativeId = S.FeatureRepresentativeDimId3, T.FeatureRepresentativeAssignedDateId = S.FeatureRepresentativeAssignedDate_id3
),
UPD_claim_payment_category_insert AS (
	SELECT
	claim_pay_ctgry_fact_id AS claim_pay_ctgry_fact_id1, 
	claim_pay_ctgry_id AS claim_pay_ctgry_id1, 
	claim_pay_ctgry_ak_id AS claim_pay_ctgry_ak_id1, 
	claim_pay_ak_id AS claim_pay_ak_id1, 
	claim_pay_ctgry_type AS claim_pay_ctgry_type1, 
	claim_pay_ctgry_seq_num AS claim_pay_ctgry_seq_num1, 
	claim_pay_ctgry_amt AS claim_pay_ctgry_amt1, 
	claim_pay_ctgry_earned_amt AS claim_pay_ctgry_earned_amt1, 
	claim_pay_ctgry_billed_amt AS claim_pay_ctgry_billed_amt1, 
	claim_pay_ctgry_start_date_id AS claim_pay_ctgry_start_date_id1, 
	claim_pay_ctgry_end_date_id AS claim_pay_ctgry_end_date_id1, 
	claim_financial_type_dim_id AS claim_financial_type_dim_id1, 
	invc_num AS invc_num1, 
	cost_containment_saving_amt AS cost_containment_saving_amt1, 
	cost_containment_red_amt AS cost_containment_red_amt1, 
	cost_containment_ppo_amt AS cost_containment_ppo_amt1, 
	attorney_fee_amt AS attorney_fee_amt1, 
	attorney_cost_amt AS attorney_cost_amt1, 
	attorney_file_num AS attorney_file_num1, 
	hourly_rate AS hourly_rate1, 
	hours_worked AS hours_worked1, 
	num_of_days AS num_of_days1, 
	num_of_weeks AS num_of_weeks1, 
	tpd_rate AS tpd_rate1, 
	tpd_rate_fac AS tpd_rate_fac1, 
	tpd_wage_loss AS tpd_wage_loss1, 
	tpd_wkly_wage AS tpd_wkly_wage1, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	claimant_dim_id AS claimant_dim_id1, 
	claimant_cov_dim_id AS claimant_cov_dim_id1, 
	pol_key_dim_id AS pol_key_dim_id1, 
	agency_dim_id AS agency_dim_id1, 
	claim_pay_dim_id AS claim_pay_dim_id1, 
	claim_rep_dim_prim_claim_rep_id, 
	claim_rep_dim_examiner_id_out AS claim_rep_dim_examiner_id_out1, 
	claim_rep_dim_prim_litigation_handler_id_out AS claim_rep_dim_prim_litigation_handler_id_out1, 
	claim_pay_ctgry_type_dim_id AS claim_pay_ctgry_type_dim_id1, 
	audit_id AS audit_id1, 
	cov_dim_id AS cov_dim_id1, 
	claim_case_dim_id_out AS claim_case_dim_id_out1, 
	claim_loss_date_id, 
	claim_discovery_date_id, 
	claim_scripted_date_id, 
	source_claim_scripted_date_id, 
	claim_rpted_date_id, 
	claim_open_date_id, 
	claim_close_date_id, 
	claim_reopen_date_id, 
	claim_closed_after_reopen_date_id, 
	claim_notice_only_date_id, 
	claim_cat_start_date_id, 
	claim_cat_end_date_id, 
	claim_rep_assigned_date_id, 
	claim_rep_unassigned_date_id, 
	pol_eff_date_id, 
	pol_exp_date_id, 
	pay_issued_date_id, 
	pay_cashed_date_id, 
	pay_voided_date_id, 
	pay_reposted_date_id, 
	default_dim_id AS default_dim_id1, 
	claim_created_by_dim_id_out AS claim_created_by_dim_id_out1, 
	contract_cust_dim_id AS contract_cust_dim_id1, 
	strtgc_bus_dvsn_dim_id AS strtgc_bus_dvsn_dim_id1, 
	FeatureRepresentativeDimId AS FeatureRepresentativeDimId1, 
	FeatureRepresentativeAssignedDate_id AS FeatureRepresentativeAssignedDate_id1
	FROM RTR_claim_payment_category_fact_INSERT
),
claim_payment_category_fact_insert AS (
	INSERT INTO claim_payment_category_fact
	(edw_claim_pay_ctgry_pk_id, edw_claim_pay_ctgry_ak_id, claim_occurrence_dim_id, claimant_dim_id, claimant_cov_dim_id, cov_dim_id, claim_rep_dim_prim_claim_rep_id, claim_rep_dim_examiner_id, claim_rep_dim_prim_litigation_handler_id, claim_rep_dim_trans_entry_oper_id, claim_rep_dim_claim_created_by_id, pol_dim_id, agency_dim_id, claim_pay_dim_id, claim_financial_type_dim_id, claim_pay_ctgry_type_dim_id, claim_pay_ctgry_amt, claim_pay_ctgry_earned_amt, claim_pay_ctgry_billed_amt, claim_pay_ctgry_start_date_id, claim_pay_ctgry_end_date_id, claim_loss_date_id, claim_discovery_date_id, claim_scripted_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_open_date_id, claim_close_date_id, claim_reopen_date_id, claim_closed_after_reopen_date_id, claim_notice_only_date_id, claim_cat_start_date_id, claim_cat_end_date_id, claim_rep_assigned_date_id, claim_rep_unassigned_date_id, pol_eff_date_id, pol_exp_date_id, pay_issued_date_id, pay_cashed_date_id, pay_voided_date_id, pay_reposted_date_id, invc_num, cost_containment_saving_amt, cost_containment_red_amt, cost_containment_ppo_amt, attorney_fee_amt, attorney_cost_amt, attorney_file_num, hourly_rate, hours_worked, num_of_days, num_of_weeks, tpd_rate, tpd_rate_fac, tpd_wage_loss, tpd_wkly_wage, audit_id, claim_case_dim_id, contract_cust_dim_id, strtgc_bus_dvsn_dim_id, ClaimRepresentativeDimFeatureClaimRepresentativeId, FeatureRepresentativeAssignedDateId)
	SELECT 
	claim_pay_ctgry_id1 AS EDW_CLAIM_PAY_CTGRY_PK_ID, 
	claim_pay_ctgry_ak_id1 AS EDW_CLAIM_PAY_CTGRY_AK_ID, 
	claim_occurrence_dim_id1 AS CLAIM_OCCURRENCE_DIM_ID, 
	claimant_dim_id1 AS CLAIMANT_DIM_ID, 
	claimant_cov_dim_id1 AS CLAIMANT_COV_DIM_ID, 
	cov_dim_id1 AS COV_DIM_ID, 
	CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	claim_rep_dim_examiner_id_out1 AS CLAIM_REP_DIM_EXAMINER_ID, 
	claim_rep_dim_prim_litigation_handler_id_out1 AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_ID, 
	default_dim_id1 AS CLAIM_REP_DIM_TRANS_ENTRY_OPER_ID, 
	claim_created_by_dim_id_out1 AS CLAIM_REP_DIM_CLAIM_CREATED_BY_ID, 
	pol_key_dim_id1 AS POL_DIM_ID, 
	agency_dim_id1 AS AGENCY_DIM_ID, 
	claim_pay_dim_id1 AS CLAIM_PAY_DIM_ID, 
	claim_financial_type_dim_id1 AS CLAIM_FINANCIAL_TYPE_DIM_ID, 
	claim_pay_ctgry_type_dim_id1 AS CLAIM_PAY_CTGRY_TYPE_DIM_ID, 
	claim_pay_ctgry_amt1 AS CLAIM_PAY_CTGRY_AMT, 
	claim_pay_ctgry_earned_amt1 AS CLAIM_PAY_CTGRY_EARNED_AMT, 
	claim_pay_ctgry_billed_amt1 AS CLAIM_PAY_CTGRY_BILLED_AMT, 
	claim_pay_ctgry_start_date_id1 AS CLAIM_PAY_CTGRY_START_DATE_ID, 
	claim_pay_ctgry_end_date_id1 AS CLAIM_PAY_CTGRY_END_DATE_ID, 
	CLAIM_LOSS_DATE_ID, 
	CLAIM_DISCOVERY_DATE_ID, 
	CLAIM_SCRIPTED_DATE_ID, 
	source_claim_scripted_date_id AS SOURCE_CLAIM_RPTED_DATE_ID, 
	CLAIM_RPTED_DATE_ID, 
	CLAIM_OPEN_DATE_ID, 
	CLAIM_CLOSE_DATE_ID, 
	CLAIM_REOPEN_DATE_ID, 
	CLAIM_CLOSED_AFTER_REOPEN_DATE_ID, 
	CLAIM_NOTICE_ONLY_DATE_ID, 
	CLAIM_CAT_START_DATE_ID, 
	CLAIM_CAT_END_DATE_ID, 
	CLAIM_REP_ASSIGNED_DATE_ID, 
	CLAIM_REP_UNASSIGNED_DATE_ID, 
	POL_EFF_DATE_ID, 
	POL_EXP_DATE_ID, 
	PAY_ISSUED_DATE_ID, 
	PAY_CASHED_DATE_ID, 
	PAY_VOIDED_DATE_ID, 
	PAY_REPOSTED_DATE_ID, 
	invc_num1 AS INVC_NUM, 
	cost_containment_saving_amt1 AS COST_CONTAINMENT_SAVING_AMT, 
	cost_containment_red_amt1 AS COST_CONTAINMENT_RED_AMT, 
	cost_containment_ppo_amt1 AS COST_CONTAINMENT_PPO_AMT, 
	attorney_fee_amt1 AS ATTORNEY_FEE_AMT, 
	attorney_cost_amt1 AS ATTORNEY_COST_AMT, 
	attorney_file_num1 AS ATTORNEY_FILE_NUM, 
	hourly_rate1 AS HOURLY_RATE, 
	hours_worked1 AS HOURS_WORKED, 
	num_of_days1 AS NUM_OF_DAYS, 
	num_of_weeks1 AS NUM_OF_WEEKS, 
	tpd_rate1 AS TPD_RATE, 
	tpd_rate_fac1 AS TPD_RATE_FAC, 
	tpd_wage_loss1 AS TPD_WAGE_LOSS, 
	tpd_wkly_wage1 AS TPD_WKLY_WAGE, 
	audit_id1 AS AUDIT_ID, 
	claim_case_dim_id_out1 AS CLAIM_CASE_DIM_ID, 
	contract_cust_dim_id1 AS CONTRACT_CUST_DIM_ID, 
	strtgc_bus_dvsn_dim_id1 AS STRTGC_BUS_DVSN_DIM_ID, 
	FeatureRepresentativeDimId1 AS CLAIMREPRESENTATIVEDIMFEATURECLAIMREPRESENTATIVEID, 
	FeatureRepresentativeAssignedDate_id1 AS FEATUREREPRESENTATIVEASSIGNEDDATEID
	FROM UPD_claim_payment_category_insert
),