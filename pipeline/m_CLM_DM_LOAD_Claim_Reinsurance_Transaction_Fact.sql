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
LKP_Reins_Cov_Dim_Id AS (
	SELECT
	reins_cov_dim_id,
	edw_reins_cov_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			reins_cov_dim_id,
			edw_reins_cov_ak_id,
			eff_from_date,
			eff_to_date
		FROM reinsurance_coverage_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_reins_cov_ak_id,eff_from_date,eff_to_date ORDER BY reins_cov_dim_id) = 1
),
LKP_claim_reinsurance_transaction AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_reins_financial_type_code,
	claim_reins_trans_date
	FROM (
		SELECT claim_reinsurance_transaction.claimant_cov_det_ak_id as claimant_cov_det_ak_id, claim_reinsurance_transaction.claim_reins_financial_type_code as claim_reins_financial_type_code, claim_reinsurance_transaction.claim_reins_trans_date as claim_reins_trans_date 
		 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_reinsurance_transaction
		where claim_reins_trans_code= 23
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,claim_reins_financial_type_code,claim_reins_trans_date ORDER BY claimant_cov_det_ak_id) = 1
),
SQ_claim_reinsurance_transaction AS (
	SELECT CRT.claim_reins_trans_id,
	       CRT.source_sys_id,
	       CRT.logical_flag,
	       CRT.claim_reins_trans_ak_id,
	       CRT.claimant_cov_det_ak_id,
	       CRT.reins_cov_ak_id,
	       CRT.sar_id,
	       CRT.cause_of_loss,
	       CRT.reserve_ctgry,
	       CRT.type_disability,
	       CRT.claim_reins_pms_trans_code,
	       CRT.claim_reins_trans_base_type_code,
	       CRT.claim_reins_financial_type_code,
	       CRT.trans_ctgry_code,
	       CRT.claim_reins_trans_code,
	       CRT.claim_reins_trans_amt,
	       CRT.claim_reins_trans_hist_amt,
	       CRT.claim_reins_trans_date,
	       CRT.claim_reins_acct_entered_date,
	       CRT.offset_onset_ind,
	       CRT.reprocess_date
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_reinsurance_transaction CRT
),
EXP_get_values AS (
	SELECT
	claim_reins_trans_id,
	claim_reins_trans_ak_id,
	claimant_cov_det_ak_id,
	reins_cov_ak_id,
	claim_reins_financial_type_code,
	claim_reins_trans_code,
	claim_reins_trans_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS v_trans_date,
	v_trans_date AS trans_date_out,
	claim_reins_trans_base_type_code,
	claim_reins_trans_amt,
	claim_reins_trans_hist_amt,
	reprocess_date,
	source_sys_id,
	claim_reins_acct_entered_date,
	offset_onset_ind,
	logical_flag,
	trans_ctgry_code,
	'C' AS trans_kind_code,
	sar_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	claim_reins_pms_trans_code
	FROM SQ_claim_reinsurance_transaction
),
LKP_Claim_Transaction_Type_Dim AS (
	SELECT
	claim_trans_type_dim_id,
	trans_ctgry_code,
	trans_base_type_code,
	trans_kind_code,
	trans_code,
	type_disability,
	offset_onset_ind
	FROM (
		SELECT 
			claim_trans_type_dim_id,
			trans_ctgry_code,
			trans_base_type_code,
			trans_kind_code,
			trans_code,
			type_disability,
			offset_onset_ind
		FROM claim_transaction_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_ctgry_code,trans_base_type_code,trans_kind_code,trans_code,type_disability,offset_onset_ind ORDER BY claim_trans_type_dim_id DESC) = 1
),
LKP_claim_financial_type_dim AS (
	SELECT
	claim_financial_type_dim_id,
	financial_type_code
	FROM (
		SELECT claim_financial_type_dim.claim_financial_type_dim_id as claim_financial_type_dim_id, claim_financial_type_dim.financial_type_code as financial_type_code FROM claim_financial_type_dim
		WHERE CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY financial_type_code ORDER BY claim_financial_type_dim_id) = 1
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
		IFF(claim_occurrence_ak_id IS NULL,
			- 1,
			claim_occurrence_ak_id
		) AS claim_occurrence_ak_id_out,
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
mplt_Claim_occurrence_dim_hist_id AS (WITH
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
		IFF(claim_occurrence_ak_id IS NULL,
			- 1,
			claim_occurrence_ak_id
		) AS claim_occurrence_ak_id_out,
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
mplt_Claimant_dim_hist_id AS (WITH
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
mplt_Strategic_Business_Division_Dim AS (WITH
	INPUT_Strategic_Business_Division AS (
		
	),
	EXP_inputs AS (
		SELECT
		policy_symbol,
		policy_number,
		policy_eff_date AS policy_eff_date_in,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_symbol)='N/A','N/A',substr(policy_symbol,1,1))
		IFF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_symbol
			) = 'N/A',
			'N/A',
			substr(policy_symbol, 1, 1
			)
		) AS policy_symbol_position_1,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_number)='N/A','N/A',substr(policy_number,1,1))
		IFF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_number
			) = 'N/A',
			'N/A',
			substr(policy_number, 1, 1
			)
		) AS policy_number_position_1,
		-- *INF*: IIF(isnull(policy_eff_date_in),SYSDATE,policy_eff_date_in)
		IFF(policy_eff_date_in IS NULL,
			SYSDATE,
			policy_eff_date_in
		) AS policy_eff_date
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
		IFF(strtgc_bus_dvsn_dim_id IS NULL,
			- 1,
			strtgc_bus_dvsn_dim_id
		) AS strtgc_bus_dvsn_id_out,
		-- *INF*: IIF(isnull(edw_strtgc_bus_dvsn_ak_id),-1,edw_strtgc_bus_dvsn_ak_id)
		IFF(edw_strtgc_bus_dvsn_ak_id IS NULL,
			- 1,
			edw_strtgc_bus_dvsn_ak_id
		) AS edw_strtgc_bus_dvsn_ak_id_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code),'N/A',strtgc_bus_dvsn_code)
		IFF(strtgc_bus_dvsn_code IS NULL,
			'N/A',
			strtgc_bus_dvsn_code
		) AS strtgc_bus_dvsn_code_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code_descript),'N/A',strtgc_bus_dvsn_code_descript)
		IFF(strtgc_bus_dvsn_code_descript IS NULL,
			'N/A',
			strtgc_bus_dvsn_code_descript
		) AS strtgc_bus_dvsn_code_descript_out
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
mplt_claimant_coverage_dim_hist_id AS (WITH
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
mplt_coverage_dim_hist_id AS (WITH
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
EXP_Values AS (
	SELECT
	EXP_get_values.logical_flag,
	mplt_Strategic_Business_Division_Dim.strtgc_bus_dvsn_dim_id,
	EXP_get_values.reins_cov_ak_id,
	EXP_get_values.claim_reins_trans_date,
	EXP_get_values.trans_date_out,
	mplt_Claim_occurence_dim_id.claim_occurrence_dim_id,
	mplt_Claim_occurrence_dim_hist_id.claim_occurrence_dim_id AS claim_occurrence_dim_hist_id,
	mplt_Claimant_dim_id.claimant_dim_id,
	mplt_Claimant_dim_hist_id.claimant_dim_id AS claimant_dim_hist_id,
	mplt_claimant_coverage_dim_id.claimant_cov_dim_id,
	mplt_claimant_coverage_dim_hist_id.claimant_cov_dim_id AS claimant_cov_dim_hist_id,
	mplt_coverage_dim_id.cov_dim_id,
	mplt_coverage_dim_hist_id.cov_dim_id AS cov_dim_hist_id,
	LKP_Claim_Transaction_Type_Dim.claim_trans_type_dim_id,
	LKP_claim_financial_type_dim.claim_financial_type_dim_id,
	mplt_Claim_occurence_dim_id.claim_rep_dim_prim_claim_rep_id AS claim_rep_dim_prim_claim_rep_id1,
	mplt_Claim_occurrence_dim_hist_id.claim_rep_dim_prim_claim_rep_id,
	mplt_Claim_occurence_dim_id.pol_key_dim_id,
	mplt_Claim_occurrence_dim_hist_id.pol_key_dim_id AS pol_key_dim_hist_id,
	EXP_get_values.reprocess_date,
	mplt_Claim_occurence_dim_id.claim_loss_date,
	mplt_Claim_occurence_dim_id.claim_discovery_date,
	mplt_Claim_occurence_dim_id.claim_scripted_date,
	mplt_Claim_occurence_dim_id.source_claim_rpted_date,
	mplt_Claim_occurence_dim_id.claim_occurrence_rpted_date,
	mplt_Claim_occurence_dim_id.claim_open_date,
	mplt_Claim_occurence_dim_id.claim_close_date,
	mplt_Claim_occurence_dim_id.claim_reopen_date,
	mplt_Claim_occurence_dim_id.claim_closed_after_reopen_date,
	mplt_Claim_occurence_dim_id.claim_notice_only_date,
	mplt_Claim_occurence_dim_id.claim_cat_start_date,
	mplt_Claim_occurence_dim_id.claim_cat_end_date,
	mplt_Claim_occurence_dim_id.claim_rep_assigned_date,
	mplt_Claim_occurence_dim_id.claim_rep_unassigned_date,
	EXP_get_values.claim_reins_trans_id,
	mplt_Claim_occurence_dim_id.pol_eff_date,
	mplt_Claim_occurence_dim_id.pol_exp_date,
	EXP_get_values.claimant_cov_det_ak_id,
	EXP_get_values.claim_reins_financial_type_code,
	EXP_get_values.claim_reins_trans_code,
	EXP_get_values.trans_ctgry_code,
	EXP_get_values.claim_reins_trans_amt,
	EXP_get_values.claim_reins_trans_hist_amt,
	EXP_get_values.source_sys_id,
	mplt_Claim_occurence_dim_id.agency_dim_id,
	mplt_Claim_occurrence_dim_hist_id.agency_dim_id AS agency_dim_hist_id,
	EXP_get_values.claim_reins_trans_ak_id,
	EXP_get_values.claim_reins_draft_num,
	EXP_get_values.claim_reins_acct_entered_date AS IN_claim_reins_acct_entered_date,
	EXP_get_values.offset_onset_ind,
	mplt_Claim_occurence_dim_id.claim_rep_dim_examiner_id,
	mplt_Claim_occurence_dim_id.claim_rep_dim_prim_litigation_handler_id,
	mplt_Claim_occurrence_dim_hist_id.claim_rep_dim_examiner_id AS claim_rep_dim_examiner_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_rep_dim_prim_litigation_handler_id AS claim_rep_dim_prim_litigation_handler_hist_id,
	mplt_Claim_occurence_dim_id.claim_created_by_id,
	mplt_Claim_occurence_dim_id.claim_case_dim_id,
	mplt_Claim_occurrence_dim_hist_id.claim_case_dim_id AS claim_case_dim_hist_id,
	mplt_Claim_occurence_dim_id.contract_cust_dim_id,
	mplt_Claim_occurrence_dim_hist_id.contract_cust_dim_id AS contract_cust_dim_hist_id,
	mplt_Claim_occurence_dim_id.AgencyDimID,
	mplt_Claim_occurrence_dim_hist_id.SalesDirectorAKID,
	mplt_Claim_occurrence_dim_hist_id.AgencyDimID AS AgencyDimID1,
	mplt_Claim_occurence_dim_id.SalesDirectorAKID AS SalesDirectorAKID1,
	mplt_Claim_occurrence_dim_hist_id.PolicyAkid
	FROM EXP_get_values
	 -- Manually join with mplt_Claim_occurence_dim_id
	 -- Manually join with mplt_Claim_occurrence_dim_hist_id
	 -- Manually join with mplt_Claimant_dim_hist_id
	 -- Manually join with mplt_Claimant_dim_id
	 -- Manually join with mplt_Strategic_Business_Division_Dim
	 -- Manually join with mplt_claimant_coverage_dim_hist_id
	 -- Manually join with mplt_claimant_coverage_dim_id
	 -- Manually join with mplt_coverage_dim_hist_id
	 -- Manually join with mplt_coverage_dim_id
	LEFT JOIN LKP_Claim_Transaction_Type_Dim
	ON LKP_Claim_Transaction_Type_Dim.trans_ctgry_code = EXP_get_values.trans_ctgry_code AND LKP_Claim_Transaction_Type_Dim.trans_base_type_code = EXP_get_values.claim_reins_trans_base_type_code AND LKP_Claim_Transaction_Type_Dim.trans_kind_code = EXP_get_values.trans_kind_code AND LKP_Claim_Transaction_Type_Dim.trans_code = EXP_get_values.claim_reins_trans_code AND LKP_Claim_Transaction_Type_Dim.type_disability = EXP_get_values.type_disability AND LKP_Claim_Transaction_Type_Dim.offset_onset_ind = EXP_get_values.offset_onset_ind
	LEFT JOIN LKP_claim_financial_type_dim
	ON LKP_claim_financial_type_dim.financial_type_code = EXP_get_values.claim_reins_financial_type_code
),
LKP_CoverageDetailDim AS (
	SELECT
	CoverageDetailDimId,
	claim_reins_trans_id,
	PolicyAkid
	FROM (
		SELECT CDD.CoverageDetailDimId as CoverageDetailDimId,
		CRT.claim_reins_trans_id as claim_reins_trans_id,
		PC.PolicyAkid as PolicyAkid       
		FROM    @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_reinsurance_transaction CRT
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		on CCD.claimant_cov_det_ak_id = CRT.claimant_cov_det_ak_id and CCD.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on CCD.RatingCoverageAKId=RC.RatingCoverageAKID
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on RC.RatingCoverageAKID=PT.RatingCoverageAKId
		and RC.EffectiveDate=PT.EffectiveDate
		and PT.SourceSystemID='DCT'
		Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		and PC.SourceSystemID='DCT'
		join @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		on PT.PremiumTransactionID=CDD.EDWPremiumTransactionPKId and CDD.ExpirationDate='2100-12-31 23:59:59'
		union
		SELECT CDD.CoverageDetailDimId as CoverageDetailDimId,
		CRT.claim_reins_trans_id as claim_reins_trans_id,
		PC.PolicyAkid as PolicyAkid
		FROM    @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_reinsurance_transaction CRT
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		on CCD.claimant_cov_det_ak_id = CRT.claimant_cov_det_ak_id and CCD.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on CCD.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
		Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on SC.PolicyCoverageAKID=PC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		and PC.SourceSystemID='PMS'
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
		and PT.SourceSystemID='PMS'
		join @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		on PT.PremiumTransactionID=CDD.EDWPremiumTransactionPKId and CDD.ExpirationDate='2100-12-31 23:59:59'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_reins_trans_id,PolicyAkid ORDER BY CoverageDetailDimId) = 1
),
LKP_InsuranceReferenceCoverageDim AS (
	SELECT
	InsuranceReferenceCoverageDimId,
	claimant_cov_det_ak_id,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT T.claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
		       T.InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId,
		T.EffectiveDate as EffectiveDate,
		T.ExpirationDate as ExpirationDate
		       FROM
		( SELECT CCD.claimant_cov_det_ak_id,
		       IRC.InsuranceReferenceCoverageDimId,
		RC.RatingCoverageEffectiveDate EffectiveDate,
		RC.RatingCoverageExpirationDate ExpirationDate
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD,
			   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence  CPO,
			   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO,
		       @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC,
		       @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC,
		       @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL,
		       @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
		WHERE  SIL.ins_line_code = PC.InsuranceLine
		AND CCD.claim_party_occurrence_ak_id=CPO.claim_party_occurrence_ak_id and CPO.crrnt_snpsht_flag=1 
				AND CPO.claim_occurrence_ak_id=CO.claim_occurrence_ak_id and CO.crrnt_snpsht_flag=1
		       AND CCD.RatingCoverageAKId = rc.RatingCoverageAKID
		       AND pc.PolicyCoverageAKID = rc.PolicyCoverageAKID
			   AND co.pol_key_ak_id=pc.policyakid
		       AND IRC.DctCoverageTypeCode= RC.CoverageType 
		       AND IRC.DctRiskTypeCode =  CASE WHEN RC.CoverageForm='BusinessAuto' THEN 'N/A' ELSE RC.RiskType END 
		       AND IRC.InsuranceLineCode = SIL.StandardInsuranceLineCode
		       AND IRC.DctPerilGroup=RC.PerilGroup
		       AND NOT (IRC.DctRiskTypeCode='N/A' AND IRC.DctCoverageTypeCode='N/A' AND IRC.DctPerilGroup='N/A')
		       AND CCD.crrnt_snpsht_flag = 1
		       AND PC.CurrentSnapshotFlag = 1
		       AND SIL.crrnt_snpsht_flag = 1
		UNION ALL
		SELECT T2.claimant_cov_det_ak_id,
		       IRC.InsuranceReferenceCoverageDimId,
		'1800-1-1' EffectiveDate,
		'2100-12-31 23:59:59' ExpirationDate
		FROM (SELECT claimant_cov_det_ak_id,
					 COALESCE(StandardInsuranceLineCode,'N/A') AS StandardInsuranceLineCode,
					 COALESCE(MajorPerilCode,'N/A') AS MajorPerilCode,
							 CASE WHEN (StandardInsuranceLineCode IN ('GL') AND (MajorPerilCode<>'540'
				        OR ClassCode NOT IN ( '11111','22222','22250','92100','17000','17001','17002')))
				        OR (StandardInsuranceLineCode IN( 'WC','IM','CG','CA'))
					 OR (StandardInsuranceLineCode='N/A' AND TypeBureauCode in ('CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP'))
					 THEN 'N/A' ELSE COALESCE(RiskUnit,'N/A') END AS RiskUnit,
					 CASE WHEN StandardInsuranceLineCode='CR' 
					 OR (StandardInsuranceLineCode='N/A' AND TypeBureauCode in ('CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP'))
					 THEN 'N/A' ELSE COALESCE(RiskUnitGroup,'N/A') END AS RiskUnitGroup,
		 			CASE WHEN REPLACE(ProductTypeCode ,'0','')=''  OR StandardInsuranceLineCode<>'GL' 
				        THEN 'N/A' ELSE COALESCE(REPLACE(ProductTypeCode ,'0',''),'N/A') END AS ProductTypeCode 
			 FROM (
		SELECT CCD.claimant_cov_det_ak_id,
				     PC.TypeBureauCode,
					 			 CASE WHEN REPLACE(SC.RiskUnit,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SC.RiskUnit,' ',''))<>0 THEN 'N/A' ELSE LTRIM(RTRIM(SC.RiskUnit)) END AS RiskUnit,
					 CASE WHEN REPLACE(SC.RiskUnitGroup,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SC.RiskUnitGroup,' ',''))<>0 THEN 'N/A' ELSE LTRIM(RTRIM(SC.RiskUnitGroup)) END AS RiskUnitGroup,
					 CASE WHEN REPLACE(SC.MajorPerilCode,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SC.MajorPerilCode,' ',''))<>0 THEN 'N/A' ELSE LTRIM(RTRIM(SC.MajorPerilCode)) END MajorPerilCode,
					 CASE WHEN (REPLACE(SIL.StandardInsuranceLineCode,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SIL.StandardInsuranceLineCode,' ',''))<>0 OR SIL.StandardInsuranceLineCode='N/A') 
					 AND (PC.TypeBureauCode IN ('AL','AN','AP') OR SC.MajorPerilCode IN ('930','931')) THEN 'CA' WHEN REPLACE(SIL.StandardInsuranceLineCode,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SIL.StandardInsuranceLineCode,' ',''))<>0 THEN 'N/A' ELSE LTRIM(RTRIM(SIL.StandardInsuranceLineCode)) END AS StandardInsuranceLineCode,
		            SC.ClassCode,
		 	 SUBSTRING(sc.RiskUnitSequenceNumber,2,1) as ProductTypeCode
				FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD,
					   @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC,
					   @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC,
					   @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
				WHERE  CCD.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
					   AND PC.PolicyCoverageAKID = SC.PolicyCoverageAKID
					   AND SC.SourcesystemID = 'PMS'
					   AND SIL.ins_line_code = PC.InsuranceLine
					   AND CCD.crrnt_snpsht_flag = 1
					   AND SIL.crrnt_snpsht_flag = 1)T1) T2,    
		       @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
		WHERE  IRC.PmsRiskUnitCode = T2.RiskUnit
		       AND IRC.PmsRiskUnitGroupCode = T2.RiskUnitGroup
		       AND IRC.PmsMajorPerilCode = T2.MajorPerilCode 
		       AND IRC.InsuranceLineCode = T2.StandardInsuranceLineCode
		       AND IRC.PmsProductTypeCode = T2.ProductTypeCode
		       AND IRC.DctRiskTypeCode='N/A' 
		       AND IRC.DctCoverageTypeCode='N/A'
		       AND IRC.DctPerilGroup='N/A') T
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,EffectiveDate,ExpirationDate ORDER BY InsuranceReferenceCoverageDimId) = 1
),
LKP_InsuranceReferenceDimId AS (
	SELECT
	InsuranceReferenceDimId,
	claimant_cov_det_ak_id
	FROM (
		SELECT CCD.claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
		IRD.InsuranceReferenceDimId AS InsuranceReferenceDimId
		FROM (
		select CCD.claimant_cov_det_ak_id,
		CCD.Claim_party_occurrence_ak_id,
		CCD.ProductAKId,
		CCD.InsuranceReferenceLineOfBusinessAKID,
		PC.RatingPlanAKId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on CCD.StatisticalCoverageAKID=SC.StatisticalCoverageAKId
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on SC.PolicyCoverageAKId=PC.PolicyCoverageAKID
		where SC.SourceSystemId='PMS' and CCD.crrnt_snpsht_flag =1
		
		union all
		select CCD.claimant_cov_det_ak_id,
		CCD.Claim_party_occurrence_ak_id,
		CCD.ProductAKId,
		CCD.InsuranceReferenceLineOfBusinessAKID,
		PC.RatingPlanAKId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence  CPO
		on CCD.claim_party_occurrence_ak_id=CPO.claim_party_occurrence_ak_id
		and CPO.crrnt_snpsht_flag=1
		Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
		on CPO.claim_occurrence_ak_id=CO.claim_occurrence_ak_id
		and CO.crrnt_snpsht_flag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on CCD.RatingCoverageAKID=RC.RatingCoverageAKId 
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RC.PolicyCoverageAKId=PC.PolicyCoverageAKID 
		and co.pol_key_ak_id=pc.policyakid
		and PC.CurrentSnapshotFlag=1
		where CCD.crrnt_snpsht_flag =1
		
		union all
		select CCD.claimant_cov_det_ak_id,
		CCD.Claim_party_occurrence_ak_id,
		CCD.ProductAKId,
		CCD.InsuranceReferenceLineOfBusinessAKID,
		null RatingPlanAKId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		where CCD.crrnt_snpsht_flag =1 and CCD.StatisticalCoverageAKID=-1 and CCD.RatingCoverageAKID=-1
		 ) CCD
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
		on CCD.Claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id
		and CPO.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
		on CPO.Claim_Occurrence_ak_id = CO.claim_occurrence_ak_id
		and CO.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		on CO.pol_key_ak_id = P.pol_ak_id
		and P.crrnt_snpsht_flag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering PO
		on P.PolicyOfferingAKId = PO.PolicyOfferingAKID
		and PO.CurrentSnapshotFlag =1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISe
		on P.InsuranceSegmentAKID = ISe.InsuranceSegmentAKID
		and ISe.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC
		on P.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId
		and SPC.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup EG
		on SPC.EnterPriseGroupId = EG.EnterpriseGroupId
		and EG.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity IRE
		on SPC.InsuranceReferenceLegalEntityId = IRE.InsuranceReferenceLegalEntityId
		and IRE.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product Pr
		on CCD.ProductAKId = Pr.ProductAKId
		and Pr.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness IRLOB
		on CCD.InsuranceReferenceLineOfBusinessAKID = IRLOB.InsuranceReferenceLineOfBusinessAKId
		and IRLOB.CurrentSnapshotFlag =1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
		on CCD.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
		join @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim IRD
		on isnull(Pr.ProductCode, '000') = IRD.ProductCode 
		and isnull(PO.PolicyOfferingCode, '000') = IRD.PolicyOfferingCode 
		and isnull(ISe.InsuranceSegmentCode, 'N/A') = IRD.InsuranceSegmentCode 
		and isnull(SPC.StrategicProfitCenterCode, '6') = IRD.StrategicProfitCenterCode 
		and isnull(IRLOB.InsuranceReferenceLineOfBusinessCode, '000') = IRD.InsuranceReferenceLineOfBusinessCode 
		and isnull(EG.EnterpriseGroupCode, '1')=IRD.EnterpriseGroupCode 
		and isnull(IRE.InsuranceReferenceLegalEntityCode, '1')=IRD.InsuranceReferenceLegalEntityCode
		and isnull(RPDT.RatingPlanCode, '1')=IRD.RatingPlanCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY InsuranceReferenceDimId) = 1
),
LKP_SalesDivisionDim AS (
	SELECT
	SalesDivisionDimID,
	AgencyAKID
	FROM (
		Select A.AgencyAKID AS AgencyAKID, 
		SDD.SalesDivisionDimID AS SalesDivisionDimID
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency A,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager RSM,
		@{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim SDD
		WHERE A.CurrentSnapshotFlag =1
		AND RSM.RegionalSalesManagerAKID = A.RegionalSalesManagerAKID
		AND RSM.CurrentSnapshotFlag = 1
		AND RSM.SalesDirectorAKID = SDD.EDWSalesDirectorAKID
		AND A.SalesTerritoryAKID = SDD.EDWSalesTerritoryAKID
		AND RSM.RegionalSalesManagerAKID = SDD.EDWRegionalSalesManagerAKID
		AND SDD.CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY SalesDivisionDimID) = 1
),
EXP_Financial_Values AS (
	SELECT
	EXP_Values.logical_flag,
	EXP_Values.reins_cov_ak_id,
	EXP_Values.trans_date_out,
	EXP_Values.strtgc_bus_dvsn_dim_id,
	-- *INF*: IIF(ISNULL(strtgc_bus_dvsn_dim_id), 0,strtgc_bus_dvsn_dim_id)
	IFF(strtgc_bus_dvsn_dim_id IS NULL,
		0,
		strtgc_bus_dvsn_dim_id
	) AS strtgc_bus_dvsn_dim_id_out,
	EXP_Values.claim_occurrence_dim_id,
	-- *INF*: iif(isnull(claim_occurrence_dim_id),-1,claim_occurrence_dim_id)
	IFF(claim_occurrence_dim_id IS NULL,
		- 1,
		claim_occurrence_dim_id
	) AS claim_occurrence_dim_id_out,
	EXP_Values.claim_occurrence_dim_hist_id,
	-- *INF*: iif(isnull(claim_occurrence_dim_hist_id),-1,claim_occurrence_dim_hist_id)
	IFF(claim_occurrence_dim_hist_id IS NULL,
		- 1,
		claim_occurrence_dim_hist_id
	) AS claim_occurrence_dim_hist_id_out,
	EXP_Values.claimant_dim_id,
	-- *INF*: iif(isnull(claimant_dim_id),-1,claimant_dim_id)
	IFF(claimant_dim_id IS NULL,
		- 1,
		claimant_dim_id
	) AS claimant_dim_id_out,
	EXP_Values.claimant_dim_hist_id,
	-- *INF*: iif(isnull(claimant_dim_hist_id),-1,claimant_dim_hist_id)
	IFF(claimant_dim_hist_id IS NULL,
		- 1,
		claimant_dim_hist_id
	) AS claimant_dim_hist_id_out,
	EXP_Values.claimant_cov_dim_id,
	-- *INF*: iif(isnull(claimant_cov_dim_id),-1,claimant_cov_dim_id)
	IFF(claimant_cov_dim_id IS NULL,
		- 1,
		claimant_cov_dim_id
	) AS claimant_cov_dim_id_out,
	EXP_Values.claimant_cov_dim_hist_id,
	-- *INF*: iif(isnull(claimant_cov_dim_hist_id),-1,claimant_cov_dim_hist_id)
	IFF(claimant_cov_dim_hist_id IS NULL,
		- 1,
		claimant_cov_dim_hist_id
	) AS claimant_cov_dim_hist_id_out,
	EXP_Values.cov_dim_id,
	-- *INF*: iif(isnull(cov_dim_id),-1,cov_dim_id)
	IFF(cov_dim_id IS NULL,
		- 1,
		cov_dim_id
	) AS cov_dim_id_out,
	EXP_Values.cov_dim_hist_id,
	-- *INF*: iif(isnull(cov_dim_hist_id),-1,cov_dim_hist_id)
	IFF(cov_dim_hist_id IS NULL,
		- 1,
		cov_dim_hist_id
	) AS cov_dim_hist_id_out,
	EXP_Values.claim_trans_type_dim_id,
	-- *INF*: iif(isnull(claim_trans_type_dim_id),-1,claim_trans_type_dim_id)
	IFF(claim_trans_type_dim_id IS NULL,
		- 1,
		claim_trans_type_dim_id
	) AS claim_trans_type_dim_id_out,
	EXP_Values.claim_financial_type_dim_id,
	-- *INF*: iif(isnull(claim_financial_type_dim_id),-1,claim_financial_type_dim_id)
	IFF(claim_financial_type_dim_id IS NULL,
		- 1,
		claim_financial_type_dim_id
	) AS claim_financial_type_dim_id_out,
	EXP_Values.claim_rep_dim_prim_claim_rep_id1 AS claim_rep_dim_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_id), -1, claim_rep_dim_id)
	IFF(claim_rep_dim_id IS NULL,
		- 1,
		claim_rep_dim_id
	) AS claim_rep_dim_prim_claim_rep_id,
	EXP_Values.claim_rep_dim_prim_claim_rep_id AS claim_rep_dim_hist_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_hist_id), -1, claim_rep_dim_hist_id)
	IFF(claim_rep_dim_hist_id IS NULL,
		- 1,
		claim_rep_dim_hist_id
	) AS claim_rep_dim_prim_claim_rep_hist_id,
	EXP_Values.pol_key_dim_id,
	-- *INF*: IIF(ISNULL(pol_key_dim_id), -1, pol_key_dim_id)
	IFF(pol_key_dim_id IS NULL,
		- 1,
		pol_key_dim_id
	) AS pol_key_dim_id_out,
	EXP_Values.pol_key_dim_hist_id,
	-- *INF*: IIF(ISNULL(pol_key_dim_hist_id), -1, pol_key_dim_hist_id)
	IFF(pol_key_dim_hist_id IS NULL,
		- 1,
		pol_key_dim_hist_id
	) AS pol_key_dim_hist_id_out,
	-1 AS claim_created_by_dim_id,
	-1 AS claim_trans_oper_dim_id,
	EXP_Values.claim_reins_trans_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_reins_trans_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_reins_trans_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_trans_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_trans_date_id), v_claim_trans_date_id, -1)
	IFF(v_claim_trans_date_id IS NOT NULL,
		v_claim_trans_date_id,
		- 1
	) AS claim_reins_trans_date_id,
	EXP_Values.reprocess_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(reprocess_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_reprocess_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_reprocess_date_id,
	-- *INF*: IIF(NOT ISNULL(v_reprocess_date_id), v_reprocess_date_id, -1)
	IFF(v_reprocess_date_id IS NOT NULL,
		v_reprocess_date_id,
		- 1
	) AS reprocess_date_id,
	EXP_Values.claim_loss_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_loss_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_loss_date_id), v_claim_loss_date_id, -1)
	IFF(v_claim_loss_date_id IS NOT NULL,
		v_claim_loss_date_id,
		- 1
	) AS claim_loss_date_id,
	EXP_Values.claim_discovery_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_discovery_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_discovery_date_id), v_claim_discovery_date_id, -1)
	IFF(v_claim_discovery_date_id IS NOT NULL,
		v_claim_discovery_date_id,
		- 1
	) AS claim_discovery_date_id,
	EXP_Values.claim_scripted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_scripted_date_id,
	-- *INF*: IIF(ISNULL(v_claim_scripted_date_id),-1,v_claim_scripted_date_id)
	IFF(v_claim_scripted_date_id IS NULL,
		- 1,
		v_claim_scripted_date_id
	) AS claim_scripted_date_id,
	EXP_Values.source_claim_rpted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_source_claim_rpted_date_id,
	-- *INF*: IIF(ISNULL(v_source_claim_rpted_date_id),-1,v_source_claim_rpted_date_id)
	IFF(v_source_claim_rpted_date_id IS NULL,
		- 1,
		v_source_claim_rpted_date_id
	) AS source_claim_rpted_date_id,
	EXP_Values.claim_occurrence_rpted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_occurrence_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_occurrence_rpted_date_id), v_claim_occurrence_rpted_date_id, -1)
	IFF(v_claim_occurrence_rpted_date_id IS NOT NULL,
		v_claim_occurrence_rpted_date_id,
		- 1
	) AS claim_occurrence_rpted_date_id,
	EXP_Values.claim_open_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_open_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_open_date), v_claim_open_date, -1)
	IFF(v_claim_open_date IS NOT NULL,
		v_claim_open_date,
		- 1
	) AS claim_open_date_id,
	EXP_Values.claim_close_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_close_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_close_date), v_claim_close_date, -1)
	IFF(v_claim_close_date IS NOT NULL,
		v_claim_close_date,
		- 1
	) AS claim_close_date_id,
	EXP_Values.claim_reopen_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_reopen_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_reopen_date), v_claim_reopen_date, -1)
	IFF(v_claim_reopen_date IS NOT NULL,
		v_claim_reopen_date,
		- 1
	) AS claim_reopen_date_id,
	EXP_Values.claim_closed_after_reopen_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_closed_after_reopen_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_closed_after_reopen_date), v_claim_closed_after_reopen_date, -1)
	IFF(v_claim_closed_after_reopen_date IS NOT NULL,
		v_claim_closed_after_reopen_date,
		- 1
	) AS claim_closed_after_reopen_date_id,
	EXP_Values.claim_notice_only_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_notice_only_date,
	-- *INF*: IIF(NOT ISNULL(v_claim_notice_only_date), v_claim_notice_only_date, -1)
	IFF(v_claim_notice_only_date IS NOT NULL,
		v_claim_notice_only_date,
		- 1
	) AS claim_notice_only_date_id,
	EXP_Values.claim_cat_start_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_start_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_start_date_id), v_claim_cat_start_date_id, -1)
	IFF(v_claim_cat_start_date_id IS NOT NULL,
		v_claim_cat_start_date_id,
		- 1
	) AS claim_cat_start_date_id,
	EXP_Values.claim_cat_end_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_end_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_end_date_id), v_claim_cat_end_date_id, -1)
	IFF(v_claim_cat_end_date_id IS NOT NULL,
		v_claim_cat_end_date_id,
		- 1
	) AS claim_cat_end_date_id,
	EXP_Values.claim_rep_assigned_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_assigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_assigned_date_id), v_claim_rep_assigned_date_id, -1)
	IFF(v_claim_rep_assigned_date_id IS NOT NULL,
		v_claim_rep_assigned_date_id,
		- 1
	) AS claim_rep_assigned_date_id,
	EXP_Values.claim_rep_unassigned_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_unassigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_unassigned_date_id), v_claim_rep_unassigned_date_id, -1)
	IFF(v_claim_rep_unassigned_date_id IS NOT NULL,
		v_claim_rep_unassigned_date_id,
		- 1
	) AS claim_rep_unassigned_date_id,
	EXP_Values.claim_reins_trans_id,
	EXP_Values.pol_eff_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_eff_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pol_eff_date_id), v_pol_eff_date_id, -1)
	IFF(v_pol_eff_date_id IS NOT NULL,
		v_pol_eff_date_id,
		- 1
	) AS pol_eff_date_id,
	EXP_Values.pol_exp_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_exp_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pol_exp_date_id), v_pol_exp_date_id, -1 )
	IFF(v_pol_exp_date_id IS NOT NULL,
		v_pol_exp_date_id,
		- 1
	) AS pol_exp_date_id,
	EXP_Values.claimant_cov_det_ak_id,
	EXP_Values.claim_reins_financial_type_code,
	EXP_Values.claim_reins_trans_code,
	EXP_Values.trans_ctgry_code,
	EXP_Values.claim_reins_trans_amt,
	EXP_Values.claim_reins_trans_hist_amt,
	EXP_Values.source_sys_id,
	-- *INF*: :LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D', claim_reins_trans_date)
	-- 
	-- 
	LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id AS var_LKP_claim_transaction,
	-- *INF*: IIF(claim_reins_financial_type_code = 'D', 
	-- DECODE(claim_reins_trans_code,  '20', claim_reins_trans_amt, 
	-- '21',claim_reins_trans_amt, 
	-- '22', claim_reins_trans_amt, 
	-- '23',claim_reins_trans_amt, 
	-- '24', claim_reins_trans_amt, 
	-- '28', claim_reins_trans_amt, 
	-- '29', claim_reins_trans_amt, 
	-- '41', 0, 
	-- '42', 0, 
	-- '43', 0, 
	-- '65',0, 
	-- '66', 0, 
	-- '90', 0, 
	-- '91', 0, 
	-- '92', 0, 0),0)
	IFF(claim_reins_financial_type_code = 'D',
		DECODE(claim_reins_trans_code,
		'20', claim_reins_trans_amt,
		'21', claim_reins_trans_amt,
		'22', claim_reins_trans_amt,
		'23', claim_reins_trans_amt,
		'24', claim_reins_trans_amt,
		'28', claim_reins_trans_amt,
		'29', claim_reins_trans_amt,
		'41', 0,
		'42', 0,
		'43', 0,
		'65', 0,
		'66', 0,
		'90', 0,
		'91', 0,
		'92', 0,
		0
		),
		0
	) AS var_ceded_loss_paid,
	var_ceded_loss_paid AS ceded_loss_paid,
	-- *INF*: IIF(claim_reins_financial_type_code = 'D', 
	-- DECODE(claim_reins_trans_code, '20', 0,
	-- '21', claim_reins_trans_amt * -1, 
	-- '22', (claim_reins_trans_amt  -  claim_reins_trans_hist_amt ) * -1, 
	-- '23', 0, 
	-- '24', 0, 
	-- '28', claim_reins_trans_amt * -1, 
	-- '29', 0, 
	-- '41', claim_reins_trans_hist_amt, 
	-- '42', claim_reins_trans_hist_amt, 
	-- '43', 0, 
	-- '65', claim_reins_trans_hist_amt, 
	-- '66', claim_reins_trans_hist_amt, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 0))
	IFF(claim_reins_financial_type_code = 'D',
		DECODE(claim_reins_trans_code,
		'20', 0,
		'21', claim_reins_trans_amt * - 1,
		'22', ( claim_reins_trans_amt - claim_reins_trans_hist_amt 
			) * - 1,
		'23', 0,
		'24', 0,
		'28', claim_reins_trans_amt * - 1,
		'29', 0,
		'41', claim_reins_trans_hist_amt,
		'42', claim_reins_trans_hist_amt,
		'43', 0,
		'65', claim_reins_trans_hist_amt,
		'66', claim_reins_trans_hist_amt,
		'90', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		'91', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		'92', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		0
		)
	) AS var_ceded_loss_outstanding,
	var_ceded_loss_outstanding AS ceded_loss_outstanding,
	-- *INF*: IIF(claim_reins_financial_type_code = 'D', 
	-- DECODE(claim_reins_trans_code, '20', claim_reins_trans_amt,
	-- '21', 0, 
	-- '22', claim_reins_trans_hist_amt, 
	-- '23', claim_reins_trans_amt, 
	-- '24', claim_reins_trans_amt, 
	-- '28',0, 
	-- '29', claim_reins_trans_amt, 
	-- '41', claim_reins_trans_hist_amt, 
	-- '42', claim_reins_trans_hist_amt, 
	-- '43', 0, 
	-- '65', claim_reins_trans_hist_amt, 
	-- '66', claim_reins_trans_hist_amt, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 1111))
	IFF(claim_reins_financial_type_code = 'D',
		DECODE(claim_reins_trans_code,
		'20', claim_reins_trans_amt,
		'21', 0,
		'22', claim_reins_trans_hist_amt,
		'23', claim_reins_trans_amt,
		'24', claim_reins_trans_amt,
		'28', 0,
		'29', claim_reins_trans_amt,
		'41', claim_reins_trans_hist_amt,
		'42', claim_reins_trans_hist_amt,
		'43', 0,
		'65', claim_reins_trans_hist_amt,
		'66', claim_reins_trans_hist_amt,
		'90', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		'91', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		'92', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		1111
		)
	) AS var_ceded_loss_incurred,
	var_ceded_loss_incurred AS ceded_loss_incurred,
	-- *INF*: IIF(claim_reins_financial_type_code = 'E', 
	-- DECODE(claim_reins_trans_code,  '20', claim_reins_trans_amt, 
	-- '21',claim_reins_trans_amt, 
	-- '22', claim_reins_trans_amt, 
	-- '23',claim_reins_trans_amt, 
	-- '24', claim_reins_trans_amt, 
	-- '28', claim_reins_trans_amt, 
	-- '29', claim_reins_trans_amt,
	-- '40',0, 
	-- '41', 0, 
	-- '42', 0, 
	-- '43', 0, 
	-- '65',0, 
	-- '66', 0, 
	-- '90', 0, 
	-- '91', 0, 
	-- '92', 0, 0),0)
	IFF(claim_reins_financial_type_code = 'E',
		DECODE(claim_reins_trans_code,
		'20', claim_reins_trans_amt,
		'21', claim_reins_trans_amt,
		'22', claim_reins_trans_amt,
		'23', claim_reins_trans_amt,
		'24', claim_reins_trans_amt,
		'28', claim_reins_trans_amt,
		'29', claim_reins_trans_amt,
		'40', 0,
		'41', 0,
		'42', 0,
		'43', 0,
		'65', 0,
		'66', 0,
		'90', 0,
		'91', 0,
		'92', 0,
		0
		),
		0
	) AS var_ceded_alae_paid,
	var_ceded_alae_paid AS ceded_alae_paid,
	-- *INF*: IIF(claim_reins_financial_type_code = 'E' and source_sys_id = 'EXCEED',
	-- DECODE(claim_reins_trans_code, '20', 0,
	-- '21', claim_reins_trans_amt * -1, 
	-- '22', (claim_reins_trans_amt -  claim_reins_trans_hist_amt ) * -1, 
	-- '23', 0, 
	-- '24', 0, 
	-- '28', claim_reins_trans_amt * -1, 
	-- '29', 0,
	-- '40',claim_reins_trans_hist_amt, 
	-- '41', claim_reins_trans_hist_amt, 
	-- '42', claim_reins_trans_hist_amt, 
	-- '43', 0, 
	-- '65', claim_reins_trans_hist_amt, 
	-- '66', claim_reins_trans_hist_amt, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 0),
	-- 0)
	IFF(claim_reins_financial_type_code = 'E' 
		AND source_sys_id = 'EXCEED',
		DECODE(claim_reins_trans_code,
		'20', 0,
		'21', claim_reins_trans_amt * - 1,
		'22', ( claim_reins_trans_amt - claim_reins_trans_hist_amt 
			) * - 1,
		'23', 0,
		'24', 0,
		'28', claim_reins_trans_amt * - 1,
		'29', 0,
		'40', claim_reins_trans_hist_amt,
		'41', claim_reins_trans_hist_amt,
		'42', claim_reins_trans_hist_amt,
		'43', 0,
		'65', claim_reins_trans_hist_amt,
		'66', claim_reins_trans_hist_amt,
		'90', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		'91', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		'92', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		0
		),
		0
	) AS var_ceded_alae_outstanding,
	var_ceded_alae_outstanding AS ceded_alae_outstanding,
	-- *INF*: IIF(claim_reins_financial_type_code = 'E' and source_sys_id = 'EXCEED',
	-- DECODE(claim_reins_trans_code, '20', claim_reins_trans_amt,
	-- '21', 0, 
	-- '22', claim_reins_trans_hist_amt, 
	-- '23', claim_reins_trans_amt, 
	-- '24', claim_reins_trans_amt, 
	-- '28',claim_reins_trans_amt, 
	-- '29', claim_reins_trans_amt,
	-- '40', claim_reins_trans_hist_amt, 
	-- '41', claim_reins_trans_hist_amt, 
	-- '42', claim_reins_trans_hist_amt, 
	-- '43', 0, 
	-- '65', claim_reins_trans_hist_amt, 
	-- '66', claim_reins_trans_hist_amt, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 0),
	-- 0)
	IFF(claim_reins_financial_type_code = 'E' 
		AND source_sys_id = 'EXCEED',
		DECODE(claim_reins_trans_code,
		'20', claim_reins_trans_amt,
		'21', 0,
		'22', claim_reins_trans_hist_amt,
		'23', claim_reins_trans_amt,
		'24', claim_reins_trans_amt,
		'28', claim_reins_trans_amt,
		'29', claim_reins_trans_amt,
		'40', claim_reins_trans_hist_amt,
		'41', claim_reins_trans_hist_amt,
		'42', claim_reins_trans_hist_amt,
		'43', 0,
		'65', claim_reins_trans_hist_amt,
		'66', claim_reins_trans_hist_amt,
		'90', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		'91', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		'92', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
				0,
				claim_reins_trans_hist_amt
			),
		0
		),
		0
	) AS var_ceded_alae_incurred,
	-- *INF*: var_ceded_alae_paid + var_ceded_alae_outstanding
	-- 
	-- ---var_ceded_alae_incurred
	var_ceded_alae_paid + var_ceded_alae_outstanding AS ceded_alae_incurred,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	EXP_Values.agency_dim_id,
	-- *INF*: IIF(ISNULL(agency_dim_id), -1, agency_dim_id)
	IFF(agency_dim_id IS NULL,
		- 1,
		agency_dim_id
	) AS agency_dim_id_out,
	EXP_Values.AgencyDimID1 AS agency_dim_hist_id,
	-- *INF*: IIF(ISNULL(agency_dim_hist_id), -1, agency_dim_hist_id)
	IFF(agency_dim_hist_id IS NULL,
		- 1,
		agency_dim_hist_id
	) AS agency_dim_hist_id_out,
	EXP_Values.claim_reins_trans_ak_id,
	EXP_Values.claim_reins_draft_num,
	EXP_Values.IN_claim_reins_acct_entered_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(IN_claim_reins_acct_entered_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_IN_claim_reins_acct_entered_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS V_claim_reins_acct_entered_date,
	-- *INF*: IIF(NOT ISNULL(V_claim_reins_acct_entered_date), V_claim_reins_acct_entered_date, -1)
	IFF(V_claim_reins_acct_entered_date IS NOT NULL,
		V_claim_reins_acct_entered_date,
		- 1
	) AS OUT_claim_reins_acct_entered_date_Id,
	EXP_Values.offset_onset_ind,
	'000000' AS Error_Flag,
	EXP_Values.claim_rep_dim_examiner_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_examiner_id), -1, claim_rep_dim_examiner_id)
	IFF(claim_rep_dim_examiner_id IS NULL,
		- 1,
		claim_rep_dim_examiner_id
	) AS claim_rep_dim_examiner_id_Out,
	EXP_Values.claim_rep_dim_prim_litigation_handler_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_litigation_handler_id), -1, claim_rep_dim_prim_litigation_handler_id)
	IFF(claim_rep_dim_prim_litigation_handler_id IS NULL,
		- 1,
		claim_rep_dim_prim_litigation_handler_id
	) AS claim_rep_dim_prim_litigation_handler_id_out,
	EXP_Values.claim_rep_dim_examiner_hist_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_examiner_hist_id), -1, claim_rep_dim_examiner_hist_id)
	IFF(claim_rep_dim_examiner_hist_id IS NULL,
		- 1,
		claim_rep_dim_examiner_hist_id
	) AS claim_rep_dim_examiner_hist_id_out,
	EXP_Values.claim_rep_dim_prim_litigation_handler_hist_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_litigation_handler_hist_id), -1, claim_rep_dim_prim_litigation_handler_hist_id)
	IFF(claim_rep_dim_prim_litigation_handler_hist_id IS NULL,
		- 1,
		claim_rep_dim_prim_litigation_handler_hist_id
	) AS claim_rep_dim_prim_litigation_handler_hist_id_out,
	EXP_Values.claim_created_by_id,
	-- *INF*: IIF(ISNULL(claim_created_by_id),-1,claim_created_by_id)
	IFF(claim_created_by_id IS NULL,
		- 1,
		claim_created_by_id
	) AS claim_created_by_dim_id_out,
	EXP_Values.claim_case_dim_id,
	-- *INF*: IIF(ISNULL(claim_case_dim_id),-1,claim_case_dim_id)
	IFF(claim_case_dim_id IS NULL,
		- 1,
		claim_case_dim_id
	) AS claim_case_dim_id_out,
	EXP_Values.claim_case_dim_hist_id,
	-- *INF*: IIF(ISNULL(claim_case_dim_hist_id),-1,claim_case_dim_hist_id)
	IFF(claim_case_dim_hist_id IS NULL,
		- 1,
		claim_case_dim_hist_id
	) AS claim_case_dim_hist_id_out,
	EXP_Values.contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(contract_cust_dim_id),-1,contract_cust_dim_id)
	IFF(contract_cust_dim_id IS NULL,
		- 1,
		contract_cust_dim_id
	) AS contract_cust_dim_id_out,
	EXP_Values.contract_cust_dim_hist_id,
	-- *INF*: IIF(ISNULL(contract_cust_dim_hist_id),-1,contract_cust_dim_hist_id)
	IFF(contract_cust_dim_hist_id IS NULL,
		- 1,
		contract_cust_dim_hist_id
	) AS contract_cust_dim_hist_id_out,
	-- *INF*: IIF(claim_reins_financial_type_code = 'S', 
	-- DECODE(claim_reins_trans_code, 
	-- '25', claim_reins_trans_amt * -1, 
	-- '31', claim_reins_trans_amt * -1, 
	-- '32', claim_reins_trans_amt * -1, 
	-- '33', claim_reins_trans_amt * -1, 
	-- '34', claim_reins_trans_amt * -1, 
	-- '38', claim_reins_trans_amt * -1, 
	-- '39', claim_reins_trans_amt * -1 
	-- , 0),0)
	IFF(claim_reins_financial_type_code = 'S',
		DECODE(claim_reins_trans_code,
		'25', claim_reins_trans_amt * - 1,
		'31', claim_reins_trans_amt * - 1,
		'32', claim_reins_trans_amt * - 1,
		'33', claim_reins_trans_amt * - 1,
		'34', claim_reins_trans_amt * - 1,
		'38', claim_reins_trans_amt * - 1,
		'39', claim_reins_trans_amt * - 1,
		0
		),
		0
	) AS var_ceded_salvage_paid,
	var_ceded_salvage_paid AS ceded_salvage_paid,
	-- *INF*: IIF(claim_reins_financial_type_code = 'B', 
	-- DECODE(claim_reins_trans_code, 
	-- '25', claim_reins_trans_amt * -1, 
	-- '31', claim_reins_trans_amt * -1, 
	-- '32', claim_reins_trans_amt * -1, 
	-- '33', claim_reins_trans_amt * -1, 
	-- '34', claim_reins_trans_amt * -1, 
	-- '38', claim_reins_trans_amt * -1, 
	-- '39', claim_reins_trans_amt * -1 
	-- , 0),0)
	IFF(claim_reins_financial_type_code = 'B',
		DECODE(claim_reins_trans_code,
		'25', claim_reins_trans_amt * - 1,
		'31', claim_reins_trans_amt * - 1,
		'32', claim_reins_trans_amt * - 1,
		'33', claim_reins_trans_amt * - 1,
		'34', claim_reins_trans_amt * - 1,
		'38', claim_reins_trans_amt * - 1,
		'39', claim_reins_trans_amt * - 1,
		0
		),
		0
	) AS var_ceded_subrogation_paid,
	var_ceded_subrogation_paid AS ceded_subrogation_paid,
	-- *INF*: IIF(claim_reins_financial_type_code = 'R' and trans_ctgry_code<>'EX', 
	-- DECODE(claim_reins_trans_code, 
	-- '25', claim_reins_trans_amt * -1, 
	-- '31', claim_reins_trans_amt * -1, 
	-- '32', claim_reins_trans_amt * -1, 
	-- '33', claim_reins_trans_amt * -1, 
	-- '34', claim_reins_trans_amt * -1, 
	-- '38', claim_reins_trans_amt * -1, 
	-- '39', claim_reins_trans_amt * -1 
	-- , 0),0)
	IFF(claim_reins_financial_type_code = 'R' 
		AND trans_ctgry_code <> 'EX',
		DECODE(claim_reins_trans_code,
		'25', claim_reins_trans_amt * - 1,
		'31', claim_reins_trans_amt * - 1,
		'32', claim_reins_trans_amt * - 1,
		'33', claim_reins_trans_amt * - 1,
		'34', claim_reins_trans_amt * - 1,
		'38', claim_reins_trans_amt * - 1,
		'39', claim_reins_trans_amt * - 1,
		0
		),
		0
	) AS var_ceded_other_recovery_loss_paid,
	var_ceded_other_recovery_loss_paid AS ceded_other_recovery_loss_paid,
	-- *INF*: IIF(claim_reins_financial_type_code = 'R' and trans_ctgry_code = 'EX', 
	-- DECODE(claim_reins_trans_code, 
	-- '25', claim_reins_trans_amt * -1, 
	-- '31', claim_reins_trans_amt * -1, 
	-- '32', claim_reins_trans_amt * -1, 
	-- '33', claim_reins_trans_amt * -1, 
	-- '34', claim_reins_trans_amt * -1, 
	-- '38', claim_reins_trans_amt * -1, 
	-- '39', claim_reins_trans_amt * -1 
	-- , 0),0)
	IFF(claim_reins_financial_type_code = 'R' 
		AND trans_ctgry_code = 'EX',
		DECODE(claim_reins_trans_code,
		'25', claim_reins_trans_amt * - 1,
		'31', claim_reins_trans_amt * - 1,
		'32', claim_reins_trans_amt * - 1,
		'33', claim_reins_trans_amt * - 1,
		'34', claim_reins_trans_amt * - 1,
		'38', claim_reins_trans_amt * - 1,
		'39', claim_reins_trans_amt * - 1,
		0
		),
		0
	) AS var_ceded_other_recovery_alae_paid,
	var_ceded_other_recovery_alae_paid AS ceded_other_recovery_alae_paid,
	-- *INF*: round(var_ceded_salvage_paid+var_ceded_subrogation_paid+var_ceded_other_recovery_loss_paid,2)
	round(var_ceded_salvage_paid + var_ceded_subrogation_paid + var_ceded_other_recovery_loss_paid, 2
	) AS var_total_ceded_loss_recovery_paid,
	var_total_ceded_loss_recovery_paid AS total_ceded_loss_recovery_paid,
	var_ceded_loss_paid *-1 AS net_loss_paid,
	var_ceded_loss_outstanding*-1 AS net_loss_outstanding,
	var_ceded_loss_incurred  *  -1 AS net_loss_incurred,
	var_ceded_alae_paid*-1 AS net_alae_paid,
	var_ceded_alae_outstanding*-1 AS net_alae_outstanding,
	var_ceded_alae_incurred*-1 AS net_alae_incurred,
	-1 AS default_id,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'))
	LKP_CALENDER_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_id AS DEFAULT_DATE_ID,
	'N/A' AS DEFAULT_STRING,
	0 AS DEFAULT_AMOUNT,
	EXP_Values.AgencyDimID,
	-- *INF*: iif(isnull(AgencyDimID),-1,AgencyDimID)
	IFF(AgencyDimID IS NULL,
		- 1,
		AgencyDimID
	) AS AgencyDimID_out,
	LKP_SalesDivisionDim.SalesDivisionDimID,
	-- *INF*: iif(isnull(SalesDivisionDimID),-1,SalesDivisionDimID)
	IFF(SalesDivisionDimID IS NULL,
		- 1,
		SalesDivisionDimID
	) AS SalesDivisionDimID_out,
	LKP_InsuranceReferenceDimId.InsuranceReferenceDimId,
	-- *INF*: iif(isnull(InsuranceReferenceDimId),-1,InsuranceReferenceDimId)
	IFF(InsuranceReferenceDimId IS NULL,
		- 1,
		InsuranceReferenceDimId
	) AS InsuranceReferenceDimId_out,
	LKP_InsuranceReferenceCoverageDim.InsuranceReferenceCoverageDimId,
	-- *INF*: iif(isnull(InsuranceReferenceCoverageDimId),-1,InsuranceReferenceCoverageDimId)
	IFF(InsuranceReferenceCoverageDimId IS NULL,
		- 1,
		InsuranceReferenceCoverageDimId
	) AS InsuranceReferenceCoverageDimId_out,
	LKP_CoverageDetailDim.CoverageDetailDimId,
	-- *INF*: iif(isnull(CoverageDetailDimId),-1,CoverageDetailDimId)
	IFF(CoverageDetailDimId IS NULL,
		- 1,
		CoverageDetailDimId
	) AS CoverageDetailDimId_out
	FROM EXP_Values
	LEFT JOIN LKP_CoverageDetailDim
	ON LKP_CoverageDetailDim.claim_reins_trans_id = EXP_Values.claim_reins_trans_id AND LKP_CoverageDetailDim.PolicyAkid = EXP_Values.PolicyAkid
	LEFT JOIN LKP_InsuranceReferenceCoverageDim
	ON LKP_InsuranceReferenceCoverageDim.claimant_cov_det_ak_id = EXP_Values.claimant_cov_det_ak_id AND LKP_InsuranceReferenceCoverageDim.EffectiveDate <= EXP_Values.trans_date_out AND LKP_InsuranceReferenceCoverageDim.ExpirationDate >= EXP_Values.trans_date_out
	LEFT JOIN LKP_InsuranceReferenceDimId
	ON LKP_InsuranceReferenceDimId.claimant_cov_det_ak_id = EXP_Values.claimant_cov_det_ak_id
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.AgencyAKID = mplt_Claim_occurence_dim_id.AgencyAKID
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_reins_trans_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_reins_trans_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_reins_trans_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_reprocess_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_reprocess_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(reprocess_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_loss_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_discovery_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_scripted_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(source_claim_rpted_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_open_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_close_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_reopen_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_notice_only_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_cat_start_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_cat_end_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(pol_eff_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(pol_exp_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CLAIM_REINSURANCE_TRANSACTION LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date
	ON LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claim_reins_financial_type_code = 'D'
	AND LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claim_reins_trans_date = claim_reins_trans_date

	LEFT JOIN LKP_CLAIM_REINSURANCE_TRANSACTION LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date
	ON LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claim_reins_financial_type_code = 'E'
	AND LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claim_reins_trans_date = claim_reins_trans_date

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_IN_claim_reins_acct_entered_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_IN_claim_reins_acct_entered_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(IN_claim_reins_acct_entered_date, 'MM/DD/YYYY'
	), 'MM/DD/YYYY'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS
	ON LKP_CALENDER_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_date = TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
)

),
EXP_Dim_IDs AS (
	SELECT
	claim_occurrence_dim_id_out,
	claim_occurrence_dim_hist_id_out,
	claimant_dim_id_out,
	claimant_dim_hist_id_out,
	claimant_cov_dim_id_out,
	claimant_cov_dim_hist_id_out,
	cov_dim_id_out,
	cov_dim_hist_id_out,
	claim_trans_type_dim_id_out,
	claim_financial_type_dim_id_out,
	claim_rep_dim_prim_claim_rep_id,
	claim_rep_dim_prim_claim_rep_hist_id,
	pol_key_dim_id_out,
	pol_key_dim_hist_id_out,
	claim_reins_trans_date_id,
	reprocess_date_id,
	claim_loss_date_id,
	claim_discovery_date_id,
	claim_scripted_date_id,
	source_claim_rpted_date_id,
	claim_occurrence_rpted_date_id,
	claim_open_date_id,
	claim_close_date_id,
	claim_reopen_date_id,
	claim_closed_after_reopen_date_id,
	claim_notice_only_date_id,
	claim_cat_start_date_id,
	claim_cat_end_date_id,
	claim_rep_assigned_date_id,
	claim_rep_unassigned_date_id,
	claim_reins_trans_id,
	pol_eff_date_id,
	pol_exp_date_id,
	claim_reins_trans_amt,
	claim_reins_trans_hist_amt,
	ceded_loss_paid,
	ceded_loss_outstanding,
	ceded_loss_incurred,
	ceded_alae_paid,
	ceded_alae_outstanding,
	ceded_alae_incurred,
	audit_id,
	agency_dim_id_out,
	agency_dim_hist_id_out,
	claim_reins_trans_ak_id,
	claim_reins_draft_num,
	OUT_claim_reins_acct_entered_date_Id,
	offset_onset_ind,
	claim_rep_dim_examiner_id_Out,
	claim_rep_dim_prim_litigation_handler_id_out,
	claim_rep_dim_examiner_hist_id_out,
	claim_rep_dim_prim_litigation_handler_hist_id_out,
	claim_created_by_dim_id_out,
	claim_case_dim_id_out,
	claim_case_dim_hist_id_out,
	contract_cust_dim_id_out,
	contract_cust_dim_hist_id_out,
	ceded_salvage_paid,
	ceded_subrogation_paid,
	ceded_other_recovery_loss_paid,
	ceded_other_recovery_alae_paid,
	total_ceded_loss_recovery_paid,
	net_loss_paid,
	net_loss_outstanding,
	net_loss_incurred,
	net_alae_paid,
	net_alae_outstanding,
	net_alae_incurred,
	default_id,
	DEFAULT_DATE_ID,
	DEFAULT_STRING,
	DEFAULT_AMOUNT,
	reins_cov_ak_id,
	claim_reins_trans_date,
	trans_date_out,
	-- *INF*: :LKP.LKP_REINS_COV_DIM_ID(reins_cov_ak_id,trans_date_out)
	LKP_REINS_COV_DIM_ID_reins_cov_ak_id_trans_date_out.reins_cov_dim_id AS V_reins_cov_dim_id,
	-- *INF*: IIF(ISNULL(V_reins_cov_dim_id), -1,V_reins_cov_dim_id)
	IFF(V_reins_cov_dim_id IS NULL,
		- 1,
		V_reins_cov_dim_id
	) AS reins_cov_dim_id_out,
	-- *INF*: :LKP.LKP_REINS_COV_DIM_ID(reins_cov_ak_id,claim_reins_trans_date)
	LKP_REINS_COV_DIM_ID_reins_cov_ak_id_claim_reins_trans_date.reins_cov_dim_id AS V_reins_cov_dim_hist_id,
	-- *INF*: IIF(ISNULL(V_reins_cov_dim_hist_id), -1,V_reins_cov_dim_hist_id)
	IFF(V_reins_cov_dim_hist_id IS NULL,
		- 1,
		V_reins_cov_dim_hist_id
	) AS reins_cov_dim_hist_id_out,
	Error_Flag,
	strtgc_bus_dvsn_dim_id_out AS strtgc_bus_dvsn_dim_id,
	AgencyDimID_out AS AgencyDimID,
	SalesDivisionDimID_out AS SalesDivisionDimID,
	InsuranceReferenceDimId_out AS InsuranceReferenceDimId,
	InsuranceReferenceCoverageDimId_out AS InsuranceReferenceCoverageDimId,
	CoverageDetailDimId_out AS CoverageDetailDimId,
	SYSDATE AS ModifiedDate
	FROM EXP_Financial_Values
	LEFT JOIN LKP_REINS_COV_DIM_ID LKP_REINS_COV_DIM_ID_reins_cov_ak_id_trans_date_out
	ON LKP_REINS_COV_DIM_ID_reins_cov_ak_id_trans_date_out.edw_reins_cov_ak_id = reins_cov_ak_id
	AND LKP_REINS_COV_DIM_ID_reins_cov_ak_id_trans_date_out.eff_from_date = trans_date_out

	LEFT JOIN LKP_REINS_COV_DIM_ID LKP_REINS_COV_DIM_ID_reins_cov_ak_id_claim_reins_trans_date
	ON LKP_REINS_COV_DIM_ID_reins_cov_ak_id_claim_reins_trans_date.edw_reins_cov_ak_id = reins_cov_ak_id
	AND LKP_REINS_COV_DIM_ID_reins_cov_ak_id_claim_reins_trans_date.eff_from_date = claim_reins_trans_date

),
LKP_claim_loss_transaction_fact AS (
	SELECT
	claim_loss_trans_fact_id,
	IN_claim_reins_trans_id,
	edw_claim_reins_trans_pk_id
	FROM (
		SELECT CLTF.claim_loss_trans_fact_id    AS claim_loss_trans_fact_id,
		       CLTF.edw_claim_reins_trans_pk_id AS edw_claim_reins_trans_pk_id
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact CLTF
		WHERE CLTF.edw_claim_reins_trans_pk_id <> -1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_reins_trans_pk_id ORDER BY claim_loss_trans_fact_id DESC) = 1
),
RTR_claim_reinsurance_transaction_fact AS (
	SELECT
	LKP_claim_loss_transaction_fact.claim_loss_trans_fact_id AS claim_reins_trans_fact_id,
	EXP_Dim_IDs.claim_occurrence_dim_id_out AS claim_occurrence_dim_id,
	EXP_Dim_IDs.claim_occurrence_dim_hist_id_out AS claim_occurrence_dim_hist_id,
	EXP_Dim_IDs.claimant_dim_id_out AS claimant_dim_id,
	EXP_Dim_IDs.claimant_dim_hist_id_out AS claimant_dim_hist_id,
	EXP_Dim_IDs.claimant_cov_dim_id_out AS claimant_cov_dim_id,
	EXP_Dim_IDs.claimant_cov_dim_hist_id_out AS claimant_cov_dim_hist_id,
	EXP_Dim_IDs.cov_dim_id_out AS cov_dim_id,
	EXP_Dim_IDs.cov_dim_hist_id_out AS cov_dim_hist_id,
	EXP_Dim_IDs.claim_trans_type_dim_id_out AS claim_trans_type_dim_id,
	EXP_Dim_IDs.claim_financial_type_dim_id_out AS claim_financial_type_dim_id,
	EXP_Dim_IDs.claim_rep_dim_prim_claim_rep_id,
	EXP_Dim_IDs.claim_rep_dim_prim_claim_rep_hist_id,
	EXP_Dim_IDs.pol_key_dim_id_out AS pol_key_dim_id,
	EXP_Dim_IDs.pol_key_dim_hist_id_out AS pol_key_dim_hist_id,
	EXP_Dim_IDs.claim_reins_trans_date_id,
	EXP_Dim_IDs.reprocess_date_id,
	EXP_Dim_IDs.claim_reins_trans_id,
	EXP_Dim_IDs.claim_loss_date_id,
	EXP_Dim_IDs.claim_discovery_date_id,
	EXP_Dim_IDs.claim_scripted_date_id,
	EXP_Dim_IDs.source_claim_rpted_date_id,
	EXP_Dim_IDs.claim_occurrence_rpted_date_id,
	EXP_Dim_IDs.claim_open_date_id,
	EXP_Dim_IDs.claim_close_date_id,
	EXP_Dim_IDs.claim_reopen_date_id,
	EXP_Dim_IDs.claim_closed_after_reopen_date_id,
	EXP_Dim_IDs.claim_notice_only_date_id,
	EXP_Dim_IDs.claim_cat_start_date_id,
	EXP_Dim_IDs.claim_cat_end_date_id,
	EXP_Dim_IDs.claim_rep_assigned_date_id,
	EXP_Dim_IDs.claim_rep_unassigned_date_id,
	EXP_Dim_IDs.pol_eff_date_id,
	EXP_Dim_IDs.pol_exp_date_id,
	EXP_Dim_IDs.claim_reins_trans_amt,
	EXP_Dim_IDs.claim_reins_trans_hist_amt,
	EXP_Dim_IDs.ceded_loss_paid,
	EXP_Dim_IDs.ceded_loss_outstanding,
	EXP_Dim_IDs.ceded_loss_incurred,
	EXP_Dim_IDs.ceded_alae_paid,
	EXP_Dim_IDs.ceded_alae_outstanding,
	EXP_Dim_IDs.ceded_alae_incurred,
	EXP_Dim_IDs.audit_id,
	EXP_Dim_IDs.agency_dim_id_out AS agency_dim_id,
	EXP_Dim_IDs.agency_dim_hist_id_out AS agency_dim_hist_id,
	EXP_Dim_IDs.claim_reins_trans_ak_id,
	EXP_Dim_IDs.claim_reins_draft_num,
	EXP_Dim_IDs.OUT_claim_reins_acct_entered_date_Id AS claim_reins_acct_entered_date_ID,
	EXP_Dim_IDs.offset_onset_ind,
	EXP_Dim_IDs.Error_Flag,
	EXP_Dim_IDs.claim_rep_dim_examiner_id_Out,
	EXP_Dim_IDs.claim_rep_dim_prim_litigation_handler_id_out,
	EXP_Dim_IDs.claim_rep_dim_examiner_hist_id_out,
	EXP_Dim_IDs.claim_rep_dim_prim_litigation_handler_hist_id_out,
	EXP_Dim_IDs.claim_created_by_dim_id_out,
	EXP_Dim_IDs.claim_case_dim_id_out,
	EXP_Dim_IDs.claim_case_dim_hist_id_out,
	EXP_Dim_IDs.contract_cust_dim_id_out AS contract_cust_dim_id,
	EXP_Dim_IDs.contract_cust_dim_hist_id_out AS contract_cust_dim_hist_id,
	EXP_Dim_IDs.ceded_salvage_paid,
	EXP_Dim_IDs.ceded_subrogation_paid,
	EXP_Dim_IDs.ceded_other_recovery_loss_paid,
	EXP_Dim_IDs.ceded_other_recovery_alae_paid,
	EXP_Dim_IDs.total_ceded_loss_recovery_paid,
	EXP_Dim_IDs.net_loss_paid,
	EXP_Dim_IDs.net_loss_outstanding,
	EXP_Dim_IDs.net_loss_incurred,
	EXP_Dim_IDs.net_alae_paid,
	EXP_Dim_IDs.net_alae_outstanding,
	EXP_Dim_IDs.net_alae_incurred,
	EXP_Dim_IDs.default_id,
	EXP_Dim_IDs.DEFAULT_DATE_ID,
	EXP_Dim_IDs.DEFAULT_STRING,
	EXP_Dim_IDs.DEFAULT_AMOUNT,
	EXP_Dim_IDs.reins_cov_dim_id_out,
	EXP_Dim_IDs.reins_cov_dim_hist_id_out,
	EXP_Dim_IDs.strtgc_bus_dvsn_dim_id,
	EXP_Dim_IDs.AgencyDimID,
	EXP_Dim_IDs.SalesDivisionDimID,
	EXP_Dim_IDs.InsuranceReferenceDimId,
	EXP_Dim_IDs.InsuranceReferenceCoverageDimId,
	EXP_Dim_IDs.CoverageDetailDimId,
	EXP_Dim_IDs.ModifiedDate
	FROM EXP_Dim_IDs
	LEFT JOIN LKP_claim_loss_transaction_fact
	ON LKP_claim_loss_transaction_fact.edw_claim_reins_trans_pk_id = EXP_Dim_IDs.claim_reins_trans_id
),
RTR_claim_reinsurance_transaction_fact_INSERT AS (SELECT * FROM RTR_claim_reinsurance_transaction_fact WHERE ISNULL(claim_reins_trans_fact_id)),
RTR_claim_reinsurance_transaction_fact_DEFAULT1 AS (SELECT * FROM RTR_claim_reinsurance_transaction_fact WHERE NOT ( (ISNULL(claim_reins_trans_fact_id)) )),
UPD_claim_loss_transaction_fact_update AS (
	SELECT
	claim_reins_trans_fact_id, 
	claim_occurrence_dim_hist_id AS claim_occurrence_dim_hist_id2, 
	claimant_dim_id AS claimant_dim_id2, 
	claimant_dim_hist_id AS claimant_dim_hist_id2, 
	claimant_cov_dim_id AS claimant_cov_dim_id2, 
	claimant_cov_dim_hist_id AS claimant_cov_dim_hist_id2, 
	cov_dim_id AS cov_dim_id2, 
	cov_dim_hist_id AS cov_dim_hist_id2, 
	claim_trans_type_dim_id AS claim_trans_type_dim_id2, 
	claim_financial_type_dim_id AS claim_financial_type_dim_id2, 
	claim_rep_dim_prim_claim_rep_id AS claim_rep_dim_id2, 
	claim_rep_dim_prim_claim_rep_hist_id AS claim_rep_dim_hist_id2, 
	pol_key_dim_id AS pol_key_dim_id2, 
	pol_key_dim_hist_id AS pol_key_dim_hist_id2, 
	claim_reins_trans_date_id AS claim_reins_trans_date_id2, 
	reprocess_date_id AS reprocess_date_id2, 
	claim_reins_trans_id, 
	claim_reins_trans_amt, 
	claim_reins_trans_hist_amt, 
	ceded_loss_paid, 
	ceded_loss_outstanding, 
	ceded_loss_incurred, 
	ceded_alae_paid, 
	ceded_alae_outstanding, 
	ceded_alae_incurred, 
	agency_dim_id AS agency_dim_id2, 
	agency_dim_hist_id AS agency_dim_hist_id2, 
	claim_reins_trans_ak_id, 
	claim_reins_draft_num, 
	claim_reins_acct_entered_date_ID, 
	offset_onset_ind, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	audit_id AS audit_id2, 
	Error_Flag, 
	claim_loss_date_id AS claim_loss_date_id2, 
	claim_discovery_date_id AS claim_discovery_date_id2, 
	claim_scripted_date_id, 
	source_claim_rpted_date_id, 
	claim_occurrence_rpted_date_id AS claim_occurrence_rpted_date_id2, 
	claim_open_date_id AS claim_open_date_id2, 
	claim_close_date_id AS claim_close_date_id2, 
	claim_reopen_date_id AS claim_reopen_date_id2, 
	claim_closed_after_reopen_date_id AS claim_closed_after_reopen_date_id2, 
	claim_notice_only_date_id, 
	claim_cat_start_date_id AS claim_cat_start_date_id2, 
	claim_cat_end_date_id AS claim_cat_end_date_id2, 
	claim_rep_assigned_date_id AS claim_rep_assigned_date_id2, 
	claim_rep_unassigned_date_id AS claim_rep_unassigned_date_id2, 
	pol_eff_date_id AS pol_eff_date_id2, 
	pol_exp_date_id AS pol_exp_date_id2, 
	claim_rep_dim_examiner_id_Out AS claim_rep_dim_examiner_id_Out2, 
	claim_rep_dim_prim_litigation_handler_id_out AS claim_rep_dim_prim_litigation_handler_id_out2, 
	claim_rep_dim_examiner_hist_id_out AS claim_rep_dim_examiner_hist_id_out2, 
	claim_rep_dim_prim_litigation_handler_hist_id_out AS claim_rep_dim_prim_litigation_handler_hist_id_out2, 
	claim_created_by_dim_id_out AS claim_created_by_dim_id_out2, 
	claim_case_dim_id_out AS claim_case_dim_id_out2, 
	claim_case_dim_hist_id_out AS claim_case_dim_hist_id_out2, 
	contract_cust_dim_id, 
	contract_cust_dim_hist_id, 
	ceded_salvage_paid, 
	ceded_subrogation_paid, 
	ceded_other_recovery_loss_paid, 
	ceded_other_recovery_alae_paid, 
	total_ceded_loss_recovery_paid, 
	net_loss_paid AS net_loss_paid2, 
	net_loss_outstanding AS net_loss_outstanding2, 
	net_loss_incurred AS net_loss_incurred2, 
	net_alae_paid AS net_alae_paid2, 
	net_alae_outstanding AS net_alae_outstanding2, 
	net_alae_incurred AS net_alae_incurred2, 
	default_id AS default_id2, 
	DEFAULT_DATE_ID AS DEFAULT_DATE_ID2, 
	DEFAULT_STRING AS DEFAULT_STRING2, 
	DEFAULT_AMOUNT AS DEFAULT_AMOUNT2, 
	reins_cov_dim_id_out AS reins_cov_dim_id_out2, 
	reins_cov_dim_hist_id_out AS reins_cov_dim_hist_id_out2, 
	strtgc_bus_dvsn_dim_id AS strtgc_bus_dvsn_dim_id2, 
	AgencyDimID AS AgencyDimID2, 
	SalesDivisionDimID AS SalesDivisionDimID2, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId2, 
	InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId2, 
	CoverageDetailDimId AS CoverageDetailDimId2, 
	ModifiedDate AS ModifiedDate2
	FROM RTR_claim_reinsurance_transaction_fact_DEFAULT1
),
claim_loss_transaction_fact_update AS (
	MERGE INTO claim_loss_transaction_fact AS T
	USING UPD_claim_loss_transaction_fact_update AS S
	ON T.claim_loss_trans_fact_id = S.claim_reins_trans_fact_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.err_flag = S.Error_Flag, T.edw_claim_trans_pk_id = S.default_id2, T.edw_claim_reins_trans_pk_id = S.claim_reins_trans_id, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.claim_occurrence_dim_hist_id = S.claim_occurrence_dim_hist_id2, T.claimant_dim_id = S.claimant_dim_id2, T.claimant_dim_hist_id = S.claimant_dim_hist_id2, T.claimant_cov_dim_id = S.claimant_cov_dim_id2, T.claimant_cov_dim_hist_id = S.claimant_cov_dim_hist_id2, T.cov_dim_id = S.cov_dim_id2, T.cov_dim_hist_id = S.cov_dim_hist_id2, T.claim_trans_type_dim_id = S.claim_trans_type_dim_id2, T.claim_financial_type_dim_id = S.claim_financial_type_dim_id2, T.reins_cov_dim_id = S.reins_cov_dim_id_out2, T.reins_cov_dim_hist_id = S.reins_cov_dim_hist_id_out2, T.claim_rep_dim_prim_claim_rep_id = S.claim_rep_dim_id2, T.claim_rep_dim_prim_claim_rep_hist_id = S.claim_rep_dim_hist_id2, T.claim_rep_dim_examiner_id = S.claim_rep_dim_examiner_id_Out2, T.claim_rep_dim_examiner_hist_id = S.claim_rep_dim_examiner_hist_id_out2, T.claim_rep_dim_prim_litigation_handler_id = S.claim_rep_dim_prim_litigation_handler_id_out2, T.claim_rep_dim_prim_litigation_handler_hist_id = S.claim_rep_dim_prim_litigation_handler_hist_id_out2, T.claim_rep_dim_trans_entry_oper_id = S.default_id2, T.claim_rep_dim_trans_entry_oper_hist_id = S.default_id2, T.claim_rep_dim_claim_created_by_id = S.claim_created_by_dim_id_out2, T.pol_dim_id = S.pol_key_dim_id2, T.pol_dim_hist_id = S.pol_key_dim_hist_id2, T.agency_dim_id = S.agency_dim_id2, T.agency_dim_hist_id = S.agency_dim_hist_id2, T.claim_pay_dim_id = S.default_id2, T.claim_pay_dim_hist_id = S.default_id2, T.claim_pay_ctgry_type_dim_id = S.default_id2, T.claim_pay_ctgry_type_dim_hist_id = S.default_id2, T.claim_case_dim_id = S.claim_case_dim_id_out2, T.claim_case_dim_hist_id = S.claim_case_dim_hist_id_out2, T.contract_cust_dim_id = S.contract_cust_dim_id, T.contract_cust_dim_hist_id = S.contract_cust_dim_hist_id, T.claim_master_1099_list_dim_id = S.default_id2, T.claim_subrogation_dim_id = S.default_id2, T.claim_trans_date_id = S.claim_reins_trans_date_id2, T.claim_trans_reprocess_date_id = S.reprocess_date_id2, T.claim_loss_date_id = S.claim_loss_date_id2, T.claim_discovery_date_id = S.claim_discovery_date_id2, T.claim_scripted_date_id = S.claim_scripted_date_id, T.source_claim_rpted_date_id = S.source_claim_rpted_date_id, T.claim_rpted_date_id = S.claim_occurrence_rpted_date_id2, T.claim_open_date_id = S.claim_open_date_id2, T.claim_close_date_id = S.claim_close_date_id2, T.claim_reopen_date_id = S.claim_reopen_date_id2, T.claim_closed_after_reopen_date_id = S.claim_closed_after_reopen_date_id2, T.claim_notice_only_date_id = S.claim_notice_only_date_id, T.claim_cat_start_date_id = S.claim_cat_start_date_id2, T.claim_cat_end_date_id = S.claim_cat_end_date_id2, T.claim_rep_assigned_date_id = S.claim_rep_assigned_date_id2, T.claim_rep_unassigned_date_id = S.claim_rep_unassigned_date_id2, T.pol_eff_date_id = S.pol_eff_date_id2, T.pol_exp_date_id = S.pol_exp_date_id2, T.claim_subrogation_referred_to_subrogation_date_id = S.DEFAULT_DATE_ID2, T.claim_subrogation_pay_start_date_id = S.DEFAULT_DATE_ID2, T.claim_subrogation_closure_date_id = S.DEFAULT_DATE_ID2, T.acct_entered_date_id = S.claim_reins_acct_entered_date_ID, T.trans_amt = S.claim_reins_trans_amt, T.trans_hist_amt = S.claim_reins_trans_hist_amt, T.tax_id = S.DEFAULT_STRING2, T.direct_loss_paid_excluding_recoveries = S.DEFAULT_AMOUNT2, T.direct_loss_outstanding_excluding_recoveries = S.DEFAULT_AMOUNT2, T.direct_loss_incurred_excluding_recoveries = S.DEFAULT_AMOUNT2, T.direct_alae_paid_excluding_recoveries = S.DEFAULT_AMOUNT2, T.direct_alae_outstanding_excluding_recoveries = S.DEFAULT_AMOUNT2, T.direct_alae_incurred_excluding_recoveries = S.DEFAULT_AMOUNT2, T.direct_loss_paid_including_recoveries = S.DEFAULT_AMOUNT2, T.direct_loss_outstanding_including_recoveries = S.DEFAULT_AMOUNT2, T.direct_loss_incurred_including_recoveries = S.DEFAULT_AMOUNT2, T.direct_alae_paid_including_recoveries = S.DEFAULT_AMOUNT2, T.direct_alae_outstanding_including_recoveries = S.DEFAULT_AMOUNT2, T.direct_alae_incurred_including_recoveries = S.DEFAULT_AMOUNT2, T.direct_subrogation_paid = S.DEFAULT_AMOUNT2, T.direct_subrogation_outstanding = S.DEFAULT_AMOUNT2, T.direct_subrogation_incurred = S.DEFAULT_AMOUNT2, T.direct_salvage_paid = S.DEFAULT_AMOUNT2, T.direct_salvage_outstanding = S.DEFAULT_AMOUNT2, T.direct_salvage_incurred = S.DEFAULT_AMOUNT2, T.direct_other_recovery_loss_paid = S.DEFAULT_AMOUNT2, T.direct_other_recovery_loss_outstanding = S.DEFAULT_AMOUNT2, T.direct_other_recovery_loss_incurred = S.DEFAULT_AMOUNT2, T.direct_other_recovery_alae_paid = S.DEFAULT_AMOUNT2, T.direct_other_recovery_alae_outstanding = S.DEFAULT_AMOUNT2, T.direct_other_recovery_alae_incurred = S.DEFAULT_AMOUNT2, T.total_direct_loss_recovery_paid = S.DEFAULT_AMOUNT2, T.total_direct_loss_recovery_outstanding = S.DEFAULT_AMOUNT2, T.total_direct_loss_recovery_incurred = S.DEFAULT_AMOUNT2, T.direct_other_recovery_paid = S.DEFAULT_AMOUNT2, T.direct_other_recovery_outstanding = S.DEFAULT_AMOUNT2, T.direct_other_recovery_incurred = S.DEFAULT_AMOUNT2, T.ceded_loss_paid = S.ceded_loss_paid, T.ceded_loss_outstanding = S.ceded_loss_outstanding, T.ceded_loss_incurred = S.ceded_loss_incurred, T.ceded_alae_paid = S.ceded_alae_paid, T.ceded_alae_outstanding = S.ceded_alae_outstanding, T.ceded_alae_incurred = S.ceded_alae_incurred, T.ceded_salvage_paid = S.ceded_salvage_paid, T.ceded_subrogation_paid = S.ceded_subrogation_paid, T.ceded_other_recovery_loss_paid = S.ceded_other_recovery_loss_paid, T.ceded_other_recovery_alae_paid = S.ceded_other_recovery_alae_paid, T.total_ceded_loss_recovery_paid = S.total_ceded_loss_recovery_paid, T.net_loss_paid = S.net_loss_paid2, T.net_loss_outstanding = S.net_loss_outstanding2, T.net_loss_incurred = S.net_loss_incurred2, T.net_alae_paid = S.net_alae_paid2, T.net_alae_outstanding = S.net_alae_outstanding2, T.net_alae_incurred = S.net_alae_incurred2, T.asl_dim_id = S.default_id2, T.asl_prdct_code_dim_id = S.default_id2, T.loss_master_dim_id = S.default_id2, T.strtgc_bus_dvsn_dim_id = S.strtgc_bus_dvsn_dim_id2, T.prdct_code_dim_id = S.default_id2, T.ClaimReserveDimId = S.default_id2, T.ClaimRepresentativeDimFeatureClaimRepresentativeId = S.default_id2, T.FeatureRepresentativeAssignedDateId = S.DEFAULT_DATE_ID2, T.InsuranceReferenceDimId = S.InsuranceReferenceDimId2, T.AgencyDimId = S.AgencyDimID2, T.SalesDivisionDimId = S.SalesDivisionDimID2, T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDimId2, T.CoverageDetailDimId = S.CoverageDetailDimId2, T.ModifiedDate = S.ModifiedDate2
),
UPD_claim_loss_transaction_fact_insert AS (
	SELECT
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	claim_occurrence_dim_hist_id AS claim_occurrence_dim_hist_id1, 
	claimant_dim_id AS claimant_dim_id1, 
	claimant_dim_hist_id AS claimant_dim_hist_id1, 
	claimant_cov_dim_id AS claimant_cov_dim_id1, 
	claimant_cov_dim_hist_id AS claimant_cov_dim_hist_id1, 
	cov_dim_id AS cov_dim_id1, 
	cov_dim_hist_id AS cov_dim_hist_id1, 
	claim_trans_type_dim_id AS claim_trans_type_dim_id1, 
	claim_financial_type_dim_id AS claim_financial_type_dim_id1, 
	claim_rep_dim_prim_claim_rep_id, 
	claim_rep_dim_prim_claim_rep_hist_id, 
	pol_key_dim_id AS pol_key_dim_id1, 
	pol_key_dim_hist_id AS pol_key_dim_hist_id1, 
	claim_reins_trans_date_id AS claim_reins_trans_date_id1, 
	reprocess_date_id AS reprocess_date_id1, 
	claim_reins_trans_id, 
	claim_reins_trans_amt, 
	claim_reins_trans_hist_amt, 
	ceded_loss_paid, 
	ceded_loss_outstanding, 
	ceded_loss_incurred, 
	ceded_alae_paid, 
	ceded_alae_outstanding, 
	ceded_alae_incurred, 
	audit_id AS audit_id1, 
	agency_dim_id AS agency_dim_id1, 
	agency_dim_hist_id AS agency_dim_hist_id1, 
	claim_reins_trans_ak_id, 
	claim_reins_acct_entered_date_ID, 
	Error_Flag, 
	claim_loss_date_id AS claim_loss_date_id1, 
	claim_discovery_date_id AS claim_discovery_date_id1, 
	claim_scripted_date_id, 
	source_claim_rpted_date_id, 
	claim_occurrence_rpted_date_id AS claim_occurrence_rpted_date_id1, 
	claim_open_date_id AS claim_open_date_id1, 
	claim_close_date_id AS claim_close_date_id1, 
	claim_reopen_date_id AS claim_reopen_date_id1, 
	claim_closed_after_reopen_date_id AS claim_closed_after_reopen_date_id1, 
	claim_notice_only_date_id, 
	claim_cat_start_date_id AS claim_cat_start_date_id1, 
	claim_cat_end_date_id AS claim_cat_end_date_id1, 
	claim_rep_assigned_date_id AS claim_rep_assigned_date_id1, 
	claim_rep_unassigned_date_id AS claim_rep_unassigned_date_id1, 
	pol_eff_date_id AS pol_eff_date_id1, 
	pol_exp_date_id AS pol_exp_date_id1, 
	claim_rep_dim_examiner_id_Out AS claim_rep_dim_examiner_id_Out1, 
	claim_rep_dim_examiner_hist_id_out AS claim_rep_dim_examiner_hist_id_out1, 
	claim_rep_dim_prim_litigation_handler_id_out AS claim_rep_dim_prim_litigation_handler_id_out1, 
	claim_rep_dim_prim_litigation_handler_hist_id_out AS claim_rep_dim_prim_litigation_handler_hist_id_out1, 
	claim_created_by_dim_id_out AS claim_created_by_dim_id_out1, 
	claim_case_dim_id_out AS claim_case_dim_id_out1, 
	claim_case_dim_hist_id_out AS claim_case_dim_hist_id_out1, 
	contract_cust_dim_id, 
	contract_cust_dim_hist_id, 
	ceded_salvage_paid, 
	ceded_subrogation_paid, 
	ceded_other_recovery_loss_paid, 
	ceded_other_recovery_alae_paid, 
	total_ceded_loss_recovery_paid, 
	net_loss_paid AS net_loss_paid1, 
	net_loss_outstanding AS net_loss_outstanding1, 
	net_loss_incurred AS net_loss_incurred1, 
	net_alae_paid AS net_alae_paid1, 
	net_alae_outstanding AS net_alae_outstanding1, 
	net_alae_incurred AS net_alae_incurred1, 
	default_id AS default_id1, 
	DEFAULT_DATE_ID AS DEFAULT_DATE_ID1, 
	DEFAULT_STRING AS DEFAULT_STRING1, 
	DEFAULT_AMOUNT AS DEFAULT_AMOUNT1, 
	reins_cov_dim_id_out AS reins_cov_dim_id_out1, 
	reins_cov_dim_hist_id_out AS reins_cov_dim_hist_id_out1, 
	strtgc_bus_dvsn_dim_id AS strtgc_bus_dvsn_dim_id1, 
	AgencyDimID AS AgencyDimID1, 
	SalesDivisionDimID AS SalesDivisionDimID1, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId1, 
	InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId1, 
	CoverageDetailDimId AS CoverageDetailDimId1, 
	ModifiedDate AS ModifiedDate1
	FROM RTR_claim_reinsurance_transaction_fact_INSERT
),
claim_loss_transaction_fact_insert AS (
	INSERT INTO claim_loss_transaction_fact
	(err_flag, audit_id, edw_claim_trans_pk_id, edw_claim_reins_trans_pk_id, claim_occurrence_dim_id, claim_occurrence_dim_hist_id, claimant_dim_id, claimant_dim_hist_id, claimant_cov_dim_id, claimant_cov_dim_hist_id, cov_dim_id, cov_dim_hist_id, claim_trans_type_dim_id, claim_financial_type_dim_id, reins_cov_dim_id, reins_cov_dim_hist_id, claim_rep_dim_prim_claim_rep_id, claim_rep_dim_prim_claim_rep_hist_id, claim_rep_dim_examiner_id, claim_rep_dim_examiner_hist_id, claim_rep_dim_prim_litigation_handler_id, claim_rep_dim_prim_litigation_handler_hist_id, claim_rep_dim_trans_entry_oper_id, claim_rep_dim_trans_entry_oper_hist_id, claim_rep_dim_claim_created_by_id, pol_dim_id, pol_dim_hist_id, agency_dim_id, agency_dim_hist_id, claim_pay_dim_id, claim_pay_dim_hist_id, claim_pay_ctgry_type_dim_id, claim_pay_ctgry_type_dim_hist_id, claim_case_dim_id, claim_case_dim_hist_id, contract_cust_dim_id, contract_cust_dim_hist_id, claim_master_1099_list_dim_id, claim_subrogation_dim_id, claim_trans_date_id, claim_trans_reprocess_date_id, claim_loss_date_id, claim_discovery_date_id, claim_scripted_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_open_date_id, claim_close_date_id, claim_reopen_date_id, claim_closed_after_reopen_date_id, claim_notice_only_date_id, claim_cat_start_date_id, claim_cat_end_date_id, claim_rep_assigned_date_id, claim_rep_unassigned_date_id, pol_eff_date_id, pol_exp_date_id, claim_subrogation_referred_to_subrogation_date_id, claim_subrogation_pay_start_date_id, claim_subrogation_closure_date_id, acct_entered_date_id, trans_amt, trans_hist_amt, tax_id, direct_loss_paid_excluding_recoveries, direct_loss_outstanding_excluding_recoveries, direct_loss_incurred_excluding_recoveries, direct_alae_paid_excluding_recoveries, direct_alae_outstanding_excluding_recoveries, direct_alae_incurred_excluding_recoveries, direct_loss_paid_including_recoveries, direct_loss_outstanding_including_recoveries, direct_loss_incurred_including_recoveries, direct_alae_paid_including_recoveries, direct_alae_outstanding_including_recoveries, direct_alae_incurred_including_recoveries, direct_subrogation_paid, direct_subrogation_outstanding, direct_subrogation_incurred, direct_salvage_paid, direct_salvage_outstanding, direct_salvage_incurred, direct_other_recovery_loss_paid, direct_other_recovery_loss_outstanding, direct_other_recovery_loss_incurred, direct_other_recovery_alae_paid, direct_other_recovery_alae_outstanding, direct_other_recovery_alae_incurred, total_direct_loss_recovery_paid, total_direct_loss_recovery_outstanding, total_direct_loss_recovery_incurred, direct_other_recovery_paid, direct_other_recovery_outstanding, direct_other_recovery_incurred, ceded_loss_paid, ceded_loss_outstanding, ceded_loss_incurred, ceded_alae_paid, ceded_alae_outstanding, ceded_alae_incurred, ceded_salvage_paid, ceded_subrogation_paid, ceded_other_recovery_loss_paid, ceded_other_recovery_alae_paid, total_ceded_loss_recovery_paid, net_loss_paid, net_loss_outstanding, net_loss_incurred, net_alae_paid, net_alae_outstanding, net_alae_incurred, asl_dim_id, asl_prdct_code_dim_id, loss_master_dim_id, strtgc_bus_dvsn_dim_id, prdct_code_dim_id, ClaimReserveDimId, ClaimRepresentativeDimFeatureClaimRepresentativeId, FeatureRepresentativeAssignedDateId, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, ModifiedDate)
	SELECT 
	Error_Flag AS ERR_FLAG, 
	audit_id1 AS AUDIT_ID, 
	default_id1 AS EDW_CLAIM_TRANS_PK_ID, 
	claim_reins_trans_id AS EDW_CLAIM_REINS_TRANS_PK_ID, 
	claim_occurrence_dim_id1 AS CLAIM_OCCURRENCE_DIM_ID, 
	claim_occurrence_dim_hist_id1 AS CLAIM_OCCURRENCE_DIM_HIST_ID, 
	claimant_dim_id1 AS CLAIMANT_DIM_ID, 
	claimant_dim_hist_id1 AS CLAIMANT_DIM_HIST_ID, 
	claimant_cov_dim_id1 AS CLAIMANT_COV_DIM_ID, 
	claimant_cov_dim_hist_id1 AS CLAIMANT_COV_DIM_HIST_ID, 
	cov_dim_id1 AS COV_DIM_ID, 
	cov_dim_hist_id1 AS COV_DIM_HIST_ID, 
	claim_trans_type_dim_id1 AS CLAIM_TRANS_TYPE_DIM_ID, 
	claim_financial_type_dim_id1 AS CLAIM_FINANCIAL_TYPE_DIM_ID, 
	reins_cov_dim_id_out1 AS REINS_COV_DIM_ID, 
	reins_cov_dim_hist_id_out1 AS REINS_COV_DIM_HIST_ID, 
	CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	CLAIM_REP_DIM_PRIM_CLAIM_REP_HIST_ID, 
	claim_rep_dim_examiner_id_Out1 AS CLAIM_REP_DIM_EXAMINER_ID, 
	claim_rep_dim_examiner_hist_id_out1 AS CLAIM_REP_DIM_EXAMINER_HIST_ID, 
	claim_rep_dim_prim_litigation_handler_id_out1 AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_ID, 
	claim_rep_dim_prim_litigation_handler_hist_id_out1 AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_HIST_ID, 
	default_id1 AS CLAIM_REP_DIM_TRANS_ENTRY_OPER_ID, 
	default_id1 AS CLAIM_REP_DIM_TRANS_ENTRY_OPER_HIST_ID, 
	claim_created_by_dim_id_out1 AS CLAIM_REP_DIM_CLAIM_CREATED_BY_ID, 
	pol_key_dim_id1 AS POL_DIM_ID, 
	pol_key_dim_hist_id1 AS POL_DIM_HIST_ID, 
	agency_dim_id1 AS AGENCY_DIM_ID, 
	agency_dim_hist_id1 AS AGENCY_DIM_HIST_ID, 
	default_id1 AS CLAIM_PAY_DIM_ID, 
	default_id1 AS CLAIM_PAY_DIM_HIST_ID, 
	default_id1 AS CLAIM_PAY_CTGRY_TYPE_DIM_ID, 
	default_id1 AS CLAIM_PAY_CTGRY_TYPE_DIM_HIST_ID, 
	claim_case_dim_id_out1 AS CLAIM_CASE_DIM_ID, 
	claim_case_dim_hist_id_out1 AS CLAIM_CASE_DIM_HIST_ID, 
	CONTRACT_CUST_DIM_ID, 
	CONTRACT_CUST_DIM_HIST_ID, 
	default_id1 AS CLAIM_MASTER_1099_LIST_DIM_ID, 
	default_id1 AS CLAIM_SUBROGATION_DIM_ID, 
	claim_reins_trans_date_id1 AS CLAIM_TRANS_DATE_ID, 
	reprocess_date_id1 AS CLAIM_TRANS_REPROCESS_DATE_ID, 
	claim_loss_date_id1 AS CLAIM_LOSS_DATE_ID, 
	claim_discovery_date_id1 AS CLAIM_DISCOVERY_DATE_ID, 
	CLAIM_SCRIPTED_DATE_ID, 
	SOURCE_CLAIM_RPTED_DATE_ID, 
	claim_occurrence_rpted_date_id1 AS CLAIM_RPTED_DATE_ID, 
	claim_open_date_id1 AS CLAIM_OPEN_DATE_ID, 
	claim_close_date_id1 AS CLAIM_CLOSE_DATE_ID, 
	claim_reopen_date_id1 AS CLAIM_REOPEN_DATE_ID, 
	claim_closed_after_reopen_date_id1 AS CLAIM_CLOSED_AFTER_REOPEN_DATE_ID, 
	CLAIM_NOTICE_ONLY_DATE_ID, 
	claim_cat_start_date_id1 AS CLAIM_CAT_START_DATE_ID, 
	claim_cat_end_date_id1 AS CLAIM_CAT_END_DATE_ID, 
	claim_rep_assigned_date_id1 AS CLAIM_REP_ASSIGNED_DATE_ID, 
	claim_rep_unassigned_date_id1 AS CLAIM_REP_UNASSIGNED_DATE_ID, 
	pol_eff_date_id1 AS POL_EFF_DATE_ID, 
	pol_exp_date_id1 AS POL_EXP_DATE_ID, 
	DEFAULT_DATE_ID1 AS CLAIM_SUBROGATION_REFERRED_TO_SUBROGATION_DATE_ID, 
	DEFAULT_DATE_ID1 AS CLAIM_SUBROGATION_PAY_START_DATE_ID, 
	DEFAULT_DATE_ID1 AS CLAIM_SUBROGATION_CLOSURE_DATE_ID, 
	claim_reins_acct_entered_date_ID AS ACCT_ENTERED_DATE_ID, 
	claim_reins_trans_amt AS TRANS_AMT, 
	claim_reins_trans_hist_amt AS TRANS_HIST_AMT, 
	DEFAULT_STRING1 AS TAX_ID, 
	DEFAULT_AMOUNT1 AS DIRECT_LOSS_PAID_EXCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_LOSS_OUTSTANDING_EXCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_LOSS_INCURRED_EXCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_ALAE_PAID_EXCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_ALAE_OUTSTANDING_EXCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_ALAE_INCURRED_EXCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_LOSS_PAID_INCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_LOSS_OUTSTANDING_INCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_LOSS_INCURRED_INCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_ALAE_PAID_INCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_ALAE_OUTSTANDING_INCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_ALAE_INCURRED_INCLUDING_RECOVERIES, 
	DEFAULT_AMOUNT1 AS DIRECT_SUBROGATION_PAID, 
	DEFAULT_AMOUNT1 AS DIRECT_SUBROGATION_OUTSTANDING, 
	DEFAULT_AMOUNT1 AS DIRECT_SUBROGATION_INCURRED, 
	DEFAULT_AMOUNT1 AS DIRECT_SALVAGE_PAID, 
	DEFAULT_AMOUNT1 AS DIRECT_SALVAGE_OUTSTANDING, 
	DEFAULT_AMOUNT1 AS DIRECT_SALVAGE_INCURRED, 
	DEFAULT_AMOUNT1 AS DIRECT_OTHER_RECOVERY_LOSS_PAID, 
	DEFAULT_AMOUNT1 AS DIRECT_OTHER_RECOVERY_LOSS_OUTSTANDING, 
	DEFAULT_AMOUNT1 AS DIRECT_OTHER_RECOVERY_LOSS_INCURRED, 
	DEFAULT_AMOUNT1 AS DIRECT_OTHER_RECOVERY_ALAE_PAID, 
	DEFAULT_AMOUNT1 AS DIRECT_OTHER_RECOVERY_ALAE_OUTSTANDING, 
	DEFAULT_AMOUNT1 AS DIRECT_OTHER_RECOVERY_ALAE_INCURRED, 
	DEFAULT_AMOUNT1 AS TOTAL_DIRECT_LOSS_RECOVERY_PAID, 
	DEFAULT_AMOUNT1 AS TOTAL_DIRECT_LOSS_RECOVERY_OUTSTANDING, 
	DEFAULT_AMOUNT1 AS TOTAL_DIRECT_LOSS_RECOVERY_INCURRED, 
	DEFAULT_AMOUNT1 AS DIRECT_OTHER_RECOVERY_PAID, 
	DEFAULT_AMOUNT1 AS DIRECT_OTHER_RECOVERY_OUTSTANDING, 
	DEFAULT_AMOUNT1 AS DIRECT_OTHER_RECOVERY_INCURRED, 
	CEDED_LOSS_PAID, 
	CEDED_LOSS_OUTSTANDING, 
	CEDED_LOSS_INCURRED, 
	CEDED_ALAE_PAID, 
	CEDED_ALAE_OUTSTANDING, 
	CEDED_ALAE_INCURRED, 
	CEDED_SALVAGE_PAID, 
	CEDED_SUBROGATION_PAID, 
	CEDED_OTHER_RECOVERY_LOSS_PAID, 
	CEDED_OTHER_RECOVERY_ALAE_PAID, 
	TOTAL_CEDED_LOSS_RECOVERY_PAID, 
	net_loss_paid1 AS NET_LOSS_PAID, 
	net_loss_outstanding1 AS NET_LOSS_OUTSTANDING, 
	net_loss_incurred1 AS NET_LOSS_INCURRED, 
	net_alae_paid1 AS NET_ALAE_PAID, 
	net_alae_outstanding1 AS NET_ALAE_OUTSTANDING, 
	net_alae_incurred1 AS NET_ALAE_INCURRED, 
	default_id1 AS ASL_DIM_ID, 
	default_id1 AS ASL_PRDCT_CODE_DIM_ID, 
	default_id1 AS LOSS_MASTER_DIM_ID, 
	strtgc_bus_dvsn_dim_id1 AS STRTGC_BUS_DVSN_DIM_ID, 
	default_id1 AS PRDCT_CODE_DIM_ID, 
	default_id1 AS CLAIMRESERVEDIMID, 
	default_id1 AS CLAIMREPRESENTATIVEDIMFEATURECLAIMREPRESENTATIVEID, 
	DEFAULT_DATE_ID1 AS FEATUREREPRESENTATIVEASSIGNEDDATEID, 
	InsuranceReferenceDimId1 AS INSURANCEREFERENCEDIMID, 
	AgencyDimID1 AS AGENCYDIMID, 
	SalesDivisionDimID1 AS SALESDIVISIONDIMID, 
	InsuranceReferenceCoverageDimId1 AS INSURANCEREFERENCECOVERAGEDIMID, 
	CoverageDetailDimId1 AS COVERAGEDETAILDIMID, 
	ModifiedDate1 AS MODIFIEDDATE
	FROM UPD_claim_loss_transaction_fact_insert
),