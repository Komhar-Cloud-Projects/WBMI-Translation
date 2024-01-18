WITH
LKP_SupClaimsCentersOfExpertiseCoverage AS (
	SELECT
	SourceColumnAbbreviation,
	SupClaimsCentersOfExpertiseCoverageId,
	AuditId,
	CreatedDate,
	ModifiedDate,
	SourceColumnName,
	SourceColumnValue,
	i_SourceColumnName,
	i_SourceColumnValue
	FROM (
		SELECT 
			SourceColumnAbbreviation,
			SupClaimsCentersOfExpertiseCoverageId,
			AuditId,
			CreatedDate,
			ModifiedDate,
			SourceColumnName,
			SourceColumnValue,
			i_SourceColumnName,
			i_SourceColumnValue
		FROM SupClaimsCentersOfExpertiseCoverage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourceColumnName,SourceColumnValue ORDER BY SourceColumnAbbreviation) = 1
),
LKP_SupTypeOfLossRules AS (
	SELECT
	ClaimTypeCategory,
	InsuranceSegmentCode,
	MajorPerilCode,
	CauseOfLoss,
	i_InsuranceSegmentCode,
	i_MajorPerilCode,
	i_CauseOfLoss
	FROM (
		SELECT SupTypeOfLossRules.ClaimTypeCategory as ClaimTypeCategory, SupTypeOfLossRules.InsuranceSegmentCode as InsuranceSegmentCode, SupTypeOfLossRules.MajorPerilCode as MajorPerilCode, SupTypeOfLossRules.CauseOfLoss as CauseOfLoss FROM  @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.SupTypeOfLossRules
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentCode,MajorPerilCode,CauseOfLoss ORDER BY ClaimTypeCategory) = 1
),
SQ_Work_Claims_Centers_of_Expertise AS (
	-- Claim, coverage or claimant changed
	SELECT vw_claim_party1.claim_party_key,
		claimant_coverage_detail.ins_line,
		claim_occurrence_dim.claim_num,
		coverage_dim.major_peril_descript,
		claimant_coverage_dim.cause_of_loss_long_descript,
		policy_dim.pol_num,
		claim_occurrence_dim.claim_loss_date,
		claim_occurrence_dim.claim_open_date,
		claim_occurrence_dim.claim_close_date,
		claim_occurrence_dim.claim_reopen_date,
		claim_occurrence_dim.claim_closed_after_reopen_date,
		claim_representative_dim.claim_rep_full_name,
		claim_representative_dim.claim_rep_num,
		claim_representative_dim.handling_office_mgr,
		claim_representative_dim.handling_office_descript,
		vw_claimant_dim1.claimant_full_name,
		claim_case_dim.suit_open_date,
		claim_occurrence_dim.loss_loc_state,
		product_code_dim.prdct_code_descript,
		strategic_business_division_dim.strtgc_bus_dvsn_code_descript,
		vw_claimant_dim1.jurisdiction_state_code,
		claim_representative_dim.claim_rep_email,
		claim_representative_dim.handling_office_mgr_email,
		claim_occurrence_dim.loss_loc_zip,
		claim_occurrence_dim.claim_cat_code,
		contract_customer_dim.name,
		coverage_dim.major_peril_code,
		claimant_coverage_dim.cause_of_loss,
		claimant_coverage_dim.reserve_ctgry,
		claim_payment_category_type_dim.claim_pay_ctgry_type,
		claimant_coverage_dim.reserve_ctgry_descript,
		claim_occurrence_dim.source_claim_occurrence_status_code,
		coverage_dim.type_bureau_code,
		coverage_dim.risk_unit_grp,
		vw_claim_loss_transaction_fact.direct_loss_outstanding_excluding_recoveries,
		vw_claim_loss_transaction_fact.direct_alae_outstanding_excluding_recoveries,
		vw_claim_loss_transaction_fact.direct_loss_paid_including_recoveries,
		vw_claim_loss_transaction_fact.direct_alae_paid_including_recoveries,
		vw_claim_loss_transaction_fact.direct_subrogation_paid,
		vw_claim_loss_transaction_fact.direct_subrogation_outstanding,
		vw_claim_loss_transaction_fact.direct_salvage_paid,
		vw_claim_loss_transaction_fact.direct_salvage_outstanding,
		claim_representative_dim.claim_rep_dim_id,
		vw_claim_loss_transaction_fact.direct_other_recovery_loss_outstanding,
		vw_claim_loss_transaction_fact.direct_other_recovery_alae_outstanding,
		vw_claim_loss_transaction_fact.claimant_dim_id,
		vw_claim_loss_transaction_fact.claim_occurrence_dim_id,
		asl_dim.asl_code,
		InsuranceReferenceDim.InsuranceSegmentCode,
		InsuranceReferenceDim.ProductDescription,
		InsuranceReferenceDim.StrategicProfitCenterAbbreviation,
		InsuranceReferenceDim.StrategicProfitCenterDescription,
		v3.AgencyDim.AgencyCode,
		asl_dim.sub_non_asl_code_descript,
		Claimant_coverage_dim.MajorPerilCodeDescription,
		Claimant_coverage_dim.MajorPerilCode,
		-- added as per requirement in PROD-11861
		vw_claim_loss_transaction_fact.ClaimRepresentativeDimFeatureClaimRepresentativeId,
		InsuranceReferenceDim.InsuranceReferenceLineOfBusinessAbbreviation
		, claim_occurrence_dim.claim_loss_descript
		, claim_occurrence_dim.source_claim_rpted_date
		, policy_dim.pol_eff_date
		, policy_dim.pol_exp_date
	FROM dbo.policy_dim WITH (NOLOCK)
	INNER JOIN dbo.vw_claim_loss_transaction_fact WITH (NOLOCK) ON (dbo.policy_dim.pol_dim_id = dbo.vw_claim_loss_transaction_fact.pol_dim_id)
	INNER JOIN dbo.coverage_dim WITH (NOLOCK) ON (dbo.coverage_dim.cov_dim_id = dbo.vw_claim_loss_transaction_fact.cov_dim_id)
	INNER JOIN dbo.claim_occurrence_dim WITH (NOLOCK) ON (claim_occurrence_dim.claim_occurrence_dim_id = dbo.vw_claim_loss_transaction_fact.claim_occurrence_dim_id)
	INNER JOIN dbo.vw_claimant_dim1 WITH (NOLOCK) ON (vw_claimant_dim1.claimant_dim_id = dbo.vw_claim_loss_transaction_fact.claimant_dim_id)
	INNER JOIN dbo.claimant_coverage_dim WITH (NOLOCK) ON (claimant_coverage_dim.claimant_cov_dim_id = dbo.vw_claim_loss_transaction_fact.claimant_cov_dim_id)
	INNER JOIN dbo.claim_representative_dim WITH (NOLOCK) ON (dbo.claim_representative_dim.claim_rep_dim_id = dbo.vw_claim_loss_transaction_fact.claim_rep_dim_prim_claim_rep_id)
	INNER JOIN dbo.claim_case_dim WITH (NOLOCK) ON (dbo.claim_case_dim.claim_case_dim_id = dbo.vw_claim_loss_transaction_fact.claim_case_dim_id)
	INNER JOIN dbo.contract_customer_dim WITH (NOLOCK) ON (dbo.contract_customer_dim.contract_cust_dim_id = dbo.vw_claim_loss_transaction_fact.contract_cust_dim_id)
	INNER JOIN dbo.strategic_business_division_dim WITH (NOLOCK) ON (dbo.strategic_business_division_dim.strtgc_bus_dvsn_dim_id = dbo.vw_claim_loss_transaction_fact.strtgc_bus_dvsn_dim_id)
	INNER JOIN dbo.product_code_dim WITH (NOLOCK) ON (dbo.product_code_dim.prdct_code_dim_id = dbo.vw_claim_loss_transaction_fact.prdct_code_dim_id)
	LEFT OUTER JOIN dbo.claim_payment_category_type_dim WITH (NOLOCK) ON (dbo.claim_payment_category_type_dim.claim_pay_ctgry_type_dim_id = dbo.vw_claim_loss_transaction_fact.claim_pay_ctgry_type_dim_id)
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claimant_coverage_detail WITH (NOLOCK) ON claimant_coverage_detail.claimant_cov_det_ak_id = coverage_dim.edw_claimant_cov_det_ak_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_party_occurrence WITH (NOLOCK) ON claim_party_occurrence.claim_party_occurrence_ak_id = claimant_coverage_detail.claim_party_occurrence_ak_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.vw_claim_party1 WITH (NOLOCK) ON vw_claim_party1.claim_party_ak_id = claim_party_occurrence.claim_party_ak_id
	INNER JOIN dbo.asl_dim WITH (NOLOCK) ON asl_dim.asl_dim_id = vw_claim_loss_transaction_fact.asl_dim_id
	INNER JOIN v3.AgencyDim WITH (NOLOCK) ON v3.AgencyDim.AgencyDimid = vw_claim_loss_transaction_fact.AgencyDimid
	INNER JOIN dbo.InsuranceReferenceDim WITH (NOLOCK) ON InsuranceReferenceDim.InsuranceReferenceDimid = vw_claim_loss_transaction_fact.InsuranceReferenceDimid
	WHERE claim_occurrence_dim.crrnt_snpsht_flag = 1
		AND vw_claimant_dim1.crrnt_snpsht_flag = 1
		AND claimant_coverage_dim.crrnt_snpsht_flag = 1
		AND claimant_coverage_detail.crrnt_snpsht_flag = 1
		AND claim_party_occurrence.crrnt_snpsht_flag = 1
		AND vw_claim_party1.crrnt_snpsht_flag = 1
		AND coverage_dim.crrnt_snpsht_flag = 1
		AND claim_representative_dim.crrnt_snpsht_flag = 1
		AND claimant_coverage_detail.source_sys_id = 'EXCEED'
		AND (
			claim_occurrence_dim.claim_open_date > '@{pipeline().parameters.SELECTION_BASE_DATE}'
			OR claim_occurrence_dim.claim_close_date > '@{pipeline().parameters.SELECTION_BASE_DATE}'
			)
		AND (
			claim_occurrence_dim.modified_date > '@{pipeline().parameters.SELECTION_START_TS}'
			OR vw_claimant_dim1.modified_date > '@{pipeline().parameters.SELECTION_START_TS}'
			OR claimant_coverage_dim.modified_date > '@{pipeline().parameters.SELECTION_START_TS}'
			OR claimant_coverage_detail.modified_date > '@{pipeline().parameters.SELECTION_START_TS}'
			OR claim_party_occurrence.modified_date > '@{pipeline().parameters.SELECTION_START_TS}'
			OR vw_claim_party1.modified_date > '@{pipeline().parameters.SELECTION_START_TS}'
			OR coverage_dim.modified_date > '@{pipeline().parameters.SELECTION_START_TS}'
			)
	
	UNION
	
	-- Nurse assignment changed
	SELECT DISTINCT vw_claim_party1.claim_party_key,
		claimant_coverage_detail.ins_line,
		claim_occurrence_dim.claim_num,
		coverage_dim.major_peril_descript,
		claimant_coverage_dim.cause_of_loss_long_descript,
		policy_dim.pol_num,
		claim_occurrence_dim.claim_loss_date,
		claim_occurrence_dim.claim_open_date,
		claim_occurrence_dim.claim_close_date,
		claim_occurrence_dim.claim_reopen_date,
		claim_occurrence_dim.claim_closed_after_reopen_date,
		claim_representative_dim.claim_rep_full_name,
		claim_representative_dim.claim_rep_num,
		claim_representative_dim.handling_office_mgr,
		claim_representative_dim.handling_office_descript,
		vw_claimant_dim1.claimant_full_name,
		claim_case_dim.suit_open_date,
		claim_occurrence_dim.loss_loc_state,
		product_code_dim.prdct_code_descript,
		strategic_business_division_dim.strtgc_bus_dvsn_code_descript,
		vw_claimant_dim1.jurisdiction_state_code,
		claim_representative_dim.claim_rep_email,
		claim_representative_dim.handling_office_mgr_email,
		claim_occurrence_dim.loss_loc_zip,
		claim_occurrence_dim.claim_cat_code,
		contract_customer_dim.name,
		coverage_dim.major_peril_code,
		claimant_coverage_dim.cause_of_loss,
		claimant_coverage_dim.reserve_ctgry,
		claim_payment_category_type_dim.claim_pay_ctgry_type,
		claimant_coverage_dim.reserve_ctgry_descript,
		claim_occurrence_dim.source_claim_occurrence_status_code,
		coverage_dim.type_bureau_code,
		coverage_dim.risk_unit_grp,
		vw_claim_loss_transaction_fact.direct_loss_outstanding_excluding_recoveries,
		vw_claim_loss_transaction_fact.direct_alae_outstanding_excluding_recoveries,
		vw_claim_loss_transaction_fact.direct_loss_paid_including_recoveries,
		vw_claim_loss_transaction_fact.direct_alae_paid_including_recoveries,
		vw_claim_loss_transaction_fact.direct_subrogation_paid,
		vw_claim_loss_transaction_fact.direct_subrogation_outstanding,
		vw_claim_loss_transaction_fact.direct_salvage_paid,
		vw_claim_loss_transaction_fact.direct_salvage_outstanding,
		claim_representative_dim.claim_rep_dim_id,
		vw_claim_loss_transaction_fact.direct_other_recovery_loss_outstanding,
		vw_claim_loss_transaction_fact.direct_other_recovery_alae_outstanding,
		vw_claim_loss_transaction_fact.claimant_dim_id,
		vw_claim_loss_transaction_fact.claim_occurrence_dim_id,
		asl_dim.asl_code,
		InsuranceReferenceDim.insurancesegmentcode,
		InsuranceReferenceDim.productdescription,
		InsuranceReferenceDim.StrategicProfitCenterAbbreviation,
		InsuranceReferenceDim.strategicprofitcenterdescription,
		v3.AgencyDim.agencycode,
		asl_dim.sub_non_asl_code_descript,
		Claimant_coverage_dim.MajorPerilCodeDescription,
		Claimant_coverage_dim.MajorPerilCode,
		-- added as per requirement in PROD-11861
		vw_claim_loss_transaction_fact.ClaimRepresentativeDimFeatureClaimRepresentativeId,
		InsuranceReferenceDim.InsuranceReferenceLineOfBusinessAbbreviation
		, claim_occurrence_dim.claim_loss_descript
		, claim_occurrence_dim.source_claim_rpted_date
		, policy_dim.pol_eff_date
		, policy_dim.pol_exp_date
	FROM dbo.policy_dim
	INNER JOIN dbo.vw_claim_loss_transaction_fact WITH (NOLOCK) ON (dbo.policy_dim.pol_dim_id = dbo.vw_claim_loss_transaction_fact.pol_dim_id)
	INNER JOIN dbo.coverage_dim WITH (NOLOCK) ON (dbo.coverage_dim.cov_dim_id = dbo.vw_claim_loss_transaction_fact.cov_dim_id)
	INNER JOIN dbo.claim_occurrence_dim WITH (NOLOCK) ON (claim_occurrence_dim.claim_occurrence_dim_id = dbo.vw_claim_loss_transaction_fact.claim_occurrence_dim_id)
	INNER JOIN dbo.vw_claimant_dim1 WITH (NOLOCK) ON (vw_claimant_dim1.claimant_dim_id = dbo.vw_claim_loss_transaction_fact.claimant_dim_id)
	INNER JOIN dbo.claimant_coverage_dim WITH (NOLOCK) ON (claimant_coverage_dim.claimant_cov_dim_id = dbo.vw_claim_loss_transaction_fact.claimant_cov_dim_id)
	INNER JOIN dbo.claim_representative_dim WITH (NOLOCK) ON (dbo.claim_representative_dim.claim_rep_dim_id = dbo.vw_claim_loss_transaction_fact.claim_rep_dim_prim_claim_rep_id)
	INNER JOIN dbo.claim_case_dim WITH (NOLOCK) ON (dbo.claim_case_dim.claim_case_dim_id = dbo.vw_claim_loss_transaction_fact.claim_case_dim_id)
	INNER JOIN dbo.contract_customer_dim WITH (NOLOCK) ON (dbo.contract_customer_dim.contract_cust_dim_id = dbo.vw_claim_loss_transaction_fact.contract_cust_dim_id)
	INNER JOIN dbo.strategic_business_division_dim WITH (NOLOCK) ON (dbo.strategic_business_division_dim.strtgc_bus_dvsn_dim_id = dbo.vw_claim_loss_transaction_fact.strtgc_bus_dvsn_dim_id)
	INNER JOIN dbo.product_code_dim WITH (NOLOCK) ON (dbo.product_code_dim.prdct_code_dim_id = dbo.vw_claim_loss_transaction_fact.prdct_code_dim_id)
	LEFT OUTER JOIN dbo.claim_payment_category_type_dim WITH (NOLOCK) ON (dbo.claim_payment_category_type_dim.claim_pay_ctgry_type_dim_id = dbo.vw_claim_loss_transaction_fact.claim_pay_ctgry_type_dim_id)
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claimant_coverage_detail WITH (NOLOCK) ON claimant_coverage_detail.claimant_cov_det_ak_id = coverage_dim.edw_claimant_cov_det_ak_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_party_occurrence WITH (NOLOCK) ON claim_party_occurrence.claim_party_occurrence_ak_id = claimant_coverage_detail.claim_party_occurrence_ak_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.vw_claim_party1 WITH (NOLOCK) ON vw_claim_party1.claim_party_ak_id = claim_party_occurrence.claim_party_ak_id
	INNER JOIN nurseassignmentfact naf WITH (NOLOCK) ON naf.claim_occurrence_dim_id = claim_occurrence_dim.claim_occurrence_dim_id
		AND naf.claimant_dim_id = vw_claimant_dim1.claimant_dim_id
	INNER JOIN nurseassignmentdim nad WITH (NOLOCK) ON nad.nurseassignmentdimid = naf.nurseassignmentdimid
		AND nad.modifieddate > '@{pipeline().parameters.SELECTION_START_TS}'
	INNER JOIN dbo.asl_dim WITH (NOLOCK) ON asl_dim.asl_dim_id = vw_claim_loss_transaction_fact.asl_dim_id
	INNER JOIN v3.AgencyDim WITH (NOLOCK) ON v3.AgencyDim.AgencyDimid = vw_claim_loss_transaction_fact.AgencyDimid
	INNER JOIN dbo.InsuranceReferenceDim WITH (NOLOCK) ON InsuranceReferenceDim.InsuranceReferenceDimid = vw_claim_loss_transaction_fact.InsuranceReferenceDimid
	WHERE claim_occurrence_dim.crrnt_snpsht_flag = 1
		AND vw_claimant_dim1.crrnt_snpsht_flag = 1
		AND claimant_coverage_dim.crrnt_snpsht_flag = 1
		AND claimant_coverage_detail.crrnt_snpsht_flag = 1
		AND claim_party_occurrence.crrnt_snpsht_flag = 1
		AND vw_claim_party1.crrnt_snpsht_flag = 1
		AND coverage_dim.crrnt_snpsht_flag = 1
		AND claim_representative_dim.crrnt_snpsht_flag = 1
		AND claimant_coverage_detail.source_sys_id = 'EXCEED'
		AND (
			claim_occurrence_dim.claim_open_date > '@{pipeline().parameters.SELECTION_BASE_DATE}'
			OR claim_occurrence_dim.claim_close_date > '@{pipeline().parameters.SELECTION_BASE_DATE}'
			)
	
	UNION
	
	-- Nurse referral changed
	SELECT DISTINCT vw_claim_party1.claim_party_key,
		claimant_coverage_detail.ins_line,
		claim_occurrence_dim.claim_num,
		coverage_dim.major_peril_descript,
		claimant_coverage_dim.cause_of_loss_long_descript,
		policy_dim.pol_num,
		claim_occurrence_dim.claim_loss_date,
		claim_occurrence_dim.claim_open_date,
		claim_occurrence_dim.claim_close_date,
		claim_occurrence_dim.claim_reopen_date,
		claim_occurrence_dim.claim_closed_after_reopen_date,
		claim_representative_dim.claim_rep_full_name,
		claim_representative_dim.claim_rep_num,
		claim_representative_dim.handling_office_mgr,
		claim_representative_dim.handling_office_descript,
		vw_claimant_dim1.claimant_full_name,
		claim_case_dim.suit_open_date,
		claim_occurrence_dim.loss_loc_state,
		product_code_dim.prdct_code_descript,
		strategic_business_division_dim.strtgc_bus_dvsn_code_descript,
		vw_claimant_dim1.jurisdiction_state_code,
		claim_representative_dim.claim_rep_email,
		claim_representative_dim.handling_office_mgr_email,
		claim_occurrence_dim.loss_loc_zip,
		claim_occurrence_dim.claim_cat_code,
		contract_customer_dim.name,
		coverage_dim.major_peril_code,
		claimant_coverage_dim.cause_of_loss,
		claimant_coverage_dim.reserve_ctgry,
		claim_payment_category_type_dim.claim_pay_ctgry_type,
		claimant_coverage_dim.reserve_ctgry_descript,
		claim_occurrence_dim.source_claim_occurrence_status_code,
		coverage_dim.type_bureau_code,
		coverage_dim.risk_unit_grp,
		vw_claim_loss_transaction_fact.direct_loss_outstanding_excluding_recoveries,
		vw_claim_loss_transaction_fact.direct_alae_outstanding_excluding_recoveries,
		vw_claim_loss_transaction_fact.direct_loss_paid_including_recoveries,
		vw_claim_loss_transaction_fact.direct_alae_paid_including_recoveries,
		vw_claim_loss_transaction_fact.direct_subrogation_paid,
		vw_claim_loss_transaction_fact.direct_subrogation_outstanding,
		vw_claim_loss_transaction_fact.direct_salvage_paid,
		vw_claim_loss_transaction_fact.direct_salvage_outstanding,
		claim_representative_dim.claim_rep_dim_id,
		vw_claim_loss_transaction_fact.direct_other_recovery_loss_outstanding,
		vw_claim_loss_transaction_fact.direct_other_recovery_alae_outstanding,
		vw_claim_loss_transaction_fact.claimant_dim_id,
		vw_claim_loss_transaction_fact.claim_occurrence_dim_id,
		asl_dim.asl_code,
		InsuranceReferenceDim.insurancesegmentcode,
		InsuranceReferenceDim.productdescription,
		InsuranceReferenceDim.StrategicProfitCenterAbbreviation,
		InsuranceReferenceDim.strategicprofitcenterdescription,
		v3.AgencyDim.agencycode,
		asl_dim.sub_non_asl_code_descript,
		Claimant_coverage_dim.MajorPerilCodeDescription,
		Claimant_coverage_dim.MajorPerilCode,
		-- added as per requirement in PROD-11861
		vw_claim_loss_transaction_fact.ClaimRepresentativeDimFeatureClaimRepresentativeId,
		InsuranceReferenceDim.InsuranceReferenceLineOfBusinessAbbreviation
		, claim_occurrence_dim.claim_loss_descript
		, claim_occurrence_dim.source_claim_rpted_date
		, policy_dim.pol_eff_date
		, policy_dim.pol_exp_date
	FROM dbo.policy_dim
	INNER JOIN dbo.vw_claim_loss_transaction_fact WITH (NOLOCK) ON (dbo.policy_dim.pol_dim_id = dbo.vw_claim_loss_transaction_fact.pol_dim_id)
	INNER JOIN dbo.coverage_dim WITH (NOLOCK) ON (dbo.coverage_dim.cov_dim_id = dbo.vw_claim_loss_transaction_fact.cov_dim_id)
	INNER JOIN dbo.claim_occurrence_dim WITH (NOLOCK) ON (claim_occurrence_dim.claim_occurrence_dim_id = dbo.vw_claim_loss_transaction_fact.claim_occurrence_dim_id)
	INNER JOIN dbo.vw_claimant_dim1 WITH (NOLOCK) ON (vw_claimant_dim1.claimant_dim_id = dbo.vw_claim_loss_transaction_fact.claimant_dim_id)
	INNER JOIN dbo.claimant_coverage_dim WITH (NOLOCK) ON (claimant_coverage_dim.claimant_cov_dim_id = dbo.vw_claim_loss_transaction_fact.claimant_cov_dim_id)
	INNER JOIN dbo.claim_representative_dim ON (dbo.claim_representative_dim.claim_rep_dim_id = dbo.vw_claim_loss_transaction_fact.claim_rep_dim_prim_claim_rep_id)
	INNER JOIN dbo.claim_case_dim WITH (NOLOCK) ON (dbo.claim_case_dim.claim_case_dim_id = dbo.vw_claim_loss_transaction_fact.claim_case_dim_id)
	INNER JOIN dbo.contract_customer_dim WITH (NOLOCK) ON (dbo.contract_customer_dim.contract_cust_dim_id = dbo.vw_claim_loss_transaction_fact.contract_cust_dim_id)
	INNER JOIN dbo.strategic_business_division_dim WITH (NOLOCK) ON (dbo.strategic_business_division_dim.strtgc_bus_dvsn_dim_id = dbo.vw_claim_loss_transaction_fact.strtgc_bus_dvsn_dim_id)
	INNER JOIN dbo.product_code_dim WITH (NOLOCK) ON (dbo.product_code_dim.prdct_code_dim_id = dbo.vw_claim_loss_transaction_fact.prdct_code_dim_id)
	LEFT OUTER JOIN dbo.claim_payment_category_type_dim WITH (NOLOCK) ON (dbo.claim_payment_category_type_dim.claim_pay_ctgry_type_dim_id = dbo.vw_claim_loss_transaction_fact.claim_pay_ctgry_type_dim_id)
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claimant_coverage_detail WITH (NOLOCK) ON claimant_coverage_detail.claimant_cov_det_ak_id = coverage_dim.edw_claimant_cov_det_ak_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.claim_party_occurrence WITH (NOLOCK) ON claim_party_occurrence.claim_party_occurrence_ak_id = claimant_coverage_detail.claim_party_occurrence_ak_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.vw_claim_party1 WITH (NOLOCK) ON vw_claim_party1.claim_party_ak_id = claim_party_occurrence.claim_party_ak_id
	INNER JOIN nursereferralfact nrf WITH (NOLOCK) ON nrf.claim_occurrence_dim_id = claim_occurrence_dim.claim_occurrence_dim_id
		AND nrf.claimant_dim_id = vw_claimant_dim1.claimant_dim_id
	INNER JOIN nursereferraldim nrd WITH (NOLOCK) ON nrd.nursereferraldimid = nrf.nursereferraldimid
		AND nrd.modifieddate > '@{pipeline().parameters.SELECTION_START_TS}'
	INNER JOIN dbo.asl_dim WITH (NOLOCK) ON asl_dim.asl_dim_id = vw_claim_loss_transaction_fact.asl_dim_id
	INNER JOIN v3.AgencyDim WITH (NOLOCK) ON v3.AgencyDim.AgencyDimid = vw_claim_loss_transaction_fact.AgencyDimid
	INNER JOIN dbo.InsuranceReferenceDim WITH (NOLOCK) ON InsuranceReferenceDim.InsuranceReferenceDimid = vw_claim_loss_transaction_fact.InsuranceReferenceDimid
	WHERE claim_occurrence_dim.crrnt_snpsht_flag = 1
		AND vw_claimant_dim1.crrnt_snpsht_flag = 1
		AND claimant_coverage_dim.crrnt_snpsht_flag = 1
		AND claimant_coverage_detail.crrnt_snpsht_flag = 1
		AND claim_party_occurrence.crrnt_snpsht_flag = 1
		AND vw_claim_party1.crrnt_snpsht_flag = 1
		AND coverage_dim.crrnt_snpsht_flag = 1
		AND claim_representative_dim.crrnt_snpsht_flag = 1
		AND claimant_coverage_detail.source_sys_id = 'EXCEED'
		AND (
			claim_occurrence_dim.claim_open_date > '@{pipeline().parameters.SELECTION_BASE_DATE}'
			OR claim_occurrence_dim.claim_close_date > '@{pipeline().parameters.SELECTION_BASE_DATE}'
			)
	ORDER BY claim_occurrence_dim.claim_num,
		coverage_dim.major_peril_code,
		claimant_coverage_dim.cause_of_loss,
		claimant_coverage_dim.reserve_ctgry,
		vw_claim_party1.claim_party_key
),
LKP_NurseAssignmentFact AS (
	SELECT
	CaseManagerName,
	ClosedDate,
	ModifiedDate,
	claimant_dim_id,
	claim_occurrence_dim_id
	FROM (
		select  
		NAD.ClosedDate as ClosedDate,
		NAD.ModifiedDate as ModifiedDate,
		NAD.NurseFullName as CaseManagerName,
		NAF.claimant_dim_id as claimant_dim_id,
		NAF.claim_occurrence_dim_id as claim_occurrence_dim_id
		from NurseAssignmentFact NAF 
		inner join NurseAssignmentDim NAD on NAD.NurseAssignmentDimId = NAF.NurseAssignmentDimId
		and NAD.CurrentSnapshotFlag=1
		--Order by NAD.ClosedDate DESC,NAD.ModifiedDate DESC,NAD.NurseFullName,NAF.claimant_dim_id ,NAF.claim_occurrence_dim_id--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_dim_id,claim_occurrence_dim_id ORDER BY CaseManagerName DESC) = 1
),
LKP_NurseReferralFact AS (
	SELECT
	ModifiedDate,
	ReferralDate,
	CaseManagerName,
	IN_claimant_dim_id,
	IN_claim_occurrence_dim_id,
	claimant_dim_id,
	claim_occurrence_dim_id
	FROM (
		SELECT
		NRD.ModifiedDate as ModifiedDate, 
		NRD.ReferralDate as ReferralDate, 
		NRD.NurseFullName as CaseManagerName, 
		NRF.claimant_dim_id as claimant_dim_id, 
		NRF.claim_occurrence_dim_id as claim_occurrence_dim_id 
		FROM 
		NurseReferralFact NRF
		 inner join NurseReferralDim NRD on NRF.NurseReferralDimId = NRD.NurseReferralDimId 
		and NRD.CurrentSnapshotFlag=1
		--Order by NRD.ModifiedDate DESC, NRD.ReferralDate DESC, NRD.NurseFullName,NRF.claimant_dim_id,NRF.claim_occurrence_dim_id --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_dim_id,claim_occurrence_dim_id ORDER BY ModifiedDate DESC) = 1
),
EXPTRANS_caseManager AS (
	SELECT
	LKP_NurseAssignmentFact.CaseManagerName AS CaseManagerNameAssignment,
	LKP_NurseReferralFact.CaseManagerName AS CaseManagerNameReferral,
	-- *INF*: IIF(ISNULL(CaseManagerNameAssignment),CaseManagerNameReferral,CaseManagerNameAssignment)
	-- 
	-- -- prefer assignment over referral, and if they are both null then it will get caught later.
	-- 
	IFF(CaseManagerNameAssignment IS NULL, CaseManagerNameReferral, CaseManagerNameAssignment) AS CaseManager,
	LKP_NurseAssignmentFact.ClosedDate,
	LKP_NurseReferralFact.ModifiedDate,
	LKP_NurseReferralFact.ReferralDate,
	LKP_NurseAssignmentFact.ModifiedDate AS ModifiedDate1
	FROM 
	LEFT JOIN LKP_NurseAssignmentFact
	ON LKP_NurseAssignmentFact.claimant_dim_id = SQ_Work_Claims_Centers_of_Expertise.claimant_dim_id AND LKP_NurseAssignmentFact.claim_occurrence_dim_id = SQ_Work_Claims_Centers_of_Expertise.claim_occurrence_dim_id
	LEFT JOIN LKP_NurseReferralFact
	ON LKP_NurseReferralFact.claimant_dim_id = SQ_Work_Claims_Centers_of_Expertise.claimant_dim_id AND LKP_NurseReferralFact.claim_occurrence_dim_id = SQ_Work_Claims_Centers_of_Expertise.claim_occurrence_dim_id
),
LKP_WorkSupCCEInsuranceLineRiskUnitGroup AS (
	SELECT
	CustomCoverageTypeDescription,
	InsuranceLine,
	RiskUnitGroup
	FROM (
		SELECT 
			CustomCoverageTypeDescription,
			InsuranceLine,
			RiskUnitGroup
		FROM WorkSupCCEInsuranceLineRiskUnitGroup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLine,RiskUnitGroup ORDER BY CustomCoverageTypeDescription DESC) = 1
),
LKP_WorkSupDwellingFireDecoder AS (
	SELECT
	CustomCoverageTypeDescription,
	TypeBureauCode,
	MajorPerilCode
	FROM (
		SELECT 
			CustomCoverageTypeDescription,
			TypeBureauCode,
			MajorPerilCode
		FROM WorkSupDwellingFireDecoder
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TypeBureauCode,MajorPerilCode ORDER BY CustomCoverageTypeDescription DESC) = 1
),
LKP_WorkSupGLPLDecoder AS (
	SELECT
	CustomCoverageTypeDescription,
	TypeBureauCode,
	CauseOfLossCode,
	MajorPerilCode
	FROM (
		SELECT 
			CustomCoverageTypeDescription,
			TypeBureauCode,
			CauseOfLossCode,
			MajorPerilCode
		FROM WorkSupGLPLDecoder
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TypeBureauCode,CauseOfLossCode,MajorPerilCode ORDER BY CustomCoverageTypeDescription DESC) = 1
),
EXP_CustomCoverageDescr AS (
	SELECT
	LKP_WorkSupGLPLDecoder.CustomCoverageTypeDescription,
	LKP_WorkSupDwellingFireDecoder.CustomCoverageTypeDescription AS CustomCoverageTypeDescription1,
	LKP_WorkSupCCEInsuranceLineRiskUnitGroup.CustomCoverageTypeDescription AS CustomCoverageTypeDescription2,
	-- *INF*: IIF(not ISNULL(CustomCoverageTypeDescription),CustomCoverageTypeDescription,
	--   IIF(not isnull(CustomCoverageTypeDescription1),CustomCoverageTypeDescription1,
	-- 	IIF(not isnull(CustomCoverageTypeDescription2),CustomCoverageTypeDescription2,
	-- 	'N/A')
	--    )
	-- )
	-- 
	IFF(
	    CustomCoverageTypeDescription IS NOT NULL, CustomCoverageTypeDescription,
	    IFF(
	        CustomCoverageTypeDescription1 IS NOT NULL, CustomCoverageTypeDescription1,
	        IFF(
	            CustomCoverageTypeDescription2 IS NOT NULL, CustomCoverageTypeDescription2,
	            'N/A'
	        )
	    )
	) AS description_OUT
	FROM 
	LEFT JOIN LKP_WorkSupCCEInsuranceLineRiskUnitGroup
	ON LKP_WorkSupCCEInsuranceLineRiskUnitGroup.InsuranceLine = SQ_Work_Claims_Centers_of_Expertise.ins_line AND LKP_WorkSupCCEInsuranceLineRiskUnitGroup.RiskUnitGroup = SQ_Work_Claims_Centers_of_Expertise.risk_unit_grp
	LEFT JOIN LKP_WorkSupDwellingFireDecoder
	ON LKP_WorkSupDwellingFireDecoder.TypeBureauCode = SQ_Work_Claims_Centers_of_Expertise.type_bureau_code AND LKP_WorkSupDwellingFireDecoder.MajorPerilCode = SQ_Work_Claims_Centers_of_Expertise.major_peril_code
	LEFT JOIN LKP_WorkSupGLPLDecoder
	ON LKP_WorkSupGLPLDecoder.TypeBureauCode = SQ_Work_Claims_Centers_of_Expertise.type_bureau_code AND LKP_WorkSupGLPLDecoder.CauseOfLossCode = SQ_Work_Claims_Centers_of_Expertise.cause_of_loss AND LKP_WorkSupGLPLDecoder.MajorPerilCode = SQ_Work_Claims_Centers_of_Expertise.major_peril_code
),
LKP_Claim_Representative_Dim AS (
	SELECT
	dept_mgr,
	handling_office_code,
	claim_rep_dim_id
	FROM (
		SELECT 
		claim_representative_dim.dept_mgr as dept_mgr, 
		claim_representative_dim.handling_office_code as handling_office_code, 
		claim_representative_dim.handling_office_mgr as handling_office_mgr, 
		claim_representative_dim.claim_rep_dim_id as claim_rep_dim_id
		FROM claim_representative_dim
		where claim_representative_dim.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_dim_id ORDER BY dept_mgr DESC) = 1
),
LKP_Feature_Claim_Representative_Dim AS (
	SELECT
	claim_rep_dim_id,
	claim_rep_full_name,
	claim_rep_num,
	i_ClaimRepresentativeDimFeatureClaimRepresentativeId
	FROM (
		SELECT 
			claim_rep_dim_id,
			claim_rep_full_name,
			claim_rep_num,
			i_ClaimRepresentativeDimFeatureClaimRepresentativeId
		FROM claim_representative_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_dim_id ORDER BY claim_rep_dim_id) = 1
),
LKP_Sup_Report_Office_Stage AS (
	SELECT
	claim_manager_code,
	director_code,
	report_office_code
	FROM (
		SELECT sup_report_office_stage.claim_manager_code as claim_manager_code, sup_report_office_stage.director_code as director_code, sup_report_office_stage.report_office_code as report_office_code FROM  dbo.sup_report_office_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY report_office_code ORDER BY claim_manager_code) = 1
),
EXP_DeriveFields AS (
	SELECT
	-- *INF*: IIF(isnull(CustomCoverageTypeDescription) or 
	-- length(rtrim(ltrim(CustomCoverageTypeDescription)))=0,'N/A', rtrim(ltrim(CustomCoverageTypeDescription)) )
	IFF(
	    CustomCoverageTypeDescription IS NULL
	    or length(rtrim(ltrim(CustomCoverageTypeDescription))) = 0,
	    'N/A',
	    rtrim(ltrim(CustomCoverageTypeDescription))
	) AS vCustomCoverageTypeDescription,
	SQ_Work_Claims_Centers_of_Expertise.claim_num,
	SYSDATE AS m_d,
	-- *INF*: SUBSTR( major_peril_code || reserve_ctgry  ||cause_of_loss || claim_party_key,0,25)
	SUBSTR(major_peril_code || reserve_ctgry || cause_of_loss || claim_party_key, 0, 25) AS subclaim,
	SQ_Work_Claims_Centers_of_Expertise.sub_non_asl_code_descript,
	SQ_Work_Claims_Centers_of_Expertise.InsuranceSegmentCode,
	SQ_Work_Claims_Centers_of_Expertise.reserve_ctgory_descript AS reserve_ctgry_descript,
	SQ_Work_Claims_Centers_of_Expertise.MajorPerilCodeDescription,
	SQ_Work_Claims_Centers_of_Expertise.MajorPerilCode,
	-- *INF*: decode(true,
	-- ltrim(rtrim(InsuranceSegmentCode))='1','PL',
	-- ltrim(rtrim(InsuranceSegmentCode))='2','CL',
	-- ltrim(rtrim(InsuranceSegmentCode))='3','POOL',
	-- 'N/A')
	-- 
	-- 
	decode(
	    true,
	    ltrim(rtrim(InsuranceSegmentCode)) = '1', 'PL',
	    ltrim(rtrim(InsuranceSegmentCode)) = '2', 'CL',
	    ltrim(rtrim(InsuranceSegmentCode)) = '3', 'POOL',
	    'N/A'
	) AS v_insurancesegment,
	-- *INF*: IIF (ISNULL(
	-- :LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'INSURANCESEGMENT',upper(v_insurancesegment))),v_insurancesegment, :LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'INSURANCESEGMENT',upper(v_insurancesegment)))
	IFF(
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__INSURANCESEGMENT_upper_v_insurancesegment.SourceColumnAbbreviation IS NULL,
	    v_insurancesegment,
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__INSURANCESEGMENT_upper_v_insurancesegment.SourceColumnAbbreviation
	) AS v_cvg_type_insurancesegment,
	-- *INF*: IIF (ISNULL(
	-- :LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'INSURANCESEGMENT',upper(v_insurancesegment))),v_insurancesegment, NULL)
	IFF(
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__INSURANCESEGMENT_upper_v_insurancesegment.SourceColumnAbbreviation IS NULL,
	    v_insurancesegment,
	    NULL
	) AS missing_insurancesegment_cvgtype,
	-- *INF*: DECODE(TRUE,
	-- UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))='FIRE','PROPERTY',
	-- UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))='ALLIED LINES','PROPERTY',
	-- UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))='CMP - LIABILITY','BOP',
	-- UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))='CMP - PROPERTY','BOP',
	-- 'N/A')
	DECODE(
	    TRUE,
	    UPPER(LTRIM(RTRIM(sub_non_asl_code_descript))) = 'FIRE', 'PROPERTY',
	    UPPER(LTRIM(RTRIM(sub_non_asl_code_descript))) = 'ALLIED LINES', 'PROPERTY',
	    UPPER(LTRIM(RTRIM(sub_non_asl_code_descript))) = 'CMP - LIABILITY', 'BOP',
	    UPPER(LTRIM(RTRIM(sub_non_asl_code_descript))) = 'CMP - PROPERTY', 'BOP',
	    'N/A'
	) AS v_cvg_type_non_asl_code_descript_p1,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'SUBNONASLDESCRIPT',UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))
	-- )),sub_non_asl_code_descript,
	-- :LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'SUBNONASLDESCRIPT',UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))
	-- )
	-- )
	IFF(
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__SUBNONASLDESCRIPT_UPPER_LTRIM_RTRIM_sub_non_asl_code_descript.SourceColumnAbbreviation IS NULL,
	    sub_non_asl_code_descript,
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__SUBNONASLDESCRIPT_UPPER_LTRIM_RTRIM_sub_non_asl_code_descript.SourceColumnAbbreviation
	) AS v_cvg_type_non_asl_code_descript_p2,
	-- *INF*: IIF(v_cvg_type_non_asl_code_descript_p1='N/A',v_cvg_type_non_asl_code_descript_p2,v_cvg_type_non_asl_code_descript_p1)
	IFF(
	    v_cvg_type_non_asl_code_descript_p1 = 'N/A', v_cvg_type_non_asl_code_descript_p2,
	    v_cvg_type_non_asl_code_descript_p1
	) AS v_cvg_type_non_asl_code_descript,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'SUBNONASLDESCRIPT',UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))
	-- )),LTRIM(RTRIM(sub_non_asl_code_descript)),
	-- NULL
	-- )
	IFF(
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__SUBNONASLDESCRIPT_UPPER_LTRIM_RTRIM_sub_non_asl_code_descript.SourceColumnAbbreviation IS NULL,
	    LTRIM(RTRIM(sub_non_asl_code_descript)),
	    NULL
	) AS missing_sub_non_asl_code_descript_cvgtype,
	-- *INF*: IIF ( 
	-- IN (UPPER(LTRIM(RTRIM(MajorPerilCodeDescription))),
	-- 'BOP BLDG','BOP B&ROB','BOP CONTS','BOP FIDELT',
	-- 'BOP MISC','BRD COVG','BROADENED COLL',
	-- 'BROADENED COVERAGE','CAR DMG REPLACEMENT',
	-- 'CUSTOMIZATION COV','DLRS BLANKET COLL',
	-- 'IDENTITY THEFT','LIMITED SPEC. PERIL',
	-- 'MEDICAL PAYMENTS','PERS INJ PROTECTION',
	-- 'PERSONAL UMBRELLA','PICKUP & DELIVERY',
	-- 'PROP DMG BUYBACK','RENTAL REIMBURSMENT',
	-- 'TOWING','UMBRELLA','UNDERINS MOTRIST BI',
	-- 'UNDERINS MOTRIST SL','UNINSRD MOTRIST BI',
	-- 'UNINSRD MOTRIST PD','UNINSRD MOTRIST SL',
	-- 'WATER BKUP & SUMP-DP','WATER BKUP & SUMP-HO',
	-- 'GL OCCUR'),
	-- LTRIM(RTRIM(MajorPerilCodeDescription)),
	-- null)
	IFF(
	    UPPER(LTRIM(RTRIM(MajorPerilCodeDescription))) IN ('BOP BLDG','BOP B&ROB','BOP CONTS','BOP FIDELT','BOP MISC','BRD COVG','BROADENED COLL','BROADENED COVERAGE','CAR DMG REPLACEMENT','CUSTOMIZATION COV','DLRS BLANKET COLL','IDENTITY THEFT','LIMITED SPEC. PERIL','MEDICAL PAYMENTS','PERS INJ PROTECTION','PERSONAL UMBRELLA','PICKUP & DELIVERY','PROP DMG BUYBACK','RENTAL REIMBURSMENT','TOWING','UMBRELLA','UNDERINS MOTRIST BI','UNDERINS MOTRIST SL','UNINSRD MOTRIST BI','UNINSRD MOTRIST PD','UNINSRD MOTRIST SL','WATER BKUP & SUMP-DP','WATER BKUP & SUMP-HO','GL OCCUR'),
	    LTRIM(RTRIM(MajorPerilCodeDescription)),
	    null
	) AS v_cvg_type_majorperil_p1,
	-- *INF*: IIF( ISNULL(:LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'MAJORPERIL',UPPER(LTRIM(RTRIM(v_cvg_type_majorperil_p1)))
	-- )),
	-- v_cvg_type_majorperil_p1,
	-- :LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'MAJORPERIL',UPPER(LTRIM(RTRIM(v_cvg_type_majorperil_p1)))
	-- ))
	IFF(
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__MAJORPERIL_UPPER_LTRIM_RTRIM_v_cvg_type_majorperil_p1.SourceColumnAbbreviation IS NULL,
	    v_cvg_type_majorperil_p1,
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__MAJORPERIL_UPPER_LTRIM_RTRIM_v_cvg_type_majorperil_p1.SourceColumnAbbreviation
	) AS v_cvg_type_majorperil,
	-- *INF*: IIF( ISNULL(:LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'MAJORPERIL',UPPER(LTRIM(RTRIM(v_cvg_type_majorperil_p1)))
	-- )),
	-- LTRIM(RTRIM(v_cvg_type_majorperil_p1)),
	-- NULL)
	IFF(
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__MAJORPERIL_UPPER_LTRIM_RTRIM_v_cvg_type_majorperil_p1.SourceColumnAbbreviation IS NULL,
	    LTRIM(RTRIM(v_cvg_type_majorperil_p1)),
	    NULL
	) AS missing_majorperil_cvgtype,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(v_insurancesegment))='PL',reserve_ctgry_descript,
	-- UPPER(LTRIM(RTRIM(MajorPerilCodeDescription)))='UMBRELLA',reserve_ctgry_descript,
	-- UPPER(LTRIM(RTRIM(MajorPerilCodeDescription)))='PERSONAL UMBRELLA',reserve_ctgry_descript,
	-- UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))='FIRE',reserve_ctgry_descript,
	-- UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))='ALLIED LINES',reserve_ctgry_descript,null)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(v_insurancesegment)) = 'PL', reserve_ctgry_descript,
	    UPPER(LTRIM(RTRIM(MajorPerilCodeDescription))) = 'UMBRELLA', reserve_ctgry_descript,
	    UPPER(LTRIM(RTRIM(MajorPerilCodeDescription))) = 'PERSONAL UMBRELLA', reserve_ctgry_descript,
	    UPPER(LTRIM(RTRIM(sub_non_asl_code_descript))) = 'FIRE', reserve_ctgry_descript,
	    UPPER(LTRIM(RTRIM(sub_non_asl_code_descript))) = 'ALLIED LINES', reserve_ctgry_descript,
	    null
	) AS v_cvg_type_reserve_ctgry_descript_p1,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'RESERVECATEGORY',UPPER(LTRIM(RTRIM(v_cvg_type_reserve_ctgry_descript_p1)))
	-- )),
	-- v_cvg_type_reserve_ctgry_descript_p1,
	-- :LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'RESERVECATEGORY',UPPER(LTRIM(RTRIM(v_cvg_type_reserve_ctgry_descript_p1)))
	-- ))
	IFF(
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__RESERVECATEGORY_UPPER_LTRIM_RTRIM_v_cvg_type_reserve_ctgry_descript_p1.SourceColumnAbbreviation IS NULL,
	    v_cvg_type_reserve_ctgry_descript_p1,
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__RESERVECATEGORY_UPPER_LTRIM_RTRIM_v_cvg_type_reserve_ctgry_descript_p1.SourceColumnAbbreviation
	) AS v_cvg_type_reserve_ctgry_descript,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SupClaimsCentersOfExpertiseCoverage(
	-- 'RESERVECATEGORY',UPPER(LTRIM(RTRIM(v_cvg_type_reserve_ctgry_descript_p1)))
	-- )),
	-- LTRIM(RTRIM(v_cvg_type_reserve_ctgry_descript_p1)),
	-- NULL)
	IFF(
	    LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__RESERVECATEGORY_UPPER_LTRIM_RTRIM_v_cvg_type_reserve_ctgry_descript_p1.SourceColumnAbbreviation IS NULL,
	    LTRIM(RTRIM(v_cvg_type_reserve_ctgry_descript_p1)),
	    NULL
	) AS missing_reserve_ctgry_descript_cvgtype,
	-- *INF*: --LTRIM(RTRIM(v_cvg_type_insurancesegment))||IIF(ISNULL(LTRIM(RTRIM(v_cvg_type_majorperil))),'',' '||LTRIM(RTRIM(v_cvg_type_majorperil)))||IIF(ISNULL(LTRIM(RTRIM(v_cvg_type_reserve_ctgry_descript))),'',' '||LTRIM(RTRIM(v_cvg_type_reserve_ctgry_descript)))||IIF(ISNULL(LTRIM(RTRIM(v_cvg_type_non_asl_code_descript))),'',' '||LTRIM(RTRIM(v_cvg_type_non_asl_code_descript)))
	-- 
	-- MajorPerilCodeDescription
	-- --As part of prod-11861 'MajorPerilCodeDescription' is populating on CoverageType in target table.
	MajorPerilCodeDescription AS o_coveragetype,
	EXP_CustomCoverageDescr.description_OUT AS CustomCoverageTypeDescription,
	SQ_Work_Claims_Centers_of_Expertise.major_peril_descript,
	SQ_Work_Claims_Centers_of_Expertise.claim_party_key,
	SQ_Work_Claims_Centers_of_Expertise.cause_of_loss_long_descript,
	-- *INF*:  IIF(major_peril_descript= 'HOMEOWNERS' AND 
	-- IN 
	-- (
	-- cause_of_loss_long_descript ,
	-- 'HO LIABILITY BODILY INJURY INCLUDING WATERCRAFT', 
	-- 'HO LIABILITY MEDICAL PAY INCLUDING WATERCRAFT', 
	-- 'HO LIABILITY PD INCLUDING WATERCRAFT', 
	-- 'HO LIABILITY PERSONAL INJURY-HO-82/HO2482', 
	-- 'MEDICAL PAYMENTS'
	-- ) ,'GENERAL LIABILITY',  
	-- 
	-- IIF (
	-- IN (
	-- major_peril_descript,
	-- 'AUTO BI', 'AUTO BI/PD SNG LIMIT', 'AUTO BODILY INJURY' , 'AUTO LOAN/LEASE' , 'AUTO LOAN/LEASE COV' , 'AUTO-PD COLLISION' , 'BRD COVG' , 'BROADENED COVERAGE' , 'BROADENED COVERAGE' , 'CAR DAMAGE REPLACE' , 'CAR DMG REPLACEMENT' , 'COLLISION' , 'COMB COLLISION-COMP' , 'COMB COMP/COLL' , 'COMB SINGLE LIMIT' , 'COMMERCIAL UMBRELLA' , 'COMPREHENSIVE' , 'DLRS BLANKET COLL' , 'HIGHWAY EMERGENCY' , 'MEDICAL PAYMENTS' , 'PERS INJ PROTECTION' , 'PERSONAL UMBRELLA' , 'PI PROTECTION' , 'PROP.DAM.' , 'PROPERTY DAMAGE' , 'RENTAL REIMBURSEMENT' , 'SOUND REC & TRANS' , 'SPECIAL EQUIPMENT ' , 'TOWING' , 'TOWING & LABOR' , 'TRAVELNET' , 'UMBRELLA' , 'UNDERINS MOTRIST BI' , 'UNDERINS MOTRIST SL' , 'UNDERINS MTRST-BI' , 'UNDERINS MTRST-PD ' , 'UNDERINS MTRST-SL' , 'UNINS MOTORIST-BI' , 'UNINS MOTORIST-SL' , 'UNINSRD MOTRIST BI' , 'UNINSRD MOTRIST SL' , 'UN-PD' , 'VAN/TRUCK CUSTOM COV'
	-- ) , 'AUTO',
	-- IIF
	-- (
	-- IN (major_peril_descript,'BI & PD CLAIMS-MADE' , 'BI & PD OCCURRENCE', 'BOATOWNERS LIABILITY' , 'C-BOP LIAB', 'GENERAL LIABILITY-PD' , 
	-- 'GL CLM-MDE' , 'GL OCCUR' , 'GL PRODW' , 'SNOWMOBILE LIAB') ,  'GENERAL LIABILITY','PROPERTY' 
	--        )
	-- )
	-- )
	--  
	IFF(
	    major_peril_descript = 'HOMEOWNERS'
	    and cause_of_loss_long_descript IN ('HO LIABILITY BODILY INJURY INCLUDING WATERCRAFT','HO LIABILITY MEDICAL PAY INCLUDING WATERCRAFT','HO LIABILITY PD INCLUDING WATERCRAFT','HO LIABILITY PERSONAL INJURY-HO-82/HO2482','MEDICAL PAYMENTS'),
	    'GENERAL LIABILITY',
	    IFF(
	        major_peril_descript IN ('AUTO BI','AUTO BI/PD SNG LIMIT','AUTO BODILY INJURY','AUTO LOAN/LEASE','AUTO LOAN/LEASE COV','AUTO-PD COLLISION','BRD COVG','BROADENED COVERAGE','BROADENED COVERAGE','CAR DAMAGE REPLACE','CAR DMG REPLACEMENT','COLLISION','COMB COLLISION-COMP','COMB COMP/COLL','COMB SINGLE LIMIT','COMMERCIAL UMBRELLA','COMPREHENSIVE','DLRS BLANKET COLL','HIGHWAY EMERGENCY','MEDICAL PAYMENTS','PERS INJ PROTECTION','PERSONAL UMBRELLA','PI PROTECTION','PROP.DAM.','PROPERTY DAMAGE','RENTAL REIMBURSEMENT','SOUND REC & TRANS','SPECIAL EQUIPMENT ','TOWING','TOWING & LABOR','TRAVELNET','UMBRELLA','UNDERINS MOTRIST BI','UNDERINS MOTRIST SL','UNDERINS MTRST-BI','UNDERINS MTRST-PD ','UNDERINS MTRST-SL','UNINS MOTORIST-BI','UNINS MOTORIST-SL','UNINSRD MOTRIST BI','UNINSRD MOTRIST SL','UN-PD','VAN/TRUCK CUSTOM COV'),
	        'AUTO',
	        IFF(
	            major_peril_descript IN ('BI & PD CLAIMS-MADE','BI & PD OCCURRENCE','BOATOWNERS LIABILITY','C-BOP LIAB','GENERAL LIABILITY-PD','GL CLM-MDE','GL OCCUR','GL PRODW','SNOWMOBILE LIAB'),
	            'GENERAL LIABILITY',
	            'PROPERTY'
	        )
	    )
	) AS claimtype_1,
	SQ_Work_Claims_Centers_of_Expertise.StrategicProfitCenterAbbreviation,
	SQ_Work_Claims_Centers_of_Expertise.StrategicProfitCenterDescription,
	-- *INF*: DECODE(TRUE,
	-- 
	-- --removed TypeBereauCode checks per PROD11861
	-- 
	-- IN(asl_code, '220', '230','240','250','100','200') AND IN(InsuranceSegmentCode,'1'),'PL GENERAL LIABILITY',
	-- 
	-- IN(asl_code, '220', '230','240','250','100','200') AND IN(InsuranceSegmentCode,'2'),'CL GENERAL LIABILITY',
	-- 
	-- IN(asl_code, '340','500','260','440') AND IN(InsuranceSegmentCode,'1'),'PL AUTO',
	-- 
	-- IN(asl_code, '340','500','260','440') AND IN(InsuranceSegmentCode,'2'),'CL AUTO',
	-- 
	-- IN(asl_code, '40','20','640','660','80','140','60','120') AND IN(InsuranceSegmentCode,'1'),'PL PROPERTY',
	-- 
	-- IN(asl_code, '40','20','640','660','80','140','60','120') AND IN(InsuranceSegmentCode,'2'),'CL PROPERTY',
	-- 
	-- IN(asl_code,'160') AND IN(InsuranceSegmentCode,'3'),'POOL SERVICES',
	-- 
	-- IN(asl_code,'160') AND IN(InsuranceSegmentCode,'2'),'WORK COMP',
	-- 
	-- IN(asl_code,' ') AND IN(InsuranceSegmentCode,'2'),'CL OTHER',
	-- 
	-- IN(asl_code,' ') AND IN(InsuranceSegmentCode,'1'),'PL OTHER',
	-- 
	-- IN(asl_code,' ') AND IN(InsuranceSegmentCode,' '),'N/A',
	-- 
	-- IN(asl_code, '600','620') AND IN(InsuranceSegmentCode,'2'),'BONDS',
	-- 
	-- 'OTHER')
	DECODE(
	    TRUE,
	    asl_code IN ('220','230','240','250','100','200') AND InsuranceSegmentCode IN ('1'), 'PL GENERAL LIABILITY',
	    asl_code IN ('220','230','240','250','100','200') AND InsuranceSegmentCode IN ('2'), 'CL GENERAL LIABILITY',
	    asl_code IN ('340','500','260','440') AND InsuranceSegmentCode IN ('1'), 'PL AUTO',
	    asl_code IN ('340','500','260','440') AND InsuranceSegmentCode IN ('2'), 'CL AUTO',
	    asl_code IN ('40','20','640','660','80','140','60','120') AND InsuranceSegmentCode IN ('1'), 'PL PROPERTY',
	    asl_code IN ('40','20','640','660','80','140','60','120') AND InsuranceSegmentCode IN ('2'), 'CL PROPERTY',
	    asl_code IN ('160') AND InsuranceSegmentCode IN ('3'), 'POOL SERVICES',
	    asl_code IN ('160') AND InsuranceSegmentCode IN ('2'), 'WORK COMP',
	    asl_code IN (' ') AND InsuranceSegmentCode IN ('2'), 'CL OTHER',
	    asl_code IN (' ') AND InsuranceSegmentCode IN ('1'), 'PL OTHER',
	    asl_code IN (' ') AND InsuranceSegmentCode IN (' '), 'N/A',
	    asl_code IN ('600','620') AND InsuranceSegmentCode IN ('2'), 'BONDS',
	    'OTHER'
	) AS Claimtype,
	-- *INF*: LTRIM(RTRIM(StrategicProfitCenterAbbreviation))
	LTRIM(RTRIM(StrategicProfitCenterAbbreviation)) AS out_CompanyName,
	-- *INF*: IIF(  direct_loss_outstanding_excluding_recoveries=0  AND direct_alae_outstanding_excluding_recoveries =0 AND direct_salvage_outstanding=0 AND direct_subrogation_outstanding=0 AND direct_other_recovery_loss_outstanding=0 AND direct_other_recovery_alae_outstanding=0,'close','open')
	IFF(
	    direct_loss_outstanding_excluding_recoveries = 0
	    and direct_alae_outstanding_excluding_recoveries = 0
	    and direct_salvage_outstanding = 0
	    and direct_subrogation_outstanding = 0
	    and direct_other_recovery_loss_outstanding = 0
	    and direct_other_recovery_alae_outstanding = 0,
	    'close',
	    'open'
	) AS status,
	SQ_Work_Claims_Centers_of_Expertise.pol_num,
	SQ_Work_Claims_Centers_of_Expertise.direct_loss_paid_including_recoveries,
	SQ_Work_Claims_Centers_of_Expertise.direct_alae_paid_including_recoveries,
	SQ_Work_Claims_Centers_of_Expertise.direct_salvage_paid,
	SQ_Work_Claims_Centers_of_Expertise.direct_subrogation_paid,
	SQ_Work_Claims_Centers_of_Expertise.claim_loss_date,
	SQ_Work_Claims_Centers_of_Expertise.claim_open_date,
	SQ_Work_Claims_Centers_of_Expertise.claim_close_date,
	-- *INF*: IIF(claim_reopen_date<> TO_DATE('01/01/1800' , 'MM/DD/YYYY'),claim_closed_after_reopen_date,claim_close_date)
	IFF(
	    claim_reopen_date <> TO_TIMESTAMP('01/01/1800', 'MM/DD/YYYY'),
	    claim_closed_after_reopen_date,
	    claim_close_date
	) AS o_claim_close_date,
	SQ_Work_Claims_Centers_of_Expertise.claim_rep_full_name,
	SQ_Work_Claims_Centers_of_Expertise.claim_rep_num,
	SQ_Work_Claims_Centers_of_Expertise.handling_office_mgr,
	SQ_Work_Claims_Centers_of_Expertise.handling_office_descript,
	SQ_Work_Claims_Centers_of_Expertise.claimant_full_name,
	-- *INF*: REPLACECHR(TRUE,claimant_full_name,'"',' ') 
	-- 
	-- 
	-- 
	-- 
	-- 
	REGEXP_REPLACE(claimant_full_name,'"',' ') AS o_claimant_full_name,
	SQ_Work_Claims_Centers_of_Expertise.claim_rep_email,
	SQ_Work_Claims_Centers_of_Expertise.InsuranceReferenceLineOfBusinessAbbreviation,
	-- *INF*: IIF (cause_of_loss = '05' AND LTRIM(RTRIM(InsuranceReferenceLineOfBusinessAbbreviation))='WC' ,direct_loss_paid_including_recoveries)  
	-- 
	IFF(
	    cause_of_loss = '05' AND LTRIM(RTRIM(InsuranceReferenceLineOfBusinessAbbreviation)) = 'WC',
	    direct_loss_paid_including_recoveries
	) AS LossIndemnity,
	-- *INF*: IIF (cause_of_loss = '06'  AND LTRIM(RTRIM(InsuranceReferenceLineOfBusinessAbbreviation))='WC',direct_loss_paid_including_recoveries)  
	-- 
	IFF(
	    cause_of_loss = '06' AND LTRIM(RTRIM(InsuranceReferenceLineOfBusinessAbbreviation)) = 'WC',
	    direct_loss_paid_including_recoveries
	) AS LossMedical,
	-- *INF*: IIF( suit_open_date = TO_DATE('01-JAN-1800', 'DD-MON-YYYY') , 'N' , 'Y' )
	-- 
	IFF(suit_open_date = TO_TIMESTAMP('01-JAN-1800', 'DD-MON-YYYY'), 'N', 'Y') AS LegalIndicator,
	SQ_Work_Claims_Centers_of_Expertise.cause_of_loss,
	SQ_Work_Claims_Centers_of_Expertise.reserve_ctgry,
	SQ_Work_Claims_Centers_of_Expertise.suit_open_date,
	SQ_Work_Claims_Centers_of_Expertise.loss_loc_state,
	SQ_Work_Claims_Centers_of_Expertise.prdct_code_descript,
	SQ_Work_Claims_Centers_of_Expertise.strtgc_bus_dvsn_code AS strtgc_bus_dvsn_code_descript,
	SQ_Work_Claims_Centers_of_Expertise.jurisdiction_state_code,
	SQ_Work_Claims_Centers_of_Expertise.direct_loss_outstanding_excluding_recoveries,
	SQ_Work_Claims_Centers_of_Expertise.direct_alae_outstanding_excluding_recoveries,
	SQ_Work_Claims_Centers_of_Expertise.handling_office_mgr_email,
	SQ_Work_Claims_Centers_of_Expertise.loss_loc_zip,
	SQ_Work_Claims_Centers_of_Expertise.claim_cat_code,
	-- *INF*: IIF (claim_cat_code = 'N/A','N','Y')
	-- 
	IFF(claim_cat_code = 'N/A', 'N', 'Y') AS CategoryIndicator,
	SQ_Work_Claims_Centers_of_Expertise.name,
	SQ_Work_Claims_Centers_of_Expertise.direct_salvage_outstanding,
	SQ_Work_Claims_Centers_of_Expertise.direct_subrogation_outstanding,
	SQ_Work_Claims_Centers_of_Expertise.claim_pay_ctgry_type,
	-- *INF*: IIF(claim_pay_ctgry_type='IA','Y','N')
	IFF(claim_pay_ctgry_type = 'IA', 'Y', 'N') AS ExAdjusterIndicator,
	SQ_Work_Claims_Centers_of_Expertise.major_peril_code,
	-- *INF*: IIF(IN (major_peril_code,'210', '002') AND  IN(cause_of_loss ,'16','26','36','46','06','07','08'), 'CASUALTY',
	-- (IIF(IN(major_peril_code,'901','902') and IN(cause_of_loss ,'16','26','97','77','67','57'),'CASUALTY',
	-- IIF(IN(major_peril_code, '042' ,'017' ,'065' ,'066' ,'067',
	-- '084' ,'085' ,'100' ,'101' ,'110' ,'112' ,'114' ,'115' ,'116' ,'118' ,'119' , 
	-- '120' ,'125' ,'130' ,'145' ,'146' ,'147' ,'272' ,'206' ,'165' ,'166' ,'168' ,
	-- '169' ,'170' ,'171' ,'173' ,'174' ,'178' ,'155' ,'269' ,'270' ,'271' ,'024' 
	-- ,'480' ,'517' ,'530' ,'534' ,'540' ,'904' ,'010' ,'910' ,'273' ,'912' ,'919' ,
	-- '044'),'CASUALTY',NULL))))
	-- 
	-- 
	-- 
	-- 
	IFF(
	    major_peril_code IN ('210','002') AND cause_of_loss IN ('16','26','36','46','06','07','08'),
	    'CASUALTY',
	    (
	        IFF(
	            major_peril_code IN ('901','902')
	    and cause_of_loss IN ('16','26','97','77','67','57'),
	            'CASUALTY',
	            IFF(
	                major_peril_code IN ('042','017','065','066','067','084','085','100','101','110','112','114','115','116','118','119','120','125','130','145','146','147','272','206','165','166','168','169','170','171','173','174','178','155','269','270','271','024','480','517','530','534','540','904','010','910','273','912','919','044'),
	                'CASUALTY',
	                NULL
	            )
	        ))
	) AS v_genmisc1_casualty,
	-- *INF*: IIF( IN  (major_peril_code,
	-- '490' ,'148' ,'015' ,'016' ,'909' ,'050' ,'062' ,'907' ,'071' ,'097' ,'200' ,
	-- '201' ,'210' ,'211' ,'220' ,'221','226' ,'227' ,'228' ,'229' ,'249' ,'250' ,
	-- '260' ,'280' ,'415' ,'425' ,'426' ,'435' , '455' ,'463' ,'911' ,'496' ,'550' ,'551' ,'565' ,'566' ,'570' ,'901' ,
	-- '902' ,'903' ,'905' ,'906' ,'908' , '914' , '002', '226'),'PROPERTY',null)
	-- 
	-- 
	-- 
	-- 
	IFF(
	    major_peril_code IN ('490','148','015','016','909','050','062','907','071','097','200','201','210','211','220','221','226','227','228','229','249','250','260','280','415','425','426','435','455','463','911','496','550','551','565','566','570','901','902','903','905','906','908','914','002','226'),
	    'PROPERTY',
	    null
	) AS v_genmisc1_property,
	-- *INF*: IIF( IN  (major_peril_code,
	-- '920','921','922','924','925'),'BONDS',
	-- (IIF(major_peril_code='032','WORK COMP',NULL))
	-- )
	-- 
	-- 
	-- 
	IFF(
	    major_peril_code IN ('920','921','922','924','925'), 'BONDS',
	    (
	        IFF(
	            major_peril_code = '032', 'WORK COMP', NULL
	        ))
	) AS v_genmisc1_other,
	-- *INF*: --IIF(NOT ISNULL(v_genmisc1_casualty),v_genmisc1_casualty,(IIF(NOT ISNULL(v_genmisc1_property),v_genmisc1_property,(IIF(NOT ISNULL(v_genmisc1_other),v_genmisc1_other,(IIF(ISNULL(major_peril_code) AND ISNULL(cause_of_loss),'N/A')))))))
	-- 
	-- ltrim(rtrim(:LKP.LKP_SupTypeOfLossRules(InsuranceSegmentCode, MajorPerilCode, cause_of_loss)))
	-- --PROD-11861
	ltrim(rtrim(LKP_SUPTYPEOFLOSSRULES_InsuranceSegmentCode_MajorPerilCode_cause_of_loss.ClaimTypeCategory)) AS v_GeneralMiscellaneous1,
	-- *INF*: IIF(ISNULL(ltrim(rtrim(v_GeneralMiscellaneous1))),'N/A',ltrim(rtrim(v_GeneralMiscellaneous1)))
	IFF(
	    ltrim(rtrim(v_GeneralMiscellaneous1)) IS NULL, 'N/A', ltrim(rtrim(v_GeneralMiscellaneous1))
	) AS out_GeneralMiscellaneous1,
	-- *INF*: --LTRIM(RTRIM(Claimtype))
	-- --PROD-11861
	-- IIF(ltrim(rtrim(Claimtype))='PL PROPERTY' AND UPPER(v_GeneralMiscellaneous1)='CASUALTY','PL General Liability',IIF(ltrim(rtrim(Claimtype))='CL PROPERTY' AND UPPER(v_GeneralMiscellaneous1)='CASUALTY','CL General Liability',ltrim(rtrim(Claimtype))))
	IFF(
	    ltrim(rtrim(Claimtype)) = 'PL PROPERTY' AND UPPER(v_GeneralMiscellaneous1) = 'CASUALTY',
	    'PL General Liability',
	    IFF(
	        ltrim(rtrim(Claimtype)) = 'CL PROPERTY'
	    and UPPER(v_GeneralMiscellaneous1) = 'CASUALTY',
	        'CL General Liability',
	        ltrim(rtrim(Claimtype))
	    )
	) AS out_Claimtype,
	-- *INF*: IIF(claim_pay_ctgry_type='IP','Y','N')
	IFF(claim_pay_ctgry_type = 'IP', 'Y', 'N') AS GeneralMiscellaneous2,
	SQ_Work_Claims_Centers_of_Expertise.source_claim_occurence_status_code,
	SQ_Work_Claims_Centers_of_Expertise.type_bureau_code,
	LKP_Sup_Report_Office_Stage.claim_manager_code AS SupervisorCode,
	-- *INF*: IIF(ISNULL(SupervisorCode),'N/A',SupervisorCode)
	IFF(SupervisorCode IS NULL, 'N/A', SupervisorCode) AS o_SupervisorCode,
	LKP_Claim_Representative_Dim.handling_office_code AS OfficeCode,
	-- *INF*: IIF(ISNULL(OfficeCode),'N/A',OfficeCode)
	IFF(OfficeCode IS NULL, 'N/A', OfficeCode) AS O_Officecode,
	LKP_Claim_Representative_Dim.dept_mgr AS UnitName,
	-- *INF*: IIF(ISNULL(UnitName),'N/A',UnitName)
	IFF(UnitName IS NULL, 'N/A', UnitName) AS O_UnitName,
	LKP_Sup_Report_Office_Stage.director_code AS UnitCode,
	-- *INF*: IIF(ISNULL(UnitCode),'N/A',UnitCode)
	IFF(UnitCode IS NULL, 'N/A', UnitCode) AS O_Unitcode,
	-- *INF*: IIF(ISNULL(GeneralMiscellaneous10),'N','Y')
	IFF(GeneralMiscellaneous10 IS NULL, 'N', 'Y') AS out_GeneralMiscellaneous9,
	EXPTRANS_caseManager.CaseManager AS GeneralMiscellaneous10,
	-- *INF*: IIF(ISNULL(GeneralMiscellaneous10),'N/A',rtrim(ltrim(GeneralMiscellaneous10)))
	IFF(GeneralMiscellaneous10 IS NULL, 'N/A', rtrim(ltrim(GeneralMiscellaneous10))) AS out_GeneralMiscellaneous10,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SQ_Work_Claims_Centers_of_Expertise.direct_other_recovery_loss_outstanding,
	SQ_Work_Claims_Centers_of_Expertise.direct_other_recovery_alae_outstanding,
	SQ_Work_Claims_Centers_of_Expertise.claim_reopen_date,
	SQ_Work_Claims_Centers_of_Expertise.claim_closed_after_reopen_date,
	SQ_Work_Claims_Centers_of_Expertise.asl_code,
	SQ_Work_Claims_Centers_of_Expertise.ProductDescription,
	SQ_Work_Claims_Centers_of_Expertise.AgencyCode,
	LKP_Feature_Claim_Representative_Dim.claim_rep_full_name AS ClaimFeatureRepresentativeName,
	LKP_Feature_Claim_Representative_Dim.claim_rep_num AS ClaimFeatureRepresentativeCode,
	SQ_Work_Claims_Centers_of_Expertise.claim_loss_descript,
	SQ_Work_Claims_Centers_of_Expertise.source_claim_rpted_date,
	SQ_Work_Claims_Centers_of_Expertise.pol_eff_date,
	SQ_Work_Claims_Centers_of_Expertise.pol_exp_date
	FROM EXPTRANS_caseManager
	 -- Manually join with EXP_CustomCoverageDescr
	 -- Manually join with SQ_Work_Claims_Centers_of_Expertise
	LEFT JOIN LKP_Claim_Representative_Dim
	ON LKP_Claim_Representative_Dim.claim_rep_dim_id = SQ_Work_Claims_Centers_of_Expertise.claim_rep_dim_id
	LEFT JOIN LKP_Feature_Claim_Representative_Dim
	ON LKP_Feature_Claim_Representative_Dim.claim_rep_dim_id = SQ_Work_Claims_Centers_of_Expertise.ClaimRepresentativeDimFeatureClaimRepresentativeId
	LEFT JOIN LKP_Sup_Report_Office_Stage
	ON LKP_Sup_Report_Office_Stage.report_office_code = LKP_Claim_Representative_Dim.handling_office_code
	LEFT JOIN LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__INSURANCESEGMENT_upper_v_insurancesegment
	ON LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__INSURANCESEGMENT_upper_v_insurancesegment.SourceColumnName = 'INSURANCESEGMENT'
	AND LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__INSURANCESEGMENT_upper_v_insurancesegment.SourceColumnValue = upper(v_insurancesegment)

	LEFT JOIN LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__SUBNONASLDESCRIPT_UPPER_LTRIM_RTRIM_sub_non_asl_code_descript
	ON LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__SUBNONASLDESCRIPT_UPPER_LTRIM_RTRIM_sub_non_asl_code_descript.SourceColumnName = 'SUBNONASLDESCRIPT'
	AND LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__SUBNONASLDESCRIPT_UPPER_LTRIM_RTRIM_sub_non_asl_code_descript.SourceColumnValue = UPPER(LTRIM(RTRIM(sub_non_asl_code_descript)))

	LEFT JOIN LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__MAJORPERIL_UPPER_LTRIM_RTRIM_v_cvg_type_majorperil_p1
	ON LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__MAJORPERIL_UPPER_LTRIM_RTRIM_v_cvg_type_majorperil_p1.SourceColumnName = 'MAJORPERIL'
	AND LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__MAJORPERIL_UPPER_LTRIM_RTRIM_v_cvg_type_majorperil_p1.SourceColumnValue = UPPER(LTRIM(RTRIM(v_cvg_type_majorperil_p1)))

	LEFT JOIN LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__RESERVECATEGORY_UPPER_LTRIM_RTRIM_v_cvg_type_reserve_ctgry_descript_p1
	ON LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__RESERVECATEGORY_UPPER_LTRIM_RTRIM_v_cvg_type_reserve_ctgry_descript_p1.SourceColumnName = 'RESERVECATEGORY'
	AND LKP_SUPCLAIMSCENTERSOFEXPERTISECOVERAGE__RESERVECATEGORY_UPPER_LTRIM_RTRIM_v_cvg_type_reserve_ctgry_descript_p1.SourceColumnValue = UPPER(LTRIM(RTRIM(v_cvg_type_reserve_ctgry_descript_p1)))

	LEFT JOIN LKP_SUPTYPEOFLOSSRULES LKP_SUPTYPEOFLOSSRULES_InsuranceSegmentCode_MajorPerilCode_cause_of_loss
	ON LKP_SUPTYPEOFLOSSRULES_InsuranceSegmentCode_MajorPerilCode_cause_of_loss.InsuranceSegmentCode = InsuranceSegmentCode
	AND LKP_SUPTYPEOFLOSSRULES_InsuranceSegmentCode_MajorPerilCode_cause_of_loss.MajorPerilCode = MajorPerilCode
	AND LKP_SUPTYPEOFLOSSRULES_InsuranceSegmentCode_MajorPerilCode_cause_of_loss.CauseOfLoss = cause_of_loss

),
AGG_Claims AS (
	SELECT
	claim_num,
	pol_num,
	claim_loss_date,
	claim_open_date,
	o_claim_close_date AS claim_close_date,
	claim_rep_full_name,
	claim_rep_num,
	handling_office_mgr,
	handling_office_descript,
	loss_loc_state,
	strtgc_bus_dvsn_code_descript,
	direct_loss_paid_including_recoveries,
	-- *INF*: SUM(direct_loss_paid_including_recoveries)
	SUM(direct_loss_paid_including_recoveries) AS LossMiscellaneous1,
	direct_loss_outstanding_excluding_recoveries,
	-- *INF*: SUM(direct_loss_outstanding_excluding_recoveries)
	SUM(direct_loss_outstanding_excluding_recoveries) AS LossReserveMiscellaneous1,
	direct_alae_paid_including_recoveries,
	-- *INF*: SUM(direct_alae_paid_including_recoveries)
	SUM(direct_alae_paid_including_recoveries) AS ExpenseMiscellaneous1,
	direct_alae_outstanding_excluding_recoveries,
	-- *INF*: SUM(direct_alae_outstanding_excluding_recoveries)
	SUM(direct_alae_outstanding_excluding_recoveries) AS ExpenseReserveMiscellaneous1,
	claim_rep_email,
	handling_office_mgr_email,
	loss_loc_zip,
	CategoryIndicator,
	name,
	source_claim_occurence_status_code,
	claim_cat_code,
	LossIndemnity AS i_LossIndemnity,
	-- *INF*: SUM(i_LossIndemnity)
	SUM(i_LossIndemnity) AS o_LossIndemnity,
	LossMedical AS i_LossMedical,
	-- *INF*: SUM(i_LossMedical)
	SUM(i_LossMedical) AS o_LossMedical,
	out_CompanyName AS CompanyName,
	AgencyCode,
	ClaimFeatureRepresentativeName,
	ClaimFeatureRepresentativeCode,
	claim_loss_descript,
	source_claim_rpted_date,
	pol_eff_date,
	pol_exp_date
	FROM EXP_DeriveFields
	GROUP BY claim_num
),
SRT_claims AS (
	SELECT
	claim_num, 
	pol_num, 
	claim_loss_date, 
	claim_open_date, 
	claim_close_date, 
	claim_rep_full_name, 
	claim_rep_num, 
	handling_office_mgr, 
	handling_office_descript, 
	loss_loc_state, 
	strtgc_bus_dvsn_code_descript AS strtgc_bus_dvsn_code, 
	LossMiscellaneous1, 
	LossReserveMiscellaneous1, 
	ExpenseMiscellaneous1, 
	ExpenseReserveMiscellaneous1, 
	claim_rep_email, 
	handling_office_mgr_email, 
	loss_loc_zip, 
	CategoryIndicator, 
	name, 
	source_claim_occurence_status_code, 
	claim_cat_code, 
	o_LossIndemnity AS LossIndemnity, 
	o_LossMedical AS LossMedical, 
	CompanyName, 
	AgencyCode, 
	ClaimFeatureRepresentativeName, 
	ClaimFeatureRepresentativeCode, 
	claim_loss_descript, 
	source_claim_rpted_date, 
	pol_eff_date, 
	pol_exp_date
	FROM AGG_Claims
	ORDER BY claim_num ASC
),
AGG_SubClaims AS (
	SELECT
	claim_num,
	subclaim,
	m_d,
	out_Claimtype AS Claimtype,
	missing_insurancesegment_cvgtype,
	missing_sub_non_asl_code_descript_cvgtype,
	missing_majorperil_cvgtype,
	missing_reserve_ctgry_descript_cvgtype,
	o_coveragetype AS CoverageType,
	direct_loss_paid_including_recoveries,
	-- *INF*: SUM(direct_loss_paid_including_recoveries)
	SUM(direct_loss_paid_including_recoveries) AS LossTotal,
	direct_alae_paid_including_recoveries,
	-- *INF*: SUM(direct_alae_paid_including_recoveries)
	SUM(direct_alae_paid_including_recoveries) AS ExpenseTotal,
	direct_salvage_paid,
	-- *INF*: SUM(direct_salvage_paid)
	SUM(direct_salvage_paid) AS SalvageTotal,
	direct_subrogation_paid,
	-- *INF*: SUM(direct_subrogation_paid)
	SUM(direct_subrogation_paid) AS SubrogationTotal,
	o_claimant_full_name AS claimant_full_name,
	LegalIndicator,
	cause_of_loss_long_descript,
	jurisdiction_state_code,
	direct_loss_outstanding_excluding_recoveries,
	-- *INF*: SUM(direct_loss_outstanding_excluding_recoveries)
	SUM(direct_loss_outstanding_excluding_recoveries) AS LossReserveTotal,
	direct_alae_outstanding_excluding_recoveries,
	-- *INF*: SUM(direct_alae_outstanding_excluding_recoveries)
	SUM(direct_alae_outstanding_excluding_recoveries) AS ExpenseReserveTotal,
	ExAdjusterIndicator,
	out_GeneralMiscellaneous1 AS GeneralMiscellaneous1,
	GeneralMiscellaneous2,
	reserve_ctgry_descript AS GeneralMiscellaneous3,
	major_peril_descript AS GeneralMiscellaneous8,
	out_GeneralMiscellaneous9 AS GeneralMiscellaneous9,
	out_GeneralMiscellaneous10 AS GeneralMiscellaneous10,
	o_SupervisorCode AS SupervisorCode,
	O_Officecode AS OfficeCode,
	O_UnitName AS UnitName,
	O_Unitcode AS UnitCode,
	AuditId,
	major_peril_code,
	cause_of_loss,
	reserve_ctgry,
	direct_other_recovery_loss_outstanding,
	-- *INF*: SUM(direct_other_recovery_loss_outstanding)
	SUM(direct_other_recovery_loss_outstanding) AS o_direct_other_recovery_loss_outstanding,
	direct_other_recovery_alae_outstanding,
	-- *INF*: SUM(direct_other_recovery_alae_outstanding)
	SUM(direct_other_recovery_alae_outstanding) AS o_direct_other_recovery_alae_outstanding,
	direct_salvage_outstanding,
	-- *INF*: SUM(direct_salvage_outstanding)
	SUM(direct_salvage_outstanding) AS o_direct_salvage_outstanding,
	direct_subrogation_outstanding,
	-- *INF*: SUM(direct_subrogation_outstanding)
	SUM(direct_subrogation_outstanding) AS o_direct_subrogation_outstanding,
	ProductDescription
	FROM EXP_DeriveFields
	GROUP BY claim_num, subclaim
),
SRT_subclaims AS (
	SELECT
	claim_num, 
	m_d, 
	subclaim, 
	Claimtype, 
	missing_insurancesegment_cvgtype, 
	missing_sub_non_asl_code_descript_cvgtype, 
	missing_majorperil_cvgtype, 
	missing_reserve_ctgry_descript_cvgtype, 
	CoverageType, 
	status, 
	LossTotal, 
	ExpenseTotal, 
	SalvageTotal, 
	SubrogationTotal, 
	claimant_full_name, 
	LegalIndicator, 
	cause_of_loss_long_descript, 
	jurisdiction_state_code, 
	LossReserveTotal, 
	ExpenseReserveTotal, 
	SalvageIndicator, 
	SubrogationIndicator, 
	ExAdjusterIndicator, 
	GeneralMiscellaneous1, 
	GeneralMiscellaneous2, 
	GeneralMiscellaneous3 AS reserve_ctgry_descript, 
	GeneralMiscellaneous8 AS major_peril_descript, 
	SupervisorCode, 
	OfficeCode, 
	UnitName, 
	UnitCode, 
	GeneralMiscellaneous9, 
	GeneralMiscellaneous10, 
	AuditId, 
	o_direct_other_recovery_loss_outstanding, 
	o_direct_other_recovery_alae_outstanding, 
	o_direct_salvage_outstanding, 
	o_direct_subrogation_outstanding, 
	direct_salvage_paid, 
	direct_subrogation_paid, 
	direct_salvage_outstanding, 
	direct_subrogation_outstanding, 
	ProductDescription
	FROM AGG_SubClaims
	ORDER BY claim_num ASC
),
JNR_cce AS (SELECT
	SRT_subclaims.claim_num, 
	SRT_subclaims.m_d, 
	SRT_subclaims.subclaim, 
	SRT_subclaims.Claimtype, 
	SRT_subclaims.missing_insurancesegment_cvgtype, 
	SRT_subclaims.missing_sub_non_asl_code_descript_cvgtype, 
	SRT_subclaims.missing_majorperil_cvgtype, 
	SRT_subclaims.missing_reserve_ctgry_descript_cvgtype, 
	SRT_subclaims.CoverageType, 
	SRT_subclaims.status, 
	SRT_subclaims.LossTotal, 
	SRT_subclaims.ExpenseTotal, 
	SRT_subclaims.SalvageTotal, 
	SRT_subclaims.SubrogationTotal, 
	SRT_subclaims.claimant_full_name, 
	SRT_claims.LossIndemnity, 
	SRT_claims.LossMedical, 
	SRT_subclaims.LegalIndicator, 
	SRT_subclaims.cause_of_loss_long_descript, 
	SRT_subclaims.jurisdiction_state_code, 
	SRT_subclaims.LossReserveTotal, 
	SRT_subclaims.ExpenseReserveTotal, 
	SRT_subclaims.SalvageIndicator, 
	SRT_subclaims.SubrogationIndicator, 
	SRT_subclaims.ExAdjusterIndicator, 
	SRT_subclaims.GeneralMiscellaneous1, 
	SRT_subclaims.GeneralMiscellaneous2, 
	SRT_subclaims.reserve_ctgry_descript, 
	SRT_subclaims.major_peril_descript, 
	SRT_subclaims.SupervisorCode, 
	SRT_subclaims.OfficeCode, 
	SRT_subclaims.UnitName, 
	SRT_subclaims.UnitCode, 
	SRT_subclaims.GeneralMiscellaneous9, 
	SRT_subclaims.GeneralMiscellaneous10, 
	SRT_subclaims.AuditId, 
	SRT_claims.claim_num AS claim_num1, 
	SRT_claims.pol_num, 
	SRT_claims.claim_loss_date, 
	SRT_claims.claim_open_date, 
	SRT_claims.claim_close_date, 
	SRT_claims.claim_rep_full_name, 
	SRT_claims.claim_rep_num, 
	SRT_claims.handling_office_mgr, 
	SRT_claims.handling_office_descript, 
	SRT_claims.loss_loc_state, 
	SRT_claims.prdct_code_descript, 
	SRT_claims.strtgc_bus_dvsn_code AS strtgc_bus_dvsn_code_descript, 
	SRT_claims.claim_loss_descript, 
	SRT_claims.source_claim_rpted_date, 
	SRT_claims.pol_eff_date, 
	SRT_claims.pol_exp_date, 
	SRT_claims.LossMiscellaneous1, 
	SRT_claims.LossReserveMiscellaneous1, 
	SRT_claims.ExpenseMiscellaneous1, 
	SRT_claims.ExpenseReserveMiscellaneous1, 
	SRT_claims.claim_rep_email, 
	SRT_claims.handling_office_mgr_email, 
	SRT_claims.loss_loc_zip, 
	SRT_claims.CategoryIndicator, 
	SRT_claims.name, 
	SRT_claims.source_claim_occurence_status_code, 
	SRT_claims.claim_cat_code, 
	SRT_subclaims.o_direct_other_recovery_loss_outstanding, 
	SRT_subclaims.o_direct_other_recovery_alae_outstanding, 
	SRT_subclaims.o_direct_salvage_outstanding, 
	SRT_subclaims.o_direct_subrogation_outstanding, 
	SRT_subclaims.direct_salvage_paid, 
	SRT_subclaims.direct_subrogation_paid, 
	SRT_subclaims.direct_salvage_outstanding, 
	SRT_subclaims.direct_subrogation_outstanding, 
	SRT_subclaims.ProductDescription, 
	SRT_claims.CompanyName, 
	SRT_claims.AgencyCode, 
	SRT_claims.ClaimFeatureRepresentativeName, 
	SRT_claims.ClaimFeatureRepresentativeCode
	FROM SRT_subclaims
	LEFT OUTER JOIN SRT_claims
	ON SRT_claims.claim_num = SRT_subclaims.claim_num
),
EXP_passthrough AS (
	SELECT
	AuditId,
	m_d AS ModifiedDate,
	claim_num AS ClaimNumber,
	subclaim AS SubClaimId,
	Claimtype,
	missing_insurancesegment_cvgtype,
	missing_sub_non_asl_code_descript_cvgtype,
	missing_majorperil_cvgtype,
	missing_reserve_ctgry_descript_cvgtype,
	CoverageType,
	status AS Status,
	-- *INF*: IIF(LossReserveTotal=0 AND
	-- ExpenseReserveTotal=0 AND
	-- o_direct_salvage_outstanding=0 AND
	-- o_direct_subrogation_outstanding=0 AND
	-- o_direct_other_recovery_loss_outstanding=0 AND
	-- o_direct_other_recovery_alae_outstanding=0,'CLOSE','OPEN')
	IFF(
	    LossReserveTotal = 0
	    and ExpenseReserveTotal = 0
	    and o_direct_salvage_outstanding = 0
	    and o_direct_subrogation_outstanding = 0
	    and o_direct_other_recovery_loss_outstanding = 0
	    and o_direct_other_recovery_alae_outstanding = 0,
	    'CLOSE',
	    'OPEN'
	) AS o_Status,
	pol_num AS PolicyNumber,
	LossTotal,
	ExpenseTotal,
	SalvageTotal,
	SubrogationTotal,
	claim_loss_date AS LossDate,
	claim_open_date AS OpenedDate,
	claim_close_date AS ClosedDate,
	claim_rep_full_name AS AdjusterName,
	claim_rep_num AS AdjusterCode,
	SupervisorCode,
	handling_office_mgr AS SupervisorName,
	OfficeCode,
	handling_office_descript AS OfficeName,
	UnitName,
	UnitCode,
	claimant_full_name AS ClaimantName,
	LossIndemnity,
	LossMedical,
	LegalIndicator,
	loss_loc_state AS LossState,
	ProductDescription AS PolicyType,
	cause_of_loss_long_descript AS LossType,
	CompanyName,
	jurisdiction_state_code AS Jurisdiction,
	LossMiscellaneous1,
	LossReserveTotal,
	LossReserveMiscellaneous1,
	ExpenseMiscellaneous1,
	ExpenseReserveTotal,
	ExpenseReserveMiscellaneous1,
	claim_rep_email AS AdjusterEmail,
	handling_office_mgr_email AS SupervisorEmail,
	loss_loc_zip AS LossPostal,
	CategoryIndicator,
	name AS InsuredName,
	SalvageIndicator,
	-- *INF*: IIF (SalvageTotal < 0 OR o_direct_salvage_outstanding < 0 ,'Y','N')
	IFF(SalvageTotal < 0 OR o_direct_salvage_outstanding < 0, 'Y', 'N') AS o_SalvageIndicator,
	SubrogationIndicator,
	-- *INF*: IIF (SubrogationTotal < 0 OR o_direct_subrogation_outstanding < 0 ,'Y','N')  
	IFF(SubrogationTotal < 0 OR o_direct_subrogation_outstanding < 0, 'Y', 'N') AS o_SubrogationIndicator,
	ExAdjusterIndicator,
	GeneralMiscellaneous1,
	GeneralMiscellaneous2,
	reserve_ctgry_descript AS GeneralMiscellaneous3,
	source_claim_occurence_status_code AS GeneralMiscellaneous4,
	claim_cat_code AS GeneralMiscellaneous6,
	major_peril_descript AS GeneralMiscellaneous8,
	GeneralMiscellaneous9,
	GeneralMiscellaneous10,
	o_direct_other_recovery_loss_outstanding,
	o_direct_other_recovery_alae_outstanding,
	o_direct_salvage_outstanding,
	o_direct_subrogation_outstanding,
	direct_salvage_paid,
	direct_salvage_outstanding,
	direct_subrogation_paid,
	direct_subrogation_outstanding,
	'N/A' AS defaultValue1,
	AgencyCode,
	ClaimFeatureRepresentativeName,
	ClaimFeatureRepresentativeCode,
	claim_loss_descript,
	source_claim_rpted_date,
	pol_eff_date,
	pol_exp_date,
	o_direct_salvage_outstanding AS SalvageReserveAmount,
	o_direct_subrogation_outstanding AS SubrogationReserveAmount,
	ExpenseTotal + ExpenseReserveTotal AS ExpenseIncurred,
	LossTotal + LossReserveTotal AS LossIncurred,
	SalvageTotal + SalvageReserveAmount AS SalvageIncurred,
	SubrogationTotal + SubrogationReserveAmount AS SubrogationIncurred
	FROM JNR_cce
),
LKP_WorkClaimsCentersOfExpertiseExtract AS (
	SELECT
	WorkClaimsCentersOfExpertiseId,
	Claimtype,
	CoverageType,
	Status,
	PolicyNumber,
	LossTotal,
	ExpenseTotal,
	SalvageTotal,
	SubrogationTotal,
	LossDate,
	OpenedDate,
	ClosedDate,
	AdjusterName,
	AdjusterCode,
	SupervisorCode,
	SupervisorName,
	OfficeCode,
	OfficeName,
	UnitName,
	UnitCode,
	ClaimantName,
	LossIndemnity,
	LossMedical,
	LegalIndicator,
	LossState,
	PolicyType,
	LossType,
	CompanyName,
	Jurisdiction,
	LossMiscellaneous1,
	LossReserveTotal,
	LossReserveMiscellaneous1,
	ExpenseMiscellaneous1,
	ExpenseReserveTotal,
	ExpenseReserveMiscellaneous1,
	AdjusterEmail,
	SupervisorEmail,
	LossPostal,
	CategoryIndicator,
	InsuredName,
	GeneralMiscellaneous1,
	GeneralMiscellaneous3,
	GeneralMiscellaneous4,
	GeneralMiscellaneous6,
	GeneralMiscellaneous8,
	GeneralMiscellaneous9,
	GeneralMiscellaneous10,
	AgencyCode,
	ClaimFeatureRepresentativeCode,
	ClaimFeatureRepresentativeName,
	LossDescription,
	DateNotification,
	PolicyEffectiveDate,
	PolicyTerminationDate,
	ClaimNumber,
	SubClaimId
	FROM (
		SELECT WorkClaimsCentersOfExpertiseExtract.WorkClaimsCentersOfExpertiseExtractId AS WorkClaimsCentersOfExpertiseId,
			WorkClaimsCentersOfExpertiseExtract.Claimtype AS Claimtype,
			WorkClaimsCentersOfExpertiseExtract.CoverageType AS CoverageType,
			WorkClaimsCentersOfExpertiseExtract.STATUS AS STATUS,
			WorkClaimsCentersOfExpertiseExtract.PolicyNumber AS PolicyNumber,
			WorkClaimsCentersOfExpertiseExtract.LossTotal AS LossTotal,
			WorkClaimsCentersOfExpertiseExtract.ExpenseTotal AS ExpenseTotal,
			WorkClaimsCentersOfExpertiseExtract.SalvageTotal AS SalvageTotal,
			WorkClaimsCentersOfExpertiseExtract.SubrogationTotal AS SubrogationTotal,
			WorkClaimsCentersOfExpertiseExtract.LossDate AS LossDate,
			WorkClaimsCentersOfExpertiseExtract.OpenedDate AS OpenedDate,
			WorkClaimsCentersOfExpertiseExtract.ClosedDate AS ClosedDate,
			WorkClaimsCentersOfExpertiseExtract.AdjusterName AS AdjusterName,
			WorkClaimsCentersOfExpertiseExtract.AdjusterCode AS AdjusterCode,
			WorkClaimsCentersOfExpertiseExtract.SupervisorCode AS SupervisorCode,
			WorkClaimsCentersOfExpertiseExtract.SupervisorName AS SupervisorName,
			WorkClaimsCentersOfExpertiseExtract.OfficeCode AS OfficeCode,
			WorkClaimsCentersOfExpertiseExtract.OfficeName AS OfficeName,
			WorkClaimsCentersOfExpertiseExtract.UnitName AS UnitName,
			WorkClaimsCentersOfExpertiseExtract.UnitCode AS UnitCode,
			WorkClaimsCentersOfExpertiseExtract.ClaimantName AS ClaimantName,
			WorkClaimsCentersOfExpertiseExtract.LossIndemnity AS LossIndemnity,
			WorkClaimsCentersOfExpertiseExtract.LossMedical AS LossMedical,
			WorkClaimsCentersOfExpertiseExtract.LegalIndicator AS LegalIndicator,
			WorkClaimsCentersOfExpertiseExtract.LossState AS LossState,
			WorkClaimsCentersOfExpertiseExtract.PolicyType AS PolicyType,
			WorkClaimsCentersOfExpertiseExtract.LossType AS LossType,
			WorkClaimsCentersOfExpertiseExtract.CompanyName AS CompanyName,
			WorkClaimsCentersOfExpertiseExtract.Jurisdiction AS Jurisdiction,
			WorkClaimsCentersOfExpertiseExtract.LossMiscellaneous1 AS LossMiscellaneous1,
			WorkClaimsCentersOfExpertiseExtract.LossReserveTotal AS LossReserveTotal,
			WorkClaimsCentersOfExpertiseExtract.LossReserveMiscellaneous1 AS LossReserveMiscellaneous1,
			WorkClaimsCentersOfExpertiseExtract.ExpenseMiscellaneous1 AS ExpenseMiscellaneous1,
			WorkClaimsCentersOfExpertiseExtract.ExpenseReserveTotal AS ExpenseReserveTotal,
			WorkClaimsCentersOfExpertiseExtract.ExpenseReserveMiscellaneous1 AS ExpenseReserveMiscellaneous1,
			WorkClaimsCentersOfExpertiseExtract.AdjusterEmail AS AdjusterEmail,
			WorkClaimsCentersOfExpertiseExtract.SupervisorEmail AS SupervisorEmail,
			WorkClaimsCentersOfExpertiseExtract.LossPostal AS LossPostal,
			WorkClaimsCentersOfExpertiseExtract.CategoryIndicator AS CategoryIndicator,
			WorkClaimsCentersOfExpertiseExtract.InsuredName AS InsuredName,
			WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous1 AS GeneralMiscellaneous1,
			WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous3 AS GeneralMiscellaneous3,
			WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous4 AS GeneralMiscellaneous4,
			WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous6 AS GeneralMiscellaneous6,
			WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous8 AS GeneralMiscellaneous8,
			WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous9 AS GeneralMiscellaneous9,
			WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous10 AS GeneralMiscellaneous10,
			WorkClaimsCentersOfExpertiseExtract.AgencyCode AS AgencyCode,
			WorkClaimsCentersOfExpertiseExtract.ClaimNumber AS ClaimNumber,
			WorkClaimsCentersOfExpertiseExtract.SubClaimId AS SubClaimId,
			WorkClaimsCentersOfExpertiseExtract.ClaimFeatureRepresentativeCode AS ClaimFeatureRepresentativeCode,
			WorkClaimsCentersOfExpertiseExtract.ClaimFeatureRepresentativeName AS ClaimFeatureRepresentativeName
			, WorkClaimsCentersOfExpertiseExtract.LossDescription AS LossDescription
			, WorkClaimsCentersOfExpertiseExtract.DateNotification AS DateNotification
			, WorkClaimsCentersOfExpertiseExtract.PolicyEffectiveDate AS PolicyEffectiveDate
			, WorkClaimsCentersOfExpertiseExtract.PolicyTerminationDate AS PolicyTerminationDate
		FROM WorkClaimsCentersOfExpertiseExtract
		ORDER BY WorkClaimsCentersOfExpertiseExtract.ModifiedDate DESC --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimNumber,SubClaimId ORDER BY WorkClaimsCentersOfExpertiseId) = 1
),
EXP_Flag_Row AS (
	SELECT
	LKP_WorkClaimsCentersOfExpertiseExtract.WorkClaimsCentersOfExpertiseId AS LKP_WorkClaimsCentersOfExpertiseId,
	LKP_WorkClaimsCentersOfExpertiseExtract.Claimtype AS LKP_Claimtype,
	LKP_WorkClaimsCentersOfExpertiseExtract.CoverageType AS LKP_CoverageType,
	LKP_WorkClaimsCentersOfExpertiseExtract.Status AS LKP_PolicyNumber,
	LKP_WorkClaimsCentersOfExpertiseExtract.PolicyNumber AS LKP_LossTotal,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossTotal AS LKP_ExpenseTotal,
	LKP_WorkClaimsCentersOfExpertiseExtract.ExpenseTotal AS LKP_SalvageTotal,
	LKP_WorkClaimsCentersOfExpertiseExtract.SalvageTotal AS LKP_SubrogationTotal,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossDate AS LKP_LossDate,
	LKP_WorkClaimsCentersOfExpertiseExtract.OpenedDate AS LKP_OpenedDate,
	LKP_WorkClaimsCentersOfExpertiseExtract.ClosedDate AS LKP_ClosedDate,
	LKP_WorkClaimsCentersOfExpertiseExtract.AdjusterName AS LKP_AdjusterName,
	LKP_WorkClaimsCentersOfExpertiseExtract.AdjusterCode AS LKP_AdjusterCode,
	LKP_WorkClaimsCentersOfExpertiseExtract.SupervisorCode AS LKP_SupervisorCode,
	LKP_WorkClaimsCentersOfExpertiseExtract.SupervisorName AS LKP_SupervisorName,
	LKP_WorkClaimsCentersOfExpertiseExtract.OfficeCode AS LKP_OfficeCode,
	LKP_WorkClaimsCentersOfExpertiseExtract.OfficeName AS LKP_OfficeName,
	LKP_WorkClaimsCentersOfExpertiseExtract.UnitName AS LKP_UnitName,
	LKP_WorkClaimsCentersOfExpertiseExtract.UnitCode AS LKP_UnitCode,
	LKP_WorkClaimsCentersOfExpertiseExtract.ClaimantName AS LKP_ClaimantName,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossIndemnity AS LKP_LossIndemnity,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossMedical AS LKP_LossMedical,
	LKP_WorkClaimsCentersOfExpertiseExtract.LegalIndicator AS LKP_LegalIndicator,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossState AS LKP_LossState,
	LKP_WorkClaimsCentersOfExpertiseExtract.PolicyType AS LKP_PolicyType,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossType AS LKP_LossType,
	LKP_WorkClaimsCentersOfExpertiseExtract.CompanyName AS LKP_CompanyName,
	LKP_WorkClaimsCentersOfExpertiseExtract.Jurisdiction AS LKP_Jurisdiction,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossMiscellaneous1 AS LKP_LossMiscellaneous1,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossReserveTotal AS LKP_LossReserveTotal,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossReserveMiscellaneous1 AS LKP_LossReserveMiscellaneous1,
	LKP_WorkClaimsCentersOfExpertiseExtract.ExpenseMiscellaneous1 AS LKP_ExpenseMiscellaneous1,
	LKP_WorkClaimsCentersOfExpertiseExtract.ExpenseReserveTotal AS LKP_ExpenseReserveTotal,
	LKP_WorkClaimsCentersOfExpertiseExtract.ExpenseReserveMiscellaneous1 AS LKP_ExpenseReserveMiscellaneous1,
	LKP_WorkClaimsCentersOfExpertiseExtract.AdjusterEmail AS LKP_AdjusterEmail,
	LKP_WorkClaimsCentersOfExpertiseExtract.SupervisorEmail AS LKP_SupervisorEmail,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossPostal AS LKP_LossPostal,
	LKP_WorkClaimsCentersOfExpertiseExtract.CategoryIndicator AS LKP_CategoryIndicator,
	LKP_WorkClaimsCentersOfExpertiseExtract.InsuredName AS LKP_InsuredName,
	LKP_WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous1 AS LKP_GeneralMiscellaneous1,
	LKP_WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous3 AS LKP_GeneralMiscellaneous3,
	LKP_WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous4 AS LKP_GeneralMiscellaneous4,
	LKP_WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous6 AS LKP_GeneralMiscellaneous6,
	LKP_WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous8 AS LKP_GeneralMiscellaneous8,
	LKP_WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous9 AS LKP_GeneralMiscellaneous9,
	LKP_WorkClaimsCentersOfExpertiseExtract.GeneralMiscellaneous10 AS LKP_GeneralMiscellaneous10,
	LKP_WorkClaimsCentersOfExpertiseExtract.AgencyCode AS LKP_AgencyCode,
	LKP_WorkClaimsCentersOfExpertiseExtract.ClaimFeatureRepresentativeCode AS LKP_ClaimFeatureRepresentativeCode,
	LKP_WorkClaimsCentersOfExpertiseExtract.ClaimFeatureRepresentativeName AS LKP_ClaimFeatureRepresentativeName,
	LKP_WorkClaimsCentersOfExpertiseExtract.LossDescription AS LKP_LossDescription,
	LKP_WorkClaimsCentersOfExpertiseExtract.DateNotification AS LKP_DateNotification,
	LKP_WorkClaimsCentersOfExpertiseExtract.PolicyEffectiveDate AS LKP_PolicyEffectiveDate,
	LKP_WorkClaimsCentersOfExpertiseExtract.PolicyTerminationDate AS LKP_PolicyTerminationDate,
	EXP_passthrough.AuditId,
	EXP_passthrough.ModifiedDate,
	EXP_passthrough.ClaimNumber,
	EXP_passthrough.SubClaimId,
	EXP_passthrough.Claimtype,
	EXP_passthrough.missing_insurancesegment_cvgtype,
	EXP_passthrough.missing_sub_non_asl_code_descript_cvgtype,
	EXP_passthrough.missing_majorperil_cvgtype,
	EXP_passthrough.missing_reserve_ctgry_descript_cvgtype,
	EXP_passthrough.CoverageType,
	EXP_passthrough.o_Status AS Status,
	EXP_passthrough.PolicyNumber,
	EXP_passthrough.LossTotal,
	EXP_passthrough.ExpenseTotal,
	EXP_passthrough.SalvageTotal,
	EXP_passthrough.SubrogationTotal,
	EXP_passthrough.LossDate,
	EXP_passthrough.OpenedDate,
	EXP_passthrough.ClosedDate,
	EXP_passthrough.AdjusterName,
	EXP_passthrough.AdjusterCode,
	EXP_passthrough.SupervisorCode,
	EXP_passthrough.SupervisorName,
	EXP_passthrough.OfficeCode,
	EXP_passthrough.OfficeName,
	EXP_passthrough.UnitName,
	EXP_passthrough.UnitCode,
	EXP_passthrough.ClaimantName,
	EXP_passthrough.LossIndemnity,
	EXP_passthrough.LossMedical,
	EXP_passthrough.LegalIndicator,
	EXP_passthrough.LossState,
	EXP_passthrough.PolicyType,
	EXP_passthrough.LossType,
	EXP_passthrough.CompanyName,
	EXP_passthrough.Jurisdiction,
	EXP_passthrough.LossMiscellaneous1,
	EXP_passthrough.LossReserveTotal,
	EXP_passthrough.LossReserveMiscellaneous1,
	EXP_passthrough.ExpenseMiscellaneous1,
	EXP_passthrough.ExpenseReserveTotal AS ExpenseReserveTotal1,
	EXP_passthrough.ExpenseReserveMiscellaneous1,
	EXP_passthrough.AdjusterEmail,
	EXP_passthrough.SupervisorEmail,
	EXP_passthrough.LossPostal,
	EXP_passthrough.CategoryIndicator,
	EXP_passthrough.InsuredName,
	EXP_passthrough.o_SalvageIndicator AS SalvageIndicator,
	EXP_passthrough.o_SubrogationIndicator AS SubrogationIndicator,
	EXP_passthrough.ExAdjusterIndicator,
	EXP_passthrough.GeneralMiscellaneous1,
	EXP_passthrough.GeneralMiscellaneous2,
	EXP_passthrough.GeneralMiscellaneous3,
	EXP_passthrough.GeneralMiscellaneous4,
	EXP_passthrough.GeneralMiscellaneous6,
	EXP_passthrough.GeneralMiscellaneous8,
	EXP_passthrough.GeneralMiscellaneous9,
	EXP_passthrough.GeneralMiscellaneous10,
	EXP_passthrough.AgencyCode,
	EXP_passthrough.ClaimFeatureRepresentativeCode,
	EXP_passthrough.ClaimFeatureRepresentativeName,
	EXP_passthrough.claim_loss_descript,
	EXP_passthrough.source_claim_rpted_date,
	EXP_passthrough.pol_eff_date,
	EXP_passthrough.pol_exp_date,
	EXP_passthrough.defaultValue1,
	-- *INF*: IIF(ISNULL(LKP_WorkClaimsCentersOfExpertiseId),'INSERT',
	-- IIF(NOT ISNULL(LKP_WorkClaimsCentersOfExpertiseId) AND (LKP_CoverageType<>CoverageType OR LKP_PolicyNumber<>PolicyNumber OR LKP_Claimtype<>Claimtype OR
	-- 
	-- ROUND(abs(LKP_LossTotal-LossTotal ),2)>.001  OR 
	-- ROUND(abs(LKP_ExpenseTotal-ExpenseTotal ),2)>.001OR
	-- ROUND(abs(LKP_SalvageTotal-SalvageTotal ),2)>.001OR
	-- ROUND(abs(LKP_SubrogationTotal-SubrogationTotal ),2)>.001 OR
	-- 
	-- LKP_LossDate<>LossDate OR LKP_OpenedDate<>OpenedDate OR LKP_ClosedDate<>ClosedDate OR LKP_AdjusterName<>AdjusterName OR LKP_AdjusterCode<>AdjusterCode OR LKP_SupervisorCode<>SupervisorCode OR LKP_SupervisorName<>SupervisorName OR LKP_OfficeCode<>OfficeCode OR LKP_OfficeName<>OfficeName OR LKP_UnitName<>UnitName OR LKP_UnitCode<>UnitCode OR LKP_ClaimantName<>ClaimantName OR 
	-- 
	-- ROUND(abs(LKP_LossIndemnity-LossIndemnity ),2)>.001OR
	-- ROUND(abs(LKP_LossMedical-LossMedical ),2)>.001OR
	-- 
	-- LKP_LegalIndicator<>LegalIndicator OR LKP_LossState<>LossState OR LKP_PolicyType<>PolicyType OR LKP_LossType<>LossType OR LKP_CompanyName<>CompanyName OR LKP_Jurisdiction<>Jurisdiction OR
	-- 
	-- ROUND(abs(LKP_LossMiscellaneous1-LossMiscellaneous1 ),2)>.001 OR
	-- ROUND(abs(LKP_LossReserveTotal-LossReserveTotal ),2)>.001 OR
	-- ROUND(abs(LKP_LossReserveMiscellaneous1-LossReserveMiscellaneous1),2)>.001 OR
	-- ROUND(abs(LKP_ExpenseMiscellaneous1-ExpenseMiscellaneous1),2)>.001 OR
	-- ROUND(abs(LKP_ExpenseReserveTotal-ExpenseReserveTotal1),2)>.001OR
	-- ROUND(abs(LKP_ExpenseReserveMiscellaneous1-ExpenseReserveMiscellaneous1),2)>0.01 OR
	-- 
	-- LKP_AdjusterEmail<>AdjusterEmail OR LKP_SupervisorEmail<>SupervisorEmail OR LKP_LossPostal<>LossPostal OR LKP_CategoryIndicator<>CategoryIndicator OR LKP_InsuredName<>InsuredName OR LKP_GeneralMiscellaneous1<>GeneralMiscellaneous1 OR LKP_GeneralMiscellaneous3<>GeneralMiscellaneous3 OR LKP_GeneralMiscellaneous4<>GeneralMiscellaneous4 OR  LKP_GeneralMiscellaneous6<>GeneralMiscellaneous6 OR LKP_GeneralMiscellaneous8<>GeneralMiscellaneous8 OR LKP_GeneralMiscellaneous9<>GeneralMiscellaneous9 OR LKP_GeneralMiscellaneous10<>GeneralMiscellaneous10 OR LKP_AgencyCode<>AgencyCode OR LKP_ClaimFeatureRepresentativeCode<>ClaimFeatureRepresentativeCode OR LKP_ClaimFeatureRepresentativeName<>ClaimFeatureRepresentativeName OR
	-- LKP_LossDescription<>claim_loss_descript OR 
	-- LKP_DateNotification<>source_claim_rpted_date OR 
	-- LKP_PolicyEffectiveDate<>pol_eff_date OR 
	-- LKP_PolicyTerminationDate<>pol_exp_date
	-- ),'INSERT','NO CHANGE'))
	IFF(
	    LKP_WorkClaimsCentersOfExpertiseId IS NULL, 'INSERT',
	    IFF(
	        LKP_WorkClaimsCentersOfExpertiseId IS NULL
	        and (LKP_CoverageType <> CoverageType
	        or LKP_PolicyNumber <> PolicyNumber
	        or LKP_Claimtype <> Claimtype
	        or ROUND(abs(LKP_LossTotal - LossTotal), 2) > .001
	        or ROUND(abs(LKP_ExpenseTotal - ExpenseTotal), 2) > .001
	        or ROUND(abs(LKP_SalvageTotal - SalvageTotal), 2) > .001
	        or ROUND(abs(LKP_SubrogationTotal - SubrogationTotal), 2) > .001
	        or LKP_LossDate <> LossDate
	        or LKP_OpenedDate <> OpenedDate
	        or LKP_ClosedDate <> ClosedDate
	        or LKP_AdjusterName <> AdjusterName
	        or LKP_AdjusterCode <> AdjusterCode
	        or LKP_SupervisorCode <> SupervisorCode
	        or LKP_SupervisorName <> SupervisorName
	        or LKP_OfficeCode <> OfficeCode
	        or LKP_OfficeName <> OfficeName
	        or LKP_UnitName <> UnitName
	        or LKP_UnitCode <> UnitCode
	        or LKP_ClaimantName <> ClaimantName
	        or ROUND(abs(LKP_LossIndemnity - LossIndemnity), 2) > .001
	        or ROUND(abs(LKP_LossMedical - LossMedical), 2) > .001
	        or LKP_LegalIndicator <> LegalIndicator
	        or LKP_LossState <> LossState
	        or LKP_PolicyType <> PolicyType
	        or LKP_LossType <> LossType
	        or LKP_CompanyName <> CompanyName
	        or LKP_Jurisdiction <> Jurisdiction
	        or ROUND(abs(LKP_LossMiscellaneous1 - LossMiscellaneous1), 2) > .001
	        or ROUND(abs(LKP_LossReserveTotal - LossReserveTotal), 2) > .001
	        or ROUND(abs(LKP_LossReserveMiscellaneous1 - LossReserveMiscellaneous1), 2) > .001
	        or ROUND(abs(LKP_ExpenseMiscellaneous1 - ExpenseMiscellaneous1), 2) > .001
	        or ROUND(abs(LKP_ExpenseReserveTotal - ExpenseReserveTotal1), 2) > .001
	        or ROUND(abs(LKP_ExpenseReserveMiscellaneous1 - ExpenseReserveMiscellaneous1), 2) > 0.01
	        or LKP_AdjusterEmail <> AdjusterEmail
	        or LKP_SupervisorEmail <> SupervisorEmail
	        or LKP_LossPostal <> LossPostal
	        or LKP_CategoryIndicator <> CategoryIndicator
	        or LKP_InsuredName <> InsuredName
	        or LKP_GeneralMiscellaneous1 <> GeneralMiscellaneous1
	        or LKP_GeneralMiscellaneous3 <> GeneralMiscellaneous3
	        or LKP_GeneralMiscellaneous4 <> GeneralMiscellaneous4
	        or LKP_GeneralMiscellaneous6 <> GeneralMiscellaneous6
	        or LKP_GeneralMiscellaneous8 <> GeneralMiscellaneous8
	        or LKP_GeneralMiscellaneous9 <> GeneralMiscellaneous9
	        or LKP_GeneralMiscellaneous10 <> GeneralMiscellaneous10
	        or LKP_AgencyCode <> AgencyCode
	        or LKP_ClaimFeatureRepresentativeCode <> ClaimFeatureRepresentativeCode
	        or LKP_ClaimFeatureRepresentativeName <> ClaimFeatureRepresentativeName
	        or LKP_LossDescription <> claim_loss_descript
	        or LKP_DateNotification <> source_claim_rpted_date
	        or LKP_PolicyEffectiveDate <> pol_eff_date
	        or LKP_PolicyTerminationDate <> pol_exp_dNOT ate),
	        'INSERT',
	        'NO CHANGE'
	    )
	) AS Row_Flag,
	-- *INF*: IIF(ClaimNumber='N/A','IGNORE','INSERT')
	IFF(ClaimNumber = 'N/A', 'IGNORE', 'INSERT') AS NA_CLAIM_NO_FILTER,
	-- *INF*: IIF(LKP_Claimtype<>Claimtype,'TRUE','FALSE')
	IFF(LKP_Claimtype <> Claimtype, 'TRUE', 'FALSE') AS V_ClaimType,
	-- *INF*: IIF(LKP_CoverageType<>CoverageType,'TRUE','FALSE')
	IFF(LKP_CoverageType <> CoverageType, 'TRUE', 'FALSE') AS V_CoverageType,
	-- *INF*: IIF(LKP_PolicyNumber<>PolicyNumber,'TRUE','FALSE')
	IFF(LKP_PolicyNumber <> PolicyNumber, 'TRUE', 'FALSE') AS V_PolicyNumber,
	-- *INF*: ROUND(abs(LKP_LossTotal-LossTotal ),2)>.001
	ROUND(abs(LKP_LossTotal - LossTotal), 2) > .001 AS V_LossTotal,
	-- *INF*: ROUND(abs(LKP_ExpenseTotal-ExpenseTotal ),2)>.001
	ROUND(abs(LKP_ExpenseTotal - ExpenseTotal), 2) > .001 AS V_ExpenseTotal,
	-- *INF*: ROUND(abs(LKP_SalvageTotal-SalvageTotal ),2)>.001
	ROUND(abs(LKP_SalvageTotal - SalvageTotal), 2) > .001 AS V_SalvageTotal,
	-- *INF*: ROUND(abs(LKP_SubrogationTotal-SubrogationTotal ),2)>.001
	ROUND(abs(LKP_SubrogationTotal - SubrogationTotal), 2) > .001 AS V_SubrogationTotal,
	-- *INF*: IIF(LKP_LossDate<>LossDate,'TRUE','FALSE')
	IFF(LKP_LossDate <> LossDate, 'TRUE', 'FALSE') AS V_LossDate,
	-- *INF*: IIF(LKP_OpenedDate<>OpenedDate,'TRUE','FALSE')
	IFF(LKP_OpenedDate <> OpenedDate, 'TRUE', 'FALSE') AS V_OpenedDate,
	-- *INF*: IIF(LKP_ClosedDate<>ClosedDate,'TRUE','FALSE')
	IFF(LKP_ClosedDate <> ClosedDate, 'TRUE', 'FALSE') AS V_ClosedDate,
	-- *INF*: IIF(LKP_AdjusterName<>AdjusterName ,'TRUE','FALSE')
	IFF(LKP_AdjusterName <> AdjusterName, 'TRUE', 'FALSE') AS V_AdjusterName,
	-- *INF*: IIF(LKP_AdjusterCode<>AdjusterCode ,'TRUE','FALSE')
	IFF(LKP_AdjusterCode <> AdjusterCode, 'TRUE', 'FALSE') AS V_AdjusterCode,
	-- *INF*: IIF(LKP_SupervisorCode<>SupervisorCode,'TRUE','FALSE')
	IFF(LKP_SupervisorCode <> SupervisorCode, 'TRUE', 'FALSE') AS V_SupervisorCode,
	-- *INF*: IIF(LKP_SupervisorName<>SupervisorName,'TRUE','FALSE')
	IFF(LKP_SupervisorName <> SupervisorName, 'TRUE', 'FALSE') AS V_SuperVisorName,
	-- *INF*: IIF(LKP_OfficeCode<>OfficeCode,'TRUE','FALSE')
	IFF(LKP_OfficeCode <> OfficeCode, 'TRUE', 'FALSE') AS V_OfficeCode,
	-- *INF*: IIF(LKP_OfficeName<>OfficeName,'TRUE','FALSE')
	-- 
	-- 
	IFF(LKP_OfficeName <> OfficeName, 'TRUE', 'FALSE') AS V_Officename,
	-- *INF*: IIF( LKP_UnitName<>UnitName,'TRUE','FALSE')
	IFF(LKP_UnitName <> UnitName, 'TRUE', 'FALSE') AS V_Unitname,
	-- *INF*: IIF(LKP_UnitCode<>UnitCode,'TRUE','FALSE')
	IFF(LKP_UnitCode <> UnitCode, 'TRUE', 'FALSE') AS V_UnitCode,
	-- *INF*: IIF(LKP_ClaimantName<>ClaimantName,'TRUE','FALSE')
	IFF(LKP_ClaimantName <> ClaimantName, 'TRUE', 'FALSE') AS V_ClaimantName,
	-- *INF*: ROUND(abs(LKP_LossIndemnity-LossIndemnity ),2)>.001
	-- 
	ROUND(abs(LKP_LossIndemnity - LossIndemnity), 2) > .001 AS V_LossIndemnity,
	-- *INF*: ROUND(abs(LKP_LossMedical-LossMedical ),2)>.001
	ROUND(abs(LKP_LossMedical - LossMedical), 2) > .001 AS V_LossMedical,
	-- *INF*: IIF(LKP_LegalIndicator<>LegalIndicator,'TRUE','FALSE')
	IFF(LKP_LegalIndicator <> LegalIndicator, 'TRUE', 'FALSE') AS V_LegalIndicator,
	-- *INF*: IIF(LKP_LossState<>LossState,'TRUE','FALSE')
	IFF(LKP_LossState <> LossState, 'TRUE', 'FALSE') AS V_LossState,
	-- *INF*: IIF(LKP_PolicyType<>PolicyType,'TRUE','FALSE')
	IFF(LKP_PolicyType <> PolicyType, 'TRUE', 'FALSE') AS V_PolicyType,
	-- *INF*: IIF(LKP_LossType<>LossType,'TRUE','FALSE')
	IFF(LKP_LossType <> LossType, 'TRUE', 'FALSE') AS V_LossType,
	-- *INF*: IIF(LKP_CompanyName<>CompanyName,'TRUE','FALSE')
	IFF(LKP_CompanyName <> CompanyName, 'TRUE', 'FALSE') AS V_CompanyName,
	-- *INF*: IIf(LKP_Jurisdiction<>Jurisdiction,'TRUE','FALSE')
	IFF(LKP_Jurisdiction <> Jurisdiction, 'TRUE', 'FALSE') AS V_Jurisdiction,
	-- *INF*: ROUND(abs(LKP_LossMiscellaneous1-LossMiscellaneous1 ),2)>.001
	ROUND(abs(LKP_LossMiscellaneous1 - LossMiscellaneous1), 2) > .001 AS V_LossMiscellaneous1,
	-- *INF*: ROUND(abs(LKP_LossReserveTotal-LossReserveTotal ),2)>.001
	ROUND(abs(LKP_LossReserveTotal - LossReserveTotal), 2) > .001 AS V_LossReserveTotal,
	-- *INF*: ROUND(abs(LKP_LossReserveMiscellaneous1-LossReserveMiscellaneous1),2)>.001
	ROUND(abs(LKP_LossReserveMiscellaneous1 - LossReserveMiscellaneous1), 2) > .001 AS V_LossReserveMiscellaneous1,
	-- *INF*: ROUND(abs(LKP_ExpenseMiscellaneous1-ExpenseMiscellaneous1),2)>.001
	ROUND(abs(LKP_ExpenseMiscellaneous1 - ExpenseMiscellaneous1), 2) > .001 AS V_ExpenseMiscellaneous1,
	-- *INF*: ROUND(abs(LKP_ExpenseReserveTotal-ExpenseReserveTotal1),2)>.001
	ROUND(abs(LKP_ExpenseReserveTotal - ExpenseReserveTotal1), 2) > .001 AS V_ExpenseReserveTotal1,
	-- *INF*: ROUND(abs(LKP_ExpenseReserveMiscellaneous1-ExpenseReserveMiscellaneous1),2)>0.01
	ROUND(abs(LKP_ExpenseReserveMiscellaneous1 - ExpenseReserveMiscellaneous1), 2) > 0.01 AS V_ExpenseReserveMiscellaneous1,
	-- *INF*: IIF(LKP_AdjusterEmail<>AdjusterEmail,'TRUE','FALSE')
	IFF(LKP_AdjusterEmail <> AdjusterEmail, 'TRUE', 'FALSE') AS V_AdjusterEmail,
	-- *INF*: IIF(LKP_SupervisorEmail<>SupervisorEmail,'TRUE','FALSE')
	IFF(LKP_SupervisorEmail <> SupervisorEmail, 'TRUE', 'FALSE') AS V_SupervisorEmail,
	-- *INF*: IIF(LKP_LossPostal<>LossPostal,'TRUE','FALSE')
	IFF(LKP_LossPostal <> LossPostal, 'TRUE', 'FALSE') AS V_LossPostal,
	-- *INF*: IIf(LKP_CategoryIndicator<>CategoryIndicator,'TRUE','FALSE')
	IFF(LKP_CategoryIndicator <> CategoryIndicator, 'TRUE', 'FALSE') AS V_CategoryIndicator,
	-- *INF*: IIF(LKP_InsuredName<>InsuredName,'TRUE','FALSE')
	IFF(LKP_InsuredName <> InsuredName, 'TRUE', 'FALSE') AS V_InsuredName,
	-- *INF*: IIF(LKP_GeneralMiscellaneous1<>GeneralMiscellaneous1,'TRUE','FALSE')
	IFF(LKP_GeneralMiscellaneous1 <> GeneralMiscellaneous1, 'TRUE', 'FALSE') AS V_GeneralMiscellaneous1,
	-- *INF*: IIF(LKP_GeneralMiscellaneous3<>GeneralMiscellaneous3,'TRUE','FALSE')
	IFF(LKP_GeneralMiscellaneous3 <> GeneralMiscellaneous3, 'TRUE', 'FALSE') AS V_GeneralMiscellaneous3,
	-- *INF*: IIF(LKP_GeneralMiscellaneous4<>GeneralMiscellaneous4,'TRUE','FALSE')
	IFF(LKP_GeneralMiscellaneous4 <> GeneralMiscellaneous4, 'TRUE', 'FALSE') AS V_GeneralMiscellaneous4,
	-- *INF*: IIF(LKP_GeneralMiscellaneous6<>GeneralMiscellaneous6 ,'TRUE','FALSE')
	IFF(LKP_GeneralMiscellaneous6 <> GeneralMiscellaneous6, 'TRUE', 'FALSE') AS V_GeneralMiscellaneous6,
	-- *INF*: IIf(LKP_GeneralMiscellaneous8<>GeneralMiscellaneous8,'TRUE','FALSE')
	IFF(LKP_GeneralMiscellaneous8 <> GeneralMiscellaneous8, 'TRUE', 'FALSE') AS V_GeneralMiscellaneous8,
	-- *INF*: IIF(LKP_GeneralMiscellaneous9<>GeneralMiscellaneous9,'TRUE','FALSE')
	IFF(LKP_GeneralMiscellaneous9 <> GeneralMiscellaneous9, 'TRUE', 'FALSE') AS V_GeneralMiscellaneous9,
	-- *INF*: IIF(LKP_GeneralMiscellaneous10<>GeneralMiscellaneous10 ,'TRUE','FALSE')
	IFF(LKP_GeneralMiscellaneous10 <> GeneralMiscellaneous10, 'TRUE', 'FALSE') AS V_GeneralMiscellaneous10,
	-- *INF*: IIF(LKP_ClaimFeatureRepresentativeCode<>ClaimFeatureRepresentativeCode,'TRUE','FALSE')
	IFF(LKP_ClaimFeatureRepresentativeCode <> ClaimFeatureRepresentativeCode, 'TRUE', 'FALSE') AS V_ClaimFeatureRepresentativeCode,
	-- *INF*: IIF(LKP_ClaimFeatureRepresentativeName<>ClaimFeatureRepresentativeName,'TRUE','FALSE')
	IFF(LKP_ClaimFeatureRepresentativeName <> ClaimFeatureRepresentativeName, 'TRUE', 'FALSE') AS V_ClaimFeatureRepresentativeName,
	-- *INF*: IIF(LKP_AgencyCode<>AgencyCode ,'TRUE','FALSE')
	IFF(LKP_AgencyCode <> AgencyCode, 'TRUE', 'FALSE') AS V_AgencyCode,
	EXP_passthrough.SalvageReserveAmount,
	EXP_passthrough.SubrogationReserveAmount,
	EXP_passthrough.ExpenseIncurred,
	EXP_passthrough.LossIncurred,
	EXP_passthrough.SalvageIncurred,
	EXP_passthrough.SubrogationIncurred
	FROM EXP_passthrough
	LEFT JOIN LKP_WorkClaimsCentersOfExpertiseExtract
	ON LKP_WorkClaimsCentersOfExpertiseExtract.ClaimNumber = EXP_passthrough.ClaimNumber AND LKP_WorkClaimsCentersOfExpertiseExtract.SubClaimId = EXP_passthrough.SubClaimId
),
FIL_Inserts AS (
	SELECT
	AuditId, 
	ModifiedDate, 
	ClaimNumber, 
	SubClaimId, 
	Claimtype, 
	CoverageType, 
	Status, 
	PolicyNumber, 
	LossTotal, 
	ExpenseTotal, 
	SalvageTotal, 
	SubrogationTotal, 
	LossDate, 
	OpenedDate, 
	ClosedDate, 
	AdjusterName, 
	AdjusterCode, 
	SupervisorCode, 
	SupervisorName, 
	OfficeCode, 
	OfficeName, 
	UnitName, 
	UnitCode, 
	ClaimantName, 
	LossIndemnity, 
	LossMedical, 
	LegalIndicator, 
	LossState, 
	PolicyType, 
	LossType, 
	CompanyName, 
	Jurisdiction, 
	LossMiscellaneous1, 
	LossReserveTotal, 
	LossReserveMiscellaneous1, 
	ExpenseMiscellaneous1, 
	ExpenseReserveTotal1, 
	ExpenseReserveMiscellaneous1, 
	AdjusterEmail, 
	SupervisorEmail, 
	LossPostal, 
	CategoryIndicator, 
	InsuredName, 
	SalvageIndicator, 
	SubrogationIndicator, 
	ExAdjusterIndicator, 
	GeneralMiscellaneous1, 
	GeneralMiscellaneous2, 
	GeneralMiscellaneous3, 
	GeneralMiscellaneous4, 
	GeneralMiscellaneous6, 
	GeneralMiscellaneous8, 
	GeneralMiscellaneous9, 
	GeneralMiscellaneous10, 
	AgencyCode, 
	ClaimFeatureRepresentativeCode, 
	ClaimFeatureRepresentativeName, 
	claim_loss_descript, 
	source_claim_rpted_date, 
	pol_eff_date, 
	pol_exp_date, 
	defaultValue1, 
	Row_Flag, 
	NA_CLAIM_NO_FILTER, 
	SalvageReserveAmount, 
	SubrogationReserveAmount, 
	ExpenseIncurred, 
	LossIncurred, 
	SalvageIncurred, 
	SubrogationIncurred
	FROM EXP_Flag_Row
	WHERE Row_Flag='INSERT' AND NA_CLAIM_NO_FILTER='INSERT'
),
Shortcut_to_WorkClaimsCentersOfExpertiseExtract AS (
	INSERT INTO Shortcut_to_WorkClaimsCentersOfExpertiseExtract
	(AuditId, ModifiedDate, ClaimNumber, SubClaimId, Claimtype, CoverageType, Status, PolicyNumber, LossTotal, ExpenseTotal, SalvageTotal, SubrogationTotal, LossDate, OpenedDate, ClosedDate, AdjusterName, AdjusterCode, SupervisorCode, SupervisorName, OfficeCode, OfficeName, UnitName, UnitCode, ClaimantName, LossIndemnity, LossMedical, LegalIndicator, LossState, PolicyType, LossType, CompanyName, Jurisdiction, LossMiscellaneous1, LossReserveTotal, LossReserveMiscellaneous1, ExpenseMiscellaneous1, ExpenseReserveTotal, ExpenseReserveMiscellaneous1, AdjusterEmail, SupervisorEmail, LossPostal, CategoryIndicator, InsuredName, SalvageIndicator, SubrogationIndicator, ExAdjusterIndicator, GeneralMiscellaneous1, GeneralMiscellaneous2, GeneralMiscellaneous3, GeneralMiscellaneous4, GeneralMiscellaneous6, GeneralMiscellaneous8, GeneralMiscellaneous9, GeneralMiscellaneous10, AgencyCode, ClaimFeatureRepresentativeCode, ClaimFeatureRepresentativeName, LossDescription, DateNotification, PolicyEffectiveDate, PolicyTerminationDate, SalvageReserveAmount, SubrogationReserveAmount, ExpenseIncurred, LossIncurred, SalvageIncurred, SubrogationIncurred)
	SELECT 
	AUDITID, 
	MODIFIEDDATE, 
	CLAIMNUMBER, 
	SUBCLAIMID, 
	CLAIMTYPE, 
	COVERAGETYPE, 
	STATUS, 
	POLICYNUMBER, 
	LOSSTOTAL, 
	EXPENSETOTAL, 
	SALVAGETOTAL, 
	SUBROGATIONTOTAL, 
	LOSSDATE, 
	OPENEDDATE, 
	CLOSEDDATE, 
	ADJUSTERNAME, 
	ADJUSTERCODE, 
	SUPERVISORCODE, 
	SUPERVISORNAME, 
	OFFICECODE, 
	OFFICENAME, 
	UNITNAME, 
	UNITCODE, 
	CLAIMANTNAME, 
	LOSSINDEMNITY, 
	LOSSMEDICAL, 
	LEGALINDICATOR, 
	LOSSSTATE, 
	POLICYTYPE, 
	LOSSTYPE, 
	COMPANYNAME, 
	JURISDICTION, 
	LOSSMISCELLANEOUS1, 
	LOSSRESERVETOTAL, 
	LOSSRESERVEMISCELLANEOUS1, 
	EXPENSEMISCELLANEOUS1, 
	ExpenseReserveTotal1 AS EXPENSERESERVETOTAL, 
	EXPENSERESERVEMISCELLANEOUS1, 
	ADJUSTEREMAIL, 
	SUPERVISOREMAIL, 
	LOSSPOSTAL, 
	CATEGORYINDICATOR, 
	INSUREDNAME, 
	SALVAGEINDICATOR, 
	SUBROGATIONINDICATOR, 
	EXADJUSTERINDICATOR, 
	GENERALMISCELLANEOUS1, 
	GENERALMISCELLANEOUS2, 
	GENERALMISCELLANEOUS3, 
	GENERALMISCELLANEOUS4, 
	GENERALMISCELLANEOUS6, 
	GENERALMISCELLANEOUS8, 
	GENERALMISCELLANEOUS9, 
	GENERALMISCELLANEOUS10, 
	AGENCYCODE, 
	CLAIMFEATUREREPRESENTATIVECODE, 
	CLAIMFEATUREREPRESENTATIVENAME, 
	claim_loss_descript AS LOSSDESCRIPTION, 
	source_claim_rpted_date AS DATENOTIFICATION, 
	pol_eff_date AS POLICYEFFECTIVEDATE, 
	pol_exp_date AS POLICYTERMINATIONDATE, 
	SALVAGERESERVEAMOUNT, 
	SUBROGATIONRESERVEAMOUNT, 
	EXPENSEINCURRED, 
	LOSSINCURRED, 
	SALVAGEINCURRED, 
	SUBROGATIONINCURRED
	FROM FIL_Inserts
),