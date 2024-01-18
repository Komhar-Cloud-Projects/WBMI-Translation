WITH
LKP_calendar_dim AS (
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
LKP_InsuranceReferenceDim AS (
	SELECT
	InsuranceReferenceDimId,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	RatingPlanCode
	FROM (
		SELECT 
			InsuranceReferenceDimId,
			StrategicProfitCenterCode,
			InsuranceSegmentCode,
			PolicyOfferingCode,
			ProductCode,
			InsuranceReferenceLineOfBusinessCode,
			RatingPlanCode
		FROM InsuranceReferenceDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode ORDER BY InsuranceReferenceDimId DESC) = 1
),
LKP_strategic_business_division_dim AS (
	SELECT
	strtgc_bus_dvsn_dim_id,
	strtgc_bus_dvsn_code
	FROM (
		SELECT strategic_business_division_dim.strtgc_bus_dvsn_dim_id as strtgc_bus_dvsn_dim_id, strategic_business_division_dim.strtgc_bus_dvsn_code as strtgc_bus_dvsn_code FROM strategic_business_division_dim
		WHERE
		pol_sym_1 = 'N/A'
		
		--- We are filtering the records as we need to tie to DuckCreek Claims.
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY strtgc_bus_dvsn_code ORDER BY strtgc_bus_dvsn_dim_id DESC) = 1
),
SQ_claimant_coverage_dim AS (
	SELECT CCDim.claimant_cov_dim_id, CCDim.edw_claimant_cov_det_ak_id
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_dim CCDim
	WHERE CCDim.claimant_cov_dim_id NOT IN  (SELECT claimant_cov_dim_id FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact)
	AND CCDim.crrnt_snpsht_flag = 1 AND CCDim.claimant_cov_dim_id <> -1 AND CCDim.audit_id >0
),
EXP_get_values AS (
	SELECT
	claimant_cov_dim_id,
	edw_claimant_cov_det_ak_id,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS v_trans_date,
	v_trans_date AS trans_date
	FROM SQ_claimant_coverage_dim
),
LKP_claim_subrogation_dim AS (
	SELECT
	claim_subrogation_dim_id,
	referred_to_subrogation_date,
	pay_start_date,
	closure_date,
	edw_claimant_cov_det_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			claim_subrogation_dim_id,
			referred_to_subrogation_date,
			pay_start_date,
			closure_date,
			edw_claimant_cov_det_ak_id,
			eff_from_date,
			eff_to_date
		FROM claim_subrogation_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_subrogation_dim_id) = 1
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
EXP_Cov_Dim_Ids AS (
	SELECT
	EXP_get_values.claimant_cov_dim_id,
	mplt_coverage_dim_id.cov_dim_id,
	LKP_claim_subrogation_dim.claim_subrogation_dim_id,
	LKP_claim_subrogation_dim.referred_to_subrogation_date,
	LKP_claim_subrogation_dim.pay_start_date,
	LKP_claim_subrogation_dim.closure_date,
	EXP_get_values.edw_claimant_cov_det_ak_id,
	EXP_get_values.trans_date
	FROM EXP_get_values
	 -- Manually join with mplt_coverage_dim_id
	LEFT JOIN LKP_claim_subrogation_dim
	ON LKP_claim_subrogation_dim.edw_claimant_cov_det_ak_id = EXP_get_values.edw_claimant_cov_det_ak_id AND LKP_claim_subrogation_dim.eff_from_date <= EXP_get_values.trans_date AND LKP_claim_subrogation_dim.eff_to_date >= EXP_get_values.trans_date
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
EXP_Claimany_Dim_Ids AS (
	SELECT
	EXP_Cov_Dim_Ids.cov_dim_id,
	EXP_Cov_Dim_Ids.claim_subrogation_dim_id,
	EXP_Cov_Dim_Ids.referred_to_subrogation_date,
	EXP_Cov_Dim_Ids.pay_start_date,
	EXP_Cov_Dim_Ids.closure_date,
	EXP_Cov_Dim_Ids.trans_date,
	EXP_Cov_Dim_Ids.edw_claimant_cov_det_ak_id,
	EXP_Cov_Dim_Ids.claimant_cov_dim_id,
	mplt_Claimant_dim_id.claimant_dim_id
	FROM EXP_Cov_Dim_Ids
	 -- Manually join with mplt_Claimant_dim_id
),
LKP_InsuranceReferenceDimId AS (
	SELECT
	InsuranceReferenceDimId,
	StrategicProfitCenterCode,
	claimant_cov_det_ak_id
	FROM (
		SELECT CCD.claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
		IRD.InsuranceReferenceDimId AS InsuranceReferenceDimId,
		IRD.StrategicProfitCenterCode as StrategicProfitCenterCode
		FROM (
		select CCD.claimant_cov_det_ak_id,
		CCD.Claim_party_occurrence_ak_id,
		CCD.ProductAKId,
		CCD.InsuranceReferenceLineOfBusinessAKID,
		PC.RatingPlanAKId
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on CCD.StatisticalCoverageAKID=SC.StatisticalCoverageAKId
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on SC.PolicyCoverageAKId=PC.PolicyCoverageAKID
		where SC.SourceSystemId='PMS' and CCD.crrnt_snpsht_flag =1
		
		union all
		select CCD.claimant_cov_det_ak_id,
		CCD.Claim_party_occurrence_ak_id,
		CCD.ProductAKId,
		CCD.InsuranceReferenceLineOfBusinessAKID,
		PC.RatingPlanAKId
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on CCD.RatingCoverageAKID=RC.RatingCoverageAKId and RC.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RC.PolicyCoverageAKId=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
		where CCD.crrnt_snpsht_flag =1
		
		union all
		select CCD.claimant_cov_det_ak_id,
		CCD.Claim_party_occurrence_ak_id,
		CCD.ProductAKId,
		CCD.InsuranceReferenceLineOfBusinessAKID,
		null RatingPlanAKId
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		where CCD.crrnt_snpsht_flag =1 and CCD.StatisticalCoverageAKID=-1 and CCD.RatingCoverageAKID=-1
		 ) CCD
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
		on CCD.Claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id
		and CPO.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
		on CPO.Claim_Occurrence_ak_id = CO.claim_occurrence_ak_id
		and CO.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		on CO.pol_key_ak_id = P.pol_ak_id
		and P.crrnt_snpsht_flag = 1
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering PO
		on P.PolicyOfferingAKId = PO.PolicyOfferingAKID
		and PO.CurrentSnapshotFlag =1
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISe
		on P.InsuranceSegmentAKID = ISe.InsuranceSegmentAKID
		and ISe.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC
		on P.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId
		and SPC.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup EG
		on SPC.EnterPriseGroupId = EG.EnterpriseGroupId
		and EG.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity IRE
		on SPC.InsuranceReferenceLegalEntityId = IRE.InsuranceReferenceLegalEntityId
		and IRE.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Product Pr
		on CCD.ProductAKId = Pr.ProductAKId
		and Pr.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness IRLOB
		on CCD.InsuranceReferenceLineOfBusinessAKID = IRLOB.InsuranceReferenceLineOfBusinessAKId
		and IRLOB.CurrentSnapshotFlag =1
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
		on CCD.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim IRD
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
LKP_SalesDivisionDim AS (
	SELECT
	SalesDivisionDimID,
	AgencyAKID
	FROM (
		Select A.AgencyAKID AS AgencyAKID, 
		SDD.SalesDivisionDimID AS SalesDivisionDimID
		FROM 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency A,
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager RSM,@{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim SDD
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
SEQ_Dummy_Transactions AS (
	CREATE SEQUENCE SEQ_Dummy_Transactions
	START = 1
	INCREMENT = 1;
),
mplt_Strategic_Business_Division_Dim1 AS (WITH
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
EXP_Dim_ids AS (
	SELECT
	-1 AS default_dim_id,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'))
	LKP_CALENDAR_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_id AS v_Default_date_id,
	v_Default_date_id AS Default_date_id,
	mplt_Claim_occurence_dim_id.claim_occurrence_dim_id,
	-- *INF*: IIF(ISNULL(claim_occurrence_dim_id),-1,claim_occurrence_dim_id)
	IFF(claim_occurrence_dim_id IS NULL, - 1, claim_occurrence_dim_id) AS claim_occurrence_dim_id_OUT,
	EXP_Claimany_Dim_Ids.claimant_dim_id,
	-- *INF*: IIF(ISNULL(claimant_dim_id),-1,claimant_dim_id)
	IFF(claimant_dim_id IS NULL, - 1, claimant_dim_id) AS claimant_dim_id_OUT,
	EXP_Claimany_Dim_Ids.claimant_cov_dim_id,
	EXP_Claimany_Dim_Ids.cov_dim_id,
	-- *INF*: IIF(ISNULL(cov_dim_id),-1,cov_dim_id)
	IFF(cov_dim_id IS NULL, - 1, cov_dim_id) AS cov_dim_id_OUT,
	mplt_Claim_occurence_dim_id.pol_key_dim_id,
	-- *INF*: IIF(ISNULL(pol_key_dim_id),-1,pol_key_dim_id)
	IFF(pol_key_dim_id IS NULL, - 1, pol_key_dim_id) AS pol_key_dim_id_OUT,
	mplt_Claim_occurence_dim_id.contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(contract_cust_dim_id),-1,contract_cust_dim_id)
	IFF(contract_cust_dim_id IS NULL, - 1, contract_cust_dim_id) AS contract_cust_dim_id_out,
	mplt_Claim_occurence_dim_id.agency_dim_id,
	-- *INF*: IIF(ISNULL(agency_dim_id),-1,agency_dim_id)
	IFF(agency_dim_id IS NULL, - 1, agency_dim_id) AS agency_dim_id_OUT,
	mplt_Claim_occurence_dim_id.claim_loss_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_loss_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_loss_date_id), v_claim_loss_date_id, -1)
	IFF(v_claim_loss_date_id IS NOT NULL, v_claim_loss_date_id, - 1) AS claim_loss_date_id,
	mplt_Claim_occurence_dim_id.claim_discovery_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_discovery_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_discovery_date_id), v_claim_discovery_date_id, -1)
	IFF(v_claim_discovery_date_id IS NOT NULL, v_claim_discovery_date_id, - 1) AS claim_discovery_date_id,
	mplt_Claim_occurence_dim_id.claim_scripted_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_scripted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_scripted_date_id), v_claim_scripted_date_id, -1)
	IFF(v_claim_scripted_date_id IS NOT NULL, v_claim_scripted_date_id, - 1) AS claim_scripted_date_id,
	mplt_Claim_occurence_dim_id.source_claim_rpted_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_source_claim_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_source_claim_rpted_date_id), v_source_claim_rpted_date_id, -1)
	IFF(v_source_claim_rpted_date_id IS NOT NULL, v_source_claim_rpted_date_id, - 1) AS source_claim_rpted_date_id,
	mplt_Claim_occurence_dim_id.claim_occurrence_rpted_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_occurrence_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_occurrence_rpted_date_id), v_claim_occurrence_rpted_date_id, -1)
	IFF(v_claim_occurrence_rpted_date_id IS NOT NULL, v_claim_occurrence_rpted_date_id, - 1) AS claim_occurrence_rpted_date_id,
	mplt_Claim_occurence_dim_id.claim_open_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_open_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_open_date_id), v_claim_open_date_id, -1)
	IFF(v_claim_open_date_id IS NOT NULL, v_claim_open_date_id, - 1) AS claim_open_date_id,
	mplt_Claim_occurence_dim_id.claim_close_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_close_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_close_date_id), v_claim_close_date_id, -1)
	IFF(v_claim_close_date_id IS NOT NULL, v_claim_close_date_id, - 1) AS claim_close_date_id,
	mplt_Claim_occurence_dim_id.claim_reopen_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_reopen_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_reopen_date_id), v_claim_reopen_date_id, -1)
	IFF(v_claim_reopen_date_id IS NOT NULL, v_claim_reopen_date_id, - 1) AS claim_reopen_date_id,
	mplt_Claim_occurence_dim_id.claim_closed_after_reopen_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_closed_after_reopen_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_closed_after_reopen_date_id), v_claim_closed_after_reopen_date_id, -1)
	IFF(
	    v_claim_closed_after_reopen_date_id IS NOT NULL, v_claim_closed_after_reopen_date_id, - 1
	) AS claim_closed_after_reopen_date_id,
	mplt_Claim_occurence_dim_id.claim_notice_only_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_notice_only_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_notice_only_date_id), v_claim_notice_only_date_id, -1)
	IFF(v_claim_notice_only_date_id IS NOT NULL, v_claim_notice_only_date_id, - 1) AS claim_notice_only_date_id,
	mplt_Claim_occurence_dim_id.claim_cat_start_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_start_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_start_date_id), v_claim_cat_start_date_id, -1)
	IFF(v_claim_cat_start_date_id IS NOT NULL, v_claim_cat_start_date_id, - 1) AS claim_cat_start_date_id,
	mplt_Claim_occurence_dim_id.claim_cat_end_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_end_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_end_date_id), v_claim_cat_end_date_id, -1)
	IFF(v_claim_cat_end_date_id IS NOT NULL, v_claim_cat_end_date_id, - 1) AS claim_cat_end_date_id,
	mplt_Claim_occurence_dim_id.claim_rep_assigned_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_assigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_assigned_date_id), v_claim_rep_assigned_date_id, v_Default_date_id)
	IFF(
	    v_claim_rep_assigned_date_id IS NOT NULL, v_claim_rep_assigned_date_id, v_Default_date_id
	) AS claim_rep_assigned_date_id,
	mplt_Claim_occurence_dim_id.claim_rep_unassigned_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_unassigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_unassigned_date_id), v_claim_rep_unassigned_date_id, v_Default_date_id)
	IFF(
	    v_claim_rep_unassigned_date_id IS NOT NULL, v_claim_rep_unassigned_date_id,
	    v_Default_date_id
	) AS claim_rep_unassigned_date_id,
	mplt_Claim_occurence_dim_id.claim_rep_dim_prim_claim_rep_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_claim_rep_id),-1,claim_rep_dim_prim_claim_rep_id)
	IFF(claim_rep_dim_prim_claim_rep_id IS NULL, - 1, claim_rep_dim_prim_claim_rep_id) AS claim_rep_dim_prim_claim_rep_id_OUT,
	mplt_Claim_occurence_dim_id.claim_rep_dim_examiner_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_examiner_id),-1,claim_rep_dim_examiner_id)
	IFF(claim_rep_dim_examiner_id IS NULL, - 1, claim_rep_dim_examiner_id) AS claim_rep_dim_examiner_id_OUT,
	mplt_Claim_occurence_dim_id.claim_rep_dim_prim_litigation_handler_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_litigation_handler_id),-1,claim_rep_dim_prim_litigation_handler_id)
	IFF(
	    claim_rep_dim_prim_litigation_handler_id IS NULL, - 1,
	    claim_rep_dim_prim_litigation_handler_id
	) AS claim_rep_dim_prim_litigation_handler_id_OUT,
	mplt_Claim_occurence_dim_id.pol_eff_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_eff_date_id,
	-- *INF*: IIF(ISNULL(v_pol_eff_date_id),-1,v_pol_eff_date_id)
	IFF(v_pol_eff_date_id IS NULL, - 1, v_pol_eff_date_id) AS pol_eff_date_id,
	mplt_Claim_occurence_dim_id.pol_exp_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_exp_date_id,
	-- *INF*: IIF(ISNULL(v_pol_exp_date_id),-1,v_pol_exp_date_id)
	-- 
	-- 
	-- 
	IFF(v_pol_exp_date_id IS NULL, - 1, v_pol_exp_date_id) AS pol_exp_date_id,
	0 AS default_amt,
	-1 AS default_audit_id,
	'0000000000000000000000000' AS err_flag,
	-1 AS claim_rep_dim_trans_entry_oper_id,
	mplt_Claim_occurence_dim_id.AgencyDimID,
	-- *INF*: IIF(ISNULL(AgencyDimID),-1,AgencyDimID)
	IFF(AgencyDimID IS NULL, - 1, AgencyDimID) AS AgencyDimID_OUT,
	mplt_Claim_occurence_dim_id.claim_created_by_id,
	-- *INF*: IIF(ISNULL(claim_created_by_id),-1,claim_created_by_id)
	IFF(claim_created_by_id IS NULL, - 1, claim_created_by_id) AS claim_created_by_id_OUT,
	mplt_Claim_occurence_dim_id.claim_case_dim_id,
	-- *INF*: IIF(ISNULL(claim_case_dim_id),-1,claim_case_dim_id)
	IFF(claim_case_dim_id IS NULL, - 1, claim_case_dim_id) AS claim_case_dim_id_out,
	SEQ_Dummy_Transactions.NEXTVAL,
	-1 * NEXTVAL AS out_edw_claim_trans_pk_id,
	EXP_Claimany_Dim_Ids.claim_subrogation_dim_id,
	-- *INF*: IIF(ISNULL(claim_subrogation_dim_id), -1, claim_subrogation_dim_id)
	IFF(claim_subrogation_dim_id IS NULL, - 1, claim_subrogation_dim_id) AS claim_subrogation_dim_id_out,
	EXP_Claimany_Dim_Ids.referred_to_subrogation_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(referred_to_subrogation_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_referred_to_subrogation_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_referred_to_subrogation_date_id,
	-- *INF*: IIF(ISNULL(v_referred_to_subrogation_date_id), -1, v_referred_to_subrogation_date_id)
	IFF(v_referred_to_subrogation_date_id IS NULL, - 1, v_referred_to_subrogation_date_id) AS referred_to_subrogation_date_id,
	EXP_Claimany_Dim_Ids.pay_start_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(pay_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_pay_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_start_date_id,
	-- *INF*: IIF(ISNULL(v_pay_start_date_id), -1, v_pay_start_date_id)
	IFF(v_pay_start_date_id IS NULL, - 1, v_pay_start_date_id) AS pay_start_date_id,
	EXP_Claimany_Dim_Ids.closure_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(closure_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	-- 
	-- 
	LKP_CALENDAR_DIM_to_date_to_char_closure_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_closure_date_id,
	-- *INF*: IIF(ISNULL(v_closure_date_id), -1, v_closure_date_id)
	IFF(v_closure_date_id IS NULL, - 1, v_closure_date_id) AS closure_date_id,
	'N/A' AS default_string,
	mplt_Strategic_Business_Division_Dim1.strtgc_bus_dvsn_dim_id,
	mplt_Claim_occurence_dim_id.AgencyAKID,
	mplt_Claim_occurence_dim_id.SalesTerritoryAKID,
	mplt_Claim_occurence_dim_id.RegionalSalesManagerAKID,
	mplt_Claim_occurence_dim_id.SalesDirectorAKID,
	mplt_Claim_occurence_dim_id.StrategicProfitCenterAKId,
	mplt_Claim_occurence_dim_id.InsuranceSegmentAKId,
	mplt_Claim_occurence_dim_id.PolicyOfferingAKId,
	LKP_SalesDivisionDim.SalesDivisionDimID,
	-- *INF*: IIF(ISNULL(SalesDivisionDimID),-1,SalesDivisionDimID)
	IFF(SalesDivisionDimID IS NULL, - 1, SalesDivisionDimID) AS SalesDivisionDimID_out,
	mplt_Claim_occurence_dim_id.pol_sym,
	LKP_InsuranceReferenceDimId.InsuranceReferenceDimId AS LKP_InsuranceReferenceDimId,
	LKP_InsuranceReferenceDimId.StrategicProfitCenterCode AS LKP_StrategicProfitCenterCode,
	-- *INF*: :LKP.LKP_STRATEGIC_BUSINESS_DIVISION_DIM(LKP_StrategicProfitCenterCode)
	LKP_STRATEGIC_BUSINESS_DIVISION_DIM_LKP_StrategicProfitCenterCode.strtgc_bus_dvsn_dim_id AS v_strtgc_bus_dvsn_dim_id_DCT,
	-- *INF*: IIF(isnull(v_strtgc_bus_dvsn_dim_id_DCT),-1,v_strtgc_bus_dvsn_dim_id_DCT)
	IFF(v_strtgc_bus_dvsn_dim_id_DCT IS NULL, - 1, v_strtgc_bus_dvsn_dim_id_DCT) AS v_strtgc_bus_dvsn_dim_id_DCT_out,
	-- *INF*: IIF(pol_sym = '000',v_strtgc_bus_dvsn_dim_id_DCT_out,strtgc_bus_dvsn_dim_id)
	IFF(pol_sym = '000', v_strtgc_bus_dvsn_dim_id_DCT_out, strtgc_bus_dvsn_dim_id) AS strtgc_bus_dvsn_dim_id_out,
	-- *INF*: iif(isnull(LKP_InsuranceReferenceDimId),-1,LKP_InsuranceReferenceDimId)
	IFF(LKP_InsuranceReferenceDimId IS NULL, - 1, LKP_InsuranceReferenceDimId) AS v_InsuranceReferenceDimId_out,
	v_InsuranceReferenceDimId_out AS InsuranceReferenceDimId,
	SYSDATE AS ModifiedDate
	FROM EXP_Claimany_Dim_Ids
	 -- Manually join with mplt_Claim_occurence_dim_id
	 -- Manually join with mplt_Strategic_Business_Division_Dim1
	LEFT JOIN LKP_InsuranceReferenceDimId
	ON LKP_InsuranceReferenceDimId.claimant_cov_det_ak_id = EXP_Claimany_Dim_Ids.edw_claimant_cov_det_ak_id
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.AgencyAKID = mplt_Claim_occurence_dim_id.AgencyAKID
	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS
	ON LKP_CALENDAR_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_date = TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_referred_to_subrogation_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_referred_to_subrogation_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(referred_to_subrogation_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_pay_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_pay_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pay_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_closure_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_closure_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(closure_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_STRATEGIC_BUSINESS_DIVISION_DIM LKP_STRATEGIC_BUSINESS_DIVISION_DIM_LKP_StrategicProfitCenterCode
	ON LKP_STRATEGIC_BUSINESS_DIVISION_DIM_LKP_StrategicProfitCenterCode.strtgc_bus_dvsn_code = LKP_StrategicProfitCenterCode

),
claim_loss_transaction_fact_dummy_claimant_cov_transaction_row AS (
	INSERT INTO claim_loss_transaction_fact
	(err_flag, audit_id, edw_claim_trans_pk_id, edw_claim_reins_trans_pk_id, claim_occurrence_dim_id, claim_occurrence_dim_hist_id, claimant_dim_id, claimant_dim_hist_id, claimant_cov_dim_id, claimant_cov_dim_hist_id, cov_dim_id, cov_dim_hist_id, claim_trans_type_dim_id, claim_financial_type_dim_id, reins_cov_dim_id, reins_cov_dim_hist_id, claim_rep_dim_prim_claim_rep_id, claim_rep_dim_prim_claim_rep_hist_id, claim_rep_dim_examiner_id, claim_rep_dim_examiner_hist_id, claim_rep_dim_prim_litigation_handler_id, claim_rep_dim_prim_litigation_handler_hist_id, claim_rep_dim_trans_entry_oper_id, claim_rep_dim_trans_entry_oper_hist_id, claim_rep_dim_claim_created_by_id, pol_dim_id, pol_dim_hist_id, agency_dim_id, agency_dim_hist_id, claim_pay_dim_id, claim_pay_dim_hist_id, claim_pay_ctgry_type_dim_id, claim_pay_ctgry_type_dim_hist_id, claim_case_dim_id, claim_case_dim_hist_id, contract_cust_dim_id, contract_cust_dim_hist_id, claim_master_1099_list_dim_id, claim_subrogation_dim_id, claim_trans_date_id, claim_trans_reprocess_date_id, claim_loss_date_id, claim_discovery_date_id, claim_scripted_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_open_date_id, claim_close_date_id, claim_reopen_date_id, claim_closed_after_reopen_date_id, claim_notice_only_date_id, claim_cat_start_date_id, claim_cat_end_date_id, claim_rep_assigned_date_id, claim_rep_unassigned_date_id, pol_eff_date_id, pol_exp_date_id, claim_subrogation_referred_to_subrogation_date_id, claim_subrogation_pay_start_date_id, claim_subrogation_closure_date_id, acct_entered_date_id, trans_amt, trans_hist_amt, tax_id, direct_loss_paid_excluding_recoveries, direct_loss_outstanding_excluding_recoveries, direct_loss_incurred_excluding_recoveries, direct_alae_paid_excluding_recoveries, direct_alae_outstanding_excluding_recoveries, direct_alae_incurred_excluding_recoveries, direct_loss_paid_including_recoveries, direct_loss_outstanding_including_recoveries, direct_loss_incurred_including_recoveries, direct_alae_paid_including_recoveries, direct_alae_outstanding_including_recoveries, direct_alae_incurred_including_recoveries, direct_subrogation_paid, direct_subrogation_outstanding, direct_subrogation_incurred, direct_salvage_paid, direct_salvage_outstanding, direct_salvage_incurred, direct_other_recovery_loss_paid, direct_other_recovery_loss_outstanding, direct_other_recovery_loss_incurred, direct_other_recovery_alae_paid, direct_other_recovery_alae_outstanding, direct_other_recovery_alae_incurred, total_direct_loss_recovery_paid, total_direct_loss_recovery_outstanding, total_direct_loss_recovery_incurred, direct_other_recovery_paid, direct_other_recovery_outstanding, direct_other_recovery_incurred, ceded_loss_paid, ceded_loss_outstanding, ceded_loss_incurred, ceded_alae_paid, ceded_alae_outstanding, ceded_alae_incurred, ceded_salvage_paid, ceded_subrogation_paid, ceded_other_recovery_loss_paid, ceded_other_recovery_alae_paid, total_ceded_loss_recovery_paid, net_loss_paid, net_loss_outstanding, net_loss_incurred, net_alae_paid, net_alae_outstanding, net_alae_incurred, asl_dim_id, asl_prdct_code_dim_id, loss_master_dim_id, strtgc_bus_dvsn_dim_id, prdct_code_dim_id, ClaimReserveDimId, ClaimRepresentativeDimFeatureClaimRepresentativeId, FeatureRepresentativeAssignedDateId, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, ModifiedDate)
	SELECT 
	ERR_FLAG, 
	default_audit_id AS AUDIT_ID, 
	out_edw_claim_trans_pk_id AS EDW_CLAIM_TRANS_PK_ID, 
	out_edw_claim_trans_pk_id AS EDW_CLAIM_REINS_TRANS_PK_ID, 
	claim_occurrence_dim_id_OUT AS CLAIM_OCCURRENCE_DIM_ID, 
	claim_occurrence_dim_id_OUT AS CLAIM_OCCURRENCE_DIM_HIST_ID, 
	claimant_dim_id_OUT AS CLAIMANT_DIM_ID, 
	claimant_dim_id_OUT AS CLAIMANT_DIM_HIST_ID, 
	CLAIMANT_COV_DIM_ID, 
	claimant_cov_dim_id AS CLAIMANT_COV_DIM_HIST_ID, 
	cov_dim_id_OUT AS COV_DIM_ID, 
	cov_dim_id_OUT AS COV_DIM_HIST_ID, 
	default_dim_id AS CLAIM_TRANS_TYPE_DIM_ID, 
	default_dim_id AS CLAIM_FINANCIAL_TYPE_DIM_ID, 
	default_dim_id AS REINS_COV_DIM_ID, 
	default_dim_id AS REINS_COV_DIM_HIST_ID, 
	claim_rep_dim_prim_claim_rep_id_OUT AS CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	claim_rep_dim_prim_claim_rep_id_OUT AS CLAIM_REP_DIM_PRIM_CLAIM_REP_HIST_ID, 
	claim_rep_dim_examiner_id_OUT AS CLAIM_REP_DIM_EXAMINER_ID, 
	claim_rep_dim_examiner_id_OUT AS CLAIM_REP_DIM_EXAMINER_HIST_ID, 
	claim_rep_dim_prim_litigation_handler_id_OUT AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_ID, 
	claim_rep_dim_prim_litigation_handler_id_OUT AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_HIST_ID, 
	CLAIM_REP_DIM_TRANS_ENTRY_OPER_ID, 
	claim_rep_dim_trans_entry_oper_id AS CLAIM_REP_DIM_TRANS_ENTRY_OPER_HIST_ID, 
	claim_created_by_id_OUT AS CLAIM_REP_DIM_CLAIM_CREATED_BY_ID, 
	pol_key_dim_id_OUT AS POL_DIM_ID, 
	pol_key_dim_id_OUT AS POL_DIM_HIST_ID, 
	agency_dim_id_OUT AS AGENCY_DIM_ID, 
	AgencyDimID_OUT AS AGENCY_DIM_HIST_ID, 
	default_dim_id AS CLAIM_PAY_DIM_ID, 
	default_dim_id AS CLAIM_PAY_DIM_HIST_ID, 
	default_dim_id AS CLAIM_PAY_CTGRY_TYPE_DIM_ID, 
	default_dim_id AS CLAIM_PAY_CTGRY_TYPE_DIM_HIST_ID, 
	claim_case_dim_id_out AS CLAIM_CASE_DIM_ID, 
	claim_case_dim_id_out AS CLAIM_CASE_DIM_HIST_ID, 
	contract_cust_dim_id_out AS CONTRACT_CUST_DIM_ID, 
	contract_cust_dim_id_out AS CONTRACT_CUST_DIM_HIST_ID, 
	default_dim_id AS CLAIM_MASTER_1099_LIST_DIM_ID, 
	claim_subrogation_dim_id_out AS CLAIM_SUBROGATION_DIM_ID, 
	Default_date_id AS CLAIM_TRANS_DATE_ID, 
	Default_date_id AS CLAIM_TRANS_REPROCESS_DATE_ID, 
	CLAIM_LOSS_DATE_ID, 
	CLAIM_DISCOVERY_DATE_ID, 
	CLAIM_SCRIPTED_DATE_ID, 
	SOURCE_CLAIM_RPTED_DATE_ID, 
	claim_occurrence_rpted_date_id AS CLAIM_RPTED_DATE_ID, 
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
	referred_to_subrogation_date_id AS CLAIM_SUBROGATION_REFERRED_TO_SUBROGATION_DATE_ID, 
	pay_start_date_id AS CLAIM_SUBROGATION_PAY_START_DATE_ID, 
	closure_date_id AS CLAIM_SUBROGATION_CLOSURE_DATE_ID, 
	Default_date_id AS ACCT_ENTERED_DATE_ID, 
	default_amt AS TRANS_AMT, 
	default_amt AS TRANS_HIST_AMT, 
	default_dim_id AS TAX_ID, 
	default_amt AS DIRECT_LOSS_PAID_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_OUTSTANDING_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_INCURRED_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_PAID_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_OUTSTANDING_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_INCURRED_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_PAID_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_OUTSTANDING_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_INCURRED_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_PAID_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_OUTSTANDING_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_INCURRED_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_SUBROGATION_PAID, 
	default_amt AS DIRECT_SUBROGATION_OUTSTANDING, 
	default_amt AS DIRECT_SUBROGATION_INCURRED, 
	default_amt AS DIRECT_SALVAGE_PAID, 
	default_amt AS DIRECT_SALVAGE_OUTSTANDING, 
	default_amt AS DIRECT_SALVAGE_INCURRED, 
	default_amt AS DIRECT_OTHER_RECOVERY_LOSS_PAID, 
	default_amt AS DIRECT_OTHER_RECOVERY_LOSS_OUTSTANDING, 
	default_amt AS DIRECT_OTHER_RECOVERY_LOSS_INCURRED, 
	default_amt AS DIRECT_OTHER_RECOVERY_ALAE_PAID, 
	default_amt AS DIRECT_OTHER_RECOVERY_ALAE_OUTSTANDING, 
	default_amt AS DIRECT_OTHER_RECOVERY_ALAE_INCURRED, 
	default_amt AS TOTAL_DIRECT_LOSS_RECOVERY_PAID, 
	default_amt AS TOTAL_DIRECT_LOSS_RECOVERY_OUTSTANDING, 
	default_amt AS TOTAL_DIRECT_LOSS_RECOVERY_INCURRED, 
	default_amt AS DIRECT_OTHER_RECOVERY_PAID, 
	default_amt AS DIRECT_OTHER_RECOVERY_OUTSTANDING, 
	default_amt AS DIRECT_OTHER_RECOVERY_INCURRED, 
	default_amt AS CEDED_LOSS_PAID, 
	default_amt AS CEDED_LOSS_OUTSTANDING, 
	default_amt AS CEDED_LOSS_INCURRED, 
	default_amt AS CEDED_ALAE_PAID, 
	default_amt AS CEDED_ALAE_OUTSTANDING, 
	default_amt AS CEDED_ALAE_INCURRED, 
	default_amt AS CEDED_SALVAGE_PAID, 
	default_amt AS CEDED_SUBROGATION_PAID, 
	default_amt AS CEDED_OTHER_RECOVERY_LOSS_PAID, 
	default_amt AS CEDED_OTHER_RECOVERY_ALAE_PAID, 
	default_amt AS TOTAL_CEDED_LOSS_RECOVERY_PAID, 
	default_amt AS NET_LOSS_PAID, 
	default_amt AS NET_LOSS_OUTSTANDING, 
	default_amt AS NET_LOSS_INCURRED, 
	default_amt AS NET_ALAE_PAID, 
	default_amt AS NET_ALAE_OUTSTANDING, 
	default_amt AS NET_ALAE_INCURRED, 
	default_dim_id AS ASL_DIM_ID, 
	default_dim_id AS ASL_PRDCT_CODE_DIM_ID, 
	default_dim_id AS LOSS_MASTER_DIM_ID, 
	strtgc_bus_dvsn_dim_id_out AS STRTGC_BUS_DVSN_DIM_ID, 
	default_dim_id AS PRDCT_CODE_DIM_ID, 
	default_dim_id AS CLAIMRESERVEDIMID, 
	default_dim_id AS CLAIMREPRESENTATIVEDIMFEATURECLAIMREPRESENTATIVEID, 
	Default_date_id AS FEATUREREPRESENTATIVEASSIGNEDDATEID, 
	INSURANCEREFERENCEDIMID, 
	AgencyDimID_OUT AS AGENCYDIMID, 
	SalesDivisionDimID_out AS SALESDIVISIONDIMID, 
	default_dim_id AS INSURANCEREFERENCECOVERAGEDIMID, 
	default_dim_id AS COVERAGEDETAILDIMID, 
	MODIFIEDDATE
	FROM EXP_Dim_ids
),
SQ_claimant_dim AS (
	SELECT CD.claimant_dim_id, CD.edw_claim_party_occurrence_ak_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_dim CD, @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	WHERE claimant_dim_id NOT IN  (SELECT claimant_dim_id from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact)
	AND CD.crrnt_snpsht_flag = 1 AND  CPO.crrnt_snpsht_flag = 1 AND 
	edw_claim_party_occurrence_pk_id = CPO.claim_party_occurrence_id AND
	CPO.claim_party_role_code IN ('CLMT','CMT')  AND  CD.claimant_dim_id  <> -1
),
EXP_Claimant_dim_values AS (
	SELECT
	claimant_dim_id,
	edw_claim_party_occurrence_ak_id,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS v_trans_date,
	v_trans_date AS trans_date
	FROM SQ_claimant_dim
),
LKP_Claim_Party_Occurrence AS (
	SELECT
	claim_occurrence_ak_id,
	claim_case_ak_id,
	claim_party_occurrence_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			claim_party_occurrence.claim_occurrence_ak_id       AS claim_occurrence_ak_id,
		       claim_party_occurrence.claim_case_ak_id             AS claim_case_ak_id,
		       claim_party_occurrence.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id,
		       claim_party_occurrence.eff_from_date                AS eff_from_date,
		       claim_party_occurrence.eff_to_date                  AS eff_to_date
		FROM   claim_party_occurrence 
		WHERE  crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_ak_id) = 1
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
mplt_Claim_occurrence_CD AS (WITH
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
	LKP_claim_occurence_reserve_calc AS (
		SELECT
		claim_occurrence_reserve_calculation_id,
		claim_occurrence_ak_id,
		financial_type_code,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_reserve_calculation_id,
				claim_occurrence_ak_id,
				financial_type_code,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_reserve_calculation
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,financial_type_code,eff_from_date,eff_to_date ORDER BY claim_occurrence_reserve_calculation_id DESC) = 1
	),
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claim_occurrence_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_claim_occurrence_calc AS (
		SELECT
		claim_occurrence_calculation_id,
		claim_occurrence_reported_date,
		claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_calculation_id,
				claim_occurrence_reported_date,
				claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_calculation
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_calculation_id DESC) = 1
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
	LKP_claim_occurrence AS (
		SELECT
		claim_occurrence_id,
		pol_key,
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
				pol_key,
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
	LKP_V2_policy AS (
		SELECT
		pol_id,
		contract_cust_ak_id,
		agency_ak_id,
		pol_sym,
		pol_num,
		pol_eff_date,
		pol_exp_date,
		strtgc_bus_dvsn_ak_id,
		AgencyAKID,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		PolicyOfferingCode,
		pol_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			policy.pol_id as pol_id, 
			policy.contract_cust_ak_id as contract_cust_ak_id, 
			policy.agency_ak_id as agency_ak_id, 
			policy.pol_sym as pol_sym, 
			policy.pol_num as pol_num, 
			policy.pol_eff_date as pol_eff_date, 
			policy.pol_exp_date as pol_exp_date, 
			policy.strtgc_bus_dvsn_ak_id as strtgc_bus_dvsn_ak_id,
			policy.AgencyAKID as AgencyAKID,
			SPC.StrategicProfitCenterCode as StrategicProfitCenterCode, INSG.InsuranceSegmentCode as InsuranceSegmentCode, 
			PO.PolicyOfferingCode as PolicyOfferingCode, 
			policy.pol_key as pol_key, 
			policy.eff_from_date as eff_from_date, 
			policy.eff_to_date as eff_to_date 
			FROM 
			v2.policy policy,
			StrategicProfitCenter SPC,
			InsuranceSegment INSG,
			PolicyOffering PO
			WHERE 
			policy.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId and SPC.CurrentSnapshotFlag =  1
			and policy.InsuranceSegmentAKId = INSG.InsuranceSegmentAKId and INSG.CurrentSnapshotFlag = 1
			and policy.PolicyOfferingAKId = PO.PolicyOfferingAKId and PO.CurrentSnapshotFlag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,eff_from_date,eff_to_date ORDER BY pol_id DESC) = 1
	),
	EXP_get_reserve_calc_ids AS (
		SELECT
		LKP_claim_occurrence.claim_occurrence_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_id), -1, claim_occurrence_id)
		IFF(claim_occurrence_id IS NULL, - 1, claim_occurrence_id) AS claim_occurrence_id_out,
		-- *INF*: IIF(ISNULL(claim_rep_occurrence_id), -1, claim_rep_occurrence_id)
		IFF(claim_rep_occurrence_id IS NULL, - 1, claim_rep_occurrence_id) AS claim_rep_occurrence_id_out,
		LKP_claim_occurrence_calc.claim_occurrence_calculation_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_calculation_id), -1, claim_occurrence_calculation_id)
		IFF(claim_occurrence_calculation_id IS NULL, - 1, claim_occurrence_calculation_id) AS claim_occurrence_calculation_id_out,
		EXP_get_values.IN_claim_occurrence_ak_id AS claim_occurrence_ak_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_ak_id), -1, claim_occurrence_ak_id)
		IFF(claim_occurrence_ak_id IS NULL, - 1, claim_occurrence_ak_id) AS claim_occurrence_ak_id_out,
		EXP_get_values.IN_trans_date,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'D', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_direct_loss_id,
		-- *INF*: iif(isnull(claim_occurrence_reserve_calc_direct_loss_id), -1, claim_occurrence_reserve_calc_direct_loss_id)
		IFF(
		    claim_occurrence_reserve_calc_direct_loss_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_direct_loss_id
		) AS out_claim_occurrence_reserve_calc_direct_loss_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'E', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_exp_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_exp_id), -1, claim_occurrence_reserve_calc_exp_id)
		IFF(claim_occurrence_reserve_calc_exp_id IS NULL, - 1, claim_occurrence_reserve_calc_exp_id) AS out_claim_occurrence_reserve_calc_exp_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'B', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_subrogation_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_subrogation_id), -1, claim_occurrence_reserve_calc_subrogation_id)
		IFF(
		    claim_occurrence_reserve_calc_subrogation_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_subrogation_id
		) AS out_claim_occurrence_reserve_calc_subrogation_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'S', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_salvage_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_salvage_id), -1, claim_occurrence_reserve_calc_salvage_id)
		IFF(
		    claim_occurrence_reserve_calc_salvage_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_salvage_id
		) AS out_claim_occurrence_reserve_calc_salvage_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'R', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_recovery_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_recovery_id), -1, claim_occurrence_reserve_calc_recovery_id)
		IFF(
		    claim_occurrence_reserve_calc_recovery_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_recovery_id
		) AS out_claim_occurrence_reserve_calc_recovery_id,
		LKP_claim_occurrence_calc.claim_occurrence_reported_date AS claim_occurrence_rpted_date
		FROM EXP_get_values
		LEFT JOIN LKP_claim_occurrence
		ON LKP_claim_occurrence.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_claim_occurrence.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_calc
		ON LKP_claim_occurrence_calc.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_claim_occurrence_calc.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence_calc.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.financial_type_code = 'D'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.financial_type_code = 'E'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.financial_type_code = 'B'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.financial_type_code = 'S'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.financial_type_code = 'R'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.eff_from_date = IN_trans_date
	
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
	LKP_Agency_Key AS (
		SELECT
		agency_key,
		agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				agency_key,
				agency_ak_id,
				eff_from_date,
				eff_to_date
			FROM agency
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_key DESC) = 1
	),
	LKP_contract_customer AS (
		SELECT
		contract_cust_id,
		contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_id,
				contract_cust_ak_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_id DESC) = 1
	),
	LKP_Policy_Dim AS (
		SELECT
		pol_dim_id,
		edw_pol_pk_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pol_dim_id,
				edw_pol_pk_id,
				eff_from_date,
				eff_to_date
			FROM policy_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_pk_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
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
	EXP_Claim_Rep_Lkp_Values AS (
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
		ON LKP_Claim_Created_by_rep_ak_id.claim_rep_key = LKP_claim_occurrence.claim_created_by_key AND LKP_Claim_Created_by_rep_ak_id.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Created_by_rep_ak_id.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Examiner
		ON LKP_Claim_Rep_Occurrence_Examiner.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Examiner.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Examiner.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_PLH
		ON LKP_Claim_Rep_Occurrence_PLH.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_PLH.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_PLH.eff_to_date >= EXP_get_values.IN_trans_date
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
	LKP_agency_Dim AS (
		SELECT
		agency_dim_id,
		agency_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				agency_dim_id,
				agency_key,
				eff_from_date,
				eff_to_date
			FROM V2.agency_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_contract_customer_dim AS (
		SELECT
		contract_cust_dim_id,
		edw_contract_cust_pk_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_dim_id,
				edw_contract_cust_pk_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_pk_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		LKP_claim_occurrence_dim.claim_occurrence_dim_id, 
		LKP_claim_occurrence.claim_loss_date, 
		LKP_claim_occurrence.claim_discovery_date, 
		LKP_claim_occurrence_dim.claim_scripted_date, 
		LKP_claim_occurrence_dim.source_claim_rpted_date, 
		LKP_claim_occurrence_dim.claim_rpted_date AS claim_occurrence_rpted_date, 
		LKP_claim_occurrence_dim.claim_open_date, 
		LKP_claim_occurrence_dim.claim_close_date, 
		LKP_claim_occurrence_dim.claim_reopen_date, 
		LKP_claim_occurrence_dim.claim_closed_after_reopen_date, 
		LKP_claim_occurrence_dim.claim_notice_only_date, 
		LKP_claim_occurrence.claim_cat_start_date, 
		LKP_claim_occurrence.claim_cat_end_date, 
		LKP_Claim_Rep_Occurrence_Handler.claim_assigned_date AS claim_rep_assigned_date, 
		LKP_Claim_Rep_Occurrence_Handler.eff_to_date AS claim_rep_unassigned_date, 
		EXP_Claim_Rep_Lkp_Values.claim_rep_dim_prim_claim_rep_id, 
		EXP_Claim_Rep_Lkp_Values.claim_rep_dim_examiner_id, 
		EXP_Claim_Rep_Lkp_Values.claim_rep_dim_prim_litigation_handler_id, 
		LKP_Policy_Dim.pol_dim_id AS pol_key_dim_id, 
		LKP_V2_policy.pol_eff_date, 
		LKP_V2_policy.pol_exp_date, 
		LKP_agency_Dim.agency_dim_id, 
		EXP_Claim_Rep_Lkp_Values.claim_created_by_id, 
		LKP_contract_customer_dim.contract_cust_dim_id, 
		LKP_V2_policy.pol_sym, 
		LKP_V2_policy.pol_num, 
		LKP_V2_policy.strtgc_bus_dvsn_ak_id, 
		LKP_AgencyDim.AgencyDimID, 
		LKP_V2_policy.StrategicProfitCenterCode, 
		LKP_V2_policy.InsuranceSegmentCode, 
		LKP_V2_policy.PolicyOfferingCode
		FROM EXP_Claim_Rep_Lkp_Values
		LEFT JOIN LKP_AgencyDim
		ON LKP_AgencyDim.EDWAgencyAKID = LKP_V2_policy.AgencyAKID AND LKP_AgencyDim.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_AgencyDim.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Policy_Dim
		ON LKP_Policy_Dim.edw_pol_pk_id = LKP_V2_policy.pol_id AND LKP_Policy_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Policy_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_V2_policy
		ON LKP_V2_policy.pol_key = LKP_claim_occurrence.pol_key AND LKP_V2_policy.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_V2_policy.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_agency_Dim
		ON LKP_agency_Dim.agency_key = LKP_Agency_Key.agency_key AND LKP_agency_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_agency_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence
		ON LKP_claim_occurrence.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_claim_occurrence.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_dim
		ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = EXP_get_reserve_calc_ids.claim_occurrence_ak_id_out AND LKP_claim_occurrence_dim.eff_from_date <= EXP_get_reserve_calc_ids.IN_trans_date AND LKP_claim_occurrence_dim.eff_to_date >= EXP_get_reserve_calc_ids.IN_trans_date
		LEFT JOIN LKP_contract_customer_dim
		ON LKP_contract_customer_dim.edw_contract_cust_pk_id = LKP_contract_customer.contract_cust_id AND LKP_contract_customer_dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_contract_customer_dim.eff_to_date >= EXP_get_values.IN_trans_date
	),
),
mplt_Strategic_Business_Division_Dim2 AS (WITH
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
EXP_Dim_Ids_Claimant_Dim AS (
	SELECT
	-1 AS default_dim_id,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'))
	LKP_CALENDAR_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_id AS v_default_date_id,
	v_default_date_id AS Default_date_id,
	mplt_Claim_occurrence_CD.claim_occurrence_dim_id,
	-- *INF*: IIF(ISNULL(claim_occurrence_dim_id),-1,claim_occurrence_dim_id)
	IFF(claim_occurrence_dim_id IS NULL, - 1, claim_occurrence_dim_id) AS claim_occurrence_dim_id_OUT,
	EXP_Claimant_dim_values.claimant_dim_id,
	mplt_Claim_occurrence_CD.pol_key_dim_id,
	-- *INF*: IIF(ISNULL(pol_key_dim_id),-1,pol_key_dim_id)
	IFF(pol_key_dim_id IS NULL, - 1, pol_key_dim_id) AS pol_key_dim_id_OUT,
	mplt_Claim_occurrence_CD.contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(contract_cust_dim_id),-1,contract_cust_dim_id)
	IFF(contract_cust_dim_id IS NULL, - 1, contract_cust_dim_id) AS contract_cust_dim_id_out,
	mplt_Claim_occurrence_CD.agency_dim_id,
	-- *INF*: IIF(ISNULL(agency_dim_id),-1,agency_dim_id)
	IFF(agency_dim_id IS NULL, - 1, agency_dim_id) AS agency_dim_id_OUT,
	mplt_Claim_occurrence_CD.claim_loss_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_loss_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_loss_date_id), v_claim_loss_date_id, -1)
	IFF(v_claim_loss_date_id IS NOT NULL, v_claim_loss_date_id, - 1) AS claim_loss_date_id,
	mplt_Claim_occurrence_CD.claim_discovery_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_discovery_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_discovery_date_id), v_claim_discovery_date_id, -1)
	IFF(v_claim_discovery_date_id IS NOT NULL, v_claim_discovery_date_id, - 1) AS claim_discovery_date_id,
	mplt_Claim_occurrence_CD.claim_scripted_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_scripted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_scripted_date_id), v_claim_scripted_date_id, -1)
	IFF(v_claim_scripted_date_id IS NOT NULL, v_claim_scripted_date_id, - 1) AS claim_scripted_date_id,
	mplt_Claim_occurrence_CD.source_claim_rpted_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_source_claim_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_source_claim_rpted_date_id), v_source_claim_rpted_date_id, -1)
	IFF(v_source_claim_rpted_date_id IS NOT NULL, v_source_claim_rpted_date_id, - 1) AS source_claim_rpted_date_id,
	mplt_Claim_occurrence_CD.claim_occurrence_rpted_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_occurrence_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_occurrence_rpted_date_id), v_claim_occurrence_rpted_date_id, -1)
	IFF(v_claim_occurrence_rpted_date_id IS NOT NULL, v_claim_occurrence_rpted_date_id, - 1) AS claim_occurrence_rpted_date_id,
	mplt_Claim_occurrence_CD.claim_open_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_open_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_open_date_id),v_claim_open_date_id, -1)
	IFF(v_claim_open_date_id IS NOT NULL, v_claim_open_date_id, - 1) AS claim_open_date_id,
	mplt_Claim_occurrence_CD.claim_close_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_close_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_close_date_id), v_claim_close_date_id, -1)
	IFF(v_claim_close_date_id IS NOT NULL, v_claim_close_date_id, - 1) AS claim_close_date_id,
	mplt_Claim_occurrence_CD.claim_reopen_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_reopen_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_reopen_date_id), v_claim_reopen_date_id, -1)
	IFF(v_claim_reopen_date_id IS NOT NULL, v_claim_reopen_date_id, - 1) AS claim_reopen_date_id,
	mplt_Claim_occurrence_CD.claim_closed_after_reopen_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_closed_after_reopen_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_closed_after_reopen_date_id), v_claim_closed_after_reopen_date_id, -1)
	IFF(
	    v_claim_closed_after_reopen_date_id IS NOT NULL, v_claim_closed_after_reopen_date_id, - 1
	) AS claim_closed_after_reopen_date_id,
	mplt_Claim_occurrence_CD.claim_notice_only_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_notice_only_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_notice_only_date_id), v_claim_notice_only_date_id, -1)
	IFF(v_claim_notice_only_date_id IS NOT NULL, v_claim_notice_only_date_id, - 1) AS claim_notice_only_date_id,
	mplt_Claim_occurrence_CD.claim_cat_start_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_start_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_start_date_id), v_claim_cat_start_date_id, -1)
	IFF(v_claim_cat_start_date_id IS NOT NULL, v_claim_cat_start_date_id, - 1) AS claim_cat_start_date_id,
	mplt_Claim_occurrence_CD.claim_cat_end_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_end_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_end_date_id), v_claim_cat_end_date_id, -1)
	IFF(v_claim_cat_end_date_id IS NOT NULL, v_claim_cat_end_date_id, - 1) AS claim_cat_end_date_id,
	mplt_Claim_occurrence_CD.claim_rep_assigned_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_assigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_assigned_date_id), v_claim_rep_assigned_date_id, v_default_date_id)
	IFF(
	    v_claim_rep_assigned_date_id IS NOT NULL, v_claim_rep_assigned_date_id, v_default_date_id
	) AS claim_rep_assigned_date_id,
	mplt_Claim_occurrence_CD.claim_rep_unassigned_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_unassigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_unassigned_date_id), v_claim_rep_unassigned_date_id, v_default_date_id)
	IFF(
	    v_claim_rep_unassigned_date_id IS NOT NULL, v_claim_rep_unassigned_date_id,
	    v_default_date_id
	) AS claim_rep_unassigned_date_id,
	mplt_Claim_occurrence_CD.claim_rep_dim_prim_claim_rep_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_claim_rep_id),-1,claim_rep_dim_prim_claim_rep_id)
	IFF(claim_rep_dim_prim_claim_rep_id IS NULL, - 1, claim_rep_dim_prim_claim_rep_id) AS claim_rep_dim_prim_claim_rep_id_OUT,
	mplt_Claim_occurrence_CD.claim_rep_dim_examiner_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_examiner_id),-1,claim_rep_dim_examiner_id)
	IFF(claim_rep_dim_examiner_id IS NULL, - 1, claim_rep_dim_examiner_id) AS claim_rep_dim_examiner_id_OUT,
	mplt_Claim_occurrence_CD.claim_rep_dim_prim_litigation_handler_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_litigation_handler_id),-1,claim_rep_dim_prim_litigation_handler_id)
	IFF(
	    claim_rep_dim_prim_litigation_handler_id IS NULL, - 1,
	    claim_rep_dim_prim_litigation_handler_id
	) AS claim_rep_dim_prim_litigation_handler_id_OUT,
	mplt_Claim_occurrence_CD.pol_eff_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_eff_date_id,
	-- *INF*: IIF(ISNULL(v_pol_eff_date_id),-1,v_pol_eff_date_id)
	IFF(v_pol_eff_date_id IS NULL, - 1, v_pol_eff_date_id) AS pol_eff_date_id,
	mplt_Claim_occurrence_CD.pol_exp_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_exp_date_id,
	-- *INF*: IIF(ISNULL(v_pol_exp_date_id),-1,v_pol_exp_date_id)
	-- 
	-- 
	IFF(v_pol_exp_date_id IS NULL, - 1, v_pol_exp_date_id) AS pol_exp_date_id,
	0 AS default_amt,
	-2 AS default_audit_id,
	'0000000000000000000000000' AS err_flag,
	-1 AS claim_rep_dim_trans_entry_oper_id,
	mplt_Claim_occurrence_CD.claim_created_by_id,
	-- *INF*: IIF(ISNULL(claim_created_by_id),-1,claim_created_by_id)
	IFF(claim_created_by_id IS NULL, - 1, claim_created_by_id) AS claim_created_by_id_OUT,
	SEQ_Dummy_Transactions.NEXTVAL,
	-1 * NEXTVAL AS out_edw_claim_trans_pk_id,
	LKP_Claim_Case_Dim.claim_case_dim_id,
	-- *INF*: IIF(ISNULL(claim_case_dim_id), -1,claim_case_dim_id)
	IFF(claim_case_dim_id IS NULL, - 1, claim_case_dim_id) AS claim_case_dim_id_out,
	'N/A' AS default_string,
	mplt_Strategic_Business_Division_Dim2.strtgc_bus_dvsn_dim_id,
	mplt_Claim_occurrence_CD.pol_sym,
	mplt_Claim_occurrence_CD.strtgc_bus_dvsn_ak_id,
	mplt_Claim_occurrence_CD.StrategicProfitCenterCode,
	mplt_Claim_occurrence_CD.InsuranceSegmentCode,
	mplt_Claim_occurrence_CD.PolicyOfferingCode,
	'N/A' AS ProductCode,
	'N/A' AS InsuranceReferenceLineOfBusinessCode,
	'N/A' AS v_RatingPlanCode,
	-- *INF*: :LKP.LKP_STRATEGIC_BUSINESS_DIVISION_DIM(StrategicProfitCenterCode)
	LKP_STRATEGIC_BUSINESS_DIVISION_DIM_StrategicProfitCenterCode.strtgc_bus_dvsn_dim_id AS v_strtgc_bus_dvsn_dim_id_DCT,
	-- *INF*: IIF(isnull(v_strtgc_bus_dvsn_dim_id_DCT),-1,v_strtgc_bus_dvsn_dim_id_DCT)
	IFF(v_strtgc_bus_dvsn_dim_id_DCT IS NULL, - 1, v_strtgc_bus_dvsn_dim_id_DCT) AS v_strtgc_bus_dvsn_dim_id_DCT_out,
	-- *INF*: IIF(pol_sym = '000',v_strtgc_bus_dvsn_dim_id_DCT_out,strtgc_bus_dvsn_dim_id)
	IFF(pol_sym = '000', v_strtgc_bus_dvsn_dim_id_DCT_out, strtgc_bus_dvsn_dim_id) AS strtgc_bus_dvsn_dim_id_out,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCEDIM(StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode, v_RatingPlanCode)
	LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.InsuranceReferenceDimId AS v_InsuranceReferenceDimId,
	-- *INF*: iif(isnull(v_InsuranceReferenceDimId),-1,v_InsuranceReferenceDimId)
	IFF(v_InsuranceReferenceDimId IS NULL, - 1, v_InsuranceReferenceDimId) AS v_InsuranceReferenceDimId_out,
	v_InsuranceReferenceDimId_out AS InsuranceReferenceDimId,
	mplt_Claim_occurrence_CD.AgencyDimID,
	-- *INF*: iif(isnull(AgencyDimID),-1,AgencyDimID)
	IFF(AgencyDimID IS NULL, - 1, AgencyDimID) AS AgencyDimID_out,
	SYSDATE AS ModifiedDate
	FROM EXP_Claimant_dim_values
	 -- Manually join with mplt_Claim_occurrence_CD
	 -- Manually join with mplt_Strategic_Business_Division_Dim2
	LEFT JOIN LKP_Claim_Case_Dim
	ON LKP_Claim_Case_Dim.edw_claim_case_ak_id = LKP_Claim_Case.claim_case_ak_id AND LKP_Claim_Case_Dim.eff_from_date <= EXP_Claimant_dim_values.trans_date AND LKP_Claim_Case_Dim.eff_to_date >= EXP_Claimant_dim_values.trans_date
	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS
	ON LKP_CALENDAR_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_date = TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_STRATEGIC_BUSINESS_DIVISION_DIM LKP_STRATEGIC_BUSINESS_DIVISION_DIM_StrategicProfitCenterCode
	ON LKP_STRATEGIC_BUSINESS_DIVISION_DIM_StrategicProfitCenterCode.strtgc_bus_dvsn_code = StrategicProfitCenterCode

	LEFT JOIN LKP_INSURANCEREFERENCEDIM LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode
	ON LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.StrategicProfitCenterCode = StrategicProfitCenterCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.InsuranceSegmentCode = InsuranceSegmentCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.PolicyOfferingCode = PolicyOfferingCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.ProductCode = ProductCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.InsuranceReferenceLineOfBusinessCode = InsuranceReferenceLineOfBusinessCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.RatingPlanCode = v_RatingPlanCode

),
claim_loss_transaction_fact_dummy_claimant_transaction_row AS (
	INSERT INTO claim_loss_transaction_fact
	(err_flag, audit_id, edw_claim_trans_pk_id, edw_claim_reins_trans_pk_id, claim_occurrence_dim_id, claim_occurrence_dim_hist_id, claimant_dim_id, claimant_dim_hist_id, claimant_cov_dim_id, claimant_cov_dim_hist_id, cov_dim_id, cov_dim_hist_id, claim_trans_type_dim_id, claim_financial_type_dim_id, reins_cov_dim_id, reins_cov_dim_hist_id, claim_rep_dim_prim_claim_rep_id, claim_rep_dim_prim_claim_rep_hist_id, claim_rep_dim_examiner_id, claim_rep_dim_examiner_hist_id, claim_rep_dim_prim_litigation_handler_id, claim_rep_dim_prim_litigation_handler_hist_id, claim_rep_dim_trans_entry_oper_id, claim_rep_dim_trans_entry_oper_hist_id, claim_rep_dim_claim_created_by_id, pol_dim_id, pol_dim_hist_id, agency_dim_id, agency_dim_hist_id, claim_pay_dim_id, claim_pay_dim_hist_id, claim_pay_ctgry_type_dim_id, claim_pay_ctgry_type_dim_hist_id, claim_case_dim_id, claim_case_dim_hist_id, contract_cust_dim_id, contract_cust_dim_hist_id, claim_master_1099_list_dim_id, claim_subrogation_dim_id, claim_trans_date_id, claim_trans_reprocess_date_id, claim_loss_date_id, claim_discovery_date_id, claim_scripted_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_open_date_id, claim_close_date_id, claim_reopen_date_id, claim_closed_after_reopen_date_id, claim_notice_only_date_id, claim_cat_start_date_id, claim_cat_end_date_id, claim_rep_assigned_date_id, claim_rep_unassigned_date_id, pol_eff_date_id, pol_exp_date_id, claim_subrogation_referred_to_subrogation_date_id, claim_subrogation_pay_start_date_id, claim_subrogation_closure_date_id, acct_entered_date_id, trans_amt, trans_hist_amt, tax_id, direct_loss_paid_excluding_recoveries, direct_loss_outstanding_excluding_recoveries, direct_loss_incurred_excluding_recoveries, direct_alae_paid_excluding_recoveries, direct_alae_outstanding_excluding_recoveries, direct_alae_incurred_excluding_recoveries, direct_loss_paid_including_recoveries, direct_loss_outstanding_including_recoveries, direct_loss_incurred_including_recoveries, direct_alae_paid_including_recoveries, direct_alae_outstanding_including_recoveries, direct_alae_incurred_including_recoveries, direct_subrogation_paid, direct_subrogation_outstanding, direct_subrogation_incurred, direct_salvage_paid, direct_salvage_outstanding, direct_salvage_incurred, direct_other_recovery_loss_paid, direct_other_recovery_loss_outstanding, direct_other_recovery_loss_incurred, direct_other_recovery_alae_paid, direct_other_recovery_alae_outstanding, direct_other_recovery_alae_incurred, total_direct_loss_recovery_paid, total_direct_loss_recovery_outstanding, total_direct_loss_recovery_incurred, direct_other_recovery_paid, direct_other_recovery_outstanding, direct_other_recovery_incurred, ceded_loss_paid, ceded_loss_outstanding, ceded_loss_incurred, ceded_alae_paid, ceded_alae_outstanding, ceded_alae_incurred, ceded_salvage_paid, ceded_subrogation_paid, ceded_other_recovery_loss_paid, ceded_other_recovery_alae_paid, total_ceded_loss_recovery_paid, net_loss_paid, net_loss_outstanding, net_loss_incurred, net_alae_paid, net_alae_outstanding, net_alae_incurred, asl_dim_id, asl_prdct_code_dim_id, loss_master_dim_id, strtgc_bus_dvsn_dim_id, prdct_code_dim_id, ClaimReserveDimId, ClaimRepresentativeDimFeatureClaimRepresentativeId, FeatureRepresentativeAssignedDateId, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, ModifiedDate)
	SELECT 
	ERR_FLAG, 
	default_audit_id AS AUDIT_ID, 
	out_edw_claim_trans_pk_id AS EDW_CLAIM_TRANS_PK_ID, 
	out_edw_claim_trans_pk_id AS EDW_CLAIM_REINS_TRANS_PK_ID, 
	claim_occurrence_dim_id_OUT AS CLAIM_OCCURRENCE_DIM_ID, 
	claim_occurrence_dim_id_OUT AS CLAIM_OCCURRENCE_DIM_HIST_ID, 
	CLAIMANT_DIM_ID, 
	claimant_dim_id AS CLAIMANT_DIM_HIST_ID, 
	default_dim_id AS CLAIMANT_COV_DIM_ID, 
	default_dim_id AS CLAIMANT_COV_DIM_HIST_ID, 
	default_dim_id AS COV_DIM_ID, 
	default_dim_id AS COV_DIM_HIST_ID, 
	default_dim_id AS CLAIM_TRANS_TYPE_DIM_ID, 
	default_dim_id AS CLAIM_FINANCIAL_TYPE_DIM_ID, 
	default_dim_id AS REINS_COV_DIM_ID, 
	default_dim_id AS REINS_COV_DIM_HIST_ID, 
	claim_rep_dim_prim_claim_rep_id_OUT AS CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	claim_rep_dim_prim_claim_rep_id_OUT AS CLAIM_REP_DIM_PRIM_CLAIM_REP_HIST_ID, 
	claim_rep_dim_examiner_id_OUT AS CLAIM_REP_DIM_EXAMINER_ID, 
	claim_rep_dim_examiner_id_OUT AS CLAIM_REP_DIM_EXAMINER_HIST_ID, 
	claim_rep_dim_prim_litigation_handler_id_OUT AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_ID, 
	claim_rep_dim_prim_litigation_handler_id_OUT AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_HIST_ID, 
	CLAIM_REP_DIM_TRANS_ENTRY_OPER_ID, 
	claim_rep_dim_trans_entry_oper_id AS CLAIM_REP_DIM_TRANS_ENTRY_OPER_HIST_ID, 
	claim_created_by_id_OUT AS CLAIM_REP_DIM_CLAIM_CREATED_BY_ID, 
	pol_key_dim_id_OUT AS POL_DIM_ID, 
	pol_key_dim_id_OUT AS POL_DIM_HIST_ID, 
	agency_dim_id_OUT AS AGENCY_DIM_ID, 
	default_dim_id AS AGENCY_DIM_HIST_ID, 
	default_dim_id AS CLAIM_PAY_DIM_ID, 
	default_dim_id AS CLAIM_PAY_DIM_HIST_ID, 
	default_dim_id AS CLAIM_PAY_CTGRY_TYPE_DIM_ID, 
	default_dim_id AS CLAIM_PAY_CTGRY_TYPE_DIM_HIST_ID, 
	claim_case_dim_id_out AS CLAIM_CASE_DIM_ID, 
	claim_case_dim_id_out AS CLAIM_CASE_DIM_HIST_ID, 
	contract_cust_dim_id_out AS CONTRACT_CUST_DIM_ID, 
	contract_cust_dim_id_out AS CONTRACT_CUST_DIM_HIST_ID, 
	default_dim_id AS CLAIM_MASTER_1099_LIST_DIM_ID, 
	default_dim_id AS CLAIM_SUBROGATION_DIM_ID, 
	Default_date_id AS CLAIM_TRANS_DATE_ID, 
	Default_date_id AS CLAIM_TRANS_REPROCESS_DATE_ID, 
	CLAIM_LOSS_DATE_ID, 
	CLAIM_DISCOVERY_DATE_ID, 
	CLAIM_SCRIPTED_DATE_ID, 
	SOURCE_CLAIM_RPTED_DATE_ID, 
	claim_occurrence_rpted_date_id AS CLAIM_RPTED_DATE_ID, 
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
	Default_date_id AS CLAIM_SUBROGATION_REFERRED_TO_SUBROGATION_DATE_ID, 
	Default_date_id AS CLAIM_SUBROGATION_PAY_START_DATE_ID, 
	Default_date_id AS CLAIM_SUBROGATION_CLOSURE_DATE_ID, 
	Default_date_id AS ACCT_ENTERED_DATE_ID, 
	default_amt AS TRANS_AMT, 
	default_amt AS TRANS_HIST_AMT, 
	default_audit_id AS TAX_ID, 
	default_amt AS DIRECT_LOSS_PAID_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_OUTSTANDING_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_INCURRED_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_PAID_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_OUTSTANDING_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_INCURRED_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_PAID_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_OUTSTANDING_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_INCURRED_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_PAID_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_OUTSTANDING_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_INCURRED_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_SUBROGATION_PAID, 
	default_amt AS DIRECT_SUBROGATION_OUTSTANDING, 
	default_amt AS DIRECT_SUBROGATION_INCURRED, 
	default_amt AS DIRECT_SALVAGE_PAID, 
	default_amt AS DIRECT_SALVAGE_OUTSTANDING, 
	default_amt AS DIRECT_SALVAGE_INCURRED, 
	default_amt AS DIRECT_OTHER_RECOVERY_LOSS_PAID, 
	default_amt AS DIRECT_OTHER_RECOVERY_LOSS_OUTSTANDING, 
	default_amt AS DIRECT_OTHER_RECOVERY_LOSS_INCURRED, 
	default_amt AS DIRECT_OTHER_RECOVERY_ALAE_PAID, 
	default_amt AS DIRECT_OTHER_RECOVERY_ALAE_OUTSTANDING, 
	default_amt AS DIRECT_OTHER_RECOVERY_ALAE_INCURRED, 
	default_amt AS TOTAL_DIRECT_LOSS_RECOVERY_PAID, 
	default_amt AS TOTAL_DIRECT_LOSS_RECOVERY_OUTSTANDING, 
	default_amt AS TOTAL_DIRECT_LOSS_RECOVERY_INCURRED, 
	default_amt AS DIRECT_OTHER_RECOVERY_PAID, 
	default_amt AS DIRECT_OTHER_RECOVERY_OUTSTANDING, 
	default_amt AS DIRECT_OTHER_RECOVERY_INCURRED, 
	default_amt AS CEDED_LOSS_PAID, 
	default_amt AS CEDED_LOSS_OUTSTANDING, 
	default_amt AS CEDED_LOSS_INCURRED, 
	default_amt AS CEDED_ALAE_PAID, 
	default_amt AS CEDED_ALAE_OUTSTANDING, 
	default_amt AS CEDED_ALAE_INCURRED, 
	default_amt AS CEDED_SALVAGE_PAID, 
	default_amt AS CEDED_SUBROGATION_PAID, 
	default_amt AS CEDED_OTHER_RECOVERY_LOSS_PAID, 
	default_amt AS CEDED_OTHER_RECOVERY_ALAE_PAID, 
	default_amt AS TOTAL_CEDED_LOSS_RECOVERY_PAID, 
	default_amt AS NET_LOSS_PAID, 
	default_amt AS NET_LOSS_OUTSTANDING, 
	default_amt AS NET_LOSS_INCURRED, 
	default_amt AS NET_ALAE_PAID, 
	default_amt AS NET_ALAE_OUTSTANDING, 
	default_amt AS NET_ALAE_INCURRED, 
	default_dim_id AS ASL_DIM_ID, 
	default_dim_id AS ASL_PRDCT_CODE_DIM_ID, 
	default_dim_id AS LOSS_MASTER_DIM_ID, 
	strtgc_bus_dvsn_dim_id_out AS STRTGC_BUS_DVSN_DIM_ID, 
	default_dim_id AS PRDCT_CODE_DIM_ID, 
	default_dim_id AS CLAIMRESERVEDIMID, 
	default_dim_id AS CLAIMREPRESENTATIVEDIMFEATURECLAIMREPRESENTATIVEID, 
	Default_date_id AS FEATUREREPRESENTATIVEASSIGNEDDATEID, 
	INSURANCEREFERENCEDIMID, 
	AgencyDimID_out AS AGENCYDIMID, 
	default_dim_id AS SALESDIVISIONDIMID, 
	default_dim_id AS INSURANCEREFERENCECOVERAGEDIMID, 
	default_dim_id AS COVERAGEDETAILDIMID, 
	MODIFIEDDATE
	FROM EXP_Dim_Ids_Claimant_Dim
),
SQ_claim_occurrence_dim AS (
	SELECT claim_occurrence_dim.claim_occurrence_dim_id, claim_occurrence_dim.edw_claim_occurrence_ak_id 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_dim
	WHERE claim_occurrence_dim_id NOT IN (
	SELECT claim_occurrence_dim_id FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact )
	AND crrnt_snpsht_flag = 1 AND claim_occurrence_dim_id <> -1
),
EXP_Claim_Occurrence_dim_Values AS (
	SELECT
	claim_occurrence_dim_id,
	edw_claim_occurrence_ak_id,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS v_trans_date,
	v_trans_date AS trans_date
	FROM SQ_claim_occurrence_dim
),
mplt_Claim_occurrence_COD AS (WITH
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
	LKP_claim_occurence_reserve_calc AS (
		SELECT
		claim_occurrence_reserve_calculation_id,
		claim_occurrence_ak_id,
		financial_type_code,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_reserve_calculation_id,
				claim_occurrence_ak_id,
				financial_type_code,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_reserve_calculation
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,financial_type_code,eff_from_date,eff_to_date ORDER BY claim_occurrence_reserve_calculation_id DESC) = 1
	),
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claim_occurrence_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_claim_occurrence_calc AS (
		SELECT
		claim_occurrence_calculation_id,
		claim_occurrence_reported_date,
		claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_calculation_id,
				claim_occurrence_reported_date,
				claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_calculation
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_calculation_id DESC) = 1
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
	LKP_claim_occurrence AS (
		SELECT
		claim_occurrence_id,
		pol_key,
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
				pol_key,
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
	LKP_V2_policy AS (
		SELECT
		pol_id,
		contract_cust_ak_id,
		agency_ak_id,
		pol_sym,
		pol_num,
		pol_eff_date,
		pol_exp_date,
		strtgc_bus_dvsn_ak_id,
		AgencyAKID,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		PolicyOfferingCode,
		pol_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			policy.pol_id as pol_id, 
			policy.contract_cust_ak_id as contract_cust_ak_id, 
			policy.agency_ak_id as agency_ak_id, 
			policy.pol_sym as pol_sym, 
			policy.pol_num as pol_num, 
			policy.pol_eff_date as pol_eff_date, 
			policy.pol_exp_date as pol_exp_date, 
			policy.strtgc_bus_dvsn_ak_id as strtgc_bus_dvsn_ak_id,
			policy.AgencyAKID as AgencyAKID,
			SPC.StrategicProfitCenterCode as StrategicProfitCenterCode, INSG.InsuranceSegmentCode as InsuranceSegmentCode, 
			PO.PolicyOfferingCode as PolicyOfferingCode, 
			policy.pol_key as pol_key, 
			policy.eff_from_date as eff_from_date, 
			policy.eff_to_date as eff_to_date 
			FROM 
			v2.policy policy,
			StrategicProfitCenter SPC,
			InsuranceSegment INSG,
			PolicyOffering PO
			WHERE 
			policy.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId and SPC.CurrentSnapshotFlag =  1
			and policy.InsuranceSegmentAKId = INSG.InsuranceSegmentAKId and INSG.CurrentSnapshotFlag = 1
			and policy.PolicyOfferingAKId = PO.PolicyOfferingAKId and PO.CurrentSnapshotFlag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,eff_from_date,eff_to_date ORDER BY pol_id DESC) = 1
	),
	EXP_get_reserve_calc_ids AS (
		SELECT
		LKP_claim_occurrence.claim_occurrence_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_id), -1, claim_occurrence_id)
		IFF(claim_occurrence_id IS NULL, - 1, claim_occurrence_id) AS claim_occurrence_id_out,
		-- *INF*: IIF(ISNULL(claim_rep_occurrence_id), -1, claim_rep_occurrence_id)
		IFF(claim_rep_occurrence_id IS NULL, - 1, claim_rep_occurrence_id) AS claim_rep_occurrence_id_out,
		LKP_claim_occurrence_calc.claim_occurrence_calculation_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_calculation_id), -1, claim_occurrence_calculation_id)
		IFF(claim_occurrence_calculation_id IS NULL, - 1, claim_occurrence_calculation_id) AS claim_occurrence_calculation_id_out,
		EXP_get_values.IN_claim_occurrence_ak_id AS claim_occurrence_ak_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_ak_id), -1, claim_occurrence_ak_id)
		IFF(claim_occurrence_ak_id IS NULL, - 1, claim_occurrence_ak_id) AS claim_occurrence_ak_id_out,
		EXP_get_values.IN_trans_date,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'D', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_direct_loss_id,
		-- *INF*: iif(isnull(claim_occurrence_reserve_calc_direct_loss_id), -1, claim_occurrence_reserve_calc_direct_loss_id)
		IFF(
		    claim_occurrence_reserve_calc_direct_loss_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_direct_loss_id
		) AS out_claim_occurrence_reserve_calc_direct_loss_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'E', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_exp_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_exp_id), -1, claim_occurrence_reserve_calc_exp_id)
		IFF(claim_occurrence_reserve_calc_exp_id IS NULL, - 1, claim_occurrence_reserve_calc_exp_id) AS out_claim_occurrence_reserve_calc_exp_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'B', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_subrogation_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_subrogation_id), -1, claim_occurrence_reserve_calc_subrogation_id)
		IFF(
		    claim_occurrence_reserve_calc_subrogation_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_subrogation_id
		) AS out_claim_occurrence_reserve_calc_subrogation_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'S', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_salvage_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_salvage_id), -1, claim_occurrence_reserve_calc_salvage_id)
		IFF(
		    claim_occurrence_reserve_calc_salvage_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_salvage_id
		) AS out_claim_occurrence_reserve_calc_salvage_id,
		-- *INF*: :LKP.LKP_CLAIM_OCCURENCE_RESERVE_CALC(claim_occurrence_ak_id, 'R', IN_trans_date)
		LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.claim_occurrence_reserve_calculation_id AS claim_occurrence_reserve_calc_recovery_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_reserve_calc_recovery_id), -1, claim_occurrence_reserve_calc_recovery_id)
		IFF(
		    claim_occurrence_reserve_calc_recovery_id IS NULL, - 1,
		    claim_occurrence_reserve_calc_recovery_id
		) AS out_claim_occurrence_reserve_calc_recovery_id,
		LKP_claim_occurrence_calc.claim_occurrence_reported_date AS claim_occurrence_rpted_date
		FROM EXP_get_values
		LEFT JOIN LKP_claim_occurrence
		ON LKP_claim_occurrence.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_claim_occurrence.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_calc
		ON LKP_claim_occurrence_calc.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_claim_occurrence_calc.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence_calc.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.financial_type_code = 'D'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_D_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.financial_type_code = 'E'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_E_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.financial_type_code = 'B'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_B_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.financial_type_code = 'S'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_S_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_OCCURENCE_RESERVE_CALC LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date
		ON LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.claim_occurrence_ak_id = claim_occurrence_ak_id
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.financial_type_code = 'R'
		AND LKP_CLAIM_OCCURENCE_RESERVE_CALC_claim_occurrence_ak_id_R_IN_trans_date.eff_from_date = IN_trans_date
	
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
	LKP_Agency_Key AS (
		SELECT
		agency_key,
		agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				agency_key,
				agency_ak_id,
				eff_from_date,
				eff_to_date
			FROM agency
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_key DESC) = 1
	),
	LKP_contract_customer AS (
		SELECT
		contract_cust_id,
		contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_id,
				contract_cust_ak_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_id DESC) = 1
	),
	LKP_Policy_Dim AS (
		SELECT
		pol_dim_id,
		edw_pol_pk_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pol_dim_id,
				edw_pol_pk_id,
				eff_from_date,
				eff_to_date
			FROM policy_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_pk_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
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
	EXP_Claim_Rep_Lkp_Values AS (
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
		ON LKP_Claim_Created_by_rep_ak_id.claim_rep_key = LKP_claim_occurrence.claim_created_by_key AND LKP_Claim_Created_by_rep_ak_id.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Created_by_rep_ak_id.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Examiner
		ON LKP_Claim_Rep_Occurrence_Examiner.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Examiner.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Examiner.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_PLH
		ON LKP_Claim_Rep_Occurrence_PLH.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_PLH.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_PLH.eff_to_date >= EXP_get_values.IN_trans_date
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
	LKP_agency_Dim AS (
		SELECT
		agency_dim_id,
		agency_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				agency_dim_id,
				agency_key,
				eff_from_date,
				eff_to_date
			FROM V2.agency_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_contract_customer_dim AS (
		SELECT
		contract_cust_dim_id,
		edw_contract_cust_pk_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_dim_id,
				edw_contract_cust_pk_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_pk_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		LKP_claim_occurrence_dim.claim_occurrence_dim_id, 
		LKP_claim_occurrence.claim_loss_date, 
		LKP_claim_occurrence.claim_discovery_date, 
		LKP_claim_occurrence_dim.claim_scripted_date, 
		LKP_claim_occurrence_dim.source_claim_rpted_date, 
		LKP_claim_occurrence_dim.claim_rpted_date AS claim_occurrence_rpted_date, 
		LKP_claim_occurrence_dim.claim_open_date, 
		LKP_claim_occurrence_dim.claim_close_date, 
		LKP_claim_occurrence_dim.claim_reopen_date, 
		LKP_claim_occurrence_dim.claim_closed_after_reopen_date, 
		LKP_claim_occurrence_dim.claim_notice_only_date, 
		LKP_claim_occurrence.claim_cat_start_date, 
		LKP_claim_occurrence.claim_cat_end_date, 
		LKP_Claim_Rep_Occurrence_Handler.claim_assigned_date AS claim_rep_assigned_date, 
		LKP_Claim_Rep_Occurrence_Handler.eff_to_date AS claim_rep_unassigned_date, 
		EXP_Claim_Rep_Lkp_Values.claim_rep_dim_prim_claim_rep_id, 
		EXP_Claim_Rep_Lkp_Values.claim_rep_dim_examiner_id, 
		EXP_Claim_Rep_Lkp_Values.claim_rep_dim_prim_litigation_handler_id, 
		LKP_Policy_Dim.pol_dim_id AS pol_key_dim_id, 
		LKP_V2_policy.pol_eff_date, 
		LKP_V2_policy.pol_exp_date, 
		LKP_agency_Dim.agency_dim_id, 
		EXP_Claim_Rep_Lkp_Values.claim_created_by_id, 
		LKP_contract_customer_dim.contract_cust_dim_id, 
		LKP_V2_policy.pol_sym, 
		LKP_V2_policy.pol_num, 
		LKP_V2_policy.strtgc_bus_dvsn_ak_id, 
		LKP_AgencyDim.AgencyDimID, 
		LKP_V2_policy.StrategicProfitCenterCode, 
		LKP_V2_policy.InsuranceSegmentCode, 
		LKP_V2_policy.PolicyOfferingCode
		FROM EXP_Claim_Rep_Lkp_Values
		LEFT JOIN LKP_AgencyDim
		ON LKP_AgencyDim.EDWAgencyAKID = LKP_V2_policy.AgencyAKID AND LKP_AgencyDim.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_AgencyDim.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Policy_Dim
		ON LKP_Policy_Dim.edw_pol_pk_id = LKP_V2_policy.pol_id AND LKP_Policy_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Policy_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_V2_policy
		ON LKP_V2_policy.pol_key = LKP_claim_occurrence.pol_key AND LKP_V2_policy.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_V2_policy.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_agency_Dim
		ON LKP_agency_Dim.agency_key = LKP_Agency_Key.agency_key AND LKP_agency_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_agency_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence
		ON LKP_claim_occurrence.claim_occurrence_ak_id = EXP_get_values.IN_claim_occurrence_ak_id AND LKP_claim_occurrence.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_dim
		ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = EXP_get_reserve_calc_ids.claim_occurrence_ak_id_out AND LKP_claim_occurrence_dim.eff_from_date <= EXP_get_reserve_calc_ids.IN_trans_date AND LKP_claim_occurrence_dim.eff_to_date >= EXP_get_reserve_calc_ids.IN_trans_date
		LEFT JOIN LKP_contract_customer_dim
		ON LKP_contract_customer_dim.edw_contract_cust_pk_id = LKP_contract_customer.contract_cust_id AND LKP_contract_customer_dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_contract_customer_dim.eff_to_date >= EXP_get_values.IN_trans_date
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
EXP_get_Dim_Ids AS (
	SELECT
	-1 AS default_dim_id,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'))
	LKP_CALENDAR_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_id AS v_default_date_id,
	v_default_date_id AS Default_date_id,
	mplt_Claim_occurrence_COD.claim_occurrence_dim_id,
	-- *INF*: IIF(ISNULL(claim_occurrence_dim_id),-1,claim_occurrence_dim_id)
	IFF(claim_occurrence_dim_id IS NULL, - 1, claim_occurrence_dim_id) AS claim_occurrence_dim_id_OUT,
	mplt_Claim_occurrence_COD.pol_key_dim_id,
	-- *INF*: IIF(ISNULL(pol_key_dim_id),-1,pol_key_dim_id)
	IFF(pol_key_dim_id IS NULL, - 1, pol_key_dim_id) AS pol_key_dim_id_OUT,
	mplt_Claim_occurrence_COD.contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(contract_cust_dim_id),-1,contract_cust_dim_id)
	IFF(contract_cust_dim_id IS NULL, - 1, contract_cust_dim_id) AS contract_cust_dim_id_out,
	mplt_Claim_occurrence_COD.agency_dim_id,
	-- *INF*: IIF(ISNULL(agency_dim_id),-1,agency_dim_id)
	IFF(agency_dim_id IS NULL, - 1, agency_dim_id) AS agency_dim_id_OUT,
	mplt_Claim_occurrence_COD.claim_loss_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_loss_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_loss_date_id), v_claim_loss_date_id, -1)
	IFF(v_claim_loss_date_id IS NOT NULL, v_claim_loss_date_id, - 1) AS claim_loss_date_id,
	mplt_Claim_occurrence_COD.claim_discovery_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_discovery_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_discovery_date_id), v_claim_discovery_date_id, -1)
	IFF(v_claim_discovery_date_id IS NOT NULL, v_claim_discovery_date_id, - 1) AS claim_discovery_date_id,
	mplt_Claim_occurrence_COD.claim_scripted_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_scripted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_scripted_date_id), v_claim_scripted_date_id, -1)
	IFF(v_claim_scripted_date_id IS NOT NULL, v_claim_scripted_date_id, - 1) AS claim_scripted_date_id,
	mplt_Claim_occurrence_COD.source_claim_rpted_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_source_claim_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_source_claim_rpted_date_id), v_source_claim_rpted_date_id, -1)
	IFF(v_source_claim_rpted_date_id IS NOT NULL, v_source_claim_rpted_date_id, - 1) AS source_claim_rpted_date_id,
	mplt_Claim_occurrence_COD.claim_occurrence_rpted_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_occurrence_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_occurrence_rpted_date_id), v_claim_occurrence_rpted_date_id, -1)
	IFF(v_claim_occurrence_rpted_date_id IS NOT NULL, v_claim_occurrence_rpted_date_id, - 1) AS claim_occurrence_rpted_date_id,
	mplt_Claim_occurrence_COD.claim_open_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_open_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_open_date_id), v_claim_open_date_id, -1)
	IFF(v_claim_open_date_id IS NOT NULL, v_claim_open_date_id, - 1) AS claim_open_date_id,
	mplt_Claim_occurrence_COD.claim_close_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_close_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_close_date_id), v_claim_close_date_id, -1)
	IFF(v_claim_close_date_id IS NOT NULL, v_claim_close_date_id, - 1) AS claim_close_date_id,
	mplt_Claim_occurrence_COD.claim_reopen_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_reopen_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_reopen_date_id), v_claim_reopen_date_id, -1)
	IFF(v_claim_reopen_date_id IS NOT NULL, v_claim_reopen_date_id, - 1) AS claim_reopen_date_id,
	mplt_Claim_occurrence_COD.claim_closed_after_reopen_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_closed_after_reopen_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_closed_after_reopen_date_id), v_claim_closed_after_reopen_date_id, -1)
	IFF(
	    v_claim_closed_after_reopen_date_id IS NOT NULL, v_claim_closed_after_reopen_date_id, - 1
	) AS claim_closed_after_reopen_date_id,
	mplt_Claim_occurrence_COD.claim_notice_only_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_notice_only_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_notice_only_date_id), v_claim_notice_only_date_id, -1)
	IFF(v_claim_notice_only_date_id IS NOT NULL, v_claim_notice_only_date_id, - 1) AS claim_notice_only_date_id,
	mplt_Claim_occurrence_COD.claim_cat_start_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_start_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_start_date_id), v_claim_cat_start_date_id, -1)
	IFF(v_claim_cat_start_date_id IS NOT NULL, v_claim_cat_start_date_id, - 1) AS claim_cat_start_date_id,
	mplt_Claim_occurrence_COD.claim_cat_end_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_end_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_end_date_id), v_claim_cat_end_date_id, -1)
	IFF(v_claim_cat_end_date_id IS NOT NULL, v_claim_cat_end_date_id, - 1) AS claim_cat_end_date_id,
	mplt_Claim_occurrence_COD.claim_rep_assigned_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_assigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_assigned_date_id), v_claim_rep_assigned_date_id, v_default_date_id)
	IFF(
	    v_claim_rep_assigned_date_id IS NOT NULL, v_claim_rep_assigned_date_id, v_default_date_id
	) AS claim_rep_assigned_date_id,
	mplt_Claim_occurrence_COD.claim_rep_unassigned_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_unassigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_unassigned_date_id), v_claim_rep_unassigned_date_id, v_default_date_id)
	IFF(
	    v_claim_rep_unassigned_date_id IS NOT NULL, v_claim_rep_unassigned_date_id,
	    v_default_date_id
	) AS claim_rep_unassigned_date_id,
	mplt_Claim_occurrence_COD.claim_rep_dim_prim_claim_rep_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_claim_rep_id),-1,claim_rep_dim_prim_claim_rep_id)
	IFF(claim_rep_dim_prim_claim_rep_id IS NULL, - 1, claim_rep_dim_prim_claim_rep_id) AS claim_rep_dim_prim_claim_rep_id_OUT,
	mplt_Claim_occurrence_COD.claim_rep_dim_examiner_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_examiner_id),-1,claim_rep_dim_examiner_id)
	IFF(claim_rep_dim_examiner_id IS NULL, - 1, claim_rep_dim_examiner_id) AS claim_rep_dim_examiner_id_OUT,
	mplt_Claim_occurrence_COD.claim_rep_dim_prim_litigation_handler_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_litigation_handler_id),-1,claim_rep_dim_prim_litigation_handler_id)
	IFF(
	    claim_rep_dim_prim_litigation_handler_id IS NULL, - 1,
	    claim_rep_dim_prim_litigation_handler_id
	) AS claim_rep_dim_prim_litigation_handler_id_OUT,
	mplt_Claim_occurrence_COD.pol_eff_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_eff_date_id,
	-- *INF*: IIF(ISNULL(v_pol_eff_date_id),-1,v_pol_eff_date_id)
	IFF(v_pol_eff_date_id IS NULL, - 1, v_pol_eff_date_id) AS pol_eff_date_id,
	mplt_Claim_occurrence_COD.pol_exp_date,
	-- *INF*: :LKP.LKP_CALENDAR_DIM(to_date(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDAR_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_exp_date_id,
	-- *INF*: IIF(ISNULL(v_pol_exp_date_id),-1,v_pol_exp_date_id)
	-- 
	IFF(v_pol_exp_date_id IS NULL, - 1, v_pol_exp_date_id) AS pol_exp_date_id,
	0 AS default_amt,
	-3 AS default_audit_id,
	'0000000000000000000000000' AS err_flag,
	-1 AS claim_rep_dim_trans_entry_oper_id,
	mplt_Claim_occurrence_COD.claim_created_by_id,
	-- *INF*: IIF(ISNULL(claim_created_by_id),-1,claim_created_by_id)
	IFF(claim_created_by_id IS NULL, - 1, claim_created_by_id) AS claim_created_by_id_OUT,
	SEQ_Dummy_Transactions.NEXTVAL,
	-1 * NEXTVAL AS out_edw_claim_trans_pk_id,
	'N/A' AS default_string,
	mplt_Strategic_Business_Division_Dim.strtgc_bus_dvsn_dim_id,
	mplt_Claim_occurrence_COD.pol_sym,
	mplt_Claim_occurrence_COD.strtgc_bus_dvsn_ak_id,
	mplt_Claim_occurrence_COD.StrategicProfitCenterCode,
	mplt_Claim_occurrence_COD.InsuranceSegmentCode,
	mplt_Claim_occurrence_COD.PolicyOfferingCode,
	'N/A' AS ProductCode,
	'N/A' AS InsuranceReferenceLineOfBusinessCode,
	'N/A' AS v_RatingPlanCode,
	-- *INF*: :LKP.LKP_STRATEGIC_BUSINESS_DIVISION_DIM(StrategicProfitCenterCode)
	LKP_STRATEGIC_BUSINESS_DIVISION_DIM_StrategicProfitCenterCode.strtgc_bus_dvsn_dim_id AS v_strtgc_bus_dvsn_dim_id_DCT,
	-- *INF*: IIF(isnull(v_strtgc_bus_dvsn_dim_id_DCT),-1,v_strtgc_bus_dvsn_dim_id_DCT)
	IFF(v_strtgc_bus_dvsn_dim_id_DCT IS NULL, - 1, v_strtgc_bus_dvsn_dim_id_DCT) AS v_strtgc_bus_dvsn_dim_id_DCT_out,
	-- *INF*: IIF(pol_sym = '000',v_strtgc_bus_dvsn_dim_id_DCT_out,strtgc_bus_dvsn_dim_id)
	IFF(pol_sym = '000', v_strtgc_bus_dvsn_dim_id_DCT_out, strtgc_bus_dvsn_dim_id) AS strtgc_bus_dvsn_dim_id_out,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCEDIM(StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode, v_RatingPlanCode)
	LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.InsuranceReferenceDimId AS v_InsuranceReferenceDimId,
	-- *INF*: iif(isnull(v_InsuranceReferenceDimId),-1,v_InsuranceReferenceDimId)
	IFF(v_InsuranceReferenceDimId IS NULL, - 1, v_InsuranceReferenceDimId) AS v_InsuranceReferenceDimId_out,
	v_InsuranceReferenceDimId_out AS InsuranceReferenceDimId,
	mplt_Claim_occurrence_COD.AgencyDimID AS AgencyDimId,
	-- *INF*: iif(isnull(AgencyDimId),-1,AgencyDimId)
	IFF(AgencyDimId IS NULL, - 1, AgencyDimId) AS AgencyDimId_out,
	SYSDATE AS ModifiedDate
	FROM mplt_Claim_occurrence_COD
	 -- Manually join with mplt_Strategic_Business_Division_Dim
	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS
	ON LKP_CALENDAR_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_date = TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM LKP_CALENDAR_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_STRATEGIC_BUSINESS_DIVISION_DIM LKP_STRATEGIC_BUSINESS_DIVISION_DIM_StrategicProfitCenterCode
	ON LKP_STRATEGIC_BUSINESS_DIVISION_DIM_StrategicProfitCenterCode.strtgc_bus_dvsn_code = StrategicProfitCenterCode

	LEFT JOIN LKP_INSURANCEREFERENCEDIM LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode
	ON LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.StrategicProfitCenterCode = StrategicProfitCenterCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.InsuranceSegmentCode = InsuranceSegmentCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.PolicyOfferingCode = PolicyOfferingCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.ProductCode = ProductCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.InsuranceReferenceLineOfBusinessCode = InsuranceReferenceLineOfBusinessCode
	AND LKP_INSURANCEREFERENCEDIM_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_v_RatingPlanCode.RatingPlanCode = v_RatingPlanCode

),
claim_loss_transaction_fact_dummy_Claim_Occurrence_row AS (
	INSERT INTO claim_loss_transaction_fact
	(err_flag, audit_id, edw_claim_trans_pk_id, edw_claim_reins_trans_pk_id, claim_occurrence_dim_id, claim_occurrence_dim_hist_id, claimant_dim_id, claimant_dim_hist_id, claimant_cov_dim_id, claimant_cov_dim_hist_id, cov_dim_id, cov_dim_hist_id, claim_trans_type_dim_id, claim_financial_type_dim_id, reins_cov_dim_id, reins_cov_dim_hist_id, claim_rep_dim_prim_claim_rep_id, claim_rep_dim_prim_claim_rep_hist_id, claim_rep_dim_examiner_id, claim_rep_dim_examiner_hist_id, claim_rep_dim_prim_litigation_handler_id, claim_rep_dim_prim_litigation_handler_hist_id, claim_rep_dim_trans_entry_oper_id, claim_rep_dim_trans_entry_oper_hist_id, claim_rep_dim_claim_created_by_id, pol_dim_id, pol_dim_hist_id, agency_dim_id, agency_dim_hist_id, claim_pay_dim_id, claim_pay_dim_hist_id, claim_pay_ctgry_type_dim_id, claim_pay_ctgry_type_dim_hist_id, claim_case_dim_id, claim_case_dim_hist_id, contract_cust_dim_id, contract_cust_dim_hist_id, claim_master_1099_list_dim_id, claim_subrogation_dim_id, claim_trans_date_id, claim_trans_reprocess_date_id, claim_loss_date_id, claim_discovery_date_id, claim_scripted_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_open_date_id, claim_close_date_id, claim_reopen_date_id, claim_closed_after_reopen_date_id, claim_notice_only_date_id, claim_cat_start_date_id, claim_cat_end_date_id, claim_rep_assigned_date_id, claim_rep_unassigned_date_id, pol_eff_date_id, pol_exp_date_id, claim_subrogation_referred_to_subrogation_date_id, claim_subrogation_pay_start_date_id, claim_subrogation_closure_date_id, acct_entered_date_id, trans_amt, trans_hist_amt, tax_id, direct_loss_paid_excluding_recoveries, direct_loss_outstanding_excluding_recoveries, direct_loss_incurred_excluding_recoveries, direct_alae_paid_excluding_recoveries, direct_alae_outstanding_excluding_recoveries, direct_alae_incurred_excluding_recoveries, direct_loss_paid_including_recoveries, direct_loss_outstanding_including_recoveries, direct_loss_incurred_including_recoveries, direct_alae_paid_including_recoveries, direct_alae_outstanding_including_recoveries, direct_alae_incurred_including_recoveries, direct_subrogation_paid, direct_subrogation_outstanding, direct_subrogation_incurred, direct_salvage_paid, direct_salvage_outstanding, direct_salvage_incurred, direct_other_recovery_loss_paid, direct_other_recovery_loss_outstanding, direct_other_recovery_loss_incurred, direct_other_recovery_alae_paid, direct_other_recovery_alae_outstanding, direct_other_recovery_alae_incurred, total_direct_loss_recovery_paid, total_direct_loss_recovery_outstanding, total_direct_loss_recovery_incurred, direct_other_recovery_paid, direct_other_recovery_outstanding, direct_other_recovery_incurred, ceded_loss_paid, ceded_loss_outstanding, ceded_loss_incurred, ceded_alae_paid, ceded_alae_outstanding, ceded_alae_incurred, ceded_salvage_paid, ceded_subrogation_paid, ceded_other_recovery_loss_paid, ceded_other_recovery_alae_paid, total_ceded_loss_recovery_paid, net_loss_paid, net_loss_outstanding, net_loss_incurred, net_alae_paid, net_alae_outstanding, net_alae_incurred, asl_dim_id, asl_prdct_code_dim_id, loss_master_dim_id, strtgc_bus_dvsn_dim_id, prdct_code_dim_id, ClaimReserveDimId, ClaimRepresentativeDimFeatureClaimRepresentativeId, FeatureRepresentativeAssignedDateId, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, ModifiedDate)
	SELECT 
	ERR_FLAG, 
	default_audit_id AS AUDIT_ID, 
	out_edw_claim_trans_pk_id AS EDW_CLAIM_TRANS_PK_ID, 
	out_edw_claim_trans_pk_id AS EDW_CLAIM_REINS_TRANS_PK_ID, 
	claim_occurrence_dim_id_OUT AS CLAIM_OCCURRENCE_DIM_ID, 
	claim_occurrence_dim_id_OUT AS CLAIM_OCCURRENCE_DIM_HIST_ID, 
	default_dim_id AS CLAIMANT_DIM_ID, 
	default_dim_id AS CLAIMANT_DIM_HIST_ID, 
	default_dim_id AS CLAIMANT_COV_DIM_ID, 
	default_dim_id AS CLAIMANT_COV_DIM_HIST_ID, 
	default_dim_id AS COV_DIM_ID, 
	default_dim_id AS COV_DIM_HIST_ID, 
	default_dim_id AS CLAIM_TRANS_TYPE_DIM_ID, 
	default_dim_id AS CLAIM_FINANCIAL_TYPE_DIM_ID, 
	default_dim_id AS REINS_COV_DIM_ID, 
	default_dim_id AS REINS_COV_DIM_HIST_ID, 
	claim_rep_dim_prim_claim_rep_id_OUT AS CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	claim_rep_dim_prim_claim_rep_id_OUT AS CLAIM_REP_DIM_PRIM_CLAIM_REP_HIST_ID, 
	claim_rep_dim_examiner_id_OUT AS CLAIM_REP_DIM_EXAMINER_ID, 
	claim_rep_dim_examiner_id_OUT AS CLAIM_REP_DIM_EXAMINER_HIST_ID, 
	claim_rep_dim_prim_litigation_handler_id_OUT AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_ID, 
	claim_rep_dim_prim_litigation_handler_id_OUT AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_HIST_ID, 
	CLAIM_REP_DIM_TRANS_ENTRY_OPER_ID, 
	claim_rep_dim_trans_entry_oper_id AS CLAIM_REP_DIM_TRANS_ENTRY_OPER_HIST_ID, 
	claim_created_by_id_OUT AS CLAIM_REP_DIM_CLAIM_CREATED_BY_ID, 
	pol_key_dim_id_OUT AS POL_DIM_ID, 
	pol_key_dim_id_OUT AS POL_DIM_HIST_ID, 
	agency_dim_id_OUT AS AGENCY_DIM_ID, 
	default_dim_id AS AGENCY_DIM_HIST_ID, 
	default_dim_id AS CLAIM_PAY_DIM_ID, 
	default_dim_id AS CLAIM_PAY_DIM_HIST_ID, 
	default_dim_id AS CLAIM_PAY_CTGRY_TYPE_DIM_ID, 
	default_dim_id AS CLAIM_PAY_CTGRY_TYPE_DIM_HIST_ID, 
	default_dim_id AS CLAIM_CASE_DIM_ID, 
	default_dim_id AS CLAIM_CASE_DIM_HIST_ID, 
	contract_cust_dim_id_out AS CONTRACT_CUST_DIM_ID, 
	contract_cust_dim_id_out AS CONTRACT_CUST_DIM_HIST_ID, 
	default_dim_id AS CLAIM_MASTER_1099_LIST_DIM_ID, 
	default_dim_id AS CLAIM_SUBROGATION_DIM_ID, 
	Default_date_id AS CLAIM_TRANS_DATE_ID, 
	Default_date_id AS CLAIM_TRANS_REPROCESS_DATE_ID, 
	CLAIM_LOSS_DATE_ID, 
	CLAIM_DISCOVERY_DATE_ID, 
	CLAIM_SCRIPTED_DATE_ID, 
	SOURCE_CLAIM_RPTED_DATE_ID, 
	claim_occurrence_rpted_date_id AS CLAIM_RPTED_DATE_ID, 
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
	Default_date_id AS CLAIM_SUBROGATION_REFERRED_TO_SUBROGATION_DATE_ID, 
	Default_date_id AS CLAIM_SUBROGATION_PAY_START_DATE_ID, 
	Default_date_id AS CLAIM_SUBROGATION_CLOSURE_DATE_ID, 
	Default_date_id AS ACCT_ENTERED_DATE_ID, 
	default_amt AS TRANS_AMT, 
	default_amt AS TRANS_HIST_AMT, 
	default_audit_id AS TAX_ID, 
	default_amt AS DIRECT_LOSS_PAID_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_OUTSTANDING_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_INCURRED_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_PAID_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_OUTSTANDING_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_INCURRED_EXCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_PAID_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_OUTSTANDING_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_LOSS_INCURRED_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_PAID_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_OUTSTANDING_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_ALAE_INCURRED_INCLUDING_RECOVERIES, 
	default_amt AS DIRECT_SUBROGATION_PAID, 
	default_amt AS DIRECT_SUBROGATION_OUTSTANDING, 
	default_amt AS DIRECT_SUBROGATION_INCURRED, 
	default_amt AS DIRECT_SALVAGE_PAID, 
	default_amt AS DIRECT_SALVAGE_OUTSTANDING, 
	default_amt AS DIRECT_SALVAGE_INCURRED, 
	default_amt AS DIRECT_OTHER_RECOVERY_LOSS_PAID, 
	default_amt AS DIRECT_OTHER_RECOVERY_LOSS_OUTSTANDING, 
	default_amt AS DIRECT_OTHER_RECOVERY_LOSS_INCURRED, 
	default_amt AS DIRECT_OTHER_RECOVERY_ALAE_PAID, 
	default_amt AS DIRECT_OTHER_RECOVERY_ALAE_OUTSTANDING, 
	default_amt AS DIRECT_OTHER_RECOVERY_ALAE_INCURRED, 
	default_amt AS TOTAL_DIRECT_LOSS_RECOVERY_PAID, 
	default_amt AS TOTAL_DIRECT_LOSS_RECOVERY_OUTSTANDING, 
	default_amt AS TOTAL_DIRECT_LOSS_RECOVERY_INCURRED, 
	default_amt AS DIRECT_OTHER_RECOVERY_PAID, 
	default_amt AS DIRECT_OTHER_RECOVERY_OUTSTANDING, 
	default_amt AS DIRECT_OTHER_RECOVERY_INCURRED, 
	default_amt AS CEDED_LOSS_PAID, 
	default_amt AS CEDED_LOSS_OUTSTANDING, 
	default_amt AS CEDED_LOSS_INCURRED, 
	default_amt AS CEDED_ALAE_PAID, 
	default_amt AS CEDED_ALAE_OUTSTANDING, 
	default_amt AS CEDED_ALAE_INCURRED, 
	default_amt AS CEDED_SALVAGE_PAID, 
	default_amt AS CEDED_SUBROGATION_PAID, 
	default_amt AS CEDED_OTHER_RECOVERY_LOSS_PAID, 
	default_amt AS CEDED_OTHER_RECOVERY_ALAE_PAID, 
	default_amt AS TOTAL_CEDED_LOSS_RECOVERY_PAID, 
	default_amt AS NET_LOSS_PAID, 
	default_amt AS NET_LOSS_OUTSTANDING, 
	default_amt AS NET_LOSS_INCURRED, 
	default_amt AS NET_ALAE_PAID, 
	default_amt AS NET_ALAE_OUTSTANDING, 
	default_amt AS NET_ALAE_INCURRED, 
	default_dim_id AS ASL_DIM_ID, 
	default_dim_id AS ASL_PRDCT_CODE_DIM_ID, 
	default_dim_id AS LOSS_MASTER_DIM_ID, 
	strtgc_bus_dvsn_dim_id_out AS STRTGC_BUS_DVSN_DIM_ID, 
	default_dim_id AS PRDCT_CODE_DIM_ID, 
	default_dim_id AS CLAIMRESERVEDIMID, 
	default_dim_id AS CLAIMREPRESENTATIVEDIMFEATURECLAIMREPRESENTATIVEID, 
	Default_date_id AS FEATUREREPRESENTATIVEASSIGNEDDATEID, 
	INSURANCEREFERENCEDIMID, 
	AgencyDimId_out AS AGENCYDIMID, 
	default_dim_id AS SALESDIVISIONDIMID, 
	default_dim_id AS INSURANCEREFERENCECOVERAGEDIMID, 
	default_dim_id AS COVERAGEDETAILDIMID, 
	MODIFIEDDATE
	FROM EXP_get_Dim_Ids
),