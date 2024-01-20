WITH
LKP_MultiStatePolicy AS (
	SELECT
	PolicyAKId
	FROM (
		select PolicyAKId as PolicyAKId
		from (
		SELECT distinct RL.PolicyAKId, RL.StateProvinceCode 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.CurrentSnapshotFlag = 1
		and PC.CurrentSnapshotFlag  = 1
		)src
		group by PolicyAKId having count(*)>1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId ORDER BY PolicyAKId) = 1
),
LKP_sup_state AS (
	SELECT
	state_abbrev,
	state_code
	FROM (
		SELECT LTRIM(RTRIM(state_code)) as state_code,
		LTRIM(RTRIM(state_abbrev)) as state_abbrev 
		FROM sup_state
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_abbrev) = 1
),
lkp_ClaimNumber AS (
	SELECT
	TypeOfRecoveryCode,
	Claim_Number,
	claim_party_occurrence_ak_id
	FROM (
		select distinct RTRIM(Claim_Number) as Claim_Number, ,claim_party_occurrence_ak_id as claim_party_occurrence_ak_id,
		TypeOfRecoveryCode as TypeOfRecoveryCode 
		from (
		SELECT (CASE WHEN rtrim(ltrim(co.s3p_claim_num)) ='N/A' THEN (RIGHT('000'+CAST((DATEDIFF(d, DATEADD(yy, DATEDIFF(yy,0,co.claim_loss_date),0),co.claim_loss_date)+1) AS VARCHAR),3)+co.claim_occurrence_num+cpo.claimant_num) ELSE co.s3p_claim_num END) AS Claim_Number,
		MAX(case when (cttd.pms_trans_code in ('81','82','83','84','85','86','87','88','89')) AND cltf.direct_subrogation_paid <> 0  then '03' else '01' end) AS TypeOfRecoveryCode
		,cpo.claim_party_occurrence_ak_id
		FROM claim_loss_transaction_fact cltf
		inner join claimant_coverage_dim  ccdim on ccdim.claimant_cov_dim_id = cltf.claimant_cov_dim_id
		INNER JOIN loss_master_fact LMF ON LMF.claimant_cov_dim_id=ccdim.claimant_cov_dim_id
		inner join claim_transaction_type_dim  cttd on cltf.claim_trans_type_dim_id =cttd.claim_trans_type_dim_id and trans_kind_code = 'D' 
		inner join calendar_dim c on cltf.claim_trans_date_id=c.clndr_id
		inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME_IL}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail  ccd
		on ccdim.edw_claimant_cov_det_ak_id =ccd.claimant_cov_det_ak_id  and ccd.crrnt_snpsht_flag=1
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_IL}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence   cpo
		ON ccd.claim_party_occurrence_ak_id=cpo.claim_party_occurrence_ak_id 
		AND cpo.crrnt_snpsht_flag=1 AND ccd.crrnt_snpsht_flag = 1
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_IL}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence co
		ON cpo.claim_occurrence_ak_id=co.claim_occurrence_ak_id 
		AND co.crrnt_snpsht_flag=1 
		WHERE c.clndr_date <DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
		GROUP BY (CASE WHEN rtrim(ltrim(co.s3p_claim_num)) ='N/A' THEN (RIGHT('000'+CAST((DATEDIFF(d, DATEADD(yy, DATEDIFF(yy,0,co.claim_loss_date),0),co.claim_loss_date)+1) AS VARCHAR),3)+co.claim_occurrence_num+cpo.claimant_num) ELSE co.s3p_claim_num END)
		,cpo.claim_party_occurrence_ak_id
		HAVING MAX(case when (cttd.pms_trans_code in ('81','82','83','84','85','86','87','88','89')) AND cltf.direct_subrogation_paid <> 0  then '03' else '01' end) ='03'
		) t
		order by Claim_Number, claim_party_occurrence_ak_id,TypeOfRecoveryCode
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Claim_Number,claim_party_occurrence_ak_id ORDER BY TypeOfRecoveryCode) = 1
),
LKP_PremiumTransaction_pol_ak_id AS (
	SELECT
	pol_ak_id
	FROM (
		SELECT DISTINCT P.pol_ak_id AS pol_ak_id
		   from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
		and SC.CurrentSnapshotFlag=1 and  PT.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		and RL.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		on P.pol_ak_id=RL.PolicyAKID 
		and P.crrnt_snpsht_flag=1 
		
		where SC.SourceSystemID='PMS'  
		and PC.TypeBureauCode in ('WC','WP','WorkersCompensation')  and  PT.PremiumType='D'   and PT.ReasonAmendedCode != 'CWO'
		and DATEDIFF(MM,P.pol_eff_date, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0) )  > 18
		and PT.PremiumTransactionBookedDate>=DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0)
		and PT.PremiumTransactionBookedDate<DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
		
		UNION ALL
		
		SELECT DISTINCT  P.pol_ak_id AS pol_ak_id
		   from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on RC.RatingCoverageAKID=PT.RatingCoverageAKId
		and RC.EffectiveDate=PT.EffectiveDate and PT.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		and RL.CurrentSnapshotFlag=1
		
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		on P.pol_ak_id=RL.PolicyAKID 
		and P.crrnt_snpsht_flag=1 
		
		where RC.SourceSystemID='DCT' and PC.TypeBureauCode in ('WC','WP','WorkersCompensation')  and PT.PremiumType='D'
		and PT.ReasonAmendedCode NOT IN ('CWO','Claw Back')
		and DATEDIFF(MM,P.pol_eff_date, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0) )  > 18
		and PT.PremiumTransactionBookedDate>=DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}, 0)
		and PT.PremiumTransactionBookedDate<DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + @{pipeline().parameters.NUM_OF_MONTHS}+1, 0)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY pol_ak_id) = 1
),
LKP_SupWorkersCompensationPremiumModifierClass AS (
	SELECT
	SupWorkersCompensationPremiumModifierClassId,
	ClassCode,
	StateCode,
	in_ClassCode,
	in_StateCode
	FROM (
		SELECT 
			SupWorkersCompensationPremiumModifierClassId,
			ClassCode,
			StateCode,
			in_ClassCode,
			in_StateCode
		FROM SupWorkersCompensationPremiumModifierClass
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,StateCode ORDER BY SupWorkersCompensationPremiumModifierClassId) = 1
),
SEQTRANS AS (
	CREATE SEQUENCE SEQTRANS
	START = 0
	INCREMENT = 1;
),
SQ_WorkWCSTATLoss AS (
	SELECT
		WorkWCSTATLossId AS WorkWCStatLossID,
		AuditId AS AuditID,
		CreatedDate AS Createddate,
		ModifiedDate,
		TypeBureauCode,
		PolicyNumber AS pol_num,
		PolicyModulus AS pol_mod,
		PolicySymbol AS pol_sym,
		StateProvinceCode,
		PolicyEffectiveDate AS pol_eff_date,
		PolicyExpiryDate AS pol_exp_date,
		PolicyCancellationDate AS pol_cancellation_date,
		PolicyCancellationIndicator AS pol_cancellation_ind,
		WCInterStateRiskId,
		FederalTaxId AS fed_tax_id,
		PolicyTerm AS pol_term,
		InsuranceSegmentAbbreviation,
		CustomerRole AS cust_role,
		Name AS name,
		AddressLine1 AS addr_line_1,
		CityName AS city_name,
		StateProvCodeContractCustomerAddress AS state_prov_code_Contract_Customer_Address,
		ZipPostalCode AS zip_postal_code,
		Exposure,
		ClassCode AS class_code,
		ClaimLossDate AS claim_loss_date,
		ClaimOccurrenceNumber AS claim_occurrence_num,
		S3pClaimNumber AS s3p_claim_num,
		ClaimantStatusType AS claimant_status_type,
		ClaimCatCode AS claim_cat_code,
		LossLocationState AS loss_loc_state,
		PolicyAKId AS pol_ak_id,
		LossCondition AS loss_condition,
		ManagedCareOrganisationType AS managed_care_org_type,
		ClaimPayCategoryLumpSumInd AS claim_pay_ctgry_lump_sum_ind,
		EstimatedAuditCode,
		AuditStatus,
		ClaimPayCategoryType AS claim_pay_ctgry_type,
		PaidIndemnityAmount,
		PaidMedicalAmount,
		DeductibleReimbursementAmount,
		PaidAllocatedLossAdjustmentExpenseAmount,
		IncurredAllocatedLossAdjustmentExpenseAmount,
		IncurredIndemnityAmount,
		IncurredMedicalAmount,
		ClaimantNum AS claimant_num,
		JurisdictionStateCode AS jurisdiction_state_code,
		ClaimPartyOccurrenceAKId AS claim_party_occurrence_ak_id,
		TypeOfRecoveryCode,
		TypeOfLossCode AS type_of_loss_code,
		BodyPartCode AS body_part_code,
		NatureOfInjCode AS nature_of_inj_code,
		CauseInjCode AS cause_inj_code,
		ClaimRelationshipKey,
		SuitHowClaimClosed,
		OccupationDescription,
		WeeklyWageAmount,
		EmployerAttorneyFees
	FROM WorkWCSTATLoss
	WHERE AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Loss AS (
	SELECT
	WorkWCStatLossID,
	AuditID AS AuditId,
	Createddate AS CreatedDate,
	ModifiedDate,
	TypeBureauCode AS i_TypeBureauCode,
	pol_num AS i_pol_num,
	pol_mod AS i_pol_mod,
	pol_sym AS i_pol_sym,
	StateProvinceCode AS i_StateProvinceCode_RiskLocation,
	pol_eff_date AS i_pol_eff_date,
	pol_exp_date AS i_pol_exp_date,
	pol_cancellation_date AS i_pol_cancellation_date,
	pol_cancellation_ind AS i_pol_cancellation_ind,
	WCInterStateRiskId AS i_WCInterStateRiskId,
	fed_tax_id AS i_fed_tax_id,
	pol_term AS i_pol_term,
	InsuranceSegmentAbbreviation AS i_InsuranceSegmentAbbreviation,
	cust_role AS i_cust_role,
	name AS i_name,
	addr_line_1 AS i_addr_line_1,
	city_name AS i_city_name,
	state_prov_code_Contract_Customer_Address AS i_state_prov_code_Contract_Customer_Address,
	zip_postal_code AS i_zip_postal_code,
	Exposure AS i_Exposure,
	class_code AS i_class_code,
	claim_loss_date AS i_claim_loss_date,
	claim_occurrence_num AS i_claim_occurrence_num,
	s3p_claim_num AS i_s3p_claim_num,
	claimant_status_type,
	claim_cat_code AS i_claim_cat_code,
	loss_loc_state AS i_loss_loc_state,
	pol_ak_id AS i_pol_ak_id,
	loss_condition AS i_loss_condition,
	managed_care_org_type AS i_managed_care_org_type,
	claim_pay_ctgry_lump_sum_ind AS i_claim_pay_ctgry_lump_sum_ind,
	EstimatedAuditCode AS i_EstimatedAuditCode,
	AuditStatus AS i_AuditStatus,
	claim_pay_ctgry_type AS i_claim_pay_ctgry_type,
	PaidIndemnityAmount AS i_PaidIndemnityAmount,
	PaidMedicalAmount AS i_PaidMedicalAmount,
	DeductibleReimbursementAmount AS i_DeductibleReimbursementAmount,
	PaidAllocatedLossAdjustmentExpenseAmount AS i_PaidAllocatedLossAdjustmentExpenseAmount,
	IncurredAllocatedLossAdjustmentExpenseAmount AS i_IncurredAllocatedLossAdjustmentExpenseAmount,
	IncurredIndemnityAmount AS i_IncurredIndemnityAmount,
	IncurredMedicalAmount AS i_IncurredMedicalAmount,
	claimant_num AS i_claimant_num,
	jurisdiction_state_code AS i_jurisdiction_state_code,
	claim_party_occurrence_ak_id,
	-- *INF*: IIF(LTRIM(RTRIM(i_addr_line_1))='N/A', '', LTRIM(RTRIM(i_addr_line_1)))
	IFF(LTRIM(RTRIM(i_addr_line_1)) = 'N/A', '', LTRIM(RTRIM(i_addr_line_1))) AS v_addr_line_1,
	TypeOfRecoveryCode AS lkp_TypeOfRecoveryCode,
	type_of_loss_code AS lkp_type_of_loss_code,
	body_part_code AS lkp_body_part_code,
	nature_of_inj_code AS lkp_nature_of_inj_code,
	cause_inj_code AS lkp_cause_inj_code,
	-- *INF*: IIF(LTRIM(RTRIM(i_city_name))='N/A', '', LTRIM(RTRIM(i_city_name)))
	IFF(LTRIM(RTRIM(i_city_name)) = 'N/A', '', LTRIM(RTRIM(i_city_name))) AS v_city_name,
	-- *INF*: IIF(LTRIM(RTRIM(i_state_prov_code_Contract_Customer_Address))='N/A', '', LTRIM(RTRIM(i_state_prov_code_Contract_Customer_Address)))
	IFF(
	    LTRIM(RTRIM(i_state_prov_code_Contract_Customer_Address)) = 'N/A', '',
	    LTRIM(RTRIM(i_state_prov_code_Contract_Customer_Address))
	) AS v_state_prov_code,
	-- *INF*: IIF(LTRIM(RTRIM(i_zip_postal_code))='N/A', '', LTRIM(RTRIM(i_zip_postal_code)))
	IFF(LTRIM(RTRIM(i_zip_postal_code)) = 'N/A', '', LTRIM(RTRIM(i_zip_postal_code))) AS v_zip_postal_code,
	-- *INF*: :LKP.LKP_sup_state(LTRIM(RTRIM(i_jurisdiction_state_code)))
	LKP_SUP_STATE_LTRIM_RTRIM_i_jurisdiction_state_code.state_abbrev AS v_JurisdictionStateCode,
	-- *INF*: IIF(ISNULL(i_claim_occurrence_num) OR IS_SPACES(i_claim_occurrence_num) OR LENGTH(i_claim_occurrence_num)=0 OR i_claim_occurrence_num='N/A' OR IS_NUMBER(i_claim_occurrence_num)=0, 0, TO_INTEGER(i_claim_occurrence_num))
	IFF(
	    i_claim_occurrence_num IS NULL
	    or LENGTH(i_claim_occurrence_num)>0
	    and TRIM(i_claim_occurrence_num)=''
	    or LENGTH(i_claim_occurrence_num) = 0
	    or i_claim_occurrence_num = 'N/A'
	    or REGEXP_LIKE(i_claim_occurrence_num, '^[0-9]+$') = 0,
	    0,
	    CAST(i_claim_occurrence_num AS INTEGER)
	) AS v_claim_occurrence_num,
	-- *INF*: IIF(ISNULL(:LKP.LKP_MULTISTATEPOLICY(i_pol_ak_id)), '0', '1')
	IFF(LKP_MULTISTATEPOLICY_i_pol_ak_id.PolicyAKId IS NULL, '0', '1') AS v_MultistatePolicyIndicator,
	-- *INF*: IIF( rtrim(ltrim(i_s3p_claim_num)) ='N/A', TO_CHAR(i_claim_loss_date , 'DDD')  || i_claim_occurrence_num||i_claimant_num, i_s3p_claim_num)
	IFF(
	    rtrim(ltrim(i_s3p_claim_num)) = 'N/A',
	    TO_CHAR(i_claim_loss_date, 'DDD') || i_claim_occurrence_num || i_claimant_num,
	    i_s3p_claim_num
	) AS v_ClaimNumber,
	-- *INF*: IIF(
	-- NOT ISNULL(:LKP.LKP_CLAIMNUMBER(v_ClaimNumber,claim_party_occurrence_ak_id)),
	-- :LKP.LKP_CLAIMNUMBER(v_ClaimNumber,claim_party_occurrence_ak_id),
	-- IIF(ISNULL(LTRIM(RTRIM(lkp_TypeOfRecoveryCode))),'N/A',
	-- LTRIM(RTRIM(lkp_TypeOfRecoveryCode))))
	IFF(
	    LKP_CLAIMNUMBER_v_ClaimNumber_claim_party_occurrence_ak_id.TypeOfRecoveryCode IS NOT NULL,
	    LKP_CLAIMNUMBER_v_ClaimNumber_claim_party_occurrence_ak_id.TypeOfRecoveryCode,
	    IFF(
	        LTRIM(RTRIM(lkp_TypeOfRecoveryCode)) IS NULL, 'N/A',
	        LTRIM(RTRIM(lkp_TypeOfRecoveryCode))
	    )
	) AS o_TypeOfRecoveryCode,
	claim_party_occurrence_ak_id AS o_loss_master_calculation_id,
	-- *INF*: RTRIM(LTRIM(i_TypeBureauCode))
	RTRIM(LTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: ADD_TO_DATE(LAST_DAY(SESSSTARTTIME),'MM',@{pipeline().parameters.NUM_OF_MONTHS})
	-- 
	DATEADD(MONTH,@{pipeline().parameters.NUM_OF_MONTHS},LAST_DAY(SESSSTARTTIME)) AS o_LossMasterRunDate,
	'17124' AS o_BureauCompanyCode,
	-- *INF*: RTRIM(LTRIM(i_pol_sym || i_pol_num || i_pol_mod))
	RTRIM(LTRIM(i_pol_sym || i_pol_num || i_pol_mod)) AS o_PolicyKey,
	-- *INF*: RTRIM(LTRIM(i_StateProvinceCode_RiskLocation))
	RTRIM(LTRIM(i_StateProvinceCode_RiskLocation)) AS o_StateProvinceCode,
	-- *INF*: TRUNC(i_pol_eff_date, 'DD')
	CAST(TRUNC(i_pol_eff_date, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_PolicyEffectiveDate,
	-- *INF*: IIF(i_pol_cancellation_ind='Y' AND i_pol_cancellation_date  != TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), TRUNC(i_pol_cancellation_date, 'DD'), TRUNC(i_pol_exp_date,'DD'))
	IFF(
	    i_pol_cancellation_ind = 'Y'
	    and i_pol_cancellation_date != TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    CAST(TRUNC(i_pol_cancellation_date, 'DAY') AS TIMESTAMP_NTZ(0)),
	    CAST(TRUNC(i_pol_exp_date, 'DAY') AS TIMESTAMP_NTZ(0))
	) AS o_PolicyEndDate,
	-- *INF*: IIF(ISNULL(i_WCInterStateRiskId), LPAD('', 9, '0'), RTRIM(LTRIM(TO_CHAR(i_WCInterStateRiskId))))
	IFF(
	    i_WCInterStateRiskId IS NULL, LPAD('', 9, '0'), RTRIM(LTRIM(TO_CHAR(i_WCInterStateRiskId)))
	) AS o_InterstateRiskId,
	-- *INF*: --Removed lookup as part of PROD-6820 and PROD-13914/12901/13877
	-- -- As per historic data, it was never used as this form was on none of the policies
	-- --IIF( NOT ISNULL(:LKP.LKP_POLICYFORM_FORM(i_pol_id, 'WC000316')), 'E', '')
	-- ''
	'' AS o_EmployeeLeasingCode,
	-- *INF*: IIF(i_fed_tax_id='N/A', LPAD('',10, ' '),LPAD(SUBSTR(i_fed_tax_id, 1, 10), 10, '0'))
	IFF(i_fed_tax_id = 'N/A', LPAD('', 10, ' '), LPAD(SUBSTR(i_fed_tax_id, 1, 10), 10, '0')) AS o_FederalTaxId,
	-- *INF*: IIF(LTRIM(i_pol_term,'0')='36', '1', '0')
	IFF(LTRIM(i_pol_term, '0') = '36', '1', '0') AS o_ThreeYearFixedRatePolicyIndicator,
	v_MultistatePolicyIndicator AS o_MultistatePolicyIndicator,
	-- *INF*: IIF(i_StateProvinceCode_RiskLocation='21', 0,
	--      IIF(LENGTH(i_WCInterStateRiskId)=9,1,0))
	-- 
	-- --IIF(ISNULL(i_WCInterStateRiskId) OR i_WCInterStateRiskId='-1' OR i_WCInterStateRiskId='N/A'  , '0', '1')
	IFF(
	    i_StateProvinceCode_RiskLocation = '21', 0,
	    IFF(
	        LENGTH(i_WCInterStateRiskId) = 9, 1, 0
	    )
	) AS o_InterstateRatedPolicyIndicator,
	'0' AS o_RetrospectiveRatedPolicyIndicator,
	-- *INF*: IIF(i_pol_cancellation_date   >=   i_pol_eff_date  AND  i_pol_cancellation_date  <=  i_pol_exp_date, '1', '0')
	IFF(
	    i_pol_cancellation_date >= i_pol_eff_date AND i_pol_cancellation_date <= i_pol_exp_date, '1',
	    '0'
	) AS o_CancelledMidTermPolicyIndicator,
	-- *INF*: IIF(IN(i_StateProvinceCode_RiskLocation, '22', '21'), '0', '1')
	IFF(i_StateProvinceCode_RiskLocation IN ('22','21'), '0', '1') AS o_ManagedCareOrganizationPolicyIndicator,
	'01' AS o_TypeOfCoverageIdCode,
	-- *INF*: IIF(i_InsuranceSegmentAbbreviation='Pool', '02', '01')
	IFF(i_InsuranceSegmentAbbreviation = 'Pool', '02', '01') AS o_TypeOfPlan,
	'0' AS o_DeductibleAmountPerClaimAccident,
	-- *INF*: IIF(UPPER(i_cust_role)='INSURED',RTRIM(LTRIM( i_name)), 'N/A')
	IFF(UPPER(i_cust_role) = 'INSURED', RTRIM(LTRIM(i_name)), 'N/A') AS o_InsuredName,
	-- *INF*: substr(v_addr_line_1 || ' ' || v_city_name || ' ,' || v_state_prov_code || ' ' || v_zip_postal_code,1,500)
	substr(v_addr_line_1 || ' ' || v_city_name || ' ,' || v_state_prov_code || ' ' || v_zip_postal_code, 1, 500) AS o_WCSTATAddress,
	i_Exposure AS o_Exposure,
	-- *INF*: RTRIM(LTRIM(i_class_code))
	RTRIM(LTRIM(i_class_code)) AS o_LossMasterClassCode,
	-- *INF*: TRUNC(i_claim_loss_date,'DD')
	CAST(TRUNC(i_claim_loss_date, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_ClaimLossDate,
	-- *INF*: RTRIM(LTRIM(v_ClaimNumber))
	RTRIM(LTRIM(v_ClaimNumber)) AS o_ClaimNumber,
	-- *INF*: RTRIM(LTRIM(claimant_status_type))
	RTRIM(LTRIM(claimant_status_type)) AS o_ClaimOccurrenceStatusCode,
	-- *INF*: -- This field is coded in the last expression transformation
	-- 
	-- ''
	'' AS o_InjuryTypeCode,
	-- *INF*: RTRIM(LTRIM(i_claim_cat_code))
	RTRIM(LTRIM(i_claim_cat_code)) AS o_CatastropheCode,
	'N/A' AS o_CauseOfLoss,
	v_JurisdictionStateCode AS o_JurisdictionStateCode,
	-- *INF*: RTRIM(LTRIM(lkp_body_part_code))
	RTRIM(LTRIM(lkp_body_part_code)) AS o_BodyPartCode,
	-- *INF*: RTRIM(LTRIM(lkp_nature_of_inj_code))
	RTRIM(LTRIM(lkp_nature_of_inj_code)) AS o_NatureOfInjuryCode,
	-- *INF*: RTRIM(LTRIM(lkp_cause_inj_code))
	RTRIM(LTRIM(lkp_cause_inj_code)) AS o_CauseOfInjuryCode,
	-- *INF*: IIF(IN(RTRIM(LTRIM(lkp_type_of_loss_code)),'01','02','03'),RTRIM(LTRIM(lkp_type_of_loss_code)),'01')
	-- 
	IFF(
	    RTRIM(LTRIM(lkp_type_of_loss_code)) IN ('01','02','03'), RTRIM(LTRIM(lkp_type_of_loss_code)),
	    '01'
	) AS o_type_of_loss_code,
	-- *INF*: IIF(IN(i_StateProvinceCode_RiskLocation, '22', '21'), '00', '03')
	IFF(i_StateProvinceCode_RiskLocation IN ('22','21'), '00', '03') AS o_managed_care_org_type,
	-- *INF*: IIF(ISNULL(i_claim_pay_ctgry_lump_sum_ind), 'N',RTRIM(LTRIM(i_claim_pay_ctgry_lump_sum_ind)))
	-- 
	IFF(
	    i_claim_pay_ctgry_lump_sum_ind IS NULL, 'N', RTRIM(LTRIM(i_claim_pay_ctgry_lump_sum_ind))
	) AS o_LumpSumIndicator,
	-- *INF*: IIF(ISNULL(i_EstimatedAuditCode),'N/A',i_EstimatedAuditCode)
	IFF(i_EstimatedAuditCode IS NULL, 'N/A', i_EstimatedAuditCode) AS o_EstimatedAuditCode,
	-- *INF*: RTRIM(LTRIM(i_claim_pay_ctgry_type))
	RTRIM(LTRIM(i_claim_pay_ctgry_type)) AS o_ClaimPayCategoryCode,
	ClaimRelationshipKey,
	SuitHowClaimClosed,
	OccupationDescription,
	WeeklyWageAmount,
	EmployerAttorneyFees
	FROM SQ_WorkWCSTATLoss
	LEFT JOIN LKP_SUP_STATE LKP_SUP_STATE_LTRIM_RTRIM_i_jurisdiction_state_code
	ON LKP_SUP_STATE_LTRIM_RTRIM_i_jurisdiction_state_code.state_code = LTRIM(RTRIM(i_jurisdiction_state_code))

	LEFT JOIN LKP_MULTISTATEPOLICY LKP_MULTISTATEPOLICY_i_pol_ak_id
	ON LKP_MULTISTATEPOLICY_i_pol_ak_id.PolicyAKId = i_pol_ak_id

	LEFT JOIN LKP_CLAIMNUMBER LKP_CLAIMNUMBER_v_ClaimNumber_claim_party_occurrence_ak_id
	ON LKP_CLAIMNUMBER_v_ClaimNumber_claim_party_occurrence_ak_id.Claim_Number = v_ClaimNumber
	AND LKP_CLAIMNUMBER_v_ClaimNumber_claim_party_occurrence_ak_id.claim_party_occurrence_ak_id = claim_party_occurrence_ak_id

),
LKP_WorkWCSTATExtract_Loss AS (
	SELECT
	Exposure,
	CorrectionSeqNumber,
	PolicyKey,
	StateProvinceCode,
	LossMasterClassCode
	FROM (
		SELECT A.Exposure as Exposure, 
		A.CorrectionSeqNumber as CorrectionSeqNumber,
		A.PolicyKey as PolicyKey, 
		A.StateProvinceCode as StateProvinceCode, 
		A.LossMasterClassCode as LossMasterClassCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkWCSTATExtract A
		join (
		SELECT max(LossMasterRunDate) as LossMasterRunDate, 
		max(CorrectionSeqNumber) as CorrectionSeqNumber,
		PolicyKey as PolicyKey, 
		StateProvinceCode as StateProvinceCode, 
		LossMasterClassCode as LossMasterClassCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkWCSTATExtract
		where EDWLossMasterCalculationPKId<>-1
		group by PolicyKey, 
		StateProvinceCode, 
		LossMasterClassCode
		) B
		on A.PolicyKey=B.PolicyKey and A.StateProvinceCode=B.StateProvinceCode and A.LossMasterClassCode=B.LossMasterClassCode
		and A.LossMasterRunDate=B.LossMasterRunDate and A.CorrectionSeqNumber=B.CorrectionSeqNumber
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,StateProvinceCode,LossMasterClassCode ORDER BY Exposure) = 1
),
EXP_GetValueForLoss AS (
	SELECT
	LKP_WorkWCSTATExtract_Loss.Exposure AS i_lkp_Exposure,
	LKP_WorkWCSTATExtract_Loss.CorrectionSeqNumber AS i_lkp_CorrectionSeqNumber,
	EXP_Loss.i_AuditStatus,
	-1 AS EDWPremiumMasterCalculationPKId,
	EXP_Loss.o_loss_master_calculation_id AS EDWLossMasterCalculationPKId,
	EXP_Loss.o_TypeBureauCode AS TypeBureauCode,
	-- *INF*:  TO_DATE('1800-01-01', 'YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS o_premium_master_run_date_loss,
	EXP_Loss.o_LossMasterRunDate AS LossMasterRunDate,
	EXP_Loss.o_BureauCompanyCode AS BureauCompanyCode,
	EXP_Loss.o_PolicyKey AS PolicyKey,
	EXP_Loss.o_StateProvinceCode AS StateProvinceCode,
	EXP_Loss.o_PolicyEffectiveDate AS PolicyEffectiveDate,
	EXP_Loss.o_PolicyEndDate AS PolicyEndDate,
	EXP_Loss.o_InterstateRiskId AS InterstateRiskId,
	EXP_Loss.o_EmployeeLeasingCode AS EmployeeLeasingCode,
	-- *INF*:  TO_DATE('1800-01-01', 'YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS o_state_rating_eff_date_loss,
	EXP_Loss.o_FederalTaxId AS FederalTaxId,
	EXP_Loss.o_ThreeYearFixedRatePolicyIndicator AS ThreeYearFixedRatePolicyIndicator,
	EXP_Loss.o_MultistatePolicyIndicator AS MultistatePolicyIndicator,
	EXP_Loss.o_InterstateRatedPolicyIndicator AS InterstateRatedPolicyIndicator,
	EXP_Loss.o_RetrospectiveRatedPolicyIndicator AS RetrospectiveRatedPolicyIndicator,
	EXP_Loss.o_CancelledMidTermPolicyIndicator AS CancelledMidTermPolicyIndicator,
	EXP_Loss.o_ManagedCareOrganizationPolicyIndicator AS ManagedCareOrganizationPolicyIndicator,
	EXP_Loss.o_TypeOfCoverageIdCode AS TypeOfCoverageIdCode,
	EXP_Loss.o_TypeOfPlan AS TypeOfPlan,
	EXP_Loss.o_DeductibleAmountPerClaimAccident AS DeductibleAmountPerClaimAccident,
	EXP_Loss.o_InsuredName AS InsuredName,
	EXP_Loss.o_WCSTATAddress AS WCSTATAddress,
	'N/A' AS o_premium_master_classcode_loss,
	0 AS o_experience_modiyfactor_loss,
	-- *INF*:  TO_DATE('1800-01-01', 'YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS o_experience_modifyeffdate_loss,
	EXP_Loss.o_Exposure AS Exposure,
	0 AS o_PremiumMasterDirectWrittenPremiumAmount_loss,
	0 AS o_ManualChargedRate_loss,
	EXP_Loss.o_LossMasterClassCode AS LossMasterClassCode,
	EXP_Loss.o_ClaimLossDate AS ClaimLossDate,
	EXP_Loss.i_claim_loss_date AS ClaimLossDate1,
	EXP_Loss.o_ClaimNumber AS ClaimNumber,
	EXP_Loss.o_ClaimOccurrenceStatusCode AS ClaimOccurrenceStatusCode,
	EXP_Loss.o_InjuryTypeCode AS InjuryTypeCode,
	EXP_Loss.o_CatastropheCode AS CatastropheCode,
	EXP_Loss.i_IncurredIndemnityAmount,
	-- *INF*: IIF(ISNULL(i_IncurredIndemnityAmount),0,i_IncurredIndemnityAmount)
	IFF(i_IncurredIndemnityAmount IS NULL, 0, i_IncurredIndemnityAmount) AS v_IncurredIndemnityAmount,
	v_IncurredIndemnityAmount AS o_IncurredIndemnityAmount,
	EXP_Loss.i_IncurredMedicalAmount,
	-- *INF*: IIF(ISNULL(i_IncurredMedicalAmount),0,i_IncurredMedicalAmount)
	IFF(i_IncurredMedicalAmount IS NULL, 0, i_IncurredMedicalAmount) AS v_IncurredMedicalAmount,
	v_IncurredMedicalAmount AS o_IncurredMedicalAmount,
	EXP_Loss.o_CauseOfLoss AS CauseOfLoss,
	EXP_Loss.o_TypeOfRecoveryCode AS TypeOfRecoveryCode,
	EXP_Loss.o_JurisdictionStateCode AS JurisdictionStateCode,
	EXP_Loss.o_BodyPartCode AS BodyPartCode,
	EXP_Loss.o_NatureOfInjuryCode AS NatureOfInjuryCode,
	EXP_Loss.o_CauseOfInjuryCode AS CauseOfInjuryCode,
	EXP_Loss.i_PaidIndemnityAmount,
	-- *INF*: IIF(ISNULL(i_PaidIndemnityAmount),0,i_PaidIndemnityAmount)
	IFF(i_PaidIndemnityAmount IS NULL, 0, i_PaidIndemnityAmount) AS PaidIndemnityAmount,
	EXP_Loss.i_PaidMedicalAmount,
	-- *INF*: IIF(ISNULL(i_PaidMedicalAmount),0,i_PaidMedicalAmount)
	IFF(i_PaidMedicalAmount IS NULL, 0, i_PaidMedicalAmount) AS PaidMedicalAmount,
	EXP_Loss.i_DeductibleReimbursementAmount,
	-- *INF*: IIF(ISNULL(i_DeductibleReimbursementAmount),0,i_DeductibleReimbursementAmount)
	IFF(i_DeductibleReimbursementAmount IS NULL, 0, i_DeductibleReimbursementAmount) AS DeductibleReimbursementAmount,
	EXP_Loss.i_PaidAllocatedLossAdjustmentExpenseAmount,
	-- *INF*: IIF(ISNULL(i_PaidAllocatedLossAdjustmentExpenseAmount),0,i_PaidAllocatedLossAdjustmentExpenseAmount)
	IFF(
	    i_PaidAllocatedLossAdjustmentExpenseAmount IS NULL, 0,
	    i_PaidAllocatedLossAdjustmentExpenseAmount
	) AS PaidAllocatedLossAdjustmentExpenseAmount,
	EXP_Loss.i_IncurredAllocatedLossAdjustmentExpenseAmount,
	-- *INF*: IIF(ISNULL(i_IncurredAllocatedLossAdjustmentExpenseAmount),0,i_IncurredAllocatedLossAdjustmentExpenseAmount)
	IFF(
	    i_IncurredAllocatedLossAdjustmentExpenseAmount IS NULL, 0,
	    i_IncurredAllocatedLossAdjustmentExpenseAmount
	) AS v_IncurredAllocatedLossAdjustmentExpenseAmount,
	v_IncurredAllocatedLossAdjustmentExpenseAmount AS o_IncurredAllocatedLossAdjustmentExpenseAmount,
	EXP_Loss.o_type_of_loss_code AS type_of_loss_code,
	EXP_Loss.SuitHowClaimClosed,
	-- *INF*: DECODE (TRUE,
	-- v_IncurredIndemnityAmount=0 AND v_IncurredMedicalAmount=0 AND v_IncurredAllocatedLossAdjustmentExpenseAmount>=0 AND TypeOfRecoveryCode<>'03'  ,'05',
	-- v_IncurredIndemnityAmount=0 AND v_IncurredMedicalAmount=0 AND v_IncurredAllocatedLossAdjustmentExpenseAmount>0 AND TypeOfRecoveryCode='03' ,'00',
	-- SuitHowClaimClosed='B' OR  SuitHowClaimClosed= 'U','04',
	-- SuitHowClaimClosed='S', '06',
	-- '00')
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --'00'
	DECODE(
	    TRUE,
	    v_IncurredIndemnityAmount = 0 AND v_IncurredMedicalAmount = 0 AND v_IncurredAllocatedLossAdjustmentExpenseAmount >= 0 AND TypeOfRecoveryCode <> '03', '05',
	    v_IncurredIndemnityAmount = 0 AND v_IncurredMedicalAmount = 0 AND v_IncurredAllocatedLossAdjustmentExpenseAmount > 0 AND TypeOfRecoveryCode = '03', '00',
	    SuitHowClaimClosed = 'B' OR SuitHowClaimClosed = 'U', '04',
	    SuitHowClaimClosed = 'S', '06',
	    '00'
	) AS TypeOfSettlement,
	EXP_Loss.o_managed_care_org_type AS managed_care_org_type,
	EXP_Loss.o_LumpSumIndicator AS LumpSumIndicator,
	EXP_Loss.o_EstimatedAuditCode AS EstimatedAuditCode,
	-- *INF*: IIF(IS_NUMBER(i_lkp_CorrectionSeqNumber), TO_INTEGER(i_lkp_CorrectionSeqNumber), 0)
	IFF(
	    REGEXP_LIKE(i_lkp_CorrectionSeqNumber, '^[0-9]+$'),
	    CAST(i_lkp_CorrectionSeqNumber AS INTEGER),
	    0
	) AS v_lkp_CorrectionSeqNumber,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_lkp_Exposure),
	-- '',
	-- i_lkp_Exposure != Exposure AND IN(UPPER(i_AuditStatus), 'COMPLETED', 'ESTIMATED', 'AMENDED', 'CANCELLED'),
	-- TO_CHAR(v_lkp_CorrectionSeqNumber+1),
	-- ''
	-- )
	DECODE(
	    TRUE,
	    i_lkp_Exposure IS NULL, '',
	    i_lkp_Exposure != Exposure AND UPPER(i_AuditStatus) IN ('COMPLETED','ESTIMATED','AMENDED','CANCELLED'), TO_CHAR(v_lkp_CorrectionSeqNumber + 1),
	    ''
	) AS o_CorrectionSeqNumber,
	EXP_Loss.o_ClaimPayCategoryCode AS ClaimPayCategoryCode,
	EXP_Loss.i_claimant_num AS claimant_num,
	EXP_Loss.ClaimRelationshipKey,
	EXP_Loss.claim_party_occurrence_ak_id,
	EXP_Loss.OccupationDescription,
	EXP_Loss.WeeklyWageAmount,
	EXP_Loss.EmployerAttorneyFees
	FROM EXP_Loss
	LEFT JOIN LKP_WorkWCSTATExtract_Loss
	ON LKP_WorkWCSTATExtract_Loss.PolicyKey = EXP_Loss.o_PolicyKey AND LKP_WorkWCSTATExtract_Loss.StateProvinceCode = EXP_Loss.o_StateProvinceCode AND LKP_WorkWCSTATExtract_Loss.LossMasterClassCode = EXP_Loss.o_LossMasterClassCode
),
AGG_summarize_amounts AS (
	SELECT
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	-- *INF*: max(EDWLossMasterCalculationPKId)
	max(EDWLossMasterCalculationPKId) AS o_EDWLossMasterCalculationPKId,
	TypeBureauCode,
	o_premium_master_run_date_loss AS PremiumMasterRunDate,
	LossMasterRunDate,
	BureauCompanyCode,
	PolicyKey,
	StateProvinceCode,
	PolicyEffectiveDate,
	PolicyEndDate,
	InterstateRiskId,
	EmployeeLeasingCode,
	o_state_rating_eff_date_loss AS StateRatingEffectiveDate,
	FederalTaxId,
	ThreeYearFixedRatePolicyIndicator,
	MultistatePolicyIndicator,
	InterstateRatedPolicyIndicator,
	RetrospectiveRatedPolicyIndicator AS RetrospectiveratedPolicyIndicator,
	CancelledMidTermPolicyIndicator,
	ManagedCareOrganizationPolicyIndicator,
	TypeOfCoverageIdCode,
	TypeOfPlan,
	DeductibleAmountPerClaimAccident,
	InsuredName,
	WCSTATAddress,
	o_premium_master_classcode_loss AS PremiumMasterClassCode,
	o_experience_modiyfactor_loss AS ExperienceModificationFactor,
	o_experience_modifyeffdate_loss AS ExperienceModificationEffectiveDate,
	Exposure,
	o_PremiumMasterDirectWrittenPremiumAmount_loss AS PremiumMasterDirectWrittenPremiumAmount,
	o_ManualChargedRate_loss AS ManualChargedRate,
	LossMasterClassCode,
	ClaimLossDate,
	ClaimLossDate1,
	ClaimNumber,
	ClaimOccurrenceStatusCode,
	InjuryTypeCode,
	-- *INF*: max(InjuryTypeCode)
	max(InjuryTypeCode) AS out_InjuryTypeCode,
	CatastropheCode,
	o_IncurredIndemnityAmount AS IncurredIndemnityAmount,
	-- *INF*: SUM(IncurredIndemnityAmount)
	SUM(IncurredIndemnityAmount) AS o_IncurredIndemnityAmount,
	o_IncurredMedicalAmount AS IncurredMedicalAmount,
	-- *INF*: SUM(IncurredMedicalAmount)
	SUM(IncurredMedicalAmount) AS o_IncurredMedicalAmount,
	CauseOfLoss,
	TypeOfRecoveryCode,
	JurisdictionStateCode,
	BodyPartCode,
	NatureOfInjuryCode,
	CauseOfInjuryCode,
	PaidIndemnityAmount,
	-- *INF*: SUM(PaidIndemnityAmount)
	SUM(PaidIndemnityAmount) AS o_PaidIndemnityAmount,
	PaidMedicalAmount,
	-- *INF*: SUM(PaidMedicalAmount)
	SUM(PaidMedicalAmount) AS o_PaidMedicalAmount,
	DeductibleReimbursementAmount,
	-- *INF*: SUM(DeductibleReimbursementAmount)
	SUM(DeductibleReimbursementAmount) AS o_DeductibleReimbursementAmount,
	PaidAllocatedLossAdjustmentExpenseAmount,
	-- *INF*: SUM(PaidAllocatedLossAdjustmentExpenseAmount)
	SUM(PaidAllocatedLossAdjustmentExpenseAmount) AS o_PaidAllocatedLossAdjustmentExpenseAmount,
	o_IncurredAllocatedLossAdjustmentExpenseAmount AS IncurredAllocatedLossAdjustmentExpenseAmount,
	-- *INF*: SUM(IncurredAllocatedLossAdjustmentExpenseAmount)
	SUM(IncurredAllocatedLossAdjustmentExpenseAmount) AS o_IncurredAllocatedLossAdjustmentExpenseAmount,
	ClaimPayCategoryCode,
	-- *INF*: MIN(ClaimPayCategoryCode)
	MIN(ClaimPayCategoryCode) AS o_ClaimPayCategoryCode,
	type_of_loss_code,
	TypeOfSettlement,
	managed_care_org_type AS ManagedCareOrganizationType,
	LumpSumIndicator,
	-- *INF*: MAX(LumpSumIndicator)
	MAX(LumpSumIndicator) AS o_LumpSumIndicator,
	EstimatedAuditCode,
	o_CorrectionSeqNumber AS CorrectionSeqNumber,
	claimant_num,
	ClaimRelationshipKey,
	claim_party_occurrence_ak_id,
	OccupationDescription,
	WeeklyWageAmount,
	EmployerAttorneyFees
	FROM EXP_GetValueForLoss
	GROUP BY EDWPremiumMasterCalculationPKId, TypeBureauCode, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyCode, PolicyKey, StateProvinceCode, PolicyEffectiveDate, PolicyEndDate, InterstateRiskId, EmployeeLeasingCode, StateRatingEffectiveDate, FederalTaxId, ThreeYearFixedRatePolicyIndicator, MultistatePolicyIndicator, InterstateRatedPolicyIndicator, RetrospectiveratedPolicyIndicator, CancelledMidTermPolicyIndicator, ManagedCareOrganizationPolicyIndicator, TypeOfCoverageIdCode, TypeOfPlan, DeductibleAmountPerClaimAccident, InsuredName, WCSTATAddress, PremiumMasterClassCode, ExperienceModificationFactor, ExperienceModificationEffectiveDate, Exposure, PremiumMasterDirectWrittenPremiumAmount, ManualChargedRate, LossMasterClassCode, ClaimLossDate, ClaimNumber, ClaimOccurrenceStatusCode, CatastropheCode, CauseOfLoss, TypeOfRecoveryCode, JurisdictionStateCode, BodyPartCode, NatureOfInjuryCode, CauseOfInjuryCode, type_of_loss_code, TypeOfSettlement, ManagedCareOrganizationType, EstimatedAuditCode, CorrectionSeqNumber, claimant_num
),
FIL_Filter_Transacitons AS (
	SELECT
	EDWPremiumMasterCalculationPKId AS EDWPremiumMasterCalculationPKId3, 
	o_EDWLossMasterCalculationPKId AS EDWLossMasterCalculationPKId3, 
	TypeBureauCode AS TypeBureauCode2, 
	PremiumMasterRunDate AS PremiumMasterRunDate2, 
	LossMasterRunDate AS LossMasterRunDate2, 
	BureauCompanyCode AS BureauCompanyCode2, 
	PolicyKey AS PolicyKey2, 
	StateProvinceCode AS StateProvinceCode2, 
	PolicyEffectiveDate AS PolicyEffectiveDate2, 
	PolicyEndDate AS PolicyEndDate2, 
	InterstateRiskId AS InterstateRiskId2, 
	EmployeeLeasingCode AS EmployeeLeasingCode2, 
	StateRatingEffectiveDate AS StateRatingEffectiveDate2, 
	FederalTaxId AS FederalTaxId2, 
	ThreeYearFixedRatePolicyIndicator AS ThreeYearFixedRatePolicyIndicator2, 
	MultistatePolicyIndicator AS MultistatePolicyIndicator2, 
	InterstateRatedPolicyIndicator AS InterstateRatedPolicyIndicator2, 
	RetrospectiveratedPolicyIndicator AS RetrospectiveratedPolicyIndicator2, 
	CancelledMidTermPolicyIndicator AS CancelledMidTermPolicyIndicator2, 
	ManagedCareOrganizationPolicyIndicator AS ManagedCareOrganizationPolicyIndicator2, 
	TypeOfCoverageIdCode AS TypeOfCoverageIdCode2, 
	TypeOfPlan AS TypeOfPlan2, 
	DeductibleAmountPerClaimAccident AS DeductibleAmountPerClaimAccident2, 
	InsuredName AS InsuredName2, 
	WCSTATAddress AS WCSTATAddress2, 
	PremiumMasterClassCode AS PremiumMasterClassCode2, 
	ExperienceModificationFactor AS ExperienceModificationFactor2, 
	ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate2, 
	Exposure AS Exposure2, 
	PremiumMasterDirectWrittenPremiumAmount AS PremiumMasterDirectWrittenPremiumAmount2, 
	ManualChargedRate AS ManualChargedRate2, 
	LossMasterClassCode AS LossMasterClassCode2, 
	ClaimLossDate AS ClaimLossDate2, 
	ClaimLossDate1 AS ClaimLossDate21, 
	ClaimOccurrenceStatusCode AS ClaimOccurrenceStatusCode2, 
	out_InjuryTypeCode AS InjuryTypeCode2, 
	CatastropheCode AS CatastropheCode2, 
	o_IncurredIndemnityAmount AS IncurredIndemnityAmount2, 
	o_IncurredMedicalAmount AS IncurredMedicalAmount2, 
	CauseOfLoss AS CauseOfLoss2, 
	TypeOfRecoveryCode AS TypeOfRecoveryCode2, 
	JurisdictionStateCode AS JurisdictionStateCode2, 
	BodyPartCode AS BodyPartCode2, 
	NatureOfInjuryCode AS NatureOfInjuryCode2, 
	CauseOfInjuryCode AS CauseOfInjuryCode2, 
	o_PaidIndemnityAmount AS PaidIndemnityAmount2, 
	o_PaidMedicalAmount AS PaidMedicalAmount2, 
	o_DeductibleReimbursementAmount AS DeductibleReimbursementAmount2, 
	o_PaidAllocatedLossAdjustmentExpenseAmount AS PaidAllocatedLossAdjustmentExpenseAmount2, 
	o_IncurredAllocatedLossAdjustmentExpenseAmount AS IncurredAllocatedLossAdjustmentExpenseAmount2, 
	type_of_loss_code AS type_of_loss_code3, 
	TypeOfSettlement AS TypeOfSettlement3, 
	ManagedCareOrganizationType AS ManagedCareOrganizationType3, 
	o_LumpSumIndicator AS LumpSumIndicator3, 
	EstimatedAuditCode AS EstimatedAuditCode3, 
	CorrectionSeqNumber AS CorrectionSeqNumber3, 
	ClaimNumber AS ClaimNumber2, 
	o_ClaimPayCategoryCode AS ClaimPayCategoryCode, 
	claimant_num, 
	ClaimRelationshipKey, 
	claim_party_occurrence_ak_id, 
	OccupationDescription, 
	WeeklyWageAmount, 
	EmployerAttorneyFees
	FROM AGG_summarize_amounts
	WHERE TypeOfRecoveryCode2 !='N/A' AND NOT (IncurredAllocatedLossAdjustmentExpenseAmount2 = 0 and IncurredMedicalAmount2=0 and IncurredIndemnityAmount2= 0 and PaidIndemnityAmount2 = 0 and PaidMedicalAmount2= 0 and PaidAllocatedLossAdjustmentExpenseAmount2=0  and IncurredAllocatedLossAdjustmentExpenseAmount2 = 0)
),
SRT_Sort_Transactions AS (
	SELECT
	EDWPremiumMasterCalculationPKId3, 
	EDWLossMasterCalculationPKId3, 
	TypeBureauCode2, 
	PremiumMasterRunDate2, 
	LossMasterRunDate2, 
	BureauCompanyCode2, 
	PolicyKey2, 
	StateProvinceCode2, 
	PolicyEffectiveDate2, 
	PolicyEndDate2, 
	InterstateRiskId2, 
	EmployeeLeasingCode2, 
	StateRatingEffectiveDate2, 
	FederalTaxId2, 
	ThreeYearFixedRatePolicyIndicator2, 
	MultistatePolicyIndicator2, 
	InterstateRatedPolicyIndicator2, 
	RetrospectiveratedPolicyIndicator2, 
	CancelledMidTermPolicyIndicator2, 
	ManagedCareOrganizationPolicyIndicator2, 
	TypeOfCoverageIdCode2, 
	TypeOfPlan2, 
	DeductibleAmountPerClaimAccident2, 
	InsuredName2, 
	WCSTATAddress2, 
	PremiumMasterClassCode2, 
	ExperienceModificationFactor2, 
	ExperienceModificationEffectiveDate2, 
	Exposure2, 
	PremiumMasterDirectWrittenPremiumAmount2, 
	ManualChargedRate2, 
	LossMasterClassCode2, 
	ClaimLossDate2, 
	ClaimLossDate21, 
	ClaimOccurrenceStatusCode2, 
	InjuryTypeCode2, 
	CatastropheCode2, 
	IncurredIndemnityAmount2, 
	IncurredMedicalAmount2, 
	CauseOfLoss2, 
	TypeOfRecoveryCode2, 
	JurisdictionStateCode2, 
	BodyPartCode2, 
	NatureOfInjuryCode2, 
	CauseOfInjuryCode2, 
	PaidIndemnityAmount2, 
	PaidMedicalAmount2, 
	DeductibleReimbursementAmount2, 
	PaidAllocatedLossAdjustmentExpenseAmount2, 
	IncurredAllocatedLossAdjustmentExpenseAmount2, 
	type_of_loss_code3, 
	TypeOfSettlement3, 
	ManagedCareOrganizationType3, 
	LumpSumIndicator3, 
	EstimatedAuditCode3, 
	CorrectionSeqNumber3, 
	ClaimNumber2, 
	ClaimPayCategoryCode, 
	claimant_num, 
	ClaimRelationshipKey, 
	claim_party_occurrence_ak_id, 
	OccupationDescription, 
	WeeklyWageAmount, 
	EmployerAttorneyFees
	FROM FIL_Filter_Transacitons
	ORDER BY PolicyKey2 ASC, ClaimLossDate2 ASC, ClaimLossDate21 ASC, ClaimNumber2 ASC, claimant_num DESC, ClaimRelationshipKey ASC
),
EXP_CatastropheCodeCalc AS (
	SELECT
	EDWPremiumMasterCalculationPKId3,
	EDWLossMasterCalculationPKId3,
	TypeBureauCode2,
	PremiumMasterRunDate2,
	LossMasterRunDate2,
	BureauCompanyCode2,
	PolicyKey2 AS PolicyKey,
	StateProvinceCode2,
	PolicyEffectiveDate2,
	PolicyEndDate2,
	InterstateRiskId2,
	EmployeeLeasingCode2,
	StateRatingEffectiveDate2,
	FederalTaxId2,
	ThreeYearFixedRatePolicyIndicator2,
	MultistatePolicyIndicator2,
	InterstateRatedPolicyIndicator2,
	RetrospectiveratedPolicyIndicator2,
	CancelledMidTermPolicyIndicator2,
	ManagedCareOrganizationPolicyIndicator2,
	TypeOfCoverageIdCode2,
	TypeOfPlan2,
	DeductibleAmountPerClaimAccident2,
	InsuredName2,
	WCSTATAddress2,
	PremiumMasterClassCode2,
	ExperienceModificationFactor2,
	ExperienceModificationEffectiveDate2,
	Exposure2,
	PremiumMasterDirectWrittenPremiumAmount2,
	ManualChargedRate2,
	LossMasterClassCode2,
	ClaimLossDate2 AS O_ClaimLossDate1,
	ClaimLossDate21 AS ClaimLossDate,
	ClaimOccurrenceStatusCode2,
	InjuryTypeCode2,
	CatastropheCode2,
	IncurredIndemnityAmount2,
	IncurredMedicalAmount2,
	CauseOfLoss2,
	TypeOfRecoveryCode2,
	JurisdictionStateCode2,
	BodyPartCode2,
	NatureOfInjuryCode2,
	CauseOfInjuryCode2,
	PaidIndemnityAmount2,
	PaidMedicalAmount2,
	DeductibleReimbursementAmount2,
	PaidAllocatedLossAdjustmentExpenseAmount2,
	IncurredAllocatedLossAdjustmentExpenseAmount2,
	type_of_loss_code3,
	TypeOfSettlement3,
	ManagedCareOrganizationType3,
	LumpSumIndicator3,
	EstimatedAuditCode3,
	CorrectionSeqNumber3,
	ClaimNumber2 AS ClaimNumber,
	ClaimPayCategoryCode,
	claimant_num,
	ClaimRelationshipKey,
	-- *INF*: to_char(get_date_part(ClaimLossDate,'YYYY'))||to_char(get_date_part(ClaimLossDate,'MM'))||to_char(get_date_part(ClaimLossDate,'DD')) 
	-- --TO_CHAR(TO_DATE(SUBSTR(ClaimLossDate,1,10),'MMDDYYYY') )
	to_char(DATE_PART(ClaimLossDate, 'YYYY')) || to_char(DATE_PART(ClaimLossDate, 'MM')) || to_char(DATE_PART(ClaimLossDate, 'DD')) AS V_GetClaimLossDate,
	-- *INF*: to_char(get_date_part(ClaimLossDate,'HH24'))||to_char(get_date_part(ClaimLossDate,'MI'))||to_char(get_date_part(ClaimLossDate,'SS'))
	to_char(DATE_PART(ClaimLossDate, 'HH24')) || to_char(DATE_PART(ClaimLossDate, 'MI')) || to_char(DATE_PART(ClaimLossDate, 'SS')) AS V_GetClaimLossDateTime,
	V_GetClaimLossDate AS V_ClaimLossDate,
	V_GetClaimLossDateTime AS V_ClaimLossDateTime,
	-- *INF*:  IIF(PolicyKey=PrevPolicyKey AND V_GetClaimLossDate=PrevClaimLossDate 
	--  AND V_ClaimLossDateTime=PrevClaimLossDateTime,
	--  IIF(ClaimNumber=PrevClaimNumber and claimant_num<>Prevclaimant_num,V_AssignCount_claimant_num+1,
	-- --IIF(ClaimNumber<>PrevClaimNumber and claimant_num<>
	-- --Prevclaimant_num,V_AssignCount_claimant_num+1)
	-- 0),0)
	-- 
	-- --Get the count and assign the incremental number to each claim based on the ClaimantNumber key per above logic if it is with same policy,claimlossdate,claimnumber otherwise assign 0.
	IFF(
	    PolicyKey = PrevPolicyKey
	    and V_GetClaimLossDate = PrevClaimLossDate
	    and V_ClaimLossDateTime = PrevClaimLossDateTime,
	    IFF(
	        ClaimNumber = PrevClaimNumber
	    and claimant_num <> Prevclaimant_num,
	        V_AssignCount_claimant_num + 1,
	        0
	    ),
	    0
	) AS V_AssignCount_claimant_num,
	-- *INF*: IIF(V_AssignCount_claimant_num<1,V_Assign_GroupCount_claimant_num+1,V_Assign_GroupCount_claimant_num)
	-- 
	-- --Assign the Claimant_number group count incremental number to each match group.
	IFF(
	    V_AssignCount_claimant_num < 1, V_Assign_GroupCount_claimant_num + 1,
	    V_Assign_GroupCount_claimant_num
	) AS V_Assign_GroupCount_claimant_num,
	-- *INF*:  IIF(ClaimRelationshipKey<>'N/A',IIF(PolicyKey=PrevPolicyKey 
	-- --AND V_GetClaimLossDate=PrevClaimLossDate AND V_ClaimLossDateTime=PrevClaimLossDateTime
	-- ,IIF(ClaimRelationshipKey=PrevClaimRelationshipKey AND ClaimNumber<>PrevClaimNumber,V_AssignCount_ClaimRelKey+1,
	-- --IIF(ClaimRelationshipKey<>----PrevClaimRelationshipKey,V_AssignCount_ClaimRelKey+1)
	-- 0)),0)
	-- 
	-- --Get the count and assign the incremental number to each claim based on the ClaimRelationship key per above logic if it is with same policy,claimlossdate,claimnumber otherwise assign 0.
	IFF(
	    ClaimRelationshipKey <> 'N/A',
	    IFF(
	        PolicyKey = PrevPolicyKey,
	        IFF(
	            ClaimRelationshipKey = PrevClaimRelationshipKey
	        and ClaimNumber <> PrevClaimNumber,
	            V_AssignCount_ClaimRelKey + 1,
	            0
	        )
	    ),
	    0
	) AS V_AssignCount_ClaimRelKey,
	-- *INF*: IIF(V_AssignCount_ClaimRelKey<1,V_Assign_GroupCount_ClaimRelKey+1,V_Assign_GroupCount_ClaimRelKey)
	-- 
	-- ----Assign the ClaimRelationship group count incremental number to each match group.
	IFF(
	    V_AssignCount_ClaimRelKey < 1, V_Assign_GroupCount_ClaimRelKey + 1,
	    V_Assign_GroupCount_ClaimRelKey
	) AS V_Assign_GroupCount_ClaimRelKey,
	-- *INF*: DECODE(TRUE,ClaimRelationshipKey='N/A',V_AssignCount_claimant_num,V_AssignCount_ClaimRelKey)
	-- 
	-- 
	-- --Get the final incremental count for each Claimant_number and ClaimRelationshipKey
	DECODE(
	    TRUE,
	    ClaimRelationshipKey = 'N/A', V_AssignCount_claimant_num,
	    V_AssignCount_ClaimRelKey
	) AS V_Get_CommonCount,
	-- *INF*: IIF(V_Get_CommonCount<1,V_Get_CommonGroupCount+1,V_Get_CommonGroupCount)
	-- 
	-- --Get the final Group count for claimnut num and claimRelationshipkey
	IFF(V_Get_CommonCount < 1, V_Get_CommonGroupCount + 1, V_Get_CommonGroupCount) AS V_Get_CommonGroupCount,
	V_Get_CommonCount AS Out_CommonCount,
	V_Get_CommonGroupCount AS Out_CommonGroupCount,
	V_GetClaimLossDate AS PrevClaimLossDate,
	V_GetClaimLossDateTime AS PrevClaimLossDateTime,
	PolicyKey AS PrevPolicyKey,
	ClaimNumber AS PrevClaimNumber,
	claimant_num AS Prevclaimant_num,
	ClaimRelationshipKey AS PrevClaimRelationshipKey,
	claim_party_occurrence_ak_id,
	OccupationDescription,
	WeeklyWageAmount,
	EmployerAttorneyFees
	FROM SRT_Sort_Transactions
),
AGG_Count_Groups_with_multiple_claims AS (
	SELECT
	PolicyKey,
	Out_CommonGroupCount,
	Out_CommonCount,
	-- *INF*: COUNT(Out_CommonCount)
	COUNT(Out_CommonCount) AS TotalCommonCount
	FROM EXP_CatastropheCodeCalc
	GROUP BY PolicyKey, Out_CommonGroupCount
),
JNR_AGG_To_Flow AS (SELECT
	AGG_Count_Groups_with_multiple_claims.PolicyKey AS PolicyKey_AGG, 
	AGG_Count_Groups_with_multiple_claims.TotalCommonCount, 
	AGG_Count_Groups_with_multiple_claims.Out_CommonGroupCount AS Out_CommonGroupCount_AGG, 
	EXP_CatastropheCodeCalc.EDWPremiumMasterCalculationPKId3, 
	EXP_CatastropheCodeCalc.EDWLossMasterCalculationPKId3, 
	EXP_CatastropheCodeCalc.TypeBureauCode2, 
	EXP_CatastropheCodeCalc.PremiumMasterRunDate2, 
	EXP_CatastropheCodeCalc.LossMasterRunDate2, 
	EXP_CatastropheCodeCalc.BureauCompanyCode2, 
	EXP_CatastropheCodeCalc.PolicyKey, 
	EXP_CatastropheCodeCalc.StateProvinceCode2, 
	EXP_CatastropheCodeCalc.PolicyEffectiveDate2, 
	EXP_CatastropheCodeCalc.PolicyEndDate2, 
	EXP_CatastropheCodeCalc.InterstateRiskId2, 
	EXP_CatastropheCodeCalc.EmployeeLeasingCode2, 
	EXP_CatastropheCodeCalc.StateRatingEffectiveDate2, 
	EXP_CatastropheCodeCalc.FederalTaxId2, 
	EXP_CatastropheCodeCalc.ThreeYearFixedRatePolicyIndicator2, 
	EXP_CatastropheCodeCalc.MultistatePolicyIndicator2, 
	EXP_CatastropheCodeCalc.InterstateRatedPolicyIndicator2, 
	EXP_CatastropheCodeCalc.RetrospectiveratedPolicyIndicator2, 
	EXP_CatastropheCodeCalc.CancelledMidTermPolicyIndicator2, 
	EXP_CatastropheCodeCalc.ManagedCareOrganizationPolicyIndicator2, 
	EXP_CatastropheCodeCalc.TypeOfCoverageIdCode2, 
	EXP_CatastropheCodeCalc.TypeOfPlan2, 
	EXP_CatastropheCodeCalc.DeductibleAmountPerClaimAccident2, 
	EXP_CatastropheCodeCalc.InsuredName2, 
	EXP_CatastropheCodeCalc.WCSTATAddress2, 
	EXP_CatastropheCodeCalc.PremiumMasterClassCode2, 
	EXP_CatastropheCodeCalc.ExperienceModificationFactor2, 
	EXP_CatastropheCodeCalc.ExperienceModificationEffectiveDate2, 
	EXP_CatastropheCodeCalc.Exposure2, 
	EXP_CatastropheCodeCalc.PremiumMasterDirectWrittenPremiumAmount2, 
	EXP_CatastropheCodeCalc.ManualChargedRate2, 
	EXP_CatastropheCodeCalc.LossMasterClassCode2, 
	EXP_CatastropheCodeCalc.O_ClaimLossDate1, 
	EXP_CatastropheCodeCalc.ClaimLossDate, 
	EXP_CatastropheCodeCalc.ClaimOccurrenceStatusCode2, 
	EXP_CatastropheCodeCalc.InjuryTypeCode2, 
	EXP_CatastropheCodeCalc.CatastropheCode2, 
	EXP_CatastropheCodeCalc.IncurredIndemnityAmount2, 
	EXP_CatastropheCodeCalc.IncurredMedicalAmount2, 
	EXP_CatastropheCodeCalc.CauseOfLoss2, 
	EXP_CatastropheCodeCalc.TypeOfRecoveryCode2, 
	EXP_CatastropheCodeCalc.JurisdictionStateCode2, 
	EXP_CatastropheCodeCalc.BodyPartCode2, 
	EXP_CatastropheCodeCalc.NatureOfInjuryCode2, 
	EXP_CatastropheCodeCalc.CauseOfInjuryCode2, 
	EXP_CatastropheCodeCalc.PaidIndemnityAmount2, 
	EXP_CatastropheCodeCalc.PaidMedicalAmount2, 
	EXP_CatastropheCodeCalc.DeductibleReimbursementAmount2, 
	EXP_CatastropheCodeCalc.PaidAllocatedLossAdjustmentExpenseAmount2, 
	EXP_CatastropheCodeCalc.IncurredAllocatedLossAdjustmentExpenseAmount2, 
	EXP_CatastropheCodeCalc.type_of_loss_code3, 
	EXP_CatastropheCodeCalc.TypeOfSettlement3, 
	EXP_CatastropheCodeCalc.ManagedCareOrganizationType3, 
	EXP_CatastropheCodeCalc.LumpSumIndicator3, 
	EXP_CatastropheCodeCalc.EstimatedAuditCode3, 
	EXP_CatastropheCodeCalc.CorrectionSeqNumber3, 
	EXP_CatastropheCodeCalc.ClaimNumber, 
	EXP_CatastropheCodeCalc.ClaimPayCategoryCode, 
	EXP_CatastropheCodeCalc.claimant_num, 
	EXP_CatastropheCodeCalc.ClaimRelationshipKey, 
	EXP_CatastropheCodeCalc.Out_CommonCount, 
	EXP_CatastropheCodeCalc.Out_CommonGroupCount, 
	EXP_CatastropheCodeCalc.claim_party_occurrence_ak_id, 
	EXP_CatastropheCodeCalc.OccupationDescription, 
	EXP_CatastropheCodeCalc.WeeklyWageAmount, 
	EXP_CatastropheCodeCalc.EmployerAttorneyFees
	FROM AGG_Count_Groups_with_multiple_claims
	INNER JOIN EXP_CatastropheCodeCalc
	ON EXP_CatastropheCodeCalc.PolicyKey = AGG_Count_Groups_with_multiple_claims.PolicyKey AND EXP_CatastropheCodeCalc.Out_CommonGroupCount = AGG_Count_Groups_with_multiple_claims.Out_CommonGroupCount
),
EXP_Group_Count_to_NULL AS (
	SELECT
	EDWPremiumMasterCalculationPKId3,
	EDWLossMasterCalculationPKId3,
	TypeBureauCode2,
	PremiumMasterRunDate2,
	LossMasterRunDate2,
	BureauCompanyCode2,
	PolicyKey AS PolicyKey1,
	StateProvinceCode2,
	PolicyEffectiveDate2,
	PolicyEndDate2,
	InterstateRiskId2,
	EmployeeLeasingCode2,
	StateRatingEffectiveDate2,
	FederalTaxId2,
	ThreeYearFixedRatePolicyIndicator2,
	MultistatePolicyIndicator2,
	InterstateRatedPolicyIndicator2,
	RetrospectiveratedPolicyIndicator2,
	CancelledMidTermPolicyIndicator2,
	ManagedCareOrganizationPolicyIndicator2,
	TypeOfCoverageIdCode2,
	TypeOfPlan2,
	DeductibleAmountPerClaimAccident2,
	InsuredName2,
	WCSTATAddress2,
	PremiumMasterClassCode2,
	ExperienceModificationFactor2,
	ExperienceModificationEffectiveDate2,
	Exposure2,
	PremiumMasterDirectWrittenPremiumAmount2,
	ManualChargedRate2,
	LossMasterClassCode2,
	O_ClaimLossDate1,
	ClaimLossDate AS ClaimLossDate1,
	ClaimOccurrenceStatusCode2,
	InjuryTypeCode2,
	CatastropheCode2,
	IncurredIndemnityAmount2,
	IncurredMedicalAmount2,
	CauseOfLoss2,
	TypeOfRecoveryCode2,
	JurisdictionStateCode2,
	BodyPartCode2,
	NatureOfInjuryCode2,
	CauseOfInjuryCode2,
	PaidIndemnityAmount2,
	PaidMedicalAmount2,
	DeductibleReimbursementAmount2,
	PaidAllocatedLossAdjustmentExpenseAmount2,
	IncurredAllocatedLossAdjustmentExpenseAmount2,
	type_of_loss_code3,
	TypeOfSettlement3,
	ManagedCareOrganizationType3,
	LumpSumIndicator3,
	EstimatedAuditCode3,
	CorrectionSeqNumber3,
	ClaimNumber AS ClaimNumber1,
	ClaimPayCategoryCode,
	claimant_num AS claimant_num1,
	ClaimRelationshipKey AS ClaimRelationshipKey1,
	PolicyKey_AGG,
	TotalCommonCount,
	Out_CommonGroupCount_AGG AS in_CommonGroupCount_AGG,
	-- *INF*: DECODE(TRUE, TotalCommonCount = 1, NULL, in_CommonGroupCount_AGG)
	DECODE(
	    TRUE,
	    TotalCommonCount = 1, NULL,
	    in_CommonGroupCount_AGG
	) AS Out_CommonGroupCount_AGG,
	Out_CommonGroupCount,
	Out_CommonCount,
	claim_party_occurrence_ak_id,
	OccupationDescription,
	WeeklyWageAmount,
	EmployerAttorneyFees
	FROM JNR_AGG_To_Flow
),
SRT_CatastropheInput AS (
	SELECT
	EDWPremiumMasterCalculationPKId3, 
	EDWLossMasterCalculationPKId3, 
	TypeBureauCode2, 
	PremiumMasterRunDate2, 
	LossMasterRunDate2, 
	BureauCompanyCode2, 
	PolicyKey1, 
	StateProvinceCode2, 
	PolicyEffectiveDate2, 
	PolicyEndDate2, 
	InterstateRiskId2, 
	EmployeeLeasingCode2, 
	StateRatingEffectiveDate2, 
	FederalTaxId2, 
	ThreeYearFixedRatePolicyIndicator2, 
	MultistatePolicyIndicator2, 
	InterstateRatedPolicyIndicator2, 
	RetrospectiveratedPolicyIndicator2, 
	CancelledMidTermPolicyIndicator2, 
	ManagedCareOrganizationPolicyIndicator2, 
	TypeOfCoverageIdCode2, 
	TypeOfPlan2, 
	DeductibleAmountPerClaimAccident2, 
	InsuredName2, 
	WCSTATAddress2, 
	PremiumMasterClassCode2, 
	ExperienceModificationFactor2, 
	ExperienceModificationEffectiveDate2, 
	Exposure2, 
	PremiumMasterDirectWrittenPremiumAmount2, 
	ManualChargedRate2, 
	LossMasterClassCode2, 
	O_ClaimLossDate1, 
	ClaimLossDate1, 
	ClaimOccurrenceStatusCode2, 
	InjuryTypeCode2, 
	CatastropheCode2, 
	IncurredIndemnityAmount2, 
	IncurredMedicalAmount2, 
	CauseOfLoss2, 
	TypeOfRecoveryCode2, 
	JurisdictionStateCode2, 
	BodyPartCode2, 
	NatureOfInjuryCode2, 
	CauseOfInjuryCode2, 
	PaidIndemnityAmount2, 
	PaidMedicalAmount2, 
	DeductibleReimbursementAmount2, 
	PaidAllocatedLossAdjustmentExpenseAmount2, 
	IncurredAllocatedLossAdjustmentExpenseAmount2, 
	type_of_loss_code3, 
	TypeOfSettlement3, 
	ManagedCareOrganizationType3, 
	LumpSumIndicator3, 
	EstimatedAuditCode3, 
	CorrectionSeqNumber3, 
	ClaimNumber1, 
	ClaimPayCategoryCode, 
	claimant_num1, 
	ClaimRelationshipKey1, 
	PolicyKey_AGG, 
	TotalCommonCount, 
	Out_CommonGroupCount_AGG, 
	Out_CommonGroupCount, 
	Out_CommonCount, 
	claim_party_occurrence_ak_id, 
	OccupationDescription, 
	WeeklyWageAmount, 
	EmployerAttorneyFees
	FROM EXP_Group_Count_to_NULL
	ORDER BY PolicyKey1 ASC, Out_CommonGroupCount_AGG ASC, Out_CommonGroupCount ASC, Out_CommonCount DESC
),
EXP_GetCatastropheCode AS (
	SELECT
	EDWPremiumMasterCalculationPKId3,
	EDWLossMasterCalculationPKId3,
	TypeBureauCode2,
	PremiumMasterRunDate2,
	LossMasterRunDate2,
	BureauCompanyCode2,
	PolicyKey1,
	StateProvinceCode2,
	PolicyEffectiveDate2,
	PolicyEndDate2,
	InterstateRiskId2,
	EmployeeLeasingCode2,
	StateRatingEffectiveDate2,
	FederalTaxId2,
	ThreeYearFixedRatePolicyIndicator2,
	MultistatePolicyIndicator2,
	InterstateRatedPolicyIndicator2,
	RetrospectiveratedPolicyIndicator2,
	CancelledMidTermPolicyIndicator2,
	ManagedCareOrganizationPolicyIndicator2,
	TypeOfCoverageIdCode2,
	TypeOfPlan2,
	DeductibleAmountPerClaimAccident2,
	InsuredName2,
	WCSTATAddress2,
	PremiumMasterClassCode2,
	ExperienceModificationFactor2,
	ExperienceModificationEffectiveDate2,
	Exposure2,
	PremiumMasterDirectWrittenPremiumAmount2,
	ManualChargedRate2,
	LossMasterClassCode2,
	O_ClaimLossDate1,
	ClaimLossDate1,
	ClaimOccurrenceStatusCode2,
	InjuryTypeCode2,
	CatastropheCode2,
	IncurredIndemnityAmount2,
	IncurredMedicalAmount2,
	CauseOfLoss2,
	TypeOfRecoveryCode2,
	JurisdictionStateCode2,
	BodyPartCode2,
	NatureOfInjuryCode2,
	CauseOfInjuryCode2,
	PaidIndemnityAmount2,
	PaidMedicalAmount2,
	DeductibleReimbursementAmount2,
	PaidAllocatedLossAdjustmentExpenseAmount2,
	IncurredAllocatedLossAdjustmentExpenseAmount2,
	type_of_loss_code3,
	TypeOfSettlement3,
	ManagedCareOrganizationType3,
	LumpSumIndicator3,
	EstimatedAuditCode3,
	CorrectionSeqNumber3,
	ClaimNumber1,
	ClaimPayCategoryCode,
	claimant_num1,
	Out_CommonCount,
	Out_CommonGroupCount,
	TotalCommonCount,
	-- *INF*: DECODE(TRUE,
	-- TO_INTEGER(CatastropheCode2)>10,TO_INTEGER(CatastropheCode2),
	-- 
	-- DECODE(TRUE,PolicyKey1=V_PrevPolicyKey and TotalCommonCount > 1,
	-- DECODE(TRUE, Out_CommonGroupCount=V_PrevCommonGroupCount, 
	-- V_PrevGetCataStrophe, V_PrevGetCataStrophe + 1),
	-- PolicyKey1 <>  V_PrevPolicyKey and TotalCommonCount > 1, 01,
	-- 00))
	-- --Removed as part of PROD-12901 
	-- --IIF(Out_CommonCount>=1,1,IIF(Out_CommonGroupCount=V_PrevCommonGroupCount,1,00))
	-- 
	-- --IIF(Count_claimnum1>=1,01,IIF(Count_claimnum2=PrevCount_claimnum2,01,00))
	-- 
	-- --If the commoncount is >1 it means first record has the same policy asssociated to assign the catastrophe.
	-- 
	DECODE(
	    TRUE,
	    CAST(CatastropheCode2 AS INTEGER) > 10, CAST(CatastropheCode2 AS INTEGER),
	    DECODE(
	        TRUE,
	        PolicyKey1 = V_PrevPolicyKey and TotalCommonCount > 1, DECODE(
	            TRUE,
	            Out_CommonGroupCount = V_PrevCommonGroupCount, V_PrevGetCataStrophe,
	            V_PrevGetCataStrophe + 1
	        ),
	        PolicyKey1 <> V_PrevPolicyKey and TotalCommonCount > 1, 01,
	        00
	    )
	) AS V_AssignCataStrophe_Other_State,
	-- *INF*: IIF(StateProvinceCode2='21',IIF(IncurredIndemnityAmount2+IncurredMedicalAmount2=
	-- 20000,V_AssignCataStrophe_Other_State,00),V_AssignCataStrophe_Other_State)
	-- 
	-- -- if state is Michigan and amount > @{pipeline().parameters.20000} then assign catastrophe code otherwise 00, else pass the otherstatecatastrophe as it is.
	IFF(
	    StateProvinceCode2 = '21',
	    IFF(
	        IncurredIndemnityAmount2 + IncurredMedicalAmount2 = 20000,
	        V_AssignCataStrophe_Other_State,
	        00
	    ),
	    V_AssignCataStrophe_Other_State
	) AS V_AssignCataStrophe_MI_State,
	V_AssignCataStrophe_MI_State AS V_GetCataStrophe_number,
	-- *INF*: TO_CHAR(LPAD(V_AssignCataStrophe_MI_State,2,'0'))
	TO_CHAR(LPAD(V_AssignCataStrophe_MI_State, 2, '0')) AS V_GetCataStrophe,
	V_GetCataStrophe AS O_GetCataStrophe,
	PolicyKey1 AS V_PrevPolicyKey,
	Out_CommonCount AS V_PrevCommonCount,
	Out_CommonGroupCount AS V_PrevCommonGroupCount,
	V_GetCataStrophe_number AS V_PrevGetCataStrophe,
	-- *INF*: IIF(StateProvinceCode2='21',IIF(V_AssignCataStrophe_Other_State=1,claimant_num1,'000'),IIF(V_GetCataStrophe='00' OR ClaimRelationshipKey1<>'N/A','000',claimant_num1))
	IFF(
	    StateProvinceCode2 = '21',
	    IFF(
	        V_AssignCataStrophe_Other_State = 1, claimant_num1, '000'
	    ),
	    IFF(
	        V_GetCataStrophe = '00' OR ClaimRelationshipKey1 <> 'N/A', '000', claimant_num1
	    )
	) AS O_claimant_num,
	ClaimRelationshipKey1,
	-- *INF*: MAKE_DATE_TIME(1800,01,01)
	TIMESTAMP_FROM_PARTS(1800,01,01,00,00,00) AS RateEffectiveDate,
	claim_party_occurrence_ak_id,
	OccupationDescription,
	WeeklyWageAmount,
	EmployerAttorneyFees
	FROM SRT_CatastropheInput
),
LKP_claim_loss_trans_fact_catgry_type AS (
	SELECT
	claim_pay_ctgry_type,
	clndr_date,
	claim_num,
	edw_claim_party_occurrence_ak_id
	FROM (
		SELECT cpctd.claim_pay_ctgry_type as claim_pay_ctgry_type, c.clndr_date as
		 clndr_date, cod.claim_num as claim_num, 
		cd.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id FROM
		@{pipeline().parameters.SOURCE_DATABASE_NAME_DM}.dbo.claim_loss_transaction_fact cltf
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DM}.dbo.Claim_occurrence_dim cod on cltf.claim_occurrence_dim_id = cod.claim_occurrence_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DM}.dbo.claimant_dim cd on cd.claimant_dim_id = cltf.claimant_dim_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DM}.dbo.calendar_dim c on cltf.claim_trans_date_id=c.clndr_id
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DM}.dbo.claim_payment_category_type_dim cpctd on
		 cpctd.claim_pay_ctgry_type_dim_id = cltf.claim_pay_ctgry_type_dim_id
		where     claim_pay_ctgry_type in('VC', 'VD', 'VE', 'VO', 'VR', 'VT','VM', 'XV')
		order by c.clndr_date DESC --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id,claim_num ORDER BY claim_pay_ctgry_type) = 1
),
EXP_Loss_Prem AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TRUNC(SESSSTARTTIME,'D')
	CAST(TRUNC(SESSSTARTTIME, 'DAY') AS TIMESTAMP_NTZ(0)) AS o_CreateDate,
	EXP_GetCatastropheCode.EDWPremiumMasterCalculationPKId3 AS EDWPremiumMasterCalculationPKId,
	EXP_GetCatastropheCode.EDWLossMasterCalculationPKId3 AS EDWLossMasterCalculationPKId,
	EXP_GetCatastropheCode.TypeBureauCode2 AS TypeBureauCode,
	EXP_GetCatastropheCode.PremiumMasterRunDate2 AS PremiumMasterRunDate,
	EXP_GetCatastropheCode.LossMasterRunDate2 AS LossMasterRunDate,
	EXP_GetCatastropheCode.BureauCompanyCode2 AS BureauCompanyCode,
	EXP_GetCatastropheCode.PolicyKey1 AS PolicyKey,
	EXP_GetCatastropheCode.StateProvinceCode2 AS StateProvinceCode,
	EXP_GetCatastropheCode.PolicyEffectiveDate2 AS i_PolicyEffectiveDate,
	i_PolicyEffectiveDate AS PolicyEffectiveDate,
	EXP_GetCatastropheCode.PolicyEndDate2 AS PolicyEndDate,
	EXP_GetCatastropheCode.InterstateRiskId2 AS InterstateRiskId,
	EXP_GetCatastropheCode.EmployeeLeasingCode2 AS EmployeeLeasingCode,
	EXP_GetCatastropheCode.StateRatingEffectiveDate2 AS StateRatingEffectiveDate,
	EXP_GetCatastropheCode.FederalTaxId2 AS FederalTaxId,
	EXP_GetCatastropheCode.ThreeYearFixedRatePolicyIndicator2 AS ThreeYearFixedRatePolicyIndicator,
	EXP_GetCatastropheCode.MultistatePolicyIndicator2 AS MultistatePolicyIndicator,
	EXP_GetCatastropheCode.InterstateRatedPolicyIndicator2 AS InterstateRatedPolicyIndicator,
	EXP_GetCatastropheCode.EstimatedAuditCode3 AS EstimatedAuditCode,
	EXP_GetCatastropheCode.RetrospectiveratedPolicyIndicator2 AS RetrospectiveratedPolicyIndicator,
	EXP_GetCatastropheCode.CancelledMidTermPolicyIndicator2 AS CancelledMidTermPolicyIndicator,
	EXP_GetCatastropheCode.ManagedCareOrganizationPolicyIndicator2 AS ManagedCareOrganizationPolicyIndicator,
	EXP_GetCatastropheCode.TypeOfCoverageIdCode2 AS TypeOfCoverageIdCode,
	EXP_GetCatastropheCode.TypeOfPlan2 AS TypeOfPlan,
	EXP_GetCatastropheCode.ClaimPayCategoryCode AS ClaimPayCategoryType,
	'00' AS LossSubjectToDeductibleCode,
	'00' AS BasisOfDeductibleCalculationCode,
	EXP_GetCatastropheCode.DeductibleAmountPerClaimAccident2 AS DeductibleAmountPerClaimAccident,
	EXP_GetCatastropheCode.InsuredName2 AS i_InsuredName,
	-- *INF*: REPLACECHR(0,i_InsuredName,CHR(34),'')
	REGEXP_REPLACE(i_InsuredName,CHR(34),'','i') AS o_InsuredName,
	EXP_GetCatastropheCode.WCSTATAddress2 AS WCSTATAddress,
	EXP_GetCatastropheCode.PremiumMasterClassCode2 AS PremiumMasterClassCode,
	EXP_GetCatastropheCode.ExperienceModificationFactor2 AS ExperienceModificationFactor,
	EXP_GetCatastropheCode.ExperienceModificationEffectiveDate2 AS ExperienceModificationEffectiveDate,
	-- *INF*: IIF(IN(SUBSTR(PremiumMasterClassCode,1,4) ,'0908','0913','9108'),(i_Exposure*10),i_Exposure)
	IFF(
	    SUBSTR(PremiumMasterClassCode, 1, 4) IN ('0908','0913','9108'), (i_Exposure * 10),
	    i_Exposure
	) AS v_Exposure,
	-- *INF*: ROUND(IIF(ISNULL(v_Exposure),0,v_Exposure))
	ROUND(
	    IFF(
	        v_Exposure IS NULL, 0, v_Exposure
	    )) AS o_Exposure,
	EXP_GetCatastropheCode.PremiumMasterDirectWrittenPremiumAmount2 AS i_PremiumMasterDirectWrittenPremiumAmount,
	-- *INF*: ROUND(i_PremiumMasterDirectWrittenPremiumAmount)
	ROUND(i_PremiumMasterDirectWrittenPremiumAmount) AS o_PremiumMasterDirectWrittenPremiumAmount,
	EXP_GetCatastropheCode.ManualChargedRate2 AS ManualChargedRate,
	EXP_GetCatastropheCode.LossMasterClassCode2 AS LossMasterClassCode,
	EXP_GetCatastropheCode.O_ClaimLossDate1 AS ClaimLossDate,
	EXP_GetCatastropheCode.ClaimNumber1 AS ClaimNumber,
	EXP_GetCatastropheCode.ClaimOccurrenceStatusCode2 AS ClaimOccurrenceStatusCode,
	EXP_GetCatastropheCode.InjuryTypeCode2 AS InjuryTypeCode,
	EXP_GetCatastropheCode.O_GetCataStrophe AS CatastropheCode,
	EXP_GetCatastropheCode.IncurredIndemnityAmount2 AS IncurredIndemnityAmount,
	EXP_GetCatastropheCode.IncurredMedicalAmount2 AS IncurredMedicalAmount,
	EXP_GetCatastropheCode.CauseOfLoss2 AS CauseOfLoss,
	EXP_GetCatastropheCode.TypeOfRecoveryCode2 AS TypeOfRecoveryCode,
	EXP_GetCatastropheCode.JurisdictionStateCode2 AS JurisdictionStateCode,
	EXP_GetCatastropheCode.BodyPartCode2 AS BodyPartCode,
	EXP_GetCatastropheCode.NatureOfInjuryCode2 AS NatureOfInjuryCode,
	EXP_GetCatastropheCode.CauseOfInjuryCode2 AS CauseOfInjuryCode,
	EXP_GetCatastropheCode.PaidIndemnityAmount2 AS PaidIndemnityAmount,
	EXP_GetCatastropheCode.PaidMedicalAmount2 AS PaidMedicalAmount,
	EXP_GetCatastropheCode.DeductibleReimbursementAmount2 AS DeductibleReimbursementAmount,
	EXP_GetCatastropheCode.PaidAllocatedLossAdjustmentExpenseAmount2 AS PaidAllocatedLossAdjustmentExpenseAmount,
	EXP_GetCatastropheCode.IncurredAllocatedLossAdjustmentExpenseAmount2 AS IncurredAllocatedLossAdjustmentExpenseAmount,
	'N/A' AS SubjectPremiumTotal,
	'N/A' AS StandarPremiumTotal,
	'0' AS CorrectionSeqNumber,
	'N' AS ReplacementReportCode,
	'' AS CorrectionTypeCode,
	'01' AS TypeOfNonStandardIdCode,
	-- *INF*: TO_INTEGER(DATE_DIFF(TRUNC(SYSDATE,'MM'),TRUNC(i_PolicyEffectiveDate,'MM'),'MM'),TRUE)
	CAST(DATEDIFF(MONTH,CAST(TRUNC(CURRENT_TIMESTAMP, 'MONTH') AS TIMESTAMP_NTZ(0)),CAST(TRUNC(i_PolicyEffectiveDate, 'MONTH') AS TIMESTAMP_NTZ(0))) AS INTEGER) AS v_AfterEffDate,
	-- *INF*: DECODE(v_AfterEffDate,
	-- 18,'1',
	-- 30,'2',
	-- 42,'3',
	-- 54,'4',
	-- 66,'5',
	-- 78,'6',
	-- 90,'7',
	-- 102,'8',
	-- 114,'9',
	-- 126,'A',
	-- '0')
	DECODE(
	    v_AfterEffDate,
	    18, '1',
	    30, '2',
	    42, '3',
	    54, '4',
	    66, '5',
	    78, '6',
	    90, '7',
	    102, '8',
	    114, '9',
	    126, 'A',
	    '0'
	) AS v_PreviousReportLevelCodeReportNumber,
	v_PreviousReportLevelCodeReportNumber AS PreviousReportLevelCodeReportNumber,
	'P' AS UpdateTypeCode,
	EXP_GetCatastropheCode.type_of_loss_code3 AS TypeOfLoss,
	'01' AS TypeOfClaim,
	EXP_GetCatastropheCode.TypeOfSettlement3 AS i_TypeOfSettlement,
	i_TypeOfSettlement AS o_TypeOfSettlement,
	EXP_GetCatastropheCode.ManagedCareOrganizationType3 AS ManagedCareOrganizationType,
	LKP_claim_loss_trans_fact_catgry_type.claim_pay_ctgry_type AS i_claim_pay_ctgry_type,
	-- *INF*: IN(i_claim_pay_ctgry_type,'VC','VD','VE','VO','VR','VT', 'VM','XV','Y','N')
	-- 
	-- 
	-- 
	-- 
	-- --'N'
	i_claim_pay_ctgry_type IN ('VC','VD','VE','VO','VR','VT','VM','XV','Y','N') AS v_VocationalRehabIndicator,
	-- *INF*: IIF(v_VocationalRehabIndicator='1','Y','N')
	IFF(v_VocationalRehabIndicator = '1', 'Y', 'N') AS o_VocationalRehabIndicator,
	EXP_GetCatastropheCode.LumpSumIndicator3 AS i_LumpSumIndicator,
	-- *INF*: IIF( i_TypeOfSettlement='06','Y','N')
	IFF(i_TypeOfSettlement = '06', 'Y', 'N') AS o_LumpSumIndicator,
	'00' AS FraudulentClaimCode,
	-- *INF*: DECODE(TRUE, 
	-- EDWPremiumMasterCalculationPKId != -1,'',
	-- IN(RTRIM(LTRIM(ClaimPayCategoryType)), 'DT'),'01',
	-- IN(RTRIM(LTRIM(ClaimPayCategoryType)), 'PT'),'02',
	-- StateProvinceCode = '21' AND IN(RTRIM(LTRIM(ClaimPayCategoryType)), 'PP','DF'),'03',
	-- StateProvinceCode <> '21' AND IN(RTRIM(LTRIM(ClaimPayCategoryType)), 'PP','DF','PB'),'09',
	-- StateProvinceCode = '21' AND IN(RTRIM(LTRIM(ClaimPayCategoryType)), 'PB'),'04',
	-- IN(RTRIM(LTRIM(ClaimPayCategoryType)), 'PD','TD','VR','SI','1B') AND IN(RTRIM(LTRIM(StateProvinceCode)), '22','48') AND DATE_DIFF(SYSDATE,i_PolicyEffectiveDate,'MM')  <= 30  AND  IncurredIndemnityAmount >  0 ,'05',
	-- IN(RTRIM(LTRIM(ClaimPayCategoryType)), 'PD','TD','VR') AND IN(RTRIM(LTRIM(StateProvinceCode)), '22','48') AND DATE_DIFF(SYSDATE,i_PolicyEffectiveDate,'MM')  > 30  and  IncurredIndemnityAmount  >  0,'09',
	-- IncurredMedicalAmount+IncurredIndemnityAmount = 0  and  IncurredAllocatedLossAdjustmentExpenseAmount > 0,'06',
	--  IncurredMedicalAmount>0 and   IncurredIndemnityAmount = 0,'06',
	-- IncurredIndemnityAmount>0,'05',
	-- '00'
	-- )
	-- 
	-- 
	DECODE(
	    TRUE,
	    EDWPremiumMasterCalculationPKId != - 1, '',
	    RTRIM(LTRIM(ClaimPayCategoryType)) IN ('DT'), '01',
	    RTRIM(LTRIM(ClaimPayCategoryType)) IN ('PT'), '02',
	    StateProvinceCode = '21' AND RTRIM(LTRIM(ClaimPayCategoryType)) IN ('PP','DF'), '03',
	    StateProvinceCode <> '21' AND RTRIM(LTRIM(ClaimPayCategoryType)) IN ('PP','DF','PB'), '09',
	    StateProvinceCode = '21' AND RTRIM(LTRIM(ClaimPayCategoryType)) IN ('PB'), '04',
	    RTRIM(LTRIM(ClaimPayCategoryType)) IN ('PD','TD','VR','SI','1B') AND RTRIM(LTRIM(StateProvinceCode)) IN ('22','48') AND DATEDIFF(MONTH,CURRENT_TIMESTAMP,i_PolicyEffectiveDate) <= 30 AND IncurredIndemnityAmount > 0, '05',
	    RTRIM(LTRIM(ClaimPayCategoryType)) IN ('PD','TD','VR') AND RTRIM(LTRIM(StateProvinceCode)) IN ('22','48') AND DATEDIFF(MONTH,CURRENT_TIMESTAMP,i_PolicyEffectiveDate) > 30 and IncurredIndemnityAmount > 0, '09',
	    IncurredMedicalAmount + IncurredIndemnityAmount = 0 and IncurredAllocatedLossAdjustmentExpenseAmount > 0, '06',
	    IncurredMedicalAmount > 0 and IncurredIndemnityAmount = 0, '06',
	    IncurredIndemnityAmount > 0, '05',
	    '00'
	) AS o_InjuryTypecode,
	EXP_GetCatastropheCode.O_claimant_num AS i_claimant_num,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_claimant_num)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_claimant_num) AS claimant_num,
	EXP_GetCatastropheCode.RateEffectiveDate,
	'N/A' AS SplitPeriodCode,
	EXP_GetCatastropheCode.OccupationDescription,
	EXP_GetCatastropheCode.WeeklyWageAmount,
	EXP_GetCatastropheCode.EmployerAttorneyFees
	FROM EXP_GetCatastropheCode
	LEFT JOIN LKP_claim_loss_trans_fact_catgry_type
	ON LKP_claim_loss_trans_fact_catgry_type.edw_claim_party_occurrence_ak_id = EXP_GetCatastropheCode.claim_party_occurrence_ak_id AND LKP_claim_loss_trans_fact_catgry_type.claim_num = EXP_GetCatastropheCode.ClaimNumber1
),
TGT_WorkWCSTATExtract_Insert AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkWCSTATExtract
	(AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyCode, PolicyKey, StateProvinceCode, PolicyEffectiveDate, PolicyEndDate, InterstateRiskId, EmployeeLeasingCode, StateRatingEffectiveDate, FederalTaxId, ThreeYearFixedRatePolicyIndicator, MultistatePolicyIndicator, InterstateRatedPolicyIndicator, EstimatedAuditCode, RetrospectiveratedPolicyIndicator, CancelledMidTermPolicyIndicator, ManagedCareOrganizationPolicyIndicator, TypeOfCoverageIdCode, TypeOfPlan, LossSubjectToDeductibleCode, BasisOfDeductibleCalculationCode, DeductibleAmountPerClaimAccident, InsuredName, WCSTATAddress, PremiumMasterClassCode, ExperienceModificationFactor, ExperienceModificationEffectiveDate, Exposure, PremiumMasterDirectWrittenPremiumAmount, ManualChargedRate, LossMasterClassCode, ClaimLossDate, ClaimNumber, ClaimOccurrenceStatusCode, InjuryTypeCode, CatastropheCode, IncurredIndemnityAmount, IncurredMedicalAmount, CauseOfLoss, TypeOfRecoveryCode, JurisdictionStateCode, BodyPartCode, NatureOfInjuryCode, CauseOfInjuryCode, PaidIndemnityAmount, PaidMedicalAmount, DeductibleReimbursementAmount, PaidAllocatedLossAdjustmentExpenseAmount, IncurredAllocatedLossAdjustmentExpenseAmount, SubjectPremiumTotal, StandarPremiumTotal, CorrectionSeqNumber, ReplacementReportCode, CorrectionTypeCode, TypeOfNonStandardIdCode, PreviousReportLevelCodeReportNumber, UpdateTypeCode, TypeOfLoss, TypeOfClaim, TypeOfSettlement, ManagedCareOrganizationType, VocationalRehabIndicator, LumpSumIndicator, FraudulentClaimCode, ClaimantNumber, RateEffectiveDate, SplitPeriodCode, OccupationDescription, WeeklyWageAmount, EmployerAttorneyFees)
	SELECT 
	o_AuditID AS AUDITID, 
	o_CreateDate AS CREATEDDATE, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	BUREAUCOMPANYCODE, 
	POLICYKEY, 
	STATEPROVINCECODE, 
	POLICYEFFECTIVEDATE, 
	POLICYENDDATE, 
	INTERSTATERISKID, 
	EMPLOYEELEASINGCODE, 
	STATERATINGEFFECTIVEDATE, 
	FEDERALTAXID, 
	THREEYEARFIXEDRATEPOLICYINDICATOR, 
	MULTISTATEPOLICYINDICATOR, 
	INTERSTATERATEDPOLICYINDICATOR, 
	ESTIMATEDAUDITCODE, 
	RETROSPECTIVERATEDPOLICYINDICATOR, 
	CANCELLEDMIDTERMPOLICYINDICATOR, 
	MANAGEDCAREORGANIZATIONPOLICYINDICATOR, 
	TYPEOFCOVERAGEIDCODE, 
	TYPEOFPLAN, 
	LOSSSUBJECTTODEDUCTIBLECODE, 
	BASISOFDEDUCTIBLECALCULATIONCODE, 
	DEDUCTIBLEAMOUNTPERCLAIMACCIDENT, 
	o_InsuredName AS INSUREDNAME, 
	WCSTATADDRESS, 
	PREMIUMMASTERCLASSCODE, 
	EXPERIENCEMODIFICATIONFACTOR, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	o_Exposure AS EXPOSURE, 
	o_PremiumMasterDirectWrittenPremiumAmount AS PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	MANUALCHARGEDRATE, 
	LOSSMASTERCLASSCODE, 
	CLAIMLOSSDATE, 
	CLAIMNUMBER, 
	CLAIMOCCURRENCESTATUSCODE, 
	o_InjuryTypecode AS INJURYTYPECODE, 
	CATASTROPHECODE, 
	INCURREDINDEMNITYAMOUNT, 
	INCURREDMEDICALAMOUNT, 
	CAUSEOFLOSS, 
	TYPEOFRECOVERYCODE, 
	JURISDICTIONSTATECODE, 
	BODYPARTCODE, 
	NATUREOFINJURYCODE, 
	CAUSEOFINJURYCODE, 
	PAIDINDEMNITYAMOUNT, 
	PAIDMEDICALAMOUNT, 
	DEDUCTIBLEREIMBURSEMENTAMOUNT, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	INCURREDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	SUBJECTPREMIUMTOTAL, 
	STANDARPREMIUMTOTAL, 
	CORRECTIONSEQNUMBER, 
	REPLACEMENTREPORTCODE, 
	CORRECTIONTYPECODE, 
	TYPEOFNONSTANDARDIDCODE, 
	PREVIOUSREPORTLEVELCODEREPORTNUMBER, 
	UPDATETYPECODE, 
	TYPEOFLOSS, 
	TYPEOFCLAIM, 
	o_TypeOfSettlement AS TYPEOFSETTLEMENT, 
	MANAGEDCAREORGANIZATIONTYPE, 
	o_VocationalRehabIndicator AS VOCATIONALREHABINDICATOR, 
	o_LumpSumIndicator AS LUMPSUMINDICATOR, 
	FRAUDULENTCLAIMCODE, 
	claimant_num AS CLAIMANTNUMBER, 
	RATEEFFECTIVEDATE, 
	SPLITPERIODCODE, 
	OCCUPATIONDESCRIPTION, 
	WEEKLYWAGEAMOUNT, 
	EMPLOYERATTORNEYFEES
	FROM EXP_Loss_Prem
),