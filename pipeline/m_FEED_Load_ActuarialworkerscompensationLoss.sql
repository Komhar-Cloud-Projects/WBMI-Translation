WITH
LKP_PhysicalState AS (
	SELECT
	StateCode_Desc,
	StateAbbreviation
	FROM (
		select 
		StateAbbreviation as StateAbbreviation,
		StateCode+'|'+StateDescription as StateCode_Desc
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.StateDim
		where CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateAbbreviation ORDER BY StateCode_Desc) = 1
),
SQ_vwLossMasterFact AS (
	Declare @BaseDate as date
	Declare @Date as int
	
	set @BaseDate = case when '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QA%' or '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QC%'
	then
	convert(date,EOMONTH(getdate(),0))
	else
	convert(date,EOMONTH(getdate(),-1))
	end 
	
	SET @Date=CONCAT (datepart (yy,@BaseDate), FORMAT (@BaseDate,'MM'))
	
	SELECT 
	vwLossMasterFact.loss_master_fact_id,			
	InsuranceReferenceDim.EnterpriseGroupCode,			
	InsuranceReferenceDim.EnterpriseGroupAbbreviation,			
	InsuranceReferenceDim.StrategicProfitCenterCode,			
	InsuranceReferenceDim.StrategicProfitCenterAbbreviation,			
	InsuranceReferenceDim.InsuranceReferenceLegalEntityCode,			
	InsuranceReferenceDim.InsuranceReferenceLegalEntityAbbreviation,			
	InsuranceReferenceDim.PolicyOfferingCode,			
	InsuranceReferenceDim.PolicyOfferingAbbreviation,			
	InsuranceReferenceDim.PolicyOfferingDescription,			
	InsuranceReferenceDim.ProductCode,			
	InsuranceReferenceDim.ProductAbbreviation,			
	InsuranceReferenceDim.ProductDescription,			
	InsuranceReferenceDim.InsuranceReferenceLineOfBusinessCode,			
	InsuranceReferenceDim.InsuranceReferenceLineOfBusinessAbbreviation,			
	InsuranceReferenceDim.InsuranceReferenceLineOfBusinessDescription,			
	InsuranceReferenceDim.AccountingProductCode,			
	InsuranceReferenceDim.AccountingProductAbbreviation,			
	InsuranceReferenceDim.AccountingProductDescription,			
	InsuranceReferenceDim.RatingPlanCode,			
	InsuranceReferenceDim.RatingPlanDescription,			
	InsuranceReferenceDim.InsuranceSegmentCode,			
	InsuranceReferenceDim.InsuranceSegmentDescription,			
	acc.clndr_date as AccountingDate,		
	vwLossMasterFact.DirectLossIncurredIR as DirectLossIncurred,			
	(vwLossMasterFact.DirectALAEPaidIR+vwLossMasterFact.DirectALAEOutstandingER) as DirectALAEIncurred,			
	(vwLossMasterFact.DirectLossIncurredIR+vwLossMasterFact.DirectALAEPaidIR+vwLossMasterFact.DirectALAEOutstandingER) as DirectLossandALAEIncurred,		
	(vwLossMasterFact.DirectOtherRecoveryPaid+vwLossMasterFact.DirectSalvagePaid+vwLossMasterFact.DirectSubrogationPaid) as DirectLossRecoveriesPaid,		
	vwLossMasterFact.DirectOtherRecoveryALAEPaid as DirectALAERecoveriesPaid,			
	vwLossMasterFact.DirectOtherRecoveryPaid as DirectOtherRecoveriesPaid,			
	vwLossMasterFact.DirectSalvagePaid as DirectSalvagePaid,			
	vwLossMasterFact.DirectSubrogationPaid as DirectSubrogationPaid,			
	vwLossMasterFact.InsuranceReferenceCoverageDimId,			
	pol.edw_pol_ak_id as pol_ak_id,			
	CDD.DeductibleAmount as DeductibleAmount,			
	SUBSTRING(CDD.ClassCode,1,4) as ClassCode,
	CDD.ClassDescription as ClassCodeDescription,			
	vwLossMasterFact.DirectLossPaidIR as DirectLossPaid,			
	vwLossMasterFact.DirectALAEPaidIR as DirectALAEPaid,			
	claimant_coverage_dim.TypeOfLoss as TypeOfLoss,			
	claim_occurrence_dim.claim_occurrence_key as ClaimOccurrenceKey,			
	claim_occurrence_dim.claim_occurrence_num as ClaimOccurrenceNumber,			
	claim_occurrence_dim.claim_num as ClaimNumber,			
	claim_occurrence_dim.claim_loss_date as DateofLoss,			
	FIRST_VALUE(case_dim.edw_claim_case_ak_id) OVER (PARTITION BY claimant_dim.edw_claim_party_occurrence_ak_id ORDER BY case_dim.edw_claim_case_ak_id DESC) as edw_claim_case_ak_id,
	--case_dim_current.suit_status_code as LitigationStatus,
	AgencyDim.EDWAgencyAKID as EDWAgencyAKID,
	contract_customer_dim.edw_contract_cust_ak_id as edw_contract_cust_ak_id,
	CDD.CoverageGuid as CoverageGuid,
	claim_occurrence_dim.edw_claim_occurrence_ak_id as edw_claim_occurrence_ak_id,
	claimant_dim.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id,
	CDD.RiskGradeCode as RiskGradeCode,
	
	claim_occurrence_dim.Catalyst as Catalyst,
	claim_occurrence_dim.CauseOfDamage as CauseOfDamage,
	claim_occurrence_dim.DamageCaused as DamageCaused,
	claim_occurrence_dim.ItemDamaged as ItemDamaged
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact	
	Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on vwLossMasterFact.pol_dim_id= pol.pol_dim_id 	
	Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim  AgencyDim on vwLossMasterFact.AgencyDimId =AgencyDim.AgencyDimID 	
	Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim on vwLossMasterFact.contract_cust_dim_id=contract_customer_dim.contract_cust_dim_id	
	Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim acc on acc.clndr_id = vwLossMasterFact.loss_master_run_date_id	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD on CDD.CoverageDetailDimId = vwLossMasterFact.CoverageDetailDimId
	Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim on claim_occurrence_dim.claim_occurrence_dim_id = vwLossMasterFact.claim_occurrence_dim_id
	Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_dim on claimant_dim.claimant_dim_id = vwLossMasterFact.claimant_dim_id
	Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_case_dim case_dim on case_dim.claim_case_dim_id = vwLossMasterFact.claim_case_dim_id
	--This Join is removed as part of AP-663
	--Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_case_dim case_dim_current on case_dim.edw_claim_case_ak_id =  case_dim_Current.edw_claim_case_ak_id and case_dim_Current.crrnt_snpsht_flag = 1
	Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim on claimant_coverage_dim.claimant_cov_dim_id = vwLossMasterFact.claimant_cov_dim_id
	Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim on InsuranceReferenceDim.InsuranceReferenceDimid= vwLossMasterFact.InsuranceReferenceDimId and 
	PolicyOfferingAbbreviation='WC'
	where acc.CalendarYearMonth  <= @Date
),
LKP_AD_Current_SD AS (
	SELECT
	EDWAgencyAKID,
	AgencyCode,
	AgencyDoingBusinessAsName,
	AgencyStateAbbreviation,
	AgencyPhysicalAddressCity,
	AgencyZIPCode,
	SalesDivisionDimId
	FROM (
		select  ad_Current.EDWAgencyAKID as EDWAgencyAKID,
		 ad_Current.AgencyCode as AgencyCode,
		 ad_Current.AgencyDoingBusinessAsName as AgencyDoingBusinessAsName,
		 ad_current.PhysicalStateAbbreviation as AgencyStateAbbreviation,
		 ad_Current.PhysicalCity as AgencyPhysicalAddressCity,
		 ad_Current.PhysicalZipCode as AgencyZIPCode,
		-- Removed below columns as per WREQ-13710
		-- ad_Current.LegalPrimaryAgencyCode as PrimaryAgencyCode,
		-- SD.AgencyDoingBusinessAsName as PrimaryAgencyDoingBusinessAsName,
		-- SD.PhysicalStateAbbreviation as PrimaryAgencyStateAbbreviation,
		-- SD.PhysicalCity as PrimaryAgencyPhysicalAddressCity,
		-- SD.PhysicalZipCode as PrimaryAgencyZIPCode,
		 ad_current.SalesDivisionDimId as SalesDivisionDimId
		 from
		@{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad_Current --to get the current Agency attribute 
		--LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD on SD.AgencyCode=ad_Current.LegalPrimaryAgencyCode AND SD.CurrentSnapshotFlag= 1 
		where ad_Current.CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID ORDER BY EDWAgencyAKID) = 1
),
LKP_AgencyRelationship AS (
	SELECT
	Prim_Agency_Code,
	Prim_Agency_State_Code,
	prim_agency_name,
	prim_agency_status_code,
	prim_agency_state_abbr,
	prim_agency_city,
	prim_agency_zip_code,
	EDWAgencyAKID
	FROM (
		select Latest_LPAC.EDWAgencyAKId as EDWAgencyAKId ,
		  SD.agencycode as prim_agency_code,
		  Left((SD.agencycode),2) as prim_agency_state_code,
		  SD.AgencyDoingBusinessAsName as prim_agency_name,
		SD.AgencyStatusCode as prim_agency_status_code,
		SD.PhysicalStateAbbreviation as prim_agency_state_abbr,
		SD.PhysicalCity as prim_agency_city,
		SD.PhysicalZipCode as prim_agency_zip_code
		from @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD 
		Inner Join  (
		 Select EDWAgencyAKId,EDWLegalPrimaryAgencyAKId from 
		  (
			select EDWAgencyAKId, 
		  EDWLegalPrimaryAgencyAKId, row_number() over (partition by edwAgencyAKID order by AgencyRelationshipexpirationdate desc ) As rowNum 
		  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyRelationshipCurrent
		  where AgencyRelationshipexpirationdate != '1800-01-01'
		  ) a
		  where a.rowNum = 1 
		  ) Latest_LPAC
		  on Latest_LPAC.EDWLegalPrimaryAgencyAKId=SD.EDWAgencyAKId
		  where SD.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID ORDER BY Prim_Agency_Code) = 1
),
LKP_CCD_Current AS (
	SELECT
	cust_num,
	name,
	sic_code,
	sic_code_descript,
	naics_code,
	naics_code_descript,
	fed_tax_id,
	edw_contract_cust_ak_id
	FROM (
		SELECT cust_num as cust_num, 
		[name] as name, 
		sic_code as sic_code, 
		sic_code_descript as sic_code_descript, 
		naics_code as naics_code ,
		naics_code_descript as naics_code_descript,
		fed_tax_id as fed_tax_id , 
		edw_contract_cust_ak_id  as edw_contract_cust_ak_id
		FROM DBO.contract_customer_dim WHERE crrnt_snpsht_flag =1 
		and edw_contract_cust_ak_id in (
		SELECT DISTINCT CC.edw_contract_cust_ak_id
		FROM dbo.vwLossMasterFact LMF 
		INNER JOIN DBO.contract_customer_dim CC on LMF.contract_cust_dim_id= CC.contract_cust_dim_id 
		INNER JOIN DBO.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= LMF.InsuranceReferenceDimId and PolicyOfferingAbbreviation='WC'
		)
		ORDER BY edw_contract_cust_ak_id --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id ORDER BY cust_num) = 1
),
LKP_Claim_Case_Dim AS (
	SELECT
	edw_claim_case_ak_id,
	suit_status_code
	FROM (
		SELECT 
			edw_claim_case_ak_id,
			suit_status_code
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_case_dim
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_case_ak_id ORDER BY edw_claim_case_ak_id) = 1
),
LKP_Pol_Current AS (
	SELECT
	edw_pol_ak_id,
	ProgramCode,
	ProgramDescription,
	AssociationCode,
	AssociationDescription,
	industry_risk_grade_code,
	industry_risk_grade_code_descript,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_rsn_code,
	pol_cancellation_rsn_code_descript,
	orig_incptn_date,
	renl_code,
	renl_code_descript,
	pol_status_code,
	pol_status_code_descript,
	pol_key,
	pol_sym,
	pol_num,
	pol_mod,
	pol_issue_code,
	pol_issue_code_descript,
	state_of_domicile_code,
	state_of_domicile_abbrev,
	state_of_domicile_code_descript,
	prim_bus_class_code,
	prim_bus_class_code_descript,
	BusinessClassDimId,
	UnderwritingDivisionDimId,
	serv_center_support_code,
	prior_pol_key,
	RolloverPriorCarrier,
	AgencyEmployeeDimID,
	PriorPolicyNumber,
	PriorPolicySymbol,
	PriorPolicyVersion,
	PriorPolicyEffectiveDate
	FROM (
		SELECT policy_dim.edw_pol_ak_id as edw_pol_ak_id,
		policy_dim.ProgramCode as ProgramCode, 
		policy_dim.ProgramDescription as ProgramDescription, 
		policy_dim.AssociationCode as AssociationCode, 
		policy_dim.AssociationDescription as AssociationDescription, 
		policy_dim.industry_risk_grade_code as industry_risk_grade_code, 
		policy_dim.industry_risk_grade_code_descript as industry_risk_grade_code_descript, 
		policy_dim.pol_eff_date as pol_eff_date, policy_dim.pol_exp_date as pol_exp_date, 
		policy_dim.pol_cancellation_rsn_code as pol_cancellation_rsn_code, 
		policy_dim.pol_cancellation_rsn_code_descript as pol_cancellation_rsn_code_descript, 
		policy_dim.orig_incptn_date as orig_incptn_date, 
		policy_dim.renl_code as renl_code, 
		policy_dim.renl_code_descript as renl_code_descript, 
		policy_dim.pol_status_code as pol_status_code, 
		policy_dim.pol_status_code_descript as pol_status_code_descript, 
		policy_dim.pol_key as pol_key, 
		policy_dim.pol_sym as pol_sym, 
		policy_dim.pol_num as pol_num, 
		policy_dim.pol_mod as pol_mod, 
		policy_dim.pol_issue_code as pol_issue_code, 
		policy_dim.pol_issue_code_descript as pol_issue_code_descript, 
		policy_dim.state_of_domicile_code as state_of_domicile_code, 
		policy_dim.state_of_domicile_abbrev as state_of_domicile_abbrev, 
		policy_dim.state_of_domicile_code_descript as state_of_domicile_code_descript, 
		policy_dim.prim_bus_class_code as prim_bus_class_code, 
		policy_dim.prim_bus_class_code_descript as prim_bus_class_code_descript, 
		policy_dim.BusinessClassDimId as BusinessClassDimId, 
		policy_dim.UnderwritingDivisionDimId as UnderwritingDivisionDimId, 
		policy_dim.serv_center_support_code as serv_center_support_code, 
		policy_dim.prior_pol_key as prior_pol_key, 
		policy_dim.RolloverPriorCarrier as RolloverPriorCarrier, 
		policy_dim.AgencyEmployeeDimID as AgencyEmployeeDimID, 
		prior_pol.pol_num as PriorPolicyNumber,
		prior_pol.pol_sym as PriorPolicySymbol,
		prior_pol.pol_mod as PriorPolicyVersion,
		prior_pol.pol_eff_date as PriorPolicyEffectiveDate
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim policy_dim
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim prior_pol on prior_pol.pol_key= policy_dim.prior_pol_key 
		and prior_pol.crrnt_snpsht_flag =1 and policy_dim.crrnt_snpsht_flag = 1
		WHERE policy_dim.edw_pol_ak_id in (
		SELECT DISTINCT POL.edw_pol_ak_id
		FROM dbo.vwLossMasterFact LMF  
		INNER JOIN DBO.policy_dim pol on LMF.pol_dim_id= pol.pol_dim_id 
		INNER JOIN DBO.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= LMF.InsuranceReferenceDimId and PolicyOfferingAbbreviation='WC' )
		ORDER BY policy_dim.edw_pol_ak_id --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id ORDER BY edw_pol_ak_id) = 1
),
LKP_aed_current AS (
	SELECT
	ProducerCode,
	ProducerFullName,
	i_AgencyEmployeeDimID,
	AgencyEmployeeDimID
	FROM (
		select AED.AgencyEmployeeDimID as AgencyEmployeeDimID,
		aed_current.ProducerCode as ProducerCode,
		(case when aed_current.AgencyEmployeeRole= 'Producer' then 
		aed_current.AgencyEmployeeFirstName + ' ' + aed_current.agencyemployeelastname else 'N/A' end ) as ProducerFullName
		from
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim AED 
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim aed_current --to get the current AgencyEmployeeDim attribute
		on aed.EDWAgencyEmployeeAKID = aed_current.EDWAgencyEmployeeAKID  and aed_current.CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyEmployeeDimID ORDER BY ProducerCode) = 1
),
EXP_Pass_Values AS (
	SELECT
	SQ_vwLossMasterFact.loss_master_fact_id,
	SQ_vwLossMasterFact.EnterpriseGroupCode,
	SQ_vwLossMasterFact.EnterpriseGroupAbbreviation,
	SQ_vwLossMasterFact.StrategicProfitCenterCode,
	SQ_vwLossMasterFact.StrategicProfitCenterAbbreviation,
	SQ_vwLossMasterFact.InsuranceReferenceLegalEntityCode,
	SQ_vwLossMasterFact.InsuranceReferenceLegalEntityAbbreviation,
	SQ_vwLossMasterFact.PolicyOfferingCode,
	SQ_vwLossMasterFact.PolicyOfferingAbbreviation,
	SQ_vwLossMasterFact.PolicyOfferingDescription,
	SQ_vwLossMasterFact.ProductCode,
	SQ_vwLossMasterFact.ProductAbbreviation,
	SQ_vwLossMasterFact.ProductDescription,
	SQ_vwLossMasterFact.InsuranceReferenceLineOfBusinessCode,
	SQ_vwLossMasterFact.InsuranceReferenceLineOfBusinessAbbreviation,
	SQ_vwLossMasterFact.InsuranceReferenceLineOfBusinessDescription,
	SQ_vwLossMasterFact.AccountingProductCode,
	SQ_vwLossMasterFact.AccountingProductAbbreviation,
	SQ_vwLossMasterFact.AccountingProductDescription,
	SQ_vwLossMasterFact.RatingPlanCode,
	SQ_vwLossMasterFact.RatingPlanDescription,
	SQ_vwLossMasterFact.InsuranceSegmentCode,
	SQ_vwLossMasterFact.InsuranceSegmentDescription,
	LKP_Pol_Current.ProgramCode,
	LKP_Pol_Current.ProgramDescription,
	LKP_Pol_Current.AssociationCode,
	LKP_Pol_Current.AssociationDescription,
	SQ_vwLossMasterFact.RiskGradeCode AS IndustryRiskGradeCode,
	-- *INF*: DECODE (TRUE,IndustryRiskGradeCode='1' , 'Excellent' ,
	-- IndustryRiskGradeCode='2' , 'Excellent' ,
	-- IndustryRiskGradeCode='3' , 'Good' ,
	-- IndustryRiskGradeCode='4' , 'Good' ,
	-- IndustryRiskGradeCode='5' , 'Average' ,
	-- IndustryRiskGradeCode='6' , 'Marginal' ,
	-- IndustryRiskGradeCode='7' , 'Marginal' ,
	-- IndustryRiskGradeCode='8' , 'Poor' ,
	-- IndustryRiskGradeCode='9' , 'Poor' ,
	-- IndustryRiskGradeCode='0' , 'NSI Bonds' ,
	-- IndustryRiskGradeCode='D' , 'Do Not Write' ,
	-- IndustryRiskGradeCode='N/A' , 'Not Available' ,
	-- IndustryRiskGradeCode='DNW' , 'Do Not Write' ,
	-- IndustryRiskGradeCode='NSI' , 'NSI' ,
	-- IndustryRiskGradeCode='Argent' , 'Argent' , 'Not Available')
	DECODE(
	    TRUE,
	    IndustryRiskGradeCode = '1', 'Excellent',
	    IndustryRiskGradeCode = '2', 'Excellent',
	    IndustryRiskGradeCode = '3', 'Good',
	    IndustryRiskGradeCode = '4', 'Good',
	    IndustryRiskGradeCode = '5', 'Average',
	    IndustryRiskGradeCode = '6', 'Marginal',
	    IndustryRiskGradeCode = '7', 'Marginal',
	    IndustryRiskGradeCode = '8', 'Poor',
	    IndustryRiskGradeCode = '9', 'Poor',
	    IndustryRiskGradeCode = '0', 'NSI Bonds',
	    IndustryRiskGradeCode = 'D', 'Do Not Write',
	    IndustryRiskGradeCode = 'N/A', 'Not Available',
	    IndustryRiskGradeCode = 'DNW', 'Do Not Write',
	    IndustryRiskGradeCode = 'NSI', 'NSI',
	    IndustryRiskGradeCode = 'Argent', 'Argent',
	    'Not Available'
	) AS v_IndustryRiskGradeDescription,
	v_IndustryRiskGradeDescription AS IndustryRiskGradeDescription1,
	LKP_Pol_Current.pol_eff_date AS PolicyEffectiveDate,
	LKP_Pol_Current.pol_exp_date AS PolicyExpirationDate,
	LKP_Pol_Current.pol_cancellation_rsn_code AS PolicyCancellationReasonCode,
	LKP_Pol_Current.pol_cancellation_rsn_code_descript AS PolicyCancellationReasonCodeDescription,
	LKP_Pol_Current.orig_incptn_date AS PolicyOriginalInceptionDate,
	LKP_Pol_Current.renl_code AS PolicyRenewalCode,
	LKP_Pol_Current.renl_code_descript AS PolicyRenewalDescription,
	LKP_Pol_Current.pol_status_code AS PolicyStatusCode,
	LKP_Pol_Current.pol_status_code_descript AS PolicyStatusCodeDescription,
	SQ_vwLossMasterFact.AccountingDate,
	LKP_Pol_Current.pol_key AS PolicyKey,
	LKP_Pol_Current.pol_sym AS PolicySymbol,
	LKP_Pol_Current.pol_num AS PolicyNumber,
	LKP_Pol_Current.pol_mod AS PolicyVersion,
	LKP_Pol_Current.pol_issue_code AS PolicyIssueCode,
	LKP_Pol_Current.pol_issue_code_descript AS PolicyIssueCodeDescription,
	LKP_Pol_Current.state_of_domicile_code AS PrimaryRatingStateCode,
	LKP_Pol_Current.state_of_domicile_abbrev AS PrimaryRatingStateAbbreviation,
	LKP_Pol_Current.state_of_domicile_code_descript AS PrimaryRatingStateDescription,
	LKP_Pol_Current.prim_bus_class_code AS PrimaryBusinessClassificationCode,
	LKP_Pol_Current.prim_bus_class_code_descript AS PrimaryBusinessClassificationDescription,
	LKP_AD_Current_SD.AgencyCode,
	LKP_AD_Current_SD.AgencyDoingBusinessAsName,
	LKP_AD_Current_SD.AgencyStateAbbreviation,
	LKP_AD_Current_SD.AgencyPhysicalAddressCity,
	LKP_AD_Current_SD.AgencyZIPCode,
	LKP_aed_current.ProducerCode,
	LKP_aed_current.ProducerFullName,
	LKP_AgencyRelationship.Prim_Agency_Code AS PrimaryAgencyCode,
	LKP_AgencyRelationship.prim_agency_name AS PrimaryAgencyDoingBusinessAsName,
	LKP_AgencyRelationship.prim_agency_state_abbr AS PrimaryAgencyStateAbbreviation,
	LKP_AgencyRelationship.prim_agency_city AS PrimaryAgencyPhysicalAddressCity,
	LKP_AgencyRelationship.prim_agency_zip_code AS PrimaryAgencyZIPCode,
	LKP_CCD_Current.cust_num AS CustomerNumber,
	LKP_CCD_Current.name AS FirstNamedInsured,
	LKP_CCD_Current.sic_code AS SICCode,
	LKP_CCD_Current.sic_code_descript AS SICDescription,
	LKP_CCD_Current.naics_code AS NAICSCode,
	LKP_CCD_Current.naics_code_descript AS NAICSDescription,
	LKP_CCD_Current.fed_tax_id AS FederalEmployerIDNumber,
	LKP_Pol_Current.serv_center_support_code AS CustomerCareIndicator,
	SQ_vwLossMasterFact.DirectLossIncurred,
	SQ_vwLossMasterFact.DirectALAEIncurred,
	SQ_vwLossMasterFact.DirectLossandALAEIncurred,
	SQ_vwLossMasterFact.DirectLossRecoveriesPaid,
	SQ_vwLossMasterFact.DirectALAERecoveriesPaid,
	SQ_vwLossMasterFact.DirectOtherRecoveriesPaid,
	SQ_vwLossMasterFact.DirectSalvagePaid,
	SQ_vwLossMasterFact.DirectSubrogationPaid,
	LKP_Pol_Current.RolloverPriorCarrier AS PriorCarrier,
	SQ_vwLossMasterFact.InsuranceReferenceCoverageDimId,
	LKP_Pol_Current.edw_pol_ak_id AS pol_ak_id,
	LKP_Pol_Current.BusinessClassDimId,
	LKP_Pol_Current.UnderwritingDivisionDimId,
	LKP_AD_Current_SD.SalesDivisionDimId,
	-- *INF*: IIF(ISNULL(PolicyEffectiveDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),PolicyEffectiveDate)
	IFF(
	    PolicyEffectiveDate IS NULL, TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    PolicyEffectiveDate
	) AS v_pol_eff_date,
	-- *INF*: IIF(ISNULL(PolicyExpirationDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),PolicyExpirationDate)
	-- 
	IFF(
	    PolicyExpirationDate IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    PolicyExpirationDate
	) AS v_pol_exp_date,
	-- *INF*: IIF(ISNULL(AccountingDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),AccountingDate)
	IFF(
	    AccountingDate IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    AccountingDate
	) AS v_acctg_date,
	-- *INF*: GET_DATE_PART(v_pol_eff_date,'YYYY')
	DATE_PART(v_pol_eff_date, 'YYYY') AS o_pol_eff_year,
	-- *INF*: TO_INTEGER(TO_CHAR(v_pol_eff_date,'Q'))
	CAST(TO_CHAR(v_pol_eff_date, 'Q') AS INTEGER) AS o_pol_eff_qtr,
	-- *INF*: GET_DATE_PART(v_pol_eff_date,'MM')
	DATE_PART(v_pol_eff_date, 'MM') AS o_pol_eff_mo,
	-- *INF*: TO_CHAR(v_pol_eff_date,'MONTH')
	TO_CHAR(v_pol_eff_date, 'MONTH') AS o_pol_eff_mo_desc,
	-- *INF*: GET_DATE_PART(v_pol_exp_date,'YYYY')
	DATE_PART(v_pol_exp_date, 'YYYY') AS o_pol_exp_year,
	-- *INF*: TO_INTEGER(TO_CHAR(v_pol_exp_date,'Q'))
	CAST(TO_CHAR(v_pol_exp_date, 'Q') AS INTEGER) AS o_pol_exp_qtr,
	-- *INF*: GET_DATE_PART(v_pol_exp_date,'MM')
	DATE_PART(v_pol_exp_date, 'MM') AS o_pol_exp_mo,
	-- *INF*: TO_CHAR(v_pol_exp_date,'MONTH')
	TO_CHAR(v_pol_exp_date, 'MONTH') AS o_pol_exp_mo_desc,
	-- *INF*: GET_DATE_PART(v_acctg_date,'YYYY')
	DATE_PART(v_acctg_date, 'YYYY') AS o_acctg_year,
	-- *INF*: TO_INTEGER(TO_CHAR(v_acctg_date,'Q'))
	CAST(TO_CHAR(v_acctg_date, 'Q') AS INTEGER) AS o_acctg_qtr,
	-- *INF*: GET_DATE_PART(v_acctg_date,'MM')
	DATE_PART(v_acctg_date, 'MM') AS o_acctg_mo,
	-- *INF*: TO_CHAR(v_acctg_date,'MONTH')
	TO_CHAR(v_acctg_date, 'MONTH') AS o_acctg_mo_desc,
	-- *INF*: IIF(PolicyIssueCode='N','Y','N')
	-- 
	-- 
	IFF(PolicyIssueCode = 'N', 'Y', 'N') AS o_new_bus_indic,
	-- *INF*: ADD_TO_DATE(ADD_TO_DATE(TRUNC(LAST_DAY(ADD_TO_DATE(SYSDATE,'MM',-1))),'DD',1),'SS',-1)
	DATEADD(SECOND,- 1,DATEADD(DAY,1,TRUNC(LAST_DAY(DATEADD(MONTH,- 1,CURRENT_TIMESTAMP))))) AS o_last_booked_date,
	LKP_Pol_Current.prior_pol_key AS PriorPolicyKey,
	LKP_Pol_Current.PriorPolicyNumber,
	LKP_Pol_Current.PriorPolicySymbol,
	LKP_Pol_Current.PriorPolicyVersion,
	LKP_Pol_Current.PriorPolicyEffectiveDate,
	SQ_vwLossMasterFact.DeductibleAmount,
	SQ_vwLossMasterFact.ClassCode,
	SQ_vwLossMasterFact.ClassCodeDescription AS ClassDescription,
	SQ_vwLossMasterFact.DirectLossPaidIR,
	SQ_vwLossMasterFact.DirectALAEPaidIR,
	SQ_vwLossMasterFact.TypeOfLoss,
	SQ_vwLossMasterFact.ClaimOccurrenceKey,
	SQ_vwLossMasterFact.ClaimOccurrenceNumber,
	SQ_vwLossMasterFact.ClaimNumber,
	SQ_vwLossMasterFact.DateOfLoss,
	LKP_Claim_Case_Dim.suit_status_code AS LitigationStatus,
	SQ_vwLossMasterFact.CoverageGuid,
	SQ_vwLossMasterFact.edw_claim_occurrence_ak_id,
	SQ_vwLossMasterFact.edw_claim_party_occurrence_ak_id,
	SQ_vwLossMasterFact.Catalyst,
	SQ_vwLossMasterFact.CauseOfDamage,
	SQ_vwLossMasterFact.DamageCaused,
	SQ_vwLossMasterFact.ItemDamaged
	FROM SQ_vwLossMasterFact
	LEFT JOIN LKP_AD_Current_SD
	ON LKP_AD_Current_SD.EDWAgencyAKID = SQ_vwLossMasterFact.EDWAgencyAKID
	LEFT JOIN LKP_AgencyRelationship
	ON LKP_AgencyRelationship.EDWAgencyAKID = LKP_AD_Current_SD.EDWAgencyAKID
	LEFT JOIN LKP_CCD_Current
	ON LKP_CCD_Current.edw_contract_cust_ak_id = SQ_vwLossMasterFact.edw_contract_cust_ak_id
	LEFT JOIN LKP_Claim_Case_Dim
	ON LKP_Claim_Case_Dim.edw_claim_case_ak_id = SQ_vwLossMasterFact.edw_claim_case_ak_id
	LEFT JOIN LKP_Pol_Current
	ON LKP_Pol_Current.edw_pol_ak_id = SQ_vwLossMasterFact.pol_ak_id
	LEFT JOIN LKP_aed_current
	ON LKP_aed_current.AgencyEmployeeDimID = LKP_Pol_Current.AgencyEmployeeDimID
),
LKP_BusinessClassDim AS (
	SELECT
	BusinessSegmentCode,
	BusinessSegmentDescription,
	StrategicBusinessGroupCode,
	StrategicBusinessGroupDescription,
	BusinessClassDimId
	FROM (
		SELECT 
			BusinessSegmentCode,
			BusinessSegmentDescription,
			StrategicBusinessGroupCode,
			StrategicBusinessGroupDescription,
			BusinessClassDimId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.BusinessClassDim
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BusinessClassDimId ORDER BY BusinessSegmentCode) = 1
),
LKP_Claim_Occurrence_Dim_Current AS (
	SELECT
	ClaimReportedDate,
	ClaimStatus,
	InternalCatastropheCode,
	LossDescription,
	LossLocationStateAbbreviation,
	edw_claim_occurrence_ak_id
	FROM (
		select edw_claim_occurrence_ak_id as edw_claim_occurrence_ak_id, 
		claim_rpted_date as ClaimReportedDate, 
		source_claim_occurrence_status_code as ClaimStatus, 
		claim_cat_code as InternalCatastropheCode, 
		claim_loss_descript as LossDescription, 
		loss_loc_state as LossLocationStateAbbreviation
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim with (nolock)
		where crrnt_snpsht_flag = 1
		order by edw_claim_occurrence_ak_id --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id ORDER BY ClaimReportedDate) = 1
),
LKP_CoverageDetailDim_Current AS (
	SELECT
	HazardGroupCode,
	PolicyAggregateLimit,
	PolicyPerAccidentLimit,
	PolicyPerDiseaseLimit,
	CoverageCancellationDate,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	LocationNumber,
	RatingCity,
	RatingCounty,
	RatingPostalCode,
	RatingStateProvinceAbbreviation,
	RatingStateProvinceCode,
	edw_pol_ak_id,
	CoverageGuid
	FROM (
		SELECT Latest_CDD_Per_Coverage.edw_pol_ak_id AS edw_pol_ak_id,
		    Latest_CDD_Per_Coverage.CoverageGuid AS CoverageGuid,
		    CWCD.HazardGroupCode as HazardGroupCode,
		    CWCD.PolicyAggregateLimit as PolicyAggregateLimit, 
		    CWCD.PolicyPerAccidentLimit as PolicyPerAccidentLimit, 
		    CWCD.PolicyPerDiseaseLimit as PolicyPerDiseaseLimit,
		    CDD.CoverageCancellationDate as CoverageCancellationDate,
		    CDD.CoverageEffectiveDate as CoverageEffectiveDate,
		    CDD.CoverageExpirationDate as CoverageExpirationDate,
		    CDD.LocationNumber as LocationNumber,
		    CDD.RatingCity as RatingCity,
		    CDD.RatingCounty as RatingCounty,
		    CDD.RatingPostalCode as RatingPostalCode,
		    CDD.RatingStateProvinceAbbreviation as RatingStateProvinceAbbreviation,
		    CDD.RatingStateProvinceCode as RatingStateProvinceCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD with (nolock) 
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensationDim CWCD with (nolock)
		on CWCD.CoverageDetailDimId = CDD.CoverageDetailDimId
		join (
		        SELECT pol.edw_pol_ak_id as edw_pol_ak_id,
		            CDD.CoverageGuid AS CoverageGuid,
		            max(CDD.CoverageDetailDimId) AS CoverageDetailID
		        FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact LMVW
		        LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensationDim CDWCD ON CDWCD.CoverageDetailDimId = LMVW.CoverageDetailDimId
		        INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD ON CDD.CoverageDetailDimId = LMVW.CoverageDetailDimId        
		        INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol ON LMVW.pol_dim_id = pol.pol_dim_id
		        GROUP BY pol.edw_pol_ak_id,
		            CDD.CoverageGuid
		        ) Latest_CDD_Per_Coverage ON CDD.CoverageDetailDimId = Latest_CDD_Per_Coverage.CoverageDetailID
		ORDER BY Latest_CDD_Per_Coverage.edw_pol_ak_id,
		    Latest_CDD_Per_Coverage.CoverageGuid --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,CoverageGuid ORDER BY HazardGroupCode) = 1
),
LKP_CoverageDetailWorkersCompensationDim AS (
	SELECT
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongShoreAndHarborWorkersCompensationActFlag,
	edw_pol_ak_id,
	ClassCode
	FROM (
		select Act_Code_Coverage.edw_pol_ak_id as edw_pol_ak_id,
		Act_Code_Coverage.ClassCode as ClassCode,
		CWCD.AdmiraltyActFlag as AdmiraltyActFlag,
		CWCD.FederalEmployersLiabilityActFlag as FederalEmployersLiabilityActFlag,
		CWCD.USLongShoreAndHarborWorkersCompensationActFlag as USLongShoreAndHarborWorkersCompensationActFlag
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensationDiM CWCD,
		(select pol.edw_pol_ak_id,substring(CDD.ClassCode,1,4) as ClassCode,max(CDD.CoverageDetailDimId) as CoverageDetailID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact LMVW 
		Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on LMVW.pol_dim_id= pol.pol_dim_id  
		Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoveragedetailDim CDD on CDD.CoverageDetailDimId = LMVW.CoverageDetailDimId 
		Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensationDim CDWCD on CDWCD.CoverageDetailDimId = LMVW.CoverageDetailDimId 
		group by pol.edw_pol_ak_id,substring(CDD.ClassCode,1,4)) Act_Code_Coverage 
		WHERE CWCD.CoverageDetailDimId=Act_Code_Coverage.CoverageDetailID 
		order by Act_Code_Coverage.edw_pol_ak_id,Act_Code_Coverage.ClassCode--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,ClassCode ORDER BY AdmiraltyActFlag) = 1
),
LKP_StateDim AS (
	SELECT
	StateDimID,
	StateAbbreviation
	FROM (
		SELECT 
			StateDimID,
			StateAbbreviation
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StateDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateAbbreviation ORDER BY StateDimID) = 1
),
LKP_DividendFact AS (
	SELECT
	DividendTypeDimId,
	pol_ak_id,
	StateDimId
	FROM (
		Select pol.edw_pol_ak_id as pol_ak_id ,
		DF.StateDimId as StateDimId ,
		DF.DividendTypeDimId as DividendTypeDimId 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendFact DF 
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on DF.StrategicProfitCenterDimId = IRD.InsuranceReferenceDimId and IRD.PolicyOfferingAbbreviation='WC'
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on DF.PolicyDimId= pol.pol_dim_id 
		Order by pol.edw_pol_ak_id,DF.StateDimId,DF.DividendRundateId desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,StateDimId ORDER BY DividendTypeDimId) = 1
),
LKP_DividendTypeDim AS (
	SELECT
	DividendType,
	DividendPlan,
	DividendTypeDimId
	FROM (
		SELECT 
			DividendType,
			DividendPlan,
			DividendTypeDimId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendTypeDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DividendTypeDimId ORDER BY DividendType) = 1
),
LKP_Injury_Type AS (
	SELECT
	edw_claim_party_occurrence_ak_id,
	Injury_Order,
	in_edw_claim_party_occurrence_ak_id
	FROM (
		select z.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id,min(z.Activity_Order) as Injury_Order
		 from 
		(select CD.edw_claim_party_occurrence_ak_id, CD.act_status_code,
		(case when CD.act_status_code='DE' 
		              THEN 1 
		                   when CD.act_status_code='PT'
		                   THEN 2
		                         when CD.act_status_code IN('CI','CJ')
		                         THEN 3 
		                               when CD.act_status_code IN ('CC','CD','CH','CK','FA')
		                               THEN 4
		                                     when CD.act_status_code IN ('CF','CG','SB')
		                                     THEN 5
		                                           when CD.act_status_code IN ('DM','DN','DO','DP','SD')
		                                           THEN 6
		                                                when CD.act_status_code IN ('CA','CB','CE','CL','MO','PI','SA','SC','SE','SF','SG','SH','SI','SJ','SK')
		                                                THEN 7 
		                                                    when CD.act_status_code='NO'
		                                                    THEN 8
		ELSE 9 END) as Activity_Order
		From @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_dim CD with (nolock)
		)z
		group by z.edw_claim_party_occurrence_ak_id
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id ORDER BY edw_claim_party_occurrence_ak_id) = 1
),
LKP_InsuranceReferencecoverageDim AS (
	SELECT
	CoverageCode,
	CoverageDescription,
	RatedCoverageCode,
	RatedCoverageDescription,
	CoverageGroupCode,
	CoverageGroupDescription,
	CoverageSummaryCode,
	CoverageSummaryDescription,
	InsuranceReferenceCoverageDimId
	FROM (
		SELECT 
			CoverageCode,
			CoverageDescription,
			RatedCoverageCode,
			RatedCoverageDescription,
			CoverageGroupCode,
			CoverageGroupDescription,
			CoverageSummaryCode,
			CoverageSummaryDescription,
			InsuranceReferenceCoverageDimId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceCoverageDimId ORDER BY CoverageCode) = 1
),
LKP_PolicyCurrentStatusDim AS (
	SELECT
	PolicyCancellationDate,
	PolicyStatusCode,
	PolicyStatusDescription,
	i_pol_ak_id,
	EDWPolicyAKId
	FROM (
		select EDWPolicyAKId as EDWPolicyAKId, PolicyCancellationDate as PolicyCancellationDate,
		case 
		when PolicyStatusDescription = 'Cancelled' then 'C'
		when PolicyStatusDescription = 'Future Inforce' then 'F'
		when PolicyStatusDescription = 'Inforce' then 'I'
		when PolicyStatusDescription = 'Not Inforce' then 'N'
		else 'N/A'
		end as PolicyStatusCode,
		PolicyStatusDescription as PolicyStatusDescription
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCurrentStatusDim pol_cstatus
		where RunDate = (select max(b.rundate) from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCurrentStatusDim b where b.EDWPolicyAKId=pol_cstatus.EDWPolicyAKId)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPolicyAKId ORDER BY PolicyCancellationDate) = 1
),
LKP_SalesDivisionDim AS (
	SELECT
	RegionalSalesManagerDisplayName,
	SalesTerritoryCode,
	SalesDivisionDimID
	FROM (
		SELECT 
			RegionalSalesManagerDisplayName,
			SalesTerritoryCode,
			SalesDivisionDimID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesDivisionDim
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesDivisionDimID ORDER BY RegionalSalesManagerDisplayName) = 1
),
LKP_UnderwritingDivisionDim AS (
	SELECT
	UnderwriterDisplayName,
	UnderwriterManagerDisplayName,
	UnderwritingRegionCodeDescription,
	UnderwritingDivisionDimID
	FROM (
		SELECT 
			UnderwriterDisplayName,
			UnderwriterManagerDisplayName,
			UnderwritingRegionCodeDescription,
			UnderwritingDivisionDimID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwritingDivisionDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwritingDivisionDimID ORDER BY UnderwriterDisplayName) = 1
),
LKP_claimant_dim_Current AS (
	SELECT
	body_part_code,
	body_part_code_descript,
	nature_inj_code,
	nature_inj_code_descript,
	cause_inj_code,
	cause_inj_code_descript,
	claimant_full_name,
	death_date,
	i_edw_claim_party_occurrence_ak_id,
	edw_claim_party_occurrence_ak_id
	FROM (
		select cd.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id,
		    cd.body_part_code as body_part_code,
		    cd.body_part_code_descript as body_part_code_descript,
		    cd.nature_inj_code as nature_inj_code,
		    cd.nature_inj_code_descript as nature_inj_code_descript,
		    cd.cause_inj_code as cause_inj_code,
		    cd.cause_inj_code_descript as cause_inj_code_descript,
		    cd.claimant_full_name as claimant_full_name,
		    cd.death_date as death_date
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_dim cd with (nolock)
		where cd.crrnt_snpsht_flag = 1
		order by cd.edw_claim_party_occurrence_ak_id --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id ORDER BY body_part_code) = 1
),
EXP_Get_Values AS (
	SELECT
	EXP_Pass_Values.EnterpriseGroupCode,
	EXP_Pass_Values.EnterpriseGroupAbbreviation,
	EXP_Pass_Values.StrategicProfitCenterCode,
	EXP_Pass_Values.StrategicProfitCenterAbbreviation,
	EXP_Pass_Values.InsuranceReferenceLegalEntityCode,
	EXP_Pass_Values.InsuranceReferenceLegalEntityAbbreviation,
	EXP_Pass_Values.PolicyOfferingCode,
	EXP_Pass_Values.PolicyOfferingAbbreviation,
	EXP_Pass_Values.PolicyOfferingDescription,
	EXP_Pass_Values.ProductCode,
	EXP_Pass_Values.ProductAbbreviation,
	EXP_Pass_Values.ProductDescription,
	EXP_Pass_Values.InsuranceReferenceLineOfBusinessCode,
	EXP_Pass_Values.InsuranceReferenceLineOfBusinessAbbreviation,
	EXP_Pass_Values.InsuranceReferenceLineOfBusinessDescription,
	EXP_Pass_Values.AccountingProductCode,
	EXP_Pass_Values.AccountingProductAbbreviation,
	EXP_Pass_Values.AccountingProductDescription,
	EXP_Pass_Values.RatingPlanCode,
	EXP_Pass_Values.RatingPlanDescription,
	EXP_Pass_Values.InsuranceSegmentCode,
	EXP_Pass_Values.InsuranceSegmentDescription,
	EXP_Pass_Values.ProgramCode,
	EXP_Pass_Values.ProgramDescription,
	EXP_Pass_Values.AssociationCode,
	EXP_Pass_Values.AssociationDescription,
	LKP_InsuranceReferencecoverageDim.CoverageSummaryCode AS i_CoverageSummaryCode,
	LKP_InsuranceReferencecoverageDim.CoverageSummaryDescription AS i_CoverageSummaryDescription,
	LKP_InsuranceReferencecoverageDim.CoverageGroupCode AS i_CoverageGroupCode,
	LKP_InsuranceReferencecoverageDim.CoverageGroupDescription AS i_CoverageGroupDescription,
	LKP_InsuranceReferencecoverageDim.CoverageCode AS i_CoverageCode,
	LKP_InsuranceReferencecoverageDim.CoverageDescription AS i_CoverageDescription,
	LKP_InsuranceReferencecoverageDim.RatedCoverageCode AS i_RatedCoverageCode,
	LKP_InsuranceReferencecoverageDim.RatedCoverageDescription AS i_RatedCoverageDescription,
	EXP_Pass_Values.Catalyst AS i_Catalyst,
	EXP_Pass_Values.CauseOfDamage AS i_CauseOfDamage,
	EXP_Pass_Values.DamageCaused AS i_DamageCaused,
	EXP_Pass_Values.ItemDamaged AS i_ItemDamaged,
	EXP_Pass_Values.ClassCode,
	EXP_Pass_Values.ClassDescription AS ClassCodeDescription,
	EXP_Pass_Values.IndustryRiskGradeCode,
	EXP_Pass_Values.IndustryRiskGradeDescription1 AS IndustryRiskGradeDescription,
	EXP_Pass_Values.o_pol_eff_year AS pol_eff_year,
	EXP_Pass_Values.o_pol_eff_qtr AS pol_eff_qtr,
	EXP_Pass_Values.o_pol_eff_mo AS pol_eff_mo,
	EXP_Pass_Values.o_pol_eff_mo_desc AS pol_eff_mo_desc,
	EXP_Pass_Values.PolicyEffectiveDate,
	EXP_Pass_Values.o_pol_exp_year AS pol_exp_year,
	EXP_Pass_Values.o_pol_exp_qtr AS pol_exp_qtr,
	EXP_Pass_Values.o_pol_exp_mo AS pol_exp_mo,
	EXP_Pass_Values.o_pol_exp_mo_desc AS pol_exp_mo_desc,
	EXP_Pass_Values.PolicyExpirationDate,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate AS i_PolicyCancellationDate,
	EXP_Pass_Values.PolicyCancellationReasonCode,
	EXP_Pass_Values.PolicyCancellationReasonCodeDescription,
	EXP_Pass_Values.PolicyOriginalInceptionDate,
	EXP_Pass_Values.PolicyRenewalCode,
	EXP_Pass_Values.PolicyRenewalDescription,
	LKP_PolicyCurrentStatusDim.PolicyStatusCode,
	LKP_PolicyCurrentStatusDim.PolicyStatusDescription AS PolicyStatusCodeDescription,
	EXP_Pass_Values.AccountingDate,
	EXP_Pass_Values.o_acctg_year AS acctg_year,
	EXP_Pass_Values.o_acctg_qtr AS acctg_qtr,
	EXP_Pass_Values.o_acctg_mo AS acctg_mo,
	EXP_Pass_Values.o_acctg_mo_desc AS acctg_mo_desc,
	LKP_CoverageDetailDim_Current.RatingStateProvinceCode AS RatingStateCode,
	LKP_CoverageDetailDim_Current.RatingStateProvinceAbbreviation,
	LKP_CoverageDetailDim_Current.LocationNumber,
	LKP_CoverageDetailDim_Current.RatingCity,
	LKP_CoverageDetailDim_Current.RatingCounty,
	LKP_CoverageDetailDim_Current.RatingPostalCode AS RatingLocationZIPCode,
	EXP_Pass_Values.PolicyKey,
	EXP_Pass_Values.PolicySymbol,
	EXP_Pass_Values.PolicyNumber,
	EXP_Pass_Values.PolicyVersion,
	EXP_Pass_Values.PolicyIssueCode,
	EXP_Pass_Values.PolicyIssueCodeDescription,
	EXP_Pass_Values.PrimaryRatingStateCode,
	EXP_Pass_Values.PrimaryRatingStateAbbreviation,
	EXP_Pass_Values.PrimaryRatingStateDescription,
	EXP_Pass_Values.PrimaryBusinessClassificationCode,
	EXP_Pass_Values.PrimaryBusinessClassificationDescription,
	LKP_BusinessClassDim.BusinessSegmentCode AS i_BusinessSegmentCode,
	LKP_BusinessClassDim.BusinessSegmentDescription AS i_BusinessSegmentDescription,
	LKP_BusinessClassDim.StrategicBusinessGroupCode AS i_StrategicBusinessGroupCode,
	LKP_BusinessClassDim.StrategicBusinessGroupDescription AS i_StrategicBusinessGroupDescription,
	EXP_Pass_Values.AgencyCode,
	EXP_Pass_Values.AgencyDoingBusinessAsName,
	EXP_Pass_Values.AgencyStateAbbreviation,
	EXP_Pass_Values.AgencyPhysicalAddressCity,
	EXP_Pass_Values.AgencyZIPCode,
	EXP_Pass_Values.ProducerCode,
	EXP_Pass_Values.ProducerFullName,
	LKP_UnderwritingDivisionDim.UnderwriterDisplayName,
	LKP_UnderwritingDivisionDim.UnderwriterManagerDisplayName,
	LKP_UnderwritingDivisionDim.UnderwritingRegionCodeDescription,
	EXP_Pass_Values.PrimaryAgencyCode,
	EXP_Pass_Values.PrimaryAgencyDoingBusinessAsName,
	EXP_Pass_Values.PrimaryAgencyStateAbbreviation,
	EXP_Pass_Values.PrimaryAgencyPhysicalAddressCity,
	EXP_Pass_Values.PrimaryAgencyZIPCode,
	LKP_SalesDivisionDim.RegionalSalesManagerDisplayName AS i_RegionalSalesManagerDisplayName,
	LKP_SalesDivisionDim.SalesTerritoryCode AS i_SalesTerritoryCode,
	EXP_Pass_Values.CustomerNumber,
	EXP_Pass_Values.FirstNamedInsured,
	EXP_Pass_Values.SICCode,
	EXP_Pass_Values.SICDescription,
	EXP_Pass_Values.NAICSCode,
	EXP_Pass_Values.NAICSDescription,
	EXP_Pass_Values.CustomerCareIndicator,
	EXP_Pass_Values.o_new_bus_indic AS new_bus_indic,
	EXP_Pass_Values.PriorPolicyKey,
	EXP_Pass_Values.PriorPolicyNumber,
	EXP_Pass_Values.PriorPolicySymbol,
	EXP_Pass_Values.PriorPolicyVersion,
	EXP_Pass_Values.o_last_booked_date AS last_booked_date,
	LKP_CoverageDetailDim_Current.HazardGroupCode,
	LKP_DividendTypeDim.DividendPlan,
	LKP_DividendTypeDim.DividendType,
	EXP_Pass_Values.DeductibleAmount,
	LKP_CoverageDetailDim_Current.PolicyPerAccidentLimit,
	LKP_CoverageDetailDim_Current.PolicyPerDiseaseLimit,
	LKP_CoverageDetailDim_Current.PolicyAggregateLimit,
	LKP_CoverageDetailWorkersCompensationDim.AdmiraltyActFlag,
	LKP_CoverageDetailWorkersCompensationDim.FederalEmployersLiabilityActFlag,
	LKP_CoverageDetailWorkersCompensationDim.USLongShoreAndHarborWorkersCompensationActFlag,
	EXP_Pass_Values.TypeOfLoss,
	EXP_Pass_Values.DirectLossPaidIR,
	EXP_Pass_Values.DirectALAEPaidIR,
	EXP_Pass_Values.DirectLossIncurred,
	EXP_Pass_Values.DirectALAEIncurred,
	EXP_Pass_Values.DirectLossandALAEIncurred,
	EXP_Pass_Values.DirectLossRecoveriesPaid,
	EXP_Pass_Values.DirectALAERecoveriesPaid,
	EXP_Pass_Values.DirectOtherRecoveriesPaid,
	EXP_Pass_Values.DirectSalvagePaid,
	EXP_Pass_Values.DirectSubrogationPaid,
	EXP_Pass_Values.ClaimOccurrenceKey,
	EXP_Pass_Values.ClaimOccurrenceNumber,
	EXP_Pass_Values.ClaimNumber,
	LKP_Claim_Occurrence_Dim_Current.LossDescription,
	LKP_Claim_Occurrence_Dim_Current.LossLocationStateAbbreviation,
	LKP_claimant_dim_Current.body_part_code AS BodyPartCode,
	LKP_claimant_dim_Current.body_part_code_descript AS BodyPartDescription,
	LKP_claimant_dim_Current.nature_inj_code AS NatureofInjuryCode,
	LKP_claimant_dim_Current.nature_inj_code_descript AS NatureofInjuryDescription,
	LKP_claimant_dim_Current.cause_inj_code AS CauseofInjuryCode,
	LKP_claimant_dim_Current.cause_inj_code_descript AS CauseofInjuryDescription,
	LKP_Claim_Occurrence_Dim_Current.ClaimStatus,
	EXP_Pass_Values.DateOfLoss,
	LKP_Claim_Occurrence_Dim_Current.ClaimReportedDate,
	LKP_claimant_dim_Current.claimant_full_name AS ClaimantName,
	LKP_Claim_Occurrence_Dim_Current.InternalCatastropheCode,
	EXP_Pass_Values.LitigationStatus,
	EXP_Pass_Values.FederalEmployerIDNumber,
	EXP_Pass_Values.PriorCarrier,
	LKP_CoverageDetailDim_Current.CoverageEffectiveDate,
	LKP_CoverageDetailDim_Current.CoverageExpirationDate,
	LKP_CoverageDetailDim_Current.CoverageCancellationDate,
	EXP_Pass_Values.PriorPolicyEffectiveDate,
	LKP_Injury_Type.Injury_Order AS InjuryType_Order,
	-- *INF*: DECODE(True, InjuryType_Order=1 ,  'DEATH' ,
	-- InjuryType_Order=2, 'PTD' ,
	-- InjuryType_Order=3, 'PPD' ,
	-- InjuryType_Order=4,'TTD' ,
	-- InjuryType_Order=5,'MO' ,
	-- InjuryType_Order=6,'NC' ,
	-- InjuryType_Order=7,'TTD-Pre', 
	-- InjuryType_Order=8,'NO',
	--  'N/A' )
	DECODE(
	    True,
	    InjuryType_Order = 1, 'DEATH',
	    InjuryType_Order = 2, 'PTD',
	    InjuryType_Order = 3, 'PPD',
	    InjuryType_Order = 4, 'TTD',
	    InjuryType_Order = 5, 'MO',
	    InjuryType_Order = 6, 'NC',
	    InjuryType_Order = 7, 'TTD-Pre',
	    InjuryType_Order = 8, 'NO',
	    'N/A'
	) AS v_InjuryTypeCode,
	-- *INF*: DECODE(True, v_InjuryTypeCode= 'DEATH' , 'DEATH', 
	-- v_InjuryTypeCode='PTD' ,'Permanent Total Disability', 
	-- v_InjuryTypeCode= 'PPD','Permanent Partial Disability' ,
	-- v_InjuryTypeCode='TTD','Temporary Total Disability', 
	-- v_InjuryTypeCode='MO','Medical Only' ,
	-- v_InjuryTypeCode='NC' ,'Non-Compensable', 
	-- v_InjuryTypeCode='TTD-Pre', 'Temporary Total Disability-Potential' ,
	-- v_InjuryTypeCode='NO','Notice Only',
	--  'N/A' )
	-- 
	DECODE(
	    True,
	    v_InjuryTypeCode = 'DEATH', 'DEATH',
	    v_InjuryTypeCode = 'PTD', 'Permanent Total Disability',
	    v_InjuryTypeCode = 'PPD', 'Permanent Partial Disability',
	    v_InjuryTypeCode = 'TTD', 'Temporary Total Disability',
	    v_InjuryTypeCode = 'MO', 'Medical Only',
	    v_InjuryTypeCode = 'NC', 'Non-Compensable',
	    v_InjuryTypeCode = 'TTD-Pre', 'Temporary Total Disability-Potential',
	    v_InjuryTypeCode = 'NO', 'Notice Only',
	    'N/A'
	) AS v_InjuryTypeDescript,
	LKP_claimant_dim_Current.death_date AS DeathDate,
	EXP_Pass_Values.loss_master_fact_id,
	-- *INF*: IIF(ISNULL(i_PolicyCancellationDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),i_PolicyCancellationDate)
	IFF(
	    i_PolicyCancellationDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    i_PolicyCancellationDate
	) AS v_PolicyCancellationDate,
	-- *INF*: GET_DATE_PART(v_PolicyCancellationDate,'YYYY')
	DATE_PART(v_PolicyCancellationDate, 'YYYY') AS v_pol_canc_year,
	-- *INF*: TO_INTEGER(TO_CHAR(v_PolicyCancellationDate,'Q'))
	CAST(TO_CHAR(v_PolicyCancellationDate, 'Q') AS INTEGER) AS v_pol_canc_qtr,
	-- *INF*: GET_DATE_PART(v_PolicyCancellationDate,'MM')
	DATE_PART(v_PolicyCancellationDate, 'MM') AS v_pol_canc_mo,
	-- *INF*: TO_CHAR(v_PolicyCancellationDate,'MONTH')
	TO_CHAR(v_PolicyCancellationDate, 'MONTH') AS v_pol_canc_mo_desc,
	-- *INF*: :LKP.LKP_PhysicalState(RatingStateProvinceAbbreviation)
	LKP_PHYSICALSTATE_RatingStateProvinceAbbreviation.StateCode_Desc AS v_RatingStateDescription,
	-- *INF*: :LKP.LKP_PhysicalState(AgencyStateAbbreviation)
	LKP_PHYSICALSTATE_AgencyStateAbbreviation.StateCode_Desc AS v_AgencyStateDescription,
	-- *INF*: :LKP.LKP_PhysicalState(PrimaryAgencyStateAbbreviation)
	LKP_PHYSICALSTATE_PrimaryAgencyStateAbbreviation.StateCode_Desc AS v_PrimaryAgencyStateDescription,
	-- *INF*: :LKP.LKP_PhysicalState(LossLocationStateAbbreviation)
	LKP_PHYSICALSTATE_LossLocationStateAbbreviation.StateCode_Desc AS v_LossLocationStateDescript,
	-- *INF*: TO_DECIMAL(DeductibleAmount,4)
	CAST(DeductibleAmount AS FLOAT) AS v_DeductibleAmount,
	-- *INF*: IIF(ISNULL(DateOfLoss),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),DateOfLoss)
	IFF(
	    DateOfLoss IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), DateOfLoss
	) AS v_DateofLoss,
	-- *INF*: IIF(ISNULL(ClaimReportedDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),ClaimReportedDate)
	IFF(
	    ClaimReportedDate IS NULL, TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    ClaimReportedDate
	) AS v_ClaimReportedDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	loss_master_fact_id AS EDWLossMasterFactId,
	-- *INF*: IIF(ISNULL(EnterpriseGroupCode),'N/A',EnterpriseGroupCode)
	IFF(EnterpriseGroupCode IS NULL, 'N/A', EnterpriseGroupCode) AS o_EnterpriseGroupCode,
	-- *INF*: IIF(ISNULL(EnterpriseGroupAbbreviation),'N/A',EnterpriseGroupAbbreviation)
	IFF(EnterpriseGroupAbbreviation IS NULL, 'N/A', EnterpriseGroupAbbreviation) AS o_EnterpriseGroupAbbreviation,
	-- *INF*: IIF(ISNULL(StrategicProfitCenterCode),'N/A',StrategicProfitCenterCode)
	IFF(StrategicProfitCenterCode IS NULL, 'N/A', StrategicProfitCenterCode) AS o_StrategicProfitCenterCode,
	-- *INF*: IIF(ISNULL(StrategicProfitCenterAbbreviation),'N/A',StrategicProfitCenterAbbreviation)
	IFF(StrategicProfitCenterAbbreviation IS NULL, 'N/A', StrategicProfitCenterAbbreviation) AS o_StrategicProfitCenterAbbreviation,
	-- *INF*: IIF(ISNULL(InsuranceReferenceLegalEntityCode),'N/A',InsuranceReferenceLegalEntityCode)
	IFF(InsuranceReferenceLegalEntityCode IS NULL, 'N/A', InsuranceReferenceLegalEntityCode) AS o_LegalEntityCode,
	-- *INF*: IIF(ISNULL(InsuranceReferenceLegalEntityAbbreviation),'N/A',InsuranceReferenceLegalEntityAbbreviation)
	IFF(
	    InsuranceReferenceLegalEntityAbbreviation IS NULL, 'N/A',
	    InsuranceReferenceLegalEntityAbbreviation
	) AS o_LegalEntityAbbreviation,
	-- *INF*: IIF(ISNULL(PolicyOfferingCode),'N/A',PolicyOfferingCode)
	IFF(PolicyOfferingCode IS NULL, 'N/A', PolicyOfferingCode) AS o_PolicyOfferingCode,
	-- *INF*: IIF(ISNULL(PolicyOfferingAbbreviation),'N/A',PolicyOfferingAbbreviation)
	IFF(PolicyOfferingAbbreviation IS NULL, 'N/A', PolicyOfferingAbbreviation) AS o_PolicyOfferingAbbreviation,
	-- *INF*: IIF(ISNULL(PolicyOfferingDescription),'N/A',PolicyOfferingDescription)
	IFF(PolicyOfferingDescription IS NULL, 'N/A', PolicyOfferingDescription) AS o_PolicyOfferingDescription,
	-- *INF*: IIF(ISNULL(ProductCode),'N/A',ProductCode)
	IFF(ProductCode IS NULL, 'N/A', ProductCode) AS o_ProductCode,
	-- *INF*: IIF(ISNULL(ProductAbbreviation),'N/A',ProductAbbreviation)
	IFF(ProductAbbreviation IS NULL, 'N/A', ProductAbbreviation) AS o_ProductAbbreviation,
	-- *INF*: IIF(ISNULL(ProductDescription),'N/A',ProductDescription)
	IFF(ProductDescription IS NULL, 'N/A', ProductDescription) AS o_ProductDescription,
	-- *INF*: IIF(ISNULL(InsuranceReferenceLineOfBusinessCode),'N/A',InsuranceReferenceLineOfBusinessCode)
	IFF(
	    InsuranceReferenceLineOfBusinessCode IS NULL, 'N/A', InsuranceReferenceLineOfBusinessCode
	) AS o_LineofBusinessCode,
	-- *INF*: IIF(ISNULL(InsuranceReferenceLineOfBusinessAbbreviation),'N/A',InsuranceReferenceLineOfBusinessAbbreviation)
	IFF(
	    InsuranceReferenceLineOfBusinessAbbreviation IS NULL, 'N/A',
	    InsuranceReferenceLineOfBusinessAbbreviation
	) AS o_LineofBusinessAbbreviation,
	-- *INF*: IIF(ISNULL(InsuranceReferenceLineOfBusinessDescription),'N/A',InsuranceReferenceLineOfBusinessDescription)
	IFF(
	    InsuranceReferenceLineOfBusinessDescription IS NULL, 'N/A',
	    InsuranceReferenceLineOfBusinessDescription
	) AS o_LineofBusinessDescription,
	-- *INF*: IIF(ISNULL(AccountingProductCode),'N/A',AccountingProductCode)
	IFF(AccountingProductCode IS NULL, 'N/A', AccountingProductCode) AS o_AccountingProductCode,
	-- *INF*: IIF(ISNULL(AccountingProductAbbreviation),'N/A',AccountingProductAbbreviation)
	IFF(AccountingProductAbbreviation IS NULL, 'N/A', AccountingProductAbbreviation) AS o_AccountingProductAbbreviation,
	-- *INF*: IIF(ISNULL(AccountingProductDescription),'N/A',AccountingProductDescription)
	IFF(AccountingProductDescription IS NULL, 'N/A', AccountingProductDescription) AS o_AccountingProductDescription,
	-- *INF*: IIF(ISNULL(RatingPlanCode),'N/A',RatingPlanCode)
	IFF(RatingPlanCode IS NULL, 'N/A', RatingPlanCode) AS o_RatingPlanCode,
	-- *INF*: IIF(ISNULL(RatingPlanDescription),'N/A',RatingPlanDescription)
	IFF(RatingPlanDescription IS NULL, 'N/A', RatingPlanDescription) AS o_RatingPlanDescription,
	-- *INF*: IIF(ISNULL(InsuranceSegmentCode),'N/A',InsuranceSegmentCode)
	IFF(InsuranceSegmentCode IS NULL, 'N/A', InsuranceSegmentCode) AS o_InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(InsuranceSegmentDescription),'N/A',InsuranceSegmentDescription)
	IFF(InsuranceSegmentDescription IS NULL, 'N/A', InsuranceSegmentDescription) AS o_InsuranceSegmentDescription,
	-- *INF*: IIF(ISNULL(ProgramCode),'N/A',ProgramCode)
	IFF(ProgramCode IS NULL, 'N/A', ProgramCode) AS o_ProgramCode,
	-- *INF*: IIF(ISNULL(ProgramDescription),'N/A',ProgramDescription)
	IFF(ProgramDescription IS NULL, 'N/A', ProgramDescription) AS o_ProgramDescription,
	-- *INF*: IIF(ISNULL(AssociationCode),'N/A',AssociationCode)
	IFF(AssociationCode IS NULL, 'N/A', AssociationCode) AS o_AssociationCode,
	-- *INF*: IIF(ISNULL(AssociationDescription),'N/A',AssociationDescription)
	IFF(AssociationDescription IS NULL, 'N/A', AssociationDescription) AS o_AssociationDescription,
	-- *INF*: IIF(ISNULL(i_CoverageSummaryCode),'N/A',i_CoverageSummaryCode)
	IFF(i_CoverageSummaryCode IS NULL, 'N/A', i_CoverageSummaryCode) AS o_CoverageSummaryCode,
	-- *INF*: IIF(ISNULL(i_CoverageSummaryDescription),'N/A',i_CoverageSummaryDescription)
	IFF(i_CoverageSummaryDescription IS NULL, 'N/A', i_CoverageSummaryDescription) AS o_CoverageSummaryDescription,
	-- *INF*: IIF(ISNULL(i_CoverageGroupCode),'N/A',i_CoverageGroupCode)
	IFF(i_CoverageGroupCode IS NULL, 'N/A', i_CoverageGroupCode) AS o_CoverageGroupCode,
	-- *INF*: IIF(ISNULL(i_CoverageGroupDescription),'N/A',i_CoverageGroupDescription)
	IFF(i_CoverageGroupDescription IS NULL, 'N/A', i_CoverageGroupDescription) AS o_CoverageGroupDescription,
	-- *INF*: IIF(ISNULL(i_CoverageCode),'N/A',i_CoverageCode)
	IFF(i_CoverageCode IS NULL, 'N/A', i_CoverageCode) AS o_CoverageCode,
	-- *INF*: IIF(ISNULL(i_CoverageDescription),'N/A',i_CoverageDescription)
	IFF(i_CoverageDescription IS NULL, 'N/A', i_CoverageDescription) AS o_CoverageDescription,
	-- *INF*: IIF(ISNULL(ClassCode),'N/A',ClassCode)
	IFF(ClassCode IS NULL, 'N/A', ClassCode) AS o_ClassCode,
	-- *INF*: IIF(ISNULL(ClassCodeDescription),'N/A',ClassCodeDescription)
	IFF(ClassCodeDescription IS NULL, 'N/A', ClassCodeDescription) AS o_ClassCodeDescription,
	-- *INF*: IIF(ISNULL(IndustryRiskGradeCode),'N/A',IndustryRiskGradeCode)
	IFF(IndustryRiskGradeCode IS NULL, 'N/A', IndustryRiskGradeCode) AS o_IndustryRiskGradeCode,
	-- *INF*: IIF(ISNULL(IndustryRiskGradeDescription),'N/A',IndustryRiskGradeDescription)
	IFF(IndustryRiskGradeDescription IS NULL, 'N/A', IndustryRiskGradeDescription) AS o_IndustryRiskGradeDescription,
	pol_eff_year AS o_PolicyEffectiveYear,
	pol_eff_qtr AS o_PolicyEffectiveQuarter,
	pol_eff_mo AS o_PolicyEffectiveMonthNumber,
	pol_eff_mo_desc AS o_PolicyEffectiveMonthDescription,
	PolicyEffectiveDate AS o_PolicyEffectiveDate,
	pol_exp_year AS o_PolicyExpirationYear,
	pol_exp_qtr AS o_PolicyExpirationQuarter,
	pol_exp_mo AS o_PolicyExpirationMonthNumber,
	pol_exp_mo_desc AS o_PolicyExpirationMonthDescription,
	PolicyExpirationDate AS o_PolicyExpirationDate,
	v_pol_canc_year AS o_PolicyCancellationYear,
	v_pol_canc_qtr AS o_PolicyCancellationQuarter,
	v_pol_canc_mo AS o_PolicyCancellationMonth,
	v_pol_canc_mo_desc AS o_PolicyCancellationMonthDescription,
	v_PolicyCancellationDate AS o_PolicyCancellationDate,
	-- *INF*: IIF(ISNULL(PolicyCancellationReasonCode),'N/A',PolicyCancellationReasonCode)
	IFF(PolicyCancellationReasonCode IS NULL, 'N/A', PolicyCancellationReasonCode) AS o_PolicyCancellationReasonCode,
	-- *INF*: IIF(ISNULL(PolicyCancellationReasonCodeDescription),'N/A',PolicyCancellationReasonCodeDescription)
	IFF(
	    PolicyCancellationReasonCodeDescription IS NULL, 'N/A',
	    PolicyCancellationReasonCodeDescription
	) AS o_PolicyCancellationReasonCodeDescription,
	-- *INF*: IIF(ISNULL(PolicyOriginalInceptionDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),PolicyOriginalInceptionDate)
	IFF(
	    PolicyOriginalInceptionDate IS NULL,
	    TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    PolicyOriginalInceptionDate
	) AS o_PolicyOriginalInceptionDate,
	-- *INF*: IIF(ISNULL(PolicyRenewalCode),'N/A',PolicyRenewalCode)
	IFF(PolicyRenewalCode IS NULL, 'N/A', PolicyRenewalCode) AS o_PolicyRenewalCode,
	-- *INF*: IIF(ISNULL(PolicyRenewalDescription),'N/A',PolicyRenewalDescription)
	IFF(PolicyRenewalDescription IS NULL, 'N/A', PolicyRenewalDescription) AS o_PolicyRenewalDescription,
	-- *INF*: IIF(ISNULL(PolicyStatusCode),'N/A',PolicyStatusCode)
	IFF(PolicyStatusCode IS NULL, 'N/A', PolicyStatusCode) AS o_PolicyStatusCode,
	-- *INF*: IIF(ISNULL(PolicyStatusCodeDescription),'N/A',PolicyStatusCodeDescription)
	IFF(PolicyStatusCodeDescription IS NULL, 'N/A', PolicyStatusCodeDescription) AS o_PolicyStatusCodeDescription,
	acctg_year AS o_AccountingYear,
	acctg_qtr AS o_AccountingMonthQuarter,
	acctg_mo AS o_AccountingMonthNumber,
	acctg_mo_desc AS o_AccountingMonthName,
	AccountingDate AS o_AccountingDate,
	-- *INF*: IIF(ISNULL(RatingStateCode),'N/A',RatingStateCode)
	IFF(RatingStateCode IS NULL, 'N/A', RatingStateCode) AS o_RatingStateCode,
	-- *INF*: IIF(ISNULL(RatingStateProvinceAbbreviation),'N/A',RatingStateProvinceAbbreviation)
	IFF(RatingStateProvinceAbbreviation IS NULL, 'N/A', RatingStateProvinceAbbreviation) AS o_RatingStateAbbreviation,
	-- *INF*: IIF(ISNULL(v_RatingStateDescription),'N/A',SUBSTR(v_RatingStateDescription,(INSTR(v_RatingStateDescription,'|')+1),LENGTH(v_RatingStateDescription)))
	IFF(
	    v_RatingStateDescription IS NULL, 'N/A',
	    SUBSTR(v_RatingStateDescription, (REGEXP_INSTR(v_RatingStateDescription, '|') + 1), LENGTH(v_RatingStateDescription))
	) AS o_RatingStateName,
	-- *INF*: IIF(ISNULL(LocationNumber),'N/A',LocationNumber)
	IFF(LocationNumber IS NULL, 'N/A', LocationNumber) AS o_LocationNumber,
	-- *INF*: IIF(ISNULL(RatingCity),'N/A',RatingCity)
	IFF(RatingCity IS NULL, 'N/A', RatingCity) AS o_RatingLocationCity,
	-- *INF*: IIF(ISNULL(RatingCounty),'N/A',RatingCounty)
	IFF(RatingCounty IS NULL, 'N/A', RatingCounty) AS o_RatingLocationCounty,
	-- *INF*: IIF(ISNULL(RatingLocationZIPCode),'N/A',RatingLocationZIPCode)
	IFF(RatingLocationZIPCode IS NULL, 'N/A', RatingLocationZIPCode) AS o_RatingLocationZIPCode,
	-- *INF*: IIF(ISNULL(PolicyKey),'N/A',PolicyKey)
	IFF(PolicyKey IS NULL, 'N/A', PolicyKey) AS o_PolicyKey,
	-- *INF*: IIF(ISNULL(PolicySymbol),'N/A',PolicySymbol)
	IFF(PolicySymbol IS NULL, 'N/A', PolicySymbol) AS o_PolicySymbol,
	-- *INF*: IIF(ISNULL(PolicyNumber),'N/A',PolicyNumber)
	IFF(PolicyNumber IS NULL, 'N/A', PolicyNumber) AS o_PolicyNumber,
	-- *INF*: IIF(ISNULL(PolicyVersion),'N/A',PolicyVersion)
	IFF(PolicyVersion IS NULL, 'N/A', PolicyVersion) AS o_PolicyVersion,
	-- *INF*: IIF(ISNULL(PolicyIssueCode),'N/A',PolicyIssueCode)
	IFF(PolicyIssueCode IS NULL, 'N/A', PolicyIssueCode) AS o_PolicyIssueCode,
	-- *INF*: IIF(ISNULL(PolicyIssueCodeDescription),'N/A',PolicyIssueCodeDescription)
	IFF(PolicyIssueCodeDescription IS NULL, 'N/A', PolicyIssueCodeDescription) AS o_PolicyIssueCodeDescription,
	-- *INF*: IIF(ISNULL(PrimaryRatingStateCode),'N/A',PrimaryRatingStateCode)
	IFF(PrimaryRatingStateCode IS NULL, 'N/A', PrimaryRatingStateCode) AS o_PrimaryRatingStateCode,
	-- *INF*: IIF(ISNULL(PrimaryRatingStateAbbreviation),'N/A',PrimaryRatingStateAbbreviation)
	IFF(PrimaryRatingStateAbbreviation IS NULL, 'N/A', PrimaryRatingStateAbbreviation) AS o_PrimaryRatingStateAbbreviation,
	-- *INF*: IIF(ISNULL(PrimaryRatingStateDescription),'N/A',PrimaryRatingStateDescription)
	IFF(PrimaryRatingStateDescription IS NULL, 'N/A', PrimaryRatingStateDescription) AS o_PrimaryRatingStateDescription,
	-- *INF*: IIF(ISNULL(PrimaryBusinessClassificationCode),'N/A',PrimaryBusinessClassificationCode)
	IFF(PrimaryBusinessClassificationCode IS NULL, 'N/A', PrimaryBusinessClassificationCode) AS o_PrimaryBusinessClassificationCode,
	-- *INF*: IIF(ISNULL(PrimaryBusinessClassificationDescription),'N/A',PrimaryBusinessClassificationDescription)
	IFF(
	    PrimaryBusinessClassificationDescription IS NULL, 'N/A',
	    PrimaryBusinessClassificationDescription
	) AS o_PrimaryBusinessClassificationDescription,
	-- *INF*: IIF(ISNULL(i_BusinessSegmentCode),'N/A',i_BusinessSegmentCode)
	IFF(i_BusinessSegmentCode IS NULL, 'N/A', i_BusinessSegmentCode) AS o_BusinessSegmentCode,
	-- *INF*: IIF(ISNULL(i_BusinessSegmentDescription),'N/A',i_BusinessSegmentDescription)
	IFF(i_BusinessSegmentDescription IS NULL, 'N/A', i_BusinessSegmentDescription) AS o_BusinessSegmentDescription,
	-- *INF*: IIF(ISNULL(i_StrategicBusinessGroupCode),'N/A',i_StrategicBusinessGroupCode)
	IFF(i_StrategicBusinessGroupCode IS NULL, 'N/A', i_StrategicBusinessGroupCode) AS o_StrategicBusinessGroupCode,
	-- *INF*: IIF(ISNULL(i_StrategicBusinessGroupDescription),'N/A',i_StrategicBusinessGroupDescription)
	IFF(i_StrategicBusinessGroupDescription IS NULL, 'N/A', i_StrategicBusinessGroupDescription) AS o_StrategicBusinessGroupDescription,
	-- *INF*: IIF(ISNULL(AgencyCode),'N/A',AgencyCode)
	IFF(AgencyCode IS NULL, 'N/A', AgencyCode) AS o_AgencyCode,
	-- *INF*: IIF(ISNULL(AgencyDoingBusinessAsName),'N/A',AgencyDoingBusinessAsName)
	IFF(AgencyDoingBusinessAsName IS NULL, 'N/A', AgencyDoingBusinessAsName) AS o_AgencyDoingBusinessAsName,
	-- *INF*: IIF(ISNULL(v_AgencyStateDescription),'N/A',SUBSTR(v_AgencyStateDescription,1,(INSTR(v_AgencyStateDescription,'|')-1)))
	IFF(
	    v_AgencyStateDescription IS NULL, 'N/A',
	    SUBSTR(v_AgencyStateDescription, 1, (REGEXP_INSTR(v_AgencyStateDescription, '|') - 1))
	) AS o_AgencyStateCode,
	-- *INF*: IIF(ISNULL(AgencyStateAbbreviation),'NA',AgencyStateAbbreviation)
	IFF(AgencyStateAbbreviation IS NULL, 'NA', AgencyStateAbbreviation) AS o_AgencyStateAbbreviation,
	-- *INF*: IIF(ISNULL(v_AgencyStateDescription),'N/A',SUBSTR(v_AgencyStateDescription,(INSTR(v_AgencyStateDescription,'|')+1),LENGTH(v_AgencyStateDescription)))
	IFF(
	    v_AgencyStateDescription IS NULL, 'N/A',
	    SUBSTR(v_AgencyStateDescription, (REGEXP_INSTR(v_AgencyStateDescription, '|') + 1), LENGTH(v_AgencyStateDescription))
	) AS o_AgencyStateDescription,
	-- *INF*: IIF(ISNULL(AgencyPhysicalAddressCity),'N/A',AgencyPhysicalAddressCity)
	IFF(AgencyPhysicalAddressCity IS NULL, 'N/A', AgencyPhysicalAddressCity) AS o_AgencyPhysicalAddressCity,
	-- *INF*: IIF(ISNULL(AgencyZIPCode),'N/A',AgencyZIPCode)
	IFF(AgencyZIPCode IS NULL, 'N/A', AgencyZIPCode) AS o_AgencyZIPCode,
	-- *INF*: IIF(ISNULL(ProducerCode),'N/A',ProducerCode)
	IFF(ProducerCode IS NULL, 'N/A', ProducerCode) AS o_ProducerCode,
	-- *INF*: IIF(ISNULL(ProducerFullName),'N/A',ProducerFullName)
	IFF(ProducerFullName IS NULL, 'N/A', ProducerFullName) AS o_ProducerFullName,
	-- *INF*: IIF(ISNULL(UnderwriterDisplayName),'N/A',UnderwriterDisplayName)
	IFF(UnderwriterDisplayName IS NULL, 'N/A', UnderwriterDisplayName) AS o_UnderwriterFullName,
	-- *INF*: IIF(ISNULL(UnderwriterManagerDisplayName),'N/A',UnderwriterManagerDisplayName)
	IFF(UnderwriterManagerDisplayName IS NULL, 'N/A', UnderwriterManagerDisplayName) AS o_UnderwritingManagerName,
	-- *INF*: IIF(ISNULL(UnderwritingRegionCodeDescription),'N/A',UnderwritingRegionCodeDescription)
	IFF(UnderwritingRegionCodeDescription IS NULL, 'N/A', UnderwritingRegionCodeDescription) AS o_UnderwritingRegionName,
	-- *INF*: IIF(ISNULL(PrimaryAgencyCode),'N/A',PrimaryAgencyCode)
	IFF(PrimaryAgencyCode IS NULL, 'N/A', PrimaryAgencyCode) AS o_PrimaryAgencyCode,
	-- *INF*: IIF(ISNULL(PrimaryAgencyDoingBusinessAsName),'N/A',PrimaryAgencyDoingBusinessAsName)
	IFF(PrimaryAgencyDoingBusinessAsName IS NULL, 'N/A', PrimaryAgencyDoingBusinessAsName) AS o_PrimaryAgencyDoingBusinessAsName,
	-- *INF*: IIF(ISNULL(v_PrimaryAgencyStateDescription),'N/A',SUBSTR(v_PrimaryAgencyStateDescription,1,(INSTR(v_PrimaryAgencyStateDescription,'|')-1)))
	IFF(
	    v_PrimaryAgencyStateDescription IS NULL, 'N/A',
	    SUBSTR(v_PrimaryAgencyStateDescription, 1, (REGEXP_INSTR(v_PrimaryAgencyStateDescription, '|') - 1))
	) AS o_PrimaryAgencyStateCode,
	-- *INF*: IIF(ISNULL(PrimaryAgencyStateAbbreviation),'NA',PrimaryAgencyStateAbbreviation)
	IFF(PrimaryAgencyStateAbbreviation IS NULL, 'NA', PrimaryAgencyStateAbbreviation) AS o_PrimaryAgencyStateAbbreviation,
	-- *INF*: IIF(ISNULL(v_PrimaryAgencyStateDescription),'N/A',SUBSTR(v_PrimaryAgencyStateDescription,(INSTR(v_PrimaryAgencyStateDescription,'|')+1),LENGTH(v_PrimaryAgencyStateDescription)))
	IFF(
	    v_PrimaryAgencyStateDescription IS NULL, 'N/A',
	    SUBSTR(v_PrimaryAgencyStateDescription, (REGEXP_INSTR(v_PrimaryAgencyStateDescription, '|') + 1), LENGTH(v_PrimaryAgencyStateDescription))
	) AS o_PrimaryAgencyStateDescription,
	-- *INF*: IIF(ISNULL(PrimaryAgencyPhysicalAddressCity),'N/A',PrimaryAgencyPhysicalAddressCity)
	IFF(PrimaryAgencyPhysicalAddressCity IS NULL, 'N/A', PrimaryAgencyPhysicalAddressCity) AS o_PrimaryAgencyPhysicalAddressCity,
	-- *INF*: IIF(ISNULL(PrimaryAgencyZIPCode),'N/A',PrimaryAgencyZIPCode)
	IFF(PrimaryAgencyZIPCode IS NULL, 'N/A', PrimaryAgencyZIPCode) AS o_PrimaryAgencyZIPCode,
	-- *INF*: IIF(ISNULL(i_RegionalSalesManagerDisplayName),'N/A',i_RegionalSalesManagerDisplayName)
	IFF(i_RegionalSalesManagerDisplayName IS NULL, 'N/A', i_RegionalSalesManagerDisplayName) AS o_RegionalSalesManagerFullName,
	-- *INF*: IIF(ISNULL(i_SalesTerritoryCode),'N/A',i_SalesTerritoryCode)
	IFF(i_SalesTerritoryCode IS NULL, 'N/A', i_SalesTerritoryCode) AS o_SalesTerritoryCode,
	-- *INF*: IIF(ISNULL(CustomerNumber),'N/A',CustomerNumber)
	IFF(CustomerNumber IS NULL, 'N/A', CustomerNumber) AS o_CustomerNumber,
	-- *INF*: IIF(ISNULL(FirstNamedInsured),'N/A',FirstNamedInsured)
	IFF(FirstNamedInsured IS NULL, 'N/A', FirstNamedInsured) AS o_FirstNamedInsured,
	-- *INF*: IIF(ISNULL(SICCode),'N/A',SICCode)
	IFF(SICCode IS NULL, 'N/A', SICCode) AS o_SICCode,
	-- *INF*: IIF(ISNULL(SICDescription),'N/A',SICDescription)
	IFF(SICDescription IS NULL, 'N/A', SICDescription) AS o_SICDescription,
	-- *INF*: IIF(ISNULL(NAICSCode),'N/A',NAICSCode)
	IFF(NAICSCode IS NULL, 'N/A', NAICSCode) AS o_NAICSCode,
	-- *INF*: IIF(ISNULL(NAICSDescription),'N/A',NAICSDescription)
	IFF(NAICSDescription IS NULL, 'N/A', NAICSDescription) AS o_NAICSDescription,
	-- *INF*: IIF(ISNULL(CustomerCareIndicator),'N/A',CustomerCareIndicator)
	IFF(CustomerCareIndicator IS NULL, 'N/A', CustomerCareIndicator) AS o_CustomerCareIndicator,
	-- *INF*: IIF(ISNULL(new_bus_indic),'N/A',new_bus_indic)
	IFF(new_bus_indic IS NULL, 'N/A', new_bus_indic) AS o_NewBusinessIndicator,
	-- *INF*: IIF(ISNULL(PriorPolicyKey),'N/A',PriorPolicyKey)
	IFF(PriorPolicyKey IS NULL, 'N/A', PriorPolicyKey) AS o_PriorPolicyKey,
	-- *INF*: IIF(ISNULL(PriorPolicyNumber),'N/A',PriorPolicyNumber)
	IFF(PriorPolicyNumber IS NULL, 'N/A', PriorPolicyNumber) AS o_PriorPolicyNumber,
	-- *INF*: IIF(ISNULL(PriorPolicySymbol),'N/A',PriorPolicySymbol)
	IFF(PriorPolicySymbol IS NULL, 'N/A', PriorPolicySymbol) AS o_PriorPolicySymbol,
	-- *INF*: IIF(ISNULL(PriorPolicyVersion),'N/A',PriorPolicyVersion)
	IFF(PriorPolicyVersion IS NULL, 'N/A', PriorPolicyVersion) AS o_PriorPolicyVersion,
	last_booked_date AS o_LastBookedDate,
	-- *INF*: IIF(ISNULL(HazardGroupCode),'N/A',HazardGroupCode)
	IFF(HazardGroupCode IS NULL, 'N/A', HazardGroupCode) AS o_HazardGroupCode,
	-- *INF*: IIF(ISNULL(DividendPlan),'N/A',DividendPlan)
	IFF(DividendPlan IS NULL, 'N/A', DividendPlan) AS o_DividendPlan,
	-- *INF*: IIF(ISNULL(DividendType),'N/A',DividendType)
	IFF(DividendType IS NULL, 'N/A', DividendType) AS o_DividendType,
	-- *INF*: IIF(ISNULL(v_DeductibleAmount),0.00,v_DeductibleAmount)
	IFF(v_DeductibleAmount IS NULL, 0.00, v_DeductibleAmount) AS o_DeductibleAmount,
	-- *INF*: IIF(ISNULL(PolicyPerAccidentLimit),'N/A',PolicyPerAccidentLimit)
	IFF(PolicyPerAccidentLimit IS NULL, 'N/A', PolicyPerAccidentLimit) AS o_LimitperAccident,
	-- *INF*: IIF(ISNULL(PolicyPerDiseaseLimit),'N/A',PolicyPerDiseaseLimit)
	IFF(PolicyPerDiseaseLimit IS NULL, 'N/A', PolicyPerDiseaseLimit) AS o_LimitperDisease,
	-- *INF*: IIF(ISNULL(PolicyAggregateLimit),'N/A',PolicyAggregateLimit)
	IFF(PolicyAggregateLimit IS NULL, 'N/A', PolicyAggregateLimit) AS o_AggregateLimit,
	-- *INF*: IIF(ISNULL(AdmiraltyActFlag),'0',(DECODE(AdmiraltyActFlag,'T','1','F','0','0')))
	IFF(
	    AdmiraltyActFlag IS NULL, '0',
	    (DECODE(
	            AdmiraltyActFlag,
	            'T', '1',
	            'F', '0',
	            '0'
	        ))
	) AS o_AdmiraltyActFlag,
	-- *INF*: IIF(ISNULL(FederalEmployersLiabilityActFlag),'0',(DECODE(FederalEmployersLiabilityActFlag,'T','1','F','0','0')))
	IFF(
	    FederalEmployersLiabilityActFlag IS NULL, '0',
	    (DECODE(
	            FederalEmployersLiabilityActFlag,
	            'T', '1',
	            'F', '0',
	            '0'
	        ))
	) AS o_FederalEmployersLiabilityActFlag,
	-- *INF*: IIF(ISNULL(USLongShoreAndHarborWorkersCompensationActFlag),'0',(DECODE(USLongShoreAndHarborWorkersCompensationActFlag,'T','1','F','0','0')))
	IFF(
	    USLongShoreAndHarborWorkersCompensationActFlag IS NULL, '0',
	    (DECODE(
	            USLongShoreAndHarborWorkersCompensationActFlag,
	            'T', '1',
	            'F', '0',
	            '0'
	        ))
	) AS o_USLongshoreandHarborWorkersCompensationActFlag,
	-- *INF*: IIF(ISNULL(DirectALAEPaidIR),0.00,DirectALAEPaidIR)
	IFF(DirectALAEPaidIR IS NULL, 0.00, DirectALAEPaidIR) AS v_DirectALAEPaid,
	-- *INF*: IIF(ISNULL(DirectLossPaidIR),0.00,DirectLossPaidIR)
	IFF(DirectLossPaidIR IS NULL, 0.00, DirectLossPaidIR) AS v_DirectLossPaid,
	-- *INF*: DECODE(LTRIM(RTRIM(TypeOfLoss)),'Medical',v_DirectLossPaid,0.0 )
	DECODE(
	    LTRIM(RTRIM(TypeOfLoss)),
	    'Medical', v_DirectLossPaid,
	    0.0
	) AS o_DirectLossPaidMedical,
	-- *INF*: DECODE(LTRIM(RTRIM(TypeOfLoss)),'Indemnity',v_DirectLossPaid,0.0 )
	DECODE(
	    LTRIM(RTRIM(TypeOfLoss)),
	    'Indemnity', v_DirectLossPaid,
	    0.0
	) AS o_DirectLossPaidIndemnity,
	v_DirectLossPaid AS o_DirectLossPaid,
	v_DirectALAEPaid AS o_DirectALAEPaid,
	v_DirectALAEPaid+v_DirectLossPaid AS o_DirectLossandALAEPaid,
	-- *INF*: IIF(ISNULL(DirectLossIncurred),0.00,DirectLossIncurred)
	IFF(DirectLossIncurred IS NULL, 0.00, DirectLossIncurred) AS v_DirectLossIncurred,
	-- *INF*: IIF(ISNULL(DirectALAEIncurred),0.00,DirectALAEIncurred)
	IFF(DirectALAEIncurred IS NULL, 0.00, DirectALAEIncurred) AS v_DirectALAEIncurred,
	-- *INF*: DECODE(LTRIM(RTRIM(TypeOfLoss)),'Medical',v_DirectLossIncurred,0.0)
	DECODE(
	    LTRIM(RTRIM(TypeOfLoss)),
	    'Medical', v_DirectLossIncurred,
	    0.0
	) AS o_DirectLossIncurredMedical,
	-- *INF*: DECODE(LTRIM(RTRIM(TypeOfLoss)),'Indemnity',v_DirectLossIncurred,0.0 )
	DECODE(
	    LTRIM(RTRIM(TypeOfLoss)),
	    'Indemnity', v_DirectLossIncurred,
	    0.0
	) AS o_DirectLossIncurredIndemnity,
	v_DirectLossIncurred AS o_DirectLossIncurred,
	v_DirectALAEIncurred AS o_DirectALAEIncurred,
	-- *INF*: IIF(ISNULL(DirectLossandALAEIncurred),0.00,DirectLossandALAEIncurred)
	IFF(DirectLossandALAEIncurred IS NULL, 0.00, DirectLossandALAEIncurred) AS o_DirectLossandALAEIncurred,
	-- *INF*: IIF(ISNULL(DirectLossRecoveriesPaid),0.00,DirectLossRecoveriesPaid)
	IFF(DirectLossRecoveriesPaid IS NULL, 0.00, DirectLossRecoveriesPaid) AS o_DirectLossRecoveriesPaid,
	-- *INF*: IIF(ISNULL(DirectALAERecoveriesPaid),0.00,DirectALAERecoveriesPaid)
	IFF(DirectALAERecoveriesPaid IS NULL, 0.00, DirectALAERecoveriesPaid) AS o_DirectALAERecoveriesPaid,
	-- *INF*: IIF(ISNULL(DirectOtherRecoveriesPaid),0.00,DirectOtherRecoveriesPaid)
	IFF(DirectOtherRecoveriesPaid IS NULL, 0.00, DirectOtherRecoveriesPaid) AS o_DirectOtherRecoveriesPaid,
	-- *INF*: IIF(ISNULL(DirectSalvagePaid),0.00,DirectSalvagePaid)
	IFF(DirectSalvagePaid IS NULL, 0.00, DirectSalvagePaid) AS o_DirectSalvagePaid,
	-- *INF*: IIF(ISNULL(DirectSubrogationPaid),0.00,DirectSubrogationPaid)
	IFF(DirectSubrogationPaid IS NULL, 0.00, DirectSubrogationPaid) AS o_DirectSubrogationPaid,
	-- *INF*: IIF(ISNULL(ClaimOccurrenceKey),'N/A',ClaimOccurrenceKey)
	IFF(ClaimOccurrenceKey IS NULL, 'N/A', ClaimOccurrenceKey) AS o_ClaimOccurrenceKey,
	-- *INF*: IIF(ISNULL(ClaimOccurrenceNumber),'N/A',ClaimOccurrenceNumber)
	IFF(ClaimOccurrenceNumber IS NULL, 'N/A', ClaimOccurrenceNumber) AS o_ClaimOccurrenceNumber,
	-- *INF*: IIF(ISNULL(ClaimNumber),'N/A',ClaimNumber)
	IFF(ClaimNumber IS NULL, 'N/A', ClaimNumber) AS o_ClaimNumber,
	-- *INF*: IIF(ISNULL(LossDescription),'N/A',LossDescription)
	IFF(LossDescription IS NULL, 'N/A', LossDescription) AS o_LossDescription,
	-- *INF*: IIF(ISNULL(v_LossLocationStateDescript),'N/A',SUBSTR(v_LossLocationStateDescript,1,(INSTR(v_LossLocationStateDescript,'|')-1)))
	IFF(
	    v_LossLocationStateDescript IS NULL, 'N/A',
	    SUBSTR(v_LossLocationStateDescript, 1, (REGEXP_INSTR(v_LossLocationStateDescript, '|') - 1))
	) AS o_LossLocationStateCode,
	-- *INF*: IIF(ISNULL(LossLocationStateAbbreviation),'N/A',LossLocationStateAbbreviation)
	IFF(LossLocationStateAbbreviation IS NULL, 'N/A', LossLocationStateAbbreviation) AS o_LossLocationStateAbbreviation,
	-- *INF*: IIF(ISNULL(v_LossLocationStateDescript),'N/A',SUBSTR(v_LossLocationStateDescript,(INSTR(v_LossLocationStateDescript,'|')+1),LENGTH(v_LossLocationStateDescript)))
	IFF(
	    v_LossLocationStateDescript IS NULL, 'N/A',
	    SUBSTR(v_LossLocationStateDescript, (REGEXP_INSTR(v_LossLocationStateDescript, '|') + 1), LENGTH(v_LossLocationStateDescript))
	) AS o_LossLocationStateDescription,
	-- *INF*: IIF(ISNULL(BodyPartCode),'N/A',BodyPartCode)
	IFF(BodyPartCode IS NULL, 'N/A', BodyPartCode) AS o_BodyPartCode,
	-- *INF*: IIF(ISNULL(BodyPartDescription),'N/A',BodyPartDescription)
	IFF(BodyPartDescription IS NULL, 'N/A', BodyPartDescription) AS o_BodyPartDescription,
	-- *INF*: IIF(ISNULL(NatureofInjuryCode),'N/A',NatureofInjuryCode)
	IFF(NatureofInjuryCode IS NULL, 'N/A', NatureofInjuryCode) AS o_NatureofInjuryCode,
	-- *INF*: IIF(ISNULL(NatureofInjuryDescription),'N/A',NatureofInjuryDescription)
	IFF(NatureofInjuryDescription IS NULL, 'N/A', NatureofInjuryDescription) AS o_NatureofInjuryDescription,
	-- *INF*: IIF(ISNULL(CauseofInjuryCode),'N/A',CauseofInjuryCode)
	IFF(CauseofInjuryCode IS NULL, 'N/A', CauseofInjuryCode) AS o_CauseofInjuryCode,
	-- *INF*: IIF(ISNULL(CauseofInjuryDescription),'N/A',CauseofInjuryDescription)
	IFF(CauseofInjuryDescription IS NULL, 'N/A', CauseofInjuryDescription) AS o_CauseofInjuryDescription,
	-- *INF*: IIF(ISNULL(ClaimStatus),'N/A',ClaimStatus)
	IFF(ClaimStatus IS NULL, 'N/A', ClaimStatus) AS o_ClaimStatus,
	v_DateofLoss AS o_DateofLoss,
	-- *INF*: GET_DATE_PART(v_DateofLoss,'YYYY')
	DATE_PART(v_DateofLoss, 'YYYY') AS o_DateofLossYear,
	-- *INF*: GET_DATE_PART(v_DateofLoss,'MM')
	DATE_PART(v_DateofLoss, 'MM') AS o_DateofLossMonth,
	-- *INF*: TO_CHAR(v_DateofLoss,'MONTH')
	TO_CHAR(v_DateofLoss, 'MONTH') AS o_DateofLossMonthDescription,
	-- *INF*: TO_INTEGER(TO_CHAR(v_DateofLoss,'Q'))
	CAST(TO_CHAR(v_DateofLoss, 'Q') AS INTEGER) AS o_DateofLossQuarter,
	v_ClaimReportedDate AS o_ClaimReportedDate,
	-- *INF*: GET_DATE_PART(v_ClaimReportedDate,'YYYY')
	DATE_PART(v_ClaimReportedDate, 'YYYY') AS o_ClaimReportedDateYear,
	-- *INF*: GET_DATE_PART(v_ClaimReportedDate,'MM')
	DATE_PART(v_ClaimReportedDate, 'MM') AS o_ClaimReportedDateMonth,
	-- *INF*: TO_CHAR(v_ClaimReportedDate,'MONTH')
	TO_CHAR(v_ClaimReportedDate, 'MONTH') AS o_ClaimReportedDateMonthDescription,
	-- *INF*: TO_INTEGER(TO_CHAR(v_ClaimReportedDate,'Q'))
	CAST(TO_CHAR(v_ClaimReportedDate, 'Q') AS INTEGER) AS o_ClaimReportedDateQuarter,
	-- *INF*: IIF(ISNULL(ClaimantName),'N/A',ClaimantName)
	IFF(ClaimantName IS NULL, 'N/A', ClaimantName) AS o_ClaimantName,
	-- *INF*: IIF(ISNULL(InternalCatastropheCode),'N/A',InternalCatastropheCode)
	IFF(InternalCatastropheCode IS NULL, 'N/A', InternalCatastropheCode) AS o_InternalCatastropheCode,
	-- *INF*: IIF(ISNULL(LitigationStatus),'N/A',LitigationStatus)
	IFF(LitigationStatus IS NULL, 'N/A', LitigationStatus) AS o_LitigationStatus,
	-- *INF*: DECODE(LTRIM(RTRIM(ClaimStatus)),
	-- 'C',         'CLOSED(EXCEED)',
	-- 'CNP',	  'CLOSED CLAIM WITH NO PAYMENT (PMS)',
	-- 'CWP',  'CLOSED CLAIM WITH PAYMENT (PMS)',
	-- 'E',	        'OPENED IN ERROR (EXCEED)',
	-- 'NOT',  'NOTICE ONLY (PMS)',
	-- 'O',        'OPEN (EXCEED)',
	-- 'OFF',	  'OFFSET (PMS)',
	-- 'OPE',	  'OPEN (PMS)',
	-- 'NO',	  'NOTICE ONLY (EXCEED)')
	DECODE(
	    LTRIM(RTRIM(ClaimStatus)),
	    'C', 'CLOSED(EXCEED)',
	    'CNP', 'CLOSED CLAIM WITH NO PAYMENT (PMS)',
	    'CWP', 'CLOSED CLAIM WITH PAYMENT (PMS)',
	    'E', 'OPENED IN ERROR (EXCEED)',
	    'NOT', 'NOTICE ONLY (PMS)',
	    'O', 'OPEN (EXCEED)',
	    'OFF', 'OFFSET (PMS)',
	    'OPE', 'OPEN (PMS)',
	    'NO', 'NOTICE ONLY (EXCEED)'
	) AS o_ClaimSourceStatusDescription,
	-- *INF*: IIF(ISNULL(FederalEmployerIDNumber),'N/A',FederalEmployerIDNumber)
	IFF(FederalEmployerIDNumber IS NULL, 'N/A', FederalEmployerIDNumber) AS o_FederalEmployerIDNumber,
	-- *INF*: IIF(ISNULL(PriorCarrier),'N/A',PriorCarrier)
	IFF(PriorCarrier IS NULL, 'N/A', PriorCarrier) AS o_PriorCarrier,
	-- *INF*: IIF(ISNULL(CoverageEffectiveDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),CoverageEffectiveDate)
	IFF(
	    CoverageEffectiveDate IS NULL, TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    CoverageEffectiveDate
	) AS o_CoverageEffectiveDate,
	-- *INF*: IIF(ISNULL(CoverageExpirationDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),CoverageExpirationDate)
	IFF(
	    CoverageExpirationDate IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    CoverageExpirationDate
	) AS o_CoverageExpirationDate,
	-- *INF*: IIF(ISNULL(CoverageCancellationDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),CoverageCancellationDate)
	IFF(
	    CoverageCancellationDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    CoverageCancellationDate
	) AS o_CoverageCancellationDate,
	-- *INF*: IIF(ISNULL(PriorPolicyEffectiveDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),PriorPolicyEffectiveDate)
	IFF(
	    PriorPolicyEffectiveDate IS NULL,
	    TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    PriorPolicyEffectiveDate
	) AS o_PriorPolicyEffectiveDate,
	-- *INF*: IIF(ISNULL(v_InjuryTypeCode),'N/A',v_InjuryTypeCode)
	IFF(v_InjuryTypeCode IS NULL, 'N/A', v_InjuryTypeCode) AS o_WorkersCompensationInjuryTypeAbbreviation,
	-- *INF*: IIF(ISNULL(v_InjuryTypeDescript),'N/A',v_InjuryTypeDescript)
	IFF(v_InjuryTypeDescript IS NULL, 'N/A', v_InjuryTypeDescript) AS o_WorkerscompensationInjuryTypeDescription,
	-- *INF*: IIF(ISNULL(DeathDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),DeathDate)
	IFF(
	    DeathDate IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), DeathDate
	) AS o_DeathDate,
	-- *INF*: IIF(ISNULL(i_RatedCoverageCode),'N/A',i_RatedCoverageCode)
	-- 
	IFF(i_RatedCoverageCode IS NULL, 'N/A', i_RatedCoverageCode) AS o_RatedCoverageCode,
	-- *INF*: IIF(ISNULL(i_RatedCoverageDescription),'N/A',i_RatedCoverageDescription)
	IFF(i_RatedCoverageDescription IS NULL, 'N/A', i_RatedCoverageDescription) AS o_RatedCoverageDescription,
	-- *INF*: IIF(ISNULL(i_Catalyst),'N/A',i_Catalyst)
	IFF(i_Catalyst IS NULL, 'N/A', i_Catalyst) AS o_Catalyst,
	-- *INF*: IIF(ISNULL(i_CauseOfDamage),'N/A',i_CauseOfDamage)
	IFF(i_CauseOfDamage IS NULL, 'N/A', i_CauseOfDamage) AS o_CauseOfDamage,
	-- *INF*: IIF(ISNULL(i_DamageCaused),'N/A',i_DamageCaused)
	IFF(i_DamageCaused IS NULL, 'N/A', i_DamageCaused) AS o_DamageCaused,
	-- *INF*: IIF(ISNULL(i_ItemDamaged),'N/A',i_ItemDamaged)
	IFF(i_ItemDamaged IS NULL, 'N/A', i_ItemDamaged) AS o_ItemDamaged
	FROM EXP_Pass_Values
	LEFT JOIN LKP_BusinessClassDim
	ON LKP_BusinessClassDim.BusinessClassDimId = EXP_Pass_Values.BusinessClassDimId
	LEFT JOIN LKP_Claim_Occurrence_Dim_Current
	ON LKP_Claim_Occurrence_Dim_Current.edw_claim_occurrence_ak_id = EXP_Pass_Values.edw_claim_occurrence_ak_id
	LEFT JOIN LKP_CoverageDetailDim_Current
	ON LKP_CoverageDetailDim_Current.edw_pol_ak_id = EXP_Pass_Values.pol_ak_id AND LKP_CoverageDetailDim_Current.CoverageGuid = EXP_Pass_Values.CoverageGuid
	LEFT JOIN LKP_CoverageDetailWorkersCompensationDim
	ON LKP_CoverageDetailWorkersCompensationDim.edw_pol_ak_id = EXP_Pass_Values.pol_ak_id AND LKP_CoverageDetailWorkersCompensationDim.ClassCode = EXP_Pass_Values.ClassCode
	LEFT JOIN LKP_DividendTypeDim
	ON LKP_DividendTypeDim.DividendTypeDimId = LKP_DividendFact.DividendTypeDimId
	LEFT JOIN LKP_Injury_Type
	ON LKP_Injury_Type.edw_claim_party_occurrence_ak_id = EXP_Pass_Values.edw_claim_party_occurrence_ak_id
	LEFT JOIN LKP_InsuranceReferencecoverageDim
	ON LKP_InsuranceReferencecoverageDim.InsuranceReferenceCoverageDimId = EXP_Pass_Values.InsuranceReferenceCoverageDimId
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = EXP_Pass_Values.pol_ak_id
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.SalesDivisionDimID = EXP_Pass_Values.SalesDivisionDimId
	LEFT JOIN LKP_UnderwritingDivisionDim
	ON LKP_UnderwritingDivisionDim.UnderwritingDivisionDimID = EXP_Pass_Values.UnderwritingDivisionDimId
	LEFT JOIN LKP_claimant_dim_Current
	ON LKP_claimant_dim_Current.edw_claim_party_occurrence_ak_id = EXP_Pass_Values.edw_claim_party_occurrence_ak_id
	LEFT JOIN LKP_PHYSICALSTATE LKP_PHYSICALSTATE_RatingStateProvinceAbbreviation
	ON LKP_PHYSICALSTATE_RatingStateProvinceAbbreviation.StateAbbreviation = RatingStateProvinceAbbreviation

	LEFT JOIN LKP_PHYSICALSTATE LKP_PHYSICALSTATE_AgencyStateAbbreviation
	ON LKP_PHYSICALSTATE_AgencyStateAbbreviation.StateAbbreviation = AgencyStateAbbreviation

	LEFT JOIN LKP_PHYSICALSTATE LKP_PHYSICALSTATE_PrimaryAgencyStateAbbreviation
	ON LKP_PHYSICALSTATE_PrimaryAgencyStateAbbreviation.StateAbbreviation = PrimaryAgencyStateAbbreviation

	LEFT JOIN LKP_PHYSICALSTATE LKP_PHYSICALSTATE_LossLocationStateAbbreviation
	ON LKP_PHYSICALSTATE_LossLocationStateAbbreviation.StateAbbreviation = LossLocationStateAbbreviation

),
ActuarialWorkersCompensationLoss AS (
	TRUNCATE TABLE ActuarialWorkersCompensationLoss;
	INSERT INTO ActuarialWorkersCompensationLoss
	(AuditId, CreatedDate, ModifiedDate, EDWLossMasterFactId, EnterpriseGroupCode, EnterpriseGroupAbbreviation, StrategicProfitCenterCode, StrategicProfitCenterAbbreviation, LegalEntityCode, LegalEntityAbbreviation, PolicyOfferingCode, PolicyOfferingAbbreviation, PolicyOfferingDescription, ProductCode, ProductAbbreviation, ProductDescription, LineofBusinessCode, LineofBusinessAbbreviation, LineofBusinessDescription, AccountingProductCode, AccountingProductAbbreviation, AccountingProductDescription, RatingPlanCode, RatingPlanDescription, InsuranceSegmentCode, InsuranceSegmentDescription, ProgramCode, ProgramDescription, AssociationCode, AssociationDescription, CoverageSummaryCode, CoverageSummaryDescription, CoverageGroupCode, CoverageGroupDescription, CoverageCode, CoverageDescription, ClassCode, ClassCodeDescription, IndustryRiskGradeCode, IndustryRiskGradeDescription, PolicyEffectiveYear, PolicyEffectiveQuarter, PolicyEffectiveMonthNumber, PolicyEffectiveMonthDescription, PolicyEffectiveDate, PolicyExpirationYear, PolicyExpirationQuarter, PolicyExpirationMonthNumber, PolicyExpirationMonthDescription, PolicyExpirationDate, PolicyCancellationYear, PolicyCancellationQuarter, PolicyCancellationMonth, PolicyCancellationMonthDescription, PolicyCancellationDate, PolicyCancellationReasonCode, PolicyCancellationReasonCodeDescription, PolicyOriginalInceptionDate, PolicyRenewalCode, PolicyRenewalDescription, PolicyStatusCode, PolicyStatusCodeDescription, AccountingYear, AccountingMonthQuarter, AccountingMonthNumber, AccountingMonthName, AccountingDate, RatingStateCode, RatingStateAbbreviation, RatingStateName, LocationNumber, RatingLocationCity, RatingLocationCounty, RatingLocationZIPCode, PolicyKey, PolicySymbol, PolicyNumber, PolicyVersion, PolicyIssueCode, PolicyIssueCodeDescription, PrimaryRatingStateCode, PrimaryRatingStateAbbreviation, PrimaryRatingStateDescription, PrimaryBusinessClassificationCode, PrimaryBusinessClassificationDescription, BusinessSegmentCode, BusinessSegmentDescription, StrategicBusinessGroupCode, StrategicBusinessGroupDescription, AgencyCode, AgencyDoingBusinessAsName, AgencyStateCode, AgencyStateAbbreviation, AgencyStateDescription, AgencyPhysicalAddressCity, AgencyZIPCode, ProducerCode, ProducerFullName, UnderwriterFullName, UnderwritingManagerName, UnderwritingRegionName, PrimaryAgencyCode, PrimaryAgencyDoingBusinessAsName, PrimaryAgencyStateCode, PrimaryAgencyStateAbbreviation, PrimaryAgencyStateDescription, PrimaryAgencyPhysicalAddressCity, PrimaryAgencyZIPCode, RegionalSalesManagerFullName, SalesTerritoryCode, CustomerNumber, FirstNamedInsured, SICCode, SICDescription, NAICSCode, NAICSDescription, CustomerCareIndicator, NewBusinessIndicator, PriorPolicyKey, PriorPolicyNumber, PriorPolicySymbol, PriorPolicyVersion, LastBookedDate, HazardGroupCode, DividendPlan, DividendType, DeductibleAmount, LimitperAccident, LimitperDisease, AggregateLimit, AdmiraltyActFlag, FederalEmployersLiabilityActFlag, USLongshoreandHarborWorkersCompensationActFlag, DirectLossPaidMedical, DirectLossPaidIndemnity, DirectLossPaid, DirectALAEPaid, DirectLossandALAEPaid, DirectLossIncurredMedical, DirectLossIncurredIndemnity, DirectLossIncurred, DirectALAEIncurred, DirectLossandALAEIncurred, DirectLossRecoveriesPaid, DirectALAERecoveriesPaid, DirectOtherRecoveriesPaid, DirectSalvagePaid, DirectSubrogationPaid, ClaimOccurrenceKey, ClaimOccurrenceNumber, ClaimNumber, LossDescription, LossLocationStateCode, LossLocationStateAbbreviation, LossLocationStateDescription, BodyPartCode, BodyPartDescription, NatureofInjuryCode, NatureofInjuryDescription, CauseofInjuryCode, CauseofInjuryDescription, ClaimStatus, DateofLoss, DateofLossYear, DateofLossMonth, DateofLossMonthDescription, DateofLossQuarter, ClaimReportedDate, ClaimReportedDateYear, ClaimReportedDateMonth, ClaimReportedDateMonthDescription, ClaimReportedDateQuarter, ClaimantName, InternalCatastropheCode, LitigationStatus, ClaimSourceStatusDescription, FederalEmployerIDNumber, PriorCarrier, CoverageEffectiveDate, CoverageExpirationDate, CoverageCancellationDate, PriorPolicyEffectiveDate, WorkersCompensationInjuryTypeAbbreviation, WorkerscompensationInjuryTypeDescription, DeathDate, RatedCoverageCode, RatedCoverageDescription, cs_catalyst, cs_cause_of_damage, cs_damage_caused, cs_item_damaged)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	EDWLOSSMASTERFACTID, 
	o_EnterpriseGroupCode AS ENTERPRISEGROUPCODE, 
	o_EnterpriseGroupAbbreviation AS ENTERPRISEGROUPABBREVIATION, 
	o_StrategicProfitCenterCode AS STRATEGICPROFITCENTERCODE, 
	o_StrategicProfitCenterAbbreviation AS STRATEGICPROFITCENTERABBREVIATION, 
	o_LegalEntityCode AS LEGALENTITYCODE, 
	o_LegalEntityAbbreviation AS LEGALENTITYABBREVIATION, 
	o_PolicyOfferingCode AS POLICYOFFERINGCODE, 
	o_PolicyOfferingAbbreviation AS POLICYOFFERINGABBREVIATION, 
	o_PolicyOfferingDescription AS POLICYOFFERINGDESCRIPTION, 
	o_ProductCode AS PRODUCTCODE, 
	o_ProductAbbreviation AS PRODUCTABBREVIATION, 
	o_ProductDescription AS PRODUCTDESCRIPTION, 
	o_LineofBusinessCode AS LINEOFBUSINESSCODE, 
	o_LineofBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	o_LineofBusinessDescription AS LINEOFBUSINESSDESCRIPTION, 
	o_AccountingProductCode AS ACCOUNTINGPRODUCTCODE, 
	o_AccountingProductAbbreviation AS ACCOUNTINGPRODUCTABBREVIATION, 
	o_AccountingProductDescription AS ACCOUNTINGPRODUCTDESCRIPTION, 
	o_RatingPlanCode AS RATINGPLANCODE, 
	o_RatingPlanDescription AS RATINGPLANDESCRIPTION, 
	o_InsuranceSegmentCode AS INSURANCESEGMENTCODE, 
	o_InsuranceSegmentDescription AS INSURANCESEGMENTDESCRIPTION, 
	o_ProgramCode AS PROGRAMCODE, 
	o_ProgramDescription AS PROGRAMDESCRIPTION, 
	o_AssociationCode AS ASSOCIATIONCODE, 
	o_AssociationDescription AS ASSOCIATIONDESCRIPTION, 
	o_CoverageSummaryCode AS COVERAGESUMMARYCODE, 
	o_CoverageSummaryDescription AS COVERAGESUMMARYDESCRIPTION, 
	o_CoverageGroupCode AS COVERAGEGROUPCODE, 
	o_CoverageGroupDescription AS COVERAGEGROUPDESCRIPTION, 
	o_CoverageCode AS COVERAGECODE, 
	o_CoverageDescription AS COVERAGEDESCRIPTION, 
	o_ClassCode AS CLASSCODE, 
	o_ClassCodeDescription AS CLASSCODEDESCRIPTION, 
	o_IndustryRiskGradeCode AS INDUSTRYRISKGRADECODE, 
	o_IndustryRiskGradeDescription AS INDUSTRYRISKGRADEDESCRIPTION, 
	o_PolicyEffectiveYear AS POLICYEFFECTIVEYEAR, 
	o_PolicyEffectiveQuarter AS POLICYEFFECTIVEQUARTER, 
	o_PolicyEffectiveMonthNumber AS POLICYEFFECTIVEMONTHNUMBER, 
	o_PolicyEffectiveMonthDescription AS POLICYEFFECTIVEMONTHDESCRIPTION, 
	o_PolicyEffectiveDate AS POLICYEFFECTIVEDATE, 
	o_PolicyExpirationYear AS POLICYEXPIRATIONYEAR, 
	o_PolicyExpirationQuarter AS POLICYEXPIRATIONQUARTER, 
	o_PolicyExpirationMonthNumber AS POLICYEXPIRATIONMONTHNUMBER, 
	o_PolicyExpirationMonthDescription AS POLICYEXPIRATIONMONTHDESCRIPTION, 
	o_PolicyExpirationDate AS POLICYEXPIRATIONDATE, 
	o_PolicyCancellationYear AS POLICYCANCELLATIONYEAR, 
	o_PolicyCancellationQuarter AS POLICYCANCELLATIONQUARTER, 
	o_PolicyCancellationMonth AS POLICYCANCELLATIONMONTH, 
	o_PolicyCancellationMonthDescription AS POLICYCANCELLATIONMONTHDESCRIPTION, 
	o_PolicyCancellationDate AS POLICYCANCELLATIONDATE, 
	o_PolicyCancellationReasonCode AS POLICYCANCELLATIONREASONCODE, 
	o_PolicyCancellationReasonCodeDescription AS POLICYCANCELLATIONREASONCODEDESCRIPTION, 
	o_PolicyOriginalInceptionDate AS POLICYORIGINALINCEPTIONDATE, 
	o_PolicyRenewalCode AS POLICYRENEWALCODE, 
	o_PolicyRenewalDescription AS POLICYRENEWALDESCRIPTION, 
	o_PolicyStatusCode AS POLICYSTATUSCODE, 
	o_PolicyStatusCodeDescription AS POLICYSTATUSCODEDESCRIPTION, 
	o_AccountingYear AS ACCOUNTINGYEAR, 
	o_AccountingMonthQuarter AS ACCOUNTINGMONTHQUARTER, 
	o_AccountingMonthNumber AS ACCOUNTINGMONTHNUMBER, 
	o_AccountingMonthName AS ACCOUNTINGMONTHNAME, 
	o_AccountingDate AS ACCOUNTINGDATE, 
	o_RatingStateCode AS RATINGSTATECODE, 
	o_RatingStateAbbreviation AS RATINGSTATEABBREVIATION, 
	o_RatingStateName AS RATINGSTATENAME, 
	o_LocationNumber AS LOCATIONNUMBER, 
	o_RatingLocationCity AS RATINGLOCATIONCITY, 
	o_RatingLocationCounty AS RATINGLOCATIONCOUNTY, 
	o_RatingLocationZIPCode AS RATINGLOCATIONZIPCODE, 
	o_PolicyKey AS POLICYKEY, 
	o_PolicySymbol AS POLICYSYMBOL, 
	o_PolicyNumber AS POLICYNUMBER, 
	o_PolicyVersion AS POLICYVERSION, 
	o_PolicyIssueCode AS POLICYISSUECODE, 
	o_PolicyIssueCodeDescription AS POLICYISSUECODEDESCRIPTION, 
	o_PrimaryRatingStateCode AS PRIMARYRATINGSTATECODE, 
	o_PrimaryRatingStateAbbreviation AS PRIMARYRATINGSTATEABBREVIATION, 
	o_PrimaryRatingStateDescription AS PRIMARYRATINGSTATEDESCRIPTION, 
	o_PrimaryBusinessClassificationCode AS PRIMARYBUSINESSCLASSIFICATIONCODE, 
	o_PrimaryBusinessClassificationDescription AS PRIMARYBUSINESSCLASSIFICATIONDESCRIPTION, 
	o_BusinessSegmentCode AS BUSINESSSEGMENTCODE, 
	o_BusinessSegmentDescription AS BUSINESSSEGMENTDESCRIPTION, 
	o_StrategicBusinessGroupCode AS STRATEGICBUSINESSGROUPCODE, 
	o_StrategicBusinessGroupDescription AS STRATEGICBUSINESSGROUPDESCRIPTION, 
	o_AgencyCode AS AGENCYCODE, 
	o_AgencyDoingBusinessAsName AS AGENCYDOINGBUSINESSASNAME, 
	o_AgencyStateCode AS AGENCYSTATECODE, 
	o_AgencyStateAbbreviation AS AGENCYSTATEABBREVIATION, 
	o_AgencyStateDescription AS AGENCYSTATEDESCRIPTION, 
	o_AgencyPhysicalAddressCity AS AGENCYPHYSICALADDRESSCITY, 
	o_AgencyZIPCode AS AGENCYZIPCODE, 
	o_ProducerCode AS PRODUCERCODE, 
	o_ProducerFullName AS PRODUCERFULLNAME, 
	o_UnderwriterFullName AS UNDERWRITERFULLNAME, 
	o_UnderwritingManagerName AS UNDERWRITINGMANAGERNAME, 
	o_UnderwritingRegionName AS UNDERWRITINGREGIONNAME, 
	o_PrimaryAgencyCode AS PRIMARYAGENCYCODE, 
	o_PrimaryAgencyDoingBusinessAsName AS PRIMARYAGENCYDOINGBUSINESSASNAME, 
	o_PrimaryAgencyStateCode AS PRIMARYAGENCYSTATECODE, 
	o_PrimaryAgencyStateAbbreviation AS PRIMARYAGENCYSTATEABBREVIATION, 
	o_PrimaryAgencyStateDescription AS PRIMARYAGENCYSTATEDESCRIPTION, 
	o_PrimaryAgencyPhysicalAddressCity AS PRIMARYAGENCYPHYSICALADDRESSCITY, 
	o_PrimaryAgencyZIPCode AS PRIMARYAGENCYZIPCODE, 
	o_RegionalSalesManagerFullName AS REGIONALSALESMANAGERFULLNAME, 
	o_SalesTerritoryCode AS SALESTERRITORYCODE, 
	o_CustomerNumber AS CUSTOMERNUMBER, 
	o_FirstNamedInsured AS FIRSTNAMEDINSURED, 
	o_SICCode AS SICCODE, 
	o_SICDescription AS SICDESCRIPTION, 
	o_NAICSCode AS NAICSCODE, 
	o_NAICSDescription AS NAICSDESCRIPTION, 
	o_CustomerCareIndicator AS CUSTOMERCAREINDICATOR, 
	o_NewBusinessIndicator AS NEWBUSINESSINDICATOR, 
	o_PriorPolicyKey AS PRIORPOLICYKEY, 
	o_PriorPolicyNumber AS PRIORPOLICYNUMBER, 
	o_PriorPolicySymbol AS PRIORPOLICYSYMBOL, 
	o_PriorPolicyVersion AS PRIORPOLICYVERSION, 
	o_LastBookedDate AS LASTBOOKEDDATE, 
	o_HazardGroupCode AS HAZARDGROUPCODE, 
	o_DividendPlan AS DIVIDENDPLAN, 
	o_DividendType AS DIVIDENDTYPE, 
	o_DeductibleAmount AS DEDUCTIBLEAMOUNT, 
	o_LimitperAccident AS LIMITPERACCIDENT, 
	o_LimitperDisease AS LIMITPERDISEASE, 
	o_AggregateLimit AS AGGREGATELIMIT, 
	o_AdmiraltyActFlag AS ADMIRALTYACTFLAG, 
	o_FederalEmployersLiabilityActFlag AS FEDERALEMPLOYERSLIABILITYACTFLAG, 
	o_USLongshoreandHarborWorkersCompensationActFlag AS USLONGSHOREANDHARBORWORKERSCOMPENSATIONACTFLAG, 
	o_DirectLossPaidMedical AS DIRECTLOSSPAIDMEDICAL, 
	o_DirectLossPaidIndemnity AS DIRECTLOSSPAIDINDEMNITY, 
	o_DirectLossPaid AS DIRECTLOSSPAID, 
	o_DirectALAEPaid AS DIRECTALAEPAID, 
	o_DirectLossandALAEPaid AS DIRECTLOSSANDALAEPAID, 
	o_DirectLossIncurredMedical AS DIRECTLOSSINCURREDMEDICAL, 
	o_DirectLossIncurredIndemnity AS DIRECTLOSSINCURREDINDEMNITY, 
	o_DirectLossIncurred AS DIRECTLOSSINCURRED, 
	o_DirectALAEIncurred AS DIRECTALAEINCURRED, 
	o_DirectLossandALAEIncurred AS DIRECTLOSSANDALAEINCURRED, 
	o_DirectLossRecoveriesPaid AS DIRECTLOSSRECOVERIESPAID, 
	o_DirectALAERecoveriesPaid AS DIRECTALAERECOVERIESPAID, 
	o_DirectOtherRecoveriesPaid AS DIRECTOTHERRECOVERIESPAID, 
	o_DirectSalvagePaid AS DIRECTSALVAGEPAID, 
	o_DirectSubrogationPaid AS DIRECTSUBROGATIONPAID, 
	o_ClaimOccurrenceKey AS CLAIMOCCURRENCEKEY, 
	o_ClaimOccurrenceNumber AS CLAIMOCCURRENCENUMBER, 
	o_ClaimNumber AS CLAIMNUMBER, 
	o_LossDescription AS LOSSDESCRIPTION, 
	o_LossLocationStateCode AS LOSSLOCATIONSTATECODE, 
	o_LossLocationStateAbbreviation AS LOSSLOCATIONSTATEABBREVIATION, 
	o_LossLocationStateDescription AS LOSSLOCATIONSTATEDESCRIPTION, 
	o_BodyPartCode AS BODYPARTCODE, 
	o_BodyPartDescription AS BODYPARTDESCRIPTION, 
	o_NatureofInjuryCode AS NATUREOFINJURYCODE, 
	o_NatureofInjuryDescription AS NATUREOFINJURYDESCRIPTION, 
	o_CauseofInjuryCode AS CAUSEOFINJURYCODE, 
	o_CauseofInjuryDescription AS CAUSEOFINJURYDESCRIPTION, 
	o_ClaimStatus AS CLAIMSTATUS, 
	o_DateofLoss AS DATEOFLOSS, 
	o_DateofLossYear AS DATEOFLOSSYEAR, 
	o_DateofLossMonth AS DATEOFLOSSMONTH, 
	o_DateofLossMonthDescription AS DATEOFLOSSMONTHDESCRIPTION, 
	o_DateofLossQuarter AS DATEOFLOSSQUARTER, 
	o_ClaimReportedDate AS CLAIMREPORTEDDATE, 
	o_ClaimReportedDateYear AS CLAIMREPORTEDDATEYEAR, 
	o_ClaimReportedDateMonth AS CLAIMREPORTEDDATEMONTH, 
	o_ClaimReportedDateMonthDescription AS CLAIMREPORTEDDATEMONTHDESCRIPTION, 
	o_ClaimReportedDateQuarter AS CLAIMREPORTEDDATEQUARTER, 
	o_ClaimantName AS CLAIMANTNAME, 
	o_InternalCatastropheCode AS INTERNALCATASTROPHECODE, 
	o_LitigationStatus AS LITIGATIONSTATUS, 
	o_ClaimSourceStatusDescription AS CLAIMSOURCESTATUSDESCRIPTION, 
	o_FederalEmployerIDNumber AS FEDERALEMPLOYERIDNUMBER, 
	o_PriorCarrier AS PRIORCARRIER, 
	o_CoverageEffectiveDate AS COVERAGEEFFECTIVEDATE, 
	o_CoverageExpirationDate AS COVERAGEEXPIRATIONDATE, 
	o_CoverageCancellationDate AS COVERAGECANCELLATIONDATE, 
	o_PriorPolicyEffectiveDate AS PRIORPOLICYEFFECTIVEDATE, 
	o_WorkersCompensationInjuryTypeAbbreviation AS WORKERSCOMPENSATIONINJURYTYPEABBREVIATION, 
	o_WorkerscompensationInjuryTypeDescription AS WORKERSCOMPENSATIONINJURYTYPEDESCRIPTION, 
	o_DeathDate AS DEATHDATE, 
	o_RatedCoverageCode AS RATEDCOVERAGECODE, 
	o_RatedCoverageDescription AS RATEDCOVERAGEDESCRIPTION, 
	o_Catalyst AS CS_CATALYST, 
	o_CauseOfDamage AS CS_CAUSE_OF_DAMAGE, 
	o_DamageCaused AS CS_DAMAGE_CAUSED, 
	o_ItemDamaged AS CS_ITEM_DAMAGED
	FROM EXP_Get_Values
),