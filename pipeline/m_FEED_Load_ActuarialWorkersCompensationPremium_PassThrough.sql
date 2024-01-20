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
LKP_ActuarialWorkersCompensationPremium_Taxes_Tgt AS (
	SELECT
	ActuarialWorkersCompensationPremiumId,
	EDWActuarialWorkersCompensationFactId
	FROM (
		SELECT 
			ActuarialWorkersCompensationPremiumId,
			EDWActuarialWorkersCompensationFactId
		FROM ActuarialWorkersCompensationPremium
		WHERE EDWActuarialWorkersCompensationFactType = 'PassThrough'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWActuarialWorkersCompensationFactId ORDER BY ActuarialWorkersCompensationPremiumId) = 1
),
SQ_ActuarialAnalysis23Monthly_PassThrough AS (
	Declare @Date as date
	
	set @Date = case when '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QA%' or '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QC%'
	then
	convert(date,EOMONTH(getdate(),0))
	else
	convert(date,EOMONTH(getdate(),-1))
	end ;
	
	WITH CTE   --- This temp table is being used to get the WorkerCompensation Policy list		
	AS		
	(		
	Select edw_pol_ak_id,InsuranceReferenceDimId,EnterpriseGroupCode,EnterpriseGroupAbbreviation,StrategicProfitCenterCode,StrategicProfitCenterAbbreviation,		
	InsuranceReferenceLegalEntityCode,InsuranceReferenceLegalEntityAbbreviation,PolicyOfferingCode,PolicyOfferingAbbreviation,PolicyOfferingDescription,		
	ProductCode,ProductAbbreviation,ProductDescription,InsuranceReferenceLineOfBusinessCode,InsuranceReferenceLineOfBusinessAbbreviation,		
	InsuranceReferenceLineOfBusinessDescription,AccountingProductCode,AccountingProductAbbreviation,AccountingProductDescription,InsuranceSegmentCode,InsuranceSegmentDescription		
	FROM		
	(		
	select  PD.edw_pol_ak_id,PMF.InsuranceReferenceDimId,IRD.EnterpriseGroupCode,IRD.EnterpriseGroupAbbreviation,IRD.StrategicProfitCenterCode,IRD.StrategicProfitCenterAbbreviation,		
	IRD.InsuranceReferenceLegalEntityCode,IRD.InsuranceReferenceLegalEntityAbbreviation,IRD.PolicyOfferingCode,IRD.PolicyOfferingAbbreviation,IRD.PolicyOfferingDescription,		
	IRD.ProductCode,IRD.ProductAbbreviation,IRD.ProductDescription,IRD.InsuranceReferenceLineOfBusinessCode,IRD.InsuranceReferenceLineOfBusinessAbbreviation,		
	IRD.InsuranceReferenceLineOfBusinessDescription,IRD.AccountingProductCode,IRD.AccountingProductAbbreviation,IRD.AccountingProductDescription,IRD.InsuranceSegmentCode,		
	IRD.InsuranceSegmentDescription,ROW_NUMBER() Over (Partition by PD.edw_pol_ak_id order by PD.Pol_Dim_Id desc) as Row_num 		
	from 		
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on PMF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId and IRD.PolicyOfferingAbbreviation='WC'		
	Inner join policy_dim PD on PMF.PolicyDimID = PD.pol_dim_id 
	--and PD.crrnt_snpsht_flag=1		
	) CTE where CTE.Row_num=1		
	)		
	
	SELECT 
	PCTF.PassThroughChargeTransactionFactID as ActuarialworkerscompensationFactID,
	 CTE.EnterpriseGroupCode as EnterpriseGroupCode,
	 CTE.EnterpriseGroupAbbreviation as EnterpriseGroupAbbreviation,
	 CTE.StrategicProfitCenterCode as StrategicProfitCenterCode,
	 CTE.StrategicProfitCenterAbbreviation as StrategicProfitCenterAbbreviation,
	 CTE.InsuranceReferenceLegalEntityCode as LegalEntityCode,
	 CTE.InsuranceReferenceLegalEntityAbbreviation as LegalEntityAbbreviation,
	 CTE.PolicyOfferingCode as PolicyOfferingCode,
	 CTE.PolicyOfferingAbbreviation as PolicyOfferingAbbreviation,
	 CTE.PolicyOfferingDescription as PolicyOfferingDescription,
	 CTE.ProductCode as ProductCode,
	 CTE.ProductAbbreviation as ProductAbbreviation,
	 CTE.ProductDescription as ProductDescription,
	 CTE.InsuranceReferenceLineOfBusinessCode as LineofBusinessCode,
	 CTE.InsuranceReferenceLineOfBusinessAbbreviation as LineofBusinessAbbreviation,
	 CTE.InsuranceReferenceLineOfBusinessDescription as LineofBusinessDescription,
	 CTE.AccountingProductCode as AccountingProductCode,
	 CTE.AccountingProductAbbreviation as AccountingProductAbbreviation,
	 CTE.AccountingProductDescription as AccountingProductDescription,
	 IRD.RatingPlanCode as RatingPlanCode,
	 IRD.RatingPlanDescription as RatingPlanDescription, 
	 CTE.InsuranceSegmentCode as InsuranceSegmentCode,
	 CTE.InsuranceSegmentDescription as InsuranceSegmentDescription,
	 pol_Current.ProgramCode as ProgramCode,
	 pol_Current.ProgramDescription as ProgramDescription,
	 pol_Current.AssociationCode as AssociationCode,
	 pol_Current.AssociationDescription as AssociationDescription,
	 pol_Current.industry_risk_grade_code as IndustryRiskGradeCode,
	 pol_Current.industry_risk_grade_code_descript as IndustryRiskGradeDescription,
	 pol_Current.pol_eff_date as PolicyEffectiveDate,
	 pol_Current.pol_exp_date as PolicyExpirationDate,
	 pol_Current.pol_cancellation_rsn_code as PolicyCancellationReasonCode,
	 pol_Current.pol_cancellation_rsn_code_descript as PolicyCancellationReasonCodeDescription,
	 pol_Current.orig_incptn_date as PolicyOriginalInceptionDate,
	 pol_Current.renl_code as PolicyRenewalCode,
	 pol_Current.renl_code_descript as PolicyRenewalDescription,
	 pol_Current.pol_status_code as PolicyStatusCode,
	 pol_Current.pol_status_code_descript as PolicyStatusCodeDescription,
	 CD.clndr_date as AccountingDate,
	 RLD.StateProvinceCode as RatingStateCode,
	 RLD.StateProvinceCodeAbbreviation as RatingStateAbbreviation,
	 RLD.LocationNumber as LocationNumber,
	 RLD.RatingCity as RatingLocationCity,
	 RLD.RatingCounty as RatingLocationCounty,
	 RLD.ZipPostalCode as RatingLocationZIPCode,
	 pol_Current.pol_key as PolicyKey,
	 pol_Current.pol_sym as PolicySymbol,
	 pol_Current.pol_num as PolicyNumber,
	 pol_Current.pol_mod as PolicyVersion,
	 pol_Current.pol_issue_code as PolicyIssueCode,
	 pol_Current.pol_issue_code_descript as PolicyIssueCodeDescription,
	 pol_Current.state_of_domicile_code as PrimaryRatingStateCode,
	 pol_Current.state_of_domicile_abbrev as PrimaryRatingStateAbbreviation,
	 pol_Current.state_of_domicile_code_descript as PrimaryRatingStateDescription,
	 pol_Current.prim_bus_class_code as PrimaryBusinessClassificationCode,
	 pol_Current.prim_bus_class_code_descript as PrimaryBusinessClassificationDescription,
	 pol_Current.BusinessClassDimId as BusinessClassDimId,
	 ad_Current.AgencyCode as AgencyCode,
	 ad_Current.AgencyDoingBusinessAsName as AgencyDoingBusinessAsName,
	 ad_current.PhysicalStateAbbreviation as AgencyStateAbbreviation,
	 ad_Current.PhysicalCity as AgencyPhysicalAddressCity,
	 ad_Current.PhysicalZipCode as AgencyZIPCode,
	 aed_current.ProducerCode as ProducerCode,
	 (case when aed_current.AgencyEmployeeRole= 'Producer' then 
	aed_current.AgencyEmployeeFirstName + ' ' + aed_current.agencyemployeelastname else 'N/A' end ) as ProducerFullName,
	pol_Current.UnderwritingDivisionDimId as UnderwritingDivisionDimId,
	-- Removed below columns as per WREQ-13710
	-- ad_Current.LegalPrimaryAgencyCode as PrimaryAgencyCode,
	-- SD.AgencyDoingBusinessAsName as PrimaryAgencyDoingBusinessAsName,
	-- SD.PhysicalStateAbbreviation as PrimaryAgencyStateAbbreviation,
	-- SD.PhysicalCity as PrimaryAgencyPhysicalAddressCity,
	-- SD.PhysicalZipCode as PrimaryAgencyZIPCode,
	 ad_current.SalesDivisionDimId as SalesDivisionDimId,
	 ccd_Current.cust_num as CustomerNumber,
	 ccd_Current.name as FirstNamedInsured,
	 ccd_Current.sic_code as SICCode,
	 ccd_Current.sic_code_descript as SICDescription,
	 ccd_Current.naics_code as NAICSCode,
	 ccd_Current.naics_code_descript as NAICSDescription,
	 pol_Current.serv_center_support_code as CustomerCareIndicator,
	 pol_Current.prior_pol_key as PriorPolicyKey,
	 prior_pol.pol_num as PriorPolicyNumber,
	 prior_pol.pol_sym as PriorPolicySymbol,
	 prior_pol.pol_mod as PriorPolicyVersion,
	 pol_Current.edw_pol_ak_id as pol_ak_id,
	 PCTF.PassThroughChargeTransactionAmount as pass_thru_amt,
	 ccd_Current.fed_tax_id as FederalEmployerIDNumberfein,
	 pol_Current.RolloverPriorCarrier as PriorCarrier,
	 prior_pol.pol_eff_date as PriorPolicyEffectiveDate,
	 AD_Current.EDWAgencyAKID
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PassThroughChargeTransactionFact PCTF 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on PCTF.PolicyDimId= pol.pol_dim_id 
	INNER JOIN CTE on pol.edw_pol_ak_id= CTE.edw_pol_ak_id
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol_Current -- to get the current policy attribute 
	on CTE.edw_pol_ak_id =pol_Current.edw_pol_ak_id and pol_Current.crrnt_snpsht_flag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim AD on PCTF.AgencyDimId =AD.AgencyDimID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim AD_Current -- to get the current Agency attribute 
	on AD.EDWAgencyAKID= AD_Current.EDWAgencyAKID and AD_Current.CurrentSnapshotFlag =1 
	--INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD on SD.AgencyCode=ad_Current.LegalPrimaryAgencyCode AND SD.CurrentSnapshotFlag= 1 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim CCD on PCTF.ContractCustomerDimId=CCD.contract_cust_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim ccd_Current -- to get the current customer attribute
	on ccd.edw_contract_cust_ak_id= ccd_Current.edw_contract_cust_ak_id and ccd_Current.crrnt_snpsht_flag =1 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim AED on AED.AgencyEmployeeDimID= pol_Current.AgencyEmployeeDimID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim aed_current --to get the current AgencyEmployeeDim attribute
	on aed.EDWAgencyEmployeeAKID = aed_current.EDWAgencyEmployeeAKID  and aed_current.CurrentSnapshotFlag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD on CD.clndr_id = PCTF.PassThroughChargeTransactionBookedDateId		
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= PCTF.InsuranceReferenceDimId --and InsuranceReferenceLineOfBusinessAbbreviation='WC'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocationDim RLD on PCTF.RiskLocationDimID = RLD.RiskLocationDimID and RLD.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim prior_pol on prior_pol.pol_key= pol_Current.prior_pol_key and prior_pol.crrnt_snpsht_flag =1 
	where convert(date,CD.clndr_date) <= @Date
),
EXP_Get_Values AS (
	SELECT
	PassThroughChargeTransactionFactID,
	EnterpriseGroupCode,
	EnterpriseGroupAbbreviation,
	StrategicProfitCenterCode,
	StrategicProfitCenterAbbreviation,
	LegalEntityCode,
	LegalEntityAbbreviation,
	PolicyOfferingCode,
	PolicyOfferingAbbreviation,
	PolicyOfferingDescription,
	ProductCode,
	ProductAbbreviation,
	ProductDescription,
	LineOfBusinessCode,
	LineOfBusinessAbbreviation,
	LineOfBusinessDescription,
	AccountingProductCode,
	AccountingProductAbbreviation,
	AccountingProductDescription,
	RatingPlanCode,
	RatingPlanDescription,
	InsuranceSegmentCode,
	InsuranceSegmentDescription,
	ProgramCode,
	ProgramDescription,
	AssociationCode,
	AssociationDescription,
	IndustryRiskGradeCode,
	IndustryRiskGradeDescription,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	PolicyCancellationReasonCode,
	PolicyCancellationReasonCodeDescription,
	PolicyOriginalInceptionDate,
	PolicyRenewalCode,
	PolicyRenewalDescription,
	PolicyStatusCode,
	PolicyStatusCodeDescription,
	AccountingDate,
	RatingStateCode,
	RatingStateAbbreviation,
	LocationNumber,
	RatingLocationCity,
	RatingLocationCounty,
	RatingLocationZIPCode,
	PolicyKey,
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	PolicyIssueCode,
	PolicyIssueCodeDescription,
	PrimaryRatingStateCode,
	PrimaryRatingStateAbbreviation,
	PrimaryRatingStateDescription,
	PrimaryBusinessClassificationCode,
	PrimaryBusinessClassificationDescription,
	BusinessClassDimId,
	AgencyCode,
	AgencyDoingBusinessAsName,
	AgencyStateAbbreviation,
	AgencyPhysicalAddressCity,
	AgencyZIPCode,
	ProducerCode,
	ProducerFullName,
	UnderwritingDivisionDimId,
	SalesDivisionDimId,
	CustomerNumber,
	FirstNamedInsured,
	SICCode,
	SICDescription,
	NAICSCode,
	NAICSDescription,
	CustomerCareIndicator,
	PriorPolicyKey,
	PriorPolicyNumber,
	PriorPolicySymbol,
	PriorPolicyVersion,
	pol_ak_id,
	PassThroughChargeTransactionAmount,
	FederalEmployerIDNumberfein,
	RolloverPriorCarrier,
	PriorPolicyEffectiveDate,
	EDWAgencyAKID
	FROM SQ_ActuarialAnalysis23Monthly_PassThrough
),
SQ_Pol_Dim AS (
	Declare @Date as date
	
	set @Date = case when '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QA%' or '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QC%'
	then
	convert(date,EOMONTH(getdate(),0))
	else
	convert(date,EOMONTH(getdate(),-1))
	end ;
	
	SELECT 
	distinct Pol.edw_pol_ak_id
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PassThroughChargeTransactionFact PCTF 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on PCTF.PolicyDimId= pol.pol_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD on CD.clndr_id = PCTF.PassThroughChargeTransactionBookedDateId	
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= PCTF.InsuranceReferenceDimId and IRD.PolicyOfferingAbbreviation='WC'	
	where convert(date,CD.clndr_date) <= @Date
),
mplt_Determine_RiskGrade_Code_and_Description AS (WITH
	INPUT AS (
		
	),
	LKP_PMF_CDD_RiskGradeCode AS (
		SELECT
		PremiumMasterPremium,
		RiskGradeCode,
		InsuranceLineCode,
		in_PolicyAKID,
		edw_pol_ak_id
		FROM (
			SELECT 
			SUM(pmf.PremiumMasterPremium) as PremiumMasterPremium,
			cdd.RiskGradeCode as RiskGradeCode,
			ircd.InsuranceLineCode as InsuranceLineCode,
			pd.edw_pol_ak_id as edw_pol_ak_id 
			FROM policy_dim pd
			inner join dbo.PremiumMasterFact pmf on pmf.PolicyDimID = pd.pol_dim_id
			inner join dbo.InsuranceReferenceCoverageDim ircd on ircd.InsuranceReferenceCoverageDimId = pmf.InsuranceReferenceCoverageDimId
			inner join dbo.CoverageDetailDim cdd on cdd.CoverageDetailDimId = pmf.CoverageDetailDimId
			group by cdd.RiskGradeCode,ircd.InsuranceLineCode,pd.edw_pol_ak_id
			order by pd.edw_pol_ak_id,ircd.InsuranceLineCode,cdd.RiskGradeCode
			--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id ORDER BY PremiumMasterPremium) = 1
	),
	AGG_Pol_InsuranceLine_RiskGrade AS (
		SELECT
		in_PolicyAKID,
		InsuranceLineCode,
		RiskGradeCode,
		PremiumMasterPremium,
		-- *INF*: ROUND(SUM(PremiumMasterPremium),4)
		ROUND(SUM(PremiumMasterPremium), 4) AS NetPremium
		FROM LKP_PMF_CDD_RiskGradeCode
		GROUP BY in_PolicyAKID, InsuranceLineCode, RiskGradeCode
	),
	FLT_LOB_Premium_not_zero AS (
		SELECT
		in_PolicyAKID, 
		InsuranceLineCode, 
		RiskGradeCode, 
		NetPremium
		FROM AGG_Pol_InsuranceLine_RiskGrade
		WHERE NetPremium<>0.0
	),
	EXP_Translate_RiskGradeCode AS (
		SELECT
		in_PolicyAKID,
		InsuranceLineCode,
		RiskGradeCode,
		-- *INF*: DECODE (TRUE, RiskGradeCode='1' , 1 ,
		-- RiskGradeCode='2' , 2 ,
		-- RiskGradeCode='3' , 3 ,
		-- RiskGradeCode='4' , 4 ,
		-- RiskGradeCode='5' , 5 ,
		-- RiskGradeCode='6' , 6 ,
		-- RiskGradeCode='7' , 7 ,
		-- RiskGradeCode='8' , 8 ,
		-- RiskGradeCode='9' , 9 ,
		-- RiskGradeCode='0' , 0 ,
		-- RiskGradeCode='D' , 10 ,
		-- RiskGradeCode='N/A' , -3 ,
		-- RiskGradeCode='DNW' , 11 ,
		-- RiskGradeCode='NSI' , -1 ,
		-- RiskGradeCode='Argent' , -2 ,-3)
		DECODE(
		    TRUE,
		    RiskGradeCode = '1', 1,
		    RiskGradeCode = '2', 2,
		    RiskGradeCode = '3', 3,
		    RiskGradeCode = '4', 4,
		    RiskGradeCode = '5', 5,
		    RiskGradeCode = '6', 6,
		    RiskGradeCode = '7', 7,
		    RiskGradeCode = '8', 8,
		    RiskGradeCode = '9', 9,
		    RiskGradeCode = '0', 0,
		    RiskGradeCode = 'D', 10,
		    RiskGradeCode = 'N/A', - 3,
		    RiskGradeCode = 'DNW', 11,
		    RiskGradeCode = 'NSI', - 1,
		    RiskGradeCode = 'Argent', - 2,
		    - 3
		) AS RiskGradeValue
		FROM FLT_LOB_Premium_not_zero
	),
	AGG_Max_RiskGrade_Pol AS (
		SELECT
		in_PolicyAKID,
		InsuranceLineCode,
		RiskGradeValue,
		-- *INF*: MAX(RiskGradeValue)
		MAX(RiskGradeValue) AS o_maxRiskGradeValue
		FROM EXP_Translate_RiskGradeCode
		GROUP BY in_PolicyAKID
	),
	EXP_Decode_RiskGradeValue AS (
		SELECT
		in_PolicyAKID,
		o_maxRiskGradeValue,
		-- *INF*: DECODE (TRUE,o_maxRiskGradeValue=1 , '1' ,
		-- o_maxRiskGradeValue=2 , '2' ,
		-- o_maxRiskGradeValue=3 , '3' ,
		-- o_maxRiskGradeValue=4 , '4' ,
		-- o_maxRiskGradeValue=5 , '5' ,
		-- o_maxRiskGradeValue=6 , '6' ,
		-- o_maxRiskGradeValue=7 , '7' ,
		-- o_maxRiskGradeValue=8 , '8' ,
		-- o_maxRiskGradeValue=9 , '9' ,
		-- o_maxRiskGradeValue=0 , '0' ,
		-- o_maxRiskGradeValue=10 , 'D' ,
		-- o_maxRiskGradeValue=-3 , 'N/A' ,
		-- o_maxRiskGradeValue=11 , 'DNW' ,
		-- o_maxRiskGradeValue=-1 , 'NSI' ,
		-- o_maxRiskGradeValue=-2 , 'Argent' ,'N/A')
		DECODE(
		    TRUE,
		    o_maxRiskGradeValue = 1, '1',
		    o_maxRiskGradeValue = 2, '2',
		    o_maxRiskGradeValue = 3, '3',
		    o_maxRiskGradeValue = 4, '4',
		    o_maxRiskGradeValue = 5, '5',
		    o_maxRiskGradeValue = 6, '6',
		    o_maxRiskGradeValue = 7, '7',
		    o_maxRiskGradeValue = 8, '8',
		    o_maxRiskGradeValue = 9, '9',
		    o_maxRiskGradeValue = 0, '0',
		    o_maxRiskGradeValue = 10, 'D',
		    o_maxRiskGradeValue = - 3, 'N/A',
		    o_maxRiskGradeValue = 11, 'DNW',
		    o_maxRiskGradeValue = - 1, 'NSI',
		    o_maxRiskGradeValue = - 2, 'Argent',
		    'N/A'
		) AS RiskGradeCode,
		-- *INF*: DECODE (TRUE,o_maxRiskGradeValue=1 , 'Excellent' ,
		-- o_maxRiskGradeValue=2 , 'Excellent' ,
		-- o_maxRiskGradeValue=3 , 'Good' ,
		-- o_maxRiskGradeValue=4 , 'Good' ,
		-- o_maxRiskGradeValue=5 , 'Average' ,
		-- o_maxRiskGradeValue=6 , 'Marginal' ,
		-- o_maxRiskGradeValue=7 , 'Marginal' ,
		-- o_maxRiskGradeValue=8 , 'Poor' ,
		-- o_maxRiskGradeValue=9 , 'Poor' ,
		-- o_maxRiskGradeValue=0 , 'NSI Bonds' ,
		-- o_maxRiskGradeValue=10 , 'Do Not Write' ,
		-- o_maxRiskGradeValue=-3 , 'Not Available' ,
		-- o_maxRiskGradeValue=11 , 'Do Not Write' ,
		-- o_maxRiskGradeValue=-1 , 'NSI' ,
		-- o_maxRiskGradeValue=-2 , 'Argent' , 'Not Available')
		DECODE(
		    TRUE,
		    o_maxRiskGradeValue = 1, 'Excellent',
		    o_maxRiskGradeValue = 2, 'Excellent',
		    o_maxRiskGradeValue = 3, 'Good',
		    o_maxRiskGradeValue = 4, 'Good',
		    o_maxRiskGradeValue = 5, 'Average',
		    o_maxRiskGradeValue = 6, 'Marginal',
		    o_maxRiskGradeValue = 7, 'Marginal',
		    o_maxRiskGradeValue = 8, 'Poor',
		    o_maxRiskGradeValue = 9, 'Poor',
		    o_maxRiskGradeValue = 0, 'NSI Bonds',
		    o_maxRiskGradeValue = 10, 'Do Not Write',
		    o_maxRiskGradeValue = - 3, 'Not Available',
		    o_maxRiskGradeValue = 11, 'Do Not Write',
		    o_maxRiskGradeValue = - 1, 'NSI',
		    o_maxRiskGradeValue = - 2, 'Argent',
		    'Not Available'
		) AS RiskGradeDescription
		FROM AGG_Max_RiskGrade_Pol
	),
	OUTPUT AS (
		SELECT
		in_PolicyAKID, 
		RiskGradeCode, 
		RiskGradeDescription
		FROM EXP_Decode_RiskGradeValue
	),
),
JNR_DividendFact_policyDim_by_Pol_akid AS (SELECT
	EXP_Get_Values.PassThroughChargeTransactionFactID, 
	EXP_Get_Values.EnterpriseGroupCode, 
	EXP_Get_Values.EnterpriseGroupAbbreviation, 
	EXP_Get_Values.StrategicProfitCenterCode, 
	EXP_Get_Values.StrategicProfitCenterAbbreviation, 
	EXP_Get_Values.LegalEntityCode, 
	EXP_Get_Values.LegalEntityAbbreviation, 
	EXP_Get_Values.PolicyOfferingCode, 
	EXP_Get_Values.PolicyOfferingAbbreviation, 
	EXP_Get_Values.PolicyOfferingDescription, 
	EXP_Get_Values.ProductCode, 
	EXP_Get_Values.ProductAbbreviation, 
	EXP_Get_Values.ProductDescription, 
	EXP_Get_Values.LineOfBusinessCode, 
	EXP_Get_Values.LineOfBusinessAbbreviation, 
	EXP_Get_Values.LineOfBusinessDescription, 
	EXP_Get_Values.AccountingProductCode, 
	EXP_Get_Values.AccountingProductAbbreviation, 
	EXP_Get_Values.AccountingProductDescription, 
	EXP_Get_Values.RatingPlanCode, 
	EXP_Get_Values.RatingPlanDescription, 
	EXP_Get_Values.InsuranceSegmentCode, 
	EXP_Get_Values.InsuranceSegmentDescription, 
	EXP_Get_Values.ProgramCode, 
	EXP_Get_Values.ProgramDescription, 
	EXP_Get_Values.AssociationCode, 
	EXP_Get_Values.AssociationDescription, 
	mplt_Determine_RiskGrade_Code_and_Description.RiskGradeCode AS IndustryRiskGradeCode, 
	mplt_Determine_RiskGrade_Code_and_Description.RiskGradeDescription AS IndustryRiskGradeDescription, 
	EXP_Get_Values.PolicyEffectiveDate, 
	EXP_Get_Values.PolicyExpirationDate, 
	EXP_Get_Values.PolicyCancellationReasonCode, 
	EXP_Get_Values.PolicyCancellationReasonCodeDescription, 
	EXP_Get_Values.PolicyOriginalInceptionDate, 
	EXP_Get_Values.PolicyRenewalCode, 
	EXP_Get_Values.PolicyRenewalDescription, 
	EXP_Get_Values.PolicyStatusCode, 
	EXP_Get_Values.PolicyStatusCodeDescription, 
	EXP_Get_Values.AccountingDate, 
	EXP_Get_Values.RatingStateCode, 
	EXP_Get_Values.RatingStateAbbreviation, 
	EXP_Get_Values.LocationNumber, 
	EXP_Get_Values.RatingLocationCity, 
	EXP_Get_Values.RatingLocationCounty, 
	EXP_Get_Values.RatingLocationZIPCode, 
	EXP_Get_Values.PolicyKey, 
	EXP_Get_Values.PolicySymbol, 
	EXP_Get_Values.PolicyNumber, 
	EXP_Get_Values.PolicyVersion, 
	EXP_Get_Values.PolicyIssueCode, 
	EXP_Get_Values.PolicyIssueCodeDescription, 
	EXP_Get_Values.PrimaryRatingStateCode, 
	EXP_Get_Values.PrimaryRatingStateAbbreviation, 
	EXP_Get_Values.PrimaryRatingStateDescription, 
	EXP_Get_Values.PrimaryBusinessClassificationCode, 
	EXP_Get_Values.PrimaryBusinessClassificationDescription, 
	EXP_Get_Values.BusinessClassDimId, 
	EXP_Get_Values.AgencyCode, 
	EXP_Get_Values.AgencyDoingBusinessAsName, 
	EXP_Get_Values.AgencyStateAbbreviation, 
	EXP_Get_Values.AgencyPhysicalAddressCity, 
	EXP_Get_Values.AgencyZIPCode, 
	EXP_Get_Values.ProducerCode, 
	EXP_Get_Values.ProducerFullName, 
	EXP_Get_Values.UnderwritingDivisionDimId, 
	EXP_Get_Values.SalesDivisionDimId, 
	EXP_Get_Values.CustomerNumber, 
	EXP_Get_Values.FirstNamedInsured, 
	EXP_Get_Values.SICCode, 
	EXP_Get_Values.SICDescription, 
	EXP_Get_Values.NAICSCode, 
	EXP_Get_Values.NAICSDescription, 
	EXP_Get_Values.CustomerCareIndicator, 
	EXP_Get_Values.PriorPolicyKey, 
	EXP_Get_Values.PriorPolicyNumber, 
	EXP_Get_Values.PriorPolicySymbol, 
	EXP_Get_Values.PriorPolicyVersion, 
	EXP_Get_Values.pol_ak_id, 
	EXP_Get_Values.PassThroughChargeTransactionAmount, 
	EXP_Get_Values.FederalEmployerIDNumberfein, 
	EXP_Get_Values.RolloverPriorCarrier, 
	EXP_Get_Values.PriorPolicyEffectiveDate, 
	EXP_Get_Values.EDWAgencyAKID, 
	mplt_Determine_RiskGrade_Code_and_Description.in_PolicyAKID1 AS in_PolicyAKID
	FROM EXP_Get_Values
	INNER JOIN mplt_Determine_RiskGrade_Code_and_Description
	ON mplt_Determine_RiskGrade_Code_and_Description.in_PolicyAKID1 = EXP_Get_Values.pol_ak_id
),
EXP_PassVal AS (
	SELECT
	PassThroughChargeTransactionFactID AS ModifiedPremiumTransactionMonthlyFactId,
	EnterpriseGroupCode,
	EnterpriseGroupAbbreviation,
	StrategicProfitCenterCode,
	StrategicProfitCenterAbbreviation,
	LegalEntityCode,
	LegalEntityAbbreviation,
	PolicyOfferingCode,
	PolicyOfferingAbbreviation,
	PolicyOfferingDescription,
	ProductCode,
	ProductAbbreviation,
	ProductDescription,
	LineOfBusinessCode,
	LineOfBusinessAbbreviation,
	LineOfBusinessDescription,
	AccountingProductCode,
	AccountingProductAbbreviation,
	AccountingProductDescription,
	RatingPlanCode,
	RatingPlanDescription,
	InsuranceSegmentCode,
	InsuranceSegmentDescription,
	ProgramCode,
	ProgramDescription,
	AssociationCode,
	AssociationDescription,
	IndustryRiskGradeCode,
	IndustryRiskGradeDescription,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	PolicyCancellationReasonCode,
	PolicyCancellationReasonCodeDescription,
	PolicyOriginalInceptionDate,
	PolicyRenewalCode,
	PolicyRenewalDescription,
	PolicyStatusCode,
	PolicyStatusCodeDescription,
	AccountingDate,
	RatingStateCode,
	RatingStateAbbreviation,
	LocationNumber,
	RatingLocationCity,
	RatingLocationCounty,
	RatingLocationZIPCode,
	PolicyKey,
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	PolicyIssueCode,
	PolicyIssueCodeDescription,
	PrimaryRatingStateCode,
	PrimaryRatingStateAbbreviation,
	PrimaryRatingStateDescription,
	PrimaryBusinessClassificationCode,
	PrimaryBusinessClassificationDescription,
	BusinessClassDimId,
	AgencyCode,
	AgencyDoingBusinessAsName,
	AgencyStateAbbreviation,
	AgencyPhysicalAddressCity,
	AgencyZIPCode,
	ProducerCode,
	ProducerFullName,
	UnderwritingDivisionDimId,
	SalesDivisionDimId,
	CustomerNumber,
	FirstNamedInsured,
	SICCode,
	SICDescription,
	NAICSCode,
	NAICSDescription,
	CustomerCareIndicator,
	PriorPolicyKey,
	PriorPolicyNumber,
	PriorPolicySymbol,
	PriorPolicyVersion,
	pol_ak_id,
	PassThroughChargeTransactionAmount,
	FederalEmployerIDNumberfein,
	RolloverPriorCarrier,
	PriorPolicyEffectiveDate,
	EDWAgencyAKID
	FROM JNR_DividendFact_policyDim_by_Pol_akid
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
LKP_StateDim AS (
	SELECT
	StateDimID,
	StateDescription,
	in_RatingStateAbbreviation,
	StateAbbreviation
	FROM (
		SELECT 
			StateDimID,
			StateDescription,
			in_RatingStateAbbreviation,
			StateAbbreviation
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StateDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateAbbreviation ORDER BY StateDimID) = 1
),
LKP_DividendFact AS (
	SELECT
	DividendTypeDimId,
	in_pol_ak_id,
	in_StateDimID,
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
	in_DividendTypeDimId,
	DividendTypeDimId
	FROM (
		SELECT 
			DividendType,
			DividendPlan,
			in_DividendTypeDimId,
			DividendTypeDimId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendTypeDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DividendTypeDimId ORDER BY DividendType) = 1
),
LKP_PolicyCurrentStatusDim AS (
	SELECT
	PolicyCancellationDate,
	PolicyStatusCode,
	PolicyStatusDescription,
	in_pol_ak_id,
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
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwritingDivisionDimID ORDER BY UnderwriterDisplayName) = 1
),
EXP_GetValues AS (
	SELECT
	EXP_PassVal.ModifiedPremiumTransactionMonthlyFactId AS i_ModifiedPremiumTransactionMonthlyFactId,
	EXP_PassVal.EnterpriseGroupCode AS i_EnterpriseGroupCode,
	EXP_PassVal.EnterpriseGroupAbbreviation AS i_EnterpriseGroupAbbreviation,
	EXP_PassVal.StrategicProfitCenterCode AS i_StrategicProfitCenterCode,
	EXP_PassVal.StrategicProfitCenterAbbreviation AS i_StrategicProfitCenterAbbreviation,
	EXP_PassVal.LegalEntityCode AS i_LegalEntityCode,
	EXP_PassVal.LegalEntityAbbreviation AS i_LegalEntityAbbreviation,
	EXP_PassVal.PolicyOfferingCode AS i_PolicyOfferingCode,
	EXP_PassVal.PolicyOfferingAbbreviation AS i_PolicyOfferingAbbreviation,
	EXP_PassVal.PolicyOfferingDescription AS i_PolicyOfferingDescription,
	EXP_PassVal.ProductCode AS i_ProductCode,
	EXP_PassVal.ProductAbbreviation AS i_ProductAbbreviation,
	EXP_PassVal.ProductDescription AS i_ProductDescription,
	EXP_PassVal.LineOfBusinessCode AS i_LineOfBusinessCode,
	EXP_PassVal.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	EXP_PassVal.LineOfBusinessDescription AS i_LineOfBusinessDescription,
	EXP_PassVal.AccountingProductCode AS i_AccountingProductCode,
	EXP_PassVal.AccountingProductAbbreviation AS i_AccountingProductAbbreviation,
	EXP_PassVal.AccountingProductDescription AS i_AccountingProductDescription,
	EXP_PassVal.RatingPlanCode AS i_RatingPlanCode,
	EXP_PassVal.RatingPlanDescription AS i_RatingPlanDescription,
	EXP_PassVal.InsuranceSegmentCode AS i_InsuranceSegmentCode,
	EXP_PassVal.InsuranceSegmentDescription AS i_InsuranceSegmentDescription,
	EXP_PassVal.ProgramCode AS i_ProgramCode,
	EXP_PassVal.ProgramDescription AS i_ProgramDescription,
	EXP_PassVal.AssociationCode AS i_AssociationCode,
	EXP_PassVal.AssociationDescription AS i_AssociationDescription,
	EXP_PassVal.IndustryRiskGradeCode AS i_IndustryRiskGradeCode,
	EXP_PassVal.IndustryRiskGradeDescription AS i_IndustryRiskGradeDescription,
	EXP_PassVal.PolicyEffectiveDate AS i_PolicyEffectiveDate,
	EXP_PassVal.PolicyExpirationDate AS i_PolicyExpirationDate,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate AS i_PolicyCancellationDate,
	EXP_PassVal.PolicyCancellationReasonCode AS i_PolicyCancellationReasonCode,
	EXP_PassVal.PolicyCancellationReasonCodeDescription AS i_PolicyCancellationReasonCodeDescription,
	EXP_PassVal.PolicyOriginalInceptionDate AS i_PolicyOriginalInceptionDate,
	EXP_PassVal.PolicyRenewalCode AS i_PolicyRenewalCode,
	EXP_PassVal.PolicyRenewalDescription AS i_PolicyRenewalDescription,
	LKP_PolicyCurrentStatusDim.PolicyStatusCode AS i_PolicyStatusCode,
	LKP_PolicyCurrentStatusDim.PolicyStatusDescription AS i_PolicyStatusCodeDescription,
	EXP_PassVal.AccountingDate AS i_AccountingDate,
	EXP_PassVal.RatingStateCode AS i_RatingStateCode,
	EXP_PassVal.RatingStateAbbreviation AS i_RatingStateAbbreviation,
	LKP_StateDim.StateDescription AS lkp_RatingStateName,
	EXP_PassVal.LocationNumber AS i_LocationNumber,
	EXP_PassVal.RatingLocationCity AS i_RatingLocationCity,
	EXP_PassVal.RatingLocationCounty AS i_RatingLocationCounty,
	EXP_PassVal.RatingLocationZIPCode AS i_RatingLocationZIPCode,
	EXP_PassVal.PolicyKey AS i_PolicyKey,
	EXP_PassVal.PolicySymbol AS i_PolicySymbol,
	EXP_PassVal.PolicyNumber AS i_PolicyNumber,
	EXP_PassVal.PolicyVersion AS i_PolicyVersion,
	EXP_PassVal.PolicyIssueCode AS i_PolicyIssueCode,
	EXP_PassVal.PolicyIssueCodeDescription AS i_PolicyIssueCodeDescription,
	EXP_PassVal.PrimaryRatingStateCode AS i_PrimaryRatingStateCode,
	EXP_PassVal.PrimaryRatingStateAbbreviation AS i_PrimaryRatingStateAbbreviation,
	EXP_PassVal.PrimaryRatingStateDescription AS i_PrimaryRatingStateDescription,
	EXP_PassVal.PrimaryBusinessClassificationCode AS i_PrimaryBusinessClassificationCode,
	EXP_PassVal.PrimaryBusinessClassificationDescription AS i_PrimaryBusinessClassificationDescription,
	LKP_BusinessClassDim.BusinessSegmentCode AS lkp_BusinessSegmentCode,
	LKP_BusinessClassDim.BusinessSegmentDescription AS lkp_BusinessSegmentDescription,
	LKP_BusinessClassDim.StrategicBusinessGroupCode AS lkp_StrategicBusinessGroupCode,
	LKP_BusinessClassDim.StrategicBusinessGroupDescription AS lkp_StrategicBusinessGroupDescription,
	EXP_PassVal.AgencyCode AS i_AgencyCode,
	EXP_PassVal.AgencyDoingBusinessAsName AS i_AgencyDoingBusinessAsName,
	EXP_PassVal.AgencyStateAbbreviation AS i_AgencyStateAbbreviation,
	EXP_PassVal.AgencyPhysicalAddressCity AS i_AgencyPhysicalAddressCity,
	EXP_PassVal.AgencyZIPCode AS i_AgencyZIPCode,
	EXP_PassVal.ProducerCode AS i_ProducerCode,
	EXP_PassVal.ProducerFullName AS i_ProducerFullName,
	LKP_UnderwritingDivisionDim.UnderwriterDisplayName AS lkp_UnderwriterDisplayName,
	LKP_UnderwritingDivisionDim.UnderwriterManagerDisplayName AS lkp_UnderwriterManagerDisplayName,
	LKP_UnderwritingDivisionDim.UnderwritingRegionCodeDescription AS lkp_UnderwritingRegionCodeDescription,
	LKP_AgencyRelationship.Prim_Agency_Code AS i_PrimaryAgencyCode,
	LKP_AgencyRelationship.prim_agency_name AS i_PrimaryAgencyDoingBusinessAsName,
	LKP_AgencyRelationship.prim_agency_state_abbr AS i_PrimaryAgencyStateAbbreviation,
	LKP_AgencyRelationship.prim_agency_city AS i_PrimaryAgencyPhysicalAddressCity,
	LKP_AgencyRelationship.prim_agency_zip_code AS i_PrimaryAgencyZIPCode,
	LKP_SalesDivisionDim.RegionalSalesManagerDisplayName AS lkp_RegionalSalesManagerDisplayName,
	LKP_SalesDivisionDim.SalesTerritoryCode AS lkp_SalesTerritoryCode,
	EXP_PassVal.CustomerNumber AS i_CustomerNumber,
	EXP_PassVal.FirstNamedInsured AS i_FirstNamedInsured,
	EXP_PassVal.SICCode AS i_SICCode,
	EXP_PassVal.SICDescription AS i_SICDescription,
	EXP_PassVal.NAICSCode AS i_NAICSCode,
	EXP_PassVal.NAICSDescription AS i_NAICSDescription,
	EXP_PassVal.CustomerCareIndicator AS i_CustomerCareIndicator,
	EXP_PassVal.PriorPolicyKey AS i_PriorPolicyKey,
	EXP_PassVal.PriorPolicyNumber AS i_PriorPolicyNumber,
	EXP_PassVal.PriorPolicySymbol AS i_PriorPolicySymbol,
	EXP_PassVal.PriorPolicyVersion AS i_PriorPolicyVersion,
	LKP_DividendTypeDim.DividendPlan AS lkp_DividendPlan,
	LKP_DividendTypeDim.DividendType AS lkp_DividendType,
	EXP_PassVal.PassThroughChargeTransactionAmount AS i_PassThroughChargeTransactionAmount,
	EXP_PassVal.FederalEmployerIDNumberfein AS i_FederalEmployerIDNumberfein,
	EXP_PassVal.RolloverPriorCarrier AS i_RolloverPriorCarrier,
	EXP_PassVal.PriorPolicyEffectiveDate AS i_PriorPolicyEffectiveDate,
	-- *INF*: :LKP.LKP_PhysicalState(i_AgencyStateAbbreviation)
	LKP_PHYSICALSTATE_i_AgencyStateAbbreviation.StateCode_Desc AS v_AgencyStateCode_desc,
	-- *INF*: :LKP.LKP_PhysicalState(i_PrimaryAgencyStateAbbreviation)
	LKP_PHYSICALSTATE_i_PrimaryAgencyStateAbbreviation.StateCode_Desc AS v_PrimaryAgencyStateCode_desc,
	-- *INF*: IIF(i_PolicyIssueCode='N','Y','N')
	-- 
	-- 
	IFF(i_PolicyIssueCode = 'N', 'Y', 'N') AS v_NewBusinessIndicator,
	-- *INF*: IIF(ISNULL(i_PolicyEffectiveDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),i_PolicyEffectiveDate)
	IFF(
	    i_PolicyEffectiveDate IS NULL, TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    i_PolicyEffectiveDate
	) AS v_PolicyEffectiveDate,
	-- *INF*: IIF(ISNULL(i_PolicyExpirationDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),i_PolicyExpirationDate)
	IFF(
	    i_PolicyExpirationDate IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    i_PolicyExpirationDate
	) AS v_PolicyExpirationDate,
	-- *INF*: IIF(ISNULL(i_PolicyCancellationDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),i_PolicyCancellationDate)
	IFF(
	    i_PolicyCancellationDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    i_PolicyCancellationDate
	) AS v_PolicyCancellationDate,
	-- *INF*: IIF(ISNULL(i_AccountingDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),i_AccountingDate)
	IFF(
	    i_AccountingDate IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    i_AccountingDate
	) AS v_AccountingDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	-- *INF*: IIF(ISNULL(i_ModifiedPremiumTransactionMonthlyFactId ), -1, i_ModifiedPremiumTransactionMonthlyFactId)
	-- 
	--  
	IFF(
	    i_ModifiedPremiumTransactionMonthlyFactId IS NULL, - 1,
	    i_ModifiedPremiumTransactionMonthlyFactId
	) AS o_EDWActuarialWorkersCompensationFactId,
	'PassThrough' AS o_EDWActuarialWorkersCompensationFactType,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupCode ),'N/A',i_EnterpriseGroupCode ) 
	IFF(i_EnterpriseGroupCode IS NULL, 'N/A', i_EnterpriseGroupCode) AS o_EnterpriseGroupCode,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupAbbreviation ),'N/A', i_EnterpriseGroupAbbreviation)
	IFF(i_EnterpriseGroupAbbreviation IS NULL, 'N/A', i_EnterpriseGroupAbbreviation) AS o_EnterpriseGroupAbbreviation,
	-- *INF*: IIF(ISNULL( i_StrategicProfitCenterCode),'N/A', i_StrategicProfitCenterCode)
	IFF(i_StrategicProfitCenterCode IS NULL, 'N/A', i_StrategicProfitCenterCode) AS o_StrategicProfitCenterCode,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterAbbreviation ),'N/A',i_StrategicProfitCenterAbbreviation )
	IFF(i_StrategicProfitCenterAbbreviation IS NULL, 'N/A', i_StrategicProfitCenterAbbreviation) AS o_StrategicProfitCenterAbbreviation,
	-- *INF*: IIF(ISNULL(i_LegalEntityCode ),'N/A',i_LegalEntityCode )
	IFF(i_LegalEntityCode IS NULL, 'N/A', i_LegalEntityCode) AS o_LegalEntityCode,
	-- *INF*: IIF(ISNULL( i_LegalEntityAbbreviation),'N/A',i_LegalEntityAbbreviation )
	IFF(i_LegalEntityAbbreviation IS NULL, 'N/A', i_LegalEntityAbbreviation) AS o_LegalEntityAbbreviation,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingCode ),'N/A',i_PolicyOfferingCode )
	IFF(i_PolicyOfferingCode IS NULL, 'N/A', i_PolicyOfferingCode) AS o_PolicyOfferingCode,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingAbbreviation ),'N/A',i_PolicyOfferingAbbreviation )
	IFF(i_PolicyOfferingAbbreviation IS NULL, 'N/A', i_PolicyOfferingAbbreviation) AS o_PolicyOfferingAbbreviation,
	-- *INF*: IIF(ISNULL( i_PolicyOfferingDescription),'N/A', i_PolicyOfferingDescription)
	IFF(i_PolicyOfferingDescription IS NULL, 'N/A', i_PolicyOfferingDescription) AS o_PolicyOfferingDescription,
	-- *INF*: IIF(ISNULL( i_ProductCode),'N/A',  i_ProductCode)
	IFF(i_ProductCode IS NULL, 'N/A', i_ProductCode) AS o_ProductCode,
	-- *INF*: IIF(ISNULL(i_ProductAbbreviation ),'N/A', i_ProductAbbreviation)
	IFF(i_ProductAbbreviation IS NULL, 'N/A', i_ProductAbbreviation) AS o_ProductAbbreviation,
	-- *INF*: IIF(ISNULL( i_ProductDescription),'N/A',i_ProductDescription )
	IFF(i_ProductDescription IS NULL, 'N/A', i_ProductDescription) AS o_ProductDescription,
	-- *INF*: IIF(ISNULL(i_LineOfBusinessCode ),'N/A',i_LineOfBusinessCode )
	IFF(i_LineOfBusinessCode IS NULL, 'N/A', i_LineOfBusinessCode) AS o_LineOfBusinessCode,
	-- *INF*: IIF(ISNULL( i_LineOfBusinessAbbreviation),'N/A',i_LineOfBusinessAbbreviation )
	IFF(i_LineOfBusinessAbbreviation IS NULL, 'N/A', i_LineOfBusinessAbbreviation) AS o_LineOfBusinessAbbreviation,
	-- *INF*: IIF(ISNULL(i_LineOfBusinessDescription ),'N/A',i_LineOfBusinessDescription )
	IFF(i_LineOfBusinessDescription IS NULL, 'N/A', i_LineOfBusinessDescription) AS o_LineOfBusinessDescription,
	-- *INF*: IIF(ISNULL( i_AccountingProductCode),'N/A',i_AccountingProductCode )
	IFF(i_AccountingProductCode IS NULL, 'N/A', i_AccountingProductCode) AS o_AccountingProductCode,
	-- *INF*: IIF(ISNULL( i_AccountingProductAbbreviation),'N/A',i_AccountingProductAbbreviation )
	IFF(i_AccountingProductAbbreviation IS NULL, 'N/A', i_AccountingProductAbbreviation) AS o_AccountingProductAbbreviation,
	-- *INF*: IIF(ISNULL( i_AccountingProductDescription),'N/A', i_AccountingProductDescription)
	IFF(i_AccountingProductDescription IS NULL, 'N/A', i_AccountingProductDescription) AS o_AccountingProductDescription,
	-- *INF*: IIF(ISNULL(i_RatingPlanCode ),'N/A', i_RatingPlanCode)
	IFF(i_RatingPlanCode IS NULL, 'N/A', i_RatingPlanCode) AS o_RatingPlanCode,
	-- *INF*: IIF(ISNULL( i_RatingPlanDescription),'N/A', i_RatingPlanDescription)
	IFF(i_RatingPlanDescription IS NULL, 'N/A', i_RatingPlanDescription) AS o_RatingPlanDescription,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentCode ),'N/A',i_InsuranceSegmentCode )
	IFF(i_InsuranceSegmentCode IS NULL, 'N/A', i_InsuranceSegmentCode) AS o_InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL( i_InsuranceSegmentDescription),'N/A',i_InsuranceSegmentDescription )
	IFF(i_InsuranceSegmentDescription IS NULL, 'N/A', i_InsuranceSegmentDescription) AS o_InsuranceSegmentDescription,
	-- *INF*: IIF(ISNULL( i_ProgramCode),'N/A', i_ProgramCode)
	IFF(i_ProgramCode IS NULL, 'N/A', i_ProgramCode) AS o_ProgramCode,
	-- *INF*: IIF(ISNULL( i_ProgramDescription),'N/A', i_ProgramDescription)
	IFF(i_ProgramDescription IS NULL, 'N/A', i_ProgramDescription) AS o_ProgramDescription,
	-- *INF*: IIF(ISNULL(i_AssociationCode ),'N/A',i_AssociationCode )
	IFF(i_AssociationCode IS NULL, 'N/A', i_AssociationCode) AS o_AssociationCode,
	-- *INF*: IIF(ISNULL( i_AssociationDescription),'N/A', i_AssociationDescription)
	IFF(i_AssociationDescription IS NULL, 'N/A', i_AssociationDescription) AS o_AssociationDescription,
	'N/A' AS o_CoverageSummaryCode,
	'N/A' AS o_CoverageSummaryDescription,
	'N/A' AS o_CoverageGroupCode,
	'N/A' AS o_CoverageGroupDescription,
	'N/A' AS o_CoverageCode,
	'N/A' AS o_CoverageDescription,
	'N/A' AS o_Rated_CoverageCode,
	'N/A' AS o_Rated_CoverageDescription,
	'N/A' AS o_ClassCode,
	'N/A' AS o_ClassDescription,
	-- *INF*: IIF(ISNULL(i_IndustryRiskGradeCode ),'N/A', i_IndustryRiskGradeCode)
	IFF(i_IndustryRiskGradeCode IS NULL, 'N/A', i_IndustryRiskGradeCode) AS o_IndustryRiskGradeCode,
	-- *INF*: IIF(ISNULL( i_IndustryRiskGradeDescription),'N/A',i_IndustryRiskGradeDescription )
	IFF(i_IndustryRiskGradeDescription IS NULL, 'N/A', i_IndustryRiskGradeDescription) AS o_IndustryRiskGradeDescription,
	-- *INF*: GET_DATE_PART(v_PolicyEffectiveDate,'YYYY')
	DATE_PART(v_PolicyEffectiveDate, 'YYYY') AS o_PolicyEffectiveYear,
	-- *INF*: TO_INTEGER(TO_CHAR(v_PolicyEffectiveDate,'Q'))
	CAST(TO_CHAR(v_PolicyEffectiveDate, 'Q') AS INTEGER) AS o_PolicyEffectiveQuarter,
	-- *INF*: GET_DATE_PART(v_PolicyEffectiveDate,'MM')
	DATE_PART(v_PolicyEffectiveDate, 'MM') AS o_PolicyEffectiveMonthNumber,
	-- *INF*: TO_CHAR(v_PolicyEffectiveDate,'MONTH')
	TO_CHAR(v_PolicyEffectiveDate, 'MONTH') AS o_PolicyEffectiveMonthDescription,
	v_PolicyEffectiveDate AS o_PolicyEffectiveDate,
	-- *INF*: GET_DATE_PART(v_PolicyExpirationDate,'YYYY')
	DATE_PART(v_PolicyExpirationDate, 'YYYY') AS o_PolicyExpirationYear,
	-- *INF*: TO_INTEGER(TO_CHAR(v_PolicyExpirationDate,'Q'))
	CAST(TO_CHAR(v_PolicyExpirationDate, 'Q') AS INTEGER) AS o_PolicyExpirationQuarter,
	-- *INF*: GET_DATE_PART(v_PolicyExpirationDate,'MM')
	DATE_PART(v_PolicyExpirationDate, 'MM') AS o_PolicyExpirationMonthNumber,
	-- *INF*: TO_CHAR(v_PolicyExpirationDate,'MONTH')
	TO_CHAR(v_PolicyExpirationDate, 'MONTH') AS o_PolicyExpirationMonthDescription,
	v_PolicyExpirationDate AS o_PolicyExpirationDate,
	-- *INF*: GET_DATE_PART(v_PolicyCancellationDate,'YYYY')
	DATE_PART(v_PolicyCancellationDate, 'YYYY') AS o_PolicyCancellationYear,
	-- *INF*: TO_INTEGER(TO_CHAR(v_PolicyCancellationDate,'Q'))
	CAST(TO_CHAR(v_PolicyCancellationDate, 'Q') AS INTEGER) AS o_PolicyCancellationQuarter,
	-- *INF*: GET_DATE_PART(v_PolicyCancellationDate,'MM')
	DATE_PART(v_PolicyCancellationDate, 'MM') AS o_PolicyCancellationMonth,
	-- *INF*: TO_CHAR(v_PolicyCancellationDate,'MONTH')
	TO_CHAR(v_PolicyCancellationDate, 'MONTH') AS o_PolicyCancellationMonthDescription,
	v_PolicyCancellationDate AS o_PolicyCancellationDate,
	-- *INF*: IIF(ISNULL(i_PolicyCancellationReasonCode ),'N/A', i_PolicyCancellationReasonCode)
	IFF(i_PolicyCancellationReasonCode IS NULL, 'N/A', i_PolicyCancellationReasonCode) AS o_PolicyCancellationReasonCode,
	-- *INF*: IIF(ISNULL( i_PolicyCancellationReasonCodeDescription),'N/A', i_PolicyCancellationReasonCodeDescription)
	IFF(
	    i_PolicyCancellationReasonCodeDescription IS NULL, 'N/A',
	    i_PolicyCancellationReasonCodeDescription
	) AS o_PolicyCancellationReasonCodeDescription,
	-- *INF*: IIF(ISNULL(i_PolicyOriginalInceptionDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),i_PolicyOriginalInceptionDate)
	IFF(
	    i_PolicyOriginalInceptionDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    i_PolicyOriginalInceptionDate
	) AS o_PolicyOriginalInceptionDate,
	-- *INF*: IIF(ISNULL( i_PolicyRenewalCode),'N/A',i_PolicyRenewalCode )
	IFF(i_PolicyRenewalCode IS NULL, 'N/A', i_PolicyRenewalCode) AS o_PolicyRenewalCode,
	-- *INF*: IIF(ISNULL(i_PolicyRenewalDescription ),'N/A',i_PolicyRenewalDescription )
	IFF(i_PolicyRenewalDescription IS NULL, 'N/A', i_PolicyRenewalDescription) AS o_PolicyRenewalDescription,
	-- *INF*: IIF(ISNULL( i_PolicyStatusCode),'N/A',i_PolicyStatusCode )
	IFF(i_PolicyStatusCode IS NULL, 'N/A', i_PolicyStatusCode) AS o_PolicyStatusCode,
	-- *INF*: IIF(ISNULL(i_PolicyStatusCodeDescription ),'N/A', i_PolicyStatusCodeDescription)
	IFF(i_PolicyStatusCodeDescription IS NULL, 'N/A', i_PolicyStatusCodeDescription) AS o_PolicyStatusCodeDescription,
	-- *INF*: GET_DATE_PART(v_AccountingDate,'YYYY')
	DATE_PART(v_AccountingDate, 'YYYY') AS o_AccountingYear,
	-- *INF*: TO_INTEGER(TO_CHAR(v_AccountingDate,'Q'))
	CAST(TO_CHAR(v_AccountingDate, 'Q') AS INTEGER) AS o_AccountingMonthQuarter,
	-- *INF*: GET_DATE_PART(v_AccountingDate,'MM')
	DATE_PART(v_AccountingDate, 'MM') AS o_AccountingMonthNumber,
	-- *INF*: TO_CHAR(v_AccountingDate,'MONTH')
	TO_CHAR(v_AccountingDate, 'MONTH') AS o_AccountingMonthName,
	v_AccountingDate AS o_AccountingDate,
	-- *INF*: IIF(ISNULL( i_RatingStateCode),'N/A',i_RatingStateCode )
	IFF(i_RatingStateCode IS NULL, 'N/A', i_RatingStateCode) AS o_RatingStateCode,
	-- *INF*: IIF(ISNULL(i_RatingStateAbbreviation ),'N/A', i_RatingStateAbbreviation)
	IFF(i_RatingStateAbbreviation IS NULL, 'N/A', i_RatingStateAbbreviation) AS o_RatingStateAbbreviation,
	-- *INF*: IIF(ISNULL( lkp_RatingStateName),'N/A', lkp_RatingStateName)
	IFF(lkp_RatingStateName IS NULL, 'N/A', lkp_RatingStateName) AS o_RatingStateName,
	-- *INF*: IIF(ISNULL( i_LocationNumber),'N/A',i_LocationNumber )
	IFF(i_LocationNumber IS NULL, 'N/A', i_LocationNumber) AS o_LocationNumber,
	-- *INF*: IIF(ISNULL(i_RatingLocationCity ),'N/A',i_RatingLocationCity)
	IFF(i_RatingLocationCity IS NULL, 'N/A', i_RatingLocationCity) AS o_RatingLocationCity,
	-- *INF*: IIF(ISNULL(i_RatingLocationCounty ),'N/A',i_RatingLocationCounty )
	IFF(i_RatingLocationCounty IS NULL, 'N/A', i_RatingLocationCounty) AS o_RatingLocationCounty,
	-- *INF*: IIF(ISNULL(i_RatingLocationZIPCode ),'N/A', i_RatingLocationZIPCode)
	IFF(i_RatingLocationZIPCode IS NULL, 'N/A', i_RatingLocationZIPCode) AS o_RatingLocationZIPCode,
	-- *INF*: IIF(ISNULL( i_PolicyKey),'N/A', i_PolicyKey)
	IFF(i_PolicyKey IS NULL, 'N/A', i_PolicyKey) AS o_PolicyKey,
	-- *INF*: IIF(ISNULL( i_PolicySymbol),'N/A',i_PolicySymbol )
	IFF(i_PolicySymbol IS NULL, 'N/A', i_PolicySymbol) AS o_PolicySymbol,
	-- *INF*: IIF(ISNULL( i_PolicyNumber),'N/A', i_PolicyNumber)
	IFF(i_PolicyNumber IS NULL, 'N/A', i_PolicyNumber) AS o_PolicyNumber,
	-- *INF*: IIF(ISNULL(i_PolicyVersion ),'NA',i_PolicyVersion)
	IFF(i_PolicyVersion IS NULL, 'NA', i_PolicyVersion) AS o_PolicyVersion,
	-- *INF*: IIF(ISNULL(i_PolicyIssueCode ),'N/A',i_PolicyIssueCode )
	IFF(i_PolicyIssueCode IS NULL, 'N/A', i_PolicyIssueCode) AS o_PolicyIssueCode,
	-- *INF*: IIF(ISNULL(i_PolicyIssueCodeDescription ),'N/A',i_PolicyIssueCodeDescription )
	IFF(i_PolicyIssueCodeDescription IS NULL, 'N/A', i_PolicyIssueCodeDescription) AS o_PolicyIssueCodeDescription,
	-- *INF*: IIF(ISNULL( i_PrimaryRatingStateCode),'N/A', i_PrimaryRatingStateCode)
	IFF(i_PrimaryRatingStateCode IS NULL, 'N/A', i_PrimaryRatingStateCode) AS o_PrimaryRatingStateCode,
	-- *INF*: IIF(ISNULL(i_PrimaryRatingStateAbbreviation ),'N/A',i_PrimaryRatingStateAbbreviation )
	IFF(i_PrimaryRatingStateAbbreviation IS NULL, 'N/A', i_PrimaryRatingStateAbbreviation) AS o_PrimaryRatingStateAbbreviation,
	-- *INF*: IIF(ISNULL(i_PrimaryRatingStateDescription ),'N/A', i_PrimaryRatingStateDescription)
	IFF(i_PrimaryRatingStateDescription IS NULL, 'N/A', i_PrimaryRatingStateDescription) AS o_PrimaryRatingStateDescription,
	-- *INF*: IIF(ISNULL(i_PrimaryBusinessClassificationCode ),'N/A', i_PrimaryBusinessClassificationCode)
	IFF(i_PrimaryBusinessClassificationCode IS NULL, 'N/A', i_PrimaryBusinessClassificationCode) AS o_PrimaryBusinessClassificationCode,
	-- *INF*: IIF(ISNULL( i_PrimaryBusinessClassificationDescription),'N/A',i_PrimaryBusinessClassificationDescription )
	IFF(
	    i_PrimaryBusinessClassificationDescription IS NULL, 'N/A',
	    i_PrimaryBusinessClassificationDescription
	) AS o_PrimaryBusinessClassificationDescription,
	-- *INF*: IIF(ISNULL(lkp_BusinessSegmentCode ),'N/A', lkp_BusinessSegmentCode)
	IFF(lkp_BusinessSegmentCode IS NULL, 'N/A', lkp_BusinessSegmentCode) AS o_BusinessSegmentCode,
	-- *INF*: IIF(ISNULL(lkp_BusinessSegmentDescription ),'N/A',lkp_BusinessSegmentDescription)
	IFF(lkp_BusinessSegmentDescription IS NULL, 'N/A', lkp_BusinessSegmentDescription) AS o_BusinessSegmentDescription,
	-- *INF*: IIF(ISNULL(lkp_StrategicBusinessGroupCode ),'N/A', lkp_StrategicBusinessGroupCode)
	IFF(lkp_StrategicBusinessGroupCode IS NULL, 'N/A', lkp_StrategicBusinessGroupCode) AS o_StrategicBusinessGroupCode,
	-- *INF*: IIF(ISNULL( lkp_StrategicBusinessGroupDescription),'N/A', lkp_StrategicBusinessGroupDescription)
	IFF(
	    lkp_StrategicBusinessGroupDescription IS NULL, 'N/A', lkp_StrategicBusinessGroupDescription
	) AS o_StrategicBusinessGroupDescription,
	-- *INF*: IIF(ISNULL( i_AgencyCode),'N/A',i_AgencyCode )
	IFF(i_AgencyCode IS NULL, 'N/A', i_AgencyCode) AS o_AgencyCode,
	-- *INF*: IIF(ISNULL(i_AgencyDoingBusinessAsName ),'N/A',i_AgencyDoingBusinessAsName )
	IFF(i_AgencyDoingBusinessAsName IS NULL, 'N/A', i_AgencyDoingBusinessAsName) AS o_AgencyDoingBusinessAsName,
	-- *INF*: IIF( ISNULL(v_AgencyStateCode_desc) ,'N/A' ,
	-- SUBSTR(v_AgencyStateCode_desc,1,(INSTR(v_AgencyStateCode_desc,'|')-1)) )
	IFF(
	    v_AgencyStateCode_desc IS NULL, 'N/A',
	    SUBSTR(v_AgencyStateCode_desc, 1, (REGEXP_INSTR(v_AgencyStateCode_desc, '|') - 1))
	) AS o_AgencyStateCode,
	-- *INF*: IIF(ISNULL(i_AgencyStateAbbreviation ),'NA', i_AgencyStateAbbreviation)
	IFF(i_AgencyStateAbbreviation IS NULL, 'NA', i_AgencyStateAbbreviation) AS o_AgencyStateAbbreviation,
	-- *INF*: IIF( ISNULL(v_AgencyStateCode_desc) ,'N/A' ,SUBSTR(v_AgencyStateCode_desc,(INSTR(v_AgencyStateCode_desc,'|')+1),LENGTH(v_AgencyStateCode_desc)))
	IFF(
	    v_AgencyStateCode_desc IS NULL, 'N/A',
	    SUBSTR(v_AgencyStateCode_desc, (REGEXP_INSTR(v_AgencyStateCode_desc, '|') + 1), LENGTH(v_AgencyStateCode_desc))
	) AS o_AgencyStateDescription,
	-- *INF*: IIF(ISNULL( i_AgencyPhysicalAddressCity),'N/A', i_AgencyPhysicalAddressCity)
	IFF(i_AgencyPhysicalAddressCity IS NULL, 'N/A', i_AgencyPhysicalAddressCity) AS o_AgencyPhysicalAddressCity,
	-- *INF*: IIF(ISNULL(i_AgencyZIPCode ),'N/A', i_AgencyZIPCode)
	IFF(i_AgencyZIPCode IS NULL, 'N/A', i_AgencyZIPCode) AS o_AgencyZIPCode,
	-- *INF*: IIF(ISNULL( i_ProducerCode),'N/A',i_ProducerCode )
	IFF(i_ProducerCode IS NULL, 'N/A', i_ProducerCode) AS o_ProducerCode,
	-- *INF*: IIF(ISNULL( i_ProducerFullName),'N/A', i_ProducerFullName)
	IFF(i_ProducerFullName IS NULL, 'N/A', i_ProducerFullName) AS o_ProducerFullName,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterDisplayName),'N/A', lkp_UnderwriterDisplayName)
	IFF(lkp_UnderwriterDisplayName IS NULL, 'N/A', lkp_UnderwriterDisplayName) AS o_UnderwriterDisplayName,
	-- *INF*: IIF(ISNULL( lkp_UnderwriterManagerDisplayName),'N/A', lkp_UnderwriterManagerDisplayName)
	IFF(lkp_UnderwriterManagerDisplayName IS NULL, 'N/A', lkp_UnderwriterManagerDisplayName) AS o_UnderwriterManagerDisplayName,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingRegionCodeDescription ),'N/A',lkp_UnderwritingRegionCodeDescription )
	IFF(
	    lkp_UnderwritingRegionCodeDescription IS NULL, 'N/A', lkp_UnderwritingRegionCodeDescription
	) AS o_UnderwritingRegionCodeDescription,
	-- *INF*: IIF(ISNULL(i_PrimaryAgencyCode ),'N/A',i_PrimaryAgencyCode)
	IFF(i_PrimaryAgencyCode IS NULL, 'N/A', i_PrimaryAgencyCode) AS o_PrimaryAgencyCode,
	-- *INF*: IIF(ISNULL( i_PrimaryAgencyDoingBusinessAsName),'N/A',i_PrimaryAgencyDoingBusinessAsName )
	IFF(i_PrimaryAgencyDoingBusinessAsName IS NULL, 'N/A', i_PrimaryAgencyDoingBusinessAsName) AS o_PrimaryAgencyDoingBusinessAsName,
	-- *INF*: IIF( ISNULL(v_PrimaryAgencyStateCode_desc) ,'N/A' ,
	-- SUBSTR(v_PrimaryAgencyStateCode_desc,1,(INSTR(v_PrimaryAgencyStateCode_desc,'|')-1)) )
	IFF(
	    v_PrimaryAgencyStateCode_desc IS NULL, 'N/A',
	    SUBSTR(v_PrimaryAgencyStateCode_desc, 1, (REGEXP_INSTR(v_PrimaryAgencyStateCode_desc, '|') - 1))
	) AS o_PrimaryAgencyStateCode,
	-- *INF*: IIF(ISNULL(i_PrimaryAgencyStateAbbreviation ),'NA', i_PrimaryAgencyStateAbbreviation)
	IFF(i_PrimaryAgencyStateAbbreviation IS NULL, 'NA', i_PrimaryAgencyStateAbbreviation) AS o_PrimaryAgencyStateAbbreviation,
	-- *INF*: IIF( ISNULL(v_PrimaryAgencyStateCode_desc) ,'N/A' ,SUBSTR(v_PrimaryAgencyStateCode_desc,(INSTR(v_PrimaryAgencyStateCode_desc,'|')+1),LENGTH(v_PrimaryAgencyStateCode_desc)))
	IFF(
	    v_PrimaryAgencyStateCode_desc IS NULL, 'N/A',
	    SUBSTR(v_PrimaryAgencyStateCode_desc, (REGEXP_INSTR(v_PrimaryAgencyStateCode_desc, '|') + 1), LENGTH(v_PrimaryAgencyStateCode_desc))
	) AS o_PrimaryAgencyStateDescription,
	-- *INF*: IIF(ISNULL( i_PrimaryAgencyPhysicalAddressCity),'N/A', i_PrimaryAgencyPhysicalAddressCity)
	IFF(i_PrimaryAgencyPhysicalAddressCity IS NULL, 'N/A', i_PrimaryAgencyPhysicalAddressCity) AS o_PrimaryAgencyPhysicalAddressCity,
	-- *INF*: IIF(ISNULL( i_PrimaryAgencyZIPCode),'N/A',i_PrimaryAgencyZIPCode )
	IFF(i_PrimaryAgencyZIPCode IS NULL, 'N/A', i_PrimaryAgencyZIPCode) AS o_PrimaryAgencyZIPCode,
	-- *INF*: IIF(ISNULL(lkp_RegionalSalesManagerDisplayName ),'N/A', lkp_RegionalSalesManagerDisplayName)
	IFF(lkp_RegionalSalesManagerDisplayName IS NULL, 'N/A', lkp_RegionalSalesManagerDisplayName) AS o_RegionalSalesManagerDisplayName,
	-- *INF*: IIF(ISNULL( lkp_SalesTerritoryCode),'N/A',lkp_SalesTerritoryCode )
	IFF(lkp_SalesTerritoryCode IS NULL, 'N/A', lkp_SalesTerritoryCode) AS o_SalesTerritoryCode,
	-- *INF*: IIF(ISNULL( i_CustomerNumber),'N/A',i_CustomerNumber )
	IFF(i_CustomerNumber IS NULL, 'N/A', i_CustomerNumber) AS o_CustomerNumber,
	-- *INF*: IIF(ISNULL( i_FirstNamedInsured),'N/A', i_FirstNamedInsured)
	IFF(i_FirstNamedInsured IS NULL, 'N/A', i_FirstNamedInsured) AS o_FirstNamedInsured,
	-- *INF*: IIF(ISNULL( i_SICCode),'N/A', i_SICCode)
	IFF(i_SICCode IS NULL, 'N/A', i_SICCode) AS o_SICCode,
	-- *INF*: IIF(ISNULL(i_SICDescription ),'N/A', i_SICDescription)
	IFF(i_SICDescription IS NULL, 'N/A', i_SICDescription) AS o_SICDescription,
	-- *INF*: IIF(ISNULL( i_NAICSCode),'N/A',i_NAICSCode)
	IFF(i_NAICSCode IS NULL, 'N/A', i_NAICSCode) AS o_NAICSCode,
	-- *INF*: IIF(ISNULL( i_NAICSDescription),'N/A',i_NAICSDescription )
	IFF(i_NAICSDescription IS NULL, 'N/A', i_NAICSDescription) AS o_NAICSDescription,
	-- *INF*: IIF(ISNULL(i_CustomerCareIndicator ),'N/A', i_CustomerCareIndicator)
	IFF(i_CustomerCareIndicator IS NULL, 'N/A', i_CustomerCareIndicator) AS o_CustomerCareIndicator,
	v_NewBusinessIndicator AS o_NewBusinessIndicator,
	-- *INF*: IIF(ISNULL(i_PriorPolicyKey ),'N/A', i_PriorPolicyKey)
	IFF(i_PriorPolicyKey IS NULL, 'N/A', i_PriorPolicyKey) AS o_PriorPolicyKey,
	-- *INF*: IIF(ISNULL( i_PriorPolicyNumber),'N/A',i_PriorPolicyNumber )
	IFF(i_PriorPolicyNumber IS NULL, 'N/A', i_PriorPolicyNumber) AS o_PriorPolicyNumber,
	-- *INF*: IIF(ISNULL(i_PriorPolicySymbol ),'N/A', i_PriorPolicySymbol)
	IFF(i_PriorPolicySymbol IS NULL, 'N/A', i_PriorPolicySymbol) AS o_PriorPolicySymbol,
	-- *INF*: IIF(ISNULL( i_PriorPolicyVersion),'N/A',i_PriorPolicyVersion )
	IFF(i_PriorPolicyVersion IS NULL, 'N/A', i_PriorPolicyVersion) AS o_PriorPolicyVersion,
	-- *INF*: ADD_TO_DATE(ADD_TO_DATE(TRUNC(LAST_DAY(ADD_TO_DATE(SYSDATE,'MM',-1))),'DD',1),'SS',-1)
	-- 
	-- -- to get the previous month last date
	DATEADD(SECOND,- 1,DATEADD(DAY,1,TRUNC(LAST_DAY(DATEADD(MONTH,- 1,CURRENT_TIMESTAMP))))) AS o_LastBookedDate,
	'N/A' AS o_HazardGroupCode,
	-- *INF*: IIF(ISNULL(lkp_DividendPlan ),'N/A',lkp_DividendPlan )
	IFF(lkp_DividendPlan IS NULL, 'N/A', lkp_DividendPlan) AS o_DividendPlan,
	-- *INF*: IIF(ISNULL( lkp_DividendType),'N/A', lkp_DividendType)
	IFF(lkp_DividendType IS NULL, 'N/A', lkp_DividendType) AS o_DividendType,
	0.00 AS o_DeductibleAmount,
	'N/A' AS o_PolicyPerAccidentLimit,
	'N/A' AS o_PolicyPerDiseaseLimit,
	'N/A' AS o_PolicyAggregateLimit,
	'N/A' AS o_ExposureBasis,
	0.00 AS o_DirectWrittenExposure,
	0.00 AS o_DirectWrittenPremium,
	0.00 AS o_SubjectWrittenPremium,
	0.00 AS o_ExperienceModifiedPremium,
	0.00 AS o_ScheduleModifiedPremium,
	0.00 AS o_OtherModifiedPremium,
	0.00 AS o_ClassifiedPremium,
	0.00 AS o_DirectEarnedExposure,
	0.00 AS o_DirectEarnedPremium,
	0.00 AS o_SubjectDirectEarnedPremium,
	0.00 AS o_ExperienceModifiedDirectEarnedPremium,
	0.00 AS o_ScheduleModifiedDirectEarnedPremium,
	0.00 AS o_OtherModifiedDirectEarnedPremium,
	0.00 AS o_ClassifiedDirectEarnedPremium,
	-- *INF*: IIF(ISNULL( i_PassThroughChargeTransactionAmount) , 0.00 ,i_PassThroughChargeTransactionAmount)
	IFF(
	    i_PassThroughChargeTransactionAmount IS NULL, 0.00, i_PassThroughChargeTransactionAmount
	) AS o_PassThroughChargeAmount,
	'N' AS o_AdmiraltyActFlag,
	'N' AS o_FederalEmployersLiabilityActFlag,
	'N' AS o_USLongShoreAndHarborWorkersCompensationActFlag,
	-- *INF*: IIF(ISNULL(i_FederalEmployerIDNumberfein ),'N/A', i_FederalEmployerIDNumberfein)
	IFF(i_FederalEmployerIDNumberfein IS NULL, 'N/A', i_FederalEmployerIDNumberfein) AS o_FederalEmployerIDNumberfein,
	-- *INF*: IIF(ISNULL( i_RolloverPriorCarrier),'N/A',i_RolloverPriorCarrier )
	IFF(i_RolloverPriorCarrier IS NULL, 'N/A', i_RolloverPriorCarrier) AS o_RolloverPriorCarrier,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_CoverageEffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_CoverageExpirationDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_CoverageCancellationDate,
	-- *INF*: IIF(ISNULL(i_PriorPolicyEffectiveDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),i_PriorPolicyEffectiveDate)
	IFF(
	    i_PriorPolicyEffectiveDate IS NULL,
	    TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    i_PriorPolicyEffectiveDate
	) AS o_PriorPolicyEffectiveDate,
	-- *INF*: :LKP.LKP_ACTUARIALWORKERSCOMPENSATIONPREMIUM_TAXES_TGT(i_ModifiedPremiumTransactionMonthlyFactId)
	LKP_ACTUARIALWORKERSCOMPENSATIONPREMIUM_TAXES_TGT_i_ModifiedPremiumTransactionMonthlyFactId.ActuarialWorkersCompensationPremiumId AS v_FilterFlag,
	-- *INF*: IIF(ISNULL(v_FilterFlag), 'INSERT','FILTER')
	IFF(v_FilterFlag IS NULL, 'INSERT', 'FILTER') AS o_FilterFlag
	FROM EXP_PassVal
	LEFT JOIN LKP_AgencyRelationship
	ON LKP_AgencyRelationship.EDWAgencyAKID = EXP_PassVal.EDWAgencyAKID
	LEFT JOIN LKP_BusinessClassDim
	ON LKP_BusinessClassDim.BusinessClassDimId = EXP_PassVal.BusinessClassDimId
	LEFT JOIN LKP_DividendTypeDim
	ON LKP_DividendTypeDim.DividendTypeDimId = LKP_DividendFact.DividendTypeDimId
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = EXP_PassVal.pol_ak_id
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.SalesDivisionDimID = EXP_PassVal.SalesDivisionDimId
	LEFT JOIN LKP_StateDim
	ON LKP_StateDim.StateAbbreviation = EXP_PassVal.RatingStateAbbreviation
	LEFT JOIN LKP_UnderwritingDivisionDim
	ON LKP_UnderwritingDivisionDim.UnderwritingDivisionDimID = EXP_PassVal.UnderwritingDivisionDimId
	LEFT JOIN LKP_PHYSICALSTATE LKP_PHYSICALSTATE_i_AgencyStateAbbreviation
	ON LKP_PHYSICALSTATE_i_AgencyStateAbbreviation.StateAbbreviation = i_AgencyStateAbbreviation

	LEFT JOIN LKP_PHYSICALSTATE LKP_PHYSICALSTATE_i_PrimaryAgencyStateAbbreviation
	ON LKP_PHYSICALSTATE_i_PrimaryAgencyStateAbbreviation.StateAbbreviation = i_PrimaryAgencyStateAbbreviation

	LEFT JOIN LKP_ACTUARIALWORKERSCOMPENSATIONPREMIUM_TAXES_TGT LKP_ACTUARIALWORKERSCOMPENSATIONPREMIUM_TAXES_TGT_i_ModifiedPremiumTransactionMonthlyFactId
	ON LKP_ACTUARIALWORKERSCOMPENSATIONPREMIUM_TAXES_TGT_i_ModifiedPremiumTransactionMonthlyFactId.EDWActuarialWorkersCompensationFactId = i_ModifiedPremiumTransactionMonthlyFactId

),
FIL_ExistingTargetRows AS (
	SELECT
	AuditId, 
	CreatedDate, 
	ModifiedDate, 
	o_EDWActuarialWorkersCompensationFactId, 
	o_EDWActuarialWorkersCompensationFactType, 
	o_EnterpriseGroupCode, 
	o_EnterpriseGroupAbbreviation, 
	o_StrategicProfitCenterCode, 
	o_StrategicProfitCenterAbbreviation, 
	o_LegalEntityCode, 
	o_LegalEntityAbbreviation, 
	o_PolicyOfferingCode, 
	o_PolicyOfferingAbbreviation, 
	o_PolicyOfferingDescription, 
	o_ProductCode, 
	o_ProductAbbreviation, 
	o_ProductDescription, 
	o_LineOfBusinessCode, 
	o_LineOfBusinessAbbreviation, 
	o_LineOfBusinessDescription, 
	o_AccountingProductCode, 
	o_AccountingProductAbbreviation, 
	o_AccountingProductDescription, 
	o_RatingPlanCode, 
	o_RatingPlanDescription, 
	o_InsuranceSegmentCode, 
	o_InsuranceSegmentDescription, 
	o_ProgramCode, 
	o_ProgramDescription, 
	o_AssociationCode, 
	o_AssociationDescription, 
	o_CoverageSummaryCode, 
	o_CoverageSummaryDescription, 
	o_CoverageGroupCode, 
	o_CoverageGroupDescription, 
	o_CoverageCode, 
	o_CoverageDescription, 
	o_ClassCode, 
	o_ClassDescription, 
	o_IndustryRiskGradeCode, 
	o_IndustryRiskGradeDescription, 
	o_PolicyEffectiveYear, 
	o_PolicyEffectiveQuarter, 
	o_PolicyEffectiveMonthNumber, 
	o_PolicyEffectiveMonthDescription, 
	o_PolicyEffectiveDate, 
	o_PolicyExpirationYear, 
	o_PolicyExpirationQuarter, 
	o_PolicyExpirationMonthNumber, 
	o_PolicyExpirationMonthDescription, 
	o_PolicyExpirationDate, 
	o_PolicyCancellationYear, 
	o_PolicyCancellationQuarter, 
	o_PolicyCancellationMonth, 
	o_PolicyCancellationMonthDescription, 
	o_PolicyCancellationDate, 
	o_PolicyCancellationReasonCode, 
	o_PolicyCancellationReasonCodeDescription, 
	o_PolicyOriginalInceptionDate, 
	o_PolicyRenewalCode, 
	o_PolicyRenewalDescription, 
	o_PolicyStatusCode, 
	o_PolicyStatusCodeDescription, 
	o_AccountingYear, 
	o_AccountingMonthQuarter, 
	o_AccountingMonthNumber, 
	o_AccountingMonthName, 
	o_AccountingDate, 
	o_RatingStateCode, 
	o_RatingStateAbbreviation, 
	o_RatingStateName, 
	o_LocationNumber, 
	o_RatingLocationCity, 
	o_RatingLocationCounty, 
	o_RatingLocationZIPCode, 
	o_PolicyKey, 
	o_PolicySymbol, 
	o_PolicyNumber, 
	o_PolicyVersion, 
	o_PolicyIssueCode, 
	o_PolicyIssueCodeDescription, 
	o_PrimaryRatingStateCode, 
	o_PrimaryRatingStateAbbreviation, 
	o_PrimaryRatingStateDescription, 
	o_PrimaryBusinessClassificationCode, 
	o_PrimaryBusinessClassificationDescription, 
	o_BusinessSegmentCode, 
	o_BusinessSegmentDescription, 
	o_StrategicBusinessGroupCode, 
	o_StrategicBusinessGroupDescription, 
	o_AgencyCode, 
	o_AgencyDoingBusinessAsName, 
	o_AgencyStateCode, 
	o_AgencyStateAbbreviation, 
	o_AgencyStateDescription, 
	o_AgencyPhysicalAddressCity, 
	o_AgencyZIPCode, 
	o_ProducerCode, 
	o_ProducerFullName, 
	o_UnderwriterDisplayName, 
	o_UnderwriterManagerDisplayName, 
	o_UnderwritingRegionCodeDescription, 
	o_PrimaryAgencyCode, 
	o_PrimaryAgencyDoingBusinessAsName, 
	o_PrimaryAgencyStateCode, 
	o_PrimaryAgencyStateAbbreviation, 
	o_PrimaryAgencyStateDescription, 
	o_PrimaryAgencyPhysicalAddressCity, 
	o_PrimaryAgencyZIPCode, 
	o_RegionalSalesManagerDisplayName, 
	o_SalesTerritoryCode, 
	o_CustomerNumber, 
	o_FirstNamedInsured, 
	o_SICCode, 
	o_SICDescription, 
	o_NAICSCode, 
	o_NAICSDescription, 
	o_CustomerCareIndicator, 
	o_NewBusinessIndicator, 
	o_PriorPolicyKey, 
	o_PriorPolicyNumber, 
	o_PriorPolicySymbol, 
	o_PriorPolicyVersion, 
	o_LastBookedDate, 
	o_HazardGroupCode, 
	o_DividendPlan, 
	o_DividendType, 
	o_DeductibleAmount, 
	o_PolicyPerAccidentLimit, 
	o_PolicyPerDiseaseLimit, 
	o_PolicyAggregateLimit, 
	o_ExposureBasis, 
	o_DirectWrittenExposure, 
	o_DirectWrittenPremium, 
	o_SubjectWrittenPremium, 
	o_ExperienceModifiedPremium, 
	o_ScheduleModifiedPremium, 
	o_OtherModifiedPremium, 
	o_ClassifiedPremium, 
	o_DirectEarnedExposure, 
	o_DirectEarnedPremium, 
	o_SubjectDirectEarnedPremium, 
	o_ExperienceModifiedDirectEarnedPremium, 
	o_ScheduleModifiedDirectEarnedPremium, 
	o_OtherModifiedDirectEarnedPremium, 
	o_ClassifiedDirectEarnedPremium, 
	o_PassThroughChargeAmount, 
	o_AdmiraltyActFlag, 
	o_FederalEmployersLiabilityActFlag, 
	o_USLongShoreAndHarborWorkersCompensationActFlag, 
	o_FederalEmployerIDNumberfein, 
	o_RolloverPriorCarrier, 
	o_CoverageEffectiveDate, 
	o_CoverageExpirationDate, 
	o_CoverageCancellationDate, 
	o_PriorPolicyEffectiveDate, 
	o_FilterFlag, 
	o_Rated_CoverageCode, 
	o_Rated_CoverageDescription
	FROM EXP_GetValues
	WHERE o_FilterFlag='INSERT'
),
ActuarialWorkersCompensationPremium AS (
	INSERT INTO ActuarialWorkersCompensationPremium
	(AuditId, CreatedDate, ModifiedDate, EDWActuarialWorkersCompensationFactId, EDWActuarialWorkersCompensationFactType, EnterpriseGroupCode, EnterpriseGroupAbbreviation, StrategicProfitCenterCode, StrategicProfitCenterAbbreviation, LegalEntityCode, LegalEntityAbbreviation, PolicyOfferingCode, PolicyOfferingAbbreviation, PolicyOfferingDescription, ProductCode, ProductAbbreviation, ProductDescription, LineofBusinessCode, LineofBusinessAbbreviation, LineofBusinessDescription, AccountingProductCode, AccountingProductAbbreviation, AccountingProductDescription, RatingPlanCode, RatingPlanDescription, InsuranceSegmentCode, InsuranceSegmentDescription, ProgramCode, ProgramDescription, AssociationCode, AssociationDescription, CoverageSummaryCode, CoverageSummaryDescription, CoverageGroupCode, CoverageGroupDescription, CoverageCode, CoverageDescription, ClassCode, ClassCodeDescription, IndustryRiskGradeCode, IndustryRiskGradeDescription, PolicyEffectiveYear, PolicyEffectiveQuarter, PolicyEffectiveMonthNumber, PolicyEffectiveMonthDescription, PolicyEffectiveDate, PolicyExpirationYear, PolicyExpirationQuarter, PolicyExpirationMonthNumber, PolicyExpirationMonthDescription, PolicyExpirationDate, PolicyCancellationYear, PolicyCancellationQuarter, PolicyCancellationMonth, PolicyCancellationMonthDescription, PolicyCancellationDate, PolicyCancellationReasonCode, PolicyCancellationReasonCodeDescription, PolicyOriginalInceptionDate, PolicyRenewalCode, PolicyRenewalDescription, PolicyStatusCode, PolicyStatusCodeDescription, AccountingYear, AccountingMonthQuarter, AccountingMonthNumber, AccountingMonthName, AccountingDate, RatingStateCode, RatingStateAbbreviation, RatingStateName, LocationNumber, RatingLocationCity, RatingLocationCounty, RatingLocationZIPCode, PolicyKey, PolicySymbol, PolicyNumber, PolicyVersion, PolicyIssueCode, PolicyIssueCodeDescription, PrimaryRatingStateCode, PrimaryRatingStateAbbreviation, PrimaryRatingStateDescription, PrimaryBusinessClassificationCode, PrimaryBusinessClassificationDescription, BusinessSegmentCode, BusinessSegmentDescription, StrategicBusinessGroupCode, StrategicBusinessGroupDescription, AgencyCode, AgencyDoingBusinessAsName, AgencyStateCode, AgencyStateAbbreviation, AgencyStateDescription, AgencyPhysicalAddressCity, AgencyZIPCode, ProducerCode, ProducerFullName, UnderwriterFullName, UnderwritingManagerName, UnderwritingRegionName, PrimaryAgencyCode, PrimaryAgencyDoingBusinessAsName, PrimaryAgencyStateCode, PrimaryAgencyStateAbbreviation, PrimaryAgencyStateDescription, PrimaryAgencyPhysicalAddressCity, PrimaryAgencyZIPCode, RegionalSalesManagerFullName, SalesTerritoryCode, CustomerNumber, FirstNamedInsured, SICCode, SICDescription, NAICSCode, NAICSDescription, CustomerCareIndicator, NewBusinessIndicator, PriorPolicyKey, PriorPolicyNumber, PriorPolicySymbol, PriorPolicyVersion, LastBookedDate, HazardGroupCode, DividendPlan, DividendType, DeductibleAmount, LimitPerAccident, LimitPerDisease, AggregateLimit, ExposureBasis, DirectWrittenExposure, DirectWrittenPremium, SubjectDirectWrittenPremium, ExperienceModifiedDirectWrittenPremium, ScheduleModifiedDirectWrittenPremium, OtherModifiedDirectWrittenPremium, ClassifiedDirectWrittenPremium, DirectEarnedExposure, DirectEarnedPremium, SubjectDirectEarnedPremium, ExperienceModifiedDirectEarnedPremium, ScheduleModifiedDirectEarnedPremium, OtherModifiedDirectEarnedPremium, ClassifiedDirectEarnedPremium, PassThroughChargeAmount, AdmiraltyActFlag, FederalEmployersLiabilityActFlag, USLongshoreAndHarborWorkersCompensationActFlag, FederalEmployerIDNumber, PriorCarrier, CoverageEffectiveDate, CoverageExpirationDate, CoverageCancellationDate, PriorPolicyEffectiveDate, RatedCoverageCode, RatedCoverageDescription)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	o_EDWActuarialWorkersCompensationFactId AS EDWACTUARIALWORKERSCOMPENSATIONFACTID, 
	o_EDWActuarialWorkersCompensationFactType AS EDWACTUARIALWORKERSCOMPENSATIONFACTTYPE, 
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
	o_LineOfBusinessCode AS LINEOFBUSINESSCODE, 
	o_LineOfBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	o_LineOfBusinessDescription AS LINEOFBUSINESSDESCRIPTION, 
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
	o_ClassDescription AS CLASSCODEDESCRIPTION, 
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
	o_UnderwriterDisplayName AS UNDERWRITERFULLNAME, 
	o_UnderwriterManagerDisplayName AS UNDERWRITINGMANAGERNAME, 
	o_UnderwritingRegionCodeDescription AS UNDERWRITINGREGIONNAME, 
	o_PrimaryAgencyCode AS PRIMARYAGENCYCODE, 
	o_PrimaryAgencyDoingBusinessAsName AS PRIMARYAGENCYDOINGBUSINESSASNAME, 
	o_PrimaryAgencyStateCode AS PRIMARYAGENCYSTATECODE, 
	o_PrimaryAgencyStateAbbreviation AS PRIMARYAGENCYSTATEABBREVIATION, 
	o_PrimaryAgencyStateDescription AS PRIMARYAGENCYSTATEDESCRIPTION, 
	o_PrimaryAgencyPhysicalAddressCity AS PRIMARYAGENCYPHYSICALADDRESSCITY, 
	o_PrimaryAgencyZIPCode AS PRIMARYAGENCYZIPCODE, 
	o_RegionalSalesManagerDisplayName AS REGIONALSALESMANAGERFULLNAME, 
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
	o_PolicyPerAccidentLimit AS LIMITPERACCIDENT, 
	o_PolicyPerDiseaseLimit AS LIMITPERDISEASE, 
	o_PolicyAggregateLimit AS AGGREGATELIMIT, 
	o_ExposureBasis AS EXPOSUREBASIS, 
	o_DirectWrittenExposure AS DIRECTWRITTENEXPOSURE, 
	o_DirectWrittenPremium AS DIRECTWRITTENPREMIUM, 
	o_SubjectWrittenPremium AS SUBJECTDIRECTWRITTENPREMIUM, 
	o_ExperienceModifiedPremium AS EXPERIENCEMODIFIEDDIRECTWRITTENPREMIUM, 
	o_ScheduleModifiedPremium AS SCHEDULEMODIFIEDDIRECTWRITTENPREMIUM, 
	o_OtherModifiedPremium AS OTHERMODIFIEDDIRECTWRITTENPREMIUM, 
	o_ClassifiedPremium AS CLASSIFIEDDIRECTWRITTENPREMIUM, 
	o_DirectEarnedExposure AS DIRECTEARNEDEXPOSURE, 
	o_DirectEarnedPremium AS DIRECTEARNEDPREMIUM, 
	o_SubjectDirectEarnedPremium AS SUBJECTDIRECTEARNEDPREMIUM, 
	o_ExperienceModifiedDirectEarnedPremium AS EXPERIENCEMODIFIEDDIRECTEARNEDPREMIUM, 
	o_ScheduleModifiedDirectEarnedPremium AS SCHEDULEMODIFIEDDIRECTEARNEDPREMIUM, 
	o_OtherModifiedDirectEarnedPremium AS OTHERMODIFIEDDIRECTEARNEDPREMIUM, 
	o_ClassifiedDirectEarnedPremium AS CLASSIFIEDDIRECTEARNEDPREMIUM, 
	o_PassThroughChargeAmount AS PASSTHROUGHCHARGEAMOUNT, 
	o_AdmiraltyActFlag AS ADMIRALTYACTFLAG, 
	o_FederalEmployersLiabilityActFlag AS FEDERALEMPLOYERSLIABILITYACTFLAG, 
	o_USLongShoreAndHarborWorkersCompensationActFlag AS USLONGSHOREANDHARBORWORKERSCOMPENSATIONACTFLAG, 
	o_FederalEmployerIDNumberfein AS FEDERALEMPLOYERIDNUMBER, 
	o_RolloverPriorCarrier AS PRIORCARRIER, 
	o_CoverageEffectiveDate AS COVERAGEEFFECTIVEDATE, 
	o_CoverageExpirationDate AS COVERAGEEXPIRATIONDATE, 
	o_CoverageCancellationDate AS COVERAGECANCELLATIONDATE, 
	o_PriorPolicyEffectiveDate AS PRIORPOLICYEFFECTIVEDATE, 
	o_Rated_CoverageCode AS RATEDCOVERAGECODE, 
	o_Rated_CoverageDescription AS RATEDCOVERAGEDESCRIPTION
	FROM FIL_ExistingTargetRows
),