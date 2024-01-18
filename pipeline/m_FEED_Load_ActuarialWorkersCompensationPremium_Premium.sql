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
SQ_ActuarialAnalysis23Monthly_Premium AS (
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
	
	PTF.modifiedPremiumtransactionmonthlyfactID as ActuarialworkerscompensationFactID,
	 IRD.EnterpriseGroupCode as EnterpriseGroupCode,
	 IRD.EnterpriseGroupAbbreviation as EnterpriseGroupAbbreviation,
	 IRD.StrategicProfitCenterCode as StrategicProfitCenterCode,
	 IRD.StrategicProfitCenterAbbreviation as StrategicProfitCenterAbbreviation,
	 IRD.InsuranceReferenceLegalEntityCode as LegalEntityCode,
	 IRD.InsuranceReferenceLegalEntityAbbreviation as LegalEntityAbbreviation,
	 IRD.PolicyOfferingCode as PolicyOfferingCode,
	 IRD.PolicyOfferingAbbreviation as PolicyOfferingAbbreviation,
	 IRD.PolicyOfferingDescription as PolicyOfferingDescription,
	 IRD.ProductCode as ProductCode,
	 IRD.ProductAbbreviation as ProductAbbreviation,
	 IRD.ProductDescription as ProductDescription,
	 IRD.InsuranceReferenceLineOfBusinessCode as LineofBusinessCode,
	 IRD.InsuranceReferenceLineOfBusinessAbbreviation as LineofBusinessAbbreviation,
	 IRD.InsuranceReferenceLineOfBusinessDescription as LineofBusinessDescription,
	 IRD.AccountingProductCode as AccountingProductCode,
	 IRD.AccountingProductAbbreviation as AccountingProductAbbreviation,
	 IRD.AccountingProductDescription as AccountingProductDescription,
	 IRD.RatingPlanCode as RatingPlanCode,
	 IRD.RatingPlanDescription as RatingPlanDescription, 
	 IRD.InsuranceSegmentCode as InsuranceSegmentCode,
	 IRD.InsuranceSegmentDescription as InsuranceSegmentDescription,
	 IRCD.CoverageSummaryCode as CoverageSummaryCode,
	 IRCD.CoverageSummaryDescription as CoverageSummaryDescription,
	 IRCD.CoverageGroupCode as CoverageGroupCode,
	 IRCD.CoverageGroupDescription as CoverageGroupDescription,
	 IRCD.CoverageCode as CoverageCode,
	 IRCD.CoverageDescription as CoverageDescription,
	 IRCD.RatedCoverageCode as RatedCoverageCode,
	 IRCD.RatedCoverageDescription as RatedCoverageDescription,
	 SUBSTRING(CDD.ClassCode,1,4) as ClassCode, 
	 CDD.ClassDescription as ClassDescription,
	 CDD.RiskGradeCode as RiskGradeCode,
	 CD.clndr_date as AccountingDate,
	 CDD.RatingStateProvinceCode as RatingStateCode,
	 CDD.RatingStateProvinceAbbreviation as RatingStateAbbreviation,
	 CDD.LocationNumber as LocationNumber,
	 CDD.RatingCity as RatingLocationCity,
	 CDD.RatingCounty as RatingLocationCounty,
	 CDD.RatingPostalCode as RatingLocationZIPCode,
	 AD.EDWAgencyAKID as EDWAgencyAKID,
	 ccd.edw_contract_cust_ak_id as edw_contract_cust_ak_id,
	 pol.edw_pol_ak_id as pol_ak_id,
	 CDWCD.HazardGroupCode as HazardGroupCode,
	 CDD.DeductibleAmount as DeductibleAmount,
	 CDWCD.PolicyPerAccidentLimit as LimitPerAccident,
	 CDWCD.PolicyPerDiseaseLimit as LimitPerDisease,
	 CDWCD.PolicyAggregateLimit as AggregateLimit,
	 CDD.ExposureBasis as ExposureBasis,
	 PTF.EDWPremiumMasterCalculationPKId as EDWPremiumMasterCalculationPKId,
	 PTF.DirectWrittenPremium as DirectWrittenPremium,
	 PTF.SubjectWrittenPremium as SubjectDirectWrittenPremium,
	 PTF.ExperienceModifiedPremium as ExperienceModifiedDirectWrittenPremium,
	 PTF.ScheduleModifiedPremium as ScheduleModifiedDirectWrittenPremium,
	 PTF.OtherModifiedPremium as OtherModifiedDirectWrittenPremium,
	 PTF.ClassifiedPremium as ClassifiedDirectWrittenPremium,
	 CDWCD.AdmiraltyActFlag as Admir_act_ind,
	 CDWCD.FederalEmployersLiabilityActFlag as Fela_ind,
	 CDWCD.USLongShoreAndHarborWorkersCompensationActFlag as Uslh_ind,
	 CDD.CoverageEffectiveDate as CoverageEffectiveDate,
	 CDD.CoverageExpirationDate as CoverageExpirationDate,
	 CDD.CoverageCancellationDate as CoverageCancellationDate,
	 PTF.GeneratedRecordFlag as GeneratedRecordFlag
	FROM 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.ModifiedPremiumTransactionMonthlyFact PTF 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on PTF.PolicyDimId= pol.pol_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim AD on PTF.AgencyDimId =AD.AgencyDimID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim CCD on PTF.ContractCustomerDimId=CCD.contract_cust_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD on CD.clndr_id = PTF.RunDateId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= PTF.InsuranceReferenceDimId and PolicyOfferingAbbreviation='WC'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRCD on IRCD.InsuranceReferenceCoverageDimId = PTF.InsuranceReferenceCoverageDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD on PTF.CoverageDetailDimId = CDD.CoverageDetailDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensationDim CDWCD on CDD.CoverageDetailDimId=CDWCD.CoverageDetailDimId
	where CD.CalendarYearMonth  <= @Date
	AND PTF.modifiedPremiumtransactionmonthlyfactID % @{pipeline().parameters.NUM_OF_PARTITIONS}=1
	
	UNION ALL
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
	
	PTF.modifiedPremiumtransactionmonthlyfactID as ActuarialworkerscompensationFactID,
	 IRD.EnterpriseGroupCode as EnterpriseGroupCode,
	 IRD.EnterpriseGroupAbbreviation as EnterpriseGroupAbbreviation,
	 IRD.StrategicProfitCenterCode as StrategicProfitCenterCode,
	 IRD.StrategicProfitCenterAbbreviation as StrategicProfitCenterAbbreviation,
	 IRD.InsuranceReferenceLegalEntityCode as LegalEntityCode,
	 IRD.InsuranceReferenceLegalEntityAbbreviation as LegalEntityAbbreviation,
	 IRD.PolicyOfferingCode as PolicyOfferingCode,
	 IRD.PolicyOfferingAbbreviation as PolicyOfferingAbbreviation,
	 IRD.PolicyOfferingDescription as PolicyOfferingDescription,
	 IRD.ProductCode as ProductCode,
	 IRD.ProductAbbreviation as ProductAbbreviation,
	 IRD.ProductDescription as ProductDescription,
	 IRD.InsuranceReferenceLineOfBusinessCode as LineofBusinessCode,
	 IRD.InsuranceReferenceLineOfBusinessAbbreviation as LineofBusinessAbbreviation,
	 IRD.InsuranceReferenceLineOfBusinessDescription as LineofBusinessDescription,
	 IRD.AccountingProductCode as AccountingProductCode,
	 IRD.AccountingProductAbbreviation as AccountingProductAbbreviation,
	 IRD.AccountingProductDescription as AccountingProductDescription,
	 IRD.RatingPlanCode as RatingPlanCode,
	 IRD.RatingPlanDescription as RatingPlanDescription, 
	 IRD.InsuranceSegmentCode as InsuranceSegmentCode,
	 IRD.InsuranceSegmentDescription as InsuranceSegmentDescription,
	 IRCD.CoverageSummaryCode as CoverageSummaryCode,
	 IRCD.CoverageSummaryDescription as CoverageSummaryDescription,
	 IRCD.CoverageGroupCode as CoverageGroupCode,
	 IRCD.CoverageGroupDescription as CoverageGroupDescription,
	 IRCD.CoverageCode as CoverageCode,
	 IRCD.CoverageDescription as CoverageDescription,
	 IRCD.RatedCoverageCode as RatedCoverageCode,
	 IRCD.RatedCoverageDescription as RatedCoverageDescription,
	 SUBSTRING(CDD.ClassCode,1,4) as ClassCode, 
	 CDD.ClassDescription as ClassDescription,
	 CDD.RiskGradeCode as RiskGradeCode,
	 CD.clndr_date as AccountingDate,
	 CDD.RatingStateProvinceCode as RatingStateCode,
	 CDD.RatingStateProvinceAbbreviation as RatingStateAbbreviation,
	 CDD.LocationNumber as LocationNumber,
	 CDD.RatingCity as RatingLocationCity,
	 CDD.RatingCounty as RatingLocationCounty,
	 CDD.RatingPostalCode as RatingLocationZIPCode,
	 AD.EDWAgencyAKID as EDWAgencyAKID,
	 ccd.edw_contract_cust_ak_id as edw_contract_cust_ak_id,
	 pol.edw_pol_ak_id as pol_ak_id,
	 CDWCD.HazardGroupCode as HazardGroupCode,
	 CDD.DeductibleAmount as DeductibleAmount,
	 CDWCD.PolicyPerAccidentLimit as LimitPerAccident,
	 CDWCD.PolicyPerDiseaseLimit as LimitPerDisease,
	 CDWCD.PolicyAggregateLimit as AggregateLimit,
	 CDD.ExposureBasis as ExposureBasis,
	 PTF.EDWPremiumMasterCalculationPKId as EDWPremiumMasterCalculationPKId,
	 PTF.DirectWrittenPremium as DirectWrittenPremium,
	 PTF.SubjectWrittenPremium as SubjectDirectWrittenPremium,
	 PTF.ExperienceModifiedPremium as ExperienceModifiedDirectWrittenPremium,
	 PTF.ScheduleModifiedPremium as ScheduleModifiedDirectWrittenPremium,
	 PTF.OtherModifiedPremium as OtherModifiedDirectWrittenPremium,
	 PTF.ClassifiedPremium as ClassifiedDirectWrittenPremium,
	 CDWCD.AdmiraltyActFlag as Admir_act_ind,
	 CDWCD.FederalEmployersLiabilityActFlag as Fela_ind,
	 CDWCD.USLongShoreAndHarborWorkersCompensationActFlag as Uslh_ind,
	 CDD.CoverageEffectiveDate as CoverageEffectiveDate,
	 CDD.CoverageExpirationDate as CoverageExpirationDate,
	 CDD.CoverageCancellationDate as CoverageCancellationDate,
	 PTF.GeneratedRecordFlag as GeneratedRecordFlag
	FROM 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.ModifiedPremiumTransactionMonthlyFact PTF 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on PTF.PolicyDimId= pol.pol_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim AD on PTF.AgencyDimId =AD.AgencyDimID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim CCD on PTF.ContractCustomerDimId=CCD.contract_cust_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD on CD.clndr_id = PTF.RunDateId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= PTF.InsuranceReferenceDimId and PolicyOfferingAbbreviation='WC'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRCD on IRCD.InsuranceReferenceCoverageDimId = PTF.InsuranceReferenceCoverageDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD on PTF.CoverageDetailDimId = CDD.CoverageDetailDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensationDim CDWCD on CDD.CoverageDetailDimId=CDWCD.CoverageDetailDimId
	where CD.CalendarYearMonth  <= @Date
	AND PTF.modifiedPremiumtransactionmonthlyfactID % @{pipeline().parameters.NUM_OF_PARTITIONS}=2
	
	UNION ALL
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
	
	PTF.modifiedPremiumtransactionmonthlyfactID as ActuarialworkerscompensationFactID,
	 IRD.EnterpriseGroupCode as EnterpriseGroupCode,
	 IRD.EnterpriseGroupAbbreviation as EnterpriseGroupAbbreviation,
	 IRD.StrategicProfitCenterCode as StrategicProfitCenterCode,
	 IRD.StrategicProfitCenterAbbreviation as StrategicProfitCenterAbbreviation,
	 IRD.InsuranceReferenceLegalEntityCode as LegalEntityCode,
	 IRD.InsuranceReferenceLegalEntityAbbreviation as LegalEntityAbbreviation,
	 IRD.PolicyOfferingCode as PolicyOfferingCode,
	 IRD.PolicyOfferingAbbreviation as PolicyOfferingAbbreviation,
	 IRD.PolicyOfferingDescription as PolicyOfferingDescription,
	 IRD.ProductCode as ProductCode,
	 IRD.ProductAbbreviation as ProductAbbreviation,
	 IRD.ProductDescription as ProductDescription,
	 IRD.InsuranceReferenceLineOfBusinessCode as LineofBusinessCode,
	 IRD.InsuranceReferenceLineOfBusinessAbbreviation as LineofBusinessAbbreviation,
	 IRD.InsuranceReferenceLineOfBusinessDescription as LineofBusinessDescription,
	 IRD.AccountingProductCode as AccountingProductCode,
	 IRD.AccountingProductAbbreviation as AccountingProductAbbreviation,
	 IRD.AccountingProductDescription as AccountingProductDescription,
	 IRD.RatingPlanCode as RatingPlanCode,
	 IRD.RatingPlanDescription as RatingPlanDescription, 
	 IRD.InsuranceSegmentCode as InsuranceSegmentCode,
	 IRD.InsuranceSegmentDescription as InsuranceSegmentDescription,
	 IRCD.CoverageSummaryCode as CoverageSummaryCode,
	 IRCD.CoverageSummaryDescription as CoverageSummaryDescription,
	 IRCD.CoverageGroupCode as CoverageGroupCode,
	 IRCD.CoverageGroupDescription as CoverageGroupDescription,
	 IRCD.CoverageCode as CoverageCode,
	 IRCD.CoverageDescription as CoverageDescription,
	 IRCD.RatedCoverageCode as RatedCoverageCode,
	 IRCD.RatedCoverageDescription as RatedCoverageDescription,
	 SUBSTRING(CDD.ClassCode,1,4) as ClassCode, 
	 CDD.ClassDescription as ClassDescription,
	 CDD.RiskGradeCode as RiskGradeCode,
	 CD.clndr_date as AccountingDate,
	 CDD.RatingStateProvinceCode as RatingStateCode,
	 CDD.RatingStateProvinceAbbreviation as RatingStateAbbreviation,
	 CDD.LocationNumber as LocationNumber,
	 CDD.RatingCity as RatingLocationCity,
	 CDD.RatingCounty as RatingLocationCounty,
	 CDD.RatingPostalCode as RatingLocationZIPCode,
	 AD.EDWAgencyAKID as EDWAgencyAKID,
	 ccd.edw_contract_cust_ak_id as edw_contract_cust_ak_id,
	 pol.edw_pol_ak_id as pol_ak_id,
	 CDWCD.HazardGroupCode as HazardGroupCode,
	 CDD.DeductibleAmount as DeductibleAmount,
	 CDWCD.PolicyPerAccidentLimit as LimitPerAccident,
	 CDWCD.PolicyPerDiseaseLimit as LimitPerDisease,
	 CDWCD.PolicyAggregateLimit as AggregateLimit,
	 CDD.ExposureBasis as ExposureBasis,
	 PTF.EDWPremiumMasterCalculationPKId as EDWPremiumMasterCalculationPKId,
	 PTF.DirectWrittenPremium as DirectWrittenPremium,
	 PTF.SubjectWrittenPremium as SubjectDirectWrittenPremium,
	 PTF.ExperienceModifiedPremium as ExperienceModifiedDirectWrittenPremium,
	 PTF.ScheduleModifiedPremium as ScheduleModifiedDirectWrittenPremium,
	 PTF.OtherModifiedPremium as OtherModifiedDirectWrittenPremium,
	 PTF.ClassifiedPremium as ClassifiedDirectWrittenPremium,
	 CDWCD.AdmiraltyActFlag as Admir_act_ind,
	 CDWCD.FederalEmployersLiabilityActFlag as Fela_ind,
	 CDWCD.USLongShoreAndHarborWorkersCompensationActFlag as Uslh_ind,
	 CDD.CoverageEffectiveDate as CoverageEffectiveDate,
	 CDD.CoverageExpirationDate as CoverageExpirationDate,
	 CDD.CoverageCancellationDate as CoverageCancellationDate,
	 PTF.GeneratedRecordFlag as GeneratedRecordFlag
	FROM 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.ModifiedPremiumTransactionMonthlyFact PTF 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on PTF.PolicyDimId= pol.pol_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim AD on PTF.AgencyDimId =AD.AgencyDimID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim CCD on PTF.ContractCustomerDimId=CCD.contract_cust_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD on CD.clndr_id = PTF.RunDateId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= PTF.InsuranceReferenceDimId and PolicyOfferingAbbreviation='WC'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRCD on IRCD.InsuranceReferenceCoverageDimId = PTF.InsuranceReferenceCoverageDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD on PTF.CoverageDetailDimId = CDD.CoverageDetailDimId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailWorkersCompensationDim CDWCD on CDD.CoverageDetailDimId=CDWCD.CoverageDetailDimId
	where CD.CalendarYearMonth  <= @Date
	AND PTF.modifiedPremiumtransactionmonthlyfactID % @{pipeline().parameters.NUM_OF_PARTITIONS}=3
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
		select 
		 ad_Current.EDWAgencyAKID as EDWAgencyAKID,
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
		SELECT cust_num as cust_num, [name] as name, sic_code as sic_code, sic_code_descript as sic_code_descript, naics_code as naics_code ,
		naics_code_descript as naics_code_descript, fed_tax_id as fed_tax_id , edw_contract_cust_ak_id  as edw_contract_cust_ak_id
		FROM DBO.contract_customer_dim WHERE crrnt_snpsht_flag =1 
		and edw_contract_cust_ak_id in (
		SELECT DISTINCT CC.edw_contract_cust_ak_id
		FROM DBO.premiummasterfact PTF 
		INNER JOIN DBO.contract_customer_dim CC on PTF.ContractCustomerDimID= CC.contract_cust_dim_id 
		INNER JOIN DBO.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= PTF.InsuranceReferenceDimId and PolicyOfferingAbbreviation='WC'
		)
		ORDER BY edw_contract_cust_ak_id --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id ORDER BY cust_num) = 1
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
		FROM DBO.premiummasterfact PTF 
		INNER JOIN DBO.policy_dim pol on PTF.PolicyDimId= pol.pol_dim_id 
		INNER JOIN DBO.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= PTF.InsuranceReferenceDimId and PolicyOfferingAbbreviation='WC')
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
EXP_PassVal AS (
	SELECT
	SQ_ActuarialAnalysis23Monthly_Premium.ModifiedPremiumTransactionMonthlyFactId,
	SQ_ActuarialAnalysis23Monthly_Premium.EnterpriseGroupCode,
	SQ_ActuarialAnalysis23Monthly_Premium.EnterpriseGroupAbbreviation,
	SQ_ActuarialAnalysis23Monthly_Premium.StrategicProfitCenterCode,
	SQ_ActuarialAnalysis23Monthly_Premium.StrategicProfitCenterAbbreviation,
	SQ_ActuarialAnalysis23Monthly_Premium.LegalEntityCode,
	SQ_ActuarialAnalysis23Monthly_Premium.LegalEntityAbbreviation,
	SQ_ActuarialAnalysis23Monthly_Premium.PolicyOfferingCode,
	SQ_ActuarialAnalysis23Monthly_Premium.PolicyOfferingAbbreviation,
	SQ_ActuarialAnalysis23Monthly_Premium.PolicyOfferingDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.ProductCode,
	SQ_ActuarialAnalysis23Monthly_Premium.ProductAbbreviation,
	SQ_ActuarialAnalysis23Monthly_Premium.ProductDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.LineOfBusinessCode,
	SQ_ActuarialAnalysis23Monthly_Premium.LineOfBusinessAbbreviation,
	SQ_ActuarialAnalysis23Monthly_Premium.LineOfBusinessDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.AccountingProductCode,
	SQ_ActuarialAnalysis23Monthly_Premium.AccountingProductAbbreviation,
	SQ_ActuarialAnalysis23Monthly_Premium.AccountingProductDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.RatingPlanCode,
	SQ_ActuarialAnalysis23Monthly_Premium.RatingPlanDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.InsuranceSegmentCode,
	SQ_ActuarialAnalysis23Monthly_Premium.InsuranceSegmentDescription,
	LKP_Pol_Current.ProgramCode,
	LKP_Pol_Current.ProgramDescription,
	LKP_Pol_Current.AssociationCode,
	LKP_Pol_Current.AssociationDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.CoverageSummaryCode,
	SQ_ActuarialAnalysis23Monthly_Premium.CoverageSummaryDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.CoverageGroupCode,
	SQ_ActuarialAnalysis23Monthly_Premium.CoverageGroupDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.CoverageCode,
	SQ_ActuarialAnalysis23Monthly_Premium.CoverageDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.ClassCode,
	SQ_ActuarialAnalysis23Monthly_Premium.ClassDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.RiskGradeCode AS IndustryRiskGradeCode,
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
	v_IndustryRiskGradeDescription AS IndustryRiskGradeDescription,
	LKP_Pol_Current.pol_eff_date AS PolicyEffectiveDate,
	LKP_Pol_Current.pol_exp_date AS PolicyExpirationDate,
	LKP_Pol_Current.pol_cancellation_rsn_code AS PolicyCancellationReasonCode,
	LKP_Pol_Current.pol_cancellation_rsn_code_descript AS PolicyCancellationReasonCodeDescription,
	LKP_Pol_Current.orig_incptn_date AS PolicyOriginalInceptionDate,
	LKP_Pol_Current.renl_code AS PolicyRenewalCode,
	LKP_Pol_Current.renl_code_descript AS PolicyRenewalDescription,
	LKP_Pol_Current.pol_status_code AS PolicyStatusCode,
	LKP_Pol_Current.pol_status_code_descript AS PolicyStatusCodeDescription,
	SQ_ActuarialAnalysis23Monthly_Premium.AccountingDate,
	SQ_ActuarialAnalysis23Monthly_Premium.RatingStateCode,
	SQ_ActuarialAnalysis23Monthly_Premium.RatingStateAbbreviation,
	SQ_ActuarialAnalysis23Monthly_Premium.LocationNumber,
	SQ_ActuarialAnalysis23Monthly_Premium.RatingLocationCity,
	SQ_ActuarialAnalysis23Monthly_Premium.RatingLocationCounty,
	SQ_ActuarialAnalysis23Monthly_Premium.RatingLocationZIPCode,
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
	LKP_Pol_Current.BusinessClassDimId,
	LKP_AD_Current_SD.AgencyCode,
	LKP_AD_Current_SD.AgencyDoingBusinessAsName,
	LKP_AD_Current_SD.AgencyStateAbbreviation,
	LKP_AD_Current_SD.AgencyPhysicalAddressCity,
	LKP_AD_Current_SD.AgencyZIPCode,
	LKP_aed_current.ProducerCode,
	LKP_aed_current.ProducerFullName,
	LKP_Pol_Current.UnderwritingDivisionDimId,
	LKP_AgencyRelationship.Prim_Agency_Code AS PrimaryAgencyCode,
	LKP_AgencyRelationship.prim_agency_name AS PrimaryAgencyDoingBusinessAsName,
	LKP_AgencyRelationship.prim_agency_state_abbr AS PrimaryAgencyStateAbbreviation,
	LKP_AgencyRelationship.prim_agency_city AS PrimaryAgencyPhysicalAddressCity,
	LKP_AgencyRelationship.prim_agency_zip_code AS PrimaryAgencyZIPCode,
	LKP_AD_Current_SD.SalesDivisionDimId,
	LKP_CCD_Current.cust_num AS CustomerNumber,
	LKP_CCD_Current.name AS FirstNamedInsured,
	LKP_CCD_Current.sic_code AS SICCode,
	LKP_CCD_Current.sic_code_descript AS SICDescription,
	LKP_CCD_Current.naics_code AS NAICSCode,
	LKP_CCD_Current.naics_code_descript AS NAICSDescription,
	LKP_Pol_Current.serv_center_support_code AS CustomerCareIndicator,
	LKP_Pol_Current.prior_pol_key AS PriorPolicyKey,
	LKP_Pol_Current.PriorPolicyNumber,
	LKP_Pol_Current.PriorPolicySymbol,
	LKP_Pol_Current.PriorPolicyVersion,
	SQ_ActuarialAnalysis23Monthly_Premium.pol_ak_id,
	SQ_ActuarialAnalysis23Monthly_Premium.HazardGroupCode,
	SQ_ActuarialAnalysis23Monthly_Premium.DeductibleAmount,
	SQ_ActuarialAnalysis23Monthly_Premium.PolicyPerAccidentLimit,
	SQ_ActuarialAnalysis23Monthly_Premium.PolicyPerDiseaseLimit,
	SQ_ActuarialAnalysis23Monthly_Premium.PolicyAggregateLimit,
	SQ_ActuarialAnalysis23Monthly_Premium.ExposureBasis,
	SQ_ActuarialAnalysis23Monthly_Premium.EDWPremiumMasterCalculationPKId,
	SQ_ActuarialAnalysis23Monthly_Premium.DirectWrittenPremium,
	SQ_ActuarialAnalysis23Monthly_Premium.SubjectWrittenPremium,
	SQ_ActuarialAnalysis23Monthly_Premium.ExperienceModifiedPremium,
	SQ_ActuarialAnalysis23Monthly_Premium.ScheduleModifiedPremium,
	SQ_ActuarialAnalysis23Monthly_Premium.OtherModifiedPremium,
	SQ_ActuarialAnalysis23Monthly_Premium.ClassifiedPremium,
	SQ_ActuarialAnalysis23Monthly_Premium.AdmiraltyActFlag,
	SQ_ActuarialAnalysis23Monthly_Premium.FederalEmployersLiabilityActFlag,
	SQ_ActuarialAnalysis23Monthly_Premium.USLongShoreAndHarborWorkersCompensationActFlag,
	LKP_CCD_Current.fed_tax_id AS FederalEmployerIDNumberfein,
	LKP_Pol_Current.RolloverPriorCarrier,
	SQ_ActuarialAnalysis23Monthly_Premium.CoverageEffectiveDate,
	SQ_ActuarialAnalysis23Monthly_Premium.CoverageExpirationDate,
	SQ_ActuarialAnalysis23Monthly_Premium.CoverageCancellationDate,
	LKP_Pol_Current.PriorPolicyEffectiveDate,
	SQ_ActuarialAnalysis23Monthly_Premium.GeneratedRecordFlag,
	SQ_ActuarialAnalysis23Monthly_Premium.RatedCoverageCode,
	SQ_ActuarialAnalysis23Monthly_Premium.RatedCoverageDescription
	FROM SQ_ActuarialAnalysis23Monthly_Premium
	LEFT JOIN LKP_AD_Current_SD
	ON LKP_AD_Current_SD.EDWAgencyAKID = SQ_ActuarialAnalysis23Monthly_Premium.EDWAgencyAKID
	LEFT JOIN LKP_AgencyRelationship
	ON LKP_AgencyRelationship.EDWAgencyAKID = LKP_AD_Current_SD.EDWAgencyAKID
	LEFT JOIN LKP_CCD_Current
	ON LKP_CCD_Current.edw_contract_cust_ak_id = SQ_ActuarialAnalysis23Monthly_Premium.edw_contract_cust_ak_id
	LEFT JOIN LKP_Pol_Current
	ON LKP_Pol_Current.edw_pol_ak_id = SQ_ActuarialAnalysis23Monthly_Premium.pol_ak_id
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
		SELECT EDWPolicyAKId as EDWPolicyAKId, PolicyCancellationDate as PolicyCancellationDate,
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
		AND EDWPolicyAKId in (
		SELECT DISTINCT POL.edw_pol_ak_id
		FROM DBO.premiummasterfact PTF 
		INNER JOIN DBO.policy_dim pol on PTF.PolicyDimId= pol.pol_dim_id 
		INNER JOIN DBO.InsuranceReferenceDim IRD on IRD.InsuranceReferenceDimid= PTF.InsuranceReferenceDimId and PolicyOfferingAbbreviation='WC')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPolicyAKId ORDER BY PolicyCancellationDate) = 1
),
LKP_PremiumMasterFact AS (
	SELECT
	EDWPremiumMasterCalculationPKID,
	WrittenExposure,
	in_EDWPremiumMasterCalculationPKId
	FROM (
		Select PMF.EDWPremiumMasterCalculationPKID as EDWPremiumMasterCalculationPKID ,
		PMF.WrittenExposure as WrittenExposure
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF 
		Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD on PMF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId  
		WHERE IRD.PolicyOfferingAbbreviation='WC' and PMF.AuditID > 0 
		and PMF.EDWPremiumMasterCalculationPKID <> -1 
		and PMF.WrittenExposure <> 0.00
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumMasterCalculationPKID ORDER BY EDWPremiumMasterCalculationPKID) = 1
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
	EXP_PassVal.CoverageSummaryCode AS i_CoverageSummaryCode,
	EXP_PassVal.CoverageSummaryDescription AS i_CoverageSummaryDescription,
	EXP_PassVal.CoverageGroupCode AS i_CoverageGroupCode,
	EXP_PassVal.CoverageGroupDescription AS i_CoverageGroupDescription,
	EXP_PassVal.CoverageCode AS i_CoverageCode,
	EXP_PassVal.CoverageDescription AS i_CoverageDescription,
	EXP_PassVal.RatedCoverageCode AS i_RatedCoverageCode,
	EXP_PassVal.RatedCoverageDescription AS i_RatedCoverageDescription,
	EXP_PassVal.ClassCode AS i_ClassCode,
	EXP_PassVal.ClassDescription AS i_ClassDescription,
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
	EXP_PassVal.PrimaryAgencyCode AS i_PrimaryAgencyCode,
	EXP_PassVal.PrimaryAgencyDoingBusinessAsName AS i_PrimaryAgencyDoingBusinessAsName,
	EXP_PassVal.PrimaryAgencyStateAbbreviation AS i_PrimaryAgencyStateAbbreviation,
	EXP_PassVal.PrimaryAgencyPhysicalAddressCity AS i_PrimaryAgencyPhysicalAddressCity,
	EXP_PassVal.PrimaryAgencyZIPCode AS i_PrimaryAgencyZIPCode,
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
	EXP_PassVal.HazardGroupCode AS i_HazardGroupCode,
	EXP_PassVal.DeductibleAmount AS i_DeductibleAmount,
	EXP_PassVal.PolicyPerAccidentLimit AS i_PolicyPerAccidentLimit,
	EXP_PassVal.PolicyPerDiseaseLimit AS i_PolicyPerDiseaseLimit,
	EXP_PassVal.PolicyAggregateLimit AS i_PolicyAggregateLimit,
	EXP_PassVal.ExposureBasis AS i_ExposureBasis,
	EXP_PassVal.DirectWrittenPremium AS i_DirectWrittenPremium,
	EXP_PassVal.SubjectWrittenPremium AS i_SubjectWrittenPremium,
	EXP_PassVal.ExperienceModifiedPremium AS i_ExperienceModifiedPremium,
	EXP_PassVal.ScheduleModifiedPremium AS i_ScheduleModifiedPremium,
	EXP_PassVal.OtherModifiedPremium AS i_OtherModifiedPremium,
	EXP_PassVal.ClassifiedPremium AS i_ClassifiedPremium,
	EXP_PassVal.AdmiraltyActFlag AS i_AdmiraltyActFlag,
	EXP_PassVal.FederalEmployersLiabilityActFlag AS i_FederalEmployersLiabilityActFlag,
	EXP_PassVal.USLongShoreAndHarborWorkersCompensationActFlag AS i_USLongShoreAndHarborWorkersCompensationActFlag,
	EXP_PassVal.FederalEmployerIDNumberfein AS i_FederalEmployerIDNumberfein,
	EXP_PassVal.RolloverPriorCarrier AS i_RolloverPriorCarrier,
	EXP_PassVal.CoverageEffectiveDate AS i_CoverageEffectiveDate,
	EXP_PassVal.CoverageExpirationDate AS i_CoverageExpirationDate,
	EXP_PassVal.CoverageCancellationDate AS i_CoverageCancellationDate,
	EXP_PassVal.PriorPolicyEffectiveDate AS i_PriorPolicyEffectiveDate,
	EXP_PassVal.GeneratedRecordFlag AS i_GeneratedRecordFlag,
	LKP_PremiumMasterFact.EDWPremiumMasterCalculationPKID AS lkp_EDWPremiumMasterCalculationPKID,
	LKP_PremiumMasterFact.WrittenExposure AS lkp_WrittenExposure,
	-- *INF*: :LKP.LKP_PhysicalState(i_AgencyStateAbbreviation)
	LKP_PHYSICALSTATE_i_AgencyStateAbbreviation.StateCode_Desc AS v_AgencyStateCode_desc,
	-- *INF*: :LKP.LKP_PhysicalState(i_PrimaryAgencyStateAbbreviation)
	LKP_PHYSICALSTATE_i_PrimaryAgencyStateAbbreviation.StateCode_Desc AS v_PrimaryAgencyStateCode_desc,
	-- *INF*: IIF(i_PolicyIssueCode='N','Y','N')
	-- 
	IFF(i_PolicyIssueCode = 'N', 'Y', 'N') AS v_NewBusinessIndicator,
	-- *INF*: IIF( ISNULL(lkp_EDWPremiumMasterCalculationPKID) , 0.00 ,IIF(i_GeneratedRecordFlag='F' or i_GeneratedRecordFlag = '0',lkp_WrittenExposure,0.00))
	IFF(
	    lkp_EDWPremiumMasterCalculationPKID IS NULL, 0.00,
	    IFF(
	        i_GeneratedRecordFlag = 'F'
	    or i_GeneratedRecordFlag = '0', lkp_WrittenExposure,
	        0.00
	    )
	) AS v_DirectWrittenExposure,
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
	-- *INF*: DECODE(i_AdmiraltyActFlag, 'F' , 'N' , 'T' , 'Y' , 'N' )
	DECODE(
	    i_AdmiraltyActFlag,
	    'F', 'N',
	    'T', 'Y',
	    'N'
	) AS v_AdmiraltyActFlag,
	-- *INF*: DECODE(i_FederalEmployersLiabilityActFlag, 'F' , 'N' , 'T' , 'Y' , 'N' )
	DECODE(
	    i_FederalEmployersLiabilityActFlag,
	    'F', 'N',
	    'T', 'Y',
	    'N'
	) AS v_FederalEmployersLiabilityActFlag,
	-- *INF*: DECODE(i_USLongShoreAndHarborWorkersCompensationActFlag, 'F' , 'N' , 'T' , 'Y' , 'N' )
	DECODE(
	    i_USLongShoreAndHarborWorkersCompensationActFlag,
	    'F', 'N',
	    'T', 'Y',
	    'N'
	) AS v_USLongshoreAndHarborWorkersCompensationActFlag,
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
	'WrittenPremium' AS o_EDWActuarialWorkersCompensationFactType,
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
	-- *INF*: IIF(ISNULL( i_CoverageSummaryCode),'N/A',i_CoverageSummaryCode )
	IFF(i_CoverageSummaryCode IS NULL, 'N/A', i_CoverageSummaryCode) AS o_CoverageSummaryCode,
	-- *INF*: IIF(ISNULL(i_CoverageSummaryDescription ),'N/A', i_CoverageSummaryDescription)
	IFF(i_CoverageSummaryDescription IS NULL, 'N/A', i_CoverageSummaryDescription) AS o_CoverageSummaryDescription,
	-- *INF*: IIF(ISNULL(i_CoverageGroupCode ),'N/A', i_CoverageGroupCode)
	IFF(i_CoverageGroupCode IS NULL, 'N/A', i_CoverageGroupCode) AS o_CoverageGroupCode,
	-- *INF*: IIF(ISNULL(i_CoverageGroupDescription ),'N/A', i_CoverageGroupDescription)
	IFF(i_CoverageGroupDescription IS NULL, 'N/A', i_CoverageGroupDescription) AS o_CoverageGroupDescription,
	-- *INF*: IIF(ISNULL( i_CoverageCode),'N/A', i_CoverageCode)
	IFF(i_CoverageCode IS NULL, 'N/A', i_CoverageCode) AS o_CoverageCode,
	-- *INF*: IIF(ISNULL( i_CoverageDescription),'N/A', i_CoverageDescription)
	IFF(i_CoverageDescription IS NULL, 'N/A', i_CoverageDescription) AS o_CoverageDescription,
	-- *INF*: IIF(ISNULL( i_RatedCoverageCode),'N/A', i_RatedCoverageCode)
	IFF(i_RatedCoverageCode IS NULL, 'N/A', i_RatedCoverageCode) AS o_RatedCoverageCode,
	-- *INF*: IIF(ISNULL( i_RatedCoverageDescription),'N/A', i_RatedCoverageDescription)
	IFF(i_RatedCoverageDescription IS NULL, 'N/A', i_RatedCoverageDescription) AS o_RatedCoverageDescription,
	-- *INF*: IIF(ISNULL(i_ClassCode ),'N/A', i_ClassCode)
	IFF(i_ClassCode IS NULL, 'N/A', i_ClassCode) AS o_ClassCode,
	-- *INF*: IIF(ISNULL(i_ClassDescription ),'N/A',i_ClassDescription )
	IFF(i_ClassDescription IS NULL, 'N/A', i_ClassDescription) AS o_ClassDescription,
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
	-- *INF*: IIF(ISNULL( i_HazardGroupCode),'N/A', i_HazardGroupCode)
	IFF(i_HazardGroupCode IS NULL, 'N/A', i_HazardGroupCode) AS o_HazardGroupCode,
	-- *INF*: IIF(ISNULL(lkp_DividendPlan ),'N/A',lkp_DividendPlan )
	IFF(lkp_DividendPlan IS NULL, 'N/A', lkp_DividendPlan) AS o_DividendPlan,
	-- *INF*: IIF(ISNULL( lkp_DividendType),'N/A', lkp_DividendType)
	IFF(lkp_DividendType IS NULL, 'N/A', lkp_DividendType) AS o_DividendType,
	-- *INF*: IIF(ISNULL( i_DeductibleAmount),'N/A', i_DeductibleAmount)
	IFF(i_DeductibleAmount IS NULL, 'N/A', i_DeductibleAmount) AS o_DeductibleAmount,
	-- *INF*: IIF(ISNULL(i_PolicyPerAccidentLimit ),'N/A', i_PolicyPerAccidentLimit)
	IFF(i_PolicyPerAccidentLimit IS NULL, 'N/A', i_PolicyPerAccidentLimit) AS o_PolicyPerAccidentLimit,
	-- *INF*: IIF(ISNULL(i_PolicyPerDiseaseLimit ),'N/A', i_PolicyPerDiseaseLimit)
	IFF(i_PolicyPerDiseaseLimit IS NULL, 'N/A', i_PolicyPerDiseaseLimit) AS o_PolicyPerDiseaseLimit,
	-- *INF*: IIF(ISNULL( i_PolicyAggregateLimit),'N/A', i_PolicyAggregateLimit)
	IFF(i_PolicyAggregateLimit IS NULL, 'N/A', i_PolicyAggregateLimit) AS o_PolicyAggregateLimit,
	-- *INF*: IIF(ISNULL( i_ExposureBasis),'N/A', i_ExposureBasis)
	IFF(i_ExposureBasis IS NULL, 'N/A', i_ExposureBasis) AS o_ExposureBasis,
	v_DirectWrittenExposure AS o_DirectWrittenExposure,
	-- *INF*: IIF(ISNULL(i_DirectWrittenPremium ),0.00, i_DirectWrittenPremium)
	-- 
	IFF(i_DirectWrittenPremium IS NULL, 0.00, i_DirectWrittenPremium) AS o_DirectWrittenPremium,
	-- *INF*: IIF(ISNULL(i_SubjectWrittenPremium ),0.00,i_SubjectWrittenPremium )
	IFF(i_SubjectWrittenPremium IS NULL, 0.00, i_SubjectWrittenPremium) AS o_SubjectWrittenPremium,
	-- *INF*: IIF(ISNULL(i_ExperienceModifiedPremium),0.00,i_ExperienceModifiedPremium )
	IFF(i_ExperienceModifiedPremium IS NULL, 0.00, i_ExperienceModifiedPremium) AS o_ExperienceModifiedPremium,
	-- *INF*: IIF(ISNULL( i_ScheduleModifiedPremium),0.00, i_ScheduleModifiedPremium)
	IFF(i_ScheduleModifiedPremium IS NULL, 0.00, i_ScheduleModifiedPremium) AS o_ScheduleModifiedPremium,
	-- *INF*: IIF(ISNULL(i_OtherModifiedPremium ),0.00,i_OtherModifiedPremium )
	IFF(i_OtherModifiedPremium IS NULL, 0.00, i_OtherModifiedPremium) AS o_OtherModifiedPremium,
	-- *INF*: IIF(ISNULL( i_ClassifiedPremium),0.00, i_ClassifiedPremium)
	IFF(i_ClassifiedPremium IS NULL, 0.00, i_ClassifiedPremium) AS o_ClassifiedPremium,
	0.00 AS o_DirectEarnedExposure,
	0.00 AS o_DirectEarnedPremium,
	0.00 AS o_SubjectDirectEarnedPremium,
	0.00 AS o_ExperienceModifiedDirectEarnedPremium,
	0.00 AS o_ScheduleModifiedDirectEarnedPremium,
	0.00 AS o_OtherModifiedDirectEarnedPremium,
	0.00 AS o_ClassifiedDirectEarnedPremium,
	0.00 AS o_PassThroughChargeAmount,
	v_AdmiraltyActFlag AS o_AdmiraltyActFlag,
	v_FederalEmployersLiabilityActFlag AS o_FederalEmployersLiabilityActFlag,
	v_USLongshoreAndHarborWorkersCompensationActFlag AS o_USLongShoreAndHarborWorkersCompensationActFlag,
	-- *INF*: IIF(ISNULL(i_FederalEmployerIDNumberfein ),'N/A', i_FederalEmployerIDNumberfein)
	IFF(i_FederalEmployerIDNumberfein IS NULL, 'N/A', i_FederalEmployerIDNumberfein) AS o_FederalEmployerIDNumberfein,
	-- *INF*: IIF(ISNULL( i_RolloverPriorCarrier),'N/A',i_RolloverPriorCarrier )
	IFF(i_RolloverPriorCarrier IS NULL, 'N/A', i_RolloverPriorCarrier) AS o_RolloverPriorCarrier,
	-- *INF*: IIF(ISNULL(i_CoverageEffectiveDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),i_CoverageEffectiveDate)
	IFF(
	    i_CoverageEffectiveDate IS NULL,
	    TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    i_CoverageEffectiveDate
	) AS o_CoverageEffectiveDate,
	-- *INF*: IIF(ISNULL(i_CoverageExpirationDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),i_CoverageExpirationDate)
	IFF(
	    i_CoverageExpirationDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    i_CoverageExpirationDate
	) AS o_CoverageExpirationDate,
	-- *INF*: IIF(ISNULL(i_CoverageCancellationDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),i_CoverageCancellationDate)
	IFF(
	    i_CoverageCancellationDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    i_CoverageCancellationDate
	) AS o_CoverageCancellationDate,
	-- *INF*: IIF(ISNULL(i_PriorPolicyEffectiveDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),i_PriorPolicyEffectiveDate)
	IFF(
	    i_PriorPolicyEffectiveDate IS NULL,
	    TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    i_PriorPolicyEffectiveDate
	) AS o_PriorPolicyEffectiveDate
	FROM EXP_PassVal
	LEFT JOIN LKP_BusinessClassDim
	ON LKP_BusinessClassDim.BusinessClassDimId = EXP_PassVal.BusinessClassDimId
	LEFT JOIN LKP_DividendTypeDim
	ON LKP_DividendTypeDim.DividendTypeDimId = LKP_DividendFact.DividendTypeDimId
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = EXP_PassVal.pol_ak_id
	LEFT JOIN LKP_PremiumMasterFact
	ON LKP_PremiumMasterFact.EDWPremiumMasterCalculationPKID = EXP_PassVal.EDWPremiumMasterCalculationPKId
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

),
ActuarialWorkersCompensationPremium AS (
	TRUNCATE TABLE ActuarialWorkersCompensationPremium;
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
	o_RatedCoverageCode AS RATEDCOVERAGECODE, 
	o_RatedCoverageDescription AS RATEDCOVERAGEDESCRIPTION
	FROM EXP_GetValues
),