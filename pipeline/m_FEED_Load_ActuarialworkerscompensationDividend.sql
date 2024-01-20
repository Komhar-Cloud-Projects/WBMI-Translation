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
SQ_DividendFact AS (
	Declare @Date as date
	
	set @Date = case when '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QA%' or '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QC%'
	then
	convert(date,EOMONTH(getdate(),0))
	else
	convert(date,EOMONTH(getdate(),-1))
	end 
	
	select * from
	(
	SELECT 
	 DividendFact.DividendFactId,
	 pol_Current.edw_pol_ak_id,
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
	 pol_Current.ProgramCode,
	 pol_Current.ProgramDescription,
	 pol_Current.AssociationCode,
	 pol_Current.AssociationDescription,
	 pol_Current.industry_risk_grade_code,
	 pol_Current.industry_risk_grade_code_descript,
	 pol_Current.pol_eff_date,
	 pol_Current.pol_exp_date,
	 pol_Current.pol_cancellation_rsn_code,
	 pol_Current.pol_cancellation_rsn_code_descript,
	 pol_Current.orig_incptn_date,
	 pol_Current.renl_code,
	 pol_Current.renl_code_descript,
	 pol_Current.pol_status_code,
	 pol_Current.pol_status_code_descript,
	 pol_Current.pol_key,
	 pol_Current.pol_sym,
	 pol_Current.pol_num,
	 pol_Current.pol_mod,
	 pol_Current.pol_issue_code,
	 pol_Current.pol_issue_code_descript,
	 pol_Current.state_of_domicile_code,
	 pol_Current.state_of_domicile_abbrev,
	 pol_Current.state_of_domicile_code_descript,
	 pol_Current.prim_bus_class_code,
	 pol_Current.prim_bus_class_code_descript,
	 ad_Current.AgencyCode,
	 ad_Current.AgencyDoingBusinessAsName,
	 ad_Current.PhysicalStateAbbreviation,
	 ad_Current.PhysicalCity,
	 ad_Current.PhysicalZipCode,
	 ccd_Current.cust_num,
	 ccd_Current.name,
	 ccd_Current.sic_code,
	 ccd_Current.sic_code_descript,
	 ccd_Current.naics_code,
	 ccd_Current.naics_code_descript,
	 pol_Current.serv_center_support_code,
	 pol_Current.prior_pol_key,
	 prior_pol.pol_num prior_pol_num,
	 prior_pol.pol_sym prior_pol_sym,
	 prior_pol.pol_mod prior_pol_ver,
	 DividendFact.DividendPaidAmount,
	 DividendFact.DividendPayableAmount,
	 ccd_Current.fed_tax_id,
	 pol_Current.RolloverPriorCarrier,
	 prior_pol.pol_eff_date prior_pol_eff_date,
	 aed_current.ProducerCode,
	 (case when aed_current.AgencyEmployeeRole = 'Producer' then 
	 aed_current.AgencyEmployeeFirstName +' '+ aed_current.agencyemployeelastname else 'N/A' end) as Producer_FullName,
	 DividendTypeDim.DividendPlan,
	 DividendTypeDim.DividendType,
	 calendar_dim.clndr_date Accounting_Date,
	--Removed below columns as per WREQ-13710
	--SD.AgencyCode as prim_agency_code, 
	--SD.AgencyDoingBusinessAsName as prim_agency_doing_business_Name,
	--SD.PhysicalStateAbbreviation as prim_agency_state_abbr,
	--SD.PhysicalCity as prim_agency_city,
	--SD.PhysicalZipCode as prim_agency_zip_code,
	 pol_Current.pol_dim_id,
	 ad_Current.SalesDivisionDimId,
	 DividendFact.StateDimId,
	 pol_Current.UnderwritingDivisionDimId,
	 pol_Current.BusinessClassDimId,
	 ad_Current.EDWAgencyAKID 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendFact 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on DividendFact.PolicyDimId= pol.pol_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol_Current on pol_Current.edw_pol_ak_id=pol.edw_pol_ak_id and pol_Current.crrnt_snpsht_flag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim AgencyDim on DividendFact.AgencyDimId =AgencyDim.AgencyDimID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad_Current --To get the current Agency attribute 
	on AgencyDim.EDWAgencyAKID= ad_Current.EDWAgencyAKID and ad_Current.CurrentSnapshotFlag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim on DividendFact.ContractCustomerDimId=contract_customer_dim.contract_cust_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim ccd_Current --To get the current customer attribute 
	on contract_customer_dim.edw_contract_cust_ak_id= ccd_Current.edw_contract_cust_ak_id and ccd_Current.crrnt_snpsht_flag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim on AgencyEmployeeDim.AgencyEmployeeDimID= pol_Current.AgencyEmployeeDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim aed_current
	on AgencyEmployeeDim.EDWAgencyEmployeeAKID = aed_current.EDWAgencyEmployeeAKID and aed_current.CurrentSnapshotFlag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendTypeDim on DividendTypeDim.DividendTypeDimId = DividendFact.DividendTypeDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim on calendar_dim.clndr_id = DividendFact.DividendRunDateId
	--INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD on SD.AgencyCode=AD_current.LegalPrimaryAgencyCode AND SD.CurrentSnapshotFlag= 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim on InsuranceReferenceDim.InsuranceReferenceDimid= DividendFact.StrategicProfitCenterDimId and 
	PolicyOfferingAbbreviation='WC'
	Left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim prior_pol on prior_pol.pol_key= pol_Current.prior_pol_key and prior_pol.crrnt_snpsht_flag =1
	where convert(date,calendar_dim.clndr_date) <=  @Date AND pol.pol_sym <> '000'
	Union All
	SELECT 
	 DCTDividendFact.DCTDividendFactId as DividendFactId,
	 pol_Current.edw_pol_ak_id,
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
	 pol_Current.ProgramCode,
	 pol_Current.ProgramDescription,
	 pol_Current.AssociationCode,
	 pol_Current.AssociationDescription,
	 pol_Current.industry_risk_grade_code,
	 pol_Current.industry_risk_grade_code_descript,
	 pol_Current.pol_eff_date,
	 pol_Current.pol_exp_date,
	 pol_Current.pol_cancellation_rsn_code,
	 pol_Current.pol_cancellation_rsn_code_descript,
	 pol_Current.orig_incptn_date,
	 pol_Current.renl_code,
	 pol_Current.renl_code_descript,
	 pol_Current.pol_status_code,
	 pol_Current.pol_status_code_descript,
	 pol_Current.pol_key,
	 pol_Current.pol_sym,
	 pol_Current.pol_num,
	 pol_Current.pol_mod,
	 pol_Current.pol_issue_code,
	 pol_Current.pol_issue_code_descript,
	 pol_Current.state_of_domicile_code,
	 pol_Current.state_of_domicile_abbrev,
	 pol_Current.state_of_domicile_code_descript,
	 pol_Current.prim_bus_class_code,
	 pol_Current.prim_bus_class_code_descript,
	 ad_Current.AgencyCode,
	 ad_Current.AgencyDoingBusinessAsName,
	 ad_Current.PhysicalStateAbbreviation,
	 ad_Current.PhysicalCity,
	 ad_Current.PhysicalZipCode,
	 ccd_Current.cust_num,
	 ccd_Current.name,
	 ccd_Current.sic_code,
	 ccd_Current.sic_code_descript,
	 ccd_Current.naics_code,
	 ccd_Current.naics_code_descript,
	 pol_Current.serv_center_support_code,
	 pol_Current.prior_pol_key,
	 prior_pol.pol_num prior_pol_num,
	 prior_pol.pol_sym prior_pol_sym,
	 prior_pol.pol_mod prior_pol_ver,
	 DCTDividendFact.DividendPaidAmount,
	 0.00 as DividendPayableAmount,
	 ccd_Current.fed_tax_id,
	 pol_Current.RolloverPriorCarrier,
	 prior_pol.pol_eff_date prior_pol_eff_date,
	 aed_current.ProducerCode,
	 (case when aed_current.AgencyEmployeeRole = 'Producer' then 
	 aed_current.AgencyEmployeeFirstName +' '+ aed_current.agencyemployeelastname else 'N/A' end) as Producer_FullName,
	 DividendTypeDim.DividendPlan,
	 DividendTypeDim.DividendType,
	 calendar_dim.clndr_date Accounting_Date,
	--Removed below columns as per WREQ-13710
	--SD.AgencyCode as prim_agency_code, 
	--SD.AgencyDoingBusinessAsName as prim_agency_doing_business_Name,
	--SD.PhysicalStateAbbreviation as prim_agency_state_abbr,
	--SD.PhysicalCity as prim_agency_city,
	--SD.PhysicalZipCode as prim_agency_zip_code,
	 pol_Current.pol_dim_id,
	 ad_Current.SalesDivisionDimId,
	 DCTDividendFact.StateDimId,
	 pol_Current.UnderwritingDivisionDimId,
	 pol_Current.BusinessClassDimId,
	 ad_Current.EDWAgencyAKID 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTDividendFact
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on DCTDividendFact.PolicyDimId= pol.pol_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol_Current on pol_Current.edw_pol_ak_id=pol.edw_pol_ak_id and pol_Current.crrnt_snpsht_flag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim AgencyDim on DCTDividendFact.AgencyDimId =AgencyDim.AgencyDimID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim ad_Current --To get the current Agency attribute 
	on AgencyDim.EDWAgencyAKID= ad_Current.EDWAgencyAKID and ad_Current.CurrentSnapshotFlag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim on DCTDividendFact.ContractCustomerDimId=contract_customer_dim.contract_cust_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim ccd_Current --To get the current customer attribute 
	on contract_customer_dim.edw_contract_cust_ak_id= ccd_Current.edw_contract_cust_ak_id and ccd_Current.crrnt_snpsht_flag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim on AgencyEmployeeDim.AgencyEmployeeDimID= pol_Current.AgencyEmployeeDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployeeDim aed_current
	on AgencyEmployeeDim.EDWAgencyEmployeeAKID = aed_current.EDWAgencyEmployeeAKID and aed_current.CurrentSnapshotFlag =1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendTypeDim on DividendTypeDim.DividendTypeDimId = DCTDividendFact.DividendTypeDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim on calendar_dim.clndr_id = DCTDividendFact.DividendRunDateId
	--INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim SD on SD.AgencyCode=AD_current.LegalPrimaryAgencyCode AND SD.CurrentSnapshotFlag= 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim on InsuranceReferenceDim.InsuranceReferenceDimid= DCTDividendFact.InsuranceReferenceDimid and 
	PolicyOfferingAbbreviation='WC'
	Left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim prior_pol on prior_pol.pol_key= pol_Current.prior_pol_key and prior_pol.crrnt_snpsht_flag =1
	where convert(date,calendar_dim.clndr_date) <=  @Date) dividend
	order by dividend.edw_pol_ak_id desc
),
EXP_GetValues AS (
	SELECT
	DividendFactId,
	edw_pol_ak_id,
	EnterpriseGroupCode,
	EnterpriseGroupAbbreviation,
	StrategicProfitCenterCode,
	StrategicProfitCenterAbbreviation,
	InsuranceReferenceLegalEntityCode,
	InsuranceReferenceLegalEntityAbbreviation,
	PolicyOfferingCode,
	PolicyOfferingAbbreviation,
	PolicyOfferingDescription,
	ProductCode,
	ProductAbbreviation,
	ProductDescription,
	InsuranceReferenceLineOfBusinessCode,
	InsuranceReferenceLineOfBusinessAbbreviation,
	InsuranceReferenceLineOfBusinessDescription,
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
	AgencyCode,
	AgencyDoingBusinessAsName,
	PhysicalStateAbbreviation,
	PhysicalCity,
	PhysicalZipCode,
	cust_num,
	name,
	sic_code,
	sic_code_descript,
	naics_code,
	naics_code_descript,
	serv_center_support_code,
	prior_pol_key,
	prior_pol_num,
	prior_pol_sym,
	prior_pol_ver,
	DividendPaidAmount,
	DividendPayableAmount,
	fed_tax_id,
	RolloverPriorCarrier,
	prior_pol_eff_date,
	ProducerCode,
	Producer_FullName,
	DividendPlan,
	DividendType,
	Accounting_Date,
	PolicyDimId,
	SalesDivisionDimId,
	StateDimId,
	UnderwritingDivisionDimId,
	BusinessClassDimId,
	EDWAgencyAKID
	FROM SQ_DividendFact
),
SQ_policy_dim AS (
	Declare @Date as date
	
	set @Date = case when '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QA%' or '@{pipeline().parameters.PMINTEGRATIONSERVICENAME}' like '%QC%'
	then
	convert(date,EOMONTH(getdate(),0))
	else
	convert(date,EOMONTH(getdate(),-1))
	end 
	
	SELECT 
	distinct pol.edw_pol_ak_id 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendFact 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim pol on DividendFact.PolicyDimId= pol.pol_dim_id 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DividendTypeDim on DividendTypeDim.DividendTypeDimId = DividendFact.DividendTypeDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim on calendar_dim.clndr_id = DividendFact.DividendRunDateId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim on InsuranceReferenceDim.InsuranceReferenceDimid= DividendFact.StrategicProfitCenterDimId and 
	PolicyOfferingAbbreviation='WC'
	where convert(date,calendar_dim.clndr_date) <= @Date
	order by pol.edw_pol_ak_id desc
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
	EXP_GetValues.DividendFactId, 
	EXP_GetValues.edw_pol_ak_id, 
	EXP_GetValues.EnterpriseGroupCode, 
	EXP_GetValues.EnterpriseGroupAbbreviation, 
	EXP_GetValues.StrategicProfitCenterCode, 
	EXP_GetValues.StrategicProfitCenterAbbreviation, 
	EXP_GetValues.InsuranceReferenceLegalEntityCode, 
	EXP_GetValues.InsuranceReferenceLegalEntityAbbreviation, 
	EXP_GetValues.PolicyOfferingCode, 
	EXP_GetValues.PolicyOfferingAbbreviation, 
	EXP_GetValues.PolicyOfferingDescription, 
	EXP_GetValues.ProductCode, 
	EXP_GetValues.ProductAbbreviation, 
	EXP_GetValues.ProductDescription, 
	EXP_GetValues.InsuranceReferenceLineOfBusinessCode, 
	EXP_GetValues.InsuranceReferenceLineOfBusinessAbbreviation, 
	EXP_GetValues.InsuranceReferenceLineOfBusinessDescription, 
	EXP_GetValues.AccountingProductCode, 
	EXP_GetValues.AccountingProductAbbreviation, 
	EXP_GetValues.AccountingProductDescription, 
	EXP_GetValues.RatingPlanCode, 
	EXP_GetValues.RatingPlanDescription, 
	EXP_GetValues.InsuranceSegmentCode, 
	EXP_GetValues.InsuranceSegmentDescription, 
	EXP_GetValues.ProgramCode, 
	EXP_GetValues.ProgramDescription, 
	EXP_GetValues.AssociationCode, 
	EXP_GetValues.AssociationDescription, 
	mplt_Determine_RiskGrade_Code_and_Description.RiskGradeCode, 
	mplt_Determine_RiskGrade_Code_and_Description.RiskGradeDescription, 
	EXP_GetValues.pol_eff_date, 
	EXP_GetValues.pol_exp_date, 
	EXP_GetValues.pol_cancellation_rsn_code, 
	EXP_GetValues.pol_cancellation_rsn_code_descript, 
	EXP_GetValues.orig_incptn_date, 
	EXP_GetValues.renl_code, 
	EXP_GetValues.renl_code_descript, 
	EXP_GetValues.pol_status_code, 
	EXP_GetValues.pol_status_code_descript, 
	EXP_GetValues.pol_key, 
	EXP_GetValues.pol_sym, 
	EXP_GetValues.pol_num, 
	EXP_GetValues.pol_mod, 
	EXP_GetValues.pol_issue_code, 
	EXP_GetValues.pol_issue_code_descript, 
	EXP_GetValues.state_of_domicile_code, 
	EXP_GetValues.state_of_domicile_abbrev, 
	EXP_GetValues.state_of_domicile_code_descript, 
	EXP_GetValues.prim_bus_class_code, 
	EXP_GetValues.prim_bus_class_code_descript, 
	EXP_GetValues.AgencyCode, 
	EXP_GetValues.AgencyDoingBusinessAsName, 
	EXP_GetValues.PhysicalStateAbbreviation, 
	EXP_GetValues.PhysicalCity, 
	EXP_GetValues.PhysicalZipCode, 
	EXP_GetValues.cust_num, 
	EXP_GetValues.name, 
	EXP_GetValues.sic_code, 
	EXP_GetValues.sic_code_descript, 
	EXP_GetValues.naics_code, 
	EXP_GetValues.naics_code_descript, 
	EXP_GetValues.serv_center_support_code, 
	EXP_GetValues.prior_pol_key, 
	EXP_GetValues.prior_pol_num, 
	EXP_GetValues.prior_pol_sym, 
	EXP_GetValues.prior_pol_ver, 
	EXP_GetValues.DividendPaidAmount, 
	EXP_GetValues.DividendPayableAmount, 
	EXP_GetValues.fed_tax_id, 
	EXP_GetValues.RolloverPriorCarrier, 
	EXP_GetValues.prior_pol_eff_date, 
	EXP_GetValues.ProducerCode, 
	EXP_GetValues.Producer_FullName, 
	EXP_GetValues.DividendPlan, 
	EXP_GetValues.DividendType, 
	EXP_GetValues.Accounting_Date, 
	EXP_GetValues.PolicyDimId, 
	EXP_GetValues.SalesDivisionDimId, 
	EXP_GetValues.StateDimId, 
	EXP_GetValues.UnderwritingDivisionDimId, 
	EXP_GetValues.BusinessClassDimId, 
	EXP_GetValues.EDWAgencyAKID, 
	mplt_Determine_RiskGrade_Code_and_Description.in_PolicyAKID1
	FROM EXP_GetValues
	INNER JOIN mplt_Determine_RiskGrade_Code_and_Description
	ON mplt_Determine_RiskGrade_Code_and_Description.in_PolicyAKID1 = EXP_GetValues.edw_pol_ak_id
),
EXP_Pass_Values AS (
	SELECT
	DividendFactId,
	edw_pol_ak_id,
	EnterpriseGroupCode,
	EnterpriseGroupAbbreviation,
	StrategicProfitCenterCode,
	StrategicProfitCenterAbbreviation,
	InsuranceReferenceLegalEntityCode,
	InsuranceReferenceLegalEntityAbbreviation,
	PolicyOfferingCode,
	PolicyOfferingAbbreviation,
	PolicyOfferingDescription,
	ProductCode,
	ProductAbbreviation,
	ProductDescription,
	InsuranceReferenceLineOfBusinessCode,
	InsuranceReferenceLineOfBusinessAbbreviation,
	InsuranceReferenceLineOfBusinessDescription,
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
	RiskGradeCode AS industry_risk_grade_code,
	RiskGradeDescription AS industry_risk_grade_code_descript,
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
	AgencyCode,
	AgencyDoingBusinessAsName,
	PhysicalStateAbbreviation,
	PhysicalCity,
	PhysicalZipCode,
	cust_num,
	name,
	sic_code,
	sic_code_descript,
	naics_code,
	naics_code_descript,
	serv_center_support_code,
	prior_pol_key,
	prior_pol_num,
	prior_pol_sym,
	prior_pol_ver,
	DividendPaidAmount,
	DividendPayableAmount,
	fed_tax_id,
	RolloverPriorCarrier,
	prior_pol_eff_date,
	ProducerCode,
	Producer_FullName,
	DividendPlan,
	DividendType,
	Accounting_Date,
	PolicyDimId,
	SalesDivisionDimId,
	StateDimId,
	UnderwritingDivisionDimId,
	BusinessClassDimId,
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
LKP_PolicyCurrentStatusDim AS (
	SELECT
	PolicyCancellationDate,
	PolicyStatusCode,
	PolicyStatusDescription,
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
LKP_StateDim AS (
	SELECT
	StateCode,
	StateAbbreviation,
	StateDescription,
	StateDimID
	FROM (
		SELECT 
			StateCode,
			StateAbbreviation,
			StateDescription,
			StateDimID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StateDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateDimID ORDER BY StateCode) = 1
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
EXP_Get_Values AS (
	SELECT
	EXP_Pass_Values.DividendFactId,
	EXP_Pass_Values.edw_pol_ak_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
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
	EXP_Pass_Values.industry_risk_grade_code,
	EXP_Pass_Values.industry_risk_grade_code_descript,
	EXP_Pass_Values.pol_eff_date,
	EXP_Pass_Values.pol_exp_date,
	EXP_Pass_Values.pol_cancellation_rsn_code,
	EXP_Pass_Values.pol_cancellation_rsn_code_descript,
	EXP_Pass_Values.orig_incptn_date,
	EXP_Pass_Values.renl_code,
	EXP_Pass_Values.renl_code_descript,
	EXP_Pass_Values.pol_key,
	EXP_Pass_Values.pol_sym,
	EXP_Pass_Values.pol_num,
	EXP_Pass_Values.pol_mod,
	EXP_Pass_Values.pol_issue_code,
	EXP_Pass_Values.pol_issue_code_descript,
	EXP_Pass_Values.state_of_domicile_code,
	EXP_Pass_Values.state_of_domicile_abbrev,
	EXP_Pass_Values.state_of_domicile_code_descript,
	EXP_Pass_Values.prim_bus_class_code,
	EXP_Pass_Values.prim_bus_class_code_descript,
	EXP_Pass_Values.AgencyCode,
	EXP_Pass_Values.AgencyDoingBusinessAsName,
	EXP_Pass_Values.PhysicalStateAbbreviation,
	EXP_Pass_Values.PhysicalCity,
	EXP_Pass_Values.PhysicalZipCode,
	EXP_Pass_Values.ProducerCode,
	EXP_Pass_Values.Producer_FullName,
	EXP_Pass_Values.cust_num,
	EXP_Pass_Values.name,
	EXP_Pass_Values.sic_code,
	EXP_Pass_Values.sic_code_descript,
	EXP_Pass_Values.naics_code,
	EXP_Pass_Values.naics_code_descript,
	EXP_Pass_Values.serv_center_support_code,
	EXP_Pass_Values.prior_pol_key,
	EXP_Pass_Values.DividendPaidAmount,
	EXP_Pass_Values.DividendPayableAmount,
	EXP_Pass_Values.fed_tax_id,
	EXP_Pass_Values.RolloverPriorCarrier,
	EXP_Pass_Values.DividendPlan,
	EXP_Pass_Values.DividendType,
	LKP_AgencyRelationship.Prim_Agency_Code AS i_prim_agency_code,
	LKP_AgencyRelationship.prim_agency_name AS i_prim_agency_doing_business_Name,
	LKP_AgencyRelationship.prim_agency_state_abbr AS i_prim_agency_state_abbr,
	LKP_AgencyRelationship.prim_agency_city AS i_prim_agency_city,
	LKP_AgencyRelationship.prim_agency_zip_code AS i_prim_agency_zip_code,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate AS i_PolicyCancellationDate,
	LKP_PolicyCurrentStatusDim.PolicyStatusCode AS i_pol_status_code,
	LKP_PolicyCurrentStatusDim.PolicyStatusDescription AS i_pol_status_code_descript,
	LKP_StateDim.StateCode AS i_StateCode,
	LKP_StateDim.StateAbbreviation AS i_StateAbbreviation,
	LKP_StateDim.StateDescription AS i_StateDescription,
	LKP_SalesDivisionDim.RegionalSalesManagerDisplayName AS i_RegionalSalesManagerDisplayName,
	LKP_SalesDivisionDim.SalesTerritoryCode AS i_SalesTerritoryCode,
	EXP_Pass_Values.prior_pol_num AS i_prior_pol_num,
	EXP_Pass_Values.prior_pol_sym AS i_prior_pol_sym,
	EXP_Pass_Values.prior_pol_ver AS i_prior_pol_mod,
	LKP_UnderwritingDivisionDim.UnderwriterDisplayName AS i_UnderwriterDisplayName,
	LKP_UnderwritingDivisionDim.UnderwriterManagerDisplayName AS i_UnderwriterManagerDisplayName,
	LKP_UnderwritingDivisionDim.UnderwritingRegionCodeDescription AS i_UnderwritingRegionCodeDescription,
	LKP_BusinessClassDim.BusinessSegmentCode AS i_BusinessSegmentCode,
	LKP_BusinessClassDim.BusinessSegmentDescription AS i_BusinessSegmentDescription,
	LKP_BusinessClassDim.StrategicBusinessGroupCode AS i_StrategicBusinessGroupCode,
	LKP_BusinessClassDim.StrategicBusinessGroupDescription AS i_StrategicBusinessGroupDescription,
	EXP_Pass_Values.Accounting_Date AS i_accounting_date,
	EXP_Pass_Values.prior_pol_eff_date AS i_prior_pol_eff_date,
	-- *INF*: IIF(ISNULL(i_PolicyCancellationDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),i_PolicyCancellationDate)
	IFF(
	    i_PolicyCancellationDate IS NULL,
	    TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    i_PolicyCancellationDate
	) AS v_PolicyCancellationDate,
	-- *INF*: IIF(ISNULL(i_accounting_date),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),i_accounting_date)
	IFF(
	    i_accounting_date IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    i_accounting_date
	) AS v_accounting_date,
	-- *INF*: :LKP.LKP_PhysicalState(PhysicalStateAbbreviation)
	LKP_PHYSICALSTATE_PhysicalStateAbbreviation.StateCode_Desc AS v_StateCode_Desc,
	-- *INF*: :LKP.LKP_PhysicalState(i_prim_agency_state_abbr)
	LKP_PHYSICALSTATE_i_prim_agency_state_abbr.StateCode_Desc AS v_StateCode_Desc_prim,
	-- *INF*: IIF(ISNULL(i_prim_agency_code),'N/A',i_prim_agency_code)
	IFF(i_prim_agency_code IS NULL, 'N/A', i_prim_agency_code) AS o_prim_agency_code,
	-- *INF*: IIF(ISNULL(i_prim_agency_doing_business_Name),'N/A',i_prim_agency_doing_business_Name)
	IFF(i_prim_agency_doing_business_Name IS NULL, 'N/A', i_prim_agency_doing_business_Name) AS o_prim_agency_doing_business_Name,
	v_PolicyCancellationDate AS o_PolicyCancellationDate,
	-- *INF*: IIF(ISNULL(i_prim_agency_state_abbr),'N/A',i_prim_agency_state_abbr)
	IFF(i_prim_agency_state_abbr IS NULL, 'N/A', i_prim_agency_state_abbr) AS o_prim_agency_state_abbr,
	-- *INF*: IIF(ISNULL(i_prim_agency_city),'N/A',i_prim_agency_city)
	IFF(i_prim_agency_city IS NULL, 'N/A', i_prim_agency_city) AS o_prim_agency_city,
	-- *INF*: IIF(ISNULL(i_prim_agency_zip_code),'N/A',i_prim_agency_zip_code)
	IFF(i_prim_agency_zip_code IS NULL, 'N/A', i_prim_agency_zip_code) AS o_prim_agency_zip_code,
	-- *INF*: IIF(ISNULL(i_pol_status_code),'N/A',i_pol_status_code)
	IFF(i_pol_status_code IS NULL, 'N/A', i_pol_status_code) AS o_pol_status_code,
	-- *INF*: IIF(ISNULL(i_pol_status_code_descript),'N/A',i_pol_status_code_descript)
	IFF(i_pol_status_code_descript IS NULL, 'N/A', i_pol_status_code_descript) AS o_pol_status_code_descript,
	-- *INF*: IIF(ISNULL(i_StateCode),'N/A',i_StateCode)
	IFF(i_StateCode IS NULL, 'N/A', i_StateCode) AS o_StateCode,
	-- *INF*: IIF(ISNULL(i_StateAbbreviation),'N/A',i_StateAbbreviation)
	IFF(i_StateAbbreviation IS NULL, 'N/A', i_StateAbbreviation) AS o_StateAbbreviation,
	-- *INF*: IIF(ISNULL(i_StateDescription),'N/A',i_StateDescription)
	IFF(i_StateDescription IS NULL, 'N/A', i_StateDescription) AS o_StateDescription,
	-- *INF*: IIF(ISNULL(i_RegionalSalesManagerDisplayName),'N/A',i_RegionalSalesManagerDisplayName)
	IFF(i_RegionalSalesManagerDisplayName IS NULL, 'N/A', i_RegionalSalesManagerDisplayName) AS o_RegionalSalesManagerDisplayName,
	-- *INF*: IIF(ISNULL(i_SalesTerritoryCode),'N/A',i_SalesTerritoryCode)
	IFF(i_SalesTerritoryCode IS NULL, 'N/A', i_SalesTerritoryCode) AS o_SalesTerritoryCode,
	-- *INF*: ADD_TO_DATE(ADD_TO_DATE(TRUNC(LAST_DAY(ADD_TO_DATE(SYSDATE,'MM',-1))),'DD',1),'SS',-1)
	DATEADD(SECOND,- 1,DATEADD(DAY,1,TRUNC(LAST_DAY(DATEADD(MONTH,- 1,CURRENT_TIMESTAMP))))) AS o_Last_Booked_Date,
	-- *INF*: GET_DATE_PART(pol_eff_date,'YYYY')
	DATE_PART(pol_eff_date, 'YYYY') AS o_pol_eff_yr,
	-- *INF*: TO_INTEGER(TO_CHAR(pol_eff_date,'Q'))
	CAST(TO_CHAR(pol_eff_date, 'Q') AS INTEGER) AS o_pol_eff_qtr,
	-- *INF*: GET_DATE_PART(pol_eff_date,'MM')
	DATE_PART(pol_eff_date, 'MM') AS o_pol_eff_month,
	-- *INF*: TO_CHAR(pol_eff_date,'MONTH')
	TO_CHAR(pol_eff_date, 'MONTH') AS o_pol_eff_month_descript,
	-- *INF*: GET_DATE_PART(pol_exp_date,'YYYY')
	DATE_PART(pol_exp_date, 'YYYY') AS o_pol_exp_yr,
	-- *INF*: TO_INTEGER(TO_CHAR(pol_exp_date,'Q'))
	CAST(TO_CHAR(pol_exp_date, 'Q') AS INTEGER) AS o_pol_exp_qtr,
	-- *INF*: GET_DATE_PART(pol_exp_date,'MM')
	DATE_PART(pol_exp_date, 'MM') AS o_pol_exp_month,
	-- *INF*: TO_CHAR(pol_exp_date,'MONTH')
	TO_CHAR(pol_exp_date, 'MONTH') AS o_pol_exp_month_descript,
	-- *INF*: IIF(ISNULL(i_prior_pol_num),'N/A',i_prior_pol_num)
	IFF(i_prior_pol_num IS NULL, 'N/A', i_prior_pol_num) AS o_prior_pol_num,
	-- *INF*: IIF(ISNULL(i_prior_pol_sym),'N/A',i_prior_pol_sym)
	IFF(i_prior_pol_sym IS NULL, 'N/A', i_prior_pol_sym) AS o_prior_pol_sym,
	-- *INF*: IIF(ISNULL(i_prior_pol_mod),'N/A',i_prior_pol_mod)
	IFF(i_prior_pol_mod IS NULL, 'N/A', i_prior_pol_mod) AS o_prior_pol_mod,
	-- *INF*: IIF(ISNULL(i_UnderwriterDisplayName),'N/A',i_UnderwriterDisplayName)
	IFF(i_UnderwriterDisplayName IS NULL, 'N/A', i_UnderwriterDisplayName) AS o_UnderwriterDisplayName,
	-- *INF*: IIF(ISNULL(i_UnderwriterManagerDisplayName),'N/A',i_UnderwriterManagerDisplayName)
	IFF(i_UnderwriterManagerDisplayName IS NULL, 'N/A', i_UnderwriterManagerDisplayName) AS o_UnderwriterManagerDisplayName,
	-- *INF*: IIF(ISNULL(i_UnderwritingRegionCodeDescription),'N/A',i_UnderwritingRegionCodeDescription)
	IFF(i_UnderwritingRegionCodeDescription IS NULL, 'N/A', i_UnderwritingRegionCodeDescription) AS o_UnderwritingRegionCodeDescription,
	-- *INF*: IIF(ISNULL(i_BusinessSegmentCode),'N/A',i_BusinessSegmentCode)
	IFF(i_BusinessSegmentCode IS NULL, 'N/A', i_BusinessSegmentCode) AS o_BusinessSegmentCode,
	-- *INF*: IIF(ISNULL(i_BusinessSegmentDescription),'N/A',i_BusinessSegmentDescription)
	IFF(i_BusinessSegmentDescription IS NULL, 'N/A', i_BusinessSegmentDescription) AS o_BusinessSegmentDescription,
	-- *INF*: IIF(ISNULL(i_StrategicBusinessGroupCode),'N/A',i_StrategicBusinessGroupCode)
	IFF(i_StrategicBusinessGroupCode IS NULL, 'N/A', i_StrategicBusinessGroupCode) AS o_StrategicBusinessGroupCode,
	-- *INF*: IIF(ISNULL(i_StrategicBusinessGroupDescription),'N/A',i_StrategicBusinessGroupDescription)
	IFF(i_StrategicBusinessGroupDescription IS NULL, 'N/A', i_StrategicBusinessGroupDescription) AS o_StrategicBusinessGroupDescription,
	v_accounting_date AS o_accounting_date,
	-- *INF*: GET_DATE_PART(v_accounting_date,'YYYY')
	DATE_PART(v_accounting_date, 'YYYY') AS o_accounting_yr,
	-- *INF*: TO_INTEGER(TO_CHAR(v_accounting_date,'Q'))
	CAST(TO_CHAR(v_accounting_date, 'Q') AS INTEGER) AS o_accounting_qtr,
	-- *INF*: GET_DATE_PART(v_accounting_date,'MM')
	DATE_PART(v_accounting_date, 'MM') AS o_accounting_month,
	-- *INF*: TO_CHAR(v_accounting_date,'MONTH')
	TO_CHAR(v_accounting_date, 'MONTH') AS o_accounting_month_descript,
	-- *INF*: GET_DATE_PART(v_PolicyCancellationDate,'YYYY')
	DATE_PART(v_PolicyCancellationDate, 'YYYY') AS o_pol_cancellation_yr,
	-- *INF*: TO_INTEGER(TO_CHAR(v_PolicyCancellationDate,'Q'))
	CAST(TO_CHAR(v_PolicyCancellationDate, 'Q') AS INTEGER) AS o_pol_cancellation_qtr,
	-- *INF*: GET_DATE_PART(v_PolicyCancellationDate,'MM')
	DATE_PART(v_PolicyCancellationDate, 'MM') AS o_pol_cancellation_month,
	-- *INF*: TO_CHAR(v_PolicyCancellationDate,'MONTH')
	TO_CHAR(v_PolicyCancellationDate, 'MONTH') AS o_pol_cancellation_month_descript,
	-- *INF*: IIF(pol_issue_code='N','Y','N')
	-- 
	-- -- N = new (Y)es, R = renewal (N)o
	-- 
	IFF(pol_issue_code = 'N', 'Y', 'N') AS o_new_bus_indic,
	-- *INF*: IIF(ISNULL(v_StateCode_Desc),'N/A',
	-- SUBSTR(v_StateCode_Desc,1,(INSTR(v_StateCode_Desc,'|')-1)))
	-- 
	-- 
	IFF(
	    v_StateCode_Desc IS NULL, 'N/A',
	    SUBSTR(v_StateCode_Desc, 1, (REGEXP_INSTR(v_StateCode_Desc, '|') - 1))
	) AS o_AgencyStateCode,
	-- *INF*: IIF(ISNULL(v_StateCode_Desc),'N/A',
	-- SUBSTR(v_StateCode_Desc,
	-- (INSTR(v_StateCode_Desc,'|')+1),LENGTH(v_StateCode_Desc)))
	IFF(
	    v_StateCode_Desc IS NULL, 'N/A',
	    SUBSTR(v_StateCode_Desc, (REGEXP_INSTR(v_StateCode_Desc, '|') + 1), LENGTH(v_StateCode_Desc))
	) AS o_AgencyStateDescription,
	-- *INF*: IIF(ISNULL(v_StateCode_Desc_prim),'N/A',
	-- SUBSTR(v_StateCode_Desc_prim,1,(INSTR(v_StateCode_Desc_prim,'|')-1)))
	-- 
	-- 
	IFF(
	    v_StateCode_Desc_prim IS NULL, 'N/A',
	    SUBSTR(v_StateCode_Desc_prim, 1, (REGEXP_INSTR(v_StateCode_Desc_prim, '|') - 1))
	) AS o_PrimaryAgencyStateCode,
	-- *INF*: IIF(ISNULL(v_StateCode_Desc_prim),'N/A',
	-- SUBSTR(v_StateCode_Desc_prim,
	-- (INSTR(v_StateCode_Desc_prim,'|')+1),LENGTH(v_StateCode_Desc_prim)))
	IFF(
	    v_StateCode_Desc_prim IS NULL, 'N/A',
	    SUBSTR(v_StateCode_Desc_prim, (REGEXP_INSTR(v_StateCode_Desc_prim, '|') + 1), LENGTH(v_StateCode_Desc_prim))
	) AS o_PrimaryAgencyStateDescription,
	-- *INF*: IIF(ISNULL(i_prior_pol_eff_date),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),i_prior_pol_eff_date)
	IFF(
	    i_prior_pol_eff_date IS NULL, TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    i_prior_pol_eff_date
	) AS o_prior_pol_eff_date,
	-- *INF*: IIF(ISNULL(pol_eff_date),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),pol_eff_date)
	IFF(
	    pol_eff_date IS NULL, TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    pol_eff_date
	) AS o_pol_eff_date,
	-- *INF*: IIF(ISNULL(pol_exp_date),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),pol_exp_date)
	IFF(
	    pol_exp_date IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    pol_exp_date
	) AS o_pol_exp_date,
	-- *INF*: IIF(ISNULL(orig_incptn_date),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),orig_incptn_date)
	IFF(
	    orig_incptn_date IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    orig_incptn_date
	) AS o_orig_incptn_date
	FROM EXP_Pass_Values
	LEFT JOIN LKP_AgencyRelationship
	ON LKP_AgencyRelationship.EDWAgencyAKID = EXP_Pass_Values.EDWAgencyAKID
	LEFT JOIN LKP_BusinessClassDim
	ON LKP_BusinessClassDim.BusinessClassDimId = EXP_Pass_Values.BusinessClassDimId
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.EDWPolicyAKId = EXP_Pass_Values.edw_pol_ak_id
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.SalesDivisionDimID = EXP_Pass_Values.SalesDivisionDimId
	LEFT JOIN LKP_StateDim
	ON LKP_StateDim.StateDimID = EXP_Pass_Values.StateDimId
	LEFT JOIN LKP_UnderwritingDivisionDim
	ON LKP_UnderwritingDivisionDim.UnderwritingDivisionDimID = EXP_Pass_Values.UnderwritingDivisionDimId
	LEFT JOIN LKP_PHYSICALSTATE LKP_PHYSICALSTATE_PhysicalStateAbbreviation
	ON LKP_PHYSICALSTATE_PhysicalStateAbbreviation.StateAbbreviation = PhysicalStateAbbreviation

	LEFT JOIN LKP_PHYSICALSTATE LKP_PHYSICALSTATE_i_prim_agency_state_abbr
	ON LKP_PHYSICALSTATE_i_prim_agency_state_abbr.StateAbbreviation = i_prim_agency_state_abbr

),
ActuarialWorkersCompensationDividend AS (
	TRUNCATE TABLE ActuarialWorkersCompensationDividend;
	INSERT INTO ActuarialWorkersCompensationDividend
	(AuditId, DividendFactID, CreatedDate, ModifiedDate, EnterpriseGroupCode, EnterpriseGroupAbbreviation, StrategicProfitCenterCode, StrategicProfitCenterAbbreviation, LegalEntityCode, LegalEntityAbbreviation, PolicyOfferingCode, PolicyOfferingAbbreviation, PolicyOfferingDescription, ProductCode, ProductAbbreviation, ProductDescription, LineofBusinessCode, LineofBusinessAbbreviation, LineofBusinessDescription, AccountingProductCode, AccountingProductAbbreviation, AccountingProductDescription, RatingPlanCode, RatingPlanDescription, InsuranceSegmentCode, InsuranceSegmentDescription, ProgramCode, ProgramDescription, AssociationCode, AssociationDescription, IndustryRiskGradeCode, IndustryRiskGradeDescription, PolicyEffectiveYear, PolicyEffectiveQuarter, PolicyEffectiveMonthNumber, PolicyEffectiveMonthDescription, PolicyEffectiveDate, PolicyExpirationYear, PolicyExpirationQuarter, PolicyExpirationMonthNumber, PolicyExpirationMonthDescription, PolicyExpirationDate, PolicyCancellationYear, PolicyCancellationQuarter, PolicyCancellationMonth, PolicyCancellationMonthDescription, PolicyCancellationDate, PolicyCancellationReasonCode, PolicyCancellationReasonCodeDescription, PolicyOriginalInceptionDate, PolicyRenewalCode, PolicyRenewalDescription, PolicyStatusCode, PolicyStatusCodeDescription, AccountingYear, AccountingMonthQuarter, AccountingMonthNumber, AccountingMonthName, AccountingDate, RatingStateCode, RatingStateAbbreviation, RatingStateName, PolicyKey, PolicySymbol, PolicyNumber, PolicyVersion, PolicyIssueCode, PolicyIssueCodeDescription, PrimaryRatingStateCode, PrimaryRatingStateAbbreviation, PrimaryRatingStateDescription, PrimaryBusinessClassificationCode, PrimaryBusinessClassificationDescription, BusinessSegmentCode, BusinessSegmentDescription, StrategicBusinessGroupCode, StrategicBusinessGroupDescription, AgencyCode, AgencyDoingBusinessAsName, AgencyStateCode, AgencyStateAbbreviation, AgencyStateDescription, AgencyPhysicalAddressCity, AgencyZIPCode, ProducerCode, ProducerFullName, UnderwriterFullName, UnderwritingManagerName, UnderwritingRegionName, PrimaryAgencyCode, PrimaryAgencyDoingBusinessAsName, PrimaryAgencyStateCode, PrimaryAgencyStateAbbreviation, PrimaryAgencyStateDescription, PrimaryAgencyPhysicalAddressCity, PrimaryAgencyZIPCode, RegionalSalesManagerFullName, SalesTerritoryCode, CustomerNumber, FirstNamedInsured, SICCode, SICDescription, NAICSCode, NAICSDescription, CustomerCareIndicator, NewBusinessIndicator, PriorPolicyKey, PriorPolicyNumber, PriorPolicySymbol, PriorPolicyVersion, LastBookedDate, DividendPlan, DividendType, PolicyholderDividendPaid, PolicyholderDividendPayable, FederalEmployerIDNumber, PriorCarrier, PriorPolicyEffectiveDate)
	SELECT 
	AUDITID, 
	DividendFactId AS DIVIDENDFACTID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	ENTERPRISEGROUPCODE, 
	ENTERPRISEGROUPABBREVIATION, 
	STRATEGICPROFITCENTERCODE, 
	STRATEGICPROFITCENTERABBREVIATION, 
	InsuranceReferenceLegalEntityCode AS LEGALENTITYCODE, 
	InsuranceReferenceLegalEntityAbbreviation AS LEGALENTITYABBREVIATION, 
	POLICYOFFERINGCODE, 
	POLICYOFFERINGABBREVIATION, 
	POLICYOFFERINGDESCRIPTION, 
	PRODUCTCODE, 
	PRODUCTABBREVIATION, 
	PRODUCTDESCRIPTION, 
	InsuranceReferenceLineOfBusinessCode AS LINEOFBUSINESSCODE, 
	InsuranceReferenceLineOfBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	InsuranceReferenceLineOfBusinessDescription AS LINEOFBUSINESSDESCRIPTION, 
	ACCOUNTINGPRODUCTCODE, 
	ACCOUNTINGPRODUCTABBREVIATION, 
	ACCOUNTINGPRODUCTDESCRIPTION, 
	RATINGPLANCODE, 
	RATINGPLANDESCRIPTION, 
	INSURANCESEGMENTCODE, 
	INSURANCESEGMENTDESCRIPTION, 
	PROGRAMCODE, 
	PROGRAMDESCRIPTION, 
	ASSOCIATIONCODE, 
	ASSOCIATIONDESCRIPTION, 
	industry_risk_grade_code AS INDUSTRYRISKGRADECODE, 
	industry_risk_grade_code_descript AS INDUSTRYRISKGRADEDESCRIPTION, 
	o_pol_eff_yr AS POLICYEFFECTIVEYEAR, 
	o_pol_eff_qtr AS POLICYEFFECTIVEQUARTER, 
	o_pol_eff_month AS POLICYEFFECTIVEMONTHNUMBER, 
	o_pol_eff_month_descript AS POLICYEFFECTIVEMONTHDESCRIPTION, 
	o_pol_eff_date AS POLICYEFFECTIVEDATE, 
	o_pol_exp_yr AS POLICYEXPIRATIONYEAR, 
	o_pol_exp_qtr AS POLICYEXPIRATIONQUARTER, 
	o_pol_exp_month AS POLICYEXPIRATIONMONTHNUMBER, 
	o_pol_exp_month_descript AS POLICYEXPIRATIONMONTHDESCRIPTION, 
	o_pol_exp_date AS POLICYEXPIRATIONDATE, 
	o_pol_cancellation_yr AS POLICYCANCELLATIONYEAR, 
	o_pol_cancellation_qtr AS POLICYCANCELLATIONQUARTER, 
	o_pol_cancellation_month AS POLICYCANCELLATIONMONTH, 
	o_pol_cancellation_month_descript AS POLICYCANCELLATIONMONTHDESCRIPTION, 
	o_PolicyCancellationDate AS POLICYCANCELLATIONDATE, 
	pol_cancellation_rsn_code AS POLICYCANCELLATIONREASONCODE, 
	pol_cancellation_rsn_code_descript AS POLICYCANCELLATIONREASONCODEDESCRIPTION, 
	o_orig_incptn_date AS POLICYORIGINALINCEPTIONDATE, 
	renl_code AS POLICYRENEWALCODE, 
	renl_code_descript AS POLICYRENEWALDESCRIPTION, 
	o_pol_status_code AS POLICYSTATUSCODE, 
	o_pol_status_code_descript AS POLICYSTATUSCODEDESCRIPTION, 
	o_accounting_yr AS ACCOUNTINGYEAR, 
	o_accounting_qtr AS ACCOUNTINGMONTHQUARTER, 
	o_accounting_month AS ACCOUNTINGMONTHNUMBER, 
	o_accounting_month_descript AS ACCOUNTINGMONTHNAME, 
	o_accounting_date AS ACCOUNTINGDATE, 
	o_StateCode AS RATINGSTATECODE, 
	o_StateAbbreviation AS RATINGSTATEABBREVIATION, 
	o_StateDescription AS RATINGSTATENAME, 
	pol_key AS POLICYKEY, 
	pol_sym AS POLICYSYMBOL, 
	pol_num AS POLICYNUMBER, 
	pol_mod AS POLICYVERSION, 
	pol_issue_code AS POLICYISSUECODE, 
	pol_issue_code_descript AS POLICYISSUECODEDESCRIPTION, 
	state_of_domicile_code AS PRIMARYRATINGSTATECODE, 
	state_of_domicile_abbrev AS PRIMARYRATINGSTATEABBREVIATION, 
	state_of_domicile_code_descript AS PRIMARYRATINGSTATEDESCRIPTION, 
	prim_bus_class_code AS PRIMARYBUSINESSCLASSIFICATIONCODE, 
	prim_bus_class_code_descript AS PRIMARYBUSINESSCLASSIFICATIONDESCRIPTION, 
	o_BusinessSegmentCode AS BUSINESSSEGMENTCODE, 
	o_BusinessSegmentDescription AS BUSINESSSEGMENTDESCRIPTION, 
	o_StrategicBusinessGroupCode AS STRATEGICBUSINESSGROUPCODE, 
	o_StrategicBusinessGroupDescription AS STRATEGICBUSINESSGROUPDESCRIPTION, 
	AGENCYCODE, 
	AGENCYDOINGBUSINESSASNAME, 
	o_AgencyStateCode AS AGENCYSTATECODE, 
	PhysicalStateAbbreviation AS AGENCYSTATEABBREVIATION, 
	o_AgencyStateDescription AS AGENCYSTATEDESCRIPTION, 
	PhysicalCity AS AGENCYPHYSICALADDRESSCITY, 
	PhysicalZipCode AS AGENCYZIPCODE, 
	PRODUCERCODE, 
	Producer_FullName AS PRODUCERFULLNAME, 
	o_UnderwriterDisplayName AS UNDERWRITERFULLNAME, 
	o_UnderwriterManagerDisplayName AS UNDERWRITINGMANAGERNAME, 
	o_UnderwritingRegionCodeDescription AS UNDERWRITINGREGIONNAME, 
	o_prim_agency_code AS PRIMARYAGENCYCODE, 
	o_prim_agency_doing_business_Name AS PRIMARYAGENCYDOINGBUSINESSASNAME, 
	o_PrimaryAgencyStateCode AS PRIMARYAGENCYSTATECODE, 
	o_prim_agency_state_abbr AS PRIMARYAGENCYSTATEABBREVIATION, 
	o_PrimaryAgencyStateDescription AS PRIMARYAGENCYSTATEDESCRIPTION, 
	o_prim_agency_city AS PRIMARYAGENCYPHYSICALADDRESSCITY, 
	o_prim_agency_zip_code AS PRIMARYAGENCYZIPCODE, 
	o_RegionalSalesManagerDisplayName AS REGIONALSALESMANAGERFULLNAME, 
	o_SalesTerritoryCode AS SALESTERRITORYCODE, 
	cust_num AS CUSTOMERNUMBER, 
	name AS FIRSTNAMEDINSURED, 
	sic_code AS SICCODE, 
	sic_code_descript AS SICDESCRIPTION, 
	naics_code AS NAICSCODE, 
	naics_code_descript AS NAICSDESCRIPTION, 
	serv_center_support_code AS CUSTOMERCAREINDICATOR, 
	o_new_bus_indic AS NEWBUSINESSINDICATOR, 
	prior_pol_key AS PRIORPOLICYKEY, 
	o_prior_pol_num AS PRIORPOLICYNUMBER, 
	o_prior_pol_sym AS PRIORPOLICYSYMBOL, 
	o_prior_pol_mod AS PRIORPOLICYVERSION, 
	o_Last_Booked_Date AS LASTBOOKEDDATE, 
	DIVIDENDPLAN, 
	DIVIDENDTYPE, 
	DividendPaidAmount AS POLICYHOLDERDIVIDENDPAID, 
	DividendPayableAmount AS POLICYHOLDERDIVIDENDPAYABLE, 
	fed_tax_id AS FEDERALEMPLOYERIDNUMBER, 
	RolloverPriorCarrier AS PRIORCARRIER, 
	o_prior_pol_eff_date AS PRIORPOLICYEFFECTIVEDATE
	FROM EXP_Get_Values
),