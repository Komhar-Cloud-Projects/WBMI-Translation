WITH
LKP_sup_state AS (
	SELECT
	state_code,
	state_abbrev
	FROM (
		SELECT sup_state.state_code as state_code, sup_state.state_abbrev as state_abbrev FROM sup_state
		where crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY state_code) = 1
),
LKP_Agency AS (
	SELECT
	AgencyCode,
	AgencyAKID
	FROM (
		SELECT v2.Agency.AgencyCode as AgencyCode, v2.Agency.AgencyAKID as AgencyAKID FROM v2.Agency
		where SourceSystemID='AgencyODS' and CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY AgencyCode) = 1
),
SQ_Equipment_PremiumAmount AS (
	SELECT p.pol_ak_id,pc.InsuranceLine,pd.ProductAKId,pt.PremiumTransactionEnteredDate,pt.PremiumTransactionEffectiveDate,
	case when pt.OffsetOnsetCode<>'Offset' then pt.EffectiveDate else pt.PremiumTransactionEnteredDate end as OriginalTransactionEnteredDate, 
	sum(pt.PremiumTransactionAmount) EquipmentBreakdownPremium 
	FROM v2.policy p
	INNER JOIN  StrategicProfitCenter spc
	on spc.StrategicProfitCenterAKId=p.StrategicProfitCenterAKId and spc.CurrentSnapshotFlag=1 and p.crrnt_snpsht_flag=1 and p.source_sys_id='DCT'
	and spc.StrategicProfitCenterDescription =@{pipeline().parameters.STRATEGIC_PROFITCENTER_DESCRIPTION}
	INNER JOIN  InsuranceSegment i
	on i.InsuranceSegmentAKId=p.InsuranceSegmentAKId and i.CurrentSnapshotFlag=1 and i.InsuranceSegmentAbbreviation='CL'
	INNER JOIN  PolicyCoverage pc on pc.PolicyAKID=p.pol_ak_id and pc.CurrentSnapshotFlag=1 and pc.SourceSystemID='DCT'
	and pc.InsuranceLine =@{pipeline().parameters.INSURANCE_LINE}
	INNER JOIN  RatingCoverage rc on pc.PolicyCoverageAKID=rc.PolicyCoverageAKID and rc.CoverageType in ('EquipmentBreakdown','EquipBreakdown')
	INNER JOIN  Product pd on rc.ProductAKId=pd.ProductAKId and pd.CurrentSnapshotFlag=1 
	and pd.ProductDescription = @{pipeline().parameters.PRODUCT_DESCRIPTION}
	INNER JOIN  InsuranceReferenceLineOfBusiness lob on lob.InsuranceReferenceLineOfBusinessAKId=rc.InsuranceReferenceLineOfBusinessAKId
	and lob.CurrentSnapshotFlag=1 and lob.InsuranceReferenceLineOfBusinessDescription='Commercial Boiler & Machinery'
	INNER JOIN  PremiumTransaction pt on pt.RatingCoverageAKId=rc.RatingCoverageAKId and pt.EffectiveDate=rc.EffectiveDate
	and pt.SourceSystemID='DCT'
	and not pt.PremiumTransactionCode like '%Audit%'
	and not pt.ReasonAmendedCode in ('CWO','Claw Back')
	group by p.pol_ak_id,pc.InsuranceLine,pd.ProductAKId,pt.PremiumTransactionEnteredDate, pt.PremiumTransactionEffectiveDate, case when pt.OffsetOnsetCode<>'Offset' then pt.EffectiveDate else pt.PremiumTransactionEnteredDate end
	----
),
SQ_WorkHsbEquipmentBreakdown AS (
	DECLARE @date DATETIME
	set @date = cast(DATEADD(dd, 1, EOMONTH(GETDATE(),-2)) as datetime)
	
	SELECT p.pol_ak_id as PolicyAKId
	,p.AgencyAKid as AgencyAKId
	,right(r.RiskLocationKey,33) as LocationXMLId
	,pt.PremiumTransactionEnteredDate as TransactionEnteredDate
	,pt.PremiumTransactionEffectiveDate as TransactionEffectiveDate
	,pt.PremiumTransactionBookedDate as TransactionBookedDate
	,p.pol_num as PolicyNum
	,p.prior_pol_key as PriorPolicyKey
	,pt.PremiumTransactionCode as TransactionCode
	,pt.OffsetOnsetCode as OffsetOnsetCode
	,cc.name as NameOfInsured
	,ca.addr_line_1 as MailingStreetAddress1
	,ca.addr_line_2 as MailingStreetAddress2
	,ca.addr_line_3 as MailingStreetAddress3
	,ca.city_name as MailingCity
	,ca.state_prov_code as MailingStateProvinceAbbreviation
	,ca.zip_postal_code as MailingZipCode
	,ca.zip_postal_code_extension as MailingZipExtension
	,p.pol_eff_date as PolicyEffectiveDate
	,p.pol_exp_date as PolicyExpirationDate
	,0 as EquipmentBreakdownPremium
	,spc.StrategicProfitCenterAbbreviation+'-'+po.PolicyOfferingAbbreviation as ProgramId
	,r.StreetAddress as RiskStreetAddress
	,r.RatingCity as RiskCity
	,r.StateProvinceCode as RiskStateProvinceAbbreviation
	,r.ZipPostalCode as RiskZipCode
	,rc.OccupancyClassDescription as Occupancy
	,pc.InsuranceLine as InsuranceLine
	,rc.RiskType as RiskType
	,rc.CoverageType as CoverageType
	,rc.PerilGroup as PerilGroup
	,rc.SubCoverageTypecode as SubCoverageTypecode
	,rc.CoverageVersion as CoverageVersion 
	,rc.RatingCoverageCancellationDate as RatingCoverageCancellationDate 
	,case when pt.OffsetOnsetCode<>'Offset' then pt.EffectiveDate else pt.PremiumTransactionEnteredDate end as OriginalTransactionEnteredDate,
	pd.ProductAKId,
	rc.ActiveBuildingFlag,
	rc.SubLocationUnitNumber,
	len(rc.OccupancyClassDescription) as OccupancyClassDescription,
	len(p.prim_bus_class_code) as prim_bus_class_code,
	RIGHT(p.prim_bus_class_code,4) as BCCCode,
	po.PolicyOfferingAbbreviation,
	rc.ClassCode, A.LegalName,A.PrimaryEmailAddress, A.PrimaryPhoneNumber
	FROM v2.policy p
	INNER JOIN StrategicProfitCenter spc on spc.StrategicProfitCenterAKId=p.StrategicProfitCenterAKId and spc.CurrentSnapshotFlag=1 and p.source_sys_id='DCT'
	and spc.StrategicProfitCenterDescription=@{pipeline().parameters.STRATEGIC_PROFITCENTER_DESCRIPTION}
	INNER JOIN InsuranceSegment i on i.InsuranceSegmentAKId=p.InsuranceSegmentAKId and i.CurrentSnapshotFlag=1 and i.InsuranceSegmentAbbreviation='CL'
	INNER JOIN PolicyOffering po on po.PolicyOfferingAKId=p.PolicyOfferingAKId and po.CurrentSnapshotFlag=1
	INNER JOIN VWContractCustomer cc on p.contract_cust_ak_id=cc.contract_cust_ak_id and cc.source_sys_id='DCT'
	INNER JOIN contract_customer_address ca on ca.contract_cust_ak_id=cc.contract_cust_ak_id and ca.addr_type='MAILING' and ca.source_sys_id='DCT'
	INNER JOIN RiskLocation r on p.pol_ak_id=r.PolicyAKID and r.CurrentSnapshotFlag=1 and r.SourceSystemID='DCT'
	INNER JOIN PolicyCoverage pc on pc.RiskLocationAKID=r.RiskLocationAKID and pc.CurrentSnapshotFlag=1 and pc.SourceSystemID='DCT'
	and pc.InsuranceLine=@{pipeline().parameters.INSURANCE_LINE}
	INNER JOIN RatingCoverage rc on pc.PolicyCoverageAKID=rc.PolicyCoverageAKID and rc.CoverageType NOT in ('LiabilityOnly','MoneyAndSecurities')
	INNER JOIN Product pd on rc.ProductAKId=pd.ProductAKId and pd.CurrentSnapshotFlag=1 and pd.ProductDescription =@{pipeline().parameters.PRODUCT_DESCRIPTION}
	INNER JOIN PremiumTransaction pt on pt.RatingCoverageAKId=rc.RatingCoverageAKId and pt.EffectiveDate=rc.EffectiveDate and pt.SourceSystemID='DCT'
	inner join V2.agency A on P.AgencyAKId = A.AgencyAKID and A.CurrentSnapshotFlag = 1
	and not pt.PremiumTransactionCode like '%Audit%'
	and not pt.ReasonAmendedCode in ('CWO','Claw Back')
	and pt.PremiumTransactionEnteredDate between p.eff_from_date and p.eff_to_date
	and pt.PremiumTransactionEnteredDate between cc.eff_from_date and cc.eff_to_date
	and pt.PremiumTransactionEnteredDate between ca.eff_from_date and ca.eff_to_date
	and pt.PremiumTransactionBookedDate >= @Date
	and exists (
	SELECT 1 FROM  rpt_edm..WorkPremiumTransactionHsbEquipBreakdown a
	WHERE  a.PremiumTransactionAKId = pt.PremiumTransactionAKID)
),
JNR_EquipBreakdown AS (SELECT
	SQ_WorkHsbEquipmentBreakdown.PolicyAKId, 
	SQ_WorkHsbEquipmentBreakdown.AgencyAKId, 
	SQ_WorkHsbEquipmentBreakdown.LocationXMLId, 
	SQ_WorkHsbEquipmentBreakdown.TransactionEnteredDate, 
	SQ_WorkHsbEquipmentBreakdown.TransactionEffectiveDate, 
	SQ_WorkHsbEquipmentBreakdown.TransactionBookedDate, 
	SQ_WorkHsbEquipmentBreakdown.PolicyNum, 
	SQ_WorkHsbEquipmentBreakdown.PriorPolicyKey, 
	SQ_WorkHsbEquipmentBreakdown.TransactionCode, 
	SQ_WorkHsbEquipmentBreakdown.OffsetOnsetCode, 
	SQ_WorkHsbEquipmentBreakdown.NameOfInsured, 
	SQ_WorkHsbEquipmentBreakdown.MailingStreetAddress1, 
	SQ_WorkHsbEquipmentBreakdown.MailingStreetAddress2, 
	SQ_WorkHsbEquipmentBreakdown.MailingStreetAddress3, 
	SQ_WorkHsbEquipmentBreakdown.MailingCity, 
	SQ_WorkHsbEquipmentBreakdown.MailingStateProvinceAbbreviation, 
	SQ_WorkHsbEquipmentBreakdown.MailingZipCode, 
	SQ_WorkHsbEquipmentBreakdown.zip_postal_code_extension, 
	SQ_WorkHsbEquipmentBreakdown.PolicyEffectiveDate, 
	SQ_WorkHsbEquipmentBreakdown.PolicyExpirationDate, 
	SQ_WorkHsbEquipmentBreakdown.EquipmentBreakdownPremium, 
	SQ_WorkHsbEquipmentBreakdown.ProgramId, 
	SQ_WorkHsbEquipmentBreakdown.RiskStreetAddress, 
	SQ_WorkHsbEquipmentBreakdown.RiskCity, 
	SQ_WorkHsbEquipmentBreakdown.RiskStateProvinceAbbreviation, 
	SQ_WorkHsbEquipmentBreakdown.RiskZipCode, 
	SQ_WorkHsbEquipmentBreakdown.Occupancy, 
	SQ_WorkHsbEquipmentBreakdown.InsuranceLine, 
	SQ_WorkHsbEquipmentBreakdown.RiskType, 
	SQ_WorkHsbEquipmentBreakdown.CoverageType, 
	SQ_WorkHsbEquipmentBreakdown.PerilGroup, 
	SQ_WorkHsbEquipmentBreakdown.SubCoverageTypecode, 
	SQ_WorkHsbEquipmentBreakdown.CoverageVersion, 
	SQ_WorkHsbEquipmentBreakdown.RatingCoverageCancellationDate, 
	SQ_WorkHsbEquipmentBreakdown.OriginalTransactionEnteredDate, 
	SQ_WorkHsbEquipmentBreakdown.ProductAKId, 
	SQ_WorkHsbEquipmentBreakdown.ActiveBuildingFlag, 
	SQ_WorkHsbEquipmentBreakdown.SubLocationUnitNumber, 
	SQ_WorkHsbEquipmentBreakdown.OccupancyClassDescription, 
	SQ_WorkHsbEquipmentBreakdown.prim_bus_class_code, 
	SQ_WorkHsbEquipmentBreakdown.BCCCode, 
	SQ_WorkHsbEquipmentBreakdown.PolicyOfferingAbbreviation, 
	SQ_WorkHsbEquipmentBreakdown.ClassCode, 
	SQ_Equipment_PremiumAmount.Pol_Ak_Id AS Equip_Pol_Ak_Id, 
	SQ_Equipment_PremiumAmount.InsuranceLine AS Equip_InsuranceLine, 
	SQ_Equipment_PremiumAmount.ProductAKId AS Equip_ProductAKId, 
	SQ_Equipment_PremiumAmount.PremiumTransactionEnteredDate AS Equip_PremiumTransactionEnteredDate, 
	SQ_Equipment_PremiumAmount.PremiumTransactionEffectiveDate AS Equip_PremiumTransactionEffectiveDate, 
	SQ_Equipment_PremiumAmount.OriginalTransactionEnteredDate AS Equip_OriginalTransactionEnteredDate, 
	SQ_Equipment_PremiumAmount.EquipmentBreakdownPremium AS Equip_EquipmentBreakdownPremium, 
	SQ_WorkHsbEquipmentBreakdown.LegalName, 
	SQ_WorkHsbEquipmentBreakdown.PrimaryPhoneNumber, 
	SQ_WorkHsbEquipmentBreakdown.PrimaryEmailAddress
	FROM SQ_WorkHsbEquipmentBreakdown
	INNER JOIN SQ_Equipment_PremiumAmount
	ON SQ_Equipment_PremiumAmount.Pol_Ak_Id = SQ_WorkHsbEquipmentBreakdown.PolicyAKId AND SQ_Equipment_PremiumAmount.InsuranceLine = SQ_WorkHsbEquipmentBreakdown.InsuranceLine AND SQ_Equipment_PremiumAmount.ProductAKId = SQ_WorkHsbEquipmentBreakdown.ProductAKId AND SQ_Equipment_PremiumAmount.PremiumTransactionEnteredDate = SQ_WorkHsbEquipmentBreakdown.TransactionEnteredDate AND SQ_Equipment_PremiumAmount.PremiumTransactionEffectiveDate = SQ_WorkHsbEquipmentBreakdown.TransactionEffectiveDate AND SQ_Equipment_PremiumAmount.OriginalTransactionEnteredDate = SQ_WorkHsbEquipmentBreakdown.OriginalTransactionEnteredDate
),
SRT_HSB_Equipment AS (
	SELECT
	PolicyAKId, 
	InsuranceLine, 
	AgencyAKId, 
	LocationXMLId, 
	TransactionEffectiveDate, 
	TransactionEnteredDate, 
	OriginalTransactionEnteredDate, 
	TransactionBookedDate, 
	PolicyNum, 
	PriorPolicyKey, 
	TransactionCode, 
	OffsetOnsetCode, 
	NameOfInsured, 
	MailingStreetAddress1, 
	MailingStreetAddress2, 
	MailingStreetAddress3, 
	MailingCity, 
	MailingStateProvinceAbbreviation, 
	MailingZipCode, 
	zip_postal_code_extension, 
	PolicyEffectiveDate, 
	PolicyExpirationDate, 
	Equip_EquipmentBreakdownPremium AS EquipmentBreakdownPremium, 
	ProgramId, 
	RiskStreetAddress, 
	RiskCity, 
	RiskStateProvinceAbbreviation, 
	RiskZipCode, 
	Occupancy, 
	RiskType, 
	CoverageType, 
	PerilGroup, 
	SubCoverageTypecode, 
	CoverageVersion, 
	RatingCoverageCancellationDate, 
	ProductAKId, 
	ActiveBuildingFlag, 
	SubLocationUnitNumber, 
	OccupancyClassDescription, 
	prim_bus_class_code, 
	BCCCode, 
	PolicyOfferingAbbreviation, 
	ClassCode, 
	LegalName, 
	PrimaryPhoneNumber, 
	PrimaryEmailAddress
	FROM JNR_EquipBreakdown
	ORDER BY PolicyAKId ASC, InsuranceLine ASC, LocationXMLId ASC, TransactionEffectiveDate ASC, TransactionEnteredDate ASC, OriginalTransactionEnteredDate ASC, OffsetOnsetCode ASC, ActiveBuildingFlag ASC, SubLocationUnitNumber DESC, OccupancyClassDescription ASC, prim_bus_class_code ASC
),
EXPTRANS AS (
	SELECT
	PolicyAKId,
	AgencyAKId,
	LocationXMLId,
	TransactionEnteredDate,
	TransactionEffectiveDate,
	TransactionBookedDate,
	PolicyNum,
	PriorPolicyKey,
	TransactionCode,
	OffsetOnsetCode,
	NameOfInsured,
	MailingStreetAddress1,
	MailingStreetAddress2,
	MailingStreetAddress3,
	MailingCity,
	MailingStateProvinceAbbreviation,
	MailingZipCode,
	zip_postal_code_extension,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	EquipmentBreakdownPremium,
	ProgramId,
	RiskStreetAddress,
	RiskCity,
	RiskStateProvinceAbbreviation,
	RiskZipCode,
	Occupancy,
	InsuranceLine,
	RiskType,
	CoverageType,
	PerilGroup,
	SubCoverageTypecode,
	CoverageVersion,
	RatingCoverageCancellationDate,
	OriginalTransactionEnteredDate,
	BCCCode,
	PolicyOfferingAbbreviation,
	ClassCode,
	LegalName,
	PrimaryPhoneNumber,
	PrimaryEmailAddress
	FROM SRT_HSB_Equipment
),
SRC_Anchor AS (
	SELECT
	PolicyAKId,
	AgencyAKId,
	LocationXMLId,
	TransactionEnteredDate,
	TransactionEffectiveDate,
	TransactionBookedDate,
	PolicyNum AS PolicyKey,
	PriorPolicyKey,
	TransactionCode,
	OffsetOnsetCode,
	NameOfInsured,
	MailingStreetAddress1,
	MailingStreetAddress2,
	MailingStreetAddress3,
	-- *INF*: CONCAT(CONCAT(IIF(MailingStreetAddress1<>'N/A', MailingStreetAddress1, ''),IIF(MailingStreetAddress2<>'N/A', MailingStreetAddress2,'')), IIF(MailingStreetAddress3<>'N/A', MailingStreetAddress3, ''))
	CONCAT(CONCAT(
	        IFF(
	            MailingStreetAddress1 <> 'N/A', MailingStreetAddress1, ''
	        ), 
	        IFF(
	            MailingStreetAddress2 <> 'N/A', MailingStreetAddress2, ''
	        )), 
	    IFF(
	        MailingStreetAddress3 <> 'N/A', MailingStreetAddress3, ''
	    )) AS MailingStreetAddress,
	MailingCity,
	MailingStateProvinceAbbreviation,
	MailingZipCode,
	zip_postal_code_extension,
	-- *INF*: CONCAT(REG_REPLACE(MailingZipCode,'-',''),IIF(zip_postal_code_extension<>'N/A',zip_postal_code_extension,''))
	CONCAT(REGEXP_REPLACE(MailingZipCode, '-', ''), 
	    IFF(
	        zip_postal_code_extension <> 'N/A', zip_postal_code_extension, ''
	    )) AS o_MailingZipCode,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	EquipmentBreakdownPremium,
	ProgramId,
	RiskStreetAddress,
	RiskCity,
	RiskStateProvinceAbbreviation,
	-- *INF*: :LKP.LKP_sup_state(RiskStateProvinceAbbreviation)
	LKP_SUP_STATE_RiskStateProvinceAbbreviation.state_code AS RiskStateProvinceAbbreviation1,
	RiskZipCode,
	Occupancy,
	InsuranceLine,
	RatingCoverageCancellationDate,
	-- *INF*: IIF(OffsetOnsetCode='Offset',0,IIF(RatingCoverageCancellationDate>=TO_DATE('21001231','YYYYMMDD'),1,0))
	IFF(
	    OffsetOnsetCode = 'Offset', 0,
	    IFF(
	        RatingCoverageCancellationDate >= TO_TIMESTAMP('21001231', 'YYYYMMDD'), 1, 0
	    )
	) AS ActiveBuildingFlag,
	OriginalTransactionEnteredDate,
	BCCCode,
	PolicyOfferingAbbreviation,
	ClassCode,
	LegalName,
	PrimaryPhoneNumber,
	PrimaryEmailAddress
	FROM EXPTRANS
	LEFT JOIN LKP_SUP_STATE LKP_SUP_STATE_RiskStateProvinceAbbreviation
	ON LKP_SUP_STATE_RiskStateProvinceAbbreviation.state_abbrev = RiskStateProvinceAbbreviation

),
AGG_location_grain AS (
	SELECT
	PolicyAKId,
	AgencyAKId,
	InsuranceLine,
	LocationXMLId,
	TransactionEffectiveDate,
	TransactionEnteredDate,
	TransactionBookedDate,
	PolicyKey,
	PriorPolicyKey,
	TransactionCode,
	NameOfInsured,
	MailingStreetAddress,
	MailingCity,
	MailingStateProvinceAbbreviation,
	o_MailingZipCode AS MailingZipCode,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	EquipmentBreakdownPremium,
	ProgramId,
	RiskStreetAddress,
	RiskCity,
	RiskStateProvinceAbbreviation1 AS RiskStateProvinceAbbreviation,
	RiskZipCode,
	Occupancy,
	ActiveBuildingFlag AS i_ActiveBuildingFlag,
	-- *INF*: MAX(i_ActiveBuildingFlag)
	MAX(i_ActiveBuildingFlag) AS ActiveBuildingFlag,
	OriginalTransactionEnteredDate,
	BCCCode,
	PolicyOfferingAbbreviation,
	ClassCode,
	LegalName,
	PrimaryPhoneNumber,
	PrimaryEmailAddress
	FROM SRC_Anchor
	GROUP BY PolicyAKId, InsuranceLine, LocationXMLId, TransactionEffectiveDate, TransactionEnteredDate, OriginalTransactionEnteredDate
),
LKP_RatingLocationLimit AS (
	SELECT
	RatingLocationLimitValue,
	PolicyAKId,
	InsuranceLine,
	RatingLocationKey,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT a.RatingLocationLimitValue as RatingLocationLimitValue,
		a.PolicyAKId as PolicyAKId,
		a.InsuranceLine as InsuranceLine,
		a.RatingLocationKey as RatingLocationKey,
		a.EffectiveDate as EffectiveDate,
		a.ExpirationDate as ExpirationDate
		FROM RatingLocationLimit a
		where a.InsuranceLine in ('Property','SBOPProperty','BusinessOwners')
		and a.RatingLocationLimitType='EquipmentBreakdown'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,InsuranceLine,RatingLocationKey,EffectiveDate,ExpirationDate ORDER BY RatingLocationLimitValue) = 1
),
EXP_Calc AS (
	SELECT
	AGG_location_grain.PolicyAKId,
	AGG_location_grain.AgencyAKId,
	AGG_location_grain.InsuranceLine,
	AGG_location_grain.LocationXMLId,
	AGG_location_grain.TransactionEffectiveDate,
	AGG_location_grain.TransactionEnteredDate,
	AGG_location_grain.TransactionBookedDate,
	AGG_location_grain.PolicyKey,
	AGG_location_grain.PriorPolicyKey,
	-- *INF*: DECODE(TRUE,
	-- PriorPolicyKey='N/A','',
	-- LENGTH(PriorPolicyKey)<=9,SUBSTR(PriorPolicyKey,1,7),
	-- SUBSTR(PriorPolicyKey,-9,7))
	DECODE(
	    TRUE,
	    PriorPolicyKey = 'N/A', '',
	    LENGTH(PriorPolicyKey) <= 9, SUBSTR(PriorPolicyKey, 1, 7),
	    SUBSTR(PriorPolicyKey, - 9, 7)
	) AS o_PriorPolicyKey,
	AGG_location_grain.TransactionCode AS i_TransactionCode,
	-- *INF*: i_TransactionCode
	-- 
	-- --DECODE(i_TransactionCode,'New','01','Endorse','30','Cancel','03','Reinstate','10','Rewrite','04','Reissue','04','Renew','07','NonRenew','03')
	i_TransactionCode AS TransactionCode,
	AGG_location_grain.NameOfInsured AS i_NameOfInsured,
	-- *INF*: SUBSTR(i_NameOfInsured,1,55)
	SUBSTR(i_NameOfInsured, 1, 55) AS NameOfInsured,
	AGG_location_grain.MailingStreetAddress AS i_MailingStreetAddress,
	-- *INF*: SUBSTR(i_MailingStreetAddress,1,55)
	SUBSTR(i_MailingStreetAddress, 1, 55) AS MailingStreetAddress,
	AGG_location_grain.MailingCity AS i_MailingCity,
	-- *INF*: SUBSTR(i_MailingCity,1,20)
	SUBSTR(i_MailingCity, 1, 20) AS MailingCity,
	AGG_location_grain.MailingStateProvinceAbbreviation,
	AGG_location_grain.MailingZipCode,
	AGG_location_grain.PolicyEffectiveDate,
	AGG_location_grain.PolicyExpirationDate,
	LKP_RatingLocationLimit.RatingLocationLimitValue AS Value,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(Value),-1,
	-- IS_NUMBER(Value),TO_DECIMAL(Value),
	-- -1)
	DECODE(
	    TRUE,
	    Value IS NULL, - 1,
	    REGEXP_LIKE(Value, '^[0-9]+$'), CAST(Value AS FLOAT),
	    - 1
	) AS o_Value,
	AGG_location_grain.EquipmentBreakdownPremium,
	-- *INF*: round(EquipmentBreakdownPremium,0)*100
	round(EquipmentBreakdownPremium, 0) * 100 AS o_EquipmentBreakdownPremium,
	AGG_location_grain.ProgramId,
	AGG_location_grain.RiskStreetAddress AS i_RiskStreetAddress,
	-- *INF*: SUBSTR(i_RiskStreetAddress,1,55)
	SUBSTR(i_RiskStreetAddress, 1, 55) AS RiskStreetAddress,
	AGG_location_grain.RiskCity AS i_RiskCity,
	-- *INF*: SUBSTR(i_RiskCity,1,20)
	SUBSTR(i_RiskCity, 1, 20) AS RiskCity,
	AGG_location_grain.RiskStateProvinceAbbreviation,
	AGG_location_grain.RiskZipCode AS i_RiskZipCode,
	-- *INF*: SUBSTR(REG_REPLACE(i_RiskZipCode,'-',''),1,13)
	SUBSTR(REGEXP_REPLACE(i_RiskZipCode, '-', ''), 1, 13) AS RiskZipCode,
	AGG_location_grain.BCCCode,
	AGG_location_grain.PolicyOfferingAbbreviation,
	AGG_location_grain.ClassCode,
	AGG_location_grain.Occupancy AS i_Occupancy,
	-- *INF*: LTRIM(RTRIM(
	-- IIF(
	-- REG_MATCH(i_Occupancy,'(.*)\[([0-9]*)\](.*)'),REG_EXTRACT(i_Occupancy,'(.*)\[([0-9]*)\](.*)',2),
	-- 'N/A')
	-- ))
	LTRIM(RTRIM(
	        IFF(
	            REGEXP_LIKE(i_Occupancy, '(.*)\[([0-9]*)\](.*)'),
	            REG_EXTRACT(i_Occupancy, '(.*)\[([0-9]*)\](.*)', 2),
	            'N/A'
	        ))) AS v_Occupancy,
	-- *INF*: DECODE(TRUE,
	-- v_Occupancy='N/A' and PolicyOfferingAbbreviation = 'CPP', ClassCode,
	-- v_Occupancy='N/A' and PolicyOfferingAbbreviation = 'SMART', BCCCode,
	-- v_Occupancy)
	DECODE(
	    TRUE,
	    v_Occupancy = 'N/A' and PolicyOfferingAbbreviation = 'CPP', ClassCode,
	    v_Occupancy = 'N/A' and PolicyOfferingAbbreviation = 'SMART', BCCCode,
	    v_Occupancy
	) AS Occupancy,
	AGG_location_grain.ActiveBuildingFlag,
	AGG_location_grain.OriginalTransactionEnteredDate,
	AGG_location_grain.LegalName,
	AGG_location_grain.PrimaryPhoneNumber,
	AGG_location_grain.PrimaryEmailAddress
	FROM AGG_location_grain
	LEFT JOIN LKP_RatingLocationLimit
	ON LKP_RatingLocationLimit.PolicyAKId = AGG_location_grain.PolicyAKId AND LKP_RatingLocationLimit.InsuranceLine = AGG_location_grain.InsuranceLine AND LKP_RatingLocationLimit.RatingLocationKey = AGG_location_grain.LocationXMLId AND LKP_RatingLocationLimit.EffectiveDate <= AGG_location_grain.TransactionEnteredDate AND LKP_RatingLocationLimit.ExpirationDate >= AGG_location_grain.TransactionEnteredDate
),
EXP_RowChange_Logic AS (
	SELECT
	PolicyAKId,
	AgencyAKId,
	InsuranceLine,
	LocationXMLId,
	TransactionEnteredDate,
	TransactionEffectiveDate,
	TransactionBookedDate,
	PolicyKey,
	o_PriorPolicyKey AS PriorPolicyKey,
	TransactionCode,
	NameOfInsured,
	MailingStreetAddress,
	MailingCity,
	MailingStateProvinceAbbreviation,
	MailingZipCode,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	o_Value AS Value,
	o_EquipmentBreakdownPremium AS EquipmentBreakdownPremium,
	ProgramId,
	RiskStreetAddress,
	RiskCity,
	RiskStateProvinceAbbreviation,
	RiskZipCode,
	Occupancy,
	ActiveBuildingFlag,
	-- *INF*: IIF(PolicyAKId=v_prev_PolicyAKId AND InsuranceLine=v_prev_InsuranceLine AND LocationXMLId=v_prev_LocationXMLId,1,0)
	IFF(
	    PolicyAKId = v_prev_PolicyAKId
	    and InsuranceLine = v_prev_InsuranceLine
	    and LocationXMLId = v_prev_LocationXMLId,
	    1,
	    0
	) AS v_SameGroupFlag,
	-- *INF*: MD5(NameOfInsured||
	-- MailingStreetAddress||
	-- MailingCity||
	-- MailingStateProvinceAbbreviation||
	-- MailingZipCode||
	-- RiskStreetAddress||
	-- RiskCity||
	-- RiskStateProvinceAbbreviation||
	-- RiskZipCode||
	-- Occupancy)
	MD5(NameOfInsured || MailingStreetAddress || MailingCity || MailingStateProvinceAbbreviation || MailingZipCode || RiskStreetAddress || RiskCity || RiskStateProvinceAbbreviation || RiskZipCode || Occupancy) AS v_HashValue,
	-- *INF*: DECODE(TRUE,
	-- EquipmentBreakdownPremium!=0,1,
	-- v_SameGroupFlag=0,1,
	-- v_HashValue!=v_prev_HashValue,1,
	-- Value!=v_prev_Value,1,
	-- TransactionCode='03',1,
	-- 0)
	DECODE(
	    TRUE,
	    EquipmentBreakdownPremium != 0, 1,
	    v_SameGroupFlag = 0, 1,
	    v_HashValue != v_prev_HashValue, 1,
	    Value != v_prev_Value, 1,
	    TransactionCode = '03', 1,
	    0
	) AS v_FilterFlag,
	-- *INF*: TRUNC(ADD_TO_DATE(SYSDATE,'MM',-(TO_INTEGER(@{pipeline().parameters.NO_OF_MONTHS}))-1),'MM')
	CAST(TRUNC(DATEADD(MONTH,- (CAST(@{pipeline().parameters.NO_OF_MONTHS} AS INTEGER)) - 1,CURRENT_TIMESTAMP), 'MONTH') AS TIMESTAMP_NTZ(0)) AS v_RunDate,
	OriginalTransactionEnteredDate,
	-- *INF*: to_char(TransactionBookedDate,'YYYYMM')
	to_char(TransactionBookedDate, 'YYYYMM') AS o_RunDate,
	PolicyAKId AS v_prev_PolicyAKId,
	LocationXMLId AS v_prev_LocationXMLId,
	InsuranceLine AS v_prev_InsuranceLine,
	v_HashValue AS v_prev_HashValue,
	Value AS v_prev_Value,
	-- *INF*: DECODE(TRUE,
	-- v_FilterFlag!=1,0,
	-- TRUNC(TransactionBookedDate,'MM')>=v_RunDate AND TRUNC(TransactionBookedDate,'MM')<=GREATEST(ADD_TO_DATE(SYSDATE,'MM',@{pipeline().parameters.NO_OF_FUTUREMONTHS}),v_RunDate),1,
	-- 0)
	DECODE(
	    TRUE,
	    v_FilterFlag != 1, 0,
	    CAST(TRUNC(TransactionBookedDate, 'MONTH') AS TIMESTAMP_NTZ(0)) >= v_RunDate AND CAST(TRUNC(TransactionBookedDate, 'MONTH') AS TIMESTAMP_NTZ(0)) <= GREATEST(DATEADD(MONTH,@{pipeline().parameters.NO_OF_FUTUREMONTHS},CURRENT_TIMESTAMP), v_RunDate), 1,
	    0
	) AS o_FilterFlag,
	LegalName,
	PrimaryPhoneNumber,
	PrimaryEmailAddress
	FROM EXP_Calc
),
mplt_value_type AS (WITH
	INPUT AS (
		
	),
	LKPTRANS AS (
		SELECT
		CoverageGroupDescription,
		DctRiskTypeCode,
		DctCoverageTypeCode,
		DctPerilGroup,
		DctSubCoverageTypeCode,
		DctCoverageVersion
		FROM (
			SELECT 
				CoverageGroupDescription,
				DctRiskTypeCode,
				DctCoverageTypeCode,
				DctPerilGroup,
				DctSubCoverageTypeCode,
				DctCoverageVersion
			FROM InsuranceReferenceCoverageDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY DctRiskTypeCode,DctCoverageTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY CoverageGroupDescription) = 1
	),
	EXPTRANS AS (
		SELECT
		INPUT.PolicyAKId,
		INPUT.InsuranceLine,
		INPUT.LocationXMLId,
		INPUT.TransactionEffectiveDate,
		INPUT.TransactionEnteredDate,
		LKPTRANS.CoverageGroupDescription,
		-- *INF*: decode(CoverageGroupDescription,
		-- 'Building',1,
		-- 'Blanket Building',1,
		-- 'Contents',10,
		-- 'Blanket Contents',10,
		-- 'Blanket Building and Contents',11,0
		-- )
		decode(
		    CoverageGroupDescription,
		    'Building', 1,
		    'Blanket Building', 1,
		    'Contents', 10,
		    'Blanket Contents', 10,
		    'Blanket Building and Contents', 11,
		    0
		) AS v_CovGrpDesc
		FROM INPUT
		LEFT JOIN LKPTRANS
		ON LKPTRANS.DctRiskTypeCode = INPUT.Risktype AND LKPTRANS.DctCoverageTypeCode = INPUT.CoverageType AND LKPTRANS.DctPerilGroup = INPUT.PerilGroup AND LKPTRANS.DctSubCoverageTypeCode = INPUT.subcoveragetypecode AND LKPTRANS.DctCoverageVersion = INPUT.coverageversion
	),
	SRTTRANS AS (
		SELECT
		PolicyAKId, 
		InsuranceLine, 
		LocationXMLId, 
		TransactionEffectiveDate, 
		TransactionEnteredDate, 
		v_CovGrpDesc
		FROM EXPTRANS
		ORDER BY PolicyAKId ASC, InsuranceLine ASC, LocationXMLId ASC, TransactionEffectiveDate ASC, TransactionEnteredDate ASC, v_CovGrpDesc ASC
	),
	AGG_Value_Type AS (
		SELECT
		PolicyAKId,
		InsuranceLine,
		LocationXMLId,
		TransactionEffectiveDate,
		TransactionEnteredDate,
		v_CovGrpDesc AS i_CovGrpDesc,
		-- *INF*: sum(i_CovGrpDesc)
		sum(i_CovGrpDesc) AS CovGrpDesc
		FROM SRTTRANS
		GROUP BY PolicyAKId, InsuranceLine, LocationXMLId, TransactionEffectiveDate, TransactionEnteredDate
	),
	EXP_CALC AS (
		SELECT
		PolicyAKId,
		InsuranceLine,
		LocationXMLId,
		TransactionEffectiveDate,
		TransactionEnteredDate,
		CovGrpDesc AS v_CovGrpDesc,
		-- *INF*: DECODE(TRUE,
		-- v_CovGrpDesc=1,'B',
		-- v_CovGrpDesc=10,'C',
		-- v_CovGrpDesc>=11,'T',
		-- 'B')
		DECODE(
		    TRUE,
		    v_CovGrpDesc = 1, 'B',
		    v_CovGrpDesc = 10, 'C',
		    v_CovGrpDesc >= 11, 'T',
		    'B'
		) AS Value_Type
		FROM AGG_Value_Type
	),
	OUTPUT AS (
		SELECT
		PolicyAKId, 
		InsuranceLine, 
		LocationXMLId, 
		TransactionEffectiveDate, 
		TransactionEnteredDate, 
		Value_Type
		FROM EXP_CALC
	),
),
JNRTRANS AS (SELECT
	EXP_RowChange_Logic.PolicyAKId, 
	EXP_RowChange_Logic.AgencyAKId, 
	EXP_RowChange_Logic.InsuranceLine, 
	EXP_RowChange_Logic.LocationXMLId, 
	EXP_RowChange_Logic.TransactionEffectiveDate, 
	EXP_RowChange_Logic.TransactionEnteredDate, 
	EXP_RowChange_Logic.TransactionBookedDate, 
	EXP_RowChange_Logic.PolicyKey, 
	EXP_RowChange_Logic.PriorPolicyKey, 
	EXP_RowChange_Logic.TransactionCode, 
	EXP_RowChange_Logic.NameOfInsured, 
	EXP_RowChange_Logic.MailingStreetAddress, 
	EXP_RowChange_Logic.MailingCity, 
	EXP_RowChange_Logic.MailingStateProvinceAbbreviation, 
	EXP_RowChange_Logic.MailingZipCode, 
	EXP_RowChange_Logic.PolicyEffectiveDate, 
	EXP_RowChange_Logic.PolicyExpirationDate, 
	EXP_RowChange_Logic.Value, 
	EXP_RowChange_Logic.EquipmentBreakdownPremium, 
	EXP_RowChange_Logic.ProgramId, 
	EXP_RowChange_Logic.RiskStreetAddress, 
	EXP_RowChange_Logic.RiskCity, 
	EXP_RowChange_Logic.RiskStateProvinceAbbreviation, 
	EXP_RowChange_Logic.RiskZipCode, 
	EXP_RowChange_Logic.Occupancy, 
	EXP_RowChange_Logic.ActiveBuildingFlag, 
	EXP_RowChange_Logic.OriginalTransactionEnteredDate, 
	EXP_RowChange_Logic.o_RunDate AS ReportingPeriod, 
	EXP_RowChange_Logic.o_FilterFlag AS FilterFlag, 
	mplt_value_type.PolicyAKId1 AS in_PolicyAKId, 
	mplt_value_type.InsuranceLine1 AS in_InsuranceLine, 
	mplt_value_type.LocationXMLId1 AS in_LocationXMLId, 
	mplt_value_type.TransactionEffectiveDate1 AS in_TransactionEffectiveDate, 
	mplt_value_type.TransactionEnteredDate1 AS in_TransactionEnteredDate, 
	mplt_value_type.Value_Type, 
	EXP_RowChange_Logic.LegalName, 
	EXP_RowChange_Logic.PrimaryPhoneNumber, 
	EXP_RowChange_Logic.PrimaryEmailAddress
	FROM EXP_RowChange_Logic
	INNER JOIN mplt_value_type
	ON mplt_value_type.PolicyAKId1 = EXP_RowChange_Logic.PolicyAKId AND mplt_value_type.InsuranceLine1 = EXP_RowChange_Logic.InsuranceLine AND mplt_value_type.LocationXMLId1 = EXP_RowChange_Logic.LocationXMLId AND mplt_value_type.TransactionEffectiveDate1 = EXP_RowChange_Logic.TransactionEffectiveDate AND mplt_value_type.TransactionEnteredDate1 = EXP_RowChange_Logic.TransactionEnteredDate
),
FIL_NoChange AS (
	SELECT
	PolicyAKId, 
	AgencyAKId, 
	LocationXMLId, 
	TransactionEnteredDate, 
	TransactionEffectiveDate, 
	TransactionBookedDate, 
	PolicyKey, 
	PriorPolicyKey, 
	TransactionCode, 
	NameOfInsured, 
	MailingStreetAddress, 
	MailingCity, 
	MailingStateProvinceAbbreviation, 
	MailingZipCode, 
	PolicyEffectiveDate, 
	PolicyExpirationDate, 
	Value, 
	EquipmentBreakdownPremium, 
	ProgramId, 
	RiskStreetAddress, 
	RiskCity, 
	RiskStateProvinceAbbreviation, 
	RiskZipCode, 
	Occupancy, 
	ActiveBuildingFlag, 
	OriginalTransactionEnteredDate, 
	ReportingPeriod, 
	FilterFlag, 
	Value_Type, 
	LegalName, 
	PrimaryPhoneNumber, 
	PrimaryEmailAddress
	FROM JNRTRANS
	WHERE FilterFlag=1
),
SRT_Target AS (
	SELECT
	ReportingPeriod, 
	PolicyAKId, 
	AgencyAKId, 
	LocationXMLId, 
	TransactionEnteredDate, 
	TransactionEffectiveDate, 
	PolicyKey, 
	PriorPolicyKey, 
	TransactionCode, 
	NameOfInsured, 
	MailingStreetAddress, 
	MailingCity, 
	MailingStateProvinceAbbreviation, 
	MailingZipCode, 
	PolicyEffectiveDate, 
	PolicyExpirationDate, 
	Value, 
	EquipmentBreakdownPremium, 
	ProgramId, 
	RiskStreetAddress, 
	RiskCity, 
	RiskStateProvinceAbbreviation, 
	RiskZipCode, 
	Occupancy, 
	ActiveBuildingFlag, 
	Value_Type, 
	OriginalTransactionEnteredDate, 
	LegalName, 
	PrimaryPhoneNumber, 
	PrimaryEmailAddress
	FROM FIL_NoChange
	ORDER BY ReportingPeriod ASC, PolicyAKId ASC, TransactionEnteredDate ASC, TransactionEffectiveDate ASC
),
TGT_Anchor AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	ReportingPeriod,
	PolicyAKId,
	AgencyAKId,
	TransactionEnteredDate,
	TransactionEffectiveDate,
	LocationXMLId,
	OriginalTransactionEnteredDate,
	-- *INF*: DECODE(TRUE,
	-- ReportingPeriod!=v_prev_ReportingPeriod,1,
	-- ISNULL(v_prev_PolicyAKId),1,
	-- PolicyAKId=v_prev_PolicyAKId AND TransactionEnteredDate=v_prev_TransactionEnteredDate AND TransactionEffectiveDate=v_prev_TransactionEffectiveDate AND OriginalTransactionEnteredDate=v_OriginalTransactionEnteredDate,v_TransactionNumber,
	-- v_TransactionNumber+1)
	DECODE(
	    TRUE,
	    ReportingPeriod != v_prev_ReportingPeriod, 1,
	    v_prev_PolicyAKId IS NULL, 1,
	    PolicyAKId = v_prev_PolicyAKId AND TransactionEnteredDate = v_prev_TransactionEnteredDate AND TransactionEffectiveDate = v_prev_TransactionEffectiveDate AND OriginalTransactionEnteredDate = v_OriginalTransactionEnteredDate, v_TransactionNumber,
	    v_TransactionNumber + 1
	) AS v_TransactionNumber,
	ReportingPeriod AS v_prev_ReportingPeriod,
	PolicyAKId AS v_prev_PolicyAKId,
	TransactionEnteredDate AS v_prev_TransactionEnteredDate,
	TransactionEffectiveDate AS v_prev_TransactionEffectiveDate,
	OriginalTransactionEnteredDate AS v_OriginalTransactionEnteredDate,
	-- *INF*: LPAD(v_TransactionNumber,5,'0')
	LPAD(v_TransactionNumber, 5, '0') AS TransactionNumber,
	PolicyKey AS CurrentPolicyNumber,
	-- *INF*: :UDF.FORMAT_PADDING(CurrentPolicyNumber,20,' ')
	UDF_FORMAT_PADDING(CurrentPolicyNumber, 20, ' ') AS o_CurrentPolicyNumber,
	'000118' AS CompanyCode,
	TransactionCode,
	NameOfInsured,
	-- *INF*: :UDF.FORMAT_PADDING(NameOfInsured,55,' ')
	UDF_FORMAT_PADDING(NameOfInsured, 55, ' ') AS o_NameOfInsured,
	MailingStreetAddress,
	-- *INF*: :UDF.FORMAT_PADDING(MailingStreetAddress,55,' ')
	UDF_FORMAT_PADDING(MailingStreetAddress, 55, ' ') AS o_MailingStreetAddress,
	MailingCity,
	-- *INF*: :UDF.FORMAT_PADDING(MailingCity,20,' ')
	UDF_FORMAT_PADDING(MailingCity, 20, ' ') AS o_MailingCity,
	MailingStateProvinceAbbreviation,
	-- *INF*: :UDF.FORMAT_PADDING(MailingStateProvinceAbbreviation,2,' ')
	UDF_FORMAT_PADDING(MailingStateProvinceAbbreviation, 2, ' ') AS o_MailingStateProvinceAbbreviation,
	MailingZipCode AS in_MailingZipCode,
	-- *INF*: :UDF.FORMAT_PADDING(in_MailingZipCode,13,'0')
	UDF_FORMAT_PADDING(in_MailingZipCode, 13, '0') AS MailingZipCode1,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	'013' AS Coverage,
	EquipmentBreakdownPremium AS EBGrossPremium,
	-- *INF*: ROUND(0.7*EBGrossPremium,-2)
	ROUND(0.7 * EBGrossPremium, - 2) AS EBNetPremium,
	-- *INF*: :UDF.FORMAT_PADDING('0',9,'0')
	UDF_FORMAT_PADDING('0', 9, '0') AS Deductible,
	Occupancy,
	-- *INF*: :UDF.FORMAT_PADDING(Occupancy,6,' ')
	UDF_FORMAT_PADDING(Occupancy, 6, ' ') AS o_Occupancy,
	Value,
	Value_Type AS ValueType,
	-- *INF*: :UDF.FORMAT_PADDING('005',5,' ')
	UDF_FORMAT_PADDING('005', 5, ' ') AS BranchCode,
	-- *INF*: :LKP.LKP_Agency(AgencyAKId)
	LKP_AGENCY_AgencyAKId.AgencyCode AS v_AgencyCode,
	-- *INF*: :UDF.FORMAT_PADDING(v_AgencyCode,15,' ')
	UDF_FORMAT_PADDING(v_AgencyCode, 15, ' ') AS AgencyCode,
	PriorPolicyKey AS PreviousPolicyNumber,
	-- *INF*: :UDF.FORMAT_PADDING(PreviousPolicyNumber,20,' ')
	UDF_FORMAT_PADDING(PreviousPolicyNumber, 20, ' ') AS o_PreviousPolicyNumber,
	ProgramId,
	-- *INF*: :UDF.FORMAT_PADDING(ProgramId,20,' ')
	UDF_FORMAT_PADDING(ProgramId, 20, ' ') AS o_ProgramId,
	-- *INF*: :UDF.FORMAT_PADDING('1000225',10,' ')
	UDF_FORMAT_PADDING('1000225', 10, ' ') AS TreatyNumber,
	-- *INF*: :UDF.FORMAT_PADDING('',2,' ')
	UDF_FORMAT_PADDING('', 2, ' ') AS ISOType,
	-- *INF*: :UDF.FORMAT_PADDING(' ',55,' ')
	UDF_FORMAT_PADDING(' ', 55, ' ') AS LocationName,
	RiskStreetAddress,
	-- *INF*: :UDF.FORMAT_PADDING(RiskStreetAddress,55,' ')
	UDF_FORMAT_PADDING(RiskStreetAddress, 55, ' ') AS o_RiskStreetAddress,
	RiskCity,
	-- *INF*: :UDF.FORMAT_PADDING(RiskCity,20,' ')
	UDF_FORMAT_PADDING(RiskCity, 20, ' ') AS o_RiskCity,
	RiskStateProvinceAbbreviation,
	-- *INF*: :UDF.FORMAT_PADDING(RiskStateProvinceAbbreviation,2,' ')
	UDF_FORMAT_PADDING(RiskStateProvinceAbbreviation, 2, ' ') AS o_RiskStateProvinceAbbreviation,
	RiskZipCode,
	-- *INF*: :UDF.FORMAT_PADDING(RiskZipCode,13,'0')
	UDF_FORMAT_PADDING(RiskZipCode, 13, '0') AS o_RiskZipCode,
	-- *INF*: DECODE(ValueType,
	-- 'T','O',
	-- 'C','T',
	-- 'B','O' ,' ')
	DECODE(
	    ValueType,
	    'T', 'O',
	    'C', 'T',
	    'B', 'O',
	    ' '
	) AS OTIndicator,
	-- *INF*: :UDF.FORMAT_PADDING(' ',55,' ')
	UDF_FORMAT_PADDING(' ', 55, ' ') AS InspectionContactName,
	'' AS ContactPhoneNumber,
	ActiveBuildingFlag,
	LegalName,
	PrimaryPhoneNumber,
	PrimaryEmailAddress
	FROM SRT_Target
	LEFT JOIN LKP_AGENCY LKP_AGENCY_AgencyAKId
	ON LKP_AGENCY_AgencyAKId.AgencyAKID = AgencyAKId

),
WorkHSBEquipmentBreakdownExtract AS (
	INSERT INTO WorkHSBEquipmentBreakdownExtract
	(AuditId, CreatedDate, PolicyAKId, LocationXMLId, TransactionNumber, CurrentPolicyNumber, CompanyCode, TransactionCode, NameOfInsured, MailingStreetAddress, MailingCity, MailingStateProvinceAbbreviation, MailingZipCode, TransactionEnteredDate, TransactionEffectiveDate, PolicyEffectiveDate, PolicyExpirationDate, Coverage, EBGrossPremium, EBNetPremium, Deductible, Occupancy, Value, ValueType, BranchCode, AgencyCode, PreviousPolicyNumber, ProgramId, TreatyNumber, ISOType, LocationName, RiskStreetAddress, RiskCity, RiskStateProvinceAbbreviation, RiskZipCode, OTIndicator, InspectionContactName, ContactPhoneNumber, ReportingPeriod, ActiveLocationFlag, OriginalTransactionalEnteredDate, AgencyName, AgencyEmailAddress, AgencyPhoneNumber)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	POLICYAKID, 
	LOCATIONXMLID, 
	TRANSACTIONNUMBER, 
	o_CurrentPolicyNumber AS CURRENTPOLICYNUMBER, 
	COMPANYCODE, 
	TRANSACTIONCODE, 
	o_NameOfInsured AS NAMEOFINSURED, 
	o_MailingStreetAddress AS MAILINGSTREETADDRESS, 
	o_MailingCity AS MAILINGCITY, 
	o_MailingStateProvinceAbbreviation AS MAILINGSTATEPROVINCEABBREVIATION, 
	MailingZipCode1 AS MAILINGZIPCODE, 
	TRANSACTIONENTEREDDATE, 
	TRANSACTIONEFFECTIVEDATE, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	COVERAGE, 
	EBGROSSPREMIUM, 
	EBNETPREMIUM, 
	DEDUCTIBLE, 
	o_Occupancy AS OCCUPANCY, 
	VALUE, 
	VALUETYPE, 
	BRANCHCODE, 
	AGENCYCODE, 
	o_PreviousPolicyNumber AS PREVIOUSPOLICYNUMBER, 
	o_ProgramId AS PROGRAMID, 
	TREATYNUMBER, 
	ISOTYPE, 
	LOCATIONNAME, 
	o_RiskStreetAddress AS RISKSTREETADDRESS, 
	o_RiskCity AS RISKCITY, 
	o_RiskStateProvinceAbbreviation AS RISKSTATEPROVINCEABBREVIATION, 
	o_RiskZipCode AS RISKZIPCODE, 
	OTINDICATOR, 
	INSPECTIONCONTACTNAME, 
	CONTACTPHONENUMBER, 
	REPORTINGPERIOD, 
	ActiveBuildingFlag AS ACTIVELOCATIONFLAG, 
	OriginalTransactionEnteredDate AS ORIGINALTRANSACTIONALENTEREDDATE, 
	LegalName AS AGENCYNAME, 
	PrimaryEmailAddress AS AGENCYEMAILADDRESS, 
	PrimaryPhoneNumber AS AGENCYPHONENUMBER
	FROM TGT_Anchor
),