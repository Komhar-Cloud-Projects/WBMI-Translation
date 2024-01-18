WITH
SQ_WorkCatastropheExposureDeductible AS (
	SELECT D.PolicyKey as PolicyKey, D.LocationNumber as LocationNumber, D.BuildingNumber as BuildingNumber, D.BusinessType as BusinessType, 
	 RelevantDeductibles.TargetColumn AS TargetColumn,
	 (CASE WHEN RelevantDeductibles.CalculationRule = 'MAX' THEN MAX(CONVERT(BIGINT, D.DeductibleValue)) 
	  WHEN RelevantDeductibles.CalculationRule = 'MIN' THEN MIN(CONVERT(BIGINT, D.DeductibleValue)) 
	  WHEN RelevantDeductibles.CalculationRule = 'SUM' THEN SUM(CONVERT(BIGINT, D.DeductibleValue)) 
	  ELSE 0 
	 END) AS DeductibleValue
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureDeductible D
	INNER JOIN (SELECT DISTINCT BusinessType, DeductibleType, CalculationRule, TargetColumn 
	  FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupCatastropheExposureDeductibleRule 
	  WHERE SourceSystemId = 'DCT') RelevantDeductibles ON D.BusinessType = RelevantDeductibles.BusinessType AND D.DeductibleType = RelevantDeductibles.DeductibleType
	WHERE D.BusinessType IN ('Commercial Property', 'SBOP', 'SMARTbusiness') 
	GROUP BY D.PolicyKey, D.LocationNumber, D.BuildingNumber, D.BusinessType, RelevantDeductibles.TargetColumn, RelevantDeductibles.CalculationRule
	ORDER BY D.PolicyKey, D.LocationNumber, D.BuildingNumber, D.BusinessType, RelevantDeductibles.TargetColumn, RelevantDeductibles.CalculationRule
),
EXP_ConcatDeductible AS (
	SELECT
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType,
	TargetColumn,
	DeductibleValue,
	-- *INF*: IIF(TargetColumn='Pol Blkt Ded', TO_BIGINT(DeductibleValue), 0)
	IFF(TargetColumn = 'Pol Blkt Ded', CAST(DeductibleValue AS BIGINT), 0) AS o_PolicyBlanketDeductible,
	-- *INF*: IIF(TargetColumn='Loc Bldg Deductible', TO_BIGINT(DeductibleValue), 0)
	IFF(TargetColumn = 'Loc Bldg Deductible', CAST(DeductibleValue AS BIGINT), 0) AS o_LocationBuildingDeductible,
	-- *INF*: IIF(TargetColumn='Loc Conts Deductible', TO_BIGINT(DeductibleValue), 0)
	IFF(TargetColumn = 'Loc Conts Deductible', CAST(DeductibleValue AS BIGINT), 0) AS o_LocationContentsDeductible,
	-- *INF*: IIF(TargetColumn='Loc BI Deductible', TO_BIGINT(DeductibleValue), 0)
	IFF(TargetColumn = 'Loc BI Deductible', CAST(DeductibleValue AS BIGINT), 0) AS o_LocationBIDeductible,
	-- *INF*: IIF(TargetColumn='BPP Earthquake Deductible Percentage', TO_BIGINT(DeductibleValue), 0)
	IFF(
	    TargetColumn = 'BPP Earthquake Deductible Percentage', CAST(DeductibleValue AS BIGINT), 0
	) AS o_BPPEarthquakeDeductiblePercentage,
	-- *INF*: IIF(TargetColumn='Building Earthquake Deductible Percentage', TO_BIGINT(DeductibleValue), 0)
	IFF(
	    TargetColumn = 'Building Earthquake Deductible Percentage', CAST(DeductibleValue AS BIGINT),
	    0
	) AS o_BuildingEarthquakeDeductiblePercentage,
	-- *INF*: IIF(TargetColumn='Earthquake Deductible Percentage', TO_BIGINT(DeductibleValue), 0)
	IFF(TargetColumn = 'Earthquake Deductible Percentage', CAST(DeductibleValue AS BIGINT), 0) AS o_EarthquakeDeductiblePercentage,
	-- *INF*: IIF(TargetColumn='Loc BI Deductible-Building', TO_BIGINT(DeductibleValue), 0)
	IFF(TargetColumn = 'Loc BI Deductible-Building', CAST(DeductibleValue AS BIGINT), 0) AS LocBIDeductibleBuilding,
	-- *INF*: IIF(TargetColumn='Loc BI Deductible-Contents', TO_BIGINT(DeductibleValue), 0)
	IFF(TargetColumn = 'Loc BI Deductible-Contents', CAST(DeductibleValue AS BIGINT), 0) AS LocBIDeductibleContents
	FROM SQ_WorkCatastropheExposureDeductible
),
AGG_RowToColumn_Deductible AS (
	SELECT
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType,
	o_PolicyBlanketDeductible AS PolicyBlanketDeductible,
	o_LocationBuildingDeductible AS LocationBuildingDeductible,
	o_LocationContentsDeductible AS LocationContentsDeductible,
	o_LocationBIDeductible AS LocationBIDeductible,
	o_BPPEarthquakeDeductiblePercentage AS BPPEarthquakeDeductiblePercentage,
	o_BuildingEarthquakeDeductiblePercentage AS BuildingEarthquakeDeductiblePercentage,
	o_EarthquakeDeductiblePercentage AS EarthquakeDeductiblePercentage,
	LocBIDeductibleBuilding,
	LocBIDeductibleContents,
	-- *INF*: SUM(PolicyBlanketDeductible)
	SUM(PolicyBlanketDeductible) AS o_PolicyBlanketDeductible,
	-- *INF*: SUM(LocationBuildingDeductible)
	SUM(LocationBuildingDeductible) AS o_LocationBuildingDeductible,
	-- *INF*: SUM(LocationContentsDeductible)
	SUM(LocationContentsDeductible) AS o_LocationContentsDeductible,
	-- *INF*: SUM(LocationBIDeductible)
	SUM(LocationBIDeductible) AS o_LocationBIDeductible,
	-- *INF*: SUM(BPPEarthquakeDeductiblePercentage)
	SUM(BPPEarthquakeDeductiblePercentage) AS o_BPPEarthquakeDeductiblePercentage,
	-- *INF*: SUM(BuildingEarthquakeDeductiblePercentage)
	SUM(BuildingEarthquakeDeductiblePercentage) AS o_BuildingEarthquakeDeductiblePercentage,
	-- *INF*: SUM(EarthquakeDeductiblePercentage)
	SUM(EarthquakeDeductiblePercentage) AS o_EarthquakeDeductiblePercentage,
	-- *INF*: Sum(LocBIDeductibleBuilding)
	Sum(LocBIDeductibleBuilding) AS o_LocBIDeductibleBuilding,
	-- *INF*: SUM(LocBIDeductibleContents)
	SUM(LocBIDeductibleContents) AS o_LocBIDeductibleContents
	FROM EXP_ConcatDeductible
	GROUP BY PolicyKey, LocationNumber, BuildingNumber, BusinessType
),
EXP_SplitDeductible AS (
	SELECT
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType,
	o_PolicyBlanketDeductible AS PolicyBlanketDeductible,
	o_LocationBuildingDeductible AS LocationBuildingDeductible,
	o_LocationContentsDeductible AS LocationContentsDeductible,
	o_LocationBIDeductible AS LocationBIDeductible,
	o_LocBIDeductibleBuilding AS LocBIDeductibleBuilding,
	o_LocBIDeductibleContents AS LocBIDeductibleContents,
	o_BPPEarthquakeDeductiblePercentage AS BPPEarthquakeDeductiblePercentage,
	o_BuildingEarthquakeDeductiblePercentage AS BuildingEarthquakeDeductiblePercentage,
	o_EarthquakeDeductiblePercentage AS EarthquakeDeductiblePercentage,
	-- *INF*: IIF(PolicyBlanketDeductible=0, NULL,PolicyBlanketDeductible)
	-- 
	-- --DECODE(TRUE,BusinessType='SBOP', LocationContentsDeductible, IN(BusinessType, 'Commercial Property', 'SMARTbusiness'), NULL,NULL)
	IFF(PolicyBlanketDeductible = 0, NULL, PolicyBlanketDeductible) AS o_PolicyBlanketDeductible,
	-- *INF*: IIF(LocationBuildingDeductible=0 AND LocationContentsDeductible=0 AND LocationBIDeductible=0, NULL,  GREATEST(LocationBuildingDeductible,LocationContentsDeductible,LocationBIDeductible))
	IFF(
	    LocationBuildingDeductible = 0 AND LocationContentsDeductible = 0 AND LocationBIDeductible = 0,
	    NULL,
	    GREATEST(LocationBuildingDeductible, LocationContentsDeductible, LocationBIDeductible)
	) AS o_LocationDeductible,
	-- *INF*: IIF(LocationBuildingDeductible=0, NULL, LocationBuildingDeductible)
	IFF(LocationBuildingDeductible = 0, NULL, LocationBuildingDeductible) AS o_LocationBuildingDeductible,
	-- *INF*: IIF(LocationContentsDeductible=0, NULL, LocationContentsDeductible)
	IFF(LocationContentsDeductible = 0, NULL, LocationContentsDeductible) AS o_LocationContentsDeductible,
	-- *INF*: IIF(LocationBIDeductible=0, NULL, LocationBIDeductible)
	IFF(LocationBIDeductible = 0, NULL, LocationBIDeductible) AS o_LocationBIDeductible,
	-- *INF*:  IIF(LocBIDeductibleBuilding=0, NULL,LocBIDeductibleBuilding)
	IFF(LocBIDeductibleBuilding = 0, NULL, LocBIDeductibleBuilding) AS o_LocBIDeductibleBuilding,
	-- *INF*: IIF(LocBIDeductibleContents=0, NULL,LocBIDeductibleContents)
	IFF(LocBIDeductibleContents = 0, NULL, LocBIDeductibleContents) AS o_LocBIDeductibleContents
	FROM AGG_RowToColumn_Deductible
),
SQ_WorkCatastropheExposureTransaction AS (
	SELECT WCET.SourceSystemID,
	WCET.PolicyKey,
	WCET.LocationNumber,
	WCET.BuildingNumber,
	WCET.BusinessType,
	IRLE.InsuranceReferenceLegalEntityDescription,
	WCET.PremiumTransactionAmount,
	RL.StreetAddress,
	RL.RatingCity,
	SS.state_code,
	RL.ZipPostalCode,
	RL.RatingCounty,
	PT.ConstructionCode,
	AGY.AgencyCode,
	SPC.StrategicProfitCenterDescription, 
	ISG.InsuranceSegmentDescription,
	CC.name,
	CC.cust_num ,
	POL.pol_eff_date,
	POL.pol_exp_date,
	RTRIM(SUBSTRING(SBCC.StandardBusinessClassCode, 1, 5)) OccupancyCode,
	CC.sic_code,
	@{pipeline().parameters.PROCESS_DATE} ProcessDate,
	WCET.CoverageDescription,
	WCET.AddToLocationPremiumFlag
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction WCET
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	ON WCET.PremiumTransactionAKId=PT.PremiumTransactionAKID
	AND PT.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	ON PT.RatingCoverageAKID=RC.RatingCoverageAKID
	AND RC.EffectiveDate=PT.EffectiveDate 
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	AND PC.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	ON PC.RiskLocationAKID=RL.RiskLocationAKID
	AND RL.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	ON POL.pol_ak_id=RL.PolicyAKID
	AND POL.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer CC
	ON CC.contract_cust_ak_id=POL.contract_cust_ak_id
	AND CC.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency AGY
	ON AGY.AgencyAKID=POL.AgencyAKId
	AND AGY.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_business_classification_code SBCC
	ON SBCC.sup_bus_class_code_id=POL.sup_bus_class_code_id 
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state SS
	ON (CASE WHEN len(SS.state_abbrev) = 1 THEN '0' + SS.state_abbrev ELSE SS.state_abbrev END) = (CASE WHEN len(RL.StateProvinceCode) = 1 THEN '0' + RL.StateProvinceCode ELSE RL.StateProvinceCode END)
	AND SS.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	ON POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	AND ISG.CurrentSnapshotFlag=1
	AND ISG.InsuranceSegmentAbbreviation = 'CL'
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC
	ON POL.StrategicProfitCenterAKId=SPC.StrategicProfitCenterAKId
	AND SPC.CurrentSnapshotFlag=1 
	AND SPC.StrategicProfitCenterDescription IN ('West Bend Commercial Lines', 'NSI')
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity IRLE
	ON SPC.InsuranceReferenceLegalEntityId = IRLE.InsuranceReferenceLegalEntityId 
	AND IRLE.CurrentSnapshotFlag = 1
	WHERE WCET.BusinessType IN ('Commercial Property','SBOP','SMARTbusiness')
	AND WCET.BuildingNumber <> '000' 
	
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Cal AS (
	SELECT
	SourceSystemId,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	PremiumTransactionAmount,
	StreetAddress,
	RatingCity,
	state_code,
	ZipPostalCode,
	RatingCounty,
	ConstructionCode,
	-- *INF*: LTRIM(RTRIM(ConstructionCode))
	LTRIM(RTRIM(ConstructionCode)) AS ConstructionCode1,
	AgencyCode,
	StrategicProfitCenterDescription,
	InsuranceSegmentDescription,
	name,
	cust_num,
	pol_eff_date,
	pol_exp_date,
	OccupancyCode,
	sic_code,
	ProcessDate,
	CoverageDescription,
	AddToLocationPremiumFlag,
	-- *INF*: IIF(state_code='N/A', '', RTRIM(state_code))
	IFF(state_code = 'N/A', '', RTRIM(state_code)) AS o_RatingStateProvinceAbbreviation,
	-- *INF*: SUBSTR(LTRIM(ZipPostalCode), 0, 5)
	SUBSTR(LTRIM(ZipPostalCode), 0, 5) AS o_RatingPostalCode,
	AgencyCode AS o_AgencyCode,
	name AS o_InsuredName,
	-- *INF*: RTRIM(SUBSTR(cust_num, 1, 12))
	RTRIM(SUBSTR(cust_num, 1, 12)) AS o_CustomerNumber,
	pol_eff_date AS o_PolicyEffectiveDate,
	pol_exp_date AS o_PolicyExpirationDate,
	-- *INF*: RTRIM(SUBSTR(sic_code, 1, 4))
	RTRIM(SUBSTR(sic_code, 1, 4)) AS o_SicCode
	FROM SQ_WorkCatastropheExposureTransaction
),
AGG_BuildingPremium AS (
	SELECT
	SourceSystemId,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	PremiumTransactionAmount,
	StreetAddress,
	RatingCity,
	o_RatingStateProvinceAbbreviation AS RatingStateProvinceAbbreviation,
	o_RatingPostalCode AS RatingPostalCode,
	RatingCounty,
	ConstructionCode1 AS ConstructionCode,
	o_AgencyCode AS AgencyCode,
	StrategicProfitCenterDescription,
	InsuranceSegmentDescription,
	o_InsuredName AS InsuredName,
	o_CustomerNumber AS CustomerNumber,
	o_PolicyEffectiveDate AS PolicyEffectiveDate,
	o_PolicyExpirationDate AS PolicyExpirationDate,
	OccupancyCode,
	o_SicCode AS SicCode,
	ProcessDate,
	CoverageDescription,
	AddToLocationPremiumFlag,
	-- *INF*: MAX(ConstructionCode, ConstructionCode != 'N/A')
	MAX(ConstructionCode, ConstructionCode != 'N/A') AS o_ConstructionCode,
	-- *INF*: SUM(PremiumTransactionAmount, AddToLocationPremiumFlag='T')
	SUM(PremiumTransactionAmount, AddToLocationPremiumFlag = 'T') AS o_BuildingPremium
	FROM EXP_Cal
	GROUP BY PolicyKey, LocationNumber, BuildingNumber, BusinessType
),
SQ_WorkCatastropheExposureLimit AS (
	SELECT L.PolicyKey as PolicyKey , L.LocationNumber as LocationNumber , L.BuildingNumber as BuildingNumber, L.BusinessType as BusinessType, 
	 RelevantLimits.TargetColumn AS TargetColumn,
	 (CASE WHEN RelevantLimits.CalculationRule = 'MAX' THEN MAX(CONVERT(BIGINT, L.LimitValue)) 
	  WHEN RelevantLimits.CalculationRule = 'MIN' THEN MIN(CONVERT(BIGINT, L.LimitValue)) 
	  WHEN RelevantLimits.CalculationRule = 'SUM' THEN SUM(CONVERT(BIGINT, L.LimitValue)) 
	  ELSE 0 
	 END) AS LimitValue
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureLimit L
	INNER JOIN (SELECT DISTINCT BusinessType, LimitType, CalculationRule, TargetColumn 
	  FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupCatastropheExposureLimitRule 
	  WHERE SourceSystemId = 'DCT' 
	  AND TargetColumn <> 'Pol Blkt Limit') RelevantLimits ON L.BusinessType = RelevantLimits.BusinessType AND L.LimitType = RelevantLimits.LimitType
	WHERE L.BusinessType IN ('Commercial Property', 'SBOP', 'SMARTbusiness') 
	GROUP BY L.PolicyKey, L.LocationNumber, L.BuildingNumber, L.BusinessType, RelevantLimits.TargetColumn, RelevantLimits.CalculationRule
	ORDER BY L.PolicyKey, L.LocationNumber, L.BuildingNumber, L.BusinessType, RelevantLimits.TargetColumn, RelevantLimits.CalculationRule
),
EXP_ConcatLimit AS (
	SELECT
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType,
	TargetColumn,
	LimitValue,
	-- *INF*: IIF(TargetColumn='Loc Bldg Limit', TO_BIGINT(LimitValue), 0)
	IFF(TargetColumn = 'Loc Bldg Limit', CAST(LimitValue AS BIGINT), 0) AS o_LocationBuildingLimit,
	-- *INF*: IIF(TargetColumn='Loc Conts Limit', TO_BIGINT(LimitValue), 0)
	IFF(TargetColumn = 'Loc Conts Limit', CAST(LimitValue AS BIGINT), 0) AS o_LocationContentsLimit,
	-- *INF*: IIF(TargetColumn='Loc BI Limit', TO_BIGINT(LimitValue), 0)
	IFF(TargetColumn = 'Loc BI Limit', CAST(LimitValue AS BIGINT), 0) AS o_LocationBILimit
	FROM SQ_WorkCatastropheExposureLimit
),
AGG_RowToColumn_Limit AS (
	SELECT
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType,
	o_LocationBuildingLimit AS LocationBuildingLimit,
	o_LocationContentsLimit AS LocationContentsLimit,
	o_LocationBILimit AS LocationBILimit,
	-- *INF*: SUM(LocationBuildingLimit)
	SUM(LocationBuildingLimit) AS o_LocationBuildingLimit,
	-- *INF*: SUM(LocationContentsLimit)
	SUM(LocationContentsLimit) AS o_LocationContentsLimit,
	-- *INF*: SUM(LocationBILimit)
	SUM(LocationBILimit) AS o_LocationBILimit
	FROM EXP_ConcatLimit
	GROUP BY PolicyKey, LocationNumber, BuildingNumber, BusinessType
),
EXP_SplitLimit AS (
	SELECT
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType,
	o_LocationBuildingLimit AS LocationBuildingLimit,
	o_LocationContentsLimit AS LocationContentsLimit,
	o_LocationBILimit AS LocationBILimit,
	-- *INF*: DECODE(TRUE, 
	-- BusinessType= 'Commercial Property', LocationBILimit, 
	-- BusinessType= 'SBOP',LocationBuildingLimit * 0.2,
	-- BusinessType='SMARTbusiness' ,LocationBuildingLimit * 0.2,
	-- 0)
	-- 
	-- 
	-- -----IIF(LocationBILimit=0, NULL, LocationBILimit)
	DECODE(
	    TRUE,
	    BusinessType = 'Commercial Property', LocationBILimit,
	    BusinessType = 'SBOP', LocationBuildingLimit * 0.2,
	    BusinessType = 'SMARTbusiness', LocationBuildingLimit * 0.2,
	    0
	) AS v_LocationBILimit,
	-- *INF*: IIF(LocationBuildingLimit=0 AND LocationContentsLimit=0 AND v_LocationBILimit=0, NULL, 
	-- IIF(BusinessType='SBOP',LocationBuildingLimit+LocationContentsLimit,
	-- LocationBuildingLimit+LocationContentsLimit+v_LocationBILimit))
	IFF(
	    LocationBuildingLimit = 0 AND LocationContentsLimit = 0 AND v_LocationBILimit = 0, NULL,
	    IFF(
	        BusinessType = 'SBOP', LocationBuildingLimit + LocationContentsLimit,
	        LocationBuildingLimit + LocationContentsLimit + v_LocationBILimit
	    )
	) AS o_LocationLimit,
	-- *INF*: IIF(LocationBuildingLimit=0, NULL, LocationBuildingLimit)
	IFF(LocationBuildingLimit = 0, NULL, LocationBuildingLimit) AS o_LocationBuildingLimit,
	-- *INF*: IIF(LocationContentsLimit=0, NULL, LocationContentsLimit)
	IFF(LocationContentsLimit = 0, NULL, LocationContentsLimit) AS o_LocationContentsLimit,
	-- *INF*: IIF(v_LocationBILimit=0, NULL, v_LocationBILimit)
	IFF(v_LocationBILimit = 0, NULL, v_LocationBILimit) AS o_LocationBILimit
	FROM AGG_RowToColumn_Limit
),
JNR_Limit AS (SELECT
	AGG_BuildingPremium.SourceSystemId, 
	AGG_BuildingPremium.PolicyKey, 
	AGG_BuildingPremium.LocationNumber, 
	AGG_BuildingPremium.BuildingNumber, 
	AGG_BuildingPremium.BusinessType, 
	AGG_BuildingPremium.InsuranceReferenceLegalEntityDescription, 
	AGG_BuildingPremium.StreetAddress, 
	AGG_BuildingPremium.RatingCity, 
	AGG_BuildingPremium.RatingStateProvinceAbbreviation, 
	AGG_BuildingPremium.RatingPostalCode, 
	AGG_BuildingPremium.RatingCounty, 
	AGG_BuildingPremium.o_ConstructionCode AS ConstructionCode, 
	AGG_BuildingPremium.AgencyCode, 
	AGG_BuildingPremium.StrategicProfitCenterDescription, 
	AGG_BuildingPremium.InsuranceSegmentDescription, 
	AGG_BuildingPremium.InsuredName, 
	AGG_BuildingPremium.CustomerNumber, 
	AGG_BuildingPremium.PolicyEffectiveDate, 
	AGG_BuildingPremium.PolicyExpirationDate, 
	AGG_BuildingPremium.OccupancyCode, 
	AGG_BuildingPremium.SicCode, 
	AGG_BuildingPremium.ProcessDate, 
	AGG_BuildingPremium.o_BuildingPremium AS BuildingPremium, 
	EXP_SplitLimit.PolicyKey AS PolicyKey_Limit, 
	EXP_SplitLimit.LocationNumber AS LocationNumber_Limit, 
	EXP_SplitLimit.BuildingNumber AS BuildingNumber_Limit, 
	EXP_SplitLimit.BusinessType AS BusinessType_Limit, 
	EXP_SplitLimit.o_LocationLimit AS LocationLimit, 
	EXP_SplitLimit.o_LocationBuildingLimit AS LocationBuildingLimit, 
	EXP_SplitLimit.o_LocationContentsLimit AS LocationContentsLimit, 
	EXP_SplitLimit.o_LocationBILimit AS LocationBILimit
	FROM AGG_BuildingPremium
	LEFT OUTER JOIN EXP_SplitLimit
	ON EXP_SplitLimit.PolicyKey = AGG_BuildingPremium.PolicyKey AND EXP_SplitLimit.LocationNumber = AGG_BuildingPremium.LocationNumber AND EXP_SplitLimit.BuildingNumber = AGG_BuildingPremium.BuildingNumber AND EXP_SplitLimit.BusinessType = AGG_BuildingPremium.BusinessType
),
JNR_Deductible AS (SELECT
	JNR_Limit.SourceSystemId, 
	JNR_Limit.PolicyKey, 
	JNR_Limit.LocationNumber, 
	JNR_Limit.BuildingNumber, 
	JNR_Limit.BusinessType, 
	JNR_Limit.InsuranceReferenceLegalEntityDescription, 
	JNR_Limit.StreetAddress, 
	JNR_Limit.RatingCity, 
	JNR_Limit.RatingStateProvinceAbbreviation, 
	JNR_Limit.RatingPostalCode, 
	JNR_Limit.RatingCounty, 
	JNR_Limit.ConstructionCode, 
	JNR_Limit.AgencyCode, 
	JNR_Limit.StrategicProfitCenterDescription, 
	JNR_Limit.InsuranceSegmentDescription, 
	JNR_Limit.InsuredName, 
	JNR_Limit.CustomerNumber, 
	JNR_Limit.PolicyEffectiveDate, 
	JNR_Limit.PolicyExpirationDate, 
	JNR_Limit.OccupancyCode, 
	JNR_Limit.SicCode, 
	JNR_Limit.ProcessDate, 
	JNR_Limit.BuildingPremium, 
	JNR_Limit.LocationLimit, 
	JNR_Limit.LocationBuildingLimit, 
	JNR_Limit.LocationContentsLimit, 
	JNR_Limit.LocationBILimit, 
	EXP_SplitDeductible.PolicyKey AS PolicyKey_Deductible, 
	EXP_SplitDeductible.LocationNumber AS LocationNumber_Deductible, 
	EXP_SplitDeductible.BuildingNumber AS BuildingNumber_Deductible, 
	EXP_SplitDeductible.BusinessType AS BusinessType_Deductible, 
	EXP_SplitDeductible.BPPEarthquakeDeductiblePercentage, 
	EXP_SplitDeductible.BuildingEarthquakeDeductiblePercentage, 
	EXP_SplitDeductible.EarthquakeDeductiblePercentage, 
	EXP_SplitDeductible.o_PolicyBlanketDeductible AS PolicyBlanketDeductible, 
	EXP_SplitDeductible.o_LocationDeductible AS LocationDeductible, 
	EXP_SplitDeductible.o_LocationBuildingDeductible AS LocationBuildingDeductible, 
	EXP_SplitDeductible.o_LocationContentsDeductible AS LocationContentsDeductible, 
	EXP_SplitDeductible.o_LocationBIDeductible AS LocationBIDeductible, 
	EXP_SplitDeductible.o_LocBIDeductibleBuilding, 
	EXP_SplitDeductible.o_LocBIDeductibleContents
	FROM JNR_Limit
	LEFT OUTER JOIN EXP_SplitDeductible
	ON EXP_SplitDeductible.PolicyKey = JNR_Limit.PolicyKey AND EXP_SplitDeductible.LocationNumber = JNR_Limit.LocationNumber AND EXP_SplitDeductible.BuildingNumber = JNR_Limit.BuildingNumber AND EXP_SplitDeductible.BusinessType = JNR_Limit.BusinessType
),
LKP_WorkCatastropheExposureLimit_CPP_SBOP AS (
	SELECT
	LimitValue,
	PolicyKey,
	BusinessType
	FROM (
		SELECT L.PolicyKey AS PolicyKey
		     , L.BusinessType AS BusinessType
			 , (CASE WHEN RelevantLimits.CalculationRule = 'MAX' THEN MAX(CONVERT(BIGINT, L.LimitValue))
		  WHEN RelevantLimits.CalculationRule = 'MIN' THEN MIN(CONVERT(BIGINT, L.LimitValue))
		  WHEN RelevantLimits.CalculationRule = 'SUM' THEN SUM(CONVERT(BIGINT, L.LimitValue))
		  ELSE 0
		 END) AS LimitValue
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureLimit L
		INNER JOIN (SELECT DISTINCT BusinessType, LimitType, CalculationRule, TargetColumn
		  FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupCatastropheExposureLimitRule
		  WHERE SourceSystemId = 'DCT'
		  AND TargetColumn = 'Pol Blkt Limit') RelevantLimits ON L.BusinessType = RelevantLimits.BusinessType AND L.LimitType = RelevantLimits.LimitType
		WHERE L.BusinessType IN ('Commercial Property', 'SBOP')
		GROUP BY L.PolicyKey, L.BusinessType, RelevantLimits.CalculationRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,BusinessType ORDER BY LimitValue) = 1
),
LKP_WorkCatastropheExposureLimit_SMARTBlanket AS (
	SELECT
	LimitValue,
	PolicyKey
	FROM (
		select L.PolicyKey as PolicyKey, SUM(CONVERT(BIGINT, L.LimitValue)) as LimitValue
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureLimit L with (nolock)
		where L.BusinessType = 'SMARTbusiness'
		and (L.LimitType like '%Blanket Building' or L.LimitType like '%Blanket Building and Personal Property' or L.LimitType like '%Blanket Contents')
		group by L.PolicyKey
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY LimitValue) = 1
),
LKP_WorkCatastropheExposureTransaction_BalnketDeductible AS (
	SELECT
	CoverageGroupDescription,
	PolicyKey,
	BusinessType
	FROM (
		SELECT 
			CoverageGroupDescription,
			PolicyKey,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageGroupDescription like '%blanket%' AND BusinessType='SBOP'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,BusinessType ORDER BY CoverageGroupDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_CD_ALS AS (
	SELECT
	DctCoverageTypeCode,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			DctCoverageTypeCode,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE DctCoverageTypeCode = 'ALS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY DctCoverageTypeCode) = 1
),
LKP_WorkCatastropheExposureTransaction_CD_BusinessPersonalProperty AS (
	SELECT
	CoverageDescription,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			CoverageDescription,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageDescription = 'Business Personal Property'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY CoverageDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_CD_Earthquake AS (
	SELECT
	CoverageDescription,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			CoverageDescription,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageDescription='Earthquake'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY CoverageDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBuilding AS (
	SELECT
	CoverageDescription,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			CoverageDescription,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageDescription='Earthquake - Building'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY CoverageDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBusinessPersonalProperty AS (
	SELECT
	CoverageDescription,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			CoverageDescription,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageDescription='Earthquake - Business Personal Property'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY CoverageDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_CD_EarthquakePersonalProperty AS (
	SELECT
	CoverageDescription,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			CoverageDescription,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageDescription='Earthquake - Personal Property'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY CoverageDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_CGD_BusinessIncome AS (
	SELECT
	CoverageGroupDescription,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			CoverageGroupDescription,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageGroupDescription = 'Business Income'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY CoverageGroupDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_Earthquake AS (
	SELECT
	CoverageGroupDescription,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			CoverageGroupDescription,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageGroupDescription='Earthquake'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY CoverageGroupDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_Elite AS (
	SELECT
	CoverageDescription,
	PolicyKey,
	BusinessType
	FROM (
		SELECT 
			CoverageDescription,
			PolicyKey,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageDescription='Enhancement Endorsement Elite - Property'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,BusinessType ORDER BY CoverageDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_Essential AS (
	SELECT
	CoverageDescription,
	PolicyKey,
	BusinessType
	FROM (
		SELECT 
			CoverageDescription,
			PolicyKey,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageDescription='Enhancement Endorsement Essential - Property'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,BusinessType ORDER BY CoverageDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_LocationEarthquakePremium AS (
	SELECT
	LocationEarthquakePremium,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
		PolicyKey as PolicyKey, 
		LocationNumber as LocationNumber, 
		BuildingNumber as BuildingNumber, 
		BusinessType as BusinessType,
		SUM(PremiumTransactionAmount) as LocationEarthquakePremium
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageGroupDescription='Earthquake'
		GROUP BY 
		PolicyKey, 
		LocationNumber, 
		BuildingNumber, 
		BusinessType 
		ORDER BY
		PolicyKey, 
		LocationNumber, 
		BuildingNumber, 
		BusinessType
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY LocationEarthquakePremium) = 1
),
LKP_WorkCatastropheExposureTransaction_PlusPak AS (
	SELECT
	CoverageDescription,
	PolicyKey,
	BusinessType
	FROM (
		SELECT 
			CoverageDescription,
			PolicyKey,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageDescription='Plus Pak'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,BusinessType ORDER BY CoverageDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_PolicyBlanketPremium AS (
	SELECT
	PolicyBlanketPremium,
	PolicyKey,
	BusinessType
	FROM (
		SELECT
		PolicyKey as PolicyKey, 
		BusinessType as BusinessType, 
		SUM(PremiumTransactionAmount) as PolicyBlanketPremium
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE CoverageGroupDescription IN ('Blanket Building','Blanket Building and Contents','Blanket Business Income','Blanket Contents') 
		GROUP BY 
		PolicyKey, 
		BusinessType 
		ORDER BY
		PolicyKey, 
		BusinessType
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,BusinessType ORDER BY PolicyBlanketPremium) = 1
),
LKP_WorkCatastropheExposureTransaction_Terrorism AS (
	SELECT
	CoverageGroupDescription,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			CoverageGroupDescription,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE (TerrorismRiskIndicator = 1
		  or (CoverageGroupDescription = 'Terrorism' and PremiumTransactionAmount > 0.0)
		  )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY CoverageGroupDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_WindCoverageFlag AS (
	SELECT
	WindCoverageFlag,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		SELECT 
			WindCoverageFlag,
			PolicyKey,
			LocationNumber,
			BuildingNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE WindCoverageFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY WindCoverageFlag) = 1
),
LKP_WorkCatastropheExposureTransaction_YearBuilt AS (
	SELECT
	YearBuilt,
	i_PolicyKey,
	i_LocationNumber,
	i_BuildingNumber,
	i_BusinessType,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	BusinessType
	FROM (
		select distinct t.PolicyKey as PolicyKey,
		 t.LocationNumber as LocationNumber, t.BuildingNumber as BuildingNumber, t.BusinessType as BusinessType,
		   FIRST_VALUE(pt.YearBuilt) OVER (
		  PARTITION BY t.PolicyKey, t.LocationNumber, t.BuildingNumber, t.BusinessType
		  ORDER BY PT.PremiumTransactionEffectiveDate desc, PT.PremiumTransactionEnteredDate desc, 
		  PT.OffsetOnsetCode desc) as YearBuilt
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction t with (nolock)
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt with (nolock) on t.PremiumTransactionAKID = pt.PremiumTransactionAKID
		 and pt.YearBuilt <> '0000'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BuildingNumber,BusinessType ORDER BY YearBuilt) = 1
),
EXP_MetaData AS (
	SELECT
	JNR_Deductible.SourceSystemId,
	JNR_Deductible.PolicyKey,
	JNR_Deductible.LocationNumber,
	JNR_Deductible.BuildingNumber,
	JNR_Deductible.BusinessType,
	JNR_Deductible.InsuranceReferenceLegalEntityDescription,
	JNR_Deductible.StreetAddress,
	JNR_Deductible.RatingCity,
	JNR_Deductible.RatingStateProvinceAbbreviation,
	JNR_Deductible.RatingPostalCode,
	JNR_Deductible.RatingCounty,
	JNR_Deductible.ConstructionCode,
	LKP_WorkCatastropheExposureTransaction_YearBuilt.YearBuilt,
	JNR_Deductible.AgencyCode,
	JNR_Deductible.StrategicProfitCenterDescription,
	JNR_Deductible.InsuranceSegmentDescription,
	JNR_Deductible.InsuredName,
	JNR_Deductible.CustomerNumber,
	JNR_Deductible.PolicyEffectiveDate,
	JNR_Deductible.PolicyExpirationDate,
	JNR_Deductible.OccupancyCode,
	JNR_Deductible.SicCode,
	JNR_Deductible.ProcessDate,
	JNR_Deductible.BuildingPremium,
	BuildingPremium AS o_Source_BuildingPremium,
	LKP_WorkCatastropheExposureLimit_CPP_SBOP.LimitValue AS PolicyBlanketLimit_CPP_SBOP,
	LKP_WorkCatastropheExposureLimit_SMARTBlanket.LimitValue AS PolicyBlanketLimit_SMART,
	JNR_Deductible.LocationLimit,
	JNR_Deductible.LocationBuildingLimit,
	JNR_Deductible.LocationContentsLimit,
	JNR_Deductible.LocationBILimit,
	JNR_Deductible.BPPEarthquakeDeductiblePercentage,
	JNR_Deductible.BuildingEarthquakeDeductiblePercentage,
	JNR_Deductible.EarthquakeDeductiblePercentage,
	JNR_Deductible.PolicyBlanketDeductible,
	JNR_Deductible.LocationDeductible,
	JNR_Deductible.LocationBuildingDeductible,
	JNR_Deductible.LocationContentsDeductible,
	JNR_Deductible.LocationBIDeductible,
	JNR_Deductible.o_LocBIDeductibleBuilding AS LocBIDeductibleBuilding,
	JNR_Deductible.o_LocBIDeductibleContents AS LocBIDeductibleContents,
	-- *INF*: IIF(ISNULL(LocBIDeductibleBuilding),LocBIDeductibleContents,LocBIDeductibleBuilding)
	IFF(LocBIDeductibleBuilding IS NULL, LocBIDeductibleContents, LocBIDeductibleBuilding) AS v_LocBIDedu_Buld_Cont,
	LKP_WorkCatastropheExposureTransaction_PolicyBlanketPremium.PolicyBlanketPremium AS lkp_PolicyBlanketPremium,
	LKP_WorkCatastropheExposureTransaction_PlusPak.CoverageDescription AS lkp_CoverageDescription_PlusPak,
	LKP_WorkCatastropheExposureTransaction_Essential.CoverageDescription AS lkp_CoverageDescription_Essential,
	LKP_WorkCatastropheExposureTransaction_Elite.CoverageDescription AS lkp_CoverageDescription_Elite,
	LKP_WorkCatastropheExposureTransaction_WindCoverageFlag.WindCoverageFlag AS lkp_WindCoverageFlag,
	LKP_WorkCatastropheExposureTransaction_Earthquake.CoverageGroupDescription AS lkp_CoverageGroupDescription_Earthquake,
	LKP_WorkCatastropheExposureTransaction_LocationEarthquakePremium.LocationEarthquakePremium AS lkp_LocationEarthquakePremium,
	LKP_WorkCatastropheExposureTransaction_CD_EarthquakePersonalProperty.CoverageDescription AS lkp_CoverageDescription_EarthquakePersonalProperty,
	LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBusinessPersonalProperty.CoverageDescription AS lkp_CoverageDescription_EarthquakeBusinessPersonalProperty,
	LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBuilding.CoverageDescription AS lkp_CoverageDescription_EarthquakeBuilding,
	LKP_WorkCatastropheExposureTransaction_CD_Earthquake.CoverageDescription AS lkp_CoverageDescription_Earthquake,
	LKP_WorkCatastropheExposureTransaction_CD_ALS.DctCoverageTypeCode AS lkp_DctCoverageTypeCode,
	LKP_WorkCatastropheExposureTransaction_Terrorism.CoverageGroupDescription AS lkp_CoverageGroupDescription_Terrorism,
	LKP_WorkCatastropheExposureTransaction_BalnketDeductible.CoverageGroupDescription AS lkp_CoverageGroupDescription_BlanketDed,
	LKP_WorkCatastropheExposureTransaction_CD_BusinessPersonalProperty.CoverageDescription AS lkp_CoverageDescription_BusinessPersonalProperty,
	LKP_WorkCatastropheExposureTransaction_CGD_BusinessIncome.CoverageGroupDescription AS lkp_CoverageGroupDescription_BusinessIncome,
	-- *INF*: IIF((BusinessType = 'SBOP'  OR Not ISNULL(lkp_CoverageDescription_PlusPak)),'1','0')
	-- 
	IFF((BusinessType = 'SBOP' OR lkp_CoverageDescription_PlusPak IS NOT NULL), '1', '0') AS v_PlusPakFlag,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(lkp_CoverageDescription_Elite),'L',
	-- NOT ISNULL(lkp_CoverageDescription_Essential),'S',
	-- 'N')
	-- -- Flag Elite as 'L', Essential as 'S' and neither as 'N'
	-- 
	DECODE(
	    TRUE,
	    lkp_CoverageDescription_Elite IS NOT NULL, 'L',
	    lkp_CoverageDescription_Essential IS NOT NULL, 'S',
	    'N'
	) AS v_EliteFlag,
	-- *INF*: IIF(ISNULL(lkp_PolicyBlanketPremium), NULL, ROUND(lkp_PolicyBlanketPremium,0))
	IFF(lkp_PolicyBlanketPremium IS NULL, NULL, ROUND(lkp_PolicyBlanketPremium, 0)) AS v_PolicyBlanketPremium,
	-- *INF*: DECODE(TRUE,
	-- v_EliteFlag = 'L', 1.08,
	-- v_EliteFlag = 'S', 1.05,
	-- v_PlusPakFlag = '1', 1.05,
	-- 1.00)
	-- --Elite gets a modifier of 1.08, Essential and/or PlusPak get 1.05 else we default to 1
	DECODE(
	    TRUE,
	    v_EliteFlag = 'L', 1.08,
	    v_EliteFlag = 'S', 1.05,
	    v_PlusPakFlag = '1', 1.05,
	    1.00
	) AS v_Multiplier,
	-- *INF*: ROUND(LocationBuildingLimit*v_Multiplier, 0)
	-- 
	-- 
	-- --DECODE(TRUE, ISNULL(LocationBuildingLimit), 0,((BusinessType<>'SBOP' OR BusinessType<>'SMARTbusiness' OR BusinessType<>'Commercial Property') AND ISNULL(lkp_CoverageDescription_PlusPak)), LocationBuildingLimit,  ROUND(LocationBuildingLimit*1.05, 0))
	-- 
	-- --DECODE(TRUE, ISNULL(LocationBuildingLimit), 0,BusinessType<>'SBOP' AND ISNULL(lkp_CoverageDescription_PlusPak), LocationBuildingLimit,  ROUND(LocationBuildingLimit*1.05, 0))
	-- 
	-- 
	-- --DECODE(TRUE, ISNULL(LocationBuildingLimit), 0,BusinessType<>'SBOP' AND ISNULL(lkp_CoverageDescription_PlusPak), LocationBuildingLimit, BusinessType='SBOP', ROUND(LocationBuildingLimit*1.05, 0), 0)
	ROUND(LocationBuildingLimit * v_Multiplier, 0) AS v_AdjustedBuildingLimit,
	-- *INF*: ROUND(LocationContentsLimit*v_Multiplier, 0)
	-- --DECODE(TRUE,ISNULL(LocationContentsLimit),0,((BusinessType<>'SBOP' OR BusinessType<>'SMARTbusiness' OR BusinessType<>'Commercial Property') and ISNULL(lkp_CoverageDescription_PlusPak)), LocationContentsLimit, ROUND(LocationContentsLimit*1.05, 0))
	-- 
	-- --DECODE(TRUE, ISNULL(LocationContentsLimit),0, BusinessType<>'SBOP' and ISNULL(lkp_CoverageDescription_PlusPak), LocationContentsLimit,    BusinessType='SBOP',ROUND(LocationContentsLimit*1.05, 0),0)
	ROUND(LocationContentsLimit * v_Multiplier, 0) AS v_AdjustedContentsLimit,
	-- *INF*: ROUND(LocationBILimit*v_Multiplier, 0)
	-- 
	-- --DECODE(TRUE,ISNULL(LocationBILimit),0,((BusinessType<>'SBOP' OR BusinessType<>'SMARTbusiness' OR BusinessType<>'Commercial Property') and ISNULL(lkp_CoverageDescription_PlusPak)), LocationBILimit, ROUND(LocationBILimit*1.05, 0))
	-- 
	-- --DECODE(TRUE, ISNULL(LocationBILimit),0, BusinessType<>'SBOP' and ISNULL(lkp_CoverageDescription_PlusPak), LocationBILimit, BusinessType='SBOP', ROUND(LocationBILimit*1.05, 0),0)
	ROUND(LocationBILimit * v_Multiplier, 0) AS v_AdjustedBILimit,
	-- *INF*: IIF(ISNULL(v_AdjustedBuildingLimit),0,v_AdjustedBuildingLimit)+
	-- IIF(ISNULL(v_AdjustedContentsLimit),0,v_AdjustedContentsLimit)+
	-- IIF(ISNULL(v_AdjustedBILimit),0,v_AdjustedBILimit)
	IFF(v_AdjustedBuildingLimit IS NULL, 0, v_AdjustedBuildingLimit) + IFF(v_AdjustedContentsLimit IS NULL, 0, v_AdjustedContentsLimit) + IFF(v_AdjustedBILimit IS NULL, 0, v_AdjustedBILimit) AS v_AdjustedTotalInsuredValue,
	-- *INF*: IIF(ISNULL(lkp_WindCoverageFlag), '0', '1')
	IFF(lkp_WindCoverageFlag IS NULL, '0', '1') AS v_LocationWindCoverageFlag,
	-- *INF*: IIF(ISNULL(lkp_CoverageGroupDescription_Earthquake), '0', '1')
	IFF(lkp_CoverageGroupDescription_Earthquake IS NULL, '0', '1') AS v_LocationEarthquakeFlag,
	-- *INF*: IIF(ISNULL(lkp_LocationEarthquakePremium), NULL, ROUND(lkp_LocationEarthquakePremium,0))
	IFF(lkp_LocationEarthquakePremium IS NULL, NULL, ROUND(lkp_LocationEarthquakePremium, 0)) AS v_LocationEarthquakePremium,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(LocationContentsLimit), 0,
	-- (BusinessType='SBOP' AND (NOT ISNULL(lkp_CoverageDescription_EarthquakePersonalProperty)
	-- OR NOT ISNULL(lkp_CoverageDescription_EarthquakeBusinessPersonalProperty))
	-- ), LocationContentsLimit,
	-- (BusinessType='Commercial Property' )AND NOT ISNULL(lkp_CoverageDescription_EarthquakePersonalProperty), LocationContentsLimit
	-- ,0)
	DECODE(
	    TRUE,
	    LocationContentsLimit IS NULL, 0,
	    (BusinessType = 'SBOP' AND (lkp_CoverageDescription_EarthquakePersonalProperty IS NULL OR lkp_CoverageDescription_EarthquakeBusinessPersonalProperty IS NOT NOT NULL)), LocationContentsLimit,
	    (BusinessType = 'Commercial Property') AND lkp_CoverageDescription_EarthquakePersonalProperty IS NOT NULL, LocationContentsLimit,
	    0
	) AS v_Earthquake_LocationContentsLimit_CP_SBOP,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(LocationBuildingLimit), 0,
	-- (BusinessType='SBOP' OR BusinessType='Commercial Property') AND NOT ISNULL(lkp_CoverageDescription_EarthquakeBuilding), LocationBuildingLimit
	-- ,0)
	DECODE(
	    TRUE,
	    LocationBuildingLimit IS NULL, 0,
	    (BusinessType = 'SBOP' OR BusinessType = 'Commercial Property') AND lkp_CoverageDescription_EarthquakeBuilding IS NOT NULL, LocationBuildingLimit,
	    0
	) AS v_Earthquake_LocationBuildingLimit_CP_SBOP,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(LocationBuildingLimit),0,
	-- BusinessType='SMARTbusiness' AND NOT ISNULL(lkp_CoverageDescription_Earthquake), LocationBuildingLimit,
	-- 0)
	DECODE(
	    TRUE,
	    LocationBuildingLimit IS NULL, 0,
	    BusinessType = 'SMARTbusiness' AND lkp_CoverageDescription_Earthquake IS NOT NULL, LocationBuildingLimit,
	    0
	) AS v_Earthquake_LocationBuildingLimit_SMT,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(LocationContentsLimit),0,
	-- BusinessType='SMARTbusiness' AND NOT ISNULL(lkp_CoverageDescription_Earthquake), LocationContentsLimit,
	-- 0)
	DECODE(
	    TRUE,
	    LocationContentsLimit IS NULL, 0,
	    BusinessType = 'SMARTbusiness' AND lkp_CoverageDescription_Earthquake IS NOT NULL, LocationContentsLimit,
	    0
	) AS v_Earthquake_LocationContentsLimit_SMT,
	-- *INF*: DECODE(TRUE,
	-- (v_Earthquake_LocationContentsLimit_CP_SBOP+v_Earthquake_LocationBuildingLimit_CP_SBOP) != 0, v_Earthquake_LocationContentsLimit_CP_SBOP+v_Earthquake_LocationBuildingLimit_CP_SBOP,
	-- (v_Earthquake_LocationBuildingLimit_SMT+v_Earthquake_LocationContentsLimit_SMT) != 0, v_Earthquake_LocationBuildingLimit_SMT+v_Earthquake_LocationContentsLimit_SMT,
	-- NULL)
	-- 
	-- 
	DECODE(
	    TRUE,
	    (v_Earthquake_LocationContentsLimit_CP_SBOP + v_Earthquake_LocationBuildingLimit_CP_SBOP) != 0, v_Earthquake_LocationContentsLimit_CP_SBOP + v_Earthquake_LocationBuildingLimit_CP_SBOP,
	    (v_Earthquake_LocationBuildingLimit_SMT + v_Earthquake_LocationContentsLimit_SMT) != 0, v_Earthquake_LocationBuildingLimit_SMT + v_Earthquake_LocationContentsLimit_SMT,
	    NULL
	) AS v_LocationEarthquakeLimit,
	-- *INF*: Decode(TRUE,BusinessType = 'SBOP' OR BusinessType ='SMARTbusiness' OR NOT ISNULL(lkp_DctCoverageTypeCode) ,1,0)
	-- 
	-- --IIF(ISNULL(lkp_DctCoverageTypeCode), '0', '1')
	Decode(
	    TRUE,
	    BusinessType = 'SBOP' OR BusinessType = 'SMARTbusiness' OR lkp_DctCoverageTypeCode IS NOT NULL, 1,
	    0
	) AS v_ActualLossSustainedCoverageFlag,
	-- *INF*: IIF(ISNULL(lkp_CoverageGroupDescription_Terrorism), '0', '1')
	IFF(lkp_CoverageGroupDescription_Terrorism IS NULL, '0', '1') AS v_TerrorismFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	SourceSystemId AS o_SourceSystemId,
	PolicyKey AS o_PolicyKey,
	LocationNumber AS o_LocationNumber,
	BuildingNumber AS o_BuildingNumber,
	BusinessType AS o_BusinessType,
	InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription,
	v_PolicyBlanketPremium AS o_PolicyBlanketPremium,
	-- *INF*: DECODE(BusinessType,
	-- 'Commercial Property',
	-- PolicyBlanketLimit_CPP_SBOP,
	-- 'SBOP',
	-- PolicyBlanketLimit_CPP_SBOP,
	-- 'SMARTbusiness',
	-- PolicyBlanketLimit_SMART,
	-- 0)
	DECODE(
	    BusinessType,
	    'Commercial Property', PolicyBlanketLimit_CPP_SBOP,
	    'SBOP', PolicyBlanketLimit_CPP_SBOP,
	    'SMARTbusiness', PolicyBlanketLimit_SMART,
	    0
	) AS v_PolicyBlanketLimit,
	v_PolicyBlanketLimit AS o_PolicyBlanketLimit,
	-- *INF*: DECODE(TRUE, BusinessType='SBOP' AND NOT ISNULL(lkp_CoverageGroupDescription_BlanketDed), PolicyBlanketDeductible,NULL )
	-- 
	DECODE(
	    TRUE,
	    BusinessType = 'SBOP' AND lkp_CoverageGroupDescription_BlanketDed IS NOT NULL, PolicyBlanketDeductible,
	    NULL
	) AS o_PolicyBlanketDeductible,
	StreetAddress AS o_RatingAddress,
	RatingCity AS o_RatingCity,
	RatingStateProvinceAbbreviation AS o_RatingStateProvinceAbbreviation,
	RatingPostalCode AS o_RatingPostalCode,
	RatingCounty AS o_RatingCounty,
	-- *INF*: IIF(ISNULL(ConstructionCode),'N/A',ConstructionCode)
	IFF(ConstructionCode IS NULL, 'N/A', ConstructionCode) AS o_ConstructionClass,
	-- *INF*: IIF(ISNULL(YearBuilt),'0000',YearBuilt)
	IFF(YearBuilt IS NULL, '0000', YearBuilt) AS o_YearBuilt,
	-- *INF*: IIF(ISNULL(BuildingPremium),0, BuildingPremium)
	IFF(BuildingPremium IS NULL, 0, BuildingPremium) AS v_BuildingPremium,
	-- *INF*: IIF(v_BuildingPremium=0,
	-- IIF(ISNULL(v_PolicyBlanketPremium),0,v_PolicyBlanketPremium),v_BuildingPremium)
	IFF(
	    v_BuildingPremium = 0,
	    IFF(
	        v_PolicyBlanketPremium IS NULL, 0, v_PolicyBlanketPremium
	    ),
	    v_BuildingPremium
	) AS o_BuildingPremium,
	LocationLimit AS o_LocationLimit,
	LocationDeductible AS o_LocationDeductible,
	LocationBuildingLimit AS o_LocationBuildingLimit,
	LocationBuildingDeductible AS o_LocationBuildingDeductible,
	LocationContentsLimit AS o_LocationContentsLimit,
	-- *INF*: LocationContentsDeductible
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --BusinessType='SMARTbusiness' and NOT ISNULL(lkp_CoverageDescription_BusinessPersonalProperty), LocationBuildingDeductible,
	-- --IN (BusinessType, 'Commercial Property','SBOP'), LocationContentsDeductible,
	-- --NULL)
	LocationContentsDeductible AS o_LocationContentsDeductible,
	LocationBILimit AS o_LocationBILimit,
	-- *INF*: DECODE(TRUE,
	-- IN(BusinessType,'SMARTbusiness') AND NOT ISNULL(lkp_CoverageGroupDescription_BusinessIncome),
	-- LocationBIDeductible,
	-- BusinessType='SBOP', LocationBIDeductible,
	-- BusinessType= 'Commercial Property' AND NOT ISNULL(lkp_CoverageGroupDescription_BusinessIncome),v_LocBIDedu_Buld_Cont,
	-- NULL)
	DECODE(
	    TRUE,
	    BusinessType IN ('SMARTbusiness') AND lkp_CoverageGroupDescription_BusinessIncome IS NOT NULL, LocationBIDeductible,
	    BusinessType = 'SBOP', LocationBIDeductible,
	    BusinessType = 'Commercial Property' AND lkp_CoverageGroupDescription_BusinessIncome IS NOT NULL, v_LocBIDedu_Buld_Cont,
	    NULL
	) AS o_LocationBIDeductible,
	v_PlusPakFlag AS o_PlusPakFlag,
	-- *INF*: IIF(v_AdjustedBuildingLimit=0, NULL, v_AdjustedBuildingLimit)
	IFF(v_AdjustedBuildingLimit = 0, NULL, v_AdjustedBuildingLimit) AS o_AdjustedBuildingLimit,
	-- *INF*: IIF(v_AdjustedContentsLimit=0, NULL, v_AdjustedContentsLimit)
	IFF(v_AdjustedContentsLimit = 0, NULL, v_AdjustedContentsLimit) AS o_AdjustedContentsLimit,
	-- *INF*: IIF(v_AdjustedBILimit=0, NULL, v_AdjustedBILimit)
	IFF(v_AdjustedBILimit = 0, NULL, v_AdjustedBILimit) AS o_AdjustedBILimit,
	v_AdjustedTotalInsuredValue AS o_AdjustedTotalInsuredValue,
	v_LocationWindCoverageFlag AS o_LocationWindCoverageFlag,
	v_LocationEarthquakeFlag AS o_LocationEarthquakeFlag,
	v_LocationEarthquakePremium AS o_LocationEarthquakePremium,
	v_LocationEarthquakeLimit AS o_LocationEarthquakeLimit,
	-- *INF*: DECODE(TRUE,
	-- BusinessType='Commercial Property' and
	-- BPPEarthquakeDeductiblePercentage!=0 AND BuildingEarthquakeDeductiblePercentage!=0 , IIF(ISNULL(BuildingEarthquakeDeductiblePercentage/100*LocationBuildingLimit), 0,BuildingEarthquakeDeductiblePercentage/100*LocationBuildingLimit) ,
	-- 0)
	DECODE(
	    TRUE,
	    BusinessType = 'Commercial Property' and BPPEarthquakeDeductiblePercentage != 0 AND BuildingEarthquakeDeductiblePercentage != 0, IFF(
	        BuildingEarthquakeDeductiblePercentage / 100 * LocationBuildingLimit IS NULL, 0,
	        BuildingEarthquakeDeductiblePercentage / 100 * LocationBuildingLimit
	    ),
	    0
	) AS v_BuildingEarthquakeDeductible,
	-- *INF*: DECODE(TRUE,
	-- BusinessType='Commercial Property' and
	-- BPPEarthquakeDeductiblePercentage!=0 AND BuildingEarthquakeDeductiblePercentage!=0 , IIF(ISNULL(BPPEarthquakeDeductiblePercentage/100*LocationContentsLimit), 0,BPPEarthquakeDeductiblePercentage/100*LocationContentsLimit) ,
	-- 0)
	DECODE(
	    TRUE,
	    BusinessType = 'Commercial Property' and BPPEarthquakeDeductiblePercentage != 0 AND BuildingEarthquakeDeductiblePercentage != 0, IFF(
	        BPPEarthquakeDeductiblePercentage / 100 * LocationContentsLimit IS NULL, 0,
	        BPPEarthquakeDeductiblePercentage / 100 * LocationContentsLimit
	    ),
	    0
	) AS v_BPPEarthquakeDeductible,
	-- *INF*: DECODE(TRUE,
	-- BusinessType='Commercial Property', DECODE(TRUE,
	-- BPPEarthquakeDeductiblePercentage!=0 AND BuildingEarthquakeDeductiblePercentage!=0 , GREATEST(v_BuildingEarthquakeDeductible , v_BPPEarthquakeDeductible),
	-- BPPEarthquakeDeductiblePercentage!=0 AND BuildingEarthquakeDeductiblePercentage=0 ,  BPPEarthquakeDeductiblePercentage/100*LocationContentsLimit,
	-- BPPEarthquakeDeductiblePercentage=0 AND BuildingEarthquakeDeductiblePercentage!=0 ,  BuildingEarthquakeDeductiblePercentage/100*LocationBuildingLimit ,
	-- NULL),
	-- BusinessType='SMARTbusiness', DECODE(TRUE,
	-- EarthquakeDeductiblePercentage!=0, EarthquakeDeductiblePercentage/100*v_LocationEarthquakeLimit ,
	-- NULL)
	-- ,BusinessType='SBOP',DECODE(TRUE,
	-- BPPEarthquakeDeductiblePercentage!=0 AND BuildingEarthquakeDeductiblePercentage!=0 ,BuildingEarthquakeDeductiblePercentage/100*LocationBuildingLimit ,
	-- BPPEarthquakeDeductiblePercentage!=0 AND BuildingEarthquakeDeductiblePercentage=0 ,  BPPEarthquakeDeductiblePercentage/100*LocationContentsLimit,
	-- BPPEarthquakeDeductiblePercentage=0 AND BuildingEarthquakeDeductiblePercentage!=0 ,  
	-- BuildingEarthquakeDeductiblePercentage/100*LocationBuildingLimit,
	-- NULL),
	-- NULL)
	DECODE(
	    TRUE,
	    BusinessType = 'Commercial Property', DECODE(
	        TRUE,
	        BPPEarthquakeDeductiblePercentage != 0 AND BuildingEarthquakeDeductiblePercentage != 0, GREATEST(v_BuildingEarthquakeDeductible, v_BPPEarthquakeDeductible),
	        BPPEarthquakeDeductiblePercentage != 0 AND BuildingEarthquakeDeductiblePercentage = 0, BPPEarthquakeDeductiblePercentage / 100 * LocationContentsLimit,
	        BPPEarthquakeDeductiblePercentage = 0 AND BuildingEarthquakeDeductiblePercentage != 0, BuildingEarthquakeDeductiblePercentage / 100 * LocationBuildingLimit,
	        NULL
	    ),
	    BusinessType = 'SMARTbusiness', DECODE(
	        TRUE,
	        EarthquakeDeductiblePercentage != 0, EarthquakeDeductiblePercentage / 100 * v_LocationEarthquakeLimit,
	        NULL
	    ),
	    BusinessType = 'SBOP', DECODE(
	        TRUE,
	        BPPEarthquakeDeductiblePercentage != 0 AND BuildingEarthquakeDeductiblePercentage != 0, BuildingEarthquakeDeductiblePercentage / 100 * LocationBuildingLimit,
	        BPPEarthquakeDeductiblePercentage != 0 AND BuildingEarthquakeDeductiblePercentage = 0, BPPEarthquakeDeductiblePercentage / 100 * LocationContentsLimit,
	        BPPEarthquakeDeductiblePercentage = 0 AND BuildingEarthquakeDeductiblePercentage != 0, BuildingEarthquakeDeductiblePercentage / 100 * LocationBuildingLimit,
	        NULL
	    ),
	    NULL
	) AS v_LocationEarthquakeDeductible,
	-- *INF*: IIF(v_LocationEarthquakeDeductible=0, NULL, v_LocationEarthquakeDeductible)
	IFF(v_LocationEarthquakeDeductible = 0, NULL, v_LocationEarthquakeDeductible) AS o_LocationEarthquakeDeductible,
	AgencyCode AS o_AgencyCode,
	StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription,
	InsuranceSegmentDescription AS o_InsuranceSegmentDescription,
	v_ActualLossSustainedCoverageFlag AS o_ActualLossSustainedCoverageFlag,
	InsuredName AS o_InsuredName,
	CustomerNumber AS o_CustomerNumber,
	PolicyEffectiveDate AS o_PolicyEffectiveDate,
	PolicyExpirationDate AS o_PolicyExpirationDate,
	OccupancyCode AS o_OccupancyCode,
	SicCode AS o_SicCode,
	v_TerrorismFlag AS o_TerrorismFlag,
	ProcessDate AS o_ProcessDate,
	v_EliteFlag AS o_EliteFlag
	FROM JNR_Deductible
	LEFT JOIN LKP_WorkCatastropheExposureLimit_CPP_SBOP
	ON LKP_WorkCatastropheExposureLimit_CPP_SBOP.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureLimit_CPP_SBOP.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureLimit_SMARTBlanket
	ON LKP_WorkCatastropheExposureLimit_SMARTBlanket.PolicyKey = JNR_Deductible.PolicyKey
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_BalnketDeductible
	ON LKP_WorkCatastropheExposureTransaction_BalnketDeductible.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_BalnketDeductible.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_CD_ALS
	ON LKP_WorkCatastropheExposureTransaction_CD_ALS.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_CD_ALS.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_CD_ALS.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_CD_ALS.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_CD_BusinessPersonalProperty
	ON LKP_WorkCatastropheExposureTransaction_CD_BusinessPersonalProperty.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_CD_BusinessPersonalProperty.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_CD_BusinessPersonalProperty.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_CD_BusinessPersonalProperty.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_CD_Earthquake
	ON LKP_WorkCatastropheExposureTransaction_CD_Earthquake.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_CD_Earthquake.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_CD_Earthquake.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_CD_Earthquake.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBuilding
	ON LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBuilding.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBuilding.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBuilding.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBuilding.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBusinessPersonalProperty
	ON LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBusinessPersonalProperty.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBusinessPersonalProperty.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBusinessPersonalProperty.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_CD_EarthquakeBusinessPersonalProperty.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_CD_EarthquakePersonalProperty
	ON LKP_WorkCatastropheExposureTransaction_CD_EarthquakePersonalProperty.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_CD_EarthquakePersonalProperty.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_CD_EarthquakePersonalProperty.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_CD_EarthquakePersonalProperty.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_CGD_BusinessIncome
	ON LKP_WorkCatastropheExposureTransaction_CGD_BusinessIncome.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_CGD_BusinessIncome.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_CGD_BusinessIncome.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_CGD_BusinessIncome.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_Earthquake
	ON LKP_WorkCatastropheExposureTransaction_Earthquake.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_Earthquake.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_Earthquake.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_Earthquake.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_Elite
	ON LKP_WorkCatastropheExposureTransaction_Elite.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_Elite.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_Essential
	ON LKP_WorkCatastropheExposureTransaction_Essential.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_Essential.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_LocationEarthquakePremium
	ON LKP_WorkCatastropheExposureTransaction_LocationEarthquakePremium.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_LocationEarthquakePremium.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_LocationEarthquakePremium.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_LocationEarthquakePremium.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_PlusPak
	ON LKP_WorkCatastropheExposureTransaction_PlusPak.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_PlusPak.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_PolicyBlanketPremium
	ON LKP_WorkCatastropheExposureTransaction_PolicyBlanketPremium.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_PolicyBlanketPremium.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_Terrorism
	ON LKP_WorkCatastropheExposureTransaction_Terrorism.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_Terrorism.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_Terrorism.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_Terrorism.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_WindCoverageFlag
	ON LKP_WorkCatastropheExposureTransaction_WindCoverageFlag.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_WindCoverageFlag.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_WindCoverageFlag.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_WindCoverageFlag.BusinessType = JNR_Deductible.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_YearBuilt
	ON LKP_WorkCatastropheExposureTransaction_YearBuilt.PolicyKey = JNR_Deductible.PolicyKey AND LKP_WorkCatastropheExposureTransaction_YearBuilt.LocationNumber = JNR_Deductible.LocationNumber AND LKP_WorkCatastropheExposureTransaction_YearBuilt.BuildingNumber = JNR_Deductible.BuildingNumber AND LKP_WorkCatastropheExposureTransaction_YearBuilt.BusinessType = JNR_Deductible.BusinessType
),
Fil_Null_Values AS (
	SELECT
	o_AuditId AS AuditId, 
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_SourceSystemId AS SourceSystemId, 
	o_PolicyKey AS PolicyKey, 
	o_LocationNumber AS LocationNumber, 
	o_BuildingNumber AS BuildingNumber, 
	o_BusinessType AS BusinessType, 
	o_InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription, 
	o_PolicyBlanketPremium AS PolicyBlanketPremium, 
	o_PolicyBlanketLimit AS PolicyBlanketLimit, 
	o_PolicyBlanketDeductible AS PolicyBlanketDeductible, 
	o_RatingAddress AS RatingAddress, 
	o_RatingCity AS RatingCity, 
	o_RatingStateProvinceAbbreviation AS RatingStateProvinceAbbreviation, 
	o_RatingPostalCode AS RatingPostalCode, 
	o_RatingCounty AS RatingCounty, 
	o_ConstructionClass AS ConstructionClass, 
	o_YearBuilt AS YearBuilt, 
	o_BuildingPremium AS BuildingPremium, 
	o_LocationLimit AS LocationLimit, 
	o_LocationDeductible AS LocationDeductible, 
	o_LocationBuildingLimit AS LocationBuildingLimit, 
	o_LocationBuildingDeductible AS LocationBuildingDeductible, 
	o_LocationContentsLimit AS LocationContentsLimit, 
	o_LocationContentsDeductible AS LocationContentsDeductible, 
	o_LocationBILimit AS LocationBILimit, 
	o_LocationBIDeductible AS LocationBIDeductible, 
	o_PlusPakFlag AS PlusPakFlag, 
	o_AdjustedBuildingLimit AS AdjustedBuildingLimit, 
	o_AdjustedContentsLimit AS AdjustedContentsLimit, 
	o_AdjustedBILimit AS AdjustedBILimit, 
	o_AdjustedTotalInsuredValue AS AdjustedTotalInsuredValue, 
	o_LocationWindCoverageFlag AS LocationWindCoverageFlag, 
	o_LocationEarthquakeFlag AS LocationEarthquakeFlag, 
	o_LocationEarthquakePremium AS LocationEarthquakePremium, 
	o_LocationEarthquakeLimit AS LocationEarthquakeLimit, 
	o_LocationEarthquakeDeductible AS LocationEarthquakeDeductible, 
	o_AgencyCode AS AgencyCode, 
	o_StrategicProfitCenterDescription AS StrategicProfitCenterDescription, 
	o_InsuranceSegmentDescription AS InsuranceSegmentDescription, 
	o_ActualLossSustainedCoverageFlag AS ActualLossSustainedCoverageFlag, 
	o_InsuredName AS InsuredName, 
	o_CustomerNumber AS CustomerNumber, 
	o_PolicyEffectiveDate AS PolicyEffectiveDate, 
	o_PolicyExpirationDate AS PolicyExpirationDate, 
	o_OccupancyCode AS OccupancyCode, 
	o_SicCode AS SicCode, 
	o_TerrorismFlag AS TerrorismFlag, 
	o_ProcessDate AS ProcessDate, 
	o_Source_BuildingPremium AS Source_BuildingPremium, 
	o_EliteFlag AS EliteFlag
	FROM EXP_MetaData
	WHERE IIF(ISNULL(Source_BuildingPremium),0,Source_BuildingPremium)!=0
OR
IIF(ISNULL(LocationLimit),0,LocationLimit)!=0
OR
IIF(ISNULL(LocationDeductible),0,LocationDeductible)!=0 
OR
IIF(ISNULL(LocationBuildingLimit),0,LocationBuildingLimit)!=0
OR
IIF(ISNULL(LocationBuildingDeductible),0,LocationBuildingDeductible)!=0
OR
IIF(ISNULL(LocationContentsLimit),0,LocationContentsLimit)!=0 
OR
IIF(ISNULL(LocationContentsDeductible),0,LocationContentsDeductible)!=0 
OR
IIF(ISNULL(LocationBILimit),0,LocationBILimit)!=0 
OR
IIF(ISNULL(LocationBIDeductible),0,LocationBIDeductible)!=0
),
WorkCatastropheExposureBuilding AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureBuilding;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureBuilding
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, PolicyKey, LocationNumber, BuildingNumber, BusinessType, InsuranceReferenceLegalEntityDescription, PolicyBlanketPremium, PolicyBlanketLimit, PolicyBlanketDeductible, RatingAddress, RatingCity, RatingStateProvinceAbbreviation, RatingPostalCode, RatingCounty, ConstructionClass, YearBuilt, BuildingPremium, LocationLimit, LocationDeductible, LocationBuildingLimit, LocationBuildingDeductible, LocationContentsLimit, LocationContentsDeductible, LocationBILimit, LocationBIDeductible, AdjustedBuildingLimit, AdjustedContentsLimit, AdjustedBILimit, AdjustedTotalInsuredValue, LocationWindCoverageFlag, LocationEarthquakeFlag, LocationEarthquakePremium, LocationEarthquakeLimit, LocationEarthquakeDeductible, AgencyCode, StrategicProfitCenterDescription, InsuranceSegmentDescription, PlusPakFlag, ActualLossSustainedCoverageFlag, InsuredName, CustomerNumber, PolicyEffectiveDate, PolicyExpirationDate, OccupancyCode, SicCode, TerrorismFlag, ProcessDate, EliteFlag)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SOURCESYSTEMID, 
	POLICYKEY, 
	LOCATIONNUMBER, 
	BUILDINGNUMBER, 
	BUSINESSTYPE, 
	INSURANCEREFERENCELEGALENTITYDESCRIPTION, 
	POLICYBLANKETPREMIUM, 
	POLICYBLANKETLIMIT, 
	POLICYBLANKETDEDUCTIBLE, 
	RATINGADDRESS, 
	RATINGCITY, 
	RATINGSTATEPROVINCEABBREVIATION, 
	RATINGPOSTALCODE, 
	RATINGCOUNTY, 
	CONSTRUCTIONCLASS, 
	YEARBUILT, 
	BUILDINGPREMIUM, 
	LOCATIONLIMIT, 
	LOCATIONDEDUCTIBLE, 
	LOCATIONBUILDINGLIMIT, 
	LOCATIONBUILDINGDEDUCTIBLE, 
	LOCATIONCONTENTSLIMIT, 
	LOCATIONCONTENTSDEDUCTIBLE, 
	LOCATIONBILIMIT, 
	LOCATIONBIDEDUCTIBLE, 
	ADJUSTEDBUILDINGLIMIT, 
	ADJUSTEDCONTENTSLIMIT, 
	ADJUSTEDBILIMIT, 
	ADJUSTEDTOTALINSUREDVALUE, 
	LOCATIONWINDCOVERAGEFLAG, 
	LOCATIONEARTHQUAKEFLAG, 
	LOCATIONEARTHQUAKEPREMIUM, 
	LOCATIONEARTHQUAKELIMIT, 
	LOCATIONEARTHQUAKEDEDUCTIBLE, 
	AGENCYCODE, 
	STRATEGICPROFITCENTERDESCRIPTION, 
	INSURANCESEGMENTDESCRIPTION, 
	PLUSPAKFLAG, 
	ACTUALLOSSSUSTAINEDCOVERAGEFLAG, 
	INSUREDNAME, 
	CUSTOMERNUMBER, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	OCCUPANCYCODE, 
	SICCODE, 
	TERRORISMFLAG, 
	PROCESSDATE, 
	ELITEFLAG
	FROM Fil_Null_Values
),