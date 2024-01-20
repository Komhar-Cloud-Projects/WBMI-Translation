WITH
SQ_WorkCatastropheExposureTransaction AS (
	select WCET.SourceSystemID,
	POL.pol_key,
	WCET.VehicleNumber,
	WCET.BusinessType,
	IRLE.InsuranceReferenceLegalEntityDescription,
	RL.StreetAddress,
	RL.RatingCity,
	SS.state_code,
	RL.ZipPostalCode,
	RL.RatingCounty,
	CDCA.VehicleYear,
	PT.PremiumTransactionAmount,
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
	WCET.AddToLocationPremiumFlag, 
	WCET.TerrorismRiskIndicator 
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction WCET
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on WCET.PremiumTransactionAKId=PT.PremiumTransactionAKID
	and PT.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKID=RC.RatingCoverageAKID
	and RC.EffectiveDate=PT.EffectiveDate 
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	on POL.pol_ak_id=RL.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer CC
	on CC.contract_cust_ak_id=POL.contract_cust_ak_id
	and CC.crrnt_snpsht_flag=1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency AGY
	on AGY.AgencyAKID=POL.AgencyAKId
	and AGY.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_business_classification_code SBCC
	on SBCC.sup_bus_class_code_id=POL.sup_bus_class_code_id 
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state SS
	on (CASE WHEN len(SS.state_abbrev) = 1 THEN '0' + SS.state_abbrev ELSE SS.state_abbrev END) = (CASE WHEN len(RL.StateProvinceCode) = 1 THEN '0' + RL.StateProvinceCode ELSE RL.StateProvinceCode END)
	and SS.crrnt_snpsht_flag = 1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	and ISG.InsuranceSegmentAbbreviation = 'CL'
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC
	on POL.StrategicProfitCenterAKId=SPC.StrategicProfitCenterAKId
	and SPC.CurrentSnapshotFlag=1 
	and SPC.StrategicProfitCenterDescription IN ('West Bend Commercial Lines', 'NSI')
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity IRLE
	on SPC.InsuranceReferenceLegalEntityId = IRLE.InsuranceReferenceLegalEntityId 
	and IRLE.CurrentSnapshotFlag = 1
	
	left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA
	on CDCA.PremiumTransactionId=PT.PremiumTransactionId
	where WCET.BusinessType = 'Commercial Auto'
	AND WCET.VehicleNumber > 0
	
	
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Cal AS (
	SELECT
	SourceSystemId,
	pol_key,
	VehicleNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	StreetAddress,
	RatingCity,
	state_code,
	ZipPostalCode,
	RatingCounty,
	VehicleYear,
	PremiumTransactionAmount,
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
	AddToLocationPremiumFlag,
	TerrorismRiskIndicator,
	pol_key AS o_PolicyKey,
	-- *INF*: IIF(state_code='N/A', '', RTRIM(state_code))
	IFF(state_code = 'N/A', '', RTRIM(state_code)) AS o_RatingStateProvinceAbbreviation,
	-- *INF*: SUBSTR(LTRIM(ZipPostalCode), 0, 5)
	SUBSTR(LTRIM(ZipPostalCode), 0, 5) AS o_RatingPostalCode,
	-- *INF*: IIF(ISNULL(VehicleYear), '', VehicleYear)
	IFF(VehicleYear IS NULL, '', VehicleYear) AS o_ModelYear,
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
AGG_VehiclePremium AS (
	SELECT
	SourceSystemId,
	o_PolicyKey AS PolicyKey,
	VehicleNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	StreetAddress,
	RatingCity,
	o_RatingStateProvinceAbbreviation AS RatingStateProvinceAbbreviation,
	o_RatingPostalCode AS RatingPostalCode,
	RatingCounty,
	o_ModelYear AS ModelYear,
	PremiumTransactionAmount,
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
	AddToLocationPremiumFlag,
	TerrorismRiskIndicator,
	-- *INF*: SUM(PremiumTransactionAmount, AddToLocationPremiumFlag='T')
	SUM(PremiumTransactionAmount, AddToLocationPremiumFlag = 'T') AS o_VehiclePremium
	FROM EXP_Cal
	GROUP BY PolicyKey, VehicleNumber, BusinessType
),
LKP_WorkCatastropheExposureDeductible AS (
	SELECT
	DeductibleValue,
	PolicyKey,
	VehicleNumber,
	BusinessType
	FROM (
		SELECT D.PolicyKey AS PolicyKey , D.VehicleNumber as VehicleNumber , D.BusinessType as BusinessType, 
		 RelevantDeductibles.TargetColumn AS TargetColumn,
		 (CASE WHEN RelevantDeductibles.CalculationRule = 'MAX' THEN MAX(CONVERT(BIGINT, D.DeductibleValue)) 
		  WHEN RelevantDeductibles.CalculationRule = 'MIN' THEN MIN(CONVERT(BIGINT, D.DeductibleValue)) 
		  WHEN RelevantDeductibles.CalculationRule = 'SUM' THEN SUM(CONVERT(BIGINT, D.DeductibleValue)) 
		  ELSE 0 
		 END) AS DeductibleValue
		FROM  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureDeductible D
		INNER JOIN (SELECT DISTINCT BusinessType, DeductibleType, CalculationRule, TargetColumn 
		  FROM  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupCatastropheExposureDeductibleRule 
		  WHERE TargetColumn = 'Loc Dedctbl' 
		  AND SourceSystemId = 'DCT') RelevantDeductibles ON D.BusinessType = RelevantDeductibles.BusinessType AND D.DeductibleType = RelevantDeductibles.DeductibleType
		WHERE D.BusinessType = 'Commercial Auto'
		GROUP BY D.PolicyKey, D.VehicleNumber, D.BusinessType, RelevantDeductibles.TargetColumn, RelevantDeductibles.CalculationRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,VehicleNumber,BusinessType ORDER BY DeductibleValue) = 1
),
LKP_WorkCatastropheExposureLimit AS (
	SELECT
	LimitValue,
	PolicyKey,
	VehicleNumber,
	BusinessType
	FROM (
		SELECT L.PolicyKey as PolicyKey, L.VehicleNumber as VehicleNumber, L.BusinessType as BusinessType, 
		 RelevantLimits.TargetColumn AS TargetColumn,
		 (CASE WHEN RelevantLimits.CalculationRule = 'MAX' THEN MAX(CONVERT(BIGINT, L.LimitValue)) 
		  WHEN RelevantLimits.CalculationRule = 'MIN' THEN MIN(CONVERT(BIGINT, L.LimitValue)) 
		  WHEN RelevantLimits.CalculationRule = 'SUM' THEN SUM(CONVERT(BIGINT, L.LimitValue)) 
		  ELSE 0 
		 END) AS LimitValue
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureLimit L
		INNER JOIN (SELECT DISTINCT BusinessType, LimitType, CalculationRule, TargetColumn 
		  FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupCatastropheExposureLimitRule 
		  WHERE TargetColumn = 'Loc Limit' 
		  AND SourceSystemId = 'DCT') RelevantLimits ON L.BusinessType = RelevantLimits.BusinessType AND L.LimitType = RelevantLimits.LimitType
		WHERE L.BusinessType = 'Commercial Auto'
		GROUP BY L.PolicyKey, L.VehicleNumber, L.BusinessType, RelevantLimits.TargetColumn, RelevantLimits.CalculationRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,VehicleNumber,BusinessType ORDER BY LimitValue) = 1
),
LKP_WorkCatastropheExposureTransaction_ALS AS (
	SELECT
	DCTCoverageTypeCode,
	PolicyKey,
	VehicleNumber,
	BusinessType
	FROM (
		SELECT 
			DCTCoverageTypeCode,
			PolicyKey,
			VehicleNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE DctCoverageTypeCode = 'ALS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,VehicleNumber,BusinessType ORDER BY DCTCoverageTypeCode) = 1
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
LKP_WorkCatastropheExposureTransaction_Terrorismflag AS (
	SELECT
	PolicyKey,
	BusinessType,
	TerrorismRiskIndicator,
	i_PolicyKey,
	i_BusinessType
	FROM (
		SELECT 
			PolicyKey,
			BusinessType,
			TerrorismRiskIndicator,
			i_PolicyKey,
			i_BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE TerrorismRiskIndicator = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,BusinessType ORDER BY PolicyKey) = 1
),
EXP_MetaData AS (
	SELECT
	AGG_VehiclePremium.SourceSystemId,
	AGG_VehiclePremium.PolicyKey,
	AGG_VehiclePremium.VehicleNumber,
	AGG_VehiclePremium.BusinessType,
	AGG_VehiclePremium.InsuranceReferenceLegalEntityDescription,
	AGG_VehiclePremium.StreetAddress,
	AGG_VehiclePremium.RatingCity,
	AGG_VehiclePremium.RatingStateProvinceAbbreviation,
	AGG_VehiclePremium.RatingPostalCode,
	AGG_VehiclePremium.RatingCounty,
	AGG_VehiclePremium.ModelYear,
	AGG_VehiclePremium.o_VehiclePremium AS VehiclePremium,
	-- *INF*: IIF(ISNULL(VehiclePremium),0,VehiclePremium)
	IFF(VehiclePremium IS NULL, 0, VehiclePremium) AS o_VehiclePremium,
	LKP_WorkCatastropheExposureLimit.LimitValue AS VehicleLimit,
	LKP_WorkCatastropheExposureDeductible.DeductibleValue AS VehicleDeductible,
	AGG_VehiclePremium.AgencyCode,
	AGG_VehiclePremium.StrategicProfitCenterDescription,
	AGG_VehiclePremium.InsuranceSegmentDescription,
	AGG_VehiclePremium.InsuredName,
	AGG_VehiclePremium.CustomerNumber,
	AGG_VehiclePremium.PolicyEffectiveDate,
	AGG_VehiclePremium.PolicyExpirationDate,
	AGG_VehiclePremium.OccupancyCode,
	AGG_VehiclePremium.SicCode,
	AGG_VehiclePremium.ProcessDate,
	AGG_VehiclePremium.TerrorismRiskIndicator,
	LKP_WorkCatastropheExposureTransaction_PlusPak.CoverageDescription AS lkp_CoverageDescription_PlusPak,
	LKP_WorkCatastropheExposureTransaction_ALS.DCTCoverageTypeCode AS lkp_DCTCoverageTypeCode_ALS,
	LKP_WorkCatastropheExposureTransaction_Terrorismflag.TerrorismRiskIndicator AS lkp_TerrorismRiskIndicator_Terrorismflag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	VehicleLimit AS o_AdjustedTotalInsuredValue,
	-- *INF*: IIF(ISNULL(lkp_CoverageDescription_PlusPak), '0','1')
	IFF(lkp_CoverageDescription_PlusPak IS NULL, '0', '1') AS o_PlusPakFlag,
	-- *INF*: IIF(ISNULL(lkp_DCTCoverageTypeCode_ALS), '0','1')
	IFF(lkp_DCTCoverageTypeCode_ALS IS NULL, '0', '1') AS o_ActualLossSustainedCoverageFlag,
	-- *INF*: IIF(ISNULL(lkp_TerrorismRiskIndicator_Terrorismflag), '0','1')
	IFF(lkp_TerrorismRiskIndicator_Terrorismflag IS NULL, '0', '1') AS o_TerrorismFlag
	FROM AGG_VehiclePremium
	LEFT JOIN LKP_WorkCatastropheExposureDeductible
	ON LKP_WorkCatastropheExposureDeductible.PolicyKey = AGG_VehiclePremium.PolicyKey AND LKP_WorkCatastropheExposureDeductible.VehicleNumber = AGG_VehiclePremium.VehicleNumber AND LKP_WorkCatastropheExposureDeductible.BusinessType = AGG_VehiclePremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureLimit
	ON LKP_WorkCatastropheExposureLimit.PolicyKey = AGG_VehiclePremium.PolicyKey AND LKP_WorkCatastropheExposureLimit.VehicleNumber = AGG_VehiclePremium.VehicleNumber AND LKP_WorkCatastropheExposureLimit.BusinessType = AGG_VehiclePremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_ALS
	ON LKP_WorkCatastropheExposureTransaction_ALS.PolicyKey = AGG_VehiclePremium.PolicyKey AND LKP_WorkCatastropheExposureTransaction_ALS.VehicleNumber = AGG_VehiclePremium.VehicleNumber AND LKP_WorkCatastropheExposureTransaction_ALS.BusinessType = AGG_VehiclePremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_PlusPak
	ON LKP_WorkCatastropheExposureTransaction_PlusPak.PolicyKey = AGG_VehiclePremium.PolicyKey AND LKP_WorkCatastropheExposureTransaction_PlusPak.BusinessType = AGG_VehiclePremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_Terrorismflag
	ON LKP_WorkCatastropheExposureTransaction_Terrorismflag.PolicyKey = AGG_VehiclePremium.PolicyKey AND LKP_WorkCatastropheExposureTransaction_Terrorismflag.BusinessType = AGG_VehiclePremium.BusinessType
),
FIL_VehiclePremium AS (
	SELECT
	SourceSystemId, 
	PolicyKey, 
	VehicleNumber, 
	BusinessType, 
	InsuranceReferenceLegalEntityDescription, 
	StreetAddress, 
	RatingCity, 
	RatingStateProvinceAbbreviation, 
	RatingPostalCode, 
	RatingCounty, 
	ModelYear, 
	o_VehiclePremium AS VehiclePremium, 
	VehicleLimit, 
	VehicleDeductible, 
	AgencyCode, 
	StrategicProfitCenterDescription, 
	InsuranceSegmentDescription, 
	InsuredName, 
	CustomerNumber, 
	PolicyEffectiveDate, 
	PolicyExpirationDate, 
	OccupancyCode, 
	SicCode, 
	ProcessDate, 
	o_AuditId, 
	o_CreatedDate, 
	o_ModifiedDate, 
	o_AdjustedTotalInsuredValue, 
	o_PlusPakFlag, 
	o_ActualLossSustainedCoverageFlag, 
	o_TerrorismFlag
	FROM EXP_MetaData
	WHERE VehiclePremium != 0
),
WorkCatastropheExposureVehicle AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureVehicle;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureVehicle
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, PolicyKey, VehicleNumber, BusinessType, InsuranceReferenceLegalEntityDescription, RatingAddress, RatingCity, RatingStateProvinceAbbreviation, RatingPostalCode, RatingCounty, ModelYear, VehiclePremium, VehicleLimit, VehicleDeductible, AdjustedTotalInsuredValue, AgencyCode, StrategicProfitCenterDescription, InsuranceSegmentDescription, PlusPakFlag, ActualLossSustainedCoverageFlag, InsuredName, CustomerNumber, PolicyEffectiveDate, PolicyExpirationDate, OccupancyCode, SicCode, TerrorismFlag, ProcessDate)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	SOURCESYSTEMID, 
	POLICYKEY, 
	VEHICLENUMBER, 
	BUSINESSTYPE, 
	INSURANCEREFERENCELEGALENTITYDESCRIPTION, 
	StreetAddress AS RATINGADDRESS, 
	RATINGCITY, 
	RATINGSTATEPROVINCEABBREVIATION, 
	RATINGPOSTALCODE, 
	RATINGCOUNTY, 
	MODELYEAR, 
	VEHICLEPREMIUM, 
	VEHICLELIMIT, 
	VEHICLEDEDUCTIBLE, 
	o_AdjustedTotalInsuredValue AS ADJUSTEDTOTALINSUREDVALUE, 
	AGENCYCODE, 
	STRATEGICPROFITCENTERDESCRIPTION, 
	INSURANCESEGMENTDESCRIPTION, 
	o_PlusPakFlag AS PLUSPAKFLAG, 
	o_ActualLossSustainedCoverageFlag AS ACTUALLOSSSUSTAINEDCOVERAGEFLAG, 
	INSUREDNAME, 
	CUSTOMERNUMBER, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	OCCUPANCYCODE, 
	SICCODE, 
	o_TerrorismFlag AS TERRORISMFLAG, 
	PROCESSDATE
	FROM FIL_VehiclePremium
),