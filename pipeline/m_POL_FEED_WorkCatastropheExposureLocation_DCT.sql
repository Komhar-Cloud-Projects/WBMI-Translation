WITH
SQ_WorkCatastropheExposureTransaction AS (
	select WCET.SourceSystemID,
	POL.pol_key,
	WCET.LocationNumber,
	WCET.BusinessType,
	IRLE.InsuranceReferenceLegalEntityDescription,
	RL.StreetAddress,
	RL.RatingCity,
	SS.state_code,
	RL.ZipPostalCode,
	RL.RatingCounty,
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
	where WCET.BusinessType in ('Commercial Inland Marine', 'Dealers Physical Damage' , 'Garagekeepers Liability') 
	AND WCET.LocationNumber <> '0000' 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Cal AS (
	SELECT
	SourceSystemId,
	pol_key,
	LocationNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	StreetAddress,
	RatingCity,
	state_code,
	ZipPostalCode,
	RatingCounty,
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
AGG_LocationPremium AS (
	SELECT
	SourceSystemId,
	o_PolicyKey AS PolicyKey,
	LocationNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	StreetAddress,
	RatingCity,
	o_RatingStateProvinceAbbreviation AS RatingStateProvinceAbbreviation,
	o_RatingPostalCode AS RatingPostalCode,
	RatingCounty,
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
	SUM(PremiumTransactionAmount, AddToLocationPremiumFlag = 'T') AS o_LocationPremium
	FROM EXP_Cal
	GROUP BY PolicyKey, LocationNumber, BusinessType
),
LKP_WorkCatastropheExposureDeductible AS (
	SELECT
	DeductibleValue,
	PolicyKey,
	LocationNumber,
	BusinessType
	FROM (
		SELECT D.PolicyKey AS PolicyKey, D.LocationNumber AS LocationNumber, D.BusinessType AS BusinessType,
		 RelevantDeductibles.TargetColumn AS TargetColumn,
		 (CASE WHEN RelevantDeductibles.CalculationRule = 'MAX' THEN MAX(CONVERT(BIGINT, D.DeductibleValue)) 
		  WHEN RelevantDeductibles.CalculationRule = 'MIN' THEN MIN(CONVERT(BIGINT, D.DeductibleValue)) 
		  WHEN RelevantDeductibles.CalculationRule = 'SUM' THEN SUM(CONVERT(BIGINT, D.DeductibleValue)) 
		  ELSE 0 
		 END) AS DeductibleValue
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureDeductible D
		INNER JOIN (SELECT DISTINCT BusinessType, DeductibleType, CalculationRule, TargetColumn 
		  FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupCatastropheExposureDeductibleRule 
		  WHERE TargetColumn = 'Loc Dedctbl' 
		  AND SourceSystemId = 'DCT') RelevantDeductibles ON D.BusinessType = RelevantDeductibles.BusinessType AND D.DeductibleType = RelevantDeductibles.DeductibleType
		WHERE D.BusinessType IN ('Commercial Inland Marine', 'Dealers Physical Damage', 'Garagekeepers Liability')
		GROUP BY D.PolicyKey, D.LocationNumber, D.BusinessType, RelevantDeductibles.TargetColumn, RelevantDeductibles.CalculationRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BusinessType ORDER BY DeductibleValue) = 1
),
LKP_WorkCatastropheExposureLimit AS (
	SELECT
	LimitValue,
	PolicyKey,
	LocationNumber,
	BusinessType
	FROM (
		SELECT L.PolicyKey AS PolicyKey, L.LocationNumber AS LocationNumber, L.BusinessType AS BusinessType,
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
		WHERE L.BusinessType IN ('Commercial Inland Marine', 'Dealers Physical Damage', 'Garagekeepers Liability')
		GROUP BY L.PolicyKey, L.LocationNumber, L.BusinessType, RelevantLimits.TargetColumn, RelevantLimits.CalculationRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BusinessType ORDER BY LimitValue) = 1
),
LKP_WorkCatastropheExposureTransaction_ALS AS (
	SELECT
	DCTCoverageTypeCode,
	PolicyKey,
	LocationNumber,
	BusinessType
	FROM (
		SELECT 
			DCTCoverageTypeCode,
			PolicyKey,
			LocationNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE DctCoverageTypeCode = 'ALS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BusinessType ORDER BY DCTCoverageTypeCode) = 1
),
LKP_WorkCatastropheExposureTransaction_CLM AS (
	SELECT
	CoverageGroupDescription,
	PolicyKey,
	LocationNumber,
	BusinessType
	FROM (
		SELECT 
			CoverageGroupDescription,
			PolicyKey,
			LocationNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE (TerrorismRiskIndicator = 1   or (CoverageGroupDescription = 'Terrorism' and PremiumTransactionAmount > 0.0)   )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BusinessType ORDER BY CoverageGroupDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_DPD AS (
	SELECT
	TerrorismRiskIndicator,
	PolicyKey,
	BusinessType
	FROM (
		SELECT 
			TerrorismRiskIndicator,
			PolicyKey,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE TerrorismRiskIndicator=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,BusinessType ORDER BY TerrorismRiskIndicator) = 1
),
LKP_WorkCatastropheExposureTransaction_PlusPak AS (
	SELECT
	CoverageDescription,
	PolicyKey
	FROM (
		SELECT 
			CoverageDescription,
			PolicyKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE DctCoverageTypeCode in ('PlusPakGarage','PlusPakAuto')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY CoverageDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_PlusPak_CIM AS (
	SELECT
	PolicyKey,
	i_PolicyKey
	FROM (
		SELECT 
			PolicyKey,
			i_PolicyKey
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE BusinessType = 'SBOP' or 
		(BusinessType in ('SMARTbusiness','Commercial Property') and
		CoverageDescription = 'Plus Pak')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY PolicyKey) = 1
),
LKP_WorkCatastropheExposureTransaction_SMART AS (
	SELECT
	PolicyKey,
	LocationNumber,
	in_PolicyKey,
	in_LocationNumber
	FROM (
		SELECT 
			PolicyKey,
			LocationNumber,
			in_PolicyKey,
			in_LocationNumber
		FROM WorkCatastropheExposureTransaction
		WHERE BusinessType = 'SMARTbusiness' and PolicyOfferingDescription = 'SMARTbusiness' and CoverageGroupDescription = 'Terrorism'  and (TerrorismRiskIndicator = 1
		  or (CoverageGroupDescription = 'Terrorism' and PremiumTransactionAmount > 0.0)
		  )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber ORDER BY PolicyKey) = 1
),
LKP_WorkCatastropheExposureTransaction_TerrorismCoverage AS (
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
		WHERE CoverageGroupDescription='Terrorism'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,BusinessType ORDER BY CoverageGroupDescription) = 1
),
LKP_WorkCatastropheExposureTransaction_WindCoverageFlag AS (
	SELECT
	WindCoverageFlag,
	PolicyKey,
	LocationNumber,
	BusinessType
	FROM (
		SELECT 
			WindCoverageFlag,
			PolicyKey,
			LocationNumber,
			BusinessType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction
		WHERE WindCoverageFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationNumber,BusinessType ORDER BY WindCoverageFlag) = 1
),
EXP_MetaData AS (
	SELECT
	AGG_LocationPremium.SourceSystemId,
	AGG_LocationPremium.PolicyKey,
	AGG_LocationPremium.LocationNumber,
	AGG_LocationPremium.BusinessType,
	AGG_LocationPremium.InsuranceReferenceLegalEntityDescription,
	AGG_LocationPremium.StreetAddress,
	AGG_LocationPremium.RatingCity,
	AGG_LocationPremium.RatingStateProvinceAbbreviation,
	AGG_LocationPremium.RatingPostalCode,
	AGG_LocationPremium.RatingCounty,
	AGG_LocationPremium.o_LocationPremium AS i_LocationPremium,
	-- *INF*: IIF(ISNULL(i_LocationPremium),0,i_LocationPremium)
	IFF(i_LocationPremium IS NULL, 0, i_LocationPremium) AS o_LocationPremium,
	LKP_WorkCatastropheExposureLimit.LimitValue AS LocationLimit,
	LKP_WorkCatastropheExposureDeductible.DeductibleValue AS LocationDeductible,
	AGG_LocationPremium.AgencyCode,
	AGG_LocationPremium.StrategicProfitCenterDescription,
	AGG_LocationPremium.InsuranceSegmentDescription,
	AGG_LocationPremium.InsuredName,
	AGG_LocationPremium.CustomerNumber,
	AGG_LocationPremium.PolicyEffectiveDate,
	AGG_LocationPremium.PolicyExpirationDate,
	AGG_LocationPremium.OccupancyCode,
	AGG_LocationPremium.SicCode,
	AGG_LocationPremium.ProcessDate,
	AGG_LocationPremium.TerrorismRiskIndicator,
	LKP_WorkCatastropheExposureTransaction_PlusPak.CoverageDescription AS lkp_CoverageDescription,
	LKP_WorkCatastropheExposureTransaction_ALS.DCTCoverageTypeCode AS lkp_DCTCoverageTypeCode,
	LKP_WorkCatastropheExposureTransaction_WindCoverageFlag.WindCoverageFlag AS lkp_WindCoverageFlag,
	LKP_WorkCatastropheExposureTransaction_TerrorismCoverage.CoverageGroupDescription AS lkp_CoverageGroupDescription,
	LKP_WorkCatastropheExposureTransaction_CLM.CoverageGroupDescription AS lkp_Coverage_Terrorism,
	LKP_WorkCatastropheExposureTransaction_PlusPak_CIM.PolicyKey AS lkp_PolicyKey_CIM,
	LKP_WorkCatastropheExposureTransaction_SMART.PolicyKey AS lkp_PolicyKey_SMART,
	LKP_WorkCatastropheExposureTransaction_DPD.TerrorismRiskIndicator AS lkp_TerrorismRiskIndicator,
	-- *INF*: IIF( in(BusinessType,'Dealers Physical Damage','Garagekeepers Liability') and NOT ISNULL(lkp_CoverageDescription), '1', 
	-- IIF( BusinessType='Commercial Inland Marine' and NOT ISNULL(lkp_PolicyKey_CIM), 
	-- '1','0'))
	IFF(
	    BusinessType IN ('Dealers Physical Damage','Garagekeepers Liability')
	    and lkp_CoverageDescription IS NOT NULL,
	    '1',
	    IFF(
	        BusinessType = 'Commercial Inland Marine'
	    and lkp_PolicyKey_CIM IS NOT NULL, '1',
	        '0'
	    )
	) AS v_PlusPakFlag,
	-- *INF*: IIF(v_PlusPakFlag ='1' , ROUND(LocationLimit*1.05, 0),LocationLimit)
	IFF(v_PlusPakFlag = '1', ROUND(LocationLimit * 1.05, 0), LocationLimit) AS v_AdjustedTotalInsuredValue,
	-- *INF*: DECODE(TRUE,
	-- IN(BusinessType, 'Dealers Physical Damage', 'Garagekeepers Liability') AND lkp_WindCoverageFlag='T', '1',
	-- BusinessType='Commercial Inland Marine', '0',
	-- '0')
	DECODE(
	    TRUE,
	    BusinessType IN ('Dealers Physical Damage','Garagekeepers Liability') AND lkp_WindCoverageFlag = 'T', '1',
	    BusinessType = 'Commercial Inland Marine', '0',
	    '0'
	) AS v_LocationWindCoverageFlag,
	-- *INF*: IIF(lkp_DCTCoverageTypeCode = 'ALS', '1', '0')
	IFF(lkp_DCTCoverageTypeCode = 'ALS', '1', '0') AS v_ActualLossSustainedCoverageFlag,
	-- *INF*: IIF( in(BusinessType,'Dealers Physical Damage','Garagekeepers Liability') AND NOT ISNULL(lkp_TerrorismRiskIndicator),'1',
	-- IIF( BusinessType='Commercial Inland Marine' AND  NOT ISNULL(lkp_Coverage_Terrorism),'1',
	-- IIF(BusinessType='Commercial Inland Marine'  AND  NOT  ISNULL(lkp_PolicyKey_SMART),'1','0')))
	-- 
	--  
	IFF(
	    BusinessType IN ('Dealers Physical Damage','Garagekeepers Liability')
	    and lkp_TerrorismRiskIndicator IS NOT NULL,
	    '1',
	    IFF(
	        BusinessType = 'Commercial Inland Marine'
	    and lkp_Coverage_Terrorism IS NOT NULL,
	        '1',
	        IFF(
	            BusinessType = 'Commercial Inland Marine'
	        and lkp_PolicyKey_SMART IS NOT NULL,
	            '1',
	            '0'
	        )
	    )
	) AS v_TerrorismFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	v_AdjustedTotalInsuredValue AS o_AdjustedTotalInsuredValue,
	v_LocationWindCoverageFlag AS o_LocationWindCoverageFlag,
	v_PlusPakFlag AS o_PlusPakFlag,
	v_ActualLossSustainedCoverageFlag AS o_ActualLossSustainedCoverageFlag,
	v_TerrorismFlag AS o_TerrorismFlag
	FROM AGG_LocationPremium
	LEFT JOIN LKP_WorkCatastropheExposureDeductible
	ON LKP_WorkCatastropheExposureDeductible.PolicyKey = AGG_LocationPremium.PolicyKey AND LKP_WorkCatastropheExposureDeductible.LocationNumber = AGG_LocationPremium.LocationNumber AND LKP_WorkCatastropheExposureDeductible.BusinessType = AGG_LocationPremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureLimit
	ON LKP_WorkCatastropheExposureLimit.PolicyKey = AGG_LocationPremium.PolicyKey AND LKP_WorkCatastropheExposureLimit.LocationNumber = AGG_LocationPremium.LocationNumber AND LKP_WorkCatastropheExposureLimit.BusinessType = AGG_LocationPremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_ALS
	ON LKP_WorkCatastropheExposureTransaction_ALS.PolicyKey = AGG_LocationPremium.PolicyKey AND LKP_WorkCatastropheExposureTransaction_ALS.LocationNumber = AGG_LocationPremium.LocationNumber AND LKP_WorkCatastropheExposureTransaction_ALS.BusinessType = AGG_LocationPremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_CLM
	ON LKP_WorkCatastropheExposureTransaction_CLM.PolicyKey = AGG_LocationPremium.PolicyKey AND LKP_WorkCatastropheExposureTransaction_CLM.LocationNumber = AGG_LocationPremium.LocationNumber AND LKP_WorkCatastropheExposureTransaction_CLM.BusinessType = AGG_LocationPremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_DPD
	ON LKP_WorkCatastropheExposureTransaction_DPD.PolicyKey = AGG_LocationPremium.PolicyKey AND LKP_WorkCatastropheExposureTransaction_DPD.BusinessType = AGG_LocationPremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_PlusPak
	ON LKP_WorkCatastropheExposureTransaction_PlusPak.PolicyKey = AGG_LocationPremium.PolicyKey
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_PlusPak_CIM
	ON LKP_WorkCatastropheExposureTransaction_PlusPak_CIM.PolicyKey = AGG_LocationPremium.PolicyKey
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_SMART
	ON LKP_WorkCatastropheExposureTransaction_SMART.PolicyKey = AGG_LocationPremium.PolicyKey AND LKP_WorkCatastropheExposureTransaction_SMART.LocationNumber = AGG_LocationPremium.LocationNumber
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_TerrorismCoverage
	ON LKP_WorkCatastropheExposureTransaction_TerrorismCoverage.PolicyKey = AGG_LocationPremium.PolicyKey AND LKP_WorkCatastropheExposureTransaction_TerrorismCoverage.BusinessType = AGG_LocationPremium.BusinessType
	LEFT JOIN LKP_WorkCatastropheExposureTransaction_WindCoverageFlag
	ON LKP_WorkCatastropheExposureTransaction_WindCoverageFlag.PolicyKey = AGG_LocationPremium.PolicyKey AND LKP_WorkCatastropheExposureTransaction_WindCoverageFlag.LocationNumber = AGG_LocationPremium.LocationNumber AND LKP_WorkCatastropheExposureTransaction_WindCoverageFlag.BusinessType = AGG_LocationPremium.BusinessType
),
Fil_Null_Values AS (
	SELECT
	SourceSystemId, 
	PolicyKey, 
	LocationNumber, 
	BusinessType, 
	InsuranceReferenceLegalEntityDescription, 
	StreetAddress, 
	RatingCity, 
	RatingStateProvinceAbbreviation, 
	RatingPostalCode, 
	RatingCounty, 
	o_LocationPremium AS LocationPremium, 
	LocationLimit, 
	LocationDeductible, 
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
	o_LocationWindCoverageFlag, 
	o_PlusPakFlag, 
	o_ActualLossSustainedCoverageFlag, 
	o_TerrorismFlag
	FROM EXP_MetaData
	WHERE IIF(ISNULL(LocationPremium),0, LocationPremium)!=0
OR
IIF(ISNULL(LocationLimit),0, LocationLimit)!=0 
OR
IIF(ISNULL(LocationDeductible),0, LocationDeductible)!=0
),
WorkCatastropheExposureLocation AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureLocation;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureLocation
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, PolicyKey, LocationNumber, BusinessType, InsuranceReferenceLegalEntityDescription, RatingAddress, RatingCity, RatingStateProvinceAbbreviation, RatingPostalCode, RatingCounty, LocationPremium, LocationLimit, LocationDeductible, AdjustedTotalInsuredValue, LocationWindCoverageFlag, AgencyCode, StrategicProfitCenterDescription, InsuranceSegmentDescription, PlusPakFlag, ActualLossSustainedCoverageFlag, InsuredName, CustomerNumber, PolicyEffectiveDate, PolicyExpirationDate, OccupancyCode, SicCode, TerrorismFlag, ProcessDate)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	SOURCESYSTEMID, 
	POLICYKEY, 
	LOCATIONNUMBER, 
	BUSINESSTYPE, 
	INSURANCEREFERENCELEGALENTITYDESCRIPTION, 
	StreetAddress AS RATINGADDRESS, 
	RATINGCITY, 
	RATINGSTATEPROVINCEABBREVIATION, 
	RATINGPOSTALCODE, 
	RATINGCOUNTY, 
	LOCATIONPREMIUM, 
	LOCATIONLIMIT, 
	LOCATIONDEDUCTIBLE, 
	o_AdjustedTotalInsuredValue AS ADJUSTEDTOTALINSUREDVALUE, 
	o_LocationWindCoverageFlag AS LOCATIONWINDCOVERAGEFLAG, 
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
	FROM Fil_Null_Values
),