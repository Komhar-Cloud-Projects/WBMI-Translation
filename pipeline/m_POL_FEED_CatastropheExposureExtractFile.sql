WITH
SQ_CatastropheExposureExtract AS (
	SELECT
		CatastropheExposureExtractId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		SourceSystemId,
		InsuranceReferenceLegalEntityDescription,
		PolicyKey,
		PolicyBlanketPremium,
		PolicyBlanketLimit,
		PolicyBlanketDeductible,
		LineOfBusiness,
		PolicyForm,
		StreetNumber,
		StreetName,
		City,
		State,
		ZipCode,
		County,
		ConstructionClass,
		OccupancyType,
		YearBuilt,
		ModelYear,
		LocationPremium,
		LocationLimit,
		LocationDeductible,
		LocationBuildingLimit,
		LocationBuildingDeductible,
		LocationOtherLimit,
		LocationOtherDeductible,
		LocationContentsLimit,
		LocationContentsDeductible,
		LocationBILimit,
		LocationBIDeductible,
		AdjustedBuildingLimit,
		AdjustedContentsLimit,
		AdjustedBILimit,
		AdjustedOtherLimit,
		AdjustedTotalInsuredValue,
		LocationWindCoverageFlag,
		LocationEarthquakeFlag,
		LocationEarthquakePremium,
		LocationEarthquakeLimit,
		LocationEarthquakeDeductible,
		AgencyCode,
		StrategicProfitCenterDescription,
		InsuranceSegmentDescription,
		PlusPakFlag,
		ActualLossSustainedCoverageFlag,
		InsuredName,
		CustomerNumber,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		OccupancyCode,
		SicCode,
		TerrorismFlag,
		RoofMaterial,
		RoofYear,
		HailResistiveRoofFlag,
		WindHailLossSettlement,
		WindHailDeductibleAmount,
		ProcessDate,
		LocationNumber,
		BuildingNumber,
		VehicleNumber
	FROM CatastropheExposureExtract
	INNER JOIN CatastropheExposureExtract
	WHERE @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Cal AS (
	SELECT
	CatastropheExposureExtractId,
	AuditId AS i_AuditId,
	CreatedDate AS i_CreatedDate,
	ModifiedDate AS i_ModifiedDate,
	SourceSystemId AS i_SourceSystemId,
	InsuranceReferenceLegalEntityDescription AS i_InsuranceReferenceLegalEntityDescription,
	PolicyKey AS i_PolicyKey,
	PolicyBlanketPremium AS i_PolicyBlanketPremium,
	PolicyBlanketLimit AS i_PolicyBlanketLimit,
	PolicyBlanketDeductible AS i_PolicyBlanketDeductible,
	LineOfBusiness AS i_LineOfBusiness,
	PolicyForm AS i_PolicyForm,
	StreetNumber AS i_StreetNumber,
	StreetName AS i_StreetName,
	City AS i_City,
	State AS i_State,
	ZipCode AS i_ZipCode,
	County AS i_County,
	ConstructionClass AS i_ConstructionClass,
	OccupancyType AS i_OccupancyType,
	YearBuilt AS i_YearBuilt,
	ModelYear AS i_ModelYear,
	LocationPremium AS i_LocationPremium,
	LocationLimit AS i_LocationLimit,
	LocationDeductible AS i_LocationDeductible,
	LocationBuildingLimit AS i_LocationBuildingLimit,
	LocationBuildingDeductible AS i_LocationBuildingDeductible,
	LocationOtherLimit AS i_LocationOtherLimit,
	LocationOtherDeductible AS i_LocationOtherDeductible,
	LocationContentsLimit AS i_LocationContentsLimit,
	LocationContentsDeductible AS i_LocationContentsDeductible,
	LocationBILimit AS i_LocationBILimit,
	LocationBIDeductible AS i_LocationBIDeductible,
	AdjustedBuildingLimit AS i_AdjustedBuildingLimit,
	AdjustedContentsLimit AS i_AdjustedContentsLimit,
	AdjustedBILimit AS i_AdjustedBILimit,
	AdjustedOtherLimit AS i_AdjustedOtherLimit,
	AdjustedTotalInsuredValue AS i_AdjustedTotalInsuredValue,
	LocationWindCoverageFlag AS i_LocationWindCoverageFlag,
	LocationEarthquakeFlag AS i_LocationEarthquakeFlag,
	LocationEarthquakePremium AS i_LocationEarthquakePremium,
	LocationEarthquakeLimit AS i_LocationEarthquakeLimit,
	LocationEarthquakeDeductible AS i_LocationEarthquakeDeductible,
	AgencyCode AS i_AgencyCode,
	StrategicProfitCenterDescription AS i_StrategicProfitCenterDescription,
	InsuranceSegmentDescription AS i_InsuranceSegmentDescription,
	PlusPakFlag AS i_PlusPakFlag,
	ActualLossSustainedCoverageFlag AS i_ActualLossSustainedCoverageFlag,
	InsuredName AS i_InsuredName,
	CustomerNumber AS i_CustomerNumber,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	PolicyExpirationDate AS i_PolicyExpirationDate,
	OccupancyCode AS i_OccupancyCode,
	SicCode AS i_SicCode,
	TerrorismFlag AS i_TerrorismFlag,
	RoofMaterial AS i_RoofMaterial,
	RoofYear AS i_RoofYear,
	HailResistiveRoofFlag AS i_HailResistiveRoofFlag,
	WindHailLossSettlement AS i_WindHailLossSettlement,
	WindHailDeductibleAmount AS i_WindHailDeductibleAmount,
	ProcessDate AS i_ProcessDate,
	LocationNumber AS i_LocationNumber,
	BuildingNumber AS i_BuildingNumber,
	VehicleNumber AS i_VehicleNumber,
	'~' AS v_Separator,
	-- *INF*: RPAD(IIF(ISNULL(i_InsuranceReferenceLegalEntityDescription),' ',i_InsuranceReferenceLegalEntityDescription) , 50, ' ')
	RPAD(
	    IFF(
	        i_InsuranceReferenceLegalEntityDescription IS NULL, ' ',
	        i_InsuranceReferenceLegalEntityDescription
	    ), 50, ' ') AS v_InsuranceReferenceLegalEntityDescription,
	-- *INF*: RPAD(IIF(ISNULL(i_PolicyKey),' ', i_PolicyKey), 100, ' ')
	RPAD(
	    IFF(
	        i_PolicyKey IS NULL, ' ', i_PolicyKey
	    ), 100, ' ') AS v_PolicyKey,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_PolicyBlanketPremium), LPAD('0',12,'0'),
	-- TO_BIGINT(i_PolicyBlanketPremium)>=0, LPAD(i_PolicyBlanketPremium,12,'0'),
	-- TO_BIGINT(i_PolicyBlanketPremium)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_PolicyBlanketPremium))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_PolicyBlanketPremium),'0', i_PolicyBlanketPremium), 12, '0')
	DECODE(
	    TRUE,
	    i_PolicyBlanketPremium IS NULL, LPAD('0', 12, '0'),
	    CAST(i_PolicyBlanketPremium AS BIGINT) >= 0, LPAD(i_PolicyBlanketPremium, 12, '0'),
	    CAST(i_PolicyBlanketPremium AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_PolicyBlanketPremium AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_PolicyBlanketPremium,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_PolicyBlanketLimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_PolicyBlanketLimit)>=0, LPAD(i_PolicyBlanketLimit,12,'0'),
	-- TO_BIGINT(i_PolicyBlanketLimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_PolicyBlanketLimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_PolicyBlanketLimit), '0', i_PolicyBlanketLimit), 12, '0')
	DECODE(
	    TRUE,
	    i_PolicyBlanketLimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_PolicyBlanketLimit AS BIGINT) >= 0, LPAD(i_PolicyBlanketLimit, 12, '0'),
	    CAST(i_PolicyBlanketLimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_PolicyBlanketLimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_PolicyBlanketLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_PolicyBlanketDeductible), LPAD('0',12,'0'),
	-- TO_BIGINT(i_PolicyBlanketDeductible)>=0, LPAD(i_PolicyBlanketDeductible,12,'0'),
	-- TO_BIGINT(i_PolicyBlanketDeductible)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_PolicyBlanketDeductible))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_PolicyBlanketDeductible), '0', i_PolicyBlanketDeductible), 12, '0')
	DECODE(
	    TRUE,
	    i_PolicyBlanketDeductible IS NULL, LPAD('0', 12, '0'),
	    CAST(i_PolicyBlanketDeductible AS BIGINT) >= 0, LPAD(i_PolicyBlanketDeductible, 12, '0'),
	    CAST(i_PolicyBlanketDeductible AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_PolicyBlanketDeductible AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_PolicyBlanketDeductible,
	-- *INF*: RPAD(IIF(ISNULL(i_LineOfBusiness), ' ',i_LineOfBusiness), 3, ' ')
	RPAD(
	    IFF(
	        i_LineOfBusiness IS NULL, ' ', i_LineOfBusiness
	    ), 3, ' ') AS v_LineOfBusiness,
	-- *INF*: RPAD(IIF(ISNULL(i_PolicyForm), ' ', i_PolicyForm), 10, ' ')
	RPAD(
	    IFF(
	        i_PolicyForm IS NULL, ' ', i_PolicyForm
	    ), 10, ' ') AS v_PolicyForm,
	-- *INF*: RPAD(IIF(ISNULL(i_StreetNumber), ' ', i_StreetNumber), 12, ' ')
	RPAD(
	    IFF(
	        i_StreetNumber IS NULL, ' ', i_StreetNumber
	    ), 12, ' ') AS v_StreetNumber,
	-- *INF*: RPAD(IIF(ISNULL(i_StreetName), ' ', i_StreetName), 255, ' ')
	RPAD(
	    IFF(
	        i_StreetName IS NULL, ' ', i_StreetName
	    ), 255, ' ') AS v_StreetName,
	-- *INF*: RPAD(IIF(ISNULL(i_City), ' ', i_City), 255, ' ')
	RPAD(
	    IFF(
	        i_City IS NULL, ' ', i_City
	    ), 255, ' ') AS v_City,
	-- *INF*: RPAD(IIF(ISNULL(i_State), ' ', i_State), 2, ' ')
	RPAD(
	    IFF(
	        i_State IS NULL, ' ', i_State
	    ), 2, ' ') AS v_State,
	-- *INF*: RPAD(IIF(ISNULL(i_ZipCode), ' ', i_ZipCode), 5, ' ')
	RPAD(
	    IFF(
	        i_ZipCode IS NULL, ' ', i_ZipCode
	    ), 5, ' ') AS v_ZipCode,
	-- *INF*: RPAD(IIF(ISNULL(i_County), ' ', i_County), 255, ' ')
	RPAD(
	    IFF(
	        i_County IS NULL, ' ', i_County
	    ), 255, ' ') AS v_County,
	-- *INF*: RPAD(IIF(ISNULL(i_ConstructionClass), ' ', i_ConstructionClass), 50, ' ')
	RPAD(
	    IFF(
	        i_ConstructionClass IS NULL, ' ', i_ConstructionClass
	    ), 50, ' ') AS v_ConstructionClass,
	-- *INF*: RPAD(IIF(ISNULL(i_OccupancyType), ' ', i_OccupancyType), 4, ' ')
	RPAD(
	    IFF(
	        i_OccupancyType IS NULL, ' ', i_OccupancyType
	    ), 4, ' ') AS v_OccupancyType,
	-- *INF*: RPAD(IIF(ISNULL(i_YearBuilt), ' ', i_YearBuilt), 4, ' ')
	RPAD(
	    IFF(
	        i_YearBuilt IS NULL, ' ', i_YearBuilt
	    ), 4, ' ') AS v_YearBuilt,
	-- *INF*: RPAD(IIF(ISNULL(i_ModelYear), ' ', i_ModelYear), 4, ' ')
	RPAD(
	    IFF(
	        i_ModelYear IS NULL, ' ', i_ModelYear
	    ), 4, ' ') AS v_ModelYear,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationPremium), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationPremium)>=0, LPAD(i_LocationPremium,12,'0'),
	-- TO_BIGINT(i_LocationPremium)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationPremium))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationPremium), '0', i_LocationPremium), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationPremium IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationPremium AS BIGINT) >= 0, LPAD(i_LocationPremium, 12, '0'),
	    CAST(i_LocationPremium AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationPremium AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationPremium,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationLimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationLimit)>=0, LPAD(i_LocationLimit,12,'0'),
	-- TO_BIGINT(i_LocationLimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationLimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationLimit), '0', i_LocationLimit), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationLimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationLimit AS BIGINT) >= 0, LPAD(i_LocationLimit, 12, '0'),
	    CAST(i_LocationLimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationLimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationDeductible), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationDeductible)>=0, LPAD(i_LocationDeductible,12,'0'),
	-- TO_BIGINT(i_LocationDeductible)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationDeductible))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationDeductible), '0', i_LocationDeductible), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationDeductible IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationDeductible AS BIGINT) >= 0, LPAD(i_LocationDeductible, 12, '0'),
	    CAST(i_LocationDeductible AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationDeductible AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationDeductible,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationBuildingLimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationBuildingLimit)>=0, LPAD(i_LocationBuildingLimit,12,'0'),
	-- TO_BIGINT(i_LocationBuildingLimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationBuildingLimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationBuildingLimit), '0', i_LocationBuildingLimit), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationBuildingLimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationBuildingLimit AS BIGINT) >= 0, LPAD(i_LocationBuildingLimit, 12, '0'),
	    CAST(i_LocationBuildingLimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationBuildingLimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationBuildingLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationBuildingDeductible), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationBuildingDeductible)>=0, LPAD(i_LocationBuildingDeductible,12,'0'),
	-- TO_BIGINT(i_LocationBuildingDeductible)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationBuildingDeductible))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationBuildingDeductible), '0', i_LocationBuildingDeductible), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationBuildingDeductible IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationBuildingDeductible AS BIGINT) >= 0, LPAD(i_LocationBuildingDeductible, 12, '0'),
	    CAST(i_LocationBuildingDeductible AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationBuildingDeductible AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationBuildingDeductible,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationOtherLimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationOtherLimit)>=0, LPAD(i_LocationOtherLimit,12,'0'),
	-- TO_BIGINT(i_LocationOtherLimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationOtherLimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationOtherLimit), '0', i_LocationOtherLimit), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationOtherLimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationOtherLimit AS BIGINT) >= 0, LPAD(i_LocationOtherLimit, 12, '0'),
	    CAST(i_LocationOtherLimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationOtherLimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationOtherLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationOtherDeductible), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationOtherDeductible)>=0, LPAD(i_LocationOtherDeductible,12,'0'),
	-- TO_BIGINT(i_LocationOtherDeductible)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationOtherDeductible))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationOtherDeductible), '0', i_LocationOtherDeductible), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationOtherDeductible IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationOtherDeductible AS BIGINT) >= 0, LPAD(i_LocationOtherDeductible, 12, '0'),
	    CAST(i_LocationOtherDeductible AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationOtherDeductible AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationOtherDeductible,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationContentsLimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationContentsLimit)>=0, LPAD(i_LocationContentsLimit,12,'0'),
	-- TO_BIGINT(i_LocationContentsLimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationContentsLimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationContentsLimit), '0', i_LocationContentsLimit), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationContentsLimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationContentsLimit AS BIGINT) >= 0, LPAD(i_LocationContentsLimit, 12, '0'),
	    CAST(i_LocationContentsLimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationContentsLimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationContentsLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationContentsDeductible), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationContentsDeductible)>=0, LPAD(i_LocationContentsDeductible,12,'0'),
	-- TO_BIGINT(i_LocationContentsDeductible)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationContentsDeductible))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationContentsDeductible), '0', i_LocationContentsDeductible), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationContentsDeductible IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationContentsDeductible AS BIGINT) >= 0, LPAD(i_LocationContentsDeductible, 12, '0'),
	    CAST(i_LocationContentsDeductible AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationContentsDeductible AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationContentsDeductible,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationBILimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationBILimit)>=0, LPAD(i_LocationBILimit,12,'0'),
	-- TO_BIGINT(i_LocationBILimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationBILimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationBILimit), '0', i_LocationBILimit), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationBILimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationBILimit AS BIGINT) >= 0, LPAD(i_LocationBILimit, 12, '0'),
	    CAST(i_LocationBILimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationBILimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationBILimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationBIDeductible), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationBIDeductible)>=0, LPAD(i_LocationBIDeductible,12,'0'),
	-- TO_BIGINT(i_LocationBIDeductible)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationBIDeductible))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationBIDeductible), '0', i_LocationBIDeductible), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationBIDeductible IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationBIDeductible AS BIGINT) >= 0, LPAD(i_LocationBIDeductible, 12, '0'),
	    CAST(i_LocationBIDeductible AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationBIDeductible AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationBIDeductible,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_AdjustedBuildingLimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_AdjustedBuildingLimit)>=0, LPAD(i_AdjustedBuildingLimit,12,'0'),
	-- TO_BIGINT(i_AdjustedBuildingLimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_AdjustedBuildingLimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_AdjustedBuildingLimit), '0', i_AdjustedBuildingLimit), 12, '0')
	DECODE(
	    TRUE,
	    i_AdjustedBuildingLimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_AdjustedBuildingLimit AS BIGINT) >= 0, LPAD(i_AdjustedBuildingLimit, 12, '0'),
	    CAST(i_AdjustedBuildingLimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_AdjustedBuildingLimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_AdjustedBuildingLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_AdjustedContentsLimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_AdjustedContentsLimit)>=0, LPAD(i_AdjustedContentsLimit,12,'0'),
	-- TO_BIGINT(i_AdjustedContentsLimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_AdjustedContentsLimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_AdjustedContentsLimit), '0', i_AdjustedContentsLimit), 12, '0')
	DECODE(
	    TRUE,
	    i_AdjustedContentsLimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_AdjustedContentsLimit AS BIGINT) >= 0, LPAD(i_AdjustedContentsLimit, 12, '0'),
	    CAST(i_AdjustedContentsLimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_AdjustedContentsLimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_AdjustedContentsLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_AdjustedBILimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_AdjustedBILimit)>=0, LPAD(i_AdjustedBILimit,12,'0'),
	-- TO_BIGINT(i_AdjustedBILimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_AdjustedBILimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_AdjustedBILimit), '0', i_AdjustedBILimit), 12, '0')
	DECODE(
	    TRUE,
	    i_AdjustedBILimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_AdjustedBILimit AS BIGINT) >= 0, LPAD(i_AdjustedBILimit, 12, '0'),
	    CAST(i_AdjustedBILimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_AdjustedBILimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_AdjustedBILimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_AdjustedOtherLimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_AdjustedOtherLimit)>=0, LPAD(i_AdjustedOtherLimit,12,'0'),
	-- TO_BIGINT(i_AdjustedOtherLimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_AdjustedOtherLimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_AdjustedOtherLimit), '0', i_AdjustedOtherLimit), 12, '0')
	DECODE(
	    TRUE,
	    i_AdjustedOtherLimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_AdjustedOtherLimit AS BIGINT) >= 0, LPAD(i_AdjustedOtherLimit, 12, '0'),
	    CAST(i_AdjustedOtherLimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_AdjustedOtherLimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_AdjustedOtherLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_AdjustedTotalInsuredValue), LPAD('0',12,'0'),
	-- TO_BIGINT(i_AdjustedTotalInsuredValue)>=0, LPAD(i_AdjustedTotalInsuredValue,12,'0'),
	-- TO_BIGINT(i_AdjustedTotalInsuredValue)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_AdjustedTotalInsuredValue))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_AdjustedTotalInsuredValue), '0', i_AdjustedTotalInsuredValue), 12, '0')
	DECODE(
	    TRUE,
	    i_AdjustedTotalInsuredValue IS NULL, LPAD('0', 12, '0'),
	    CAST(i_AdjustedTotalInsuredValue AS BIGINT) >= 0, LPAD(i_AdjustedTotalInsuredValue, 12, '0'),
	    CAST(i_AdjustedTotalInsuredValue AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_AdjustedTotalInsuredValue AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_AdjustedTotalInsuredValue,
	-- *INF*: RPAD(IIF(ISNULL(i_LocationWindCoverageFlag), ' ', i_LocationWindCoverageFlag), 1, ' ')
	-- 
	-- 
	-- 
	-- --RPAD(IIF(ISNULL(v_LocationWindCoverageIndicator_Convert), ' ', v_LocationWindCoverageIndicator_Convert), 1, ' ')
	RPAD(
	    IFF(
	        i_LocationWindCoverageFlag IS NULL, ' ', i_LocationWindCoverageFlag
	    ), 1, ' ') AS v_LocationWindCoverageFlag,
	-- *INF*: RPAD(IIF(ISNULL(i_LocationEarthquakeFlag), ' ', i_LocationEarthquakeFlag), 1, ' ')
	-- 
	-- 
	-- 
	-- --RPAD(IIF(ISNULL(v_LocationEarthquakeIndicator_Convert), ' ', v_LocationEarthquakeIndicator_Convert), 1, ' ')
	RPAD(
	    IFF(
	        i_LocationEarthquakeFlag IS NULL, ' ', i_LocationEarthquakeFlag
	    ), 1, ' ') AS v_LocationEarthquakeFlag,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationEarthquakePremium), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationEarthquakePremium)>=0, LPAD(i_LocationEarthquakePremium,12,'0'),
	-- TO_BIGINT(i_LocationEarthquakePremium)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationEarthquakePremium))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationEarthquakePremium), '0', i_LocationEarthquakePremium), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationEarthquakePremium IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationEarthquakePremium AS BIGINT) >= 0, LPAD(i_LocationEarthquakePremium, 12, '0'),
	    CAST(i_LocationEarthquakePremium AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationEarthquakePremium AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationEarthquakePremium,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationEarthquakeLimit), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationEarthquakeLimit)>=0, LPAD(i_LocationEarthquakeLimit,12,'0'),
	-- TO_BIGINT(i_LocationEarthquakeLimit)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationEarthquakeLimit))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationEarthquakeLimit), '0', i_LocationEarthquakeLimit), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationEarthquakeLimit IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationEarthquakeLimit AS BIGINT) >= 0, LPAD(i_LocationEarthquakeLimit, 12, '0'),
	    CAST(i_LocationEarthquakeLimit AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationEarthquakeLimit AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationEarthquakeLimit,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_LocationEarthquakeDeductible), LPAD('0',12,'0'),
	-- TO_BIGINT(i_LocationEarthquakeDeductible)>=0, LPAD(i_LocationEarthquakeDeductible,12,'0'),
	-- TO_BIGINT(i_LocationEarthquakeDeductible)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_LocationEarthquakeDeductible))),11,'0')),
	-- LPAD('0',12,'0')
	-- )
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_LocationEarthquakeDeductible), '0', i_LocationEarthquakeDeductible), 12, '0')
	DECODE(
	    TRUE,
	    i_LocationEarthquakeDeductible IS NULL, LPAD('0', 12, '0'),
	    CAST(i_LocationEarthquakeDeductible AS BIGINT) >= 0, LPAD(i_LocationEarthquakeDeductible, 12, '0'),
	    CAST(i_LocationEarthquakeDeductible AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_LocationEarthquakeDeductible AS BIGINT))), 11, '0')),
	    LPAD('0', 12, '0')
	) AS v_LocationEarthquakeDeductible,
	-- *INF*: RPAD(IIF(ISNULL(i_AgencyCode), ' ', i_AgencyCode), 10, ' ')
	RPAD(
	    IFF(
	        i_AgencyCode IS NULL, ' ', i_AgencyCode
	    ), 10, ' ') AS v_AgentCode,
	-- *INF*: RPAD(IIF(ISNULL(i_StrategicProfitCenterDescription), ' ', i_StrategicProfitCenterDescription), 50 , ' ')
	RPAD(
	    IFF(
	        i_StrategicProfitCenterDescription IS NULL, ' ', i_StrategicProfitCenterDescription
	    ), 50, ' ') AS v_StrategicProfitCenterDescription,
	-- *INF*: RPAD(IIF(ISNULL(i_InsuranceSegmentDescription), ' ', i_InsuranceSegmentDescription), 50 , ' ')
	RPAD(
	    IFF(
	        i_InsuranceSegmentDescription IS NULL, ' ', i_InsuranceSegmentDescription
	    ), 50, ' ') AS v_InsuranceSegmentDescription,
	-- *INF*: RPAD(IIF(ISNULL(i_PlusPakFlag), ' ', i_PlusPakFlag), 1, ' ')
	-- 
	-- 
	-- 
	-- ---RPAD(IIF(ISNULL(v_PlusPakIndicator_Convert), ' ', v_PlusPakIndicator_Convert), 1, ' ')
	RPAD(
	    IFF(
	        i_PlusPakFlag IS NULL, ' ', i_PlusPakFlag
	    ), 1, ' ') AS v_PlusPakFlag,
	-- *INF*: RPAD(IIF(ISNULL(i_ActualLossSustainedCoverageFlag), ' ', i_ActualLossSustainedCoverageFlag), 1, ' ')
	-- 
	-- 
	-- 
	-- ---RPAD(IIF(ISNULL(v_ActualLossSustainedCoverageIndicator_Convert), ' ', v_ActualLossSustainedCoverageIndicator_Convert), 1, ' ')
	RPAD(
	    IFF(
	        i_ActualLossSustainedCoverageFlag IS NULL, ' ', i_ActualLossSustainedCoverageFlag
	    ), 1, ' ') AS v_ActualLossSustainedCoverageFlag,
	-- *INF*: RPAD(IIF(ISNULL(i_InsuredName), ' ', i_InsuredName), 255, ' ')
	RPAD(
	    IFF(
	        i_InsuredName IS NULL, ' ', i_InsuredName
	    ), 255, ' ') AS v_InsuredName,
	-- *INF*: RPAD(IIF(ISNULL(i_CustomerNumber), ' ', i_CustomerNumber), 12, ' ')
	RPAD(
	    IFF(
	        i_CustomerNumber IS NULL, ' ', i_CustomerNumber
	    ), 12, ' ') AS v_CustomerNumber,
	-- *INF*: RPAD(IIF(ISNULL(i_PolicyEffectiveDate), ' ', i_PolicyEffectiveDate), 10, ' ')
	RPAD(
	    IFF(
	        i_PolicyEffectiveDate IS NULL, ' ', i_PolicyEffectiveDate
	    ), 10, ' ') AS v_PolicyEffectiveDate,
	-- *INF*: RPAD(IIF(ISNULL(i_PolicyExpirationDate), ' ', i_PolicyExpirationDate), 10, ' ')
	RPAD(
	    IFF(
	        i_PolicyExpirationDate IS NULL, ' ', i_PolicyExpirationDate
	    ), 10, ' ') AS v_PolicyExpirationDate,
	-- *INF*: RPAD(IIF(ISNULL(i_OccupancyCode), ' ', i_OccupancyCode), 5, ' ')
	RPAD(
	    IFF(
	        i_OccupancyCode IS NULL, ' ', i_OccupancyCode
	    ), 5, ' ') AS v_OccupancyCode,
	-- *INF*: RPAD(IIF(ISNULL(i_SicCode), ' ', i_SicCode), 4, ' ')
	RPAD(
	    IFF(
	        i_SicCode IS NULL, ' ', i_SicCode
	    ), 4, ' ') AS v_SicCode,
	-- *INF*: RPAD(IIF(ISNULL(i_TerrorismFlag), ' ', i_TerrorismFlag), 1, ' ')
	-- 
	-- 
	-- ---RPAD(IIF(ISNULL(v_TerrorismIndicator_Convert), ' ', v_TerrorismIndicator_Convert), 1, ' ')
	RPAD(
	    IFF(
	        i_TerrorismFlag IS NULL, ' ', i_TerrorismFlag
	    ), 1, ' ') AS v_TerrorismFlag,
	-- *INF*: RPAD(IIF(ISNULL(i_RoofMaterial), ' ', i_RoofMaterial), 1, ' ')
	RPAD(
	    IFF(
	        i_RoofMaterial IS NULL, ' ', i_RoofMaterial
	    ), 1, ' ') AS v_RoofMaterial,
	-- *INF*: RPAD(IIF(ISNULL(i_RoofYear), ' ', i_RoofYear), 4, ' ')
	RPAD(
	    IFF(
	        i_RoofYear IS NULL, ' ', i_RoofYear
	    ), 4, ' ') AS v_RoofYear,
	-- *INF*: RPAD(IIF(ISNULL(i_HailResistiveRoofFlag), ' ', i_HailResistiveRoofFlag), 1, ' ')
	RPAD(
	    IFF(
	        i_HailResistiveRoofFlag IS NULL, ' ', i_HailResistiveRoofFlag
	    ), 1, ' ') AS v_HailResistiveRoofFlag,
	-- *INF*: RPAD(IIF(ISNULL(i_WindHailLossSettlement), ' ', i_WindHailLossSettlement), 3, ' ')
	RPAD(
	    IFF(
	        i_WindHailLossSettlement IS NULL, ' ', i_WindHailLossSettlement
	    ), 3, ' ') AS v_WindHailLossSettlement,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_WindHailDeductibleAmount), LPAD('0',20,'0'),
	-- TO_BIGINT(i_WindHailDeductibleAmount)>=0, LPAD(i_WindHailDeductibleAmount, 20,'0'),
	-- TO_BIGINT(i_WindHailDeductibleAmount)<0, CONCAT('-', LPAD(TO_CHAR(ABS(TO_BIGINT(i_WindHailDeductibleAmount))),19,'0')),
	-- LPAD('0',20,'0')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --LPAD(IIF(ISNULL(i_WindHailDeductibleAmount), '0', i_WindHailDeductibleAmount), 20, '0')
	DECODE(
	    TRUE,
	    i_WindHailDeductibleAmount IS NULL, LPAD('0', 20, '0'),
	    CAST(i_WindHailDeductibleAmount AS BIGINT) >= 0, LPAD(i_WindHailDeductibleAmount, 20, '0'),
	    CAST(i_WindHailDeductibleAmount AS BIGINT) < 0, CONCAT('-', LPAD(TO_CHAR(ABS(CAST(i_WindHailDeductibleAmount AS BIGINT))), 19, '0')),
	    LPAD('0', 20, '0')
	) AS v_WindHailDeductibleAmount,
	-- *INF*: LPAD(IIF(ISNULL(i_LocationNumber), '0', i_LocationNumber), 4, '0')
	LPAD(
	    IFF(
	        i_LocationNumber IS NULL, '0', i_LocationNumber
	    ), 4, '0') AS v_LocationNumber,
	-- *INF*: LPAD(IIF(ISNULL(i_BuildingNumber), '0', i_BuildingNumber), 3, '0')
	LPAD(
	    IFF(
	        i_BuildingNumber IS NULL, '0', i_BuildingNumber
	    ), 3, '0') AS v_BuildingNumber,
	-- *INF*: LPAD(IIF(ISNULL(i_VehicleNumber), '0', TO_CHAR(i_VehicleNumber)), 5, '0')
	LPAD(
	    IFF(
	        i_VehicleNumber IS NULL, '0', TO_CHAR(i_VehicleNumber)
	    ), 5, '0') AS v_VehicleNumber,
	-- *INF*: RPAD(i_ProcessDate, 6, ' ')
	RPAD(i_ProcessDate, 6, ' ') AS v_ProcessDate,
	v_InsuranceReferenceLegalEntityDescription || v_Separator ||
v_PolicyKey || v_Separator ||
v_PolicyBlanketPremium || v_Separator ||
v_PolicyBlanketLimit || v_Separator ||
v_PolicyBlanketDeductible || v_Separator ||
v_LineOfBusiness || v_Separator ||
v_PolicyForm || v_Separator ||
v_StreetNumber || v_Separator ||
v_StreetName || v_Separator ||
v_City || v_Separator ||
v_State || v_Separator ||
v_ZipCode || v_Separator ||
v_County || v_Separator ||
v_ConstructionClass || v_Separator ||
v_OccupancyType || v_Separator ||
v_YearBuilt || v_Separator ||
v_ModelYear || v_Separator ||
v_LocationPremium || v_Separator ||
v_LocationLimit || v_Separator ||
v_LocationDeductible || v_Separator ||
v_LocationBuildingLimit || v_Separator ||
v_LocationBuildingDeductible || v_Separator ||
v_LocationOtherLimit || v_Separator ||
v_LocationOtherDeductible || v_Separator ||
v_LocationContentsLimit || v_Separator ||
v_LocationContentsDeductible || v_Separator ||
v_LocationBILimit || v_Separator ||
v_LocationBIDeductible || v_Separator ||
v_AdjustedBuildingLimit || v_Separator ||
v_AdjustedContentsLimit || v_Separator ||
v_AdjustedBILimit || v_Separator ||
v_AdjustedOtherLimit || v_Separator ||
v_AdjustedTotalInsuredValue || v_Separator ||
v_LocationWindCoverageFlag || v_Separator ||
v_LocationEarthquakeFlag || v_Separator ||
v_LocationEarthquakePremium || v_Separator ||
v_LocationEarthquakeLimit || v_Separator ||
v_LocationEarthquakeDeductible || v_Separator ||
v_AgentCode || v_Separator ||
v_StrategicProfitCenterDescription || v_Separator ||
v_InsuranceSegmentDescription || v_Separator ||
v_PlusPakFlag || v_Separator ||
v_ActualLossSustainedCoverageFlag || v_Separator ||
v_InsuredName || v_Separator ||
v_CustomerNumber || v_Separator ||
v_PolicyEffectiveDate || v_Separator ||
v_PolicyExpirationDate || v_Separator ||
v_OccupancyCode || v_Separator ||
v_SicCode || v_Separator ||
v_TerrorismFlag || v_Separator ||
v_RoofMaterial || v_Separator || 
v_RoofYear || v_Separator || 
v_HailResistiveRoofFlag || v_Separator ||
v_WindHailLossSettlement || v_Separator ||
v_WindHailDeductibleAmount || v_Separator ||
v_ProcessDate || v_Separator ||
v_LocationNumber || v_Separator ||
v_BuildingNumber || v_Separator ||
v_VehicleNumber  || v_Separator AS o_Record,
	-- *INF*:  'WB12529_' || RPAD(i_ProcessDate, 6, ' ') || @{pipeline().parameters.FILE_EXTENSION}
	'WB12529_' || RPAD(i_ProcessDate, 6, ' ') || @{pipeline().parameters.FILE_EXTENSION} AS o_FileName,
	2 AS o_Order
	FROM SQ_CatastropheExposureExtract
),
SQ_Title AS (
	SELECT DISTINCT 
	ProcessDate
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CatastropheExposureExtract
	WHERE
	@{pipeline().parameters.WHERE_CLAUSE_T}
),
EXP_Title AS (
	SELECT
	ProcessDate AS i_ProcessDate,
	'~' AS v_Separator,
	-- *INF*: 'LEGAL ENTITY' || v_Separator || 
	-- 'POLICY KEY' || v_Separator || 
	-- 'POL BLKT PREM' || v_Separator || 
	-- 'POL BLKT LIMIT' || v_Separator || 
	-- 'POL BLKT DED' || v_Separator || 
	-- 'LOB' || v_Separator || 
	-- 'COVERAGE FORM' || v_Separator || 
	-- 'STREET NUMBER' || v_Separator || 
	-- 'STREET NAME' || v_Separator || 
	-- 'RATING LOCATION CITY' || v_Separator || 
	-- 'RATING LOCATION STATE ABBREVIATION' || v_Separator || 
	-- 'RATING LOCATION ZIP CODE' || v_Separator || 
	-- 'RATING LOCATION COUNTY' || v_Separator || 
	-- 'CONSTRUCTION TYPE' || v_Separator || 
	-- 'RESIDENTIAL OCCUPANCY TYPE' || v_Separator || 
	-- 'YEAR BUILT' || v_Separator || 
	-- 'MODEL YEAR' || v_Separator || 
	-- 'LOC PREM' || v_Separator || 
	-- 'LOC LIMIT' || v_Separator || 
	-- 'LOC DEDCTBL' || v_Separator || 
	-- 'LOC BLDG LIMIT' || v_Separator || 
	-- 'LOC BLDG DEDUCTIBLE' || v_Separator || 
	-- 'LOC OTR LIMIT' || v_Separator || 
	-- 'LOC OTR DEDUCTIBLE' || v_Separator || 
	-- 'LOC CONTS LIMIT' || v_Separator || 
	-- 'LOC CONTS DEDUCTIBLE' || v_Separator || 
	-- 'LOC BI LIMIT' || v_Separator || 
	-- 'LOC BI DEDUCTIBLE' || v_Separator || 
	-- 'ADJUSTED BUILDING LIMIT' || v_Separator || 
	-- 'ADJUSTED CONTENTS LIMIT' || v_Separator || 
	-- 'ADJUSTED BUSINESS INCOME LIMIT' || v_Separator || 
	-- 'ADJUSTED OTHER LIMIT' || v_Separator || 
	-- 'ADJUSTED TOTAL INSURED VALUE' || v_Separator || 
	-- 'LOC WIND COV IND' || v_Separator || 
	-- 'EARTHQUAKE COVERAGE INDICATOR' || v_Separator || 
	-- 'LOC EARTHQUAKE PREM' || v_Separator || 
	-- 'LOC EARTHQUAKE LIMIT' || v_Separator || 
	-- 'LOC EARTHQUAKE DEDBL' || v_Separator || 
	-- 'AGENCY CODE' || v_Separator || 
	-- 'STRATEGIC PROFIT CENTER' || v_Separator || 
	-- 'INSURANCE SEGMENT' || v_Separator || 
	-- 'PLUS PAK COVERAGE INDICATOR' || v_Separator || 
	-- 'ACTUAL LOSS SUSTAINED INDICATOR' || v_Separator || 
	-- 'FIRST NAMED INSURED' || v_Separator || 
	-- 'CUSTOMER NUMBER' || v_Separator || 
	-- 'POLICY EFFECTIVE DATE' || v_Separator || 
	-- 'POLICY EXPIRATION DATE' || v_Separator || 
	-- 'BUSINESS CLASSIFICATION CODE' || v_Separator || 
	-- 'SIC CODE' || v_Separator || 
	-- 'TERRORISM COVERAGE INDICATOR' || v_Separator || 
	-- 'ROOF MATERIAL' || v_Separator || 
	-- 'ROOF YEAR' || v_Separator || 
	-- 'HAIL RESISTIVE' || v_Separator || 
	-- 'WIND HAIL LOSS SETTLEMENT' || v_Separator || 
	-- 'WIND/HAIL DEDUCTIBLE AMOUNT ' || v_Separator || 
	-- 'AS OF DATE (RUN DATE)' || v_Separator || 
	-- 'LOC NBR' || v_Separator || 
	-- 'BLD NBR' || v_Separator || 
	-- 'VEH NBR' || v_Separator 
	'LEGAL ENTITY' || v_Separator || 'POLICY KEY' || v_Separator || 'POL BLKT PREM' || v_Separator || 'POL BLKT LIMIT' || v_Separator || 'POL BLKT DED' || v_Separator || 'LOB' || v_Separator || 'COVERAGE FORM' || v_Separator || 'STREET NUMBER' || v_Separator || 'STREET NAME' || v_Separator || 'RATING LOCATION CITY' || v_Separator || 'RATING LOCATION STATE ABBREVIATION' || v_Separator || 'RATING LOCATION ZIP CODE' || v_Separator || 'RATING LOCATION COUNTY' || v_Separator || 'CONSTRUCTION TYPE' || v_Separator || 'RESIDENTIAL OCCUPANCY TYPE' || v_Separator || 'YEAR BUILT' || v_Separator || 'MODEL YEAR' || v_Separator || 'LOC PREM' || v_Separator || 'LOC LIMIT' || v_Separator || 'LOC DEDCTBL' || v_Separator || 'LOC BLDG LIMIT' || v_Separator || 'LOC BLDG DEDUCTIBLE' || v_Separator || 'LOC OTR LIMIT' || v_Separator || 'LOC OTR DEDUCTIBLE' || v_Separator || 'LOC CONTS LIMIT' || v_Separator || 'LOC CONTS DEDUCTIBLE' || v_Separator || 'LOC BI LIMIT' || v_Separator || 'LOC BI DEDUCTIBLE' || v_Separator || 'ADJUSTED BUILDING LIMIT' || v_Separator || 'ADJUSTED CONTENTS LIMIT' || v_Separator || 'ADJUSTED BUSINESS INCOME LIMIT' || v_Separator || 'ADJUSTED OTHER LIMIT' || v_Separator || 'ADJUSTED TOTAL INSURED VALUE' || v_Separator || 'LOC WIND COV IND' || v_Separator || 'EARTHQUAKE COVERAGE INDICATOR' || v_Separator || 'LOC EARTHQUAKE PREM' || v_Separator || 'LOC EARTHQUAKE LIMIT' || v_Separator || 'LOC EARTHQUAKE DEDBL' || v_Separator || 'AGENCY CODE' || v_Separator || 'STRATEGIC PROFIT CENTER' || v_Separator || 'INSURANCE SEGMENT' || v_Separator || 'PLUS PAK COVERAGE INDICATOR' || v_Separator || 'ACTUAL LOSS SUSTAINED INDICATOR' || v_Separator || 'FIRST NAMED INSURED' || v_Separator || 'CUSTOMER NUMBER' || v_Separator || 'POLICY EFFECTIVE DATE' || v_Separator || 'POLICY EXPIRATION DATE' || v_Separator || 'BUSINESS CLASSIFICATION CODE' || v_Separator || 'SIC CODE' || v_Separator || 'TERRORISM COVERAGE INDICATOR' || v_Separator || 'ROOF MATERIAL' || v_Separator || 'ROOF YEAR' || v_Separator || 'HAIL RESISTIVE' || v_Separator || 'WIND HAIL LOSS SETTLEMENT' || v_Separator || 'WIND/HAIL DEDUCTIBLE AMOUNT ' || v_Separator || 'AS OF DATE (RUN DATE)' || v_Separator || 'LOC NBR' || v_Separator || 'BLD NBR' || v_Separator || 'VEH NBR' || v_Separator AS v_Title,
	-- *INF*: UPPER(v_Title)
	UPPER(v_Title) AS o_Title,
	-- *INF*:  'WB12529_' || RPAD(i_ProcessDate, 6, ' ') || @{pipeline().parameters.FILE_EXTENSION}
	'WB12529_' || RPAD(i_ProcessDate, 6, ' ') || @{pipeline().parameters.FILE_EXTENSION} AS o_FileName,
	1 AS o_Order
	FROM SQ_Title
),
Union AS (
	SELECT o_Title AS Record, o_FileName AS FileName, o_Order AS Order
	FROM EXP_Title
	UNION
	SELECT o_Record AS Record, o_FileName AS FileName, o_Order AS Order
	FROM EXP_Cal
),
SRT_Order AS (
	SELECT
	Record, 
	FileName, 
	Order
	FROM Union
	ORDER BY Order ASC
),
CatastropheExposureExtract AS (
	INSERT INTO EPLIFlatFile
	(Record, FileName)
	SELECT 
	RECORD, 
	FILENAME
	FROM SRT_Order
),