WITH
SQ_WorkCatastropheExposure AS (
	SELECT
	AuditId,
	CreatedDate,
	ModifiedDate,
	SourceSystemId,
	PolicyKey,
	VehicleNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	RatingAddress,
	RatingCity,
	RatingStateProvinceAbbreviation,
	RatingPostalCode,
	RatingCounty,
	ModelYear,
	VehiclePremium,
	VehicleLimit,
	VehicleDeductible,
	AdjustedTotalInsuredValue,
	AgencyCode,
	StrategicProfitCenterDescription,
	InsuranceSegmentDescription,
	PlusPakFlag,
	'N' as EliteFlag,
	ActualLossSustainedCoverageFlag,
	InsuredName,
	CustomerNumber,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	OccupancyCode,
	SicCode,
	TerrorismFlag,
	ProcessDate,
	NULL as LocationNumber,
	NULL as BuildingNumber,
	NULL as PolicyBlanketPremium,
	NULL as PolicyBlanketLimit,
	NULL as PolicyBlanketDeductible,
	NULL as ConstructionClass,
	NULL as YearBuilt,
	NULL as BuildingPremium,
	NULL as LocationLimit,
	NULL as LocationDeductible,
	NULL as LocationBuildingLimit,
	NULL as LocationBuildingDeductible,
	NULL as LocationContentsLimit,
	NULL as LocationContentsDeductible,
	NULL as LocationBILimit,
	NULL as LocationBIDeductible,
	NULL as AdjustedBuildingLimit,
	NULL as AdjustedContentsLimit,
	NULL as AdjustedBILimit,
	NULL as LocationWindCoverageFlag,
	NULL as LocationEarthquakeFlag,
	NULL as LocationEarthquakePremium,
	NULL as LocationEarthquakeLimit,
	NULL as LocationEarthquakeDeductible,
	NULL as LocationPremium
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkCatastropheExposureVehicle
	
	UNION ALL
	
	SELECT
	AuditId,
	CreatedDate,
	ModifiedDate,
	SourceSystemId,
	PolicyKey,
	NULL as VehicleNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	RatingAddress,
	RatingCity,
	RatingStateProvinceAbbreviation,
	RatingPostalCode,
	RatingCounty,
	NULL as ModelYear,
	NULL as VehiclePremium,
	NULL as VehicleLimit,
	NULL as VehicleDeductible,
	AdjustedTotalInsuredValue,
	AgencyCode,
	StrategicProfitCenterDescription,
	InsuranceSegmentDescription,
	PlusPakFlag,
	EliteFlag,
	ActualLossSustainedCoverageFlag,
	InsuredName,
	CustomerNumber,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	OccupancyCode,
	SicCode,
	TerrorismFlag,
	ProcessDate,
	LocationNumber,
	BuildingNumber,
	PolicyBlanketPremium,
	PolicyBlanketLimit,
	PolicyBlanketDeductible,
	ConstructionClass,
	YearBuilt,
	BuildingPremium,
	LocationLimit,
	LocationDeductible,
	LocationBuildingLimit,
	LocationBuildingDeductible,
	LocationContentsLimit,
	LocationContentsDeductible,
	LocationBILimit,
	LocationBIDeductible,
	AdjustedBuildingLimit,
	AdjustedContentsLimit,
	AdjustedBILimit,
	LocationWindCoverageFlag,
	LocationEarthquakeFlag,
	LocationEarthquakePremium,
	LocationEarthquakeLimit,
	LocationEarthquakeDeductible,
	NULL as LocationPremium
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkCatastropheExposureBuilding
	
	UNION ALL
	
	SELECT
	AuditId,
	CreatedDate,
	ModifiedDate,
	SourceSystemId,
	PolicyKey,
	NULL as VehicleNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	RatingAddress,
	RatingCity,
	RatingStateProvinceAbbreviation,
	RatingPostalCode,
	RatingCounty,
	NULL as ModelYear,
	NULL as VehiclePremium,
	NULL as VehicleLimit,
	NULL as VehicleDeductible,
	AdjustedTotalInsuredValue,
	AgencyCode,
	StrategicProfitCenterDescription,
	InsuranceSegmentDescription,
	PlusPakFlag,
	'N' as EliteFlag,
	ActualLossSustainedCoverageFlag,
	InsuredName,
	CustomerNumber,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	OccupancyCode,
	SicCode,
	TerrorismFlag,
	ProcessDate,
	LocationNumber,
	NULL as BuildingNumber,
	NULL as PolicyBlanketPremium,
	NULL as PolicyBlanketLimit,
	NULL as PolicyBlanketDeductible,
	NULL as ConstructionClass,
	NULL as YearBuilt,
	NULL as BuildingPremium,
	LocationLimit,
	LocationDeductible,
	NULL as LocationBuildingLimit,
	NULL as LocationBuildingDeductible,
	NULL as LocationContentsLimit,
	NULL as LocationContentsDeductible,
	NULL as LocationBILimit,
	NULL as LocationBIDeductible,
	NULL as AdjustedBuildingLimit,
	NULL as AdjustedContentsLimit,
	NULL as AdjustedBILimit,
	LocationWindCoverageFlag,
	NULL as LocationEarthquakeFlag,
	NULL as LocationEarthquakePremium,
	NULL as LocationEarthquakeLimit,
	NULL as LocationEarthquakeDeductible,
	LocationPremium
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkCatastropheExposureLocation
	
	---@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Cal AS (
	SELECT
	AuditId,
	CreatedDate,
	ModifiedDate,
	SourceSystemId,
	PolicyKey,
	VehicleNumber,
	BusinessType,
	InsuranceReferenceLegalEntityDescription,
	RatingAddress,
	RatingCity,
	RatingStateProvinceAbbreviation,
	RatingPostalCode,
	RatingCounty,
	ModelYear,
	VehiclePremium,
	VehicleLimit,
	VehicleDeductible,
	AdjustedTotalInsuredValue,
	AgencyCode,
	StrategicProfitCenterDescription,
	InsuranceSegmentDescription,
	PlusPakFlag,
	EliteFlag,
	ActualLossSustainedCoverageFlag,
	InsuredName,
	CustomerNumber,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	OccupancyCode,
	SicCode,
	TerrorismFlag,
	ProcessDate,
	LocationNumber,
	BuildingNumber,
	PolicyBlanketPremium,
	PolicyBlanketLimit,
	PolicyBlanketDeductible,
	ConstructionClass,
	YearBuilt,
	BuildingPremium,
	LocationLimit,
	LocationDeductible,
	LocationBuildingLimit,
	LocationBuildingDeductible,
	LocationContentsLimit,
	LocationContentsDeductible,
	LocationBILimit,
	LocationBIDeductible,
	AdjustedBuildingLimit,
	AdjustedContentsLimit,
	AdjustedBILimit,
	LocationWindCoverageFlag,
	LocationEarthquakeFlag,
	LocationEarthquakePremium,
	LocationEarthquakeLimit,
	LocationEarthquakeDeductible,
	LocationPremium,
	AuditId AS o_AuditId,
	CreatedDate AS o_CreatedDate,
	ModifiedDate AS o_ModifiedDate,
	SourceSystemId AS o_SourceSystemId,
	InsuranceReferenceLegalEntityDescription AS o_InsuranceReferenceLegalEntityDescription,
	PolicyKey AS o_PolicyKey,
	-- *INF*: IIF(ISNULL(PolicyBlanketPremium), '', TO_CHAR(PolicyBlanketPremium))
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(PolicyBlanketPremium), '', SUBSTR(TO_CHAR(PolicyBlanketPremium),1,12))
	IFF(PolicyBlanketPremium IS NULL, '', TO_CHAR(PolicyBlanketPremium)) AS o_PolicyBlanketPremium,
	-- *INF*: IIF(ISNULL(PolicyBlanketLimit), '', TO_CHAR(PolicyBlanketLimit))
	-- 
	-- 
	-- 
	-- ---IIF(ISNULL(PolicyBlanketLimit), '', SUBSTR(TO_CHAR(PolicyBlanketLimit),1,12))
	IFF(PolicyBlanketLimit IS NULL, '', TO_CHAR(PolicyBlanketLimit)) AS o_PolicyBlanketLimit,
	-- *INF*: IIF(ISNULL(PolicyBlanketDeductible), '', TO_CHAR(PolicyBlanketDeductible))
	-- 
	-- 
	-- 
	-- ---IIF(ISNULL(PolicyBlanketDeductible), '', SUBSTR(TO_CHAR(PolicyBlanketDeductible),1,12))
	IFF(PolicyBlanketDeductible IS NULL, '', TO_CHAR(PolicyBlanketDeductible)) AS o_PolicyBlanketDeductible,
	-- *INF*: ''
	-- 
	-- --NULL
	'' AS o_PolicyForm,
	-- *INF*: BusinessType
	-- --DECODE(TRUE,
	-- --BusinessType='Commercial Auto', 'CAU',
	-- --BusinessType='Commercial Property', 'CFR',
	-- --BusinessType='SBOP', 'BOP',
	-- --BusinessType='SMARTbusiness', 'BOP',
	-- --BusinessType='Commercial Inland Marine', 'CIM',
	-- --BusinessType='Dealers Physical Damage', 'DPD',
	-- --BusinessType='Garagekeepers Liability', 'GKL',
	--  --'')
	BusinessType AS o_BusinessType,
	-- *INF*: DECODE(TRUE,
	-- BusinessType='Commercial Auto', 'CAU',
	-- BusinessType='Commercial Property', 'CFR',
	-- BusinessType='SBOP', 'BOP',
	-- BusinessType='SMARTbusiness', 'BOP',
	-- BusinessType='Commercial Inland Marine', 'CIM',
	-- BusinessType='Dealers Physical Damage', 'DPD',
	-- BusinessType='Garagekeepers Liability', 'GKL',
	-- '')
	-- 
	DECODE(
	    TRUE,
	    BusinessType = 'Commercial Auto', 'CAU',
	    BusinessType = 'Commercial Property', 'CFR',
	    BusinessType = 'SBOP', 'BOP',
	    BusinessType = 'SMARTbusiness', 'BOP',
	    BusinessType = 'Commercial Inland Marine', 'CIM',
	    BusinessType = 'Dealers Physical Damage', 'DPD',
	    BusinessType = 'Garagekeepers Liability', 'GKL',
	    ''
	) AS o_LineOfBusiness,
	-- *INF*: IIF(INSTR(LTRIM(RatingAddress), ' ')=0, '', SUBSTR(SUBSTR(RatingAddress, 0, INSTR(LTRIM(RatingAddress), ' ') - 1),0,12))
	IFF(
	    REGEXP_INSTR(LTRIM(RatingAddress), ' ') = 0, '',
	    SUBSTR(SUBSTR(RatingAddress, 0, REGEXP_INSTR(LTRIM(RatingAddress), ' ') - 1), 0, 12)
	) AS o_StreetNumber,
	-- *INF*: IIF(INSTR(LTRIM(RatingAddress), ' ')=0, RatingAddress, SUBSTR(RatingAddress, INSTR(LTRIM(RatingAddress), ' ') + 1))
	IFF(
	    REGEXP_INSTR(LTRIM(RatingAddress), ' ') = 0, RatingAddress,
	    SUBSTR(RatingAddress, REGEXP_INSTR(LTRIM(RatingAddress), ' ') + 1)
	) AS o_StreetName,
	RatingCity AS o_City,
	RatingStateProvinceAbbreviation AS o_State,
	RatingPostalCode AS o_ZipCode,
	RatingCounty AS o_County,
	-- *INF*: IIF(ISNULL(ConstructionClass), '', ConstructionClass)
	IFF(ConstructionClass IS NULL, '', ConstructionClass) AS o_ConstructionClass,
	-- *INF*: ''
	-- 
	-- 
	-- --NULL
	'' AS o_OccupancyType,
	-- *INF*: IIF(ISNULL(YearBuilt), '', YearBuilt)
	IFF(YearBuilt IS NULL, '', YearBuilt) AS o_YearBuilt,
	-- *INF*: IIF(ISNULL(ModelYear), '', ModelYear)
	IFF(ModelYear IS NULL, '', ModelYear) AS o_ModelYear,
	-- *INF*: DECODE(TRUE,BusinessType='Commercial Auto' ,
	--     IIF(ISNULL(VehiclePremium), '', SUBSTR(TO_CHAR(VehiclePremium),1,12)),
	-- (BusinessType='Commercial Property' OR BusinessType='SBOP' OR BusinessType='SMARTbusiness'),
	--     IIF(ISNULL(BuildingPremium), '', SUBSTR(TO_CHAR(BuildingPremium),1,12)),
	-- (BusinessType='Commercial Inland Marine' OR BusinessType='Dealers Physical Damage' OR BusinessType='Garagekeepers Liability'),
	--     IIF(ISNULL(LocationPremium), '', SUBSTR(TO_CHAR(LocationPremium),1,12)),
	-- '')
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --	CatastropheExposureStageTable='WorkCatastropheExposureVehicle',
	-- --	IIF(ISNULL(VehiclePremium), '', SUBSTR(TO_CHAR(VehiclePremium),1,12)),
	-- --	CatastropheExposureStageTable='WorkCatastropheExposureBuilding',
	-- --	IIF(ISNULL(BuildingPremium), '', SUBSTR(TO_CHAR(BuildingPremium),1,12)),
	-- --	CatastropheExposureStageTable='WorkCatastropheExposureLocation',
	-- --	IIF(ISNULL(LocationPremium), '', SUBSTR(TO_CHAR(LocationPremium),1,12)),
	-- --	''
	-- --	)
	DECODE(
	    TRUE,
	    BusinessType = 'Commercial Auto', IFF(
	        VehiclePremium IS NULL, '', SUBSTR(TO_CHAR(VehiclePremium), 1, 12)
	    ),
	    (BusinessType = 'Commercial Property' OR BusinessType = 'SBOP' OR BusinessType = 'SMARTbusiness'), IFF(
	        BuildingPremium IS NULL, '', SUBSTR(TO_CHAR(BuildingPremium), 1, 12)
	    ),
	    (BusinessType = 'Commercial Inland Marine' OR BusinessType = 'Dealers Physical Damage' OR BusinessType = 'Garagekeepers Liability'), IFF(
	        LocationPremium IS NULL, '', SUBSTR(TO_CHAR(LocationPremium), 1, 12)
	    ),
	    ''
	) AS o_LocationPremium,
	-- *INF*: DECODE(TRUE,BusinessType='Commercial Auto' , 
	--    IIF(ISNULL(VehicleLimit), '', SUBSTR(TO_CHAR(VehicleLimit),1,12)),
	--     IIF(ISNULL(LocationLimit), '', SUBSTR(TO_CHAR(LocationLimit),1,12))
	-- )
	-- 
	-- --DECODE(TRUE,
	-- --	CatastropheExposureStageTable='WorkCatastropheExposureVehicle',
	-- --	IIF(ISNULL(VehicleLimit), '', SUBSTR(TO_CHAR(VehicleLimit),1,12)),
	-- --	CatastropheExposureStageTable='WorkCatastropheExposureBuilding' OR ----CatastropheExposureStageTable='WorkCatastropheExposureLocation',
	-- --	IIF(ISNULL(LocationLimit), '', SUBSTR(TO_CHAR(LocationLimit),1,12)),
	-- --	''
	-- --	)
	DECODE(
	    TRUE,
	    BusinessType = 'Commercial Auto', IFF(
	        VehicleLimit IS NULL, '', SUBSTR(TO_CHAR(VehicleLimit), 1, 12)
	    ),
	    IFF(
	        LocationLimit IS NULL, '', SUBSTR(TO_CHAR(LocationLimit), 1, 12)
	    )
	) AS o_LocationLimit,
	-- *INF*: DECODE(TRUE,BusinessType='Commercial Auto',
	--     IIF(ISNULL(VehicleDeductible), '', SUBSTR(TO_CHAR(VehicleDeductible),1,12)),
	--     IIF(ISNULL(LocationDeductible), '', SUBSTR(TO_CHAR(LocationDeductible),1,12))
	-- )
	-- --DECODE(TRUE,
	-- --	CatastropheExposureStageTable='WorkCatastropheExposureVehicle',
	-- --	IIF(ISNULL(VehicleDeductible), '', SUBSTR(TO_CHAR(VehicleDeductible),1,12)),
	-- --	CatastropheExposureStageTable='WorkCatastropheExposureBuilding' OR --CatastropheExposureStageTable='WorkCatastropheExposureLocation',
	-- --	IIF(ISNULL(LocationDeductible), '', SUBSTR(TO_CHAR(LocationDeductible),1,12)),
	-- --	''
	-- --	)
	DECODE(
	    TRUE,
	    BusinessType = 'Commercial Auto', IFF(
	        VehicleDeductible IS NULL, '', SUBSTR(TO_CHAR(VehicleDeductible), 1, 12)
	    ),
	    IFF(
	        LocationDeductible IS NULL, '', SUBSTR(TO_CHAR(LocationDeductible), 1, 12)
	    )
	) AS o_LocationDeductible,
	-- *INF*: IIF(ISNULL(LocationBuildingLimit), '', TO_CHAR(LocationBuildingLimit))
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(LocationBuildingLimit), '', SUBSTR(TO_CHAR(LocationBuildingLimit),1,12))
	IFF(LocationBuildingLimit IS NULL, '', TO_CHAR(LocationBuildingLimit)) AS o_LocationBuildingLimit,
	-- *INF*: IIF(ISNULL(LocationBuildingDeductible), '', TO_CHAR(LocationBuildingDeductible))
	-- 
	-- 
	-- --IIF(ISNULL(LocationBuildingDeductible), '', SUBSTR(TO_CHAR(LocationBuildingDeductible),1,12))
	IFF(LocationBuildingDeductible IS NULL, '', TO_CHAR(LocationBuildingDeductible)) AS o_LocationBuildingDeductible,
	-- *INF*: ''
	-- 
	-- --NULL
	'' AS o_LocationOtherLimit,
	-- *INF*: ''
	-- 
	-- 
	-- --NULL
	'' AS o_LocationOtherDeductible,
	-- *INF*: IIF(ISNULL(LocationContentsLimit), '', TO_CHAR(LocationContentsLimit))
	-- 
	-- --IIF(ISNULL(LocationContentsLimit), '', SUBSTR(TO_CHAR(LocationContentsLimit),1,12))
	IFF(LocationContentsLimit IS NULL, '', TO_CHAR(LocationContentsLimit)) AS o_LocationContentsLimit,
	-- *INF*: IIF(ISNULL(LocationContentsDeductible), '', TO_CHAR(LocationContentsDeductible))
	-- 
	-- 
	-- --IIF(ISNULL(LocationContentsDeductible), '', SUBSTR(TO_CHAR(LocationContentsDeductible),1,12))
	IFF(LocationContentsDeductible IS NULL, '', TO_CHAR(LocationContentsDeductible)) AS o_LocationContentsDeductible,
	-- *INF*: IIF(ISNULL(LocationBILimit), '', TO_CHAR(LocationBILimit))
	-- 
	-- 
	-- --IIF(ISNULL(LocationBILimit), '', SUBSTR(TO_CHAR(LocationBILimit),1,12))
	IFF(LocationBILimit IS NULL, '', TO_CHAR(LocationBILimit)) AS o_LocationBILimit,
	-- *INF*: IIF(ISNULL(LocationBIDeductible), '', TO_CHAR(LocationBIDeductible))
	-- 
	-- 
	-- --IIF(ISNULL(LocationBIDeductible), '', SUBSTR(TO_CHAR(LocationBIDeductible),1,12))
	IFF(LocationBIDeductible IS NULL, '', TO_CHAR(LocationBIDeductible)) AS o_LocationBIDeductible,
	-- *INF*: IIF(ISNULL(AdjustedBuildingLimit), '', TO_CHAR(AdjustedBuildingLimit))
	-- 
	-- 
	-- 
	-- ---IIF(ISNULL(AdjustedBuildingLimit), '', SUBSTR(TO_CHAR(AdjustedBuildingLimit),1,12))
	IFF(AdjustedBuildingLimit IS NULL, '', TO_CHAR(AdjustedBuildingLimit)) AS o_AdjustedBuildingLimit,
	-- *INF*: IIF(ISNULL(AdjustedContentsLimit), '', TO_CHAR(AdjustedContentsLimit))
	-- 
	-- 
	-- ---IIF(ISNULL(AdjustedContentsLimit), '', SUBSTR(TO_CHAR(AdjustedContentsLimit),1,12))
	IFF(AdjustedContentsLimit IS NULL, '', TO_CHAR(AdjustedContentsLimit)) AS o_AdjustedContentsLimit,
	-- *INF*: IIF(ISNULL(AdjustedBILimit), '', TO_CHAR(AdjustedBILimit))
	-- 
	-- 
	-- 
	-- 
	-- ---IIF(ISNULL(AdjustedBILimit), '', SUBSTR(TO_CHAR(AdjustedBILimit),1,12))
	IFF(AdjustedBILimit IS NULL, '', TO_CHAR(AdjustedBILimit)) AS o_AdjustedBILimit,
	-- *INF*: ''
	-- 
	-- --NULL
	'' AS o_AdjustedOtherLimit,
	-- *INF*: IIF(ISNULL(AdjustedTotalInsuredValue), '', SUBSTR(TO_CHAR(AdjustedTotalInsuredValue),1,12))
	IFF(
	    AdjustedTotalInsuredValue IS NULL, '', SUBSTR(TO_CHAR(AdjustedTotalInsuredValue), 1, 12)
	) AS o_AdjustedTotalInsuredValue,
	-- *INF*: IIF ((BusinessType='Commercial Auto'  or
	-- BusinessType='Commercial Inland Marine' 
	-- ),'',IIF(LocationWindCoverageFlag='T','Y','')
	-- )
	-- 
	-- 
	-- 
	-- --DECODE(LocationWindCoverageIndicator,'T',1,'F',0,NULL)
	IFF(
	    (BusinessType = 'Commercial Auto' or BusinessType = 'Commercial Inland Marine'), '',
	    IFF(
	        LocationWindCoverageFlag = 'T', 'Y', ''
	    )
	) AS o_LocationWindCoverageFlag,
	-- *INF*: IIF ((BusinessType='Commercial Auto'  or BusinessType='Commercial Inland Marine'  or 
	-- BusinessType='Dealers Physical Damage'  or
	-- BusinessType='Garagekeepers Liability'),'',IIF(LocationEarthquakeFlag='T','Y','N')
	-- )
	-- 
	-- --DECODE(LocationEarthquakeIndicator,'T',1,'F',0,NULL)
	IFF(
	    (BusinessType = 'Commercial Auto'
	    or BusinessType = 'Commercial Inland Marine'
	    or BusinessType = 'Dealers Physical Damage'
	    or BusinessType = 'Garagekeepers Liability'),
	    '',
	    IFF(
	        LocationEarthquakeFlag = 'T', 'Y', 'N'
	    )
	) AS o_LocationEarthquakeFlag,
	-- *INF*: IIF(ISNULL(LocationEarthquakePremium), '', TO_CHAR(LocationEarthquakePremium))
	-- 
	-- 
	-- --IIF(ISNULL(LocationEarthquakePremium), '', SUBSTR(TO_CHAR(LocationEarthquakePremium),1,12))
	IFF(LocationEarthquakePremium IS NULL, '', TO_CHAR(LocationEarthquakePremium)) AS o_LocationEarthquakePremium,
	-- *INF*: IIF(ISNULL(LocationEarthquakeLimit), '', TO_CHAR(LocationEarthquakeLimit))
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(LocationEarthquakeLimit), '', SUBSTR(TO_CHAR(LocationEarthquakeLimit),1,12))
	IFF(LocationEarthquakeLimit IS NULL, '', TO_CHAR(LocationEarthquakeLimit)) AS o_LocationEarthquakeLimit,
	-- *INF*: IIF(ISNULL(LocationEarthquakeDeductible), '', TO_CHAR(LocationEarthquakeDeductible))
	-- 
	-- 
	-- --IIF(ISNULL(LocationEarthquakeDeductible), '', SUBSTR(TO_CHAR(LocationEarthquakeDeductible),1,12))
	IFF(LocationEarthquakeDeductible IS NULL, '', TO_CHAR(LocationEarthquakeDeductible)) AS o_LocationEarthquakeDeductible,
	AgencyCode AS o_AgencyCode,
	StrategicProfitCenterDescription AS o_StrategicProfitCenterDescription,
	InsuranceSegmentDescription AS o_InsuranceSegmentDescription,
	-- *INF*: DECODE(TRUE,
	-- EliteFlag = 'L','L',
	-- EliteFlag = 'S','S',
	-- PlusPakFlag = 'T','Y',
	-- 'N')
	-- --indicate L for Elite, S for Essential, Y for PlusPak
	-- --IIF(PlusPakFlag='T','Y','N')
	-- 
	-- 
	-- --DECODE(PlusPakIndicator,'T',1,'F',0,NULL)
	DECODE(
	    TRUE,
	    EliteFlag = 'L', 'L',
	    EliteFlag = 'S', 'S',
	    PlusPakFlag = 'T', 'Y',
	    'N'
	) AS o_PlusPakFlag,
	-- *INF*: IIF(ActualLossSustainedCoverageFlag='T','Y','N')
	-- 
	-- 
	-- --DECODE(ActualLossSustainedCoverageIndicator,'T',1,'F',0,NULL)
	IFF(ActualLossSustainedCoverageFlag = 'T', 'Y', 'N') AS o_ActualLossSustainedCoverageFlag,
	InsuredName AS o_InsuredName,
	CustomerNumber AS o_CustomerNumber,
	-- *INF*: SUBSTR(TO_CHAR(PolicyEffectiveDate, 'MM/DD/YYYY HH24:MI:SS'),1,10)
	SUBSTR(TO_CHAR(PolicyEffectiveDate, 'MM/DD/YYYY HH24:MI:SS'), 1, 10) AS o_PolicyEffectiveDate,
	-- *INF*: SUBSTR(TO_CHAR(PolicyExpirationDate, 'MM/DD/YYYY HH24:MI:SS'),1,10)
	SUBSTR(TO_CHAR(PolicyExpirationDate, 'MM/DD/YYYY HH24:MI:SS'), 1, 10) AS o_PolicyExpirationDate,
	-- *INF*: DECODE(TRUE, OccupancyCode='N/A', '' ,OccupancyCode)
	-- 
	-- --OccupancyCode
	DECODE(
	    TRUE,
	    OccupancyCode = 'N/A', '',
	    OccupancyCode
	) AS o_OccupancyCode,
	SicCode AS o_SicCode,
	-- *INF*: IIF(TerrorismFlag='T','Y','N')
	-- 
	-- 
	-- 
	-- --DECODE(TerrorismIndicator,'T',1,'F',0,NULL)
	IFF(TerrorismFlag = 'T', 'Y', 'N') AS o_TerrorismFlag,
	-- *INF*: ''
	-- 
	-- --NULL
	'' AS o_RoofMaterial,
	-- *INF*: ''
	-- 
	-- --NULL
	'' AS o_RoofYear,
	-- *INF*: ''
	-- 
	-- --NULL
	'' AS o_HailResistiveRoofFlag,
	-- *INF*: ''
	-- 
	-- --NULL
	'' AS o_WindHailLossSettlement,
	-- *INF*: ''
	-- 
	-- --NULL
	'' AS o_WindHailDeductibleAmount,
	-- *INF*: REPLACECHR(0, SUBSTR(TO_CHAR(ProcessDate, 'YYYY-MM-DD HH24:MI:SS'),1,7), '-', '')
	REGEXP_REPLACE(SUBSTR(TO_CHAR(ProcessDate, 'YYYY-MM-DD HH24:MI:SS'), 1, 7),'-','','i') AS o_ProcessDate
	FROM SQ_WorkCatastropheExposure
),
FIL_Remove_ZeroValue_Limits_Deductibles AS (
	SELECT
	o_AuditId AS AuditId, 
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_SourceSystemId AS SourceSystemId, 
	o_InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription, 
	o_PolicyKey AS PolicyKey, 
	o_PolicyBlanketPremium AS PolicyBlanketPremium, 
	o_PolicyBlanketLimit AS PolicyBlanketLimit, 
	o_PolicyBlanketDeductible AS PolicyBlanketDeductible, 
	o_BusinessType AS BusinessType, 
	VehicleNumber, 
	o_LineOfBusiness AS LineOfBusiness, 
	LocationNumber, 
	BuildingNumber, 
	o_PolicyForm AS PolicyForm, 
	o_StreetNumber AS StreetNumber, 
	o_StreetName AS StreetName, 
	o_City AS City, 
	o_State AS State, 
	o_ZipCode AS ZipCode, 
	o_County AS County, 
	o_ConstructionClass AS ConstructionClass, 
	o_OccupancyType AS OccupancyType, 
	o_YearBuilt AS YearBuilt, 
	o_ModelYear AS ModelYear, 
	o_LocationPremium AS LocationPremium, 
	o_LocationLimit AS LocationLimit, 
	o_LocationDeductible AS LocationDeductible, 
	o_LocationBuildingLimit AS LocationBuildingLimit, 
	o_LocationBuildingDeductible AS LocationBuildingDeductible, 
	o_LocationOtherLimit AS LocationOtherLimit, 
	o_LocationOtherDeductible AS LocationOtherDeductible, 
	o_LocationContentsLimit AS LocationContentsLimit, 
	o_LocationContentsDeductible AS LocationContentsDeductible, 
	o_LocationBILimit AS LocationBILimit, 
	o_LocationBIDeductible AS LocationBIDeductible, 
	o_AdjustedBuildingLimit AS AdjustedBuildingLimit, 
	o_AdjustedContentsLimit AS AdjustedContentsLimit, 
	o_AdjustedBILimit AS AdjustedBILimit, 
	o_AdjustedOtherLimit AS AdjustedOtherLimit, 
	o_AdjustedTotalInsuredValue AS AdjustedTotalInsuredValue, 
	o_LocationWindCoverageFlag AS LocationWindCoverageFlag, 
	o_LocationEarthquakeFlag AS LocationEarthquakeFlag, 
	o_LocationEarthquakePremium AS LocationEarthquakePremium, 
	o_LocationEarthquakeLimit AS LocationEarthquakeLimit, 
	o_LocationEarthquakeDeductible AS LocationEarthquakeDeductible, 
	o_AgencyCode AS AgencyCode, 
	o_StrategicProfitCenterDescription AS StrategicProfitCenterDescription, 
	o_InsuranceSegmentDescription AS InsuranceSegmentDescription, 
	o_PlusPakFlag AS PlusPakFlag, 
	o_ActualLossSustainedCoverageFlag AS ActualLossSustainedCoverageFlag, 
	o_InsuredName AS InsuredName, 
	o_CustomerNumber AS CustomerNumber, 
	o_PolicyEffectiveDate AS PolicyEffectiveDate, 
	o_PolicyExpirationDate AS PolicyExpirationDate, 
	o_OccupancyCode AS OccupancyCode, 
	o_SicCode AS SicCode, 
	o_TerrorismFlag AS TerrorismFlag, 
	o_RoofMaterial AS RoofMaterial, 
	o_RoofYear AS RoofYear, 
	o_HailResistiveRoofFlag AS HailResistiveRoofFlag, 
	o_WindHailLossSettlement AS WindHailLossSettlement, 
	o_WindHailDeductibleAmount AS WindHailDeductibleAmount, 
	o_ProcessDate AS ProcessDate
	FROM EXP_Cal
	WHERE IIF((PolicyBlanketLimit='0' or PolicyBlanketLimit='')
and (PolicyBlanketDeductible='0' or PolicyBlanketDeductible='')
and (LocationLimit='0' or LocationLimit='')
and (LocationDeductible='0' or LocationDeductible='')
and (LocationBuildingLimit='0' or LocationBuildingLimit='')
and (LocationBuildingDeductible='0' or LocationBuildingDeductible='')
and (LocationOtherLimit='0' or LocationOtherLimit='')
and (LocationOtherDeductible='0' or LocationOtherDeductible='')
and (LocationContentsLimit='0' or LocationContentsLimit='')
and (LocationContentsDeductible='0' or LocationContentsDeductible='')
and (LocationBILimit='0' or LocationBILimit='')
and (LocationBIDeductible='0' or LocationBIDeductible='')
and (AdjustedBuildingLimit='0' or AdjustedBuildingLimit='')
and (AdjustedContentsLimit='0' or AdjustedContentsLimit='')
and (AdjustedBILimit='0' or AdjustedBILimit='')
and (AdjustedOtherLimit='0' or AdjustedOtherLimit='')
and (LocationEarthquakeLimit='0' or LocationEarthquakeLimit='')
and (LocationEarthquakeDeductible='0' or LocationEarthquakeDeductible='')
and (WindHailDeductibleAmount='0' or WindHailDeductibleAmount=''),
0,1)=1
),
CatastropheExposureExtract AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CatastropheExposureExtract
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, PolicyKey, LocationNumber, BuildingNumber, VehicleNumber, BusinessType, InsuranceReferenceLegalEntityDescription, PolicyBlanketPremium, PolicyBlanketLimit, PolicyBlanketDeductible, LineOfBusiness, PolicyForm, StreetNumber, StreetName, City, State, ZipCode, County, ConstructionClass, OccupancyType, YearBuilt, ModelYear, LocationPremium, LocationLimit, LocationDeductible, LocationBuildingLimit, LocationBuildingDeductible, LocationOtherLimit, LocationOtherDeductible, LocationContentsLimit, LocationContentsDeductible, LocationBILimit, LocationBIDeductible, AdjustedBuildingLimit, AdjustedContentsLimit, AdjustedBILimit, AdjustedOtherLimit, AdjustedTotalInsuredValue, LocationWindCoverageFlag, LocationEarthquakeFlag, LocationEarthquakePremium, LocationEarthquakeLimit, LocationEarthquakeDeductible, AgencyCode, StrategicProfitCenterDescription, InsuranceSegmentDescription, PlusPakFlag, ActualLossSustainedCoverageFlag, InsuredName, CustomerNumber, PolicyEffectiveDate, PolicyExpirationDate, OccupancyCode, SicCode, TerrorismFlag, RoofMaterial, RoofYear, HailResistiveRoofFlag, WindHailLossSettlement, WindHailDeductibleAmount, ProcessDate)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SOURCESYSTEMID, 
	POLICYKEY, 
	LOCATIONNUMBER, 
	BUILDINGNUMBER, 
	VEHICLENUMBER, 
	BUSINESSTYPE, 
	INSURANCEREFERENCELEGALENTITYDESCRIPTION, 
	POLICYBLANKETPREMIUM, 
	POLICYBLANKETLIMIT, 
	POLICYBLANKETDEDUCTIBLE, 
	LINEOFBUSINESS, 
	POLICYFORM, 
	STREETNUMBER, 
	STREETNAME, 
	CITY, 
	STATE, 
	ZIPCODE, 
	COUNTY, 
	CONSTRUCTIONCLASS, 
	OCCUPANCYTYPE, 
	YEARBUILT, 
	MODELYEAR, 
	LOCATIONPREMIUM, 
	LOCATIONLIMIT, 
	LOCATIONDEDUCTIBLE, 
	LOCATIONBUILDINGLIMIT, 
	LOCATIONBUILDINGDEDUCTIBLE, 
	LOCATIONOTHERLIMIT, 
	LOCATIONOTHERDEDUCTIBLE, 
	LOCATIONCONTENTSLIMIT, 
	LOCATIONCONTENTSDEDUCTIBLE, 
	LOCATIONBILIMIT, 
	LOCATIONBIDEDUCTIBLE, 
	ADJUSTEDBUILDINGLIMIT, 
	ADJUSTEDCONTENTSLIMIT, 
	ADJUSTEDBILIMIT, 
	ADJUSTEDOTHERLIMIT, 
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
	ROOFMATERIAL, 
	ROOFYEAR, 
	HAILRESISTIVEROOFFLAG, 
	WINDHAILLOSSSETTLEMENT, 
	WINDHAILDEDUCTIBLEAMOUNT, 
	PROCESSDATE
	FROM FIL_Remove_ZeroValue_Limits_Deductibles
),