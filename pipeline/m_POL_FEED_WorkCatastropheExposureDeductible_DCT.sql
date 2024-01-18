WITH
SQ_WorkCatastropheExposureTransaction AS (
	select distinct WCET.SourceSystemId,
	POL.pol_key,
	RL.LocationUnitNumber,
	RC.RiskType,
	RC.SubLocationUnitNumber,
	CDCA.VehicleNumber,
	WCET.BusinessType,
	CD.CoverageDeductibleType,
	FIRST_VALUE(CD.CoverageDeductibleValue) OVER (PARTITION BY WCET.SourceSystemId, POL.pol_key, RL.LocationUnitNumber, RC.RiskType, RC.SubLocationUnitNumber, CDCA.VehicleNumber, WCET.BusinessType, CD.CoverageDeductibleType ORDER BY PT.PremiumTransactionEffectiveDate desc, PT.PremiumTransactionEnteredDate desc, PT.Effectivedate desc, CDB.CreatedDate desc) AS CoverageDeductibleValue
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
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
	on CDB.PremiumTransactionAKId = PT.PremiumTransactionAKID
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible CD 
	on CDB.CoverageDeductibleId = CD.CoverageDeductibleId
	left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA
	on CDCA.PremiumTransactionId=PT.PremiumTransactionId
	
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	SourceSystemId AS i_SourceSystemId,
	pol_key AS i_pol_key,
	LocationUnitNumber AS i_LocationUnitNumber,
	RiskType AS i_RiskType,
	SubLocationUnitNumber AS i_SubLocationUnitNumber,
	VehicleNumber AS i_VehicleNumber,
	BusinessType AS i_BusinessType,
	CoverageDeductibleType AS i_CoverageDeductibleType,
	CoverageDeductibleValue AS i_CoverageDeductibleValue,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_SourceSystemId), 'N/A',i_SourceSystemId)
	IFF(i_SourceSystemId IS NULL, 'N/A', i_SourceSystemId) AS o_SourceSystemId,
	-- *INF*: IIF(ISNULL(i_pol_key), 'N/A', i_pol_key)
	IFF(i_pol_key IS NULL, 'N/A', i_pol_key) AS o_PolicyKey,
	-- *INF*: IIF(IN(i_BusinessType, 'Commercial Property','SBOP','SMARTbusiness','Dealers Physical Damage','Garagekeepers Liability', 'Commercial Inland Marine'), i_LocationUnitNumber, NULL)
	IFF(
	    i_BusinessType IN ('Commercial Property','SBOP','SMARTbusiness','Dealers Physical Damage','Garagekeepers Liability','Commercial Inland Marine'),
	    i_LocationUnitNumber,
	    NULL
	) AS o_LocationNumber,
	-- *INF*: IIF(IN(i_BusinessType, 'Commercial Property','SBOP','SMARTbusiness'), i_SubLocationUnitNumber, NULL)
	IFF(
	    i_BusinessType IN ('Commercial Property','SBOP','SMARTbusiness'), i_SubLocationUnitNumber,
	    NULL
	) AS o_BuildingNumber,
	-- *INF*: IIF(i_BusinessType= 'Commercial Auto', i_VehicleNumber, NULL)
	IFF(i_BusinessType = 'Commercial Auto', i_VehicleNumber, NULL) AS o_VehicleNumber,
	i_BusinessType AS o_BusinessType,
	-- *INF*: DECODE(TRUE,
	-- i_BusinessType= 'Commercial Property' and i_CoverageDeductibleType= 'EarthquakeStandard', i_CoverageDeductibleType || i_RiskType,
	-- i_BusinessType= 'SBOP' and i_CoverageDeductibleType= 'EarthquakeStandard', i_CoverageDeductibleType || i_RiskType,
	-- i_CoverageDeductibleType)
	DECODE(
	    TRUE,
	    i_BusinessType = 'Commercial Property' and i_CoverageDeductibleType = 'EarthquakeStandard', i_CoverageDeductibleType || i_RiskType,
	    i_BusinessType = 'SBOP' and i_CoverageDeductibleType = 'EarthquakeStandard', i_CoverageDeductibleType || i_RiskType,
	    i_CoverageDeductibleType
	) AS o_DeductibleType,
	i_CoverageDeductibleValue AS o_DeductibleValue
	FROM SQ_WorkCatastropheExposureTransaction
),
WorkCatastropheExposureDeductible AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureDeductible;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureDeductible
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, PolicyKey, LocationNumber, BuildingNumber, VehicleNumber, BusinessType, DeductibleType, DeductibleValue)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_PolicyKey AS POLICYKEY, 
	o_LocationNumber AS LOCATIONNUMBER, 
	o_BuildingNumber AS BUILDINGNUMBER, 
	o_VehicleNumber AS VEHICLENUMBER, 
	o_BusinessType AS BUSINESSTYPE, 
	o_DeductibleType AS DEDUCTIBLETYPE, 
	o_DeductibleValue AS DEDUCTIBLEVALUE
	FROM EXP_MetaData
),