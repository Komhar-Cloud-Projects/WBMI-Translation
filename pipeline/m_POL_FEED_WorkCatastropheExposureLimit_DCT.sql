WITH
SQ_WorkCatastropheExposureTransaction AS (
	SELECT DISTINCT WCET.SourceSystemId,
		POL.pol_key,
		RL.LocationUnitNumber,
		RC.SubLocationUnitNumber,
		NULL as VehicleNumber,
		WCET.BusinessType,
		LMT.CoverageLimitType,
	      FIRST_VALUE(LMT.CoverageLimitValue) OVER (PARTITION BY WCET.SourceSystemId, POL.pol_key, RL.LocationUnitNumber, RC.CoverageType, RC.SubLocationUnitNumber, WCET.BusinessType, LMT.CoverageLimitType ORDER BY pt.PremiumTransactionEffectiveDate desc, pt.PremiumTransactionEnteredDate desc,PT.Effectivedate desc, CLB.CreatedDate desc,lmt.coveragelimitvalue desc) AS CoverageLimitValue,
		0.0 as CostNew,
	      RC.CoverageType
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction WCET
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON WCET.PremiumTransactionAKId = PT.PremiumTransactionAKID
		AND PT.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC ON PT.RatingCoverageAKID = RC.RatingCoverageAKID
		AND RC.EffectiveDate = PT.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL ON PC.RiskLocationAKID = RL.RiskLocationAKID
		AND RL.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL ON POL.pol_ak_id = RL.PolicyAKID
		AND POL.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB ON CLB.PremiumTransactionAKId = PT.PremiumTransactionAKID
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit LMT ON CLB.CoverageLimitId = LMT.CoverageLimitId
	WHERE WCET.BusinessType <> 'Commercial Auto'
	and LMT.CoverageLimitType Not In (@{pipeline().parameters.SUM_UP_LIMIT_TYPES}) 
	
	UNION ALL
	
	SELECT DISTINCT WCET.SourceSystemId,
		POL.pol_key,
		NULL as LocationUnitNumber,
		NULL as SubLocationUnitNumber,
		CDCA.VehicleNumber,
		WCET.BusinessType,
		NULL as CoverageLimitType,
		NULL as CoverageLimitValue,
	      FIRST_VALUE(CDCA.CostNew) OVER (PARTITION BY WCET.SourceSystemId, POL.pol_key, CDCA.VehicleNumber, WCET.BusinessType ORDER BY PT.PremiumTransactionEffectiveDate desc, PT.PremiumTransactionEnteredDate desc, PT.Effectivedate desc, CDCA.CreatedDate desc) AS CostNew,
	      RC.CoverageType
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction WCET
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON WCET.PremiumTransactionAKId = PT.PremiumTransactionAKID
		AND PT.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC ON PT.RatingCoverageAKID = RC.RatingCoverageAKID
		AND RC.EffectiveDate = PT.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL ON PC.RiskLocationAKID = RL.RiskLocationAKID
		AND RL.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL ON POL.pol_ak_id = RL.PolicyAKID
		AND POL.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA ON CDCA.PremiumTransactionId = PT.PremiumTransactionId
	WHERE WCET.BusinessType = 'Commercial Auto'
	
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	
	select distinct SourceSystemId,
					pol_key,
					LocationUnitNumber,
					SubLocationUnitNumber,
					VehicleNumber,
					BusinessType,
					CoverageLimitType,
					FIRST_VALUE(cast(CoverageLimitValue as varchar)) OVER (PARTITION BY SourceSystemId, pol_key, LocationUnitNumber, SubLocationUnitNumber, BusinessType,CoverageType, CoverageLimitType ORDER BY PremiumTransactionEffectiveDate desc, PremiumTransactionEnteredDate desc,Effectivedate desc, CreatedDate desc),
					CostNew,
					CoverageType
					from (
	SELECT DISTINCT WCET.SourceSystemId,
					POL.pol_key,
					RL.LocationUnitNumber,
					RC.SubLocationUnitNumber,
					NULL as VehicleNumber,
					WCET.BusinessType,
					LMT.CoverageLimitType,
					RC.CoverageType,
	 (Case When LMT.CoverageLimitType in ('Scheduled Property','Equipment Scheduled') then
			        Sum(cast(LMT.CoverageLimitValue as bigint)*CLB.CoverageLimitIDCount) OVER (PARTITION BY WCET.SourceSystemId, POL.pol_key, RL.LocationUnitNumber, RC.SubLocationUnitNumber, WCET.BusinessType, RC.CoverageType,LMT.CoverageLimitType ,pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate ) 
	              when LMT.CoverageLimitType in ('GKLL') then
		               max(cast(LMT.CoverageLimitValue as bigint)) OVER (PARTITION BY WCET.SourceSystemId, POL.pol_key, RL.LocationUnitNumber, RC.SubLocationUnitNumber, WCET.BusinessType, RC.CoverageType,LMT.CoverageLimitType ,pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate ) 
				     else
					Sum(cast(LMT.CoverageLimitValue as bigint)) OVER (PARTITION BY WCET.SourceSystemId, POL.pol_key, RL.LocationUnitNumber, RC.SubLocationUnitNumber, WCET.BusinessType, RC.CoverageType,LMT.CoverageLimitType ,pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate ) 
				    end) AS CoverageLimitValue,			  
		  pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate,
		0.0 as CostNew
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureTransaction WCET
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON WCET.PremiumTransactionAKId = PT.PremiumTransactionAKID
		AND PT.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC ON PT.RatingCoverageAKID = RC.RatingCoverageAKID
		AND RC.EffectiveDate = PT.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL ON PC.RiskLocationAKID = RL.RiskLocationAKID
		AND RL.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL ON POL.pol_ak_id = RL.PolicyAKID
		AND POL.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB ON CLB.PremiumTransactionAKId = PT.PremiumTransactionAKID
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit LMT ON CLB.CoverageLimitId = LMT.CoverageLimitId
	WHERE WCET.BusinessType <> 'Commercial Auto'
	and LMT.CoverageLimitType In (@{pipeline().parameters.SUM_UP_LIMIT_TYPES})  
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
),
EXP_MetaData AS (
	SELECT
	SourceSystemId AS i_SourceSystemId,
	pol_key AS i_pol_key,
	LocationUnitNumber AS i_LocationUnitNumber,
	SubLocationUnitNumber AS i_SubLocationUnitNumber,
	VehicleNumber AS i_VehicleNumber,
	BusinessType AS i_BusinessType,
	CoverageLimitType AS i_CoverageLimitType,
	CoverageLimitValue AS i_CoverageLimitValue,
	CostNew AS i_CostNew,
	CoverageType AS i_CoverageType,
	-- *INF*: DECODE(TRUE,
	-- i_BusinessType =  'Commercial Auto',
	-- 'TotalVehicleCost', 
	-- i_BusinessType = 'SMARTbusiness' and IN(i_CoverageType, 'Earthquake', 'Building') and i_CoverageLimitType = 'Building', 
	-- i_CoverageLimitType||i_CoverageType,
	-- i_CoverageLimitType)
	-- 
	-- 
	-- --IIF(i_BusinessType= 'Commercial Auto',
	-- --'TotalVehicleCost', 
	-- --i_CoverageLimitType)
	-- 
	-- --IIF(i_BusinessType= 'Commercial Auto', IIF(i_StatedAmount='1', 'StatedAmount', NULL), i_CoverageLimitType)
	DECODE(
	    TRUE,
	    i_BusinessType = 'Commercial Auto', 'TotalVehicleCost',
	    i_BusinessType = 'SMARTbusiness' and i_CoverageType IN ('Earthquake','Building') and i_CoverageLimitType = 'Building', i_CoverageLimitType || i_CoverageType,
	    i_CoverageLimitType
	) AS v_LimitType,
	-- *INF*: IIF(i_BusinessType= 'Commercial Auto', 
	-- TO_CHAR(i_CostNew), 
	-- i_CoverageLimitValue)
	-- 
	-- --IIF(i_BusinessType= 'Commercial Auto', IIF(i_StatedAmount='1', TO_CHAR(i_CostNew), NULL), i_CoverageLimitValue)
	IFF(i_BusinessType = 'Commercial Auto', TO_CHAR(i_CostNew), i_CoverageLimitValue) AS v_LimitValue,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_SourceSystemId AS o_SourceSystemId,
	i_pol_key AS o_PolicyKey,
	-- *INF*: IIF(IN(i_BusinessType, 'Commercial Property','SBOP','SMARTbusiness','Dealers Physical Damage','Garagekeepers Liability','Commercial Inland Marine'), i_LocationUnitNumber,NULL)
	IFF(
	    i_BusinessType IN ('Commercial Property','SBOP','SMARTbusiness','Dealers Physical Damage','Garagekeepers Liability','Commercial Inland Marine'),
	    i_LocationUnitNumber,
	    NULL
	) AS o_LocationNumber,
	-- *INF*: IIF(IN(i_BusinessType, 'Commercial Property','SBOP','SMARTbusiness'),  i_SubLocationUnitNumber,NULL)
	IFF(
	    i_BusinessType IN ('Commercial Property','SBOP','SMARTbusiness'), i_SubLocationUnitNumber,
	    NULL
	) AS o_BuildingNumber,
	-- *INF*: IIF(i_BusinessType= 'Commercial Auto', i_VehicleNumber, NULL)
	IFF(i_BusinessType = 'Commercial Auto', i_VehicleNumber, NULL) AS o_VehicleNumber,
	i_BusinessType AS o_BusinessType,
	-- *INF*: SUBSTR(v_LimitType, INSTR(REG_REPLACE(v_LimitType,'[a-zA-Z]','#'),'#',1), LENGTH(v_LimitType))
	-- 
	-- 
	-- --RTRIM(LTRIM(REG_REPLACE(v_LimitType, '[^0-9]', '')))
	SUBSTR(v_LimitType, REGEXP_INSTR(REGEXP_REPLACE(v_LimitType, '[a-zA-Z]', '#'), '#', 1), LENGTH(v_LimitType)) AS o_LimitType,
	v_LimitValue AS o_LimitValue
	FROM SQ_WorkCatastropheExposureTransaction
),
FIL_NULL AS (
	SELECT
	o_AuditId AS AuditId, 
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_SourceSystemId AS SourceSystemId, 
	o_PolicyKey AS PolicyKey, 
	o_LocationNumber AS LocationNumber, 
	o_BuildingNumber AS BuildingNumber, 
	o_VehicleNumber AS VehicleNumber, 
	o_BusinessType AS BusinessType, 
	o_LimitType AS LimitType, 
	o_LimitValue AS LimitValue
	FROM EXP_MetaData
	WHERE NOT ISNULL(LimitType)
),
WorkCatastropheExposureLimit AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureLimit;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureLimit
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, PolicyKey, LocationNumber, BuildingNumber, VehicleNumber, BusinessType, LimitType, LimitValue)
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
	LIMITTYPE, 
	LIMITVALUE
	FROM FIL_NULL
),