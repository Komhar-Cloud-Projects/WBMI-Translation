WITH
LKP_FiveColumns AS (
	SELECT
	lkp_result,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT ClassCode as ClassCode,
		RatingStateCode as RatingStateCode,
			VehicleTypeSize+'@1'
		       +BusinessUseClass+'@2'
			   +SecondaryClass+'@3'
			   +FleetType+'@4'
			   +SecondaryClassGroup+'@5'
			+RadiusofOperation+'@6'
		      as lkp_result
		  FROM dbo.SupClassificationCommercialAuto
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY lkp_result) = 1
),
SQ_CoverageDetailCommercialAuto_DCT AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	--select T.PremiumTransactionID as PremiumTransactionID,
	--sc.ClassCode as ClassCode,
	--rl.StateProvinceCode as StateCode,
	--pt.EffectiveDate as PTExpDate
	 --from   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	--inner join PremiumTransaction PT
	--on T.PremiumTransactionID=PT.PremiumTransactionID
	--inner join StatisticalCoverage SC 
	--on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	--inner join PolicyCoverage PC 
	--on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
	--inner join RiskLocation RL 
	--on RL.RiskLocationAKID = PC.RiskLocationAKID
	--and PT.SourceSystemID = 'PMS'
	--@{pipeline().parameters.WHERE_CLAUSE}
	--union all
	select T.PremiumTransactionID as PremiumTransactionID,
	RC.ClassCode as ClassCode,
	RL.StateProvinceCode as StateCode,
	PT.EffectiveDate as PTExpDate
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT on T.PremiumTransactionID=PT.PremiumTransactionID
			and PT.SourceSystemID = 'DCT' and T.SourceSystemID='DCT'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC on PT.RatingCoverageAKId=RC.RatingCoverageAKID 
		and PT.EffectiveDate=RC.EffectiveDate
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC on PC.PolicyCoverageAKID = RC.PolicyCoverageAKID 
		and PC.SourceSystemID='DCT'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID 
		and RL.SourceSystemID='DCT' and RL.CurrentSnapshotFlag=1
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=0 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	--select T.PremiumTransactionID as PremiumTransactionID,
	--sc.ClassCode as ClassCode,
	--rl.StateProvinceCode as StateCode,
	--pt.EffectiveDate as PTExpDate
	 --from   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	--inner join PremiumTransaction PT
	--on T.PremiumTransactionID=PT.PremiumTransactionID
	--inner join StatisticalCoverage SC 
	--on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	--inner join PolicyCoverage PC 
	--on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
	--inner join RiskLocation RL 
	--on RL.RiskLocationAKID = PC.RiskLocationAKID
	--and PT.SourceSystemID = 'PMS'
	--@{pipeline().parameters.WHERE_CLAUSE}
	--union all
	select T.PremiumTransactionID as PremiumTransactionID,
	RC.ClassCode as ClassCode,
	RL.StateProvinceCode as StateCode,
	PT.EffectiveDate as PTExpDate
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT on T.PremiumTransactionID=PT.PremiumTransactionID
			and PT.SourceSystemID = 'DCT' and T.SourceSystemID='DCT'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC on PT.RatingCoverageAKId=RC.RatingCoverageAKID 
		and PT.EffectiveDate=RC.EffectiveDate
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC on PC.PolicyCoverageAKID = RC.PolicyCoverageAKID 
		and PC.SourceSystemID='DCT'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID 
		and RL.SourceSystemID='DCT' and RL.CurrentSnapshotFlag=1
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=1 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	--select T.PremiumTransactionID as PremiumTransactionID,
	--sc.ClassCode as ClassCode,
	--rl.StateProvinceCode as StateCode,
	--pt.EffectiveDate as PTExpDate
	 --from   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	--inner join PremiumTransaction PT
	--on T.PremiumTransactionID=PT.PremiumTransactionID
	--inner join StatisticalCoverage SC 
	--on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	--inner join PolicyCoverage PC 
	--on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
	--inner join RiskLocation RL 
	--on RL.RiskLocationAKID = PC.RiskLocationAKID
	--and PT.SourceSystemID = 'PMS'
	--@{pipeline().parameters.WHERE_CLAUSE}
	--union all
	select T.PremiumTransactionID as PremiumTransactionID,
	RC.ClassCode as ClassCode,
	RL.StateProvinceCode as StateCode,
	PT.EffectiveDate as PTExpDate
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT on T.PremiumTransactionID=PT.PremiumTransactionID
			and PT.SourceSystemID = 'DCT' and T.SourceSystemID='DCT'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC on PT.RatingCoverageAKId=RC.RatingCoverageAKID 
		and PT.EffectiveDate=RC.EffectiveDate
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC on PC.PolicyCoverageAKID = RC.PolicyCoverageAKID 
		and PC.SourceSystemID='DCT'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID 
		and RL.SourceSystemID='DCT' and RL.CurrentSnapshotFlag=1
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=2 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	--select T.PremiumTransactionID as PremiumTransactionID,
	--sc.ClassCode as ClassCode,
	--rl.StateProvinceCode as StateCode,
	--pt.EffectiveDate as PTExpDate
	 --from   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	--inner join PremiumTransaction PT
	--on T.PremiumTransactionID=PT.PremiumTransactionID
	--inner join StatisticalCoverage SC 
	--on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	--inner join PolicyCoverage PC 
	--on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
	--inner join RiskLocation RL 
	--on RL.RiskLocationAKID = PC.RiskLocationAKID
	--and PT.SourceSystemID = 'PMS'
	--@{pipeline().parameters.WHERE_CLAUSE}
	--union all
	select T.PremiumTransactionID as PremiumTransactionID,
	RC.ClassCode as ClassCode,
	RL.StateProvinceCode as StateCode,
	PT.EffectiveDate as PTExpDate
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT on T.PremiumTransactionID=PT.PremiumTransactionID
			and PT.SourceSystemID = 'DCT' and T.SourceSystemID='DCT'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC on PT.RatingCoverageAKId=RC.RatingCoverageAKID 
		and PT.EffectiveDate=RC.EffectiveDate
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC on PC.PolicyCoverageAKID = RC.PolicyCoverageAKID 
		and PC.SourceSystemID='DCT'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID 
		and RL.SourceSystemID='DCT' and RL.CurrentSnapshotFlag=1
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=3 
	@{pipeline().parameters.WHERE_CLAUSE}
),
SQ_CoverageDetailCommercialAuto_PMS AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select T.PremiumTransactionID as PremiumTransactionID,
	SC.ClassCode as ClassCode,
	RL.StateProvinceCode as StateCode,
	PT.EffectiveDate as PTExpDate
	 from   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT on T.PremiumTransactionID=PT.PremiumTransactionID 
		and PT.SourceSystemID = 'PMS' and T.SourceSystemID='PMS'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
		and PC.SourceSystemID='PMS'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID 
		and RL.SourceSystemID='PMS' and RL.CurrentSnapshotFlag=1
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=0 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select T.PremiumTransactionID as PremiumTransactionID,
	SC.ClassCode as ClassCode,
	RL.StateProvinceCode as StateCode,
	PT.EffectiveDate as PTExpDate
	 from   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT on T.PremiumTransactionID=PT.PremiumTransactionID 
		and PT.SourceSystemID = 'PMS' and T.SourceSystemID='PMS'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
		and PC.SourceSystemID='PMS'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID 
		and RL.SourceSystemID='PMS' and RL.CurrentSnapshotFlag=1
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=1 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select T.PremiumTransactionID as PremiumTransactionID,
	SC.ClassCode as ClassCode,
	RL.StateProvinceCode as StateCode,
	PT.EffectiveDate as PTExpDate
	 from   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT on T.PremiumTransactionID=PT.PremiumTransactionID 
		and PT.SourceSystemID = 'PMS' and T.SourceSystemID='PMS'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
		and PC.SourceSystemID='PMS'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID 
		and RL.SourceSystemID='PMS' and RL.CurrentSnapshotFlag=1
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=2 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select T.PremiumTransactionID as PremiumTransactionID,
	SC.ClassCode as ClassCode,
	RL.StateProvinceCode as StateCode,
	PT.EffectiveDate as PTExpDate
	 from   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto T
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT on T.PremiumTransactionID=PT.PremiumTransactionID 
		and PT.SourceSystemID = 'PMS' and T.SourceSystemID='PMS'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
		and PC.SourceSystemID='PMS'
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID 
		and RL.SourceSystemID='PMS' and RL.CurrentSnapshotFlag=1
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=3 
	@{pipeline().parameters.WHERE_CLAUSE}
),
Union_PMS_DCT AS (
	SELECT PremiumTransactionID, ClassCode, StateProvinceCode, EffectiveDate
	FROM SQ_CoverageDetailCommercialAuto_PMS
	UNION
	SELECT PremiumTransactionID, ClassCode, StateProvinceCode, EffectiveDate
	FROM SQ_CoverageDetailCommercialAuto_DCT
),
EXP_MetaData AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	ClassCode AS i_ClassCode,
	StateProvinceCode AS i_StateCode,
	EffectiveDate AS i_PTExpDate,
	-- *INF*: DECODE(true,
	-- NOT ISNULL(:LKP.LKP_FiveColumns(i_ClassCode,i_StateCode)),:LKP.LKP_FiveColumns(i_ClassCode,i_StateCode),
	-- NOT ISNULL(:LKP.LKP_FiveColumns(i_ClassCode,'99')),:LKP.LKP_FiveColumns(i_ClassCode,'99'),
	-- 'N/A')
	DECODE(true,
		LKP_FIVECOLUMNS_i_ClassCode_i_StateCode.lkp_result IS NOT NULL, LKP_FIVECOLUMNS_i_ClassCode_i_StateCode.lkp_result,
		LKP_FIVECOLUMNS_i_ClassCode_99.lkp_result IS NOT NULL, LKP_FIVECOLUMNS_i_ClassCode_99.lkp_result,
		'N/A'
	) AS v_lkp_result,
	-- *INF*: SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1)
	SUBSTR(v_lkp_result, 1, REGEXP_INSTR(v_lkp_result, '@1'
		) - 1
	) AS v_CommercialAutoVehicleTypeSize,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@1')+2,instr(v_lkp_result,'@2')-instr(v_lkp_result,'@1')-2)
	SUBSTR(v_lkp_result, REGEXP_INSTR(v_lkp_result, '@1'
		) + 2, REGEXP_INSTR(v_lkp_result, '@2'
		) - REGEXP_INSTR(v_lkp_result, '@1'
		) - 2
	) AS v_CommercialAutoBusinessUseClass,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@2')+2,instr(v_lkp_result,'@3')-instr(v_lkp_result,'@2')-2)
	SUBSTR(v_lkp_result, REGEXP_INSTR(v_lkp_result, '@2'
		) + 2, REGEXP_INSTR(v_lkp_result, '@3'
		) - REGEXP_INSTR(v_lkp_result, '@2'
		) - 2
	) AS v_SecondaryClass,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@3')+2,instr(v_lkp_result,'@4')-instr(v_lkp_result,'@3')-2)
	SUBSTR(v_lkp_result, REGEXP_INSTR(v_lkp_result, '@3'
		) + 2, REGEXP_INSTR(v_lkp_result, '@4'
		) - REGEXP_INSTR(v_lkp_result, '@3'
		) - 2
	) AS v_FleetType,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@4')+2,instr(v_lkp_result,'@5')-instr(v_lkp_result,'@4')-2)
	SUBSTR(v_lkp_result, REGEXP_INSTR(v_lkp_result, '@4'
		) + 2, REGEXP_INSTR(v_lkp_result, '@5'
		) - REGEXP_INSTR(v_lkp_result, '@4'
		) - 2
	) AS v_SecondaryClassGroup,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@5')+2,instr(v_lkp_result,'@6')-instr(v_lkp_result,'@5')-2)
	SUBSTR(v_lkp_result, REGEXP_INSTR(v_lkp_result, '@5'
		) + 2, REGEXP_INSTR(v_lkp_result, '@6'
		) - REGEXP_INSTR(v_lkp_result, '@5'
		) - 2
	) AS v_RadiusofOperation,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: IIF(LENGTH(v_CommercialAutoVehicleTypeSize)<1,'N/A',v_CommercialAutoVehicleTypeSize)
	IFF(LENGTH(v_CommercialAutoVehicleTypeSize
		) < 1,
		'N/A',
		v_CommercialAutoVehicleTypeSize
	) AS o_CommercialAutoVehicleTypeSize,
	-- *INF*: IIF(LENGTH(v_CommercialAutoBusinessUseClass)<1,'N/A',v_CommercialAutoBusinessUseClass)
	IFF(LENGTH(v_CommercialAutoBusinessUseClass
		) < 1,
		'N/A',
		v_CommercialAutoBusinessUseClass
	) AS o_CommercialAutoBusinessUseClass,
	-- *INF*: IIF(LENGTH(v_SecondaryClass)<1,'N/A',v_SecondaryClass)
	IFF(LENGTH(v_SecondaryClass
		) < 1,
		'N/A',
		v_SecondaryClass
	) AS o_SecondaryClass,
	-- *INF*: IIF(LENGTH(v_FleetType)<1,'N/A',v_FleetType)
	IFF(LENGTH(v_FleetType
		) < 1,
		'N/A',
		v_FleetType
	) AS o_FleetType,
	-- *INF*: IIF(LENGTH(v_SecondaryClassGroup)<1,'N/A',v_SecondaryClassGroup)
	IFF(LENGTH(v_SecondaryClassGroup
		) < 1,
		'N/A',
		v_SecondaryClassGroup
	) AS o_SecondaryClassGroup,
	-- *INF*: IIF(LENGTH(v_RadiusofOperation)<1,'N/A',v_RadiusofOperation)
	IFF(LENGTH(v_RadiusofOperation
		) < 1,
		'N/A',
		v_RadiusofOperation
	) AS o_RadiusofOperation
	FROM Union_PMS_DCT
	LEFT JOIN LKP_FIVECOLUMNS LKP_FIVECOLUMNS_i_ClassCode_i_StateCode
	ON LKP_FIVECOLUMNS_i_ClassCode_i_StateCode.ClassCode = i_ClassCode
	AND LKP_FIVECOLUMNS_i_ClassCode_i_StateCode.RatingStateCode = i_StateCode

	LEFT JOIN LKP_FIVECOLUMNS LKP_FIVECOLUMNS_i_ClassCode_99
	ON LKP_FIVECOLUMNS_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_FIVECOLUMNS_i_ClassCode_99.RatingStateCode = '99'

),
UPD_ADDFIVECOLUMNS AS (
	SELECT
	o_PremiumTransactionID AS PremiumTransactionID, 
	o_CommercialAutoVehicleTypeSize AS VehicleTypeSize, 
	o_CommercialAutoBusinessUseClass AS BusinessUseClass, 
	o_SecondaryClass AS SecondaryClass, 
	o_FleetType AS FleetType, 
	o_SecondaryClassGroup AS SecondaryClassGroup, 
	o_RadiusofOperation AS RadiusofOperation
	FROM EXP_MetaData
),
CoverageDetailCommercialAuto AS (
	MERGE INTO CoverageDetailCommercialAuto AS T
	USING UPD_ADDFIVECOLUMNS AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.RadiusOfOperation = S.RadiusofOperation, T.VehicleTypeSize = S.VehicleTypeSize, T.BusinessUseClass = S.BusinessUseClass, T.SecondaryClass = S.SecondaryClass, T.FleetType = S.FleetType, T.SecondaryClassGroup = S.SecondaryClassGroup
),