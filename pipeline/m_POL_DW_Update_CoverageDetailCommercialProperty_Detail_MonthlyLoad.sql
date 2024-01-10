WITH
LKP_SupISOSpecialCauseOfLossCategoryRule AS (
	SELECT
	ISOSpecialCauseOfLossCategoryCode,
	PmsPMACode
	FROM (
		SELECT 
			ISOSpecialCauseOfLossCategoryCode,
			PmsPMACode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupISOSpecialCauseOfLossCategoryRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PmsPMACode ORDER BY ISOSpecialCauseOfLossCategoryCode) = 1
),
LKP_SupISOCommercialPropertyCauseOfLossGroup_PMS AS (
	SELECT
	ISOCommercialPropertyCauseOfLossGroup,
	ProductCode,
	MajorPerilCode
	FROM (
		SELECT 
			ISOCommercialPropertyCauseOfLossGroup,
			ProductCode,
			MajorPerilCode
		FROM SupISOCommercialPropertyCauseOfLossGroup
		WHERE CurrentSnapshotFlag=1 and SourceSystemID='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductCode,MajorPerilCode ORDER BY ISOCommercialPropertyCauseOfLossGroup) = 1
),
LKP_SupISOCommercialPropertyCauseOfLossGroup_DCT AS (
	SELECT
	ISOCommercialPropertyCauseOfLossGroup,
	ProductCode,
	SublineCode
	FROM (
		SELECT 
			ISOCommercialPropertyCauseOfLossGroup,
			ProductCode,
			SublineCode
		FROM SupISOCommercialPropertyCauseOfLossGroup
		WHERE CurrentSnapshotFlag=1 and SourceSystemID='DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductCode,SublineCode ORDER BY ISOCommercialPropertyCauseOfLossGroup) = 1
),
LKP_SupISOSpecialCauseOfLossCategoryRule_PMS AS (
	SELECT
	ISOSpecialCauseOfLossCategoryCode,
	ClassCode
	FROM (
		SELECT 
			ISOSpecialCauseOfLossCategoryCode,
			ClassCode
		FROM SupISOSpecialCauseOfLossCategoryRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode ORDER BY ISOSpecialCauseOfLossCategoryCode) = 1
),
SQ_PMS AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	SELECT distinct  pt.PremiumTransactionId,
	sc.MajorPerilCode,
	sc.ClassCode,
	sc.InsuranceReferenceLineOfBusinessAKId,
	bsc.BureauCode2,
	sc.SubLineCode,
	bsc.BureauCode1,
	RL.StateProvinceCode,
	product.ProductCode
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt on cp.PremiumTransactionId=pt.PremiumTransactionId 
		and cp.SourceSystemID='PMS' AND pt.SourceSystemId='PMS'
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage sc ON pt.StatisticalCoverageAKID=sc.StatisticalCoverageAKID 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product product ON product.ProductAKId=sc.ProductAKId
	INNER JOIN ( 
	  SELECT b2.PremiumTransactionAKID,b2.BureauCode1,b2.BureauCode2,b2.BureauCode6,b2.BureauCode9
	  FROM 
	  (
	  SELECT  b.PremiumTransactionAKID,b.BureauCode1,b.BureauCode2,b.BureauCode6,b.BureauCode9
	          ,ROW_NUMBER() OVER (PARTITION BY b.PremiumTransactionAKID  ORDER BY b.CurrentSnapshotFlag desc)  AS RN
	  FROM    @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode   b
	  WHERE B.SourceSystemID='PMS'
	  ) b2 
	  WHERE b2.RN=1
	) bsc on bsc.PremiumTransactionAKID=pt.PremiumTransactionAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON SC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.SourceSystemID='PMS'
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON   PC.RiskLocationAKId=RL.RiskLocationAKId and RL.SourceSystemID='PMS'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=0
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	SELECT distinct  pt.PremiumTransactionId,
	sc.MajorPerilCode,
	sc.ClassCode,
	sc.InsuranceReferenceLineOfBusinessAKId,
	bsc.BureauCode2,
	sc.SubLineCode,
	bsc.BureauCode1,
	RL.StateProvinceCode,
	product.ProductCode
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt on cp.PremiumTransactionId=pt.PremiumTransactionId 
		and cp.SourceSystemID='PMS' AND pt.SourceSystemId='PMS'
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage sc ON pt.StatisticalCoverageAKID=sc.StatisticalCoverageAKID 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product product ON product.ProductAKId=sc.ProductAKId
	INNER JOIN ( 
	  SELECT b2.PremiumTransactionAKID,b2.BureauCode1,b2.BureauCode2,b2.BureauCode6,b2.BureauCode9
	  FROM 
	  (
	  SELECT  b.PremiumTransactionAKID,b.BureauCode1,b.BureauCode2,b.BureauCode6,b.BureauCode9
	          ,ROW_NUMBER() OVER (PARTITION BY b.PremiumTransactionAKID  ORDER BY b.CurrentSnapshotFlag desc)  AS RN
	  FROM    @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode   b
	  WHERE B.SourceSystemID='PMS'
	  ) b2 
	  WHERE b2.RN=1
	) bsc on bsc.PremiumTransactionAKID=pt.PremiumTransactionAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON SC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.SourceSystemID='PMS'
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON   PC.RiskLocationAKId=RL.RiskLocationAKId and RL.SourceSystemID='PMS'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=1
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	SELECT distinct  pt.PremiumTransactionId,
	sc.MajorPerilCode,
	sc.ClassCode,
	sc.InsuranceReferenceLineOfBusinessAKId,
	bsc.BureauCode2,
	sc.SubLineCode,
	bsc.BureauCode1,
	RL.StateProvinceCode,
	product.ProductCode
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt on cp.PremiumTransactionId=pt.PremiumTransactionId 
		and cp.SourceSystemID='PMS' AND pt.SourceSystemId='PMS'
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage sc ON pt.StatisticalCoverageAKID=sc.StatisticalCoverageAKID 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product product ON product.ProductAKId=sc.ProductAKId
	INNER JOIN ( 
	  SELECT b2.PremiumTransactionAKID,b2.BureauCode1,b2.BureauCode2,b2.BureauCode6,b2.BureauCode9
	  FROM 
	  (
	  SELECT  b.PremiumTransactionAKID,b.BureauCode1,b.BureauCode2,b.BureauCode6,b.BureauCode9
	          ,ROW_NUMBER() OVER (PARTITION BY b.PremiumTransactionAKID  ORDER BY b.CurrentSnapshotFlag desc)  AS RN
	  FROM    @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode   b
	  WHERE B.SourceSystemID='PMS'
	  ) b2 
	  WHERE b2.RN=1
	) bsc on bsc.PremiumTransactionAKID=pt.PremiumTransactionAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON SC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.SourceSystemID='PMS'
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON   PC.RiskLocationAKId=RL.RiskLocationAKId and RL.SourceSystemID='PMS'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=2
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	SELECT distinct  pt.PremiumTransactionId,
	sc.MajorPerilCode,
	sc.ClassCode,
	sc.InsuranceReferenceLineOfBusinessAKId,
	bsc.BureauCode2,
	sc.SubLineCode,
	bsc.BureauCode1,
	RL.StateProvinceCode,
	product.ProductCode
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt on cp.PremiumTransactionId=pt.PremiumTransactionId 
		and cp.SourceSystemID='PMS' AND pt.SourceSystemId='PMS'
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage sc ON pt.StatisticalCoverageAKID=sc.StatisticalCoverageAKID 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product product ON product.ProductAKId=sc.ProductAKId
	INNER JOIN ( 
	  SELECT b2.PremiumTransactionAKID,b2.BureauCode1,b2.BureauCode2,b2.BureauCode6,b2.BureauCode9
	  FROM 
	  (
	  SELECT  b.PremiumTransactionAKID,b.BureauCode1,b.BureauCode2,b.BureauCode6,b.BureauCode9
	          ,ROW_NUMBER() OVER (PARTITION BY b.PremiumTransactionAKID  ORDER BY b.CurrentSnapshotFlag desc)  AS RN
	  FROM    @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode   b
	  WHERE B.SourceSystemID='PMS'
	  ) b2 
	  WHERE b2.RN=1
	) bsc on bsc.PremiumTransactionAKID=pt.PremiumTransactionAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON SC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.SourceSystemID='PMS'
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON   PC.RiskLocationAKId=RL.RiskLocationAKId and RL.SourceSystemID='PMS'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=3
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_CoverageDetailCommercialProperty AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	MajorPerilCode AS i_MajorPerilCode,
	ClassCode AS i_ClassCode,
	InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	BureauCode2 AS i_BureauCode2,
	SublineCode AS i_SublineCode,
	BureauCode1 AS i_BureauCode1,
	StateProvinceCode AS i_StateProvinceCode,
	ProductCode AS i_ProductCode,
	-- *INF*: :LKP.LKP_SupISOCommercialPropertyCauseOfLossGroup_PMS(i_ProductCode,i_MajorPerilCode)
	LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS_i_ProductCode_i_MajorPerilCode.ISOCommercialPropertyCauseOfLossGroup AS v_ISOPropertyCauseofLossGroup,
	-- *INF*: DECODE(true,
	-- v_ISOPropertyCauseofLossGroup='SCL' and i_BureauCode1='2',:LKP.LKP_SupISOSpecialCauseOfLossCategoryRule_PMS(i_ClassCode),
	-- v_ISOPropertyCauseofLossGroup='SCL' and i_BureauCode1!='2', '01',
	--  'N/A')
	DECODE(true,
		v_ISOPropertyCauseofLossGroup = 'SCL' 
		AND i_BureauCode1 = '2', LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORYRULE_PMS_i_ClassCode.ISOSpecialCauseOfLossCategoryCode,
		v_ISOPropertyCauseofLossGroup = 'SCL' 
		AND i_BureauCode1 != '2', '01',
		'N/A'
	) AS v_ISOSpecialCauseOfLossCategoryCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: iif(isnull(v_ISOPropertyCauseofLossGroup),'N/A',
	-- v_ISOPropertyCauseofLossGroup)
	IFF(v_ISOPropertyCauseofLossGroup IS NULL,
		'N/A',
		v_ISOPropertyCauseofLossGroup
	) AS o_ISOPropertyCauseofLossGroup,
	-- *INF*: IIF(ISNULL(v_ISOSpecialCauseOfLossCategoryCode), 'Unassigned', v_ISOSpecialCauseOfLossCategoryCode)
	IFF(v_ISOSpecialCauseOfLossCategoryCode IS NULL,
		'Unassigned',
		v_ISOSpecialCauseOfLossCategoryCode
	) AS o_ISOSpecialCauseOfLossCategory,
	-- *INF*: SUBSTR(i_ClassCode,1,4)
	-- 
	-- 
	-- --LPAD(i_ClassCode,6, '0')
	SUBSTR(i_ClassCode, 1, 4
	) AS o_ClassCode,
	-- *INF*: DECODE(TRUE,
	-- IN(i_BureauCode2, '1', '4', '5', '8'), 'Specific',
	-- IN(i_BureauCode2, '2', '3', '6', '7'), 'Class', 
	-- 'N/A')
	DECODE(TRUE,
		i_BureauCode2 IN ('1','4','5','8'), 'Specific',
		i_BureauCode2 IN ('2','3','6','7'), 'Class',
		'N/A'
	) AS o_RateType,
	i_StateProvinceCode AS o_StateProvinceCode
	FROM SQ_PMS
	LEFT JOIN LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS_i_ProductCode_i_MajorPerilCode
	ON LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS_i_ProductCode_i_MajorPerilCode.ProductCode = i_ProductCode
	AND LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS_i_ProductCode_i_MajorPerilCode.MajorPerilCode = i_MajorPerilCode

	LEFT JOIN LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORYRULE_PMS LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORYRULE_PMS_i_ClassCode
	ON LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORYRULE_PMS_i_ClassCode.ClassCode = i_ClassCode

),
SQ_DCT AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	SELECT DISTINCT pt.PremiumTransactionID as PremiumTransactionID,
	rc.ClassCode as ClassCode, 
	bridge.CauseOfLoss as CauseOfLoss, 
	bridge.PerilGroup as PerilGroup, 
	bridge.RateType as RateType, 
	bridge.PropertyType as PropertyType, 
	bridge.OccupancyCategory as OccupancyCategory,
	RL.StateProvinceCode,
	PC.InsuranceLine as LineType,
	product.ProductCode,
	rc.SublineCode
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt on cp.PremiumTransactionId=pt.PremiumTransactionId
		and cp.SourceSystemID='DCT' and pt.SourceSystemID='DCT'
	
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkPremiumTransaction wpt on pt.PremiumTransactionAKID=wpt.PremiumTransactionAKId
		and pt.SourceSystemID='DCT' and wpt.SourceSystemID='DCT' and WPT.AuditID=PT.AuditID 
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage rc on rc.RatingCoverageAKID=pt.RatingCoverageAKId 
		and RC.EffectiveDate=PT.EffectiveDate
	inner join (
	select CauseOfLoss, PerilGroup, RateType, PropertyType, OccupancyCategory, CoverageId--, LineId 
		from
		(select CauseOfLoss, PerilGroup, RateType, PropertyType, OccupancyCategory, CoverageId,-- LineId,
		ROW_NUMBER() OVER (PARTITION BY BR.CoverageId ORDER BY BR.ExtractDate desc)  AS RN
		from ArchWorkDCTTransactionInsuranceLineLocationBridge BR
		inner join ArchWorkDCTInsuranceLine L on BR.LineId=L.LineId and BR.AuditId=L.AuditId and L.LineType in ('Property','SBOPProperty','BusinessOwners')
		)b
		where RN=1
		)bridge 
	on bridge.CoverageId=wpt.PremiumTransactionStageId
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON RC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
		and PC.SourceSystemID='DCT' and PC.InsuranceLine IN ('Property','SBOPProperty','BusinessOwners')
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON   PC.RiskLocationAKId=RL.RiskLocationAKId 
		and RL.SourceSystemID='DCT' and RL.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product product ON product.ProductAKId=rc.ProductAKId 
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=0
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	SELECT DISTINCT pt.PremiumTransactionID as PremiumTransactionID,
	rc.ClassCode as ClassCode, 
	bridge.CauseOfLoss as CauseOfLoss, 
	bridge.PerilGroup as PerilGroup, 
	bridge.RateType as RateType, 
	bridge.PropertyType as PropertyType, 
	bridge.OccupancyCategory as OccupancyCategory,
	RL.StateProvinceCode,
	PC.InsuranceLine as LineType,
	product.ProductCode,
	rc.SublineCode
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt on cp.PremiumTransactionId=pt.PremiumTransactionId
		and cp.SourceSystemID='DCT' and pt.SourceSystemID='DCT'
	
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkPremiumTransaction wpt on pt.PremiumTransactionAKID=wpt.PremiumTransactionAKId
		and pt.SourceSystemID='DCT' and wpt.SourceSystemID='DCT' and WPT.AuditID=PT.AuditID 
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage rc on rc.RatingCoverageAKID=pt.RatingCoverageAKId 
		and RC.EffectiveDate=PT.EffectiveDate
	inner join (
	select CauseOfLoss, PerilGroup, RateType, PropertyType, OccupancyCategory, CoverageId--, LineId 
		from
		(select CauseOfLoss, PerilGroup, RateType, PropertyType, OccupancyCategory, CoverageId,-- LineId,
		ROW_NUMBER() OVER (PARTITION BY BR.CoverageId ORDER BY BR.ExtractDate desc)  AS RN
		from ArchWorkDCTTransactionInsuranceLineLocationBridge BR
		inner join ArchWorkDCTInsuranceLine L on BR.LineId=L.LineId and BR.AuditId=L.AuditId and L.LineType in ('Property','SBOPProperty','BusinessOwners')
		)b
		where RN=1
		)bridge 
	on bridge.CoverageId=wpt.PremiumTransactionStageId
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON RC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
		and PC.SourceSystemID='DCT' and PC.InsuranceLine IN ('Property','SBOPProperty','BusinessOwners')
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON   PC.RiskLocationAKId=RL.RiskLocationAKId 
		and RL.SourceSystemID='DCT' and RL.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product product ON product.ProductAKId=rc.ProductAKId 
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=1
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	SELECT DISTINCT pt.PremiumTransactionID as PremiumTransactionID,
	rc.ClassCode as ClassCode, 
	bridge.CauseOfLoss as CauseOfLoss, 
	bridge.PerilGroup as PerilGroup, 
	bridge.RateType as RateType, 
	bridge.PropertyType as PropertyType, 
	bridge.OccupancyCategory as OccupancyCategory,
	RL.StateProvinceCode,
	PC.InsuranceLine as LineType,
	product.ProductCode,
	rc.SublineCode
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt on cp.PremiumTransactionId=pt.PremiumTransactionId
		and cp.SourceSystemID='DCT' and pt.SourceSystemID='DCT'
	
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkPremiumTransaction wpt on pt.PremiumTransactionAKID=wpt.PremiumTransactionAKId
		and pt.SourceSystemID='DCT' and wpt.SourceSystemID='DCT' and WPT.AuditID=PT.AuditID 
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage rc on rc.RatingCoverageAKID=pt.RatingCoverageAKId 
		and RC.EffectiveDate=PT.EffectiveDate
	inner join (
	select CauseOfLoss, PerilGroup, RateType, PropertyType, OccupancyCategory, CoverageId--, LineId 
		from
		(select CauseOfLoss, PerilGroup, RateType, PropertyType, OccupancyCategory, CoverageId,-- LineId,
		ROW_NUMBER() OVER (PARTITION BY BR.CoverageId ORDER BY BR.ExtractDate desc)  AS RN
		from ArchWorkDCTTransactionInsuranceLineLocationBridge BR
		inner join ArchWorkDCTInsuranceLine L on BR.LineId=L.LineId and BR.AuditId=L.AuditId and L.LineType in ('Property','SBOPProperty','BusinessOwners')
		)b
		where RN=1
		)bridge 
	on bridge.CoverageId=wpt.PremiumTransactionStageId
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON RC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
		and PC.SourceSystemID='DCT' and PC.InsuranceLine IN ('Property','SBOPProperty','BusinessOwners')
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON   PC.RiskLocationAKId=RL.RiskLocationAKId 
		and RL.SourceSystemID='DCT' and RL.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product product ON product.ProductAKId=rc.ProductAKId 
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=2
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	SELECT DISTINCT pt.PremiumTransactionID as PremiumTransactionID,
	rc.ClassCode as ClassCode, 
	bridge.CauseOfLoss as CauseOfLoss, 
	bridge.PerilGroup as PerilGroup, 
	bridge.RateType as RateType, 
	bridge.PropertyType as PropertyType, 
	bridge.OccupancyCategory as OccupancyCategory,
	RL.StateProvinceCode,
	PC.InsuranceLine as LineType,
	product.ProductCode,
	rc.SublineCode
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt on cp.PremiumTransactionId=pt.PremiumTransactionId
		and cp.SourceSystemID='DCT' and pt.SourceSystemID='DCT'
	
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkPremiumTransaction wpt on pt.PremiumTransactionAKID=wpt.PremiumTransactionAKId
		and pt.SourceSystemID='DCT' and wpt.SourceSystemID='DCT' and WPT.AuditID=PT.AuditID 
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage rc on rc.RatingCoverageAKID=pt.RatingCoverageAKId 
		and RC.EffectiveDate=PT.EffectiveDate
	inner join (
	select CauseOfLoss, PerilGroup, RateType, PropertyType, OccupancyCategory, CoverageId--, LineId 
		from
		(select CauseOfLoss, PerilGroup, RateType, PropertyType, OccupancyCategory, CoverageId,-- LineId,
		ROW_NUMBER() OVER (PARTITION BY BR.CoverageId ORDER BY BR.ExtractDate desc)  AS RN
		from ArchWorkDCTTransactionInsuranceLineLocationBridge BR
		inner join ArchWorkDCTInsuranceLine L on BR.LineId=L.LineId and BR.AuditId=L.AuditId and L.LineType in ('Property','SBOPProperty','BusinessOwners')
		)b
		where RN=1
		)bridge 
	on bridge.CoverageId=wpt.PremiumTransactionStageId
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON RC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
		and PC.SourceSystemID='DCT' and PC.InsuranceLine IN ('Property','SBOPProperty','BusinessOwners')
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON   PC.RiskLocationAKId=RL.RiskLocationAKId 
		and RL.SourceSystemID='DCT' and RL.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product product ON product.ProductAKId=rc.ProductAKId 
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=3
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_CoverageDetailCommercialProperty1 AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	ClassCode AS i_ClassCode,
	CauseOfLoss AS i_CauseOfLoss,
	PerilGroup AS i_PerilGroup,
	RateType AS i_RateType,
	PropertyType AS i_PropertyType,
	OccupanyCategory AS i_OccupanyCategory,
	StateProvinceCode AS i_StateProvinceCode,
	LineType AS i_LineType,
	ProductCode AS i_ProductCode,
	SublineCode AS i_SublineCode,
	-- *INF*: LTRIM(RTRIM(:LKP.LKP_SupISOCommercialPropertyCauseOfLossGroup_DCT(i_ProductCode,i_SublineCode)))
	LTRIM(RTRIM(LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT_i_ProductCode_i_SublineCode.ISOCommercialPropertyCauseOfLossGroup
		)
	) AS v_ISOPropertyCauseofLossGroup,
	-- *INF*: DECODE(TRUE,
	-- v_ISOPropertyCauseofLossGroup='SCL' AND (i_PropertyType='' OR ISNULL(i_PropertyType)), 'Buildings',
	-- v_ISOPropertyCauseofLossGroup='SCL' ,iif(isnull(i_OccupanyCategory),'N/A',i_OccupanyCategory),
	-- 'N/A')
	DECODE(TRUE,
		v_ISOPropertyCauseofLossGroup = 'SCL' 
		AND ( i_PropertyType = '' 
			OR i_PropertyType IS NULL 
		), 'Buildings',
		v_ISOPropertyCauseofLossGroup = 'SCL', IFF(i_OccupanyCategory IS NULL,
			'N/A',
			i_OccupanyCategory
		),
		'N/A'
	) AS v_ISOSpecialCauseOfLossCategory,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: IIF((i_LineType='Property' or i_LineType='SBOPProperty')and not  isnull(v_ISOPropertyCauseofLossGroup),
	-- v_ISOPropertyCauseofLossGroup,'N/A')
	IFF(( i_LineType = 'Property' 
			OR i_LineType = 'SBOPProperty' 
		) 
		AND v_ISOPropertyCauseofLossGroup IS NOT NULL,
		v_ISOPropertyCauseofLossGroup,
		'N/A'
	) AS o_ISOPropertyCauseofLossGroup,
	-- *INF*: IIF(i_LineType='Property' or i_LineType='SBOPProperty',
	-- v_ISOSpecialCauseOfLossCategory,'N/A')
	IFF(i_LineType = 'Property' 
		OR i_LineType = 'SBOPProperty',
		v_ISOSpecialCauseOfLossCategory,
		'N/A'
	) AS o_ISOSpecialCauseOfLossCategory,
	-- *INF*: IIF(i_LineType='Property' or i_LineType='SBOPProperty',
	-- SUBSTR(i_ClassCode,1,4),'N/A')
	IFF(i_LineType = 'Property' 
		OR i_LineType = 'SBOPProperty',
		SUBSTR(i_ClassCode, 1, 4
		),
		'N/A'
	) AS o_ClassCode,
	-- *INF*: IIF(i_LineType='Property' or i_LineType='SBOPProperty',
	-- DECODE(TRUE,
	-- i_RateType='S', 'Specific',
	-- i_RateType='C', 'Class',
	-- 'N/A'),'N/A')
	IFF(i_LineType = 'Property' 
		OR i_LineType = 'SBOPProperty',
		DECODE(TRUE,
		i_RateType = 'S', 'Specific',
		i_RateType = 'C', 'Class',
		'N/A'
		),
		'N/A'
	) AS o_RateType,
	-- *INF*: IIF(i_LineType='Property' or i_LineType='SBOPProperty',
	-- i_StateProvinceCode,'N/A')
	-- 
	IFF(i_LineType = 'Property' 
		OR i_LineType = 'SBOPProperty',
		i_StateProvinceCode,
		'N/A'
	) AS o_StateProvinceCode
	FROM SQ_DCT
	LEFT JOIN LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT_i_ProductCode_i_SublineCode
	ON LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT_i_ProductCode_i_SublineCode.ProductCode = i_ProductCode
	AND LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT_i_ProductCode_i_SublineCode.SublineCode = i_SublineCode

),
LKP_ISOSpecialCauseOfLossCategoryCode AS (
	SELECT
	ISOSpecialCauseOfLossCategoryCode,
	ISOSpecialCauseOfLossCategoryDCTCode
	FROM (
		SELECT 
			ISOSpecialCauseOfLossCategoryCode,
			ISOSpecialCauseOfLossCategoryDCTCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupISOSpecialCauseOfLossCategory
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ISOSpecialCauseOfLossCategoryDCTCode ORDER BY ISOSpecialCauseOfLossCategoryCode) = 1
),
Union_PMS_DCT AS (
	SELECT o_PremiumTransactionID, o_ISOPropertyCauseofLossGroup, o_ISOSpecialCauseOfLossCategory, o_ClassCode, o_RateType, o_StateProvinceCode
	FROM EXP_CoverageDetailCommercialProperty
	UNION
	SELECT o_PremiumTransactionID, o_ISOPropertyCauseofLossGroup, ISOSpecialCauseOfLossCategoryCode AS o_ISOSpecialCauseOfLossCategory, o_ClassCode, o_RateType, o_StateProvinceCode
	FROM EXP_CoverageDetailCommercialProperty1
	-- Manually join with LKP_ISOSpecialCauseOfLossCategoryCode
),
LKP_SupClassificationCommercialProperty AS (
	SELECT
	ISOCPRatingGroup,
	CommercialPropertySpecialClass,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT 
			ISOCPRatingGroup,
			CommercialPropertySpecialClass,
			ClassCode,
			RatingStateCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialProperty
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY ISOCPRatingGroup) = 1
),
LKP_SupClassificationCommercialProperty_default AS (
	SELECT
	ISOCPRatingGroup,
	CommercialPropertySpecialClass,
	ClassCode
	FROM (
		SELECT 
			ISOCPRatingGroup,
			CommercialPropertySpecialClass,
			ClassCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialProperty
		WHERE CurrentSnapshotFlag=1 and RatingStateCode='99'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode ORDER BY ISOCPRatingGroup) = 1
),
EXP_Cal AS (
	SELECT
	Union_PMS_DCT.o_PremiumTransactionID AS PremiumTransactionID,
	Union_PMS_DCT.o_ISOPropertyCauseofLossGroup AS i_ISOPropertyCauseofLossGroup,
	Union_PMS_DCT.o_ISOSpecialCauseOfLossCategory AS i_ISOSpecialCauseOfLossCategory,
	Union_PMS_DCT.o_RateType AS i_RateType,
	LKP_SupClassificationCommercialProperty.ISOCPRatingGroup AS lk_ISOCPRatingGroup,
	LKP_SupClassificationCommercialProperty.CommercialPropertySpecialClass AS lk_PropertySpecialClass,
	LKP_SupClassificationCommercialProperty_default.ISOCPRatingGroup AS lk_ISOCPRatingGroup_default,
	LKP_SupClassificationCommercialProperty_default.CommercialPropertySpecialClass AS lk_PropertySpecialClass_default,
	-- *INF*: RTRIM(LTrim(IIF(i_ISOPropertyCauseofLossGroup='BGI',
	-- DECODE(TRUE,
	-- not isnull(lk_ISOCPRatingGroup),lk_ISOCPRatingGroup,
	-- not isnull(lk_ISOCPRatingGroup_default),lk_ISOCPRatingGroup_default,
	-- 'N/A'),'N/A')))
	RTRIM(LTrim(IFF(i_ISOPropertyCauseofLossGroup = 'BGI',
				DECODE(TRUE,
		lk_ISOCPRatingGroup IS NOT NULL, lk_ISOCPRatingGroup,
		lk_ISOCPRatingGroup_default IS NOT NULL, lk_ISOCPRatingGroup_default,
		'N/A'
				),
				'N/A'
			)
		)
	) AS v_ISOCPRatingGroup,
	-- *INF*: Rtrim(LTrim(DECODE(TRUE,
	-- not isnull(lk_PropertySpecialClass),lk_PropertySpecialClass,
	-- not isnull(lk_PropertySpecialClass_default),lk_PropertySpecialClass_default,
	-- 'N/A')))
	Rtrim(LTrim(DECODE(TRUE,
		lk_PropertySpecialClass IS NOT NULL, lk_PropertySpecialClass,
		lk_PropertySpecialClass_default IS NOT NULL, lk_PropertySpecialClass_default,
		'N/A'
			)
		)
	) AS v_PropertySpecialClass,
	v_ISOCPRatingGroup AS o_ISOCPRatingGroup,
	v_PropertySpecialClass AS o_PropertySpecialClass,
	-- *INF*: iif(isnull(i_ISOPropertyCauseofLossGroup),'N/A',i_ISOPropertyCauseofLossGroup)
	IFF(i_ISOPropertyCauseofLossGroup IS NULL,
		'N/A',
		i_ISOPropertyCauseofLossGroup
	) AS o_ISOPropertyCauseofLossGroup,
	-- *INF*: iif(isnull(i_ISOSpecialCauseOfLossCategory),'N/A',i_ISOSpecialCauseOfLossCategory)
	IFF(i_ISOSpecialCauseOfLossCategory IS NULL,
		'N/A',
		i_ISOSpecialCauseOfLossCategory
	) AS o_ISOSpecialCauseOfLossCategory,
	-- *INF*: iif(isnull(i_RateType),'N/A',i_RateType)
	IFF(i_RateType IS NULL,
		'N/A',
		i_RateType
	) AS o_RateType
	FROM Union_PMS_DCT
	LEFT JOIN LKP_SupClassificationCommercialProperty
	ON LKP_SupClassificationCommercialProperty.ClassCode = Union_PMS_DCT.o_ClassCode AND LKP_SupClassificationCommercialProperty.RatingStateCode = Union_PMS_DCT.o_StateProvinceCode
	LEFT JOIN LKP_SupClassificationCommercialProperty_default
	ON LKP_SupClassificationCommercialProperty_default.ClassCode = Union_PMS_DCT.o_ClassCode
),
UPD_Target AS (
	SELECT
	PremiumTransactionID AS PremiumTransactionId, 
	o_ISOPropertyCauseofLossGroup AS ISOPropertyCauseofLossGroup, 
	o_ISOCPRatingGroup AS ISOCPRatingGroup, 
	o_ISOSpecialCauseOfLossCategory AS ISOSpecialCauseOfLossCategory2, 
	o_RateType AS RateType, 
	o_PropertySpecialClass AS PropertySpecialClass
	FROM EXP_Cal
),
CoverageDetailCommercialProperty_Update AS (
	MERGE INTO CoverageDetailCommercialProperty AS T
	USING UPD_Target AS S
	ON T.PremiumTransactionID = S.PremiumTransactionId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ISOCommercialPropertyCauseofLossGroup = S.ISOPropertyCauseofLossGroup, T.ISOCommercialPropertyRatingGroupCode = S.ISOCPRatingGroup, T.ISOSpecialCauseOfLossCategoryCode = S.ISOSpecialCauseOfLossCategory2, T.RateType = S.RateType, T.CommercialPropertySpecialClass = S.PropertySpecialClass
),