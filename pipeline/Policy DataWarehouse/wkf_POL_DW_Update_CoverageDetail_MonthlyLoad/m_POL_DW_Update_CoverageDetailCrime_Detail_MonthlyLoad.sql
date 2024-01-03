WITH
LKP_SupClassificationCrime AS (
	SELECT
	IndustryGroup,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT 
			IndustryGroup,
			ClassCode,
			RatingStateCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCrime
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY IndustryGroup) = 1
),
SQ_CoverageDetailCrime AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	select t.PremiumTransactionID as PremiumTransactionID,
	sc.ClassCode as ClassCode,
	rl.StateProvinceCode as StateCode,
	pt.EffectiveDate as PTExpDate
	 from   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCrime t
	inner join PremiumTransaction PT
	on t.PremiumTransactionID=PT.PremiumTransactionID
	inner join StatisticalCoverage SC 
	on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
	and PT.EffectiveDate=SC.EffectiveDate
	inner join PolicyCoverage PC 
	on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
	inner join RiskLocation RL 
	on RL.RiskLocationAKID = PC.RiskLocationAKID
	and PT.SourceSystemID = 'PMS'
	@{pipeline().parameters.WHERE_CLAUSE}
	union all
	select t.PremiumTransactionID as PremiumTransactionID,
	rc.ClassCode as ClassCode,
	rl.StateProvinceCode as StateCode,
	pt.EffectiveDate as PTExpDate
	 from    @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCrime t
	inner join PremiumTransaction PT
	on t.PremiumTransactionID=PT.PremiumTransactionID
	inner join RatingCoverage RC 
	on PT.RatingCoverageAKId=RC.RatingCoverageAKID
	and PT.EffectiveDate=RC.EffectiveDate
	inner join PolicyCoverage PC 
	on PC.PolicyCoverageAKID = RC.PolicyCoverageAKID 
	inner join RiskLocation RL 
	on RL.RiskLocationAKID = PC.RiskLocationAKID
	and PT.SourceSystemID = 'DCT'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	ClassCode AS i_ClassCode,
	StateProvinceCode AS i_StateCode,
	EffectiveDate AS i_PTExpDate,
	-- *INF*: IIF( NOT ISNULL(:LKP.LKP_SupClassificationCrime(i_ClassCode,i_StateCode) ) , :LKP.LKP_SupClassificationCrime(i_ClassCode, i_StateCode) , 'N/A')
	IFF(NOT LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.IndustryGroup IS NULL, LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.IndustryGroup, 'N/A') AS v_lkp_result,
	-- *INF*: IIF( v_lkp_result ='N/A', 
	-- IIF( NOT ISNULL(:LKP.LKP_SupClassificationCrime(i_ClassCode,'99') ) , :LKP.LKP_SupClassificationCrime(i_ClassCode, '99') , 'N/A')
	--   ,v_lkp_result )
	-- --IIF( NOT ISNULL(:LKP.LKP_SupClassificationCrime(i_ClassCode,'99') ) , :LKP.LKP_SupClassificationCrime(i_ClassCode, '99') , 'N/A'), 
	-- 
	-- 
	IFF(v_lkp_result = 'N/A', IFF(NOT LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.IndustryGroup IS NULL, LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.IndustryGroup, 'N/A'), v_lkp_result) AS v_lkp_result_99,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: LTRIM(RTRIM( v_lkp_result_99))
	LTRIM(RTRIM(v_lkp_result_99)) AS o_IndustryGroup
	FROM SQ_CoverageDetailCrime
	LEFT JOIN LKP_SUPCLASSIFICATIONCRIME LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode
	ON LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.RatingStateCode = i_StateCode

	LEFT JOIN LKP_SUPCLASSIFICATIONCRIME LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99
	ON LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.RatingStateCode = '99'

),
UPD_ADDFIVECOLUMNS AS (
	SELECT
	o_PremiumTransactionID AS PremiumTransactionID, 
	o_IndustryGroup AS IndustryGroup
	FROM EXP_MetaData
),
CoverageDetailCrime1 AS (
	MERGE INTO CoverageDetailCrime AS T
	USING UPD_ADDFIVECOLUMNS AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.IndustryGroup = S.IndustryGroup
),