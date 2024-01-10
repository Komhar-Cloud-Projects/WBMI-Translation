WITH
LKP_CoveraeLimitValue AS (
	SELECT
	CoverageLimitValue,
	PremiumTransactionAKId,
	CoverageLimitType
	FROM (
		SELECT CL.CoverageLimitValue as CoverageLimitValue, 
		CLB.PremiumTransactionAKId as PremiumTransactionAKId, 
		CL.CoverageLimitType as CoverageLimitType 
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL ON CL.CoverageLimitId=CLB.CoverageLimitId
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON CLB.PremiumtransactionAKID = PT.PremiumtransactionAKID
		WHERE PT.modifieddate> '@{pipeline().parameters.SELECTION_START_TS}'
		order by CLB.PremiumTransactionAKId,CL.CoverageLimitType,CLB.CreatedDate desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageLimitType ORDER BY CoverageLimitValue) = 1
),
SQ_CoverageDetailCommercialUmbrella AS (
	select CDD.CoverageDetailDimId AS CoverageDetailDimId,
	CDD.EffectiveDate AS EffectiveDate,
	CDD.ExpirationDate AS ExpirationDate,
	CDD.CoverageGuid AS CoverageGuid,
	RC.CoverageType AS CoverageType,
	PT.SourceSystemID AS SourceSystemID,
	CDCU.UmbrellaCoverageScope,
	CDCU.RetroactiveDate,
	PT.PremiumTransactionAKID,
	CDCU.UmbrellaLayer,
	PL.PolicyPerOccurenceLimit,
	PL.PolicyAggregateLimit,
	PT.PremiumTransactionEnteredDate,
	ISNULL(RC.PolicyCoverageAKID,-1) PolicyCoverageAKID,
	ISNULL(PT.RatingCoverageAKID,-1) RatingCoverageAKID,
	Count(1) OVER (partition BY PremiumTransactionAKID) Num_Records
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	ON CDD.EDWPremiumTransactionPKId=PT.PremiumTransactionID
	JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialUmbrella CDCU
	ON CDCU.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	LEFT JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKId=RC.RatingCoverageAKId and RC.EffectiveDate=PT.EffectiveDate
	LEFT JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PL
	on PC.PolicyLimitAKID=PL.PolicyLimitAKId --and PL.CurrentSnapshotFlag=1 
	and PL.InsuranceLine='CommercialUmbrella'
	and PL.EffectiveDate <= PT.PremiumTransactionEnteredDate 
	and PL.ExpirationDate >= PT.PremiumTransactionEnteredDate
	where CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}' @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	CoverageDetailDimId AS i_CoverageDetailDimId,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	CoverageGuid AS i_CoverageGuid,
	CoverageType AS i_CoverageType,
	SourceSystemID AS i_SourceSystemID,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	-- *INF*: RTRIM(LTRIM(i_CoverageGuid))
	RTRIM(LTRIM(i_CoverageGuid
		)
	) AS o_CoverageGuid,
	-- *INF*: RTRIM(LTRIM(i_CoverageType))
	RTRIM(LTRIM(i_CoverageType
		)
	) AS o_CoverageType,
	-- *INF*: RTRIM(LTRIM(i_SourceSystemID))
	RTRIM(LTRIM(i_SourceSystemID
		)
	) AS o_SourceSystemID,
	UmbrellaCoverageScope AS i_UmbrellaCoverageScope,
	-- *INF*: RTRIM(LTRIM(i_UmbrellaCoverageScope))
	RTRIM(LTRIM(i_UmbrellaCoverageScope
		)
	) AS o_UmbrellaCoverageScope,
	RetroactiveDate AS i_RetroactiveDate,
	i_RetroactiveDate AS o_RetroactiveDate,
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	i_PremiumTransactionAKID AS o_PremiumTransactionAKID,
	UmbrellaLayer,
	PolicyPerOccurenceLimit,
	PolicyAggregateLimit,
	PolicyCoverageAKID,
	RatingCoverageAKID,
	Num_Records,
	-- *INF*: DECODE(TRUE,
	-- Num_Records>1, IIF((PolicyCoverageAKID=-1) OR (RatingCoverageAKID=-1),'FAIL','PASS'),
	-- 'PASS')
	DECODE(TRUE,
		Num_Records > 1, IFF(( PolicyCoverageAKID = - 1 
			) 
			OR ( RatingCoverageAKID = - 1 
			),
			'FAIL',
			'PASS'
		),
		'PASS'
	) AS CheckFlag
	FROM SQ_CoverageDetailCommercialUmbrella
),
EXP_Business_Rules AS (
	SELECT
	o_CoverageDetailDimId AS CoverageDetailDimId,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_CoverageGuid AS CoverageGuid,
	o_CoverageType AS i_CoverageType,
	o_SourceSystemID AS i_SourceSystemID,
	o_PremiumTransactionAKID AS i_PremiumTransactionAKID,
	PolicyPerOccurenceLimit AS i_PolicyPerOccurenceLimit,
	PolicyAggregateLimit AS i_PolicyAggregateLimit,
	-- *INF*: :LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Umbrella AGGREGATE LIMIT')
	LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_AGGREGATE_LIMIT.CoverageLimitValue AS v_AggregateLimit,
	-- *INF*: :LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Umbrella GENERAL AGGREGATE LIMIT')
	LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_GENERAL_AGGREGATE_LIMIT.CoverageLimitValue AS v_GeneralAggregateLimit,
	-- *INF*: :LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Umbrella EACH OCCURRENCE LIMIT')
	LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_EACH_OCCURRENCE_LIMIT.CoverageLimitValue AS v_EachOccuranceLimit,
	-- *INF*: IIF(ISNULL(v_AggregateLimit),v_GeneralAggregateLimit,v_AggregateLimit)
	IFF(v_AggregateLimit IS NULL,
		v_GeneralAggregateLimit,
		v_AggregateLimit
	) AS v_UmbrellaLimit_Agg,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS', v_UmbrellaLimit_Agg,
	-- i_SourceSystemID='DCT', i_PolicyPerOccurenceLimit,
	-- 'N/A'
	-- )
	-- 
	-- 
	-- --i_SourceSystemID='DCT',:LKP.LKP_POLICYLIMIT(i_PremiumTransactionAKID),
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', v_UmbrellaLimit_Agg,
		i_SourceSystemID = 'DCT', i_PolicyPerOccurenceLimit,
		'N/A'
	) AS v_UmbrellaLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Umbrella PERSONAL INJURY AND ADVERTISING INJURY LIMIT'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_PERSONAL_INJURY_AND_ADVERTISING_INJURY_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UmbrellaPersonalInjuryLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'UNDERINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_UNDERINSURED_MOTORISTS_COVERAGE_EACH_ACCIDENT_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UmbrellaUnderInsuredLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'UNINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_UNINSURED_MOTORISTS_COVERAGE_EACH_ACCIDENT_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UmbrellaUninsuredLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS','N/A',
	-- i_SourceSystemID='DCT',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'RetentionLimit'),
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', 'N/A',
		i_SourceSystemID = 'DCT', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_RetentionLimit.CoverageLimitValue,
		'N/A'
	) AS v_UmbrellaRetentionLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - EACH OCCURRENCE LIMIT'),
	-- i_SourceSystemID='DCT',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'OccurrenceLimit'),
	-- 'N/A'
	-- )
	-- 
	-- 
	-- 
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_OCCURRENCE_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_OccurrenceLimit.CoverageLimitValue,
		'N/A'
	) AS v_UnderlyingPerOccurrenceLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - EACH OCCURRENCE LIMIT'),
	-- i_SourceSystemID='DCT','N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_OCCURRENCE_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'N/A'
	) AS v_UnderlyingPerOccurrenceClaimLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS' ,:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - GENERAL AGGREGATE LIMIT'),
	-- i_SourceSystemID='DCT' AND i_CoverageType='CGLUnderlying',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'AggregateLimit'),
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_GENERAL_AGGREGATE_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT' 
		AND i_CoverageType = 'CGLUnderlying', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_AggregateLimit.CoverageLimitValue,
		'N/A'
	) AS v_UnderlyingAggregateLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - GENERAL AGGREGATE LIMIT'),i_SourceSystemID='DCT','N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_GENERAL_AGGREGATE_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'N/A'
	) AS v_UnderlyingPolicyAggregateLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT'),
	-- i_SourceSystemID='DCT'  AND i_CoverageType='CGLUnderlying',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'ProductsAggregateLimit'),
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_PRODUCTS_COMPLETED_OPERATIONS_AGGREGATE_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT' 
		AND i_CoverageType = 'CGLUnderlying', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_ProductsAggregateLimit.CoverageLimitValue,
		'N/A'
	) AS v_UnderlyingProductAggregateLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_PRODUCTS_COMPLETED_OPERATIONS_AGGREGATE_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingProductCompletedOperationAggregateLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - BODILY INJURY BY ACCIDENT:  EACH ACCIDENT'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_BY_ACCIDENT_EACH_ACCIDENT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingAccidentLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - BODILY INJURY BY DISEASE:   POLICY LIMIT'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_BY_DISEASE_POLICY_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingDiseaseLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - CommonCauseLimit'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_CommonCauseLimit.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingCommonCauseLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - BODILY INJURY - EACH PERSON'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_EACH_PERSON.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingEachPersonBodilyInjuryLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS' ,:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - PROPERTY DAMAGE - EACH ACCIDENT'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_PROPERTY_DAMAGE_EACH_ACCIDENT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingEachPersonPropertyDamageLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - LossOfMeansSupportLimit'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_LossOfMeansSupportLimit.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingLossOfMeansSupportLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - EACH ACCIDENT'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingCombinedSingleLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS' ,:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT_GARAGE_OPERATIONS_AUTO_ONLY.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingAccidentGarageOperationsAutoOnlyLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS' ,:LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Underlying - EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY'),
	-- i_SourceSystemID='DCT','TBD',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT_GARAGE_OPERATIONS_OTHER_THAN_AUTO_ONLY.CoverageLimitValue,
		i_SourceSystemID = 'DCT', 'TBD',
		'N/A'
	) AS v_UnderlyingAccidentGarageOperationsOtherThanAutoLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS' ,
	-- :LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Umbrella EACH OCCURRENCE LIMIT'),
	-- i_SourceSystemID='DCT', i_PolicyPerOccurenceLimit,
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_EACH_OCCURRENCE_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', i_PolicyPerOccurenceLimit,
		'N/A'
	) AS v_UmbrellaPerOccurrenceLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS' ,
	-- :LKP.LKP_COVERAELIMITVALUE(i_PremiumTransactionAKID,'Umbrella AGGREGATE LIMIT'),
	-- i_SourceSystemID='DCT', i_PolicyAggregateLimit,
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_SourceSystemID = 'PMS', LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_AGGREGATE_LIMIT.CoverageLimitValue,
		i_SourceSystemID = 'DCT', i_PolicyAggregateLimit,
		'N/A'
	) AS v_UmbrellaAggregateLimit,
	-- *INF*: IIF(ISNULL(v_UmbrellaPerOccurrenceLimit), 'N/A', v_UmbrellaPerOccurrenceLimit)
	IFF(v_UmbrellaPerOccurrenceLimit IS NULL,
		'N/A',
		v_UmbrellaPerOccurrenceLimit
	) AS o_UmbrellaPerOccurrenceLimit,
	-- *INF*: IIF(ISNULL(v_UmbrellaAggregateLimit), 'N/A', v_UmbrellaAggregateLimit)
	IFF(v_UmbrellaAggregateLimit IS NULL,
		'N/A',
		v_UmbrellaAggregateLimit
	) AS o_UmbrellaAggregateLimit,
	-- *INF*: IIF(ISNULL(v_UmbrellaLimit),'N/A',v_UmbrellaLimit)
	IFF(v_UmbrellaLimit IS NULL,
		'N/A',
		v_UmbrellaLimit
	) AS o_UmbrellaLimit,
	-- *INF*: IIF(ISNULL(v_UmbrellaPersonalInjuryLimit),'N/A',v_UmbrellaPersonalInjuryLimit)
	IFF(v_UmbrellaPersonalInjuryLimit IS NULL,
		'N/A',
		v_UmbrellaPersonalInjuryLimit
	) AS o_UmbrellaPersonalInjuryLimit,
	-- *INF*: IIF(ISNULL(v_UmbrellaUnderInsuredLimit),'N/A',v_UmbrellaUnderInsuredLimit)
	IFF(v_UmbrellaUnderInsuredLimit IS NULL,
		'N/A',
		v_UmbrellaUnderInsuredLimit
	) AS o_UmbrellaUnderInsuredLimit,
	-- *INF*: IIF(ISNULL(v_UmbrellaUninsuredLimit),'N/A',v_UmbrellaUninsuredLimit)
	IFF(v_UmbrellaUninsuredLimit IS NULL,
		'N/A',
		v_UmbrellaUninsuredLimit
	) AS o_UmbrellaUninsuredLimit,
	-- *INF*: IIF(ISNULL(v_UmbrellaRetentionLimit),'N/A',v_UmbrellaRetentionLimit)
	IFF(v_UmbrellaRetentionLimit IS NULL,
		'N/A',
		v_UmbrellaRetentionLimit
	) AS o_UmbrellaRetentionLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingPerOccurrenceLimit),'N/A',v_UnderlyingPerOccurrenceLimit)
	IFF(v_UnderlyingPerOccurrenceLimit IS NULL,
		'N/A',
		v_UnderlyingPerOccurrenceLimit
	) AS o_UnderlyingPerOccurrenceLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingPerOccurrenceClaimLimit),'N/A',v_UnderlyingPerOccurrenceClaimLimit)
	IFF(v_UnderlyingPerOccurrenceClaimLimit IS NULL,
		'N/A',
		v_UnderlyingPerOccurrenceClaimLimit
	) AS o_UnderlyingPerOccurrenceClaimLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingAggregateLimit),'N/A',v_UnderlyingAggregateLimit)
	IFF(v_UnderlyingAggregateLimit IS NULL,
		'N/A',
		v_UnderlyingAggregateLimit
	) AS o_UnderlyingAggregateLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingPolicyAggregateLimit),'N/A',v_UnderlyingPolicyAggregateLimit)
	IFF(v_UnderlyingPolicyAggregateLimit IS NULL,
		'N/A',
		v_UnderlyingPolicyAggregateLimit
	) AS o_UnderlyingPolicyAggregateLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingProductAggregateLimit),'N/A',v_UnderlyingProductAggregateLimit)
	IFF(v_UnderlyingProductAggregateLimit IS NULL,
		'N/A',
		v_UnderlyingProductAggregateLimit
	) AS o_UnderlyingProductAggregateLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingProductCompletedOperationAggregateLimit),'N/A',v_UnderlyingProductCompletedOperationAggregateLimit)
	IFF(v_UnderlyingProductCompletedOperationAggregateLimit IS NULL,
		'N/A',
		v_UnderlyingProductCompletedOperationAggregateLimit
	) AS o_UnderlyingProductCompletedOperationAggregateLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingAccidentLimit),'N/A',v_UnderlyingAccidentLimit)
	IFF(v_UnderlyingAccidentLimit IS NULL,
		'N/A',
		v_UnderlyingAccidentLimit
	) AS o_UnderlyingAccidentLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingDiseaseLimit),'N/A',v_UnderlyingDiseaseLimit)
	IFF(v_UnderlyingDiseaseLimit IS NULL,
		'N/A',
		v_UnderlyingDiseaseLimit
	) AS o_UnderlyingDiseaseLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingCommonCauseLimit),'N/A',v_UnderlyingCommonCauseLimit)
	IFF(v_UnderlyingCommonCauseLimit IS NULL,
		'N/A',
		v_UnderlyingCommonCauseLimit
	) AS o_UnderlyingCommonCauseLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingEachPersonBodilyInjuryLimit),'N/A',v_UnderlyingEachPersonBodilyInjuryLimit)
	IFF(v_UnderlyingEachPersonBodilyInjuryLimit IS NULL,
		'N/A',
		v_UnderlyingEachPersonBodilyInjuryLimit
	) AS o_UnderlyingEachPersonBodilyInjuryLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingEachPersonPropertyDamageLimit),'N/A',v_UnderlyingEachPersonPropertyDamageLimit)
	IFF(v_UnderlyingEachPersonPropertyDamageLimit IS NULL,
		'N/A',
		v_UnderlyingEachPersonPropertyDamageLimit
	) AS o_UnderlyingEachPersonPropertyDamageLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingLossOfMeansSupportLimit),'N/A',v_UnderlyingLossOfMeansSupportLimit)
	IFF(v_UnderlyingLossOfMeansSupportLimit IS NULL,
		'N/A',
		v_UnderlyingLossOfMeansSupportLimit
	) AS o_UnderlyingLossOfMeansSupportLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingCombinedSingleLimit),'N/A',v_UnderlyingCombinedSingleLimit)
	IFF(v_UnderlyingCombinedSingleLimit IS NULL,
		'N/A',
		v_UnderlyingCombinedSingleLimit
	) AS o_UnderlyingCombinedSingleLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingAccidentGarageOperationsAutoOnlyLimit),'N/A',v_UnderlyingAccidentGarageOperationsAutoOnlyLimit)
	IFF(v_UnderlyingAccidentGarageOperationsAutoOnlyLimit IS NULL,
		'N/A',
		v_UnderlyingAccidentGarageOperationsAutoOnlyLimit
	) AS UnderlyingAccidentGarageOperationsAutoOnlyLimit,
	-- *INF*: IIF(ISNULL(v_UnderlyingAccidentGarageOperationsOtherThanAutoLimit),'N/A',v_UnderlyingAccidentGarageOperationsOtherThanAutoLimit)
	IFF(v_UnderlyingAccidentGarageOperationsOtherThanAutoLimit IS NULL,
		'N/A',
		v_UnderlyingAccidentGarageOperationsOtherThanAutoLimit
	) AS UnderlyingAccidentGarageOperationsOtherThanAutoLimit,
	o_UmbrellaCoverageScope AS UmbrellaCoverageScope,
	o_RetroactiveDate AS RetroactiveDate,
	UmbrellaLayer,
	CheckFlag
	FROM EXP_MetaData
	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_AGGREGATE_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_AGGREGATE_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_AGGREGATE_LIMIT.CoverageLimitType = 'Umbrella AGGREGATE LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_GENERAL_AGGREGATE_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_GENERAL_AGGREGATE_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_GENERAL_AGGREGATE_LIMIT.CoverageLimitType = 'Umbrella GENERAL AGGREGATE LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_EACH_OCCURRENCE_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_EACH_OCCURRENCE_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_EACH_OCCURRENCE_LIMIT.CoverageLimitType = 'Umbrella EACH OCCURRENCE LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_PERSONAL_INJURY_AND_ADVERTISING_INJURY_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_PERSONAL_INJURY_AND_ADVERTISING_INJURY_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Umbrella_PERSONAL_INJURY_AND_ADVERTISING_INJURY_LIMIT.CoverageLimitType = 'Umbrella PERSONAL INJURY AND ADVERTISING INJURY LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_UNDERINSURED_MOTORISTS_COVERAGE_EACH_ACCIDENT_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_UNDERINSURED_MOTORISTS_COVERAGE_EACH_ACCIDENT_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_UNDERINSURED_MOTORISTS_COVERAGE_EACH_ACCIDENT_LIMIT.CoverageLimitType = 'UNDERINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_UNINSURED_MOTORISTS_COVERAGE_EACH_ACCIDENT_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_UNINSURED_MOTORISTS_COVERAGE_EACH_ACCIDENT_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_UNINSURED_MOTORISTS_COVERAGE_EACH_ACCIDENT_LIMIT.CoverageLimitType = 'UNINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_RetentionLimit
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_RetentionLimit.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_RetentionLimit.CoverageLimitType = 'RetentionLimit'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_OCCURRENCE_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_OCCURRENCE_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_OCCURRENCE_LIMIT.CoverageLimitType = 'Underlying - EACH OCCURRENCE LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_OccurrenceLimit
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_OccurrenceLimit.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_OccurrenceLimit.CoverageLimitType = 'OccurrenceLimit'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_GENERAL_AGGREGATE_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_GENERAL_AGGREGATE_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_GENERAL_AGGREGATE_LIMIT.CoverageLimitType = 'Underlying - GENERAL AGGREGATE LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_AggregateLimit
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_AggregateLimit.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_AggregateLimit.CoverageLimitType = 'AggregateLimit'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_PRODUCTS_COMPLETED_OPERATIONS_AGGREGATE_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_PRODUCTS_COMPLETED_OPERATIONS_AGGREGATE_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_PRODUCTS_COMPLETED_OPERATIONS_AGGREGATE_LIMIT.CoverageLimitType = 'Underlying - PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_ProductsAggregateLimit
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_ProductsAggregateLimit.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_ProductsAggregateLimit.CoverageLimitType = 'ProductsAggregateLimit'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_BY_ACCIDENT_EACH_ACCIDENT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_BY_ACCIDENT_EACH_ACCIDENT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_BY_ACCIDENT_EACH_ACCIDENT.CoverageLimitType = 'Underlying - BODILY INJURY BY ACCIDENT:  EACH ACCIDENT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_BY_DISEASE_POLICY_LIMIT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_BY_DISEASE_POLICY_LIMIT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_BY_DISEASE_POLICY_LIMIT.CoverageLimitType = 'Underlying - BODILY INJURY BY DISEASE:   POLICY LIMIT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_CommonCauseLimit
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_CommonCauseLimit.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_CommonCauseLimit.CoverageLimitType = 'Underlying - CommonCauseLimit'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_EACH_PERSON
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_EACH_PERSON.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_BODILY_INJURY_EACH_PERSON.CoverageLimitType = 'Underlying - BODILY INJURY - EACH PERSON'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_PROPERTY_DAMAGE_EACH_ACCIDENT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_PROPERTY_DAMAGE_EACH_ACCIDENT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_PROPERTY_DAMAGE_EACH_ACCIDENT.CoverageLimitType = 'Underlying - PROPERTY DAMAGE - EACH ACCIDENT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_LossOfMeansSupportLimit
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_LossOfMeansSupportLimit.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_LossOfMeansSupportLimit.CoverageLimitType = 'Underlying - LossOfMeansSupportLimit'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT.CoverageLimitType = 'Underlying - EACH ACCIDENT'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT_GARAGE_OPERATIONS_AUTO_ONLY
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT_GARAGE_OPERATIONS_AUTO_ONLY.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT_GARAGE_OPERATIONS_AUTO_ONLY.CoverageLimitType = 'Underlying - EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY'

	LEFT JOIN LKP_COVERAELIMITVALUE LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT_GARAGE_OPERATIONS_OTHER_THAN_AUTO_ONLY
	ON LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT_GARAGE_OPERATIONS_OTHER_THAN_AUTO_ONLY.PremiumTransactionAKId = i_PremiumTransactionAKID
	AND LKP_COVERAELIMITVALUE_i_PremiumTransactionAKID_Underlying_EACH_ACCIDENT_GARAGE_OPERATIONS_OTHER_THAN_AUTO_ONLY.CoverageLimitType = 'Underlying - EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY'

),
LKP_CoverageDetailCommercialUmbrellaDim AS (
	SELECT
	CoverageDetailDimId
	FROM (
		SELECT CDCUD.CoverageDetailDimId as CoverageDetailDimId 
		FROM 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialUmbrellaDim CDCUD
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		ON CDCUD.CoverageDetailDimId = CDD.CoverageDetailDimId
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialUmbrella CDCU
		ON CDCU.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
		WHERE CDD.modifeddate > '@{pipeline().parameters.SELECTION_START_TS}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDetailDimId ORDER BY CoverageDetailDimId DESC) = 1
),
EXP_Tgt AS (
	SELECT
	EXP_Business_Rules.CoverageDetailDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	EXP_Business_Rules.EffectiveDate,
	EXP_Business_Rules.ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	EXP_Business_Rules.CoverageGuid,
	EXP_Business_Rules.o_UmbrellaLimit AS UmbrellaLimit,
	EXP_Business_Rules.o_UmbrellaPersonalInjuryLimit AS UmbrellaPersonalInjuryLimit,
	EXP_Business_Rules.o_UmbrellaUnderInsuredLimit AS UmbrellaUnderInsuredLimit,
	EXP_Business_Rules.o_UmbrellaUninsuredLimit AS UmbrellaUninsuredLimit,
	EXP_Business_Rules.o_UmbrellaRetentionLimit AS UmbrellaRetentionLimit,
	EXP_Business_Rules.o_UnderlyingPerOccurrenceLimit AS UnderlyingPerOccurrenceLimit,
	EXP_Business_Rules.o_UnderlyingPerOccurrenceClaimLimit AS UnderlyingPerOccurrenceClaimLimit,
	EXP_Business_Rules.o_UnderlyingAggregateLimit AS UnderlyingAggregateLimit,
	EXP_Business_Rules.o_UnderlyingPolicyAggregateLimit AS UnderlyingPolicyAggregateLimit,
	EXP_Business_Rules.o_UnderlyingProductAggregateLimit AS UnderlyingProductAggregateLimit,
	EXP_Business_Rules.o_UnderlyingProductCompletedOperationAggregateLimit AS UnderlyingProductCompletedOperationAggregateLimit,
	EXP_Business_Rules.o_UnderlyingAccidentLimit AS UnderlyingAccidentLimit,
	EXP_Business_Rules.o_UnderlyingDiseaseLimit AS UnderlyingDiseaseLimit,
	EXP_Business_Rules.o_UnderlyingCommonCauseLimit AS UnderlyingCommonCauseLimit,
	EXP_Business_Rules.o_UnderlyingEachPersonBodilyInjuryLimit AS UnderlyingEachPersonBodilyInjuryLimit,
	EXP_Business_Rules.o_UnderlyingEachPersonPropertyDamageLimit AS UnderlyingEachPersonPropertyDamageLimit,
	EXP_Business_Rules.o_UnderlyingLossOfMeansSupportLimit AS UnderlyingLossOfMeansSupportLimit,
	EXP_Business_Rules.o_UnderlyingCombinedSingleLimit AS UnderlyingCombinedSingleLimit,
	EXP_Business_Rules.UnderlyingAccidentGarageOperationsAutoOnlyLimit,
	EXP_Business_Rules.UnderlyingAccidentGarageOperationsOtherThanAutoLimit,
	EXP_Business_Rules.UmbrellaCoverageScope,
	EXP_Business_Rules.RetroactiveDate,
	LKP_CoverageDetailCommercialUmbrellaDim.CoverageDetailDimId AS LKP_CoverageDetailDimId,
	-- *INF*: IIF(ISNULL(LKP_CoverageDetailDimId),'NEW','UPDATE')
	IFF(LKP_CoverageDetailDimId IS NULL,
		'NEW',
		'UPDATE'
	) AS ChangeFlag,
	EXP_Business_Rules.UmbrellaLayer,
	EXP_Business_Rules.o_UmbrellaPerOccurrenceLimit AS UmbrellaPerOccurrenceLimit,
	EXP_Business_Rules.o_UmbrellaAggregateLimit AS UmbrellaAggregateLimit,
	EXP_Business_Rules.CheckFlag
	FROM EXP_Business_Rules
	LEFT JOIN LKP_CoverageDetailCommercialUmbrellaDim
	ON LKP_CoverageDetailCommercialUmbrellaDim.CoverageDetailDimId = EXP_Business_Rules.CoverageDetailDimId
),
RTR_INSERT_UPDATE AS (
	SELECT
	CoverageDetailDimId,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	CreatedDate,
	ModifiedDate,
	CoverageGuid,
	UmbrellaLimit,
	UmbrellaPersonalInjuryLimit,
	UmbrellaUnderInsuredLimit,
	UmbrellaUninsuredLimit,
	UmbrellaRetentionLimit,
	UnderlyingPerOccurrenceLimit,
	UnderlyingPerOccurrenceClaimLimit,
	UnderlyingAggregateLimit,
	UnderlyingPolicyAggregateLimit,
	UnderlyingProductAggregateLimit,
	UnderlyingProductCompletedOperationAggregateLimit,
	UnderlyingAccidentLimit,
	UnderlyingDiseaseLimit,
	UnderlyingCommonCauseLimit,
	UnderlyingEachPersonBodilyInjuryLimit,
	UnderlyingEachPersonPropertyDamageLimit,
	UnderlyingLossOfMeansSupportLimit,
	UnderlyingCombinedSingleLimit,
	UnderlyingAccidentGarageOperationsAutoOnlyLimit,
	UnderlyingAccidentGarageOperationsOtherThanAutoLimit,
	UmbrellaCoverageScope,
	RetroactiveDate,
	ChangeFlag,
	UmbrellaLayer,
	UmbrellaPerOccurrenceLimit,
	UmbrellaAggregateLimit,
	CheckFlag
	FROM EXP_Tgt
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='NEW'  AND CheckFlag='PASS'),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='UPDATE'  AND CheckFlag='PASS'),
CoverageDetailCommercialUmbrellaDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialUmbrellaDim
	(CoverageDetailDimId, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, CoverageGuid, UmbrellaLimit, UmbrellaPersonalInjuryLimit, UmbrellaUnderInsuredLimit, UmbrellaUninsuredLimit, UmbrellaRetentionLimit, UmbrellaCoverageScope, RetroactiveDate, UmbrellaLayer, UmbrellaPerOccurrenceLimit, UmbrellaAggregateLimit)
	SELECT 
	COVERAGEDETAILDIMID, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	UMBRELLALIMIT, 
	UMBRELLAPERSONALINJURYLIMIT, 
	UMBRELLAUNDERINSUREDLIMIT, 
	UMBRELLAUNINSUREDLIMIT, 
	UMBRELLARETENTIONLIMIT, 
	UMBRELLACOVERAGESCOPE, 
	RETROACTIVEDATE, 
	UMBRELLALAYER, 
	UMBRELLAPEROCCURRENCELIMIT, 
	UMBRELLAAGGREGATELIMIT
	FROM RTR_INSERT_UPDATE_INSERT
),
UPD_Exists AS (
	SELECT
	CoverageDetailDimId AS CoverageDetailDimId3, 
	AuditID AS AuditID3, 
	EffectiveDate AS EffectiveDate3, 
	ExpirationDate AS ExpirationDate3, 
	ModifiedDate AS ModifiedDate3, 
	CoverageGuid AS CoverageGuid3, 
	UmbrellaLimit AS UmbrellaLimit3, 
	UmbrellaPersonalInjuryLimit AS UmbrellaPersonalInjuryLimit3, 
	UmbrellaUnderInsuredLimit AS UmbrellaUnderInsuredLimit3, 
	UmbrellaUninsuredLimit AS UmbrellaUninsuredLimit3, 
	UmbrellaRetentionLimit AS UmbrellaRetentionLimit3, 
	UnderlyingPerOccurrenceLimit AS UnderlyingPerOccurrenceLimit3, 
	UnderlyingPerOccurrenceClaimLimit AS UnderlyingPerOccurrenceClaimLimit3, 
	UnderlyingAggregateLimit AS UnderlyingAggregateLimit3, 
	UnderlyingPolicyAggregateLimit AS UnderlyingPolicyAggregateLimit3, 
	UnderlyingProductAggregateLimit AS UnderlyingProductAggregateLimit3, 
	UnderlyingProductCompletedOperationAggregateLimit AS UnderlyingProductCompletedOperationAggregateLimit3, 
	UnderlyingAccidentLimit AS UnderlyingAccidentLimit3, 
	UnderlyingDiseaseLimit AS UnderlyingDiseaseLimit3, 
	UnderlyingCommonCauseLimit AS UnderlyingCommonCauseLimit3, 
	UnderlyingEachPersonBodilyInjuryLimit AS UnderlyingEachPersonBodilyInjuryLimit3, 
	UnderlyingEachPersonPropertyDamageLimit AS UnderlyingEachPersonPropertyDamageLimit3, 
	UnderlyingLossOfMeansSupportLimit AS UnderlyingLossOfMeansSupportLimit3, 
	UnderlyingCombinedSingleLimit AS UnderlyingCombinedSingleLimit3, 
	UnderlyingAccidentGarageOperationsAutoOnlyLimit AS UnderlyingAccidentGarageOperationsAutoOnlyLimit3, 
	UnderlyingAccidentGarageOperationsOtherThanAutoLimit AS UnderlyingAccidentGarageOperationsOtherThanAutoLimit3, 
	UmbrellaCoverageScope AS UmbrellaCoverageScope3, 
	RetroactiveDate AS RetroactiveDate3, 
	UmbrellaLayer, 
	UmbrellaPerOccurrenceLimit AS UmbrellaPerOccurrenceLimit3, 
	UmbrellaAggregateLimit AS UmbrellaAggregateLimit3
	FROM RTR_INSERT_UPDATE_UPDATE
),
CoverageDetailCommercialUmbrellaDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialUmbrellaDim AS T
	USING UPD_Exists AS S
	ON T.CoverageDetailDimId = S.CoverageDetailDimId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditID3, T.EffectiveDate = S.EffectiveDate3, T.ExpirationDate = S.ExpirationDate3, T.ModifiedDate = S.ModifiedDate3, T.CoverageGuid = S.CoverageGuid3, T.UmbrellaLimit = S.UmbrellaLimit3, T.UmbrellaPersonalInjuryLimit = S.UmbrellaPersonalInjuryLimit3, T.UmbrellaUnderInsuredLimit = S.UmbrellaUnderInsuredLimit3, T.UmbrellaUninsuredLimit = S.UmbrellaUninsuredLimit3, T.UmbrellaRetentionLimit = S.UmbrellaRetentionLimit3, T.UmbrellaCoverageScope = S.UmbrellaCoverageScope3, T.RetroactiveDate = S.RetroactiveDate3, T.UmbrellaLayer = S.UmbrellaLayer, T.UmbrellaPerOccurrenceLimit = S.UmbrellaPerOccurrenceLimit3, T.UmbrellaAggregateLimit = S.UmbrellaAggregateLimit3
),