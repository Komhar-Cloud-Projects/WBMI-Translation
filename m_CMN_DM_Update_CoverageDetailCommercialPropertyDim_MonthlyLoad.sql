WITH
LKP_SupISOSpecialCauseOfLossCategory AS (
	SELECT
	ISOSpecialCauseOfLossCategoryDescription,
	ISOSpecialCauseOfLossCategoryCode
	FROM (
		SELECT 
			ISOSpecialCauseOfLossCategoryDescription,
			ISOSpecialCauseOfLossCategoryCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupISOSpecialCauseOfLossCategory
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ISOSpecialCauseOfLossCategoryCode ORDER BY ISOSpecialCauseOfLossCategoryDescription) = 1
),
SQ_CoverageDetailCommercialProperty AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	select CDD.CoverageDetailDimId AS CoverageDetailDimId
	,CDCP.ISOCommercialPropertyCauseofLossGroup AS ISOCommercialPropertyCauseofLossGroup
	,CDCP.ISOCommercialPropertyRatingGroupCode AS ISOCommercialPropertyRatingGroupCode
	,CDCP.ISOSpecialCauseOfLossCategoryCode AS ISOSpecialCauseOfLossCategoryCode
	,CDCP.RateType AS RateType
	,CDCP.CommercialPropertySpecialClass AS CommercialPropertySpecialClass
	from  CoverageDetailCommercialProperty CDCP
	INNER JOIN  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	ON CDCP.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialPropertyDim CPD
	on CDD.CoverageDetailDimId=CPD.CoverageDetailDimId
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=0 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_METADATE AS (
	SELECT
	CoverageDetailDimId,
	ISOCommercialPropertyCauseofLossGroup,
	ISOCommercialPropertyRatingGroupCode,
	ISOSpecialCauseOfLossCategoryCode,
	RateType,
	CommercialPropertySpecialClass
	FROM SQ_CoverageDetailCommercialProperty
),
LKP_SupISOCommercialPropertyRatingGroup AS (
	SELECT
	ISOCommercialPropertyRatingGroupDescription,
	ISOCommercialPropertyRatingGroupCode
	FROM (
		SELECT 
			ISOCommercialPropertyRatingGroupDescription,
			ISOCommercialPropertyRatingGroupCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupISOCommercialPropertyRatingGroup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ISOCommercialPropertyRatingGroupCode ORDER BY ISOCommercialPropertyRatingGroupDescription) = 1
),
EXP_GetData AS (
	SELECT
	EXP_METADATE.CoverageDetailDimId AS i_CoverageDetailDimId,
	EXP_METADATE.ISOCommercialPropertyCauseofLossGroup AS i_ISOCommercialPropertyCauseofLossGroup,
	EXP_METADATE.ISOCommercialPropertyRatingGroupCode AS i_ISOCommercialPropertyRatingGroupCode,
	LKP_SupISOCommercialPropertyRatingGroup.ISOCommercialPropertyRatingGroupDescription AS i_ISOCommercialPropertyRatingGroupDescription,
	EXP_METADATE.ISOSpecialCauseOfLossCategoryCode AS i_ISOSpecialCauseOfLossCategoryCode,
	EXP_METADATE.RateType AS i_RateType,
	EXP_METADATE.CommercialPropertySpecialClass AS i_CommercialPropertySpecialClass,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	-- *INF*: iif(isnull(i_ISOCommercialPropertyCauseofLossGroup),'N/A',i_ISOCommercialPropertyCauseofLossGroup)
	IFF(i_ISOCommercialPropertyCauseofLossGroup IS NULL, 'N/A', i_ISOCommercialPropertyCauseofLossGroup) AS o_ISOCommercialPropertyCauseofLossGroup,
	-- *INF*: iif(isnull(i_ISOCommercialPropertyRatingGroupCode),'N/A',i_ISOCommercialPropertyRatingGroupCode)
	IFF(i_ISOCommercialPropertyRatingGroupCode IS NULL, 'N/A', i_ISOCommercialPropertyRatingGroupCode) AS o_ISOCommercialPropertyRatingGroupCode,
	-- *INF*: iif(isnull(i_ISOCommercialPropertyRatingGroupDescription),'N/A',i_ISOCommercialPropertyRatingGroupDescription)
	IFF(i_ISOCommercialPropertyRatingGroupDescription IS NULL, 'N/A', i_ISOCommercialPropertyRatingGroupDescription) AS o_ISOCommercialPropertyRatingGroupDescription,
	-- *INF*: iif(isnull(i_ISOSpecialCauseOfLossCategoryCode),'N/A',i_ISOSpecialCauseOfLossCategoryCode)
	IFF(i_ISOSpecialCauseOfLossCategoryCode IS NULL, 'N/A', i_ISOSpecialCauseOfLossCategoryCode) AS o_ISOSpecialCauseOfLossCategoryCode,
	-- *INF*: DECODE(TRUE,
	-- i_ISOCommercialPropertyCauseofLossGroup='SCL' and not isnull(:LKP.LKP_SupISOSpecialCauseOfLossCategory(i_ISOSpecialCauseOfLossCategoryCode)),:LKP.LKP_SupISOSpecialCauseOfLossCategory(i_ISOSpecialCauseOfLossCategoryCode),'N/A')
	DECODE(TRUE,
	i_ISOCommercialPropertyCauseofLossGroup = 'SCL' AND NOT LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY_i_ISOSpecialCauseOfLossCategoryCode.ISOSpecialCauseOfLossCategoryDescription IS NULL, LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY_i_ISOSpecialCauseOfLossCategoryCode.ISOSpecialCauseOfLossCategoryDescription,
	'N/A') AS o_ISOSpecialCauseOfLossCategoryDescription,
	-- *INF*: iif(isnull(i_RateType),'N/A',i_RateType)
	IFF(i_RateType IS NULL, 'N/A', i_RateType) AS o_RateType,
	-- *INF*: iif(isnull(i_CommercialPropertySpecialClass),'N/A',i_CommercialPropertySpecialClass)
	IFF(i_CommercialPropertySpecialClass IS NULL, 'N/A', i_CommercialPropertySpecialClass) AS o_CommercialPropertySpecialClass
	FROM EXP_METADATE
	LEFT JOIN LKP_SupISOCommercialPropertyRatingGroup
	ON LKP_SupISOCommercialPropertyRatingGroup.ISOCommercialPropertyRatingGroupCode = EXP_METADATE.ISOCommercialPropertyRatingGroupCode
	LEFT JOIN LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY_i_ISOSpecialCauseOfLossCategoryCode
	ON LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY_i_ISOSpecialCauseOfLossCategoryCode.ISOSpecialCauseOfLossCategoryCode = i_ISOSpecialCauseOfLossCategoryCode

),
UPD_ADDEDCOLUMNS AS (
	SELECT
	o_CoverageDetailDimId AS CoverageDetailDimId, 
	o_ISOCommercialPropertyCauseofLossGroup AS ISOCommercialPropertyCauseofLossGroup, 
	o_ISOCommercialPropertyRatingGroupCode AS ISOCommercialPropertyRatingGroupCode, 
	o_ISOCommercialPropertyRatingGroupDescription AS ISOCommercialPropertyRatingGroupDescription, 
	o_ISOSpecialCauseOfLossCategoryCode AS ISOSpecialCauseOfLossCategoryCode, 
	o_ISOSpecialCauseOfLossCategoryDescription AS ISOSpecialCauseOfLossCategoryDescription, 
	o_RateType AS RateType, 
	o_CommercialPropertySpecialClass AS CommercialPropertySpecialClass
	FROM EXP_GetData
),
CoverageDetailCommercialPropertyDim AS (
	MERGE INTO CoverageDetailCommercialPropertyDim AS T
	USING UPD_ADDEDCOLUMNS AS S
	ON 
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageDetailDimId = S.CoverageDetailDimId, T.ISOCommercialPropertyCauseofLossGroup = S.ISOCommercialPropertyCauseofLossGroup, T.ISOCommercialPropertyRatingGroupCode = S.ISOCommercialPropertyRatingGroupCode, T.ISOCommercialPropertyRatingGroupDescription = S.ISOCommercialPropertyRatingGroupDescription, T.ISOSpecialCauseOfLossCategoryCode = S.ISOSpecialCauseOfLossCategoryCode, T.ISOSpecialCauseOfLossCategoryDescription = S.ISOSpecialCauseOfLossCategoryDescription, T.RateType = S.RateType, T.CommercialPropertySpecialClass = S.CommercialPropertySpecialClass
),