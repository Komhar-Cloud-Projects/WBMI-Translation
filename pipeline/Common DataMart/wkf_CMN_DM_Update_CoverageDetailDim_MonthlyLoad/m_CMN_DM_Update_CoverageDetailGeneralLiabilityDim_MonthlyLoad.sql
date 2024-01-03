WITH
SQ_CoverageDetailGeneralLiability AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	select CDD.CoverageDetailDimId AS CoverageDetailDimId,
	GL.ISOGeneralLiabilityClassSummary,
	GL.ISOGeneralLiabilityClassGroupCode,
	case when GL.SourceSystemID='PMS' then SC.SublineCode
	when GL.SourceSystemID='DCT' then RC.SublineCode else 'N/A' end as SublineCode
	from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability GL
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	ON  	GL.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailGeneralLiabilityDim Dim
	on CDD.CoverageDetailDimId=Dim.CoverageDetailDimId
	inner join PremiumTransaction PT
	on CDD.EDWPremiumTransactionPKID=PT.PremiumTransactionID
	left JOIN dbo.StatisticalCoverage SC
	ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID 
	AND PT.SourceSystemID = 'PMS' 
	AND SC.SourceSystemID = 'PMS' 
	left JOIN RatingCoverage RC 
	ON PT.RatingCoverageAKId = RC.RatingCoverageAKId 
	AND PT.SourceSystemID = 'DCT' 
	AND RC.SourceSystemID = 'DCT' 
	and pt.EffectiveDate=rc.EffectiveDate
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=0 
	@{pipeline().parameters.WHERE_CLAUSE}
),
LKP_SupISOClassGroup AS (
	SELECT
	ISOGeneralLiabilityClassGroupDescription,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode,
	SublineCode
	FROM (
		SELECT 
			ISOGeneralLiabilityClassGroupDescription,
			ISOGeneralLiabilityClassSummary,
			ISOGeneralLiabilityClassGroupCode,
			SublineCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupISOClassGroup
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ISOGeneralLiabilityClassSummary,ISOGeneralLiabilityClassGroupCode,SublineCode ORDER BY ISOGeneralLiabilityClassGroupDescription) = 1
),
EXP_GetMetaData AS (
	SELECT
	SQ_CoverageDetailGeneralLiability.CoverageDetailDimId,
	SQ_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassSummary AS i_ISOGeneralLiabilityClassSummary,
	SQ_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassGroupCode AS i_ISOGeneralLiabilityClassGroupCode,
	LKP_SupISOClassGroup.ISOGeneralLiabilityClassGroupDescription AS i_ISOGeneralLiabilityClassGroupDescription,
	-- *INF*: IIF(isnull(i_ISOGeneralLiabilityClassSummary),'N/A',i_ISOGeneralLiabilityClassSummary)
	IFF(i_ISOGeneralLiabilityClassSummary IS NULL, 'N/A', i_ISOGeneralLiabilityClassSummary) AS o_ISOGeneralLiabilityClassSummary,
	-- *INF*: IIF(isnull(i_ISOGeneralLiabilityClassGroupCode),'N/A',i_ISOGeneralLiabilityClassGroupCode)
	IFF(i_ISOGeneralLiabilityClassGroupCode IS NULL, 'N/A', i_ISOGeneralLiabilityClassGroupCode) AS o_ISOGeneralLiabilityClassGroupCode,
	-- *INF*: IIF(isnull(i_ISOGeneralLiabilityClassGroupDescription),'N/A',i_ISOGeneralLiabilityClassGroupDescription)
	IFF(i_ISOGeneralLiabilityClassGroupDescription IS NULL, 'N/A', i_ISOGeneralLiabilityClassGroupDescription) AS o_ISOGeneralLiabilityClassGroupDescription
	FROM SQ_CoverageDetailGeneralLiability
	LEFT JOIN LKP_SupISOClassGroup
	ON LKP_SupISOClassGroup.ISOGeneralLiabilityClassSummary = SQ_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassSummary AND LKP_SupISOClassGroup.ISOGeneralLiabilityClassGroupCode = SQ_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassGroupCode AND LKP_SupISOClassGroup.SublineCode = SQ_CoverageDetailGeneralLiability.SublineCode
),
UPD_CoverageDetailGeneralLiabilityDim AS (
	SELECT
	CoverageDetailDimId, 
	o_ISOGeneralLiabilityClassSummary AS ISOGeneralLiabilityClassSummary, 
	o_ISOGeneralLiabilityClassGroupCode AS ISOGeneralLiabilityClassGroupCode, 
	o_ISOGeneralLiabilityClassGroupDescription AS ISOGeneralLiabilityClassGroupDescription
	FROM EXP_GetMetaData
),
CoverageDetailGeneralLiabilityDim AS (
	MERGE INTO CoverageDetailGeneralLiabilityDim AS T
	USING UPD_CoverageDetailGeneralLiabilityDim AS S
	ON T.CoverageDetailDimId = S.CoverageDetailDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ISOGeneralLiabilityClassSummary = S.ISOGeneralLiabilityClassSummary, T.ISOGeneralLiabilityClassGroupCode = S.ISOGeneralLiabilityClassGroupCode, T.ISOGeneralLiabilityClassGroupDescription = S.ISOGeneralLiabilityClassGroupDescription
),