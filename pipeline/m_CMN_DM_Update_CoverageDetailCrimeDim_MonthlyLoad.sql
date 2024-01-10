WITH
SQ_CoverageDetailCommercialAuto AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0))
	select CDD.CoverageDetailDimId AS CoverageDetailDimId,
	Crime.IndustryGroup
	from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCrime Crime 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	ON  Crime.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCrimeDim Dim
	on CDD.CoverageDetailDimId=Dim.CoverageDetailDimId
	@{pipeline().parameters.WHERE_CLAUSE}
),
UPD_ADDEDCOLUMNS AS (
	SELECT
	CoverageDetailDimId, 
	IndustryGroup
	FROM SQ_CoverageDetailCommercialAuto
),
CoverageDetailCrimeDim AS (
	MERGE INTO CoverageDetailCrimeDim AS T
	USING UPD_ADDEDCOLUMNS AS S
	ON T.CoverageDetailDimId = S.CoverageDetailDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.IndustryGroup = S.IndustryGroup
),