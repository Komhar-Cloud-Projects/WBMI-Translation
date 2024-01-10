WITH
SQ_PremiumTransaction AS (
	Select DISTINCT PT.PremiumTransactionID,
	PT.PremiumTransactionAKID,
	WTLLB.CensusBlockGroup,
	WTLLB.Latitude,
	WTLLB.Longitude,
	WTLLB.RatingTerritoryCode
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT on WPT.PremiumTransactionAKID=PT.PremiumTransactionAKID
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTTransactionInsuranceLineLocationBridge WTLLB on WTLLB.CoverageId=WPT.PremiumTransactionStageId
	where  PT.SourceSystemId='DCT'
	and PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
LKP_PremiumTransactionRatingRisk AS (
	SELECT
	in_PremiumTransactionID,
	PremiumTransactionID
	FROM (
		SELECT PTRR.PremiumTransactionID as PremiumTransactionID FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionRatingRisk PTRR INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT on WPT.PremiumTransactionAKID=PTRR.PremiumTransactionAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY in_PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_PremiumTransactionRatingRisk.PremiumTransactionID AS lkp_PremiumTransactionID,
	SQ_PremiumTransaction.PremiumTransactionID AS i_PremiumTransactionID,
	SQ_PremiumTransaction.PremiumTransactionAKID AS i_PremiumTransactionAKID,
	SQ_PremiumTransaction.CensusBlockGroup AS i_CensusBlockGroup,
	SQ_PremiumTransaction.Latitude AS i_Latitude,
	SQ_PremiumTransaction.Longitude AS i_Longitude,
	SQ_PremiumTransaction.RatingTerritoryCode AS i_RatingTerritoryCode,
	-- *INF*: IIF( ISNULL(lkp_PremiumTransactionID),1,0)
	-- --1 - Insert; 0-Ignore
	IFF(lkp_PremiumTransactionID IS NULL,
		1,
		0
	) AS o_Changeflag,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	i_PremiumTransactionAKID AS o_PremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	'DCT' AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CensusBlockGroup),'N/A',
	-- LENGTH(LTRIM(RTRIM(i_CensusBlockGroup)))=10,SUBSTR(i_CensusBlockGroup,1,3),
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_CensusBlockGroup IS NULL, 'N/A',
		LENGTH(LTRIM(RTRIM(i_CensusBlockGroup
				)
			)
		) = 10, SUBSTR(i_CensusBlockGroup, 1, 3
		),
		'N/A'
	) AS o_CensusBlockGroupCountyCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CensusBlockGroup),'N/A',
	-- LENGTH(LTRIM(RTRIM(i_CensusBlockGroup)))=10,SUBSTR(i_CensusBlockGroup,4,6),
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_CensusBlockGroup IS NULL, 'N/A',
		LENGTH(LTRIM(RTRIM(i_CensusBlockGroup
				)
			)
		) = 10, SUBSTR(i_CensusBlockGroup, 4, 6
		),
		'N/A'
	) AS o_CensusBlockGroupTractCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CensusBlockGroup),'N/A',
	-- LENGTH(LTRIM(RTRIM(i_CensusBlockGroup)))=10,SUBSTR(i_CensusBlockGroup,10,1),
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_CensusBlockGroup IS NULL, 'N/A',
		LENGTH(LTRIM(RTRIM(i_CensusBlockGroup
				)
			)
		) = 10, SUBSTR(i_CensusBlockGroup, 10, 1
		),
		'N/A'
	) AS o_CensusBlockGroupBlockGroupCode,
	-- *INF*: IIF(ISNULL(i_Latitude),000.000000,i_Latitude)
	IFF(i_Latitude IS NULL,
		000.000000,
		i_Latitude
	) AS o_Latitude,
	-- *INF*: IIF(ISNULL(i_Longitude),000.000000,i_Longitude)
	IFF(i_Longitude IS NULL,
		000.000000,
		i_Longitude
	) AS o_Longitude,
	-- *INF*: IIF(ISNULL(i_RatingTerritoryCode) OR IS_SPACES(i_RatingTerritoryCode) OR LTRIM(RTRIM(i_RatingTerritoryCode))='','N/A',i_RatingTerritoryCode)
	IFF(i_RatingTerritoryCode IS NULL 
		OR LENGTH(i_RatingTerritoryCode)>0 AND TRIM(i_RatingTerritoryCode)='' 
		OR LTRIM(RTRIM(i_RatingTerritoryCode
			)
		) = '',
		'N/A',
		i_RatingTerritoryCode
	) AS o_RatingTerritoryCode
	FROM SQ_PremiumTransaction
	LEFT JOIN LKP_PremiumTransactionRatingRisk
	ON LKP_PremiumTransactionRatingRisk.PremiumTransactionID = SQ_PremiumTransaction.PremiumTransactionID
),
FIL_PassInsertOnlyRows AS (
	SELECT
	o_Changeflag AS Changeflag, 
	o_PremiumTransactionID AS PremiumTransactionID, 
	o_PremiumTransactionAKID AS PremiumTransactionAKID, 
	o_AuditID AS AuditID, 
	o_SourceSystemID AS SourceSystemID, 
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_CensusBlockGroupCountyCode AS CensusBlockGroupCountyCode, 
	o_CensusBlockGroupTractCode AS CensusBlockGroupTractCode, 
	o_CensusBlockGroupBlockGroupCode AS CensusBlockGroupBlockGroupCode, 
	o_Latitude AS Latitude, 
	o_Longitude AS Longitude, 
	o_RatingTerritoryCode AS RatingTerritoryCode
	FROM EXP_DetectChanges
	WHERE IIF(Changeflag=1,TRUE,FALSE)
),
EXP_PassthroughTarget AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	AuditID,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	CensusBlockGroupCountyCode,
	CensusBlockGroupTractCode,
	CensusBlockGroupBlockGroupCode,
	Latitude,
	Longitude,
	RatingTerritoryCode
	FROM FIL_PassInsertOnlyRows
),
PremiumTransactionRatingRisk AS (
	INSERT INTO PremiumTransactionRatingRisk
	(PremiumTransactionID, PremiumTransactionAKID, AuditID, SourceSystemID, CreatedDate, ModifiedDate, CensusBlockGroupCountyCode, CensusBlockGroupTractCode, CensusBlockGroupBlockGroupCode, Latitude, Longitude, RatingTerritoryCode)
	SELECT 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	CENSUSBLOCKGROUPCOUNTYCODE, 
	CENSUSBLOCKGROUPTRACTCODE, 
	CENSUSBLOCKGROUPBLOCKGROUPCODE, 
	LATITUDE, 
	LONGITUDE, 
	RATINGTERRITORYCODE
	FROM EXP_PassthroughTarget
),
SQ_PremiumTransactionRatingRisk_Offset AS (
	SELECT 
	WPTOL.PremiumTransactionID,
	PTRRPrevious.PremiumTransactionRatingRiskId,
	PTRRPrevious.CensusBlockGroupCountyCode,
	PTRRPrevious.CensusBlockGroupTractCode,
	PTRRPrevious.CensusBlockGroupBlockGroupCode,
	PTRRPrevious.Latitude,
	PTRRPrevious.Longitude,
	PTRRPrevious.RatingTerritoryCode
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransactionOffsetLineage WPTOL
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionRatingRisk PTRRPrevious
	on ( PTRRPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionRatingRisk PTRRToUpdate
	on ( PTRRToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.premiumtransaction pt WITH (NOLOCK) on
	WPTOL.premiumtransactionID=pt.premiumtransactionID and PT.OffsetOnsetCode='Offset'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	PTRRPrevious.CensusBlockGroupCountyCode <> PTRRToUpdate.CensusBlockGroupCountyCode 
	OR PTRRPrevious.CensusBlockGroupTractCode <> PTRRToUpdate.CensusBlockGroupTractCode 
	OR PTRRPrevious.CensusBlockGroupBlockGroupCode <> PTRRToUpdate.CensusBlockGroupBlockGroupCode 
	OR PTRRPrevious.Latitude <> PTRRToUpdate.Latitude 
	OR PTRRPrevious.Longitude <> PTRRToUpdate.Longitude 
	OR PTRRPrevious.RatingTerritoryCode <> PTRRToUpdate.RatingTerritoryCode
	)
),
Exp_PremiumTransactionRatingRisk_Offset AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionRatingRiskId,
	SYSDATE AS o_ModifiedDate,
	CensusBlockGroupCountyCode,
	CensusBlockGroupTractCode,
	CensusBlockGroupBlockGroupCode,
	Latitude,
	Longitude,
	RatingTerritoryCode
	FROM SQ_PremiumTransactionRatingRisk_Offset
),
UPD_PremiumTransactionRatingRisk_Offset AS (
	SELECT
	PremiumTransactionRatingRiskId, 
	o_ModifiedDate AS ModifiedDate, 
	CensusBlockGroupCountyCode, 
	CensusBlockGroupTractCode, 
	CensusBlockGroupBlockGroupCode, 
	Latitude, 
	Longitude, 
	RatingTerritoryCode
	FROM Exp_PremiumTransactionRatingRisk_Offset
),
PremiumTransactionRatingRisk_Upd_Offset AS (
	MERGE INTO PremiumTransactionRatingRisk AS T
	USING UPD_PremiumTransactionRatingRisk_Offset AS S
	ON T.PremiumTransactionRatingRiskId = S.PremiumTransactionRatingRiskId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CensusBlockGroupCountyCode = S.CensusBlockGroupCountyCode, T.CensusBlockGroupTractCode = S.CensusBlockGroupTractCode, T.CensusBlockGroupBlockGroupCode = S.CensusBlockGroupBlockGroupCode, T.Latitude = S.Latitude, T.Longitude = S.Longitude, T.RatingTerritoryCode = S.RatingTerritoryCode
),
SQ_PremiumTransactionRatingRisk_Deprecated AS (
	SELECT 
	WPTOL.PremiumTransactionID,
	PTRRPrevious.PremiumTransactionRatingRiskId,
	PTRRPrevious.CensusBlockGroupCountyCode,
	PTRRPrevious.CensusBlockGroupTractCode,
	PTRRPrevious.CensusBlockGroupBlockGroupCode,
	PTRRPrevious.Latitude,
	PTRRPrevious.Longitude,
	PTRRPrevious.RatingTerritoryCode
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransactionOffsetLineage WPTOL
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionRatingRisk PTRRPrevious
	on ( PTRRPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionRatingRisk PTRRToUpdate
	on ( PTRRToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.premiumtransaction pt WITH (NOLOCK) on
	WPTOL.premiumtransactionID=pt.premiumtransactionID and PT.OffsetOnsetCode='Deprecated'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	PTRRPrevious.CensusBlockGroupCountyCode <> PTRRToUpdate.CensusBlockGroupCountyCode 
	OR PTRRPrevious.CensusBlockGroupTractCode <> PTRRToUpdate.CensusBlockGroupTractCode 
	OR PTRRPrevious.CensusBlockGroupBlockGroupCode <> PTRRToUpdate.CensusBlockGroupBlockGroupCode 
	OR PTRRPrevious.Latitude <> PTRRToUpdate.Latitude 
	OR PTRRPrevious.Longitude <> PTRRToUpdate.Longitude 
	OR PTRRPrevious.RatingTerritoryCode <> PTRRToUpdate.RatingTerritoryCode
	)
),
Exp_PremiumTransactionRatingRisk_Deprecated AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionRatingRiskId,
	SYSDATE AS o_ModifiedDate,
	CensusBlockGroupCountyCode,
	CensusBlockGroupTractCode,
	CensusBlockGroupBlockGroupCode,
	Latitude,
	Longitude,
	RatingTerritoryCode
	FROM SQ_PremiumTransactionRatingRisk_Deprecated
),
UPD_PremiumTransactionRatingRisk_Deprecated AS (
	SELECT
	PremiumTransactionRatingRiskId, 
	o_ModifiedDate AS ModifiedDate, 
	CensusBlockGroupCountyCode, 
	CensusBlockGroupTractCode, 
	CensusBlockGroupBlockGroupCode, 
	Latitude, 
	Longitude, 
	RatingTerritoryCode
	FROM Exp_PremiumTransactionRatingRisk_Deprecated
),
PremiumTransactionRatingRisk_Upd_Deprecated AS (
	MERGE INTO PremiumTransactionRatingRisk AS T
	USING UPD_PremiumTransactionRatingRisk_Deprecated AS S
	ON T.PremiumTransactionRatingRiskId = S.PremiumTransactionRatingRiskId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CensusBlockGroupCountyCode = S.CensusBlockGroupCountyCode, T.CensusBlockGroupTractCode = S.CensusBlockGroupTractCode, T.CensusBlockGroupBlockGroupCode = S.CensusBlockGroupBlockGroupCode, T.Latitude = S.Latitude, T.Longitude = S.Longitude, T.RatingTerritoryCode = S.RatingTerritoryCode
),