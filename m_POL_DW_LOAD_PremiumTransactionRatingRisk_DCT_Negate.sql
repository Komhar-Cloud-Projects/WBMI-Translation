WITH
SQ_PremiumTransactionRatingRisk AS (
	SELECT 
		PT.PremiumTransactionID as NewNegatePremiumTransactionID
		,PT.PremiumTransactionAKID as NewNegatePremiumTransactionAKID
		,PTRR.PremiumTransactionID
		,PTRR.CensusBlockGroupCountyCode
		,PTRR.CensusBlockGroupTractCode
		,PTRR.CensusBlockGroupBlockGroupCode
		,PTRR.Latitude
		,PTRR.Longitude
		,PTRR.RatingTerritoryCode
	FROM dbo.PremiumTransactionRatingRisk PTRR
	INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPTDRN ON PTRR.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	INNER JOIN dbo.PremiumTransaction PT ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID
		AND PT.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_CoverageDetailCommercialProperty AS (
	SELECT
	NewNegatePremiumTransactionID,
	NewNegatePremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	PremiumTransactionID,
	CensusBlockGroupCountyCode,
	CensusBlockGroupTractCode,
	CensusBlockGroupBlockGroupCode,
	Latitude,
	Longitude,
	RatingTerritoryCode
	FROM SQ_PremiumTransactionRatingRisk
),
LKP_PremiumTransactionRatingRisk AS (
	SELECT
	PremiumTransactionID,
	NewNegatePremiumTransactionID
	FROM (
		SELECT 
			PremiumTransactionID,
			NewNegatePremiumTransactionID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionRatingRisk
		WHERE PremiumTransactionID IN ( SELECT pt.PremiumTransactionID FROM PremiumTransaction PT INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPT ON PT.PremiumTransactionAKID = WPT.NewNegatePremiumTransactionAKID)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_PremiumTransactionRatingRisk.PremiumTransactionID AS lkp_PremiumTransactionId,
	EXP_CoverageDetailCommercialProperty.NewNegatePremiumTransactionID,
	EXP_CoverageDetailCommercialProperty.NewNegatePremiumTransactionAKID,
	EXP_CoverageDetailCommercialProperty.o_AuditID AS AuditID,
	EXP_CoverageDetailCommercialProperty.o_SourceSystemID AS SourceSystemID,
	EXP_CoverageDetailCommercialProperty.o_CreatedDate AS CreatedDate,
	EXP_CoverageDetailCommercialProperty.o_ModifiedDate AS ModifiedDate,
	EXP_CoverageDetailCommercialProperty.CensusBlockGroupCountyCode,
	EXP_CoverageDetailCommercialProperty.CensusBlockGroupTractCode,
	EXP_CoverageDetailCommercialProperty.CensusBlockGroupBlockGroupCode,
	EXP_CoverageDetailCommercialProperty.Latitude,
	EXP_CoverageDetailCommercialProperty.Longitude,
	EXP_CoverageDetailCommercialProperty.RatingTerritoryCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionId),1,
	-- 0
	-- )
	-- -- 1 Insert  0 Ignore
	DECODE(TRUE,
	lkp_PremiumTransactionId IS NULL, 1,
	0) AS o_ChangeFlag
	FROM EXP_CoverageDetailCommercialProperty
	LEFT JOIN LKP_PremiumTransactionRatingRisk
	ON LKP_PremiumTransactionRatingRisk.PremiumTransactionID = EXP_CoverageDetailCommercialProperty.NewNegatePremiumTransactionID
),
FIL_InsertTarget AS (
	SELECT
	NewNegatePremiumTransactionID AS PremiumTransactionID, 
	NewNegatePremiumTransactionAKID AS PremiumTransactionAKID, 
	AuditID, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	CensusBlockGroupCountyCode, 
	CensusBlockGroupTractCode, 
	CensusBlockGroupBlockGroupCode, 
	Latitude, 
	Longitude, 
	RatingTerritoryCode, 
	o_ChangeFlag
	FROM EXP_DetectChanges
	WHERE IIF(o_ChangeFlag=1,TRUE,FALSE)
),
PremiumTransactionRatingRisk1 AS (
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
	FROM FIL_InsertTarget
),