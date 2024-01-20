WITH
SQ_SalesTerritoryRelationship_ODS AS (
	SELECT
		SalesTerritoryRelationshipID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		SalesTerritoryID
	FROM SalesTerritoryRelationship_ODS
),
LKP_AgencyCode AS (
	SELECT
	AgencyCode,
	AgencyID
	FROM (
		SELECT 
			AgencyCode,
			AgencyID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agency
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID ORDER BY AgencyCode) = 1
),
LKP_SalesTerritoryCode AS (
	SELECT
	SalesTerritoryCode,
	SalesTerritoryID
	FROM (
		SELECT 
			SalesTerritoryCode,
			SalesTerritoryID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesTerritory
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SalesTerritoryID ORDER BY SalesTerritoryCode) = 1
),
EXP_Add_MetaDataFields AS (
	SELECT
	SQ_SalesTerritoryRelationship_ODS.SalesTerritoryRelationshipID,
	SQ_SalesTerritoryRelationship_ODS.SourceSystemID,
	SQ_SalesTerritoryRelationship_ODS.HashKey,
	SQ_SalesTerritoryRelationship_ODS.ModifiedUserID,
	SQ_SalesTerritoryRelationship_ODS.ModifiedDate,
	SQ_SalesTerritoryRelationship_ODS.AgencyID,
	SQ_SalesTerritoryRelationship_ODS.SalesTerritoryID,
	LKP_AgencyCode.AgencyCode AS lkp_AgencyCode,
	LKP_SalesTerritoryCode.SalesTerritoryCode AS lkp_SalesTerritoryCode,
	sysdate AS Extract_Date,
	sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_SalesTerritoryRelationship_ODS
	LEFT JOIN LKP_AgencyCode
	ON LKP_AgencyCode.AgencyID = SQ_SalesTerritoryRelationship_ODS.AgencyID
	LEFT JOIN LKP_SalesTerritoryCode
	ON LKP_SalesTerritoryCode.SalesTerritoryID = SQ_SalesTerritoryRelationship_ODS.SalesTerritoryID
),
SalesTerritoryRelationshipStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritoryRelationshipStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritoryRelationshipStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, SalesTerritoryID, SalesTerritoryCode, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	lkp_AgencyCode AS AGENCYCODE, 
	SALESTERRITORYID, 
	lkp_SalesTerritoryCode AS SALESTERRITORYCODE, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),