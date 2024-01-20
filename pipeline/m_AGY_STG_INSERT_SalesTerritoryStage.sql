WITH
SQ_SalesTerritory_ODS AS (
	SELECT
		SalesTerritoryID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		SalesTerritoryCode,
		SalesTerritoryCodeDescription
	FROM SalesTerritory_ODS
),
EXP_Add_MetaDataFields AS (
	SELECT
	SalesTerritoryID,
	SourceSystemID,
	HashKey,
	ModifiedUserID,
	ModifiedDate,
	SalesTerritoryCode,
	SalesTerritoryCodeDescription,
	sysdate AS Extract_Date,
	sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_SalesTerritory_ODS
),
SalesTerritoryStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritoryStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesTerritoryStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, SalesTerritoryCode, SalesTerritoryCodeDescription, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	SALESTERRITORYCODE, 
	SALESTERRITORYCODEDESCRIPTION, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),