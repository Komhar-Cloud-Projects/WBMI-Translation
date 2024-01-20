WITH
SQ_UnderwritingRegion_ODS AS (
	SELECT
		UnderwritingRegionID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		UnderwritingRegionCode,
		UnderwritingRegionCodeDescription
	FROM UnderwritingRegion_ODS
),
EXP_Add_MetaDataFields AS (
	SELECT
	UnderwritingRegionID,
	SourceSystemID,
	HashKey,
	ModifiedUserID,
	ModifiedDate,
	UnderwritingRegionCode,
	UnderwritingRegionCodeDescription,
	sysdate AS Extract_Date,
	Sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_UnderwritingRegion_ODS
),
UnderwritingRegionStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingRegionStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingRegionStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, UnderwritingRegionCode, UnderwritingRegionCodeDescription, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	UNDERWRITINGREGIONCODE, 
	UNDERWRITINGREGIONCODEDESCRIPTION, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),