WITH
SQ_AgencyAddress AS (
	SELECT
		AgencyAddressID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AddressType,
		AddressLine1,
		AddressLine2,
		AddressLine3,
		City,
		ZipCode,
		CountyCode,
		CountyName,
		StateAbbreviation,
		CountryAbbreviation,
		Latitude,
		Longitude
	FROM AgencyAddress
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
EXP_Add_MetaDataFields AS (
	SELECT
	SQ_AgencyAddress.AgencyAddressID,
	SQ_AgencyAddress.SourceSystemID,
	SQ_AgencyAddress.HashKey,
	SQ_AgencyAddress.ModifiedUserID,
	SQ_AgencyAddress.ModifiedDate,
	SQ_AgencyAddress.AgencyID,
	SQ_AgencyAddress.AddressType,
	SQ_AgencyAddress.AddressLine1,
	SQ_AgencyAddress.AddressLine2,
	SQ_AgencyAddress.AddressLine3,
	SQ_AgencyAddress.City,
	SQ_AgencyAddress.ZipCode,
	SQ_AgencyAddress.CountyCode,
	SQ_AgencyAddress.CountyName,
	SQ_AgencyAddress.StateAbbreviation,
	SQ_AgencyAddress.CountryAbbreviation,
	SQ_AgencyAddress.Latitude,
	SQ_AgencyAddress.Longitude,
	LKP_AgencyCode.AgencyCode AS lkp_AgencyCode,
	sysdate AS Extract_Date,
	sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_AgencyAddress
	LEFT JOIN LKP_AgencyCode
	ON LKP_AgencyCode.AgencyID = SQ_AgencyAddress.AgencyID
),
AgencyAddressODSStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyAddressODSStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyAddressODSStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, AddressType, AddressLine1, AddressLine2, AddressLine3, City, ZipCode, CountyCode, CountyName, StateAbbreviation, CountryAbbreviation, Latitude, Longitude, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	lkp_AgencyCode AS AGENCYCODE, 
	ADDRESSTYPE, 
	ADDRESSLINE1, 
	ADDRESSLINE2, 
	ADDRESSLINE3, 
	CITY, 
	ZIPCODE, 
	COUNTYCODE, 
	COUNTYNAME, 
	STATEABBREVIATION, 
	COUNTRYABBREVIATION, 
	LATITUDE, 
	LONGITUDE, 
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),