WITH
SQ_WB_LocationAccount AS (
	WITH cte_WBLocationAccount(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LocationId, 
	X.WB_LocationAccountId, 
	X.SessionId, 
	X.Latitude, 
	X.Longitude, 
	X.ZipCodeAddOn, 
	X.ZipCodeBase, 
	X.GeocodeStatus, 
	X.AddressOverridden, 
	X.LastVerified, 
	X.OverriddenDate, 
	X.AddressStandardizationCompleted, 
	X.Country, 
	X.CityTaxCode, 
	X.CountyTaxCode, 
	X.CityTaxPercent, 
	X.CountyTaxPercent, 
	X.TaxCodeReturned, 
	X.TaxCityOverride, 
	X.TaxCountyOverride, 
	X.GeoTaxCityName, 
	X.GeoTaxCountyName, 
	X.GeoTaxCityTaxCode, 
	X.GeoTaxCityTaxPercent, 
	X.GeoTaxCountyTaxCode, 
	X.GeoTaxCountyTaxPercent, 
	X.Cleared, 
	X.GeoTaxCountyDistrictCode, 
	X.GeoTaxCityDistrictCode, 
	X.ClearedDateTimeStamp, 
	X.GeoTaxConfidence, 
	X.TerritoryCodeAuto, 
	X.TerritoryCodeCrime, 
	X.TerritoryCodeEarthQuake, 
	X.TerritoryCodeGL, 
	X.TerritoryCodeProperty, 
	X.TerritoryCounty, 
	X.TerritoryProtectionClass, 
	X.TaxCity, 
	X.TaxCounty, 
	X.TerritoryIllinoisFireTaxLocationCode,
	X.CBG
	FROM
	WB_LocationAccount X
	inner join
	cte_WBLocationAccount Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	AddressOverridden AS i_AddressOverridden,
	AddressStandardizationCompleted AS i_AddressStandardizationCompleted,
	TaxCodeReturned AS i_TaxCodeReturned,
	TaxCityOverride AS i_TaxCityOverride,
	TaxCountyOverride AS i_TaxCountyOverride,
	Cleared AS i_Cleared,
	LocationId,
	WB_LocationAccountId,
	SessionId,
	Latitude,
	Longitude,
	ZipCodeAddOn,
	ZipCodeBase,
	GeocodeStatus,
	LastVerified,
	OverriddenDate,
	Country,
	CityTaxCode,
	CountyTaxCode,
	CityTaxPercent,
	CountyTaxPercent,
	GeoTaxCityName,
	GeoTaxCountyName,
	GeoTaxCityTaxCode,
	GeoTaxCityTaxPercent,
	GeoTaxCountyTaxCode,
	GeoTaxCountyTaxPercent,
	GeoTaxCountyDistrictCode,
	GeoTaxCityDistrictCode,
	ClearedDateTimeStamp,
	GeoTaxConfidence,
	TerritoryCodeAuto,
	TerritoryCodeCrime,
	TerritoryCodeEarthQuake,
	TerritoryCodeGL,
	TerritoryCodeProperty,
	TerritoryCounty,
	TerritoryProtectionClass,
	TaxCity,
	TaxCounty,
	TerritoryIllinoisFireTaxLocationCode,
	CBG,
	-- *INF*: DECODE(i_AddressOverridden, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_AddressOverridden,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AddressOverridden,
	-- *INF*: DECODE(i_TaxCodeReturned, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_TaxCodeReturned,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaxCodeReturned,
	-- *INF*: DECODE(i_TaxCityOverride, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_TaxCityOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaxCityOverride,
	-- *INF*: DECODE(i_TaxCountyOverride, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_TaxCountyOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaxCountyOverride,
	-- *INF*: DECODE(i_AddressStandardizationCompleted, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_AddressStandardizationCompleted,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AddressStandardizationCompleted1,
	-- *INF*: DECODE(i_Cleared, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_Cleared,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Cleared,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_LocationAccount
),
WBLocationAccountStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBLocationAccountStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBLocationAccountStage
	(LocationId, WBLocationAccountId, SessionId, TaxCity, TaxCounty, Latitude, Longitude, ZipCodeAddOn, ZipCodeBase, Country, CityTaxCode, CountyTaxCode, CityTaxPercent, CountyTaxPercent, TaxCodeReturned, TaxCityOverride, TaxCountyOverride, GeocodeStatus, AddressOverridden, LastVerified, OverriddenDate, GeoTaxCityName, GeoTaxCountyName, GeoTaxCityTaxCode, GeoTaxCityTaxPercent, GeoTaxCountyTaxCode, GeoTaxCountyTaxPercent, Cleared, GeoTaxCountyDistrictCode, GeoTaxCityDistrictCode, ClearedDateTimeStamp, AddressStandardizationCompleted, GeoTaxConfidence, TerritoryCodeAuto, TerritoryCodeCrime, TerritoryCodeEarthQuake, TerritoryCodeGL, TerritoryCodeProperty, TerritoryCounty, TerritoryProtectionClass, ExtractDate, SourceSystemId, TerritoryIllinoisFireTaxLocationCode, CBG)
	SELECT 
	LOCATIONID, 
	WB_LocationAccountId AS WBLOCATIONACCOUNTID, 
	SESSIONID, 
	TAXCITY, 
	TAXCOUNTY, 
	LATITUDE, 
	LONGITUDE, 
	ZIPCODEADDON, 
	ZIPCODEBASE, 
	COUNTRY, 
	CITYTAXCODE, 
	COUNTYTAXCODE, 
	CITYTAXPERCENT, 
	COUNTYTAXPERCENT, 
	o_TaxCodeReturned AS TAXCODERETURNED, 
	o_TaxCityOverride AS TAXCITYOVERRIDE, 
	o_TaxCountyOverride AS TAXCOUNTYOVERRIDE, 
	GEOCODESTATUS, 
	o_AddressOverridden AS ADDRESSOVERRIDDEN, 
	LASTVERIFIED, 
	OVERRIDDENDATE, 
	GEOTAXCITYNAME, 
	GEOTAXCOUNTYNAME, 
	GEOTAXCITYTAXCODE, 
	GEOTAXCITYTAXPERCENT, 
	GEOTAXCOUNTYTAXCODE, 
	GEOTAXCOUNTYTAXPERCENT, 
	o_Cleared AS CLEARED, 
	GEOTAXCOUNTYDISTRICTCODE, 
	GEOTAXCITYDISTRICTCODE, 
	CLEAREDDATETIMESTAMP, 
	o_AddressStandardizationCompleted1 AS ADDRESSSTANDARDIZATIONCOMPLETED, 
	GEOTAXCONFIDENCE, 
	TERRITORYCODEAUTO, 
	TERRITORYCODECRIME, 
	TERRITORYCODEEARTHQUAKE, 
	TERRITORYCODEGL, 
	TERRITORYCODEPROPERTY, 
	TERRITORYCOUNTY, 
	TERRITORYPROTECTIONCLASS, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	TERRITORYILLINOISFIRETAXLOCATIONCODE, 
	CBG
	FROM EXP_Metadata
),