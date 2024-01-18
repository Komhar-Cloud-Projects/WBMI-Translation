WITH
SQ_WBLocationAccountStage AS (
	SELECT
		WBLocationAccountStageId,
		ExtractDate,
		SourceSystemId AS SourceSyStemId,
		LocationId,
		WBLocationAccountId,
		SessionId,
		Latitude,
		Longitude,
		ZipCodeAddOn,
		ZipCodeBase,
		GeocodeStatus,
		AddressOverridden,
		LastVerified,
		OverriddenDate,
		AddressStandardizationCompleted,
		Country,
		CityTaxCode,
		CountyTaxCode,
		CityTaxPercent,
		CountyTaxPercent,
		TaxCodeReturned,
		TaxCityOverride,
		TaxCountyOverride,
		GeoTaxCityName,
		GeoTaxCountyName,
		GeoTaxCityTaxCode,
		GeoTaxCityTaxPercent,
		GeoTaxCountyTaxCode,
		GeoTaxCountyTaxPercent,
		Cleared,
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
		CBG
	FROM WBLocationAccountStage
),
EXP_Metadata AS (
	SELECT
	WBLocationAccountStageId,
	ExtractDate,
	SourceSyStemId,
	LocationId,
	WBLocationAccountId,
	SessionId,
	Latitude,
	Longitude,
	ZipCodeAddOn,
	ZipCodeBase,
	GeocodeStatus,
	AddressOverridden,
	-- *INF*: DECODE(AddressOverridden, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AddressOverridden,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AddressOverridden,
	LastVerified,
	OverriddenDate,
	AddressStandardizationCompleted,
	Country,
	CityTaxCode,
	CountyTaxCode,
	CityTaxPercent,
	CountyTaxPercent,
	TaxCodeReturned,
	-- *INF*: DECODE(TaxCodeReturned, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TaxCodeReturned,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaxCodeReturned,
	TaxCityOverride,
	-- *INF*: DECODE(TaxCityOverride, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TaxCityOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaxCityOverride,
	TaxCountyOverride,
	-- *INF*: DECODE(TaxCountyOverride, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TaxCountyOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaxCountyOverride,
	GeoTaxCityName,
	GeoTaxCountyName,
	GeoTaxCityTaxCode,
	GeoTaxCityTaxPercent,
	GeoTaxCountyTaxCode,
	GeoTaxCountyTaxPercent,
	Cleared,
	-- *INF*: DECODE(Cleared, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Cleared,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Cleared,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CBG
	FROM SQ_WBLocationAccountStage
),
ArchWBLocationAccountStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBLocationAccountStage
	(WBLocationAccountStageId, LocationId, WBLocationAccountId, SessionId, TaxCity, TaxCounty, Latitude, Longitude, ZipCodeAddOn, ZipCodeBase, Country, CityTaxCode, CountyTaxCode, CityTaxPercent, CountyTaxPercent, TaxCodeReturned, TaxCityOverride, TaxCountyOverride, GeocodeStatus, AddressOverridden, LastVerified, OverriddenDate, GeoTaxCityName, GeoTaxCountyName, GeoTaxCityTaxCode, GeoTaxCityTaxPercent, GeoTaxCountyTaxCode, GeoTaxCountyTaxPercent, Cleared, GeoTaxCountyDistrictCode, GeoTaxCityDistrictCode, ClearedDateTimeStamp, AddressStandardizationCompleted, GeoTaxConfidence, TerritoryCodeAuto, TerritoryCodeCrime, TerritoryCodeEarthQuake, TerritoryCodeGL, TerritoryCodeProperty, TerritoryCounty, TerritoryProtectionClass, ExtractDate, SourceSystemId, AuditId, TerritoryIllinoisFireTaxLocationCode, CBG)
	SELECT 
	WBLOCATIONACCOUNTSTAGEID, 
	LOCATIONID, 
	WBLOCATIONACCOUNTID, 
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
	ADDRESSSTANDARDIZATIONCOMPLETED, 
	GEOTAXCONFIDENCE, 
	TERRITORYCODEAUTO, 
	TERRITORYCODECRIME, 
	TERRITORYCODEEARTHQUAKE, 
	TERRITORYCODEGL, 
	TERRITORYCODEPROPERTY, 
	TERRITORYCOUNTY, 
	TERRITORYPROTECTIONCLASS, 
	EXTRACTDATE, 
	SourceSyStemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	TERRITORYILLINOISFIRETAXLOCATIONCODE, 
	CBG
	FROM EXP_Metadata
),