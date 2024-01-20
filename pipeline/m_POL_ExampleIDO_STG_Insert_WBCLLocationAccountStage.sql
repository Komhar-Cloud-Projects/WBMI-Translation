WITH
SQ_WB_CL_LocationAccount AS (
	WITH cte_WBCLLocationAccount(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_LocationAccountId, 
	X.WB_CL_LocationAccountId, 
	X.SessionId, 
	X.VehicleGaragingLocation, 
	X.TaxFactorApplied, 
	X.KYTaxFactorAppliedIndicator, 
	X.AnyAlcoholSold, 
	X.AddressInVerifyState, 
	X.Confidence, 
	X.GeocodeMatchCode, 
	X.GeocodeStatus, 
	X.GeocodeStatusDisplay, 
	X.GeocodeStatusDisplayWithDate, 
	X.IsVerified, 
	X.LastVerified, 
	X.Latitude, 
	X.Longitude 
	FROM
	WB_CL_LocationAccount X
	inner join
	cte_WBCLLocationAccount Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	AnyAlcoholSold AS i_AnyAlcoholSold,
	AddressInVerifyState AS i_AddressInVerifyState,
	IsVerified AS i_IsVerified,
	WB_LocationAccountId,
	WB_CL_LocationAccountId,
	SessionId,
	VehicleGaragingLocation AS i_VehicleGaragingLocation,
	-- *INF*: DECODE(i_VehicleGaragingLocation, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_VehicleGaragingLocation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VehicleGaragingLocation,
	TaxFactorApplied,
	KYTaxFactorAppliedIndicator,
	Confidence,
	GeocodeMatchCode,
	GeocodeStatus,
	GeocodeStatusDisplay,
	GeocodeStatusDisplayWithDate,
	LastVerified,
	Latitude,
	Longitude,
	-- *INF*: DECODE(i_AnyAlcoholSold, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_AnyAlcoholSold,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AnyAlcoholSold,
	-- *INF*: DECODE(i_AddressInVerifyState, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_AddressInVerifyState,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AddressInVerifyState,
	-- *INF*: DECODE(i_IsVerified, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_IsVerified,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsVerified,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CL_LocationAccount
),
WBCLLocationAccountStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLLocationAccountStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLLocationAccountStage
	(ExtractDate, SourceSystemId, WBLocationAccountId, WBCLLocationAccountId, SessionId, VehicleGaragingLocation, TaxFactorApplied, KYTaxFactorAppliedIndicator, AnyAlcoholSold, AddressInVerifyState, Confidence, GeocodeMatchCode, GeocodeStatus, GeocodeStatusDisplay, GeocodeStatusDisplayWithDate, IsVerified, LastVerified, Latitude, Longitude)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_LocationAccountId AS WBLOCATIONACCOUNTID, 
	WB_CL_LocationAccountId AS WBCLLOCATIONACCOUNTID, 
	SESSIONID, 
	o_VehicleGaragingLocation AS VEHICLEGARAGINGLOCATION, 
	TAXFACTORAPPLIED, 
	KYTAXFACTORAPPLIEDINDICATOR, 
	o_AnyAlcoholSold AS ANYALCOHOLSOLD, 
	o_AddressInVerifyState AS ADDRESSINVERIFYSTATE, 
	CONFIDENCE, 
	GEOCODEMATCHCODE, 
	GEOCODESTATUS, 
	GEOCODESTATUSDISPLAY, 
	GEOCODESTATUSDISPLAYWITHDATE, 
	o_IsVerified AS ISVERIFIED, 
	LASTVERIFIED, 
	LATITUDE, 
	LONGITUDE
	FROM EXP_Metadata
),