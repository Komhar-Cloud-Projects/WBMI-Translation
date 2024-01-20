WITH
SQ_WBCLLocationAccountStage AS (
	SELECT
		WBCLLocationAccountStageId,
		ExtractDate,
		SourceSystemId AS SourceSyStemId,
		WBLocationAccountId,
		WBCLLocationAccountId,
		SessionId,
		VehicleGaragingLocation,
		TaxFactorApplied,
		KYTaxFactorAppliedIndicator,
		AnyAlcoholSold,
		AddressInVerifyState,
		Confidence,
		GeocodeMatchCode,
		GeocodeStatus,
		GeocodeStatusDisplay,
		GeocodeStatusDisplayWithDate,
		IsVerified,
		LastVerified,
		Latitude,
		Longitude
	FROM WBCLLocationAccountStage
),
EXP_Metadata AS (
	SELECT
	WBCLLocationAccountStageId,
	ExtractDate,
	SourceSyStemId,
	WBLocationAccountId,
	WBCLLocationAccountId,
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
	AnyAlcoholSold,
	-- *INF*: DECODE(AnyAlcoholSold, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AnyAlcoholSold,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AnyAlcoholSold,
	AddressInVerifyState,
	-- *INF*: DECODE(AddressInVerifyState, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AddressInVerifyState,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AddressInVerifyState,
	Confidence,
	GeocodeMatchCode,
	GeocodeStatus,
	GeocodeStatusDisplay,
	GeocodeStatusDisplayWithDate,
	IsVerified,
	-- *INF*: DECODE(IsVerified, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IsVerified,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsVerified,
	LastVerified,
	Latitude,
	Longitude,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCLLocationAccountStage
),
ArchWBCLLocationAccountStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCLLocationAccountStage
	(ExtractDate, SourceSystemId, AuditId, WBCLLocationAccountStageId, WBLocationAccountId, WBCLLocationAccountId, SessionId, VehicleGaragingLocation, TaxFactorApplied, KYTaxFactorAppliedIndicator, AnyAlcoholSold, AddressInVerifyState, Confidence, GeocodeMatchCode, GeocodeStatus, GeocodeStatusDisplay, GeocodeStatusDisplayWithDate, IsVerified, LastVerified, Latitude, Longitude)
	SELECT 
	EXTRACTDATE, 
	SourceSyStemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCLLOCATIONACCOUNTSTAGEID, 
	WBLOCATIONACCOUNTID, 
	WBCLLOCATIONACCOUNTID, 
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