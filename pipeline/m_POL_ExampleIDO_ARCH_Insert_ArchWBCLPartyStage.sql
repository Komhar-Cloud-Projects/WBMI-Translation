WITH
SQ_WBCLPartyStaging AS (
	SELECT
		WBCLPartyStageId AS WBCLPartyStageID,
		ExtractDate,
		SourceSystemId,
		WB_PartyId,
		WB_CL_PartyId,
		SessionId,
		County,
		AttentionLine,
		AttentionLineInput,
		ClassDescriptionVersion,
		LastVerified,
		Confidence,
		AddressInVerifyState,
		GeocodeMatchCode,
		GeocodeStatus,
		GeocodeStatusDisplay,
		IsVerified,
		Latitude,
		Longitude,
		BusinessOrIndividual
	FROM WBCLPartyStaging
),
EXPTRANS AS (
	SELECT
	WBCLPartyStageID,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	WB_PartyId,
	WB_CL_PartyId,
	SessionId,
	County,
	AttentionLine,
	AttentionLineInput,
	ClassDescriptionVersion,
	LastVerified,
	Confidence,
	AddressInVerifyState,
	-- *INF*: DECODE(AddressInVerifyState, 'T', '1', 'F', '0', NULL)
	DECODE(
	    AddressInVerifyState,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AddressInVerifyState,
	GeocodeMatchCode,
	GeocodeStatus,
	GeocodeStatusDisplay,
	IsVerified,
	-- *INF*: DECODE(IsVerified, 'T', '1', 'F', '0', NULL)
	DECODE(
	    IsVerified,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IsVerified,
	Latitude,
	Longitude,
	BusinessOrIndividual,
	-- *INF*: DECODE(BusinessOrIndividual, 'T', '1', 'F', '0', NULL)
	DECODE(
	    BusinessOrIndividual,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_BusinessOrIndividual
	FROM SQ_WBCLPartyStaging
),
ArchWBCLPartyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCLPartyStage
	(ExtractDate, SourceSystemId, AuditId, WBCLPartyStageId, WB_PartyId, WB_CL_PartyId, SessionId, County, AttentionLine, AttentionLineInput, BusinessOrIndividual, ClassDescriptionVersion, LastVerified, Confidence, AddressInVerifyState, GeocodeMatchCode, GeocodeStatus, GeocodeStatusDisplay, IsVerified, Latitude, Longitude)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditID AS AUDITID, 
	WBCLPartyStageID AS WBCLPARTYSTAGEID, 
	WB_PARTYID, 
	WB_CL_PARTYID, 
	SESSIONID, 
	COUNTY, 
	ATTENTIONLINE, 
	ATTENTIONLINEINPUT, 
	o_BusinessOrIndividual AS BUSINESSORINDIVIDUAL, 
	CLASSDESCRIPTIONVERSION, 
	LASTVERIFIED, 
	CONFIDENCE, 
	o_AddressInVerifyState AS ADDRESSINVERIFYSTATE, 
	GEOCODEMATCHCODE, 
	GEOCODESTATUS, 
	GEOCODESTATUSDISPLAY, 
	o_IsVerified AS ISVERIFIED, 
	LATITUDE, 
	LONGITUDE
	FROM EXPTRANS
),