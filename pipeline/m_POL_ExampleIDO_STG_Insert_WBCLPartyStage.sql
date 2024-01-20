WITH
SQ_WB_CL_Party AS (
	WITH cte_WBCLParty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_PartyId, 
	X.WB_CL_PartyId, 
	X.SessionId, 
	X.County, 
	X.AttentionLine, 
	X.AttentionLineInput, 
	X.ClassDescriptionVersion, 
	X.LastVerified, 
	X.Confidence, 
	X.AddressInVerifyState, 
	X.GeocodeMatchCode, 
	X.GeocodeStatus, 
	X.GeocodeStatusDisplay, 
	X.IsVerified, 
	X.Latitude, 
	X.Longitude, 
	X.BusinessOrIndividual 
	FROM
	WB_CL_Party X
	inner join
	cte_WBCLParty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	-- *INF*: DECODE(AddressInVerifyState,'T','1','F','0')
	DECODE(
	    AddressInVerifyState,
	    'T', '1',
	    'F', '0'
	) AS o_AddressInVerifyState,
	GeocodeMatchCode,
	GeocodeStatus,
	GeocodeStatusDisplay,
	IsVerified,
	-- *INF*: DECODE(IsVerified,'T','1','F','0')
	DECODE(
	    IsVerified,
	    'T', '1',
	    'F', '0'
	) AS o_IsVerified,
	Latitude,
	Longitude,
	BusinessOrIndividual,
	-- *INF*: DECODE(BusinessOrIndividual,'T','1','F','0')
	DECODE(
	    BusinessOrIndividual,
	    'T', '1',
	    'F', '0'
	) AS o_BusinessOrIndividual
	FROM SQ_WB_CL_Party
),
WBCLPartyStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLPartyStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLPartyStage
	(ExtractDate, SourceSystemId, WB_PartyId, WB_CL_PartyId, SessionId, County, AttentionLine, AttentionLineInput, BusinessOrIndividual, ClassDescriptionVersion, LastVerified, Confidence, AddressInVerifyState, GeocodeMatchCode, GeocodeStatus, GeocodeStatusDisplay, IsVerified, Latitude, Longitude)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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