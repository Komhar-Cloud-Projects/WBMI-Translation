WITH
SQ_DC_Contact AS (
	WITH cte_DCContact(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PartyId, 
	X.ContactId, 
	X.SessionId, 
	X.Type, 
	X.PhoneNumber, 
	X.PhoneExtension, 
	X.Email 
	FROM
	DC_Contact X
	inner join
	cte_DCContact Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PartyId,
	ContactId,
	SessionId,
	Type,
	PhoneNumber,
	PhoneExtension,
	Email,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Contact
),
DCContactStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCContactStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCContactStaging
	(ExtractDate, SourceSystemId, PartyId, ContactId, SessionId, Type, PhoneNumber, PhoneExtension, Email)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	PARTYID, 
	CONTACTID, 
	SESSIONID, 
	TYPE, 
	PHONENUMBER, 
	PHONEEXTENSION, 
	EMAIL
	FROM EXP_Metadata
),