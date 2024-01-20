WITH
SQ_WB_Party AS (
	WITH cte_WBParty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PartyId, 
	X.WB_PartyId, 
	X.SessionId, 
	X.CustomerNum, 
	X.FEIN, 
	X.DoingBusinessAs, 
	X.Country, 
	X.Province, 
	X.PostalCode, 
	X.ApplicantInformationUnique, 
	X.CurrentLocationID, 
	X.CustomerRecordReadOnly, 
	X.CreatedByInternalUser 
	FROM
	WB_Party X
	inner join
	cte_WBParty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PartyId,
	WB_PartyId,
	SessionId,
	CustomerNum,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	FEIN,
	DoingBusinessAs,
	Country,
	Province,
	PostalCode,
	ApplicantInformationUnique,
	CurrentLocationID,
	CustomerRecordReadOnly,
	-- *INF*: DECODE(CustomerRecordReadOnly, 'T',1,'F',0, NULL)
	DECODE(
	    CustomerRecordReadOnly,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CustomerRecordReadOnly,
	CreatedByInternalUser,
	-- *INF*: DECODE(CreatedByInternalUser, 'T',1,'F',0, NULL)
	DECODE(
	    CreatedByInternalUser,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS i_CreatedByInternalUser
	FROM SQ_WB_Party
),
WBPartyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBPartyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBPartyStaging
	(ExtractDate, SourceSystemId, PartyId, WB_PartyId, SessionId, CustomerNum, FEIN, DoingBusinessAs, Country, Province, PostalCode, ApplicantInformationUnique, CurrentLocationID, CustomerRecordReadOnly, CreatedByInternalUser)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	PARTYID, 
	WB_PARTYID, 
	SESSIONID, 
	CUSTOMERNUM, 
	FEIN, 
	DOINGBUSINESSAS, 
	COUNTRY, 
	PROVINCE, 
	POSTALCODE, 
	APPLICANTINFORMATIONUNIQUE, 
	CURRENTLOCATIONID, 
	o_CustomerRecordReadOnly AS CUSTOMERRECORDREADONLY, 
	i_CreatedByInternalUser AS CREATEDBYINTERNALUSER
	FROM EXP_Metadata
),