WITH
SQ_WBPartyStaging AS (
	SELECT
		WBPartyStagingId,
		PartyId,
		WB_PartyId,
		SessionId,
		CustomerNum,
		ExtractDate,
		SourceSystemId,
		FEIN AS Fein,
		DoingBusinessAs,
		Country,
		Province,
		PostalCode,
		ApplicantInformationUnique,
		CurrentLocationID,
		CustomerRecordReadOnly,
		CreatedByInternalUser
	FROM WBPartyStaging
),
EXP_Metadata AS (
	SELECT
	WBPartyStagingId,
	PartyId,
	WB_PartyId,
	SessionId,
	CustomerNum,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	Fein,
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
	) AS o_CreatedByInternalUser
	FROM SQ_WBPartyStaging
),
archWBPartyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBPartyStaging
	(ExtractDate, SourceSystemId, AuditId, WBPartyStagingId, PartyId, WB_PartyId, SessionId, CustomerNum, FEIN, DoingBusinessAs, Country, Province, PostalCode, ApplicantInformationUnique, CurrentLocationID, CustomerRecordReadOnly, CreatedByInternalUser)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBPARTYSTAGINGID, 
	PARTYID, 
	WB_PARTYID, 
	SESSIONID, 
	CUSTOMERNUM, 
	Fein AS FEIN, 
	DOINGBUSINESSAS, 
	COUNTRY, 
	PROVINCE, 
	POSTALCODE, 
	APPLICANTINFORMATIONUNIQUE, 
	CURRENTLOCATIONID, 
	o_CustomerRecordReadOnly AS CUSTOMERRECORDREADONLY, 
	o_CreatedByInternalUser AS CREATEDBYINTERNALUSER
	FROM EXP_Metadata
),