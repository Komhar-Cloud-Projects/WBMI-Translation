WITH
SQ_WBLocationStaging AS (
	SELECT
		WBLocationStagingId,
		LocationId,
		WB_LocationId,
		SessionId,
		LocationNumber,
		LocationName,
		ExtractDate,
		SourceSystemId,
		PrimaryEmail,
		SecondaryEmail
	FROM WBLocationStaging
),
EXP_Metadata AS (
	SELECT
	WBLocationStagingId,
	LocationId,
	WB_LocationId,
	SessionId,
	LocationNumber,
	LocationName,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	PrimaryEmail,
	SecondaryEmail
	FROM SQ_WBLocationStaging
),
archWBLocationStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBLocationStaging
	(ExtractDate, SourceSystemId, AuditId, WBLocationStagingId, LocationId, WB_LocationId, SessionId, LocationNumber, LocationName, PrimaryEmail, SecondaryEmail)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBLOCATIONSTAGINGID, 
	LOCATIONID, 
	WB_LOCATIONID, 
	SESSIONID, 
	LOCATIONNUMBER, 
	LOCATIONNAME, 
	PRIMARYEMAIL, 
	SECONDARYEMAIL
	FROM EXP_Metadata
),