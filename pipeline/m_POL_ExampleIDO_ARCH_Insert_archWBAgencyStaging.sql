WITH
SQ_archWBAgencyStaging AS (
	SELECT
		PartyId,
		WB_AgencyId,
		SessionId,
		Reference,
		ExtractDate,
		SourceSystemId
	FROM WBAgencyStaging
),
EXP_Metadata AS (
	SELECT
	PartyId,
	WB_AgencyId,
	SessionId,
	Reference,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_archWBAgencyStaging
),
archWBAgencyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBAgencyStaging
	(PartyId, WB_AgencyId, SessionId, Reference, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	PARTYID, 
	WB_AGENCYID, 
	SESSIONID, 
	REFERENCE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),