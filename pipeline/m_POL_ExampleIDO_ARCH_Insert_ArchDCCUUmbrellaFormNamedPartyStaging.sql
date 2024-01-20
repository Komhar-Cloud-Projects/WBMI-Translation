WITH
SQ_DCCUUmbrellaFormNamedPartyStaging AS (
	SELECT
		DCCUUmbrellaFormNamedPartyStagingId,
		ExtractDate,
		SourceSystemId,
		CU_UmbrellaFormId,
		CU_UmbrellaFormNamedPartyId,
		SessionId,
		Type,
		NameOfPersonOrOrganization
	FROM DCCUUmbrellaFormNamedPartyStaging
),
EXPTRANS AS (
	SELECT
	DCCUUmbrellaFormNamedPartyStagingId,
	ExtractDate,
	SourceSystemId,
	CU_UmbrellaFormId,
	CU_UmbrellaFormNamedPartyId,
	SessionId,
	Type,
	NameOfPersonOrOrganization,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_auditid
	FROM SQ_DCCUUmbrellaFormNamedPartyStaging
),
ArchDCCUUmbrellaFormNamedPartyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCUUmbrellaFormNamedPartyStaging
	(ExtractDate, SourceSystemId, AuditId, CU_UmbrellaFormId, CU_UmbrellaFormNamedPartyId, SessionId, Type, NameOfPersonOrOrganization)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_auditid AS AUDITID, 
	CU_UMBRELLAFORMID, 
	CU_UMBRELLAFORMNAMEDPARTYID, 
	SESSIONID, 
	TYPE, 
	NAMEOFPERSONORORGANIZATION
	FROM EXPTRANS
),