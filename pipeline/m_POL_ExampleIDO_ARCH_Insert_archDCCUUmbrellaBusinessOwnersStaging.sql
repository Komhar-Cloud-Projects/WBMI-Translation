WITH
SQ_DCCUUmbrellaBusinessOwnersStaging AS (
	SELECT
		DCCUUmbrellaBusinessOwnersStagingId,
		ExtractDate,
		SourceSystemId,
		LineId,
		CU_UmbrellaBusinessOwnersId,
		SessionId,
		Id,
		Description,
		EffectiveDate,
		ExpirationDate,
		PersonalLiability,
		PolicyNumber
	FROM DCCUUmbrellaBusinessOwnersStaging
),
EXP_Metadata AS (
	SELECT
	DCCUUmbrellaBusinessOwnersStagingId,
	ExtractDate,
	SourceSystemId,
	LineId,
	CU_UmbrellaBusinessOwnersId,
	SessionId,
	Id,
	Description,
	EffectiveDate,
	ExpirationDate,
	PersonalLiability,
	PolicyNumber,
	-- *INF*: DECODE(PersonalLiability, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PersonalLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PersonalLiability,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCUUmbrellaBusinessOwnersStaging
),
ArchDCCUUmbrellaBusinessOwnersStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCUUmbrellaBusinessOwnersStaging
	(ExtractDate, SourceSystemId, AuditId, LineId, CU_UmbrellaBusinessOwnersId, SessionId, Id, Description, EffectiveDate, ExpirationDate, PersonalLiability, PolicyNumber)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	LINEID, 
	CU_UMBRELLABUSINESSOWNERSID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	o_PersonalLiability AS PERSONALLIABILITY, 
	POLICYNUMBER
	FROM EXP_Metadata
),