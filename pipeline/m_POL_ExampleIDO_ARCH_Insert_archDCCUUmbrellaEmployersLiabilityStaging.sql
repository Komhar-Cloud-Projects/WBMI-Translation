WITH
SQ_DCCUUmbrellaEmployersLiabilityStaging AS (
	SELECT
		DCCUUmbrellaEmployersLiabilityStagingId,
		ExtractDate,
		SourceSystemId,
		LineId,
		CU_UmbrellaEmployersLiabilityId,
		SessionId,
		Id,
		CarrierName,
		Description,
		EffectiveDate,
		ExpirationDate,
		PolicyNumber
	FROM DCCUUmbrellaEmployersLiabilityStaging
),
EXP_Metadata AS (
	SELECT
	DCCUUmbrellaEmployersLiabilityStagingId,
	ExtractDate,
	SourceSystemId,
	LineId,
	CU_UmbrellaEmployersLiabilityId,
	SessionId,
	Id,
	CarrierName,
	Description,
	EffectiveDate,
	ExpirationDate,
	PolicyNumber,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCUUmbrellaEmployersLiabilityStaging
),
ArchDCCUUmbrellaEmployersLiabilityStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCUUmbrellaEmployersLiabilityStaging
	(ExtractDate, SourceSystemId, AuditId, LineId, CU_UmbrellaEmployersLiabilityId, SessionId, Id, CarrierName, Description, EffectiveDate, ExpirationDate, PolicyNumber)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	LINEID, 
	CU_UMBRELLAEMPLOYERSLIABILITYID, 
	SESSIONID, 
	ID, 
	CARRIERNAME, 
	DESCRIPTION, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	POLICYNUMBER
	FROM EXP_Metadata
),