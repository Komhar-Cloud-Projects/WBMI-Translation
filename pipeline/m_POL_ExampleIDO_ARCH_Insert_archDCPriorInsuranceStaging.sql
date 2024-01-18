WITH
SQ_DCPriorInsuranceStaging AS (
	SELECT
		DCPriorInsuranceStagingId,
		PolicyId,
		PriorInsuranceId,
		SessionId,
		Id,
		CarrierName,
		EffectiveDate,
		ExpirationDate,
		PolicyNumber,
		PolicyType,
		ModificationFactor,
		TotalPremium,
		ExtractDate,
		SourceSystemId
	FROM DCPriorInsuranceStaging
),
EXP_Metadata AS (
	SELECT
	DCPriorInsuranceStagingId,
	PolicyId,
	PriorInsuranceId,
	SessionId,
	Id,
	CarrierName,
	EffectiveDate,
	ExpirationDate,
	PolicyNumber,
	PolicyType,
	ModificationFactor,
	TotalPremium,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCPriorInsuranceStaging
),
archDCPriorInsuranceStaging AS (
	INSERT INTO archDCPriorInsuranceStaging
	(PolicyId, PriorInsuranceId, SessionId, Id, CarrierName, EffectiveDate, ExpirationDate, PolicyNumber, PolicyType, ModificationFactor, TotalPremium, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	POLICYID, 
	PRIORINSURANCEID, 
	SESSIONID, 
	ID, 
	CARRIERNAME, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	POLICYNUMBER, 
	POLICYTYPE, 
	MODIFICATIONFACTOR, 
	TOTALPREMIUM, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),