WITH
SQ_DCReinsuranceStaging AS (
	SELECT
		PolicyId,
		ReinsuranceId,
		SessionId,
		Id,
		Type,
		AggregateLimit,
		CertificatePolicyNumber,
		CommissionRate,
		OccurrenceLimit,
		PercentCeded,
		PercentLoss,
		Company,
		CompanyNumber,
		EffectiveDate,
		ExpirationDate,
		ExtractDate,
		SourceSystemId
	FROM DCReinsuranceStaging
),
EXP_Metadata AS (
	SELECT
	PolicyId,
	ReinsuranceId,
	SessionId,
	Id,
	Type,
	AggregateLimit,
	CertificatePolicyNumber,
	CommissionRate,
	OccurrenceLimit,
	PercentCeded,
	PercentLoss,
	Company,
	CompanyNumber,
	EffectiveDate,
	ExpirationDate,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCReinsuranceStaging
),
archDCReinsuranceStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCReinsuranceStaging
	(PolicyId, ReinsuranceId, SessionId, Id, Type, AggregateLimit, CertificatePolicyNumber, CommissionRate, OccurrenceLimit, PercentCeded, PercentLoss, Company, CompanyNumber, EffectiveDate, ExpirationDate, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	POLICYID, 
	REINSURANCEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	AGGREGATELIMIT, 
	CERTIFICATEPOLICYNUMBER, 
	COMMISSIONRATE, 
	OCCURRENCELIMIT, 
	PERCENTCEDED, 
	PERCENTLOSS, 
	COMPANY, 
	COMPANYNUMBER, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),