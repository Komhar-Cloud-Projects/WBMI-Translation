WITH
SQ_DCLineStaging AS (
	SELECT
		PolicyId,
		LineId,
		SessionId,
		Id,
		Type,
		HonorRates,
		HonoredRateEffectiveDate,
		AssignmentDate,
		AuditPeriod,
		ExtractDate,
		SourceSystemId
	FROM DCLineStaging
),
EXP_Metadata AS (
	SELECT
	PolicyId,
	LineId,
	SessionId,
	Id,
	Type,
	HonorRates,
	HonoredRateEffectiveDate,
	AssignmentDate,
	AuditPeriod,
	ExtractDate,
	SourceSystemId,
	-- *INF*: DECODE(HonorRates,'T',1,'F',0,NULL)
	DECODE(
	    HonorRates,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HonorRates,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCLineStaging
),
archDCLineStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCLineStaging
	(PolicyId, LineId, SessionId, Id, Type, HonorRates, HonoredRateEffectiveDate, AssignmentDate, AuditPeriod, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	POLICYID, 
	LINEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	o_HonorRates AS HONORRATES, 
	HONOREDRATEEFFECTIVEDATE, 
	ASSIGNMENTDATE, 
	AUDITPERIOD, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),