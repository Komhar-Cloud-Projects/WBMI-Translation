WITH
SQ_DCCREndorsementStage AS (
	SELECT
		DCCREndorsementStageId,
		CRRiskId,
		CREndorsementId,
		SessionId,
		Id,
		Type,
		FaithfulPerformanceCoverageWritten,
		ExtractDate,
		SourceSystemId
	FROM DCCREndorsementStage
),
EXP_Metadata AS (
	SELECT
	DCCREndorsementStageId,
	CRRiskId,
	CREndorsementId,
	SessionId,
	Id,
	Type,
	FaithfulPerformanceCoverageWritten,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCREndorsementStage
),
ArchDCCREndorsementStage AS (
	INSERT INTO ArchDCCREndorsementStage
	(ExtractDate, SourceSystemId, AuditId, DCCREndorsementStageId, CRRiskId, CREndorsementId, SessionId, Id, Type, FaithfulPerformanceCoverageWritten)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCRENDORSEMENTSTAGEID, 
	CRRISKID, 
	CRENDORSEMENTID, 
	SESSIONID, 
	ID, 
	TYPE, 
	FAITHFULPERFORMANCECOVERAGEWRITTEN
	FROM EXP_Metadata
),