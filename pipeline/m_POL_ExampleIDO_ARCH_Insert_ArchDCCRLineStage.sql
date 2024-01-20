WITH
SQ_DCCRLineStage AS (
	SELECT
		DCCRLineStageId,
		LineId,
		CrLineId,
		SessionId,
		Id,
		AdditionalPremises,
		CoverageType,
		Description,
		ERISARatableEmployees,
		PolicyType,
		TotalRatableEmployees,
		ExtractDate,
		SourceSystemId
	FROM DCCRLineStage
),
EXP_Metadata AS (
	SELECT
	DCCRLineStageId,
	LineId,
	CrLineId,
	SessionId,
	Id,
	AdditionalPremises,
	CoverageType,
	Description,
	ERISARatableEmployees,
	PolicyType,
	TotalRatableEmployees,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCRLineStage
),
ArchDCCRLineStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCRLineStage
	(DCCRLineStageId, LineId, CrLineId, SessionId, Id, AdditionalPremises, CoverageType, Description, ERISARatableEmployees, PolicyType, TotalRatableEmployees, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCCRLINESTAGEID, 
	LINEID, 
	CRLINEID, 
	SESSIONID, 
	ID, 
	ADDITIONALPREMISES, 
	COVERAGETYPE, 
	DESCRIPTION, 
	ERISARATABLEEMPLOYEES, 
	POLICYTYPE, 
	TOTALRATABLEEMPLOYEES, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),