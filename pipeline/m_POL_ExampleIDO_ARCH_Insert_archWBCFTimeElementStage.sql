WITH
SQ_WBCFTimeElementStage AS (
	SELECT
		WBCFTimeElementStageId,
		ExtractDate,
		SourceSystemId,
		CF_TimeElementId,
		WB_CF_TimeElementId,
		SessionId,
		LimitsOnLossPayment,
		CoverageType
	FROM WBCFTimeElementStage
),
EXP_Metadata AS (
	SELECT
	WBCFTimeElementStageId,
	ExtractDate,
	SourceSystemId,
	CF_TimeElementId,
	WB_CF_TimeElementId,
	SessionId,
	LimitsOnLossPayment,
	CoverageType,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCFTimeElementStage
),
ArchWBCFTimeElementStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCFTimeElementStage
	(ExtractDate, SourceSystemId, AuditId, WBCFTimeElementStageId, CF_TimeElementId, WB_CF_TimeElementId, SessionId, LimitsOnLossPayment, CoverageType)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCFTIMEELEMENTSTAGEID, 
	CF_TIMEELEMENTID, 
	WB_CF_TIMEELEMENTID, 
	SESSIONID, 
	LIMITSONLOSSPAYMENT, 
	COVERAGETYPE
	FROM EXP_Metadata
),