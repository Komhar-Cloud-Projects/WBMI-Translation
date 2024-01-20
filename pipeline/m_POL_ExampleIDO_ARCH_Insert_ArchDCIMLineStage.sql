WITH
SQ_DCIMLineStage AS (
	SELECT
		DCIMLineStageId,
		ExtractDate,
		SourceSystemId,
		LineId,
		IM_LineId,
		SessionId,
		Description,
		PolicyPayment
	FROM DCIMLineStage
),
EXP_Metadata AS (
	SELECT
	DCIMLineStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LineId,
	IM_LineId,
	SessionId,
	Description,
	PolicyPayment
	FROM SQ_DCIMLineStage
),
ArchDCIMLineStage AS (
	INSERT INTO ArchDCIMLineStage
	(ExtractDate, SourceSystemId, AuditId, DCIMLineStageId, LineId, IM_LineId, SessionId, Description, PolicyPayment)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCIMLINESTAGEID, 
	LINEID, 
	IM_LINEID, 
	SESSIONID, 
	DESCRIPTION, 
	POLICYPAYMENT
	FROM EXP_Metadata
),