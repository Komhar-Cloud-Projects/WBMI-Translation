WITH
SQ_WBCLPrintJobStage AS (
	SELECT
		WBCLPrintJobStageId,
		PolicyId,
		WBCLPrintJobId,
		SessionId,
		Manuscript,
		PrintJob,
		ExtractDate,
		SourceSystemId
	FROM WBCLPrintJobStage
),
EXP_Metadata AS (
	SELECT
	WBCLPrintJobStageId,
	PolicyId,
	WBCLPrintJobId,
	SessionId,
	Manuscript,
	PrintJob,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCLPrintJobStage
),
ArchWBCLPrintJobStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCLPrintJobStage
	(WBCLPrintJobStageId, PolicyId, WBCLPrintJobId, SessionId, Manuscript, PrintJob, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	WBCLPRINTJOBSTAGEID, 
	POLICYID, 
	WBCLPRINTJOBID, 
	SESSIONID, 
	MANUSCRIPT, 
	PRINTJOB, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),