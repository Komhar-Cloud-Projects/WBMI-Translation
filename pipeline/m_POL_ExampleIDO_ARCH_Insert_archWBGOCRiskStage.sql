WITH
SQ_WBGOCRiskStage AS (
	SELECT
		WBGOCRiskStageId,
		LineId,
		WBGOCRiskId,
		SessionId,
		LocationId,
		HoleInOneDescription,
		ExtractDate,
		SourceSystemId
	FROM WBGOCRiskStage1
),
EXP_Metadata AS (
	SELECT
	WBGOCRiskStageId,
	LineId,
	WBGOCRiskId,
	SessionId,
	LocationId,
	HoleInOneDescription,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBGOCRiskStage
),
ArchWBGOCRiskStage AS (
	INSERT INTO ArchWBGOCRiskStage
	(ExtractDate, SourceSystemId, AuditId, WBGOCRiskStageId, LineId, WBGOCRiskId, SessionId, LocationId, HoleInOneDescription)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBGOCRISKSTAGEID, 
	LINEID, 
	WBGOCRISKID, 
	SESSIONID, 
	LOCATIONID, 
	HOLEINONEDESCRIPTION
	FROM EXP_Metadata
),