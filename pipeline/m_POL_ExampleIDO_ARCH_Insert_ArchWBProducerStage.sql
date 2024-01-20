WITH
SQ_WBProducerStage AS (
	SELECT
		WBProducerStageID,
		PolicyId,
		WbProducerId,
		SessionId,
		Email,
		Name,
		ExtractDate,
		SourceSystemId
	FROM WBProducerStage
),
EXP_Metadata AS (
	SELECT
	WBProducerStageID,
	PolicyId,
	WbProducerId,
	SessionId,
	Email,
	Name,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBProducerStage
),
ArchWBProducerStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBProducerStage
	(WBProducerStageID, PolicyId, WbProducerId, SessionId, Email, Name, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	WBPRODUCERSTAGEID, 
	POLICYID, 
	WBPRODUCERID, 
	SESSIONID, 
	EMAIL, 
	NAME, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),