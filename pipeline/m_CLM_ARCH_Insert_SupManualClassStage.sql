WITH
SQ_SupManualClassStage AS (
	SELECT SupManualClassStage.SupManualClassStageId, SupManualClassStage.ExtractDate, SupManualClassStage.SourceSystemid, SupManualClassStage.ManualClassCode, SupManualClassStage.ManualClassDesc, SupManualClassStage.ModifiedDate, SupManualClassStage.ModifiedUserId 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupManualClassStage SupManualClassStage
	where SupManualClassStage.ModifiedDate>= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_ArchSupManualClassStage AS (
	SELECT
	SupManualClassStageId,
	ExtractDate,
	SourceSystemid,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID_OP,
	ManualClassCode,
	ManualClassDesc,
	ModifiedDate,
	ModifiedUserId
	FROM SQ_SupManualClassStage
),
ArchSupManualClassStage AS (
	INSERT INTO ArchSupManualClassStage
	(ExtractDate, SourceSystemId, AuditId, SupManualClassStageId, ManualClassCode, ManualClassDesc, ModifiedDate, ModifiedUserId)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	Audit_ID_OP AS AUDITID, 
	SUPMANUALCLASSSTAGEID, 
	MANUALCLASSCODE, 
	MANUALCLASSDESC, 
	MODIFIEDDATE, 
	MODIFIEDUSERID
	FROM EXP_ArchSupManualClassStage
),