WITH
SQ_SupTaskStatusStage AS (
	SELECT SupTaskStatusStageId, ExtractDate, SourceSystemId, SupTaskStatusId, CreatedDate, CreatedUserId, ModifiedDate, ModifiedUserId, Code, Description, SortOrder, CategoryName
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskStatusStage 
	WHERE CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}' 
	OR ModifiedDate > '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Collect AS (
	SELECT
	SupTaskStatusStageId,
	ExtractDate,
	SourceSystemId,
	SupTaskStatusId,
	CreatedDate,
	CreatedUserId,
	ModifiedDate,
	ModifiedUserId,
	Code,
	Description,
	SortOrder,
	CategoryName,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_SupTaskStatusStage
),
ArchSupTaskStatusStage AS (
	INSERT INTO ArchSupTaskStatusStage
	(ExtractDate, SourceSystemId, AuditId, SupTaskStatusId, CreatedDate, CreatedUserId, ModifiedDate, ModifiedUserId, Code, Description, SortOrder, CategoryName)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	SUPTASKSTATUSID, 
	CREATEDDATE, 
	CREATEDUSERID, 
	MODIFIEDDATE, 
	MODIFIEDUSERID, 
	CODE, 
	DESCRIPTION, 
	SORTORDER, 
	CATEGORYNAME
	FROM EXP_Collect
),