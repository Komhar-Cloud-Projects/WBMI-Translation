WITH
SQ_SupTaskStatus AS (
	SELECT
		SupTaskStatusId,
		CreatedDate,
		CreatedUserId,
		ModifiedDate,
		ModifiedUserId,
		Code,
		Description,
		SortOrder,
		CategoryName
	FROM SupTaskStatus
),
EXP_Collect AS (
	SELECT
	SYSDATE AS ExtractDate,
	'EXCEED AND PMS' AS SourceSystemId,
	SupTaskStatusId,
	CreatedDate,
	CreatedUserId,
	ModifiedDate,
	ModifiedUserId,
	Code,
	Description,
	SortOrder,
	CategoryName
	FROM SQ_SupTaskStatus
),
SupTaskStatusStage AS (
	TRUNCATE TABLE SupTaskStatusStage;
	INSERT INTO SupTaskStatusStage
	(ExtractDate, SourceSystemId, SupTaskStatusId, CreatedDate, CreatedUserId, ModifiedDate, ModifiedUserId, Code, Description, SortOrder, CategoryName)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
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