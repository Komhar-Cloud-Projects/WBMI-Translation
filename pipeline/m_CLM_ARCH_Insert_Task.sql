WITH
SQ_TaskStage AS (
	select TaskStageId, ExtractDate, SourceSystemId, TaskId, CreatedDate, CreatedUserId, CreatedUserName, ModifiedDate, ModifiedUserId, ModifiedUserName, AssignedUserId, DueDate, Title, SupTaskTypeId, SupTaskStatusId, SupTaskStatusReasonId, PercentageComplete, Description, ExtendedData, AssignedUserName, Rush, QueueId, ArrivalDate, Viewed, ClaimantName, ClaimLossDate, ClaimRepName, DocumentScanDate, DocumentBatchName, DocumentScanUser, ClaimId, MultipleWorkItems 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage with (nolock) 
	where CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}' 
	or ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXPTRANS AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	TaskStageId,
	TaskId,
	CreatedDate,
	CreatedUserId,
	CreatedUserName,
	ModifiedDate,
	ModifiedUserId,
	ModifiedUserName,
	AssignedUserId,
	DueDate,
	Title,
	SupTaskTypeId,
	SupTaskStatusId,
	SupTaskStatusReasonId,
	PercentageComplete,
	Description,
	ExtendedData,
	AssignedUserName,
	Rush,
	QueueId,
	ArrivalDate,
	Viewed,
	ClaimantName,
	ClaimLossDate,
	ClaimRepName,
	DocumentScanDate,
	DocumentBatchName,
	DocumentScanUser,
	ClaimId,
	MultipleWorkItems,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_TaskStage
),
ArchTaskStage AS (
	INSERT INTO ArchTaskStage
	(ExtractDate, SourceSystemId, AuditId, TaskStageId, TaskId, CreatedDate, CreatedUserId, CreatedUserName, ModifiedDate, ModifiedUserId, ModifiedUserName, AssignedUserId, DueDate, Title, SupTaskTypeId, SupTaskStatusId, SupTaskStatusReasonId, PercentageComplete, Description, ExtendedData, AssignedUserName, Rush, QueueId, ArrivalDate, Viewed, ClaimantName, ClaimLossDate, ClaimRepName, DocumentScanDate, DocumentBatchName, DocumentScanUser, ClaimId, MultipleWorkItems)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	TASKSTAGEID, 
	TASKID, 
	CREATEDDATE, 
	CREATEDUSERID, 
	CREATEDUSERNAME, 
	MODIFIEDDATE, 
	MODIFIEDUSERID, 
	MODIFIEDUSERNAME, 
	ASSIGNEDUSERID, 
	DUEDATE, 
	TITLE, 
	SUPTASKTYPEID, 
	SUPTASKSTATUSID, 
	SUPTASKSTATUSREASONID, 
	PERCENTAGECOMPLETE, 
	DESCRIPTION, 
	EXTENDEDDATA, 
	ASSIGNEDUSERNAME, 
	RUSH, 
	QUEUEID, 
	ARRIVALDATE, 
	VIEWED, 
	CLAIMANTNAME, 
	CLAIMLOSSDATE, 
	CLAIMREPNAME, 
	DOCUMENTSCANDATE, 
	DOCUMENTBATCHNAME, 
	DOCUMENTSCANUSER, 
	CLAIMID, 
	MULTIPLEWORKITEMS
	FROM EXPTRANS
),