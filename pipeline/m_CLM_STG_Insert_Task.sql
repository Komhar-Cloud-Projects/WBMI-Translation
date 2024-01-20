WITH
SQ_Task AS (
	SELECT T.TaskId, T.CreatedDate, T.CreatedUserId, T.CreatedUserName, T.ModifiedDate, T.ModifiedUserId, T.ModifiedUserName, T.AssignedUserId, T.DueDate, T.Title, T.SupTaskTypeId, T.SupTaskStatusId, T.SupTaskStatusReasonId, T.PercentageComplete, T.Description, T.ExtendedData, T.AssignedUserName, T.Rush, T.QueueId, T.ArrivalDate, T.Viewed, T.ClaimantName, T.ClaimLossDate, T.ClaimRepName, T.DocumentScanDate, T.DocumentBatchName, T.DocumentScanUser, T.ClaimId, T.MultipleWorkItems 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Task T
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskEntity TE on T.TaskId = TE.TaskId and TE.EntityType = 'Claim'
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskType STT on T.SupTaskTypeId = STT.SupTaskTypeId and STT.Description = 'Diary Task'
),
EXPTRANS AS (
	SELECT
	SYSDATE AS ExtractDate,
	-- *INF*: IIF(LENGTH(RTRIM(ClaimId)) = 20,
	-- 'EXCEED',
	-- 'PMS')
	IFF(LENGTH(RTRIM(ClaimId)) = 20, 'EXCEED', 'PMS') AS v_SourceSystemId,
	v_SourceSystemId AS SourceSystemId,
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
	MultipleWorkItems
	FROM SQ_Task
),
TaskStage AS (
	TRUNCATE TABLE TaskStage;
	INSERT INTO TaskStage
	(ExtractDate, SourceSystemId, TaskId, CreatedDate, CreatedUserId, CreatedUserName, ModifiedDate, ModifiedUserId, ModifiedUserName, AssignedUserId, DueDate, Title, SupTaskTypeId, SupTaskStatusId, SupTaskStatusReasonId, PercentageComplete, Description, ExtendedData, AssignedUserName, Rush, QueueId, ArrivalDate, Viewed, ClaimantName, ClaimLossDate, ClaimRepName, DocumentScanDate, DocumentBatchName, DocumentScanUser, ClaimId, MultipleWorkItems)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
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