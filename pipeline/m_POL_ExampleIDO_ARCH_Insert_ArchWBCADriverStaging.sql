WITH
SQ_WBCADriverStaging AS (
	SELECT
		WBCADriverStagingId,
		ExtractDate,
		SourceSystemId,
		CA_DriverId,
		WB_CA_DriverId,
		SessionId,
		DateOfBirth,
		Name,
		MiddleInitial,
		LastName,
		Gender,
		MaritalStatus,
		ExcludeDriver,
		WatchDriver,
		PermanentDriver,
		SelectForMVR,
		TaskFlagCAMVRViolationCategoryNotFound,
		TaskFlagCAMVRViolationCategoryNotFoundEARS,
		MVRDate,
		MVRStatus
	FROM WBCADriverStaging
),
EXP_handle AS (
	SELECT
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	CA_DriverId AS i_CA_DriverId,
	WB_CA_DriverId AS i_WB_CA_DriverId,
	SessionId AS i_SessionId,
	DateOfBirth AS i_DateOfBirth,
	Name AS i_Name,
	MiddleInitial AS i_MiddleInitial,
	LastName AS i_LastName,
	Gender AS i_Gender,
	MaritalStatus AS i_MaritalStatus,
	ExcludeDriver AS i_ExcludeDriver,
	WatchDriver AS i_WatchDriver,
	PermanentDriver AS i_PermanentDriver,
	SelectForMVR AS i_SelectForMVR,
	TaskFlagCAMVRViolationCategoryNotFound AS i_TaskFlagCAMVRViolationCategoryNotFound,
	TaskFlagCAMVRViolationCategoryNotFoundEARS AS i_TaskFlagCAMVRViolationCategoryNotFoundEARS,
	MVRDate AS i_MVRDate,
	MVRStatus AS i_MVRStatus,
	i_ExtractDate AS o_Exctracdate,
	i_SourceSystemId AS o_SourceSystemid,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_CA_DriverId AS o_CA_DriverId,
	i_WB_CA_DriverId AS o_WB_CA_DriverId,
	i_SessionId AS o_SessionId,
	i_ExcludeDriver AS o_ExcludeDriver,
	-- *INF*: decode(i_WatchDriver,'T',1,'F',0,NULL)
	decode(
	    i_WatchDriver,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WatchDriver,
	-- *INF*: decode(i_PermanentDriver,'T',1,'F',0,NULL)
	decode(
	    i_PermanentDriver,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PermanentDriver,
	-- *INF*: decode(i_SelectForMVR,'T',1,'F',0,NULL)
	decode(
	    i_SelectForMVR,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SelectForMVR,
	i_MVRDate AS o_MVRDate,
	i_MVRStatus AS o_MVRStatus,
	-- *INF*: decode(i_TaskFlagCAMVRViolationCategoryNotFound,'T',1,'F',0,NULL)
	decode(
	    i_TaskFlagCAMVRViolationCategoryNotFound,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCAMVRViolationCategoryNotFound,
	-- *INF*: DECODE(i_TaskFlagCAMVRViolationCategoryNotFoundEARS,'T',1,'F',0,NULL)
	DECODE(
	    i_TaskFlagCAMVRViolationCategoryNotFoundEARS,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TaskFlagCAMVRViolationCategoryNotFoundEARS,
	i_DateOfBirth AS o_DateOfBirth,
	i_Name AS o_Name,
	i_MiddleInitial AS o_MiddleInitial,
	i_LastName AS o_LastName,
	i_Gender AS o_Gender,
	i_MaritalStatus AS o_MaritalStatus
	FROM SQ_WBCADriverStaging
),
ArchWBCADriverStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCADriverStaging
	(ExtractDate, SourceSystemId, AuditId, CA_DriverId, WB_CA_DriverId, SessionId, ExcludeDriver, WatchDriver, PermanentDriver, DriverUnderwritingInformationSelectForMVR, MVRDate, MVRStatus, SelectForMVR, TaskFlagCAMVRViolationCategoryNotFound, TaskFlagCAMVRViolationCategoryNotFoundEARS, DateOfBirth, Name, MiddleInitial, LastName, Gender, MaritalStatus)
	SELECT 
	o_Exctracdate AS EXTRACTDATE, 
	o_SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	o_CA_DriverId AS CA_DRIVERID, 
	o_WB_CA_DriverId AS WB_CA_DRIVERID, 
	o_SessionId AS SESSIONID, 
	o_ExcludeDriver AS EXCLUDEDRIVER, 
	o_WatchDriver AS WATCHDRIVER, 
	o_PermanentDriver AS PERMANENTDRIVER, 
	o_SelectForMVR AS DRIVERUNDERWRITINGINFORMATIONSELECTFORMVR, 
	o_MVRDate AS MVRDATE, 
	o_MVRStatus AS MVRSTATUS, 
	o_SelectForMVR AS SELECTFORMVR, 
	o_TaskFlagCAMVRViolationCategoryNotFound AS TASKFLAGCAMVRVIOLATIONCATEGORYNOTFOUND, 
	o_TaskFlagCAMVRViolationCategoryNotFoundEARS AS TASKFLAGCAMVRVIOLATIONCATEGORYNOTFOUNDEARS, 
	o_DateOfBirth AS DATEOFBIRTH, 
	o_Name AS NAME, 
	o_MiddleInitial AS MIDDLEINITIAL, 
	o_LastName AS LASTNAME, 
	o_Gender AS GENDER, 
	o_MaritalStatus AS MARITALSTATUS
	FROM EXP_handle
),