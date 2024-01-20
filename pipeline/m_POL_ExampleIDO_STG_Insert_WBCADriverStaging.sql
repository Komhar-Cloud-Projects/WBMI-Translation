WITH
SQ_to_WB_CA_Driver AS (
	WITH cte_WBCADriver(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_DriverId, 
	X.WB_CA_DriverId, 
	X.SessionId, 
	X.DateOfBirth, 
	X.Name, 
	X.MiddleInitial, 
	X.LastName, 
	X.Gender, 
	X.MaritalStatus, 
	X.ExcludeDriver, 
	X.WatchDriver, 
	X.PermanentDriver, 
	X.SelectForMVR, 
	X.TaskFlagCAMVRViolationCategoryNotFound, 
	X.TaskFlagCAMVRViolationCategoryNotFoundEARS, 
	X.MVRDate, 
	X.MVRStatus 
	FROM
	WB_CA_Driver X 
	inner join
	cte_WBCADriver Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_handle AS (
	SELECT
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
	sysdate AS o_Exctracdate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemid,
	i_CA_DriverId AS o_CA_DriverId,
	i_WB_CA_DriverId AS o_WB_CA_DriverId,
	i_SessionId AS o_SessionId,
	i_DateOfBirth AS o_DateOfBirth,
	i_Name AS o_Name,
	i_MiddleInitial AS o_MiddleInitial,
	i_LastName AS o_LastName,
	i_Gender AS o_Gender,
	i_MaritalStatus AS o_MaritalStatus,
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
	i_MVRDate AS o_MVRDate,
	i_MVRStatus AS o_MVRStatus
	FROM SQ_to_WB_CA_Driver
),
WBCADriverStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCADriverStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCADriverStaging
	(ExtractDate, SourceSystemId, CA_DriverId, WB_CA_DriverId, SessionId, DateOfBirth, Name, MiddleInitial, LastName, Gender, MaritalStatus, ExcludeDriver, WatchDriver, PermanentDriver, SelectForMVR, TaskFlagCAMVRViolationCategoryNotFound, TaskFlagCAMVRViolationCategoryNotFoundEARS, MVRDate, MVRStatus)
	SELECT 
	o_Exctracdate AS EXTRACTDATE, 
	o_SourceSystemid AS SOURCESYSTEMID, 
	o_CA_DriverId AS CA_DRIVERID, 
	o_WB_CA_DriverId AS WB_CA_DRIVERID, 
	o_SessionId AS SESSIONID, 
	o_DateOfBirth AS DATEOFBIRTH, 
	o_Name AS NAME, 
	o_MiddleInitial AS MIDDLEINITIAL, 
	o_LastName AS LASTNAME, 
	o_Gender AS GENDER, 
	o_MaritalStatus AS MARITALSTATUS, 
	o_ExcludeDriver AS EXCLUDEDRIVER, 
	o_WatchDriver AS WATCHDRIVER, 
	o_PermanentDriver AS PERMANENTDRIVER, 
	o_SelectForMVR AS SELECTFORMVR, 
	o_TaskFlagCAMVRViolationCategoryNotFound AS TASKFLAGCAMVRVIOLATIONCATEGORYNOTFOUND, 
	o_TaskFlagCAMVRViolationCategoryNotFoundEARS AS TASKFLAGCAMVRVIOLATIONCATEGORYNOTFOUNDEARS, 
	o_MVRDate AS MVRDATE, 
	o_MVRStatus AS MVRSTATUS
	FROM EXP_handle
),