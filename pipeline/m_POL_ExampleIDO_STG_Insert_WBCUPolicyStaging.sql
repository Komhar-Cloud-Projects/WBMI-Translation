WITH
SQ_WB_CU_Policy AS (
	WITH cte_WBCUPolicy(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_PolicyId, 
	X.WB_CU_PolicyId, 
	X.SessionId, 
	X.ReinsuranceLiabilityLimit, 
	X.ReinsuranceLiabilityPremium, 
	X.TaskFlagFormSelectedWB1351, 
	X.TaskFlagFormSelectedWB1525UM, 
	X.TaskFlagFormSelectedWB1533UM 
	FROM
	WB_CU_Policy X
	inner join
	cte_WBCUPolicy Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	WB_CL_PolicyId,
	WB_CU_PolicyId,
	SessionId,
	ReinsuranceLiabilityLimit,
	ReinsuranceLiabilityPremium,
	TaskFlagFormSelectedWB1351,
	-- *INF*: DECODE(TaskFlagFormSelectedWB1351,'T',1,'F',0,NULL)
	DECODE(
	    TaskFlagFormSelectedWB1351,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS TaskFlagFormSelectedWB1351_out,
	TaskFlagFormSelectedWB1525UM,
	-- *INF*: DECODE(TaskFlagFormSelectedWB1525UM,'T',1,'F',0,NULL)
	DECODE(
	    TaskFlagFormSelectedWB1525UM,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS TaskFlagFormSelectedWB1525UM_out,
	TaskFlagFormSelectedWB1533UM,
	-- *INF*: DECODE(TaskFlagFormSelectedWB1533UM,'T',1,'F',0,NULL)
	DECODE(
	    TaskFlagFormSelectedWB1533UM,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS TaskFlagFormSelectedWB1533UM_out
	FROM SQ_WB_CU_Policy
),
WBCUPolicyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCUPolicyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCUPolicyStaging
	(ExtractDate, SourceSystemId, WB_CL_PolicyId, WB_CU_PolicyId, SessionId, ReinsuranceLiabilityLimit, ReinsuranceLiabilityPremium, TaskFlagFormSelectedWB1351, TaskFlagFormSelectedWB1525UM, TaskFlagFormSelectedWB1533UM)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_CL_POLICYID, 
	WB_CU_POLICYID, 
	SESSIONID, 
	REINSURANCELIABILITYLIMIT, 
	REINSURANCELIABILITYPREMIUM, 
	TaskFlagFormSelectedWB1351_out AS TASKFLAGFORMSELECTEDWB1351, 
	TaskFlagFormSelectedWB1525UM_out AS TASKFLAGFORMSELECTEDWB1525UM, 
	TaskFlagFormSelectedWB1533UM_out AS TASKFLAGFORMSELECTEDWB1533UM
	FROM EXP_Metadata
),