WITH
SQ_WBCUPolicyStaging AS (
	SELECT
		WBCUPolicyStagingId,
		ExtractDate,
		SourceSystemId,
		WB_CL_PolicyId,
		WB_CU_PolicyId,
		SessionId,
		ReinsuranceLiabilityLimit,
		ReinsuranceLiabilityPremium,
		TaskFlagFormSelectedWB1351,
		TaskFlagFormSelectedWB1525UM,
		TaskFlagFormSelectedWB1533UM
	FROM WBCUPolicyStaging
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
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
	FROM SQ_WBCUPolicyStaging
),
archWBCUPolicyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBCUPolicyStaging
	(ExtractDate, SourceSystemId, AuditId, WB_CL_PolicyId, WB_CU_PolicyId, SessionId, ReinsuranceLiabilityLimit, ReinsuranceLiabilityPremium, TaskFlagFormSelectedWB1351, TaskFlagFormSelectedWB1525UM, TaskFlagFormSelectedWB1533UM)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
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