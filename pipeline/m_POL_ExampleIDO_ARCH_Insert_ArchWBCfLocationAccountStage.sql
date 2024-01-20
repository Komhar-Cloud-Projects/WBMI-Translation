WITH
SQ_WBCFLocationAccountStage AS (
	SELECT
		WBCFLocationAccountStageId,
		ExtractDate,
		SourceSystemId,
		WBCLLocationAccountId,
		WBCFLocationAccountId,
		SessionId,
		PreferredPropertyCredit,
		LargeValueRelativityCredit,
		Width,
		FoodContaminationAdvertisingExpenseLimitStoredValue,
		FoodContaminationIndicatorStoredValue,
		FoodContaminationLimitStoredValue,
		FirstTimeOnBuildingScreen,
		PreferredPropertyCreditFactor
	FROM WBCFLocationAccountStage
),
EXP_Metadata AS (
	SELECT
	WBCFLocationAccountStageId,
	ExtractDate,
	SourceSystemId,
	WBCLLocationAccountId,
	WBCFLocationAccountId,
	SessionId,
	PreferredPropertyCredit AS i_PreferredPropertyCredit,
	-- *INF*: DECODE(i_PreferredPropertyCredit,
	-- 'T',
	-- '1',
	-- 'F',
	-- '0',
	-- NULL
	-- )
	DECODE(
	    i_PreferredPropertyCredit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_PreferredPropertyCredit,
	LargeValueRelativityCredit AS i_LargeValueRelativityCredit,
	-- *INF*: DECODE(i_LargeValueRelativityCredit,
	-- 'T',
	-- '1',
	-- 'F',
	-- '0',
	-- NULL
	-- )
	DECODE(
	    i_LargeValueRelativityCredit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_LargeValueRelativityCredit,
	Width,
	FoodContaminationAdvertisingExpenseLimitStoredValue,
	FoodContaminationIndicatorStoredValue,
	FoodContaminationLimitStoredValue,
	FirstTimeOnBuildingScreen AS i_FirstTimeOnBuildingScreen,
	-- *INF*: DECODE(i_FirstTimeOnBuildingScreen,
	-- 'T',
	-- '1',
	-- 'F',
	-- '0',
	-- NULL
	-- )
	DECODE(
	    i_FirstTimeOnBuildingScreen,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_FirstTimeOnBuildingScreen,
	PreferredPropertyCreditFactor,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCFLocationAccountStage
),
ArchWBCFLocationAccountStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Shortcut_to_ArchWBCFLocationAccountStage
	(ExtractDate, SourceSystemId, AuditId, WBCFLocationAccountStageId, WBCLLocationAccountId, WBCFLocationAccountId, SessionId, PreferredPropertyCredit, LargeValueRelativityCredit, Width, FirstTimeOnBuildingScreen, PreferredPropertyCreditFactor, FoodContaminationAdvertisingExpenseLimitStoredValue, FoodContaminationIndicatorStoredValue, FoodContaminationLimitStoredValue)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCFLOCATIONACCOUNTSTAGEID, 
	WBCLLOCATIONACCOUNTID, 
	WBCFLOCATIONACCOUNTID, 
	SESSIONID, 
	o_PreferredPropertyCredit AS PREFERREDPROPERTYCREDIT, 
	o_LargeValueRelativityCredit AS LARGEVALUERELATIVITYCREDIT, 
	WIDTH, 
	o_FirstTimeOnBuildingScreen AS FIRSTTIMEONBUILDINGSCREEN, 
	PREFERREDPROPERTYCREDITFACTOR, 
	FOODCONTAMINATIONADVERTISINGEXPENSELIMITSTOREDVALUE, 
	FOODCONTAMINATIONINDICATORSTOREDVALUE, 
	FOODCONTAMINATIONLIMITSTOREDVALUE
	FROM EXP_Metadata
),