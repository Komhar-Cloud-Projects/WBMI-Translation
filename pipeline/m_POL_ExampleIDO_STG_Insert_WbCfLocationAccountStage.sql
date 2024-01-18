WITH
SQ_WB_CF_LocationAccount AS (
	WITH cte_WBCFLocationAccount(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_LocationAccountId, 
	X.WB_CF_LocationAccountId, 
	X.SessionId, 
	X.PreferredPropertyCredit, 
	X.LargeValueRelativityCredit, 
	X.Width, 
	X.FoodContaminationAdvertisingExpenseLimitStoredValue, 
	X.FoodContaminationIndicatorStoredValue, 
	X.FoodContaminationLimitStoredValue, 
	X.FirstTimeOnBuildingScreen, 
	X.PreferredPropertyCreditFactor 
	FROM 
	DBO.WB_CF_LocationAccount x
	inner join cte_WBCFLocationAccount Y
	on X.SessionId = Y.SessionId
),
EXP_Metadata AS (
	SELECT
	WB_CL_LocationAccountId,
	WB_CF_LocationAccountId,
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CF_LocationAccount
),
WBCFLocationAccountStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.Shortcut_to_WBCFLocationAccountStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Shortcut_to_WBCFLocationAccountStage
	(ExtractDate, SourceSystemId, WBCLLocationAccountId, WBCFLocationAccountId, SessionId, PreferredPropertyCredit, LargeValueRelativityCredit, Width, FirstTimeOnBuildingScreen, PreferredPropertyCreditFactor, FoodContaminationAdvertisingExpenseLimitStoredValue, FoodContaminationIndicatorStoredValue, FoodContaminationLimitStoredValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_CL_LocationAccountId AS WBCLLOCATIONACCOUNTID, 
	WB_CF_LocationAccountId AS WBCFLOCATIONACCOUNTID, 
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