WITH
SQ_WB_CF_Line AS (
	WITH cte_WBCFLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_LineId, 
	X.WB_CF_LineId, 
	X.SessionId, 
	X.RerateIndicator, 
	X.RerateIndicatorChange, 
	X.RerateIndicatorWritten, 
	X.TerrorismForWorksheet, 
	X.ScheduleModCaption, 
	X.QuotedScheduledMod, 
	X.BlanketAgreedValue, 
	X.BlanketAgreedValueExpirationDate, 
	X.OverrideForRMFCalculation, 
	X.PolicyCoverage, 
	X.ApplyTransition, 
	X.MNFireSafetySurchargePremium 
	FROM
	WB_CF_Line X
	inner join
	cte_WBCFLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_LineId AS i_CF_LineId,
	WB_CF_LineId AS i_WB_CF_LineId,
	SessionId AS i_SessionId,
	RerateIndicator AS i_RerateIndicator,
	RerateIndicatorChange AS i_RerateIndicatorChange,
	RerateIndicatorWritten AS i_RerateIndicatorWritten,
	TerrorismForWorksheet AS i_TerrorismForWorksheet,
	ScheduleModCaption AS i_ScheduleModCaption,
	QuotedScheduledMod AS i_QuotedScheduledMod,
	BlanketAgreedValue AS i_BlanketAgreedValue,
	BlanketAgreedValueExpirationDate AS i_BlanketAgreedValueExpirationDate,
	OverrideForRMFCalculation AS i_OverrideForRMFCalculation,
	PolicyCoverage AS i_PolicyCoverage,
	ApplyTransition AS i_ApplyTransition,
	i_CF_LineId AS o_CF_LineId,
	i_WB_CF_LineId AS o_WB_CF_LineId,
	i_SessionId AS o_SessionId,
	-- *INF*: DECODE(i_RerateIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_RerateIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RerateIndicator,
	-- *INF*: DECODE(i_RerateIndicatorChange,'T',1,'F',0,NULL)
	DECODE(
	    i_RerateIndicatorChange,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RerateIndicatorChange,
	-- *INF*: DECODE(i_RerateIndicatorWritten,'T',1,'F',0,NULL)
	DECODE(
	    i_RerateIndicatorWritten,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RerateIndicatorWritten,
	i_TerrorismForWorksheet AS o_TerrorismForWorksheet,
	i_ScheduleModCaption AS o_ScheduleModCaption,
	i_QuotedScheduledMod AS o_QuotedScheduledMod,
	-- *INF*: DECODE(i_BlanketAgreedValue,'T',1,'F',0,NULL)
	DECODE(
	    i_BlanketAgreedValue,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BlanketAgreedValue,
	i_BlanketAgreedValueExpirationDate AS o_BlanketAgreedValueExpirationDate,
	i_OverrideForRMFCalculation AS o_OverrideForRMFCalculation,
	i_PolicyCoverage AS o_PolicyCoverage,
	-- *INF*: DECODE(i_ApplyTransition,'T',1,'F',0,NULL)
	DECODE(
	    i_ApplyTransition,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ApplyTransition,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	MNFireSafetySurchargePremium
	FROM SQ_WB_CF_Line
),
WbCfLineStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WbCfLineStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WbCfLineStage
	(ExtractDate, SourceSystemId, CFLineId, WBCFLineId, SessionId, RerateIndicator, RerateIndicatorChange, RerateIndicatorWritten, TerrorismForWorksheet, ScheduleModCaption, QuotedScheduledMod, PolicyCoverage, ApplyTransition, BlanketAgreedValue, BlanketAgreedValueExpirationDate, OverrideForRMFCalculation, MNFireSafetySurchargePremium)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_CF_LineId AS CFLINEID, 
	o_WB_CF_LineId AS WBCFLINEID, 
	o_SessionId AS SESSIONID, 
	o_RerateIndicator AS RERATEINDICATOR, 
	o_RerateIndicatorChange AS RERATEINDICATORCHANGE, 
	o_RerateIndicatorWritten AS RERATEINDICATORWRITTEN, 
	o_TerrorismForWorksheet AS TERRORISMFORWORKSHEET, 
	o_ScheduleModCaption AS SCHEDULEMODCAPTION, 
	o_QuotedScheduledMod AS QUOTEDSCHEDULEDMOD, 
	o_PolicyCoverage AS POLICYCOVERAGE, 
	o_ApplyTransition AS APPLYTRANSITION, 
	o_BlanketAgreedValue AS BLANKETAGREEDVALUE, 
	o_BlanketAgreedValueExpirationDate AS BLANKETAGREEDVALUEEXPIRATIONDATE, 
	o_OverrideForRMFCalculation AS OVERRIDEFORRMFCALCULATION, 
	MNFIRESAFETYSURCHARGEPREMIUM
	FROM EXP_Metadata
),