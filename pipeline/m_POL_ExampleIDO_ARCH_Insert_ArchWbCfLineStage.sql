WITH
SQ_WbCfLineStage AS (
	SELECT
		WBCFLineStageId AS WbCfLineStageID,
		ExtractDate,
		SourceSystemId,
		CFLineId AS CfLineId,
		WBCFLineId AS WbCfLineId,
		SessionId,
		RerateIndicator,
		RerateIndicatorChange,
		RerateIndicatorWritten,
		TerrorismForWorksheet,
		ScheduleModCaption,
		QuotedScheduledMod,
		PolicyCoverage,
		ApplyTransition,
		BlanketAgreedValue,
		BlanketAgreedValueExpirationDate,
		OverrideForRMFCalculation,
		MNFireSafetySurchargePremium
	FROM WbCfLineStage
),
EXP_Metadata AS (
	SELECT
	WbCfLineStageID AS i_WbCfLineStageID,
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	CfLineId AS i_CfLineId,
	WbCfLineId AS i_WbCfLineId,
	SessionId AS i_SessionId,
	RerateIndicator AS i_RerateIndicator,
	RerateIndicatorChange AS i_RerateIndicatorChange,
	RerateIndicatorWritten AS i_RerateIndicatorWritten,
	TerrorismForWorksheet AS i_TerrorismForWorksheet,
	ScheduleModCaption AS i_ScheduleModCaption,
	QuotedScheduledMod AS i_QuotedScheduledMod,
	PolicyCoverage AS i_PolicyCoverage,
	ApplyTransition AS i_ApplyTransition,
	BlanketAgreedValue AS i_BlanketAgreedValue,
	BlanketAgreedValueExpirationDate AS i_BlanketAgreedValueExpirationDate,
	OverrideForRMFCalculation AS i_OverrideForRMFCalculation,
	MNFireSafetySurchargePremium AS i_MNFireSafetySurchargePremium,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_WbCfLineStageID AS o_WbCfLineStageID,
	i_CfLineId AS o_CfLineId,
	i_WbCfLineId AS o_WbCfLineId,
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
	i_PolicyCoverage AS o_PolicyCoverage,
	-- *INF*: DECODE(i_ApplyTransition,'T',1,'F',0,NULL)
	DECODE(
	    i_ApplyTransition,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ApplyTransition,
	-- *INF*: DECODE(i_BlanketAgreedValue,'T',1,'F',0,NULL)
	DECODE(
	    i_BlanketAgreedValue,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BlanketAgreedValue,
	i_BlanketAgreedValueExpirationDate AS o_BlanketAgreedValueExpirationDate,
	i_OverrideForRMFCalculation AS o_OverrideForRMFCalculation,
	i_MNFireSafetySurchargePremium AS o_MNFireSafetySurchargePremium
	FROM SQ_WbCfLineStage
),
ArchWbCfLineStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWbCfLineStage
	(ExtractDate, SourceSystemId, AuditId, WBCFLineStageId, CFLineId, WBCFLineId, SessionId, RerateIndicator, RerateIndicatorChange, RerateIndicatorWritten, TerrorismForWorksheet, ScheduleModCaption, QuotedScheduledMod, PolicyCoverage, ApplyTransition, BlanketAgreedValue, BlanketAgreedValueExpirationDate, OverrideForRMFCalculation, MNFireSafetySurchargePremium)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	o_WbCfLineStageID AS WBCFLINESTAGEID, 
	o_CfLineId AS CFLINEID, 
	o_WbCfLineId AS WBCFLINEID, 
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
	o_MNFireSafetySurchargePremium AS MNFIRESAFETYSURCHARGEPREMIUM
	FROM EXP_Metadata
),