WITH
SQ_WB_IM_Reinsurance AS (
	WITH cte_WBIMReinsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_ReinsuranceId, 
	X.WB_IM_ReinsuranceId, 
	X.SessionId, 
	X.BlanketReinsurance, 
	X.ApplyToEntireSchedule, 
	X.ScheduleDetails, 
	X.GrossCedingLimit, 
	X.CedingLimitWithDirectFacTreaty, 
	X.WBNetReinsuranceLimit, 
	X.DirectFacTreatyPremium, 
	X.WBPremium, 
	X.Earthquake, 
	X.Flood, 
	X.InlandMarineNetReinsurancePremium, 
	X.AdditionalComments, 
	X.Underwriter, 
	X.UWDate, 
	X.UWManager, 
	X.UWManagerDate, 
	X.SpecialConditionsInclude, 
	X.SpecialConditionsExclude 
	FROM
	WB_IM_Reinsurance X
	inner join
	cte_WBIMReinsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_CL_ReinsuranceId,
	WB_IM_ReinsuranceId,
	SessionId,
	BlanketReinsurance,
	-- *INF*: DECODE(BlanketReinsurance, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BlanketReinsurance,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BlanketReinsurance,
	ApplyToEntireSchedule,
	-- *INF*: DECODE(ApplyToEntireSchedule, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ApplyToEntireSchedule,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ApplyToEntireSchedule,
	ScheduleDetails,
	GrossCedingLimit,
	CedingLimitWithDirectFacTreaty,
	WBNetReinsuranceLimit,
	DirectFacTreatyPremium,
	WBPremium,
	Earthquake,
	-- *INF*: DECODE(Earthquake, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Earthquake,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Earthquake,
	Flood,
	-- *INF*: DECODE(Flood, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Flood,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Flood,
	InlandMarineNetReinsurancePremium,
	AdditionalComments,
	Underwriter,
	UWDate,
	UWManager,
	UWManagerDate,
	SpecialConditionsInclude,
	SpecialConditionsExclude,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_IM_Reinsurance
),
WBIMReinsuranceStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBIMReinsuranceStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBIMReinsuranceStage
	(ExtractDate, SourceSystemId, WBCLReinsuranceId, WBIMReinsuranceId, SessionId, BlanketReinsurance, ApplyToEntireSchedule, ScheduleDetails, GrossCedingLimit, CedingLimitWithDirectFacTreaty, WBNetReinsuranceLimit, DirectFacTreatyPremium, WBPremium, Earthquake, Flood, InlandMarineNetReinsurancePremium, AdditionalComments, Underwriter, UWDate, UWManager, UWManagerDate, SpecialConditionsInclude, SpecialConditionsExclude)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_CL_ReinsuranceId AS WBCLREINSURANCEID, 
	WB_IM_ReinsuranceId AS WBIMREINSURANCEID, 
	SESSIONID, 
	o_BlanketReinsurance AS BLANKETREINSURANCE, 
	o_ApplyToEntireSchedule AS APPLYTOENTIRESCHEDULE, 
	SCHEDULEDETAILS, 
	GROSSCEDINGLIMIT, 
	CEDINGLIMITWITHDIRECTFACTREATY, 
	WBNETREINSURANCELIMIT, 
	DIRECTFACTREATYPREMIUM, 
	WBPREMIUM, 
	o_Earthquake AS EARTHQUAKE, 
	o_Flood AS FLOOD, 
	INLANDMARINENETREINSURANCEPREMIUM, 
	ADDITIONALCOMMENTS, 
	UNDERWRITER, 
	UWDATE, 
	UWMANAGER, 
	UWMANAGERDATE, 
	SPECIALCONDITIONSINCLUDE, 
	SPECIALCONDITIONSEXCLUDE
	FROM EXP_Metadata
),