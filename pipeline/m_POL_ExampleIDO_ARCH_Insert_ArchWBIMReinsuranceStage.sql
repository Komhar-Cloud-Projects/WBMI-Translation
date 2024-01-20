WITH
SQ_WBIMReinsuranceStage AS (
	SELECT
		WBIMReinsuranceStageId,
		ExtractDate,
		SourceSystemId,
		WBCLReinsuranceId,
		WBIMReinsuranceId,
		SessionId,
		BlanketReinsurance,
		ApplyToEntireSchedule,
		ScheduleDetails,
		GrossCedingLimit,
		CedingLimitWithDirectFacTreaty,
		WBNetReinsuranceLimit,
		DirectFacTreatyPremium,
		WBPremium,
		Earthquake,
		Flood,
		InlandMarineNetReinsurancePremium,
		AdditionalComments,
		Underwriter,
		UWDate,
		UWManager,
		UWManagerDate,
		SpecialConditionsInclude,
		SpecialConditionsExclude
	FROM WBIMReinsuranceStage
),
EXP_Metadata AS (
	SELECT
	WBIMReinsuranceStageId,
	ExtractDate,
	SourceSystemId,
	WBCLReinsuranceId,
	WBIMReinsuranceId,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WBIMReinsuranceStage
),
ArchWBIMReinsuranceStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBIMReinsuranceStage
	(ExtractDate, SourceSystemId, AuditId, WBCLReinsuranceId, WBIMReinsuranceId, SessionId, BlanketReinsurance, ApplyToEntireSchedule, ScheduleDetails, GrossCedingLimit, CedingLimitWithDirectFacTreaty, WBNetReinsuranceLimit, DirectFacTreatyPremium, WBPremium, Earthquake, Flood, InlandMarineNetReinsurancePremium, AdditionalComments, Underwriter, UWDate, UWManager, UWManagerDate, SpecialConditionsInclude, SpecialConditionsExclude)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	WBCLREINSURANCEID, 
	WBIMREINSURANCEID, 
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