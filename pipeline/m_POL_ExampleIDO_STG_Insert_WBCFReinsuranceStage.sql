WITH
SQ_WB_CF_Reinsurance AS (
	WITH cte_WBCFReinsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_ReinsuranceId, 
	X.WB_CF_ReinsuranceId, 
	X.SessionId, 
	X.BlanketReinsurance, 
	X.ApplyToEntireSchedule, 
	X.ScheduleDetails, 
	X.Building, 
	X.PersonalProperty, 
	X.BusinessIncome, 
	X.Other, 
	X.EDP, 
	X.OtherIMCoverages, 
	X.OpenLot, 
	X.GKLL, 
	X.TotalTIV, 
	X.GrossCedingLimit, 
	X.CedingLimitWithDirectFacTreaty, 
	X.WBNetReinsuranceLimit, 
	X.DirectFacTreatyPremium, 
	X.WBPremium, 
	X.PropertyNetReinsurancePremium, 
	X.InlandMarineNetReinsurancePremium, 
	X.GarageNetReinsurancePremium, 
	X.AdditionalComments, 
	X.Underwriter, 
	X.UWManager, 
	X.UWDate, 
	X.UWManagerDate, 
	X.LocationAddress, 
	X.CertificateReceived, 
	X.Vehicles, 
	X.SpecialConditionsInclude, 
	X.SpecialConditionsExclude, 
	X.TotalPremium 
	FROM
	WB_CF_Reinsurance X
	inner join
	cte_WBCFReinsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_CL_ReinsuranceId,
	WB_CF_ReinsuranceId,
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
	Building,
	PersonalProperty,
	BusinessIncome,
	Other,
	EDP,
	OtherIMCoverages,
	OpenLot,
	GKLL,
	TotalTIV,
	GrossCedingLimit,
	CedingLimitWithDirectFacTreaty,
	WBNetReinsuranceLimit,
	DirectFacTreatyPremium,
	WBPremium,
	PropertyNetReinsurancePremium,
	InlandMarineNetReinsurancePremium,
	GarageNetReinsurancePremium,
	AdditionalComments,
	Underwriter,
	UWManager,
	UWDate,
	UWManagerDate,
	LocationAddress,
	CertificateReceived,
	Vehicles,
	SpecialConditionsInclude,
	SpecialConditionsExclude,
	TotalPremium,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CF_Reinsurance
),
WBCFReinsuranceStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFReinsuranceStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFReinsuranceStage
	(ExtractDate, SourceSystemId, WBCLReinsuranceId, WBCFReinsuranceId, SessionId, BlanketReinsurance, ApplyToEntireSchedule, ScheduleDetails, Building, PersonalProperty, BusinessIncome, Other, EDP, OtherIMCoverages, OpenLot, GKLL, TotalTIV, GrossCedingLimit, CedingLimitWithDirectFacTreaty, WBNetReinsuranceLimit, DirectFacTreatyPremium, WBPremium, PropertyNetReinsurancePremium, InlandMarineNetReinsurancePremium, GarageNetReinsurancePremium, AdditionalComments, Underwriter, UWManager, UWDate, UWManagerDate, LocationAddress, CertificateReceived, Vehicles, SpecialConditionsInclude, SpecialConditionsExclude, TotalPremium)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_CL_ReinsuranceId AS WBCLREINSURANCEID, 
	WB_CF_ReinsuranceId AS WBCFREINSURANCEID, 
	SESSIONID, 
	o_BlanketReinsurance AS BLANKETREINSURANCE, 
	o_ApplyToEntireSchedule AS APPLYTOENTIRESCHEDULE, 
	SCHEDULEDETAILS, 
	BUILDING, 
	PERSONALPROPERTY, 
	BUSINESSINCOME, 
	OTHER, 
	EDP, 
	OTHERIMCOVERAGES, 
	OPENLOT, 
	GKLL, 
	TOTALTIV, 
	GROSSCEDINGLIMIT, 
	CEDINGLIMITWITHDIRECTFACTREATY, 
	WBNETREINSURANCELIMIT, 
	DIRECTFACTREATYPREMIUM, 
	WBPREMIUM, 
	PROPERTYNETREINSURANCEPREMIUM, 
	INLANDMARINENETREINSURANCEPREMIUM, 
	GARAGENETREINSURANCEPREMIUM, 
	ADDITIONALCOMMENTS, 
	UNDERWRITER, 
	UWMANAGER, 
	UWDATE, 
	UWMANAGERDATE, 
	LOCATIONADDRESS, 
	CERTIFICATERECEIVED, 
	VEHICLES, 
	SPECIALCONDITIONSINCLUDE, 
	SPECIALCONDITIONSEXCLUDE, 
	TOTALPREMIUM
	FROM EXP_Metadata
),