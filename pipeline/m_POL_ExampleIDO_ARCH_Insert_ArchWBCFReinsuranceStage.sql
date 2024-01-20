WITH
SQ_WBCFReinsuranceStage AS (
	SELECT
		WBCFReinsuranceStageId,
		ExtractDate,
		SourceSystemId,
		WBCLReinsuranceId,
		WBCFReinsuranceId,
		SessionId,
		BlanketReinsurance,
		ApplyToEntireSchedule,
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
		TotalPremium
	FROM WBCFReinsuranceStage
),
EXP_Metadata AS (
	SELECT
	WBCFReinsuranceStageId,
	ExtractDate,
	SourceSystemId,
	WBCLReinsuranceId,
	WBCFReinsuranceId,
	SessionId,
	BlanketReinsurance,
	-- *INF*: DECODE(BlanketReinsurance, 'T', 1, 'F', 0)
	DECODE(
	    BlanketReinsurance,
	    'T', 1,
	    'F', 0
	) AS o_BlanketReinsurance,
	ApplyToEntireSchedule,
	-- *INF*: DECODE(ApplyToEntireSchedule, 'T', 1, 'F', 0)
	DECODE(
	    ApplyToEntireSchedule,
	    'T', 1,
	    'F', 0
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WBCFReinsuranceStage
),
ArchWBCFReinsuranceStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCFReinsuranceStage
	(ExtractDate, SourceSystemId, AuditId, WBCFReinsuranceStageId, WBCLReinsuranceId, WBCFReinsuranceId, SessionId, BlanketReinsurance, ApplyToEntireSchedule, ScheduleDetails, Building, PersonalProperty, BusinessIncome, Other, EDP, OtherIMCoverages, OpenLot, GKLL, TotalTIV, GrossCedingLimit, CedingLimitWithDirectFacTreaty, WBNetReinsuranceLimit, DirectFacTreatyPremium, WBPremium, PropertyNetReinsurancePremium, InlandMarineNetReinsurancePremium, GarageNetReinsurancePremium, AdditionalComments, Underwriter, UWManager, UWDate, UWManagerDate, LocationAddress, CertificateReceived, Vehicles, SpecialConditionsInclude, SpecialConditionsExclude, TotalPremium)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	WBCFREINSURANCESTAGEID, 
	WBCLREINSURANCEID, 
	WBCFREINSURANCEID, 
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