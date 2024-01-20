WITH
SQ_WBCLReinsuranceStage AS (
	SELECT
		WBCLReinsuranceStageId,
		ExtractDate,
		SourceSystemId,
		WBReinsuranceId,
		WBCLReinsuranceId,
		SessionId,
		PurchasedEachAccidentLimit,
		Include,
		Exclude,
		AddedCaption,
		SpecialCondition
	FROM WBCLReinsuranceStage
),
EXP_Metadata AS (
	SELECT
	WBCLReinsuranceStageId,
	ExtractDate,
	SourceSystemId,
	WBReinsuranceId,
	WBCLReinsuranceId,
	SessionId,
	PurchasedEachAccidentLimit,
	Include,
	Exclude,
	AddedCaption,
	SpecialCondition,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WBCLReinsuranceStage
),
ArchWBCLReinsuranceStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCLReinsuranceStage
	(ExtractDate, SourceSystemId, AuditId, WBReinsuranceId, WBCLReinsuranceId, SessionId, PurchasedEachAccidentLimit, Include, Exclude, AddedCaption, SpecialCondition)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	WBREINSURANCEID, 
	WBCLREINSURANCEID, 
	SESSIONID, 
	PURCHASEDEACHACCIDENTLIMIT, 
	INCLUDE, 
	EXCLUDE, 
	ADDEDCAPTION, 
	SPECIALCONDITION
	FROM EXP_Metadata
),