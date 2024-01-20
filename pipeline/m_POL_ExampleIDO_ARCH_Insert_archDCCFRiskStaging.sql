WITH
SQ_DCCFRiskStaging AS (
	SELECT
		CF_BuildingId,
		CF_RiskId,
		SessionId,
		Id,
		RiskType,
		RiskState,
		Description,
		SpecialClass,
		HonoredRateEffectiveDate,
		PropertyEffectiveDateKey,
		ExtractDate,
		SourceSystemId
	FROM DCCFRiskStaging
),
EXP_Metadata AS (
	SELECT
	CF_BuildingId,
	CF_RiskId,
	SessionId,
	Id,
	RiskType,
	RiskState,
	Description,
	SpecialClass,
	HonoredRateEffectiveDate,
	PropertyEffectiveDateKey,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFRiskStaging
),
archDCCFRiskStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFRiskStaging
	(CF_BuildingId, CF_RiskId, SessionId, Id, RiskType, RiskState, Description, SpecialClass, HonoredRateEffectiveDate, PropertyEffectiveDateKey, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CF_BUILDINGID, 
	CF_RISKID, 
	SESSIONID, 
	ID, 
	RISKTYPE, 
	RISKSTATE, 
	DESCRIPTION, 
	SPECIALCLASS, 
	HONOREDRATEEFFECTIVEDATE, 
	PROPERTYEFFECTIVEDATEKEY, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),