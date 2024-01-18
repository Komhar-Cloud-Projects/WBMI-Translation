WITH
SQ_DC_CF_Risk AS (
	WITH cte_DCCFRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_BuildingId, 
	X.CF_RiskId, 
	X.SessionId, 
	X.Id, 
	X.RiskType, 
	X.RiskState, 
	X.Description, 
	X.SpecialClass, 
	X.HonoredRateEffectiveDate, 
	X.PropertyEffectiveDateKey 
	FROM
	DC_CF_Risk X
	inner join
	cte_DCCFRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_Risk
),
DCCFRiskStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFRiskStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFRiskStaging
	(CF_RiskId, SessionId, CF_BuildingId, Id, RiskType, RiskState, Description, SpecialClass, HonoredRateEffectiveDate, PropertyEffectiveDateKey, ExtractDate, SourceSystemId)
	SELECT 
	CF_RISKID, 
	SESSIONID, 
	CF_BUILDINGID, 
	ID, 
	RISKTYPE, 
	RISKSTATE, 
	DESCRIPTION, 
	SPECIALCLASS, 
	HONOREDRATEEFFECTIVEDATE, 
	PROPERTYEFFECTIVEDATEKEY, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),