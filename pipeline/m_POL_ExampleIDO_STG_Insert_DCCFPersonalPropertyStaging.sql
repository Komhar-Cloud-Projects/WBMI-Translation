WITH
SQ_DC_CF_PersonalProperty AS (
	WITH cte_DCCFPersonalProperty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_RiskId, 
	X.CF_PersonalPropertyId, 
	X.SessionId, 
	X.Id, 
	X.PropertyType, 
	X.PropertyTypeDesc, 
	X.AgreedValue, 
	X.ControlledAtmosphereWarehouse, 
	X.DCGovernmentBuildingContents, 
	X.VaultsOrSafeSelect, 
	X.WholesaleOrStorage, 
	X.BaseLossCostSpecific 
	FROM
	DC_CF_PersonalProperty X
	inner join
	cte_DCCFPersonalProperty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_RiskId,
	CF_PersonalPropertyId,
	SessionId,
	Id,
	PropertyType,
	PropertyTypeDesc,
	AgreedValue AS i_AgreedValue,
	ControlledAtmosphereWarehouse AS i_ControlledAtmosphereWarehouse,
	DCGovernmentBuildingContents AS i_DCGovernmentBuildingContents,
	VaultsOrSafeSelect AS i_VaultsOrSafeSelect,
	WholesaleOrStorage AS i_WholesaleOrStorage,
	-- *INF*: DECODE(i_AgreedValue,'T',1,'F',0,NULL)
	DECODE(
	    i_AgreedValue,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AgreedValue,
	-- *INF*: DECODE(i_ControlledAtmosphereWarehouse,'T',1,'F',0,NULL)
	DECODE(
	    i_ControlledAtmosphereWarehouse,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ControlledAtmosphereWarehouse,
	-- *INF*: DECODE(i_DCGovernmentBuildingContents,'T',1,'F',0,NULL)
	DECODE(
	    i_DCGovernmentBuildingContents,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DCGovernmentBuildingContents,
	-- *INF*: DECODE(i_VaultsOrSafeSelect,'T',1,'F',0,NULL)
	DECODE(
	    i_VaultsOrSafeSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VaultsOrSafeSelect,
	-- *INF*: DECODE(i_WholesaleOrStorage,'T',1,'F',0,NULL)
	DECODE(
	    i_WholesaleOrStorage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WholesaleOrStorage,
	BaseLossCostSpecific,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_PersonalProperty
),
DCCFPersonalPropertyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFPersonalPropertyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFPersonalPropertyStaging
	(CF_PersonalPropertyId, SessionId, Id, PropertyType, PropertyTypeDesc, AgreedValue, ControlledAtmosphereWarehouse, DCGovernmentBuildingContents, VaultsOrSafeSelect, WholesaleOrStorage, BaseLossCostSpecific, ExtractDate, SourceSystemId, CF_RiskId)
	SELECT 
	CF_PERSONALPROPERTYID, 
	SESSIONID, 
	ID, 
	PROPERTYTYPE, 
	PROPERTYTYPEDESC, 
	o_AgreedValue AS AGREEDVALUE, 
	o_ControlledAtmosphereWarehouse AS CONTROLLEDATMOSPHEREWAREHOUSE, 
	o_DCGovernmentBuildingContents AS DCGOVERNMENTBUILDINGCONTENTS, 
	o_VaultsOrSafeSelect AS VAULTSORSAFESELECT, 
	o_WholesaleOrStorage AS WHOLESALEORSTORAGE, 
	BASELOSSCOSTSPECIFIC, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CF_RISKID
	FROM EXP_Metadata
),