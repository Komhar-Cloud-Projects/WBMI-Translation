WITH
SQ_DCCFPersonalPropertyStaging AS (
	SELECT
		CF_PersonalPropertyId,
		SessionId,
		Id,
		PropertyType,
		PropertyTypeDesc,
		AgreedValue,
		ControlledAtmosphereWarehouse,
		DCGovernmentBuildingContents,
		VaultsOrSafeSelect,
		WholesaleOrStorage,
		BaseLossCostSpecific,
		ExtractDate,
		SourceSystemId,
		CF_RiskId
	FROM DCCFPersonalPropertyStaging
),
EXP_Metadata AS (
	SELECT
	CF_PersonalPropertyId,
	SessionId,
	Id,
	PropertyType,
	PropertyTypeDesc,
	AgreedValue AS i_AgreedValue,
	-- *INF*: DECODE(i_AgreedValue, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_AgreedValue,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AgreedValue,
	ControlledAtmosphereWarehouse AS i_ControlledAtmosphereWarehouse,
	-- *INF*: DECODE(i_ControlledAtmosphereWarehouse, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ControlledAtmosphereWarehouse,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ControlledAtmosphereWarehouse,
	DCGovernmentBuildingContents AS i_DCGovernmentBuildingContents,
	-- *INF*: DECODE(i_DCGovernmentBuildingContents, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_DCGovernmentBuildingContents,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_DCGovernmentBuildingContents,
	VaultsOrSafeSelect AS i_VaultsOrSafeSelect,
	-- *INF*: DECODE(i_VaultsOrSafeSelect, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_VaultsOrSafeSelect,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_VaultsOrSafeSelect,
	WholesaleOrStorage AS i_WholesaleOrStorage,
	-- *INF*: DECODE(i_WholesaleOrStorage, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_WholesaleOrStorage,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_WholesaleOrStorage,
	BaseLossCostSpecific,
	ExtractDate,
	SourceSystemId,
	CF_RiskId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFPersonalPropertyStaging
),
archDCCFPersonalPropertyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFPersonalPropertyStaging
	(CF_RiskId, CF_PersonalPropertyId, SessionId, Id, PropertyType, PropertyTypeDesc, AgreedValue, ControlledAtmosphereWarehouse, DCGovernmentBuildingContents, VaultsOrSafeSelect, WholesaleOrStorage, BaseLossCostSpecific, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CF_RISKID, 
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),