WITH
SQ_DCCFBuilderStaging AS (
	SELECT
		DCCFBuilderStagingId,
		ExtractDate,
		SourceSystemId,
		CF_RiskId,
		CF_BuilderId,
		SessionId,
		Id,
		Renovations,
		TheftOfBuildingMaterials,
		BuildingMaterialsSuppliesOfOthersPremium,
		Collapse,
		SubContractors,
		BuildingPremisesDescription
	FROM DCCFBuilderStaging
),
EXP_Metadata AS (
	SELECT
	DCCFBuilderStagingId,
	ExtractDate,
	SourceSystemId,
	CF_RiskId,
	CF_BuilderId,
	SessionId,
	Id,
	Renovations AS i_Renovations,
	-- *INF*: DECODE(i_Renovations, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_Renovations,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Renovations,
	TheftOfBuildingMaterials AS i_TheftOfBuildingMaterials,
	-- *INF*: DECODE(i_TheftOfBuildingMaterials, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TheftOfBuildingMaterials,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TheftOfBuildingMaterials,
	BuildingMaterialsSuppliesOfOthersPremium,
	Collapse,
	SubContractors,
	BuildingPremisesDescription,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFBuilderStaging
),
archDCCFBuilderStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFBuilderStaging
	(ExtractDate, SourceSystemId, AuditId, DCCFBuilderStagingId, CF_RiskId, CF_BuilderId, SessionId, Id, Renovations, TheftOfBuildingMaterials, BuildingMaterialsSuppliesOfOthersPremium, Collapse, SubContractors, BuildingPremisesDescription)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCFBUILDERSTAGINGID, 
	CF_RISKID, 
	CF_BUILDERID, 
	SESSIONID, 
	ID, 
	o_Renovations AS RENOVATIONS, 
	o_TheftOfBuildingMaterials AS THEFTOFBUILDINGMATERIALS, 
	BUILDINGMATERIALSSUPPLIESOFOTHERSPREMIUM, 
	COLLAPSE, 
	SUBCONTRACTORS, 
	BUILDINGPREMISESDESCRIPTION
	FROM EXP_Metadata
),