WITH
SQ_DCCRBuildingCoverageStaging AS (
	SELECT
		ExtractDate,
		SourceSystemId,
		CoverageId,
		CR_BuildingId,
		CR_BuildingCoverageId,
		SessionId,
		CoverageXmlId,
		CR_BuildingXmlId
	FROM DCCRBuildingCoverageStaging
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	CoverageId,
	CR_BuildingId,
	CR_BuildingCoverageId,
	SessionId,
	CoverageXmlId,
	CR_BuildingXmlId
	FROM SQ_DCCRBuildingCoverageStaging
),
archDCCRBuildingCoverageStaging AS (
	INSERT INTO archDCCRBuildingCoverageStaging
	(ExtractDate, SourceSystemId, AuditId, CoverageId, CR_BuildingId, CR_BuildingCoverageId, SessionId, CoverageXmlId, CR_BuildingXmlId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	COVERAGEID, 
	CR_BUILDINGID, 
	CR_BUILDINGCOVERAGEID, 
	SESSIONID, 
	COVERAGEXMLID, 
	CR_BUILDINGXMLID
	FROM EXP_Metadata
),