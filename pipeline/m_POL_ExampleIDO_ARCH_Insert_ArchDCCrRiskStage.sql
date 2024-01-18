WITH
SQ_DcCrRiskStage AS (
	SELECT
		DcCrRiskStageId,
		LineId,
		CrOccupancyId,
		CrRiskId,
		SessionId,
		Id,
		CrOccupancyXmlId,
		Manufacturers,
		RiskState,
		ExtractDate,
		SourceSystemId
	FROM DcCrRiskStage
),
EXP_Metadata AS (
	SELECT
	DcCrRiskStageId,
	LineId,
	CrOccupancyId,
	CrRiskId,
	SessionId,
	Id,
	CrOccupancyXmlId,
	Manufacturers AS i_Manufacturers,
	-- *INF*: DECODE(i_Manufacturers, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_Manufacturers,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Manufacturers,
	RiskState,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DcCrRiskStage
),
ArchDCCrRiskStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCrRiskStage
	(DcCrRiskStageId, LineId, CrOccupancyId, CrRiskId, SessionId, Id, CrOccupancyXmlId, Manufacturers, RiskState, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCCRRISKSTAGEID, 
	LINEID, 
	CROCCUPANCYID, 
	CRRISKID, 
	SESSIONID, 
	ID, 
	CROCCUPANCYXMLID, 
	o_Manufacturers AS MANUFACTURERS, 
	RISKSTATE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),