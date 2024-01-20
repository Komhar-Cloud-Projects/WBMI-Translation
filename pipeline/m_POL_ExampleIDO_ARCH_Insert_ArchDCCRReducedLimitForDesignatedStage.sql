WITH
SQ_DCCRReducedLimitForDesignatedStage AS (
	SELECT
		DCCRReducedLimitForDesignatedStageId,
		CREndorsementId,
		CRBuildingId,
		CRReducedLimitForDesignatedId,
		SessionId,
		Id,
		Deleted,
		CRBuildingXmlId,
		EndorsementReducedLimitForDesignatedNumberOfPremises,
		ExtractDate,
		SourceSystemId
	FROM DCCRReducedLimitForDesignatedStage
),
EXP_Metadata AS (
	SELECT
	DCCRReducedLimitForDesignatedStageId,
	CREndorsementId,
	CRBuildingId,
	CRReducedLimitForDesignatedId,
	SessionId,
	Id,
	Deleted,
	CRBuildingXmlId,
	EndorsementReducedLimitForDesignatedNumberOfPremises,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCRReducedLimitForDesignatedStage
),
ArchDCCRReducedLimitForDesignatedStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCRReducedLimitForDesignatedStage
	(DCCRReducedLimitForDesignatedStageId, CREndorsementId, CRBuildingId, CRReducedLimitForDesignatedId, SessionId, Id, Deleted, CRBuildingXmlId, EndorsementReducedLimitForDesignatedNumberOfPremises, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCCRREDUCEDLIMITFORDESIGNATEDSTAGEID, 
	CRENDORSEMENTID, 
	CRBUILDINGID, 
	CRREDUCEDLIMITFORDESIGNATEDID, 
	SESSIONID, 
	ID, 
	DELETED, 
	CRBUILDINGXMLID, 
	ENDORSEMENTREDUCEDLIMITFORDESIGNATEDNUMBEROFPREMISES, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),