WITH
SQ_WBCFCoverageUnmannedAircraftStage AS (
	SELECT
		WBCFCoverageUnmannedAircraftStageId,
		ExtractDate,
		SourceSystemid,
		CoverageId,
		WB_CF_CoverageUnmannedAircraftId,
		SessionId,
		BusinessInterruption,
		NewlyAcquiredProperty
	FROM WBCFCoverageUnmannedAircraftStage
),
EXP_MetaData AS (
	SELECT
	WBCFCoverageUnmannedAircraftStageId,
	ExtractDate,
	SourceSystemid,
	CoverageId,
	WB_CF_CoverageUnmannedAircraftId,
	SessionId,
	BusinessInterruption,
	NewlyAcquiredProperty,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCFCoverageUnmannedAircraftStage
),
ArchWBCFCoverageUnmannedAircraftStage AS (
	INSERT INTO ArchWBCFCoverageUnmannedAircraftStage
	(ExtractDate, SourceSystemId, AuditId, WBCFCoverageUnmannedAircraftStageId, CoverageId, WB_CF_CoverageUnmannedAircraftId, SessionId, BusinessInterruption, NewlyAcquiredProperty)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCFCOVERAGEUNMANNEDAIRCRAFTSTAGEID, 
	COVERAGEID, 
	WB_CF_COVERAGEUNMANNEDAIRCRAFTID, 
	SESSIONID, 
	BUSINESSINTERRUPTION, 
	NEWLYACQUIREDPROPERTY
	FROM EXP_MetaData
),