WITH
SQ_WBBPCoverageUnmannedAircraftPropertyStage AS (
	SELECT
		WBBPCoverageUnmannedAircraftPropertyStageId,
		ExtractDate,
		SourceSystemid,
		CoverageId,
		WB_BP_CoverageUnmannedAircraftPropertyId,
		SessionId,
		BusinessInterruption,
		NewlyAcquiredProperty
	FROM WBBPCoverageUnmannedAircraftPropertyStage
),
EXP_MetaData AS (
	SELECT
	WBBPCoverageUnmannedAircraftPropertyStageId,
	ExtractDate,
	SourceSystemid,
	CoverageId,
	WB_BP_CoverageUnmannedAircraftPropertyId,
	SessionId,
	BusinessInterruption,
	NewlyAcquiredProperty,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBBPCoverageUnmannedAircraftPropertyStage
),
ArchWBBPCoverageUnmannedAircraftPropertyStage AS (
	INSERT INTO ArchWBBPCoverageUnmannedAircraftPropertyStage
	(ExtractDate, SourceSystemId, WBBPCoverageUnmannedAircraftPropertyStageId, CoverageId, WB_BP_CoverageUnmannedAircraftPropertyId, SessionId, BusinessInterruption, NewlyAcquiredProperty)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	WBBPCOVERAGEUNMANNEDAIRCRAFTPROPERTYSTAGEID, 
	COVERAGEID, 
	WB_BP_COVERAGEUNMANNEDAIRCRAFTPROPERTYID, 
	SESSIONID, 
	BUSINESSINTERRUPTION, 
	NEWLYACQUIREDPROPERTY
	FROM EXP_MetaData
),