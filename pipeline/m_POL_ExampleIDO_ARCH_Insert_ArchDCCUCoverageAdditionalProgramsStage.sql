WITH
SQ_DCCUCoverageAdditionalProgramsStage AS (
	SELECT
		DCCUCoverageAdditionalProgramsStageId,
		CoverageId,
		CUCoverageAdditionalProgramsId,
		SessionId,
		AdditionalCoveredPrograms,
		RetroActiveDate,
		ExtractDate,
		SourceSystemId
	FROM DCCUCoverageAdditionalProgramsStage
),
EXP_Metadata AS (
	SELECT
	DCCUCoverageAdditionalProgramsStageId,
	CoverageId,
	CUCoverageAdditionalProgramsId,
	SessionId,
	AdditionalCoveredPrograms,
	RetroActiveDate,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCUCoverageAdditionalProgramsStage
),
ArchDCCUCoverageAdditionalProgramsStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCUCoverageAdditionalProgramsStage
	(DCCUCoverageAdditionalProgramsStageId, CoverageId, CUCoverageAdditionalProgramsId, SessionId, AdditionalCoveredPrograms, RetroActiveDate, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCCUCOVERAGEADDITIONALPROGRAMSSTAGEID, 
	COVERAGEID, 
	CUCOVERAGEADDITIONALPROGRAMSID, 
	SESSIONID, 
	ADDITIONALCOVEREDPROGRAMS, 
	RETROACTIVEDATE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),