WITH
SQ_DCCACoverageDriveOtherCarOTCStage AS (
	SELECT
		DCCACoverageDriveOtherCarOTCStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		CA_CoverageDriveOtherCarOTCId,
		SessionId,
		ExperienceRatingBasicLimitPremium,
		DeductibleType,
		FullGlassIndicator
	FROM DCCACoverageDriveOtherCarOTCStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCCACoverageDriveOtherCarOTCStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	CA_CoverageDriveOtherCarOTCId,
	SessionId,
	ExperienceRatingBasicLimitPremium,
	DeductibleType,
	FullGlassIndicator AS i_FullGlassIndicator,
	-- *INF*: decode(i_FullGlassIndicator,'T',1,'F',0,NULL)
	decode(
	    i_FullGlassIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FullGlassIndicator
	FROM SQ_DCCACoverageDriveOtherCarOTCStage
),
ArchDCCACoverageDriveOtherCarOTCStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCACoverageDriveOtherCarOTCStage
	(ExtractDate, SourceSystemId, AuditId, DCCACoverageDriveOtherCarOTCStageId, CoverageId, CA_CoverageDriveOtherCarOTCId, SessionId, ExperienceRatingBasicLimitPremium, DeductibleType, FullGlassIndicator)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCACOVERAGEDRIVEOTHERCAROTCSTAGEID, 
	COVERAGEID, 
	CA_COVERAGEDRIVEOTHERCAROTCID, 
	SESSIONID, 
	EXPERIENCERATINGBASICLIMITPREMIUM, 
	DEDUCTIBLETYPE, 
	o_FullGlassIndicator AS FULLGLASSINDICATOR
	FROM EXP_Metadata
),