WITH
SQ_DCCACoverageOTCStage AS (
	SELECT
		DCCACoverageOTCStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		CA_CoverageOTCId,
		SessionId,
		AcceptOTCCoverage,
		AllPerilsDeductible,
		AllPerilsDeductibleGarageKeepers,
		AntiTheftDeviceDiscount,
		FallThroughIceCoverage,
		OTCCauseOfLoss,
		OTCCoverage,
		UnderwriterOverride,
		ExperienceRatingBasicLimitPremium,
		ExperienceRatingBasicLimitPremiumGarage
	FROM DCCACoverageOTCStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCCACoverageOTCStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	CA_CoverageOTCId,
	SessionId,
	AcceptOTCCoverage AS i_AcceptOTCCoverage,
	-- *INF*: decode(i_AcceptOTCCoverage,'T',1,'F',0,NULL)
	decode(
	    i_AcceptOTCCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AcceptOTCCoverage,
	AllPerilsDeductible AS i_AllPerilsDeductible,
	-- *INF*: decode(i_AllPerilsDeductible,'T',1,'F',0,NULL)
	decode(
	    i_AllPerilsDeductible,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AllPerilsDeductible,
	AllPerilsDeductibleGarageKeepers AS i_AllPerilsDeductibleGarageKeepers,
	-- *INF*: decode(i_AllPerilsDeductibleGarageKeepers,'T',1,'F',0,NULL)
	decode(
	    i_AllPerilsDeductibleGarageKeepers,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AllPerilsDeductibleGarageKeepers,
	AntiTheftDeviceDiscount,
	FallThroughIceCoverage AS i_FallThroughIceCoverage,
	-- *INF*: decode(i_FallThroughIceCoverage,'T',1,'F',0,NULL)
	decode(
	    i_FallThroughIceCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FallThroughIceCoverage,
	OTCCauseOfLoss,
	OTCCoverage AS i_OTCCoverage,
	-- *INF*: decode(i_OTCCoverage,'T',1,'F',0,NULL)
	decode(
	    i_OTCCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OTCCoverage,
	UnderwriterOverride AS i_UnderwriterOverride,
	-- *INF*: decode(i_UnderwriterOverride,'T',1,'F',0,NULL)
	decode(
	    i_UnderwriterOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UnderwriterOverride,
	ExperienceRatingBasicLimitPremium,
	ExperienceRatingBasicLimitPremiumGarage
	FROM SQ_DCCACoverageOTCStage
),
ArchDCCACoverageOTCStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCACoverageOTCStage
	(ExtractDate, SourceSystemId, AuditId, DCCACoverageOTCStageId, CoverageId, CA_CoverageOTCId, SessionId, AcceptOTCCoverage, AllPerilsDeductible, AllPerilsDeductibleGarageKeepers, AntiTheftDeviceDiscount, FallThroughIceCoverage, OTCCauseOfLoss, OTCCoverage, UnderwriterOverride, ExperienceRatingBasicLimitPremium, ExperienceRatingBasicLimitPremiumGarage)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCACOVERAGEOTCSTAGEID, 
	COVERAGEID, 
	CA_COVERAGEOTCID, 
	SESSIONID, 
	o_AcceptOTCCoverage AS ACCEPTOTCCOVERAGE, 
	o_AllPerilsDeductible AS ALLPERILSDEDUCTIBLE, 
	o_AllPerilsDeductibleGarageKeepers AS ALLPERILSDEDUCTIBLEGARAGEKEEPERS, 
	ANTITHEFTDEVICEDISCOUNT, 
	o_FallThroughIceCoverage AS FALLTHROUGHICECOVERAGE, 
	OTCCAUSEOFLOSS, 
	o_OTCCoverage AS OTCCOVERAGE, 
	o_UnderwriterOverride AS UNDERWRITEROVERRIDE, 
	EXPERIENCERATINGBASICLIMITPREMIUM, 
	EXPERIENCERATINGBASICLIMITPREMIUMGARAGE
	FROM EXP_Metadata
),