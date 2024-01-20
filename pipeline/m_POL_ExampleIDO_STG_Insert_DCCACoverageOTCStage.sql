WITH
SQ_DC_CA_CoverageOTC AS (
	WITH cte_DCCACoverageOTCStage(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId,
	X.CA_CoverageOTCId,
	X.SessionId,
	X.AcceptOTCCoverage,
	X.AllPerilsDeductible,
	X.AllPerilsDeductibleGarageKeepers,
	X.AntiTheftDeviceDiscount,
	X.FallThroughIceCoverage,
	X.OTCCauseOfLoss,
	X.OTCCoverage,
	X.UnderwriterOverride,
	X.ExperienceRatingBasicLimitPremium,
	X.ExperienceRatingBasicLimitPremiumGarage
	FROM
	DC_CA_CoverageOTC X
	inner join
	cte_DCCACoverageOTCStage Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	-- 
	decode(
	    i_AllPerilsDeductible,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AllPerilsDeductible,
	AllPerilsDeductibleGarageKeepers AS i_AllPerilsDeductibleGarageKeepers,
	-- *INF*: decode(i_AllPerilsDeductibleGarageKeepers,'T',1,'F',0,NULL)
	-- 
	decode(
	    i_AllPerilsDeductibleGarageKeepers,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AllPerilsDeductibleGarageKeepers,
	AntiTheftDeviceDiscount,
	FallThroughIceCoverage AS i_FallThroughIceCoverage,
	-- *INF*: decode(i_FallThroughIceCoverage,'T',1,'F',0,NULL)
	-- 
	decode(
	    i_FallThroughIceCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FallThroughIceCoverage,
	OTCCauseOfLoss,
	OTCCoverage AS i_OTCCoverage,
	-- *INF*: decode(i_OTCCoverage,'T',1,'F',0,NULL)
	-- 
	decode(
	    i_OTCCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OTCCoverage,
	UnderwriterOverride AS i_UnderwriterOverride,
	-- *INF*: decode(i_UnderwriterOverride,'T',1,'F',0,NULL)
	-- 
	decode(
	    i_UnderwriterOverride,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UnderwriterOverride,
	ExperienceRatingBasicLimitPremium,
	ExperienceRatingBasicLimitPremiumGarage
	FROM SQ_DC_CA_CoverageOTC
),
DCCACoverageOTCStage AS (
	TRUNCATE TABLE DCCACoverageOTCStage;
	INSERT INTO DCCACoverageOTCStage
	(ExtractDate, SourceSystemId, CoverageId, CA_CoverageOTCId, SessionId, AcceptOTCCoverage, AllPerilsDeductible, AllPerilsDeductibleGarageKeepers, AntiTheftDeviceDiscount, FallThroughIceCoverage, OTCCauseOfLoss, OTCCoverage, UnderwriterOverride, ExperienceRatingBasicLimitPremium, ExperienceRatingBasicLimitPremiumGarage)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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