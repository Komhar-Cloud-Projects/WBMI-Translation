WITH
SQ_DC_CA_CoverageDriveOtherCarOTC AS (
	WITH cte_DCCACoverageDriveOtherCarOTCStage(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId,
	X.CA_CoverageDriveOtherCarOTCId,
	X.SessionId,
	X.ExperienceRatingBasicLimitPremium,
	X.DeductibleType,
	X.FullGlassIndicator
	FROM
	DC_CA_CoverageDriveOtherCarOTC X
	inner join
	cte_DCCACoverageDriveOtherCarOTCStage Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EX_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	FROM SQ_DC_CA_CoverageDriveOtherCarOTC
),
DCCACoverageDriveOtherCarOTCStage AS (
	TRUNCATE TABLE DCCACoverageDriveOtherCarOTCStage;
	INSERT INTO DCCACoverageDriveOtherCarOTCStage
	(ExtractDate, SourceSystemId, CoverageId, CA_CoverageDriveOtherCarOTCId, SessionId, ExperienceRatingBasicLimitPremium, DeductibleType, FullGlassIndicator)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	CA_COVERAGEDRIVEOTHERCAROTCID, 
	SESSIONID, 
	EXPERIENCERATINGBASICLIMITPREMIUM, 
	DEDUCTIBLETYPE, 
	o_FullGlassIndicator AS FULLGLASSINDICATOR
	FROM EX_Metadata
),