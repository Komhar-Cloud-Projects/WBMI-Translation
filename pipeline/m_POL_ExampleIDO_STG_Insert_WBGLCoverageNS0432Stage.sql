WITH
SQ_WB_GL_CoverageNS0432 AS (
	WITH cte_WBGLCovNS032(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_GL_CoverageNS0432Id, 
	X.SessionId, 
	X.BodilyInjuryAndPropertyDamageLimitedCoverage, 
	X.PersonalAndAdvertisingInjuryLimitedCoverage
	FROM
	 WB_GL_CoverageNS0432 X
	inner join
	cte_WBGLCovNS032 Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	WB_GL_CoverageNS0432Id,
	SessionId,
	BodilyInjuryAndPropertyDamageLimitedCoverage AS i_BodilyInjuryAndPropertyDamageLimitedCoverage,
	-- *INF*: DECODE(i_BodilyInjuryAndPropertyDamageLimitedCoverage,'T',1,'F',0,NULL)
	DECODE(
	    i_BodilyInjuryAndPropertyDamageLimitedCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BodilyInjuryAndPropertyDamageLimitedCoverage,
	PersonalAndAdvertisingInjuryLimitedCoverage AS i_PersonalAndAdvertisingInjuryLimitedCoverage,
	-- *INF*: DECODE(i_PersonalAndAdvertisingInjuryLimitedCoverage,'T',1,'F',0,NULL)
	DECODE(
	    i_PersonalAndAdvertisingInjuryLimitedCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PersonalAndAdvertisingInjuryLimitedCoverage
	FROM SQ_WB_GL_CoverageNS0432
),
WBGLCoverageNS0432Stage AS (
	TRUNCATE TABLE WBGLCoverageNS0432Stage;
	INSERT INTO WBGLCoverageNS0432Stage
	(ExtractDate, SourceSystemid, CoverageId, WB_GL_CoverageNS0432Id, SessionId, BodilyInjuryAndPropertyDamageLimitedCoverage, PersonalAndAdvertisingInjuryLimitedCoverage)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_GL_COVERAGENS0432ID, 
	SESSIONID, 
	o_BodilyInjuryAndPropertyDamageLimitedCoverage AS BODILYINJURYANDPROPERTYDAMAGELIMITEDCOVERAGE, 
	o_PersonalAndAdvertisingInjuryLimitedCoverage AS PERSONALANDADVERTISINGINJURYLIMITEDCOVERAGE
	FROM EXP_MetaData
),