WITH
SQ_WB_BP_CoverageUnmannedAircraftLimited AS (
	WITH cte_WBBPCovUnmannAircr(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_BP_CoverageUnmannedAircraftLimitedId, 
	X.SessionId, 
	X.BodilyInjuryAndPropertyDamageLimited, 
	X.PersonalAndAdvertisingInjuryLimited 
	FROM
	WB_BP_CoverageUnmannedAircraftLimited X
	inner join
	cte_WBBPCovUnmannAircr Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	WB_BP_CoverageUnmannedAircraftLimitedId,
	SessionId,
	BodilyInjuryAndPropertyDamageLimited AS i_BodilyInjuryAndPropertyDamageLimited,
	-- *INF*: DECODE(i_BodilyInjuryAndPropertyDamageLimited,'T',1,'F',0,NULL)
	DECODE(
	    i_BodilyInjuryAndPropertyDamageLimited,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BodilyInjuryAndPropertyDamageLimited,
	PersonalAndAdvertisingInjuryLimited AS i_PersonalAndAdvertisingInjuryLimited,
	-- *INF*: DECODE(i_PersonalAndAdvertisingInjuryLimited,'T',1,'F',0,NULL)
	DECODE(
	    i_PersonalAndAdvertisingInjuryLimited,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PersonalAndAdvertisingInjuryLimited
	FROM SQ_WB_BP_CoverageUnmannedAircraftLimited
),
WBBPCoverageUnmannedAircraftLimitedStage AS (
	TRUNCATE TABLE WBBPCoverageUnmannedAircraftLimitedStage;
	INSERT INTO WBBPCoverageUnmannedAircraftLimitedStage
	(ExtractDate, SourceSystemid, CoverageId, WB_BP_CoverageUnmannedAircraftLimitedId, SessionId, BodilyInjuryAndPropertyDamageLimited, PersonalAndAdvertisingInjuryLimited)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_BP_COVERAGEUNMANNEDAIRCRAFTLIMITEDID, 
	SESSIONID, 
	o_BodilyInjuryAndPropertyDamageLimited AS BODILYINJURYANDPROPERTYDAMAGELIMITED, 
	o_PersonalAndAdvertisingInjuryLimited AS PERSONALANDADVERTISINGINJURYLIMITED
	FROM EXP_MetaData
),