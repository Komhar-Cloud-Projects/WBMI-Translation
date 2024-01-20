WITH
SQ_WB_CF_CoverageUnmannedAircraft AS (
	WITH cte_WBCFCovUnmannedAir(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_CF_CoverageUnmannedAircraftId, 
	X.SessionId, 
	X.BusinessInterruption,
	X.NewlyAcquiredProperty 
	FROM
	 WB_CF_CoverageUnmannedAircraft X
	inner join
	cte_WBCFCovUnmannedAir Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	CoverageId,
	WB_CF_CoverageUnmannedAircraftId,
	SessionId,
	BusinessInterruption,
	NewlyAcquiredProperty,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CF_CoverageUnmannedAircraft
),
WBCFCoverageUnmannedAircraftStage AS (
	TRUNCATE TABLE WBCFCoverageUnmannedAircraftStage;
	INSERT INTO WBCFCoverageUnmannedAircraftStage
	(ExtractDate, SourceSystemid, CoverageId, WB_CF_CoverageUnmannedAircraftId, SessionId, BusinessInterruption, NewlyAcquiredProperty)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_CF_COVERAGEUNMANNEDAIRCRAFTID, 
	SESSIONID, 
	BUSINESSINTERRUPTION, 
	NEWLYACQUIREDPROPERTY
	FROM EXP_MetaData
),