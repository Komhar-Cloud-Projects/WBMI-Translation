WITH
SQ_WB_BP_CoverageUnmannedAircraftProperty AS (
	WITH cte_WBBPCovUnManAircraftProp(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_BP_CoverageUnmannedAircraftPropertyId, 
	X.SessionId, 
	X.BusinessInterruption,
	X.NewlyAcquiredProperty 
	FROM
	 WB_BP_CoverageUnmannedAircraftProperty X
	inner join
	cte_WBBPCovUnManAircraftProp Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	CoverageId,
	WB_BP_CoverageUnmannedAircraftPropertyId,
	SessionId,
	BusinessInterruption,
	NewlyAcquiredProperty,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_BP_CoverageUnmannedAircraftProperty
),
WBBPCoverageUnmannedAircraftPropertyStage AS (
	TRUNCATE TABLE WBBPCoverageUnmannedAircraftPropertyStage;
	INSERT INTO WBBPCoverageUnmannedAircraftPropertyStage
	(ExtractDate, SourceSystemid, CoverageId, WB_BP_CoverageUnmannedAircraftPropertyId, SessionId, BusinessInterruption, NewlyAcquiredProperty)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_BP_COVERAGEUNMANNEDAIRCRAFTPROPERTYID, 
	SESSIONID, 
	BUSINESSINTERRUPTION, 
	NEWLYACQUIREDPROPERTY
	FROM EXP_MetaData
),