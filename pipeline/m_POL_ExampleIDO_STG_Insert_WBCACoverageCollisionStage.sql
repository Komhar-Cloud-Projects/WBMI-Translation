WITH
SQ_WB_CA_CoverageCollision AS (
	WITH cte_WBCACoverageCollisionStage(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_CoverageCollisionId,
	X.WB_CA_CoverageCollisionId,
	X.SessionId,
	X.PremiumPrior,
	X.ReplacementCost
	FROM
	WB_CA_CoverageCollision X
	inner join
	cte_WBCACoverageCollisionStage Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CA_CoverageCollisionId,
	WB_CA_CoverageCollisionId,
	SessionId,
	PremiumPrior,
	ReplacementCost AS i_ReplacementCost,
	-- *INF*: decode(i_ReplacementCost,'T',1,'F',0,NULL)
	-- 
	decode(
	    i_ReplacementCost,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ReplacementCost
	FROM SQ_WB_CA_CoverageCollision
),
WBCACoverageCollisionStage AS (
	TRUNCATE TABLE WBCACoverageCollisionStage;
	INSERT INTO WBCACoverageCollisionStage
	(ExtractDate, SourceSystemId, CA_CoverageCollisionId, WB_CA_CoverageCollisionId, SessionId, PremiumPrior, ReplacementCost)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_COVERAGECOLLISIONID, 
	WB_CA_COVERAGECOLLISIONID, 
	SESSIONID, 
	PREMIUMPRIOR, 
	o_ReplacementCost AS REPLACEMENTCOST
	FROM EXP_Metadata
),