WITH
SQ_DC_BP_CoverageSpoilage AS (
	WITH cte_DCBPCoverageSpoilage(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.BP_CoverageSpoilageId, 
	X.SessionId, 
	X.Agreement, 
	X.ARate, 
	X.ClassGroup, 
	X.Type 
	FROM
	DC_BP_CoverageSpoilage X
	inner join
	cte_DCBPCoverageSpoilage Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_DefaultValues AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	BP_CoverageSpoilageId,
	SessionId,
	Agreement AS i_Agreement,
	-- *INF*: IIF(i_Agreement='T',1,0)
	IFF(i_Agreement = 'T', 1, 0) AS o_Agreement,
	ARate,
	ClassGroup,
	Type
	FROM SQ_DC_BP_CoverageSpoilage
),
DCBPCoverageSpoilageStage AS (
	TRUNCATE TABLE DCBPCoverageSpoilageStage;
	INSERT INTO DCBPCoverageSpoilageStage
	(ExtractDate, SourceSystemId, CoverageId, BP_CoverageSpoilageId, SessionId, Agreement, ARate, ClassGroup, Type)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	BP_COVERAGESPOILAGEID, 
	SESSIONID, 
	o_Agreement AS AGREEMENT, 
	ARATE, 
	CLASSGROUP, 
	TYPE
	FROM EXP_DefaultValues
),