WITH
SQ_DC_BP_CoveragePersonalProperty AS (
	WITH cte_DCBPCoveragePersonalProperty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.BP_CoveragePersonalPropertyId, 
	X.SessionId, 
	X.BlanketGroup, 
	X.BlanketPremium 
	FROM
	DC_BP_CoveragePersonalProperty X
	inner join
	cte_DCBPCoveragePersonalProperty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	BP_CoveragePersonalPropertyId,
	SessionId,
	BlanketGroup,
	BlanketPremium
	FROM SQ_DC_BP_CoveragePersonalProperty
),
DCBPCoveragePersonalPropertyStage AS (
	TRUNCATE TABLE DCBPCoveragePersonalPropertyStage;
	INSERT INTO DCBPCoveragePersonalPropertyStage
	(ExtractDate, SourceSystemId, CoverageId, BP_CoveragePersonalPropertyId, SessionId, BlanketGroup, BlanketPremium)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	BP_COVERAGEPERSONALPROPERTYID, 
	SESSIONID, 
	BLANKETGROUP, 
	BLANKETPREMIUM
	FROM EXP_Metadata
),