WITH
SQ_DC_IM_CoverageForm AS (
	WITH cte_DCIMCoverageForm(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.IM_CoverageFormId, 
	X.SessionId, 
	X.Id, 
	X.Type, 
	X.Description, 
	X.Deleted 
	FROM
	DC_IM_CoverageForm X
	inner join
	cte_DCIMCoverageForm Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	IM_CoverageFormId,
	SessionId,
	Id,
	Type,
	Description,
	Deleted,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	-- *INF*: decode(Deleted,'T','1','F','0',NULL)
	decode(
	    Deleted,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Deleted
	FROM SQ_DC_IM_CoverageForm
),
DCIMCoverageFormStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMCoverageFormStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMCoverageFormStage
	(ExtractDate, SourceSystemid, LineId, IM_CoverageFormId, SessionId, Id, Deleted, Type, Description)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	IM_COVERAGEFORMID, 
	SESSIONID, 
	ID, 
	o_Deleted AS DELETED, 
	TYPE, 
	DESCRIPTION
	FROM EXP_Metadata
),