WITH
SQ_DC_IM_Risk AS (
	WITH cte_DCIMRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.IM_RiskId, 
	X.SessionId, 
	X.Id, 
	X.Description, 
	X.IM_CoverageFormXmlId, 
	X.Deleted 
	FROM
	DC_IM_Risk X
	inner join
	cte_DCIMRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	LineId,
	IM_RiskId,
	SessionId,
	Id,
	Description,
	IM_CoverageFormXmlId,
	Deleted
	FROM SQ_DC_IM_Risk
),
DCIMRiskStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMRiskStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMRiskStage
	(ExtractDate, SourceSystemid, LineId, IM_RiskId, SessionId, Id, Deleted, Description, IM_CoverageFormXmlId)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	IM_RISKID, 
	SESSIONID, 
	ID, 
	DELETED, 
	DESCRIPTION, 
	IM_COVERAGEFORMXMLID
	FROM EXP_Metadata
),