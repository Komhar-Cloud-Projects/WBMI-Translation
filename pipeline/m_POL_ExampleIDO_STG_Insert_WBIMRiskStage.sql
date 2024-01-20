WITH
SQ_WB_IM_Risk AS (
	WITH cte_WBIMRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.IM_RiskId, 
	X.WB_IM_RiskId, 
	X.SessionId, 
	X.IM_LocationXmlId, 
	X.PurePremium 
	FROM
	WB_IM_Risk X
	inner join
	cte_WBIMRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	IM_RiskId,
	WB_IM_RiskId,
	SessionId,
	IM_LocationXmlId,
	PurePremium,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_IM_Risk
),
WBIMRiskStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBIMRiskStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBIMRiskStage
	(IMRiskId, WBIMRiskId, SessionId, IMLocationXmlId, PurePremium, ExtractDate, SourceSystemId)
	SELECT 
	IM_RiskId AS IMRISKID, 
	WB_IM_RiskId AS WBIMRISKID, 
	SESSIONID, 
	IM_LocationXmlId AS IMLOCATIONXMLID, 
	PUREPREMIUM, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),