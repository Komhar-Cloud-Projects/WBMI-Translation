WITH
SQ_WB_EC_Risk AS (
	WITH cte_WB_EC_Risk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId,
	X.WB_EC_RiskId,
	X.SessionId,
	X.LocationId
	FROM
	WB_EC_Risk X
	inner join
	cte_WB_EC_Risk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	LineId,
	WB_EC_RiskId,
	SessionId,
	LocationId
	FROM SQ_WB_EC_Risk
),
WBECRiskStage AS (
	TRUNCATE TABLE WBECRiskStage;
	INSERT INTO WBECRiskStage
	(ExtractDate, SourceSystemId, LineId, WB_EC_RiskId, SessionId, LocationId)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	WB_EC_RISKID, 
	SESSIONID, 
	LOCATIONID
	FROM EXP_Metadata
),