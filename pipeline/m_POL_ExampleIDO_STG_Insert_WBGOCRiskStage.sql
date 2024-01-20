WITH
SQ_WB_GOC_Risk AS (
	WITH cte_WBGOCRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WB_GOC_RiskId, 
	X.SessionId, 
	X.LocationId, 
	X.HoleInOneDescription 
	FROM
	WB_GOC_Risk X
	inner join
	cte_WBGOCRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata1 AS (
	SELECT
	LineId,
	WB_GOC_RiskId,
	SessionId,
	LocationId,
	HoleInOneDescription,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_GOC_Risk
),
WBGOCRiskStage AS (
	TRUNCATE TABLE WBGOCRiskStage;
	INSERT INTO WBGOCRiskStage
	(ExtractDate, SourceSystemId, LineId, WBGOCRiskId, SessionId, LocationId, HoleInOneDescription)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	WB_GOC_RiskId AS WBGOCRISKID, 
	SESSIONID, 
	LOCATIONID, 
	HOLEINONEDESCRIPTION
	FROM EXP_Metadata1
),