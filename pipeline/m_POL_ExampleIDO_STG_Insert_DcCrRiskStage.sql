WITH
SQ_CR_Risk AS (
	WITH cte_DCCRRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CR_OccupancyId, 
	X.CR_RiskId, 
	X.SessionId, 
	X.Id, 
	X.CR_OccupancyXmlId, 
	X.Manufacturers, 
	X.RiskState 
	FROM
	DC_CR_Risk X
	inner join
	cte_DCCRRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CR_OccupancyId,
	CR_RiskId,
	SessionId,
	Id,
	CR_OccupancyXmlId,
	Manufacturers AS i_Manufacturers,
	-- *INF*: DECODE(i_Manufacturers, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_Manufacturers,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Manufacturers,
	RiskState,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_CR_Risk
),
DcCrRiskStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DcCrRiskStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DcCrRiskStage
	(LineId, CrOccupancyId, CrRiskId, SessionId, Id, CrOccupancyXmlId, Manufacturers, RiskState, ExtractDate, SourceSystemId)
	SELECT 
	LINEID, 
	CR_OccupancyId AS CROCCUPANCYID, 
	CR_RiskId AS CRRISKID, 
	SESSIONID, 
	ID, 
	CR_OccupancyXmlId AS CROCCUPANCYXMLID, 
	o_Manufacturers AS MANUFACTURERS, 
	RISKSTATE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),