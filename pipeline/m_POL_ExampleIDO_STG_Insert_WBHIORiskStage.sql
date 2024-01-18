WITH
SQ_WB_HIO_Risk AS (
	WITH cte_WBHIORisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WB_HIO_RiskId, 
	X.SessionId, 
	X.LocationId 
	FROM
	WB_HIO_Risk X
	inner join
	cte_WBHIORisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	WB_HIO_RiskId,
	SessionId,
	LocationId,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_HIO_Risk
),
WBHIORiskStage AS (
	TRUNCATE TABLE WBHIORiskStage;
	INSERT INTO WBHIORiskStage
	(ExtractDate, SourceSystemId, LineId, WBHIORiskId, SessionId, LocationId)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	WB_HIO_RiskId AS WBHIORISKID, 
	SESSIONID, 
	LOCATIONID
	FROM EXP_Metadata
),