WITH
SQ_WB_CF_TimeElement AS (
	WITH cte_WBCFTimeElement(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_TimeElementId, 
	X.WB_CF_TimeElementId, 
	X.SessionId, 
	X.LimitsOnLossPayment, 
	X.CoverageType 
	FROM
	WB_CF_TimeElement X
	inner join
	cte_WBCFTimeElement Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_TimeElementId,
	WB_CF_TimeElementId,
	SessionId,
	LimitsOnLossPayment,
	CoverageType,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CF_TimeElement
),
WBCFTimeElementStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFTimeElementStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFTimeElementStage
	(ExtractDate, SourceSystemId, CF_TimeElementId, WB_CF_TimeElementId, SessionId, LimitsOnLossPayment, CoverageType)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CF_TIMEELEMENTID, 
	WB_CF_TIMEELEMENTID, 
	SESSIONID, 
	LIMITSONLOSSPAYMENT, 
	COVERAGETYPE
	FROM EXP_Metadata
),