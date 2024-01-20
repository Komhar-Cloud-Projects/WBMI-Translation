WITH
SQ_WB_Producer AS (
	WITH cte_WBProducer(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PolicyId, 
	X.WB_ProducerId, 
	X.SessionId, 
	X.Email, 
	X.Name 
	FROM
	WB_Producer X
	inner join
	cte_WBProducer Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PolicyId,
	WB_ProducerId,
	SessionId,
	Email,
	Name,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_Producer
),
WBProducerStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBProducerStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBProducerStage
	(PolicyId, WbProducerId, SessionId, Email, Name, ExtractDate, SourceSystemId)
	SELECT 
	POLICYID, 
	WB_ProducerId AS WBPRODUCERID, 
	SESSIONID, 
	EMAIL, 
	NAME, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),