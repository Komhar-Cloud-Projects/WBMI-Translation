WITH
SQ_DC_BP_BlanketGroup AS (
	WITH cte_DCBPBlanketGroup(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.BP_RiskId, 
	X.BP_BlanketGroupId, 
	X.SessionId, 
	X.Id, 
	X.ARate, 
	X.Type 
	FROM
	DC_BP_BlanketGroup X
	inner join
	cte_DCBPBlanketGroup Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Set_Metadata AS (
	SELECT
	BP_RiskId,
	BP_BlanketGroupId,
	SessionId,
	Id,
	ARate,
	Type,
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_BP_BlanketGroup
),
DCBPBlanketGroupStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCBPBlanketGroupStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCBPBlanketGroupStage
	(ExtractDate, SourceSystemId, BP_RiskId, BP_BlanketGroupId, SessionId, Id, ARate, Type)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	BP_RISKID, 
	BP_BLANKETGROUPID, 
	SESSIONID, 
	ID, 
	ARATE, 
	TYPE
	FROM EXP_Set_Metadata
),