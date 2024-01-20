WITH
SQ_to_DC_CU_UmbrellaFormNamedParty AS (
	WITH cte_DCCUUmbrellaFormNamedParty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CU_UmbrellaFormId, 
	X.CU_UmbrellaFormNamedPartyId, 
	X.SessionId, 
	X.Type 
	FROM
	DC_CU_UmbrellaFormNamedParty X
	inner join
	cte_DCCUUmbrellaFormNamedParty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_handle AS (
	SELECT
	CU_UmbrellaFormId,
	CU_UmbrellaFormNamedPartyId,
	SessionId,
	Type,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemid
	FROM SQ_to_DC_CU_UmbrellaFormNamedParty
),
DCCUUmbrellaFormNamedPartyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaFormNamedPartyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCUUmbrellaFormNamedPartyStaging
	(ExtractDate, SourceSystemId, CU_UmbrellaFormId, CU_UmbrellaFormNamedPartyId, SessionId, Type)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemid AS SOURCESYSTEMID, 
	CU_UMBRELLAFORMID, 
	CU_UMBRELLAFORMNAMEDPARTYID, 
	SESSIONID, 
	TYPE
	FROM EXP_handle
),