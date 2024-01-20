WITH
SQ_DC_CA_Motorcycle AS (
	WITH cte_DCCAMotorcycle(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_VehicleId, 
	X.CA_MotorcycleId, 
	X.SessionId, 
	X.Id, 
	X.EngineSize 
	FROM
	DC_CA_Motorcycle X
	inner join
	cte_DCCAMotorcycle Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CA_VehicleId,
	CA_MotorcycleId,
	SessionId,
	Id,
	EngineSize,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_Motorcycle
),
DCCAMotorcycleStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAMotorcycleStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAMotorcycleStaging
	(ExtractDate, SourceSystemId, CA_VehicleId, CA_MotorcycleId, SessionId, Id, EngineSize)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_VEHICLEID, 
	CA_MOTORCYCLEID, 
	SESSIONID, 
	ID, 
	ENGINESIZE
	FROM EXP_Metadata
),