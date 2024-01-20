WITH
SQ_DC_IM_Location AS (
	WITH cte_DCIMLocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.IM_LocationId, 
	X.SessionId, 
	X.Id, 
	X.CTGovernmentAgencies,
	X.Description, 
	X.[Number]   
	FROM
	DC_IM_Location X
	inner join
	cte_DCIMLocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	IM_LocationId,
	SessionId,
	Id,
	CTGovernmentAgencies,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	Description,
	Number
	FROM SQ_DC_IM_Location
),
DCIMLocationStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMLocationStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMLocationStage
	(IMLocationId, SessionId, Id, CTGovernmentAgencies, ExtractDate, SourceSystemId, Description, Number)
	SELECT 
	IM_LocationId AS IMLOCATIONID, 
	SESSIONID, 
	ID, 
	CTGOVERNMENTAGENCIES, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	DESCRIPTION, 
	NUMBER
	FROM EXP_Metadata
),