WITH
SQ_DC_CA_Location AS (
	WITH cte_DCCALocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_LocationId, 
	X.SessionId, 
	X.Id, 
	X.Description, 
	X.EstimatedAnnualRenumeration, 
	X.Territory,
	X.[Number]  
	FROM
	DC_CA_Location X
	inner join
	cte_DCCALocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CA_LocationId,
	SessionId,
	Id,
	Description,
	EstimatedAnnualRenumeration,
	Territory,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	Number
	FROM SQ_DC_CA_Location
),
DCCALocationStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCALocationStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCALocationStaging
	(ExtractDate, SourceSystemId, CA_LocationId, SessionId, Id, Description, EstimatedAnnualRenumeration, Territory, Number)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_LOCATIONID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	ESTIMATEDANNUALRENUMERATION, 
	TERRITORY, 
	NUMBER
	FROM EXP_Metadata
),