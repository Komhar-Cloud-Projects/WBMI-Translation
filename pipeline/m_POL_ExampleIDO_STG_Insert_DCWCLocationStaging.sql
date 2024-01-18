WITH
SQ_DC_WC_Location AS (
	WITH cte_DCWCLocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WC_LocationId, 
	X.SessionId, 
	X.Id, 
	X.Description, 
	X.NumberOfEmployees,
	X.WC_StateXmlId,
	X.Number   
	FROM
	DC_WC_Location X
	inner join
	cte_DCWCLocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WC_LocationId,
	SessionId,
	Id,
	Description,
	NumberOfEmployees,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	WC_StateXmlId,
	Number
	FROM SQ_DC_WC_Location
),
DCWCLocationStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCLocationStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCLocationStaging
	(WC_LocationId, SessionId, Id, Description, NumberOfEmployees, ExtractDate, SourceSystemId, WC_StateXmlId, Number)
	SELECT 
	WC_LOCATIONID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	NUMBEROFEMPLOYEES, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WC_STATEXMLID, 
	NUMBER
	FROM EXP_Metadata
),