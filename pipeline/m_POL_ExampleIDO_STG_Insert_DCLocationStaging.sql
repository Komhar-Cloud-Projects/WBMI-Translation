WITH
SQ_DC_Location AS (
	WITH cte_DCLocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LocationId, 
	X.SessionId, 
	X.LocationXmlId, 
	X.Description, 
	X.Address1, 
	X.Address2, 
	X.City, 
	X.County, 
	X.StateProv, 
	X.PostalCode, 
	X.Country,
	case when X.Deleted=0 then '0' when X.Deleted=1 then '1' else Null end Deleted 
	FROM
	DC_Location X
	inner join
	cte_DCLocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LocationId,
	SessionId,
	LocationXmlId,
	Description,
	Address1,
	Address2,
	City,
	County,
	StateProv,
	PostalCode,
	Country,
	Deleted,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	-- *INF*: substr(Description,1,255)
	substr(Description, 1, 255) AS o_Description
	FROM SQ_DC_Location
),
DCLocationStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCLocationStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCLocationStaging
	(LocationId, SessionId, LocationXmlId, Description, Address1, Address2, City, County, StateProv, PostalCode, Country, ExtractDate, SourceSystemId, deleted)
	SELECT 
	LOCATIONID, 
	SESSIONID, 
	LOCATIONXMLID, 
	o_Description AS DESCRIPTION, 
	ADDRESS1, 
	ADDRESS2, 
	CITY, 
	COUNTY, 
	STATEPROV, 
	POSTALCODE, 
	COUNTRY, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	Deleted AS DELETED
	FROM EXP_Metadata
),