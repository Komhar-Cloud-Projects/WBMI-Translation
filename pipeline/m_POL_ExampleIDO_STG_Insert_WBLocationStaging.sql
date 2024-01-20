WITH
SQ_WB_Location AS (
	WITH cte_WBLocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LocationId, 
	X.WB_LocationId, 
	X.SessionId, 
	X.LocationNumber, 
	X.LocationName, 
	X.PrimaryEmail, 
	X.SecondaryEmail 
	FROM
	WB_Location X
	inner join
	cte_WBLocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LocationId,
	WB_LocationId,
	SessionId,
	LocationNumber,
	LocationName,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	PrimaryEmail,
	SecondaryEmail
	FROM SQ_WB_Location
),
WBLocationStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBLocationStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBLocationStaging
	(ExtractDate, SourceSystemId, LocationId, WB_LocationId, SessionId, LocationNumber, LocationName, PrimaryEmail, SecondaryEmail)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LOCATIONID, 
	WB_LOCATIONID, 
	SESSIONID, 
	LOCATIONNUMBER, 
	LOCATIONNAME, 
	PRIMARYEMAIL, 
	SECONDARYEMAIL
	FROM EXP_Metadata
),