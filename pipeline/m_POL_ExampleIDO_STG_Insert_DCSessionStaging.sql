WITH
SQ_DC_Session AS (
	WITH cte_DCSession(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.SessionId, 
	X.ExampleQuoteId, 
	X.UserName, 
	X.CreateDateTime, 
	X.Purpose 
	FROM
	DC_Session X
	inner join
	cte_DCSession Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	SessionId,
	ExampleQuoteId,
	UserName,
	CreateDateTime,
	Purpose,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Session
),
DCSessionStaging AS (
	TRUNCATE TABLE DCSessionStaging;
	INSERT INTO DCSessionStaging
	(ExtractDate, SourceSystemId, SessionId, ExampleQuoteId, UserName, CreateDateTime, Purpose)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	SESSIONID, 
	EXAMPLEQUOTEID, 
	USERNAME, 
	CREATEDATETIME, 
	PURPOSE
	FROM EXPTRANS
),