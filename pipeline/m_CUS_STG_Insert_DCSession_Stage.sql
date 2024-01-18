WITH
SQ_DC_Session AS (
	SELECT DC_Session.SessionId, DC_Session.ExampleQuoteId, DC_Session.UserName, DC_Session.CreateDateTime, DC_Session.Purpose 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session 
	where
	 DC_Session.CreateDateTime >='@{pipeline().parameters.SELECTION_START_TS}' and
	 DC_Session.CreateDateTime <'@{pipeline().parameters.SELECTION_END_TS}'
	order by
	DC_Session.SessionId
),
Exp_Session AS (
	SELECT
	SessionId,
	ExampleQuoteId,
	UserName,
	CreateDateTime,
	Purpose,
	Sysdate AS ExtractDate,
	'DCT' AS SourceSystemID
	FROM SQ_DC_Session
),
DCSessionStage AS (
	INSERT INTO Shortcut_to_DCSessionStage
	(ExtractDate, SourceSystemid, SessionId, UserName, CreateDateTime, Purpose, ExampleQuoteId)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	SESSIONID, 
	USERNAME, 
	CREATEDATETIME, 
	PURPOSE, 
	EXAMPLEQUOTEID
	FROM Exp_Session
),