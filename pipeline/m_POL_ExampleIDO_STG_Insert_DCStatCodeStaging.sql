WITH
SQ_DC_StatCode AS (
	SELECT	X.ObjectId, 
			X.ObjectName, 
			X.StatCodeId, 
			X.SessionId, 
			X.Type, 
			X.Value, 
			X.Scope 
	FROM
			DC_StatCode X WITH(nolock)
			INNER JOIN wbexampledata.dbo.wb_edwdataloadincrementalsessions Y WITH(
	                  nolock)
	               ON X.SessionId = Y.SessionId 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	StatCodeId,
	SessionId,
	Type,
	Value,
	Scope,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_StatCode
),
DCStatCodeStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCStatCodeStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCStatCodeStaging
	(ObjectId, ObjectName, StatCodeId, SessionId, Type, Value, Scope, ExtractDate, SourceSystemId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	STATCODEID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	SCOPE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),