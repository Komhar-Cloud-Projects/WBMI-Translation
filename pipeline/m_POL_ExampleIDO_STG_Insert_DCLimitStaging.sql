WITH
SQ_DC_Limit AS (
	SELECT X.objectid,
	       X.objectname,
	       X.limitid,
	       X.sessionid,
	       X.type,
	       X.value,
	       X.datatype,
	       X.scope
	FROM   DC_limit X WITH(nolock)
	       INNER JOIN wbexampledata.dbo.wb_edwdataloadincrementalsessions Y WITH(
	                  nolock)
	               ON X.sessionid = Y.sessionid 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	LimitId,
	SessionId,
	Type,
	Value AS i_Value,
	-- *INF*: SUBSTR(i_Value,1,80)
	SUBSTR(i_Value, 1, 80) AS o_Value,
	DataType,
	Scope,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Limit
),
DCLimitStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCLimitStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCLimitStaging
	(ObjectId, ObjectName, LimitId, SessionId, Type, Value, DataType, Scope, ExtractDate, SourceSystemId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	LIMITID, 
	SESSIONID, 
	TYPE, 
	o_Value AS VALUE, 
	DATATYPE, 
	SCOPE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),