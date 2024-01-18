WITH
SQ_DC_Form AS (
	SELECT	X.ObjectId, 
			X.ObjectName, 
			X.FormId, 
			X.SessionId, 
			X.Type, 
			X.Value, 
			X.DataType, 
			X.Scope 
	FROM
			DC_Form X WITH(nolock)
			INNER JOIN wbexampledata.dbo.wb_edwdataloadincrementalsessions Y WITH(
	                  nolock)
	               ON X.SessionId = Y.SessionId 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	FormId,
	SessionId,
	Type,
	Value,
	-- *INF*: SUBSTR(Value,1,1000)
	SUBSTR(Value, 1, 1000) AS o_Value,
	DataType,
	Scope,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Form
),
DCFormStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCFormStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCFormStaging
	(ObjectId, ObjectName, FormId, SessionId, Type, Value, DataType, Scope, ExtractDate, SourceSystemId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	FORMID, 
	SESSIONID, 
	TYPE, 
	o_Value AS VALUE, 
	DATATYPE, 
	SCOPE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),