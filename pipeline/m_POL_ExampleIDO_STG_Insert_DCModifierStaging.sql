WITH
SQ_DC_Modifier AS (
	SELECT	X.ObjectId, 
			X.ObjectName, 
			X.ModifierId, 
			X.SessionId, 
			X.Type, 
			X.Value, 
			X.DataType, 
			X.Scope 
	FROM
			DC_Modifier X WITH(nolock)
			INNER JOIN wbexampledata.dbo.wb_edwdataloadincrementalsessions Y WITH(
	                  nolock)
	               ON X.SessionId = Y.SessionId 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	ModifierId,
	SessionId,
	Type,
	Value,
	DataType,
	Scope,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Modifier
),
DCModifierStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCModifierStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCModifierStaging
	(ExtractDate, SourceSystemId, ObjectId, ObjectName, ModifierId, SessionId, Type, Value, DataType, Scope)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	OBJECTID, 
	OBJECTNAME, 
	MODIFIERID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	DATATYPE, 
	SCOPE
	FROM EXP_Metadata
),