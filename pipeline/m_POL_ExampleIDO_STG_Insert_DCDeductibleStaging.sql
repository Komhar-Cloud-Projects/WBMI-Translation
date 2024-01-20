WITH
SQ_DC_Deductible AS (
	SELECT	X.ObjectId, 
			X.ObjectName, 
			X.DeductibleId, 
			X.SessionId, 
			X.Type, 
			X.Value, 
			X.DataType, 
			X.Scope 
	FROM	DC_Deductible X WITH(nolock)
			INNER JOIN wbexampledata.dbo.wb_edwdataloadincrementalsessions Y WITH(
	                  nolock)
	               ON X.SessionId = Y.SessionId 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	DeductibleId,
	SessionId,
	Type,
	Value,
	DataType,
	Scope,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Deductible
),
DCDeductibleStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCDeductibleStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCDeductibleStaging
	(ObjectId, ObjectName, DeductibleId, SessionId, Type, Value, DataType, Scope, ExtractDate, SourceSystemId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	DEDUCTIBLEID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	DATATYPE, 
	SCOPE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),