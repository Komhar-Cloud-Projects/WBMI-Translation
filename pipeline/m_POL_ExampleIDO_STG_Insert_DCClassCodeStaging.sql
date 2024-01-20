WITH
SQ_DC_ClassCode AS (
	WITH cte_DCClassCode(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.ObjectId, 
	X.ObjectName, 
	X.ClassCodeId, 
	X.SessionId, 
	X.Type, 
	X.Value 
	FROM
	DC_ClassCode X
	inner join
	cte_DCClassCode Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	ClassCodeId,
	SessionId,
	Type,
	Value,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_ClassCode
),
DCClassCodeStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCClassCodeStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCClassCodeStaging
	(ObjectId, ObjectName, ClassCodeId, SessionId, Type, Value, ExtractDate, SourceSystemId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	CLASSCODEID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),