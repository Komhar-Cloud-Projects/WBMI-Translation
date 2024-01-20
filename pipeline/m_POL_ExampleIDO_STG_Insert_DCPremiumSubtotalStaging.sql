WITH
SQ_DC_PremiumSubtotal AS (
	WITH cte_DCPremiumSubtotal(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.ObjectId, 
	X.ObjectName, 
	X.PremiumSubtotalId, 
	X.SessionId, 
	X.Type, 
	X.Value, 
	X.Change, 
	X.Written, 
	X.Prior 
	FROM
	DC_PremiumSubtotal X
	inner join
	cte_DCPremiumSubtotal Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	PremiumSubtotalId,
	SessionId,
	Type,
	Value,
	Change,
	Written,
	Prior,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_PremiumSubtotal
),
DCPremiumSubtotalStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPremiumSubtotalStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPremiumSubtotalStaging
	(ObjectId, ObjectName, PremiumSubtotalId, SessionId, Type, Value, Change, Written, Prior, ExtractDate, SourceSystemId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	PREMIUMSUBTOTALID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	CHANGE, 
	WRITTEN, 
	PRIOR, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),