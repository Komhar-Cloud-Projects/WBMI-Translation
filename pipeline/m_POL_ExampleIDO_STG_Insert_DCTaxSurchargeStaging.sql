WITH
SQ_DC_TaxSurcharge AS (
	WITH cte_DCTaxSurcharge(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.ObjectId, 
	X.ObjectName, 
	X.TaxSurchargeId, 
	X.SessionId, 
	X.Id, 
	X.Type, 
	X.Scope, 
	X.Amount, 
	X.Change, 
	X.Written,
	X.Rate  
	FROM
	DC_TaxSurcharge X
	inner join
	cte_DCTaxSurcharge Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	TaxSurchargeId,
	SessionId,
	Id,
	Type,
	Scope,
	Amount,
	Change,
	Written,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	Rate AS in_Rate,
	-- *INF*: LTRIM(RTRIM(in_Rate))
	LTRIM(RTRIM(in_Rate)) AS o_Rate
	FROM SQ_DC_TaxSurcharge
),
DCTaxSurchargeStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCTaxSurchargeStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCTaxSurchargeStaging
	(ObjectId, ObjectName, TaxSurchargeId, SessionId, Id, Type, Scope, Amount, Change, Written, ExtractDate, SourceSystemId, Rate)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	TAXSURCHARGEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	SCOPE, 
	AMOUNT, 
	CHANGE, 
	WRITTEN, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_Rate AS RATE
	FROM EXP_Metadata
),