WITH
SQ_WB_TaxSurcharge AS (
	WITH cte_WBTaxSurcharge(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.TaxSurchargeId, 
	X.WB_TaxSurchargeId, 
	X.SessionId, 
	X.ChangeAttr, 
	X.WrittenAttr, 
	X.fValue, 
	X.EntityType, 
	X.premium 
	FROM  
	WB_TaxSurcharge X
	inner join
	cte_WBTaxSurcharge Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	TaxSurchargeId,
	WB_TaxSurchargeId,
	SessionId,
	ChangeAttr,
	WrittenAttr,
	fValue,
	EntityType,
	premium,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_TaxSurcharge
),
WBTaxSurchargeStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBTaxSurchargeStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBTaxSurchargeStage
	(ExtractDate, SourceSyStemId, TaxSurchargeId, WBTaxSurchargeId, SessionId, ChangeAttr, WrittenAttr, fValue, EntityType, premium)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	TAXSURCHARGEID, 
	WB_TaxSurchargeId AS WBTAXSURCHARGEID, 
	SESSIONID, 
	CHANGEATTR, 
	WRITTENATTR, 
	FVALUE, 
	ENTITYTYPE, 
	PREMIUM
	FROM EXP_Metadata
),