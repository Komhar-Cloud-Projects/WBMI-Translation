WITH
SQ_WB_CL_Reinsurance AS (
	WITH cte_WBCLReinsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_ReinsuranceId, 
	X.WB_CL_ReinsuranceId, 
	X.SessionId, 
	X.PurchasedEachAccidentLimit, 
	X.Include, 
	X.Exclude, 
	X.AddedCaption, 
	X.SpecialCondition 
	FROM
	WB_CL_Reinsurance X
	inner join
	cte_WBCLReinsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_ReinsuranceId,
	WB_CL_ReinsuranceId,
	SessionId,
	PurchasedEachAccidentLimit,
	Include,
	Exclude,
	AddedCaption,
	SpecialCondition,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CL_Reinsurance
),
WBCLReinsuranceStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLReinsuranceStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLReinsuranceStage
	(ExtractDate, SourceSystemId, WBReinsuranceId, WBCLReinsuranceId, SessionId, PurchasedEachAccidentLimit, Include, Exclude, AddedCaption, SpecialCondition)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_ReinsuranceId AS WBREINSURANCEID, 
	WB_CL_ReinsuranceId AS WBCLREINSURANCEID, 
	SESSIONID, 
	PURCHASEDEACHACCIDENTLIMIT, 
	INCLUDE, 
	EXCLUDE, 
	ADDEDCAPTION, 
	SPECIALCONDITION
	FROM EXP_Metadata
),