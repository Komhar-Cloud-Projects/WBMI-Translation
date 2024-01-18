WITH
SQ_WB_CL_PriorInsurance AS (
	WITH cte_WBCLPriorInsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_PriorInsuranceId, 
	X.WB_CL_PriorInsuranceId, 
	X.SessionId, 
	X.PriorCarrierProduct, 
	X.PolicySymbol, 
	X.PolicyMod 
	FROM
	WB_CL_PriorInsurance X
	inner join
	cte_WBCLPriorInsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_PriorInsuranceId,
	WB_CL_PriorInsuranceId,
	SessionId,
	PriorCarrierProduct,
	PolicySymbol,
	PolicyMod,
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CL_PriorInsurance
),
WBCLPriorInsuranceStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLPriorInsuranceStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLPriorInsuranceStage
	(ExtractDate, SourceSystemId, WBPriorInsuranceId, WBCLPriorInsuranceId, SessionId, PriorCarrierProduct, PolicySymbol, PolicyMod)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_PriorInsuranceId AS WBPRIORINSURANCEID, 
	WB_CL_PriorInsuranceId AS WBCLPRIORINSURANCEID, 
	SESSIONID, 
	PRIORCARRIERPRODUCT, 
	POLICYSYMBOL, 
	POLICYMOD
	FROM EXP_Metadata
),