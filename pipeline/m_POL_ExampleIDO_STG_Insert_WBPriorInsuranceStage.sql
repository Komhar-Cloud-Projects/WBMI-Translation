WITH
SQ_WB_PriorInsurance AS (
	WITH cte_WBPriorInsurance(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PriorInsuranceId, 
	X.WB_PriorInsuranceId, 
	X.SessionId, 
	X.CarrierNameOther, 
	X.ExperienceMod, 
	X.LineOfBusiness, 
	X.NoPriorInsurance2 
	FROM
	WB_PriorInsurance X
	inner join
	cte_WBPriorInsurance Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PriorInsuranceId,
	WB_PriorInsuranceId,
	SessionId,
	CarrierNameOther,
	ExperienceMod,
	LineOfBusiness,
	NoPriorInsurance2 AS i_NoPriorInsurance2,
	-- *INF*: DECODE(TRUE,
	-- i_NoPriorInsurance2='T',1,
	-- i_NoPriorInsurance2='F',0
	-- )
	DECODE(
	    TRUE,
	    i_NoPriorInsurance2 = 'T', 1,
	    i_NoPriorInsurance2 = 'F', 0
	) AS o_NoPriorInsurance2,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_PriorInsurance
),
WBPriorInsuranceStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBPriorInsuranceStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBPriorInsuranceStage
	(PriorInsuranceId, WBPriorInsuranceId, SessionId, CarrierNameOther, ExperienceMod, LineOfBusiness, NoPriorInsurance2, ExtractDate, SourceSystemId)
	SELECT 
	PRIORINSURANCEID, 
	WB_PriorInsuranceId AS WBPRIORINSURANCEID, 
	SESSIONID, 
	CARRIERNAMEOTHER, 
	EXPERIENCEMOD, 
	LINEOFBUSINESS, 
	o_NoPriorInsurance2 AS NOPRIORINSURANCE2, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),