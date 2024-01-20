WITH
SQ_WB_WC_CoverageManualPremium AS (
	WITH cte_WBWCCoverageManualPremium(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WC_CoverageManualPremiumId, 
	X.WB_WC_CoverageManualPremiumId, 
	X.SessionId, 
	X.ConsentToRate, 
	X.CurrentRate, 
	X.RateOverride 
	FROM  
	WB_WC_CoverageManualPremium X
	inner join
	cte_WBWCCoverageManualPremium Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WC_CoverageManualPremiumId AS i_WC_CoverageManualPremiumId,
	WB_WC_CoverageManualPremiumId AS i_WB_WC_CoverageManualPremiumId,
	SessionId AS i_SessionId,
	ConsentToRate AS i_ConsentToRate,
	CurrentRate AS i_CurrentRate,
	RateOverride AS i_RateOverride,
	i_WC_CoverageManualPremiumId AS o_WC_CoverageManualPremiumId,
	i_WB_WC_CoverageManualPremiumId AS o_WB_WC_CoverageManualPremiumId,
	i_SessionId AS o_SessionId,
	-- *INF*: DECODE(i_ConsentToRate,'T',1,'F',0,NULL)
	DECODE(
	    i_ConsentToRate,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ConsentToRate,
	i_CurrentRate AS o_CurrentRate,
	i_RateOverride AS o_RateOverride,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_WC_CoverageManualPremium
),
WBWCCoverageManualPremiumStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBWCCoverageManualPremiumStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBWCCoverageManualPremiumStage
	(ExtractDate, SourceSystemId, WCCoverageManualPremiumId, WBWCCoverageManualPremiumId, SessionId, ConsentToRate, CurrentRate, RateOverride)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_WC_CoverageManualPremiumId AS WCCOVERAGEMANUALPREMIUMID, 
	o_WB_WC_CoverageManualPremiumId AS WBWCCOVERAGEMANUALPREMIUMID, 
	o_SessionId AS SESSIONID, 
	o_ConsentToRate AS CONSENTTORATE, 
	o_CurrentRate AS CURRENTRATE, 
	o_RateOverride AS RATEOVERRIDE
	FROM EXP_Metadata
),