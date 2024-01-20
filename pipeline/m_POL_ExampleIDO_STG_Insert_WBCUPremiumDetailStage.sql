WITH
SQ_WB_CU_PremiumDetail AS (
	WITH cte_WBCUPremiumDetail(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WB_CU_PremiumDetailId, 
	X.SessionId, 
	X.Type, 
	X.Indicator, 
	X.Million, 
	X.ReinsurerForCL, 
	X.ReinsurerForNSI, 
	X.CommissionRate, 
	X.PercentCeded, 
	X.Override, 
	X.RevisedFinalPremium, 
	X.Include, 
	X.Exclude, 
	X.CertificateReceived, 
	X.ReinsuranceEffectiveDate, 
	X.ReinsuranceExpirationDate, 
	X.FinalPremium, 
	X.FinalPremiumWritten, 
	X.FinalPremiumChange, 
	X.ReinsurerPremium, 
	X.ReinsurerFinalPremiumDisplay, 
	X.TypeDuplicate
	FROM
	WB_CU_PremiumDetail X
	inner join
	cte_WBCUPremiumDetail Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	WB_CU_PremiumDetailId,
	SessionId,
	Type,
	Indicator,
	-- *INF*: DECODE(Indicator, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Indicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Indicator,
	Million,
	ReinsurerForCL,
	ReinsurerForNSI,
	CommissionRate,
	PercentCeded,
	Override,
	-- *INF*: DECODE(Override, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Override,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Override,
	RevisedFinalPremium,
	Include,
	Exclude,
	CertificateReceived,
	ReinsuranceEffectiveDate,
	ReinsuranceExpirationDate,
	FinalPremium,
	FinalPremiumWritten,
	FinalPremiumChange,
	ReinsurerPremium,
	ReinsurerFinalPremiumDisplay,
	TypeDuplicate,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CU_PremiumDetail
),
WBCUPremiumDetailStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCUPremiumDetailStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCUPremiumDetailStage
	(LineId, WBCUPremiumDetailId, SessionId, Type, Indicator, Million, ReinsurerForCL, ReinsurerForNSI, CommissionRate, PercentCeded, Override, RevisedFinalPremium, Include, Exclude, CertificateReceived, ReinsuranceEffectiveDate, ReinsuranceExpirationDate, FinalPremium, FinalPremiumWritten, FinalPremiumChange, ReinsurerPremium, ReinsurerFinalPremiumDisplay, TypeDuplicate, ExtractDate, SourceSystemId)
	SELECT 
	LINEID, 
	WB_CU_PremiumDetailId AS WBCUPREMIUMDETAILID, 
	SESSIONID, 
	TYPE, 
	o_Indicator AS INDICATOR, 
	MILLION, 
	REINSURERFORCL, 
	REINSURERFORNSI, 
	COMMISSIONRATE, 
	PERCENTCEDED, 
	o_Override AS OVERRIDE, 
	REVISEDFINALPREMIUM, 
	INCLUDE, 
	EXCLUDE, 
	CERTIFICATERECEIVED, 
	REINSURANCEEFFECTIVEDATE, 
	REINSURANCEEXPIRATIONDATE, 
	FINALPREMIUM, 
	FINALPREMIUMWRITTEN, 
	FINALPREMIUMCHANGE, 
	REINSURERPREMIUM, 
	REINSURERFINALPREMIUMDISPLAY, 
	TYPEDUPLICATE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),