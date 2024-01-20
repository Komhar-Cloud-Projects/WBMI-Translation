WITH
SQ_WB_NDO_CoverageDirectorsAndOfficersNFP AS (
	WITH cte_WBNDOCoverageDirectorsAndOfficersNFP(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_NDO_CoverageDirectorsAndOfficersNFPId, 
	X.SessionId, 
	X.CompositeModifier, 
	X.HigherLimitRequired, 
	X.ReinsurancePremium, 
	X.RiskType, 
	X.QuotingAnEPLI, 
	X.DefenseCosts, 
	X.TotalAssets, 
	X.ARate, 
	X.RetroactiveDate, 
	X.ExtendedReportingPeriodEndorsement, 
	X.ExtendedReportingPeriod, 
	X.TaxCodeCharter, 
	X.TaxCodeCharterDescription, 
	X.PriorRetroactiveDate, 
	X.RiskTypeClassCode, 
	X.HazardGroup 
	FROM
	WB_NDO_CoverageDirectorsAndOfficersNFP X
	inner join
	cte_WBNDOCoverageDirectorsAndOfficersNFP Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Set_MetaData AS (
	SELECT
	CoverageId,
	WB_NDO_CoverageDirectorsAndOfficersNFPId,
	SessionId,
	CompositeModifier,
	HigherLimitRequired AS i_HigherLimitRequired,
	-- *INF*: DECODE(i_HigherLimitRequired,'T','1','F','0',NULL)
	DECODE(
	    i_HigherLimitRequired,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_HigherLimitRequired,
	ReinsurancePremium,
	RiskType,
	QuotingAnEPLI AS i_QuotingAnEPLI,
	-- *INF*: DECODE(i_QuotingAnEPLI,'T','1','F','0',NULL)
	DECODE(
	    i_QuotingAnEPLI,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_QuotingAnEPLI,
	DefenseCosts,
	TotalAssets,
	ARate,
	RetroactiveDate,
	ExtendedReportingPeriodEndorsement AS i_ExtendedReportingPeriodEndorsement,
	-- *INF*: DECODE(i_ExtendedReportingPeriodEndorsement,'T','1','F','0',NULL)
	DECODE(
	    i_ExtendedReportingPeriodEndorsement,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ExtendedReportingPeriodEndorsement,
	ExtendedReportingPeriod,
	TaxCodeCharter,
	TaxCodeCharterDescription,
	PriorRetroactiveDate,
	RiskTypeClassCode,
	HazardGroup,
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_NDO_CoverageDirectorsAndOfficersNFP
),
WBNDOCoverageDirectorsAndOfficersNFPStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBNDOCoverageDirectorsAndOfficersNFPStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBNDOCoverageDirectorsAndOfficersNFPStage
	(ExtractDate, SourceSystemId, CoverageId, WB_NDO_CoverageDirectorsAndOfficersNFPId, SessionId, CompositeModifier, RiskTypeClassCode, HazardGroup, HigherLimitRequired, ReinsurancePremium, RiskType, QuotingAnEPLI, DefenseCosts, TotalAssets, ARate, RetroactiveDate, ExtendedReportingPeriodEndorsement, ExtendedReportingPeriod, TaxCodeCharter, TaxCodeCharterDescription, PriorRetroactiveDate)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_NDO_COVERAGEDIRECTORSANDOFFICERSNFPID, 
	SESSIONID, 
	COMPOSITEMODIFIER, 
	RISKTYPECLASSCODE, 
	HAZARDGROUP, 
	o_HigherLimitRequired AS HIGHERLIMITREQUIRED, 
	REINSURANCEPREMIUM, 
	RISKTYPE, 
	o_QuotingAnEPLI AS QUOTINGANEPLI, 
	DEFENSECOSTS, 
	TOTALASSETS, 
	ARATE, 
	RETROACTIVEDATE, 
	o_ExtendedReportingPeriodEndorsement AS EXTENDEDREPORTINGPERIODENDORSEMENT, 
	EXTENDEDREPORTINGPERIOD, 
	TAXCODECHARTER, 
	TAXCODECHARTERDESCRIPTION, 
	PRIORRETROACTIVEDATE
	FROM EXP_Set_MetaData
),