WITH
SQ_WBNDOCoverageDirectorsAndOfficersNFPStage AS (
	SELECT
		WBNDOCoverageDirectorsAndOfficersNFPStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_NDO_CoverageDirectorsAndOfficersNFPId,
		SessionId,
		CompositeModifier,
		RiskTypeClassCode,
		HazardGroup,
		HigherLimitRequired,
		ReinsurancePremium,
		RiskType,
		QuotingAnEPLI,
		DefenseCosts,
		TotalAssets,
		ARate,
		RetroactiveDate,
		ExtendedReportingPeriodEndorsement,
		ExtendedReportingPeriod,
		TaxCodeCharter,
		TaxCodeCharterDescription,
		PriorRetroactiveDate
	FROM WBNDOCoverageDirectorsAndOfficersNFPStage
),
EXP_Set_MetaData AS (
	SELECT
	WBNDOCoverageDirectorsAndOfficersNFPStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_NDO_CoverageDirectorsAndOfficersNFPId,
	SessionId,
	CompositeModifier,
	RiskTypeClassCode,
	HazardGroup,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBNDOCoverageDirectorsAndOfficersNFPStage
),
ArchWBNDOCoverageDirectorsAndOfficersNFPStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBNDOCoverageDirectorsAndOfficersNFPStage
	(ExtractDate, SourceSystemId, WBNDOCoverageDirectorsAndOfficersNFPStageId, CoverageId, WB_NDO_CoverageDirectorsAndOfficersNFPId, SessionId, CompositeModifier, RiskTypeClassCode, HazardGroup, HigherLimitRequired, ReinsurancePremium, RiskType, QuotingAnEPLI, DefenseCosts, TotalAssets, ARate, RetroactiveDate, ExtendedReportingPeriodEndorsement, ExtendedReportingPeriod, TaxCodeCharter, TaxCodeCharterDescription, PriorRetroactiveDate)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	WBNDOCOVERAGEDIRECTORSANDOFFICERSNFPSTAGEID, 
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