WITH
SQ_WBEPLCoverageEmploymentPracticesLiabilityStage AS (
	SELECT
		WBEPLCoverageEmploymentPracticesLiabilityStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_EPL_CoverageEmploymentPracticesLiabilityId,
		SessionId,
		NumberOfFulltimeEmployees,
		NumberOfParttimeEmployees,
		NumberOfTempSeasonalLeasedEmployees,
		NumberOfIndependentContractors,
		NumberOfVolunteers,
		NumberOfEmployeesLabel,
		IncludeIndependentContractorsCoverage,
		IncludeVolunteersAsAdditonalInsureds,
		BusinessType,
		BusinessClassification,
		HigherLimitsRequired,
		ReinsurancePremium,
		DefenseCosts,
		ThirdPartyLiability,
		DAndOQuote,
		RetroactiveDate,
		PriorRetroactiveDate,
		PriorAggregateLimit,
		TotalNumberOfEmployees,
		SICFactor,
		HigherLimitMinimumValue
	FROM WBEPLCoverageEmploymentPracticesLiabilityStage
),
EXP_Metadata AS (
	SELECT
	WBEPLCoverageEmploymentPracticesLiabilityStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_EPL_CoverageEmploymentPracticesLiabilityId,
	SessionId,
	NumberOfFulltimeEmployees,
	NumberOfParttimeEmployees,
	NumberOfTempSeasonalLeasedEmployees,
	NumberOfIndependentContractors,
	NumberOfVolunteers,
	NumberOfEmployeesLabel,
	IncludeIndependentContractorsCoverage AS i_IncludeIndependentContractorsCoverage,
	IncludeVolunteersAsAdditonalInsureds AS i_IncludeVolunteersAsAdditonalInsureds,
	-- *INF*: DECODE(i_IncludeIndependentContractorsCoverage,'T',1,'F',0,NULL)
	DECODE(
	    i_IncludeIndependentContractorsCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeIndependentContractorsCoverage,
	-- *INF*: DECODE(i_IncludeVolunteersAsAdditonalInsureds,'T',1,'F',0,NULL)
	DECODE(
	    i_IncludeVolunteersAsAdditonalInsureds,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncludeVolunteersAsAdditonalInsureds,
	BusinessType,
	BusinessClassification,
	HigherLimitsRequired AS i_HigherLimitsRequired,
	-- *INF*: DECODE(i_HigherLimitsRequired,'T',1,'F',0,NULL)
	DECODE(
	    i_HigherLimitsRequired,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HigherLimitsRequired,
	ReinsurancePremium,
	DefenseCosts,
	ThirdPartyLiability,
	DAndOQuote AS i_DAndOQuote,
	-- *INF*: DECODE(i_DAndOQuote,'T',1,'F',0,NULL)
	DECODE(
	    i_DAndOQuote,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DAndOQuote,
	RetroactiveDate,
	PriorRetroactiveDate,
	PriorAggregateLimit,
	TotalNumberOfEmployees,
	SICFactor,
	HigherLimitMinimumValue,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBEPLCoverageEmploymentPracticesLiabilityStage
),
ArchWBEPLCoverageEmploymentPracticesLiabilityStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBEPLCoverageEmploymentPracticesLiabilityStage
	(ExtractDate, SourceSystemId, AuditId, WBEPLCoverageEmploymentPracticesLiabilityStageId, CoverageId, WB_EPL_CoverageEmploymentPracticesLiabilityId, SessionId, NumberOfFulltimeEmployees, NumberOfParttimeEmployees, NumberOfTempSeasonalLeasedEmployees, NumberOfIndependentContractors, NumberOfVolunteers, NumberOfEmployeesLabel, IncludeIndependentContractorsCoverage, IncludeVolunteersAsAdditonalInsureds, BusinessType, BusinessClassification, HigherLimitsRequired, ReinsurancePremium, DefenseCosts, ThirdPartyLiability, DAndOQuote, RetroactiveDate, PriorRetroactiveDate, PriorAggregateLimit, TotalNumberOfEmployees, SICFactor, HigherLimitMinimumValue)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBEPLCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGEID, 
	COVERAGEID, 
	WB_EPL_COVERAGEEMPLOYMENTPRACTICESLIABILITYID, 
	SESSIONID, 
	NUMBEROFFULLTIMEEMPLOYEES, 
	NUMBEROFPARTTIMEEMPLOYEES, 
	NUMBEROFTEMPSEASONALLEASEDEMPLOYEES, 
	NUMBEROFINDEPENDENTCONTRACTORS, 
	NUMBEROFVOLUNTEERS, 
	NUMBEROFEMPLOYEESLABEL, 
	o_IncludeIndependentContractorsCoverage AS INCLUDEINDEPENDENTCONTRACTORSCOVERAGE, 
	o_IncludeVolunteersAsAdditonalInsureds AS INCLUDEVOLUNTEERSASADDITONALINSUREDS, 
	BUSINESSTYPE, 
	BUSINESSCLASSIFICATION, 
	o_HigherLimitsRequired AS HIGHERLIMITSREQUIRED, 
	REINSURANCEPREMIUM, 
	DEFENSECOSTS, 
	THIRDPARTYLIABILITY, 
	o_DAndOQuote AS DANDOQUOTE, 
	RETROACTIVEDATE, 
	PRIORRETROACTIVEDATE, 
	PRIORAGGREGATELIMIT, 
	TOTALNUMBEROFEMPLOYEES, 
	SICFACTOR, 
	HIGHERLIMITMINIMUMVALUE
	FROM EXP_Metadata
),