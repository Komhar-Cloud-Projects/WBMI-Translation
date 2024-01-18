WITH
SQ_WB_EPL_CoverageEmploymentPracticesLiability AS (
	WITH cte_WBEPLCoverageEmploymentPracticesLiability(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_EPL_CoverageEmploymentPracticesLiabilityId, 
	X.SessionId, 
	X.NumberOfFulltimeEmployees, 
	X.NumberOfParttimeEmployees, 
	X.NumberOfTempSeasonalLeasedEmployees, 
	X.NumberOfIndependentContractors, 
	X.NumberOfVolunteers, 
	X.NumberOfEmployeesLabel, 
	X.IncludeIndependentContractorsCoverage, 
	X.IncludeVolunteersAsAdditonalInsureds, 
	X.BusinessClassification, 
	X.HigherLimitsRequired, 
	X.ReinsurancePremium, 
	X.DefenseCosts, 
	X.ThirdPartyLiability, 
	X.DAndOQuote, 
	X.RetroactiveDate, 
	X.PriorRetroactiveDate, 
	X.PriorAggregateLimit, 
	X.TotalNumberOfEmployees, 
	X.SICFactor, 
	X.HigherLimitMinimumValue 
	FROM
	WB_EPL_CoverageEmploymentPracticesLiability X
	inner join
	cte_WBEPLCoverageEmploymentPracticesLiability Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_EPL_CoverageEmploymentPracticesLiability
),
WBEPLCoverageEmploymentPracticesLiabilityStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBEPLCoverageEmploymentPracticesLiabilityStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBEPLCoverageEmploymentPracticesLiabilityStage
	(ExtractDate, SourceSystemId, CoverageId, WB_EPL_CoverageEmploymentPracticesLiabilityId, SessionId, NumberOfFulltimeEmployees, NumberOfParttimeEmployees, NumberOfTempSeasonalLeasedEmployees, NumberOfIndependentContractors, NumberOfVolunteers, NumberOfEmployeesLabel, IncludeIndependentContractorsCoverage, IncludeVolunteersAsAdditonalInsureds, BusinessType, BusinessClassification, HigherLimitsRequired, ReinsurancePremium, DefenseCosts, ThirdPartyLiability, DAndOQuote, RetroactiveDate, PriorRetroactiveDate, PriorAggregateLimit, TotalNumberOfEmployees, SICFactor, HigherLimitMinimumValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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