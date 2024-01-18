WITH
SQ_WBCLCoverageDataCompromiseStage AS (
	SELECT
		WBCLCoverageDataCompromiseStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_CL_CoverageDataCompromiseId,
		SessionId,
		EligibilityQuestion,
		DefenseAndLiabilityCoverageIndicator,
		IncreaseAnnualAggregateLimitIndicator,
		IncreasedLimitQuestionOne,
		IncreasedLimitQuestionTwo,
		IncreasedLimitQuestionThree,
		IncreasedLimitQuestionFour,
		IncreasedLimitQuestionFive,
		IncreasedLimitQuestionSix,
		IncreasedLimitQuestionSeven,
		IncreasedLimitQuestionEight,
		IncreasedLimitQuestionNine,
		IncreasedLimitQuestionTen,
		IncreasedAnnualAggregateEligibleMessage,
		DataCompromiseUnavailableMessage,
		AnnualAggregateStaticText,
		ResponseExpensesDeductible,
		AnyOnePersonalDataCompromiseStaticText,
		DefenseAndLiabilityDeductible,
		IncreasedAnnualAggregateIneligibleMessage
	FROM WBCLCoverageDataCompromiseStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBCLCoverageDataCompromiseStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_CL_CoverageDataCompromiseId,
	SessionId,
	EligibilityQuestion,
	DefenseAndLiabilityCoverageIndicator,
	-- *INF*: DECODE(DefenseAndLiabilityCoverageIndicator,'T',1,'F',0,NULL)
	DECODE(
	    DefenseAndLiabilityCoverageIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DefenseAndLiabilityCoverageIndicator,
	IncreaseAnnualAggregateLimitIndicator,
	-- *INF*: DECODE(IncreaseAnnualAggregateLimitIndicator,'T',1,'F',0,NULL)
	DECODE(
	    IncreaseAnnualAggregateLimitIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncreaseAnnualAggregateLimitIndicator,
	IncreasedLimitQuestionOne,
	IncreasedLimitQuestionTwo,
	IncreasedLimitQuestionThree,
	IncreasedLimitQuestionFour,
	IncreasedLimitQuestionFive,
	IncreasedLimitQuestionSix,
	IncreasedLimitQuestionSeven,
	IncreasedLimitQuestionEight,
	IncreasedLimitQuestionNine,
	IncreasedLimitQuestionTen,
	IncreasedAnnualAggregateEligibleMessage,
	DataCompromiseUnavailableMessage,
	AnnualAggregateStaticText,
	ResponseExpensesDeductible,
	AnyOnePersonalDataCompromiseStaticText,
	DefenseAndLiabilityDeductible,
	IncreasedAnnualAggregateIneligibleMessage
	FROM SQ_WBCLCoverageDataCompromiseStage
),
ArchWBCLCoverageDataCompromiseStage AS (
	INSERT INTO ArchWBCLCoverageDataCompromiseStage
	(ExtractDate, SourceSystemId, AuditId, WBCLCoverageDataCompromiseStageId, CoverageId, WB_CL_CoverageDataCompromiseId, SessionId, EligibilityQuestion, DefenseAndLiabilityCoverageIndicator, IncreaseAnnualAggregateLimitIndicator, IncreasedLimitQuestionOne, IncreasedLimitQuestionTwo, IncreasedLimitQuestionThree, IncreasedLimitQuestionFour, IncreasedLimitQuestionFive, IncreasedLimitQuestionSix, IncreasedLimitQuestionSeven, IncreasedLimitQuestionEight, IncreasedLimitQuestionNine, IncreasedLimitQuestionTen, IncreasedAnnualAggregateEligibleMessage, DataCompromiseUnavailableMessage, AnnualAggregateStaticText, ResponseExpensesDeductible, AnyOnePersonalDataCompromiseStaticText, DefenseAndLiabilityDeductible, IncreasedAnnualAggregateIneligibleMessage)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCLCOVERAGEDATACOMPROMISESTAGEID, 
	COVERAGEID, 
	WB_CL_COVERAGEDATACOMPROMISEID, 
	SESSIONID, 
	ELIGIBILITYQUESTION, 
	o_DefenseAndLiabilityCoverageIndicator AS DEFENSEANDLIABILITYCOVERAGEINDICATOR, 
	o_IncreaseAnnualAggregateLimitIndicator AS INCREASEANNUALAGGREGATELIMITINDICATOR, 
	INCREASEDLIMITQUESTIONONE, 
	INCREASEDLIMITQUESTIONTWO, 
	INCREASEDLIMITQUESTIONTHREE, 
	INCREASEDLIMITQUESTIONFOUR, 
	INCREASEDLIMITQUESTIONFIVE, 
	INCREASEDLIMITQUESTIONSIX, 
	INCREASEDLIMITQUESTIONSEVEN, 
	INCREASEDLIMITQUESTIONEIGHT, 
	INCREASEDLIMITQUESTIONNINE, 
	INCREASEDLIMITQUESTIONTEN, 
	INCREASEDANNUALAGGREGATEELIGIBLEMESSAGE, 
	DATACOMPROMISEUNAVAILABLEMESSAGE, 
	ANNUALAGGREGATESTATICTEXT, 
	RESPONSEEXPENSESDEDUCTIBLE, 
	ANYONEPERSONALDATACOMPROMISESTATICTEXT, 
	DEFENSEANDLIABILITYDEDUCTIBLE, 
	INCREASEDANNUALAGGREGATEINELIGIBLEMESSAGE
	FROM EXP_Metadata
),