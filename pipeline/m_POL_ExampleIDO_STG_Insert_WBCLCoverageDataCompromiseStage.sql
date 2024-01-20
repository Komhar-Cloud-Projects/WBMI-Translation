WITH
SQ_WB_CL_CoverageDataCompromise AS (
	WITH cte_WBCLCoverageDataCompromise(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_CL_CoverageDataCompromiseId, 
	X.SessionId, 
	X.EligibilityQuestion, 
	X.DefenseAndLiabilityCoverageIndicator, 
	X.IncreaseAnnualAggregateLimitIndicator, 
	X.IncreasedLimitQuestionOne, 
	X.IncreasedLimitQuestionTwo, 
	X.IncreasedLimitQuestionThree, 
	X.IncreasedLimitQuestionFour, 
	X.IncreasedLimitQuestionFive, 
	X.IncreasedLimitQuestionSix, 
	X.IncreasedLimitQuestionSeven, 
	X.IncreasedLimitQuestionEight, 
	X.IncreasedLimitQuestionNine, 
	X.IncreasedLimitQuestionTen, 
	X.IncreasedAnnualAggregateEligibleMessage, 
	X.DataCompromiseUnavailableMessage, 
	X.AnnualAggregateStaticText, 
	X.ResponseExpensesDeductible, 
	X.AnyOnePersonalDataCompromiseStaticText, 
	X.DefenseAndLiabilityDeductible, 
	X.IncreasedAnnualAggregateIneligibleMessage 
	FROM
	WB_CL_CoverageDataCompromise X
	inner join
	cte_WBCLCoverageDataCompromise Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	FROM SQ_WB_CL_CoverageDataCompromise
),
WBCLCoverageDataCompromiseStage AS (
	TRUNCATE TABLE WBCLCoverageDataCompromiseStage;
	INSERT INTO WBCLCoverageDataCompromiseStage
	(ExtractDate, SourceSystemId, CoverageId, WB_CL_CoverageDataCompromiseId, SessionId, EligibilityQuestion, DefenseAndLiabilityCoverageIndicator, IncreaseAnnualAggregateLimitIndicator, IncreasedLimitQuestionOne, IncreasedLimitQuestionTwo, IncreasedLimitQuestionThree, IncreasedLimitQuestionFour, IncreasedLimitQuestionFive, IncreasedLimitQuestionSix, IncreasedLimitQuestionSeven, IncreasedLimitQuestionEight, IncreasedLimitQuestionNine, IncreasedLimitQuestionTen, IncreasedAnnualAggregateEligibleMessage, DataCompromiseUnavailableMessage, AnnualAggregateStaticText, ResponseExpensesDeductible, AnyOnePersonalDataCompromiseStaticText, DefenseAndLiabilityDeductible, IncreasedAnnualAggregateIneligibleMessage)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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