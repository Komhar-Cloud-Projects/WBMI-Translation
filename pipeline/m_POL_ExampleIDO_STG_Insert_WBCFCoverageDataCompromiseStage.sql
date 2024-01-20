WITH
SQ_WB_CF_CoverageDataCompromise AS (
	WITH cte_WBCFCoverageDataCompromise(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_CF_CoverageDataCompromiseId, 
	X.SessionId, 
	X.ProgramType, 
	X.ProgramQuestionOne, 
	X.ProgramQuestionTwo, 
	X.ProgramQuestionThree, 
	X.ProgramQuestionFour, 
	X.AssistedLivingEligibilityQuestion, 
	X.RatingTierForProgramtype, 
	X.BillingLOB, 
	X.CommissionPlanId, 
	X.IsBillingSubline, 
	X.ParentBillingLOB, 
	X.PurePremium, 
	X.TransactionCommissionType, 
	X.TransactionCommissionValue, 
	X.TransactionFinalCommissionValue, 
	X.WB_CL_CoverageDataCompromiseId 
	FROM
	WB_CF_CoverageDataCompromise X
	inner join
	cte_WBCFCoverageDataCompromise Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	WB_CF_CoverageDataCompromiseId,
	SessionId,
	ProgramType,
	ProgramQuestionOne,
	ProgramQuestionTwo,
	ProgramQuestionThree,
	ProgramQuestionFour,
	AssistedLivingEligibilityQuestion,
	RatingTierForProgramtype,
	BillingLOB,
	CommissionPlanId,
	IsBillingSubline,
	ParentBillingLOB,
	PurePremium,
	TransactionCommissionType,
	TransactionCommissionValue,
	TransactionFinalCommissionValue,
	WB_CL_CoverageDataCompromiseId
	FROM SQ_WB_CF_CoverageDataCompromise
),
WBCFCoverageDataCompromiseStage AS (
	TRUNCATE TABLE WBCFCoverageDataCompromiseStage;
	INSERT INTO WBCFCoverageDataCompromiseStage
	(ExtractDate, SourceSystemId, CoverageId, WB_CF_CoverageDataCompromiseId, SessionId, ProgramType, ProgramQuestionOne, ProgramQuestionTwo, ProgramQuestionThree, ProgramQuestionFour, AssistedLivingEligibilityQuestion, RatingTierForProgramtype, BillingLOB, CommissionPlanId, IsBillingSubline, ParentBillingLOB, PurePremium, TransactionCommissionType, TransactionCommissionValue, TransactionFinalCommissionValue, WB_CL_CoverageDataCompromiseId)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_CF_COVERAGEDATACOMPROMISEID, 
	SESSIONID, 
	PROGRAMTYPE, 
	PROGRAMQUESTIONONE, 
	PROGRAMQUESTIONTWO, 
	PROGRAMQUESTIONTHREE, 
	PROGRAMQUESTIONFOUR, 
	ASSISTEDLIVINGELIGIBILITYQUESTION, 
	RATINGTIERFORPROGRAMTYPE, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	PUREPREMIUM, 
	TRANSACTIONCOMMISSIONTYPE, 
	TRANSACTIONCOMMISSIONVALUE, 
	TRANSACTIONFINALCOMMISSIONVALUE, 
	WB_CL_COVERAGEDATACOMPROMISEID
	FROM EXP_Metadata
),