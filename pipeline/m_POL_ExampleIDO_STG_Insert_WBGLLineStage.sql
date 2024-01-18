WITH
SQ_WB_GL_Line AS (
	With CTE_WBGLLine(sessionID) as 
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT  WBGL.GL_LineId
	      ,WBGL.WB_GL_LineId
	      ,WBGL.SessionId
	      ,WBGL.QuotedScheduleMod
	      ,WBGL.LossSensitiveCommission
	      ,WBGL.StudentGroupAccidentPolicy
	      ,WBGL.HiredAndNonOwnedAuto
	      ,WBGL.AbuseMolestationCoverage
	      ,WBGL.WaterActivities
	      ,WBGL.Lifeguard
	      ,WBGL.TypeOfWaterActivities
	      ,WBGL.OtherDescription
	      ,WBGL.WhereWaterActivitiesOccur
	      ,WBGL.EmployeeBenefitLiability
	      ,WBGL.EmployeeBenefitLiabilityRetroDate
	      ,WBGL.EmploymentPracticesLiability
	      ,WBGL.EmploymentPracticesNumberOfEmployees
	      ,WBGL.EmploymentPracticesRetroDate
	      ,WBGL.StopGapEmployersLiability
	      ,WBGL.EmploymentPracticesNumberOfEmployeesDisplay
	      ,WBGL.StopGapNumberOfEmployeesDisplay
	      ,WBGL.EmployeePracticesFlatCharge
	      ,WBGL.WaterActivitiesCaption
	      ,WBGL.Premium
	      ,WBGL.PremiumWritten
	      ,WBGL.PremiumChange
	      ,WBGL.CheckWB1372
	      ,WBGL.RetroDate2
	      ,WBGL.ReinsuranceApplies
	      ,WBGL.ReinsurancePremium
	      ,WBGL.ReinsuranceAppliesCGLMessage
	      ,WBGL.ReinsurancePremiumMessage
	      ,WBGL.PremOpBIPDDeductible
	      ,WBGL.SplitBIPDDeductible
	      ,WBGL.ProductsBIPDDeductible
	      ,WBGL.ProductWithdrawalCutoffDate
	      ,WBGL.FringeFactor
	      ,WBGL.SGAFactorForRMF
	      ,WBGL.AuditablePremium
	      ,WBGL.WB1482TotalPremium
	      ,WBGL.AnnotationForPolicyPerOccurenceLimit
	      ,WBGL.PolicyAggregateLimitAnnotation
	      ,WBGL.OCPTotalPremium
	      ,WBGL.RRTotalPremium
	      ,WBGL.ExpectedLossRatioLookup
	      ,WBGL.DeductibleBIPerClaim
	      ,WBGL.DeductibleBIPerOccurrence
	      ,WBGL.DeductiblePDPerClaim
	      ,WBGL.DeductiblePDPerOccurrence
	      FROM WB_GL_Line WBGL
	inner join CTE_WBGLLine CTE on WBGL.sessionID = CTE.sessionID
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_METADATA AS (
	SELECT
	SYSDATE AS O_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS O_SourceSystemID,
	GL_LineId,
	WB_GL_LineId,
	SessionId,
	QuotedScheduleMod,
	LossSensitiveCommission,
	StudentGroupAccidentPolicy,
	HiredAndNonOwnedAuto,
	AbuseMolestationCoverage,
	WaterActivities,
	Lifeguard,
	TypeOfWaterActivities,
	OtherDescription,
	WhereWaterActivitiesOccur,
	EmployeeBenefitLiability,
	EmployeeBenefitLiabilityRetroDate,
	EmploymentPracticesLiability,
	EmploymentPracticesNumberOfEmployees,
	EmploymentPracticesRetroDate,
	StopGapEmployersLiability,
	EmploymentPracticesNumberOfEmployeesDisplay,
	StopGapNumberOfEmployeesDisplay,
	EmployeePracticesFlatCharge,
	WaterActivitiesCaption,
	Premium,
	PremiumWritten,
	PremiumChange,
	CheckWB1372,
	RetroDate2,
	ReinsuranceApplies,
	ReinsurancePremium,
	ReinsuranceAppliesCGLMessage,
	ReinsurancePremiumMessage,
	PremOpBIPDDeductible,
	SplitBIPDDeductible,
	ProductsBIPDDeductible,
	ProductWithdrawalCutoffDate,
	FringeFactor,
	SGAFactorForRMF,
	AuditablePremium,
	WB1482TotalPremium,
	AnnotationForPolicyPerOccurenceLimit,
	PolicyAggregateLimitAnnotation,
	OCPTotalPremium,
	RRTotalPremium,
	ExpectedLossRatioLookup,
	DeductibleBIPerClaim,
	DeductibleBIPerOccurrence,
	DeductiblePDPerClaim,
	DeductiblePDPerOccurrence
	FROM SQ_WB_GL_Line
),
WBGLLineStage AS (
	TRUNCATE TABLE WBGLLineStage;
	INSERT INTO WBGLLineStage
	(ExtractDate, SourceSystemid, GL_LineId, WB_GL_LineId, SessionId, QuotedScheduleMod, LossSensitiveCommission, StudentGroupAccidentPolicy, HiredAndNonOwnedAuto, AbuseMolestationCoverage, WaterActivities, Lifeguard, TypeOfWaterActivities, OtherDescription, WhereWaterActivitiesOccur, EmployeeBenefitLiability, EmployeeBenefitLiabilityRetroDate, EmploymentPracticesLiability, EmploymentPracticesNumberOfEmployees, EmploymentPracticesRetroDate, StopGapEmployersLiability, EmploymentPracticesNumberOfEmployeesDisplay, StopGapNumberOfEmployeesDisplay, EmployeePracticesFlatCharge, WaterActivitiesCaption, Premium, PremiumWritten, PremiumChange, CheckWB1372, RetroDate2, ReinsuranceApplies, ReinsurancePremium, ReinsuranceAppliesCGLMessage, ReinsurancePremiumMessage, PremOpBIPDDeductible, SplitBIPDDeductible, ProductsBIPDDeductible, ProductWithdrawalCutoffDate, FringeFactor, SGAFactorForRMF, AuditablePremium, WB1482TotalPremium, AnnotationForPolicyPerOccurenceLimit, PolicyAggregateLimitAnnotation, OCPTotalPremium, RRTotalPremium, ExpectedLossRatioLookup, DeductibleBIPerClaim, DeductibleBIPerOccurrence, DeductiblePDPerClaim, DeductiblePDPerOccurrence)
	SELECT 
	O_ExtractDate AS EXTRACTDATE, 
	O_SourceSystemID AS SOURCESYSTEMID, 
	GL_LINEID, 
	WB_GL_LINEID, 
	SESSIONID, 
	QUOTEDSCHEDULEMOD, 
	LOSSSENSITIVECOMMISSION, 
	STUDENTGROUPACCIDENTPOLICY, 
	HIREDANDNONOWNEDAUTO, 
	ABUSEMOLESTATIONCOVERAGE, 
	WATERACTIVITIES, 
	LIFEGUARD, 
	TYPEOFWATERACTIVITIES, 
	OTHERDESCRIPTION, 
	WHEREWATERACTIVITIESOCCUR, 
	EMPLOYEEBENEFITLIABILITY, 
	EMPLOYEEBENEFITLIABILITYRETRODATE, 
	EMPLOYMENTPRACTICESLIABILITY, 
	EMPLOYMENTPRACTICESNUMBEROFEMPLOYEES, 
	EMPLOYMENTPRACTICESRETRODATE, 
	STOPGAPEMPLOYERSLIABILITY, 
	EMPLOYMENTPRACTICESNUMBEROFEMPLOYEESDISPLAY, 
	STOPGAPNUMBEROFEMPLOYEESDISPLAY, 
	EMPLOYEEPRACTICESFLATCHARGE, 
	WATERACTIVITIESCAPTION, 
	PREMIUM, 
	PREMIUMWRITTEN, 
	PREMIUMCHANGE, 
	CHECKWB1372, 
	RETRODATE2, 
	REINSURANCEAPPLIES, 
	REINSURANCEPREMIUM, 
	REINSURANCEAPPLIESCGLMESSAGE, 
	REINSURANCEPREMIUMMESSAGE, 
	PREMOPBIPDDEDUCTIBLE, 
	SPLITBIPDDEDUCTIBLE, 
	PRODUCTSBIPDDEDUCTIBLE, 
	PRODUCTWITHDRAWALCUTOFFDATE, 
	FRINGEFACTOR, 
	SGAFACTORFORRMF, 
	AUDITABLEPREMIUM, 
	WB1482TOTALPREMIUM, 
	ANNOTATIONFORPOLICYPEROCCURENCELIMIT, 
	POLICYAGGREGATELIMITANNOTATION, 
	OCPTOTALPREMIUM, 
	RRTOTALPREMIUM, 
	EXPECTEDLOSSRATIOLOOKUP, 
	DEDUCTIBLEBIPERCLAIM, 
	DEDUCTIBLEBIPEROCCURRENCE, 
	DEDUCTIBLEPDPERCLAIM, 
	DEDUCTIBLEPDPEROCCURRENCE
	FROM EXP_METADATA
),