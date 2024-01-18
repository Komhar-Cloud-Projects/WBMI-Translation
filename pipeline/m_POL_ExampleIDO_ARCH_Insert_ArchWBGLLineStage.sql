WITH
SQ_WBGLLineStage AS (
	SELECT
		WBGLLineStageId,
		ExtractDate,
		SourceSystemid,
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
	FROM WBGLLineStage
),
EXP_METADATA AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS O_AuditID,
	WBGLLineStageId,
	ExtractDate,
	SourceSystemid,
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
	FROM SQ_WBGLLineStage
),
ArchWBGLLineStage AS (
	INSERT INTO ArchWBGLLineStage
	(ExtractDate, SourceSystemId, AuditId, WBGLLineStageId, GL_LineId, WB_GL_LineId, SessionId, QuotedScheduleMod, LossSensitiveCommission, StudentGroupAccidentPolicy, HiredAndNonOwnedAuto, AbuseMolestationCoverage, WaterActivities, Lifeguard, TypeOfWaterActivities, OtherDescription, WhereWaterActivitiesOccur, EmployeeBenefitLiability, EmployeeBenefitLiabilityRetroDate, EmploymentPracticesLiability, EmploymentPracticesNumberOfEmployees, EmploymentPracticesRetroDate, StopGapEmployersLiability, EmploymentPracticesNumberOfEmployeesDisplay, StopGapNumberOfEmployeesDisplay, EmployeePracticesFlatCharge, WaterActivitiesCaption, Premium, PremiumWritten, PremiumChange, CheckWB1372, RetroDate2, ReinsuranceApplies, ReinsurancePremium, ReinsuranceAppliesCGLMessage, ReinsurancePremiumMessage, PremOpBIPDDeductible, SplitBIPDDeductible, ProductsBIPDDeductible, ProductWithdrawalCutoffDate, FringeFactor, SGAFactorForRMF, AuditablePremium, WB1482TotalPremium, AnnotationForPolicyPerOccurenceLimit, PolicyAggregateLimitAnnotation, OCPTotalPremium, RRTotalPremium, ExpectedLossRatioLookup, DeductibleBIPerClaim, DeductibleBIPerOccurrence, DeductiblePDPerClaim, DeductiblePDPerOccurrence)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	O_AuditID AS AUDITID, 
	WBGLLINESTAGEID, 
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