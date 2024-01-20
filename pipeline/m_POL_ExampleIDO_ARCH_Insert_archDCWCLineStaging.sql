WITH
SQ_DCWCLineStaging AS (
	SELECT
		LineId,
		WC_LineId,
		SessionId,
		Id,
		AdmiraltyMinimumPremiumMaximumState,
		AnniversaryRating,
		AnniversaryRatingDate,
		AnyARDIndicator,
		CommissionPercentage,
		Description,
		DescriptionOverride,
		EmployersLiabilityIncreasedLimitsMaximumState,
		ExpenseConstantMaximumState,
		ExperienceModType,
		ExperienceRated,
		FELAMinimumPremiumMaximumState,
		InstallmentType,
		InvalidAdmiraltyLimitsIndicator,
		InvalidELLimitsIndicator,
		InvalidFELALimitsIndicator,
		MinimumPremiumMaximumState,
		PeriodDate,
		PolicyRatingType,
		PolicyType,
		PrimaryLocationState,
		ProrateExpenseConstantIndicator,
		ProrateMinimumPremiumIndicator,
		RatingPlan,
		ValidAdmiraltyLimitsIndicator,
		ValidELLimitsIndicator,
		ValidFELALimitsIndicator,
		WaiverOfSubrogationMinimumPremiumMaximumState,
		WithoutWorkersCompensation,
		WrapUpPolicy,
		ExtractDate,
		SourceSystemId,
		CombinedPolicyPremium
	FROM DCWCLineStaging
),
EXP_Metadata AS (
	SELECT
	LineId,
	WC_LineId,
	SessionId,
	Id,
	AdmiraltyMinimumPremiumMaximumState,
	AnniversaryRating AS i_AnniversaryRating,
	-- *INF*: DECODE(i_AnniversaryRating,'T',1,'F',0,NULL)
	DECODE(
	    i_AnniversaryRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AnniversaryRating,
	AnniversaryRatingDate,
	AnyARDIndicator AS i_AnyARDIndicator,
	-- *INF*: DECODE(i_AnyARDIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_AnyARDIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AnyARDIndicator,
	CommissionPercentage,
	Description,
	DescriptionOverride,
	EmployersLiabilityIncreasedLimitsMaximumState,
	ExpenseConstantMaximumState,
	ExperienceModType,
	ExperienceRated AS i_ExperienceRated,
	-- *INF*: DECODE(i_ExperienceRated,'T',1,'F',0,NULL)
	DECODE(
	    i_ExperienceRated,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExperienceRated,
	FELAMinimumPremiumMaximumState,
	InstallmentType,
	InvalidAdmiraltyLimitsIndicator AS i_InvalidAdmiraltyLimitsIndicator,
	-- *INF*: DECODE(i_InvalidAdmiraltyLimitsIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_InvalidAdmiraltyLimitsIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_InvalidAdmiraltyLimitsIndicator,
	InvalidELLimitsIndicator AS i_InvalidELLimitsIndicator,
	-- *INF*: DECODE(i_InvalidELLimitsIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_InvalidELLimitsIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_InvalidELLimitsIndicator,
	InvalidFELALimitsIndicator AS i_InvalidFELALimitsIndicator,
	-- *INF*: DECODE(i_InvalidFELALimitsIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_InvalidFELALimitsIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_InvalidFELALimitsIndicator,
	MinimumPremiumMaximumState,
	PeriodDate,
	PolicyRatingType,
	PolicyType,
	PrimaryLocationState,
	ProrateExpenseConstantIndicator AS i_ProrateExpenseConstantIndicator,
	-- *INF*: DECODE(i_ProrateExpenseConstantIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_ProrateExpenseConstantIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ProrateExpenseConstantIndicator,
	ProrateMinimumPremiumIndicator AS i_ProrateMinimumPremiumIndicator,
	-- *INF*: DECODE(i_ProrateMinimumPremiumIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_ProrateMinimumPremiumIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ProrateMinimumPremiumIndicator,
	RatingPlan,
	ValidAdmiraltyLimitsIndicator AS i_ValidAdmiraltyLimitsIndicator,
	-- *INF*: DECODE(i_ValidAdmiraltyLimitsIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_ValidAdmiraltyLimitsIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ValidAdmiraltyLimitsIndicator,
	ValidELLimitsIndicator AS i_ValidELLimitsIndicator,
	-- *INF*: DECODE(i_ValidELLimitsIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_ValidELLimitsIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ValidELLimitsIndicator,
	ValidFELALimitsIndicator AS i_ValidFELALimitsIndicator,
	-- *INF*: DECODE(i_ValidFELALimitsIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_ValidFELALimitsIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ValidFELALimitsIndicator,
	WaiverOfSubrogationMinimumPremiumMaximumState,
	WithoutWorkersCompensation AS i_WithoutWorkersCompensation,
	-- *INF*: DECODE(i_WithoutWorkersCompensation,'T',1,'F',0,NULL)
	DECODE(
	    i_WithoutWorkersCompensation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WithoutWorkersCompensation,
	WrapUpPolicy AS i_WrapUpPolicy,
	-- *INF*: DECODE(i_WrapUpPolicy,'T',1,'F',0,NULL)
	DECODE(
	    i_WrapUpPolicy,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WrapUpPolicy,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CombinedPolicyPremium
	FROM SQ_DCWCLineStaging
),
archDCWCLineStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCWCLineStaging
	(LineId, WC_LineId, SessionId, Id, AdmiraltyMinimumPremiumMaximumState, AnniversaryRating, AnniversaryRatingDate, AnyARDIndicator, CommissionPercentage, Description, DescriptionOverride, EmployersLiabilityIncreasedLimitsMaximumState, ExpenseConstantMaximumState, ExperienceModType, ExperienceRated, FELAMinimumPremiumMaximumState, InstallmentType, InvalidAdmiraltyLimitsIndicator, InvalidELLimitsIndicator, InvalidFELALimitsIndicator, MinimumPremiumMaximumState, PeriodDate, PolicyRatingType, PolicyType, PrimaryLocationState, ProrateExpenseConstantIndicator, ProrateMinimumPremiumIndicator, RatingPlan, ValidAdmiraltyLimitsIndicator, ValidELLimitsIndicator, ValidFELALimitsIndicator, WaiverOfSubrogationMinimumPremiumMaximumState, WithoutWorkersCompensation, WrapUpPolicy, ExtractDate, SourceSystemId, AuditId, CombinedPolicyPremium)
	SELECT 
	LINEID, 
	WC_LINEID, 
	SESSIONID, 
	ID, 
	ADMIRALTYMINIMUMPREMIUMMAXIMUMSTATE, 
	o_AnniversaryRating AS ANNIVERSARYRATING, 
	ANNIVERSARYRATINGDATE, 
	o_AnyARDIndicator AS ANYARDINDICATOR, 
	COMMISSIONPERCENTAGE, 
	DESCRIPTION, 
	DESCRIPTIONOVERRIDE, 
	EMPLOYERSLIABILITYINCREASEDLIMITSMAXIMUMSTATE, 
	EXPENSECONSTANTMAXIMUMSTATE, 
	EXPERIENCEMODTYPE, 
	o_ExperienceRated AS EXPERIENCERATED, 
	FELAMINIMUMPREMIUMMAXIMUMSTATE, 
	INSTALLMENTTYPE, 
	o_InvalidAdmiraltyLimitsIndicator AS INVALIDADMIRALTYLIMITSINDICATOR, 
	o_InvalidELLimitsIndicator AS INVALIDELLIMITSINDICATOR, 
	o_InvalidFELALimitsIndicator AS INVALIDFELALIMITSINDICATOR, 
	MINIMUMPREMIUMMAXIMUMSTATE, 
	PERIODDATE, 
	POLICYRATINGTYPE, 
	POLICYTYPE, 
	PRIMARYLOCATIONSTATE, 
	o_ProrateExpenseConstantIndicator AS PRORATEEXPENSECONSTANTINDICATOR, 
	o_ProrateMinimumPremiumIndicator AS PRORATEMINIMUMPREMIUMINDICATOR, 
	RATINGPLAN, 
	o_ValidAdmiraltyLimitsIndicator AS VALIDADMIRALTYLIMITSINDICATOR, 
	o_ValidELLimitsIndicator AS VALIDELLIMITSINDICATOR, 
	o_ValidFELALimitsIndicator AS VALIDFELALIMITSINDICATOR, 
	WAIVEROFSUBROGATIONMINIMUMPREMIUMMAXIMUMSTATE, 
	o_WithoutWorkersCompensation AS WITHOUTWORKERSCOMPENSATION, 
	o_WrapUpPolicy AS WRAPUPPOLICY, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	COMBINEDPOLICYPREMIUM
	FROM EXP_Metadata
),