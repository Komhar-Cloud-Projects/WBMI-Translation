WITH
SQ_DC_WC_Line AS (
	WITH cte_DCWCLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WC_LineId, 
	X.SessionId, 
	X.Id, 
	X.AdmiraltyMinimumPremiumMaximumState, 
	X.AnniversaryRating, 
	X.AnniversaryRatingDate, 
	X.AnyARDIndicator, 
	X.CommissionPercentage, 
	X.Description, 
	X.DescriptionOverride, 
	X.EmployersLiabilityIncreasedLimitsMaximumState, 
	X.ExpenseConstantMaximumState, 
	X.ExperienceModType, 
	X.ExperienceRated, 
	X.FELAMinimumPremiumMaximumState, 
	X.InstallmentType, 
	X.InvalidAdmiraltyLimitsIndicator, 
	X.InvalidELLimitsIndicator, 
	X.InvalidFELALimitsIndicator, 
	X.MinimumPremiumMaximumState, 
	X.PeriodDate, 
	X.PolicyRatingType, 
	X.PolicyType, 
	X.PrimaryLocationState, 
	X.ProrateExpenseConstantIndicator, 
	X.ProrateMinimumPremiumIndicator, 
	X.RatingPlan, 
	X.ValidAdmiraltyLimitsIndicator, 
	X.ValidELLimitsIndicator, 
	X.ValidFELALimitsIndicator, 
	X.WaiverOfSubrogationMinimumPremiumMaximumState, 
	X.WithoutWorkersCompensation, 
	X.WrapUpPolicy,
	X.CombinedPolicyPremium  
	FROM
	DC_WC_Line X
	inner join
	cte_DCWCLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CombinedPolicyPremium
	FROM SQ_DC_WC_Line
),
DCWCLineStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCLineStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCLineStaging
	(LineId, WC_LineId, SessionId, Id, AdmiraltyMinimumPremiumMaximumState, AnniversaryRating, AnniversaryRatingDate, AnyARDIndicator, CommissionPercentage, Description, DescriptionOverride, EmployersLiabilityIncreasedLimitsMaximumState, ExpenseConstantMaximumState, ExperienceModType, ExperienceRated, FELAMinimumPremiumMaximumState, InstallmentType, InvalidAdmiraltyLimitsIndicator, InvalidELLimitsIndicator, InvalidFELALimitsIndicator, MinimumPremiumMaximumState, PeriodDate, PolicyRatingType, PolicyType, PrimaryLocationState, ProrateExpenseConstantIndicator, ProrateMinimumPremiumIndicator, RatingPlan, ValidAdmiraltyLimitsIndicator, ValidELLimitsIndicator, ValidFELALimitsIndicator, WaiverOfSubrogationMinimumPremiumMaximumState, WithoutWorkersCompensation, WrapUpPolicy, ExtractDate, SourceSystemId, CombinedPolicyPremium)
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
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COMBINEDPOLICYPREMIUM
	FROM EXP_Metadata
),