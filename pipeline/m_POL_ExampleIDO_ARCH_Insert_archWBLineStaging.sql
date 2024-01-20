WITH
SQ_WBLineStaging AS (
	SELECT
		WBLineStagingId,
		ExtractDate,
		SourceSystemId AS SourceSyStemId,
		LineId,
		WB_LineId,
		SessionId,
		PurePremium,
		Contribution,
		IsContribution,
		IsOverride,
		Override,
		IsLossSensitiveCommission,
		LossSensitiveCommission,
		LossRatio,
		ApplyLossSensitiveCommission,
		FinalCommission,
		CommissionAmount,
		IsGraduated,
		CommissionAmountGraduated,
		FinalCommissionGraduated,
		CommissionGraduatedTierLevel,
		CommissionCustomerCareAmount,
		CommissionAssociationAmount,
		CommissionProgramAmount,
		AdjustedBaseCommission,
		AdjustedBaseCommissionGraduated,
		ConsentToRate,
		ApplicableToPackage,
		IsEligibleForClearing,
		RiskGrade,
		Cleared,
		ClearedDateTimeStamp,
		TransactionCommissionGraduated,
		TransactionCommissionGraduatedValue,
		HasCommissionPlanRan,
		GraduatedRateValue,
		Maximum,
		Minimum,
		RateValue,
		Graduated,
		TransactionCommissionGraduatedTierLevel,
		OverrideCommissionPlanId,
		PolicyType,
		CommissionLOBSpecificAmount,
		GetTransactionCommissionValue,
		GetTransactionGraduatedCommissionValue,
		TransactionCommissionLOBSpecificValue,
		TransactionLOBBaseCommissionValue,
		TransactionLOBBaseCommissionGraduatedValue
	FROM WBLineStaging
),
EXP_Metadata AS (
	SELECT
	WBLineStagingId,
	ExtractDate,
	SourceSyStemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	LineId,
	WB_LineId,
	SessionId,
	PurePremium,
	Contribution,
	IsContribution,
	-- *INF*: decode(true,IsContribution='T',1,IsContribution='F',0,NULL)
	decode(
	    true,
	    IsContribution = 'T', 1,
	    IsContribution = 'F', 0,
	    NULL
	) AS IsContribution_out,
	IsOverride,
	-- *INF*: DECODE(TRUE,IsOverride='T',1,IsOverride='F',0,NULL)
	DECODE(
	    TRUE,
	    IsOverride = 'T', 1,
	    IsOverride = 'F', 0,
	    NULL
	) AS IsOverride_out,
	Override,
	IsLossSensitiveCommission,
	-- *INF*: DECODE(TRUE,IsLossSensitiveCommission='T',1,IsLossSensitiveCommission='F',0,NULL)
	DECODE(
	    TRUE,
	    IsLossSensitiveCommission = 'T', 1,
	    IsLossSensitiveCommission = 'F', 0,
	    NULL
	) AS IsLossSensitiveCommission_out,
	LossSensitiveCommission,
	LossRatio,
	ApplyLossSensitiveCommission,
	FinalCommission,
	CommissionAmount,
	IsGraduated,
	-- *INF*: DECODE(TRUE,IsGraduated='T',1,IsGraduated='F',0,NULL)
	DECODE(
	    TRUE,
	    IsGraduated = 'T', 1,
	    IsGraduated = 'F', 0,
	    NULL
	) AS IsGraduated_out,
	CommissionAmountGraduated,
	FinalCommissionGraduated,
	CommissionGraduatedTierLevel,
	CommissionCustomerCareAmount,
	CommissionAssociationAmount,
	CommissionProgramAmount,
	AdjustedBaseCommission,
	AdjustedBaseCommissionGraduated,
	ConsentToRate,
	-- *INF*: DECODE(TRUE,ConsentToRate='T',1,ConsentToRate='F',0,NULL)
	DECODE(
	    TRUE,
	    ConsentToRate = 'T', 1,
	    ConsentToRate = 'F', 0,
	    NULL
	) AS ConsentToRate_out,
	ApplicableToPackage,
	IsEligibleForClearing,
	-- *INF*: DECODE(TRUE,IsEligibleForClearing='T',1,IsEligibleForClearing='F',0,NULL)
	DECODE(
	    TRUE,
	    IsEligibleForClearing = 'T', 1,
	    IsEligibleForClearing = 'F', 0,
	    NULL
	) AS IsEligibleForClearing_out,
	RiskGrade,
	Cleared,
	-- *INF*: decode(true,Cleared='T',1,Cleared='F',0,NULL)
	decode(
	    true,
	    Cleared = 'T', 1,
	    Cleared = 'F', 0,
	    NULL
	) AS Cleared_out,
	ClearedDateTimeStamp,
	TransactionCommissionGraduated,
	-- *INF*: DECODE(TRUE,TransactionCommissionGraduated='T',1,TransactionCommissionGraduated='F',0,NULL)
	DECODE(
	    TRUE,
	    TransactionCommissionGraduated = 'T', 1,
	    TransactionCommissionGraduated = 'F', 0,
	    NULL
	) AS TransactionCommissionGraduated_out,
	TransactionCommissionGraduatedValue,
	HasCommissionPlanRan,
	-- *INF*: DECODE(TRUE,HasCommissionPlanRan='T',1,HasCommissionPlanRan='F',0,NULL)
	DECODE(
	    TRUE,
	    HasCommissionPlanRan = 'T', 1,
	    HasCommissionPlanRan = 'F', 0,
	    NULL
	) AS HasCommissionPlanRan_out,
	GraduatedRateValue,
	Maximum,
	Minimum,
	RateValue,
	Graduated,
	-- *INF*: DECODE(TRUE,Graduated='T',1,Graduated='F',0,NULL)
	DECODE(
	    TRUE,
	    Graduated = 'T', 1,
	    Graduated = 'F', 0,
	    NULL
	) AS Graduated_out,
	TransactionCommissionGraduatedTierLevel,
	OverrideCommissionPlanId,
	PolicyType,
	CommissionLOBSpecificAmount,
	GetTransactionCommissionValue,
	GetTransactionGraduatedCommissionValue,
	TransactionCommissionLOBSpecificValue,
	TransactionLOBBaseCommissionValue,
	TransactionLOBBaseCommissionGraduatedValue
	FROM SQ_WBLineStaging
),
archWBLineStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBLineStaging
	(ExtractDate, SourceSystemId, AuditId, WBLineStagingId, LineId, WB_LineId, SessionId, PurePremium, Contribution, IsContribution, IsOverride, Override, IsLossSensitiveCommission, LossSensitiveCommission, LossRatio, ApplyLossSensitiveCommission, FinalCommission, CommissionAmount, IsGraduated, CommissionAmountGraduated, FinalCommissionGraduated, CommissionGraduatedTierLevel, CommissionCustomerCareAmount, CommissionAssociationAmount, CommissionProgramAmount, AdjustedBaseCommission, AdjustedBaseCommissionGraduated, ConsentToRate, ApplicableToPackage, IsEligibleForClearing, RiskGrade, Cleared, ClearedDateTimeStamp, TransactionCommissionGraduated, TransactionCommissionGraduatedValue, HasCommissionPlanRan, GraduatedRateValue, Maximum, Minimum, RateValue, Graduated, TransactionCommissionGraduatedTierLevel, OverrideCommissionPlanId, PolicyType, CommissionLOBSpecificAmount, GetTransactionCommissionValue, GetTransactionGraduatedCommissionValue, TransactionCommissionLOBSpecificValue, TransactionLOBBaseCommissionValue, TransactionLOBBaseCommissionGraduatedValue)
	SELECT 
	EXTRACTDATE, 
	SourceSyStemId AS SOURCESYSTEMID, 
	AUDITID, 
	WBLINESTAGINGID, 
	LINEID, 
	WB_LINEID, 
	SESSIONID, 
	PUREPREMIUM, 
	CONTRIBUTION, 
	IsContribution_out AS ISCONTRIBUTION, 
	IsOverride_out AS ISOVERRIDE, 
	OVERRIDE, 
	IsLossSensitiveCommission_out AS ISLOSSSENSITIVECOMMISSION, 
	LOSSSENSITIVECOMMISSION, 
	LOSSRATIO, 
	APPLYLOSSSENSITIVECOMMISSION, 
	FINALCOMMISSION, 
	COMMISSIONAMOUNT, 
	IsGraduated_out AS ISGRADUATED, 
	COMMISSIONAMOUNTGRADUATED, 
	FINALCOMMISSIONGRADUATED, 
	COMMISSIONGRADUATEDTIERLEVEL, 
	COMMISSIONCUSTOMERCAREAMOUNT, 
	COMMISSIONASSOCIATIONAMOUNT, 
	COMMISSIONPROGRAMAMOUNT, 
	ADJUSTEDBASECOMMISSION, 
	ADJUSTEDBASECOMMISSIONGRADUATED, 
	ConsentToRate_out AS CONSENTTORATE, 
	APPLICABLETOPACKAGE, 
	IsEligibleForClearing_out AS ISELIGIBLEFORCLEARING, 
	RISKGRADE, 
	Cleared_out AS CLEARED, 
	CLEAREDDATETIMESTAMP, 
	TransactionCommissionGraduated_out AS TRANSACTIONCOMMISSIONGRADUATED, 
	TRANSACTIONCOMMISSIONGRADUATEDVALUE, 
	HasCommissionPlanRan_out AS HASCOMMISSIONPLANRAN, 
	GRADUATEDRATEVALUE, 
	MAXIMUM, 
	MINIMUM, 
	RATEVALUE, 
	Graduated_out AS GRADUATED, 
	TRANSACTIONCOMMISSIONGRADUATEDTIERLEVEL, 
	OVERRIDECOMMISSIONPLANID, 
	POLICYTYPE, 
	COMMISSIONLOBSPECIFICAMOUNT, 
	GETTRANSACTIONCOMMISSIONVALUE, 
	GETTRANSACTIONGRADUATEDCOMMISSIONVALUE, 
	TRANSACTIONCOMMISSIONLOBSPECIFICVALUE, 
	TRANSACTIONLOBBASECOMMISSIONVALUE, 
	TRANSACTIONLOBBASECOMMISSIONGRADUATEDVALUE
	FROM EXP_Metadata
),