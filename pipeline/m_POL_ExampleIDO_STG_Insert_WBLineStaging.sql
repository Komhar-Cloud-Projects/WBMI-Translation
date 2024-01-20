WITH
SQ_WB_Line AS (
	WITH cte_WBLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WB_LineId, 
	X.SessionId, 
	X.ApplicableToPackage, 
	X.IsEligibleForClearing, 
	X.Cleared, 
	X.ClearedDateTimeStamp, 
	X.Contribution, 
	X.IsContribution, 
	X.IsLossSensitiveCommission, 
	X.IsOverride, 
	X.LossSensitiveCommission, 
	X.Override, 
	X.LossRatio, 
	X.FinalCommission, 
	X.CommissionAmount, 
	X.ConsentToRate, 
	X.RiskGrade, 
	X.IsGraduated, 
	X.CommissionAmountGraduated, 
	X.FinalCommissionGraduated, 
	X.TransactionCommissionGraduated, 
	X.TransactionCommissionGraduatedValue, 
	X.HasCommissionPlanRan, 
	X.PurePremium, 
	X.CommissionGraduatedTierLevel, 
	X.TransactionCommissionGraduatedTierLevel, 
	X.GraduatedRateValue, 
	X.ApplyLossSensitiveCommission, 
	X.Graduated, 
	X.Maximum, 
	X.Minimum, 
	X.RateValue, 
	X.CommissionCustomerCareAmount, 
	X.CommissionAssociationAmount, 
	X.CommissionProgramAmount, 
	X.AdjustedBaseCommission, 
	X.AdjustedBaseCommissionGraduated, 
	X.PolicyType, 
	X.CommissionLOBSpecificAmount, 
	X.GetTransactionCommissionValue, 
	X.GetTransactionGraduatedCommissionValue, 
	X.OverrideCommissionPlanId, 
	X.TransactionCommissionLOBSpecificValue, 
	X.TransactionLOBBaseCommissionValue, 
	X.TransactionLOBBaseCommissionGraduatedValue 
	FROM
	WB_Line X
	inner join
	cte_WBLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSyStemId,
	LineId,
	WB_LineId,
	SessionId,
	ApplicableToPackage,
	IsEligibleForClearing,
	-- *INF*: DECODE(TRUE,IsEligibleForClearing='T',1,IsEligibleForClearing='F',0,NULL)
	DECODE(
	    TRUE,
	    IsEligibleForClearing = 'T', 1,
	    IsEligibleForClearing = 'F', 0,
	    NULL
	) AS IsEligibleForClearing_out,
	Cleared,
	-- *INF*: decode(true,Cleared='T',1,Cleared='F',0,NULL)
	decode(
	    true,
	    Cleared = 'T', 1,
	    Cleared = 'F', 0,
	    NULL
	) AS Cleared_out,
	ClearedDateTimeStamp,
	Contribution,
	IsContribution,
	-- *INF*: decode(true,IsContribution='T',1,IsContribution='F',0,NULL)
	decode(
	    true,
	    IsContribution = 'T', 1,
	    IsContribution = 'F', 0,
	    NULL
	) AS IsContribution_out,
	IsLossSensitiveCommission,
	-- *INF*: DECODE(TRUE,IsLossSensitiveCommission='T',1,IsLossSensitiveCommission='F',0,NULL)
	DECODE(
	    TRUE,
	    IsLossSensitiveCommission = 'T', 1,
	    IsLossSensitiveCommission = 'F', 0,
	    NULL
	) AS IsLossSensitiveCommission_out,
	IsOverride,
	-- *INF*: DECODE(TRUE,IsOverride='T',1,IsOverride='F',0,NULL)
	DECODE(
	    TRUE,
	    IsOverride = 'T', 1,
	    IsOverride = 'F', 0,
	    NULL
	) AS IsOverride_out,
	LossSensitiveCommission,
	Override,
	LossRatio,
	FinalCommission,
	-- *INF*: IIF(IsNull(FinalCommission),0,FinalCommission)
	IFF(FinalCommission IS NULL, 0, FinalCommission) AS FinalCommission_out,
	CommissionAmount,
	ConsentToRate,
	-- *INF*: DECODE(TRUE,ConsentToRate='T',1,ConsentToRate='F',0,NULL)
	DECODE(
	    TRUE,
	    ConsentToRate = 'T', 1,
	    ConsentToRate = 'F', 0,
	    NULL
	) AS ConsentToRate_out,
	RiskGrade,
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
	PurePremium,
	CommissionGraduatedTierLevel,
	TransactionCommissionGraduatedTierLevel,
	GraduatedRateValue,
	ApplyLossSensitiveCommission,
	Graduated,
	-- *INF*: DECODE(TRUE,Graduated='T',1,Graduated='F',0,NULL)
	DECODE(
	    TRUE,
	    Graduated = 'T', 1,
	    Graduated = 'F', 0,
	    NULL
	) AS Graduated_out,
	Maximum,
	Minimum,
	RateValue,
	CommissionCustomerCareAmount,
	CommissionAssociationAmount,
	CommissionProgramAmount,
	AdjustedBaseCommission,
	AdjustedBaseCommissionGraduated,
	PolicyType,
	CommissionLOBSpecificAmount,
	GetTransactionCommissionValue,
	GetTransactionGraduatedCommissionValue,
	OverrideCommissionPlanId,
	TransactionCommissionLOBSpecificValue,
	TransactionLOBBaseCommissionValue,
	TransactionLOBBaseCommissionGraduatedValue
	FROM SQ_WB_Line
),
WBLineStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBLineStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBLineStaging
	(ExtractDate, SourceSystemId, LineId, WB_LineId, SessionId, PurePremium, Contribution, IsContribution, IsOverride, Override, IsLossSensitiveCommission, LossSensitiveCommission, LossRatio, ApplyLossSensitiveCommission, FinalCommission, CommissionAmount, IsGraduated, CommissionAmountGraduated, FinalCommissionGraduated, CommissionGraduatedTierLevel, CommissionCustomerCareAmount, CommissionAssociationAmount, CommissionProgramAmount, AdjustedBaseCommission, AdjustedBaseCommissionGraduated, ConsentToRate, ApplicableToPackage, IsEligibleForClearing, RiskGrade, Cleared, ClearedDateTimeStamp, TransactionCommissionGraduated, TransactionCommissionGraduatedValue, HasCommissionPlanRan, GraduatedRateValue, Maximum, Minimum, RateValue, Graduated, TransactionCommissionGraduatedTierLevel, OverrideCommissionPlanId, PolicyType, CommissionLOBSpecificAmount, GetTransactionCommissionValue, GetTransactionGraduatedCommissionValue, TransactionCommissionLOBSpecificValue, TransactionLOBBaseCommissionValue, TransactionLOBBaseCommissionGraduatedValue)
	SELECT 
	EXTRACTDATE, 
	SourceSyStemId AS SOURCESYSTEMID, 
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
	FinalCommission_out AS FINALCOMMISSION, 
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