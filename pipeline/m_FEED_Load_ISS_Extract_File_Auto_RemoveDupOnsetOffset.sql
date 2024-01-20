WITH
SQ_ISSCA_Loss AS (
	--Select Loss Records ASIS
	SELECT ISSCommercialAutoExtract.ISSCommercialAutoExtractID, ISSCommercialAutoExtract.AuditId, ISSCommercialAutoExtract.CreatedDate, ISSCommercialAutoExtract.EDWPremiumMasterCalculationPKId, ISSCommercialAutoExtract.EDWLossMasterCalculationPKId, ISSCommercialAutoExtract.TypeBureauCode, ISSCommercialAutoExtract.BureauLineOfInsurance, ISSCommercialAutoExtract.BureauCompanyNumber, ISSCommercialAutoExtract.StateProvinceCode, ISSCommercialAutoExtract.PremiumMasterRunDate, ISSCommercialAutoExtract.LossMasterRunDate, ISSCommercialAutoExtract.PolicyKey, ISSCommercialAutoExtract.PremiumMasterClassCode, ISSCommercialAutoExtract.LossMasterClassCode, ISSCommercialAutoExtract.ClaimNumber, ISSCommercialAutoExtract.ClaimantNumber, ISSCommercialAutoExtract.RiskTerritoryCode, ISSCommercialAutoExtract.PolicyEffectiveDate, ISSCommercialAutoExtract.CauseOfLoss, ISSCommercialAutoExtract.DeductibleAmount, ISSCommercialAutoExtract.CoverageCode, ISSCommercialAutoExtract.SublineCode, ISSCommercialAutoExtract.PackageModificationAdjustmentGroupDescription, ISSCommercialAutoExtract.PremiumMasterDirectWrittenPremiumAmount, ISSCommercialAutoExtract.PaidLossAmount, ISSCommercialAutoExtract.OutstandingLossAmount, ISSCommercialAutoExtract.PolicyExpirationDate, ISSCommercialAutoExtract.InceptionToDatePaidLossAmount, ISSCommercialAutoExtract.ClaimantCoverageDetailId, ISSCommercialAutoExtract.AnnualStatementLineNumber, ISSCommercialAutoExtract.ZipPostalCode, ISSCommercialAutoExtract.DeductibleIndicatorCode, ISSCommercialAutoExtract.PolicyLowerLimit, ISSCommercialAutoExtract.PolicyUpperLimit, ISSCommercialAutoExtract.TerminalZoneCode, ISSCommercialAutoExtract.WrittenExposure, ISSCommercialAutoExtract.PaidAllocatedLossAdjustmentExpenseAmount, ISSCommercialAutoExtract.OutstandingAllocatedLossAdjustmentExpenseAmount, ISSCommercialAutoExtract.ClaimLossDate, ISSCommercialAutoExtract.TransactionEffectiveDate, ISSCommercialAutoExtract.CoverageGroupCode, ISSCommercialAutoExtract.VehicleNumber, ISSCommercialAutoExtract.IncludeUIM,
	ISSCommercialAutoExtract.RatingZoneCode
	FROM
	 ISSCommercialAutoExtract
	WHERE ISSCommercialAutoExtract.EDWLossMasterCalculationPKId<>-1
	 @{pipeline().parameters.WHERE_CLAUSE_LOSS}
),
EXP_Loss AS (
	SELECT
	ISSCommercialAutoExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	ZipPostalCode,
	DeductibleIndicatorCode,
	PolicyLowerLimit,
	PolicyUpperLimit,
	RatingZoneCode,
	TerminalZoneCode,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	TransactionEffectiveDate,
	CoverageGroupCode,
	VehicleNumber,
	IncludeUIM
	FROM SQ_ISSCA_Loss
),
SQ_ISSCA_PremiumUnique AS (
	WITH prem_unique
	AS
	(SELECT
			CONCAT(TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PolicyKey, PremiumMasterClassCode,
			ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode,
			PackageModificationAdjustmentGroupDescription,
			PolicyExpirationDate, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit,
			PolicyUpperLimit, TerminalZoneCode, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber, IncludeUIM, RatingZoneCode) AS concct
			,COUNT(*) count
		FROM ISSCommercialAutoExtract
		WHERE EDWPremiumMasterCalculationPKId <> -1
		@{pipeline().parameters.WHERE_CLAUSE_PREMIUM}
		GROUP BY CONCAT(TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PolicyKey, PremiumMasterClassCode,
		ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode,
		PackageModificationAdjustmentGroupDescription,
		PolicyExpirationDate, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit,
		PolicyUpperLimit, TerminalZoneCode, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber, IncludeUIM, RatingZoneCode)
		HAVING COUNT(*) = 1)
	
	SELECT DISTINCT
		ISSCommercialAutoExtract.ISSCommercialAutoExtractID
		,ISSCommercialAutoExtract.AuditId
		,ISSCommercialAutoExtract.CreatedDate
		,ISSCommercialAutoExtract.EDWPremiumMasterCalculationPKId
		,ISSCommercialAutoExtract.EDWLossMasterCalculationPKId
		,ISSCommercialAutoExtract.TypeBureauCode
		,ISSCommercialAutoExtract.BureauLineOfInsurance
		,ISSCommercialAutoExtract.BureauCompanyNumber
		,ISSCommercialAutoExtract.StateProvinceCode
		,ISSCommercialAutoExtract.PremiumMasterRunDate
		,ISSCommercialAutoExtract.LossMasterRunDate
		,ISSCommercialAutoExtract.PolicyKey
		,ISSCommercialAutoExtract.PremiumMasterClassCode
		,ISSCommercialAutoExtract.LossMasterClassCode
		,ISSCommercialAutoExtract.ClaimNumber
		,ISSCommercialAutoExtract.ClaimantNumber
		,ISSCommercialAutoExtract.RiskTerritoryCode
		,ISSCommercialAutoExtract.PolicyEffectiveDate
		,ISSCommercialAutoExtract.CauseOfLoss
		,ISSCommercialAutoExtract.DeductibleAmount
		,ISSCommercialAutoExtract.CoverageCode
		,ISSCommercialAutoExtract.SublineCode
		,ISSCommercialAutoExtract.PackageModificationAdjustmentGroupDescription
		,ISSCommercialAutoExtract.PremiumMasterDirectWrittenPremiumAmount
		,ISSCommercialAutoExtract.PaidLossAmount
		,ISSCommercialAutoExtract.OutstandingLossAmount
		,ISSCommercialAutoExtract.PolicyExpirationDate
		,ISSCommercialAutoExtract.InceptionToDatePaidLossAmount
		,ISSCommercialAutoExtract.ClaimantCoverageDetailId
		,ISSCommercialAutoExtract.AnnualStatementLineNumber
		,ISSCommercialAutoExtract.ZipPostalCode
		,ISSCommercialAutoExtract.DeductibleIndicatorCode
		,ISSCommercialAutoExtract.PolicyLowerLimit
		,ISSCommercialAutoExtract.PolicyUpperLimit
		,ISSCommercialAutoExtract.TerminalZoneCode
		,ISSCommercialAutoExtract.WrittenExposure
		,ISSCommercialAutoExtract.PaidAllocatedLossAdjustmentExpenseAmount
		,ISSCommercialAutoExtract.OutstandingAllocatedLossAdjustmentExpenseAmount
		,ISSCommercialAutoExtract.ClaimLossDate
		,ISSCommercialAutoExtract.TransactionEffectiveDate
		,ISSCommercialAutoExtract.CoverageGroupCode
		,ISSCommercialAutoExtract.VehicleNumber
		,ISSCommercialAutoExtract.IncludeUIM
		,ISSCommercialAutoExtract.RatingZoneCode
	FROM ISSCommercialAutoExtract
	JOIN prem_unique b
		ON (CONCAT(TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PolicyKey, PremiumMasterClassCode,
			ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode,
			PackageModificationAdjustmentGroupDescription,
			PolicyExpirationDate, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit,
			PolicyUpperLimit, TerminalZoneCode, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber, IncludeUIM, RatingZoneCode) = b.concct)
	WHERE ISSCommercialAutoExtract.EDWPremiumMasterCalculationPKId <> -1
	@{pipeline().parameters.WHERE_CLAUSE_PREMIUM}
),
EXP_PremiumUnique AS (
	SELECT
	ISSCommercialAutoExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	ZipPostalCode,
	DeductibleIndicatorCode,
	PolicyLowerLimit,
	PolicyUpperLimit,
	RatingZoneCode,
	TerminalZoneCode,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	TransactionEffectiveDate,
	CoverageGroupCode,
	VehicleNumber,
	IncludeUIM
	FROM SQ_ISSCA_PremiumUnique
),
SQ_ISSCA_Premium_Dup AS (
	WITH ROLLUP_TABLE_TEMP
	AS
	(SELECT
			SUM(PremiumMasterDirectWrittenPremiumAmount) ROLL_UP_DWP_AMT
			,MAX(ISSCommercialAutoExtractID) MAX_ISS_KEY
		FROM ISSCommercialAutoExtract
		WHERE EDWPremiumMasterCalculationPKId <> -1
		@{pipeline().parameters.WHERE_CLAUSE_PREMIUM}
		GROUP BY CONCAT(TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PolicyKey, PremiumMasterClassCode,
		ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode,
		PackageModificationAdjustmentGroupDescription,
		PolicyExpirationDate, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit,
		PolicyUpperLimit, TerminalZoneCode, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber,IncludeUIM, RatingZoneCode)
		HAVING COUNT(1) > 1
		AND SUM(PremiumMasterDirectWrittenPremiumAmount) <> 0
	)
	
	SELECT
		ISSCommercialAutoExtract.ISSCommercialAutoExtractID
		,ISSCommercialAutoExtract.AuditId
		,ISSCommercialAutoExtract.CreatedDate
		,ISSCommercialAutoExtract.EDWPremiumMasterCalculationPKId
		,ISSCommercialAutoExtract.EDWLossMasterCalculationPKId
		,ISSCommercialAutoExtract.TypeBureauCode
		,ISSCommercialAutoExtract.BureauLineOfInsurance
		,ISSCommercialAutoExtract.BureauCompanyNumber
		,ISSCommercialAutoExtract.StateProvinceCode
		,ISSCommercialAutoExtract.PremiumMasterRunDate
		,ISSCommercialAutoExtract.LossMasterRunDate
		,ISSCommercialAutoExtract.PolicyKey
		,ISSCommercialAutoExtract.PremiumMasterClassCode
		,ISSCommercialAutoExtract.LossMasterClassCode
		,ISSCommercialAutoExtract.ClaimNumber
		,ISSCommercialAutoExtract.ClaimantNumber
		,ISSCommercialAutoExtract.RiskTerritoryCode
		,ISSCommercialAutoExtract.PolicyEffectiveDate
		,ISSCommercialAutoExtract.CauseOfLoss
		,ISSCommercialAutoExtract.DeductibleAmount
		,ISSCommercialAutoExtract.CoverageCode
		,ISSCommercialAutoExtract.SublineCode
		,ISSCommercialAutoExtract.PackageModificationAdjustmentGroupDescription
		,ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT AS PremiumMasterDirectWrittenPremiumAmount
		,ISSCommercialAutoExtract.PaidLossAmount
		,ISSCommercialAutoExtract.OutstandingLossAmount
		,ISSCommercialAutoExtract.PolicyExpirationDate
		,ISSCommercialAutoExtract.InceptionToDatePaidLossAmount
		,ISSCommercialAutoExtract.ClaimantCoverageDetailId
		,ISSCommercialAutoExtract.AnnualStatementLineNumber
		,ISSCommercialAutoExtract.ZipPostalCode
		,ISSCommercialAutoExtract.DeductibleIndicatorCode
		,ISSCommercialAutoExtract.PolicyLowerLimit
		,ISSCommercialAutoExtract.PolicyUpperLimit
		,ISSCommercialAutoExtract.TerminalZoneCode
		,ISSCommercialAutoExtract.WrittenExposure
		,ISSCommercialAutoExtract.PaidAllocatedLossAdjustmentExpenseAmount
		,ISSCommercialAutoExtract.OutstandingAllocatedLossAdjustmentExpenseAmount
		,ISSCommercialAutoExtract.ClaimLossDate
		,ISSCommercialAutoExtract.TransactionEffectiveDate
		,ISSCommercialAutoExtract.CoverageGroupCode
		,ISSCommercialAutoExtract.VehicleNumber
		,ISSCommercialAutoExtract.IncludeUIM
		,ISSCommercialAutoExtract.RatingZoneCode
	FROM ROLLUP_TABLE_TEMP
	INNER JOIN ISSCommercialAutoExtract
		ON ISSCommercialAutoExtractId = ROLLUP_TABLE_TEMP.MAX_ISS_KEY
),
EXP_Premium_Dup AS (
	SELECT
	ISSCommercialAutoExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	ZipPostalCode,
	DeductibleIndicatorCode,
	PolicyLowerLimit,
	PolicyUpperLimit,
	RatingZoneCode,
	TerminalZoneCode,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	TransactionEffectiveDate,
	CoverageGroupCode,
	VehicleNumber,
	IncludeUIM
	FROM SQ_ISSCA_Premium_Dup
),
Union AS (
	SELECT ISSCommercialAutoExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit, PolicyUpperLimit, RatingZoneCode, TerminalZoneCode, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber, IncludeUIM
	FROM EXP_PremiumUnique
	UNION
	SELECT ISSCommercialAutoExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit, PolicyUpperLimit, RatingZoneCode, TerminalZoneCode, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber, IncludeUIM
	FROM EXP_Loss
	UNION
	SELECT ISSCommercialAutoExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit, PolicyUpperLimit, RatingZoneCode, TerminalZoneCode, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber, IncludeUIM
	FROM EXP_Premium_Dup
),
EXP_Get_Values AS (
	SELECT
	ISSCommercialAutoExtractId AS WorkISSExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	ZipPostalCode,
	DeductibleIndicatorCode,
	PolicyLowerLimit,
	PolicyUpperLimit,
	RatingZoneCode,
	TerminalZoneCode,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	TransactionEffectiveDate,
	CoverageGroupCode,
	VehicleNumber,
	IncludeUIM
	FROM Union
),
RTR_CL_PL AS (
	SELECT
	WorkISSExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	ConstructionCode,
	ISOFireProtectionCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	ZipPostalCode,
	DeductibleIndicatorCode,
	VehicleModelYear,
	DefensiveDriverCode,
	PolicyLowerLimit,
	PolicyUpperLimit,
	RatingZoneCode,
	TerminalZoneCode,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	TransactionEffectiveDate,
	CoverageGroupCode,
	VehicleNumber,
	IncludeUIM
	FROM EXP_Get_Values
),
RTR_CL_PL_CL AS (SELECT * FROM RTR_CL_PL WHERE IN(TypeBureauCode,'AL','AN','AP','N/A','CommercialAuto')),
RTR_CL_PL_PL AS (SELECT * FROM RTR_CL_PL WHERE IN(TypeBureauCode,'RL','RN','RP')),
EXP_Target_PL AS (
	SELECT
	-- *INF*: TRUNC(SYSDATE,'DD')
	CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)) AS v_RunDate,
	-- *INF*: 'ISS_Auto_PL_'||TO_CHAR(v_RunDate,'YYYYMMDD')||'.CSV'
	'ISS_Auto_PL_' || TO_CHAR(v_RunDate, 'YYYYMMDD') || '.CSV' AS FileName,
	WorkISSExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	ConstructionCode,
	ISOFireProtectionCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	ZipPostalCode,
	DeductibleIndicatorCode,
	VehicleModelYear,
	DefensiveDriverCode,
	PolicyLowerLimit,
	PolicyUpperLimit,
	RatingZoneCode,
	TerminalZoneCode,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	TransactionEffectiveDate,
	CoverageGroupCode,
	VehicleNumber,
	IncludeUIM
	FROM RTR_CL_PL_PL
),
SRT_ISSFlatFILE_Auto_PL AS (
	SELECT
	FileName, 
	WorkISSExtractId, 
	AuditId, 
	CreatedDate, 
	EDWPremiumMasterCalculationPKId, 
	EDWLossMasterCalculationPKId, 
	TypeBureauCode, 
	BureauLineOfInsurance, 
	BureauCompanyNumber, 
	StateProvinceCode, 
	PremiumMasterRunDate, 
	LossMasterRunDate, 
	PolicyKey, 
	PremiumMasterClassCode, 
	LossMasterClassCode, 
	ClaimNumber, 
	ClaimantNumber, 
	RiskTerritoryCode, 
	PolicyEffectiveDate, 
	CauseOfLoss, 
	DeductibleAmount, 
	CoverageCode, 
	SublineCode, 
	PackageModificationAdjustmentGroupDescription, 
	PremiumMasterDirectWrittenPremiumAmount, 
	PaidLossAmount, 
	OutstandingLossAmount, 
	PolicyExpirationDate, 
	InceptionToDatePaidLossAmount, 
	ClaimantCoverageDetailId, 
	AnnualStatementLineNumber, 
	ZipPostalCode, 
	DeductibleIndicatorCode, 
	PolicyLowerLimit, 
	PolicyUpperLimit, 
	RatingZoneCode, 
	TerminalZoneCode, 
	WrittenExposure, 
	PaidAllocatedLossAdjustmentExpenseAmount, 
	OutstandingAllocatedLossAdjustmentExpenseAmount, 
	ClaimLossDate, 
	TransactionEffectiveDate, 
	CoverageGroupCode, 
	VehicleNumber, 
	IncludeUIM
	FROM EXP_Target_PL
	ORDER BY PolicyKey ASC, PremiumMasterClassCode ASC, CoverageCode ASC, AnnualStatementLineNumber ASC, ZipPostalCode ASC
),
ISSFlatFile_Auto_PL AS (
	INSERT INTO ISSFlatFile_Auto
	(FileName, WorkISSExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit, PolicyUpperLimit, RatingZoneCode, TerminalZoneCode, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber, IncludeUIM)
	SELECT 
	FILENAME, 
	WORKISSEXTRACTID, 
	AUDITID, 
	CREATEDDATE, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	BUREAUCOMPANYNUMBER, 
	STATEPROVINCECODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	POLICYKEY, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	CLAIMNUMBER, 
	CLAIMANTNUMBER, 
	RISKTERRITORYCODE, 
	POLICYEFFECTIVEDATE, 
	CAUSEOFLOSS, 
	DEDUCTIBLEAMOUNT, 
	COVERAGECODE, 
	SUBLINECODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	ZIPPOSTALCODE, 
	DEDUCTIBLEINDICATORCODE, 
	POLICYLOWERLIMIT, 
	POLICYUPPERLIMIT, 
	RATINGZONECODE, 
	TERMINALZONECODE, 
	WRITTENEXPOSURE, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	CLAIMLOSSDATE, 
	TRANSACTIONEFFECTIVEDATE, 
	COVERAGEGROUPCODE, 
	VEHICLENUMBER, 
	INCLUDEUIM
	FROM SRT_ISSFlatFILE_Auto_PL
),
EXP_Target_CL AS (
	SELECT
	-- *INF*: TRUNC(SYSDATE,'DD')
	CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)) AS v_RunDate,
	-- *INF*: 'ISS_Auto_CL_'||TO_CHAR(v_RunDate,'YYYYMMDD')||'.CSV'
	'ISS_Auto_CL_' || TO_CHAR(v_RunDate, 'YYYYMMDD') || '.CSV' AS FileName,
	WorkISSExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	DeductibleAmount,
	CoverageCode,
	ConstructionCode,
	ISOFireProtectionCode,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	ZipPostalCode,
	DeductibleIndicatorCode,
	VehicleModelYear,
	DefensiveDriverCode,
	PolicyLowerLimit,
	PolicyUpperLimit,
	RatingZoneCode,
	TerminalZoneCode,
	WrittenExposure,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	ClaimLossDate,
	TransactionEffectiveDate,
	CoverageGroupCode,
	VehicleNumber,
	IncludeUIM
	FROM RTR_CL_PL_CL
),
SRT_ISSFlatFile_Auto_CL AS (
	SELECT
	FileName, 
	WorkISSExtractId, 
	AuditId, 
	CreatedDate, 
	EDWPremiumMasterCalculationPKId, 
	EDWLossMasterCalculationPKId, 
	TypeBureauCode, 
	BureauLineOfInsurance, 
	BureauCompanyNumber, 
	StateProvinceCode, 
	PremiumMasterRunDate, 
	LossMasterRunDate, 
	PolicyKey, 
	PremiumMasterClassCode, 
	LossMasterClassCode, 
	ClaimNumber, 
	ClaimantNumber, 
	RiskTerritoryCode, 
	PolicyEffectiveDate, 
	CauseOfLoss, 
	DeductibleAmount, 
	CoverageCode, 
	SublineCode, 
	PackageModificationAdjustmentGroupDescription, 
	PremiumMasterDirectWrittenPremiumAmount, 
	PaidLossAmount, 
	OutstandingLossAmount, 
	PolicyExpirationDate, 
	InceptionToDatePaidLossAmount, 
	ClaimantCoverageDetailId, 
	AnnualStatementLineNumber, 
	ZipPostalCode, 
	DeductibleIndicatorCode, 
	PolicyLowerLimit, 
	PolicyUpperLimit, 
	RatingZoneCode, 
	TerminalZoneCode, 
	WrittenExposure, 
	PaidAllocatedLossAdjustmentExpenseAmount, 
	OutstandingAllocatedLossAdjustmentExpenseAmount, 
	ClaimLossDate, 
	TransactionEffectiveDate, 
	CoverageGroupCode, 
	VehicleNumber, 
	IncludeUIM
	FROM EXP_Target_CL
	ORDER BY PolicyKey ASC, PremiumMasterClassCode ASC, CoverageCode ASC, AnnualStatementLineNumber ASC, ZipPostalCode ASC
),
ISSFlatFile_Auto_CL AS (
	INSERT INTO ISSFlatFile_Auto
	(FileName, WorkISSExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit, PolicyUpperLimit, RatingZoneCode, TerminalZoneCode, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber, IncludeUIM)
	SELECT 
	FILENAME, 
	WORKISSEXTRACTID, 
	AUDITID, 
	CREATEDDATE, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	BUREAUCOMPANYNUMBER, 
	STATEPROVINCECODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	POLICYKEY, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	CLAIMNUMBER, 
	CLAIMANTNUMBER, 
	RISKTERRITORYCODE, 
	POLICYEFFECTIVEDATE, 
	CAUSEOFLOSS, 
	DEDUCTIBLEAMOUNT, 
	COVERAGECODE, 
	SUBLINECODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	ZIPPOSTALCODE, 
	DEDUCTIBLEINDICATORCODE, 
	POLICYLOWERLIMIT, 
	POLICYUPPERLIMIT, 
	RATINGZONECODE, 
	TERMINALZONECODE, 
	WRITTENEXPOSURE, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	CLAIMLOSSDATE, 
	TRANSACTIONEFFECTIVEDATE, 
	COVERAGEGROUPCODE, 
	VEHICLENUMBER, 
	INCLUDEUIM
	FROM SRT_ISSFlatFile_Auto_CL
),