WITH
SQ_WorkAAIS_Loss AS (
	SELECT WorkAAISExtract.WorkAAISExtractId, WorkAAISExtract.AuditId, WorkAAISExtract.CreatedDate, WorkAAISExtract.EDWPremiumMasterCalculationPKId, WorkAAISExtract.EDWLossMasterCalculationPKId, WorkAAISExtract.TypeBureauCode, WorkAAISExtract.BureauLineOfInsurance, WorkAAISExtract.PremiumMasterRunDate, WorkAAISExtract.LossMasterRunDate, WorkAAISExtract.BureauCompanyNumber, WorkAAISExtract.PolicyKey, WorkAAISExtract.StateProvinceCode, WorkAAISExtract.RatingCounty, WorkAAISExtract.PremiumMasterDirectWrittenPremiumAmount, WorkAAISExtract.PaidLossAmount, WorkAAISExtract.OutstandingLossAmount, WorkAAISExtract.LossMasterNewClaimCount, WorkAAISExtract.PremiumMasterClassCode, WorkAAISExtract.LossMasterClassCode, WorkAAISExtract.DeductibleAmount, WorkAAISExtract.BureauAnnualStatementLineCode, WorkAAISExtract.BureauOrganizationCode, WorkAAISExtract.TerrorismRiskIndicator, WorkAAISExtract.InlandMarinePropertyAmountOfInsurance, WorkAAISExtract.ConstructionCode, WorkAAISExtract.ISOFireProtectionCode, WorkAAISExtract.InsuranceSegmentDescription, WorkAAISExtract.PolicyOfferingDescription, WorkAAISExtract.PolicyTerm, WorkAAISExtract.CauseOfLossName, WorkAAISExtract.ClaimLossDate, WorkAAISExtract.ZipPostalCode, WorkAAISExtract.CoverageEffectiveDate, WorkAAISExtract.CoverageExpirationDate, WorkAAISExtract.InceptionToDatePaidLossAmount, WorkAAISExtract.ClaimantCoverageDetailId, WorkAAISExtract.ClaimNumber, WorkAAISExtract.AnnualStatementLineNumber 
	, WorkAAISExtract.PaidAllocatedLossAdjustmentExpenseAmount
	, WorkAAISExtract.OutstandingAllocatedLossAdjustmentExpenseAmount 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkAAISExtract 
	WHERE
	 WorkAAISExtract.EDWLossMasterCalculationPKId<>-1
	@{pipeline().parameters.WHERE_CLAUSE_LOSS}
),
EXP_Loss AS (
	SELECT
	WorkAAISExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	PremiumMasterRunDate,
	LossMasterRunDate,
	BureauCompanyNumber,
	PolicyKey,
	StateProvinceCode,
	RatingCounty,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	LossMasterNewClaimCount,
	PremiumMasterClassCode,
	LossMasterClassCode,
	DeductibleAmount,
	BureauAnnualStatementLineCode,
	BureauOrganizationCode,
	TerrorismRiskIndicator,
	InlandMarinePropertyAmountOfInsurance,
	ConstructionCode,
	ISOFireProtectionCode,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	PolicyTerm,
	CauseOfLossName,
	ClaimLossDate,
	ZipPostalCode,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	ClaimNumber,
	AnnualStatementLineNumber,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM SQ_WorkAAIS_Loss
),
SQ_WorkAAIS_Prem_Unique AS (
	with prem_unique as
		(select CONCAT(
		TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey, StateProvinceCode, RatingCounty, PaidLossAmount, OutstandingLossAmount, LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator, InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, AnnualStatementLineNumber) as concct, count(*) count from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkAAISExtract
			where EDWPremiumMasterCalculationPKId<>-1
			@{pipeline().parameters.WHERE_CLAUSE_PREMIUM}
			
			group by CONCAT(
		TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey, StateProvinceCode, RatingCounty, PaidLossAmount, OutstandingLossAmount, LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator, InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, AnnualStatementLineNumber) having count(*)=1)
		
		select distinct WorkAAISExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey, StateProvinceCode, RatingCounty, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator, InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, AnnualStatementLineNumber
		,PaidAllocatedLossAdjustmentExpenseAmount,OutstandingAllocatedLossAdjustmentExpenseAmount
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkAAISExtract WorkAAISExtract join prem_unique b
		on (CONCAT(
		TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey, StateProvinceCode, RatingCounty, PaidLossAmount, OutstandingLossAmount, LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator, InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, AnnualStatementLineNumber)=b.concct)
		where WorkAAISExtract.EDWPremiumMasterCalculationPKId<>-1
		@{pipeline().parameters.WHERE_CLAUSE_PREMIUM}
),
EXP_PremUnique AS (
	SELECT
	WorkAAISExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	PremiumMasterRunDate,
	LossMasterRunDate,
	BureauCompanyNumber,
	PolicyKey,
	StateProvinceCode,
	RatingCounty,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	LossMasterNewClaimCount,
	PremiumMasterClassCode,
	LossMasterClassCode,
	DeductibleAmount,
	BureauAnnualStatementLineCode,
	BureauOrganizationCode,
	TerrorismRiskIndicator,
	InlandMarinePropertyAmountOfInsurance,
	ConstructionCode,
	ISOFireProtectionCode,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	PolicyTerm,
	CauseOfLossName,
	ClaimLossDate,
	ZipPostalCode,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	ClaimNumber,
	AnnualStatementLineNumber,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM SQ_WorkAAIS_Prem_Unique
),
SQ_WorkAAIS_Premium_Dup AS (
	WITH ROLLUP_TABLE_TEMP
	AS
	(SELECT
			SUM(PremiumMasterDirectWrittenPremiumAmount) ROLL_UP_DWP_AMT
			,MAX(WorkAAISExtractId) MAX_ISS_KEY
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkAAISExtract a
		WHERE a.EDWPremiumMasterCalculationPKId <> -1
		@{pipeline().parameters.WHERE_CLAUSE_PREMIUM}
		GROUP BY CONCAT(
		TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey,
		StateProvinceCode, RatingCounty, PaidLossAmount, OutstandingLossAmount, LossMasterNewClaimCount, PremiumMasterClassCode,
		LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator,
		InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription,
		PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate,
		CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, AnnualStatementLineNumber)
		HAVING COUNT(1) > 1
		AND SUM(PremiumMasterDirectWrittenPremiumAmount) <> 0)
	
	
	SELECT
		WorkAAISExtract.WorkAAISExtractId
		,WorkAAISExtract.AuditId
		,WorkAAISExtract.CreatedDate
		,WorkAAISExtract.EDWPremiumMasterCalculationPKId
		,WorkAAISExtract.EDWLossMasterCalculationPKId
		,WorkAAISExtract.TypeBureauCode
		,WorkAAISExtract.BureauLineOfInsurance
		,WorkAAISExtract.PremiumMasterRunDate
		,WorkAAISExtract.LossMasterRunDate
		,WorkAAISExtract.BureauCompanyNumber
		,WorkAAISExtract.PolicyKey
		,WorkAAISExtract.StateProvinceCode
		,WorkAAISExtract.RatingCounty
		,ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT AS PremiumMasterDirectWrittenPremiumAmount
		,WorkAAISExtract.PaidLossAmount
		,WorkAAISExtract.OutstandingLossAmount
		,WorkAAISExtract.LossMasterNewClaimCount
		,WorkAAISExtract.PremiumMasterClassCode
		,WorkAAISExtract.LossMasterClassCode
		,WorkAAISExtract.DeductibleAmount
		,WorkAAISExtract.BureauAnnualStatementLineCode
		,WorkAAISExtract.BureauOrganizationCode
		,WorkAAISExtract.TerrorismRiskIndicator
		,WorkAAISExtract.InlandMarinePropertyAmountOfInsurance
		,WorkAAISExtract.ConstructionCode
		,WorkAAISExtract.ISOFireProtectionCode
		,WorkAAISExtract.InsuranceSegmentDescription
		,WorkAAISExtract.PolicyOfferingDescription
		,WorkAAISExtract.PolicyTerm
		,WorkAAISExtract.CauseOfLossName
		,WorkAAISExtract.ClaimLossDate
		,WorkAAISExtract.ZipPostalCode
		,WorkAAISExtract.CoverageEffectiveDate
		,WorkAAISExtract.CoverageExpirationDate
		,WorkAAISExtract.InceptionToDatePaidLossAmount
		,WorkAAISExtract.ClaimantCoverageDetailId
		,WorkAAISExtract.ClaimNumber
		,WorkAAISExtract.AnnualStatementLineNumber
	      ,WorkAAISExtract.PaidAllocatedLossAdjustmentExpenseAmount
	      ,WorkAAISExtract.OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM ROLLUP_TABLE_TEMP
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkAAISExtract WorkAAISExtract
		ON WorkAAISExtract.WorkAAISExtractId = ROLLUP_TABLE_TEMP.MAX_ISS_KEY
),
EXP_PremiumDup AS (
	SELECT
	WorkAAISExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	PremiumMasterRunDate,
	LossMasterRunDate,
	BureauCompanyNumber,
	PolicyKey,
	StateProvinceCode,
	RatingCounty,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	LossMasterNewClaimCount,
	PremiumMasterClassCode,
	LossMasterClassCode,
	DeductibleAmount,
	BureauAnnualStatementLineCode,
	BureauOrganizationCode,
	TerrorismRiskIndicator,
	InlandMarinePropertyAmountOfInsurance,
	ConstructionCode,
	ISOFireProtectionCode,
	InsuranceSegmentDescription,
	PolicyOfferingDescription,
	PolicyTerm,
	CauseOfLossName,
	ClaimLossDate,
	ZipPostalCode,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	ClaimNumber,
	AnnualStatementLineNumber,
	PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM SQ_WorkAAIS_Premium_Dup
),
Union AS (
	SELECT WorkAAISExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey, StateProvinceCode, RatingCounty, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator, InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, AnnualStatementLineNumber, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM EXP_Loss
	UNION
	SELECT WorkAAISExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey, StateProvinceCode, RatingCounty, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator, InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, AnnualStatementLineNumber, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM EXP_PremUnique
	UNION
	SELECT WorkAAISExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey, StateProvinceCode, RatingCounty, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator, InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, AnnualStatementLineNumber, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM EXP_PremiumDup
),
EXP_Cleansing AS (
	SELECT
	WorkAAISExtractId AS i_WorkAAISExtractId,
	AuditId AS i_AuditId,
	CreatedDate AS i_CreatedDate,
	EDWPremiumMasterCalculationPKId AS i_EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId AS i_EDWLossMasterCalculationPKId,
	TypeBureauCode AS i_TypeBureauCode,
	BureauLineOfInsurance AS i_BureauLineOfInsurance,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	LossMasterRunDate AS i_LossMasterRunDate,
	BureauCompanyNumber AS i_BureauCompanyNumber,
	PolicyKey AS i_PolicyKey,
	StateProvinceCode AS i_StateProvinceCode,
	RatingCounty AS i_RatingCounty,
	PremiumMasterDirectWrittenPremiumAmount AS i_PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount AS i_PaidLossAmount,
	OutstandingLossAmount AS i_OutstandingLossAmount,
	LossMasterNewClaimCount AS i_LossMasterNewClaimCount,
	PremiumMasterClassCode AS i_PremiumMasterClassCode,
	LossMasterClassCode AS i_LossMasterClassCode,
	DeductibleAmount AS i_DeductibleAmount,
	BureauAnnualStatementLineCode AS i_BureauAnnualStatementLineCode,
	BureauOrganizationCode AS i_BureauOrganizationCode,
	TerrorismRiskIndicator AS i_TerrorismRiskIndicator,
	InlandMarinePropertyAmountOfInsurance AS i_InlandMarinePropertyAmountOfInsurance,
	ConstructionCode AS i_ConstructionCode,
	ISOFireProtectionCode AS i_ISOFireProtectionCode,
	InsuranceSegmentDescription AS i_InsuranceSegmentDescription,
	PolicyOfferingDescription AS i_PolicyOfferingDescription,
	PolicyTerm AS i_PolicyTerm,
	CauseOfLossName AS i_CauseOfLossName,
	ClaimLossDate AS i_ClaimLossDate,
	ZipPostalCode AS i_ZipPostalCode,
	CoverageEffectiveDate AS i_CoverageEffectiveDate,
	CoverageExpirationDate AS i_CoverageExpirationDate,
	InceptionToDatePaidLossAmount AS i_InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId AS i_ClaimantCoverageDetailId,
	ClaimNumber AS i_ClaimNumber,
	AnnualStatementLineNumber AS i_AnnualStatementLineNumber,
	PaidAllocatedLossAdjustmentExpenseAmount AS i_PaidAllocatedLossAdjustmentExpenseAmount,
	OutstandingAllocatedLossAdjustmentExpenseAmount AS i_OutstandingAllocatedLossAdjustmentExpenseAmount,
	i_WorkAAISExtractId AS o_WorkAAISExtractId,
	i_AuditId AS o_AuditId,
	i_CreatedDate AS o_CreatedDate,
	i_EDWPremiumMasterCalculationPKId AS o_EDWPremiumMasterCalculationPKId,
	i_EDWLossMasterCalculationPKId AS o_EDWLossMasterCalculationPKId,
	-- *INF*: RTRIM(LTRIM(i_TypeBureauCode))
	RTRIM(LTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: RTRIM(LTRIM(i_BureauLineOfInsurance))
	RTRIM(LTRIM(i_BureauLineOfInsurance)) AS o_BureauLineOfInsurance,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_LossMasterRunDate AS o_LossMasterRunDate,
	-- *INF*: RTRIM(LTRIM(i_BureauCompanyNumber))
	RTRIM(LTRIM(i_BureauCompanyNumber)) AS o_BureauCompanyNumber,
	-- *INF*: RTRIM(LTRIM(i_PolicyKey))
	RTRIM(LTRIM(i_PolicyKey)) AS o_PolicyKey,
	-- *INF*: RTRIM(LTRIM(i_StateProvinceCode))
	RTRIM(LTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	-- *INF*: RTRIM(LTRIM(i_RatingCounty))
	RTRIM(LTRIM(i_RatingCounty)) AS o_RatingCounty,
	i_PremiumMasterDirectWrittenPremiumAmount AS o_PremiumMasterDirectWrittenPremiumAmount,
	i_PaidLossAmount AS o_PaidLossAmount,
	i_OutstandingLossAmount AS o_OutstandingLossAmount,
	i_LossMasterNewClaimCount AS o_LossMasterNewClaimCount,
	-- *INF*: RTRIM(LTRIM(i_PremiumMasterClassCode))
	RTRIM(LTRIM(i_PremiumMasterClassCode)) AS o_PremiumMasterClassCode,
	-- *INF*: RTRIM(LTRIM(i_LossMasterClassCode))
	RTRIM(LTRIM(i_LossMasterClassCode)) AS o_LossMasterClassCode,
	-- *INF*: RTRIM(LTRIM(i_DeductibleAmount))
	RTRIM(LTRIM(i_DeductibleAmount)) AS o_DeductibleAmount,
	-- *INF*: RTRIM(LTRIM(i_BureauAnnualStatementLineCode))
	RTRIM(LTRIM(i_BureauAnnualStatementLineCode)) AS o_BureauAnnualStatementLineCode,
	-- *INF*: RTRIM(LTRIM(i_BureauOrganizationCode))
	RTRIM(LTRIM(i_BureauOrganizationCode)) AS o_BureauOrganizationCode,
	-- *INF*: RTRIM(LTRIM(i_TerrorismRiskIndicator))
	RTRIM(LTRIM(i_TerrorismRiskIndicator)) AS o_TerrorismRiskIndicator,
	-- *INF*: RTRIM(LTRIM(i_InlandMarinePropertyAmountOfInsurance))
	RTRIM(LTRIM(i_InlandMarinePropertyAmountOfInsurance)) AS o_InlandMarinePropertyAmountOfInsurance,
	-- *INF*: RTRIM(LTRIM(i_ConstructionCode))
	RTRIM(LTRIM(i_ConstructionCode)) AS o_ConstructionCode,
	-- *INF*: RTRIM(LTRIM(i_ISOFireProtectionCode))
	RTRIM(LTRIM(i_ISOFireProtectionCode)) AS o_ISOFireProtectionCode,
	-- *INF*: RTRIM(LTRIM(i_InsuranceSegmentDescription))
	RTRIM(LTRIM(i_InsuranceSegmentDescription)) AS o_InsuranceSegmentDescription,
	-- *INF*: RTRIM(LTRIM(i_PolicyOfferingDescription))
	RTRIM(LTRIM(i_PolicyOfferingDescription)) AS o_PolicyOfferingDescription,
	-- *INF*: RTRIM(LTRIM(i_PolicyTerm))
	RTRIM(LTRIM(i_PolicyTerm)) AS o_PolicyTerm,
	-- *INF*: RTRIM(LTRIM(i_CauseOfLossName))
	RTRIM(LTRIM(i_CauseOfLossName)) AS o_CauseOfLossName,
	i_ClaimLossDate AS o_ClaimLossDate,
	-- *INF*: RTRIM(LTRIM(i_ZipPostalCode))
	RTRIM(LTRIM(i_ZipPostalCode)) AS o_ZipPostalCode,
	i_CoverageEffectiveDate AS o_CoverageEffectiveDate,
	i_CoverageExpirationDate AS o_CoverageExpirationDate,
	i_InceptionToDatePaidLossAmount AS o_InceptionToDatePaidLossAmount,
	i_ClaimantCoverageDetailId AS o_ClaimantCoverageDetailId,
	-- *INF*: RTRIM(LTRIM(i_ClaimNumber))
	RTRIM(LTRIM(i_ClaimNumber)) AS o_ClaimNumber,
	-- *INF*: IIF(ISNULL(i_AnnualStatementLineNumber),'N/A',RTRIM(LTRIM(i_AnnualStatementLineNumber)))
	IFF(i_AnnualStatementLineNumber IS NULL, 'N/A', RTRIM(LTRIM(i_AnnualStatementLineNumber))) AS o_AnnualStatementLineNumber,
	i_PaidAllocatedLossAdjustmentExpenseAmount AS o_PaidAllocatedLossAdjustmentExpenseAmount,
	i_OutstandingAllocatedLossAdjustmentExpenseAmount AS o_OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM Union
),
EXP_Append_Filename AS (
	SELECT
	-- *INF*: TO_CHAR(TRUNC(sysdate,'DD'),'YYYYMMDD')
	TO_CHAR(CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)), 'YYYYMMDD') AS v_RunDate,
	'InlandMarine_' || v_RunDate || '.CSV' AS FileName,
	o_WorkAAISExtractId AS WorkAAISExtractId,
	o_AuditId AS AuditId,
	o_CreatedDate AS CreatedDate,
	o_EDWPremiumMasterCalculationPKId AS EDWPremiumMasterCalculationPKId,
	o_EDWLossMasterCalculationPKId AS EDWLossMasterCalculationPKId,
	o_TypeBureauCode AS TypeBureauCode,
	o_BureauLineOfInsurance AS BureauLineOfInsurance,
	o_PremiumMasterRunDate AS PremiumMasterRunDate,
	o_LossMasterRunDate AS LossMasterRunDate,
	o_BureauCompanyNumber AS BureauCompanyNumber,
	o_PolicyKey AS PolicyKey,
	o_StateProvinceCode AS StateProvinceCode,
	o_RatingCounty AS RatingCounty,
	o_PremiumMasterDirectWrittenPremiumAmount AS PremiumMasterDirectWrittenPremiumAmount,
	o_PaidLossAmount AS PaidLossAmount,
	o_OutstandingLossAmount AS OutstandingLossAmount,
	o_LossMasterNewClaimCount AS LossMasterNewClaimCount,
	o_PremiumMasterClassCode AS PremiumMasterClassCode,
	o_LossMasterClassCode AS LossMasterClassCode,
	o_DeductibleAmount AS DeductibleAmount,
	o_BureauAnnualStatementLineCode AS BureauAnnualStatementLineCode,
	o_BureauOrganizationCode AS BureauOrganizationCode,
	o_TerrorismRiskIndicator AS TerrorismRiskIndicator,
	o_InlandMarinePropertyAmountOfInsurance AS InlandMarinePropertyAmountOfInsurance,
	o_ConstructionCode AS ConstructionCode,
	o_ISOFireProtectionCode AS ISOFireProtectionCode,
	o_InsuranceSegmentDescription AS InsuranceSegmentDescription,
	o_PolicyOfferingDescription AS PolicyOfferingDescription,
	o_PolicyTerm AS PolicyTerm,
	o_CauseOfLossName AS CauseOfLossName,
	o_ClaimLossDate AS ClaimLossDate,
	o_ZipPostalCode AS ZipPostalCode,
	o_CoverageEffectiveDate AS CoverageEffectiveDate,
	o_CoverageExpirationDate AS CoverageExpirationDate,
	o_InceptionToDatePaidLossAmount AS InceptionToDatePaidLossAmount,
	o_ClaimantCoverageDetailId AS ClaimantCoverageDetailId,
	o_ClaimNumber AS ClaimNumber,
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	o_PaidAllocatedLossAdjustmentExpenseAmount,
	o_OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM EXP_Cleansing
),
SRT_AAIS_Flatfile AS (
	SELECT
	FileName, 
	WorkAAISExtractId, 
	AuditId, 
	CreatedDate, 
	EDWPremiumMasterCalculationPKId, 
	EDWLossMasterCalculationPKId, 
	TypeBureauCode, 
	BureauLineOfInsurance, 
	PremiumMasterRunDate, 
	LossMasterRunDate, 
	BureauCompanyNumber, 
	PolicyKey, 
	StateProvinceCode, 
	RatingCounty, 
	PremiumMasterDirectWrittenPremiumAmount, 
	PaidLossAmount, 
	OutstandingLossAmount, 
	LossMasterNewClaimCount, 
	PremiumMasterClassCode, 
	LossMasterClassCode, 
	DeductibleAmount, 
	BureauAnnualStatementLineCode, 
	BureauOrganizationCode, 
	TerrorismRiskIndicator, 
	InlandMarinePropertyAmountOfInsurance, 
	ConstructionCode, 
	ISOFireProtectionCode, 
	InsuranceSegmentDescription, 
	PolicyOfferingDescription, 
	PolicyTerm, 
	CauseOfLossName, 
	ClaimLossDate, 
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	InceptionToDatePaidLossAmount, 
	ClaimantCoverageDetailId, 
	ClaimNumber, 
	AnnualStatementLineNumber, 
	ZipPostalCode, 
	o_PaidAllocatedLossAdjustmentExpenseAmount, 
	o_OutstandingAllocatedLossAdjustmentExpenseAmount
	FROM EXP_Append_Filename
	ORDER BY PolicyKey ASC, PremiumMasterClassCode ASC, AnnualStatementLineNumber ASC, ZipPostalCode ASC
),
AAISFlatFile AS (
	INSERT INTO AAISFlatFile
	(FileName, WorkAAISExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyNumber, PolicyKey, StateProvinceCode, RatingCounty, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PaidAllocatedlossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, LossMasterNewClaimCount, PremiumMasterClassCode, LossMasterClassCode, DeductibleAmount, BureauAnnualStatementLineCode, BureauOrganizationCode, TerrorismRiskIndicator, InlandMarinePropertyAmountOfInsurance, ConstructionCode, ISOFireProtectionCode, InsuranceSegmentDescription, PolicyOfferingDescription, PolicyTerm, CauseOfLossName, ClaimLossDate, ZipPostalCode, CoverageEffectiveDate, CoverageExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, ClaimNumber, AnnualStatementLineNumber)
	SELECT 
	FILENAME, 
	WORKAAISEXTRACTID, 
	AUDITID, 
	CREATEDDATE, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	BUREAUCOMPANYNUMBER, 
	POLICYKEY, 
	STATEPROVINCECODE, 
	RATINGCOUNTY, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	o_PaidAllocatedLossAdjustmentExpenseAmount AS PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	o_OutstandingAllocatedLossAdjustmentExpenseAmount AS OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	LOSSMASTERNEWCLAIMCOUNT, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	DEDUCTIBLEAMOUNT, 
	BUREAUANNUALSTATEMENTLINECODE, 
	BUREAUORGANIZATIONCODE, 
	TERRORISMRISKINDICATOR, 
	INLANDMARINEPROPERTYAMOUNTOFINSURANCE, 
	CONSTRUCTIONCODE, 
	ISOFIREPROTECTIONCODE, 
	INSURANCESEGMENTDESCRIPTION, 
	POLICYOFFERINGDESCRIPTION, 
	POLICYTERM, 
	CAUSEOFLOSSNAME, 
	CLAIMLOSSDATE, 
	ZIPPOSTALCODE, 
	COVERAGEEFFECTIVEDATE, 
	COVERAGEEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	CLAIMANTCOVERAGEDETAILID, 
	CLAIMNUMBER, 
	ANNUALSTATEMENTLINENUMBER
	FROM SRT_AAIS_Flatfile
),