WITH
SQ_EarnedPremiumMonthlyCalculation_Balance AS (
	Declare @Date1 datetime
	
	set @Date1=DATEADD(SS,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-(@{pipeline().parameters.NO_OF_MONTHS}),0))
	
	
	SELECT EPMC.CurrentSnapshotFlag, EPMC.AuditID, EPMC.EffectiveDate, EPMC.ExpirationDate, EPMC.SourceSystemID, EPMC.PolicyKey, EPMC.AgencyAKID, EPMC.ContractCustomerAKID, EPMC.PolicyAKID, EPMC.RiskLocationAKID, EPMC.PolicyCoverageAKID, EPMC.StatisticalCoverageAKID, EPMC.ReinsuranceCoverageAKID, EPMC.PremiumTransactionAKID, EPMC.BureauStatisticalCodeAKID, EPMC.PremiumMasterCalculationPKID, EPMC.PolicyEffectiveDate, EPMC.PolicyExpirationDate, EPMC.StatisticalCoverageEffectiveDate, EPMC.StatisticalCoverageExpirationDate, EPMC.StatisticalCoverageCancellationDate, EPMC.PremiumTransactionEnteredDate, EPMC.PremiumTransactionEffectiveDate, EPMC.PremiumTransactionExpirationDate, EPMC.PremiumTransactionBookedDate, EPMC.PremiumTransactionCode, EPMC.PremiumTransactionAmount, EPMC.FullTermPremium, EPMC.PremiumType, EPMC.ReasonAmendedCode, EPMC.EarnedPremium, EPMC.ChangeInEarnedPremium, EPMC.UnearnedPremium, EPMC.ChangeInUnearnedPremium, EPMC.ProductCode, EPMC.AnnualStatementLineCode, EPMC.SubAnnualStatementLineCode, EPMC.NonSubAnnualStatementLineCode, EPMC.AnnualStatementLineProductCode, EPMC.LineOfBusinessCode, EPMC.PolicyOfferingCode, EPMC.RunDate, EPMC.RatingCoverageAKId, EPMC.RatingCoverageEffectiveDate, EPMC.RatingCoverageExpirationDate, EPMC.EarnedExposure, EPMC.ChangeInEarnedExposure,
	EPMC.Exposure
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.EarnedPremiumMonthlyCalculation EPMC
	where EPMC.UnearnedPremium<>0.0
	and EPMC.PremiumTransactionExpirationDate<=EPMC.Rundate
	and EPMC.StatisticalCoverageCancellationDate<>'2100-12-31 23:59:59'
	and EPMC.Rundate=@Date1
	@{pipeline().parameters.WHERE_CLAUSE_BALANCE}
	Order BY EPMC.PolicyAKID,EPMC.StatisticalCoverageAKID,EPMC.RatingCoverageAKId,EPMC.PremiumMasterCalculationPKID,EPMC.PremiumType,EPMC.AnnualStatementLineCode, EPMC.SubAnnualStatementLineCode, EPMC.NonSubAnnualStatementLineCode,EPMC.LineOfBusinessCode,EPMC.RunDate
),
EXP_Src_Data_Collect AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	PolicyKey,
	AgencyAKID,
	ContractCustomerAKID,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	ReinsuranceCoverageAKID,
	PremiumTransactionAKID,
	BureauStatisticalCodeAKID,
	PremiumMasterCalculationPKID,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	StatisticalCoverageCancellationDate,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionCode,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	ReasonAmendedCode,
	EarnedPremium,
	ChangeInEarnedPremium,
	UnearnedPremium,
	ChangeInUnearnedPremium,
	ProductCode,
	AnnualStatementLineCode,
	SubAnnualStatementLineCode,
	NonSubAnnualStatementLineCode,
	AnnualStatementLineProductCode,
	LineOfBusinessCode,
	PolicyOfferingCode,
	RunDate,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART( Last_day(ADD_TO_DATE(RunDate,'MM',1)), 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(Last_day(ADD_TO_DATE(RunDate, 'MM', 1)), 'HH', 23), 'MI', 59), 'SS', 59) AS TO_Be_Rundate,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	EarnedExposure,
	ChangeInEarnedExposure,
	Exposure
	FROM SQ_EarnedPremiumMonthlyCalculation_Balance
),
LKP_GetExposure AS (
	SELECT
	Exposure,
	in_PremiumMasterCalculationPKID,
	in_PremiumType,
	in_AnnualStatementLineCode,
	in_SubAnnualStatementLineCode,
	in_NonSubAnnualStatementLineCode,
	in_LineOfBusinessCode,
	PremiumMasterCalculationPKID,
	PremiumType,
	AnnualStatementLineCode,
	SubAnnualStatementLineCode,
	NonSubAnnualStatementLineCode,
	LineOfBusinessCode
	FROM (
		SELECT 
		EarnedPremiumMonthlyCalculation.Exposure as Exposure, 
		EarnedPremiumMonthlyCalculation.PremiumMasterCalculationPKID as PremiumMasterCalculationPKID, 
		EarnedPremiumMonthlyCalculation.PremiumType as PremiumType, 
		EarnedPremiumMonthlyCalculation.AnnualStatementLineCode as AnnualStatementLineCode, 
		EarnedPremiumMonthlyCalculation.SubAnnualStatementLineCode as SubAnnualStatementLineCode, 
		EarnedPremiumMonthlyCalculation.NonSubAnnualStatementLineCode as NonSubAnnualStatementLineCode, 
		EarnedPremiumMonthlyCalculation.LineOfBusinessCode as LineOfBusinessCode,
		EarnedPremiumMonthlyCalculation.RunDate as RunDate
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.EarnedPremiumMonthlyCalculation with (NOLOCK)
		where exists
		(SELECT EPMC.PremiumMasterCalculationPKID
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.EarnedPremiumMonthlyCalculation EPMC
		where EPMC.UnearnedPremium<>0.0
		and EPMC.PremiumTransactionExpirationDate<=EPMC.Rundate
		and EPMC.StatisticalCoverageCancellationDate<>'2100-12-31 23:59:59'
		and EPMC.Rundate=DATEADD(SS,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-(@{pipeline().parameters.NO_OF_MONTHS}),0))
		and EarnedPremiumMonthlyCalculation.PremiumMasterCalculationPKID=EPMC.PremiumMasterCalculationPKID)
		order by 
		EarnedPremiumMonthlyCalculation.PremiumMasterCalculationPKID, 
		EarnedPremiumMonthlyCalculation.PremiumType, 
		EarnedPremiumMonthlyCalculation.AnnualStatementLineCode, 
		EarnedPremiumMonthlyCalculation.SubAnnualStatementLineCode, 
		EarnedPremiumMonthlyCalculation.NonSubAnnualStatementLineCode , 
		EarnedPremiumMonthlyCalculation.LineOfBusinessCode,
		EarnedPremiumMonthlyCalculation.RunDate ASC--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationPKID,PremiumType,AnnualStatementLineCode,SubAnnualStatementLineCode,NonSubAnnualStatementLineCode,LineOfBusinessCode ORDER BY Exposure) = 1
),
LKP_GetFirstAudits AS (
	SELECT
	PremiumMasterCalculationID,
	PolicyAKID,
	PremiumMasterPremiumType
	FROM (
		select A.PremiumMasterCalculationID as PremiumMasterCalculationID,A.PolicyAKID as PolicyAKID,A.PremiumMasterPremiumType as PremiumMasterPremiumType 
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation A
		where (PremiumMasterTransactionCode in ('14','24')
		or Premiummasterrundate>PremiumMasterCoverageExpirationDate)
		and PremiumMasterRunDate=DATEADD(SS,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-(@{pipeline().parameters.NO_OF_MONTHS})+1,0))
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,PremiumMasterPremiumType ORDER BY PremiumMasterCalculationID DESC) = 1
),
EXP_SetEarnedPremiumValues AS (
	SELECT
	LKP_GetFirstAudits.PremiumMasterCalculationID,
	-- *INF*: IIF(ISNULL(PremiumMasterCalculationID),'1','2')
	IFF(PremiumMasterCalculationID IS NULL, '1', '2') AS Decision_Flag,
	EXP_Src_Data_Collect.CurrentSnapshotFlag,
	999 AS AuditID,
	EXP_Src_Data_Collect.EffectiveDate,
	EXP_Src_Data_Collect.ExpirationDate,
	EXP_Src_Data_Collect.SourceSystemID,
	CURRENT_TIMESTAMP AS CreatedDate,
	EXP_Src_Data_Collect.PolicyKey,
	EXP_Src_Data_Collect.AgencyAKID,
	EXP_Src_Data_Collect.ContractCustomerAKID,
	EXP_Src_Data_Collect.PolicyAKID,
	EXP_Src_Data_Collect.RiskLocationAKID,
	EXP_Src_Data_Collect.PolicyCoverageAKID,
	EXP_Src_Data_Collect.StatisticalCoverageAKID,
	EXP_Src_Data_Collect.ReinsuranceCoverageAKID,
	EXP_Src_Data_Collect.PremiumTransactionAKID,
	EXP_Src_Data_Collect.BureauStatisticalCodeAKID,
	EXP_Src_Data_Collect.PremiumMasterCalculationPKID,
	EXP_Src_Data_Collect.PolicyEffectiveDate,
	EXP_Src_Data_Collect.PolicyExpirationDate,
	EXP_Src_Data_Collect.StatisticalCoverageEffectiveDate,
	EXP_Src_Data_Collect.StatisticalCoverageExpirationDate,
	EXP_Src_Data_Collect.StatisticalCoverageCancellationDate,
	EXP_Src_Data_Collect.PremiumTransactionEnteredDate,
	EXP_Src_Data_Collect.PremiumTransactionEffectiveDate,
	EXP_Src_Data_Collect.PremiumTransactionExpirationDate,
	EXP_Src_Data_Collect.PremiumTransactionBookedDate,
	EXP_Src_Data_Collect.PremiumTransactionCode,
	EXP_Src_Data_Collect.PremiumTransactionAmount,
	0.0 AS O_PremiumTransactionAmount,
	EXP_Src_Data_Collect.FullTermPremium,
	0.0 AS O_FullTermPremium,
	EXP_Src_Data_Collect.PremiumType,
	EXP_Src_Data_Collect.ReasonAmendedCode,
	EXP_Src_Data_Collect.EarnedPremium,
	-- *INF*: IIF(Decision_Flag='1',EarnedPremium,EarnedPremium+UnearnedPremium)
	IFF(Decision_Flag = '1', EarnedPremium, EarnedPremium + UnearnedPremium) AS v_EarnedPremium,
	v_EarnedPremium AS O_EarnedPremium,
	EXP_Src_Data_Collect.ChangeInEarnedPremium,
	-- *INF*: IIF(Decision_Flag='1',0.0,v_EarnedPremium-EarnedPremium)
	IFF(Decision_Flag = '1', 0.0, v_EarnedPremium - EarnedPremium) AS v_ChangeInEarnedPremium,
	v_ChangeInEarnedPremium AS O_ChangeInEarnedPremium,
	EXP_Src_Data_Collect.UnearnedPremium,
	-- *INF*: IIF(Decision_Flag='1',UnearnedPremium,0.0)
	IFF(Decision_Flag = '1', UnearnedPremium, 0.0) AS v_UnearnedPremium,
	v_UnearnedPremium AS O_UnearnedPremium,
	EXP_Src_Data_Collect.ChangeInUnearnedPremium,
	-- *INF*: IIF(Decision_Flag='1',0.0,v_UnearnedPremium-UnearnedPremium)
	IFF(Decision_Flag = '1', 0.0, v_UnearnedPremium - UnearnedPremium) AS v_ChangeInUnearnedPremium,
	v_ChangeInUnearnedPremium AS O_ChangeInUnearnedPremium,
	EXP_Src_Data_Collect.ProductCode,
	EXP_Src_Data_Collect.AnnualStatementLineCode,
	EXP_Src_Data_Collect.SubAnnualStatementLineCode,
	EXP_Src_Data_Collect.NonSubAnnualStatementLineCode,
	EXP_Src_Data_Collect.AnnualStatementLineProductCode,
	EXP_Src_Data_Collect.LineOfBusinessCode,
	EXP_Src_Data_Collect.PolicyOfferingCode,
	EXP_Src_Data_Collect.TO_Be_Rundate,
	EXP_Src_Data_Collect.RatingCoverageAKId,
	EXP_Src_Data_Collect.RatingCoverageEffectiveDate,
	EXP_Src_Data_Collect.RatingCoverageExpirationDate,
	-- *INF*: --DATE_DIFF(StatisticalCoverageCancellationDate,PremiumTransactionEffectiveDate,'D')
	'' AS v_DaysTillCancellation,
	-- *INF*: --DATE_DIFF(PremiumTransactionExpirationDate,PremiumTransactionEffectiveDate,'D')
	'' AS v_TotalDays,
	LKP_GetExposure.Exposure AS LKP_Exposure,
	-- *INF*: IIF(ISNULL(LKP_Exposure),0.0,LKP_Exposure)
	IFF(LKP_Exposure IS NULL, 0.0, LKP_Exposure) AS v_Exposure,
	EXP_Src_Data_Collect.EarnedExposure,
	-- *INF*: IIF(Decision_Flag='1',EarnedExposure,v_Exposure)
	IFF(Decision_Flag = '1', EarnedExposure, v_Exposure) AS v_EarnedExposure,
	v_EarnedExposure AS O_EarnedExposure,
	EXP_Src_Data_Collect.ChangeInEarnedExposure,
	-- *INF*: IIF(Decision_Flag='1',0.0,v_Exposure-EarnedExposure)
	IFF(Decision_Flag = '1', 0.0, v_Exposure - EarnedExposure) AS v_ChangeInEarnedExposure,
	v_ChangeInEarnedExposure AS O_ChangeInEarnedExposure,
	EXP_Src_Data_Collect.Exposure
	FROM EXP_Src_Data_Collect
	LEFT JOIN LKP_GetExposure
	ON LKP_GetExposure.PremiumMasterCalculationPKID = EXP_Src_Data_Collect.PremiumMasterCalculationPKID AND LKP_GetExposure.PremiumType = EXP_Src_Data_Collect.PremiumType AND LKP_GetExposure.AnnualStatementLineCode = EXP_Src_Data_Collect.AnnualStatementLineCode AND LKP_GetExposure.SubAnnualStatementLineCode = EXP_Src_Data_Collect.SubAnnualStatementLineCode AND LKP_GetExposure.NonSubAnnualStatementLineCode = EXP_Src_Data_Collect.NonSubAnnualStatementLineCode AND LKP_GetExposure.LineOfBusinessCode = EXP_Src_Data_Collect.LineOfBusinessCode
	LEFT JOIN LKP_GetFirstAudits
	ON LKP_GetFirstAudits.PolicyAKID = EXP_Src_Data_Collect.PolicyAKID AND LKP_GetFirstAudits.PremiumMasterPremiumType = EXP_Src_Data_Collect.PremiumType
),
EarnedPremiumMonthlyCalculation AS (
	INSERT INTO EarnedPremiumMonthlyCalculation
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, PolicyKey, AgencyAKID, ContractCustomerAKID, PolicyAKID, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, ReinsuranceCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, PremiumMasterCalculationPKID, PolicyEffectiveDate, PolicyExpirationDate, StatisticalCoverageEffectiveDate, StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate, PremiumTransactionBookedDate, PremiumTransactionCode, PremiumTransactionAmount, FullTermPremium, PremiumType, ReasonAmendedCode, EarnedPremium, ChangeInEarnedPremium, UnearnedPremium, ChangeInUnearnedPremium, ProductCode, AnnualStatementLineCode, SubAnnualStatementLineCode, NonSubAnnualStatementLineCode, AnnualStatementLineProductCode, LineOfBusinessCode, PolicyOfferingCode, RunDate, RatingCoverageAKId, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, EarnedExposure, ChangeInEarnedExposure, Exposure)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	CreatedDate AS MODIFIEDDATE, 
	POLICYKEY, 
	AGENCYAKID, 
	CONTRACTCUSTOMERAKID, 
	POLICYAKID, 
	RISKLOCATIONAKID, 
	POLICYCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	REINSURANCECOVERAGEAKID, 
	PREMIUMTRANSACTIONAKID, 
	BUREAUSTATISTICALCODEAKID, 
	PREMIUMMASTERCALCULATIONPKID, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	STATISTICALCOVERAGEEFFECTIVEDATE, 
	STATISTICALCOVERAGEEXPIRATIONDATE, 
	STATISTICALCOVERAGECANCELLATIONDATE, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONEXPIRATIONDATE, 
	PREMIUMTRANSACTIONBOOKEDDATE, 
	PREMIUMTRANSACTIONCODE, 
	O_PremiumTransactionAmount AS PREMIUMTRANSACTIONAMOUNT, 
	O_FullTermPremium AS FULLTERMPREMIUM, 
	PREMIUMTYPE, 
	REASONAMENDEDCODE, 
	O_EarnedPremium AS EARNEDPREMIUM, 
	O_ChangeInEarnedPremium AS CHANGEINEARNEDPREMIUM, 
	O_UnearnedPremium AS UNEARNEDPREMIUM, 
	O_ChangeInUnearnedPremium AS CHANGEINUNEARNEDPREMIUM, 
	PRODUCTCODE, 
	ANNUALSTATEMENTLINECODE, 
	SUBANNUALSTATEMENTLINECODE, 
	NONSUBANNUALSTATEMENTLINECODE, 
	ANNUALSTATEMENTLINEPRODUCTCODE, 
	LINEOFBUSINESSCODE, 
	POLICYOFFERINGCODE, 
	TO_Be_Rundate AS RUNDATE, 
	RATINGCOVERAGEAKID, 
	RATINGCOVERAGEEFFECTIVEDATE, 
	RATINGCOVERAGEEXPIRATIONDATE, 
	O_EarnedExposure AS EARNEDEXPOSURE, 
	O_ChangeInEarnedExposure AS CHANGEINEARNEDEXPOSURE, 
	EXPOSURE
	FROM EXP_SetEarnedPremiumValues
),