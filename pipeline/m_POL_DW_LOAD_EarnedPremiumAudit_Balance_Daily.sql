WITH
SQ_EarnedPremiumDailyCalculation AS (
	Declare @Date_Monthly datetime,
	@Date_Daily datetime
	
	set @Date_Monthly=DATEADD(SS,-1,DATEADD(DD, DATEDIFF(DD,0,GETDATE())-(@{pipeline().parameters.NO_OF_DAYS}),0))
	SET @Date_Daily=DATEADD(SS,-1,DATEADD(DD, DATEDIFF(DD,0,GETDATE())-(@{pipeline().parameters.NO_OF_DAYS}),0))
	
	SELECT CurrentSnapshotFlag,AuditID,EffectiveDate,ExpirationDate,SourceSystemID,PolicyKey,AgencyAKID,ContractCustomerAKID,PolicyAKID,RiskLocationAKID,PolicyCoverageAKID,StatisticalCoverageAKID,ReinsuranceCoverageAKID,PremiumTransactionAKID,BureauStatisticalCodeAKID,PolicyEffectiveDate,PolicyExpirationDate,StatisticalCoverageEffectiveDate,StatisticalCoverageExpirationDate,StatisticalCoverageCancellationDate,PremiumTransactionEnteredDate,PremiumTransactionEffectiveDate,PremiumTransactionExpirationDate,PremiumTransactionBookedDate,PremiumTransactionCode,PremiumTransactionAmount,FullTermPremium,PremiumType,ReasonAmendedCode,EarnedPremium,ChangeInEarnedPremium,UnearnedPremium,ChangeInUnearnedPremium,ProductCode,AnnualStatementLineCode,SubAnnualStatementLineCode,NonSubAnnualStatementLineCode,AnnualStatementLineProductCode,LineOfBusinessCode,PolicyOfferingCode,RunDate,RatingCoverageAKId,RatingCoverageEffectiveDate,RatingCoverageExpirationDate,EarnedExposure,ChangeInEarnedExposure,Exposure,'Monthly' Process
	  FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.EarnedPremiumMonthlyCalculation
	  where UnearnedPremium<>0.0
	and PremiumTransactionExpirationDate<=Rundate
	and StatisticalCoverageCancellationDate<>'2100-12-31 23:59:59'
	and Rundate=@Date_Monthly
	  UNION
	SELECT CurrentSnapshotFlag,AuditID,EffectiveDate,ExpirationDate,SourceSystemID,PolicyKey,AgencyAKID,ContractCustomerAKID,PolicyAKID,RiskLocationAKID,PolicyCoverageAKID,StatisticalCoverageAKID,ReinsuranceCoverageAKID,PremiumTransactionAKID,BureauStatisticalCodeAKID,PolicyEffectiveDate,PolicyExpirationDate,StatisticalCoverageEffectiveDate,StatisticalCoverageExpirationDate,StatisticalCoverageCancellationDate,PremiumTransactionEnteredDate,PremiumTransactionEffectiveDate,PremiumTransactionExpirationDate,PremiumTransactionBookedDate,PremiumTransactionCode,PremiumTransactionAmount,FullTermPremium,PremiumType,ReasonAmendedCode,EarnedPremium,ChangeInEarnedPremium,UnearnedPremium,ChangeInUnearnedPremium,ProductCode,AnnualStatementLineCode,SubAnnualStatementLineCode,NonSubAnnualStatementLineCode,AnnualStatementLineProductCode,LineOfBusinessCode,PolicyOfferingCode,RunDate,RatingCoverageAKId,RatingCoverageEffectiveDate,RatingCoverageExpirationDate,EarnedExposure,ChangeInEarnedExposure,Exposure,'Daily' Process
	  FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.EarnedPremiumDailyCalculation
	    where UnearnedPremium<>0.0
	and PremiumTransactionExpirationDate<=Rundate
	and StatisticalCoverageCancellationDate<>'2100-12-31 23:59:59'
	and Rundate=@Date_Daily
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
	--                      SET_DATE_PART(ADD_TO_DATE(RunDate,'DD',1), 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,DATEADD(DAY,1,RunDate)),DATEADD(DAY,1,RunDate))),DATEADD(HOUR,23-DATE_PART(HOUR,DATEADD(DAY,1,RunDate)),DATEADD(DAY,1,RunDate)))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,DATEADD(DAY,1,RunDate)),DATEADD(DAY,1,RunDate))),DATEADD(HOUR,23-DATE_PART(HOUR,DATEADD(DAY,1,RunDate)),DATEADD(DAY,1,RunDate)))) AS TO_Be_Rundate,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	EarnedExposure,
	ChangeInEarnedExposure,
	Exposure,
	Process
	FROM SQ_EarnedPremiumDailyCalculation
),
LKP_GetExposure AS (
	SELECT
	Exposure,
	Premiumtransactionakid
	FROM (
		select Premiumtransactionakid as Premiumtransactionakid,Exposure as Exposure from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction
		where exposure<>0.0
		and case when PremiumTransactionBookedDate>=PremiumTransactionEnteredDate then PremiumTransactionBookedDate else PremiumTransactionEnteredDate end>='2001-01-01 00:00:00'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Premiumtransactionakid ORDER BY Exposure) = 1
),
LKP_GetFirstAudits AS (
	SELECT
	PremiumTransactionID,
	PolicyAKID,
	PremiumType
	FROM (
		select PT.Premiumtransactionid as Premiumtransactionid,P.Pol_ak_id as Policyakid,PT.PremiumType as PremiumType
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER2}.Policy P with(nolock)
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL with(nolock)
		ON RL.PolicyAKID = P.Pol_AK_ID
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with(nolock)
		ON  RL.RiskLocationAKID= PC.RiskLocationAKID
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with(nolock)
		ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		Inner Join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with(nolock)
		ON PT.RatingCoverageAKId = RC.RatingCoverageAKID
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_premium_transaction_code SPC
		on PT.SupPremiumTransactionCodeId=SPC.sup_prem_trans_code_id
		where PT.CurrentSnapshotFlag = '1' AND PT.SourceSystemID = 'DCT'
		AND PC.CurrentSnapshotFlag = '1' AND PC.SourceSystemID = 'DCT'
		AND RL.CurrentSnapshotFlag = '1' AND RL.SourceSystemID = 'DCT'
		AND P.crrnt_snpsht_flag = '1' AND P.source_sys_id = 'DCT'
		AND RC.EffectiveDate=PT.EffectiveDate
		and SPC.source_sys_id='DCT'
		and SPC.StandardPremiumTransactionCode in ('14','24')
		and convert(varchar(8),case when PT.PremiumtransactionEnteredDate>=PT.PremiumTransactionBookedDate then PT.PremiumtransactionEnteredDate else PT.PremiumTransactionBookedDate end,112) =convert(varchar(8),DATEADD(SS,-1,DATEADD(DD, DATEDIFF(D,0,GETDATE())-(@{pipeline().parameters.NO_OF_DAYS}),0)),112)
		
		UNION
		
		select PT.Premiumtransactionid as Premiumtransactionid,P.Pol_ak_id as Policyakid,PT.PremiumType as PremiumType
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER2}.Policy P with(nolock)
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL with(nolock)
		ON RL.PolicyAKID = P.Pol_AK_ID
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with(nolock)
		ON  RL.RiskLocationAKID= PC.RiskLocationAKID
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC with(nolock)
		ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		Inner Join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with(nolock)
		ON PT.RatingCoverageAKId = SC.StatisticalCoverageAKID
		where PT.CurrentSnapshotFlag = '1' AND PT.SourceSystemID = 'PMS'
		AND PC.CurrentSnapshotFlag = '1' AND PC.SourceSystemID = 'PMS'
		AND RL.CurrentSnapshotFlag = '1' AND RL.SourceSystemID = 'PMS'
		AND P.crrnt_snpsht_flag = '1' AND P.source_sys_id = 'PMS'
		and SC.CurrentSnapshotFlag=1 and SC.SourceSystemID='PMS'
		and PT.PremiumTransactionCode in ('14','24')
		and convert(varchar(8),case when PT.PremiumtransactionEnteredDate>=PT.PremiumTransactionBookedDate then PT.PremiumtransactionEnteredDate else PT.PremiumTransactionBookedDate end,112) =convert(varchar(8),DATEADD(SS,-1,DATEADD(DD, DATEDIFF(D,0,GETDATE())-(@{pipeline().parameters.NO_OF_DAYS}),0)),112)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,PremiumType ORDER BY PremiumTransactionID DESC) = 1
),
EXP_SetEarnedPremiumValues AS (
	SELECT
	LKP_GetFirstAudits.PremiumTransactionID AS PremiumMasterCalculationID,
	-- *INF*: IIF(ISNULL(PremiumMasterCalculationID),'1','2')
	IFF(PremiumMasterCalculationID IS NULL,
		'1',
		'2'
	) AS Decision_Flag,
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
	IFF(Decision_Flag = '1',
		EarnedPremium,
		EarnedPremium + UnearnedPremium
	) AS v_EarnedPremium,
	v_EarnedPremium AS O_EarnedPremium,
	EXP_Src_Data_Collect.ChangeInEarnedPremium,
	-- *INF*: IIF(Decision_Flag='1',0.0,v_EarnedPremium-EarnedPremium)
	IFF(Decision_Flag = '1',
		0.0,
		v_EarnedPremium - EarnedPremium
	) AS v_ChangeInEarnedPremium,
	v_ChangeInEarnedPremium AS O_ChangeInEarnedPremium,
	EXP_Src_Data_Collect.UnearnedPremium,
	-- *INF*: IIF(Decision_Flag='1',UnearnedPremium,0.0)
	IFF(Decision_Flag = '1',
		UnearnedPremium,
		0.0
	) AS v_UnearnedPremium,
	v_UnearnedPremium AS O_UnearnedPremium,
	EXP_Src_Data_Collect.ChangeInUnearnedPremium,
	-- *INF*: IIF(Decision_Flag='1',0.0,v_UnearnedPremium-UnearnedPremium)
	IFF(Decision_Flag = '1',
		0.0,
		v_UnearnedPremium - UnearnedPremium
	) AS v_ChangeInUnearnedPremium,
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
	IFF(LKP_Exposure IS NULL,
		0.0,
		LKP_Exposure
	) AS v_Exposure,
	EXP_Src_Data_Collect.EarnedExposure,
	-- *INF*: IIF(Decision_Flag='1',EarnedExposure,v_Exposure)
	IFF(Decision_Flag = '1',
		EarnedExposure,
		v_Exposure
	) AS v_EarnedExposure,
	v_EarnedExposure AS O_EarnedExposure,
	EXP_Src_Data_Collect.ChangeInEarnedExposure,
	-- *INF*: IIF(Decision_Flag='1',0.0,v_Exposure-EarnedExposure)
	IFF(Decision_Flag = '1',
		0.0,
		v_Exposure - EarnedExposure
	) AS v_ChangeInEarnedExposure,
	v_ChangeInEarnedExposure AS O_ChangeInEarnedExposure,
	EXP_Src_Data_Collect.Exposure
	FROM EXP_Src_Data_Collect
	LEFT JOIN LKP_GetExposure
	ON LKP_GetExposure.Premiumtransactionakid = EXP_Src_Data_Collect.PremiumTransactionAKID
	LEFT JOIN LKP_GetFirstAudits
	ON LKP_GetFirstAudits.PolicyAKID = EXP_Src_Data_Collect.PolicyAKID AND LKP_GetFirstAudits.PremiumType = EXP_Src_Data_Collect.PremiumType
),
EarnedPremiumDailyCalculation1 AS (
	INSERT INTO EarnedPremiumDailyCalculation
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, PolicyKey, AgencyAKID, ContractCustomerAKID, PolicyAKID, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, ReinsuranceCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, PolicyEffectiveDate, PolicyExpirationDate, StatisticalCoverageEffectiveDate, StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate, PremiumTransactionBookedDate, PremiumTransactionCode, PremiumTransactionAmount, FullTermPremium, PremiumType, ReasonAmendedCode, EarnedPremium, ChangeInEarnedPremium, UnearnedPremium, ChangeInUnearnedPremium, ProductCode, AnnualStatementLineCode, SubAnnualStatementLineCode, NonSubAnnualStatementLineCode, AnnualStatementLineProductCode, LineOfBusinessCode, PolicyOfferingCode, RunDate, RatingCoverageAKId, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, EarnedExposure, ChangeInEarnedExposure, Exposure)
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
	PREMIUMTRANSACTIONAMOUNT, 
	FULLTERMPREMIUM, 
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