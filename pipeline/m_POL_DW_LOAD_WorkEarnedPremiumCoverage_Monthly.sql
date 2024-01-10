WITH
LKP_WorkEarnedPremiumCoverage AS (
	SELECT
	WorkEarnedPremiumCoverageMonthlyID,
	PolicyAKID,
	StatisticalCoverageAKID,
	RatingCoverageAKID,
	RunDate
	FROM (
		SELECT A.WorkEarnedPremiumCoverageMonthlyID as WorkEarnedPremiumCoverageMonthlyID, 
		A.PolicyAKID as PolicyAKID, 
		A.StatisticalCoverageAKID as StatisticalCoverageAKID,
		A.RatingCoverageAKID as RatingCoverageAKID, 
		A.RunDate as RunDate
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkEarnedPremiumCoverageMonthly A
		WHERE A.RUNDATE>= DATEADD(mm,-@{pipeline().parameters.NO_OF_MONTHS},GETDATE())
		AND A.RUNDATE< DATEADD(mm,-@{pipeline().parameters.NO_OF_MONTHS}+1,GETDATE())
		@{pipeline().parameters.LOOKUP_WORK_CLAUSE}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,StatisticalCoverageAKID,RatingCoverageAKID,RunDate ORDER BY WorkEarnedPremiumCoverageMonthlyID) = 1
),
LKP_ClassCode_9115 AS (
	SELECT
	ClassCode,
	PolicyAkid,
	RatingCoverageAKid,
	StatisticalCoverageAKID
	FROM (
		select SC.ClassCode as ClassCode,PC.PolicyAkid as PolicyAkid,sc.StatisticalCoverageAKID as StatisticalCoverageAKID,-1 as RatingCoverageAKid
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
		where PC.InsuranceLine='WC'
		UNION
		select RC.ClassCode as ClassCode,PC.PolicyAkid as PolicyAkid,-1 as StatisticalCoverageAKID,RC.RatingCoverageAKid as RatingCoverageAKid
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		where PC.InsuranceLine='WorkersCompensation'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAkid,StatisticalCoverageAKID,RatingCoverageAKid ORDER BY ClassCode DESC) = 1
),
SQ_PremiumMasterCalculation_PMS AS (
	DECLARE @Date1 AS DATETIME,
	        @Date2 AS DATETIME,
	        @Date3 AS INT,
	        @Date4 AS INT
	        
	set @Date1=DATEADD(MM, -@{pipeline().parameters.NO_OF_MONTHS}, GETDATE())
	set @Date2=DATEADD(SS,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-(@{pipeline().parameters.NO_OF_MONTHS}-1),0))
	set @Date3=DATEPART(YEAR,DATEADD(MM, -@{pipeline().parameters.NO_OF_MONTHS}, GETDATE()))
	set @Date4=DATEPART(YEAR,DATEADD(MM, -@{pipeline().parameters.NO_OF_MONTHS}+12, GETDATE()))
	
	SELECT 
	@Date1 AS eff_from_date,
	A.PolicyKey, 
	A.AgencyAKID, 
	A.ContractCustomerAKID, 
	A.PolicyAKID,
	A.RiskLocationAKID, 
	A.PolicyCoverageAKID, 
	A.StatisticalCoverageAKID, 
	A.ReinsuranceCoverageAKID, 
	A.PremiumTransactionAKID, 
	A.PremiumMasterTransactionCode, 
	A.PremiumMasterRunDate, 
	A.PremiumTransactionEnteredDate,
	A.PremiumMasterCoverageEffectiveDate, 
	A.PremiumMasterCoverageExpirationDate,
	case when B.ChangedCoverageExpirationDate is null then A.PremiumMasterCoverageExpirationDate else B.ChangedCoverageExpirationDate end ChangedCoverageExpirationDate, 
	A.PremiumMasterPremiumType, 
	A.PremiumMasterTypeBureauCode, 
	A.PremiumMasterAgencyCommissionRate, 
	A.PremiumMasterReasonAmendedCode, 
	A.PremiumMasterPremium, 
	A.PremiumMasterFullTermPremium,
	A.RatingCoverageAKId,
	A.RatingCoverageEffectiveDate,
	A.RatingCoverageExpirationDate  
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation A with(nolock) left outer join 
	 (select AgencyAKID,ContractCustomerAKID,PolicyAKID,RiskLocationAKID,PolicyCoverageAKID,StatisticalCoverageAKID,MAX(PremiumMasterCoverageExpirationDate) ChangedCoverageExpirationDate,PremiumMasterPremiumType
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation with(nolock)
	where PremiumMasterRunDate<=@Date2
	@{pipeline().parameters.REASON_AMENDED_CODE}
	group by AgencyAKID,ContractCustomerAKID,PolicyAKID,RiskLocationAKID,PolicyCoverageAKID,StatisticalCoverageAKID,PremiumMasterPremiumType
	having SUM(premiummasterfulltermpremium)=0) B
	on A.AgencyAKID=B.AgencyAKID
	and A.ContractCustomerAKID=B.ContractCustomerAKID
	and A.PolicyAKID=B.PolicyAKID
	and A.RiskLocationAKID=B.RiskLocationAKID
	and A.PolicyCoverageAKID=B.PolicyCoverageAKID
	and A.StatisticalCoverageAKID=B.StatisticalCoverageAKID
	and A.PremiumMasterPremiumType=B.PremiumMasterPremiumType
	WHERE  A.PremiumMasterFullTermPremium <> 0.0
	AND premiummasterrundate<=@Date2
	and convert(varchar(6),case when B.ChangedCoverageExpirationDate is null then A.PremiumMasterCoverageExpirationDate else B.ChangedCoverageExpirationDate end,112)>=convert(varchar(6),@Date2,112)
	AND  A.CurrentSnapshotFlag=1 
	AND A.SourceSystemID='PMS'
	@{pipeline().parameters.REASON_AMENDED_CODE}
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
	ORDER BY A.PolicyAKID,A.RiskLocationAKID,A.PolicyCoverageAKID,A.StatisticalCoverageAKID,
	A.PremiumMasterCoverageEffectiveDate,A.PremiumMasterRunDate  desc
),
EXP_DirectTransactions AS (
	SELECT
	eff_from_date,
	AgencyAKID AS agency_ak_id,
	PolicyAKID AS pol_ak_id,
	ContractCustomerAKID AS contract_cust_ak_id,
	PolicyKey AS pol_key,
	RiskLocationAKID,
	PolicyCoverageAKID,
	PremiumMasterTypeBureauCode AS TypeBureauCode,
	StatisticalCoverageAKID,
	PremiumMasterCoverageEffectiveDate AS StatisticalCoverageEffectiveDate,
	PremiumMasterCoverageExpirationDate AS StatisticalCoverageExpirationDate,
	ChangedCoverageExpirationDate,
	PremiumMasterAgencyCommissionRate AS AgencyActualCommissionRate,
	PremiumTransactionAKID,
	ReinsuranceCoverageAKID,
	PremiumMasterTransactionCode AS PremiumTransactionCode,
	PremiumTransactionEnteredDate,
	PremiumMasterCoverageEffectiveDate AS PremiumTransactionEffectiveDate,
	PremiumMasterCoverageExpirationDate AS PremiumTransactionExpirationDate,
	PremiumMasterRunDate AS PremiumTransactionBookedDate,
	PremiumMasterPremium AS PremiumTransactionAmount,
	PremiumMasterFullTermPremium AS FullTermPremium,
	PremiumMasterPremiumType AS PremiumType,
	PremiumMasterReasonAmendedCode AS ReasonAmendedCode,
	-- *INF*: LAST_DAY(Add_To_Date(eff_from_date, 'MS', -Get_Date_Part(eff_from_date, 'MS')))
	-- 
	-- --LAST_DAY(eff_from_date)
	LAST_DAY(Add_To_Date(eff_from_date, 'MS', - Get_Date_Part(eff_from_date, 'MS'))) AS V_Last_Day_of_Last_Month,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART( V_Last_Day_of_Last_Month, 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(V_Last_Day_of_Last_Month, 'HH', 23), 'MI', 59), 'SS', 59) AS V_RunDate,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	V_RunDate AS RunDate,
	-- *INF*: LAST_DAY(ADD_TO_DATE(V_RunDate,'MM',-1))
	LAST_DAY(ADD_TO_DATE(V_RunDate, 'MM', - 1)) AS v_PreviousMonthsRunDate,
	v_PreviousMonthsRunDate AS PreviousMonthsRunDate,
	-- *INF*: SET_DATE_PART(
	--                         SET_DATE_PART(
	--                                       SET_DATE_PART(
	--                                                 SET_DATE_PART( V_Last_Day_of_Last_Month, 'DD', 1 )
	--                                            ,'HH',0),
	--                           'MI',0),
	-- 'SS',0)
	-- 
	-- ---- Changing the RunDate to FirstDay of the Run Month 
	SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(V_Last_Day_of_Last_Month, 'DD', 1), 'HH', 0), 'MI', 0), 'SS', 0) AS v_FirstDayOfRunMonth,
	v_FirstDayOfRunMonth AS FirstDayOfRunMonth,
	-- *INF*: DATE_DIFF(V_RunDate,IIF(in(PremiumTransactionCode,'14','24'),TO_DATE('1800/01/01','YYYY/MM/DD'),PremiumTransactionBookedDate),'MM')
	DATE_DIFF(V_RunDate, IFF(in(PremiumTransactionCode, '14', '24'), TO_DATE('1800/01/01', 'YYYY/MM/DD'), PremiumTransactionBookedDate), 'MM') AS LatestRecordMonth,
	-- *INF*: IIF(PremiumTransactionCode='29',1,0)
	IFF(PremiumTransactionCode = '29', 1, 0) AS CancellationSubjectedToAuditFlag
	FROM SQ_PremiumMasterCalculation_PMS
),
FIL_SourceRows AS (
	SELECT
	agency_ak_id, 
	pol_ak_id, 
	contract_cust_ak_id, 
	pol_key, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	TypeBureauCode, 
	StatisticalCoverageAKID, 
	ChangedCoverageExpirationDate, 
	StatisticalCoverageEffectiveDate, 
	StatisticalCoverageExpirationDate, 
	PremiumTransactionCode, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionBookedDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	ReasonAmendedCode, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RunDate, 
	FirstDayOfRunMonth, 
	LatestRecordMonth, 
	CancellationSubjectedToAuditFlag
	FROM EXP_DirectTransactions
	WHERE IIF(PremiumTransactionEnteredDate <= RunDate AND PremiumTransactionBookedDate <=RunDate
AND PremiumTransactionEffectiveDate <= RunDate AND ChangedCoverageExpirationDate >= FirstDayOfRunMonth ,TRUE,FALSE)
),
SRT_OrderBy_LatestCancellation AS (
	SELECT
	agency_ak_id, 
	pol_ak_id, 
	contract_cust_ak_id, 
	pol_key, 
	RiskLocationAKID, 
	LocationUnitNumber, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	SubLocationUnitNumber, 
	RiskUnitGroup, 
	RiskUnitGroupSequenceNumber, 
	RiskUnit, 
	RiskUnitSequenceNumber, 
	MajorPerilCode, 
	MajorPerilSequenceNumber, 
	StatisticalCoverageEffectiveDate, 
	StatisticalCoverageExpirationDate, 
	PremiumTransactionCode, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionBookedDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RunDate, 
	LatestRecordMonth, 
	CancellationSubjectedToAuditFlag
	FROM FIL_SourceRows
	ORDER BY agency_ak_id ASC, pol_ak_id ASC, contract_cust_ak_id ASC, RiskLocationAKID ASC, PolicyCoverageAKID ASC, StatisticalCoverageAKID ASC, PremiumTransactionEnteredDate ASC, PremiumType ASC, LatestRecordMonth DESC, CancellationSubjectedToAuditFlag ASC
),
AGG_CoverageCancellationDate AS (
	SELECT
	agency_ak_id,
	pol_ak_id,
	contract_cust_ak_id,
	pol_key,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	LocationUnitNumber,
	SubLocationUnitNumber,
	RiskUnitGroup,
	RiskUnitGroupSequenceNumber,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	MajorPerilSequenceNumber,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	PremiumTransactionCode,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	-- *INF*: SUM(PremiumTransactionAmount)
	SUM(PremiumTransactionAmount) AS TotalPremiumTransactionAmount,
	-- *INF*: SUM(FullTermPremium)
	SUM(FullTermPremium) AS TotalFullTermPremium,
	-- *INF*: MAX(PremiumTransactionAmount)
	MAX(PremiumTransactionAmount) AS Max_Premium,
	-- *INF*: MIN(ABS(PremiumTransactionAmount))
	MIN(ABS(PremiumTransactionAmount)) AS Min_Premium,
	-- *INF*: MAX(PremiumTransactionEffectiveDate)
	MAX(PremiumTransactionEffectiveDate) AS StatisticalCoverageCancellationDate,
	RunDate,
	LatestRecordMonth,
	-- *INF*: min(LatestRecordMonth)
	min(LatestRecordMonth) AS CurrentMonthFlag,
	CancellationSubjectedToAuditFlag
	FROM SRT_OrderBy_LatestCancellation
	GROUP BY agency_ak_id, pol_ak_id, contract_cust_ak_id, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumType
),
EXP_Values AS (
	SELECT
	agency_ak_id,
	pol_ak_id,
	contract_cust_ak_id,
	pol_key,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	TotalPremiumTransactionAmount,
	TotalFullTermPremium,
	StatisticalCoverageCancellationDate,
	-- *INF*: IIF(TotalFullTermPremium = 0.0 , StatisticalCoverageCancellationDate, TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'))
	IFF(TotalFullTermPremium = 0.0, StatisticalCoverageCancellationDate, TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')) AS v_StatisticalCoverageCancellationDate,
	-- *INF*: :LKP.LKP_WORKEARNEDPREMIUMCOVERAGE(pol_ak_id,StatisticalCoverageAKID,RatingCoverageAKId,RunDate)
	LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.WorkEarnedPremiumCoverageMonthlyID AS v_WorkEarnedPremiumCoverageMonthlyID,
	v_StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate_Out,
	-- *INF*: IIF(v_StatisticalCoverageCancellationDate = TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') OR NOT ISNULL(v_WorkEarnedPremiumCoverageMonthlyID),'FILTER','NOFILTER')
	IFF(v_StatisticalCoverageCancellationDate = TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') OR NOT v_WorkEarnedPremiumCoverageMonthlyID IS NULL, 'FILTER', 'NOFILTER') AS Flag,
	RunDate,
	'1' AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	Max_Premium,
	Min_Premium,
	-- *INF*: :LKP.LKP_CLASSCODE_9115(pol_ak_id,StatisticalCoverageAKID,-1)
	LKP_CLASSCODE_9115_pol_ak_id_StatisticalCoverageAKID_1.ClassCode AS LKP_ClassCode,
	-- *INF*: IIF(isnull(LKP_ClassCode),1.00,iif(LKP_ClassCode='9115',1.00,IIF(LatestRecordMonth=CurrentMonthFlag and CancellationSubjectedToAuditFlag='1',0.0,1.00)))
	-- 
	-- 
	-- --IIF(ISNULL(:LKP.LKP_POOL_POLICIES(pol_ak_id)) or (not isnull(:LKP.LKP_CLASSCODE_9115(StatisticalCoverageAKID))),1.00,Min_Premium)
	IFF(LKP_ClassCode IS NULL, 1.00, IFF(LKP_ClassCode = '9115', 1.00, IFF(LatestRecordMonth = CurrentMonthFlag AND CancellationSubjectedToAuditFlag = '1', 0.0, 1.00))) AS O_Min_Premium,
	-- *INF*: DATE_DIFF(
	-- v_StatisticalCoverageCancellationDate,
	--                             PremiumTransactionEffectiveDate,'DAY')
	-- 
	-- 
	DATE_DIFF(v_StatisticalCoverageCancellationDate, PremiumTransactionEffectiveDate, 'DAY') AS v_Numertor,
	-- *INF*: DATE_DIFF(
	-- PremiumTransactionExpirationDate,
	--                             PremiumTransactionEffectiveDate,'DAY')
	-- 
	DATE_DIFF(PremiumTransactionExpirationDate, PremiumTransactionEffectiveDate, 'DAY') AS v_Denominator,
	-- *INF*: IIF((v_Numertor  = 0 AND v_Denominator = 0)  OR v_Denominator =  0, TotalPremiumTransactionAmount,
	-- ROUND(TotalPremiumTransactionAmount * (v_Numertor/v_Denominator),4)
	-- )
	IFF(( v_Numertor = 0 AND v_Denominator = 0 ) OR v_Denominator = 0, TotalPremiumTransactionAmount, ROUND(TotalPremiumTransactionAmount * ( v_Numertor / v_Denominator ), 4)) AS v_Earned_Premium,
	v_Earned_Premium AS Earned_Premium,
	PremiumType,
	LatestRecordMonth,
	CurrentMonthFlag,
	PremiumTransactionCode,
	CancellationSubjectedToAuditFlag
	FROM AGG_CoverageCancellationDate
	LEFT JOIN LKP_WORKEARNEDPREMIUMCOVERAGE LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_RunDate
	ON LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.PolicyAKID = pol_ak_id
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.StatisticalCoverageAKID = StatisticalCoverageAKID
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.RatingCoverageAKID = RatingCoverageAKId
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.RunDate = RunDate

	LEFT JOIN LKP_CLASSCODE_9115 LKP_CLASSCODE_9115_pol_ak_id_StatisticalCoverageAKID_1
	ON LKP_CLASSCODE_9115_pol_ak_id_StatisticalCoverageAKID_1.PolicyAkid = pol_ak_id
	AND LKP_CLASSCODE_9115_pol_ak_id_StatisticalCoverageAKID_1.StatisticalCoverageAKID = StatisticalCoverageAKID
	AND LKP_CLASSCODE_9115_pol_ak_id_StatisticalCoverageAKID_1.RatingCoverageAKid = - 1

),
FIL_Active AS (
	SELECT
	agency_ak_id, 
	pol_ak_id, 
	contract_cust_ak_id, 
	pol_key, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	StatisticalCoverageEffectiveDate, 
	StatisticalCoverageExpirationDate, 
	TotalPremiumTransactionAmount, 
	TotalFullTermPremium, 
	StatisticalCoverageCancellationDate_Out AS StatisticalCoverageCancellationDate, 
	Flag, 
	RunDate, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	SourceSystemID, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	Max_Premium, 
	O_Min_Premium AS Min_Premium, 
	Earned_Premium, 
	PremiumType
	FROM EXP_Values
	WHERE Flag='NOFILTER'
),
WorkEarnedPremiumCoverageMonthly_PMS AS (
	INSERT INTO WorkEarnedPremiumCoverageMonthly
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, PolicyKey, AgencyAKID, ContractCustomerAKID, PolicyAKID, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, StatisticalCoverageEffectiveDate, StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate, StatisticalCoveragePremium, StatisticalCoverageFullTermPremium, RunDate, RatingCoverageAKId, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, MinimumPremium, PremiumType)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	CreatedDate AS MODIFIEDDATE, 
	pol_key AS POLICYKEY, 
	agency_ak_id AS AGENCYAKID, 
	contract_cust_ak_id AS CONTRACTCUSTOMERAKID, 
	pol_ak_id AS POLICYAKID, 
	RISKLOCATIONAKID, 
	POLICYCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	STATISTICALCOVERAGEEFFECTIVEDATE, 
	STATISTICALCOVERAGEEXPIRATIONDATE, 
	STATISTICALCOVERAGECANCELLATIONDATE, 
	TotalPremiumTransactionAmount AS STATISTICALCOVERAGEPREMIUM, 
	TotalFullTermPremium AS STATISTICALCOVERAGEFULLTERMPREMIUM, 
	RUNDATE, 
	RATINGCOVERAGEAKID, 
	RATINGCOVERAGEEFFECTIVEDATE, 
	RATINGCOVERAGEEXPIRATIONDATE, 
	Min_Premium AS MINIMUMPREMIUM, 
	PREMIUMTYPE
	FROM FIL_Active
),
SQ_PremiumMasterCalculation_DCT AS (
	DECLARE @FirstDayOfPrevMonth AS DATETIME,
	        @LastDayOfPrevMonth AS DATETIME
	        
	set @FirstDayOfPrevMonth=DATEADD(mm, DATEDIFF(m,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)
	set @LastDayOfPrevMonth=DATEADD(SS,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-(@{pipeline().parameters.NO_OF_MONTHS}-1),0)) 
	
	
	SELECT 
	@LastDayOfPrevMonth AS RunDate,
	A.PolicyKey, 
	A.AgencyAKID, 
	A.ContractCustomerAKID, 
	A.PolicyAKID,
	A.RiskLocationAKID, 
	A.PolicyCoverageAKID, 
	A.StatisticalCoverageAKID, 
	A.PremiumMasterCoverageEffectiveDate, 
	A.PremiumMasterCoverageExpirationDate,
	A.PremiumMasterPremiumType, 
	A.PremiumMasterPremium, 
	A.PremiumMasterFullTermPremium,
	A.RatingCoverageAKId,
	A.RatingCoverageEffectiveDate,
	A.RatingCoverageExpirationDate,
	C.RatingCoverageCancellationDate
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation A with(nolock)
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage C with (nolock)
	on C.RatingCoverageAKId=A.RatingCoverageAKId
	and A.PremiumTransactionEnteredDate between C.EffectiveDate and C.ExpirationDate
	where A.CurrentSnapshotFlag=1 AND A.SourceSystemID='DCT'
	and A.Premiummasterrundate <= @LastDayOfPrevMonth 
	AND A.PremiumMasterCoverageEffectiveDate <= @LastDayOfPrevMonth AND A.PremiumMasterCoverageExpirationDate >= @FirstDayOfPrevMonth
	AND A.PremiumMasterReasonAmendedCode not in ('CWO','CWB')
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
	ORDER BY A.RatingCoverageAKId,A.PremiumMasterCoverageEffectiveDate,A.Premiummasterrundate,A.PremiumTransactionEnteredDate
),
AGG_CoverageCancellationDate_DCT AS (
	SELECT
	RunDate,
	PolicyKey,
	AgencyAKID,
	ContractCustomerAKID,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumMasterCoverageEffectiveDate,
	PremiumMasterCoverageExpirationDate,
	PremiumMasterPremiumType,
	PremiumMasterPremium,
	PremiumMasterFullTermPremium,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageCancellationDate,
	-- *INF*: Min(abs(PremiumMasterPremium))
	Min(abs(PremiumMasterPremium)) AS MinimumPremium
	FROM SQ_PremiumMasterCalculation_DCT
	GROUP BY PolicyAKID, PremiumMasterPremiumType, RatingCoverageAKId
),
FIL_NonCancellations AS (
	SELECT
	RunDate, 
	PolicyKey, 
	AgencyAKID, 
	ContractCustomerAKID, 
	PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumMasterCoverageEffectiveDate, 
	PremiumMasterCoverageExpirationDate, 
	PremiumMasterPremiumType, 
	PremiumMasterPremium, 
	PremiumMasterFullTermPremium, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RatingCoverageCancellationDate, 
	MinimumPremium
	FROM AGG_CoverageCancellationDate_DCT
	WHERE RatingCoverageCancellationDate<TO_DATE('21001231','YYYYMMDD')
),
LKP_PremiumTransaction AS (
	SELECT
	PremiumTransactionExpirationDate,
	PolicyAkid,
	RatingCoverageAKId
	FROM (
		DECLARE @FirstDayOfPrevMonth AS DATETIME,
		        @LastDayOfPrevMonth AS DATETIME
		        
		set @FirstDayOfPrevMonth=DATEADD(mm, DATEDIFF(m,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0)
		set @LastDayOfPrevMonth=DATEADD(SS,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-(@{pipeline().parameters.NO_OF_MONTHS}-1),0))
		
		SELECT DISTINCT B.PremiumTransactionExpirationDate as PremiumTransactionExpirationDate,
		A.PolicyAkid as PolicyAkid,
		A.RatingCoverageAKId as RatingCoverageAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation A with (nolock)
		INNER JOIN
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction B with (nolock)
		on A.PremiumTransactionAKId=B.PremiumTransactionAKId AND
		       B.PremiumTransactionEnteredDate <=@LastDayOfPrevMonth
		AND B.PremiumTransactionBookedDate <=@LastDayOfPrevMonth
		AND B.PremiumTransactionEffectiveDate <= @LastDayOfPrevMonth
		AND B.PremiumTransactionExpirationDate >= @FirstDayOfPrevMonth
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage C with (nolock)
		on C.RatingCoverageAKId=B.RatingCoverageAKId
		and C.EffectiveDate=B.EffectiveDate
		and C.RatingCoverageCancellationDate='2100-12-31 23:59:59'
		where B.CurrentSnapshotFlag=1 AND B.SourceSystemID='DCT'
		ORDER BY A.RatingCoverageAKId,B.PremiumTransactionExpirationDate
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAkid,RatingCoverageAKId,PremiumTransactionExpirationDate ORDER BY PremiumTransactionExpirationDate) = 1
),
EXP_ValidateAfterCancellationExpires AS (
	SELECT
	FIL_NonCancellations.RunDate,
	FIL_NonCancellations.PolicyKey,
	FIL_NonCancellations.AgencyAKID,
	FIL_NonCancellations.ContractCustomerAKID,
	FIL_NonCancellations.PolicyAKID,
	FIL_NonCancellations.RiskLocationAKID,
	FIL_NonCancellations.PolicyCoverageAKID,
	FIL_NonCancellations.StatisticalCoverageAKID,
	FIL_NonCancellations.PremiumMasterCoverageEffectiveDate,
	FIL_NonCancellations.PremiumMasterCoverageExpirationDate,
	FIL_NonCancellations.PremiumMasterPremiumType,
	FIL_NonCancellations.PremiumMasterPremium,
	FIL_NonCancellations.PremiumMasterFullTermPremium,
	FIL_NonCancellations.RatingCoverageAKId,
	FIL_NonCancellations.RatingCoverageEffectiveDate,
	FIL_NonCancellations.RatingCoverageExpirationDate,
	FIL_NonCancellations.RatingCoverageCancellationDate,
	FIL_NonCancellations.MinimumPremium,
	LKP_PremiumTransaction.PremiumTransactionExpirationDate AS LKP_PremiumTransactionExpirationDate,
	LKP_PremiumTransaction.RatingCoverageAKId AS LKP_RatingCoverageAKId,
	LKP_PremiumTransaction.PolicyAkid AS LKP_PolicyAkid,
	-- *INF*: --:LKP.LKP_PREMIUMTRANSACTION(PolicyAKID,RatingCoverageAKId, PremiumMasterCoverageExpirationDate)
	'' AS v_RatingCoverageAKId,
	-- *INF*: IIF(PremiumMasterCoverageExpirationDate>RunDate, NULL, LKP_RatingCoverageAKId)
	IFF(PremiumMasterCoverageExpirationDate > RunDate, NULL, LKP_RatingCoverageAKId) AS o_RatingCoverageAKId
	FROM FIL_NonCancellations
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.PolicyAkid = FIL_NonCancellations.PolicyAKID AND LKP_PremiumTransaction.RatingCoverageAKId = FIL_NonCancellations.RatingCoverageAKId AND LKP_PremiumTransaction.PremiumTransactionExpirationDate > FIL_NonCancellations.PremiumMasterCoverageEffectiveDate
),
FIL_EffectiveCoverageAfterCancellationExpires AS (
	SELECT
	RunDate, 
	PolicyKey, 
	AgencyAKID, 
	ContractCustomerAKID, 
	PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumMasterCoverageEffectiveDate, 
	PremiumMasterCoverageExpirationDate, 
	PremiumMasterPremiumType, 
	PremiumMasterPremium, 
	PremiumMasterFullTermPremium, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RatingCoverageCancellationDate, 
	MinimumPremium, 
	o_RatingCoverageAKId AS lkp_RatingCoverageAKId
	FROM EXP_ValidateAfterCancellationExpires
	WHERE ISNULL(lkp_RatingCoverageAKId)
),
EXP_Values_DCT AS (
	SELECT
	RunDate,
	PolicyKey,
	AgencyAKID,
	ContractCustomerAKID,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumMasterCoverageEffectiveDate,
	PremiumMasterCoverageExpirationDate,
	PremiumMasterPremiumType,
	PremiumMasterPremium,
	PremiumMasterFullTermPremium,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageCancellationDate,
	MinimumPremium,
	-- *INF*: :LKP.LKP_CLASSCODE_9115(PolicyAKID,-1,RatingCoverageAKId)
	LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId.ClassCode AS LKP_ClassCode,
	-- *INF*: IIF(isnull(LKP_ClassCode),1.00,MinimumPremium)
	IFF(LKP_ClassCode IS NULL, 1.00, MinimumPremium) AS O_MinimumPremium,
	-- *INF*: :LKP.LKP_WORKEARNEDPREMIUMCOVERAGE(PolicyAKID,StatisticalCoverageAKID,RatingCoverageAKId,RunDate)
	LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.WorkEarnedPremiumCoverageMonthlyID AS v_WorkEarnedPremiumCoverageMonthlyID,
	-- *INF*: IIF(NOT ISNULL(v_WorkEarnedPremiumCoverageMonthlyID),'FILTER','NOFILTER')
	IFF(NOT v_WorkEarnedPremiumCoverageMonthlyID IS NULL, 'FILTER', 'NOFILTER') AS Flag,
	'1' AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	'DCT' AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM FIL_EffectiveCoverageAfterCancellationExpires
	LEFT JOIN LKP_CLASSCODE_9115 LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId
	ON LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId.PolicyAkid = PolicyAKID
	AND LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId.StatisticalCoverageAKID = - 1
	AND LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId.RatingCoverageAKid = RatingCoverageAKId

	LEFT JOIN LKP_WORKEARNEDPREMIUMCOVERAGE LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_RunDate
	ON LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.PolicyAKID = PolicyAKID
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.StatisticalCoverageAKID = StatisticalCoverageAKID
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.RatingCoverageAKID = RatingCoverageAKId
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.RunDate = RunDate

),
FIL_Active_DCT AS (
	SELECT
	Flag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	PolicyKey, 
	AgencyAKID, 
	ContractCustomerAKID, 
	PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumMasterCoverageEffectiveDate AS StatisticalCoverageEffectiveDate, 
	PremiumMasterCoverageExpirationDate AS StatisticalCoverageExpirationDate, 
	RatingCoverageCancellationDate AS StatisticalCoverageCancellationDate, 
	PremiumMasterPremium AS StatisticalCoveragePremium, 
	PremiumMasterFullTermPremium AS StatisticalCoverageFullTermPremium, 
	RunDate, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	O_MinimumPremium AS MinimumPremium, 
	PremiumMasterPremiumType AS PremiumType
	FROM EXP_Values_DCT
	WHERE Flag='NOFILTER'
),
WorkEarnedPremiumCoverageMonthly_DCT AS (
	INSERT INTO WorkEarnedPremiumCoverageMonthly
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, PolicyKey, AgencyAKID, ContractCustomerAKID, PolicyAKID, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, StatisticalCoverageEffectiveDate, StatisticalCoverageExpirationDate, StatisticalCoverageCancellationDate, StatisticalCoveragePremium, StatisticalCoverageFullTermPremium, RunDate, RatingCoverageAKId, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, MinimumPremium, PremiumType)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYKEY, 
	AGENCYAKID, 
	CONTRACTCUSTOMERAKID, 
	POLICYAKID, 
	RISKLOCATIONAKID, 
	POLICYCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	STATISTICALCOVERAGEEFFECTIVEDATE, 
	STATISTICALCOVERAGEEXPIRATIONDATE, 
	STATISTICALCOVERAGECANCELLATIONDATE, 
	STATISTICALCOVERAGEPREMIUM, 
	STATISTICALCOVERAGEFULLTERMPREMIUM, 
	RUNDATE, 
	RATINGCOVERAGEAKID, 
	RATINGCOVERAGEEFFECTIVEDATE, 
	RATINGCOVERAGEEXPIRATIONDATE, 
	MINIMUMPREMIUM, 
	PREMIUMTYPE
	FROM FIL_Active_DCT
),