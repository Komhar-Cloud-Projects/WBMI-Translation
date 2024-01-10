WITH
LKP_WorkEarnedPremiumCoverage AS (
	SELECT
	WorkEarnedPremiumCoverageDailyID,
	PolicyAKID,
	StatisticalCoverageAKID,
	RatingCoverageAKID,
	RunDate
	FROM (
		SELECT A.WorkEarnedPremiumCoverageDailyID as WorkEarnedPremiumCoverageDailyID, 
		A.PolicyAKID as PolicyAKID, 
		A.StatisticalCoverageAKID as StatisticalCoverageAKID, 
		A.RatingCoverageAKID as RatingCoverageAKID, 
		A.RunDate as RunDate
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkEarnedPremiumCoverageDaily A
		WHERE A.RunDate>=DATEADD(DD, -@{pipeline().parameters.NO_OF_DAYS}-1, GETDATE()) and A.RunDate<DATEADD(DD, -@{pipeline().parameters.NO_OF_DAYS}+1, GETDATE())
		@{pipeline().parameters.LOOKUP_WORK_CLAUSE}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,StatisticalCoverageAKID,RatingCoverageAKID,RunDate ORDER BY WorkEarnedPremiumCoverageDailyID) = 1
),
LKP_ClassCode_9115 AS (
	SELECT
	ClassCode,
	PolicyAkid,
	RatingCoverageAKid,
	StatisticalCoverageAKID
	FROM (
		select  SC.ClassCode AS ClassCode,  PC.PolicyAkid AS PolicyAkid,  sc.StatisticalCoverageAKID AS StatisticalCoverageAKID,  -1 AS RatingCoverageAKid
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
SQ_PremiumTransaction AS (
	DECLARE @Date1 AS DATETIME,
	        @Date2 AS DATETIME,
	        @Date3 AS INT,
	        @Date4 AS INT
	        
	set @Date1=DATEADD(DD, -@{pipeline().parameters.NO_OF_DAYS}, GETDATE())
	set @Date2=DATEADD(SS,-1,DATEADD(DD, DATEDIFF(D,0,GETDATE())-(@{pipeline().parameters.NO_OF_DAYS}-1),0))
	set @Date3=DATEPART(YEAR,DATEADD(DD, -@{pipeline().parameters.NO_OF_DAYS}, GETDATE()))
	set @Date4=DATEPART(YEAR,DATEADD(DD, -@{pipeline().parameters.NO_OF_DAYS}+365, GETDATE()))
	
	SELECT @Date1  AS eff_from_date,
	PT.PremiumTransactionAKID, 
	PT.ReinsuranceCoverageAKID, 
	PT.PremiumTransactionCode, 
	PT.PremiumTransactionEnteredDate, 
	PT.PremiumTransactionEffectiveDate, 
	PT.PremiumTransactionExpirationDate, 
	case when B.ChangedCoverageExpirationDate is null then PT.PremiumTransactionExpirationDate else B.ChangedCoverageExpirationDate end ChangedCoverageExpirationDate, 
	PT.PremiumTransactionBookedDate, 
	PT.PremiumTransactionAmount, 
	PT.FullTermPremium, 
	PT.PremiumType, 
	PT.ReasonAmendedCode, 
	PT.RatingCoverageAKId, 
	P.pol_ak_id, 
	P.contract_cust_ak_id,
	P.agency_ak_id, 
	P.pol_key, 
	RL.RiskLocationAKID, 
	PC.PolicyCoverageAKID, 
	PC.TypeBureauCode,
	SC.StatisticalCoverageAKID
	FROM
	(@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT WITH(nolock)
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC WITH(nolock) 
	       ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC WITH(nolock) 
	       ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL WITH(nolock) 
	       ON PC.RiskLocationAKID = RL.RiskLocationAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.V2.Policy P WITH(nolock)  
	       ON RL.PolicyAKID = P.Pol_AK_ID)
	left outer join 
	 (select 
	Pb.pol_ak_id AS PolAKID,
	SCb.StatisticalCoverageAKID as StatCovAKID,
	MAX(PTb.PremiumTransactionExpirationDate) ChangedCoverageExpirationDate,
	PTb.PremiumType as PremiumType
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PTb WITH(nolock)
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SCb WITH(nolock) 
	       ON PTb.StatisticalCoverageAKID = SCb.StatisticalCoverageAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PCb WITH(nolock) 
	       ON SCb.PolicyCoverageAKID = PCb.PolicyCoverageAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RLb WITH(nolock) 
	       ON PCb.RiskLocationAKID = RLb.RiskLocationAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER2}.Policy Pb WITH(nolock)  
	       ON RLb.PolicyAKID = Pb.Pol_AK_ID
	where PTb.premiumtransactionentereddate<=@Date2 and PTb.PremiumTransactionBookedDate <=@Date2
	and PTb.PremiumTransactionEffectiveDate <=@Date2
	       AND PTb.CurrentSnapshotFlag = '1' AND PTb.SourceSystemID = 'PMS'
	       AND SCb.CurrentSnapshotFlag = '1' AND SCb.SourceSystemID = 'PMS'
	       AND PCb.CurrentSnapshotFlag = '1' AND PCb.SourceSystemID = 'PMS'
	       AND RLb.CurrentSnapshotFlag = '1' AND RLb.SourceSystemID = 'PMS'
	       AND Pb.crrnt_snpsht_flag = '1' AND Pb.source_sys_id = 'PMS'
	@{pipeline().parameters.REASON_AMENDED_CODE1}
	group by Pb.pol_ak_id,SCb.StatisticalCoverageAKID,PTb.PremiumType
	having SUM(FullTermPremium)=0) B
	on P.pol_ak_id=B.PolAKID
	and SC.StatisticalCoverageAKID=B.StatCovAKID
	and PT.PremiumType=B.PremiumType
	WHERE  PT.FullTermPremium <> 0.0
	AND case when PT.PremiumTransactionEnteredDate<=PT.PremiumTransactionBookedDate then PT.PremiumTransactionBookedDate else PT.PremiumTransactionEnteredDate end<=@Date2
	and convert(varchar(6),case when B.ChangedCoverageExpirationDate is null then PT.PremiumTransactionExpirationDate else B.ChangedCoverageExpirationDate end,112)>=convert(varchar(6),@Date2,112)
	AND PT.CurrentSnapshotFlag = '1' AND PT.SourceSystemID = 'PMS'
	       AND SC.SourceSystemID = 'PMS'
	       AND PC.SourceSystemID = 'PMS'
	       AND RL.SourceSystemID = 'PMS'
	       AND P.crrnt_snpsht_flag = '1' AND P.source_sys_id = 'PMS'
	@{pipeline().parameters.REASON_AMENDED_CODE2}
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
	ORDER BY P.pol_ak_id,RL.RiskLocationAKID,PC.PolicyCoverageAKID,SC.StatisticalCoverageAKID,
	PT.PremiumTransactionEffectiveDate,PT.PremiumTransactionEnteredDate  desc
),
EXP_DirectTransactions AS (
	SELECT
	eff_from_date,
	AgencyAKID AS agency_ak_id,
	pol_ak_id,
	contract_cust_ak_id,
	pol_key,
	RiskLocationAKID,
	PolicyCoverageAKID,
	TypeBureauCode,
	StatisticalCoverageAKID1 AS StatisticalCoverageAKID,
	PremiumTransactionEffectiveDate AS StatisticalCoverageEffectiveDate,
	PremiumTransactionExpirationDate AS StatisticalCoverageExpirationDate,
	ChangedCoverageExpirationDate,
	PremiumTransactionAKID,
	ReinsuranceCoverageAKID,
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
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_DATE('21001231235959', 'YYYYMMDDHH24MISS') AS RatingCoverageEffectiveDate,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_DATE('21001231235959', 'YYYYMMDDHH24MISS') AS RatingCoverageExpirationDate,
	-- *INF*: Add_To_Date(eff_from_date, 'MS', -Get_Date_Part(eff_from_date, 'MS'))
	-- 
	-- --eff_from_date
	-- 
	-- --- This day is already set to day prior to current day in the source qualifier query.
	Add_To_Date(eff_from_date, 'MS', - Get_Date_Part(eff_from_date, 'MS')) AS V_Yesterday,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART( V_Yesterday, 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(V_Yesterday, 'HH', 23), 'MI', 59), 'SS', 59) AS V_RunDate,
	V_RunDate AS RunDate,
	-- *INF*: Trunc(V_RunDate,'DAY')
	Trunc(V_RunDate, 'DAY') AS RunDate_Date,
	-- *INF*: ADD_TO_DATE(V_RunDate,'DD',-1)
	ADD_TO_DATE(V_RunDate, 'DD', - 1) AS v_DayPriorToRunDate,
	v_DayPriorToRunDate AS DayPriorToRunDate,
	-- *INF*: Trunc(v_DayPriorToRunDate,'DAY')
	Trunc(v_DayPriorToRunDate, 'DAY') AS DayPriorToRunDate_Date,
	v_DayPriorToRunDate AS PreviousMonthsRunDate,
	-- *INF*: SET_DATE_PART(
	--                         SET_DATE_PART(
	--                                       SET_DATE_PART(
	--                                                 SET_DATE_PART( V_Yesterday, 'DD', 1 )
	--                                            ,'HH',0),
	--                           'MI',0),
	-- 'SS',0)
	-- 
	-- ---- Changing the RunDate to FirstDay of the Run Month 
	SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(V_Yesterday, 'DD', 1), 'HH', 0), 'MI', 0), 'SS', 0) AS v_FirstDayOfRunMonth,
	v_FirstDayOfRunMonth AS FirstDayOfRunMonth
	FROM SQ_PremiumTransaction
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
	StatisticalCoverageEffectiveDate, 
	StatisticalCoverageExpirationDate, 
	ChangedCoverageExpirationDate, 
	AgencyActualCommissionRate, 
	PremiumTransactionAKID, 
	ReinsuranceCoverageAKID, 
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
	DayPriorToRunDate, 
	FirstDayOfRunMonth, 
	RunDate_Date, 
	DayPriorToRunDate_Date
	FROM EXP_DirectTransactions
	WHERE IIF(PremiumTransactionEnteredDate <= RunDate AND PremiumTransactionBookedDate <=RunDate
AND PremiumTransactionEffectiveDate <= RunDate AND ChangedCoverageExpirationDate >= DayPriorToRunDate_Date,TRUE,FALSE)

--FirstDayOfRunMonth  may have to be changed to RunDate
),
RTR_RunDate_DayPriorRunDate AS (
	SELECT
	pol_ak_id,
	contract_cust_ak_id,
	agency_ak_id,
	pol_key,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	ChangedCoverageExpirationDate,
	PremiumTransactionCode,
	PremiumTransactionEffectiveDate AS StatisticalCoverageEffectiveDate,
	PremiumTransactionExpirationDate AS StatisticalCoverageExpirationDate,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageAKId,
	RunDate,
	DayPriorToRunDate,
	FirstDayOfRunMonth,
	RunDate_Date,
	DayPriorToRunDate_Date
	FROM FIL_SourceRows
),
RTR_RunDate_DayPriorRunDate_RUNDATE AS (SELECT * FROM RTR_RunDate_DayPriorRunDate WHERE IIF(PremiumTransactionEnteredDate <= RunDate AND PremiumTransactionBookedDate <=RunDate
AND PremiumTransactionEffectiveDate <= RunDate AND trunc(ChangedCoverageExpirationDate,'DAY')>= Trunc(RunDate,'DAY'),TRUE,FALSE)

--FirstDayOfRunMonth  may have to be changed to RunDate),
RTR_RunDate_DayPriorRunDate_DAYPRIORRUNDATE AS (SELECT * FROM RTR_RunDate_DayPriorRunDate WHERE IIF(PremiumTransactionEnteredDate <= DayPriorToRunDate AND PremiumTransactionBookedDate <=DayPriorToRunDate
AND PremiumTransactionEffectiveDate <= DayPriorToRunDate AND trunc(ChangedCoverageExpirationDate,'DAY')>=trunc( DayPriorToRunDate,'DAY') ,TRUE,FALSE)

--FirstDayOfRunMonth  may have to be changed to DayPriorToRunDate),
EXP_GetOrderRunDate AS (
	SELECT
	pol_ak_id,
	contract_cust_ak_id,
	agency_ak_id,
	pol_key,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionCode,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageAKId,
	RunDate,
	-- *INF*: DATE_DIFF(RunDate,IIF(in(PremiumTransactionCode,'14','24'),TO_DATE('1800/01/01','YYYY/MM/DD'),greatest(PremiumTransactionEnteredDate,PremiumTransactionBookedDate)),'DD')
	DATE_DIFF(RunDate, IFF(in(PremiumTransactionCode, '14', '24'), TO_DATE('1800/01/01', 'YYYY/MM/DD'), greatest(PremiumTransactionEnteredDate, PremiumTransactionBookedDate)), 'DD') AS LatestRecordDate,
	-- *INF*: IIF(PremiumTransactionCode='29',1,0)
	IFF(PremiumTransactionCode = '29', 1, 0) AS CancellationSubjectedToAuditFlag
	FROM RTR_RunDate_DayPriorRunDate_RUNDATE
),
SRT_GetOrderRunDate AS (
	SELECT
	pol_ak_id, 
	contract_cust_ak_id, 
	agency_ak_id, 
	pol_key, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumTransactionCode, 
	StatisticalCoverageEffectiveDate, 
	StatisticalCoverageExpirationDate, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionBookedDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RatingCoverageAKId, 
	RunDate, 
	LatestRecordDate, 
	CancellationSubjectedToAuditFlag
	FROM EXP_GetOrderRunDate
	ORDER BY pol_ak_id ASC, contract_cust_ak_id ASC, agency_ak_id ASC, RiskLocationAKID ASC, PolicyCoverageAKID ASC, StatisticalCoverageAKID ASC, PremiumType ASC, LatestRecordDate DESC, CancellationSubjectedToAuditFlag ASC
),
AGG_CoverageCancellationDate_RunDate AS (
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
	LatestRecordDate,
	-- *INF*: Min(LatestRecordDate)
	Min(LatestRecordDate) AS CurrentDateFalg,
	CancellationSubjectedToAuditFlag
	FROM SRT_GetOrderRunDate
	GROUP BY agency_ak_id, pol_ak_id, contract_cust_ak_id, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumType
),
EXP_Values_RunDate AS (
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
	LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.WorkEarnedPremiumCoverageDailyID AS v_WorkEarnedPremiumCoverageDailyID,
	v_StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate_Out,
	-- *INF*: IIF(v_StatisticalCoverageCancellationDate = TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') OR NOT ISNULL(v_WorkEarnedPremiumCoverageDailyID),'FILTER','NOFILTER')
	IFF(v_StatisticalCoverageCancellationDate = TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') OR NOT v_WorkEarnedPremiumCoverageDailyID IS NULL, 'FILTER', 'NOFILTER') AS Flag,
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
	-- *INF*: IIF(isnull(LKP_ClassCode),1.00,iif(LKP_ClassCode='9115',1.00,IIF(LatestRecordDate=CurrentDateFalg and CancellationSubjectedToAuditFlag='1',0.0,1.00)))
	-- 
	-- 
	-- 
	-- --IIF(isnull(LKP_ClassCode),1.00,iif(LKP_ClassCode='9115',1.00,Min_Premium))   
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(:LKP.LKP_POOL_POLICIES(pol_ak_id)) or (not isnull(:LKP.LKP_CLASSCODE_9115(StatisticalCoverageAKID))),1.00,Min_Premium)
	IFF(LKP_ClassCode IS NULL, 1.00, IFF(LKP_ClassCode = '9115', 1.00, IFF(LatestRecordDate = CurrentDateFalg AND CancellationSubjectedToAuditFlag = '1', 0.0, 1.00))) AS O_Min_Premium,
	-- *INF*: DATE_DIFF(
	-- v_StatisticalCoverageCancellationDate,
	--                             PremiumTransactionEffectiveDate,'DAY')
	-- 
	-- 
	DATE_DIFF(v_StatisticalCoverageCancellationDate, PremiumTransactionEffectiveDate, 'DAY') AS v_Numerator,
	-- *INF*: DATE_DIFF(
	-- PremiumTransactionExpirationDate,
	--                             PremiumTransactionEffectiveDate,'DAY')
	-- 
	DATE_DIFF(PremiumTransactionExpirationDate, PremiumTransactionEffectiveDate, 'DAY') AS v_Denominator,
	-- *INF*: IIF((v_Numerator  = 0 AND v_Denominator = 0)  OR v_Denominator =  0, TotalPremiumTransactionAmount,
	-- ROUND(TotalPremiumTransactionAmount * (v_Numerator/v_Denominator),4)
	-- )
	IFF(( v_Numerator = 0 AND v_Denominator = 0 ) OR v_Denominator = 0, TotalPremiumTransactionAmount, ROUND(TotalPremiumTransactionAmount * ( v_Numerator / v_Denominator ), 4)) AS v_Earned_Premium,
	v_Earned_Premium AS Earned_Premium,
	PremiumType,
	LatestRecordDate,
	CurrentDateFalg,
	CancellationSubjectedToAuditFlag
	FROM AGG_CoverageCancellationDate_RunDate
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
FIL_Active_RunDate AS (
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
	O_Min_Premium AS Min_Premium, 
	PremiumType
	FROM EXP_Values_RunDate
	WHERE Flag='NOFILTER'
),
WorkEarnedPremiumCoverageDaily_RunDate AS (
	INSERT INTO WorkEarnedPremiumCoverageDaily
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
	FROM FIL_Active_RunDate
),
EXP_GetOrderDayPriorDate AS (
	SELECT
	pol_ak_id,
	contract_cust_ak_id,
	agency_ak_id,
	pol_key,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionCode,
	StatisticalCoverageEffectiveDate,
	StatisticalCoverageExpirationDate,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageAKId,
	DayPriorToRunDate,
	-- *INF*: DATE_DIFF(DayPriorToRunDate,IIF(in(PremiumTransactionCode,'14','24'),TO_DATE('1800/01/01','YYYY/MM/DD'),greatest(PremiumTransactionEnteredDate,PremiumTransactionBookedDate)),'DD')
	DATE_DIFF(DayPriorToRunDate, IFF(in(PremiumTransactionCode, '14', '24'), TO_DATE('1800/01/01', 'YYYY/MM/DD'), greatest(PremiumTransactionEnteredDate, PremiumTransactionBookedDate)), 'DD') AS LatestRecordDate,
	-- *INF*: IIF(PremiumTransactionCode='29',1,0)
	IFF(PremiumTransactionCode = '29', 1, 0) AS CancellationSubjectedToAuditFlag
	FROM RTR_RunDate_DayPriorRunDate_DAYPRIORRUNDATE
),
SRT_SortOrderDayPriorDate AS (
	SELECT
	pol_ak_id, 
	contract_cust_ak_id, 
	agency_ak_id, 
	pol_key, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumTransactionCode, 
	StatisticalCoverageEffectiveDate, 
	StatisticalCoverageExpirationDate, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionBookedDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RatingCoverageAKId, 
	DayPriorToRunDate, 
	LatestRecordDate, 
	CancellationSubjectedToAuditFlag
	FROM EXP_GetOrderDayPriorDate
	ORDER BY pol_ak_id ASC, contract_cust_ak_id ASC, agency_ak_id ASC, RiskLocationAKID ASC, PolicyCoverageAKID ASC, StatisticalCoverageAKID ASC, PremiumType ASC, LatestRecordDate DESC, CancellationSubjectedToAuditFlag ASC
),
AGG_CoverageCancellationDate_DayPriorRunDate AS (
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
	DayPriorToRunDate,
	LatestRecordDate,
	-- *INF*: Min(LatestRecordDate)
	Min(LatestRecordDate) AS CurrentDateFlag,
	CancellationSubjectedToAuditFlag
	FROM SRT_SortOrderDayPriorDate
	GROUP BY agency_ak_id, pol_ak_id, contract_cust_ak_id, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumType
),
EXP_Values_DayPriorRunDate AS (
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
	-- *INF*: :LKP.LKP_WORKEARNEDPREMIUMCOVERAGE(pol_ak_id,StatisticalCoverageAKID,RatingCoverageAKId,DayPriorToRunDate)
	LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorToRunDate.WorkEarnedPremiumCoverageDailyID AS v_WorkEarnedPremiumCoverageDailyID,
	v_StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate_Out,
	-- *INF*: IIF(v_StatisticalCoverageCancellationDate = TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS') AND NOT(ISNULL(v_WorkEarnedPremiumCoverageDailyID)),'FILTER','NOFILTER')
	IFF(v_StatisticalCoverageCancellationDate = TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AND NOT ( v_WorkEarnedPremiumCoverageDailyID IS NULL ), 'FILTER', 'NOFILTER') AS Flag,
	DayPriorToRunDate,
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
	-- *INF*: IIF(isnull(LKP_ClassCode),1.00,iif(LKP_ClassCode='9115',1.00,IIF(LatestRecordDate=CurrentDateFlag and CancellationSubjectedToAuditFlag='1',0.0,1.00)))
	-- 
	-- --IIF(isnull(LKP_ClassCode),1.00,iif(LKP_ClassCode='9115',1.00,Min_Premium))   
	-- 
	-- --IIF(ISNULL(:LKP.LKP_POOL_POLICIES(pol_ak_id)) or (not isnull(:LKP.LKP_CLASSCODE_9115(StatisticalCoverageAKID))),1.00,Min_Premium)
	IFF(LKP_ClassCode IS NULL, 1.00, IFF(LKP_ClassCode = '9115', 1.00, IFF(LatestRecordDate = CurrentDateFlag AND CancellationSubjectedToAuditFlag = '1', 0.0, 1.00))) AS O_Min_Premium,
	-- *INF*: DATE_DIFF(
	-- v_StatisticalCoverageCancellationDate,
	--                             PremiumTransactionEffectiveDate,'DAY')
	-- 
	-- 
	DATE_DIFF(v_StatisticalCoverageCancellationDate, PremiumTransactionEffectiveDate, 'DAY') AS v_Numerator,
	-- *INF*: DATE_DIFF(
	-- PremiumTransactionExpirationDate,
	--                             PremiumTransactionEffectiveDate,'DAY')
	-- 
	DATE_DIFF(PremiumTransactionExpirationDate, PremiumTransactionEffectiveDate, 'DAY') AS v_Denominator,
	-- *INF*: IIF((v_Numerator  = 0 AND v_Denominator = 0)  OR v_Denominator =  0, TotalPremiumTransactionAmount,
	-- ROUND(TotalPremiumTransactionAmount * (v_Numerator/v_Denominator),4)
	-- )
	IFF(( v_Numerator = 0 AND v_Denominator = 0 ) OR v_Denominator = 0, TotalPremiumTransactionAmount, ROUND(TotalPremiumTransactionAmount * ( v_Numerator / v_Denominator ), 4)) AS v_Earned_Premium,
	v_Earned_Premium AS Earned_Premium,
	PremiumType,
	LatestRecordDate,
	CurrentDateFlag,
	CancellationSubjectedToAuditFlag
	FROM AGG_CoverageCancellationDate_DayPriorRunDate
	LEFT JOIN LKP_WORKEARNEDPREMIUMCOVERAGE LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorToRunDate
	ON LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorToRunDate.PolicyAKID = pol_ak_id
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorToRunDate.StatisticalCoverageAKID = StatisticalCoverageAKID
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorToRunDate.RatingCoverageAKID = RatingCoverageAKId
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_pol_ak_id_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorToRunDate.RunDate = DayPriorToRunDate

	LEFT JOIN LKP_CLASSCODE_9115 LKP_CLASSCODE_9115_pol_ak_id_StatisticalCoverageAKID_1
	ON LKP_CLASSCODE_9115_pol_ak_id_StatisticalCoverageAKID_1.PolicyAkid = pol_ak_id
	AND LKP_CLASSCODE_9115_pol_ak_id_StatisticalCoverageAKID_1.StatisticalCoverageAKID = StatisticalCoverageAKID
	AND LKP_CLASSCODE_9115_pol_ak_id_StatisticalCoverageAKID_1.RatingCoverageAKid = - 1

),
FIL_Active_DayPriorRunDate AS (
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
	DayPriorToRunDate, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	SourceSystemID, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	O_Min_Premium AS Min_Premium, 
	PremiumType
	FROM EXP_Values_DayPriorRunDate
	WHERE Flag='NOFILTER'
),
WorkEarnedPremiumCoverageDaily_DayPriorRunDate AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkEarnedPremiumCoverageDaily
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
	DayPriorToRunDate AS RUNDATE, 
	RATINGCOVERAGEAKID, 
	RATINGCOVERAGEEFFECTIVEDATE, 
	RATINGCOVERAGEEXPIRATIONDATE, 
	Min_Premium AS MINIMUMPREMIUM, 
	PREMIUMTYPE
	FROM FIL_Active_DayPriorRunDate
),
SQ_PremiumTransaction_DCT AS (
	DECLARE @PrevDay AS DATETIME,
	@DayBeforePrevDay AS DATETIME
	
	set @PrevDay=DATEADD(SS,-1,DATEADD(DD, DATEDIFF(D,0,GETDATE())-(@{pipeline().parameters.NO_OF_DAYS}-1),0))
	set @DayBeforePrevDay=DATEADD(D,-1,@PrevDay)
	
	SELECT @PrevDay AS RunDate,
	@DayBeforePrevDay AS DayPriorToRunDate,
	PT.PremiumTransactionEnteredDate, 
	PT.PremiumTransactionEffectiveDate, 
	PT.PremiumTransactionExpirationDate, 
	PT.PremiumTransactionBookedDate, 
	PT.PremiumTransactionAmount, 
	PT.FullTermPremium, 
	PT.PremiumType,
	PT.RatingCoverageAKId, 
	P.pol_ak_id, 
	P.contract_cust_ak_id,
	P.agency_ak_id as AgencyAKID, 
	P.pol_key, 
	RL.RiskLocationAKID, 
	PC.PolicyCoverageAKID, 
	-1 as StatisticalCoverageAKID,
	RC.RatingCoverageEffectiveDate,
	RC.RatingCoverageExpirationDate,
	RC.RatingCoverageCancellationDate
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT WITH(nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC WITH(nolock) 
	ON PT.RatingCoverageAKID = RC.RatingCoverageAKID
	AND PT.EffectiveDate=RC.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC WITH(nolock) 
	ON PC.PolicyCoverageAKId=RC.PolicyCoverageAKId
	AND PC.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL WITH(nolock) 
	ON RL.RiskLocationAKId=PC.RiskLocationAKId
	AND RL.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER2}.policy P WITH(nolock) 
	ON P.pol_ak_id=RL.PolicyAKId
	AND P.crrnt_snpsht_flag=1
	WHERE  PT.CurrentSnapshotFlag = '1' AND PT.SourceSystemID = 'DCT'
	AND PT.PremiumTransactionEnteredDate <= @PrevDay 
	AND PT.PremiumTransactionBookedDate <=@PrevDay 
	AND PT.PremiumTransactionEffectiveDate <= @PrevDay 
	AND cast(PT.PremiumTransactionExpirationDate as date) >= Cast(@DayBeforePrevDay as Date)
	AND PT.ReasonAmendedCode not in ('CWO','Claw back')
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
),
EXP_Src_DataCollect_DCT AS (
	SELECT
	RunDate,
	-- *INF*: Trunc(RunDate,'DAY')
	Trunc(RunDate, 'DAY') AS RunDate_Date,
	DayPriorToRunDate,
	-- *INF*: Trunc(DayPriorToRunDate,'DAY')
	Trunc(DayPriorToRunDate, 'DAY') AS DayPriorToRunDate_Date,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	RatingCoverageAKId,
	pol_ak_id,
	contract_cust_ak_id,
	AgencyAKID,
	pol_key,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageCancellationDate
	FROM SQ_PremiumTransaction_DCT
),
RTR_RunDate_DayPriorRunDate_DCT AS (
	SELECT
	pol_ak_id,
	contract_cust_ak_id,
	AgencyAKID AS agency_ak_id,
	pol_key,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageCancellationDate,
	RatingCoverageAKId,
	RunDate,
	DayPriorToRunDate,
	RunDate_Date,
	DayPriorToRunDate_Date
	FROM EXP_Src_DataCollect_DCT
),
RTR_RunDate_DayPriorRunDate_DCT_RUNDATE AS (SELECT * FROM RTR_RunDate_DayPriorRunDate_DCT WHERE PremiumTransactionEnteredDate <= RunDate AND PremiumTransactionBookedDate<= RunDate 
AND PremiumTransactionEffectiveDate <= RunDate AND Trunc(PremiumTransactionExpirationDate,'DAY')>= Trunc(RunDate,'DAY')),
RTR_RunDate_DayPriorRunDate_DCT_DAYPRIORRUNDATE AS (SELECT * FROM RTR_RunDate_DayPriorRunDate_DCT WHERE PremiumTransactionEnteredDate <= DayPriorToRunDate and PremiumTransactionBookedDate<= DayPriorToRunDate 
AND PremiumTransactionEffectiveDate <= DayPriorToRunDate AND trunc(PremiumTransactionExpirationDate,'DAY')>= Trunc(DayPriorToRunDate,'DAY')),
SRT_DayPriorRunDate AS (
	SELECT
	DayPriorToRunDate AS DayPriorRunDate, 
	pol_key AS PolicyKey, 
	agency_ak_id AS AgencyAKID, 
	contract_cust_ak_id AS ContractCustomerAKID, 
	pol_ak_id AS PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	RatingCoverageAKId, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RatingCoverageCancellationDate
	FROM RTR_RunDate_DayPriorRunDate_DCT_DAYPRIORRUNDATE
	ORDER BY PolicyAKID ASC, RatingCoverageAKId ASC, PremiumTransactionEffectiveDate ASC, PremiumTransactionEnteredDate ASC
),
AGG_CoverageCancellationDate_DayPriorRunDateDCT AS (
	SELECT
	DayPriorRunDate,
	PolicyKey,
	AgencyAKID,
	ContractCustomerAKID,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageCancellationDate,
	-- *INF*: Min(abs(PremiumTransactionAmount))
	Min(abs(PremiumTransactionAmount)) AS MinimumPremium
	FROM SRT_DayPriorRunDate
	GROUP BY PolicyAKID, PremiumType, RatingCoverageAKId
),
FIL_NonCancellations_DayPriorRunDate AS (
	SELECT
	DayPriorRunDate, 
	PolicyKey, 
	AgencyAKID, 
	ContractCustomerAKID, 
	PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RatingCoverageCancellationDate, 
	MinimumPremium
	FROM AGG_CoverageCancellationDate_DayPriorRunDateDCT
	WHERE RatingCoverageCancellationDate<TO_DATE('21001231','YYYYMMDD')
),
EXP_Values_DayPriorRunDateDCT AS (
	SELECT
	DayPriorRunDate,
	PolicyKey,
	AgencyAKID,
	ContractCustomerAKID,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageCancellationDate,
	MinimumPremium,
	-- *INF*: :LKP.LKP_CLASSCODE_9115(PolicyAKID,-1,RatingCoverageAKId)
	LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId.ClassCode AS LKP_ClassCode,
	-- *INF*: IIF(isnull(LKP_ClassCode),1.00,MinimumPremium)
	IFF(LKP_ClassCode IS NULL, 1.00, MinimumPremium) AS O_MinimumPremium,
	-- *INF*: :LKP.LKP_WORKEARNEDPREMIUMCOVERAGE(PolicyAKID,StatisticalCoverageAKID,RatingCoverageAKId,DayPriorRunDate)
	LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorRunDate.WorkEarnedPremiumCoverageDailyID AS v_WorkEarnedPremiumCoverageDailyID,
	-- *INF*: IIF(NOT ISNULL(v_WorkEarnedPremiumCoverageDailyID),'FILTER','NOFILTER')
	-- 
	-- 
	-- 
	-- --should use RatingCoverageCancellationDate
	IFF(NOT v_WorkEarnedPremiumCoverageDailyID IS NULL, 'FILTER', 'NOFILTER') AS Flag,
	'1' AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	'DCT' AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM FIL_NonCancellations_DayPriorRunDate
	LEFT JOIN LKP_CLASSCODE_9115 LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId
	ON LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId.PolicyAkid = PolicyAKID
	AND LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId.StatisticalCoverageAKID = - 1
	AND LKP_CLASSCODE_9115_PolicyAKID_1_RatingCoverageAKId.RatingCoverageAKid = RatingCoverageAKId

	LEFT JOIN LKP_WORKEARNEDPREMIUMCOVERAGE LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorRunDate
	ON LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorRunDate.PolicyAKID = PolicyAKID
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorRunDate.StatisticalCoverageAKID = StatisticalCoverageAKID
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorRunDate.RatingCoverageAKID = RatingCoverageAKId
	AND LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_DayPriorRunDate.RunDate = DayPriorRunDate

),
FIL_Active_DayPriorRunDateDCT AS (
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
	PremiumTransactionEffectiveDate AS StatisticalCoverageEffectiveDate, 
	PremiumTransactionExpirationDate AS StatisticalCoverageExpirationDate, 
	RatingCoverageCancellationDate AS StatisticalCoverageCancellationDate, 
	PremiumTransactionAmount AS StatisticalCoveragePremium, 
	FullTermPremium AS StatisticalCoverageFullTermPremium, 
	DayPriorRunDate AS RunDate, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	O_MinimumPremium AS MinimumPremium, 
	PremiumType
	FROM EXP_Values_DayPriorRunDateDCT
	WHERE Flag='NOFILTER'
),
WorkEarnedPremiumCoverageDaily_DayPriorRunDateDCT AS (
	INSERT INTO WorkEarnedPremiumCoverageDaily
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
	FROM FIL_Active_DayPriorRunDateDCT
),
SRT_RunDate AS (
	SELECT
	RunDate, 
	pol_key AS PolicyKey, 
	agency_ak_id AS AgencyAKID, 
	contract_cust_ak_id AS ContractCustomerAKID, 
	pol_ak_id AS PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	RatingCoverageAKId, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RatingCoverageCancellationDate
	FROM RTR_RunDate_DayPriorRunDate_DCT_RUNDATE
	ORDER BY PolicyAKID ASC, RatingCoverageAKId ASC, PremiumTransactionEffectiveDate ASC, PremiumTransactionEnteredDate ASC
),
AGG_CoverageCancellationDate_RunDateDCT AS (
	SELECT
	RunDate,
	PolicyKey,
	AgencyAKID,
	ContractCustomerAKID,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	RatingCoverageAKId,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	RatingCoverageCancellationDate,
	-- *INF*: Min(abs(PremiumTransactionAmount))
	Min(abs(PremiumTransactionAmount)) AS MinimumPremium
	FROM SRT_RunDate
	GROUP BY PolicyAKID, PremiumType, RatingCoverageAKId
),
FIL_NonCancellations_RunDate AS (
	SELECT
	RunDate, 
	PolicyKey, 
	AgencyAKID, 
	ContractCustomerAKID, 
	PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionExpirationDate, 
	PremiumTransactionAmount, 
	FullTermPremium, 
	PremiumType, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	RatingCoverageCancellationDate, 
	MinimumPremium
	FROM AGG_CoverageCancellationDate_RunDateDCT
	WHERE RatingCoverageCancellationDate<TO_DATE('21001231','YYYYMMDD')
),
EXP_Values_RunDateDCT AS (
	SELECT
	RunDate,
	PolicyKey,
	AgencyAKID,
	ContractCustomerAKID,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
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
	LKP_WORKEARNEDPREMIUMCOVERAGE_PolicyAKID_StatisticalCoverageAKID_RatingCoverageAKId_RunDate.WorkEarnedPremiumCoverageDailyID AS v_WorkEarnedPremiumCoverageDailyID,
	-- *INF*: IIF(NOT ISNULL(v_WorkEarnedPremiumCoverageDailyID),'FILTER','NOFILTER')
	-- 
	-- 
	-- 
	-- --should use RatingCoverageCancellationDate
	IFF(NOT v_WorkEarnedPremiumCoverageDailyID IS NULL, 'FILTER', 'NOFILTER') AS Flag,
	'1' AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	'DCT' AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM FIL_NonCancellations_RunDate
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
FIL_Active_RunDateDCT AS (
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
	PremiumTransactionEffectiveDate AS StatisticalCoverageEffectiveDate, 
	PremiumTransactionExpirationDate AS StatisticalCoverageExpirationDate, 
	RatingCoverageCancellationDate AS StatisticalCoverageCancellationDate, 
	PremiumTransactionAmount AS StatisticalCoveragePremium, 
	FullTermPremium AS StatisticalCoverageFullTermPremium, 
	RunDate, 
	RatingCoverageAKId, 
	RatingCoverageEffectiveDate, 
	RatingCoverageExpirationDate, 
	O_MinimumPremium AS MinimumPremium, 
	PremiumType
	FROM EXP_Values_RunDateDCT
	WHERE Flag='NOFILTER'
),
WorkEarnedPremiumCoverageDaily_RunDateDCT AS (
	INSERT INTO WorkEarnedPremiumCoverageDaily
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
	FROM FIL_Active_RunDateDCT
),