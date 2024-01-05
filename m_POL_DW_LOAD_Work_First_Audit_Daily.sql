WITH
SQ_PremiumTransaction AS (
	DECLARE @Date1 AS DATETIME
	        
	set @Date1=DATEADD(DD, -@{pipeline().parameters.NO_OF_DAYS}, GETDATE())
	
	select 
	A.eff_from_date,
	A.PolicyKey, 
	A.PolicyAKID, 
	A.RiskLocationAKID, 
	A.PolicyCoverageAKID, 
	A.StatisticalCoverageAKID, 
	A.RatingCoverageAKID,
	A.PremiumTransactionID,
	A.PremiumTransactionCode, 
	A.PremiumTransactionEffectiveDate,
	A.PremiumTransactionExpirationDate,
	A.PremiumTransactionPremium, 
	A.PremiumTransactionFullTermPremium,
	A.PremiumType
	 from (
	SELECT 
	P.pol_key as PolicyKey, 
	P.pol_ak_id as PolicyAKID, 
	RL.RiskLocationAKID as RiskLocationAKID, 
	PC.PolicyCoverageAKID as PolicyCoverageAKID , 
	SC.StatisticalCoverageAKID as StatisticalCoverageAKID, 
	-1 as RatingCoverageAKID,
	PT.PremiumTransactionID as PremiumTransactionID,
	PT.PremiumTransactionCode as PremiumTransactionCode, 
	@Date1  as eff_from_date, 
	PT.PremiumType as PremiumType, 
	PT.PremiumTransactionAmount as PremiumTransactionPremium, 
	PT.FullTermPremium as PremiumTransactionFullTermPremium,
	PT.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate,
	PT.PremiumTransactionExpirationDate as PremiumTransactionExpirationDate,
	WFAD.PolicyAKID as Work_PolicyAKID,
	PT.ReasonAmendedCode AS PremiumTransactionReasonAmendedCode
	FROM
	 (@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT WITH(nolock)
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC WITH(nolock) 
	       ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC WITH(nolock) 
	       ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL WITH(nolock) 
	       ON PC.RiskLocationAKID = RL.RiskLocationAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER2}.Policy P WITH(nolock)  
	       ON RL.PolicyAKID = P.Pol_AK_ID)
	left outer join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkFirstAuditDaily WFAD
	 on P.pol_AK_ID = WFAD.PolicyAKID 
	 AND SC.StatisticalCoverageAKID = WFAD.StatisticalCoverageAKID
	AND PT.PremiumType=WFAD.PremiumTransactionPremiumType
	 where (CONVERT(varchar(8),case when PremiumTransactionEnteredDate>=PremiumTransactionBookedDate and PremiumTransactionEnteredDate>=PremiumTransactionEffectiveDate
	then PremiumTransactionEnteredDate
	when PremiumTransactionBookedDate>=PremiumTransactionEnteredDate and PremiumTransactionBookedDate>=PremiumTransactionEffectiveDate
	then PremiumTransactionBookedDate
	else PremiumTransactionEffectiveDate end,112)=CONVERT(varchar(8),@Date1,112))
	and PT.PremiumTransactionCode in ('14','24')
	--and case when PT.PremiumTransactionAmount=0.0 then 'FALSE'
	--when PT.PremiumTransactionCode='15' then 'FALSE' else 'TRUE' end='TRUE'
	union all
	SELECT 
	P.pol_key as PolicyKey, 
	P.pol_ak_id as PolicyAKID, 
	RL.RiskLocationAKID as RiskLocationAKID, 
	PC.PolicyCoverageAKID as PolicyCoverageAKID , 
	-1 as StatisticalCoverageAKID, 
	RC.RatingCoverageAKID as RatingCoverageAKID,
	PT.PremiumTransactionID as PremiumTransactionID,
	PT.PremiumTransactionCode as PremiumTransactionCode, 
	@Date1  as eff_from_date, 
	PT.PremiumType as PremiumType, 
	PT.PremiumTransactionAmount as PremiumTransactionPremium, 
	PT.FullTermPremium as PremiumTransactionFullTermPremium,
	PT.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate,
	PT.PremiumTransactionExpirationDate as PremiumTransactionExpirationDate,
	WFAD.PolicyAKID as Work_PolicyAKID,
	PT.ReasonAmendedCode AS PremiumTransactionReasonAmendedCode
	FROM
	 (@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT WITH(nolock)
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC WITH(nolock) 
	       ON PT.RatingCoverageAKID = RC.RatingCoverageAKID 
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC WITH(nolock) 
	       ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL WITH(nolock) 
	       ON PC.RiskLocationAKID = RL.RiskLocationAKID
	       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER2}.Policy P WITH(nolock)  
	       ON RL.PolicyAKID = P.Pol_AK_ID)
	left outer join @{pipeline().parameters.SOURCE_DATABASE_NAME2}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkFirstAuditDaily WFAD
	 on P.pol_AK_ID = WFAD.PolicyAKID 
	 AND RC.RatingCoverageAKID = WFAD.RatingCoverageAKID
	AND PT.PremiumType=WFAD.PremiumTransactionPremiumType
	 where (CONVERT(varchar(8),case when PremiumTransactionEnteredDate>=PremiumTransactionBookedDate and PremiumTransactionEnteredDate>=PremiumTransactionEffectiveDate
	then PremiumTransactionEnteredDate
	when PremiumTransactionBookedDate>=PremiumTransactionEnteredDate and PremiumTransactionBookedDate>=PremiumTransactionEffectiveDate
	then PremiumTransactionBookedDate
	else PremiumTransactionEffectiveDate end,112)=CONVERT(varchar(8),@Date1,112))
	and PT.PremiumTransactionCode in ('FinalAudit','RevisedFinalAudit','VoidFinalAudit')
	--and case when PT.PremiumTransactionAmount=0.0 then 'FALSE'
	--when PT.PremiumTransactionCode='15' then 'FALSE' else 'TRUE' end='TRUE'
	) A
	where A.Work_PolicyAKID is null
	@{pipeline().parameters.REASON_AMENDED_CODE}
	@{pipeline().parameters.WHERE_CLAUSE}
),
AGG_Supress_Multiple_Audits AS (
	SELECT
	eff_from_date, 
	PremiumTransactionID, 
	pol_key AS PolicyKey, 
	pol_ak_id AS PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	RatingCoverageAKId, 
	PremiumTransactionCode, 
	PremiumType AS PremiumTransactionPremiumType, 
	PremiumTransactionAmount AS PremiumTransactionPremium, 
	FullTermPremium AS PremiumTransactionFullTermPremium, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionExpirationDate
	FROM SQ_PremiumTransaction
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID, StatisticalCoverageAKID, RatingCoverageAKId, PremiumTransactionPremiumType ORDER BY NULL) = 1
),
LKP_Get_CancellationDate AS (
	SELECT
	StatisticalCoverageCancellationDate,
	Min_Premium,
	Rundate,
	PremiumType,
	PolicyAKID,
	StatisticalCoverageAKID,
	RatingCoverageAKID
	FROM (
		SELECT WEPCD.StatisticalCoverageCancellationDate as StatisticalCoverageCancellationDate,
		 WEPCD.Min_Premium as Min_Premium,
		 WEPCD.PolicyAKID as PolicyAKID,
		 WEPCD.StatisticalCoverageAKID as StatisticalCoverageAKID,
		WEPCD.RatingCoverageAKID as RatingCoverageAKID, 
		WEPCD.RUNDATE as Rundate,
		WEPCD.PremiumType as PremiumType from (
		SELECT WEPCD.StatisticalCoverageCancellationDate as StatisticalCoverageCancellationDate,
		 WEPCD.MinimumPremium as Min_Premium,
		 WEPCD.PolicyAKID as PolicyAKID,
		 WEPCD.StatisticalCoverageAKID as StatisticalCoverageAKID,
		WEPCD.RatingCoverageAKID as RatingCoverageAKID, 
		WEPCD.RUNDATE as Rundate,
		WEPCD.PremiumType as PremiumType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkEarnedPremiumCoverageDaily WEPCD,(select PolicyAKID, StatisticalCoverageAKID,RatingCoverageAKID,max(RunDate) Max_Rundate
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkEarnedPremiumCoverageDaily A 
		group by PolicyAKID,StatisticalCoverageAKID,RatingCoverageAKID ) A
		where WEPCD.PolicyAKID=A.PolicyAKID
		and WEPCD.StatisticalCoverageAKID=A.StatisticalCoverageAKID
		and WEPCD.RatingCoverageAKID=A.RatingCoverageAKID
		and WEPCD.RunDate=A.Max_Rundate
		and CONVERT(varchar(6),WEPCD.Rundate,112)= CONVERT(varchar(6),DATEADD(DD, -@{pipeline().parameters.NO_OF_DAYS}, GETDATE()),112)
		and WEPCD.MinimumPremium=0
		@{pipeline().parameters.LOOKUP_WORK_CLAUSE}
		UNION
		SELECT WEPCM.StatisticalCoverageCancellationDate as StatisticalCoverageCancellationDate,
		 WEPCM.MinimumPremium as Min_Premium,
		 WEPCM.PolicyAKID as PolicyAKID,
		 WEPCM.StatisticalCoverageAKID as StatisticalCoverageAKID,
		 WEPCM.RatingCoverageAKId as RatingCoverageAKId,
		WEPCM.RUNDATE as Rundate,
		WEPCM.PremiumType as PremiumType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkEarnedPremiumCoverageMonthly WEPCM,(select PolicyAKID,StatisticalCoverageAKID,RatingCoverageAKId, max(RunDate) Max_Rundate
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkEarnedPremiumCoverageMonthly A 
		group by PolicyAKID,StatisticalCoverageAKID,RatingCoverageAKId ) A
		where WEPCM.PolicyAKID=A.PolicyAKID
		and WEPCM.StatisticalCoverageAKID=A.StatisticalCoverageAKID
		and WEPCM.RatingCoverageAKId=A.RatingCoverageAKId 
		and WEPCM.RunDate=A.Max_Rundate
		and CONVERT(varchar(6),WEPCM.Rundate,112)<=CONVERT(varchar(6),DATEADD(MM, -@{pipeline().parameters.NO_OF_DAYS}, GETDATE()),112)
		and WEPCM.MinimumPremium=0
		@{pipeline().parameters.LOOKUP_WORK_CLAUSE}) WEPCD
		ORDER BY WEPCD.PolicyAKID, WEPCD.StatisticalCoverageAKID, WEPCD.RatingCoverageAKID, WEPCD.Rundate
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,StatisticalCoverageAKID,RatingCoverageAKID ORDER BY StatisticalCoverageCancellationDate DESC) = 1
),
EXP_Flag_First_Audits AS (
	SELECT
	AGG_Supress_Multiple_Audits.PremiumTransactionID,
	AGG_Supress_Multiple_Audits.PolicyKey,
	AGG_Supress_Multiple_Audits.PolicyAKID,
	AGG_Supress_Multiple_Audits.RiskLocationAKID,
	AGG_Supress_Multiple_Audits.PolicyCoverageAKID,
	AGG_Supress_Multiple_Audits.StatisticalCoverageAKID,
	AGG_Supress_Multiple_Audits.RatingCoverageAKId,
	AGG_Supress_Multiple_Audits.PremiumTransactionCode,
	AGG_Supress_Multiple_Audits.PremiumTransactionPremiumType,
	AGG_Supress_Multiple_Audits.PremiumTransactionPremium,
	AGG_Supress_Multiple_Audits.PremiumTransactionFullTermPremium,
	LKP_Get_CancellationDate.StatisticalCoverageCancellationDate AS Lkp_StatisticalCoverageCancellationDate,
	-- *INF*: IIF(NOT ISNULL(Lkp_StatisticalCoverageCancellationDate),IIF((PremiumTransactionPremiumType='D' and Lkp_PremiumType='D') 
	-- OR (PremiumTransactionPremiumType='C' and Lkp_PremiumType='D') 
	-- OR (PremiumTransactionPremiumType='C' and Lkp_PremiumType='C')
	-- ,Lkp_StatisticalCoverageCancellationDate,TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')),Lkp_StatisticalCoverageCancellationDate)
	-- 
	-- --If premiumMasterPremiumType is Direct and the Cancelled Coverage PremiumType is Ceded then the cancelled date will be '12/31/2100 23:59:59'.
	IFF(NOT Lkp_StatisticalCoverageCancellationDate IS NULL, IFF(( PremiumTransactionPremiumType = 'D' AND Lkp_PremiumType = 'D' ) OR ( PremiumTransactionPremiumType = 'C' AND Lkp_PremiumType = 'D' ) OR ( PremiumTransactionPremiumType = 'C' AND Lkp_PremiumType = 'C' ), Lkp_StatisticalCoverageCancellationDate, TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')), Lkp_StatisticalCoverageCancellationDate) AS v_StatisticalCoverageCancellationDate,
	v_StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate,
	LKP_Get_CancellationDate.Min_Premium,
	LKP_Get_CancellationDate.Rundate AS Lkp_Rundate,
	LKP_Get_CancellationDate.PremiumType AS Lkp_PremiumType,
	AGG_Supress_Multiple_Audits.eff_from_date,
	-- *INF*: Add_To_Date(eff_from_date, 'MS', -Get_Date_Part(eff_from_date, 'MS'))
	-- 
	-- --eff_from_date
	-- 
	-- --- This day is already set to day prior to current day in the source qualifier query.
	Add_To_Date(eff_from_date, 'MS', - Get_Date_Part(eff_from_date, 'MS')) AS v_Yesterday,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART( v_Yesterday, 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(v_Yesterday, 'HH', 23), 'MI', 59), 'SS', 59) AS v_Rundate,
	v_Rundate AS Rundate,
	AGG_Supress_Multiple_Audits.PremiumTransactionEffectiveDate,
	AGG_Supress_Multiple_Audits.PremiumTransactionExpirationDate,
	-- *INF*: IIF((PremiumTransactionPremiumType='D' and Lkp_PremiumType='D') OR (PremiumTransactionPremiumType='C' and Lkp_PremiumType='D') OR (PremiumTransactionPremiumType='C' and Lkp_PremiumType='C'),'TRUE','FALSE')
	IFF(( PremiumTransactionPremiumType = 'D' AND Lkp_PremiumType = 'D' ) OR ( PremiumTransactionPremiumType = 'C' AND Lkp_PremiumType = 'D' ) OR ( PremiumTransactionPremiumType = 'C' AND Lkp_PremiumType = 'C' ), 'TRUE', 'FALSE') AS v_Direct_Ceded_Flag,
	-- *INF*: IIF((v_Rundate=Lkp_Rundate) OR (to_char(v_Yesterday,'YYYYMM')>to_char(PremiumTransactionExpirationDate,'YYYYMM')),'TRUE','FALSE')
	-- 
	-- 
	-- --IIF(PremiumMasterRunDate>=PremiumMasterCoverageEffectiveDate and PremiumMasterRunDate----<=PremiumMasterCoverageExpirationDate and v_Rundate=Lkp_Rundate,'TRUE','FLASE')
	IFF(( v_Rundate = Lkp_Rundate ) OR ( to_char(v_Yesterday, 'YYYYMM') > to_char(PremiumTransactionExpirationDate, 'YYYYMM') ), 'TRUE', 'FALSE') AS v_Audit_After_Cancellation_Reinstatement,
	-- *INF*: 'TRUE'
	-- 
	-- 
	-- --IIF(to_char(PremiumMasterRunDate,'YYYYMM')=to_char(Lkp_StatisticalCoverageCancellationDate,'YYYYMM'),'FALSE','TRUE')
	'TRUE' AS v_Flag_First_Audit,
	-- *INF*: IIF(ISNULL(v_StatisticalCoverageCancellationDate) OR v_StatisticalCoverageCancellationDate=TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'),'FALSE',IIF(Min_Premium=0.0 and v_Direct_Ceded_Flag='TRUE','TRUE','FALSE'))
	IFF(v_StatisticalCoverageCancellationDate IS NULL OR v_StatisticalCoverageCancellationDate = TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'), 'FALSE', IFF(Min_Premium = 0.0 AND v_Direct_Ceded_Flag = 'TRUE', 'TRUE', 'FALSE')) AS Flag
	FROM AGG_Supress_Multiple_Audits
	LEFT JOIN LKP_Get_CancellationDate
	ON LKP_Get_CancellationDate.PolicyAKID = AGG_Supress_Multiple_Audits.PolicyAKID AND LKP_Get_CancellationDate.StatisticalCoverageAKID = AGG_Supress_Multiple_Audits.StatisticalCoverageAKID AND LKP_Get_CancellationDate.RatingCoverageAKID = AGG_Supress_Multiple_Audits.RatingCoverageAKId
),
FIL_First_Audits AS (
	SELECT
	PremiumTransactionID, 
	PolicyKey, 
	PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	RatingCoverageAKId, 
	PremiumTransactionCode, 
	PremiumTransactionPremiumType, 
	PremiumTransactionPremium, 
	PremiumTransactionFullTermPremium, 
	StatisticalCoverageCancellationDate, 
	Flag, 
	Rundate
	FROM EXP_Flag_First_Audits
	WHERE Flag='TRUE'
),
Exp_Tgt_Data_Collect AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	PremiumTransactionID,
	PolicyKey,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	RatingCoverageAKId,
	PremiumTransactionCode,
	PremiumTransactionPremiumType,
	PremiumTransactionPremium,
	PremiumTransactionFullTermPremium,
	StatisticalCoverageCancellationDate,
	Rundate
	FROM FIL_First_Audits
),
WorkFirstAuditDaily AS (
	INSERT INTO Shortcut_to_WorkFirstAuditDaily
	(AuditId, CreatedDate, PremiumTransactionID, PolicyKey, PolicyAKID, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumTransactionCode, PremiumTransactionPremiumType, PremiumTransactionPremium, PremiumTransactionFullTermPremium, StatisticalCoverageCancellationDate, Rundate, RatingCoverageAKId)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	PREMIUMTRANSACTIONID, 
	POLICYKEY, 
	POLICYAKID, 
	RISKLOCATIONAKID, 
	POLICYCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	PREMIUMTRANSACTIONCODE, 
	PREMIUMTRANSACTIONPREMIUMTYPE, 
	PREMIUMTRANSACTIONPREMIUM, 
	PREMIUMTRANSACTIONFULLTERMPREMIUM, 
	STATISTICALCOVERAGECANCELLATIONDATE, 
	RUNDATE, 
	RATINGCOVERAGEAKID
	FROM Exp_Tgt_Data_Collect
),