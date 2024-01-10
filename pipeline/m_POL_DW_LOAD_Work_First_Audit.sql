WITH
SQ_PremiumMasterCalculation AS (
	select A.PremiumMasterCalculationID,
	 A.PolicyKey, 
	 A.PolicyAKID, 
	 A.RiskLocationAKID, 
	 A.PolicyCoverageAKID, 
	 A.StatisticalCoverageAKID, 
	 A.PremiumMasterTransactionCode, 
	 A.PremiumMasterRunDate, 
	 A.PremiumMasterPremiumType, 
	 A.PremiumMasterPremium, 
	 A.PremiumMasterFullTermPremium,
	A.PremiumMasterCoverageEffectiveDate,
	A.PremiumMasterCoverageExpirationDate,
	A.RatingCoverageAKId from (
	SELECT PMC.PremiumMasterCalculationID,
	 PMC.PolicyKey, 
	 PMC.PolicyAKID, 
	 PMC.RiskLocationAKID, 
	 PMC.PolicyCoverageAKID, 
	 PMC.StatisticalCoverageAKID, 
	 PMC.PremiumMasterTransactionCode, 
	 PMC.PremiumMasterRunDate, 
	 PMC.PremiumMasterPremiumType, 
	 PMC.PremiumMasterPremium, 
	 PMC.PremiumMasterFullTermPremium,
	PMC.PremiumMasterCoverageEffectiveDate,
	PMC.PremiumMasterCoverageExpirationDate,
	 WFA.PolicyAKID Work_PolicyAKID,
	PMC.PremiumMasterReasonAmendedCode,
	PMC.RatingCoverageAKId,
	PMC.SourceSystemID
	FROM
	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMasterCalculation PMC left outer join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkFirstAudit WFA
	 on PMC.PolicyAKID = WFA.PolicyAKID 
	 AND PMC.StatisticalCoverageAKID = WFA.StatisticalCoverageAKID
	AND PMC.RatingCoverageAKID=WFA.RatingCoverageAKID
	AND PMC.PremiumMasterPremiumType=WFA.PremiumMasterPremiumType
	 where CONVERT(varchar(6), case when PMC.Premiummasterrundate>=PMC.PremiumMasterCoverageEffectiveDate then PMC.Premiummasterrundate else PMC.PremiumMasterCoverageEffectiveDate end,112)=CONVERT(varchar(6),DATEADD(MM, -@{pipeline().parameters.NO_OF_MONTHS}, GETDATE()),112)
	--and case when PMC.PremiumMasterPremium=0.0 then 'FALSE'
	--when PMC.PremiumMasterTransactionCode='15' then 'FALSE' else 'TRUE' end='TRUE'
	and PMC.PremiumMasterTransactionCode in ('14','24')) A
	where A.Work_PolicyAKID is null
	@{pipeline().parameters.REASON_AMENDED_CODE}
	@{pipeline().parameters.WHERE_CLAUSE}
),
AGG_Supress_Multiple_Audits AS (
	SELECT
	PremiumMasterCalculationID,
	PolicyKey,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumMasterTransactionCode,
	PremiumMasterRunDate,
	PremiumMasterPremiumType,
	PremiumMasterPremium,
	PremiumMasterFullTermPremium,
	PremiumMasterCoverageEffectiveDate,
	PremiumMasterCoverageExpirationDate,
	RatingCoverageAKId
	FROM SQ_PremiumMasterCalculation
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID, StatisticalCoverageAKID, PremiumMasterPremiumType, RatingCoverageAKId ORDER BY NULL) = 1
),
LKP_Get_CancellationDate AS (
	SELECT
	StatisticalCoverageCancellationDate,
	Min_Premium,
	Rundate,
	PremiumType,
	PolicyAKID,
	StatisticalCoverageAKID,
	RatingCoverageAKId
	FROM (
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
		and CONVERT(varchar(6),WEPCM.Rundate,112)<=CONVERT(varchar(6),DATEADD(MM, -@{pipeline().parameters.NO_OF_MONTHS}, GETDATE()),112)
		and WEPCM.MinimumPremium=0
		@{pipeline().parameters.LOOKUP_WORK_CLAUSE}
		ORDER BY WEPCM.Rundate,WEPCM.PremiumType
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,StatisticalCoverageAKID,RatingCoverageAKId ORDER BY StatisticalCoverageCancellationDate DESC) = 1
),
EXP_Flag_First_Audits AS (
	SELECT
	AGG_Supress_Multiple_Audits.PremiumMasterCalculationID,
	AGG_Supress_Multiple_Audits.PolicyKey,
	AGG_Supress_Multiple_Audits.PolicyAKID,
	AGG_Supress_Multiple_Audits.RiskLocationAKID,
	AGG_Supress_Multiple_Audits.PolicyCoverageAKID,
	AGG_Supress_Multiple_Audits.StatisticalCoverageAKID,
	AGG_Supress_Multiple_Audits.PremiumMasterTransactionCode,
	AGG_Supress_Multiple_Audits.PremiumMasterPremiumType,
	AGG_Supress_Multiple_Audits.PremiumMasterPremium,
	AGG_Supress_Multiple_Audits.PremiumMasterFullTermPremium,
	LKP_Get_CancellationDate.StatisticalCoverageCancellationDate AS Lkp_StatisticalCoverageCancellationDate,
	-- *INF*: IIF(NOT ISNULL(Lkp_StatisticalCoverageCancellationDate),IIF((PremiumMasterPremiumType='D' and Lkp_PremiumType='D') 
	-- OR (PremiumMasterPremiumType='C' and Lkp_PremiumType='D') 
	-- OR (PremiumMasterPremiumType='C' and Lkp_PremiumType='C')
	-- ,Lkp_StatisticalCoverageCancellationDate,TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')),Lkp_StatisticalCoverageCancellationDate)
	-- 
	-- --If premiumMasterPremiumType is Direct and the Cancelled Coverage PremiumType is Ceeded then the cancelled date will be '12/31/2100 23:59:59'.
	IFF(NOT Lkp_StatisticalCoverageCancellationDate IS NULL, IFF(( PremiumMasterPremiumType = 'D' AND Lkp_PremiumType = 'D' ) OR ( PremiumMasterPremiumType = 'C' AND Lkp_PremiumType = 'D' ) OR ( PremiumMasterPremiumType = 'C' AND Lkp_PremiumType = 'C' ), Lkp_StatisticalCoverageCancellationDate, TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')), Lkp_StatisticalCoverageCancellationDate) AS v_StatisticalCoverageCancellationDate,
	v_StatisticalCoverageCancellationDate AS StatisticalCoverageCancellationDate,
	LKP_Get_CancellationDate.Min_Premium,
	LKP_Get_CancellationDate.Rundate AS Lkp_Rundate,
	LKP_Get_CancellationDate.PremiumType AS Lkp_PremiumType,
	AGG_Supress_Multiple_Audits.PremiumMasterRunDate,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART( PremiumMasterRunDate, 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(PremiumMasterRunDate, 'HH', 23), 'MI', 59), 'SS', 59) AS v_Rundate,
	v_Rundate AS Rundate,
	AGG_Supress_Multiple_Audits.PremiumMasterCoverageEffectiveDate,
	AGG_Supress_Multiple_Audits.PremiumMasterCoverageExpirationDate,
	-- *INF*: IIF((PremiumMasterPremiumType='D' and Lkp_PremiumType='D') OR (PremiumMasterPremiumType='C' and Lkp_PremiumType='D') OR (PremiumMasterPremiumType='C' and Lkp_PremiumType='C'),'TRUE','FALSE')
	IFF(( PremiumMasterPremiumType = 'D' AND Lkp_PremiumType = 'D' ) OR ( PremiumMasterPremiumType = 'C' AND Lkp_PremiumType = 'D' ) OR ( PremiumMasterPremiumType = 'C' AND Lkp_PremiumType = 'C' ), 'TRUE', 'FALSE') AS v_Direct_Ceeded_Flag,
	-- *INF*: IIF(TRUNC(IIF(v_Rundate>=PremiumMasterCoverageEffectiveDate,v_Rundate,PremiumMasterCoverageEffectiveDate),'MM')=TRUNC(Lkp_Rundate,'MM'),
	-- 'TRUE','FALSE')
	-- 
	-- --IIF((v_Rundate=Lkp_Rundate) OR (to_char(PremiumMasterRunDate,'YYYYMM')>to_char(PremiumMasterCoverageExpirationDate,'YYYYMM')),'TRUE','FALSE')
	-- 
	-- 
	-- --IIF(PremiumMasterRunDate>=PremiumMasterCoverageEffectiveDate and PremiumMasterRunDate----<=PremiumMasterCoverageExpirationDate and v_Rundate=Lkp_Rundate,'TRUE','FLASE')
	IFF(TRUNC(IFF(v_Rundate >= PremiumMasterCoverageEffectiveDate, v_Rundate, PremiumMasterCoverageEffectiveDate), 'MM') = TRUNC(Lkp_Rundate, 'MM'), 'TRUE', 'FALSE') AS v_Audit_After_Cancellation_Reinstatement,
	-- *INF*: 'TRUE'
	-- 
	-- 
	-- --IIF(to_char(PremiumMasterRunDate,'YYYYMM')=to_char(Lkp_StatisticalCoverageCancellationDate,'YYYYMM'),'FALSE','TRUE')
	'TRUE' AS v_Flag_First_Audit,
	AGG_Supress_Multiple_Audits.RatingCoverageAKId,
	-- *INF*: IIF(ISNULL(v_StatisticalCoverageCancellationDate) OR v_StatisticalCoverageCancellationDate=TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'),'FALSE',IIF(Min_Premium=0.0 and v_Flag_First_Audit='TRUE' and v_Audit_After_Cancellation_Reinstatement='TRUE' and v_Direct_Ceeded_Flag='TRUE','TRUE','FALSE'))
	IFF(v_StatisticalCoverageCancellationDate IS NULL OR v_StatisticalCoverageCancellationDate = TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'), 'FALSE', IFF(Min_Premium = 0.0 AND v_Flag_First_Audit = 'TRUE' AND v_Audit_After_Cancellation_Reinstatement = 'TRUE' AND v_Direct_Ceeded_Flag = 'TRUE', 'TRUE', 'FALSE')) AS Flag
	FROM AGG_Supress_Multiple_Audits
	LEFT JOIN LKP_Get_CancellationDate
	ON LKP_Get_CancellationDate.PolicyAKID = AGG_Supress_Multiple_Audits.PolicyAKID AND LKP_Get_CancellationDate.StatisticalCoverageAKID = AGG_Supress_Multiple_Audits.StatisticalCoverageAKID AND LKP_Get_CancellationDate.RatingCoverageAKId = AGG_Supress_Multiple_Audits.RatingCoverageAKId
),
FIL_First_Audits AS (
	SELECT
	PremiumMasterCalculationID, 
	PolicyKey, 
	PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageAKID, 
	StatisticalCoverageAKID, 
	PremiumMasterTransactionCode, 
	PremiumMasterPremiumType, 
	PremiumMasterPremium, 
	PremiumMasterFullTermPremium, 
	StatisticalCoverageCancellationDate, 
	Flag, 
	Rundate, 
	RatingCoverageAKId
	FROM EXP_Flag_First_Audits
	WHERE Flag='TRUE'
),
Exp_Tgt_Data_Collect AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	PremiumMasterCalculationID,
	PolicyKey,
	PolicyAKID,
	RiskLocationAKID,
	PolicyCoverageAKID,
	StatisticalCoverageAKID,
	PremiumMasterTransactionCode,
	PremiumMasterPremiumType,
	PremiumMasterPremium,
	PremiumMasterFullTermPremium,
	StatisticalCoverageCancellationDate,
	Rundate,
	RatingCoverageAKId
	FROM FIL_First_Audits
),
WorkFirstAudit AS (
	INSERT INTO Shortcut_to_WorkFirstAudit
	(AuditId, CreatedDate, PremiumMasterCalculationID, PolicyKey, PolicyAKID, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumMasterTransactionCode, PremiumMasterPremiumType, PremiumMasterPremium, PremiumMasterFullTermPremium, StatisticalCoverageCancellationDate, Rundate, RatingCoverageAKId)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	PREMIUMMASTERCALCULATIONID, 
	POLICYKEY, 
	POLICYAKID, 
	RISKLOCATIONAKID, 
	POLICYCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	PREMIUMMASTERTRANSACTIONCODE, 
	PREMIUMMASTERPREMIUMTYPE, 
	PREMIUMMASTERPREMIUM, 
	PREMIUMMASTERFULLTERMPREMIUM, 
	STATISTICALCOVERAGECANCELLATIONDATE, 
	RUNDATE, 
	RATINGCOVERAGEAKID
	FROM Exp_Tgt_Data_Collect
),