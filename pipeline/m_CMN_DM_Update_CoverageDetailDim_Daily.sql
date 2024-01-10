WITH
LKP_Policy AS (
	SELECT
	pol_cancellation_date,
	PolicyAKID
	FROM (
		select  PolicyAKID  as PolicyAKID   , pol_cancellation_date  as  pol_cancellation_date 
		from  (
		SELECT DISTINCT PC.PolicyAKID  AS PolicyAKID
		        ,P.pol_cancellation_date AS pol_cancellation_date
		  FROM  V2.policy P   INNER JOIN dbo.PolicyCoverage PC  ON   PC.PolicyAKID=P.pol_ak_id  AND  P.crrnt_snpsht_flag=1   
		  inner join StatisticalCoverage sc on sc.PolicyCoverageAKID =pc.PolicyCoverageAKID AND  PC.CurrentSnapshotFlag=1 
		  inner join PremiumTransaction pt on  pt.StatisticalCoverageAKID =sc.StatisticalCoverageAKID and sc.CurrentSnapshotFlag =1 
		  WHERE P.pol_status_code='C' and pt.CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}'
		
		union 
		
		SELECT DISTINCT PC.PolicyAKID  AS PolicyAKID
		        ,P.pol_cancellation_date AS pol_cancellation_date
		  FROM  V2.policy P   INNER JOIN dbo.PolicyCoverage PC  ON   PC.PolicyAKID=P.pol_ak_id  AND  P.crrnt_snpsht_flag=1   
		  inner join RatingCoverage rc on rc.PolicyCoverageAKID =pc.PolicyCoverageAKID AND  PC.CurrentSnapshotFlag=1 
		  inner join PremiumTransaction pt on  pt.RatingCoverageAKId  =rc.RatingCoverageAKID and pt.EffectiveDate =rc.EffectiveDate and  rc.CurrentSnapshotFlag =1 
		  WHERE P.pol_status_code='C' and pt.CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}' )   as PMSDCT
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID ORDER BY pol_cancellation_date) = 1
),
SQ_PMS AS (
	SELECT  Distinct    PT.PremiumTransactionID
	           ,SC.CoverageGuid
	           ,SC.StatisticalCoverageAKID AS CoverageAKID
	           ,SC.SourceSystemID
	           ,MIN(SC.StatisticalCoverageEffectiveDate) over (partition by SC.CoverageGuid ,SC.StatisticalCoverageAKID ,SC.SourceSystemID )AS CoverageEffectiveDate   
	           ,MIN(PT.PremiumTransactionEffectiveDate)  over (partition by SC.CoverageGuid ,SC.StatisticalCoverageAKID ,SC.SourceSystemID )AS PremiumTransactionEffectiveDate
	           ,MAX(PT.PremiumTransactionExpirationDate) over (partition by SC.CoverageGuid ,SC.StatisticalCoverageAKID ,SC.SourceSystemID )AS  PremiumTransactionExpirationDate
			   ,PC.PolicyAKID
	FROM         @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	INNER JOIN   @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	ON          SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
	AND         PT.SourceSystemID='PMS'
	INNER JOIN  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON          PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
	AND         PC.SourceSystemID='PMS'
	AND         PC.CurrentSnapshotFlag =1
	INNER JOIN ( select SC2.StatisticalCoverageAKID
	               from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2
				   INNER JOIN  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC2 ON SC2.StatisticalCoverageAKID=PT2.StatisticalCoverageAKID AND SC2.CurrentSnapshotFlag=1
				   AND PT2.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' 
	               AND PT2.SourceSystemId='PMS' ) B
				   ON B.StatisticalCoverageAKID = SC.StatisticalCoverageAKID  
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
),
SQ_StatisticalCoverage AS (
	select  
	step2.SourceSystemID as SourceSystemID
	,step2.StatisticalCoverageAKID as StatisticalCoverageAKID
	,MAX(step2.StatisticalCoverageCancellationDate) as StatisticalCoverageCancellationDate
	,STEP2.RunDate 
	,STEP2.PolicyAKID
	
	from 
	(
	select STEP1.SourceSystemID AS SourceSystemID
	       ,STEP1.StatisticalCoverageAKID  AS StatisticalCoverageAKID  
	       ,step1.StatisticalCoverageCancellationDate as StatisticalCoverageCancellationDate
		   ,MAX (rundate ) OVER ( partition by STEP1.StatisticalCoverageAKID, STEP1.PolicyAKID ) maxrundate
	       ,STEP1.RunDate AS RunDate
		   ,STEP1.PolicyAKID AS PolicyAKID 
	from 
	(
	 select A.StatisticalCoverageAKID,StatisticalCoverageCancellationDate,A.RunDate ,A.SourceSystemID,A.PolicyAKID
	 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageMonthly A
	 INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC ON A.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
	 INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID AND SC.CurrentSnapshotFlag = 1
	   AND PT.CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}' 
	 inner join 
	 (select StatisticalCoverageAKID,PolicyAKID,max(rundate) Rundate 
	  from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageMonthly
	  group by StatisticalCoverageAKID,PolicyAKID
	 ) B
	 on A.StatisticalCoverageAKID=B.StatisticalCoverageAKID
	 and A.RunDate=B.Rundate
	 and A.PolicyAKID=B.PolicyAKID
	 where A.SourceSystemID='PMS' and A.PremiumType='D' 
	
	
	 UNION
	
	 select A.StatisticalCoverageAKID,StatisticalCoverageCancellationDate,A.RunDate ,A.SourceSystemID,A.PolicyAKID
	 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageDaily A
	 INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC ON A.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
	 INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID AND SC.CurrentSnapshotFlag = 1
	   AND PT.CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}' 
	 inner join 
	 (select StatisticalCoverageAKID,PolicyAKID,max(rundate) Rundate 
	 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageDaily
	 group by StatisticalCoverageAKID,PolicyAKID) B
	 on A.StatisticalCoverageAKID=B.StatisticalCoverageAKID
	 and A.RunDate=B.Rundate
	 AND A.PolicyAKID=B.PolicyAKID
	 where A.StatisticalCoverageCancellationDate<>'2100-12-31 23:59:59'
	 and A.SourceSystemID='PMS' and A.PremiumType='D'
	 ) STEP1)
	 STEP2  
	WHERE maxrundate  = RunDate
	GROUP BY STEP2.StatisticalCoverageAKID,STEP2.PolicyAKID,STEP2.SourceSystemID,RunDate
),
JNR_GetPMSCancellationDate AS (SELECT
	SQ_StatisticalCoverage.SourceSystemID, 
	SQ_StatisticalCoverage.StatisticalCoverageAKID, 
	SQ_StatisticalCoverage.StatisticalCoverageCancellationDate, 
	SQ_StatisticalCoverage.Rundate, 
	SQ_StatisticalCoverage.PolicyAKID, 
	SQ_PMS.PremiumTransactionID, 
	SQ_PMS.CoverageGuid, 
	SQ_PMS.CoverageAKID, 
	SQ_PMS.SourceSystemID AS SourceSystemID1, 
	SQ_PMS.CoverageEffectiveDate, 
	SQ_PMS.PremiumTransactionEffectiveDate, 
	SQ_PMS.PremiumTransactionExpirationDate, 
	SQ_PMS.PolicyAKID AS PolicyAKID1
	FROM SQ_StatisticalCoverage
	RIGHT OUTER JOIN SQ_PMS
	ON SQ_PMS.CoverageAKID = SQ_StatisticalCoverage.StatisticalCoverageAKID AND SQ_PMS.PolicyAKID = SQ_StatisticalCoverage.PolicyAKID
),
SQ_DCT AS (
	WITH STEP1 AS
	(
	select DISTINCT RC3.RatingCoverageAKID
	               from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT3 
				   INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC3 
	ON RC3.RatingCoverageAKID=PT3.RatingCoverageAKID AND RC3.EffectiveDate=PT3.EffectiveDate AND RC3.CurrentSnapshotFlag = 1
						  AND PT3.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	)
	
	
	SELECT   Distinct   PT.PremiumTransactionID
	           ,RC.CoverageGuid
	           ,RC.RatingCoverageAKID AS CoverageAKID
	           ,RC.SourceSystemID
	           ,MIN(RC.RatingCoverageEffectiveDate) over (partition by RC.CoverageGuid ,PC.PolicyAKID,RC.RatingCoverageAKID ,RC.SourceSystemID )AS CoverageEffectiveDate   
	           ,MIN(PT.PremiumTransactionEffectiveDate)  over (partition by RC.CoverageGuid ,PC.PolicyAKID,RC.RatingCoverageAKID ,RC.SourceSystemID )AS PremiumTransactionEffectiveDate
	           ,MAX(PT.PremiumTransactionExpirationDate) over (partition by RC.CoverageGuid ,PC.PolicyAKID,RC.RatingCoverageAKID ,RC.SourceSystemID )AS  PremiumTransactionExpirationDate
			    ,PC.PolicyAKID
	FROM       @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	INNER JOIN  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	ON          RC.RatingCoverageAKID=PT.RatingCoverageAKID
	AND         RC.EffectiveDate=PT.EffectiveDate
	AND         PT.SourceSystemID='DCT'
	INNER JOIN  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON 			RC.PolicyCoverageAKID=PC.PolicyCoverageAKID
	AND         PC.SourceSystemID='DCT'
	INNER JOIN  STEP1   ON STEP1.RatingCoverageAKID = RC.RatingCoverageAKID
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
),
LKP_RatingCoverage AS (
	SELECT
	StatisticalCoverageCancellationDate,
	RunDate,
	RatingCoverageAKId,
	PolicyAKID
	FROM (
		SELECT Distinct 
		        STEP2.RatingCoverageAKId AS RatingCoverageAKId
			   ,STEP2.StatisticalCoverageCancellationDate  AS StatisticalCoverageCancellationDate  
			   ,STEP2.RunDate AS RunDate 
			   ,STEP2.SourceSystemID AS SourceSystemID
			   ,STEP2.PolicyAKID AS PolicyAKID
		FROM 
		(
		SELECT A.RatingCoverageAKID,A.StatisticalCoverageCancellationDate,A.RunDate,A.SourceSystemID      ,A.PolicyAKID   
		FROM     @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageMonthly A
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC ON A.RatingCoverageAKID = RC.RatingCoverageAKID
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON RC.RatingCoverageAKID=PT.RatingCoverageAKID  AND RC.EffectiveDate = PT.EffectiveDate
		AND PT.CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}' AND PT.SourceSystemID='DCT'
		INNER JOIN   (select RatingCoverageAKID,PolicyAKID,max(rundate) Rundate  
					  FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageMonthly 
		                WHERE SourceSystemID = 'DCT'
					  GROUP BY RatingCoverageAKID,PolicyAKID  ) AM
		ON          A.RatingCoverageAKID = AM.RatingCoverageAKID
		AND         A.PolicyAKID = AM.PolicyAKID
		AND         A.RunDate = AM.Rundate
		WHERE   A.SourceSystemID='DCT' and A.PremiumType='D'
		
		UNION 
		
		SELECT A.RatingCoverageAKID,A.StatisticalCoverageCancellationDate,A.RunDate,A.SourceSystemID     ,A.PolicyAKID    
		FROM     @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageDaily A
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC ON A.RatingCoverageAKID = RC.RatingCoverageAKID
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON RC.RatingCoverageAKID=PT.RatingCoverageAKID  AND RC.EffectiveDate = PT.EffectiveDate
		AND PT.CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}' AND PT.SourceSystemID='DCT'
		INNER JOIN (SELECT RatingCoverageAKID,PolicyAKID,max(rundate) Rundate  
				           FROM  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageDaily 
		                     WHERE SourceSystemID = 'DCT'
					      GROUP BY RatingCoverageAKID ,PolicyAKID ) AM
		ON          A.RatingCoverageAKID = AM.RatingCoverageAKID
		AND         A.PolicyAKID=AM.PolicyAKID
		AND         A.RunDate = AM.Rundate
		WHERE  A.SourceSystemID='DCT' and A.PremiumType='D' 
		--ADDED BY Hongjie On May 11,2015
		AND A.StatisticalCoverageCancellationDate<>'2100-12-31 23:59:59'
		) STEP2
		ORDER BY STEP2.RatingCoverageAKId, STEP2.PolicyAKID,STEP2.RunDate,  STEP2.StatisticalCoverageCancellationDate ASC 
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingCoverageAKId,PolicyAKID ORDER BY StatisticalCoverageCancellationDate DESC) = 1
),
Union AS (
	SELECT PremiumTransactionID, CoverageGuid, SourceSystemID, StatisticalCoverageCancellationDate, Rundate AS RunDate, CoverageEffectiveDate, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate, PolicyAKID1 AS PolicyAKID
	FROM JNR_GetPMSCancellationDate
	UNION
	SELECT PremiumTransactionID, CoverageGuid, SourceSystemID, StatisticalCoverageCancellationDate, RunDate, CoverageEffectiveDate, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate
	FROM LKP_RatingCoverage
	-- Manually join with SQ_DCT
),
EXP_Value AS (
	SELECT
	PremiumTransactionID,
	CoverageGuid,
	StatisticalCoverageCancellationDate AS i_StatisticalCoverageCancellationDate,
	RunDate AS i_RunDate,
	CoverageEffectiveDate AS i_CoverageEffectiveDate,
	PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate AS i_PremiumTransactionExpirationDate,
	PolicyAKID AS i_PolicyAKID,
	-- *INF*: IIF( ISNULL( i_CoverageEffectiveDate ),  TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')   , i_CoverageEffectiveDate )
	IFF(i_CoverageEffectiveDate IS NULL,
		TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		i_CoverageEffectiveDate
	) AS v_CoverageEffectiveDate,
	-- *INF*: IIF( ISNULL( i_StatisticalCoverageCancellationDate ),  TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')  , i_StatisticalCoverageCancellationDate)
	IFF(i_StatisticalCoverageCancellationDate IS NULL,
		TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
		),
		i_StatisticalCoverageCancellationDate
	) AS v_StatisticalCoverageCancellationDate,
	-- *INF*: IIF( ISNULL(  i_PremiumTransactionEffectiveDate ),  TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')  , i_PremiumTransactionEffectiveDate )
	IFF(i_PremiumTransactionEffectiveDate IS NULL,
		TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		i_PremiumTransactionEffectiveDate
	) AS v_PremiumTransactionEffectiveDate,
	-- *INF*: IIF( (v_PremiumTransactionEffectiveDate<=v_RunDate AND i_PremiumTransactionExpirationDate > v_RunDate  AND  
	-- TO_CHAR(  v_RunDate,'YYYY-MM_DD')  = TO_CHAR(SYSDATE,'YYYY-MM-DD')   )
	-- OR
	-- (   v_PremiumTransactionEffectiveDate<=v_RunDate AND GET_DATE_PART( i_PremiumTransactionExpirationDate ,'YYYY') > GET_DATE_PART( v_RunDate ,'YYYY')  )
	-- OR
	-- (   v_PremiumTransactionEffectiveDate<=v_RunDate AND GET_DATE_PART( i_PremiumTransactionExpirationDate ,'YYYY') = GET_DATE_PART( v_RunDate ,'YYYY') AND   GET_DATE_PART( i_PremiumTransactionExpirationDate ,'MM') >=  GET_DATE_PART( v_RunDate,'MM')   ), v_StatisticalCoverageCancellationDate,                                             TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	--   )
	IFF(( v_PremiumTransactionEffectiveDate <= v_RunDate 
			AND i_PremiumTransactionExpirationDate > v_RunDate 
			AND TO_CHAR(v_RunDate, 'YYYY-MM_DD'
			) = TO_CHAR(SYSDATE, 'YYYY-MM-DD'
			) 
		) 
		OR ( v_PremiumTransactionEffectiveDate <= v_RunDate 
			AND DATE_PART(i_PremiumTransactionExpirationDate, 'YYYY'
			) > DATE_PART(v_RunDate, 'YYYY'
			) 
		) 
		OR ( v_PremiumTransactionEffectiveDate <= v_RunDate 
			AND DATE_PART(i_PremiumTransactionExpirationDate, 'YYYY'
			) = DATE_PART(v_RunDate, 'YYYY'
			) 
			AND DATE_PART(i_PremiumTransactionExpirationDate, 'MM'
			) >= DATE_PART(v_RunDate, 'MM'
			) 
		),
		v_StatisticalCoverageCancellationDate,
		TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
		)
	) AS v_CoverageCancellationDate,
	-- *INF*: IIF( ISNULL( i_RunDate ),  TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')   , i_RunDate )
	IFF(i_RunDate IS NULL,
		TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		i_RunDate
	) AS v_RunDate,
	-- *INF*: (:LKP.LKP_Policy(i_PolicyAKID))
	( LKP_POLICY_i_PolicyAKID.pol_cancellation_date 
	) AS v_lkp_policy,
	-- *INF*: IIF( v_CoverageEffectiveDate>v_PremiumTransactionEffectiveDate,v_CoverageEffectiveDate,v_PremiumTransactionEffectiveDate )
	IFF(v_CoverageEffectiveDate > v_PremiumTransactionEffectiveDate,
		v_CoverageEffectiveDate,
		v_PremiumTransactionEffectiveDate
	) AS o_CoverageEffectiveDate,
	-- *INF*: IIF( ISNULL( i_PremiumTransactionExpirationDate ),  TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')  , i_PremiumTransactionExpirationDate )
	IFF(i_PremiumTransactionExpirationDate IS NULL,
		TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
		),
		i_PremiumTransactionExpirationDate
	) AS o_CoverageExpirationDate,
	-- *INF*: DECODE(TRUE,
	--  ISNULL(v_lkp_policy), v_CoverageCancellationDate,
	-- v_CoverageCancellationDate<= v_lkp_policy ,v_CoverageCancellationDate,
	-- v_lkp_policy)
	DECODE(TRUE,
		v_lkp_policy IS NULL, v_CoverageCancellationDate,
		v_CoverageCancellationDate <= v_lkp_policy, v_CoverageCancellationDate,
		v_lkp_policy
	) AS o_CoverageCancellationDate
	FROM Union
	LEFT JOIN LKP_POLICY LKP_POLICY_i_PolicyAKID
	ON LKP_POLICY_i_PolicyAKID.PolicyAKID = i_PolicyAKID

),
SQ_CoverageDetailDim AS (
	SELECT 
	CCD.CoverageDetailDimId as CoverageDetailDimId,
	CCD.CoverageGuid as CoverageGuid,
	CCD.EDWPremiumTransactionPKId as EDWPremiumTransactionPKId,
	CCD.CoverageEffectiveDate, 
	CCD.CoverageExpirationDate, 
	CCD.CoverageCancellationDate 
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim  CCD
	WHERE CCD.CoverageGuid IN 
	(
	SELECT DISTINCT CCD2.CoverageGuid
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim  CCD2
	INNER JOIN   @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT 
	ON CCD2.EDWPremiumTransactionPKId=PT.PremiumTransactionID
	WHERE PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	)
),
JNR_IL_And_DM AS (SELECT
	EXP_Value.PremiumTransactionID, 
	EXP_Value.CoverageGuid, 
	EXP_Value.o_CoverageEffectiveDate AS CoverageEffectiveDate, 
	EXP_Value.o_CoverageExpirationDate AS CoverageExpirationDate, 
	EXP_Value.o_CoverageCancellationDate AS CoverageCancellationDate, 
	SQ_CoverageDetailDim.CoverageDetailDimId, 
	SQ_CoverageDetailDim.CoverageGuid AS i_CoverageGuid, 
	SQ_CoverageDetailDim.EDWPremiumTransactionPKId AS i_EDWPremiumTransactionPKId, 
	SQ_CoverageDetailDim.CoverageEffectiveDate AS i_CoverageEffectiveDate, 
	SQ_CoverageDetailDim.CoverageExpirationDate AS i_CoverageExpirationDate, 
	SQ_CoverageDetailDim.CoverageCancellationDate AS i_CoverageCancellationDate
	FROM SQ_CoverageDetailDim
	INNER JOIN EXP_Value
	ON EXP_Value.PremiumTransactionID = SQ_CoverageDetailDim.EDWPremiumTransactionPKId AND EXP_Value.CoverageGuid = SQ_CoverageDetailDim.CoverageGuid
),
EXP_UPDATE_Change AS (
	SELECT
	CoverageEffectiveDate,
	CoverageExpirationDate,
	CoverageCancellationDate,
	CoverageDetailDimId,
	i_CoverageGuid,
	i_EDWPremiumTransactionPKId,
	i_CoverageEffectiveDate,
	i_CoverageExpirationDate,
	i_CoverageCancellationDate,
	-- *INF*: iif (
	-- 
	-- CoverageEffectiveDate=i_CoverageEffectiveDate
	-- and CoverageExpirationDate=i_CoverageExpirationDate
	-- and CoverageCancellationDate=i_CoverageCancellationDate,
	-- 'UNCHANGED',
	-- 'UPDATE')
	IFF(CoverageEffectiveDate = i_CoverageEffectiveDate 
		AND CoverageExpirationDate = i_CoverageExpirationDate 
		AND CoverageCancellationDate = i_CoverageCancellationDate,
		'UNCHANGED',
		'UPDATE'
	) AS Change_Flag
	FROM JNR_IL_And_DM
),
FLTR_ONLY_CHANGED_UPDATE AS (
	SELECT
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	CoverageCancellationDate, 
	CoverageDetailDimId, 
	Change_Flag
	FROM EXP_UPDATE_Change
	WHERE Change_Flag='UPDATE'
),
UDP_CoverageDetailDim AS (
	SELECT
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	CoverageCancellationDate, 
	CoverageDetailDimId
	FROM FLTR_ONLY_CHANGED_UPDATE
),
CoverageDetailDim AS (
	MERGE INTO CoverageDetailDim AS T
	USING UDP_CoverageDetailDim AS S
	ON T.CoverageDetailDimId = S.CoverageDetailDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageEffectiveDate = S.CoverageEffectiveDate, T.CoverageExpirationDate = S.CoverageExpirationDate, T.CoverageCancellationDate = S.CoverageCancellationDate
),