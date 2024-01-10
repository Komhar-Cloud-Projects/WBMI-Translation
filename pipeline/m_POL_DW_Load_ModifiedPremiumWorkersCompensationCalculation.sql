WITH
SQ_PremiumMasterCalculation AS (
	with f as 
	(SELECT [LineOfBusinessAbbreviation],
	       Ext_RatingStateCode AS [RatingStateCode],
	       [ClassCode] class_code,
	       [RatableClassIndicator] ratable_class_ind,
	       [SubjectToExperienceModificationClassIndicator] subject_to_exprnc_modfctn_class_ind,
	       [ExperienceModificationClassIndicator] exprnc_modfctn_class_ind,
	       [ScheduledModificationClassIndicator] sched_modfctn_class_ind,
	       [SurchargeClassIndicator] surchg_class_ind,
	       [OtherModificationClassIndicator] other_modfctn_class_ind
	FROM (SELECT [LineOfBusinessAbbreviation],
	             supState.state_abbrev Ext_RatingStateCode,
	             [RatingStateCode],
	             MIN([RatingStateCode]) OVER (PARTITION BY ClassCode, state_abbrev) MinRatingStateCode,
	             [ClassCode],
	             [RatableClassIndicator],
	             [SubjectToExperienceModificationClassIndicator],
	             [ExperienceModificationClassIndicator],
	             [ScheduledModificationClassIndicator],
	             [SurchargeClassIndicator],
	             [OtherModificationClassIndicator]
	FROM SupClassificationWorkersCompensation
	INNER JOIN (SELECT Allstate.state_abbrev AllAtateAbbrev,
	                   otherstate.state_abbrev
	FROM sup_state Allstate,
	     sup_state otherstate  --FULL OUTER JOIN
	WHERE Allstate.state_abbrev = '99'
	AND otherstate.state_abbrev != '99'
	UNION
	SELECT otherstate.state_abbrev allAtateAbbrev,
	       otherstate.state_abbrev
	FROM sup_state otherstate
	WHERE otherstate.state_abbrev != '99') AS supState
	  ON SupClassificationWorkersCompensation.RatingStateCode = supState.AllAtateAbbrev
	WHERE CurrentSnapshotFlag = 1) Extent_SupClassificationWorkersCompensation
	WHERE MinRatingStateCode = [RatingStateCode])
	----------------------------------------------------------------------
	SELECT *
	FROM (SELECT a.PremiumMasterCalculationID,
	             a.PremiumMasterPremium AS DirectWrittenPremium,
	             LEFT(b.ClassCode, 4) AS ClassCode,
	             f.ratable_class_ind,
	             f.subject_to_exprnc_modfctn_class_ind,
	             f.surchg_class_ind,
	             w.RunDate,
	             w.PolicyAKId,
	             w.RatingState,
	             w.SubjectDirectWrittenPremium,
	             w.ExperienceModifiedDirectWrittenPremium,
	             w.ScheduleModifiedDirectWrittenPremium,
	             w.OtherModifiedDirectWrittenPremium,
	             ROUND(SUM(CASE
	               WHEN z.TransactionCount > 0 THEN 0
	               WHEN f.ratable_class_ind = 'Y' AND
	                 f.subject_to_exprnc_modfctn_class_ind = 'Y' AND
	                 w.SubjectDirectWrittenPremium != 0 THEN a.PremiumMasterPremium * (w.OtherModifiedDirectWrittenPremium / w.SubjectDirectWrittenPremium)
	               WHEN f.ratable_class_ind = 'Y' AND
	                 f.subject_to_exprnc_modfctn_class_ind = 'Y' AND
	                 w.SubjectDirectWrittenPremium = 0 THEN a.PremiumMasterPremium + w.OtherModifiedDirectWrittenPremium / w.SubjectTransactionCount
	               WHEN f.ratable_class_ind = 'Y' AND
	                 f.subject_to_exprnc_modfctn_class_ind = 'N' THEN a.PremiumMasterPremium
	               ELSE 0
	             END)
	             OVER (PARTITION BY w.PolicyAKId, w.RatingState, w.RunDate), 4) AS RateableDirectWrittenPremium,
	             w.ClassifiedAdjustmentAmount +
	             ROUND(SUM(CASE
	               WHEN f.ratable_class_ind = 'N' AND
	                 f.subject_to_exprnc_modfctn_class_ind = 'Y' AND
	                 w.SubjectDirectWrittenPremium != 0 THEN a.PremiumMasterPremium * (w.OtherModifiedDirectWrittenPremium / w.SubjectDirectWrittenPremium)
	               WHEN f.ratable_class_ind = 'N' AND
	                 f.subject_to_exprnc_modfctn_class_ind = 'Y' AND
	                 w.SubjectDirectWrittenPremium = 0 THEN a.PremiumMasterPremium + w.OtherModifiedDirectWrittenPremium / w.SubjectTransactionCount
	               ELSE 0
	             END)
	             OVER (PARTITION BY w.PolicyAKId, w.RatingState, w.RunDate), 4) AS ClassifiedAdjustmentAmount,
	             w.SubjectTransactionCount,
	             w.RateableTransactionCount - ISNULL(z.TransactionCount, 0) RateableTransactionCount,
	             z.TransactionCount,
	             a.SourceSystemId,
	a.PremiumMasterRunDate as PremiumMasterRunDate
	FROM PremiumMasterCalculation a
	JOIN StatisticalCoverage b
	  ON a.StatisticalCoverageAKID = b.StatisticalCoverageAKID
	JOIN PolicyCoverage c
	  ON c.PolicyCoverageAKID = a.PolicyCoverageAKID
	  AND c.TypeBureauCode in ('WC','WP') 
	JOIN RiskLocation d
	  ON d.RiskLocationAKID = a.RiskLocationAKID  
	@{pipeline().parameters.JOIN_POLICY_LIST}  
	JOIN --master_classification_sup 
		f
	  ON f.class_code = LEFT(b.ClassCode, 4) and f.RatingStateCode=d.StateProvinceCode --add  RatingStateCode as part of join condition
	JOIN WorkRatingStatePremiumAggregation w
	  ON w.PolicyAKId = d.PolicyAKID
	  AND w.RatingState = d.StateProvinceCode
	  AND
	     CASE
	       WHEN a.PremiumMasterCoverageEffectiveDate >= a.PremiumTransactionEnteredDate THEN a.PremiumMasterCoverageEffectiveDate
	       ELSE a.PremiumTransactionEnteredDate
	     END <= w.RunDate
	LEFT JOIN WorkZeroedOutRatableClassCode z
	  ON z.PolicyAKId = d.PolicyAKID
	  AND z.RatingState = d.StateProvinceCode
	  AND z.ClassCode = LEFT(b.ClassCode, 4)
	  AND w.RunDate = z.RunDate
	WHERE a.CurrentSnapshotFlag = 1
	AND a.PremiumMasterPremiumType = 'D'
	AND a.PremiumMasterReasonAmendedCode NOT IN ('COL', 'CWO', 'Claw Back','CWB')
	AND a.SourceSystemId = 'PMS'
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
	UNION ALL
	SELECT a.PremiumMasterCalculationID,
	       a.PremiumMasterPremium AS DirectWrittenPremium,
	       LEFT(b.ClassCode, 4) AS ClassCode,
	       f.ratable_class_ind,
	       f.subject_to_exprnc_modfctn_class_ind,
	       f.surchg_class_ind,
	       w.RunDate,
	       w.PolicyAKId,
	       w.RatingState,
	       w.SubjectDirectWrittenPremium,
	       w.ExperienceModifiedDirectWrittenPremium,
	       w.ScheduleModifiedDirectWrittenPremium,
	       w.OtherModifiedDirectWrittenPremium,
	       ROUND(SUM(CASE
	         WHEN z.TransactionCount > 0 THEN 0
	         WHEN f.ratable_class_ind = 'Y' AND
	           f.subject_to_exprnc_modfctn_class_ind = 'Y' AND
	           w.SubjectDirectWrittenPremium != 0 THEN a.PremiumMasterPremium * (w.OtherModifiedDirectWrittenPremium / w.SubjectDirectWrittenPremium)
	         WHEN f.ratable_class_ind = 'Y' AND
	           f.subject_to_exprnc_modfctn_class_ind = 'Y' AND
	           w.SubjectDirectWrittenPremium = 0 THEN a.PremiumMasterPremium + w.OtherModifiedDirectWrittenPremium / w.SubjectTransactionCount
	         WHEN f.ratable_class_ind = 'Y' AND
	           f.subject_to_exprnc_modfctn_class_ind = 'N' THEN a.PremiumMasterPremium
	         ELSE 0
	       END)
	       OVER (PARTITION BY w.PolicyAKId, w.RatingState, w.RunDate), 4) AS RateableDirectWrittenPremium,
	       w.ClassifiedAdjustmentAmount +
	       ROUND(SUM(CASE
	         WHEN f.ratable_class_ind = 'N' AND
	           f.subject_to_exprnc_modfctn_class_ind = 'Y' AND
	           w.SubjectDirectWrittenPremium != 0 THEN a.PremiumMasterPremium * (w.OtherModifiedDirectWrittenPremium / w.SubjectDirectWrittenPremium)
	         WHEN f.ratable_class_ind = 'N' AND
	           f.subject_to_exprnc_modfctn_class_ind = 'Y' AND
	           w.SubjectDirectWrittenPremium = 0 THEN a.PremiumMasterPremium + w.OtherModifiedDirectWrittenPremium / w.SubjectTransactionCount
	         ELSE 0
	       END)
	       OVER (PARTITION BY w.PolicyAKId, w.RatingState, w.RunDate), 4) AS ClassifiedAdjustmentAmount,
	       w.SubjectTransactionCount,
	       w.RateableTransactionCount - ISNULL(z.TransactionCount, 0) RateableTransactionCount,
	       z.TransactionCount,
	       a.SourceSystemId,
	a.PremiumMasterRunDate as PremiumMasterRunDate
	FROM PremiumMasterCalculation a
	JOIN RatingCoverage b
	  ON a.RatingCoverageAKID = b.RatingCoverageAKID
	  AND b.EffectiveDate = a.RatingCoverageEffectiveDate
	JOIN PolicyCoverage c
	  ON c.PolicyCoverageAKID = a.PolicyCoverageAKID
	  AND c.CurrentSnapshotFlag = 1
	  AND c.InsuranceLine = 'WorkersCompensation'
	JOIN RiskLocation d
	  ON d.RiskLocationAKID = a.RiskLocationAKID
	  AND d.CurrentSnapshotFlag = 1  
	@{pipeline().parameters.JOIN_POLICY_LIST}  
	JOIN --master_classification_sup 
		f
	  ON f.class_code = LEFT(b.ClassCode, 4) and f.RatingStateCode=d.StateProvinceCode --add  RatingStateCode as part of join condition
	JOIN WorkRatingStatePremiumAggregation w
	  ON w.PolicyAKId = d.PolicyAKID
	  AND w.RatingState = d.StateProvinceCode
	  AND
	     CASE
	       WHEN a.PremiumMasterCoverageEffectiveDate >= a.PremiumTransactionEnteredDate THEN a.PremiumMasterCoverageEffectiveDate
	       ELSE a.PremiumTransactionEnteredDate
	     END <= w.RunDate
	LEFT JOIN WorkZeroedOutRatableClassCode z
	  ON z.PolicyAKId = d.PolicyAKID
	  AND z.RatingState = d.StateProvinceCode
	  AND z.ClassCode = LEFT(b.ClassCode, 4)
	  AND w.RunDate = z.RunDate
	WHERE a.CurrentSnapshotFlag = 1
	AND a.PremiumMasterPremiumType = 'D'
	AND a.PremiumMasterReasonAmendedCode NOT IN ('COL', 'CWO', 'Claw Back','CWB')
	AND a.SourceSystemId = 'DCT'
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
	) a
	ORDER BY PremiumMasterCalculationID, RunDate
),
EXP_ModifiedPremium_Calculate AS (
	SELECT
	PremiumMasterCalculationId,
	DirectWrittenPremium AS i_DirectWrittenPremium,
	ClassCode,
	ratable_class_ind AS i_ratable_class_ind,
	subject_to_exprnc_modfctn_class_ind AS i_subject_to_exprnc_modfctn_class_ind,
	surchg_class_ind AS i_surchg_class_ind,
	RunDate,
	PolicyAKId,
	RatingState,
	SubjectDirectWrittenPremium AS i_SubjectDirectWrittenPremium,
	ExperienceModifiedDirectWrittenPremium AS i_ExperienceModifiedDirectWrittenPremium,
	ScheduleModifiedDirectWrittenPremium AS i_ScheduleModifiedDirectWrittenPremium,
	OtherModifiedDirectWrittenPremium AS i_OtherModifiedDirectWrittenPremium,
	RateableDirectWrittenPremium AS i_RateableDirectWrittenPremium,
	-- *INF*: IIF(i_RateableDirectWrittenPremium<0.0010,0,i_RateableDirectWrittenPremium)
	IFF(i_RateableDirectWrittenPremium < 0.0010, 0, i_RateableDirectWrittenPremium) AS v_CorrectedRateableDirectWrittenPremium,
	ClassifiedAdjustmentAmount AS i_ClassifiedAdjustmentAmount,
	SubjectTransactionCount AS i_SubjectTransactionCount,
	RateableTransactionCount AS i_RateableTransactionCount,
	TransactionCount AS i_TransactionCount,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	-- *INF*:  i_RateableTransactionCount+IIF(ISNULL(i_TransactionCount),0,i_TransactionCount)
	i_RateableTransactionCount + IFF(i_TransactionCount IS NULL, 0, i_TransactionCount) AS v_TrueRateableCount,
	-- *INF*: IIF(PremiumMasterCalculationId=v_prev_PremiumMasterCalculationId,1,0)
	IFF(PremiumMasterCalculationId = v_prev_PremiumMasterCalculationId, 1, 0) AS v_GeneratedRecordIndicator,
	-- *INF*: IIF(v_GeneratedRecordIndicator='1',0,i_DirectWrittenPremium)
	IFF(v_GeneratedRecordIndicator = '1', 0, i_DirectWrittenPremium) AS v_DirectWrittenPremium,
	-- *INF*: IIF(i_subject_to_exprnc_modfctn_class_ind='Y',i_DirectWrittenPremium,0)
	IFF(i_subject_to_exprnc_modfctn_class_ind = 'Y', i_DirectWrittenPremium, 0) AS v_SubjectDirectWrittenPremium,
	-- *INF*: ROUND(DECODE(TRUE,
	-- i_SubjectDirectWrittenPremium!=0,v_SubjectDirectWrittenPremium*i_ExperienceModifiedDirectWrittenPremium/i_SubjectDirectWrittenPremium,
	-- i_subject_to_exprnc_modfctn_class_ind='Y',v_SubjectDirectWrittenPremium+i_ExperienceModifiedDirectWrittenPremium/i_SubjectTransactionCount,
	-- 0),4)
	ROUND(DECODE(TRUE,
		i_SubjectDirectWrittenPremium != 0, v_SubjectDirectWrittenPremium * i_ExperienceModifiedDirectWrittenPremium / i_SubjectDirectWrittenPremium,
		i_subject_to_exprnc_modfctn_class_ind = 'Y', v_SubjectDirectWrittenPremium + i_ExperienceModifiedDirectWrittenPremium / i_SubjectTransactionCount,
		0), 4) AS v_ExperienceModifiedDirectWrittenPremium,
	-- *INF*: ROUND(DECODE(TRUE,
	-- i_SubjectDirectWrittenPremium!=0,v_SubjectDirectWrittenPremium*i_ScheduleModifiedDirectWrittenPremium/i_SubjectDirectWrittenPremium,
	-- i_subject_to_exprnc_modfctn_class_ind='Y',v_SubjectDirectWrittenPremium+i_ScheduleModifiedDirectWrittenPremium/i_SubjectTransactionCount,
	-- 0),4)
	ROUND(DECODE(TRUE,
		i_SubjectDirectWrittenPremium != 0, v_SubjectDirectWrittenPremium * i_ScheduleModifiedDirectWrittenPremium / i_SubjectDirectWrittenPremium,
		i_subject_to_exprnc_modfctn_class_ind = 'Y', v_SubjectDirectWrittenPremium + i_ScheduleModifiedDirectWrittenPremium / i_SubjectTransactionCount,
		0), 4) AS v_ScheduleModifiedDirectWrittenPremium,
	-- *INF*: ROUND(DECODE(TRUE,
	-- i_SubjectDirectWrittenPremium!=0,v_SubjectDirectWrittenPremium*i_OtherModifiedDirectWrittenPremium/i_SubjectDirectWrittenPremium,
	-- i_subject_to_exprnc_modfctn_class_ind='Y',v_SubjectDirectWrittenPremium+i_OtherModifiedDirectWrittenPremium/i_SubjectTransactionCount
	-- ,0),4)
	ROUND(DECODE(TRUE,
		i_SubjectDirectWrittenPremium != 0, v_SubjectDirectWrittenPremium * i_OtherModifiedDirectWrittenPremium / i_SubjectDirectWrittenPremium,
		i_subject_to_exprnc_modfctn_class_ind = 'Y', v_SubjectDirectWrittenPremium + i_OtherModifiedDirectWrittenPremium / i_SubjectTransactionCount,
		0), 4) AS v_OtherModifiedDirectWrittenPremium,
	-- *INF*: DECODE(TRUE,
	-- i_ratable_class_ind='Y' AND i_subject_to_exprnc_modfctn_class_ind='Y',v_OtherModifiedDirectWrittenPremium,
	-- i_ratable_class_ind='Y' AND i_subject_to_exprnc_modfctn_class_ind='N',i_DirectWrittenPremium,
	-- 0)
	DECODE(TRUE,
		i_ratable_class_ind = 'Y' AND i_subject_to_exprnc_modfctn_class_ind = 'Y', v_OtherModifiedDirectWrittenPremium,
		i_ratable_class_ind = 'Y' AND i_subject_to_exprnc_modfctn_class_ind = 'N', i_DirectWrittenPremium,
		0) AS v_RateableDirectWrittenPremium,
	-- *INF*: ROUND(DECODE(TRUE,
	-- i_surchg_class_ind='Y',i_DirectWrittenPremium,
	-- i_ratable_class_ind='Y' AND NOT ISNULL(i_TransactionCount),v_RateableDirectWrittenPremium,
	-- i_ratable_class_ind='Y' AND v_CorrectedRateableDirectWrittenPremium!=0 AND i_RateableTransactionCount!=0,v_RateableDirectWrittenPremium*(i_ClassifiedAdjustmentAmount+v_CorrectedRateableDirectWrittenPremium)/v_CorrectedRateableDirectWrittenPremium,
	-- i_ratable_class_ind='Y' AND v_CorrectedRateableDirectWrittenPremium=0 AND i_RateableTransactionCount!=0,v_RateableDirectWrittenPremium+i_ClassifiedAdjustmentAmount/i_RateableTransactionCount,
	-- v_TrueRateableCount=0,i_DirectWrittenPremium,
	-- v_RateableDirectWrittenPremium),4)
	ROUND(DECODE(TRUE,
		i_surchg_class_ind = 'Y', i_DirectWrittenPremium,
		i_ratable_class_ind = 'Y' AND NOT i_TransactionCount IS NULL, v_RateableDirectWrittenPremium,
		i_ratable_class_ind = 'Y' AND v_CorrectedRateableDirectWrittenPremium != 0 AND i_RateableTransactionCount != 0, v_RateableDirectWrittenPremium * ( i_ClassifiedAdjustmentAmount + v_CorrectedRateableDirectWrittenPremium ) / v_CorrectedRateableDirectWrittenPremium,
		i_ratable_class_ind = 'Y' AND v_CorrectedRateableDirectWrittenPremium = 0 AND i_RateableTransactionCount != 0, v_RateableDirectWrittenPremium + i_ClassifiedAdjustmentAmount / i_RateableTransactionCount,
		v_TrueRateableCount = 0, i_DirectWrittenPremium,
		v_RateableDirectWrittenPremium), 4) AS v_ClassifiedDirectWrittenPremium,
	-- *INF*: IIF(v_GeneratedRecordIndicator='1',v_SubjectDirectWrittenPremium-v_prev_SubjectDirectWrittenPremium,v_SubjectDirectWrittenPremium)
	IFF(v_GeneratedRecordIndicator = '1', v_SubjectDirectWrittenPremium - v_prev_SubjectDirectWrittenPremium, v_SubjectDirectWrittenPremium) AS v_ChangeInSubjectDirectWrittenPremium,
	-- *INF*: IIF(v_GeneratedRecordIndicator='1',v_ExperienceModifiedDirectWrittenPremium-v_prev_ExperienceModifiedDirectWrittenPremium,v_ExperienceModifiedDirectWrittenPremium)
	IFF(v_GeneratedRecordIndicator = '1', v_ExperienceModifiedDirectWrittenPremium - v_prev_ExperienceModifiedDirectWrittenPremium, v_ExperienceModifiedDirectWrittenPremium) AS v_ChangeInExperienceModifiedDirectWrittenPremium,
	-- *INF*: IIF(v_GeneratedRecordIndicator='1',v_ScheduleModifiedDirectWrittenPremium-v_prev_ScheduleModifiedDirectWrittenPremium,v_ScheduleModifiedDirectWrittenPremium)
	IFF(v_GeneratedRecordIndicator = '1', v_ScheduleModifiedDirectWrittenPremium - v_prev_ScheduleModifiedDirectWrittenPremium, v_ScheduleModifiedDirectWrittenPremium) AS v_ChangeInScheduleModifiedDirectWrittenPremium,
	-- *INF*: IIF(v_GeneratedRecordIndicator='1',v_OtherModifiedDirectWrittenPremium-v_prev_OtherModifiedDirectWrittenPremium,v_OtherModifiedDirectWrittenPremium)
	IFF(v_GeneratedRecordIndicator = '1', v_OtherModifiedDirectWrittenPremium - v_prev_OtherModifiedDirectWrittenPremium, v_OtherModifiedDirectWrittenPremium) AS v_ChangeInOtherModifiedDirectWrittenPremium,
	-- *INF*: IIF(v_GeneratedRecordIndicator='1',v_RateableDirectWrittenPremium-v_prev_RateableDirectWrittenPremium,v_RateableDirectWrittenPremium)
	IFF(v_GeneratedRecordIndicator = '1', v_RateableDirectWrittenPremium - v_prev_RateableDirectWrittenPremium, v_RateableDirectWrittenPremium) AS v_ChangeInRateableDirectWrittenPremium,
	-- *INF*: DECODE(TRUE,
	-- v_GeneratedRecordIndicator='1' AND v_TrueRateableCount!=0,v_ClassifiedDirectWrittenPremium-v_prev_ClassifiedDirectWrittenPremium,
	-- v_GeneratedRecordIndicator='1' AND v_TrueRateableCount=0,v_DirectWrittenPremium,
	-- v_ClassifiedDirectWrittenPremium)
	DECODE(TRUE,
		v_GeneratedRecordIndicator = '1' AND v_TrueRateableCount != 0, v_ClassifiedDirectWrittenPremium - v_prev_ClassifiedDirectWrittenPremium,
		v_GeneratedRecordIndicator = '1' AND v_TrueRateableCount = 0, v_DirectWrittenPremium,
		v_ClassifiedDirectWrittenPremium) AS v_ChangeInClassifiedDirectWrittenPremium,
	PremiumMasterCalculationId AS v_prev_PremiumMasterCalculationId,
	v_SubjectDirectWrittenPremium AS v_prev_SubjectDirectWrittenPremium,
	v_ExperienceModifiedDirectWrittenPremium AS v_prev_ExperienceModifiedDirectWrittenPremium,
	v_ScheduleModifiedDirectWrittenPremium AS v_prev_ScheduleModifiedDirectWrittenPremium,
	v_OtherModifiedDirectWrittenPremium AS v_prev_OtherModifiedDirectWrittenPremium,
	v_RateableDirectWrittenPremium AS v_prev_RateableDirectWrittenPremium,
	v_ClassifiedDirectWrittenPremium AS v_prev_ClassifiedDirectWrittenPremium,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	v_GeneratedRecordIndicator AS GeneratedRecordIndicator,
	v_DirectWrittenPremium AS DirectWrittenPremium,
	v_ChangeInSubjectDirectWrittenPremium AS SubjectDirectWrittenPremium,
	v_ChangeInExperienceModifiedDirectWrittenPremium AS ExperienceModifiedDirectWrittenPremium,
	v_ChangeInScheduleModifiedDirectWrittenPremium AS ScheduleModifiedDirectWrittenPremium,
	v_ChangeInOtherModifiedDirectWrittenPremium AS OtherModifiedDirectWrittenPremium,
	v_ChangeInRateableDirectWrittenPremium AS RateableDirectWrittenPremium,
	v_ChangeInClassifiedDirectWrittenPremium AS ClassifiedDirectWrittenPremium,
	-- *INF*: --ADD_TO_DATE(trunc(sysdate,'MM'),'MM', -1-(@{pipeline().parameters.NO_OF_MONTHS}))
	-- 
	-- ADD_TO_DATE(trunc(sysdate,'MM'),'MM',@{pipeline().parameters.NO_OF_MONTHS})
	ADD_TO_DATE(trunc(sysdate, 'MM'), 'MM', @{pipeline().parameters.NO_OF_MONTHS}) AS v_FirstDayOfRunMonth,
	-- *INF*: IIF(
	-- TO_DATE(@{pipeline().parameters.SELECTION_START_TS},'MM/DD/YYYY HH24:MI:SS') < TO_DATE('1800-01-02' , 'YYYY-MM-DD'), TO_DATE('1800-01-01' , 'YYYY-MM-DD'),
	-- v_FirstDayOfRunMonth)
	IFF(TO_DATE(@{pipeline().parameters.SELECTION_START_TS}, 'MM/DD/YYYY HH24:MI:SS') < TO_DATE('1800-01-02', 'YYYY-MM-DD'), TO_DATE('1800-01-01', 'YYYY-MM-DD'), v_FirstDayOfRunMonth) AS v_StartDate,
	-- *INF*: ADD_TO_DATE(v_FirstDayOfRunMonth,'MM',1)
	ADD_TO_DATE(v_FirstDayOfRunMonth, 'MM', 1) AS V_EndDate,
	-- *INF*: DECODE(TRUE,
	-- i_PremiumMasterRunDate<v_StartDate,0,
	-- i_PremiumMasterRunDate>=V_EndDate,0,
	-- v_DirectWrittenPremium!=0,1,
	-- v_ChangeInSubjectDirectWrittenPremium!=0,1,
	-- v_ChangeInExperienceModifiedDirectWrittenPremium!=0,1,
	-- v_ChangeInScheduleModifiedDirectWrittenPremium!=0,1,
	-- v_ChangeInOtherModifiedDirectWrittenPremium!=0,1,
	-- v_ChangeInRateableDirectWrittenPremium!=0,1,
	-- v_ChangeInClassifiedDirectWrittenPremium!=0,1,
	-- 0)
	DECODE(TRUE,
		i_PremiumMasterRunDate < v_StartDate, 0,
		i_PremiumMasterRunDate >= V_EndDate, 0,
		v_DirectWrittenPremium != 0, 1,
		v_ChangeInSubjectDirectWrittenPremium != 0, 1,
		v_ChangeInExperienceModifiedDirectWrittenPremium != 0, 1,
		v_ChangeInScheduleModifiedDirectWrittenPremium != 0, 1,
		v_ChangeInOtherModifiedDirectWrittenPremium != 0, 1,
		v_ChangeInRateableDirectWrittenPremium != 0, 1,
		v_ChangeInClassifiedDirectWrittenPremium != 0, 1,
		0) AS FilterFlag
	FROM SQ_PremiumMasterCalculation
),
FIL_Records_ValidModifiedPremium AS (
	SELECT
	AuditId, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	PolicyAKId, 
	RatingState, 
	ClassCode, 
	RunDate, 
	PremiumMasterCalculationId, 
	GeneratedRecordIndicator, 
	DirectWrittenPremium, 
	SubjectDirectWrittenPremium, 
	ExperienceModifiedDirectWrittenPremium, 
	ScheduleModifiedDirectWrittenPremium, 
	OtherModifiedDirectWrittenPremium, 
	RateableDirectWrittenPremium, 
	ClassifiedDirectWrittenPremium, 
	FilterFlag
	FROM EXP_ModifiedPremium_Calculate
	WHERE FilterFlag=1
),
LKP_ModifiedPremiumWorkersCompensationCalculation AS (
	SELECT
	ModifiedPremiumWorkersCompensationCalculationId,
	RunDate,
	PremiumMasterCalculationId
	FROM (
		SELECT 
			ModifiedPremiumWorkersCompensationCalculationId,
			RunDate,
			PremiumMasterCalculationId
		FROM ModifiedPremiumWorkersCompensationCalculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationId,RunDate ORDER BY ModifiedPremiumWorkersCompensationCalculationId) = 1
),
EXP_TgtExists AS (
	SELECT
	FIL_Records_ValidModifiedPremium.AuditId,
	FIL_Records_ValidModifiedPremium.SourceSystemID,
	FIL_Records_ValidModifiedPremium.CreatedDate,
	FIL_Records_ValidModifiedPremium.ModifiedDate,
	FIL_Records_ValidModifiedPremium.PolicyAKId,
	FIL_Records_ValidModifiedPremium.RatingState,
	FIL_Records_ValidModifiedPremium.ClassCode,
	FIL_Records_ValidModifiedPremium.RunDate,
	FIL_Records_ValidModifiedPremium.PremiumMasterCalculationId,
	FIL_Records_ValidModifiedPremium.GeneratedRecordIndicator,
	FIL_Records_ValidModifiedPremium.DirectWrittenPremium,
	FIL_Records_ValidModifiedPremium.SubjectDirectWrittenPremium,
	FIL_Records_ValidModifiedPremium.ExperienceModifiedDirectWrittenPremium,
	FIL_Records_ValidModifiedPremium.ScheduleModifiedDirectWrittenPremium,
	FIL_Records_ValidModifiedPremium.OtherModifiedDirectWrittenPremium,
	FIL_Records_ValidModifiedPremium.RateableDirectWrittenPremium,
	FIL_Records_ValidModifiedPremium.ClassifiedDirectWrittenPremium,
	LKP_ModifiedPremiumWorkersCompensationCalculation.ModifiedPremiumWorkersCompensationCalculationId AS lkp_ModifiedPremiumWorkersCompensationCalculationId,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_ModifiedPremiumWorkersCompensationCalculationId),'NEW',
	-- 'NOCHANGE')
	DECODE(TRUE,
		lkp_ModifiedPremiumWorkersCompensationCalculationId IS NULL, 'NEW',
		'NOCHANGE') AS o_ChangeFlag
	FROM FIL_Records_ValidModifiedPremium
	LEFT JOIN LKP_ModifiedPremiumWorkersCompensationCalculation
	ON LKP_ModifiedPremiumWorkersCompensationCalculation.PremiumMasterCalculationId = FIL_Records_ValidModifiedPremium.PremiumMasterCalculationId AND LKP_ModifiedPremiumWorkersCompensationCalculation.RunDate = FIL_Records_ValidModifiedPremium.RunDate
),
RTR_Insert_Update AS (
	SELECT
	AuditId,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	PolicyAKId,
	RatingState,
	ClassCode,
	RunDate,
	PremiumMasterCalculationId,
	GeneratedRecordIndicator,
	DirectWrittenPremium,
	SubjectDirectWrittenPremium,
	ExperienceModifiedDirectWrittenPremium,
	ScheduleModifiedDirectWrittenPremium,
	OtherModifiedDirectWrittenPremium,
	RateableDirectWrittenPremium,
	ClassifiedDirectWrittenPremium,
	o_ChangeFlag
	FROM EXP_TgtExists
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE o_ChangeFlag='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE o_ChangeFlag='UPDATE'),
ModifiedPremiumWorkersCompensationCalculation_Insert AS (
	INSERT INTO ModifiedPremiumWorkersCompensationCalculation
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, RunDate, PremiumMasterCalculationId, GeneratedRecordIndicator, PolicyAKId, RatingState, ClassCode, DirectWrittenPremium, SubjectDirectWrittenPremium, ExperienceModifiedDirectWrittenPremium, ScheduleModifiedDirectWrittenPremium, OtherModifiedDirectWrittenPremium, RatableDirectWrittenPremium, ClassifiedDirectWrittenPremium)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RUNDATE, 
	PREMIUMMASTERCALCULATIONID, 
	GENERATEDRECORDINDICATOR, 
	POLICYAKID, 
	RATINGSTATE, 
	CLASSCODE, 
	DIRECTWRITTENPREMIUM, 
	SUBJECTDIRECTWRITTENPREMIUM, 
	EXPERIENCEMODIFIEDDIRECTWRITTENPREMIUM, 
	SCHEDULEMODIFIEDDIRECTWRITTENPREMIUM, 
	OTHERMODIFIEDDIRECTWRITTENPREMIUM, 
	RateableDirectWrittenPremium AS RATABLEDIRECTWRITTENPREMIUM, 
	CLASSIFIEDDIRECTWRITTENPREMIUM
	FROM RTR_Insert_Update_INSERT
),