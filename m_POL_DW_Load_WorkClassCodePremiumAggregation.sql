WITH
SQ_PremiumTransaction AS (
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
	SELECT
	  *
	FROM (SELECT
	  a.SourceSystemId,
	  d.PolicyAKID,
	  d.StateProvinceCode AS RatingState,
	  LEFT(b.ClassCode, 4) AS ClassCode,
	  CAST(FLOOR(CAST(CASE
	    WHEN a.PremiumMasterCoverageEffectiveDate >= a.PremiumTransactionEnteredDate THEN a.PremiumMasterCoverageEffectiveDate
	    ELSE a.PremiumTransactionEnteredDate
	  END AS float)) AS datetime) AS RunDate,
	  SUM(a.PremiumMasterPremium) AS DirectWrittenPremium,
	  ----------------------------------------------------------
	  SUM(CASE
	    WHEN f.subject_to_exprnc_modfctn_class_ind = 'Y' THEN a.PremiumMasterPremium --Subject To Experience Modification Class Indicator
	    ELSE 0
	  END) AS SubjectDirectWrittenPremium,
	  ----------------------------------------------------------
	  SUM(CASE
	    WHEN f.exprnc_modfctn_class_ind = 'Y' THEN a.PremiumMasterPremium --Experience Modification Class Indicator
	    ELSE 0
	  END) AS ExperienceModifiedAdjustmentAmount,
	  ----------------------------------------------------------
	  SUM(CASE
	    WHEN f.sched_modfctn_class_ind = 'Y' THEN a.PremiumMasterPremium
	    ELSE 0
	  END) AS ScheduleModifiedAdjustmentAmount,
	  ----------------------------------------------------------
	  SUM(CASE
	    --WHEN LEFT(b.ClassCode, 4) = '9046' THEN a.PremiumMasterPremium
		WHEN f.other_modfctn_class_ind='Y' THEN a.PremiumMasterPremium  --Other Modification Class Indicator
	    ELSE 0
	  END) AS OtherModifiedAdjustmentAmount,
	  ----------------------------------------------------------
	  SUM(CASE
	    WHEN f.ratable_class_ind = 'N' AND								--Ratable Class Indicator
	      f.subject_to_exprnc_modfctn_class_ind = 'N' AND				--Subject To Experience Modification Class Indicator
	      f.exprnc_modfctn_class_ind = 'N' AND							--Experience Modification Class Indicator
	      f.sched_modfctn_class_ind = 'N' AND							--Scheduled Modification Class Indicator
	      --LEFT(b.ClassCode, 4) <> '9046' AND							
		  f.other_modfctn_class_ind='N' AND								--Other Modification Class Indicator
	      f.surchg_class_ind = 'N' THEN a.PremiumMasterPremium			--Surcharge Class Indicator
	    ELSE 0
	  END) AS ClassifiedAdjustmentAmount,
	  ----------------------------------------------------------
	  SUM(CASE
	    WHEN f.subject_to_exprnc_modfctn_class_ind = 'Y' THEN 1
	    ELSE 0
	  END) AS SubjectTransactionCount,
	  ----------------------------------------------------------
	  SUM(CASE
	    WHEN f.ratable_class_ind = 'Y' THEN 1
	    ELSE 0
	  END) AS RateableTransactionCount
	  ----------------------------------------------------------
	FROM PremiumMasterCalculation a
	JOIN StatisticalCoverage b
	  ON a.StatisticalCoverageAKID = b.StatisticalCoverageAKID
	JOIN PolicyCoverage c
	  ON c.PolicyCoverageAKID = a.PolicyCoverageAKID
	  AND c.TypeBureauCode in ('WC','WP') 
	  AND c.SourceSystemID = 'PMS'
	JOIN RiskLocation d
	  ON d.RiskLocationAKID = a.RiskLocationAKID
	  AND d.SourceSystemID = 'PMS'
	JOIN --master_classification_sup 
		f
	  ON f.class_code = LEFT(b.ClassCode, 4) and f.RatingStateCode=d.StateProvinceCode
	WHERE a.CurrentSnapshotFlag = 1
	AND a.PremiumMasterPremiumType = 'D'
	AND a.SourceSystemId = 'PMS'
	AND a.PremiumMasterReasonAmendedCode NOT IN ('COL', 'CWO', 'Claw Back' ,'CWB')
	-----------------------
	
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
	GROUP BY d.PolicyAKID,
	         d.StateProvinceCode,
	         LEFT(b.ClassCode, 4),
	         CAST(FLOOR(CAST(CASE
	           WHEN a.PremiumMasterCoverageEffectiveDate >= a.PremiumTransactionEnteredDate THEN a.PremiumMasterCoverageEffectiveDate
	           ELSE a.PremiumTransactionEnteredDate
	         END AS float)) AS datetime),
	         a.SourceSystemId
	UNION ALL
	SELECT
	  a.SourceSystemId,
	  d.PolicyAKID,
	  d.StateProvinceCode AS RatingState,
	  LEFT(b.ClassCode, 4) AS ClassCode,
	  CAST(FLOOR(CAST(CASE
	    WHEN a.PremiumMasterCoverageEffectiveDate >= a.PremiumTransactionEnteredDate THEN a.PremiumMasterCoverageEffectiveDate
	    ELSE a.PremiumTransactionEnteredDate
	  END AS float)) AS datetime) AS RunDate,
	  SUM(a.PremiumMasterPremium) AS DirectWrittenPremium,
	  SUM(CASE
	    WHEN f.subject_to_exprnc_modfctn_class_ind = 'Y' THEN a.PremiumMasterPremium
	    ELSE 0
	  END) AS SubjectDirectWrittenPremium,
	  SUM(CASE
	    WHEN f.exprnc_modfctn_class_ind = 'Y' THEN a.PremiumMasterPremium
	    ELSE 0
	  END) AS ExperienceModifiedAdjustmentAmount,
	  SUM(CASE
	    WHEN f.sched_modfctn_class_ind = 'Y' THEN a.PremiumMasterPremium
	    ELSE 0
	  END) AS ScheduleModifiedAdjustmentAmount,
	  SUM(CASE
	    --WHEN LEFT(b.ClassCode, 4) = '9046' THEN a.PremiumMasterPremium
		WHEN f.other_modfctn_class_ind='Y' THEN a.PremiumMasterPremium  --Other Modification Class Indicator
	    ELSE 0
	  END) AS OtherModifiedAdjustmentAmount,
	  SUM(CASE
	    WHEN f.ratable_class_ind = 'N' AND
	      f.subject_to_exprnc_modfctn_class_ind = 'N' AND
	      f.exprnc_modfctn_class_ind = 'N' AND
	      f.sched_modfctn_class_ind = 'N' AND
	      --LEFT(b.ClassCode, 4) <> '9046' AND							
		  f.other_modfctn_class_ind='N' AND								--Other Modification Class Indicator
	      f.surchg_class_ind = 'N' THEN a.PremiumMasterPremium
	    ELSE 0
	  END) AS ClassifiedAdjustmentAmount,
	  SUM(CASE
	    WHEN f.subject_to_exprnc_modfctn_class_ind = 'Y' THEN 1
	    ELSE 0
	  END) AS SubjectTransactionCount,
	  SUM(CASE
	    WHEN f.ratable_class_ind = 'Y' THEN 1
	    ELSE 0
	  END) AS RateableTransactionCount
	FROM PremiumMasterCalculation a
	JOIN RatingCoverage b
	  ON a.RatingCoverageAKId = b.RatingCoverageAKID
	  AND b.EffectiveDate = a.RatingCoverageEffectiveDate
	JOIN PolicyCoverage c
	  ON c.PolicyCoverageAKID = b.PolicyCoverageAKID
	  AND c.CurrentSnapshotFlag = 1
	  AND c.InsuranceLine = 'WorkersCompensation'
	JOIN RiskLocation d
	  ON d.RiskLocationAKID = c.RiskLocationAKID
	  AND d.CurrentSnapshotFlag = 1
	JOIN --master_classification_sup 
		f
	  ON f.class_code = LEFT(b.ClassCode, 4) and f.RatingStateCode=d.StateProvinceCode
	WHERE a.CurrentSnapshotFlag = 1
	AND a.PremiumMasterPremiumType = 'D'
	AND a.PremiumMasterReasonAmendedCode NOT IN ('COL', 'CWO', 'Claw Back' ,'CWB')
	AND a.SourceSystemId = 'DCT'
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
	GROUP BY d.PolicyAKID,
	         d.StateProvinceCode,
	         LEFT(b.ClassCode, 4),
	         CAST(FLOOR(CAST(CASE
	           WHEN a.PremiumMasterCoverageEffectiveDate >= a.PremiumTransactionEnteredDate THEN a.PremiumMasterCoverageEffectiveDate
	           ELSE a.PremiumTransactionEnteredDate
	         END AS float)) AS datetime),
	         a.SourceSystemId) a
	ORDER BY PolicyAKId, RatingState, ClassCode, RunDate
),
EXP_ClassCode AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	PolicyAKId,
	RatingState,
	ClassCode,
	RunDate,
	-- *INF*: ADD_TO_DATE(RunDate,'SS',86399)
	ADD_TO_DATE(RunDate, 'SS', 86399) AS o_RunDate,
	DirectWrittenPremium,
	SubjectDirectWrittenPremium,
	ExperienceModifiedAdjustmentAmount,
	ScheduleModifiedAdjustmentAmount,
	OtherModifiedAdjustmentAmount,
	ClassifiedAdjustmentAmount,
	SubjectTransactionCount,
	RateableTransactionCount
	FROM SQ_PremiumTransaction
),
WorkClassCodePremiumAggregation AS (
	TRUNCATE TABLE WorkClassCodePremiumAggregation;
	INSERT INTO WorkClassCodePremiumAggregation
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, RunDate, PolicyAKId, RatingState, ClassCode, DirectWrittenPremium, SubjectDirectWrittenPremium, ExperienceModifiedAdjustmentAmount, ScheduleModifiedAdjustmentAmount, OtherModifiedAdjustmentAmount, ClassifiedAdjustmentAmount, SubjectTransactionCount, RateableTransactionCount)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	o_RunDate AS RUNDATE, 
	POLICYAKID, 
	RATINGSTATE, 
	CLASSCODE, 
	DIRECTWRITTENPREMIUM, 
	SUBJECTDIRECTWRITTENPREMIUM, 
	EXPERIENCEMODIFIEDADJUSTMENTAMOUNT, 
	SCHEDULEMODIFIEDADJUSTMENTAMOUNT, 
	OTHERMODIFIEDADJUSTMENTAMOUNT, 
	CLASSIFIEDADJUSTMENTAMOUNT, 
	SUBJECTTRANSACTIONCOUNT, 
	RATEABLETRANSACTIONCOUNT
	FROM EXP_ClassCode
),