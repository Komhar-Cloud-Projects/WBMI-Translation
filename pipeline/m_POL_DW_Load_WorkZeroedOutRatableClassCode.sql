WITH
SQ_WorkClassCodePremiumAggregation AS (
	with c as 
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
	select a.SourceSystemId,
	a.PolicyAKId,
	a.RatingState,
	a.ClassCode,
	a.RunDate,
	SUM(a.TransactionCount) over (partition by
	a.PolicyAKId,a.RatingState,a.RunDate) as TransactionCount from
	(select a.*,MAX(ABS(DirectWrittenPremium)) over (partition by
	a.PolicyAKId,a.RatingState,a.RunDate) MaxPremium from
	(select a.SourceSystemId,
	a.PolicyAKId,
	a.RatingState,
	a.ClassCode,
	b.RunDate,
	sum(a.DirectWrittenPremium) DirectWrittenPremium,
	sum(a.RateableTransactionCount) TransactionCount
	from WorkClassCodePremiumAggregation a
	join --master_classification_sup 
	c
	on a.ClassCode=c.class_code and c.RatingStateCode=a.RatingState --add ratingstate as join condition 
	and c.ratable_class_ind='Y'
	join WorkRatingStatePremiumAggregation b
	on a.RunDate<=b.RunDate
	and a.PolicyAKId=b.PolicyAKId
	and a.RatingState=b.RatingState
	group by a.PolicyAKId,a.RatingState,a.ClassCode,b.RunDate,a.SourceSystemId) a
	) a where a.MaxPremium<>0
	and a.DirectWrittenPremium=0
),
EXPTRANS AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	PolicyAKId,
	RatingState,
	ClassCode,
	RunDate,
	TransactionCount
	FROM SQ_WorkClassCodePremiumAggregation
),
WorkZeroedOutRatableClassCode AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkZeroedOutRatableClassCode;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkZeroedOutRatableClassCode
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, RunDate, PolicyAKId, RatingState, ClassCode, TransactionCount)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RUNDATE, 
	POLICYAKID, 
	RATINGSTATE, 
	CLASSCODE, 
	TRANSACTIONCOUNT
	FROM EXPTRANS
),