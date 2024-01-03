WITH
SQ_WorkClassCodePremiumAggregation AS (
	SELECT PolicyAKId,
	RatingState,
	RunDate,
	SUM(SubjectDirectWrittenPremium) AS SubjectDirectWrittenPremium,
	SUM(ExperienceModifiedAdjustmentAmount) AS ExperienceModifiedAdjustmentAmount,
	SUM(ScheduleModifiedAdjustmentAmount) AS ScheduleModifiedAdjustmentAmount,
	SUM(OtherModifiedAdjustmentAmount) AS OtherModifiedAdjustmentAmount,
	SUM(ClassifiedAdjustmentAmount) AS ClassifiedAdjustmentAmount,
	SUM(SubjectTransactionCount) AS SubjectTransactionCount,
	SUM(RateableTransactionCount) AS RateableTransactionCount,
	SourceSystemId
	FROM
	 WorkClassCodePremiumAggregation
	group by PolicyAKId, RatingState, RunDate,SourceSystemId
	ORDER BY PolicyAKId, RatingState, RunDate
),
EXPTRANS AS (
	SELECT
	PolicyAKId,
	RatingState,
	RunDate,
	SubjectDirectWrittenPremium AS i_SubjectDirectWrittenPremium,
	ExperienceModifiedAdjustmentAmount AS i_ExperienceModifiedAdjustmentAmount,
	ScheduleModifiedAdjustmentAmount AS i_ScheduleModifiedAdjustmentAmount,
	OtherModifiedAdjustmentAmount AS i_OtherModifiedAdjustmentAmount,
	ClassifiedAdjustmentAmount AS i_ClassifiedAdjustmentAmount,
	SubjectTransactionCount AS i_SubjectTransactionCount,
	RateableTransactionCount AS i_RateableTransactionCount,
	i_SubjectDirectWrittenPremium AS v_SubjectDirectWrittenPremium,
	v_SubjectDirectWrittenPremium+i_ExperienceModifiedAdjustmentAmount AS v_ExperienceModifiedDirectWrittenPremium,
	v_ExperienceModifiedDirectWrittenPremium+i_ScheduleModifiedAdjustmentAmount AS v_ScheduleModifiedDirectWrittenPremium,
	v_ScheduleModifiedDirectWrittenPremium+i_OtherModifiedAdjustmentAmount AS v_OtherModifiedDirectWrittenPremium,
	-- *INF*: IIF(PolicyAKId=v_prev_PolicyAKId AND RatingState=v_prev_RatingState,1,0)
	IFF(PolicyAKId = v_prev_PolicyAKId AND RatingState = v_prev_RatingState, 1, 0) AS v_SameGroupFlag,
	-- *INF*: IIF(v_SameGroupFlag=1,v_SubjectDirectWrittenPremium+v_Total_SubjectDirectWrittenPremium,v_SubjectDirectWrittenPremium)
	IFF(v_SameGroupFlag = 1, v_SubjectDirectWrittenPremium + v_Total_SubjectDirectWrittenPremium, v_SubjectDirectWrittenPremium) AS v_Total_SubjectDirectWrittenPremium,
	-- *INF*: IIF(v_SameGroupFlag=1,v_ExperienceModifiedDirectWrittenPremium+v_Total_ExperienceModifiedDirectWrittenPremium,v_ExperienceModifiedDirectWrittenPremium)
	IFF(v_SameGroupFlag = 1, v_ExperienceModifiedDirectWrittenPremium + v_Total_ExperienceModifiedDirectWrittenPremium, v_ExperienceModifiedDirectWrittenPremium) AS v_Total_ExperienceModifiedDirectWrittenPremium,
	-- *INF*: IIF(v_SameGroupFlag=1,v_ScheduleModifiedDirectWrittenPremium+v_Total_ScheduleModifiedDirectWrittenPremium,v_ScheduleModifiedDirectWrittenPremium)
	IFF(v_SameGroupFlag = 1, v_ScheduleModifiedDirectWrittenPremium + v_Total_ScheduleModifiedDirectWrittenPremium, v_ScheduleModifiedDirectWrittenPremium) AS v_Total_ScheduleModifiedDirectWrittenPremium,
	-- *INF*: IIF(v_SameGroupFlag=1,v_OtherModifiedDirectWrittenPremium+v_Total_OtherModifiedDirectWrittenPremium,v_OtherModifiedDirectWrittenPremium)
	IFF(v_SameGroupFlag = 1, v_OtherModifiedDirectWrittenPremium + v_Total_OtherModifiedDirectWrittenPremium, v_OtherModifiedDirectWrittenPremium) AS v_Total_OtherModifiedDirectWrittenPremium,
	-- *INF*: IIF(v_SameGroupFlag=1,i_ClassifiedAdjustmentAmount+v_Total_ClassifiedAdjustmentAmount,i_ClassifiedAdjustmentAmount)
	IFF(v_SameGroupFlag = 1, i_ClassifiedAdjustmentAmount + v_Total_ClassifiedAdjustmentAmount, i_ClassifiedAdjustmentAmount) AS v_Total_ClassifiedAdjustmentAmount,
	-- *INF*: IIF(v_SameGroupFlag=1,i_SubjectTransactionCount+v_Total_SubjectTransactionCount,i_SubjectTransactionCount)
	IFF(v_SameGroupFlag = 1, i_SubjectTransactionCount + v_Total_SubjectTransactionCount, i_SubjectTransactionCount) AS v_Total_SubjectTransactionCount,
	-- *INF*: IIF(v_SameGroupFlag=1,i_RateableTransactionCount+v_Total_RateableTransactionCount,i_RateableTransactionCount)
	IFF(v_SameGroupFlag = 1, i_RateableTransactionCount + v_Total_RateableTransactionCount, i_RateableTransactionCount) AS v_Total_RateableTransactionCount,
	PolicyAKId AS v_prev_PolicyAKId,
	RatingState AS v_prev_RatingState,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	v_Total_SubjectDirectWrittenPremium AS SubjectDirectWrittenPremium,
	v_Total_ExperienceModifiedDirectWrittenPremium AS ExperienceModifiedDirectWrittenPremium,
	v_Total_ScheduleModifiedDirectWrittenPremium AS ScheduleModifiedDirectWrittenPremium,
	v_Total_OtherModifiedDirectWrittenPremium AS OtherModifiedDirectWrittenPremium,
	v_Total_ClassifiedAdjustmentAmount AS ClassifiedAdjustmentAmount,
	v_Total_SubjectTransactionCount AS SubjectTransactionCount,
	v_Total_RateableTransactionCount AS RateableTransactionCount
	FROM SQ_WorkClassCodePremiumAggregation
),
WorkRatingStatePremiumAggregation AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkRatingStatePremiumAggregation;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkRatingStatePremiumAggregation
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, RunDate, PolicyAKId, RatingState, SubjectDirectWrittenPremium, ExperienceModifiedDirectWrittenPremium, ScheduleModifiedDirectWrittenPremium, OtherModifiedDirectWrittenPremium, ClassifiedAdjustmentAmount, SubjectTransactionCount, RateableTransactionCount)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RUNDATE, 
	POLICYAKID, 
	RATINGSTATE, 
	SUBJECTDIRECTWRITTENPREMIUM, 
	EXPERIENCEMODIFIEDDIRECTWRITTENPREMIUM, 
	SCHEDULEMODIFIEDDIRECTWRITTENPREMIUM, 
	OTHERMODIFIEDDIRECTWRITTENPREMIUM, 
	CLASSIFIEDADJUSTMENTAMOUNT, 
	SUBJECTTRANSACTIONCOUNT, 
	RATEABLETRANSACTIONCOUNT
	FROM EXPTRANS
),