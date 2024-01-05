WITH
LKP_WorkEarnedPremiumCoverageMonthly AS (
	SELECT
	StatisticalCoverageAKID,
	RunDate
	FROM (
		Declare @MONTHEND as Datetime
		
		Set @MONTHEND= @{pipeline().parameters.MONTHEND}
		
		SELECT w.StatisticalCoverageAKID as StatisticalCoverageAKID,
		w.RunDate as RunDate
		FROM WorkEarnedPremiumCoverageMonthly w
		join (
		select distinct left(WorkRatingModifierKey,12) PolicyKey
		from WorkRatingModifier
		where ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		and RunDate<@MONTHEND
		and SourceSystemId='PMS'
		) b
		on w.PolicyKey=b.PolicyKey
		order by w.StatisticalCoverageAKID,w.RunDate
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageAKID,RunDate ORDER BY StatisticalCoverageAKID) = 1
),
LKP_RatingCoverage_Cancellation AS (
	SELECT
	WorkRatingModifierAKId,
	RatingCoverageAKId,
	EffectiveDate,
	ExpirationDate
	FROM (
		Declare @MONTHEND as Datetime
		
		Set @MONTHEND= @{pipeline().parameters.MONTHEND}
		
		
		select a.RatingCoverageAKId as RatingCoverageAKId,
		a.WorkRatingModifierAKId as WorkRatingModifierAKId,
		a.EffectiveDate as EffectiveDate,
		dateadd(ss,-1,ISNULL(lead(a.EffectiveDate,1) over (partition by a.RatingCoverageAKId,a.WorkRatingModifierAKId order by EffectiveDate),'2101-1-1 00:00:00'))
		as ExpirationDate
		from (
		select distinct rc.RatingCoverageAKId as RatingCoverageAKId,
		a.WorkRatingModifierAKId as WorkRatingModifierAKId,
		dateadd(ss,86399,cast(floor(cast(case when pt.PremiumTransactionEffectiveDate>=pt.PremiumTransactionEnteredDate
		then pt.PremiumTransactionEffectiveDate else pt.PremiumTransactionEnteredDate end as float)) as datetime)) as EffectiveDate
		from RatingCoverage rc
		inner hash join PremiumTransaction pt
		on rc.RatingCoverageAKID=pt.RatingCoverageAKId
		and rc.EffectiveDate=pt.EffectiveDate
		and not pt.OffsetOnsetCode in ('Offset','Deprecated')
		and not pt.ReasonAmendedCode in ('COL','CWO','Claw Back')
		and pt.SourceSystemID='DCT'
		inner hash join WorkPremiumTransactionRatingModifierBridge a
		on a.PremiumTransactionAKId=pt.PremiumTransactionAKID
		and a.SourceSystemID='DCT'
		and a.WorkRatingModifierAKId<>-1
		inner hash join (
		select distinct WorkRatingModifierAKId
		from WorkRatingModifier a 
		where ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		and RunDate<@MONTHEND
		and SourceSystemId='DCT'
		) b
		on a.WorkRatingModifierAKId=b.WorkRatingModifierAKId
		where rc.RatingCoverageCancellationDate<'2100-12-31') a
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingCoverageAKId,WorkRatingModifierAKId,EffectiveDate,ExpirationDate ORDER BY WorkRatingModifierAKId) = 1
),
SQ_Natural AS (
	Declare @MONTHEND as Datetime
	
	Set @MONTHEND=  @{pipeline().parameters.MONTHEND}
	
	select c.PremiumMasterCalculationID,
	c.PremiumMasterPremium as PremiumTransactionAmount,
	a.RunDate,
	case when c.PremiumMasterCoverageEffectiveDate>=c.PremiumTransactionEnteredDate then c.PremiumMasterCoverageEffectiveDate else c.PremiumTransactionEnteredDate end as NewRunDate,
	a.OtherModifiedfactor,
	a.ScheduleModifiedfactor,
	a.ExperienceModifiedfactor,
	a.SourceSystemId
	from WorkRatingModifier a
	join WorkPremiumTransactionRatingModifierBridge b
	on a.WorkRatingModifierAKId=b.WorkRatingModifierAKId
	and b.RunDate=a.RunDate
	join PremiumMasterCalculation c
	on c.PremiumMasterCalculationID=b.PremiumMasterCalculationID
	and b.PremiumMasterCalculationID<>-1
	and c.CurrentSnapshotFlag=1
	and c.PremiumMasterPremiumType='D'
	and c.PremiumMasterPremium<>0
	and c.PremiumMasterReasonAmendedCode not in ('COL','CWO','Claw Back' ,'CWB')
	where b.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	and c.PremiumMasterRunDate<@MONTHEND
	@{pipeline().parameters.WHERE_CLAUSE}
	
	union all
	select c.PremiumMasterCalculationID,
	c.PremiumMasterPremium as PremiumTransactionAmount,
	a.RunDate,
	case when c.PremiumMasterCoverageEffectiveDate>=c.PremiumTransactionEnteredDate then c.PremiumMasterCoverageEffectiveDate else c.PremiumTransactionEnteredDate end as NewRunDate,
	a.OtherModifiedfactor,
	a.ScheduleModifiedfactor,
	a.ExperienceModifiedfactor,
	a.SourceSystemId
	from WorkRatingModifier a
	join WorkPremiumTransactionRatingModifierBridge b
	on a.WorkRatingModifierAKId=b.WorkRatingModifierAKId
	and b.RunDate=a.RunDate
	join PremiumMasterCalculation c
	on b.PremiumMasterCalculationID=-1
	and c.PremiumTransactionAKID = b.PremiumTransactionAKID
	and c.CurrentSnapshotFlag=1
	and c.PremiumMasterPremiumType='D'
	and c.PremiumMasterPremium<>0
	and c.PremiumMasterReasonAmendedCode not in ('COL','CWO','Claw Back' ,'CWB')
	where --convert(varchar(6),b.ModifiedDate,112)>= convert(varchar(6), cast ('@{pipeline().parameters.SELECTION_START_TS}'  as date ),112) 
	b.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	--and a.RunDate<@MONTHEND
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Natural AS (
	SELECT
	PremiumMasterCalculationID,
	PremiumTransactionAmount,
	RunDate,
	NewRunDate,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	-- *INF*: ADD_TO_DATE(TRUNC(NewRunDate,'DD'),'SS',86399)
	ADD_TO_DATE(TRUNC(NewRunDate, 'DD'), 'SS', 86399) AS v_NewRunDate,
	SourceSystemID,
	-- *INF*: GREATEST(RunDate,v_NewRunDate)
	GREATEST(RunDate, v_NewRunDate) AS o_RunDate,
	'0' AS GeneratedRecordIndicator,
	PremiumTransactionAmount AS OtherModifiedPremium,
	-- *INF*: ROUND(PremiumTransactionAmount/OtherModifiedFactor,4)
	ROUND(PremiumTransactionAmount / OtherModifiedFactor, 4) AS ScheduleModifiedPremium,
	-- *INF*: ROUND(PremiumTransactionAmount/OtherModifiedFactor/ScheduleModifiedFactor,4)
	ROUND(PremiumTransactionAmount / OtherModifiedFactor / ScheduleModifiedFactor, 4) AS ExperienceModifiedPremium,
	-- *INF*: ROUND(PremiumTransactionAmount/OtherModifiedFactor/ScheduleModifiedFactor/ExperienceModifiedFactor,4)
	ROUND(PremiumTransactionAmount / OtherModifiedFactor / ScheduleModifiedFactor / ExperienceModifiedFactor, 4) AS SubjectWrittenPremium
	FROM SQ_Natural
),
SQ_Generated AS (
	Declare @MONTHEND as Datetime
	
	Set @MONTHEND= @{pipeline().parameters.MONTHEND}
	
	
	select c.PremiumMasterCalculationID,
	c.SourceSystemID,
	c.StatisticalCoverageAKId,
	c.PremiumMasterPremium as PremiumTransactionAmount,
	c.RatingCoverageAKId,
	b.WorkRatingModifierAKId,
	a.RunDate,
	a.OtherModifiedfactor,
	a.ScheduleModifiedfactor,
	a.ExperienceModifiedfactor,
	a.PreviousOtherModifiedfactor,
	a.PreviousScheduleModifiedfactor,
	a.PreviousExperienceModifiedfactor
	from (select a.WorkRatingModifierAKId,
	a.RunDate,
	a.OtherModifiedfactor,
	a.ScheduleModifiedfactor,
	a.ExperienceModifiedfactor,
	LAG(a.OtherModifiedfactor,1) over (partition by a.WorkRatingModifierAKId order by a.RunDate) PreviousOtherModifiedfactor,
	LAG(a.ScheduleModifiedfactor,1) over (partition by a.WorkRatingModifierAKId order by a.RunDate) PreviousScheduleModifiedfactor,
	LAG(a.ExperienceModifiedfactor,1) over (partition by a.WorkRatingModifierAKId order by a.RunDate) PreviousExperienceModifiedfactor
	from WorkRatingModifier a
	inner hash join (
	select distinct WorkRatingModifierAKId
	from WorkRatingModifier a 
	where --convert(varchar(6),ModifiedDate,112)>=convert(varchar(6), cast( '@{pipeline().parameters.SELECTION_START_TS}'  as date ),112)
	ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	and RunDate<@MONTHEND
	) b
	on a.WorkRatingModifierAKId=b.WorkRatingModifierAKId
	where a.WorkRatingModifierAKId<>-1
	and a.RunDate<@MONTHEND) a 
	inner hash join WorkPremiumTransactionRatingModifierBridge b
	on a.WorkRatingModifierAKId=b.WorkRatingModifierAKId
	and b.RunDate<a.RunDate
	and b.WorkRatingModifierAKId<>-1
	inner hash join PremiumMasterCalculation c
	on c.PremiumMasterCalculationID=b.PremiumMasterCalculationID
	and b.PremiumMasterCalculationID<>-1
	and c.CurrentSnapshotFlag=1
	and c.PremiumMasterPremiumType='D'
	and c.PremiumMasterPremium<>0
	and c.PremiumMasterReasonAmendedCode not in ('COL','CWO','Claw Back' ,'CWB')
	and c.PremiumMasterCoverageExpirationDate>a.RunDate
	where a.OtherModifiedfactor<>a.PreviousOtherModifiedfactor
	and a.ScheduleModifiedfactor<>a.PreviousScheduleModifiedfactor
	and a.ExperienceModifiedfactor<>a.PreviousExperienceModifiedfactor
	
	union all
	select c.PremiumMasterCalculationID,
	c.SourceSystemID,
	c.StatisticalCoverageAKId,
	c.PremiumMasterPremium as PremiumTransactionAmount,
	c.RatingCoverageAKId,
	b.WorkRatingModifierAKId,
	a.RunDate,
	a.OtherModifiedfactor,
	a.ScheduleModifiedfactor,
	a.ExperienceModifiedfactor,
	a.PreviousOtherModifiedfactor,
	a.PreviousScheduleModifiedfactor,
	a.PreviousExperienceModifiedfactor
	from (select a.WorkRatingModifierAKId,
	a.RunDate,
	a.OtherModifiedfactor,
	a.ScheduleModifiedfactor,
	a.ExperienceModifiedfactor,
	LAG(a.OtherModifiedfactor,1) over (partition by a.WorkRatingModifierAKId order by a.RunDate) PreviousOtherModifiedfactor,
	LAG(a.ScheduleModifiedfactor,1) over (partition by a.WorkRatingModifierAKId order by a.RunDate) PreviousScheduleModifiedfactor,
	LAG(a.ExperienceModifiedfactor,1) over (partition by a.WorkRatingModifierAKId order by a.RunDate) PreviousExperienceModifiedfactor
	from WorkRatingModifier a
	inner hash join (
	select distinct WorkRatingModifierAKId
	from WorkRatingModifier a 
	where --convert(varchar(6),ModifiedDate,112)>=convert(varchar(6), cast(  '@{pipeline().parameters.SELECTION_START_TS}' as date ),112)
	ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	and RunDate<@MONTHEND
	) b
	on a.WorkRatingModifierAKId=b.WorkRatingModifierAKId
	where a.WorkRatingModifierAKId<>-1
	and a.RunDate<@MONTHEND) a 
	inner hash join WorkPremiumTransactionRatingModifierBridge b
	on a.WorkRatingModifierAKId=b.WorkRatingModifierAKId
	and b.RunDate<a.RunDate
	and b.WorkRatingModifierAKId<>-1
	inner hash join PremiumMasterCalculation c
	on b.PremiumMasterCalculationID=-1
	and c.PremiumTransactionAKID = b.PremiumTransactionAKID
	and c.CurrentSnapshotFlag=1
	and c.PremiumMasterPremiumType='D'
	and c.PremiumMasterPremium<>0
	and c.PremiumMasterReasonAmendedCode not in ('COL','CWO','Claw Back' ,'CWB')
	and c.PremiumMasterCoverageExpirationDate>a.RunDate
	where a.OtherModifiedfactor<>a.PreviousOtherModifiedfactor
	and a.ScheduleModifiedfactor<>a.PreviousScheduleModifiedfactor
	and a.ExperienceModifiedfactor<>a.PreviousExperienceModifiedfactor
),
EXP_Cancelled AS (
	SELECT
	PremiumMasterCalculationID,
	SourceSystemID,
	StatisticalCoverageAKId,
	PremiumTransactionAmount,
	RatingCoverageAKId,
	WorkRatingModifierAKId,
	RunDate,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	PreviousOtherModifiedFactor,
	PreviousScheduleModifiedFactor,
	PreviousExperienceModifiedFactor,
	-- *INF*: ADD_TO_DATE(LAST_DAY(RunDate),'SS',86399)
	ADD_TO_DATE(LAST_DAY(RunDate), 'SS', 86399) AS v_RunMonth,
	-- *INF*: DECODE(SourceSystemID,
	-- 'DCT',:LKP.LKP_RATINGCOVERAGE_CANCELLATION(RatingCoverageAKId,WorkRatingModifierAKId,RunDate),
	-- 'PMS',:LKP.LKP_WORKEARNEDPREMIUMCOVERAGEMONTHLY(StatisticalCoverageAKId,v_RunMonth)
	-- -1)
	DECODE(SourceSystemID,
	'DCT', LKP_RATINGCOVERAGE_CANCELLATION_RatingCoverageAKId_WorkRatingModifierAKId_RunDate.WorkRatingModifierAKId,
	'PMS', LKP_WORKEARNEDPREMIUMCOVERAGEMONTHLY_StatisticalCoverageAKId_v_RunMonth.StatisticalCoverageAKID - 1) AS CancelFlag
	FROM SQ_Generated
	LEFT JOIN LKP_RATINGCOVERAGE_CANCELLATION LKP_RATINGCOVERAGE_CANCELLATION_RatingCoverageAKId_WorkRatingModifierAKId_RunDate
	ON LKP_RATINGCOVERAGE_CANCELLATION_RatingCoverageAKId_WorkRatingModifierAKId_RunDate.RatingCoverageAKId = RatingCoverageAKId
	AND LKP_RATINGCOVERAGE_CANCELLATION_RatingCoverageAKId_WorkRatingModifierAKId_RunDate.WorkRatingModifierAKId = WorkRatingModifierAKId
	AND LKP_RATINGCOVERAGE_CANCELLATION_RatingCoverageAKId_WorkRatingModifierAKId_RunDate.EffectiveDate = RunDate

	LEFT JOIN LKP_WORKEARNEDPREMIUMCOVERAGEMONTHLY LKP_WORKEARNEDPREMIUMCOVERAGEMONTHLY_StatisticalCoverageAKId_v_RunMonth
	ON LKP_WORKEARNEDPREMIUMCOVERAGEMONTHLY_StatisticalCoverageAKId_v_RunMonth.StatisticalCoverageAKID = StatisticalCoverageAKId
	AND LKP_WORKEARNEDPREMIUMCOVERAGEMONTHLY_StatisticalCoverageAKId_v_RunMonth.RunDate = v_RunMonth

),
FIL_Cancelled AS (
	SELECT
	PremiumMasterCalculationID, 
	SourceSystemID, 
	PremiumTransactionAmount, 
	RunDate, 
	OtherModifiedFactor, 
	ScheduleModifiedFactor, 
	ExperienceModifiedFactor, 
	PreviousOtherModifiedFactor, 
	PreviousScheduleModifiedFactor, 
	PreviousExperienceModifiedFactor, 
	CancelFlag
	FROM EXP_Cancelled
	WHERE ISNULL(CancelFlag)
),
EXP_Generated AS (
	SELECT
	PremiumMasterCalculationID,
	SourceSystemID,
	PremiumTransactionAmount,
	RunDate,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	PreviousOtherModifiedFactor,
	PreviousScheduleModifiedFactor,
	PreviousExperienceModifiedFactor,
	PremiumTransactionAmount AS v_OtherModifiedPremium,
	-- *INF*: ROUND(PremiumTransactionAmount/OtherModifiedFactor-PremiumTransactionAmount/PreviousOtherModifiedFactor,4)
	ROUND(PremiumTransactionAmount / OtherModifiedFactor - PremiumTransactionAmount / PreviousOtherModifiedFactor, 4) AS v_ScheduleModifiedPremium,
	-- *INF*: ROUND(PremiumTransactionAmount/OtherModifiedFactor/ScheduleModifiedFactor-PremiumTransactionAmount/PreviousOtherModifiedFactor/PreviousScheduleModifiedFactor,4)
	ROUND(PremiumTransactionAmount / OtherModifiedFactor / ScheduleModifiedFactor - PremiumTransactionAmount / PreviousOtherModifiedFactor / PreviousScheduleModifiedFactor, 4) AS v_ExperienceModifiedPremium,
	-- *INF*: ROUND(PremiumTransactionAmount/OtherModifiedFactor/ScheduleModifiedFactor/ExperienceModifiedFactor-PremiumTransactionAmount/PreviousOtherModifiedFactor/PreviousScheduleModifiedFactor/PreviousExperienceModifiedFactor,4)
	ROUND(PremiumTransactionAmount / OtherModifiedFactor / ScheduleModifiedFactor / ExperienceModifiedFactor - PremiumTransactionAmount / PreviousOtherModifiedFactor / PreviousScheduleModifiedFactor / PreviousExperienceModifiedFactor, 4) AS v_SubjectWrittenPremium,
	'1' AS GeneratedRecordIndicator,
	v_OtherModifiedPremium AS OtherModifiedPremium,
	v_ScheduleModifiedPremium AS ScheduleModifiedPremium,
	v_ExperienceModifiedPremium AS ExperienceModifiedPremium,
	v_SubjectWrittenPremium AS SubjectWrittenPremium,
	-- *INF*: DECODE(TRUE,
	-- v_OtherModifiedPremium!=0,1,
	-- v_ScheduleModifiedPremium!=0,1,
	-- v_ExperienceModifiedPremium!=0,1,
	-- v_SubjectWrittenPremium!=0,1,
	-- 0)
	DECODE(TRUE,
	v_OtherModifiedPremium != 0, 1,
	v_ScheduleModifiedPremium != 0, 1,
	v_ExperienceModifiedPremium != 0, 1,
	v_SubjectWrittenPremium != 0, 1,
	0) AS FilterFlag
	FROM FIL_Cancelled
),
FILRecord_ValidModifiedPremium AS (
	SELECT
	PremiumMasterCalculationID, 
	SourceSystemID, 
	PremiumTransactionAmount, 
	RunDate, 
	GeneratedRecordIndicator, 
	OtherModifiedPremium, 
	ScheduleModifiedPremium, 
	ExperienceModifiedPremium, 
	SubjectWrittenPremium, 
	FilterFlag
	FROM EXP_Generated
	WHERE FilterFlag=1
),
UN_Generated_Natural AS (
	SELECT PremiumMasterCalculationID, PremiumTransactionAmount, RunDate, GeneratedRecordIndicator, OtherModifiedPremium, ScheduleModifiedPremium, ExperienceModifiedPremium, SubjectWrittenPremium, SourceSystemID
	FROM FILRecord_ValidModifiedPremium
	UNION
	SELECT PremiumMasterCalculationID, PremiumTransactionAmount, o_RunDate AS RunDate, GeneratedRecordIndicator, OtherModifiedPremium, ScheduleModifiedPremium, ExperienceModifiedPremium, SubjectWrittenPremium, SourceSystemID
	FROM EXP_Natural
),
LKP_ModifiedPremiumNonWorkersCompensationCalculation AS (
	SELECT
	ModifiedPremiumNonWorkersCompensationCalculationId,
	DirectWrittenPremium,
	OtherModifiedPremium,
	ScheduleModifiedPremium,
	ExperienceModifiedPremium,
	SubjectWrittenPremium,
	PremiumMasterCalculationID
	FROM (
		SELECT 
			ModifiedPremiumNonWorkersCompensationCalculationId,
			DirectWrittenPremium,
			OtherModifiedPremium,
			ScheduleModifiedPremium,
			ExperienceModifiedPremium,
			SubjectWrittenPremium,
			PremiumMasterCalculationID
		FROM ModifiedPremiumNonWorkersCompensationCalculation
		WHERE @{pipeline().parameters.WHERE_LKP_TGT} AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationID ORDER BY ModifiedPremiumNonWorkersCompensationCalculationId) = 1
),
EXP_DetectChange AS (
	SELECT
	LKP_ModifiedPremiumNonWorkersCompensationCalculation.ModifiedPremiumNonWorkersCompensationCalculationId AS lkp_ModifiedPremiumNonWorkersCompensationCalculationId,
	LKP_ModifiedPremiumNonWorkersCompensationCalculation.DirectWrittenPremium AS lkp_DirectWrittenPremium,
	LKP_ModifiedPremiumNonWorkersCompensationCalculation.OtherModifiedPremium AS lkp_OtherModifiedPremium,
	LKP_ModifiedPremiumNonWorkersCompensationCalculation.ScheduleModifiedPremium AS lkp_ScheduleModifiedPremium,
	LKP_ModifiedPremiumNonWorkersCompensationCalculation.ExperienceModifiedPremium AS lkp_ExperienceModifiedPremium,
	LKP_ModifiedPremiumNonWorkersCompensationCalculation.SubjectWrittenPremium AS lkp_SubjectWrittenPremium,
	UN_Generated_Natural.PremiumMasterCalculationID,
	UN_Generated_Natural.RunDate,
	UN_Generated_Natural.GeneratedRecordIndicator,
	UN_Generated_Natural.PremiumTransactionAmount AS DirectWrittenPremium,
	UN_Generated_Natural.OtherModifiedPremium,
	UN_Generated_Natural.ScheduleModifiedPremium,
	UN_Generated_Natural.ExperienceModifiedPremium,
	UN_Generated_Natural.SubjectWrittenPremium,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	UN_Generated_Natural.SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_ModifiedPremiumNonWorkersCompensationCalculationId),'NEW',
	-- lkp_DirectWrittenPremium!=DirectWrittenPremium,'CHANGE',
	-- lkp_OtherModifiedPremium!=OtherModifiedPremium,'CHANGE',
	-- lkp_ScheduleModifiedPremium!=ScheduleModifiedPremium,'CHANGE',
	-- lkp_ExperienceModifiedPremium!=ExperienceModifiedPremium,'CHANGE',
	-- lkp_SubjectWrittenPremium!=SubjectWrittenPremium,'CHANGE',
	-- 'NOCHANGE')
	DECODE(TRUE,
	lkp_ModifiedPremiumNonWorkersCompensationCalculationId IS NULL, 'NEW',
	lkp_DirectWrittenPremium != DirectWrittenPremium, 'CHANGE',
	lkp_OtherModifiedPremium != OtherModifiedPremium, 'CHANGE',
	lkp_ScheduleModifiedPremium != ScheduleModifiedPremium, 'CHANGE',
	lkp_ExperienceModifiedPremium != ExperienceModifiedPremium, 'CHANGE',
	lkp_SubjectWrittenPremium != SubjectWrittenPremium, 'CHANGE',
	'NOCHANGE') AS o_ChangeFlag
	FROM UN_Generated_Natural
	LEFT JOIN LKP_ModifiedPremiumNonWorkersCompensationCalculation
	ON LKP_ModifiedPremiumNonWorkersCompensationCalculation.PremiumMasterCalculationID = UN_Generated_Natural.PremiumMasterCalculationID
),
RTR_Insert_Update AS (
	SELECT
	lkp_ModifiedPremiumNonWorkersCompensationCalculationId AS ModifiedPremiumNonWorkersCompensationCalculationId,
	o_AuditID AS AuditId,
	SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	PremiumMasterCalculationID,
	RunDate,
	GeneratedRecordIndicator,
	DirectWrittenPremium,
	OtherModifiedPremium,
	ScheduleModifiedPremium,
	ExperienceModifiedPremium,
	SubjectWrittenPremium,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_DetectChange
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='UPDATE'),
TGT_ModifiedPremiumNonWorkersCompensationCalculation_Insert AS (
	INSERT INTO ModifiedPremiumNonWorkersCompensationCalculation
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, PremiumMasterCalculationId, RunDate, GeneratedRecordIndicator, DirectWrittenPremium, OtherModifiedPremium, ScheduleModifiedPremium, ExperienceModifiedPremium, SubjectWrittenPremium)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PremiumMasterCalculationID AS PREMIUMMASTERCALCULATIONID, 
	RUNDATE, 
	GENERATEDRECORDINDICATOR, 
	DIRECTWRITTENPREMIUM, 
	OTHERMODIFIEDPREMIUM, 
	SCHEDULEMODIFIEDPREMIUM, 
	EXPERIENCEMODIFIEDPREMIUM, 
	SUBJECTWRITTENPREMIUM
	FROM RTR_Insert_Update_INSERT
),
UPD_Target AS (
	SELECT
	ModifiedPremiumNonWorkersCompensationCalculationId, 
	AuditId, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	PremiumMasterCalculationID, 
	RunDate, 
	GeneratedRecordIndicator, 
	DirectWrittenPremium, 
	OtherModifiedPremium, 
	ScheduleModifiedPremium, 
	ExperienceModifiedPremium, 
	SubjectWrittenPremium
	FROM RTR_Insert_Update_UPDATE
),
TGT_ModifiedPremiumNonWorkersCompensationCalculation_Update AS (
	MERGE INTO ModifiedPremiumNonWorkersCompensationCalculation AS T
	USING UPD_Target AS S
	ON T.ModifiedPremiumNonWorkersCompensationCalculationId = S.ModifiedPremiumNonWorkersCompensationCalculationId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.SourceSystemID = S.SourceSystemID, T.CreatedDate = S.CreatedDate, T.ModifiedDate = S.ModifiedDate, T.PremiumMasterCalculationId = S.PremiumMasterCalculationID, T.RunDate = S.RunDate, T.GeneratedRecordIndicator = S.GeneratedRecordIndicator, T.DirectWrittenPremium = S.DirectWrittenPremium, T.OtherModifiedPremium = S.OtherModifiedPremium, T.ScheduleModifiedPremium = S.ScheduleModifiedPremium, T.ExperienceModifiedPremium = S.ExperienceModifiedPremium, T.SubjectWrittenPremium = S.SubjectWrittenPremium
),