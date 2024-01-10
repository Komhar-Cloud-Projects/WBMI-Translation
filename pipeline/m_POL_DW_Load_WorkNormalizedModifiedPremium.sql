WITH
SQ_Source AS (
	select a.PremiumMasterCalculationId,a.RunDate,'Classified' as ModifiedPremiumType,
	a.ClassifiedDirectWrittenPremium as ModifiedPremium,a.GeneratedRecordIndicator,b.PremiumMasterRunDate,b.PremiumTransactionEnteredDate,a.SourceSystemId
	from ModifiedPremiumWorkersCompensationCalculation a
	join PremiumMasterCalculation b
	on a.PremiumMasterCalculationId=b.PremiumMasterCalculationId
	where a.ClassifiedDirectWrittenPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_WC}
	union all
	select a.PremiumMasterCalculationId,a.RunDate,'Ratable' as ModifiedPremiumType,
	a.RatableDirectWrittenPremium as ModifiedPremium,a.GeneratedRecordIndicator,b.PremiumMasterRunDate,b.PremiumTransactionEnteredDate,a.SourceSystemId
	from ModifiedPremiumWorkersCompensationCalculation a
	join PremiumMasterCalculation b
	on a.PremiumMasterCalculationId=b.PremiumMasterCalculationId
	where a.RatableDirectWrittenPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_WC}
	union all
	select a.PremiumMasterCalculationId,a.RunDate,'Other' as ModifiedPremiumType,
	a.OtherModifiedDirectWrittenPremium as ModifiedPremium,a.GeneratedRecordIndicator,b.PremiumMasterRunDate,b.PremiumTransactionEnteredDate,a.SourceSystemId
	from ModifiedPremiumWorkersCompensationCalculation a
	join PremiumMasterCalculation b
	on a.PremiumMasterCalculationId=b.PremiumMasterCalculationId
	where a.OtherModifiedDirectWrittenPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_WC}
	union all
	select a.PremiumMasterCalculationId,a.RunDate,'Schedule' as ModifiedPremiumType,
	a.ScheduleModifiedDirectWrittenPremium as ModifiedPremium,a.GeneratedRecordIndicator,b.PremiumMasterRunDate,b.PremiumTransactionEnteredDate,a.SourceSystemId
	from ModifiedPremiumWorkersCompensationCalculation a
	join PremiumMasterCalculation b
	on a.PremiumMasterCalculationId=b.PremiumMasterCalculationId
	where a.ScheduleModifiedDirectWrittenPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_WC}
	union all
	select a.PremiumMasterCalculationId,a.RunDate,'Experience' as ModifiedPremiumType,
	a.ExperienceModifiedDirectWrittenPremium as ModifiedPremium,a.GeneratedRecordIndicator,b.PremiumMasterRunDate,b.PremiumTransactionEnteredDate,a.SourceSystemId
	from ModifiedPremiumWorkersCompensationCalculation a
	join PremiumMasterCalculation b
	on a.PremiumMasterCalculationId=b.PremiumMasterCalculationId
	where a.ExperienceModifiedDirectWrittenPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_WC}
	union all
	select a.PremiumMasterCalculationId,a.RunDate,'Subject' as ModifiedPremiumType,
	a.SubjectDirectWrittenPremium as ModifiedPremium,a.GeneratedRecordIndicator,b.PremiumMasterRunDate,b.PremiumTransactionEnteredDate,a.SourceSystemId
	from ModifiedPremiumWorkersCompensationCalculation a
	join PremiumMasterCalculation b
	on a.PremiumMasterCalculationId=b.PremiumMasterCalculationId
	where a.SubjectDirectWrittenPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_WC}
	
	union all
	select a.PremiumMasterCalculationId,a.RunDate,'Other' as ModifiedPremiumType,
	a.OtherModifiedPremium as ModifiedPremium,a.GeneratedRecordIndicator,NULL,NULL,a.SourceSystemId
	from ModifiedPremiumNonWorkersCompensationCalculation a
	where GeneratedRecordIndicator=1
	and a.OtherModifiedPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_OTHER}
	union all
	select a.PremiumMasterCalculationId,a.RunDate,'Schedule' as ModifiedPremiumType,
	a.ScheduleModifiedPremium as ModifiedPremium,a.GeneratedRecordIndicator,NULL,NULL,a.SourceSystemId
	from ModifiedPremiumNonWorkersCompensationCalculation a
	where GeneratedRecordIndicator=1
	and a.ScheduleModifiedPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_OTHER}
	union all
	select a.PremiumMasterCalculationId,a.RunDate,'Experience' as ModifiedPremiumType,
	a.ExperienceModifiedPremium as ModifiedPremium,a.GeneratedRecordIndicator,NULL,NULL,a.SourceSystemId
	from ModifiedPremiumNonWorkersCompensationCalculation a
	where GeneratedRecordIndicator=1
	and a.ExperienceModifiedPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_OTHER}
	union all
	select a.PremiumMasterCalculationId,a.RunDate,'Subject' as ModifiedPremiumType,
	a.SubjectWrittenPremium as ModifiedPremium,a.GeneratedRecordIndicator,NULL,NULL,a.SourceSystemId
	from ModifiedPremiumNonWorkersCompensationCalculation a
	where GeneratedRecordIndicator=1
	and a.SubjectWrittenPremium<>0
	and a.CreatedDate>'@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_OTHER}
),
EXP_Default AS (
	SELECT
	PremiumMasterCalculationId,
	RunDate,
	ModifiedPremiumType,
	ModifiedPremium,
	GeneratedRecordIndicator AS i_GeneratedRecordIndicator,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	-- *INF*: IIF(i_GeneratedRecordIndicator='F',i_PremiumMasterRunDate,ADD_TO_DATE(LAST_DAY(TRUNC(RunDate,'MM')),'SS',86399))
	IFF(i_GeneratedRecordIndicator = 'F',
		i_PremiumMasterRunDate,
		DATEADD(SECOND,86399,LAST_DAY(CAST(TRUNC(RunDate, 'MONTH') AS TIMESTAMP_NTZ(0))
		))
	) AS PremiumMasterRunDate,
	-- *INF*: IIF(i_GeneratedRecordIndicator='F',TRUNC(i_PremiumTransactionEnteredDate,'DD'),TRUNC(RunDate,'DD'))
	IFF(i_GeneratedRecordIndicator = 'F',
		CAST(TRUNC(i_PremiumTransactionEnteredDate, 'DAY') AS TIMESTAMP_NTZ(0)),
		CAST(TRUNC(RunDate, 'DAY') AS TIMESTAMP_NTZ(0))
	) AS PremiumTransactionEnteredDate,
	-- *INF*: IIF(i_GeneratedRecordIndicator='F','0','1')
	IFF(i_GeneratedRecordIndicator = 'F',
		'0',
		'1'
	) AS GeneratedRecordIndicator,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM SQ_Source
),
LKP_Target AS (
	SELECT
	WorkNormalizedModifiedPremiumId,
	PremiumMasterCalculationId,
	RunDate,
	ModifiedPremiumType
	FROM (
		select a.WorkNormalizedModifiedPremiumId as WorkNormalizedModifiedPremiumId,
		a.PremiumMasterCalculationId as PremiumMasterCalculationId,
		a.RunDate as RunDate,
		a.ModifiedPremiumType as ModifiedPremiumType
		from (
		SELECT a.WorkNormalizedModifiedPremiumId,
		a.PremiumMasterCalculationId,
		a.RunDate,
		a.ModifiedPremiumType
		FROM WorkNormalizedModifiedPremium a
		join ModifiedPremiumWorkersCompensationCalculation b
		on a.PremiumMasterCalculationId=b.PremiumMasterCalculationId
		and a.RunDate=b.RunDate
		and b.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		union all
		SELECT a.WorkNormalizedModifiedPremiumId,
		a.PremiumMasterCalculationId,
		a.RunDate,
		a.ModifiedPremiumType
		FROM WorkNormalizedModifiedPremium a
		join ModifiedPremiumNonWorkersCompensationCalculation b
		on a.PremiumMasterCalculationId=b.PremiumMasterCalculationId
		and a.RunDate=b.RunDate
		and b.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') a
		order by a.PremiumMasterCalculationId,a.RunDate,a.ModifiedPremiumType
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationId,RunDate,ModifiedPremiumType ORDER BY WorkNormalizedModifiedPremiumId) = 1
),
RTR_Insert AS (
	SELECT
	LKP_Target.WorkNormalizedModifiedPremiumId,
	EXP_Default.PremiumMasterCalculationId,
	EXP_Default.RunDate,
	EXP_Default.ModifiedPremiumType,
	EXP_Default.ModifiedPremium,
	EXP_Default.PremiumMasterRunDate,
	EXP_Default.PremiumTransactionEnteredDate,
	EXP_Default.GeneratedRecordIndicator,
	EXP_Default.AuditId,
	EXP_Default.SourceSystemID,
	EXP_Default.CreatedDate,
	EXP_Default.ModifiedDate
	FROM EXP_Default
	LEFT JOIN LKP_Target
	ON LKP_Target.PremiumMasterCalculationId = EXP_Default.PremiumMasterCalculationId AND LKP_Target.RunDate = EXP_Default.RunDate AND LKP_Target.ModifiedPremiumType = EXP_Default.ModifiedPremiumType
),
RTR_Insert_INSERT AS (SELECT * FROM RTR_Insert WHERE ISNULL(WorkNormalizedModifiedPremiumId)),
WorkNormalizedModifiedPremium AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkNormalizedModifiedPremium
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, PremiumMasterCalculationId, RunDate, ModifiedPremiumType, ModifiedPremium, PremiumMasterRunDate, PremiumTransactionEnteredDate, GeneratedRecordFlag)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PREMIUMMASTERCALCULATIONID, 
	RUNDATE, 
	MODIFIEDPREMIUMTYPE, 
	MODIFIEDPREMIUM, 
	PREMIUMMASTERRUNDATE, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	GeneratedRecordIndicator AS GENERATEDRECORDFLAG
	FROM RTR_Insert_INSERT
),