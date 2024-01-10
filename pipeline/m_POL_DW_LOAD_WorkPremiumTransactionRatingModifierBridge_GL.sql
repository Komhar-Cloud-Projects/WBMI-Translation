WITH
LKP_ArchPif43RXGLStage AS (
	SELECT
	Pif43RXGLStageId,
	PolicyKey,
	SublineCode
	FROM (
		SELECT MAX(ArchPif43RXGLStageId) AS Pif43RXGLStageId,
		RTRIM(PifSymbol)+PifPolicyNumber+PifModule AS PolicyKey,
		Pmdrxg1PmsDefGlSubline AS SublineCode
		FROM ArchPif43RXGLStage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(a.PifSymbol)+a.PifPolicyNumber+a.PifModule=b.PolicyKey
		WHERE Pmdrxg1SegmentStatus='A'
		GROUP BY RTRIM(PifSymbol)+PifPolicyNumber+PifModule,
		Pmdrxg1PmsDefGlSubline
		ORDER BY RTRIM(PifSymbol)+PifPolicyNumber+PifModule,
		Pmdrxg1PmsDefGlSubline
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,SublineCode ORDER BY Pif43RXGLStageId) = 1
),
SQ_PMS AS (
	select P.pol_key
	      ,SC.SublineCode
		  ,PT.PremiumTransactionAKID
		  ,PT.PremiumTransactionEnteredDate
		  ,PT.PremiumTransactionEffectiveDate
		  ,BS.BureauCode11
		  ,SC.RiskUnitGroup
		  ,SC.MajorPerilCode
		  ,SC.ClassCode
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	JOIN
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode BS
	ON
	BS.PremiumTransactionAKID=PT.PremiumTransactionAKID
	AND BS.CurrentSnapshotFlag=1
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	ON
	PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID 
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON
	SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	and PC.InsuranceLine='GL'
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
	ON
	PC.PolicyAKID = P.pol_ak_id AND P.crrnt_snpsht_flag = 1
	AND P.pms_pol_lob_code<>'BND'
	WHERE PT.CurrentSnapshotFlag=1
	AND PT.SourceSystemID = 'PMS'
	AND PT.ReasonAmendedCode not in ('COL','CWO','Claw Back')
	AND PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	AND PT.PremiumType= 'D'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Modifiers AS (
	SELECT
	pol_key AS i_PolicyKey,
	SublineCode AS i_SublineCode,
	PremiumTransactionAKID,
	PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	BureauCode11 AS i_BureauCode11,
	RiskUnitGroup AS i_RiskUnitGroup,
	MajorPerilCode AS i_MajorPerilCode,
	ClassCode AS i_ClassCode,
	-- *INF*: IIF((i_BureauCode11='100' and i_MajorPerilCode<>'919' )OR i_ClassCode='22222' OR i_ClassCode= '22250',0,1)
	IFF(( i_BureauCode11 = '100' 
			AND i_MajorPerilCode <> '919' 
		) 
		OR i_ClassCode = '22222' 
		OR i_ClassCode = '22250',
		0,
		1
	) AS v_ModifiedTransactionFlag,
	-- *INF*: DECODE(TRUE,
	-- v_ModifiedTransactionFlag=0  ,'Default',
	-- i_PolicyKey||'&GL&'||i_RiskUnitGroup)
	DECODE(TRUE,
		v_ModifiedTransactionFlag = 0, 'Default',
		i_PolicyKey || '&GL&' || i_RiskUnitGroup
	) AS v_WorkRatingModifierKey,
	v_WorkRatingModifierKey AS o_WorkRatingModifierKey,
	-- *INF*: IIF(v_ModifiedTransactionFlag=0 ,'Default',MD5(v_WorkRatingModifierKey))
	IFF(v_ModifiedTransactionFlag = 0,
		'Default',
		MD5(v_WorkRatingModifierKey
		)
	) AS o_WorkRatingModifierHashKey,
	-- *INF*: IIF(v_ModifiedTransactionFlag=0,-1,:LKP.LKP_ArchPif43RXGLStage(i_PolicyKey, i_RiskUnitGroup))
	IFF(v_ModifiedTransactionFlag = 0,
		- 1,
		LKP_ARCHPIF43RXGLSTAGE_i_PolicyKey_i_RiskUnitGroup.Pif43RXGLStageId
	) AS o_Pif43RXGLStageId,
	-- *INF*: IIF(v_ModifiedTransactionFlag=0,TO_DATE('18000101','YYYYMMDD'),ADD_TO_DATE(TRUNC(GREATEST(i_PremiumTransactionEnteredDate,PremiumTransactionEffectiveDate),'DD'),'SS',86399))
	IFF(v_ModifiedTransactionFlag = 0,
		TO_DATE('18000101', 'YYYYMMDD'
		),
		DATEADD(SECOND,86399,CAST(TRUNC(GREATEST(i_PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate
		), 'DAY') AS TIMESTAMP_NTZ(0)))
	) AS o_PremiumTransactionBookedDate
	FROM SQ_PMS
	LEFT JOIN LKP_ARCHPIF43RXGLSTAGE LKP_ARCHPIF43RXGLSTAGE_i_PolicyKey_i_RiskUnitGroup
	ON LKP_ARCHPIF43RXGLSTAGE_i_PolicyKey_i_RiskUnitGroup.PolicyKey = i_PolicyKey
	AND LKP_ARCHPIF43RXGLSTAGE_i_PolicyKey_i_RiskUnitGroup.SublineCode = i_RiskUnitGroup

),
SRTTRANS AS (
	SELECT
	PremiumTransactionAKID, 
	PremiumTransactionEffectiveDate, 
	o_WorkRatingModifierKey AS WorkRatingModifierKey, 
	o_WorkRatingModifierHashKey AS WorkRatingModifierHashKey, 
	o_Pif43RXGLStageId AS Pif43RXGLStageId, 
	o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate
	FROM EXP_Modifiers
	ORDER BY WorkRatingModifierHashKey ASC
),
AGGTRANS AS (
	SELECT
	PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	WorkRatingModifierHashKey,
	WorkRatingModifierKey,
	Pif43RXGLStageId,
	PremiumTransactionBookedDate AS i_PremiumTransactionBookedDate,
	-- *INF*: MIN(i_PremiumTransactionBookedDate)
	MIN(i_PremiumTransactionBookedDate
	) AS o_RunDate,
	-- *INF*: MIN(i_PremiumTransactionEffectiveDate)
	MIN(i_PremiumTransactionEffectiveDate
	) AS o_RatingModifierEffectiveDate
	FROM SRTTRANS
	GROUP BY WorkRatingModifierHashKey
),
LKP_Pif43RXGLStage_Modifiers AS (
	SELECT
	ArchPif43RXGLStageId,
	Pmdrxg1ScheduleMod,
	Pmdrxg1ExpenseMod
	FROM (
		SELECT ArchPif43RXGLStageId AS ArchPif43RXGLStageId,
		Pmdrxg1ScheduleMod AS Pmdrxg1ScheduleMod,
		Pmdrxg1ExpenseMod AS Pmdrxg1ExpenseMod
		FROM ArchPif43RXGLStage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(pifsymbol)+pifpolicynumber+PifModule=b.PolicyKey
		WHERE ArchPif43RXGLStageId IN (
		SELECT MAX(ArchPif43RXGLStageId ) AS ArchPif43RXGLStageId
		FROM ArchPif43RXGLStage 
		WHERE Pmdrxg1SegmentStatus='A'
		GROUP BY RTRIM(PifSymbol)+PifPolicyNumber+PifModule,
		case Pmdrxg1RiskTypeInd when 'O' then '334' when 'P' then '336' else Pmdrxg1PmsDefGlSubline end)
		ORDER BY ArchPif43RXGLStageId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ArchPif43RXGLStageId ORDER BY ArchPif43RXGLStageId) = 1
),
LKP_WorkRatingModifierAKId AS (
	SELECT
	WorkRatingModifierAKId,
	WorkRatingModifierHashKey
	FROM (
		SELECT WorkRatingModifierAKId as WorkRatingModifierAKId, WorkRatingModifierHashKey as WorkRatingModifierHashKey FROM
		(SELECT WorkRatingModifierAKId, WorkRatingModifierHashKey FROM WorkRatingModifier a
		where not exists (
		select 1 from WorkRatingModifier b
		where a.WorkRatingModifierAKId=b.WorkRatingModifierAKId
		and b.RunDate>a.RunDate)
		and SourceSystemId='PMS'
		and substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,2)='GL'
		UNION ALL
		SELECT -1 as WorkRatingModifierAKId,'Default' AS WorkRatingModifierHashKey) A
		order by WorkRatingModifierHashKey
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WorkRatingModifierHashKey ORDER BY WorkRatingModifierAKId) = 1
),
SEQ_RatingModifier AS (
	CREATE SEQUENCE SEQ_RatingModifier
	START = 0
	INCREMENT = 1;
),
EXPTRANS AS (
	SELECT
	SEQ_RatingModifier.NEXTVAL AS i_NEXTVAL,
	LKP_WorkRatingModifierAKId.WorkRatingModifierAKId AS lkp_WorkRatingModifierAKId,
	LKP_Pif43RXGLStage_Modifiers.Pmdrxg1ScheduleMod AS i_ScheduleModifiedFactor,
	LKP_Pif43RXGLStage_Modifiers.Pmdrxg1ExpenseMod AS i_ExperienceModifiedFactor,
	AGGTRANS.WorkRatingModifierHashKey,
	AGGTRANS.WorkRatingModifierKey,
	AGGTRANS.o_RunDate AS RunDate,
	AGGTRANS.o_RatingModifierEffectiveDate AS RatingModifierEffectiveDate,
	-- *INF*: IIF(ISNULL(lkp_WorkRatingModifierAKId),i_NEXTVAL,lkp_WorkRatingModifierAKId)
	IFF(lkp_WorkRatingModifierAKId IS NULL,
		i_NEXTVAL,
		lkp_WorkRatingModifierAKId
	) AS WorkRatingModifierAKId,
	1 AS o_OtherModifiedFactor,
	-- *INF*: IIF(NOT ISNULL(i_ScheduleModifiedFactor) AND i_ScheduleModifiedFactor>0,i_ScheduleModifiedFactor,1)
	IFF(i_ScheduleModifiedFactor IS NULL 
		AND i_ScheduleModifiedFactorNOT  > 0,
		i_ScheduleModifiedFactor,
		1
	) AS o_ScheduleModifiedFactor,
	-- *INF*: IIF(NOT ISNULL(i_ExperienceModifiedFactor) AND i_ExperienceModifiedFactor>0,i_ExperienceModifiedFactor,1)
	IFF(i_ExperienceModifiedFactor IS NULL 
		AND i_ExperienceModifiedFactorNOT  > 0,
		i_ExperienceModifiedFactor,
		1
	) AS o_ExperienceModifiedFactor
	FROM AGGTRANS
	LEFT JOIN LKP_Pif43RXGLStage_Modifiers
	ON LKP_Pif43RXGLStage_Modifiers.ArchPif43RXGLStageId = AGGTRANS.Pif43RXGLStageId
	LEFT JOIN LKP_WorkRatingModifierAKId
	ON LKP_WorkRatingModifierAKId.WorkRatingModifierHashKey = AGGTRANS.WorkRatingModifierHashKey
),
LKP_WorkRatingModifier AS (
	SELECT
	WorkRatingModifierHashKey,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	EffectiveDate,
	ExpirationDate
	FROM (
		select WorkRatingModifierHashKey as WorkRatingModifierHashKey,
		EffectiveDate as EffectiveDate,
		ExpirationDate as ExpirationDate,
		OtherModifiedFactor as OtherModifiedFactor,
		ScheduleModifiedFactor as ScheduleModifiedFactor,
		ExperienceModifiedFactor as ExperienceModifiedFactor from
		(select WorkRatingModifierHashKey as WorkRatingModifierHashKey,
		RunDate as EffectiveDate,
		ISNULL(lead(RunDate,1) over (partition by WorkRatingModifierAKId order by RunDate),'2100-12-31 23:59:59') as ExpirationDate,
		OtherModifiedFactor as OtherModifiedFactor,
		ScheduleModifiedFactor as ScheduleModifiedFactor,
		ExperienceModifiedFactor as ExperienceModifiedFactor
		from WorkRatingModifier
		where WorkRatingModifierAKId<>-1
		and SourceSystemId='PMS'
		and substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,2)='GL'
		union all
		select 'Default' as WorkRatingModifierHashKey,
		'1800-1-1 00:00:00' as EffectiveDate,
		'2100-12-31 23:59:59' as ExpirationDate,
		1 as OtherModifiedFactor,
		1 as ScheduleModifiedFactor,
		1 as ExperienceModifiedFactor) a
		order by WorkRatingModifierHashKey,EffectiveDate ,ExpirationDate
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WorkRatingModifierHashKey,EffectiveDate,ExpirationDate ORDER BY WorkRatingModifierHashKey) = 1
),
EXP_RatingModifier AS (
	SELECT
	LKP_WorkRatingModifier.WorkRatingModifierHashKey AS lkp_WorkRatingModifierHashKey,
	LKP_WorkRatingModifier.OtherModifiedFactor AS lkp_OtherModifiedFactor,
	LKP_WorkRatingModifier.ScheduleModifiedFactor AS lkp_ScheduleModifiedFactor,
	LKP_WorkRatingModifier.ExperienceModifiedFactor AS lkp_ExperienceModifiedFactor,
	LKP_WorkRatingModifier.EffectiveDate AS lkp_EffectiveDate,
	EXPTRANS.RunDate AS i_RunDate,
	EXPTRANS.WorkRatingModifierAKId,
	EXPTRANS.WorkRatingModifierHashKey,
	EXPTRANS.WorkRatingModifierKey,
	EXPTRANS.RatingModifierEffectiveDate,
	EXPTRANS.o_OtherModifiedFactor AS OtherModifiedFactor,
	EXPTRANS.o_ScheduleModifiedFactor AS ScheduleModifiedFactor,
	EXPTRANS.o_ExperienceModifiedFactor AS ExperienceModifiedFactor,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_WorkRatingModifierHashKey),'NEW',
	-- lkp_EffectiveDate=i_RunDate,'NOCHANGE',
	-- lkp_OtherModifiedFactor!=OtherModifiedFactor,'NEW',
	-- lkp_ScheduleModifiedFactor!=ScheduleModifiedFactor,'NEW',
	-- lkp_ExperienceModifiedFactor!=ExperienceModifiedFactor,'NEW',
	-- 'NOCHANGE')
	DECODE(TRUE,
		lkp_WorkRatingModifierHashKey IS NULL, 'NEW',
		lkp_EffectiveDate = i_RunDate, 'NOCHANGE',
		lkp_OtherModifiedFactor != OtherModifiedFactor, 'NEW',
		lkp_ScheduleModifiedFactor != ScheduleModifiedFactor, 'NEW',
		lkp_ExperienceModifiedFactor != ExperienceModifiedFactor, 'NEW',
		'NOCHANGE'
	) AS v_ChangeFlag,
	-- *INF*: IIF(v_ChangeFlag='NEW',i_RunDate,lkp_EffectiveDate)
	IFF(v_ChangeFlag = 'NEW',
		i_RunDate,
		lkp_EffectiveDate
	) AS o_RunDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	v_ChangeFlag AS o_ChangeFlag,
	-- *INF*: TO_DATE('01/01/1800 0','MM/DD/YYYY SSSSS')
	TO_DATE('01/01/1800 0', 'MM/DD/YYYY SSSSS'
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 86399','MM/DD/YYYY SSSSS')
	TO_DATE('12/31/2100 86399', 'MM/DD/YYYY SSSSS'
	) AS ExpirationDate,
	'1' AS CurrentSnapshotFlag
	FROM EXPTRANS
	LEFT JOIN LKP_WorkRatingModifier
	ON LKP_WorkRatingModifier.WorkRatingModifierHashKey = EXPTRANS.WorkRatingModifierHashKey AND LKP_WorkRatingModifier.EffectiveDate <= EXPTRANS.RunDate AND LKP_WorkRatingModifier.ExpirationDate > EXPTRANS.RunDate
),
JNRTRANS AS (SELECT
	SRTTRANS.PremiumTransactionAKID, 
	SRTTRANS.WorkRatingModifierHashKey AS i_WorkRatingModifierHashKey, 
	EXP_RatingModifier.WorkRatingModifierAKId, 
	EXP_RatingModifier.WorkRatingModifierHashKey, 
	EXP_RatingModifier.o_RunDate AS RunDate
	FROM SRTTRANS
	INNER JOIN EXP_RatingModifier
	ON EXP_RatingModifier.WorkRatingModifierHashKey = SRTTRANS.WorkRatingModifierHashKey
),
LKP_WorkPremiumTransactionRatingModifierBridge AS (
	SELECT
	PremiumTransactionAKID
	FROM (
		SELECT A.PremiumTransactionAKId as PremiumTransactionAKId FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransactionRatingModifierBridge A
		JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction C
		ON A.PremiumTransactionAKId=C.PremiumTransactionAKId
		and c.createddate>='@{pipeline().parameters.SELECTION_START_TS}'
		AND C.SourceSystemId='PMS'
		AND A.SourceSystemId='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID ORDER BY PremiumTransactionAKID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_WorkPremiumTransactionRatingModifierBridge.PremiumTransactionAKID AS lkp_PremiumTransactionAKID,
	JNRTRANS.PremiumTransactionAKID,
	JNRTRANS.WorkRatingModifierAKId,
	JNRTRANS.RunDate,
	-1 AS o_PremiumMasterCalculationId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(lkp_PremiumTransactionAKID),'NEW','NOCHANGE')
	IFF(lkp_PremiumTransactionAKID IS NULL,
		'NEW',
		'NOCHANGE'
	) AS o_ChangeFlag
	FROM JNRTRANS
	LEFT JOIN LKP_WorkPremiumTransactionRatingModifierBridge
	ON LKP_WorkPremiumTransactionRatingModifierBridge.PremiumTransactionAKID = JNRTRANS.PremiumTransactionAKID
),
RTR_INSERT_UPDATE AS (
	SELECT
	o_PremiumMasterCalculationId AS PremiumMasterCalculationId,
	PremiumTransactionAKID,
	o_AuditID AS AuditID,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	WorkRatingModifierAKId,
	RunDate,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_DetectChanges
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='NEW'),
WorkPremiumTransactionRatingModifierBridge AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransactionRatingModifierBridge
	(PremiumMasterCalculationId, PremiumTransactionAKId, AuditId, SourceSystemID, CreatedDate, ModifiedDate, WorkRatingModifierAKId, RunDate)
	SELECT 
	PREMIUMMASTERCALCULATIONID, 
	PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	AuditID AS AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	WORKRATINGMODIFIERAKID, 
	RUNDATE
	FROM RTR_INSERT_UPDATE_INSERT
),
RTR_RatingModifier AS (
	SELECT
	o_AuditID AS AuditId,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_RunDate AS RunDate,
	WorkRatingModifierAKId,
	WorkRatingModifierHashKey,
	WorkRatingModifierKey,
	RatingModifierEffectiveDate,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	o_ChangeFlag AS ChangeFlag,
	EffectiveDate,
	ExpirationDate,
	CurrentSnapshotFlag
	FROM EXP_RatingModifier
),
RTR_RatingModifier_INSERT AS (SELECT * FROM RTR_RatingModifier WHERE ChangeFlag='NEW'),
WorkRatingModifier AS (

	------------ PRE SQL ----------
	IF NOT EXISTS (
	SELECT 1 FROM WorkRatingModifier
	WHERE WorkRatingModifierAKId=-1)
	INSERT INTO WorkRatingModifier
	SELECT 1 as CurrentSnapshotFlag,
	0 AS AuditId,
	'1800-01-01 00:00:00' AS EffectiveDate,
	'2100-12-31 23:59:59' AS ExpirationDate,
	'PMS' AS SourceSystemID,
	GETDATE() AS CreatedDate,
	GETDATE() AS ModifiedDate,
	'1800-1-1' AS RunDate,
	-1 AS WorkRatingModifierAKId,
	'Default' AS WorkRatingModifierHashKey,
	'Default' AS WorkRatingModifierKey,
	'1800-1-1' AS RatingModifierEffectiveDate,
	1 AS OtherModifiedFactor,
	1 AS ScheduleModifiedFactor,
	1 AS ExperienceModifiedFactor
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkRatingModifier
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, RunDate, WorkRatingModifierAKId, WorkRatingModifierHashKey, WorkRatingModifierKey, RatingModifierEffectiveDate, OtherModifiedFactor, ScheduleModifiedFactor, ExperienceModifiedFactor)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RUNDATE, 
	WORKRATINGMODIFIERAKID, 
	WORKRATINGMODIFIERHASHKEY, 
	WORKRATINGMODIFIERKEY, 
	RATINGMODIFIEREFFECTIVEDATE, 
	OTHERMODIFIEDFACTOR, 
	SCHEDULEMODIFIEDFACTOR, 
	EXPERIENCEMODIFIEDFACTOR
	FROM RTR_RatingModifier_INSERT
),