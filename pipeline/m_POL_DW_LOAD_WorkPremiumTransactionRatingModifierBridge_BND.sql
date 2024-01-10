WITH
LKP_ArchPif08Stage AS (
	SELECT
	RatingModifier,
	PolicyKey
	FROM (
		SELECT 
		RTRIM(pifsymbol)+pifpolicynumber+pifmodule as PolicyKey,
		WBMSBNDIndividualRiskModificationFactor as RatingModifier
		FROM ArchPif08stage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(pifsymbol)+pifpolicynumber+pifmodule=b.PolicyKey
		WHERE ArchPif08stageId IN
		(SELECT MAX(ArchPif08stageId)
		FROM ArchPif08stage
		GROUP BY RTRIM(pifsymbol)+pifpolicynumber+pifmodule)
		order by RTRIM(pifsymbol)+pifpolicynumber+pifmodule
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY RatingModifier) = 1
),
SQ_PMS AS (
	select P.pol_key,PT.PremiumTransactionAKID,PT.PremiumTransactionEnteredDate,PT.PremiumTransactionEffectiveDate,PC.InsuranceLine,PC.TypeBureauCode,BS.BureauCode7,BS.BureauCode9,BS.BureauCode11,BS.BureauCode12
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode BS
	ON
	PT.PremiumTransactionAKId=BS.PremiumTransactionAKId
	AND BS.CurrentSnapshotFlag=1
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	ON
	PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID 
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON
	SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
	ON
	PC.PolicyAKID = P.pol_ak_id AND P.crrnt_snpsht_flag = 1
	AND P.pms_pol_lob_code='BND'
	WHERE PT.CurrentSnapshotFlag=1
	AND PT.SourceSystemID = 'PMS'
	and PT.ReasonAmendedCode not in ('COL','CWO','Claw Back')
	and PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	and PT.PREMIUMTYPE = 'D'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Modifiers AS (
	SELECT
	pol_key AS i_PolicyKey,
	PremiumTransactionAKId,
	PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	InsuranceLine AS i_InsuranceLine,
	TypeBureauCode AS i_TypeBureauCode,
	BureauCode7 AS i_BureauCode7,
	BureauCode9 AS i_BureauCode9,
	BureauCode11 AS i_BureauCode11,
	BureauCode12 AS i_BureauCode12,
	-- *INF*: DECODE(TRUE,
	-- i_BureauCode11='100' and i_InsuranceLine='GL',1,
	-- i_BureauCode12='100' and i_InsuranceLine='CR' and i_TypeBureauCode='CR',1,
	-- i_BureauCode9='100' and i_InsuranceLine='CR' and i_TypeBureauCode='FT',1,
	-- i_BureauCode7='100' and i_InsuranceLine='CR' and i_TypeBureauCode='BT',1, 
	-- 0)
	DECODE(TRUE,
		i_BureauCode11 = '100' AND i_InsuranceLine = 'GL', 1,
		i_BureauCode12 = '100' AND i_InsuranceLine = 'CR' AND i_TypeBureauCode = 'CR', 1,
		i_BureauCode9 = '100' AND i_InsuranceLine = 'CR' AND i_TypeBureauCode = 'FT', 1,
		i_BureauCode7 = '100' AND i_InsuranceLine = 'CR' AND i_TypeBureauCode = 'BT', 1,
		0) AS v_DefaultFlag,
	-- *INF*: IIF(v_DefaultFlag=0,
	-- :LKP.LKP_ArchPif08Stage(i_PolicyKey),
	-- 1)
	IFF(v_DefaultFlag = 0, LKP_ARCHPIF08STAGE_i_PolicyKey.RatingModifier, 1) AS v_Modifier,
	-- *INF*: IIF(v_DefaultFlag=0,
	-- i_PolicyKey||'&BND',
	-- 'Default')
	IFF(v_DefaultFlag = 0, i_PolicyKey || '&BND', 'Default') AS v_WorkRatingModifierKey,
	-- *INF*: IIF(v_DefaultFlag=0,MD5(v_WorkRatingModifierKey),'Default')
	IFF(v_DefaultFlag = 0, MD5(v_WorkRatingModifierKey), 'Default') AS o_WorkRatingModifierHashKey,
	v_WorkRatingModifierKey AS o_WorkRatingModifierKey,
	-- *INF*: IIF(NOT ISNULL(v_Modifier) AND v_Modifier>0,v_Modifier,1)
	IFF(NOT v_Modifier IS NULL AND v_Modifier > 0, v_Modifier, 1) AS o_ScheduleModifiedFactor,
	-- *INF*: IIF(v_DefaultFlag=0, 
	-- ADD_TO_DATE(TRUNC(GREATEST(i_PremiumTransactionEnteredDate,PremiumTransactionEffectiveDate),'DD'),'SS',86399),
	-- TO_DATE('18000101','YYYYMMDD'))
	IFF(v_DefaultFlag = 0, ADD_TO_DATE(TRUNC(GREATEST(i_PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate), 'DD'), 'SS', 86399), TO_DATE('18000101', 'YYYYMMDD')) AS o_PremiumTransactionBookedDate
	FROM SQ_PMS
	LEFT JOIN LKP_ARCHPIF08STAGE LKP_ARCHPIF08STAGE_i_PolicyKey
	ON LKP_ARCHPIF08STAGE_i_PolicyKey.PolicyKey = i_PolicyKey

),
SRTTRANS AS (
	SELECT
	PremiumTransactionAKId, 
	PremiumTransactionEffectiveDate, 
	o_WorkRatingModifierHashKey AS WorkRatingModifierHashKey, 
	o_WorkRatingModifierKey AS WorkRatingModifierKey, 
	o_ScheduleModifiedFactor AS ScheduleModifiedFactor, 
	o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate
	FROM EXP_Modifiers
	ORDER BY WorkRatingModifierHashKey ASC
),
AGGTRANS AS (
	SELECT
	PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	WorkRatingModifierHashKey,
	WorkRatingModifierKey,
	ScheduleModifiedFactor,
	PremiumTransactionBookedDate AS i_PremiumTransactionBookedDate,
	-- *INF*: MIN(i_PremiumTransactionBookedDate)
	MIN(i_PremiumTransactionBookedDate) AS o_RunDate,
	-- *INF*: MIN(i_PremiumTransactionEffectiveDate)
	MIN(i_PremiumTransactionEffectiveDate) AS o_RatingModifierEffectiveDate
	FROM SRTTRANS
	GROUP BY WorkRatingModifierHashKey
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
		and a.SourceSystemId='PMS'
		and substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,3)='BND'
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
	AGGTRANS.WorkRatingModifierHashKey,
	AGGTRANS.WorkRatingModifierKey,
	1 AS OtherModifiedFactor,
	AGGTRANS.ScheduleModifiedFactor,
	1 AS ExperienceModifiedFactor,
	AGGTRANS.o_RunDate AS RunDate,
	AGGTRANS.o_RatingModifierEffectiveDate AS RatingModifierEffectiveDate,
	-- *INF*: IIF(ISNULL(lkp_WorkRatingModifierAKId),i_NEXTVAL,lkp_WorkRatingModifierAKId)
	IFF(lkp_WorkRatingModifierAKId IS NULL, i_NEXTVAL, lkp_WorkRatingModifierAKId) AS WorkRatingModifierAKId
	FROM AGGTRANS
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
		and substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,3)='BND'
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
	EXPTRANS.OtherModifiedFactor,
	EXPTRANS.ScheduleModifiedFactor,
	EXPTRANS.ExperienceModifiedFactor,
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
		'NOCHANGE') AS v_ChangeFlag,
	-- *INF*: IIF(v_ChangeFlag='NEW',i_RunDate,lkp_EffectiveDate)
	IFF(v_ChangeFlag = 'NEW', i_RunDate, lkp_EffectiveDate) AS o_RunDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	v_ChangeFlag AS o_ChangeFlag,
	-- *INF*: TO_DATE('01/01/1800 0','MM/DD/YYYY SSSSS')
	TO_DATE('01/01/1800 0', 'MM/DD/YYYY SSSSS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 86399','MM/DD/YYYY SSSSS')
	TO_DATE('12/31/2100 86399', 'MM/DD/YYYY SSSSS') AS ExpirationDate,
	'1' AS CurrentSnapshotFlag
	FROM EXPTRANS
	LEFT JOIN LKP_WorkRatingModifier
	ON LKP_WorkRatingModifier.WorkRatingModifierHashKey = EXPTRANS.WorkRatingModifierHashKey AND LKP_WorkRatingModifier.EffectiveDate <= EXPTRANS.RunDate AND LKP_WorkRatingModifier.ExpirationDate > EXPTRANS.RunDate
),
JNRTRANS AS (SELECT
	SRTTRANS.PremiumTransactionAKId, 
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
	PremiumTransactionAKId
	FROM (
		SELECT A.PremiumTransactionAKId as PremiumTransactionAKId FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransactionRatingModifierBridge A
		INNER HASH JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction C
		ON A.PremiumTransactionAKId=C.PremiumTransactionAKId
		AND C.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
		AND C.SourceSystemId='PMS'
		AND A.SourceSystemId='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId ORDER BY PremiumTransactionAKId) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_WorkPremiumTransactionRatingModifierBridge.PremiumTransactionAKId AS lkp_PremiumTransactionID,
	JNRTRANS.PremiumTransactionAKId,
	JNRTRANS.WorkRatingModifierAKId,
	JNRTRANS.RunDate,
	-1 AS o_PremiumMasterCalculationId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(lkp_PremiumTransactionID),'NEW','NOCHANGE')
	IFF(lkp_PremiumTransactionID IS NULL, 'NEW', 'NOCHANGE') AS o_ChangeFlag
	FROM JNRTRANS
	LEFT JOIN LKP_WorkPremiumTransactionRatingModifierBridge
	ON LKP_WorkPremiumTransactionRatingModifierBridge.PremiumTransactionAKId = JNRTRANS.PremiumTransactionAKId
),
RTR_INSERT_UPDATE AS (
	SELECT
	o_PremiumMasterCalculationId AS PremiumMasterCalculationId,
	PremiumTransactionAKId,
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
	PREMIUMTRANSACTIONAKID, 
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
	'1800-01-01 0' AS EffectiveDate,
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