WITH
LKP_ArchPif43NXCRStage_Location AS (
	SELECT
	IRPM,
	PolicyKey,
	LocationUnitNumber
	FROM (
		SELECT 
		RTRIM(pifsymbol)+pifpolicynumber+PifPolicyModule as PolicyKey,
		right('0000'+convert(varchar(10),PMDNXC1LocationNumber),4) as LocationUnitNumber,
		PMDNXC1ScheduleMod as IRPM
		FROM ArchPif43NXCRStage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(a.PifSymbol)+a.PifPolicyNumber+a.PifPolicyModule=b.PolicyKey
		WHERE ArchPif43NXCRStageId IN
		(SELECT MAX(ArchPif43NXCRStageId)
		FROM ArchPif43NXCRStage
		where PMDNXC1SegmentStatus='A'
		GROUP BY RTRIM(pifsymbol)+pifpolicynumber+PifPolicyModule,PMDNXC1LocationNumber,PMDNXC1SubLocationNumber)
		and PMDNXC1SegmentStatus='A'
		order by RTRIM(pifsymbol)+pifpolicynumber+PifPolicyModule,
		right('0000'+convert(varchar(10),PMDNXC1LocationNumber),4),
		ArchPif43NXCRStageId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationUnitNumber ORDER BY IRPM) = 1
),
LKP_ArchPif43NXCRStage AS (
	SELECT
	IRPM,
	PolicyKey,
	LocationUnitNumber,
	SubLocationUnitNumber
	FROM (
		SELECT 
		RTRIM(pifsymbol)+pifpolicynumber+PifPolicyModule as PolicyKey,
		right('0000'+convert(varchar(10),PMDNXC1LocationNumber),4) as LocationUnitNumber,
		right('000'+convert(varchar(10),PMDNXC1SubLocationNumber),3) as SubLocationUnitNumber,
		PMDNXC1ScheduleMod as IRPM
		FROM ArchPif43NXCRStage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(a.PifSymbol)+a.PifPolicyNumber+a.PifPolicyModule=b.PolicyKey
		WHERE ArchPif43NXCRStageId IN
		(SELECT MAX(ArchPif43NXCRStageId)
		FROM ArchPif43NXCRStage
		where PMDNXC1SegmentStatus='A'
		GROUP BY RTRIM(pifsymbol)+pifpolicynumber+PifPolicyModule,PMDNXC1LocationNumber,PMDNXC1SubLocationNumber)
		and PMDNXC1SegmentStatus='A'
		order by RTRIM(pifsymbol)+pifpolicynumber+PifPolicyModule,
		right('0000'+convert(varchar(10),PMDNXC1LocationNumber),4),
		right('000'+convert(varchar(10),PMDNXC1SubLocationNumber),3)
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,LocationUnitNumber,SubLocationUnitNumber ORDER BY IRPM) = 1
),
LKP_ArchPif43NXCRStage_Final AS (
	SELECT
	IRPM,
	PolicyKey
	FROM (
		SELECT 
		RTRIM(pifsymbol)+pifpolicynumber+PifPolicyModule as PolicyKey,
		PMDNXC1ScheduleMod as IRPM
		FROM ArchPif43NXCRStage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(a.PifSymbol)+a.PifPolicyNumber+a.PifPolicyModule=b.PolicyKey
		WHERE ArchPIF43NXCRStageId IN
		(SELECT MAX(ArchPif43NXCRStageId)
		FROM ArchPif43NXCRStage
		where PMDNXC1SegmentStatus='A'
		GROUP BY RTRIM(pifsymbol)+pifpolicynumber+PifPolicyModule,PMDNXC1LocationNumber,PMDNXC1SubLocationNumber)
		and PMDNXC1SegmentStatus='A'
		order by RTRIM(pifsymbol)+pifpolicynumber+PifPolicyModule,
		ArchPif43NXCRStageId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY IRPM) = 1
),
SQ_PMS AS (
	select P.pol_key,RL.LocationUnitNumber,SC.SubLocationUnitNumber,PT.PremiumTransactionAKID,PT.PremiumTransactionEnteredDate,PT.PremiumTransactionEffectiveDate,PC.TypeBureauCode,BS.BureauCode7,BS.BureauCode9,BS.BureauCode12
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode BS
	ON
	BS.PremiumTransactionAKId=PT.PremiumTransactionAKId
	AND BS.CurrentSnapshotFlag=1
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	ON
	PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID 
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON
	SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	AND PC.InsuranceLine='CR'
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	ON
	RL.RiskLocationAKId=PC.RiskLocationAKId
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
	ON
	RL.PolicyAKID = P.pol_ak_id AND P.crrnt_snpsht_flag = 1
	AND P.pms_pol_lob_code<>'BND'
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransaction WPT 
	ON PT.PremiumTransactionAKID=WPT.PremiumTransactionAKId
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
	LocationUnitNumber AS i_LocationUnitNumber,
	SubLocationUnitNumber AS i_SubLocationUnitNumber,
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	TypeBureauCode AS i_TypeBureauCode,
	BureauCode7 AS i_BureauCode7,
	BureauCode9 AS i_BureauCode9,
	BureauCode12 AS i_BureauCode12,
	-- *INF*: IIF(i_LocationUnitNumber!='N/A',i_LocationUnitNumber,'0000')
	IFF(i_LocationUnitNumber != 'N/A', i_LocationUnitNumber, '0000') AS v_LocationUnitNumber,
	-- *INF*: IIF(i_SubLocationUnitNumber!='N/A',i_SubLocationUnitNumber,'000')
	IFF(i_SubLocationUnitNumber != 'N/A', i_SubLocationUnitNumber, '000') AS v_SubLocationUnitNumber,
	-- *INF*: DECODE(TRUE,i_BureauCode12='100' and i_TypeBureauCode='CR',1,
	-- i_BureauCode9='100' and i_TypeBureauCode='FT',1,
	-- i_BureauCode7='100' and i_TypeBureauCode='BT',1,
	-- 0)
	DECODE(TRUE,
	i_BureauCode12 = '100' AND i_TypeBureauCode = 'CR', 1,
	i_BureauCode9 = '100' AND i_TypeBureauCode = 'FT', 1,
	i_BureauCode7 = '100' AND i_TypeBureauCode = 'BT', 1,
	0) AS v_DefaultFlag,
	-- *INF*: IIF(v_DefaultFlag=0,
	-- :LKP.LKP_ArchPif43NXCRStage(i_PolicyKey,v_LocationUnitNumber,v_SubLocationUnitNumber),
	-- 1)
	IFF(v_DefaultFlag = 0, LKP_ARCHPIF43NXCRSTAGE_i_PolicyKey_v_LocationUnitNumber_v_SubLocationUnitNumber.IRPM, 1) AS v_Modifier,
	-- *INF*: IIF(NOT ISNULL(v_Modifier),v_Modifier,:LKP.LKP_ArchPif43NXCRStage_Location(i_PolicyKey,v_LocationUnitNumber))
	IFF(NOT v_Modifier IS NULL, v_Modifier, LKP_ARCHPIF43NXCRSTAGE_LOCATION_i_PolicyKey_v_LocationUnitNumber.IRPM) AS v_Modifier_Location,
	-- *INF*: IIF(NOT ISNULL(v_Modifier_Location),v_Modifier_Location,:LKP.LKP_ArchPif43NXCRStage_Final(i_PolicyKey))
	IFF(NOT v_Modifier_Location IS NULL, v_Modifier_Location, LKP_ARCHPIF43NXCRSTAGE_FINAL_i_PolicyKey.IRPM) AS v_Modifier_Final,
	-- *INF*: IIF(v_DefaultFlag=0,
	-- i_PolicyKey||'&CR&'||v_LocationUnitNumber||'&'||v_SubLocationUnitNumber,
	-- 'Default')
	IFF(v_DefaultFlag = 0, i_PolicyKey || '&CR&' || v_LocationUnitNumber || '&' || v_SubLocationUnitNumber, 'Default') AS v_WorkRatingModifierKey,
	i_PremiumTransactionAKID AS o_PremiumTransactionID,
	-- *INF*: IIF(v_DefaultFlag=0,MD5(v_WorkRatingModifierKey),'Default')
	IFF(v_DefaultFlag = 0, MD5(v_WorkRatingModifierKey), 'Default') AS o_WorkRatingModifierHashKey,
	v_WorkRatingModifierKey AS o_WorkRatingModifierKey,
	-- *INF*: IIF(NOT ISNULL(v_Modifier_Final) AND v_Modifier_Final>0,v_Modifier_Final,1)
	IFF(NOT v_Modifier_Final IS NULL AND v_Modifier_Final > 0, v_Modifier_Final, 1) AS o_ScheduleModifiedFactor,
	-- *INF*: IIF(v_DefaultFlag=0,
	-- ADD_TO_DATE(TRUNC(GREATEST(i_PremiumTransactionEnteredDate,PremiumTransactionEffectiveDate),'DD'),'SS',86399),
	-- TO_DATE('18000101','YYYYMMDD'))
	IFF(v_DefaultFlag = 0, ADD_TO_DATE(TRUNC(GREATEST(i_PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate), 'DD'), 'SS', 86399), TO_DATE('18000101', 'YYYYMMDD')) AS o_PremiumTransactionBookedDate
	FROM SQ_PMS
	LEFT JOIN LKP_ARCHPIF43NXCRSTAGE LKP_ARCHPIF43NXCRSTAGE_i_PolicyKey_v_LocationUnitNumber_v_SubLocationUnitNumber
	ON LKP_ARCHPIF43NXCRSTAGE_i_PolicyKey_v_LocationUnitNumber_v_SubLocationUnitNumber.PolicyKey = i_PolicyKey
	AND LKP_ARCHPIF43NXCRSTAGE_i_PolicyKey_v_LocationUnitNumber_v_SubLocationUnitNumber.LocationUnitNumber = v_LocationUnitNumber
	AND LKP_ARCHPIF43NXCRSTAGE_i_PolicyKey_v_LocationUnitNumber_v_SubLocationUnitNumber.SubLocationUnitNumber = v_SubLocationUnitNumber

	LEFT JOIN LKP_ARCHPIF43NXCRSTAGE_LOCATION LKP_ARCHPIF43NXCRSTAGE_LOCATION_i_PolicyKey_v_LocationUnitNumber
	ON LKP_ARCHPIF43NXCRSTAGE_LOCATION_i_PolicyKey_v_LocationUnitNumber.PolicyKey = i_PolicyKey
	AND LKP_ARCHPIF43NXCRSTAGE_LOCATION_i_PolicyKey_v_LocationUnitNumber.LocationUnitNumber = v_LocationUnitNumber

	LEFT JOIN LKP_ARCHPIF43NXCRSTAGE_FINAL LKP_ARCHPIF43NXCRSTAGE_FINAL_i_PolicyKey
	ON LKP_ARCHPIF43NXCRSTAGE_FINAL_i_PolicyKey.PolicyKey = i_PolicyKey

),
SRTTRANS AS (
	SELECT
	PremiumTransactionEffectiveDate, 
	o_PremiumTransactionID AS PremiumTransactionAKID, 
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
	MIN(i_PremiumTransactionBookedDate) AS o_RunDate, 
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
		and substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,2)='CR'
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
		and substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,2)='CR'
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
	LKP_WorkPremiumTransactionRatingModifierBridge.PremiumTransactionAKID AS lkp_PremiumTransactionID,
	JNRTRANS.PremiumTransactionAKID,
	JNRTRANS.WorkRatingModifierAKId,
	JNRTRANS.RunDate,
	-1 AS o_PremiumMasterCalcId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(lkp_PremiumTransactionID),'NEW','NOCHANGE')
	IFF(lkp_PremiumTransactionID IS NULL, 'NEW', 'NOCHANGE') AS o_ChangeFlag
	FROM JNRTRANS
	LEFT JOIN LKP_WorkPremiumTransactionRatingModifierBridge
	ON LKP_WorkPremiumTransactionRatingModifierBridge.PremiumTransactionAKID = JNRTRANS.PremiumTransactionAKID
),
RTR_INSERT_UPDATE AS (
	SELECT
	PremiumTransactionAKID AS PremiumTransactionID,
	o_AuditID AS AuditID,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	WorkRatingModifierAKId,
	RunDate,
	o_ChangeFlag AS ChangeFlag,
	o_PremiumMasterCalcId
	FROM EXP_DetectChanges
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='NEW'),
WorkPremiumTransactionRatingModifierBridge AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransactionRatingModifierBridge
	(PremiumMasterCalculationId, PremiumTransactionAKId, AuditId, SourceSystemID, CreatedDate, ModifiedDate, WorkRatingModifierAKId, RunDate)
	SELECT 
	o_PremiumMasterCalcId AS PREMIUMMASTERCALCULATIONID, 
	PremiumTransactionID AS PREMIUMTRANSACTIONAKID, 
	AuditID AS AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	WORKRATINGMODIFIERAKID, 
	RUNDATE
	FROM RTR_INSERT_UPDATE_INSERT
),