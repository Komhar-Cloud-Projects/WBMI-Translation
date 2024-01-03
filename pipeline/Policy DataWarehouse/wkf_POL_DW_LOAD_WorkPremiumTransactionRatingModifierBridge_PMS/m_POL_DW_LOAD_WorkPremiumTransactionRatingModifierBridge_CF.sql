WITH
LKP_ArchPif43NXCPStage AS (
	SELECT
	ArchPif43NXCPStageId,
	PifPolicyNumber,
	Pmdnxp1LocationNumber,
	Pmdnxp1SubLocationNumber
	FROM (
		SELECT 
		RTRIM(pifsymbol)+pifpolicynumber+PifModule as PifPolicyNumber,
		right('0000'+convert(varchar(10),Pmdnxp1LocationNumber),4) as Pmdnxp1LocationNumber,
		right('000'+convert(varchar(10),Pmdnxp1SubLocationNumber),3) as Pmdnxp1SubLocationNumber,
		ArchPif43NXCPStageId as ArchPif43NXCPStageId
		FROM ArchPif43NXCPStage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.dbo.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(pifsymbol)+pifpolicynumber+PifModule=b.PolicyKey
		WHERE ArchPif43NXCPStageId IN
		(SELECT MAX(ArchPif43NXCPStageId)
		FROM ArchPif43NXCPStage
		where Pmdnxp1SegmentStatus='A'
		GROUP BY RTRIM(pifsymbol)+pifpolicynumber+PifModule,Pmdnxp1LocationNumber,Pmdnxp1SubLocationNumber)
		and Pmdnxp1SegmentStatus='A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifPolicyNumber,Pmdnxp1LocationNumber,Pmdnxp1SubLocationNumber ORDER BY ArchPif43NXCPStageId) = 1
),
LKP_ArchPif43NXCPStage_Final AS (
	SELECT
	ArchPif43NXCPStageId,
	PifPolicyNumber
	FROM (
		SELECT 
		RTRIM(pifsymbol)+pifpolicynumber+PifModule as PifPolicyNumber,
		ArchPif43NXCPStageId as ArchPif43NXCPStageId
		FROM ArchPif43NXCPStage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.dbo.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(pifsymbol)+pifpolicynumber+PifModule=b.PolicyKey
		WHERE ArchPif43NXCPStageId IN
		(SELECT MAX(ArchPif43NXCPStageId)
		FROM ArchPif43NXCPStage
		where Pmdnxp1SegmentStatus='A'
		GROUP BY RTRIM(pifsymbol)+pifpolicynumber+PifModule,Pmdnxp1LocationNumber,Pmdnxp1SubLocationNumber)
		and Pmdnxp1SegmentStatus='A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifPolicyNumber ORDER BY ArchPif43NXCPStageId) = 1
),
LKP_ArchPif43NXCPStage_Location AS (
	SELECT
	ArchPif43NXCPStageId,
	PifPolicyNumber,
	Pmdnxp1LocationNumber
	FROM (
		SELECT 
		RTRIM(pifsymbol)+pifpolicynumber+PifModule as PifPolicyNumber,
		right('0000'+convert(varchar(10),Pmdnxp1LocationNumber),4) as Pmdnxp1LocationNumber,
		ArchPif43NXCPStageId as ArchPif43NXCPStageId
		FROM ArchPif43NXCPStage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.dbo.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(pifsymbol)+pifpolicynumber+PifModule=b.PolicyKey
		WHERE ArchPif43NXCPStageId IN
		(SELECT MAX(ArchPif43NXCPStageId)
		FROM ArchPif43NXCPStage
		where Pmdnxp1SegmentStatus='A'
		GROUP BY RTRIM(pifsymbol)+pifpolicynumber+PifModule,Pmdnxp1LocationNumber,Pmdnxp1SubLocationNumber)
		and Pmdnxp1SegmentStatus='A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifPolicyNumber,Pmdnxp1LocationNumber ORDER BY ArchPif43NXCPStageId) = 1
),
SQ_PMS AS (
	SELECT
	P.pol_key,
	PT.PremiumTransactionAKID,
	PT.PremiumTransactionEnteredDate,
	PT.PremiumTransactionEffectiveDate,
	--BS.BureauCode7, 
	CASE	WHEN SC.MajorPerilCode = '919' THEN '100'
	ELSE BS.BureauCode7
	END BureauCode7, 
	RL.LocationUnitNumber,
	SC.SubLocationUnitNumber
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	JOIN
	@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode BS
	ON
	BS.PremiumTransactionAKID=PT.PremiumTransactionAKID
	AND BS.CurrentSnapshotFlag=1
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	ON
	PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID 
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON
	SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	AND PC.InsuranceLine='CF'
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	ON RL.RiskLocationAKId=PC.RiskLocationAKId
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
	ON
	PC.PolicyAKID = P.pol_ak_id AND P.crrnt_snpsht_flag = 1 AND P.pms_pol_lob_code<>'BND'
	WHERE PT.CurrentSnapshotFlag=1
	and PT.SourceSystemID = 'PMS'
	and PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	and PT.ReasonAmendedCode not in ('COL','CWO','Claw Back')
	and PT.PremiumType = 'D'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Modifiers AS (
	SELECT
	pol_key AS i_PolicyKey,
	PremiumTransactionAKID,
	PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	BureauCode7 AS i_BureauCode7,
	-- *INF*: IIF(i_BureauCode7='100',0,1)
	IFF(i_BureauCode7 = '100', 0, 1) AS v_ModifiedTransactionFlag,
	-- *INF*: IIF(v_ModifiedTransactionFlag=0, -1, :LKP.LKP_ArchPif43NXCPStage(i_PolicyKey,i_LocationUnitNumber,i_SubLocationUnitNumber))
	IFF(v_ModifiedTransactionFlag = 0, - 1, LKP_ARCHPIF43NXCPSTAGE_i_PolicyKey_i_LocationUnitNumber_i_SubLocationUnitNumber.ArchPif43NXCPStageId) AS v_ArchPif43NXCPStageId,
	-- *INF*: IIF(NOT ISNULL(v_ArchPif43NXCPStageId), v_ArchPif43NXCPStageId, :LKP.LKP_ArchPif43NXCPStage_LOCATION(i_PolicyKey,i_LocationUnitNumber))
	IFF(NOT v_ArchPif43NXCPStageId IS NULL, v_ArchPif43NXCPStageId, LKP_ARCHPIF43NXCPSTAGE_LOCATION_i_PolicyKey_i_LocationUnitNumber.ArchPif43NXCPStageId) AS v_ArchPif43NXCPStageId_Location,
	-- *INF*: IIF(NOT ISNULL(v_ArchPif43NXCPStageId_Location), v_ArchPif43NXCPStageId_Location, :LKP.LKP_ArchPif43NXCPStage_FINAL(i_PolicyKey))
	-- 
	IFF(NOT v_ArchPif43NXCPStageId_Location IS NULL, v_ArchPif43NXCPStageId_Location, LKP_ARCHPIF43NXCPSTAGE_FINAL_i_PolicyKey.ArchPif43NXCPStageId) AS v_ArchPif43NXCPStageId_Final,
	-- *INF*: DECODE(TRUE,
	-- v_ModifiedTransactionFlag=0,'Default',
	-- NOT ISNULL(v_ArchPif43NXCPStageId),i_PolicyKey||'&CF&'||i_LocationUnitNumber||'&'||i_SubLocationUnitNumber,
	-- NOT ISNULL(v_ArchPif43NXCPStageId_Location),i_PolicyKey||'&CF&'||i_LocationUnitNumber,
	-- i_PolicyKey||'&CF')
	DECODE(TRUE,
	v_ModifiedTransactionFlag = 0, 'Default',
	NOT v_ArchPif43NXCPStageId IS NULL, i_PolicyKey || '&CF&' || i_LocationUnitNumber || '&' || i_SubLocationUnitNumber,
	NOT v_ArchPif43NXCPStageId_Location IS NULL, i_PolicyKey || '&CF&' || i_LocationUnitNumber,
	i_PolicyKey || '&CF') AS v_WorkRatingModifierKey,
	v_WorkRatingModifierKey AS o_WorkRatingModifierKey,
	-- *INF*: IIF(v_ModifiedTransactionFlag=0,'Default',MD5(v_WorkRatingModifierKey))
	IFF(v_ModifiedTransactionFlag = 0, 'Default', MD5(v_WorkRatingModifierKey)) AS o_WorkRatingModifierHashKey,
	v_ArchPif43NXCPStageId_Final AS o_ArchPif43NXCPStageId,
	-- *INF*: IIF(v_ModifiedTransactionFlag=0,TO_DATE('18000101','YYYYMMDD'),ADD_TO_DATE(TRUNC(GREATEST(i_PremiumTransactionEnteredDate,PremiumTransactionEffectiveDate),'DD'),'SS',86399))
	IFF(v_ModifiedTransactionFlag = 0, TO_DATE('18000101', 'YYYYMMDD'), ADD_TO_DATE(TRUNC(GREATEST(i_PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate), 'DD'), 'SS', 86399)) AS o_PremiumTransactionBookedDate,
	SubLocationUnitNumber AS i_SubLocationUnitNumber,
	LocationUnitNumber AS i_LocationUnitNumber
	FROM SQ_PMS
	LEFT JOIN LKP_ARCHPIF43NXCPSTAGE LKP_ARCHPIF43NXCPSTAGE_i_PolicyKey_i_LocationUnitNumber_i_SubLocationUnitNumber
	ON LKP_ARCHPIF43NXCPSTAGE_i_PolicyKey_i_LocationUnitNumber_i_SubLocationUnitNumber.PifPolicyNumber = i_PolicyKey
	AND LKP_ARCHPIF43NXCPSTAGE_i_PolicyKey_i_LocationUnitNumber_i_SubLocationUnitNumber.Pmdnxp1LocationNumber = i_LocationUnitNumber
	AND LKP_ARCHPIF43NXCPSTAGE_i_PolicyKey_i_LocationUnitNumber_i_SubLocationUnitNumber.Pmdnxp1SubLocationNumber = i_SubLocationUnitNumber

	LEFT JOIN LKP_ARCHPIF43NXCPSTAGE_LOCATION LKP_ARCHPIF43NXCPSTAGE_LOCATION_i_PolicyKey_i_LocationUnitNumber
	ON LKP_ARCHPIF43NXCPSTAGE_LOCATION_i_PolicyKey_i_LocationUnitNumber.PifPolicyNumber = i_PolicyKey
	AND LKP_ARCHPIF43NXCPSTAGE_LOCATION_i_PolicyKey_i_LocationUnitNumber.Pmdnxp1LocationNumber = i_LocationUnitNumber

	LEFT JOIN LKP_ARCHPIF43NXCPSTAGE_FINAL LKP_ARCHPIF43NXCPSTAGE_FINAL_i_PolicyKey
	ON LKP_ARCHPIF43NXCPSTAGE_FINAL_i_PolicyKey.PifPolicyNumber = i_PolicyKey

),
SRTTRANS AS (
	SELECT
	PremiumTransactionAKID, 
	PremiumTransactionEffectiveDate, 
	o_WorkRatingModifierKey AS WorkRatingModifierKey, 
	o_WorkRatingModifierHashKey AS WorkRatingModifierHashKey, 
	o_ArchPif43NXCPStageId AS ArchPif43NXCPStageId, 
	o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate
	FROM EXP_Modifiers
	ORDER BY WorkRatingModifierHashKey ASC
),
AGGTRANS AS (
	SELECT
	PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate, 
	WorkRatingModifierHashKey, 
	WorkRatingModifierKey, 
	ArchPif43NXCPStageId, 
	PremiumTransactionBookedDate AS i_PremiumTransactionBookedDate, 
	MIN(i_PremiumTransactionBookedDate) AS o_RunDate, 
	MIN(i_PremiumTransactionEffectiveDate) AS o_RatingModifierEffectiveDate
	FROM SRTTRANS
	GROUP BY WorkRatingModifierHashKey
),
LKP_ArchPif43NXCPStage_Modifers AS (
	SELECT
	ArchPif43NXCPStageId,
	Pmdnxp1Irpm,
	Pmdnxp1OtherMod,
	i_ArchPif43NXCPStageId
	FROM (
		SELECT 
		ArchPif43NXCPStageId as ArchPif43NXCPStageId,
		Pmdnxp1Irpm as Pmdnxp1Irpm, 
		Pmdnxp1OtherMod as Pmdnxp1OtherMod
		FROM ArchPif43NXCPStage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.dbo.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(pifsymbol)+pifpolicynumber+PifModule=b.PolicyKey
		WHERE ArchPif43NXCPStageId IN
		(SELECT MAX(ArchPif43NXCPStageId)
		FROM ArchPif43NXCPStage
		where Pmdnxp1SegmentStatus='A'
		GROUP BY RTRIM(pifsymbol)+pifpolicynumber+PifModule,Pmdnxp1LocationNumber,Pmdnxp1SubLocationNumber)
		and Pmdnxp1SegmentStatus='A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ArchPif43NXCPStageId ORDER BY ArchPif43NXCPStageId) = 1
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
		and substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,2)='CF'
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
	LKP_ArchPif43NXCPStage_Modifers.Pmdnxp1Irpm AS i_ScheduleModifiedFactor,
	LKP_ArchPif43NXCPStage_Modifers.Pmdnxp1OtherMod AS i_OtherModifiedFactor,
	AGGTRANS.WorkRatingModifierHashKey,
	AGGTRANS.WorkRatingModifierKey,
	AGGTRANS.o_RunDate AS RunDate,
	AGGTRANS.o_RatingModifierEffectiveDate AS RatingModifierEffectiveDate,
	-- *INF*: IIF(ISNULL(lkp_WorkRatingModifierAKId),i_NEXTVAL,lkp_WorkRatingModifierAKId)
	IFF(lkp_WorkRatingModifierAKId IS NULL, i_NEXTVAL, lkp_WorkRatingModifierAKId) AS WorkRatingModifierAKId,
	-- *INF*: IIF(NOT ISNULL(i_OtherModifiedFactor) AND i_OtherModifiedFactor>0,i_OtherModifiedFactor,1)
	IFF(NOT i_OtherModifiedFactor IS NULL AND i_OtherModifiedFactor > 0, i_OtherModifiedFactor, 1) AS o_OtherModifiedFactor,
	-- *INF*: IIF(NOT ISNULL(i_ScheduleModifiedFactor) AND i_ScheduleModifiedFactor>0,i_ScheduleModifiedFactor,1)
	IFF(NOT i_ScheduleModifiedFactor IS NULL AND i_ScheduleModifiedFactor > 0, i_ScheduleModifiedFactor, 1) AS o_ScheduleModifiedFactor,
	1 AS o_ExperienceModifiedFactor
	FROM AGGTRANS
	LEFT JOIN LKP_ArchPif43NXCPStage_Modifers
	ON LKP_ArchPif43NXCPStage_Modifers.ArchPif43NXCPStageId = AGGTRANS.ArchPif43NXCPStageId
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
		AND substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,2)='CF'
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
		AND C.SourceSystemId='PMS' AND A.SourceSystemId='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID ORDER BY PremiumTransactionAKID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_WorkPremiumTransactionRatingModifierBridge.PremiumTransactionAKID AS lkp_PremiumTransactionID,
	JNRTRANS.PremiumTransactionAKID,
	JNRTRANS.WorkRatingModifierAKId,
	JNRTRANS.RunDate,
	-1 AS o_PremiumMasterCalcID,
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
	PremiumTransactionAKID,
	o_AuditID AS AuditID,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	WorkRatingModifierAKId,
	RunDate,
	o_ChangeFlag AS ChangeFlag,
	o_PremiumMasterCalcID
	FROM EXP_DetectChanges
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='NEW'),
WorkPremiumTransactionRatingModifierBridge AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransactionRatingModifierBridge
	(PremiumMasterCalculationId, PremiumTransactionAKId, AuditId, SourceSystemID, CreatedDate, ModifiedDate, WorkRatingModifierAKId, RunDate)
	SELECT 
	o_PremiumMasterCalcID AS PREMIUMMASTERCALCULATIONID, 
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