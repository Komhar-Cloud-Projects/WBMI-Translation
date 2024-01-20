WITH
LKP_arch_pif_12_stage_state AS (
	SELECT
	Modifiers,
	PolicyKey,
	use_location,
	description_sequence,
	StateCode
	FROM (
		SELECT LTRIM(RTRIM(description_line_1))+LTRIM(RTRIM(description_line_2))+LTRIM(RTRIM(description_line_3))+LTRIM(RTRIM(description_line_4)) as Modifiers,
		RTRIM(pif_symbol)+pif_policy_number+pif_module as PolicyKey,
		LTRIM(RTRIM(use_location)) as use_location,
		description_sequence as description_sequence,
		LEFT( LTRIM(RTRIM(description_line_1)),2) as StateCode
		FROM arch_pif_12_stage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(a.pif_symbol)+a.pif_policy_number+a.pif_module=b.PolicyKey
		WHERE arch_pif_12_stage_id IN
		(SELECT MAX(arch_pif_12_stage_id)
		FROM arch_pif_12_stage
		WHERE use_code='CF'
		AND description_sequence in ('C','L','O')
		AND LEFT( LTRIM(RTRIM(description_line_1)),2)<>'99'
		GROUP BY RTRIM(pif_symbol)+pif_policy_number+pif_module,
		LTRIM(RTRIM(use_location)),
		description_sequence)
		ORDER BY RTRIM(pif_symbol)+pif_policy_number+pif_module,
		LTRIM(RTRIM(use_location)),
		description_sequence
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,use_location,description_sequence,StateCode ORDER BY Modifiers) = 1
),
LKP_arch_pif_12_stage AS (
	SELECT
	Modifiers,
	PolicyKey,
	use_location,
	description_sequence
	FROM (
		SELECT LTRIM(RTRIM(description_line_1))+LTRIM(RTRIM(description_line_2))+LTRIM(RTRIM(description_line_3))+LTRIM(RTRIM(description_line_4)) as Modifiers,
		RTRIM(pif_symbol)+pif_policy_number+pif_module as PolicyKey,
		LTRIM(RTRIM(use_location)) as use_location,
		description_sequence as description_sequence
		FROM arch_pif_12_stage a
		join (
		select distinct left(PremiumTransactionKey,12) PolicyKey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction
		where CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') b
		on RTRIM(a.pif_symbol)+a.pif_policy_number+a.pif_module=b.PolicyKey
		WHERE arch_pif_12_stage_id IN
		(SELECT MAX(arch_pif_12_stage_id)
		FROM arch_pif_12_stage
		WHERE use_code='CF'
		AND description_sequence in ('C','L','O')
		AND LEFT( LTRIM(RTRIM(description_line_1)),2)='99'
		GROUP BY RTRIM(pif_symbol)+pif_policy_number+pif_module,
		LTRIM(RTRIM(use_location)),
		description_sequence)
		ORDER BY RTRIM(pif_symbol)+pif_policy_number+pif_module,
		LTRIM(RTRIM(use_location)),
		description_sequence
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,use_location,description_sequence ORDER BY Modifiers) = 1
),
SQ_PMS AS (
	select P.pol_key,SC.RiskUnit,PC.TypeBureauCode,RL.StateProvinceCode,
	PT.PremiumTransactionAKID,PT.PremiumTransactionEnteredDate,PT.PremiumTransactionEffectiveDate,BS.BureauCode8,BS.BureauCode9
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
	and PC.TypeBureauCode IN ('AL','AN','AP') AND PC.InsuranceLine IN ('CA','N/A')
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	ON
	RL.RiskLocationAKId=PC.RiskLocationAKId
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
	ON
	RL.PolicyAKID = P.pol_ak_id AND P.crrnt_snpsht_flag = 1
	WHERE PT.CurrentSnapshotFlag=1
	AND PT.SourceSystemID = 'PMS'
	AND PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	AND PT.ReasonAmendedCode not in ('COL','CWO','Claw Back')
	AND PT.PremiumType = 'D'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Modifiers AS (
	SELECT
	pol_key AS i_PolicyKey,
	RiskUnit AS i_RiskUnit,
	TypeBureauCode AS i_TypeBureauCode,
	StateProvinceCode AS i_StateProvinceCode,
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	PremiumTransactionEnteredDate AS i_PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	BureauCode8 AS i_BureauCode8,
	BureauCode9 AS i_BureauCode9,
	-- *INF*: IIF(i_RiskUnit!='N/A',SUBSTR(i_RiskUnit,1,3),'')
	IFF(i_RiskUnit != 'N/A', SUBSTR(i_RiskUnit, 1, 3), '') AS v_RiskUnit,
	-- *INF*: IIF(i_TypeBureauCode='AP','C','L')
	IFF(i_TypeBureauCode = 'AP', 'C', 'L') AS v_DescriptionSequence,
	-- *INF*: DECODE(TRUE,
	-- i_BureauCode8='100' AND IN(i_TypeBureauCode,'AL','AP')=1,0,
	-- i_BureauCode9='100' AND i_TypeBureauCode='AN',0,
	-- 1)
	DECODE(
	    TRUE,
	    i_BureauCode8 = '100' AND i_TypeBureauCode IN ('AL','AP') = 1, 0,
	    i_BureauCode9 = '100' AND i_TypeBureauCode = 'AN', 0,
	    1
	) AS v_ModifiedTransactionFlag,
	-- *INF*: DECODE(TRUE,
	-- v_ModifiedTransactionFlag=0,'1',
	-- :LKP.LKP_ARCH_PIF_12_STAGE_STATE(i_PolicyKey,v_RiskUnit,v_DescriptionSequence,i_StateProvinceCode))
	DECODE(
	    TRUE,
	    v_ModifiedTransactionFlag = 0, '1',
	    LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence_i_StateProvinceCode.Modifiers
	) AS v_Modifier_State,
	-- *INF*: IIF(NOT ISNULL(v_Modifier_State),v_Modifier_State,:LKP.LKP_ARCH_PIF_12_STAGE_STATE(i_PolicyKey,'000',v_DescriptionSequence,i_StateProvinceCode))
	IFF(
	    v_Modifier_State IS NOT NULL, v_Modifier_State,
	    LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_000_v_DescriptionSequence_i_StateProvinceCode.Modifiers
	) AS v_Modifier_State_Default,
	-- *INF*: IIF(NOT ISNULL(v_Modifier_State_Default),v_Modifier_State_Default,:LKP.LKP_ARCH_PIF_12_STAGE(i_PolicyKey,v_RiskUnit,v_DescriptionSequence))
	IFF(
	    v_Modifier_State_Default IS NOT NULL, v_Modifier_State_Default,
	    LKP_ARCH_PIF_12_STAGE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence.Modifiers
	) AS v_Modifier,
	-- *INF*: IIF(NOT ISNULL(v_Modifier),v_Modifier,:LKP.LKP_ARCH_PIF_12_STAGE(i_PolicyKey,'000',v_DescriptionSequence))
	IFF(
	    v_Modifier IS NOT NULL, v_Modifier,
	    LKP_ARCH_PIF_12_STAGE_i_PolicyKey_000_v_DescriptionSequence.Modifiers
	) AS v_Modifier_Default,
	-- *INF*: IIF(REG_MATCH(v_Modifier_Default,'.*O=[^,]*,.*'),LTRIM(RTRIM(REG_EXTRACT(v_Modifier_Default,'(.*)O=([^,]*),(.*)',2))),'1')
	IFF(
	    REGEXP_LIKE(v_Modifier_Default, '.*O=[^,]*,.*'),
	    LTRIM(RTRIM(REG_EXTRACT(v_Modifier_Default, '(.*)O=([^,]*),(.*)', 2))),
	    '1'
	) AS v_O_Factor,
	-- *INF*: IIF(REG_MATCH(v_Modifier_Default,'.*E=[^,]*,.*'),LTRIM(RTRIM(REG_EXTRACT(v_Modifier_Default,'(.*)E=([^,]*),(.*)',2))),'1')
	IFF(
	    REGEXP_LIKE(v_Modifier_Default, '.*E=[^,]*,.*'),
	    LTRIM(RTRIM(REG_EXTRACT(v_Modifier_Default, '(.*)E=([^,]*),(.*)', 2))),
	    '1'
	) AS v_E_Factor,
	-- *INF*: IIF(REG_MATCH(v_Modifier_Default,'.*P=[^,]*,.*'),LTRIM(RTRIM(REG_EXTRACT(v_Modifier_Default,'(.*)P=([^,]*),(.*)',2))),'1')
	IFF(
	    REGEXP_LIKE(v_Modifier_Default, '.*P=[^,]*,.*'),
	    LTRIM(RTRIM(REG_EXTRACT(v_Modifier_Default, '(.*)P=([^,]*),(.*)', 2))),
	    '1'
	) AS v_P_Factor,
	-- *INF*: IIF(IS_NUMBER(v_O_Factor),TO_DECIMAL(v_O_Factor),1)
	IFF(REGEXP_LIKE(v_O_Factor, '^[0-9]+$'), CAST(v_O_Factor AS FLOAT), 1) AS v_OtherModifiedFactor,
	-- *INF*: IIF(IS_NUMBER(v_P_Factor),TO_DECIMAL(v_P_Factor),1)
	IFF(REGEXP_LIKE(v_P_Factor, '^[0-9]+$'), CAST(v_P_Factor AS FLOAT), 1) AS v_ScheduleModifiedFactor,
	-- *INF*: IIF(IS_NUMBER(v_E_Factor),TO_DECIMAL(v_E_Factor),1)
	IFF(REGEXP_LIKE(v_E_Factor, '^[0-9]+$'), CAST(v_E_Factor AS FLOAT), 1) AS v_ExperienceModifiedFactor,
	-- *INF*: DECODE(TRUE,
	-- v_ModifiedTransactionFlag=0,'Default',
	-- NOT ISNULL(v_Modifier_State),i_PolicyKey||v_RiskUnit||'&CA&'||v_DescriptionSequence||'&'||i_StateProvinceCode,
	-- NOT ISNULL(v_Modifier_State_Default),i_PolicyKey||'&CA&'||'000'||'&'||v_DescriptionSequence||'&'||i_StateProvinceCode,
	-- NOT ISNULL(v_Modifier),i_PolicyKey||'&CA&'||v_RiskUnit||'&'||v_DescriptionSequence,
	-- i_PolicyKey||'&CA&'||'000'||'&'||v_DescriptionSequence)
	DECODE(
	    TRUE,
	    v_ModifiedTransactionFlag = 0, 'Default',
	    v_Modifier_State IS NOT NULL, i_PolicyKey || v_RiskUnit || '&CA&' || v_DescriptionSequence || '&' || i_StateProvinceCode,
	    v_Modifier_State_Default IS NOT NULL, i_PolicyKey || '&CA&' || '000' || '&' || v_DescriptionSequence || '&' || i_StateProvinceCode,
	    v_Modifier IS NOT NULL, i_PolicyKey || '&CA&' || v_RiskUnit || '&' || v_DescriptionSequence,
	    i_PolicyKey || '&CA&' || '000' || '&' || v_DescriptionSequence
	) AS v_WorkRatingModifierKey,
	i_PremiumTransactionAKID AS o_PremiumTransactionAKID,
	-- *INF*: IIF(v_ModifiedTransactionFlag=0,'Default',MD5(v_WorkRatingModifierKey))
	IFF(v_ModifiedTransactionFlag = 0, 'Default', MD5(v_WorkRatingModifierKey)) AS o_WorkRatingModifierHashKey,
	v_WorkRatingModifierKey AS o_WorkRatingModifierKey,
	-- *INF*: IIF(v_OtherModifiedFactor>0,v_OtherModifiedFactor,1)
	IFF(v_OtherModifiedFactor > 0, v_OtherModifiedFactor, 1) AS o_OtherModifiedFactor,
	-- *INF*: IIF(v_ScheduleModifiedFactor>0,v_ScheduleModifiedFactor,1)
	IFF(v_ScheduleModifiedFactor > 0, v_ScheduleModifiedFactor, 1) AS o_ScheduleModifiedFactor,
	-- *INF*: IIF(v_ExperienceModifiedFactor>0,v_ExperienceModifiedFactor,1)
	IFF(v_ExperienceModifiedFactor > 0, v_ExperienceModifiedFactor, 1) AS o_ExperienceModifiedFactor,
	-- *INF*: IIF(v_ModifiedTransactionFlag=0,TO_DATE('18000101','YYYYMMDD'),ADD_TO_DATE(TRUNC(GREATEST(i_PremiumTransactionEnteredDate,PremiumTransactionEffectiveDate),'DD'),'SS',86399))
	IFF(
	    v_ModifiedTransactionFlag = 0, TO_TIMESTAMP('18000101', 'YYYYMMDD'),
	    DATEADD(SECOND,86399,CAST(TRUNC(GREATEST(i_PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate), 'DAY') AS TIMESTAMP_NTZ(0)))
	) AS o_PremiumTransactionBookedDate
	FROM SQ_PMS
	LEFT JOIN LKP_ARCH_PIF_12_STAGE_STATE LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence_i_StateProvinceCode
	ON LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence_i_StateProvinceCode.PolicyKey = i_PolicyKey
	AND LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence_i_StateProvinceCode.use_location = v_RiskUnit
	AND LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence_i_StateProvinceCode.description_sequence = v_DescriptionSequence
	AND LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence_i_StateProvinceCode.StateCode = i_StateProvinceCode

	LEFT JOIN LKP_ARCH_PIF_12_STAGE_STATE LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_000_v_DescriptionSequence_i_StateProvinceCode
	ON LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_000_v_DescriptionSequence_i_StateProvinceCode.PolicyKey = i_PolicyKey
	AND LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_000_v_DescriptionSequence_i_StateProvinceCode.use_location = '000'
	AND LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_000_v_DescriptionSequence_i_StateProvinceCode.description_sequence = v_DescriptionSequence
	AND LKP_ARCH_PIF_12_STAGE_STATE_i_PolicyKey_000_v_DescriptionSequence_i_StateProvinceCode.StateCode = i_StateProvinceCode

	LEFT JOIN LKP_ARCH_PIF_12_STAGE LKP_ARCH_PIF_12_STAGE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence
	ON LKP_ARCH_PIF_12_STAGE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence.PolicyKey = i_PolicyKey
	AND LKP_ARCH_PIF_12_STAGE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence.use_location = v_RiskUnit
	AND LKP_ARCH_PIF_12_STAGE_i_PolicyKey_v_RiskUnit_v_DescriptionSequence.description_sequence = v_DescriptionSequence

	LEFT JOIN LKP_ARCH_PIF_12_STAGE LKP_ARCH_PIF_12_STAGE_i_PolicyKey_000_v_DescriptionSequence
	ON LKP_ARCH_PIF_12_STAGE_i_PolicyKey_000_v_DescriptionSequence.PolicyKey = i_PolicyKey
	AND LKP_ARCH_PIF_12_STAGE_i_PolicyKey_000_v_DescriptionSequence.use_location = '000'
	AND LKP_ARCH_PIF_12_STAGE_i_PolicyKey_000_v_DescriptionSequence.description_sequence = v_DescriptionSequence

),
SRTTRANS AS (
	SELECT
	PremiumTransactionEffectiveDate, 
	o_PremiumTransactionAKID AS PremiumTransactionAKID, 
	o_WorkRatingModifierHashKey AS WorkRatingModifierHashKey, 
	o_WorkRatingModifierKey AS WorkRatingModifierKey, 
	o_OtherModifiedFactor AS OtherModifiedFactor, 
	o_ScheduleModifiedFactor AS ScheduleModifiedFactor, 
	o_ExperienceModifiedFactor AS ExperienceModifiedFactor, 
	o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate
	FROM EXP_Modifiers
	ORDER BY WorkRatingModifierHashKey ASC
),
AGGTRANS AS (
	SELECT
	PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	WorkRatingModifierHashKey,
	WorkRatingModifierKey,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
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
		and SourceSystemId='PMS'
		and substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,2)='CA'
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
	AGGTRANS.OtherModifiedFactor,
	AGGTRANS.ScheduleModifiedFactor,
	AGGTRANS.ExperienceModifiedFactor,
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
		and substring(WorkRatingModifierKey,charindex('&',WorkRatingModifierKey,1)+1,2)='CA'
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
	DECODE(
	    TRUE,
	    lkp_WorkRatingModifierHashKey IS NULL, 'NEW',
	    lkp_EffectiveDate = i_RunDate, 'NOCHANGE',
	    lkp_OtherModifiedFactor != OtherModifiedFactor, 'NEW',
	    lkp_ScheduleModifiedFactor != ScheduleModifiedFactor, 'NEW',
	    lkp_ExperienceModifiedFactor != ExperienceModifiedFactor, 'NEW',
	    'NOCHANGE'
	) AS v_ChangeFlag,
	-- *INF*: IIF(v_ChangeFlag='NEW',i_RunDate,lkp_EffectiveDate)
	IFF(v_ChangeFlag = 'NEW', i_RunDate, lkp_EffectiveDate) AS o_RunDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	v_ChangeFlag AS o_ChangeFlag,
	-- *INF*: TO_DATE('01/01/1800 0','MM/DD/YYYY SSSSS')
	TO_TIMESTAMP('01/01/1800 0', 'MM/DD/YYYY SSSSS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 86399','MM/DD/YYYY SSSSS')
	TO_TIMESTAMP('12/31/2100 86399', 'MM/DD/YYYY SSSSS') AS ExpirationDate,
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
		SELECT A.PremiumTransactionAKId as PremiumTransactionAKID FROM WorkPremiumTransactionRatingModifierBridge A
		JOIN PremiumTransaction C
		ON A.PremiumTransactionAKId=C.PremiumTransactionAKId
		AND C.CREATEDDATE>='@{pipeline().parameters.SELECTION_START_TS}'
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
	-1 AS o_PremiumMasterCalculationId,
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
	o_PremiumMasterCalculationId
	FROM EXP_DetectChanges
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='NEW'),
WorkPremiumTransactionRatingModifierBridge AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransactionRatingModifierBridge
	(PremiumMasterCalculationId, PremiumTransactionAKId, AuditId, SourceSystemID, CreatedDate, ModifiedDate, WorkRatingModifierAKId, RunDate)
	SELECT 
	o_PremiumMasterCalculationId AS PREMIUMMASTERCALCULATIONID, 
	PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	AuditID AS AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	WORKRATINGMODIFIERAKID, 
	RUNDATE
	FROM RTR_INSERT_UPDATE_INSERT
),