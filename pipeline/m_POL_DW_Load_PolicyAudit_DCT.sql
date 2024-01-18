WITH
LKP_EDWAuditStatus AS (
	SELECT
	AuditableFlag,
	PolicyAKId,
	InsuranceLine,
	EffectiveDate
	FROM (
		SELECT 
			AuditableFlag,
			PolicyAKId,
			InsuranceLine,
			EffectiveDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAudit
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,InsuranceLine,EffectiveDate ORDER BY AuditableFlag DESC) = 1
),
LKP_WBCLLineStage AS (
	SELECT
	IsAuditable,
	PolicyKey,
	InsuranceLine,
	CreatedDate
	FROM (
		select distinct d.PolicyNumber+ISNULL(w.PolicyVersionFormatted,'00') as PolicyKey,
		c.Type as InsuranceLine,
		f.CreatedDate as CreatedDate,
		case when d.Auditable=1 and a.IsAuditable=1 then '1' else '0' end as IsAuditable
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLLineStage a
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLineStaging b
		on a.WBLineId=b.WB_LineId
		and a.SessionId=b.SessionId
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging c
		on b.LineId=c.LineId
		and b.SessionId=c.SessionId
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging d
		on c.PolicyId=d.PolicyId
		and c.SessionId=d.SessionId
		and d.Status<>'Quote'
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging w
		on w.PolicyId=d.PolicyId
		and w.SessionId=d.SessionId
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging f
		on f.SessionId=a.SessionId
		and f.State='committed'
		left hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging g
		on f.SessionId=g.SessionId
		and g.HistoryId>f.HistoryId
		where g.TransactionId is null
		order by d.PolicyNumber+ISNULL(w.PolicyVersionFormatted,'00'),c.Type,f.CreatedDate--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,InsuranceLine,CreatedDate ORDER BY IsAuditable DESC) = 1
),
LKP_LatestPremiumTransaction AS (
	SELECT
	PremiumTransactionCode,
	PolicyAKId
	FROM (
		SELECT distinct CO.PolicyAKID AS PolicyAKId,CO.PremiumTransactionCode AS PremiumTransactionCode
		from (
		select Row_number() over(partition by e.pol_ak_id order by  PremiumTransactionEnteredDate desc ) SEQ,
		e.Pol_AK_Id PolicyAKID,PremiumTransactionEnteredDate,a.PremiumTransactionCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction a with (nolock)
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy e with (nolock)
		on e.pol_ak_id=substring(a.PremiumTransactionKey,1,charindex('~',a.PremiumTransactionKey,1)-1)
		--SUBSTRING(a.PremiumTransactionKey,1,8)
		and e.crrnt_snpsht_flag=1 and e.source_sys_id='DCT' and A.SourceSystemID='DCT'
		inner join (select distinct W.PolicyNumber+RIGHT('0'+CONVERT(VARCHAR(2),ISNULL(W.PolicyVersion,0)),2) Policykey from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy W) W
		on e.pol_key=W.Policykey
		WHERE
		a.SourceSystemID='DCT'
		and a.OffsetOnsetCode <> 'Deprecated'
		and charindex('~',a.PremiumTransactionKey,1)>0
		and len(substring(a.PremiumTransactionKey,1,charindex('~',a.PremiumTransactionKey,1)-1))>0
		and a.ReasonAmendedCode  NOT IN  ( 'CWO','Claw Back')
		and PremiumTransactionCode not in ('RetrospectiveCalculation') 
		) CO
		where SEQ=1
		ORDER BY PolicyAKId,PremiumTransactionCode--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId ORDER BY PremiumTransactionCode) = 1
),
SQ_DCLimitStaging AS (
	WITH DCPolicy AS(
	SELECT C.pol_ak_id,
	C.pol_key,
	A.PolicyId,
	A.SessionId,
	A.Auditable
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging A
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging B
	ON A.PolicyId=B.PolicyId
	AND A.SessionId=B.SessionId
	AND A.Status<>'Quote'
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy C
	ON C.pol_key=A.PolicyNumber+RIGHT('0'+CONVERT(VARCHAR(2),ISNULL(PolicyVersion,0)),2)
	AND C.crrnt_snpsht_flag=1)
	select d.pol_ak_id as PolicyAKId,
	c.Type as InsuranceLine,
	e.AssignedAuditor,
	e.AuditType,
	a.AuditPeriod,
	e.HasCorrespondingFrontingPolicy,
	e.AuditTypePolicyPeriodOverride,
	e.AuditTypePermanentOverride,
	e.AssignedAuditorPolicyPeriodOverride,
	e.AssignedAuditorPermanentOverride,
	e.CloseAudit,
	ISNULL(f.TransactionDate,f.CreatedDate),
	d.pol_key,
	case when d.Auditable=1 and a.IsAuditable=1 then '1' else '0' end as IsAuditable,
	case when a.IsAuditable=1 then '1' else '0' 
	end as LineAuditable,
	dcl.City,
	dcl.StateProv,
	e.AuditablePremium,
	f.Type,
	e.NoncomplianceofWCPoolAudit
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLLineStage a
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLineStaging b
	on a.WBLineId=b.WB_LineId
	and a.SessionId=b.SessionId
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging c
	on b.LineId=c.LineId
	and b.SessionId=c.SessionId
	inner hash join DCPolicy d
	on c.PolicyId=d.PolicyId
	and c.SessionId=d.SessionId
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLPolicyStage e
	on d.SessionId=e.SessionId
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging f
	on f.SessionId=a.SessionId
	and f.State='committed'
	and not exists (
	select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging g
	where f.SessionId=g.SessionId
	and g.HistoryId>f.HistoryId)
	left hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging dcl
	on f.SessionId=dcl.SessionId
	and dcl.Description='Audit'
	where f.Type @{pipeline().parameters.EXCLUDE_SQ_TTYPE}
	order by d.pol_ak_id,c.Type,f.CreatedDate,a.SessionId
),
EXP_UnderlyingInfo AS (
	SELECT
	PolicyAKId AS i_PolicyAKId,
	InsuranceLine AS i_InsuranceLine,
	AssignedAuditor AS i_AssignedAuditor,
	AuditType AS i_AuditType,
	AuditPeriod AS i_AuditPeriod,
	HasCorrespondingFrontingPolicy AS i_HasCorrespondingFrontingPolicy,
	AuditTypePolicyPeriodOverride AS i_AuditTypePolicyPeriodOverride,
	AuditTypePermanentOverride AS i_AuditTypePermanentOverride,
	AssignedAuditorPolicyPeriodOverride AS i_AssignedAuditorPolicyPeriodOverride,
	AssignedAuditorPermanentOverride AS i_AssignedAuditorPermanentOverride,
	CloseAudit AS i_CloseAudit,
	CreatedDate AS i_CreatedDate,
	PolicyKey AS i_PolicyKey,
	IsAuditable AS i_IsAuditable,
	LineAuditable AS i_LineAuditable,
	City AS i_City,
	StateProv AS i_StateProv,
	AuditablePremium AS i_AuditablePremium,
	-- *INF*: IIF(i_IsAuditable='1', NULL, :LKP.LKP_WBCLLINESTAGE(i_PolicyKey,i_InsuranceLine,i_CreatedDate))
	IFF(
	    i_IsAuditable = '1', NULL,
	    LKP_WBCLLINESTAGE_i_PolicyKey_i_InsuranceLine_i_CreatedDate.IsAuditable
	) AS v_StagePrevAuditableFlag,
	-- *INF*: DECODE(TRUE,
	-- i_IsAuditable='1', NULL,
	-- NOT ISNULL(v_StagePrevAuditableFlag), v_StagePrevAuditableFlag,
	-- :LKP.LKP_EDWAUDITSTATUS(i_PolicyKey,i_InsuranceLine,i_CreatedDate))
	DECODE(
	    TRUE,
	    i_IsAuditable = '1', NULL,
	    v_StagePrevAuditableFlag IS NOT NULL, v_StagePrevAuditableFlag,
	    LKP_EDWAUDITSTATUS_i_PolicyKey_i_InsuranceLine_i_CreatedDate.AuditableFlag
	) AS v_EDWPrevAuditableFlag,
	-- *INF*: IIF(v_EDWPrevAuditableFlag='T','1',v_EDWPrevAuditableFlag)
	IFF(v_EDWPrevAuditableFlag = 'T', '1', v_EDWPrevAuditableFlag) AS v_PrevAuditableFlag,
	-- *INF*: :LKP.LKP_LATESTPREMIUMTRANSACTION(i_PolicyAKId)
	LKP_LATESTPREMIUMTRANSACTION_i_PolicyAKId.PremiumTransactionCode AS v_AuditStatus,
	Type,
	NoncomplianceofWCPoolAudit,
	i_PolicyAKId AS o_PolicyAKId,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_InsuranceLine)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_InsuranceLine) AS o_InsuranceLine,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_AssignedAuditor)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_AssignedAuditor) AS o_AssignedAuditor,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_AuditPeriod)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_AuditPeriod) AS o_AuditFrequency,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_AuditType)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_AuditType) AS o_AuditType,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_City)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_City) AS o_AuditContactCity,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_StateProv)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_StateProv) AS o_AuditContactStateAbbreviation,
	-- *INF*: IIF(NOT ISNULL(i_CreatedDate),i_CreatedDate,TO_DATE('1800-1-1','YYYY-MM-DD'))
	IFF(i_CreatedDate IS NOT NULL, i_CreatedDate, TO_TIMESTAMP('1800-1-1', 'YYYY-MM-DD')) AS o_CreatedDate,
	-- *INF*: DECODE(i_AuditTypePermanentOverride,'T','1','F','0','0')
	DECODE(
	    i_AuditTypePermanentOverride,
	    'T', '1',
	    'F', '0',
	    '0'
	) AS o_PermanentOverrideFlag,
	-- *INF*: DECODE(i_AuditTypePolicyPeriodOverride,'T','1','F','0','0')
	DECODE(
	    i_AuditTypePolicyPeriodOverride,
	    'T', '1',
	    'F', '0',
	    '0'
	) AS o_PolicyPeriodOverrideFlag,
	-- *INF*: DECODE(i_HasCorrespondingFrontingPolicy,'T','1','F','0','0')
	DECODE(
	    i_HasCorrespondingFrontingPolicy,
	    'T', '1',
	    'F', '0',
	    '0'
	) AS o_FrontingPolicyFlag,
	-- *INF*: DECODE(i_CloseAudit,'T','1','F','0','0')
	DECODE(
	    i_CloseAudit,
	    'T', '1',
	    'F', '0',
	    '0'
	) AS o_AuditCloseOutFlag,
	-- *INF*: IIF(IN(LTRIM(RTRIM(v_AuditStatus)),'FinalAudit','RevisedFinalAudit'),'Completed','NotCompleted')
	-- 
	-- --IIF(IN(LTRIM(RTRIM(:LKP.LKP_LATESTPREMIUMTRANSACTION(i_PolicyAKId))),'FinalAudit','RevisedFinalAudit'),'Completed','NotCompleted')
	-- 
	-- 
	IFF(
	    LTRIM(RTRIM(v_AuditStatus)) IN ('FinalAudit','RevisedFinalAudit'), 'Completed',
	    'NotCompleted'
	) AS o_AuditStatus,
	i_IsAuditable AS o_AuditableFlag,
	i_LineAuditable AS o_LineAuditableFlag,
	-- *INF*: DECODE(TRUE,
	-- i_AssignedAuditorPolicyPeriodOverride='T' AND i_AssignedAuditorPermanentOverride='T','PERM',
	-- i_AssignedAuditorPolicyPeriodOverride='T','POL',
	-- i_AssignedAuditorPermanentOverride='T','PERM',
	-- '')
	DECODE(
	    TRUE,
	    i_AssignedAuditorPolicyPeriodOverride = 'T' AND i_AssignedAuditorPermanentOverride = 'T', 'PERM',
	    i_AssignedAuditorPolicyPeriodOverride = 'T', 'POL',
	    i_AssignedAuditorPermanentOverride = 'T', 'PERM',
	    ''
	) AS o_AssignedAuditorOverideFlag,
	-- *INF*: DECODE(TRUE,
	-- i_AuditTypePolicyPeriodOverride='T' AND i_AuditTypePermanentOverride='T','PERM',
	-- i_AuditTypePolicyPeriodOverride='T','POL',
	-- i_AuditTypePermanentOverride='T','PERM',
	-- '')
	DECODE(
	    TRUE,
	    i_AuditTypePolicyPeriodOverride = 'T' AND i_AuditTypePermanentOverride = 'T', 'PERM',
	    i_AuditTypePolicyPeriodOverride = 'T', 'POL',
	    i_AuditTypePermanentOverride = 'T', 'PERM',
	    ''
	) AS o_AuditTypeOverrideFlag,
	-- *INF*: IIF(NOT ISNULL(i_AuditablePremium),ROUND(i_AuditablePremium,4),0)
	IFF(i_AuditablePremium IS NOT NULL, ROUND(i_AuditablePremium, 4), 0) AS o_AuditablePremium,
	-- *INF*: DECODE(TRUE,
	-- i_IsAuditable='1' OR i_LineAuditable='1', '1',
	-- v_PrevAuditableFlag='1' OR i_LineAuditable='1', '1',
	-- '0')
	DECODE(
	    TRUE,
	    i_IsAuditable = '1' OR i_LineAuditable = '1', '1',
	    v_PrevAuditableFlag = '1' OR i_LineAuditable = '1', '1',
	    '0'
	) AS o_FilterFlag
	FROM SQ_DCLimitStaging
	LEFT JOIN LKP_WBCLLINESTAGE LKP_WBCLLINESTAGE_i_PolicyKey_i_InsuranceLine_i_CreatedDate
	ON LKP_WBCLLINESTAGE_i_PolicyKey_i_InsuranceLine_i_CreatedDate.PolicyKey = i_PolicyKey
	AND LKP_WBCLLINESTAGE_i_PolicyKey_i_InsuranceLine_i_CreatedDate.InsuranceLine = i_InsuranceLine
	AND LKP_WBCLLINESTAGE_i_PolicyKey_i_InsuranceLine_i_CreatedDate.CreatedDate = i_CreatedDate

	LEFT JOIN LKP_EDWAUDITSTATUS LKP_EDWAUDITSTATUS_i_PolicyKey_i_InsuranceLine_i_CreatedDate
	ON LKP_EDWAUDITSTATUS_i_PolicyKey_i_InsuranceLine_i_CreatedDate.PolicyAKId = i_PolicyKey
	AND LKP_EDWAUDITSTATUS_i_PolicyKey_i_InsuranceLine_i_CreatedDate.InsuranceLine = i_InsuranceLine
	AND LKP_EDWAUDITSTATUS_i_PolicyKey_i_InsuranceLine_i_CreatedDate.EffectiveDate = i_CreatedDate

	LEFT JOIN LKP_LATESTPREMIUMTRANSACTION LKP_LATESTPREMIUMTRANSACTION_i_PolicyAKId
	ON LKP_LATESTPREMIUMTRANSACTION_i_PolicyAKId.PolicyAKId = i_PolicyAKId

),
FILTRANS AS (
	SELECT
	Type, 
	NoncomplianceofWCPoolAudit, 
	o_PolicyAKId AS PolicyAKId, 
	o_InsuranceLine AS InsuranceLine, 
	o_AssignedAuditor AS AssignedAuditor, 
	o_AuditFrequency AS AuditFrequency, 
	o_AuditType AS AuditType, 
	o_AuditContactCity AS AuditContactCity, 
	o_AuditContactStateAbbreviation AS AuditContactStateAbbreviation, 
	o_CreatedDate AS CreatedDate, 
	o_PermanentOverrideFlag AS PermanentOverrideFlag, 
	o_PolicyPeriodOverrideFlag AS PolicyPeriodOverrideFlag, 
	o_FrontingPolicyFlag AS FrontingPolicyFlag, 
	o_AuditCloseOutFlag AS AuditCloseOutFlag, 
	o_AuditStatus AS AuditStatus, 
	o_AuditableFlag AS AuditableFlag, 
	o_LineAuditableFlag AS LineAuditableFlag, 
	o_AssignedAuditorOverideFlag AS AssignedAuditorOverideFlag, 
	o_AuditTypeOverrideFlag AS AuditTypeOverrideFlag, 
	o_AuditablePremium AS AuditablePremium, 
	o_FilterFlag AS FilterFlag
	FROM EXP_UnderlyingInfo
	WHERE FilterFlag='1'
),
AGGTRANS AS (
	SELECT
	Type,
	NoncomplianceofWCPoolAudit,
	PolicyAKId,
	InsuranceLine,
	AssignedAuditor,
	AuditFrequency,
	AuditType,
	AuditContactCity,
	AuditContactStateAbbreviation,
	CreatedDate,
	PermanentOverrideFlag,
	PolicyPeriodOverrideFlag,
	FrontingPolicyFlag,
	AuditCloseOutFlag,
	AuditStatus,
	AuditableFlag,
	LineAuditableFlag,
	AssignedAuditorOverideFlag,
	AuditTypeOverrideFlag,
	AuditablePremium
	FROM FILTRANS
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId, InsuranceLine, CreatedDate ORDER BY NULL) = 1
),
LKP_PolicyAudit AS (
	SELECT
	PolicyAuditId,
	PolicyAuditAKId,
	HashValue,
	AuditablePremium,
	EffectiveDate,
	IsAuditableFlag,
	NoncomplianceofWCPoolAudit,
	PolicyAKId,
	InsuranceLine,
	ExpirationDate
	FROM (
		SELECT 
			PolicyAuditId,
			PolicyAuditAKId,
			HashValue,
			AuditablePremium,
			EffectiveDate,
			IsAuditableFlag,
			NoncomplianceofWCPoolAudit,
			PolicyAKId,
			InsuranceLine,
			ExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAudit
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,InsuranceLine,EffectiveDate,ExpirationDate ORDER BY PolicyAuditId) = 1
),
SEQ_PolicyAuditStatus AS (
	CREATE SEQUENCE SEQ_PolicyAuditStatus
	START = 1
	INCREMENT = 1;
),
EXPTRANS AS (
	SELECT
	LKP_PolicyAudit.PolicyAuditId AS lkp_PolicyAuditId,
	LKP_PolicyAudit.PolicyAuditAKId AS lkp_PolicyAuditAKId,
	LKP_PolicyAudit.HashValue AS lkp_HashKey,
	LKP_PolicyAudit.AuditablePremium AS lkp_AuditablePremium,
	LKP_PolicyAudit.EffectiveDate AS lkp_EffectiveDate,
	LKP_PolicyAudit.IsAuditableFlag AS lkp_IsAuditableFlag,
	LKP_PolicyAudit.NoncomplianceofWCPoolAudit AS lkp_NoncomplianceofWCPoolAudit,
	SEQ_PolicyAuditStatus.NEXTVAL AS i_NEXTVAL,
	AGGTRANS.CreatedDate AS i_CreatedDate,
	AGGTRANS.Type AS i_Type,
	AGGTRANS.NoncomplianceofWCPoolAudit AS i_NoncomplianceofWCPoolAudit,
	-- *INF*: DECODE(i_NoncomplianceofWCPoolAudit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_NoncomplianceofWCPoolAudit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_NoncomplianceofWCPoolAudit,
	AGGTRANS.PolicyAKId,
	AGGTRANS.InsuranceLine,
	AGGTRANS.AssignedAuditor,
	AGGTRANS.AuditFrequency,
	AGGTRANS.AuditType,
	AGGTRANS.AuditContactCity,
	AGGTRANS.AuditContactStateAbbreviation,
	AGGTRANS.PermanentOverrideFlag,
	AGGTRANS.PolicyPeriodOverrideFlag,
	AGGTRANS.FrontingPolicyFlag,
	AGGTRANS.AuditCloseOutFlag,
	AGGTRANS.AuditStatus,
	AGGTRANS.AuditableFlag,
	AGGTRANS.LineAuditableFlag,
	AGGTRANS.AssignedAuditorOverideFlag,
	AGGTRANS.AuditTypeOverrideFlag,
	AGGTRANS.AuditablePremium,
	-- *INF*: DECODE(lkp_IsAuditableFlag,'T',1,'F',0,'1',1,'0',0,0)
	-- 
	DECODE(
	    lkp_IsAuditableFlag,
	    'T', 1,
	    'F', 0,
	    '1', 1,
	    '0', 0,
	    0
	) AS v_lkp_IsAuditableFlag,
	-- *INF*: DECODE(LineAuditableFlag,'T',1,'F',0,'1',1,'0',0,0)
	DECODE(
	    LineAuditableFlag,
	    'T', 1,
	    'F', 0,
	    '1', 1,
	    '0', 0,
	    0
	) AS v_LineAuditableFlag,
	-- *INF*: MD5(AssignedAuditor||
	-- AuditFrequency||
	-- AuditType||
	-- AuditContactCity||
	-- AuditContactStateAbbreviation||
	-- PermanentOverrideFlag||
	-- PolicyPeriodOverrideFlag||
	-- FrontingPolicyFlag||
	-- AuditStatus||
	-- AuditCloseOutFlag||
	-- AssignedAuditorOverideFlag||
	-- AuditTypeOverrideFlag||
	-- i_Type||
	-- TO_CHAR(AuditablePremium)||i_NoncomplianceofWCPoolAudit)
	MD5(AssignedAuditor || AuditFrequency || AuditType || AuditContactCity || AuditContactStateAbbreviation || PermanentOverrideFlag || PolicyPeriodOverrideFlag || FrontingPolicyFlag || AuditStatus || AuditCloseOutFlag || AssignedAuditorOverideFlag || AuditTypeOverrideFlag || i_Type || TO_CHAR(AuditablePremium) || i_NoncomplianceofWCPoolAudit) AS v_HashKey,
	-- *INF*: DECODE(TRUE,PolicyAKId=v_prev_PolicyAKId AND InsuranceLine=v_prev_InsuranceLine AND v_HashKey=v_prev_HashKey AND
	-- v_LineAuditableFlag=v_prev_LineAuditableFlag,0,
	-- ISNULL(lkp_PolicyAuditAKId),1,
	-- (lkp_HashKey!=v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag) AND IN(i_Type,'New','Renew','Reissue','Rewrite'),2,
	-- (lkp_HashKey!=v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag) AND lkp_EffectiveDate=i_CreatedDate,2,
	-- (lkp_HashKey!=v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag) AND PolicyAKId=v_prev_PolicyAKId AND InsuranceLine=v_prev_InsuranceLine,3,
	-- (lkp_HashKey!=v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag) AND ISNULL(lkp_AuditablePremium),2,
	-- (lkp_HashKey!=v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag),3,
	-- lkp_HashKey=v_HashKey and lkp_EffectiveDate != i_CreatedDate,3,
	-- 0)
	DECODE(
	    TRUE,
	    PolicyAKId = v_prev_PolicyAKId AND InsuranceLine = v_prev_InsuranceLine AND v_HashKey = v_prev_HashKey AND v_LineAuditableFlag = v_prev_LineAuditableFlag, 0,
	    lkp_PolicyAuditAKId IS NULL, 1,
	    (lkp_HashKey != v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag) AND i_Type IN ('New','Renew','Reissue','Rewrite'), 2,
	    (lkp_HashKey != v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag) AND lkp_EffectiveDate = i_CreatedDate, 2,
	    (lkp_HashKey != v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag) AND PolicyAKId = v_prev_PolicyAKId AND InsuranceLine = v_prev_InsuranceLine, 3,
	    (lkp_HashKey != v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag) AND lkp_AuditablePremium IS NULL, 2,
	    (lkp_HashKey != v_HashKey OR v_lkp_IsAuditableFlag != v_LineAuditableFlag), 3,
	    lkp_HashKey = v_HashKey and lkp_EffectiveDate != i_CreatedDate, 3,
	    0
	) AS v_ChangeFlag,
	-- *INF*: DECODE(TRUE,
	-- PolicyAKId=v_prev_PolicyAKId AND InsuranceLine=v_prev_InsuranceLine,v_PolicyAuditAKId,
	-- v_ChangeFlag=1,i_NEXTVAL,
	-- lkp_PolicyAuditAKId)
	DECODE(
	    TRUE,
	    PolicyAKId = v_prev_PolicyAKId AND InsuranceLine = v_prev_InsuranceLine, v_PolicyAuditAKId,
	    v_ChangeFlag = 1, i_NEXTVAL,
	    lkp_PolicyAuditAKId
	) AS v_PolicyAuditAKId,
	PolicyAKId AS v_prev_PolicyAKId,
	InsuranceLine AS v_prev_InsuranceLine,
	v_HashKey AS v_prev_HashKey,
	v_LineAuditableFlag AS v_prev_LineAuditableFlag,
	v_ChangeFlag AS o_ChangeFlag,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_CreatedDate AS o_EffectiveDate,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS') AS o_ExpirationDate,
	'DCT' AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	v_HashKey AS o_HashKey,
	v_PolicyAuditAKId AS o_PolicyAuditAKId
	FROM AGGTRANS
	LEFT JOIN LKP_PolicyAudit
	ON LKP_PolicyAudit.PolicyAKId = AGGTRANS.PolicyAKId AND LKP_PolicyAudit.InsuranceLine = AGGTRANS.InsuranceLine AND LKP_PolicyAudit.EffectiveDate <= AGGTRANS.CreatedDate AND LKP_PolicyAudit.ExpirationDate >= AGGTRANS.CreatedDate
),
RTR_Insert AS (
	SELECT
	lkp_PolicyAuditId,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditId AS AuditId,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_HashKey AS HashKey,
	o_PolicyAuditAKId AS PolicyAuditAKId,
	o_NoncomplianceofWCPoolAudit AS NoncomplianceofWCPoolAudit,
	PolicyAKId,
	InsuranceLine,
	AssignedAuditor,
	AuditFrequency,
	AuditType,
	AuditContactCity,
	AuditContactStateAbbreviation,
	PermanentOverrideFlag,
	PolicyPeriodOverrideFlag,
	FrontingPolicyFlag,
	AuditCloseOutFlag,
	AuditStatus,
	AuditableFlag,
	AssignedAuditorOverideFlag,
	AuditTypeOverrideFlag,
	AuditablePremium,
	LineAuditableFlag AS IsAuditableFlag,
	o_ChangeFlag AS ChangeFlag
	FROM EXPTRANS
),
RTR_Insert_INSERT AS (SELECT * FROM RTR_Insert WHERE IN(ChangeFlag,1,3)),
RTR_Insert_UPDATE AS (SELECT * FROM RTR_Insert WHERE ChangeFlag=2),
TGT_PolicyAudit_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAudit
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, HashValue, PolicyAuditAKId, PolicyAKId, InsuranceLine, AssignedAuditor, AuditFrequency, AuditType, AuditContactCity, AuditContactStateAbbreviation, PermanentOverrideFlag, PolicyPeriodOverrideFlag, FrontingPolicyFlag, AuditCloseOutFlag, AuditStatus, AuditableFlag, AssignedAuditorOveride, AuditTypeOverride, AuditablePremium, IsAuditableFlag, NoncomplianceofWCPoolAudit)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HashKey AS HASHVALUE, 
	POLICYAUDITAKID, 
	POLICYAKID, 
	INSURANCELINE, 
	ASSIGNEDAUDITOR, 
	AUDITFREQUENCY, 
	AUDITTYPE, 
	AUDITCONTACTCITY, 
	AUDITCONTACTSTATEABBREVIATION, 
	PERMANENTOVERRIDEFLAG, 
	POLICYPERIODOVERRIDEFLAG, 
	FRONTINGPOLICYFLAG, 
	AUDITCLOSEOUTFLAG, 
	AUDITSTATUS, 
	AUDITABLEFLAG, 
	AssignedAuditorOverideFlag AS ASSIGNEDAUDITOROVERIDE, 
	AuditTypeOverrideFlag AS AUDITTYPEOVERRIDE, 
	AUDITABLEPREMIUM, 
	ISAUDITABLEFLAG, 
	NONCOMPLIANCEOFWCPOOLAUDIT
	FROM RTR_Insert_INSERT
),
UPD_CodeChange AS (
	SELECT
	lkp_PolicyAuditId AS PolicyAuditId, 
	CurrentSnapshotFlag, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemId, 
	CreatedDate, 
	ModifiedDate, 
	HashKey AS HashValue, 
	PolicyAuditAKId, 
	PolicyAKId, 
	InsuranceLine, 
	AssignedAuditor, 
	AuditFrequency, 
	AuditType, 
	AuditContactCity, 
	AuditContactStateAbbreviation, 
	PermanentOverrideFlag, 
	PolicyPeriodOverrideFlag, 
	FrontingPolicyFlag, 
	AuditCloseOutFlag, 
	AuditStatus, 
	AuditableFlag, 
	AssignedAuditorOverideFlag AS AssignedAuditorOveride, 
	AuditTypeOverrideFlag AS AuditTypeOverride, 
	AuditablePremium, 
	IsAuditableFlag, 
	NoncomplianceofWCPoolAudit
	FROM RTR_Insert_UPDATE
),
TGT_PolicyAudit_CodeChange AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAudit AS T
	USING UPD_CodeChange AS S
	ON T.PolicyAuditId = S.PolicyAuditId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.HashValue = S.HashValue, T.AssignedAuditor = S.AssignedAuditor, T.AuditFrequency = S.AuditFrequency, T.AuditType = S.AuditType, T.AuditContactCity = S.AuditContactCity, T.AuditContactStateAbbreviation = S.AuditContactStateAbbreviation, T.PermanentOverrideFlag = S.PermanentOverrideFlag, T.PolicyPeriodOverrideFlag = S.PolicyPeriodOverrideFlag, T.FrontingPolicyFlag = S.FrontingPolicyFlag, T.AuditCloseOutFlag = S.AuditCloseOutFlag, T.AuditStatus = S.AuditStatus, T.AuditableFlag = S.AuditableFlag, T.AssignedAuditorOveride = S.AssignedAuditorOveride, T.AuditTypeOverride = S.AuditTypeOverride, T.AuditablePremium = S.AuditablePremium, T.IsAuditableFlag = S.IsAuditableFlag, T.NoncomplianceofWCPoolAudit = S.NoncomplianceofWCPoolAudit
),
SQ_PolicyAudit AS (
	SELECT 
		a.PolicyAuditId, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.PolicyAuditAKId
	FROM 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAudit a
	WHERE
	PolicyAuditAKId  IN
		( SELECT PolicyAuditAKId  FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAudit
		WHERE CurrentSnapshotFlag = 1 GROUP BY PolicyAuditAKId HAVING count(*) > 1) 
	AND SourceSystemID='DCT'
	ORDER BY a.PolicyAuditAKId,a.EffectiveDate DESC,a.PolicyAuditId DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	PolicyAuditAKId AS i_PolicyAuditAKId,
	EffectiveDate AS eff_from_date,
	ExpirationDate AS orig_eff_to_date,
	PolicyAuditId,
	-- *INF*: DECODE(TRUE,
	-- i_PolicyAuditAKId = v_prev_PolicyAuditAKId ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(
	    TRUE,
	    i_PolicyAuditAKId = v_prev_PolicyAuditAKId, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	i_PolicyAuditAKId AS v_prev_PolicyAuditAKId,
	eff_from_date AS v_prev_eff_from_date,
	0 AS out_crrnt_snpsht_flag,
	v_eff_to_date AS out_eff_to_date,
	SYSDATE AS out_modified_date
	FROM SQ_PolicyAudit
),
FIL_FirstRow AS (
	SELECT
	orig_eff_to_date AS i_orig_eff_to_date, 
	PolicyAuditId, 
	out_crrnt_snpsht_flag AS crrnt_snpsht_flag, 
	out_eff_to_date AS eff_to_date, 
	out_modified_date AS modified_date
	FROM EXP_Lag_eff_from_date
	WHERE i_orig_eff_to_date != eff_to_date
),
UPD_policy AS (
	SELECT
	PolicyAuditId, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM FIL_FirstRow
),
TGT_PolicyAudit_Update AS (

	------------ PRE SQL ----------
	UPDATE A
	SET A.EffectiveDate='1800-1-1'
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAudit  A
	WHERE NOT EXISTS (
	SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAudit  B
	WHERE A.PolicyAuditAKId=B.PolicyAuditAKId
	and B.EffectiveDate<A.EffectiveDate)
	AND A.EffectiveDate>'1800-1-1'
	-------------------------------


	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAudit AS T
	USING UPD_policy AS S
	ON T.PolicyAuditId = S.PolicyAuditId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.crrnt_snpsht_flag, T.ExpirationDate = S.eff_to_date, T.ModifiedDate = S.modified_date
),