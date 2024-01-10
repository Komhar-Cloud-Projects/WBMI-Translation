WITH
SQ_PremiumTransaction AS (
	SELECT d.pol_key AS PolicyKey,
	d.pol_ak_id AS PolicyAKID,
	d.pol_eff_date as PolicyEffectiveDate,
	d.pol_exp_date as PolicyExpirationDate,
	cast(floor(cast(CASE WHEN a.PremiumTransactionEnteredDate>=a.PremiumTransactionEffectiveDate
	THEN a.PremiumTransactionEnteredDate ELSE a.PremiumTransactionEffectiveDate END as float)) as datetime) AS RunDate,
	SUM(a.FullTermPremium) as FullTermPremium,
	SUM(case when a.FullTermPremium<0 then a.FullTermPremium else 0 end) as NegativeFullTerm,
	MAX(a.PremiumTransactionEffectiveDate) as MaxTransactionEffectiveDate,
	MAX(CASE WHEN b.MajorPerilCode='517' and b.MajorPerilSequenceNumber='01' and a.ReasonAmendedCode in ('PAA', 'PAL', 'PAO', 'PAP', 'PBC', 'PCA', 'PCD', 'PCL', 'PCM', 'PCO', 'PCP', 'PCV', 'PCW', 'PIF', 'PIL', 'PIO', 'PIR', 'PNB', 'PNE', 'PNP', 'PPC', 'PPF', 'PRW', 'PSD', 'PTA', 'PUN', 'PUW', 'PZZ', 'SIF', 'SIL', 'SIO') THEN a.ReasonAmendedCode
	WHEN a.PremiumTransactionCode IN ('20','21','23','25', '28', '29') THEN a.ReasonAmendedCode 
	WHEN a.PremiumTransactionCode IN ('12', '22') and a.ReasonAmendedCode in ('PAA', 'PAL', 'PAO', 'PAP', 'PBC', 'PCA', 'PCD', 'PCL', 'PCM', 'PCO', 'PCP', 'PCV', 'PCW', 'PIF', 'PIL', 'PIO', 'PIR', 'PNB', 'PNE', 'PNP', 'PPC', 'PPF', 'PRW', 'PSD', 'PTA', 'PUN', 'PUW', 'PZZ', 'SIF', 'SIL', 'SIO') THEN a.ReasonAmendedCode 
	ELSE NULL END) AS ReasonAmendedCode,
	--2 for Cancel Umbrella Policy, 1 for Other Umbrella Policy, 0 for Non-Umbrella Policy
	MAX(CASE WHEN b.MajorPerilCode='517' and b.MajorPerilSequenceNumber='01' and a.ReasonAmendedCode in ('PAA', 'PAL', 'PAO', 'PAP', 'PBC', 'PCA', 'PCD', 'PCL', 'PCM', 'PCO', 'PCP', 'PCV', 'PCW', 'PIF', 'PIL', 'PIO', 'PIR', 'PNB', 'PNE', 'PNP', 'PPC', 'PPF', 'PRW', 'PSD', 'PTA', 'PUN', 'PUW', 'PZZ', 'SIF', 'SIL', 'SIO') THEN 2
	WHEN b.MajorPerilCode='517'  THEN 1 ELSE 0 END) as IsUmbrella, 
	MAX(a.PremiumTransactionEnteredDate) as MaxTransactionEnteredDate
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction a
	join
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage b
	on a.StatisticalCoverageAKID=b.StatisticalCoverageAKID
	join
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage c
	on b.PolicyCoverageAKID=c.PolicyCoverageAKID
	join
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy d
	on c.PolicyAKID=d.pol_ak_id
	and d.crrnt_snpsht_flag=1
	and d.pol_eff_date>='2001-1-1'
	WHERE a.SourceSystemId='PMS'
	AND a.CurrentSnapshotFlag=1
	GROUP BY d.pol_key,d.pol_ak_id,d.pol_eff_date,d.pol_exp_date,
	cast(floor(cast(CASE WHEN a.PremiumTransactionEnteredDate>=a.PremiumTransactionEffectiveDate
	THEN a.PremiumTransactionEnteredDate ELSE a.PremiumTransactionEffectiveDate END as float)) as datetime)
),
LKP_sup_reason_amended_code AS (
	SELECT
	StandardReasonAmendedCode,
	rsn_amended_code
	FROM (
		SELECT 
			StandardReasonAmendedCode,
			rsn_amended_code
		FROM sup_reason_amended_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code ORDER BY StandardReasonAmendedCode) = 1
),
EXP_DefaultValue AS (
	SELECT
	SQ_PremiumTransaction.PolicyKey AS i_PolicyKey,
	SQ_PremiumTransaction.PolicyAKID AS i_PolicyAKID,
	SQ_PremiumTransaction.PolicyEffectiveDate AS i_PolicyEffectiveDate,
	SQ_PremiumTransaction.PolicyExpirationDate AS i_PolicyExpirationDate,
	SQ_PremiumTransaction.RunDate AS i_RunDate,
	SQ_PremiumTransaction.FullTermPremium AS i_FullTermPremium,
	SQ_PremiumTransaction.NegativeFullTermPremium AS i_NegativeFullTermPremium,
	SQ_PremiumTransaction.MaxTransactionEffectiveDate AS i_MaxTransactionEffectiveDate,
	LKP_sup_reason_amended_code.StandardReasonAmendedCode AS i_StandardReasonAmendedCode,
	SQ_PremiumTransaction.MaxTransactionEnteredDate AS i_MaxTransactionEnteredDate,
	SQ_PremiumTransaction.IsUmbrella AS i_IsUmbrella,
	-- *INF*: IIF(ISNULL(i_StandardReasonAmendedCode),'N/A',i_StandardReasonAmendedCode)
	IFF(i_StandardReasonAmendedCode IS NULL,
		'N/A',
		i_StandardReasonAmendedCode
	) AS v_StandardReasonAmendedCode,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_PolicyKey AS o_PolicyKey,
	i_PolicyAKID AS o_PolicyAKID,
	i_PolicyEffectiveDate AS o_PolicyEffectiveDate,
	i_PolicyExpirationDate AS o_PolicyExpirationDate,
	i_RunDate AS o_RunDate,
	i_FullTermPremium AS o_FullTermPremium,
	i_NegativeFullTermPremium AS o_NegativeFullTermPremium,
	i_MaxTransactionEffectiveDate AS o_MaxTransactionEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- i_IsUmbrella=2, CONCAT(v_StandardReasonAmendedCode,'_UmbrellaCancel'),
	-- i_IsUmbrella=1, CONCAT(v_StandardReasonAmendedCode,'_Umbrella'),
	-- v_StandardReasonAmendedCode)
	DECODE(TRUE,
		i_IsUmbrella = 2, CONCAT(v_StandardReasonAmendedCode, '_UmbrellaCancel'
		),
		i_IsUmbrella = 1, CONCAT(v_StandardReasonAmendedCode, '_Umbrella'
		),
		v_StandardReasonAmendedCode
	) AS o_StandardReasonAmendedCode,
	i_MaxTransactionEnteredDate AS o_MaxTransactionEnteredDate
	FROM SQ_PremiumTransaction
	LEFT JOIN LKP_sup_reason_amended_code
	ON LKP_sup_reason_amended_code.rsn_amended_code = SQ_PremiumTransaction.ReasonAmendedCode
),
WorkPolicyCurrentStatus AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.Shortcut_to_WorkPolicyCurrentStatus;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Shortcut_to_WorkPolicyCurrentStatus
	(AuditId, CreatedDate, ModifiedDate, PolicyKey, PolicyAKId, PolicyEffectiveDate, PolicyExpirationDate, RunDate, FullTermPremium, NegativeFullTermPremium, MaxTransactionEffectiveDate, ReasonAmendedCode)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_PolicyKey AS POLICYKEY, 
	o_PolicyAKID AS POLICYAKID, 
	o_PolicyEffectiveDate AS POLICYEFFECTIVEDATE, 
	o_PolicyExpirationDate AS POLICYEXPIRATIONDATE, 
	o_RunDate AS RUNDATE, 
	o_FullTermPremium AS FULLTERMPREMIUM, 
	o_NegativeFullTermPremium AS NEGATIVEFULLTERMPREMIUM, 
	o_MaxTransactionEffectiveDate AS MAXTRANSACTIONEFFECTIVEDATE, 
	o_StandardReasonAmendedCode AS REASONAMENDEDCODE
	FROM EXP_DefaultValue
),