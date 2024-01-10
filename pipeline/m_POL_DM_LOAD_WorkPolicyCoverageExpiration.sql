WITH
SQ_PremiumTransaction AS (
	SELECT d.pol_ak_id AS PolicyAKID,
	cast(floor(cast(CASE WHEN a.PremiumTransactionEnteredDate>=a.PremiumTransactionExpirationDate
	THEN a.PremiumTransactionEnteredDate ELSE a.PremiumTransactionExpirationDate END as float)) as datetime) AS RunDate,
	-SUM(a.FullTermPremium) as FullTermPremium
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
	GROUP BY d.pol_ak_id,
	cast(floor(cast(CASE WHEN a.PremiumTransactionEnteredDate>=a.PremiumTransactionExpirationDate
	THEN a.PremiumTransactionEnteredDate ELSE a.PremiumTransactionExpirationDate END as float)) as datetime)
),
EXP_CalculationValue AS (
	SELECT
	PolicyAKID AS i_PolicyAKID,
	RunDate AS i_RunDate,
	FullTermPremium AS i_FullTermPremium,
	-- *INF*: IIF(i_PolicyAKID=v_prev_PolicyAKID,
	-- i_FullTermPremium+v_FullTermPremium,
	-- i_FullTermPremium)
	IFF(i_PolicyAKID = v_prev_PolicyAKID, i_FullTermPremium + v_FullTermPremium, i_FullTermPremium) AS v_FullTermPremium,
	i_PolicyAKID AS v_prev_PolicyAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_RunDate AS o_RunDate,
	i_PolicyAKID AS o_PolicyAKID,
	v_FullTermPremium AS o_FullTermPremium
	FROM SQ_PremiumTransaction
),
WorkPolicyCoverageExpiration AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.Shortcut_to_WorkPolicyCoverageExpiration;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Shortcut_to_WorkPolicyCoverageExpiration
	(AuditId, CreatedDate, ModifiedDate, RunDate, PolicyAKId, FullTermPremium)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_RunDate AS RUNDATE, 
	o_PolicyAKID AS POLICYAKID, 
	o_FullTermPremium AS FULLTERMPREMIUM
	FROM EXP_CalculationValue
),