WITH
LKP_SupClassificationWorkersCompensation AS (
	SELECT
	SupClassificationWorkersCompensationId,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT 
			SupClassificationWorkersCompensationId,
			ClassCode,
			RatingStateCode
		FROM SupClassificationWorkersCompensation
		WHERE (SubjectToExperienceModificationClassIndicator = 'Y' or ExperienceModificationClassIndicator = 'Y') and CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY SupClassificationWorkersCompensationId) = 1
),
SQ_Get_Candidate_ExpModTransactions AS (
	-- identify exp mod factor and effective date changes across date bounds
	WITH  
	ClassCodeDrivers as
	(select distinct ClassCode
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupClassificationWorkersCompensation
	WHERE ExperienceModificationClassIndicator = 'Y' and currentsnapshotflag = 1)
	
	SELECT
	pc.PolicyAKID as PolicyAKID,
	rl.StateProvinceCode as StateProvinceCode,
	pt.BaseRate as ExperienceModificationFactor,
	pt.ExperienceModificationEffectiveDate as ExperienceModificationEffectiveDate,
	pt.PremiumTransactionEffectiveDate as PremiumTransactionEffectiveDate,
	pt.PremiumTransactionExpirationDate as PremiumTransactionExpirationDate,
	pt.PremiumTransactionEnteredDate as PremiumTransactionEnteredDate,
	pt.PremiumTransactionCode as PremiumTransactionCode,
	pt.OffsetOnsetCode as OffsetOnsetCode
	FROM
	 @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc
	on rc.RatingCoverageAKID=pt.RatingCoverageAKID and rc.EffectiveDate = pt.EffectiveDate
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc 
	on pc.PolicyCoverageAKID=rc.PolicyCoverageAKID and pc.CurrentSnapshotFlag = 1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation rl
	on rl.RiskLocationAKID=pc.RiskLocationAKID and rl.CurrentSnapshotFlag = 1
	inner join ClassCodeDrivers ccd on rc.ClassCode = ccd.ClassCode
	WHERE pc.TypeBureauCode in ('WC','WP','WorkersCompensation') 
	and pt.PremiumType='D'   
	and pt.ReasonAmendedCode NOT IN ('CWO','Claw Back')
	and pt.SourceSystemID = 'DCT'
	@{pipeline().parameters.WHERE_CLAUSE}
	GROUP BY pc.PolicyAKID, rl.StateProvinceCode, pt.BaseRate, pt.ExperienceModificationEffectiveDate, pt.PremiumTransactionEffectiveDate,
	pt.PremiumTransactionExpirationDate, pt.PremiumTransactionEnteredDate, pt.PremiumTransactionCode, pt.OffsetOnsetCode
),
EXP_Default AS (
	SELECT
	PolicyAKID,
	StateProvinceCode,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionEnteredDate,
	PremiumTransactionCode,
	OffsetOnsetCode,
	SYSDATE AS CreatedDate
	FROM SQ_Get_Candidate_ExpModTransactions
),
WorkDCTExperienceModPremiumTransaction AS (
	TRUNCATE TABLE WorkDCTExperienceModPremiumTransaction;
	INSERT INTO WorkDCTExperienceModPremiumTransaction
	(CreatedDate, PolicyAKID, StateProvinceCode, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate, PremiumTransactionEnteredDate, PremiumTransactionCode, OffsetOnsetCode)
	SELECT 
	CREATEDDATE, 
	POLICYAKID, 
	STATEPROVINCECODE, 
	EXPERIENCEMODIFICATIONFACTOR, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONEXPIRATIONDATE, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	PREMIUMTRANSACTIONCODE, 
	OFFSETONSETCODE
	FROM EXP_Default
),
SQ_Get_candidate_transactions AS (
	--identify eligible policyakids
	WITH
	cte_pollist as
	(SELECT DISTINCT policyakid as policyakid from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTExperienceModPremiumTransaction)
	
	--identify all underlying premiumtransaction records that need updates
	SELECT DISTINCT
	pt.PremiumTransactionID, 
	pt.ExperienceModificationFactor as oldExperienceModificationFactor,
	pt.ExperienceModificationEffectiveDate as oldExperienceModificationEffectiveDate,
	lkp.ExperienceModificationFactor as ExperienceModificationFactor,
	lkp.ExperienceModificationEffectiveDate as ExperienceModificationEffectiveDate,
	rl.StateProvinceCode,
	rc.ClassCode
	FROM 
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc on rc.RatingCoverageAKID=pt.RatingCoverageAKID and rc.EffectiveDate = pt.EffectiveDate
	INNER JOIN  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc on pc.PolicyCoverageAKID=rc.PolicyCoverageAKID and pc.CurrentSnapshotFlag = 1
	INNER JOIN  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation rl on rl.RiskLocationAKID=pc.RiskLocationAKID and rl.CurrentSnapshotFlag = 1
	INNER JOIN  cte_pollist pl on pl.PolicyAKID = pc.PolicyAKID
	INNER JOIN  -- why was this left join previously???
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTExperienceModPremiumTransaction lkp
	on lkp.PolicyAKID = PC.PolicyAKID and lkp.StateProvinceCode = rl.StateProvinceCode and lkp.PremiumTransactionEffectiveDate = pt.PremiumTransactionEffectiveDate 
	and lkp.PremiumTransactionExpirationDate = PT.PremiumTransactionExpirationDate and lkp.PremiumTransactionEnteredDate = pt.PremiumTransactionEnteredDate
	and lkp.PremiumTransactionCode = pt.PremiumTransactionCode and lkp.OffsetOnsetCode = pt.OffsetOnsetCode
	WHERE pt.ReasonAmendedCode NOT IN ('CWO','Claw Back')
	@{pipeline().parameters.WHERE_CLAUSE1}
),
EXP_Evaluate_attributes AS (
	SELECT
	PremiumTransactionID,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	StateProvinceCode,
	ClassCode,
	oldExperienceModificationFactor,
	oldExperienceModificationEffectiveDate,
	-- *INF*: :LKP.LKP_SupClassificationWorkersCompensation(ClassCode,StateProvinceCode)
	LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_ClassCode_StateProvinceCode.SupClassificationWorkersCompensationId AS ClassCodeStateId,
	-- *INF*: :LKP.LKP_SupClassificationWorkersCompensation(ClassCode,'99')
	LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_ClassCode_99.SupClassificationWorkersCompensationId AS ClassCode99Id,
	-- *INF*: IIF((NOT ISNULL(ClassCodeStateId)) or (NOT ISNULL(ClassCode99Id)),ExperienceModificationFactor,0.0)
	-- --- One of the two lookups must be successful in order to pass the EMF - else it is defaulted to zero
	IFF(( NOT ClassCodeStateId IS NULL ) OR ( NOT ClassCode99Id IS NULL ), ExperienceModificationFactor, 0.0) AS v_ExperienceModificationFactor,
	-- *INF*: IIF((NOT ISNULL(ClassCodeStateId)) or (NOT ISNULL(ClassCode99Id)),ExperienceModificationEffectiveDate,TO_DATE('12-31-2100','MM-DD-YYYY'))
	-- --- One of the two lookups must be successful in order to pass the EMF - else it is defaulted to zero
	-- 
	-- 
	IFF(( NOT ClassCodeStateId IS NULL ) OR ( NOT ClassCode99Id IS NULL ), ExperienceModificationEffectiveDate, TO_DATE('12-31-2100', 'MM-DD-YYYY')) AS v_ExperienceModificationEffectiveDate,
	v_ExperienceModificationFactor AS o_ExperienceModificationFactor,
	v_ExperienceModificationEffectiveDate AS o_ExperienceModificationEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(ClassCodeStateId) and ISNULL(ClassCode99Id),0,
	-- (v_ExperienceModificationFactor = oldExperienceModificationFactor)  AND (v_ExperienceModificationEffectiveDate = oldExperienceModificationEffectiveDate),0,
	-- 1)
	-- -- If existing values are the same as determined values then no update is necessary
	DECODE(TRUE,
		ClassCodeStateId IS NULL AND ClassCode99Id IS NULL, 0,
		( v_ExperienceModificationFactor = oldExperienceModificationFactor ) AND ( v_ExperienceModificationEffectiveDate = oldExperienceModificationEffectiveDate ), 0,
		1) AS updateflag
	FROM SQ_Get_candidate_transactions
	LEFT JOIN LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_ClassCode_StateProvinceCode
	ON LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_ClassCode_StateProvinceCode.ClassCode = ClassCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_ClassCode_StateProvinceCode.RatingStateCode = StateProvinceCode

	LEFT JOIN LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_ClassCode_99
	ON LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_ClassCode_99.ClassCode = ClassCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_ClassCode_99.RatingStateCode = '99'

),
FIL_Transactions_with_changes AS (
	SELECT
	PremiumTransactionID, 
	o_ExperienceModificationFactor, 
	o_ExperienceModificationEffectiveDate, 
	updateflag
	FROM EXP_Evaluate_attributes
	WHERE updateflag = 1
),
UPD_Exp_Mod_Effd_PremiumTransaction AS (
	SELECT
	PremiumTransactionID, 
	o_ExperienceModificationFactor AS ExperienceModificationFactor, 
	o_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate
	FROM FIL_Transactions_with_changes
),
PremiumTransaction_UPDATE AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_Exp_Mod_Effd_PremiumTransaction AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ExperienceModificationFactor = S.ExperienceModificationFactor, T.ExperienceModificationEffectiveDate = S.ExperienceModificationEffectiveDate
),