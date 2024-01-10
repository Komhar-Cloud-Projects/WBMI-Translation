WITH
SQ_PremiumTransaction_Offset AS (
	SELECT ptToUpdate.PremiumTransactionID,
		ptToUpdate.ExperienceModificationFactor,
		ptToUpdate.ExperienceModificationEffectiveDate,
		ptAttrValues.ExperienceModificationFactor,
		ptAttrValues.ExperienceModificationEffectiveDate
	FROM WorkPremiumTransactionOffsetLineage wptol
	INNER JOIN PremiumTransaction ptToUpdate ON wptol.PremiumTransactionID = ptToUpdate.PremiumTransactionID
		AND ptToUpdate.OffsetOnsetCode = 'Offset'
		AND ptToUpdate.auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	INNER JOIN PremiumTransaction ptAttrValues ON wptol.PreviousPremiumTransactionID = ptAttrValues.PremiumTransactionID
	INNER JOIN RatingCoverage RC ON RC.RatingCoverageAKID = ptToUpdate.RatingCoverageAKID
		AND RC.EffectiveDate = ptToUpdate.EffectiveDate
	INNER JOIN PolicyCoverage PC ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
		AND PC.TypeBureauCode IN ('WC', 'WP', 'WorkersCompensation')
	WHERE wptol.UpdateAttributeFlag = 1 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_offset_PTUpdate_attributes AS (
	SELECT
	ptToUpdate_PremiumTransactionID AS PremiumTransactionID,
	ptToUpdate_ExperienceModificationFactor,
	ptToUpdate_ExperienceModificationEffectiveDate,
	ptAttrValues_ExperienceModificationFactor,
	ptAttrValues_ExperienceModificationEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- 	(ptToUpdate_ExperienceModificationFactor != ptAttrValues_ExperienceModificationFactor or
	-- 	ptToUpdate_ExperienceModificationEffectiveDate != ptAttrValues_ExperienceModificationEffectiveDate),
	-- 		'UPD',
	-- 	'NOCHANGE')
	DECODE(TRUE,
		( ptToUpdate_ExperienceModificationFactor != ptAttrValues_ExperienceModificationFactor OR ptToUpdate_ExperienceModificationEffectiveDate != ptAttrValues_ExperienceModificationEffectiveDate ), 'UPD',
		'NOCHANGE') AS v_PTUpdateFlag,
	v_PTUpdateFlag AS PTUpdateFlag
	FROM SQ_PremiumTransaction_Offset
),
FLT_UpdateRecords AS (
	SELECT
	PremiumTransactionID, 
	ptAttrValues_ExperienceModificationFactor AS ExperienceModificationFactor, 
	ptAttrValues_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate, 
	PTUpdateFlag
	FROM EXP_offset_PTUpdate_attributes
	WHERE PTUpdateFlag='UPD'
),
UPD_PremiumTransaction AS (
	SELECT
	PremiumTransactionID, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate
	FROM FLT_UpdateRecords
),
PremiumTransaction_upd AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_PremiumTransaction AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ExperienceModificationFactor = S.ExperienceModificationFactor, T.ExperienceModificationEffectiveDate = S.ExperienceModificationEffectiveDate
),
SQ_PremiumTransaction_Deprecated AS (
	SELECT ptToUpdate.PremiumTransactionID,
		ptToUpdate.ExperienceModificationFactor,
		ptToUpdate.ExperienceModificationEffectiveDate,
		ptAttrValues.ExperienceModificationFactor,
		ptAttrValues.ExperienceModificationEffectiveDate
	FROM WorkPremiumTransactionOffsetLineage wptol
	INNER JOIN PremiumTransaction ptToUpdate ON wptol.PremiumTransactionID = ptToUpdate.PremiumTransactionID
		AND ptToUpdate.OffsetOnsetCode = 'Deprecated'
		AND ptToUpdate.auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	INNER JOIN PremiumTransaction ptAttrValues ON wptol.PreviousPremiumTransactionID = ptAttrValues.PremiumTransactionID
	INNER JOIN RatingCoverage RC ON RC.RatingCoverageAKID = ptToUpdate.RatingCoverageAKID
		AND RC.EffectiveDate = ptToUpdate.EffectiveDate
	INNER JOIN PolicyCoverage PC ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
		AND PC.TypeBureauCode IN ('WorkersCompensation')
	WHERE wptol.UpdateAttributeFlag = 1 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Deprecated_PTUpdate_attributes AS (
	SELECT
	ptToUpdate_PremiumTransactionID AS PremiumTransactionID,
	ptToUpdate_ExperienceModificationFactor,
	ptToUpdate_ExperienceModificationEffectiveDate,
	ptAttrValues_ExperienceModificationFactor,
	ptAttrValues_ExperienceModificationEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- 	(ptToUpdate_ExperienceModificationFactor != ptAttrValues_ExperienceModificationFactor or
	-- 	ptToUpdate_ExperienceModificationEffectiveDate != ptAttrValues_ExperienceModificationEffectiveDate),
	-- 		'UPD',
	-- 	'NOCHANGE')
	DECODE(TRUE,
		( ptToUpdate_ExperienceModificationFactor != ptAttrValues_ExperienceModificationFactor OR ptToUpdate_ExperienceModificationEffectiveDate != ptAttrValues_ExperienceModificationEffectiveDate ), 'UPD',
		'NOCHANGE') AS v_PTUpdateFlag,
	v_PTUpdateFlag AS PTUpdateFlag
	FROM SQ_PremiumTransaction_Deprecated
),
FLT_UpdateRecords_Deprecated AS (
	SELECT
	PremiumTransactionID, 
	ptAttrValues_ExperienceModificationFactor AS ExperienceModificationFactor, 
	ptAttrValues_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate, 
	PTUpdateFlag
	FROM EXP_Deprecated_PTUpdate_attributes
	WHERE PTUpdateFlag='UPD'
),
UPD_PremiumTransaction_Deprecated AS (
	SELECT
	PremiumTransactionID, 
	ExperienceModificationFactor, 
	ExperienceModificationEffectiveDate
	FROM FLT_UpdateRecords_Deprecated
),
PremiumTransaction_upd_Deprecated AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_PremiumTransaction_Deprecated AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ExperienceModificationFactor = S.ExperienceModificationFactor, T.ExperienceModificationEffectiveDate = S.ExperienceModificationEffectiveDate
),