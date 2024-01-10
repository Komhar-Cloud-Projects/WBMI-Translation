WITH
SQ_PremiumTransactionRatingModifier AS (
	SELECT 
	PT.PremiumTransactionID as NewNegatePremiumTransactionID,
	PT.PremiumTransactionAKID as NewNegatePremiumTransactionAKID,
	PTRM.PremiumTransactionRatingModifierId as PremiumTransactionRatingModifierId, 
	PTRM.PremiumTransactionID as PremiumTransactionID, 
	PTRM.OtherModifiedFactor as OtherModifiedFactor, 
	PTRM.ScheduleModifiedFactor as ScheduleModifiedFactor, 
	PTRM.ExperienceModifiedFactor as ExperienceModifiedFactor, 
	PTRM.TransitionFactor as TransitionFactor 
	FROM
	PremiumTransactionRatingModifier PTRM
	INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPTDRN ON PTRM.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	INNER JOIN dbo.PremiumTransaction PT ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID
	AND PT.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_IN_PremiumTransactionRatingModifier AS (
	SELECT
	NewNegatePremiumTransactionID,
	NewNegatePremiumTransactionAKID,
	PremiumTransactionRatingModifierId,
	PremiumTransactionID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	TransitionFactor
	FROM SQ_PremiumTransactionRatingModifier
),
LKP_PremiumTransactionRatingModifier AS (
	SELECT
	PremiumTransactionID,
	NewNegatePremiumTransactionID
	FROM (
		SELECT 
			PremiumTransactionID,
			NewNegatePremiumTransactionID
		FROM PremiumTransactionRatingModifier
		WHERE PremiumTransactionID IN ( SELECT pt.PremiumTransactionID FROM PremiumTransaction PT INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPT ON PT.PremiumTransactionAKID = WPT.NewNegatePremiumTransactionAKID)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
DetectChanges AS (
	SELECT
	LKP_PremiumTransactionRatingModifier.PremiumTransactionID AS lkp_PremiumTransactionID,
	EXP_IN_PremiumTransactionRatingModifier.NewNegatePremiumTransactionID,
	EXP_IN_PremiumTransactionRatingModifier.NewNegatePremiumTransactionAKID,
	EXP_IN_PremiumTransactionRatingModifier.o_AuditID,
	EXP_IN_PremiumTransactionRatingModifier.o_SourceSystemID,
	EXP_IN_PremiumTransactionRatingModifier.o_CreatedDate,
	EXP_IN_PremiumTransactionRatingModifier.o_ModifiedDate,
	EXP_IN_PremiumTransactionRatingModifier.OtherModifiedFactor,
	EXP_IN_PremiumTransactionRatingModifier.ScheduleModifiedFactor,
	EXP_IN_PremiumTransactionRatingModifier.ExperienceModifiedFactor,
	EXP_IN_PremiumTransactionRatingModifier.TransitionFactor,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),1,
	-- 0
	-- )
	-- -- 1 Insert  0 Ignore
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 1,
		0
	) AS DetectChanges
	FROM EXP_IN_PremiumTransactionRatingModifier
	LEFT JOIN LKP_PremiumTransactionRatingModifier
	ON LKP_PremiumTransactionRatingModifier.PremiumTransactionID = EXP_IN_PremiumTransactionRatingModifier.NewNegatePremiumTransactionID
),
FIL_KeepNull_Lkp AS (
	SELECT
	lkp_PremiumTransactionID, 
	NewNegatePremiumTransactionID AS PremiumTransactionID, 
	NewNegatePremiumTransactionAKID AS PremiumTransactionAKID, 
	o_AuditID, 
	o_SourceSystemID, 
	o_CreatedDate, 
	o_ModifiedDate, 
	OtherModifiedFactor, 
	ScheduleModifiedFactor, 
	ExperienceModifiedFactor, 
	TransitionFactor
	FROM DetectChanges
	WHERE IIF(ISNULL(lkp_PremiumTransactionID),TRUE,FALSE)
),
PremiumTransactionRatingModifier1 AS (
	INSERT INTO PremiumTransactionRatingModifier
	(PremiumTransactionID, PremiumTransactionAKID, AuditID, SourceSystemID, CreatedDate, ModifiedDate, OtherModifiedFactor, ScheduleModifiedFactor, ExperienceModifiedFactor, TransitionFactor)
	SELECT 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	o_AuditID AS AUDITID, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	OTHERMODIFIEDFACTOR, 
	SCHEDULEMODIFIEDFACTOR, 
	EXPERIENCEMODIFIEDFACTOR, 
	TRANSITIONFACTOR
	FROM FIL_KeepNull_Lkp
),