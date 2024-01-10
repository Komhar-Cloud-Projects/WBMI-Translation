WITH
SQ_CoverageDetailWorkersCompensation AS (
	SELECT CDWC.PremiumTransactionID,
	       CDWC.CoverageGuid,
	       CDWC.ConsentToRateFlag,
	       CDWC.RateOverride,
	       
		CDWC.AdmiraltyActFlag,
		CDWC.FederalEmployersLiabilityActFlag,	
		CDWC.USLongShoreAndHarborWorkersCompensationActFlag ,
		PT.PremiumTransactionID,
		CDWC.TermType, 
		CDWC.TermStartDate, 
		CDWC.TermEndDate, 
		CDWC.ARDIndicatorFlag, 
		CDWC.ExperienceRatedFlag,
		CDWC.DeductibleType,
		CDWC.DeductibleBasis
	FROM  
	dbo.CoverageDetailWorkersCompensation CDWC
	INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPTDRN 
	ON CDWC.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	INNER JOIN dbo.PremiumTransaction PT ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID 
	AND PT.SourceSystemId= '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND CDWC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
Exp_CoverageDetailGeneralLiability AS (
	SELECT
	PremiumTransactionID AS Old_PremiumTransactionID,
	CoverageGuid,
	ConsentToRateFlag,
	RateOverride,
	NewNegatePremiumTransactionID,
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongShoreAndHarborWorkersCompensationActFlag,
	TermType,
	TermStartDate,
	TermEndDate,
	ARDIndicatorFlag,
	ExperienceRatedFlag,
	DeductibleType,
	DeductibleBasis
	FROM SQ_CoverageDetailWorkersCompensation
),
EXP_Metadata AS (
	SELECT
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	CoverageGuid,
	ConsentToRateFlag,
	RateOverride,
	NewNegatePremiumTransactionID,
	NewNegatePremiumTransactionID AS o_PremiumTransactionID,
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongShoreAndHarborWorkersCompensationActFlag,
	TermType,
	TermStartDate,
	TermEndDate,
	ARDIndicatorFlag,
	ExperienceRatedFlag,
	DeductibleType,
	DeductibleBasis
	FROM Exp_CoverageDetailGeneralLiability
),
LKP_CoverageDetailWorkersCompensation AS (
	SELECT
	PremiumTransactionID,
	ConsentToRateFlag,
	RateOverride
	FROM (
		SELECT 
			PremiumTransactionID,
			ConsentToRateFlag,
			RateOverride
		FROM CoverageDetailWorkersCompensation
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND
		PremiumTransactionID IN (SELECT pt.PremiumTransactionID FROM
		PremiumTransaction pt INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate wpt
		ON pt.PremiumTransactionAKID=wpt.NewNegatePremiumTransactionAKID)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID DESC) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailWorkersCompensation.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailWorkersCompensation.ConsentToRateFlag AS lkp_ConsentToRateFlag,
	LKP_CoverageDetailWorkersCompensation.RateOverride AS lkp_RateOverride,
	EXP_Metadata.o_PremiumTransactionID AS PremiumTransactionID,
	EXP_Metadata.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_Metadata.o_AuditID AS AuditID,
	EXP_Metadata.o_EffectiveDate AS EffectiveDate,
	EXP_Metadata.o_ExpirationDate AS ExpirationDate,
	EXP_Metadata.o_SourceSystemID AS SourceSystemID,
	EXP_Metadata.o_CreatedDate AS CreatedDate,
	EXP_Metadata.o_ModifiedDate AS ModifiedDate,
	EXP_Metadata.CoverageGuid AS CoverageGUID,
	EXP_Metadata.ConsentToRateFlag,
	EXP_Metadata.RateOverride,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),'NEW',
	-- 'UPDATE'
	-- )
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 'NEW',
		'UPDATE'
	) AS o_ChangeFlag,
	EXP_Metadata.AdmiraltyActFlag,
	EXP_Metadata.FederalEmployersLiabilityActFlag,
	EXP_Metadata.USLongShoreAndHarborWorkersCompensationActFlag,
	EXP_Metadata.TermType,
	EXP_Metadata.TermStartDate,
	EXP_Metadata.TermEndDate,
	EXP_Metadata.ARDIndicatorFlag,
	EXP_Metadata.ExperienceRatedFlag,
	EXP_Metadata.DeductibleType,
	EXP_Metadata.DeductibleBasis
	FROM EXP_Metadata
	LEFT JOIN LKP_CoverageDetailWorkersCompensation
	ON LKP_CoverageDetailWorkersCompensation.PremiumTransactionID = EXP_Metadata.o_PremiumTransactionID
),
RTR_Insert_Update AS (
	SELECT
	PremiumTransactionID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	CoverageGUID,
	ConsentToRateFlag,
	RateOverride,
	o_ChangeFlag AS ChangeFlag,
	lkp_PremiumTransactionID,
	lkp_ConsentToRateFlag,
	lkp_RateOverride,
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongShoreAndHarborWorkersCompensationActFlag,
	TermType,
	TermStartDate,
	TermEndDate,
	ARDIndicatorFlag,
	ExperienceRatedFlag,
	DeductibleType,
	DeductibleBasis
	FROM EXP_DetectChanges
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='UPDATE'),
CoverageDetailWorkersCompensation_Negate_Insert AS (
	INSERT INTO CoverageDetailWorkersCompensation
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, ConsentToRateFlag, RateOverride, AdmiraltyActFlag, FederalEmployersLiabilityActFlag, USLongShoreAndHarborWorkersCompensationActFlag, TermType, TermStartDate, TermEndDate, ARDIndicatorFlag, ExperienceRatedFlag, DeductibleType, DeductibleBasis)
	SELECT 
	PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	CoverageGUID AS COVERAGEGUID, 
	CONSENTTORATEFLAG, 
	RATEOVERRIDE, 
	ADMIRALTYACTFLAG, 
	FEDERALEMPLOYERSLIABILITYACTFLAG, 
	USLONGSHOREANDHARBORWORKERSCOMPENSATIONACTFLAG, 
	TERMTYPE, 
	TERMSTARTDATE, 
	TERMENDDATE, 
	ARDINDICATORFLAG, 
	EXPERIENCERATEDFLAG, 
	DEDUCTIBLETYPE, 
	DEDUCTIBLEBASIS
	FROM RTR_Insert_Update_INSERT
),