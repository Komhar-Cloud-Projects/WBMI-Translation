WITH
SQ_Pif43LXZWCStage AS (
	SELECT PT.PremiumTransactionID,
	PT.CurrentSnapshotFlag,
	SC.StatisticalCoverageHashKey,
	BS.BureauCode5
	From @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	Inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC on PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
	Inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC on SC.PolicyCoverageAKID = PC.PolicyCoverageAKID and PC.TypeBureauCode in ('WC','WP')
	Left join @{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode BS on BS.PremiumTransactionAKID = PT.PremiumTransactionAKID and BS.CurrentSnapshotFlag = 1
	where
	PT.CreatedDate >='@{pipeline().parameters.SELECTION_START_TS}'
	and PT.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_DefaultValue AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	StatisticalCoverageHashKey AS i_StatisticalCoverageHashKey,
	BureauCode5 AS i_BureauCode5,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	i_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_StatisticalCoverageHashKey AS o_CoverageGuid,
	-- *INF*: Decode(TRUE,
	-- ISNULL(i_BureauCode5),'0',
	-- IS_SPACES(i_BureauCode5),'0',
	-- i_BureauCode5='','0',
	-- SUBSTR(i_BureauCode5,1,1)='A','1',
	-- SUBSTR(i_BureauCode5,2,1)='M','1',
	-- '0'
	-- )
	Decode(TRUE,
		i_BureauCode5 IS NULL, '0',
		IS_SPACES(i_BureauCode5), '0',
		i_BureauCode5 = '', '0',
		SUBSTR(i_BureauCode5, 1, 1) = 'A', '1',
		SUBSTR(i_BureauCode5, 2, 1) = 'M', '1',
		'0') AS o_AdmiraltyActFlag,
	-- *INF*: Decode(TRUE,
	-- ISNULL(i_BureauCode5),'0',
	-- IS_SPACES(i_BureauCode5),'0',
	-- i_BureauCode5='','0',
	-- SUBSTR(i_BureauCode5,1,1)='L','1',
	-- SUBSTR(i_BureauCode5,2,1)='M','1',
	-- '0')
	Decode(TRUE,
		i_BureauCode5 IS NULL, '0',
		IS_SPACES(i_BureauCode5), '0',
		i_BureauCode5 = '', '0',
		SUBSTR(i_BureauCode5, 1, 1) = 'L', '1',
		SUBSTR(i_BureauCode5, 2, 1) = 'M', '1',
		'0') AS o_FederalEmployersLiabilityActFlag,
	-- *INF*: Decode(TRUE,
	-- ISNULL(i_BureauCode5),'0',
	-- IS_SPACES(i_BureauCode5),'0',
	-- i_BureauCode5='','0',
	-- SUBSTR(i_BureauCode5,1,1)='F','1',
	-- SUBSTR(i_BureauCode5,1,1)='U','1',
	-- '0'
	-- )
	Decode(TRUE,
		i_BureauCode5 IS NULL, '0',
		IS_SPACES(i_BureauCode5), '0',
		i_BureauCode5 = '', '0',
		SUBSTR(i_BureauCode5, 1, 1) = 'F', '1',
		SUBSTR(i_BureauCode5, 1, 1) = 'U', '1',
		'0') AS o_USLongshoreandHarborWorkersCompensationActFlag
	FROM SQ_Pif43LXZWCStage
),
LKP_CoverageDetailWorkersCompensation AS (
	SELECT
	PremiumTransactionID,
	CoverageGuid,
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongShoreAndHarborWorkersCompensationActFlag
	FROM (
		SELECT 
			PremiumTransactionID,
			CoverageGuid,
			AdmiraltyActFlag,
			FederalEmployersLiabilityActFlag,
			USLongShoreAndHarborWorkersCompensationActFlag
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailWorkersCompensation
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChange AS (
	SELECT
	LKP_CoverageDetailWorkersCompensation.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailWorkersCompensation.CoverageGuid AS lkp_CoverageGuid,
	LKP_CoverageDetailWorkersCompensation.AdmiraltyActFlag AS lkp_AdmiraltyActFlag,
	LKP_CoverageDetailWorkersCompensation.FederalEmployersLiabilityActFlag AS lkp_FederalEmployersLiabilityActFlag,
	LKP_CoverageDetailWorkersCompensation.USLongShoreAndHarborWorkersCompensationActFlag AS lkp_USLongShoreAndHarborWorkersCompensationActFlag,
	EXP_DefaultValue.o_PremiumTransactionID AS PremiumTransactionID,
	EXP_DefaultValue.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_DefaultValue.o_AuditID AS AuditID,
	EXP_DefaultValue.o_EffectiveDate AS EffectiveDate,
	EXP_DefaultValue.o_ExpirationDate AS ExpirationDate,
	EXP_DefaultValue.o_SourceSystemID AS SourceSystemID,
	EXP_DefaultValue.o_CreatedDate AS CreatedDate,
	EXP_DefaultValue.o_ModifiedDate AS ModifiedDate,
	EXP_DefaultValue.o_CoverageGuid AS CoverageGuid,
	-- *INF*: IIF(lkp_AdmiraltyActFlag='T','1','0')
	IFF(lkp_AdmiraltyActFlag = 'T', '1', '0') AS v_AdmiraltyActFlag,
	-- *INF*: IIF(lkp_FederalEmployersLiabilityActFlag='T','1','0')
	IFF(lkp_FederalEmployersLiabilityActFlag = 'T', '1', '0') AS v_FederalEmployersLiabilityActFlag,
	-- *INF*: IIF(lkp_USLongShoreAndHarborWorkersCompensationActFlag='T','1','0')
	IFF(lkp_USLongShoreAndHarborWorkersCompensationActFlag = 'T', '1', '0') AS v_USLongShoreAndHarborWorkersCompensationActFlag,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID), 'New', 
	-- (lkp_CoverageGuid<>CoverageGuid
	--  OR v_AdmiraltyActFlag<>AdmiraltyActFlag
	--  OR v_FederalEmployersLiabilityActFlag<>FederalEmployersLiabilityActFlag
	--  OR v_USLongShoreAndHarborWorkersCompensationActFlag<>USLongshoreandHarborWorkersCompensationActFlag), 'Update',
	-- 'No Change'
	-- ) 
	-- 
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 'New',
		( lkp_CoverageGuid <> CoverageGuid OR v_AdmiraltyActFlag <> AdmiraltyActFlag OR v_FederalEmployersLiabilityActFlag <> FederalEmployersLiabilityActFlag OR v_USLongShoreAndHarborWorkersCompensationActFlag <> USLongshoreandHarborWorkersCompensationActFlag ), 'Update',
		'No Change') AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	'0' AS o_ConsentToRateFlag,
	0 AS o_RateOverride,
	EXP_DefaultValue.o_AdmiraltyActFlag AS AdmiraltyActFlag,
	EXP_DefaultValue.o_FederalEmployersLiabilityActFlag AS FederalEmployersLiabilityActFlag,
	EXP_DefaultValue.o_USLongshoreandHarborWorkersCompensationActFlag AS USLongshoreandHarborWorkersCompensationActFlag,
	'N/A' AS TermType,
	-- *INF*: TO_DATE('01/01/1800','MM/DD/YYYY')
	TO_DATE('01/01/1800', 'MM/DD/YYYY') AS Term_Start_Date,
	-- *INF*: TO_DATE('12/31/2100','MM/DD/YYYY')
	TO_DATE('12/31/2100', 'MM/DD/YYYY') AS Term_End_Date,
	0 AS ARDIndicatorFlag,
	0 AS ExperienceRatedFlag,
	'00' AS DeductibleType,
	'00' AS DeductibleBasis
	FROM EXP_DefaultValue
	LEFT JOIN LKP_CoverageDetailWorkersCompensation
	ON LKP_CoverageDetailWorkersCompensation.PremiumTransactionID = EXP_DefaultValue.o_PremiumTransactionID
),
RTR_InsertElseUpdate AS (
	SELECT
	lkp_PremiumTransactionID,
	PremiumTransactionID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	CoverageGuid,
	o_ChangeFlag AS ChangeFlag,
	o_ConsentToRateFlag AS ConsentToRateFlag,
	o_RateOverride AS RateOverride,
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongshoreandHarborWorkersCompensationActFlag,
	TermType,
	Term_Start_Date,
	Term_End_Date,
	ARDIndicatorFlag,
	ExperienceRatedFlag,
	DeductibleType,
	DeductibleBasis
	FROM EXP_DetectChange
),
RTR_InsertElseUpdate_INSERT AS (SELECT * FROM RTR_InsertElseUpdate WHERE ChangeFlag='New'),
RTR_InsertElseUpdate_UPDATE AS (SELECT * FROM RTR_InsertElseUpdate WHERE ChangeFlag='Update'),
TGT_CoverageDetailWorkersCompensation_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailWorkersCompensation
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
	COVERAGEGUID, 
	CONSENTTORATEFLAG, 
	RATEOVERRIDE, 
	ADMIRALTYACTFLAG, 
	FEDERALEMPLOYERSLIABILITYACTFLAG, 
	USLongshoreandHarborWorkersCompensationActFlag AS USLONGSHOREANDHARBORWORKERSCOMPENSATIONACTFLAG, 
	TERMTYPE, 
	Term_Start_Date AS TERMSTARTDATE, 
	Term_End_Date AS TERMENDDATE, 
	ARDINDICATORFLAG, 
	EXPERIENCERATEDFLAG, 
	DEDUCTIBLETYPE, 
	DEDUCTIBLEBASIS
	FROM RTR_InsertElseUpdate_INSERT
),
UPD_UpdateData AS (
	SELECT
	lkp_PremiumTransactionID AS PremiumTransactionID, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	CoverageGuid, 
	ConsentToRateFlag, 
	RateOverride, 
	AdmiraltyActFlag AS AdmiraltyActFlag3, 
	FederalEmployersLiabilityActFlag AS FederalEmployersLiabilityActFlag3, 
	USLongshoreandHarborWorkersCompensationActFlag AS USLongshoreandHarborWorkersCompensationActFlag3
	FROM RTR_InsertElseUpdate_UPDATE
),
TGT_CoverageDetailWorkersCompensation_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailWorkersCompensation AS T
	USING UPD_UpdateData AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.SourceSystemID = S.SourceSystemID, T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.CoverageGuid, T.ConsentToRateFlag = S.ConsentToRateFlag, T.RateOverride = S.RateOverride, T.AdmiraltyActFlag = S.AdmiraltyActFlag3, T.FederalEmployersLiabilityActFlag = S.FederalEmployersLiabilityActFlag3, T.USLongShoreAndHarborWorkersCompensationActFlag = S.USLongshoreandHarborWorkersCompensationActFlag3
),