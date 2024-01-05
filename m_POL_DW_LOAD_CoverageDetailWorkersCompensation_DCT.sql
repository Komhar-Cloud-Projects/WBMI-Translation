WITH
SQ_STG AS (
	SELECT
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageId,
		WorkDCTCoverageTransaction.CoverageGUID,
		WorkDCTInsuranceLine.LineType,
		WorkDCTPolicy.PolicyNumber,
		WorkDCTPolicy.PolicyVersion,
		WorkDCTTransactionInsuranceLineLocationBridge.SessionId,
		DCWCLineStaging.AnyARDIndicator,
		DCWCLineStaging.ExperienceRated,
		WorkDCTCoverageTransaction.ParentCoverageObjectId,
		WorkDCTCoverageTransaction.ParentCoverageObjectName,
		WorkDCTPolicy.PolicyEffectiveDate,
		WorkDCTPolicy.PolicyExpirationDate
	FROM WorkDCTPolicy
	INNER JOIN WorkDCTInsuranceLine
	INNER JOIN WorkDCTCoverageTransaction
	INNER JOIN WorkDCTTransactionInsuranceLineLocationBridge
	INNER JOIN DCWCLineStaging
	ON WorkDCTCoverageTransaction.CoverageId=WorkDCTTransactionInsuranceLineLocationBridge.CoverageId and WorkDCTTransactionInsuranceLineLocationBridge.LineId=WorkDCTInsuranceLine.LineId and WorkDCTInsuranceLine.PolicyId=WorkDCTPolicy.PolicyId and
	WorkDCTTransactionInsuranceLineLocationBridge.SessionId=DCWCLineStaging.SessionId
	 and WorkDCTInsuranceLine.LineType='WorkersCompensation'
),
EXP_PolicyKey AS (
	SELECT
	CoverageId,
	CoverageGUID,
	SessionId,
	PolicyNumber AS i_PolicyNumber,
	PolicyVersion AS i_PolicyVersion,
	-- *INF*: rtrim(ltrim(IIF(ISNULL(i_PolicyNumber) or IS_SPACES(i_PolicyNumber) or LENGTH(i_PolicyNumber)=0, 'N/A', LTRIM(RTRIM(i_PolicyNumber)))))
	rtrim(ltrim(IFF(i_PolicyNumber IS NULL OR IS_SPACES(i_PolicyNumber) OR LENGTH(i_PolicyNumber) = 0, 'N/A', LTRIM(RTRIM(i_PolicyNumber))))) AS v_PolicyNumber,
	-- *INF*: rtrim(ltrim(IIF(ISNULL(i_PolicyVersion), '00', LPAD(TO_CHAR(i_PolicyVersion),2,'0'))))
	rtrim(ltrim(IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')))) AS v_PolicyVersion,
	v_PolicyNumber||v_PolicyVersion AS o_PolicyKey,
	AnyARDIndicator,
	ExperienceRated,
	ParentCoverageObjectId,
	ParentCoverageObjectName,
	PolicyEffectiveDate,
	PolicyExpirationDate
	FROM SQ_STG
),
SQ_IL AS (
	SELECT DISTINCT PT.PremiumTransactionID, 
	PT.CurrentSnapshotFlag, 
	WPT.PremiumTransactionStageId 
	FROM
	Workpremiumtransaction WPT INNER JOIN Premiumtransaction PT ON PT.Premiumtransactionakid = WPT.Premiumtransactionakid 
	       INNER JOIN Ratingcoverage RC ON RC.Ratingcoverageakid = PT.Ratingcoverageakid AND RC.Effectivedate = PT.Effectivedate 
	       INNER JOIN Policycoverage PC ON RC.Policycoverageakid = PC.Policycoverageakid AND PC.Currentsnapshotflag =1
	       INNER JOIN Product P ON P.Productakid = RC.Productakid 
	WHERE PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' AND PT.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	AND WPT.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	AND PC.InsuranceLine = 'WorkersCompensation'
	@{pipeline().parameters.WHERE_CLAUSE}
),
JNR_IL_STG AS (SELECT
	SQ_IL.PremiumTransactionID, 
	SQ_IL.CurrentSnapshotFlag, 
	SQ_IL.PremiumTransactionStageId, 
	EXP_PolicyKey.CoverageId, 
	EXP_PolicyKey.CoverageGUID, 
	EXP_PolicyKey.SessionId, 
	EXP_PolicyKey.o_PolicyKey AS PolicyKey, 
	EXP_PolicyKey.AnyARDIndicator, 
	EXP_PolicyKey.ExperienceRated, 
	EXP_PolicyKey.ParentCoverageObjectId, 
	EXP_PolicyKey.ParentCoverageObjectName, 
	EXP_PolicyKey.PolicyEffectiveDate, 
	EXP_PolicyKey.PolicyExpirationDate
	FROM EXP_PolicyKey
	INNER JOIN SQ_IL
	ON SQ_IL.PremiumTransactionStageId = EXP_PolicyKey.CoverageId
),
AGG_DuplicateRemove AS (
	SELECT
	PremiumTransactionID, 
	CurrentSnapshotFlag, 
	CoverageGUID, 
	PolicyKey, 
	CoverageId, 
	SessionId, 
	AnyARDIndicator, 
	ExperienceRated, 
	ParentCoverageObjectId, 
	ParentCoverageObjectName, 
	PolicyEffectiveDate, 
	PolicyExpirationDate
	FROM JNR_IL_STG
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY NULL) = 1
),
EXP_DefaultValue AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	CoverageGUID AS i_CoverageGUID,
	CoverageId AS i_CoverageId,
	SessionId AS i_SessionId,
	PolicyKey,
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
	i_CoverageGUID AS o_CoverageGuid,
	i_CoverageId AS o_CoverageId,
	i_SessionId AS o_SessionId,
	AnyARDIndicator,
	ExperienceRated,
	ParentCoverageObjectId,
	ParentCoverageObjectName,
	PolicyEffectiveDate,
	PolicyExpirationDate
	FROM AGG_DuplicateRemove
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
LKP_DCStatCodeStaging AS (
	SELECT
	DeductibleType,
	DeductibleBasis,
	ObjectId,
	SessionId
	FROM (
		SELECT s.ObjectId as ObjectId,
			s.SessionId as SessionId,
			s.Value as DeductibleType,
			s2.Value as DeductibleBasis
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCStatCodeStaging s
		inner JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCStatCodeStaging s2
		on s.ObjectId = s2.ObjectId
		and s.SessionId = s2.SessionId
		and s2.ObjectName = 'DC_Coverage'
		and s2.Type = 'DeductibleBasis'
		WHERE s.ObjectName = 'DC_Coverage'
		AND s.Type = 'DeductibleType'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY DeductibleType) = 1
),
LKP_DCWCCoverageManualPremiumStage AS (
	SELECT
	WC_CoverageManualPremiumId,
	DCWCCoverageManualPremiumStagingId,
	CoverageId,
	SessionId,
	AdmiraltyProgramType,
	FELAProgramType,
	USLandHAct
	FROM (
		select distinct
		M.DCWCCoverageManualPremiumStagingId as DCWCCoverageManualPremiumStagingId,
		c.CoverageId as CoverageId,
		ISNULL(MM.WC_CoverageManualPremiumId,M.WC_CoverageManualPremiumId) as WC_CoverageManualPremiumId,
		c.SessionId as SessionId,
		case when cc.CoverageId is null then M.AdmiraltyProgramType else MM.AdmiraltyProgramType end as AdmiraltyProgramType,
		case when cc.CoverageId is null then M.FELAProgramType else MM.FELAProgramType end as FELAProgramType,
		case when cc.CoverageId is null then M.USLandHAct else MM.USLandHAct end as USLandHAct,
		case when cc.CoverageId is null then M.AdmiraltyProgramType else MM.AdmiraltyProgramType end as AdmiraltyProgramType 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging c
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCCoverageManualPremiumStaging m on M.SessionId = c.SessionId and M.CoverageId = c.CoverageId
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging cc on c.ObjectId=cc.CoverageId and c.SessionId = cc.SessionId
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCCoverageManualPremiumStaging MM on MM.SessionId = cc.SessionId and MM.CoverageId = cc.CoverageId
		order by SessionId desc--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId,SessionId ORDER BY WC_CoverageManualPremiumId) = 1
),
LKP_DCWCStateTermStaging AS (
	SELECT
	WC_StateTermId,
	PeriodStartDate,
	PeriodEndDate,
	TermType,
	ObjectName,
	ParentCoverageObjectId,
	ParentCoverageObjectName
	FROM (
		SELECT s.PeriodStartDate AS PeriodStartDate
			,s.PeriodEndDate AS PeriodEndDate
			,s.WC_StateTermId AS WC_StateTermId
			,s.TermType AS TermType
			,'DC_WC_StateTerm' AS ObjectName
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateTermStaging s
		ORDER BY s.WC_StateTermId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WC_StateTermId,ObjectName ORDER BY WC_StateTermId DESC) = 1
),
LKP_WBWCCoverageManualPremiumStage AS (
	SELECT
	WCCoverageManualPremiumId,
	ConsentToRate,
	RateOverride,
	WC_CoverageManualPremiumId
	FROM (
		SELECT 
			WCCoverageManualPremiumId,
			ConsentToRate,
			RateOverride,
			WC_CoverageManualPremiumId
		FROM WBWCCoverageManualPremiumStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCCoverageManualPremiumId ORDER BY WCCoverageManualPremiumId) = 1
),
LKP_WBWCCoverageTermStage AS (
	SELECT
	PeriodStartDate,
	PeriodEndDate,
	CoverageId,
	TermType,
	CoverageId1
	FROM (
		SELECT CT.PeriodStartDate AS PeriodStartDate
			,CT.PeriodEndDate AS PeriodEndDate
			,WBC.CoverageId AS CoverageId
			,CT.TermType AS TermType
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBWCCoverageTermStage CT
		INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCoverageStage WBC ON CT.WB_CoverageId = WBC.WBCoverageId
			AND ct.SessionId = WBC.SessionId
		ORDER BY WBC.CoverageId
			--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY PeriodStartDate DESC) = 1
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
	LKP_DCWCCoverageManualPremiumStage.AdmiraltyProgramType AS i_AdmiraltyProgramType,
	LKP_DCWCCoverageManualPremiumStage.FELAProgramType AS i_FELAProgramType,
	LKP_DCWCCoverageManualPremiumStage.USLandHAct AS i_USLandHAct,
	-- *INF*: IIF(lkp_AdmiraltyActFlag='T','1','0')
	IFF(lkp_AdmiraltyActFlag = 'T', '1', '0') AS v_lkp_AdmiraltyActFlag,
	-- *INF*: IIF(lkp_FederalEmployersLiabilityActFlag='T','1','0')
	IFF(lkp_FederalEmployersLiabilityActFlag = 'T', '1', '0') AS v_lkp_FederalEmployersLiabilityActFlag,
	-- *INF*: IIF(lkp_USLongShoreAndHarborWorkersCompensationActFlag='T','1','0')
	IFF(lkp_USLongShoreAndHarborWorkersCompensationActFlag = 'T', '1', '0') AS v_lkp_USLongShoreAndHarborWorkersCompensationActFlag,
	-- *INF*: IIF(ISNULL(i_AdmiraltyProgramType),'0','1')
	IFF(i_AdmiraltyProgramType IS NULL, '0', '1') AS v_AdmiraltyActFlag,
	-- *INF*: IIF(ISNULL(i_FELAProgramType),'0','1')
	IFF(i_FELAProgramType IS NULL, '0', '1') AS v_FederalEmployersLiabilityActFlag,
	-- *INF*: IIF(i_USLandHAct='T','1','0')
	IFF(i_USLandHAct = 'T', '1', '0') AS v_USLongShoreAndHarborWorkersCompensationActFlag,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID), 'New', 
	-- (lkp_CoverageGuid<>CoverageGuid
	--  OR v_lkp_AdmiraltyActFlag<>v_AdmiraltyActFlag
	--  OR v_lkp_FederalEmployersLiabilityActFlag<>v_FederalEmployersLiabilityActFlag
	--  OR v_lkp_USLongShoreAndHarborWorkersCompensationActFlag<>v_USLongShoreAndHarborWorkersCompensationActFlag), 'Update',
	-- 'No Change'
	-- ) 
	DECODE(TRUE,
	lkp_PremiumTransactionID IS NULL, 'New',
	( lkp_CoverageGuid <> CoverageGuid OR v_lkp_AdmiraltyActFlag <> v_AdmiraltyActFlag OR v_lkp_FederalEmployersLiabilityActFlag <> v_FederalEmployersLiabilityActFlag OR v_lkp_USLongShoreAndHarborWorkersCompensationActFlag <> v_USLongShoreAndHarborWorkersCompensationActFlag ), 'Update',
	'No Change') AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	LKP_WBWCCoverageManualPremiumStage.ConsentToRate,
	LKP_WBWCCoverageManualPremiumStage.RateOverride,
	-- *INF*: IIF(ISNULL(ConsentToRate),'0',ConsentToRate)
	IFF(ConsentToRate IS NULL, '0', ConsentToRate) AS o_ConsentToRate,
	-- *INF*: IIF(ISNULL(RateOverride),0.00,RateOverride)
	IFF(RateOverride IS NULL, 0.00, RateOverride) AS o_RateOverride,
	v_AdmiraltyActFlag AS o_AdmiraltyActFlag,
	v_FederalEmployersLiabilityActFlag AS o_FederalEmployersLiabilityActFlag,
	v_USLongShoreAndHarborWorkersCompensationActFlag AS o_USLongshoreandHarborWorkersCompensationActFlag,
	LKP_DCWCStateTermStaging.PeriodStartDate AS lkp_st_PeriodStartDate,
	LKP_DCWCStateTermStaging.PeriodEndDate AS lkp_st_PeriodEndDate,
	LKP_DCWCStateTermStaging.TermType AS lkp_st_TermType,
	LKP_WBWCCoverageTermStage.PeriodStartDate AS lkp_PeriodStartDate,
	LKP_WBWCCoverageTermStage.PeriodEndDate AS lkp_PeriodEndDate,
	LKP_WBWCCoverageTermStage.TermType AS lkp_TermType,
	LKP_DCStatCodeStaging.DeductibleType AS lkp_DeductibleType,
	LKP_DCStatCodeStaging.DeductibleBasis AS lkp_DeductibleBasis,
	-- *INF*: DECODE(TRUE, NOT ISNULL(lkp_st_PeriodStartDate),lkp_st_PeriodStartDate,
	-- NOT ISNULL(lkp_PeriodStartDate), lkp_PeriodStartDate,PolicyEffectiveDate)
	DECODE(TRUE,
	NOT lkp_st_PeriodStartDate IS NULL, lkp_st_PeriodStartDate,
	NOT lkp_PeriodStartDate IS NULL, lkp_PeriodStartDate,
	PolicyEffectiveDate) AS o_PeriodStartDate,
	-- *INF*: DECODE(TRUE, NOT ISNULL(lkp_st_PeriodEndDate),lkp_st_PeriodEndDate,
	-- NOT ISNULL(lkp_PeriodEndDate), lkp_PeriodEndDate,PolicyExpirationDate)
	DECODE(TRUE,
	NOT lkp_st_PeriodEndDate IS NULL, lkp_st_PeriodEndDate,
	NOT lkp_PeriodEndDate IS NULL, lkp_PeriodEndDate,
	PolicyExpirationDate) AS o_PeriodEndDate,
	-- *INF*: DECODE(TRUE, NOT ISNULL(lkp_st_TermType),lkp_st_TermType,
	-- NOT ISNULL(lkp_TermType), lkp_TermType,'ORG')
	DECODE(TRUE,
	NOT lkp_st_TermType IS NULL, lkp_st_TermType,
	NOT lkp_TermType IS NULL, lkp_TermType,
	'ORG') AS o_TermType,
	EXP_DefaultValue.AnyARDIndicator,
	EXP_DefaultValue.ExperienceRated,
	EXP_DefaultValue.PolicyEffectiveDate,
	EXP_DefaultValue.PolicyExpirationDate,
	-- *INF*: IIF(ISNULL(lkp_DeductibleType),'00',lkp_DeductibleType)
	IFF(lkp_DeductibleType IS NULL, '00', lkp_DeductibleType) AS o_DeductibleType,
	-- *INF*: IIF(ISNULL(lkp_DeductibleBasis),'00',lkp_DeductibleBasis)
	IFF(lkp_DeductibleBasis IS NULL, '00', lkp_DeductibleBasis) AS o_DeductibleBasis
	FROM EXP_DefaultValue
	LEFT JOIN LKP_CoverageDetailWorkersCompensation
	ON LKP_CoverageDetailWorkersCompensation.PremiumTransactionID = EXP_DefaultValue.o_PremiumTransactionID
	LEFT JOIN LKP_DCStatCodeStaging
	ON LKP_DCStatCodeStaging.ObjectId = EXP_DefaultValue.o_CoverageId AND LKP_DCStatCodeStaging.SessionId = EXP_DefaultValue.o_SessionId
	LEFT JOIN LKP_DCWCCoverageManualPremiumStage
	ON LKP_DCWCCoverageManualPremiumStage.CoverageId = EXP_DefaultValue.o_CoverageId AND LKP_DCWCCoverageManualPremiumStage.SessionId = EXP_DefaultValue.o_SessionId
	LEFT JOIN LKP_DCWCStateTermStaging
	ON LKP_DCWCStateTermStaging.WC_StateTermId = EXP_DefaultValue.ParentCoverageObjectId AND LKP_DCWCStateTermStaging.ObjectName = EXP_DefaultValue.ParentCoverageObjectName
	LEFT JOIN LKP_WBWCCoverageManualPremiumStage
	ON LKP_WBWCCoverageManualPremiumStage.WCCoverageManualPremiumId = LKP_DCWCCoverageManualPremiumStage.WC_CoverageManualPremiumId
	LEFT JOIN LKP_WBWCCoverageTermStage
	ON LKP_WBWCCoverageTermStage.CoverageId = EXP_DefaultValue.o_CoverageId
),
FIL_Records AS (
	SELECT
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
	o_ConsentToRate AS ConsentToRate, 
	o_RateOverride AS RateOverride, 
	o_AdmiraltyActFlag AS AdmiraltyActFlag, 
	o_FederalEmployersLiabilityActFlag AS FederalEmployersLiabilityActFlag, 
	o_USLongshoreandHarborWorkersCompensationActFlag AS USLongshoreandHarborWorkersCompensationActFlag, 
	o_PeriodStartDate AS PeriodStartDate, 
	o_PeriodEndDate AS PeriodEndDate, 
	o_TermType AS TermType, 
	AnyARDIndicator, 
	ExperienceRated, 
	o_DeductibleType AS DeductibleType, 
	o_DeductibleBasis AS DeductibleBasis
	FROM EXP_DetectChange
	WHERE ChangeFlag='New'
),
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
	ConsentToRate AS CONSENTTORATEFLAG, 
	RATEOVERRIDE, 
	ADMIRALTYACTFLAG, 
	FEDERALEMPLOYERSLIABILITYACTFLAG, 
	USLongshoreandHarborWorkersCompensationActFlag AS USLONGSHOREANDHARBORWORKERSCOMPENSATIONACTFLAG, 
	TERMTYPE, 
	PeriodStartDate AS TERMSTARTDATE, 
	PeriodEndDate AS TERMENDDATE, 
	AnyARDIndicator AS ARDINDICATORFLAG, 
	ExperienceRated AS EXPERIENCERATEDFLAG, 
	DEDUCTIBLETYPE, 
	DEDUCTIBLEBASIS
	FROM FIL_Records
),
SQ_CoverageDetailWorkersCompensation AS (
	SELECT 
	CDWC_ONSET.ConsentToRateFlag,
	CDWC_ONSET.RateOverride,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID,
	CDWC_ONSET.AdmiraltyActFlag,
	CDWC_ONSET.FederalEmployersLiabilityActFlag,
	CDWC_ONSET.USLongshoreandHarborWorkersCompensationActFlag 
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailWorkersCompensation CDWC_ONSET
	on ( CDWC_ONSET.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailWorkersCompensation CDWC_OFFSET
	on ( CDWC_OFFSET.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL.premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Offset'
	WHERE
	(CDWC_ONSET.ConsentToRateFlag <> CDWC_OFFSET.ConsentToRateFlag or 
	CDWC_ONSET.RateOverride <> CDWC_OFFSET.RateOverride 
	Or CDWC_ONSET.AdmiraltyActFlag <> CDWC_OFFSET.AdmiraltyActFlag 
	Or CDWC_ONSET.FederalEmployersLiabilityActFlag <> CDWC_OFFSET.FederalEmployersLiabilityActFlag Or
	CDWC_ONSET.USLongshoreandHarborWorkersCompensationActFlag <> CDWC_OFFSET.USLongshoreandHarborWorkersCompensationActFlag)
),
Exp_CoverageDetailWorkersCompensation AS (
	SELECT
	ConsentToRateFlag,
	RateOverride,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate,
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongShoreAndHarborWorkersCompensationActFlag
	FROM SQ_CoverageDetailWorkersCompensation
),
UPD_CoverageDetailWorkersCompensation AS (
	SELECT
	ConsentToRateFlag, 
	RateOverride, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate, 
	AdmiraltyActFlag, 
	FederalEmployersLiabilityActFlag, 
	USLongShoreAndHarborWorkersCompensationActFlag
	FROM Exp_CoverageDetailWorkersCompensation
),
TGT_CoverageDetailWorkersCompensation_Upd_Offsets AS (
	MERGE INTO CoverageDetailWorkersCompensation AS T
	USING UPD_CoverageDetailWorkersCompensation AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.ConsentToRateFlag = S.ConsentToRateFlag, T.RateOverride = S.RateOverride, T.AdmiraltyActFlag = S.AdmiraltyActFlag, T.FederalEmployersLiabilityActFlag = S.FederalEmployersLiabilityActFlag, T.USLongShoreAndHarborWorkersCompensationActFlag = S.USLongShoreAndHarborWorkersCompensationActFlag
),
SQ_CoverageDetailWorkersCompensation_Deprecated AS (
	SELECT 
	CDWC_ONSET.ConsentToRateFlag,
	CDWC_ONSET.RateOverride,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID,
	CDWC_ONSET.AdmiraltyActFlag,
	CDWC_ONSET.FederalEmployersLiabilityActFlag,
	CDWC_ONSET.USLongshoreandHarborWorkersCompensationActFlag 
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailWorkersCompensation CDWC_ONSET
	on ( CDWC_ONSET.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailWorkersCompensation CDWC_OFFSET
	on ( CDWC_OFFSET.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL.premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Deprecated'
	WHERE
	(CDWC_ONSET.ConsentToRateFlag <> CDWC_OFFSET.ConsentToRateFlag or 
	CDWC_ONSET.RateOverride <> CDWC_OFFSET.RateOverride 
	Or CDWC_ONSET.AdmiraltyActFlag <> CDWC_OFFSET.AdmiraltyActFlag 
	Or CDWC_ONSET.FederalEmployersLiabilityActFlag <> CDWC_OFFSET.FederalEmployersLiabilityActFlag Or
	CDWC_ONSET.USLongshoreandHarborWorkersCompensationActFlag <> CDWC_OFFSET.USLongshoreandHarborWorkersCompensationActFlag)
),
Exp_CoverageDetailWorkersCompensation_Deprecated AS (
	SELECT
	ConsentToRateFlag,
	RateOverride,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate,
	AdmiraltyActFlag,
	FederalEmployersLiabilityActFlag,
	USLongShoreAndHarborWorkersCompensationActFlag
	FROM SQ_CoverageDetailWorkersCompensation_Deprecated
),
UPD_CoverageDetailWorkersCompensation_Deprecated AS (
	SELECT
	ConsentToRateFlag, 
	RateOverride, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate, 
	AdmiraltyActFlag, 
	FederalEmployersLiabilityActFlag, 
	USLongShoreAndHarborWorkersCompensationActFlag
	FROM Exp_CoverageDetailWorkersCompensation_Deprecated
),
TGT_CoverageDetailWorkersCompensation_Upd_Deprecated AS (
	MERGE INTO CoverageDetailWorkersCompensation AS T
	USING UPD_CoverageDetailWorkersCompensation_Deprecated AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.ConsentToRateFlag = S.ConsentToRateFlag, T.RateOverride = S.RateOverride, T.AdmiraltyActFlag = S.AdmiraltyActFlag, T.FederalEmployersLiabilityActFlag = S.FederalEmployersLiabilityActFlag, T.USLongShoreAndHarborWorkersCompensationActFlag = S.USLongShoreAndHarborWorkersCompensationActFlag
),