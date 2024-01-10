WITH
LKP_SupStateCFAXRef_Percent AS (
	SELECT
	CFARate,
	StateCode,
	EffectiveFromDate,
	EffectiveToDate
	FROM (
		SELECT 
			CFARate,
			StateCode,
			EffectiveFromDate,
			EffectiveToDate
		FROM SupStateCFAXRef
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateCode,EffectiveFromDate,EffectiveToDate ORDER BY CFARate) = 1
),
LKP_ExcludePassThrough AS (
	SELECT
	RatedCoverageCode
	FROM (
		select cc.RatedCoverageCode as RatedCoverageCode 
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary CS
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup CG
		on CS.CoverageSummaryId=CG.CoverageSummaryId
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage CC
		on CG.CoverageGroupId=CC.CoverageGroupId
		where CS.CoverageSummaryCode='PASSTHRU'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatedCoverageCode ORDER BY RatedCoverageCode) = 1
),
LKP_RatingCoverage AS (
	SELECT
	RatingCoverageAKID,
	RatingCoverageId,
	RatingCoverageHashKey,
	RatingCoverageCancellationDate,
	CoverageGUID,
	CoverageType,
	SubCoverageTypeCode
	FROM (
		SELECT RC.RatingCoverageId as RatingCoverageId,
		RC.RatingCoverageAKID as RatingCoverageAKID, 
		RC.RatingCoverageHashKey as RatingCoverageHashKey, 
		RC.RatingCoverageCancellationDate as RatingCoverageCancellationDate, 
		RC.CoverageGUID as CoverageGUID,
		RC.CoverageType as CoverageType,
		RC.SubCoverageTypeCode as SubCoverageTypeCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
		on RC.CoverageGUID=C.Coveragekey
		and RC.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		order by RC.CoverageGUID,EffectiveDate desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGUID ORDER BY RatingCoverageAKID) = 1
),
SQ_WorkDCTPLCoverage AS (
	select distinct P.PolicySymbol,
	P.PolicyNumber,
	P.PolicyVersion,
	P.TransactionCreatedDate,
	P.TransactionTypeCode,
	P.PolicyEffectiveDate,
	C.MeasureName,
	C.MeasureDetailCode,
	C.CoverageEffectiveDate,
	C.CoverageExpirationDate,
	C.TransactionAmount,
	ISNULL(C.FullTermPremium,C.TransactionAmount) FullTermPremium,
	ISNULL(C.ExposureAmount,0) ExposureAmount,
	C.CoverageKey,
	C.TransactionEffectiveDate,
	C.TransactionIssueDate,
	ISNULL(C.TransactionReasonCode,'N/A') TransactionReasonCode,
	ISNULL(C.DeductibleAmount,0) DeductibleAmount,
	C.CoverageCodeKey,
	ISNULL(C.CoverageSubCd,'') CoverageSubCd,
	P.ExtractDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
	on P.PolicyKey=C.PolicyKey
	and P.StartDate=C.StartDate
	where C.MeasureName='WrittenPremium'
	and not exists(select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P2 where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	@{pipeline().parameters.WHERE_CLAUSE_RC}
	order by P.PolicySymbol,P.PolicyNumber,P.PolicyVersion,CoverageKey,P.TransactionCreatedDate
),
EXP_SRCDataCollect AS (
	SELECT
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	TransactionCreatedDate,
	TransactionTypeCode,
	PolicyEffectiveDate,
	MeasureName,
	MeasureDetailCode,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	TransactionAmount,
	FullTermPremium,
	ExposureAmount,
	CoverageKey,
	TransactionEffectiveDate,
	TransactionIssueDate,
	TransactionReasonCode,
	DeductibleAmount,
	CoverageCodeKey,
	CoverageSubCd,
	-- *INF*: :LKP.LKP_EXCLUDEPASSTHROUGH(CoverageSubCd)
	LKP_EXCLUDEPASSTHROUGH_CoverageSubCd.RatedCoverageCode AS v_LKP_PassThroughExclusion,
	-- *INF*: IIF(ISNULL(v_LKP_PassThroughExclusion),'1','0')
	IFF(v_LKP_PassThroughExclusion IS NULL,
		'1',
		'0'
	) AS o_PassThroughExclusionFlag,
	ExtractDate
	FROM SQ_WorkDCTPLCoverage
	LEFT JOIN LKP_EXCLUDEPASSTHROUGH LKP_EXCLUDEPASSTHROUGH_CoverageSubCd
	ON LKP_EXCLUDEPASSTHROUGH_CoverageSubCd.RatedCoverageCode = CoverageSubCd

),
FIL_PasThroughChargeExclusion AS (
	SELECT
	PolicySymbol, 
	PolicyNumber, 
	PolicyVersion, 
	TransactionCreatedDate, 
	TransactionTypeCode, 
	PolicyEffectiveDate, 
	MeasureName, 
	MeasureDetailCode, 
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	TransactionAmount, 
	FullTermPremium, 
	ExposureAmount, 
	CoverageKey, 
	TransactionEffectiveDate, 
	TransactionIssueDate, 
	TransactionReasonCode, 
	DeductibleAmount, 
	CoverageCodeKey, 
	CoverageSubCd, 
	o_PassThroughExclusionFlag AS PassThroughExclusionFlag, 
	ExtractDate
	FROM EXP_SRCDataCollect
	WHERE PassThroughExclusionFlag='1'
),
Exp_Get_Values AS (
	SELECT
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	TransactionCreatedDate,
	TransactionTypeCode,
	MeasureName,
	MeasureDetailCode,
	'N/A' AS v_MeasureDetailCode,
	v_MeasureDetailCode AS o_MeasureDetailCode,
	-- *INF*: Decode(TRUE,
	-- TransactionTypeCode='New','10',
	-- TransactionTypeCode='Renew','11',
	-- TransactionTypeCode='Endorse','12',
	-- TransactionTypeCode='Reinstate','15',
	-- TransactionTypeCode='Cancel','20',
	-- TransactionTypeCode='Reissue','30',
	-- TransactionTypeCode='Rewrite','31',
	-- '-1'
	-- )
	-- 
	Decode(TRUE,
		TransactionTypeCode = 'New', '10',
		TransactionTypeCode = 'Renew', '11',
		TransactionTypeCode = 'Endorse', '12',
		TransactionTypeCode = 'Reinstate', '15',
		TransactionTypeCode = 'Cancel', '20',
		TransactionTypeCode = 'Reissue', '30',
		TransactionTypeCode = 'Rewrite', '31',
		'-1'
	) AS o_standardPremiumTransactionCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_RATINGCOVERAGE(CoverageKey)),-1,:LKP.LKP_RATINGCOVERAGE(CoverageKey))
	IFF(LKP_RATINGCOVERAGE_CoverageKey.RatingCoverageAKID IS NULL,
		- 1,
		LKP_RATINGCOVERAGE_CoverageKey.RatingCoverageAKID
	) AS v_RatingCoverageAkid,
	v_RatingCoverageAkid AS o_RatingCoverageAkid,
	-1 AS o_StatisticalCoverageAkid,
	-- *INF*: MD5(v_RatingCoverageAkid||TO_CHAR(TransactionCreatedDate)|| v_MeasureDetailCode)
	MD5(v_RatingCoverageAkid || TO_CHAR(TransactionCreatedDate
		) || v_MeasureDetailCode
	) AS v_PremiunTransactionHashKey,
	v_PremiunTransactionHashKey AS o_PremiunTransactionHashKey,
	'D' AS o_PremiumType,
	PolicyEffectiveDate,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	TransactionAmount,
	FullTermPremium,
	ExposureAmount,
	CoverageKey,
	-- *INF*: CoverageKey||'||'||TO_CHAR(TransactionCreatedDate)
	CoverageKey || '||' || TO_CHAR(TransactionCreatedDate
	) AS o_PremiumTransactionKey,
	TransactionEffectiveDate,
	TransactionIssueDate,
	-- *INF*: DECODE(TRUE,
	-- TO_CHAR(ExtractDate, 'DD' ) ='02' and TO_CHAR( ExtractDate, 'DAY' )='Tuesday',1,
	-- TO_CHAR(ExtractDate, 'DD' ) ='01',1,
	-- 0
	-- )
	DECODE(TRUE,
		TO_CHAR(ExtractDate, 'DD'
		) = '02' 
		AND TO_CHAR(ExtractDate, 'DAY'
		) = 'Tuesday', 1,
		TO_CHAR(ExtractDate, 'DD'
		) = '01', 1,
		0
	) AS v_AdjustForMonthEnd,
	-- *INF*: DECODE(TRUE,
	-- v_AdjustForMonthEnd = 1, ADD_TO_DATE(ExtractDate,'MM',-1),
	-- ExtractDate)
	DECODE(TRUE,
		v_AdjustForMonthEnd = 1, DATEADD(MONTH,- 1,ExtractDate),
		ExtractDate
	) AS v_ExtractDate,
	-- *INF*: TRUNC(GREATEST(TransactionCreatedDate,TransactionEffectiveDate), 'MM')
	CAST(TRUNC(GREATEST(TransactionCreatedDate, TransactionEffectiveDate
	), 'MONTH') AS TIMESTAMP_NTZ(0)) AS v_TransactionIssueDate,
	-- *INF*: TRUNC(GREATEST(v_TransactionIssueDate,v_ExtractDate), 'MM')
	CAST(TRUNC(GREATEST(v_TransactionIssueDate, v_ExtractDate
	), 'MONTH') AS TIMESTAMP_NTZ(0)) AS o_TransactionIssueDate,
	TransactionReasonCode,
	DeductibleAmount,
	0 AS o_ExperienceModificationFactor,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_ExperienceModificationEffectiveDate,
	0 AS o_PackageModificationAdjustmentFactor,
	'0' AS o_PackageModificationAdjustmentGroupCode,
	0 AS o_IncreasedLimitFactor,
	'0' AS o_IncreasedLimitGroupCode,
	'0000' AS o_YearBuilt,
	0 AS o_AgencyActualCommissionRate,
	0.0000 AS o_BaseRate,
	'N/A' AS o_ConstructionCode,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_StateRatingEffectiveDate,
	0 AS o_IndividualRiskPremiumModification,
	'0' AS o_WindCoverageFlag,
	'N/A' AS o_DeductibleBasis,
	'N/A' AS o_ExposureBasis,
	'N/A' AS o_TransactionCreatedUserId,
	'N/A' AS o_ServiceCentreName5,
	0 AS o_NumberOfEmployees,
	'N/A' AS o_NegateRestateCode,
	CoverageCodeKey,
	CoverageSubCd,
	ExtractDate
	FROM FIL_PasThroughChargeExclusion
	LEFT JOIN LKP_RATINGCOVERAGE LKP_RATINGCOVERAGE_CoverageKey
	ON LKP_RATINGCOVERAGE_CoverageKey.CoverageGUID = CoverageKey

),
LKP_PremiumTransaction AS (
	SELECT
	PremiumTransactionAKID,
	PremiumTransactionID,
	PremiumTransactionHashKey
	FROM (
		SELECT PT.PremiumTransactionAKID as PremiumTransactionAKID, 
		PT.PremiumTransactionID as PremiumTransactionID, 
		PT.PremiumTransactionHashKey as PremiumTransactionHashKey,
		PT.NegateRestateCode as NegateRestateCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
		on PT.RatingCoverageAKId=RC.RatingCoverageAKID
		and PT.EffectiveDate=RC.EffectiveDate
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
		on C.Coveragekey = RC.CoverageGUID
		and PT.CurrentSnapshotFlag='1' 
		AND PT.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionHashKey ORDER BY PremiumTransactionAKID DESC) = 1
),
LKP_sup_premium_transaction_code AS (
	SELECT
	sup_prem_trans_code_id,
	prem_trans_code,
	StandardPremiumTransactionCode
	FROM (
		SELECT 
			sup_prem_trans_code_id,
			prem_trans_code,
			StandardPremiumTransactionCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_premium_transaction_code
		WHERE crrnt_snpsht_flag='1' AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY prem_trans_code,StandardPremiumTransactionCode ORDER BY sup_prem_trans_code_id) = 1
),
EXP_Format_PremiumTransaction AS (
	SELECT
	LKP_sup_premium_transaction_code.sup_prem_trans_code_id AS i_sup_premium_transaction_id,
	Exp_Get_Values.o_PremiumTransactionKey AS i_PremiumTransactionKey,
	Exp_Get_Values.TransactionTypeCode AS i_PremiumTransactionCode,
	Exp_Get_Values.TransactionCreatedDate AS i_PremiumTransactionEnteredDate,
	Exp_Get_Values.TransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	Exp_Get_Values.CoverageExpirationDate AS i_PremiumTransactionExpirationDate,
	Exp_Get_Values.o_TransactionIssueDate AS i_PremiumTransactionBookedDate,
	Exp_Get_Values.TransactionAmount AS i_PremiumTransactionAmount,
	Exp_Get_Values.FullTermPremium AS i_FullTermPremium,
	Exp_Get_Values.o_PremiumType AS i_PremiumType,
	Exp_Get_Values.o_MeasureDetailCode AS i_OffsetOnsetIndicator,
	Exp_Get_Values.o_StatisticalCoverageAkid AS i_StatisticalCoverageAKID,
	Exp_Get_Values.CoverageKey AS i_CoverageGUID,
	Exp_Get_Values.TransactionReasonCode AS i_ReasonAmendedCode,
	Exp_Get_Values.o_PremiunTransactionHashKey AS i_PremiumTransactionHashKey,
	Exp_Get_Values.o_RatingCoverageAkid AS i_RatingCoverageAKId,
	LKP_PremiumTransaction.PremiumTransactionAKID,
	LKP_PremiumTransaction.PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	i_PremiumTransactionEnteredDate AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	0 AS o_LogicalIndicator,
	'1' AS o_LogicalDeleteFlag,
	i_PremiumTransactionHashKey AS o_PremiumTransactionHashKey,
	1 AS o_PremiumLoadSequence,
	1 AS o_DuplicateSequence,
	-1 AS o_ReinsuranceCoverageAKID,
	-1 AS o_StatisticalCoverageAKID,
	i_PremiumTransactionKey AS o_PremiumTransactionKey,
	'N/A' AS o_PMSFunctionCode,
	i_PremiumTransactionCode AS o_PremiumTransactionCode,
	i_PremiumTransactionEnteredDate AS o_PremiumTransactionEnteredDate,
	i_PremiumTransactionEffectiveDate AS o_PremiumTransactionEffectiveDate,
	i_PremiumTransactionExpirationDate AS o_PremiumTransactionExpirationDate,
	i_PremiumTransactionBookedDate AS o_PremiumTransactionBookedDate,
	i_PremiumTransactionAmount AS o_PremiumTransactionAmount,
	i_FullTermPremium AS o_FullTermPremium,
	i_PremiumType AS o_PremiumType,
	-- *INF*: i_ReasonAmendedCode
	-- 
	-- --'TBD'
	i_ReasonAmendedCode AS o_ReasonAmendedCode,
	i_OffsetOnsetIndicator AS o_OffsetOnsetCode,
	i_sup_premium_transaction_id AS o_sup_premium_transaction_id,
	i_RatingCoverageAKId AS o_RatingCoverageAKId,
	Exp_Get_Values.DeductibleAmount,
	Exp_Get_Values.o_ExperienceModificationFactor AS ExperienceModificationFactor,
	Exp_Get_Values.o_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate,
	Exp_Get_Values.o_PackageModificationAdjustmentFactor AS PackageModificationAdjustmentFactor,
	Exp_Get_Values.o_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode,
	Exp_Get_Values.o_IncreasedLimitFactor AS IncreasedLimitFactor,
	Exp_Get_Values.o_IncreasedLimitGroupCode AS IncreasedLimitGroupCode,
	Exp_Get_Values.o_YearBuilt AS YearBuilt,
	Exp_Get_Values.o_AgencyActualCommissionRate AS i_AgencyActualCommissionRate,
	i_AgencyActualCommissionRate*0.01 AS o_AgencyActualCommissionRate,
	Exp_Get_Values.o_BaseRate AS BaseRate,
	Exp_Get_Values.o_ConstructionCode AS ConstructionCode,
	Exp_Get_Values.o_StateRatingEffectiveDate AS StateRatingEffectiveDate,
	i_CoverageId AS o_PremiumTransactionStageId,
	Exp_Get_Values.o_IndividualRiskPremiumModification AS IndividualRiskPremiumModification,
	Exp_Get_Values.o_WindCoverageFlag AS WindCoverageFlag,
	Exp_Get_Values.TransactionCreatedDate AS CreatedDate,
	Exp_Get_Values.o_DeductibleBasis AS DeductibleBasis,
	Exp_Get_Values.o_ExposureBasis AS ExposureBasis,
	Exp_Get_Values.ExposureAmount AS Exposure,
	Exp_Get_Values.o_TransactionCreatedUserId AS TransactionCreatedUserId5,
	Exp_Get_Values.o_ServiceCentreName5 AS ServiceCentreName5,
	Exp_Get_Values.o_NumberOfEmployees AS lkp_NumberOfEmployees,
	-- *INF*: IIF(ISNULL(lkp_NumberOfEmployees),0,lkp_NumberOfEmployees)
	IFF(lkp_NumberOfEmployees IS NULL,
		0,
		lkp_NumberOfEmployees
	) AS o_NumberOfEmployees,
	Exp_Get_Values.o_NegateRestateCode AS NegateRestateCode,
	Exp_Get_Values.ExposureAmount AS WrittenExposure,
	-- *INF*: substr(i_PremiumTransactionKey,1,instr(i_PremiumTransactionKey,'~',1,1))
	substr(i_PremiumTransactionKey, 1, REGEXP_INSTR(i_PremiumTransactionKey, '~', 1, 1
		)
	) AS o_Policyakid,
	0 AS DeclaredEventFlag
	FROM Exp_Get_Values
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.PremiumTransactionHashKey = Exp_Get_Values.o_PremiunTransactionHashKey
	LEFT JOIN LKP_sup_premium_transaction_code
	ON LKP_sup_premium_transaction_code.prem_trans_code = Exp_Get_Values.TransactionTypeCode AND LKP_sup_premium_transaction_code.StandardPremiumTransactionCode = Exp_Get_Values.o_standardPremiumTransactionCode
),
RTR_Insert_Update AS (
	SELECT
	PremiumTransactionAKID AS lkp_PremiumTransactionAKID,
	PremiumTransactionID AS lkp_PremiumTransactionID,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	CreatedDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LogicalIndicator AS LogicalIndicator,
	o_LogicalDeleteFlag AS LogicalDeleteFlag,
	o_PremiumTransactionHashKey AS PremiumTransactionHashKey,
	o_PremiumLoadSequence AS PremiumLoadSequence,
	o_DuplicateSequence AS DuplicateSequence,
	o_ReinsuranceCoverageAKID AS ReinsuranceCoverageAKID,
	o_StatisticalCoverageAKID AS StatisticalCoverageAKID,
	o_PremiumTransactionKey AS PremiumTransactionKey,
	o_PMSFunctionCode AS PMSFunctionCode,
	o_PremiumTransactionCode AS PremiumTransactionCode,
	o_PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate,
	o_PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate,
	o_PremiumTransactionExpirationDate AS PremiumTransactionExpirationDate,
	o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate,
	o_PremiumTransactionAmount AS PremiumTransactionAmount,
	o_FullTermPremium AS FullTermPremium,
	o_PremiumType AS PremiumType,
	o_ReasonAmendedCode AS ReasonAmendedCode,
	o_OffsetOnsetCode AS OffsetOnsetCode,
	o_sup_premium_transaction_id AS SupPremiumTransactionCodeId,
	o_RatingCoverageAKId AS RatingCoverageAKId,
	DeductibleAmount,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PackageModificationAdjustmentFactor,
	PackageModificationAdjustmentGroupCode,
	IncreasedLimitFactor,
	IncreasedLimitGroupCode,
	YearBuilt,
	o_AgencyActualCommissionRate AS AgencyActualCommissionRate,
	BaseRate,
	ConstructionCode,
	StateRatingEffectiveDate,
	IndividualRiskPremiumModification,
	WindCoverageFlag,
	DeductibleBasis,
	ExposureBasis,
	o_PremiumTransactionStageId AS PremiumTransactionStageId,
	Exposure,
	TransactionCreatedUserId5 AS TransactionCreatedUserId,
	ServiceCentreName5 AS ServiceCentreName,
	o_NumberOfEmployees AS NumberOfEmployee,
	NegateRestateCode,
	WrittenExposure,
	o_Policyakid,
	DeclaredEventFlag
	FROM EXP_Format_PremiumTransaction
),
RTR_Insert_Update_INSERT_PREMIUM AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(lkp_PremiumTransactionAKID) AND NOT (IN(OffsetOnsetCode,'Offset','Deprecated')=1 AND PremiumTransactionAmount=0)),
RTR_Insert_Update_UPDATE_PREMIUM AS (SELECT * FROM RTR_Insert_Update WHERE (NOT ISNULL(lkp_PremiumTransactionAKID)) AND (NOT (IN(OffsetOnsetCode,'Offset','Deprecated')=1 AND PremiumTransactionAmount=0))),
SEQ_PremiumTransactionAKID AS (
	CREATE SEQUENCE SEQ_PremiumTransactionAKID
	START = 0
	INCREMENT = 1;
),
EXP_Set_AKID AS (
	SELECT
	SEQ_PremiumTransactionAKID.NEXTVAL AS i_NEXTVAL,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	LogicalIndicator,
	LogicalDeleteFlag,
	PremiumTransactionHashKey,
	PremiumLoadSequence,
	DuplicateSequence,
	i_NEXTVAL AS PremiumTransactionAKID,
	ReinsuranceCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionKey,
	PMSFunctionCode,
	PremiumTransactionCode,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionExpirationDate,
	PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	FullTermPremium,
	PremiumType,
	ReasonAmendedCode,
	OffsetOnsetCode,
	SupPremiumTransactionCodeId,
	RatingCoverageAKId,
	DeductibleAmount,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate,
	PackageModificationAdjustmentFactor,
	PackageModificationAdjustmentGroupCode,
	IncreasedLimitFactor,
	IncreasedLimitGroupCode,
	YearBuilt,
	AgencyActualCommissionRate,
	BaseRate,
	ConstructionCode,
	StateRatingEffectiveDate,
	IndividualRiskPremiumModification,
	WindCoverageFlag,
	DeductibleBasis,
	ExposureBasis,
	PremiumTransactionStageId,
	Exposure,
	TransactionCreatedUserId AS TransactionCreatedUserId1,
	ServiceCentreName AS ServiceCentreName1,
	NumberOfEmployee AS NumberOfEmployee1,
	NegateRestateCode,
	WrittenExposure AS WrittenExposure1,
	DeclaredEventFlag AS DeclaredEventFlag1
	FROM RTR_Insert_Update_INSERT_PREMIUM
),
TGT_PremiumTransaction_Insert_Incremental AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


	INSERT INTO PremiumTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, PremiumTransactionHashKey, PremiumLoadSequence, DuplicateSequence, PremiumTransactionAKID, ReinsuranceCoverageAKID, StatisticalCoverageAKID, PremiumTransactionKey, PMSFunctionCode, PremiumTransactionCode, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate, PremiumTransactionBookedDate, PremiumTransactionAmount, FullTermPremium, PremiumType, ReasonAmendedCode, OffsetOnsetCode, SupPremiumTransactionCodeId, RatingCoverageAKId, DeductibleAmount, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PackageModificationAdjustmentFactor, PackageModificationAdjustmentGroupCode, IncreasedLimitFactor, IncreasedLimitGroupCode, YearBuilt, AgencyActualCommissionRate, BaseRate, ConstructionCode, StateRatingEffectiveDate, IndividualRiskPremiumModification, WindCoverageFlag, DeductibleBasis, ExposureBasis, TransactionCreatedUserId, ServiceCentreName, Exposure, NumberOfEmployee, NegateRestateCode, WrittenExposure, DeclaredEventFlag)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LOGICALINDICATOR, 
	LOGICALDELETEFLAG, 
	PREMIUMTRANSACTIONHASHKEY, 
	PREMIUMLOADSEQUENCE, 
	DUPLICATESEQUENCE, 
	PREMIUMTRANSACTIONAKID, 
	REINSURANCECOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	PREMIUMTRANSACTIONKEY, 
	PMSFUNCTIONCODE, 
	PREMIUMTRANSACTIONCODE, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONEXPIRATIONDATE, 
	PREMIUMTRANSACTIONBOOKEDDATE, 
	PREMIUMTRANSACTIONAMOUNT, 
	FULLTERMPREMIUM, 
	PREMIUMTYPE, 
	REASONAMENDEDCODE, 
	OFFSETONSETCODE, 
	SUPPREMIUMTRANSACTIONCODEID, 
	RATINGCOVERAGEAKID, 
	DEDUCTIBLEAMOUNT, 
	EXPERIENCEMODIFICATIONFACTOR, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	PACKAGEMODIFICATIONADJUSTMENTFACTOR, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPCODE, 
	INCREASEDLIMITFACTOR, 
	INCREASEDLIMITGROUPCODE, 
	YEARBUILT, 
	AGENCYACTUALCOMMISSIONRATE, 
	BASERATE, 
	CONSTRUCTIONCODE, 
	STATERATINGEFFECTIVEDATE, 
	INDIVIDUALRISKPREMIUMMODIFICATION, 
	WINDCOVERAGEFLAG, 
	DEDUCTIBLEBASIS, 
	EXPOSUREBASIS, 
	TransactionCreatedUserId1 AS TRANSACTIONCREATEDUSERID, 
	ServiceCentreName1 AS SERVICECENTRENAME, 
	EXPOSURE, 
	NumberOfEmployee1 AS NUMBEROFEMPLOYEE, 
	NEGATERESTATECODE, 
	WrittenExposure1 AS WRITTENEXPOSURE, 
	DeclaredEventFlag1 AS DECLAREDEVENTFLAG
	FROM EXP_Set_AKID

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


),
UPD_PremiumTransaction AS (
	SELECT
	lkp_PremiumTransactionID AS PremiumTransactionID, 
	PremiumTransactionCode, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionBookedDate, 
	PremiumType, 
	OffsetOnsetCode, 
	SupPremiumTransactionCodeId, 
	RatingCoverageAKId, 
	AgencyActualCommissionRate, 
	ExposureBasis, 
	Exposure, 
	TransactionCreatedUserId AS TransactionCreatedUserId3, 
	WrittenExposure AS WrittenExposure3, 
	DeclaredEventFlag AS DeclaredEventFlag3
	FROM RTR_Insert_Update_UPDATE_PREMIUM
),
TGT_PremiumTransaction_Update_Incremental AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


	MERGE INTO PremiumTransaction AS T
	USING UPD_PremiumTransaction AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumTransactionCode = S.PremiumTransactionCode, T.PremiumTransactionEnteredDate = S.PremiumTransactionEnteredDate, T.PremiumTransactionEffectiveDate = S.PremiumTransactionEffectiveDate, T.PremiumType = S.PremiumType, T.OffsetOnsetCode = S.OffsetOnsetCode, T.SupPremiumTransactionCodeId = S.SupPremiumTransactionCodeId, T.RatingCoverageAKId = S.RatingCoverageAKId, T.AgencyActualCommissionRate = S.AgencyActualCommissionRate, T.ExposureBasis = S.ExposureBasis, T.TransactionCreatedUserId = S.TransactionCreatedUserId3, T.Exposure = S.Exposure, T.WrittenExposure = S.WrittenExposure3, T.DeclaredEventFlag = S.DeclaredEventFlag3

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


),
SQ_PremiumTransaction_CFA AS (
	SELECT 
	PT.PremiumTransactionID, 
	PT.CurrentSnapshotFlag, 
	PT.AuditID, 
	PT.EffectiveDate, 
	PT.ExpirationDate, 
	PT.SourceSystemID, 
	PT.CreatedDate, 
	PT.ModifiedDate, 
	PT.LogicalIndicator, 
	PT.LogicalDeleteFlag, 
	PT.PremiumTransactionHashKey, 
	PT.PremiumLoadSequence, 
	PT.DuplicateSequence, 
	PT.PremiumTransactionAKID, 
	PT.ReinsuranceCoverageAKID, 
	PT.StatisticalCoverageAKID, 
	PT.PremiumTransactionKey, 
	PT.PMSFunctionCode, 
	PT.PremiumTransactionCode, 
	PT.PremiumTransactionEnteredDate, 
	PT.PremiumTransactionEffectiveDate, 
	PT.PremiumTransactionExpirationDate, 
	PT.PremiumTransactionBookedDate, 
	PT.PremiumTransactionAmount, 
	PT.FullTermPremium, 
	PT.PremiumType, 
	PT.ReasonAmendedCode, 
	PT.OffsetOnsetCode, 
	PT.SupPremiumTransactionCodeId, 
	PT.RatingCoverageAKId, 
	PT.DeductibleAmount, 
	PT.ExperienceModificationFactor, 
	PT.ExperienceModificationEffectiveDate, 
	PT.PackageModificationAdjustmentFactor, 
	PT.PackageModificationAdjustmentGroupCode, 
	PT.IncreasedLimitFactor, 
	PT.IncreasedLimitGroupCode, 
	PT.YearBuilt, 
	PT.AgencyActualCommissionRate, 
	PT.BaseRate, 
	PT.ConstructionCode, 
	PT.StateRatingEffectiveDate, 
	PT.IndividualRiskPremiumModification, 
	PT.WindCoverageFlag, 
	PT.DeductibleBasis, 
	PT.ExposureBasis, 
	PT.TransactionCreatedUserId, 
	PT.ServiceCentreName, 
	PT.Exposure, 
	PT.NumberOfEmployee, 
	PT.NegateRestateCode, 
	PT.WrittenExposure, 
	PT.DeclaredEventFlag, 
	WorkRC.WorkCFARatingCoverageXRefId, 
	WorkRC.WorkCFAPolicyListId, 
	WorkRC.PolicyKey, 
	WorkRC.OriginalRatingCoverageAKID, 
	WorkRC.CFARatingCoverageAKID, 
	WorkRC.OriginalCoverageGuid, 
	WorkRC.CFACoverageGuid, 
	WorkList.Status,
	WorkList.TransactionCreatedDate,
	WorkList.LineageId,
	ISNULL(WorkList.PolicyStateCode,'N/A') as PolicyStateCode,
	P.pol_eff_date
	FROM
	PremiumTransaction PT
	INNER JOIN WorkCFARatingCoverageXRef WorkRC ON PT.RatingCoverageAKId= WorkRC.OriginalRatingCoverageAKID AND PT.EffectiveDate=WorkRC.OriginalEffectiveDate
	INNER JOIN WorkCFAPolicyList  WorkList ON WorkList.WorkCFAPolicyListId=WorkRC.WorkCFAPolicyListId
	INNER JOIN V2.policy P ON WorkList.PolicyKey=P.pol_key and P.crrnt_snpsht_flag=1
	LEFT JOIN WorkCFAPremiumTransactionXRef WorkPT ON WorkPT.OriginalPremiumTransactionAKID = PT.PremiumTransactionAKID
	LEFT JOIN WorkCFAPremiumTransactionXRef WorkPT2 on WorkPT2.CFAPremiumTransactionAKID = PT.PremiumTransactionAKID
	-- add negative tests to exclude any records we may accidentally be running twice
	WHERE 
	WorkList.Status='Processed' 
	AND WorkPT.OriginalPremiumTransactionAKID IS NULL 
	AND WorkPT2.CFAPremiumTransactionAKID IS NULL
	AND NOT PT.PremiumTransactionKey like '%CFA%'
	@{pipeline().parameters.WHERE_CFA}
),
LKP_CFATransactionEffectiveDate AS (
	SELECT
	TransactionEffectiveDate,
	PolicyKey
	FROM (
		SELECT 
		C.TransactionEffectiveDate AS TransactionEffectiveDate
		,RIGHT(REPLACE(P.policykey,'||',''),LEN(REPLACE(P.policykey,'||',''))-1) AS PolicyKey
		from WorkDCTPLPolicy P  WITH (NOLOCK)
		left join WorkDCTPLCoverage C  WITH (NOLOCK)
		on P.PolicyKey=C.PolicyKey
		and P.StartDate=C.StartDate
		where C.MeasureName='WrittenPremium'
		and exists(select 1 from WorkDCTPLPolicy P2  WITH (NOLOCK) where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward') 
		
		UNION
		
		SELECT 
		C.TransactionEffectiveDate AS TransactionEffectiveDate
		,RIGHT(REPLACE(P.policykey,'||',''),LEN(REPLACE(P.policykey,'||',''))-1) AS PolicyKey
		from ArchWorkDCTPLPolicy P WITH (NOLOCK)
		left join ArchWorkDCTPLCoverage C WITH (NOLOCK)
		on P.PolicyKey=C.PolicyKey
		and P.StartDate=C.StartDate
		where C.MeasureName='WrittenPremium'
		and exists(select 1 from ArchWorkDCTPLPolicy P2 WITH (NOLOCK) where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY TransactionEffectiveDate) = 1
),
EXP_Input_CFA AS (
	SELECT
	SQ_PremiumTransaction_CFA.PremiumTransactionID,
	SQ_PremiumTransaction_CFA.CurrentSnapshotFlag,
	SQ_PremiumTransaction_CFA.AuditID,
	SQ_PremiumTransaction_CFA.EffectiveDate,
	SQ_PremiumTransaction_CFA.ExpirationDate,
	SQ_PremiumTransaction_CFA.SourceSystemID,
	SQ_PremiumTransaction_CFA.CreatedDate,
	SQ_PremiumTransaction_CFA.ModifiedDate,
	SQ_PremiumTransaction_CFA.LogicalIndicator,
	SQ_PremiumTransaction_CFA.LogicalDeleteFlag,
	SQ_PremiumTransaction_CFA.PremiumTransactionHashKey,
	SQ_PremiumTransaction_CFA.PremiumLoadSequence,
	SQ_PremiumTransaction_CFA.DuplicateSequence,
	SQ_PremiumTransaction_CFA.PremiumTransactionAKID,
	SQ_PremiumTransaction_CFA.ReinsuranceCoverageAKID,
	SQ_PremiumTransaction_CFA.StatisticalCoverageAKID,
	SQ_PremiumTransaction_CFA.PremiumTransactionKey,
	SQ_PremiumTransaction_CFA.PMSFunctionCode,
	SQ_PremiumTransaction_CFA.PremiumTransactionCode,
	SQ_PremiumTransaction_CFA.PremiumTransactionEnteredDate,
	SQ_PremiumTransaction_CFA.PremiumTransactionEffectiveDate,
	SQ_PremiumTransaction_CFA.PremiumTransactionExpirationDate,
	SQ_PremiumTransaction_CFA.PremiumTransactionBookedDate,
	SQ_PremiumTransaction_CFA.PremiumTransactionAmount,
	SQ_PremiumTransaction_CFA.FullTermPremium,
	SQ_PremiumTransaction_CFA.PremiumType,
	SQ_PremiumTransaction_CFA.ReasonAmendedCode,
	SQ_PremiumTransaction_CFA.OffsetOnsetCode,
	SQ_PremiumTransaction_CFA.SupPremiumTransactionCodeId,
	SQ_PremiumTransaction_CFA.RatingCoverageAKId,
	SQ_PremiumTransaction_CFA.DeductibleAmount,
	SQ_PremiumTransaction_CFA.ExperienceModificationFactor,
	SQ_PremiumTransaction_CFA.ExperienceModificationEffectiveDate,
	SQ_PremiumTransaction_CFA.PackageModificationAdjustmentFactor,
	SQ_PremiumTransaction_CFA.PackageModificationAdjustmentGroupCode,
	SQ_PremiumTransaction_CFA.IncreasedLimitFactor,
	SQ_PremiumTransaction_CFA.IncreasedLimitGroupCode,
	SQ_PremiumTransaction_CFA.YearBuilt,
	SQ_PremiumTransaction_CFA.AgencyActualCommissionRate,
	SQ_PremiumTransaction_CFA.BaseRate,
	SQ_PremiumTransaction_CFA.ConstructionCode,
	SQ_PremiumTransaction_CFA.StateRatingEffectiveDate,
	SQ_PremiumTransaction_CFA.IndividualRiskPremiumModification,
	SQ_PremiumTransaction_CFA.WindCoverageFlag,
	SQ_PremiumTransaction_CFA.DeductibleBasis,
	SQ_PremiumTransaction_CFA.ExposureBasis,
	SQ_PremiumTransaction_CFA.TransactionCreatedUserId,
	SQ_PremiumTransaction_CFA.ServiceCentreName,
	SQ_PremiumTransaction_CFA.Exposure,
	SQ_PremiumTransaction_CFA.NumberOfEmployee,
	SQ_PremiumTransaction_CFA.NegateRestateCode,
	SQ_PremiumTransaction_CFA.WrittenExposure,
	SQ_PremiumTransaction_CFA.DeclaredEventFlag,
	SQ_PremiumTransaction_CFA.WorkCFARatingCoverageXRefId,
	SQ_PremiumTransaction_CFA.WorkCFAPolicyListId,
	SQ_PremiumTransaction_CFA.PolicyKey,
	SQ_PremiumTransaction_CFA.OriginalRatingCoverageAKID,
	SQ_PremiumTransaction_CFA.CFARatingCoverageAKID,
	SQ_PremiumTransaction_CFA.OriginalCoverageGuid,
	SQ_PremiumTransaction_CFA.CFACoverageGuid,
	SQ_PremiumTransaction_CFA.TransactionCreatedDate AS CFATransactionCreatedDate,
	SQ_PremiumTransaction_CFA.Status,
	SQ_PremiumTransaction_CFA.LineageId,
	SQ_PremiumTransaction_CFA.PolicyStateCode,
	SQ_PremiumTransaction_CFA.pol_eff_date,
	SYSDATE AS DefaultDate,
	-- *INF*: DECODE(TRUE,
	-- TO_CHAR(SYSDATE, 'DD' ) ='02' and TO_CHAR( SYSDATE, 'DAY' )='Tuesday',1,
	-- TO_CHAR(SYSDATE, 'DD' ) ='01',1,
	-- 0
	-- )
	DECODE(TRUE,
		TO_CHAR(SYSDATE, 'DD'
		) = '02' 
		AND TO_CHAR(SYSDATE, 'DAY'
		) = 'Tuesday', 1,
		TO_CHAR(SYSDATE, 'DD'
		) = '01', 1,
		0
	) AS v_AdjustForMonthEnd,
	-- *INF*: DECODE(TRUE,
	-- v_AdjustForMonthEnd = 1, ADD_TO_DATE(SYSDATE,'MM',-1),
	-- SYSDATE)
	DECODE(TRUE,
		v_AdjustForMonthEnd = 1, DATEADD(MONTH,- 1,SYSDATE),
		SYSDATE
	) AS v_ExtractDate,
	-- *INF*: IIF
	-- (
	-- NOT ISNULL(:LKP.LKP_SUPSTATECFAXREF_PERCENT(PolicyStateCode,pol_eff_date)),
	-- :LKP.LKP_SUPSTATECFAXREF_PERCENT(PolicyStateCode,pol_eff_date),
	-- :LKP.LKP_SUPSTATECFAXREF_PERCENT('N/A',pol_eff_date)
	-- )
	IFF(LKP_SUPSTATECFAXREF_PERCENT_PolicyStateCode_pol_eff_date.CFARate IS NOT NULL,
		LKP_SUPSTATECFAXREF_PERCENT_PolicyStateCode_pol_eff_date.CFARate,
		LKP_SUPSTATECFAXREF_PERCENT__N_A_pol_eff_date.CFARate
	) AS v_CFARate,
	-- *INF*: GREATEST(TRUNC(v_ExtractDate,'MM'),TRUNC(PremiumTransactionEffectiveDate,'MM'),TRUNC(EffectiveDate,'MM'))
	-- 
	-- 
	-- 
	GREATEST(CAST(TRUNC(v_ExtractDate, 'MONTH') AS TIMESTAMP_NTZ(0)), CAST(TRUNC(PremiumTransactionEffectiveDate, 'MONTH') AS TIMESTAMP_NTZ(0)), CAST(TRUNC(EffectiveDate, 'MONTH') AS TIMESTAMP_NTZ(0))
	) AS o_BookedDate,
	'Complete' AS o_Status,
	-- *INF*: MD5(CFARatingCoverageAKID||TO_CHAR(EffectiveDate)|| 'N/A')
	MD5(CFARatingCoverageAKID || TO_CHAR(EffectiveDate
		) || 'N/A'
	) AS o_PremiumTransactionHashKey,
	-- *INF*: CFACoverageGuid||'||'||TO_CHAR(EffectiveDate)
	CFACoverageGuid || '||' || TO_CHAR(EffectiveDate
	) AS o_PremiumTransactionKey,
	-- *INF*: v_CFARate * PremiumTransactionAmount
	-- 
	-- --PremiumTransactionAmount * (-0.05)
	-- -- using hardcoded AZ rates , come back with a state specific table
	v_CFARate * PremiumTransactionAmount AS o_PremiumTransactionAmount,
	'CFA' AS o_ReasonAmendedCode,
	SEQ_PremiumTransactionAKID.NEXTVAL AS i_NEXTVAL,
	i_NEXTVAL AS o_PremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LKP_CFATransactionEffectiveDate.TransactionEffectiveDate AS LKP_CFATransactionEffectiveDate,
	-- *INF*: IIF(ISNULL(LKP_CFATransactionEffectiveDate),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),LKP_CFATransactionEffectiveDate)
	-- 
	-- 
	-- 
	IFF(LKP_CFATransactionEffectiveDate IS NULL,
		TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		LKP_CFATransactionEffectiveDate
	) AS v_CFATransactionEffectiveDate,
	v_CFATransactionEffectiveDate AS o_CFATransactionEffectiveDate
	FROM SQ_PremiumTransaction_CFA
	LEFT JOIN LKP_CFATransactionEffectiveDate
	ON LKP_CFATransactionEffectiveDate.PolicyKey = SQ_PremiumTransaction_CFA.PolicyKey
	LEFT JOIN LKP_SUPSTATECFAXREF_PERCENT LKP_SUPSTATECFAXREF_PERCENT_PolicyStateCode_pol_eff_date
	ON LKP_SUPSTATECFAXREF_PERCENT_PolicyStateCode_pol_eff_date.StateCode = PolicyStateCode
	AND LKP_SUPSTATECFAXREF_PERCENT_PolicyStateCode_pol_eff_date.EffectiveFromDate = pol_eff_date

	LEFT JOIN LKP_SUPSTATECFAXREF_PERCENT LKP_SUPSTATECFAXREF_PERCENT__N_A_pol_eff_date
	ON LKP_SUPSTATECFAXREF_PERCENT__N_A_pol_eff_date.StateCode = 'N/A'
	AND LKP_SUPSTATECFAXREF_PERCENT__N_A_pol_eff_date.EffectiveFromDate = pol_eff_date

),
AGG_WorkCFAPolicyList AS (
	SELECT
	WorkCFAPolicyListId,
	o_Status AS Status,
	DefaultDate
	FROM EXP_Input_CFA
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WorkCFAPolicyListId ORDER BY NULL) = 1
),
UPD_WorkCFA_PolicyList AS (
	SELECT
	WorkCFAPolicyListId, 
	Status, 
	DefaultDate
	FROM AGG_WorkCFAPolicyList
),
TGT_WorkCFAPolicyList_CFA AS (
	MERGE INTO WorkCFAPolicyList AS T
	USING UPD_WorkCFA_PolicyList AS S
	ON T.WorkCFAPolicyListId = S.WorkCFAPolicyListId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.Status = S.Status, T.ModifiedDate = S.DefaultDate
),
TGT_PremiumTransaction_CFA_Insert AS (
	INSERT INTO PremiumTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, PremiumTransactionHashKey, PremiumLoadSequence, DuplicateSequence, PremiumTransactionAKID, ReinsuranceCoverageAKID, StatisticalCoverageAKID, PremiumTransactionKey, PMSFunctionCode, PremiumTransactionCode, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate, PremiumTransactionBookedDate, PremiumTransactionAmount, FullTermPremium, PremiumType, ReasonAmendedCode, OffsetOnsetCode, SupPremiumTransactionCodeId, RatingCoverageAKId, DeductibleAmount, ExperienceModificationFactor, ExperienceModificationEffectiveDate, PackageModificationAdjustmentFactor, PackageModificationAdjustmentGroupCode, IncreasedLimitFactor, IncreasedLimitGroupCode, YearBuilt, AgencyActualCommissionRate, BaseRate, ConstructionCode, StateRatingEffectiveDate, IndividualRiskPremiumModification, WindCoverageFlag, DeductibleBasis, ExposureBasis, TransactionCreatedUserId, ServiceCentreName, Exposure, NumberOfEmployee, NegateRestateCode, WrittenExposure, DeclaredEventFlag)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	o_AuditId AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	DefaultDate AS CREATEDDATE, 
	DefaultDate AS MODIFIEDDATE, 
	LOGICALINDICATOR, 
	LOGICALDELETEFLAG, 
	o_PremiumTransactionHashKey AS PREMIUMTRANSACTIONHASHKEY, 
	PREMIUMLOADSEQUENCE, 
	DUPLICATESEQUENCE, 
	o_PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	REINSURANCECOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	o_PremiumTransactionKey AS PREMIUMTRANSACTIONKEY, 
	PMSFUNCTIONCODE, 
	PREMIUMTRANSACTIONCODE, 
	CFATransactionCreatedDate AS PREMIUMTRANSACTIONENTEREDDATE, 
	o_CFATransactionEffectiveDate AS PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONEXPIRATIONDATE, 
	o_BookedDate AS PREMIUMTRANSACTIONBOOKEDDATE, 
	o_PremiumTransactionAmount AS PREMIUMTRANSACTIONAMOUNT, 
	FULLTERMPREMIUM, 
	PREMIUMTYPE, 
	o_ReasonAmendedCode AS REASONAMENDEDCODE, 
	OFFSETONSETCODE, 
	SUPPREMIUMTRANSACTIONCODEID, 
	CFARatingCoverageAKID AS RATINGCOVERAGEAKID, 
	DEDUCTIBLEAMOUNT, 
	EXPERIENCEMODIFICATIONFACTOR, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	PACKAGEMODIFICATIONADJUSTMENTFACTOR, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPCODE, 
	INCREASEDLIMITFACTOR, 
	INCREASEDLIMITGROUPCODE, 
	YEARBUILT, 
	AGENCYACTUALCOMMISSIONRATE, 
	BASERATE, 
	CONSTRUCTIONCODE, 
	STATERATINGEFFECTIVEDATE, 
	INDIVIDUALRISKPREMIUMMODIFICATION, 
	WINDCOVERAGEFLAG, 
	DEDUCTIBLEBASIS, 
	EXPOSUREBASIS, 
	TRANSACTIONCREATEDUSERID, 
	SERVICECENTRENAME, 
	EXPOSURE, 
	NUMBEROFEMPLOYEE, 
	NEGATERESTATECODE, 
	WRITTENEXPOSURE, 
	DECLAREDEVENTFLAG
	FROM EXP_Input_CFA
),
TGT_WorkCFAPremiumTransactionXRef_Insert AS (
	INSERT INTO WorkCFAPremiumTransactionXRef
	(WorkCFARatingCoverageXRefId, WorkCFAPolicyListId, PolicyKey, TransactionCreatedDate, LineageId, OriginalPremiumTransactionID, OriginalPremiumTransactionAKID, OriginalPremiumTransactionHashKey, OriginalPremiumTransactionAmount, CFAPremiumTransactionAKID, CFAPremiumTransactionHashKey, CFAPremiumTransactionAmount, AuditId, SourceSysId, CreatedDate, ModifiedDate)
	SELECT 
	WORKCFARATINGCOVERAGEXREFID, 
	WORKCFAPOLICYLISTID, 
	POLICYKEY, 
	CFATransactionCreatedDate AS TRANSACTIONCREATEDDATE, 
	LINEAGEID, 
	PremiumTransactionID AS ORIGINALPREMIUMTRANSACTIONID, 
	PremiumTransactionAKID AS ORIGINALPREMIUMTRANSACTIONAKID, 
	PremiumTransactionHashKey AS ORIGINALPREMIUMTRANSACTIONHASHKEY, 
	PremiumTransactionAmount AS ORIGINALPREMIUMTRANSACTIONAMOUNT, 
	o_PremiumTransactionAKID AS CFAPREMIUMTRANSACTIONAKID, 
	o_PremiumTransactionHashKey AS CFAPREMIUMTRANSACTIONHASHKEY, 
	o_PremiumTransactionAmount AS CFAPREMIUMTRANSACTIONAMOUNT, 
	o_AuditId AS AUDITID, 
	SourceSystemID AS SOURCESYSID, 
	DefaultDate AS CREATEDDATE, 
	DefaultDate AS MODIFIEDDATE
	FROM EXP_Input_CFA
),