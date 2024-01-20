WITH
SQ_PremiumTransaction AS (

------------ PRE SQL ----------
@{pipeline().parameters.SOURCE_PRE_SQL}
----------------------


	WITH CTE_PTransMaxLoadSequence as 
	(
	SELECT Pol.pol_ak_id,RC.CoverageGUID AS CoverageGUID,
	       PT.PremiumTransactionHashKey as PremiumTransactionHashKey,
	       MAX(PT.PremiumLoadSequence)       AS PremiumLoadSequence,
	       W.PolicyKey as PolicyKey,
	       Max(W.IterationId) as IterationID,
	       Max(W.CreatedDate) as CreatedDate
	FROM  dbo.PremiumTransaction PT INNER JOIN  dbo.RatingCoverage RC ON RC.RatingCoverageAKID = PT.RatingCoverageAKId      
	AND RC.EffectiveDate = PT.EffectiveDate AND PT.SourceSystemID='DCT' AND PT.ReasonAmendedCode not in ('CWO','Claw Back') 
	INNER JOIN  dbo.PolicyCoverage PC      
	ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID AND PC.SourceSystemID='DCT'      
	AND PC.CurrentSnapshotFlag = 1   
	INNER JOIN  dbo.RiskLocation RL 
	ON PC.RiskLocationAKID = RL.RiskLocationAKID AND RL.SourceSystemID='DCT'   
	AND RL.CurrentSnapshotFlag = 1   
	INNER JOIN  V2.policy pol    
	ON pol.pol_ak_id = RL.PolicyAKID AND pol.source_sys_id='DCT'
	AND pol.crrnt_snpsht_flag = 1 
	INNER JOIN StrategicProfitCenter spc on spc.StrategicProfitCenterAKId = pol.StrategicProfitCenterAKId
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTDataRepairPolicy W ON W.PolicyKey = pol.pol_key 
	AND spc.StrategicProfitCenterAbbreviation <> 'WB - PL' 
	AND W.createddate > '@{pipeline().parameters.SELECTION_START_TS}' 
	WHERE  PT.SourceSystemID='DCT' 
	GROUP BY Pol.pol_ak_id,RC.CoverageGUID,PT.PremiumTransactionHashKey, W.PolicyKey),
	
	CTE_PTransID as 
	(
	SELECT distinct PT.PremiumTransactionID,CTE.PolicyKey,CTE.IterationId,CTE.CreatedDate as CreatedDate
	FROM dbo.PremiumTransaction pt
	INNER JOIN dbo.RatingCoverage rc
	ON rc.RatingCoverageAKID = pt.RatingCoverageAKId
	AND rc.EffectiveDate = pt.EffectiveDate 
	AND pt.SourceSystemID='DCT' AND PT.ReasonAmendedCode not in ('CWO','Claw Back') AND NegateRestateCode <> 'Negate'
	INNER JOIN CTE_PTransMaxLoadSequence CTE ON CTE.CoverageGUID = rc.CoverageGUID AND CTE.PremiumTransactionHashKey = pt.PremiumTransactionHashKey AND CTE.PremiumLoadSequence = pt.PremiumLoadSequence
	WHERE  pt.SourceSystemID = 'DCT' AND CTE.createddate > '@{pipeline().parameters.SELECTION_START_TS}' )
	
	
	SELECT PT.PremiumTransactionID,
	       PT.EffectiveDate,
	       pt.ExpirationDate,
	       pt.SourceSystemID,
	       pt.LogicalIndicator,
	       pt.PremiumTransactionHashKey,
	       pt.PremiumLoadSequence,
	       pt.DuplicateSequence,
	       pt.PremiumTransactionAKID,
	       pt.ReinsuranceCoverageAKID,
	       pt.StatisticalCoverageAKID,
	       pt.PremiumTransactionKey,
	       pt.PMSFunctionCode,
	       pt.PremiumTransactionCode,
	       pt.PremiumTransactionEnteredDate,
	       pt.PremiumTransactionEffectiveDate,
	       pt.PremiumTransactionExpirationDate,
	       pt.PremiumTransactionBookedDate,
	       pt.PremiumTransactionAmount,
	       pt.FullTermPremium,
	       pt.PremiumType,
	       pt.ReasonAmendedCode,
	       pt.OffsetOnsetCode,
	       pt.SupPremiumTransactionCodeId,
	       pt.RatingCoverageAKId,
	       pt.DeductibleAmount,
	       pt.ExperienceModificationFactor,
	       pt.ExperienceModificationEffectiveDate,
	       pt.PackageModificationAdjustmentFactor,
	       pt.PackageModificationAdjustmentGroupCode,
	       pt.IncreasedLimitFactor,
	       pt.IncreasedLimitGroupCode,
	       pt.YearBuilt,
	       pt.AgencyActualCommissionRate,
	       pt.BaseRate,
	       pt.ConstructionCode,
	       pt.StateRatingEffectiveDate,
	       pt.IndividualRiskPremiumModification,
	       pt.WindCoverageFlag,
	       pt.DeductibleBasis,
	       pt.ExposureBasis,
	       pt.TransactionCreatedUserId,
	       pt.ServiceCentreName,
	       pt.Exposure,
	       pt.NumberOfEmployee,
	       pt.WrittenExposure,
	       pt.DeclaredEventFlag,
	       CTE.PolicyKey,
	       CTE.IterationId,
	       CTE.CreatedDate as CreatedDate
	from dbo.PremiumTransaction PT 
	inner join CTE_PTransID CTE
	on CTE.PremiumTransactionID=PT.PremiumTransactionID
	AND pt.SourceSystemID='DCT' AND PT.ReasonAmendedCode not in ('CWO','Claw Back') AND NegateRestateCode <> 'Negate'
	ORDER BY PT.PremiumTransactionID
),
EXP_Default AS (
	SELECT
	PremiumTransactionID,
	'1' AS CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	LogicalIndicator,
	'0' AS LogicalDeleteFlag,
	PremiumTransactionHashKey,
	PremiumLoadSequence,
	DuplicateSequence,
	PremiumTransactionAKID,
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
	-- *INF*: TO_INTEGER(WindCoverageFlag)
	CAST(WindCoverageFlag AS INTEGER) AS o_WindCoverageFlag,
	DeductibleBasis,
	ExposureBasis,
	TransactionCreatedUserId,
	ServiceCentreName,
	Exposure,
	NumberOfEmployee,
	WrittenExposure,
	DeclaredEventFlag,
	-- *INF*: DECODE(TRUE,
	-- DeclaredEventFlag = 'T',1,
	-- DeclaredEventFlag ='F',0,
	-- ISNULL(DeclaredEventFlag),0
	-- )
	DECODE(
	    TRUE,
	    DeclaredEventFlag = 'T', 1,
	    DeclaredEventFlag = 'F', 0,
	    DeclaredEventFlag IS NULL, 0
	) AS O_DeclaredEventFlag,
	PolicyKey,
	IterationId,
	WCreatedDate
	FROM SQ_PremiumTransaction
),
SEQ_PremiumTransactionAKID AS (
	CREATE SEQUENCE SEQ_PremiumTransactionAKID
	START = 0
	INCREMENT = 1;
),
EXP_OffsetAttributes AS (
	SELECT
	CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	SYSDATE AS CreatedDate,
	LogicalIndicator,
	LogicalDeleteFlag,
	PremiumTransactionHashKey,
	IterationId AS PremiumLoadSequence,
	PremiumLoadSequence+1 AS o_PremiumLoadSequence,
	DuplicateSequence,
	SEQ_PremiumTransactionAKID.NEXTVAL AS PremiumTransactionAKID,
	ReinsuranceCoverageAKID,
	StatisticalCoverageAKID,
	PremiumTransactionKey,
	PMSFunctionCode,
	PremiumTransactionCode,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	WCreatedDate,
	PremiumTransactionExpirationDate,
	-- *INF*: TRUNC(WCreatedDate,'MM')
	CAST(TRUNC(WCreatedDate, 'MONTH') AS TIMESTAMP_NTZ(0)) AS PremiumTransactionBookedDate,
	PremiumTransactionAmount,
	-1 * PremiumTransactionAmount AS o_PremiumTransactionAmount,
	FullTermPremium,
	-1 * FullTermPremium AS O_FullTermPremium,
	PremiumType,
	ReasonAmendedCode,
	OffsetOnsetCode,
	-- *INF*: --IIF(INSTR(OffsetOnsetCode,'Negate'),OffsetOnsetCode, OffsetOnsetCode  ||  '-Negate')
	-- --OffsetOnsetCode  ||  '-Negate'
	'' AS o_OffsetOnsetCode,
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
	o_WindCoverageFlag AS WindCoverageFlag,
	DeductibleBasis,
	ExposureBasis,
	TransactionCreatedUserId,
	ServiceCentreName,
	Exposure,
	NumberOfEmployee,
	WrittenExposure,
	-- *INF*: 0
	-- --WrittenExposure *  - 1
	-- --after deprecated offset is solved, this code can be uncommented to replace the default of zero
	0 AS o_WrittenExposure,
	O_DeclaredEventFlag AS DeclaredEventFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	PremiumTransactionID AS OriginalPremiumTransactionID,
	PremiumTransactionAKID AS OriginalPremiumTransactionAKID,
	'Negate' AS NegateRestateCode,
	-1 AS NewNegatePremiumTransactionID
	FROM EXP_Default
),
LKP_PremiumTransaction AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionHashKey,
	PremiumLoadSequence,
	NegateRestateCode
	FROM (
		SELECT PT.PremiumTransactionID      AS PremiumTransactionID,
		       PT.PremiumTransactionHashKey AS PremiumTransactionHashKey,
		       PT.PremiumLoadSequence       AS PremiumLoadSequence,
		       PT.NegateRestateCode           AS NegateRestateCode
		FROM   dbo.PremiumTransactiON pt
		       INNER JOIN dbo.RatingCoverage rc
		               ON rc.RatingCoverageAKID = pt.RatingCoverageAKId
		                  AND rc.EffectiveDate = pt.EffectiveDate
		       INNER JOIN dbo.PolicyCoverage pc
		               ON pc.PolicyCoverageAKID = rc.PolicyCoverageAKID
		                  AND pc.CurrentSnapshotFlag = 1
		       INNER JOIN dbo.RiskLocatiON rl
		               ON pc.RiskLocatiONAKID = rl.RiskLocatiONAKID
		                  AND rl.CurrentSnapshotFlag = 1
		       INNER JOIN V2.policy pol
		               ON pol.pol_ak_id = pc.PolicyAKID
		                  AND pol.crrnt_snpsht_flag = 1
		       INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.WorkDCTDataRepairPolicy W
		               ON W.PolicyKey = pol.pol_key AND W.CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}'
		WHERE  pt.SourceSystemID = 'DCT' AND PT.NegateRestateCode = 'Negate'
		ORDER BY PT.CreatedDate --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionHashKey,PremiumLoadSequence,NegateRestateCode ORDER BY PremiumTransactionID DESC) = 1
),
RTR_Insert AS (
	SELECT
	LKP_PremiumTransaction.PremiumTransactionID AS lkp_PremiumTransactionID,
	EXP_OffsetAttributes.CurrentSnapshotFlag,
	EXP_OffsetAttributes.EffectiveDate,
	EXP_OffsetAttributes.ExpirationDate,
	EXP_OffsetAttributes.SourceSystemID,
	EXP_OffsetAttributes.CreatedDate,
	EXP_OffsetAttributes.LogicalIndicator,
	EXP_OffsetAttributes.LogicalDeleteFlag,
	EXP_OffsetAttributes.PremiumTransactionHashKey,
	EXP_OffsetAttributes.o_PremiumLoadSequence AS PremiumLoadSequence,
	EXP_OffsetAttributes.DuplicateSequence,
	EXP_OffsetAttributes.PremiumTransactionAKID AS PremiumTransactionAKID1,
	EXP_OffsetAttributes.ReinsuranceCoverageAKID,
	EXP_OffsetAttributes.StatisticalCoverageAKID,
	EXP_OffsetAttributes.PremiumTransactionKey,
	EXP_OffsetAttributes.PMSFunctionCode,
	EXP_OffsetAttributes.PremiumTransactionCode,
	EXP_OffsetAttributes.PremiumTransactionEnteredDate,
	EXP_OffsetAttributes.PremiumTransactionEffectiveDate,
	EXP_OffsetAttributes.PremiumTransactionExpirationDate,
	EXP_OffsetAttributes.PremiumTransactionBookedDate,
	EXP_OffsetAttributes.o_PremiumTransactionAmount AS PremiumTransactionAmount,
	EXP_OffsetAttributes.O_FullTermPremium AS FullTermPremium,
	EXP_OffsetAttributes.PremiumType,
	EXP_OffsetAttributes.ReasonAmendedCode,
	EXP_OffsetAttributes.OffsetOnsetCode,
	EXP_OffsetAttributes.SupPremiumTransactionCodeId,
	EXP_OffsetAttributes.RatingCoverageAKId,
	EXP_OffsetAttributes.DeductibleAmount,
	EXP_OffsetAttributes.ExperienceModificationFactor,
	EXP_OffsetAttributes.ExperienceModificationEffectiveDate,
	EXP_OffsetAttributes.PackageModificationAdjustmentFactor,
	EXP_OffsetAttributes.PackageModificationAdjustmentGroupCode,
	EXP_OffsetAttributes.IncreasedLimitFactor,
	EXP_OffsetAttributes.IncreasedLimitGroupCode,
	EXP_OffsetAttributes.YearBuilt,
	EXP_OffsetAttributes.AgencyActualCommissionRate,
	EXP_OffsetAttributes.BaseRate,
	EXP_OffsetAttributes.ConstructionCode,
	EXP_OffsetAttributes.StateRatingEffectiveDate,
	EXP_OffsetAttributes.IndividualRiskPremiumModification,
	EXP_OffsetAttributes.WindCoverageFlag,
	EXP_OffsetAttributes.DeductibleBasis,
	EXP_OffsetAttributes.ExposureBasis,
	EXP_OffsetAttributes.TransactionCreatedUserId,
	EXP_OffsetAttributes.ServiceCentreName,
	EXP_OffsetAttributes.Exposure,
	EXP_OffsetAttributes.NumberOfEmployee,
	EXP_OffsetAttributes.o_WrittenExposure AS WrittenExposure,
	EXP_OffsetAttributes.DeclaredEventFlag,
	EXP_OffsetAttributes.AuditID,
	EXP_OffsetAttributes.OriginalPremiumTransactionID,
	EXP_OffsetAttributes.OriginalPremiumTransactionAKID,
	EXP_OffsetAttributes.NegateRestateCode,
	EXP_OffsetAttributes.NewNegatePremiumTransactionID
	FROM EXP_OffsetAttributes
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.PremiumTransactionHashKey = EXP_OffsetAttributes.PremiumTransactionHashKey AND LKP_PremiumTransaction.PremiumLoadSequence = EXP_OffsetAttributes.o_PremiumLoadSequence AND LKP_PremiumTransaction.NegateRestateCode = EXP_OffsetAttributes.NegateRestateCode
),
RTR_Insert_Insert AS (SELECT * FROM RTR_Insert WHERE ISNULL(lkp_PremiumTransactionID)),
TGT_PremiumTransaction_Negate_Insert AS (

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
	CreatedDate AS MODIFIEDDATE, 
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
	TRANSACTIONCREATEDUSERID, 
	SERVICECENTRENAME, 
	EXPOSURE, 
	NUMBEROFEMPLOYEE, 
	NEGATERESTATECODE, 
	WRITTENEXPOSURE, 
	DECLAREDEVENTFLAG
	FROM RTR_Insert_Insert

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


),
WorkPremiumTransactionDataRepairNegate AS (
	TRUNCATE TABLE WorkPremiumTransactionDataRepairNegate;
	INSERT INTO WorkPremiumTransactionDataRepairNegate
	(SourceSystemId, CreatedDate, CreatedUserID, OriginalPremiumTransactionID, OriginalPremiumTransactionAKID, NewNegatePremiumTransactionID, NewNegatePremiumTransactionAKID, AuditId)
	SELECT 
	SourceSystemID AS SOURCESYSTEMID, 
	CREATEDDATE, 
	SourceSystemID AS CREATEDUSERID, 
	ORIGINALPREMIUMTRANSACTIONID, 
	ORIGINALPREMIUMTRANSACTIONAKID, 
	NEWNEGATEPREMIUMTRANSACTIONID, 
	PremiumTransactionAKID AS NEWNEGATEPREMIUMTRANSACTIONAKID, 
	AuditID AS AUDITID
	FROM RTR_Insert_Insert
),