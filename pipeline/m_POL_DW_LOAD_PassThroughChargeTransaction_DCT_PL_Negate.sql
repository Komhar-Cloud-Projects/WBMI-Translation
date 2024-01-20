WITH
SQ_PassThroughChargeTransaction AS (

------------ PRE SQL ----------
@{pipeline().parameters.SOURCE_PRE_SQL}
----------------------


	WITH CTE_PTCTMaxLoadSequence as 
	(
	SELECT Pol.pol_ak_id AS PolicyAKID,
	       PTCT.PassThroughChargeTransactionHashKey as PassThroughChargeTransactionHashKey,
	       MAX(PTCT.LoadSequence)       AS MaxLoadSequence
	FROM  dbo.PassThroughChargeTransaction PTCT INNER JOIN  V2.policy pol    
	ON pol.pol_ak_id = PTCT.PolicyAKID 
	AND pol.crrnt_snpsht_flag = 1 
	inner join StrategicProfitCenter spc on spc.StrategicProfitCenterAKId = pol.StrategicProfitCenterAKId
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.WorkDCTDataRepairPolicy W ON W.PolicyKey = pol.pol_key
	WHERE  PTCT.SourceSystemID='DCT' AND spc.StrategicProfitCenterAbbreviation = 'WB - PL'  AND W.createddate > '@{pipeline().parameters.SELECTION_START_TS}' 
	GROUP BY Pol.pol_ak_id , PTCT.PassThroughChargeTransactionHashKey)
	
	SELECT PTCT.PassThroughChargeTransactionID,
	       PTCT.EffectiveDate,
	       PTCT.ExpirationDate,
	       PTCT.SourceSystemID,
	       PTCT.LogicalIndicator,
	       PTCT.DuplicateSequence,
	       PTCT.PassThroughChargeTransactionHashKey,
	       PTCT.PassThroughChargeTransactionAKID,
	       PTCT.StatisticalCoverageAKID,
	       PTCT.PassThroughChargeTransactionCode,
	       PTCT.PassThroughChargeTransactionEnteredDate,
	       PTCT.PassThroughChargeTransactionEffectiveDate,
	       PTCT.PassThroughChargeTransactionExpirationDate,
	       PTCT.PassThroughChargeTransactionBookedDate,
	       PTCT.PassThroughChargeTransactionAmount,
	       PTCT.FullTermPremium,
	       PTCT.FullTaxAmount,
	       PTCT.TaxPercentageRate,
	       PTCT.ReasonAmendedCode,
	       PTCT.PassThroughChargeTransactionCodeId,
	       PTCT.RiskLocationAKID,
	       PTCT.PolicyAKID,
	       PTCT.SupLGTLineOfInsuranceID,
	       PTCT.SupSurchargeExemptID,
	       PTCT.SupPassThroughChargeTypeID,
	       PTCT.TotalAnnualPremiumSubjectToTax,
	       PTCT.RatingCoverageAKID,
	       PTCT.DCTTaxCode,
	       PTCT.OffsetOnsetCode,
	       PTCT.LoadSequence,
	       PTCT.NegateRestateCode,
	       PTCT.RatingCoverageAKID,
	       W.PolicyKey,
	       W.IterationId,
	       W.CreatedDate
	FROM    PassThroughChargeTransaction PTCT
			INNER JOIN V2.policy pol ON pol.pol_ak_id = PTCT.PolicyAKID AND pol.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.WorkDCTDataRepairPolicy W ON W.PolicyKey = pol.pol_key
			INNER JOIN CTE_PTCTMaxLoadSequence CTE ON CTE.PolicyAKID = pol.pol_ak_id
	       AND CTE.PassThroughChargeTransactionHashKey = PTCT.PassThroughChargeTransactionHashKey 
		   AND CTE.MaxLoadSequence = PTCT.LoadSequence
	WHERE  PTCT.SourceSystemID = 'DCT' AND W.createddate > '@{pipeline().parameters.SELECTION_START_TS}' 
	AND  CASE WHEN PTCT.LoadSequence = 1 THEN '1'
	                    WHEN PTCT.LoadSequence > 1 AND PTCT.NegateRestateCode  = 'Restate' 	  
	THEN '1' ELSE '0' END = 1
	@{pipeline().parameters.WHERE_CLAUSE}
	ORDER BY PTCT.PassThroughChargeTransactionID
),
EXP_Default AS (
	SELECT
	PassThroughChargeTransactionID,
	'1' AS CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	LogicalIndicator,
	'0' AS LogicalDeleteFlag,
	DuplicateSequence,
	PassThroughChargeTransactionHashKey,
	PassThroughChargeTransactionAKID,
	StatisticalCoverageAKID,
	PassThroughChargeTransactionCode,
	PassThroughChargeTransactionEnteredDate,
	PassThroughChargeTransactionEffectiveDate,
	PassThroughChargeTransactionExpirationDate,
	PassThroughChargeTransactionBookedDate,
	PassThroughChargeTransactionAmount,
	FullTermPremium,
	FullTaxAmount,
	TaxPercentageRate,
	ReasonAmendedCode,
	PassThroughChargeTransactionCodeId,
	RisklocationAKID,
	PolicyAKID,
	SupLGTLineOfInsuranceID,
	SupSurchargeExemptID,
	SupPassThroughChargeTypeID,
	TotalAnnualPremiumSubjectToTax,
	PolicyCoverageAKId,
	DCTTaxCode,
	OffsetOnsetCode,
	LoadSequence,
	NegateRestateCode,
	RatingCoverageAKID,
	PolicyKey,
	IterationId,
	WCreatedDate
	FROM SQ_PassThroughChargeTransaction
),
SEQ_PassThroughChargeTransactionAKID AS (
	CREATE SEQUENCE SEQ_PassThroughChargeTransactionAKID
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
	PassThroughChargeTransactionHashKey,
	IterationId,
	IterationId+1 AS o_LoadSequence,
	DuplicateSequence,
	SEQ_PassThroughChargeTransactionAKID.NEXTVAL AS NewPassThroughChargeTransactionAKID,
	StatisticalCoverageAKID,
	PassThroughChargeTransactionCode,
	PassThroughChargeTransactionEnteredDate,
	PassThroughChargeTransactionBookedDate AS in_PassThroughChargeTransactionBookedDate,
	PassThroughChargeTransactionEffectiveDate,
	WCreatedDate,
	PassThroughChargeTransactionExpirationDate,
	-- *INF*: TRUNC(GREATEST(in_PassThroughChargeTransactionBookedDate,WCreatedDate), 'MM')
	CAST(TRUNC(GREATEST(in_PassThroughChargeTransactionBookedDate, WCreatedDate), 'MONTH') AS TIMESTAMP_NTZ(0)) AS PassThroughChargeTransactionBookedDate,
	PassThroughChargeTransactionAmount,
	-1 * PassThroughChargeTransactionAmount AS o_PassThroughChargeTransactionAmount,
	FullTermPremium,
	-1 * FullTermPremium AS O_FullTermPremium,
	ReasonAmendedCode,
	OffsetOnsetCode,
	-- *INF*: 99999
	-- --@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	99999 AS AuditID,
	PassThroughChargeTransactionID,
	PassThroughChargeTransactionAKID,
	FullTaxAmount,
	TaxPercentageRate,
	PassThroughChargeTransactionCodeId,
	RisklocationAKID,
	PolicyAKID,
	SupLGTLineOfInsuranceID,
	SupSurchargeExemptID,
	SupPassThroughChargeTypeID,
	TotalAnnualPremiumSubjectToTax,
	PolicyCoverageAKId,
	DCTTaxCode,
	'Negate' AS NegateRestateCode,
	RatingCoverageAKID
	FROM EXP_Default
),
RTR_Insert AS (
	SELECT
	CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	LogicalIndicator,
	LogicalDeleteFlag,
	PassThroughChargeTransactionHashKey,
	DuplicateSequence,
	NewPassThroughChargeTransactionAKID,
	StatisticalCoverageAKID,
	PassThroughChargeTransactionCode,
	PassThroughChargeTransactionEnteredDate,
	PassThroughChargeTransactionEffectiveDate,
	PassThroughChargeTransactionExpirationDate,
	PassThroughChargeTransactionBookedDate,
	o_PassThroughChargeTransactionAmount,
	O_FullTermPremium AS FullTermPremium,
	PremiumType,
	ReasonAmendedCode,
	OffsetOnsetCode,
	AuditID,
	PassThroughChargeTransactionID,
	PassThroughChargeTransactionAKID,
	FullTaxAmount,
	TaxPercentageRate,
	PassThroughChargeTransactionCodeId,
	RisklocationAKID,
	PolicyAKID,
	SupLGTLineOfInsuranceID,
	SupSurchargeExemptID,
	SupPassThroughChargeTypeID,
	TotalAnnualPremiumSubjectToTax,
	PolicyCoverageAKId,
	DCTTaxCode,
	o_LoadSequence AS LoadSequence,
	NegateRestateCode,
	RatingCoverageAKID
	FROM EXP_OffsetAttributes
),
RTR_Insert_Insert AS (SELECT * FROM RTR_Insert WHERE ISNULL(lkp_PremiumTransactionID)),
PassThroughChargeTransaction_Negate_Insert AS (
	INSERT INTO PassThroughChargeTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, DuplicateSequence, PassThroughChargeTransactionHashKey, PassThroughChargeTransactionAKID, StatisticalCoverageAKID, PassThroughChargeTransactionCode, PassThroughChargeTransactionEnteredDate, PassThroughChargeTransactionEffectiveDate, PassThroughChargeTransactionExpirationDate, PassThroughChargeTransactionBookedDate, PassThroughChargeTransactionAmount, FullTermPremium, FullTaxAmount, TaxPercentageRate, ReasonAmendedCode, PassThroughChargeTransactionCodeId, RiskLocationAKID, PolicyAKID, SupLGTLineOfInsuranceID, PolicyCoverageAKID, SupSurchargeExemptID, SupPassThroughChargeTypeID, TotalAnnualPremiumSubjectToTax, DCTTaxCode, OffsetOnsetCode, LoadSequence, NegateRestateCode, RatingCoverageAKID)
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
	DUPLICATESEQUENCE, 
	PASSTHROUGHCHARGETRANSACTIONHASHKEY, 
	NewPassThroughChargeTransactionAKID AS PASSTHROUGHCHARGETRANSACTIONAKID, 
	STATISTICALCOVERAGEAKID, 
	PASSTHROUGHCHARGETRANSACTIONCODE, 
	PASSTHROUGHCHARGETRANSACTIONENTEREDDATE, 
	PASSTHROUGHCHARGETRANSACTIONEFFECTIVEDATE, 
	PASSTHROUGHCHARGETRANSACTIONEXPIRATIONDATE, 
	PASSTHROUGHCHARGETRANSACTIONBOOKEDDATE, 
	o_PassThroughChargeTransactionAmount AS PASSTHROUGHCHARGETRANSACTIONAMOUNT, 
	FULLTERMPREMIUM, 
	FULLTAXAMOUNT, 
	TAXPERCENTAGERATE, 
	REASONAMENDEDCODE, 
	PASSTHROUGHCHARGETRANSACTIONCODEID, 
	RisklocationAKID AS RISKLOCATIONAKID, 
	POLICYAKID, 
	SUPLGTLINEOFINSURANCEID, 
	PolicyCoverageAKId AS POLICYCOVERAGEAKID, 
	SUPSURCHARGEEXEMPTID, 
	SUPPASSTHROUGHCHARGETYPEID, 
	TOTALANNUALPREMIUMSUBJECTTOTAX, 
	DCTTAXCODE, 
	OFFSETONSETCODE, 
	LOADSEQUENCE, 
	NEGATERESTATECODE, 
	RATINGCOVERAGEAKID
	FROM RTR_Insert_Insert
),