WITH
SQ_PremiumMasterFact_Offsets AS (
	SELECT 
	 pmfoffset.PremiumMasterFactID AS PremiumMasterFactID
	,pmfonset.AnnualStatementLineProductCodeDimID AS AnnualStatementLineProductCodeDimID
	,pmfonset.InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId
	,pmfonset.InsuranceReferenceDimId AS InsuranceReferenceDimId
	,pmfonset.PolicyDimID AS PolicyDimID
	,pmfonset.PremiumMasterCoverageExpirationDateID AS PremiumMasterCoverageExpirationDateID
	,pmfonset.RiskLocationDimID AS RiskLocationDimID
	,pmfonset.SalesDivisionDimId AS SalesDivisionDimId
	
	FROM (
		SELECT wrk.PolicyKey
			,wrk.PremiumTransactionAKID
			,wrk.PreviousPremiumTransactionAKID
			,wrk.RatingCoverageAKID
			,pmf.AnnualStatementLineDimID
			,pmf.AnnualStatementLineProductCodeDimID
			,pmf.InsuranceReferenceCoverageDimId
			,pmf.InsuranceReferenceDimId
			,pmf.PolicyDimID
			,pmf.PremiumMasterCoverageExpirationDateID
			,pmf.RiskLocationDimID
			,pmf.SalesDivisionDimId
		FROM @{pipeline().parameters.DATAMART_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact pmf WITH (NOLOCK)
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation pmc WITH (NOLOCK) ON (pmf.EDWPremiumMasterCalculationPKID = pmc.PremiumMasterCalculationID)
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOffsetLineage wrk WITH (NOLOCK) ON (wrk.PreviousPremiumTransactionAKID = pmc.PremiumTransactionAKID and wrk.UpdateAttributeFlag = 1)
		) pmfonset -----Onset
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation pmcof WITH (NOLOCK) ON pmfonset.PremiumTransactionAKID = pmcof.PremiumTransactionAKID
	INNER JOIN @{pipeline().parameters.DATAMART_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact pmfoffset WITH (NOLOCK) ----Offset
		ON (pmcof.PremiumMasterCalculationID = pmfoffset.EDWPremiumMasterCalculationPKID)
		AND pmfonset.AnnualStatementLineDimID = pmfoffset.AnnualStatementLineDimID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT WITH (NOLOCK) on
	pmcof.PremiumTransactionAKID=PT.PremiumTransactionAKID @{pipeline().parameters.WHERE_CLAUSE}
	WHERE (
	   pmfonset.AnnualStatementLineProductCodeDimID <> pmfoffset.AnnualStatementLineProductCodeDimID
	Or pmfonset.InsuranceReferenceCoverageDimId <> pmfoffset.InsuranceReferenceCoverageDimId
	Or pmfonset.InsuranceReferenceDimId <> pmfoffset.InsuranceReferenceDimId
	Or pmfonset.PolicyDimID <> pmfoffset.PolicyDimID
	Or pmfonset.PremiumMasterCoverageExpirationDateID <> pmfoffset.PremiumMasterCoverageExpirationDateID
	Or pmfonset.RiskLocationDimID <> pmfoffset.RiskLocationDimID
	Or pmfonset.SalesDivisionDimId <> pmfoffset.SalesDivisionDimId
	)
),
EXP_PremiumMasterFact_Offsets AS (
	SELECT
	PremiumMasterFactID,
	AnnualStatementLineProductCodeDimID,
	PolicyDimID,
	RiskLocationDimID,
	PremiumMasterCoverageExpirationDateID,
	InsuranceReferenceDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId
	FROM SQ_PremiumMasterFact_Offsets
),
UPD_Update AS (
	SELECT
	PremiumMasterFactID, 
	AnnualStatementLineProductCodeDimID, 
	PolicyDimID, 
	RiskLocationDimID, 
	PremiumMasterCoverageExpirationDateID, 
	InsuranceReferenceDimId, 
	SalesDivisionDimId, 
	InsuranceReferenceCoverageDimId
	FROM EXP_PremiumMasterFact_Offsets
),
TGT_PremiumMasterFact_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMasterFact AS T
	USING UPD_Update AS S
	ON T.PremiumMasterFactID = S.PremiumMasterFactID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AnnualStatementLineProductCodeDimID = S.AnnualStatementLineProductCodeDimID, T.PolicyDimID = S.PolicyDimID, T.RiskLocationDimID = S.RiskLocationDimID, T.PremiumMasterCoverageExpirationDateID = S.PremiumMasterCoverageExpirationDateID, T.InsuranceReferenceDimId = S.InsuranceReferenceDimId, T.SalesDivisionDimId = S.SalesDivisionDimId, T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDimId
),
SQ_PremiumMasterFact_Offsets__PTTypeDimID AS (
	SELECT 
	PMF_offset.PremiumMasterFactID AS PremiumMasterFactID
	,PMF_updt.PremiumTransactionTypeDimID AS PremiumTransactionTypeDimID
	FROM (
		SELECT DISTINCT wrk.PremiumTransactionAKID		,--Offset PTAKID
			wrk.PremiumTransactionID		,--Offset PTID
			wrk.RatingCoverageAKID
			,pmf.PremiumMasterFactID
			,pmf.AnnualStatementLineDimID
			,pttd.PremiumTransactionTypeDimID		,-- Derived PTTD TypeDIMID Value
			pt.PremiumType
			,SRAC.StandardReasonAmendedCode
			,pc1.CustomerCareCommissionRate
			,sptc.StandardPremiumTransactionCode
		FROM @{pipeline().parameters.DATAMART_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact pmf WITH (NOLOCK) 
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation pmc WITH (NOLOCK) ON (pmf.EDWPremiumMasterCalculationPKID = pmc.PremiumMasterCalculationID)
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOffsetLineage wrk WITH (NOLOCK) ON (wrk.PremiumTransactionAKID = pmc.PremiumTransactionAKID and wrk.UpdateAttributeFlag = 1) -- offset PTAKID Value
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT WITH (NOLOCK) ON wrk.PremiumTransactionID = PT.PremiumTransactionID  @{pipeline().parameters.WHERE_CLAUSE}
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC1 WITH (NOLOCK) ON RC1.RatingCoverageAKID = PT.RatingCoverageAKId
			AND PT.EffectiveDate = RC1.EffectiveDate
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC1 WITH (NOLOCK) ON PC1.PolicyCoverageAKID = RC1.PolicyCoverageAKID
			AND PC1.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_premium_transaction_code SPTC WITH (NOLOCK) ON SPTC.sup_prem_trans_code_id = pt.SupPremiumTransactionCodeId
		-- and SPTC.source_sys_id='DCT'  (JIRA OAA -25 )
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_reason_amended_code SRAC WITH (NOLOCK) ON SRAC.rsn_amended_code = pt.ReasonAmendedCode
			AND SRAC.source_sys_id = 'DCT'
		LEFT JOIN @{pipeline().parameters.DATAMART_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionTypeDim PTTD WITH (NOLOCK) ON (
				PT.PremiumType = PTTD.PremiumTypeCode
				AND SRAC.StandardReasonAmendedCode = PTTD.ReasonAmendedCode
				AND SPTC.StandardPremiumTransactionCode = PTTD.PremiumTransactionCode
				AND PC1.CustomerCareCommissionRate = PTTD.CustomerCareCommissionRate
				)
		) PMF_updt
	INNER JOIN @{pipeline().parameters.DATAMART_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF_offset WITH (NOLOCK) ON PMF_offset.PremiumMasterFactID = PMF_updt.PremiumMasterFactID
	WHERE (PMF_offset.PremiumTransactionTypeDimID <> PMF_updt.PremiumTransactionTypeDimID)
),
EXP_PremiumMasterFact_Offsets__PTTypeDimID AS (
	SELECT
	PremiumMasterFactID,
	PremiumTransactionTypeDimID
	FROM SQ_PremiumMasterFact_Offsets__PTTypeDimID
),
UPD_Update__PTTypeDimID AS (
	SELECT
	PremiumMasterFactID, 
	PremiumTransactionTypeDimID
	FROM EXP_PremiumMasterFact_Offsets__PTTypeDimID
),
TGT_PremiumMasterFact_UPDATE_PTTypeDimID AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMasterFact AS T
	USING UPD_Update__PTTypeDimID AS S
	ON T.PremiumMasterFactID = S.PremiumMasterFactID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumTransactionTypeDimID = S.PremiumTransactionTypeDimID
),