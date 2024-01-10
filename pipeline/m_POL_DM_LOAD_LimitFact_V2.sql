WITH
LKP_InsuranceReferenceCoverageDim_DCT AS (
	SELECT
	InsuranceReferenceCoverageDimId,
	InsuranceLineCode,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	DctCoverageVersion
	FROM (
		SELECT 
			InsuranceReferenceCoverageDimId,
			InsuranceLineCode,
			DctRiskTypeCode,
			DctCoverageTypeCode,
			DctPerilGroup,
			DctSubCoverageTypeCode,
			DctCoverageVersion
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim
		WHERE NOT (DctRiskTypeCode='N/A' AND DctCoverageTypeCode='N/A' AND DctPerilGroup='N/A' AND DctSubCoverageTypeCode='N/A' AND DctCoverageVersion='N/A')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctRiskTypeCode,DctCoverageTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY InsuranceReferenceCoverageDimId) = 1
),
LKP_RatingPlan AS (
	SELECT
	RatingPlanCode,
	RatingPlanAKId
	FROM (
		SELECT 
			RatingPlanCode,
			RatingPlanAKId
		FROM RatingPlan
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingPlanAKId ORDER BY RatingPlanCode) = 1
),
LKP_InsuranceReferenceLegalEntity AS (
	SELECT
	InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterAKId
	FROM (
		SELECT IRLE.InsuranceReferenceLegalEntityCode as InsuranceReferenceLegalEntityCode, 
		SPC.StrategicProfitCenterAKId as StrategicProfitCenterAKId 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity IRLE
		on SPC.InsuranceReferenceLegalEntityId=IRLE.InsuranceReferenceLegalEntityId 
		and SPC.CurrentSnapshotFlag=1 and IRLE.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAKId ORDER BY InsuranceReferenceLegalEntityCode) = 1
),
LKP_InsuranceSegment AS (
	SELECT
	InsuranceSegmentCode,
	InsuranceSegmentAKId
	FROM (
		SELECT 
			InsuranceSegmentCode,
			InsuranceSegmentAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentAKId ORDER BY InsuranceSegmentCode) = 1
),
LKP_PolicyOffering AS (
	SELECT
	PolicyOfferingCode,
	PolicyOfferingAKId
	FROM (
		SELECT 
			PolicyOfferingCode,
			PolicyOfferingAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOfferingAKId ORDER BY PolicyOfferingCode) = 1
),
LKP_StrategicProfitCenter AS (
	SELECT
	StrategicProfitCenterCode,
	StrategicProfitCenterAKId
	FROM (
		SELECT 
			StrategicProfitCenterCode,
			StrategicProfitCenterAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAKId ORDER BY StrategicProfitCenterCode) = 1
),
LKP_Product AS (
	SELECT
	ProductCode,
	ProductAKId
	FROM (
		SELECT 
			ProductCode,
			ProductAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductAKId ORDER BY ProductCode) = 1
),
LKP_InsuranceReferenceLineOfBusiness AS (
	SELECT
	InsuranceReferenceLineOfBusinessCode,
	InsuranceReferenceLineOfBusinessAKId
	FROM (
		SELECT 
			InsuranceReferenceLineOfBusinessCode,
			InsuranceReferenceLineOfBusinessAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceLineOfBusinessAKId ORDER BY InsuranceReferenceLineOfBusinessCode) = 1
),
LKP_InsuranceReferenceCoverageDim_PMS AS (
	SELECT
	InsuranceReferenceCoverageDimId,
	InsuranceLineCode,
	PmsRiskUnitGroupCode,
	PmsRiskUnitCode,
	PmsMajorPerilCode,
	PmsProductTypeCode
	FROM (
		SELECT 
			InsuranceReferenceCoverageDimId,
			InsuranceLineCode,
			PmsRiskUnitGroupCode,
			PmsRiskUnitCode,
			PmsMajorPerilCode,
			PmsProductTypeCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim
		WHERE DctRiskTypeCode='N/A' AND DctCoverageTypeCode='N/A' AND DctPerilGroup='N/A' AND DctSubCoverageTypeCode='N/A' AND DctCoverageVersion='N/A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,PmsRiskUnitGroupCode,PmsRiskUnitCode,PmsMajorPerilCode,PmsProductTypeCode ORDER BY InsuranceReferenceCoverageDimId) = 1
),
lkp_Calender_Dim AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id DESC) = 1
),
LKP_EnterpriseGroup AS (
	SELECT
	EnterpriseGroupCode,
	StrategicProfitCenterAKId
	FROM (
		SELECT EG.EnterpriseGroupCode as EnterpriseGroupCode, 
		SPC.StrategicProfitCenterAKId as StrategicProfitCenterAKId 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup EG
		on SPC.EnterpriseGroupID=EG.EnterpriseGroupID and EG.CurrentSnapshotFlag=1 and SPC.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAKId ORDER BY EnterpriseGroupCode) = 1
),
SQ_PremiumTransaction_Tables AS (
	SELECT P.pol_ak_id,
		ISNULL(SIL.StandardInsuranceLineCode, 'N/A') AS StandardInsuranceLineCode,
		PC.TypeBureauCode,
		'N/A' AS RiskUnitGroup,
		'N/A' AS RiskUnit,
		'N/A' AS RiskUnitSequenceNumber,
		'N/A' AS MajorPerilCode,
		'N/A' AS ClassCode,
		RC.CoverageGUID AS CoverageGUID,
		PT.SourceSystemID,
		PT.PremiumTransactionID,
		PT.PremiumTransactionEnteredDate,
		PT.PremiumTransactionEffectiveDate,
		PT.PremiumTransactionBookedDate,
		RC.RiskType,
		RC.CoverageType,
		RC.PerilGroup AS PerilGroup,
		RC.SubCoverageTypeCode AS SubCoverageTypeCode,
		RC.CoverageVersion AS CoverageVersion,
		CL.CoverageLimitType AS CoverageLimitType,
		CL.CoverageLimitValue AS CoverageLimitValue,
		P.StrategicProfitCenterAKId,
		P.InsuranceSegmentAKId,
		P.PolicyOfferingAKId,
		RC.ProductAKId,
		RC.InsuranceReferenceLineOfBusinessAKId,
		PC.RatingPlanAKId,
		PT.OffsetOnsetCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL with (nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB with (nolock) ON CL.CoverageLimitId = CLB.CoverageLimitId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock) ON PT.PremiumTransactionAKID = CLB.PremiumTransactionAKId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock) ON RC.RatingCoverageAKID = PT.RatingCoverageAKId
		AND RC.EffectiveDate = PT.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock) ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock) ON PC.PolicyAKID = P.pol_ak_id
		AND P.crrnt_snpsht_flag = 1
	INNER JOIN (
				SELECT DISTINCT PC2.PolicyAKID
	              ,CoverageGUID
	              ,CL2.CoverageLimitType
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 with (nolock)
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB2 with (nolock) ON PT2.PremiumTransactionAKId = CLB2.PremiumTransactionAKId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL2 with (nolock) ON CLB2.CoverageLimitId = CL2.CoverageLimitId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC2 with (nolock) ON PT2.RatingCoverageAKId = RC2.RatingCoverageAKId
					AND PT2.EffectiveDate = RC2.EffectiveDate
		        INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC2 with (nolock) ON RC2.PolicyCoverageAKID=PC2.PolicyCoverageAKID
				AND PC2.CurrentSnapshotFlag=1
					WHERE CLB2.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}') x
				ON PC.PolicyAKID = x.PolicyAKID
					AND RC.CoverageGUID = x.CoverageGUID
					AND CL.CoverageLimitType = x.CoverageLimitType
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL with (nolock) ON SIL.ins_line_code = PC.InsuranceLine
		AND SIL.crrnt_snpsht_flag = 1
		AND SIL.source_sys_id = 'DCT'
	WHERE PT.SourceSystemId = 'DCT' 
	AND P.pol_ak_id%3=1
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
	
	UNION ALL
	SELECT P.pol_ak_id,
		ISNULL(SIL.StandardInsuranceLineCode, 'N/A') AS StandardInsuranceLineCode,
		PC.TypeBureauCode,
		'N/A' AS RiskUnitGroup,
		'N/A' AS RiskUnit,
		'N/A' AS RiskUnitSequenceNumber,
		'N/A' AS MajorPerilCode,
		'N/A' AS ClassCode,
		RC.CoverageGUID AS CoverageGUID,
		PT.SourceSystemID,
		PT.PremiumTransactionID,
		PT.PremiumTransactionEnteredDate,
		PT.PremiumTransactionEffectiveDate,
		PT.PremiumTransactionBookedDate,
		RC.RiskType,
		RC.CoverageType,
		RC.PerilGroup AS PerilGroup,
		RC.SubCoverageTypeCode AS SubCoverageTypeCode,
		RC.CoverageVersion AS CoverageVersion,
		CL.CoverageLimitType AS CoverageLimitType,
		CL.CoverageLimitValue AS CoverageLimitValue,
		P.StrategicProfitCenterAKId,
		P.InsuranceSegmentAKId,
		P.PolicyOfferingAKId,
		RC.ProductAKId,
		RC.InsuranceReferenceLineOfBusinessAKId,
		PC.RatingPlanAKId,
		PT.OffsetOnsetCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL with (nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB with (nolock) ON CL.CoverageLimitId = CLB.CoverageLimitId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock) ON PT.PremiumTransactionAKID = CLB.PremiumTransactionAKId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock) ON RC.RatingCoverageAKID = PT.RatingCoverageAKId
		AND RC.EffectiveDate = PT.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock) ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock) ON PC.PolicyAKID = P.pol_ak_id
		AND P.crrnt_snpsht_flag = 1
	INNER JOIN (
				SELECT DISTINCT PC2.PolicyAKID
	              ,CoverageGUID
	              ,CL2.CoverageLimitType
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 with (nolock)
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB2 with (nolock) ON PT2.PremiumTransactionAKId = CLB2.PremiumTransactionAKId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL2 with (nolock) ON CLB2.CoverageLimitId = CL2.CoverageLimitId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC2 with (nolock) ON PT2.RatingCoverageAKId = RC2.RatingCoverageAKId
					AND PT2.EffectiveDate = RC2.EffectiveDate
		        INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC2 with (nolock) ON RC2.PolicyCoverageAKID=PC2.PolicyCoverageAKID
				AND PC2.CurrentSnapshotFlag=1
					WHERE CLB2.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}') x
				ON PC.PolicyAKID = x.PolicyAKID
					AND RC.CoverageGUID = x.CoverageGUID
					AND CL.CoverageLimitType = x.CoverageLimitType
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL with (nolock) ON SIL.ins_line_code = PC.InsuranceLine
		AND SIL.crrnt_snpsht_flag = 1
		AND SIL.source_sys_id = 'DCT'
	WHERE PT.SourceSystemId = 'DCT' 
	AND P.pol_ak_id%3=2
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
),
EXP_Audit AS (
	SELECT
	pol_ak_id,
	StandardInsuranceLineCode,
	TypeBureauCode,
	RiskUnitGroup,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	ClassCode,
	CoverageGUID,
	SourceSystemID,
	PremiumTransactionID,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionBookedDate,
	RiskType,
	CoverageType,
	PerilGroup,
	SubCoverageTypeCode,
	CoverageVersion,
	CoverageLimitType,
	CoverageLimitValue,
	StrategicProfitCenterAKId,
	InsuranceSegmentAKId,
	PolicyOfferingAKId,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	RatingPlanAKId,
	OffsetOnsetCode,
	'SQ1' AS o_SourceQualifier,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP(
	) AS o_Date,
	'1' AS o_AuditID
	FROM SQ_PremiumTransaction_Tables
),
FIL_PremTrans_CovLimitNumeric AS (
	SELECT
	o_AuditID AS AuditID, 
	SourceSystemID, 
	o_Date AS Date, 
	pol_ak_id, 
	StandardInsuranceLineCode, 
	TypeBureauCode, 
	RiskUnitGroup, 
	RiskUnit, 
	RiskUnitSequenceNumber, 
	MajorPerilCode, 
	ClassCode, 
	CoverageGUID, 
	PremiumTransactionID, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionBookedDate, 
	RiskType, 
	CoverageType, 
	PerilGroup, 
	SubCoverageTypeCode, 
	CoverageVersion, 
	CoverageLimitType, 
	CoverageLimitValue, 
	StrategicProfitCenterAKId, 
	InsuranceSegmentAKId, 
	PolicyOfferingAKId, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	RatingPlanAKId, 
	OffsetOnsetCode, 
	o_SourceQualifier AS SourceQualifier
	FROM EXP_Audit
	WHERE IS_NUMBER(CoverageLimitValue)=1
),
WorkLimitFact AS (
	TRUNCATE TABLE WorkLimitFact;
	INSERT INTO WorkLimitFact
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, PolicyAKId, StandardInsuranceLineCode, TypeBureauCode, RiskUnitGroup, RiskUnit, RiskUnitSequenceNumber, MajorPerilCode, ClassCode, CoverageGUID, PremiumTransactionID, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionBookedDate, RiskType, CoverageType, PerilGroup, SubCoverageTypeCode, CoverageVersion, CoverageLimitType, CoverageLimitValue, StrategicProfitCenterAKId, InsuranceSegmentAKId, PolicyOfferingAKId, ProductAKId, InsuranceReferenceLineOfBusinessAKId, RatingPlanAKId, OffsetOnsetCode, SourceQualifier)
	SELECT 
	AuditID AS AUDITID, 
	SOURCESYSTEMID, 
	Date AS CREATEDDATE, 
	Date AS MODIFIEDDATE, 
	pol_ak_id AS POLICYAKID, 
	STANDARDINSURANCELINECODE, 
	TYPEBUREAUCODE, 
	RISKUNITGROUP, 
	RISKUNIT, 
	RISKUNITSEQUENCENUMBER, 
	MAJORPERILCODE, 
	CLASSCODE, 
	COVERAGEGUID, 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONBOOKEDDATE, 
	RISKTYPE, 
	COVERAGETYPE, 
	PERILGROUP, 
	SUBCOVERAGETYPECODE, 
	COVERAGEVERSION, 
	COVERAGELIMITTYPE, 
	COVERAGELIMITVALUE, 
	STRATEGICPROFITCENTERAKID, 
	INSURANCESEGMENTAKID, 
	POLICYOFFERINGAKID, 
	PRODUCTAKID, 
	INSURANCEREFERENCELINEOFBUSINESSAKID, 
	RATINGPLANAKID, 
	OFFSETONSETCODE, 
	SOURCEQUALIFIER
	FROM FIL_PremTrans_CovLimitNumeric
),
SQ_PremiumTransaction_Tables_1 AS (
	WITH cnt
	AS (
		SELECT 1 AS cnt
		
		UNION ALL
		
		SELECT cnt + 1
		FROM cnt
		WHERE cnt < 2
		)
	SELECT P.pol_ak_id,
		ISNULL(SIL.StandardInsuranceLineCode, 'N/A') AS StandardInsuranceLineCode,
		PC.TypeBureauCode,
		'N/A' AS RiskUnitGroup,
		'N/A' AS RiskUnit,
		'N/A' AS RiskUnitSequenceNumber,
		'N/A' AS MajorPerilCode,
		'N/A' AS ClassCode,
		RC.CoverageGUID AS CoverageGUID,
		PT.SourceSystemID,
		PT.PremiumTransactionID,
		PT.PremiumTransactionEnteredDate,
		PT.PremiumTransactionEffectiveDate,
		PT.PremiumTransactionBookedDate,
		RC.RiskType,
		RC.CoverageType,
		RC.PerilGroup AS PerilGroup,
		RC.SubCoverageTypeCode AS SubCoverageTypeCode,
		RC.CoverageVersion AS CoverageVersion,
		CASE WHEN cnt = 1 THEN 'StatedAmount' WHEN cnt = 2 THEN 'CostNew' END AS CoverageLimitType,
		CASE WHEN cnt = 1 THEN CA.StatedAmount WHEN cnt = 2 THEN convert(VARCHAR(20), CA.CostNew) END AS CoverageLimitValue,
		P.StrategicProfitCenterAKId,
		P.InsuranceSegmentAKId,
		P.PolicyOfferingAKId,
		RC.ProductAKId,
		RC.InsuranceReferenceLineOfBusinessAKId,
		PC.RatingPlanAKId,
		PT.OffsetOnsetCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA with (nolock) ON PT.PremiumTransactionID = CA.PremiumTransactionID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock) ON RC.RatingCoverageAKID = PT.RatingCoverageAKId
		AND RC.EffectiveDate = PT.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock) ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock) ON PC.PolicyAKID = P.pol_ak_id
		AND P.crrnt_snpsht_flag = 1
	INNER JOIN cnt ON cnt <= 2
	INNER JOIN (
				SELECT DISTINCT PC2.PolicyAKID,
	                    RC2.CoverageGUID
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 with (nolock)
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA2 with (nolock) ON PT2.PremiumTransactionId = CA2.PremiumTransactionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC2 with (nolock) ON PT2.RatingCoverageAKId = RC2.RatingCoverageAKId
					AND PT2.EffectiveDate = RC2.EffectiveDate
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC2 with (nolock) ON RC2.PolicyCoverageAKID=PC2.PolicyCoverageAKID
				    AND PC2.CurrentSnapshotFlag=1
				WHERE CA2.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
				) x
			ON PC.PolicyAKID = x.PolicyAKID
					AND RC.CoverageGUID = x.CoverageGUID
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL with (nolock) ON SIL.ins_line_code = PC.InsuranceLine
		AND SIL.crrnt_snpsht_flag = 1
		AND SIL.source_sys_id = 'DCT'
	WHERE PT.SourceSystemId = 'DCT'
	AND P.pol_ak_id%3=1
	@{pipeline().parameters.WHERE_CLAUSE_DCT_CA}
	
	UNION ALL
	WITH cnt
	AS (
		SELECT 1 AS cnt
		
		UNION ALL
		
		SELECT cnt + 1
		FROM cnt
		WHERE cnt < 2
		)
	SELECT P.pol_ak_id,
		ISNULL(SIL.StandardInsuranceLineCode, 'N/A') AS StandardInsuranceLineCode,
		PC.TypeBureauCode,
		'N/A' AS RiskUnitGroup,
		'N/A' AS RiskUnit,
		'N/A' AS RiskUnitSequenceNumber,
		'N/A' AS MajorPerilCode,
		'N/A' AS ClassCode,
		RC.CoverageGUID AS CoverageGUID,
		PT.SourceSystemID,
		PT.PremiumTransactionID,
		PT.PremiumTransactionEnteredDate,
		PT.PremiumTransactionEffectiveDate,
		PT.PremiumTransactionBookedDate,
		RC.RiskType,
		RC.CoverageType,
		RC.PerilGroup AS PerilGroup,
		RC.SubCoverageTypeCode AS SubCoverageTypeCode,
		RC.CoverageVersion AS CoverageVersion,
		CASE WHEN cnt = 1 THEN 'StatedAmount' WHEN cnt = 2 THEN 'CostNew' END AS CoverageLimitType,
		CASE WHEN cnt = 1 THEN CA.StatedAmount WHEN cnt = 2 THEN convert(VARCHAR(20), CA.CostNew) END AS CoverageLimitValue,
		P.StrategicProfitCenterAKId,
		P.InsuranceSegmentAKId,
		P.PolicyOfferingAKId,
		RC.ProductAKId,
		RC.InsuranceReferenceLineOfBusinessAKId,
		PC.RatingPlanAKId,
		PT.OffsetOnsetCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA with (nolock) ON PT.PremiumTransactionID = CA.PremiumTransactionID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock) ON RC.RatingCoverageAKID = PT.RatingCoverageAKId
		AND RC.EffectiveDate = PT.EffectiveDate
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock) ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock) ON PC.PolicyAKID = P.pol_ak_id
		AND P.crrnt_snpsht_flag = 1
	INNER JOIN cnt ON cnt <= 2
	INNER JOIN (
				SELECT DISTINCT PC2.PolicyAKID,
	                    RC2.CoverageGUID
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 with (nolock)
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA2 with (nolock) ON PT2.PremiumTransactionId = CA2.PremiumTransactionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC2 with (nolock) ON PT2.RatingCoverageAKId = RC2.RatingCoverageAKId
					AND PT2.EffectiveDate = RC2.EffectiveDate
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC2 with (nolock) ON RC2.PolicyCoverageAKID=PC2.PolicyCoverageAKID
				    AND PC2.CurrentSnapshotFlag=1
				WHERE CA2.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
				) x
			ON PC.PolicyAKID = x.PolicyAKID
					AND RC.CoverageGUID = x.CoverageGUID
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL with (nolock) ON SIL.ins_line_code = PC.InsuranceLine
		AND SIL.crrnt_snpsht_flag = 1
		AND SIL.source_sys_id = 'DCT'
	WHERE PT.SourceSystemId = 'DCT'
	AND P.pol_ak_id%3=2
	@{pipeline().parameters.WHERE_CLAUSE_DCT_CA}
),
EXP_Audit1 AS (
	SELECT
	pol_ak_id,
	StandardInsuranceLineCode,
	TypeBureauCode,
	RiskUnitGroup,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	ClassCode,
	CoverageGUID,
	SourceSystemID,
	PremiumTransactionID,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionBookedDate,
	RiskType,
	CoverageType,
	PerilGroup,
	SubCoverageTypeCode,
	CoverageVersion,
	CoverageLimitType,
	CoverageLimitValue,
	StrategicProfitCenterAKId,
	InsuranceSegmentAKId,
	PolicyOfferingAKId,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	RatingPlanAKId,
	OffsetOnsetCode,
	'SQ2' AS o_SourceQualifier,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP(
	) AS o_Date,
	'1' AS o_AuditID
	FROM SQ_PremiumTransaction_Tables_1
),
FIL_PremTrans1_CovLimitNumeric AS (
	SELECT
	o_AuditID AS AuditID, 
	SourceSystemID, 
	o_Date AS Date, 
	pol_ak_id, 
	StandardInsuranceLineCode, 
	TypeBureauCode, 
	RiskUnitGroup, 
	RiskUnit, 
	RiskUnitSequenceNumber, 
	MajorPerilCode, 
	ClassCode, 
	CoverageGUID, 
	PremiumTransactionID, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionBookedDate, 
	RiskType, 
	CoverageType, 
	PerilGroup, 
	SubCoverageTypeCode, 
	CoverageVersion, 
	CoverageLimitType, 
	CoverageLimitValue, 
	StrategicProfitCenterAKId, 
	InsuranceSegmentAKId, 
	PolicyOfferingAKId, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	RatingPlanAKId, 
	OffsetOnsetCode, 
	o_SourceQualifier AS SourceQualifier
	FROM EXP_Audit1
	WHERE IS_NUMBER(CoverageLimitValue)=1
),
WorkLimitFact1 AS (
	INSERT INTO WorkLimitFact
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, PolicyAKId, StandardInsuranceLineCode, TypeBureauCode, RiskUnitGroup, RiskUnit, RiskUnitSequenceNumber, MajorPerilCode, ClassCode, CoverageGUID, PremiumTransactionID, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionBookedDate, RiskType, CoverageType, PerilGroup, SubCoverageTypeCode, CoverageVersion, CoverageLimitType, CoverageLimitValue, StrategicProfitCenterAKId, InsuranceSegmentAKId, PolicyOfferingAKId, ProductAKId, InsuranceReferenceLineOfBusinessAKId, RatingPlanAKId, OffsetOnsetCode, SourceQualifier)
	SELECT 
	AuditID AS AUDITID, 
	SOURCESYSTEMID, 
	Date AS CREATEDDATE, 
	Date AS MODIFIEDDATE, 
	pol_ak_id AS POLICYAKID, 
	STANDARDINSURANCELINECODE, 
	TYPEBUREAUCODE, 
	RISKUNITGROUP, 
	RISKUNIT, 
	RISKUNITSEQUENCENUMBER, 
	MAJORPERILCODE, 
	CLASSCODE, 
	COVERAGEGUID, 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONBOOKEDDATE, 
	RISKTYPE, 
	COVERAGETYPE, 
	PERILGROUP, 
	SUBCOVERAGETYPECODE, 
	COVERAGEVERSION, 
	COVERAGELIMITTYPE, 
	COVERAGELIMITVALUE, 
	STRATEGICPROFITCENTERAKID, 
	INSURANCESEGMENTAKID, 
	POLICYOFFERINGAKID, 
	PRODUCTAKID, 
	INSURANCEREFERENCELINEOFBUSINESSAKID, 
	RATINGPLANAKID, 
	OFFSETONSETCODE, 
	SOURCEQUALIFIER
	FROM FIL_PremTrans1_CovLimitNumeric
),
SQ_PremiumTransaction_Tables_2 AS (
	WITH cnt
	AS (
		SELECT 1 AS cnt
		
		UNION ALL
		
		SELECT cnt + 1
		FROM cnt
		WHERE cnt < 2
		)
	SELECT P.pol_ak_id,
		ISNULL(SIL.StandardInsuranceLineCode, 'N/A') AS StandardInsuranceLineCode,
		PC.TypeBureauCode,
		SC.RiskUnitGroup,
		SC.RiskUnit,
		SC.RiskUnitSequenceNumber,
		SC.MajorPerilCode,
		SC.ClassCode,
		SC.CoverageGUID AS CoverageGUID,
		PT.SourceSystemID,
		PT.PremiumTransactionID,
		PT.PremiumTransactionEnteredDate,
		PT.PremiumTransactionEffectiveDate,
		PT.PremiumTransactionBookedDate,
		'N/A' AS RiskType,
		'N/A' AS CoverageType,
		'N/A' AS PerilGroup,
		'N/A' AS SubCoverageTypeCode,
		'N/A' AS CoverageVersion,
		CASE WHEN cnt = 1 THEN 'StatedAmount' WHEN cnt = 2 THEN 'CostNew' END AS CoverageLimitType,
		CASE WHEN cnt = 1 THEN CA.StatedAmount WHEN cnt = 2 THEN convert(VARCHAR(20), CA.CostNew) END AS CoverageLimitValue,
		P.StrategicProfitCenterAKId,
		P.InsuranceSegmentAKId,
		P.PolicyOfferingAKId,
		SC.ProductAKId,
		SC.InsuranceReferenceLineOfBusinessAKId,
		PC.RatingPlanAKId,
		PT.OffsetOnsetCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT  with (nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA  with (nolock) ON CA.PremiumTransactionID = PT.PremiumTransactionID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC  with (nolock) ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC  with (nolock) ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P  with (nolock) ON PC.PolicyAKID = P.pol_ak_id
		AND P.crrnt_snpsht_flag = 1
	INNER JOIN cnt ON cnt <= 2
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL  with (nolock) ON SIL.ins_line_code = PC.InsuranceLine
		AND SIL.crrnt_snpsht_flag = 1
		AND SIL.source_sys_id = 'EXCEED AND PMS'
	WHERE PT.SourceSystemId = 'PMS'
		AND EXISTS (
				SELECT 1
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 with (nolock)
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA2 with (nolock) ON PT2.PremiumTransactionId = CA2.PremiumTransactionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC2 with (nolock) ON PT2.StatisticalCoverageAKId = SC2.StatisticalCoverageAKId
				WHERE SC.CoverageGUID = SC2.CoverageGUID
					AND PT2.SourceSystemId = 'PMS'
					AND CA2.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
				)	
	@{pipeline().parameters.WHERE_CLAUSE_PMS_CA}
),
EXP_Audit2 AS (
	SELECT
	pol_ak_id,
	StandardInsuranceLineCode,
	TypeBureauCode,
	RiskUnitGroup,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	ClassCode,
	CoverageGUID,
	SourceSystemID,
	PremiumTransactionID,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionBookedDate,
	RiskType,
	CoverageType,
	PerilGroup,
	SubCoverageTypeCode,
	CoverageVersion,
	CoverageLimitType,
	CoverageLimitValue,
	StrategicProfitCenterAKId,
	InsuranceSegmentAKId,
	PolicyOfferingAKId,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	RatingPlanAKId,
	OffsetOnsetCode,
	'SQ3' AS o_SourceQualifier,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP(
	) AS o_Date,
	'1' AS o_AuditID
	FROM SQ_PremiumTransaction_Tables_2
),
FIL_PremTrans2_CovLimitNumeric AS (
	SELECT
	o_AuditID AS AuditID, 
	SourceSystemID, 
	o_Date AS Date, 
	pol_ak_id, 
	StandardInsuranceLineCode, 
	TypeBureauCode, 
	RiskUnitGroup, 
	RiskUnit, 
	RiskUnitSequenceNumber, 
	MajorPerilCode, 
	ClassCode, 
	CoverageGUID, 
	PremiumTransactionID, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionBookedDate, 
	RiskType, 
	CoverageType, 
	PerilGroup, 
	SubCoverageTypeCode, 
	CoverageVersion, 
	CoverageLimitType, 
	CoverageLimitValue, 
	StrategicProfitCenterAKId, 
	InsuranceSegmentAKId, 
	PolicyOfferingAKId, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	RatingPlanAKId, 
	OffsetOnsetCode, 
	o_SourceQualifier AS SourceQualifier
	FROM EXP_Audit2
	WHERE IS_NUMBER(CoverageLimitValue)=1
),
WorkLimitFact2 AS (
	INSERT INTO WorkLimitFact
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, PolicyAKId, StandardInsuranceLineCode, TypeBureauCode, RiskUnitGroup, RiskUnit, RiskUnitSequenceNumber, MajorPerilCode, ClassCode, CoverageGUID, PremiumTransactionID, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionBookedDate, RiskType, CoverageType, PerilGroup, SubCoverageTypeCode, CoverageVersion, CoverageLimitType, CoverageLimitValue, StrategicProfitCenterAKId, InsuranceSegmentAKId, PolicyOfferingAKId, ProductAKId, InsuranceReferenceLineOfBusinessAKId, RatingPlanAKId, OffsetOnsetCode, SourceQualifier)
	SELECT 
	AuditID AS AUDITID, 
	SOURCESYSTEMID, 
	Date AS CREATEDDATE, 
	Date AS MODIFIEDDATE, 
	pol_ak_id AS POLICYAKID, 
	STANDARDINSURANCELINECODE, 
	TYPEBUREAUCODE, 
	RISKUNITGROUP, 
	RISKUNIT, 
	RISKUNITSEQUENCENUMBER, 
	MAJORPERILCODE, 
	CLASSCODE, 
	COVERAGEGUID, 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONBOOKEDDATE, 
	RISKTYPE, 
	COVERAGETYPE, 
	PERILGROUP, 
	SUBCOVERAGETYPECODE, 
	COVERAGEVERSION, 
	COVERAGELIMITTYPE, 
	COVERAGELIMITVALUE, 
	STRATEGICPROFITCENTERAKID, 
	INSURANCESEGMENTAKID, 
	POLICYOFFERINGAKID, 
	PRODUCTAKID, 
	INSURANCEREFERENCELINEOFBUSINESSAKID, 
	RATINGPLANAKID, 
	OFFSETONSETCODE, 
	SOURCEQUALIFIER
	FROM FIL_PremTrans2_CovLimitNumeric
),
SQ_PremiumTransaction_Tables_3 AS (
	WITH cnt
	AS (
		SELECT 1 AS cnt
		
		UNION ALL
		
		SELECT cnt + 1
		FROM cnt
		WHERE cnt < 2
		)
	SELECT P.pol_ak_id,
		ISNULL(SIL.StandardInsuranceLineCode, 'N/A') AS StandardInsuranceLineCode,
		PC.TypeBureauCode,
		SC.RiskUnitGroup,
		SC.RiskUnit,
		SC.RiskUnitSequenceNumber,
		SC.MajorPerilCode,
		SC.ClassCode,
		SC.CoverageGUID AS CoverageGUID,
		PT.SourceSystemID,
		PT.PremiumTransactionID,
		PT.PremiumTransactionEnteredDate,
		PT.PremiumTransactionEffectiveDate,
		PT.PremiumTransactionBookedDate,
		'N/A' AS RiskType,
		'N/A' AS CoverageType,
		'N/A' AS PerilGroup,
		'N/A' AS SubCoverageTypeCode,
		'N/A' AS CoverageVersion,
		CL.CoverageLimitType AS CoverageLimitType,
		CL.CoverageLimitValue AS CoverageLimitValue,
		P.StrategicProfitCenterAKId,
		P.InsuranceSegmentAKId,
		P.PolicyOfferingAKId,
		SC.ProductAKId,
		SC.InsuranceReferenceLineOfBusinessAKId,
		PC.RatingPlanAKId,
		PT.OffsetOnsetCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL  with (nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB with (nolock) ON CL.CoverageLimitId = CLB.CoverageLimitId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock) ON PT.PremiumTransactionAKID = CLB.PremiumTransactionAKId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC with (nolock) ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock) ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock) ON PC.PolicyAKID = P.pol_ak_id
		AND P.crrnt_snpsht_flag = 1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL with (nolock) ON SIL.ins_line_code = PC.InsuranceLine
		AND SIL.crrnt_snpsht_flag = 1
		AND SIL.source_sys_id = 'EXCEED AND PMS'
	WHERE PT.SourceSystemId = 'PMS'
		AND EXISTS (
				SELECT 1
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 with (nolock)
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB2 with (nolock) ON PT2.PremiumTransactionAKId = CLB2.PremiumTransactionAKId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL2 with (nolock) ON CLB2.CoverageLimitId = CL2.CoverageLimitId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC2 with (nolock) ON PT2.StatisticalCoverageAKId = SC2.StatisticalCoverageAKId
				WHERE SC.CoverageGUID = SC2.CoverageGUID
					AND CL.CoverageLimitType = CL2.CoverageLimitType
					AND PT2.SourceSystemId = 'PMS'
					AND CLB2.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
				)
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
),
EXP_Audit3 AS (
	SELECT
	pol_ak_id,
	StandardInsuranceLineCode,
	TypeBureauCode,
	RiskUnitGroup,
	RiskUnit,
	RiskUnitSequenceNumber,
	MajorPerilCode,
	ClassCode,
	CoverageGUID,
	SourceSystemID,
	PremiumTransactionID,
	PremiumTransactionEnteredDate,
	PremiumTransactionEffectiveDate,
	PremiumTransactionBookedDate,
	RiskType,
	CoverageType,
	PerilGroup,
	SubCoverageTypeCode,
	CoverageVersion,
	CoverageLimitType,
	CoverageLimitValue,
	StrategicProfitCenterAKId,
	InsuranceSegmentAKId,
	PolicyOfferingAKId,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	RatingPlanAKId,
	OffsetOnsetCode,
	'SQ4' AS o_SourceQualifier,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP(
	) AS o_Date,
	'1' AS o_AuditID
	FROM SQ_PremiumTransaction_Tables_3
),
FIL_PremTrans3_CovLimitNumeric AS (
	SELECT
	o_AuditID AS AuditID, 
	SourceSystemID, 
	o_Date AS Date, 
	pol_ak_id, 
	StandardInsuranceLineCode, 
	TypeBureauCode, 
	RiskUnitGroup, 
	RiskUnit, 
	RiskUnitSequenceNumber, 
	MajorPerilCode, 
	ClassCode, 
	CoverageGUID, 
	PremiumTransactionID, 
	PremiumTransactionEnteredDate, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionBookedDate, 
	RiskType, 
	CoverageType, 
	PerilGroup, 
	SubCoverageTypeCode, 
	CoverageVersion, 
	CoverageLimitType, 
	CoverageLimitValue, 
	StrategicProfitCenterAKId, 
	InsuranceSegmentAKId, 
	PolicyOfferingAKId, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	RatingPlanAKId, 
	OffsetOnsetCode, 
	o_SourceQualifier AS SourceQualifier
	FROM EXP_Audit3
	WHERE IS_NUMBER(CoverageLimitValue)=1
),
WorkLimitFact3 AS (
	INSERT INTO WorkLimitFact
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, PolicyAKId, StandardInsuranceLineCode, TypeBureauCode, RiskUnitGroup, RiskUnit, RiskUnitSequenceNumber, MajorPerilCode, ClassCode, CoverageGUID, PremiumTransactionID, PremiumTransactionEnteredDate, PremiumTransactionEffectiveDate, PremiumTransactionBookedDate, RiskType, CoverageType, PerilGroup, SubCoverageTypeCode, CoverageVersion, CoverageLimitType, CoverageLimitValue, StrategicProfitCenterAKId, InsuranceSegmentAKId, PolicyOfferingAKId, ProductAKId, InsuranceReferenceLineOfBusinessAKId, RatingPlanAKId, OffsetOnsetCode, SourceQualifier)
	SELECT 
	AuditID AS AUDITID, 
	SOURCESYSTEMID, 
	Date AS CREATEDDATE, 
	Date AS MODIFIEDDATE, 
	pol_ak_id AS POLICYAKID, 
	STANDARDINSURANCELINECODE, 
	TYPEBUREAUCODE, 
	RISKUNITGROUP, 
	RISKUNIT, 
	RISKUNITSEQUENCENUMBER, 
	MAJORPERILCODE, 
	CLASSCODE, 
	COVERAGEGUID, 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONENTEREDDATE, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PREMIUMTRANSACTIONBOOKEDDATE, 
	RISKTYPE, 
	COVERAGETYPE, 
	PERILGROUP, 
	SUBCOVERAGETYPECODE, 
	COVERAGEVERSION, 
	COVERAGELIMITTYPE, 
	COVERAGELIMITVALUE, 
	STRATEGICPROFITCENTERAKID, 
	INSURANCESEGMENTAKID, 
	POLICYOFFERINGAKID, 
	PRODUCTAKID, 
	INSURANCEREFERENCELINEOFBUSINESSAKID, 
	RATINGPLANAKID, 
	OFFSETONSETCODE, 
	SOURCEQUALIFIER
	FROM FIL_PremTrans3_CovLimitNumeric
),
SQ_WorkLimitFact AS (
	SELECT
		WorkLimitFactId,
		AuditId,
		SourceSystemID,
		CreatedDate,
		ModifiedDate,
		PolicyAKId,
		StandardInsuranceLineCode,
		TypeBureauCode,
		RiskUnitGroup,
		RiskUnit,
		RiskUnitSequenceNumber,
		MajorPerilCode,
		ClassCode,
		CoverageGUID,
		PremiumTransactionID,
		PremiumTransactionEnteredDate,
		PremiumTransactionEffectiveDate,
		PremiumTransactionBookedDate,
		RiskType,
		CoverageType,
		PerilGroup,
		SubCoverageTypeCode,
		CoverageVersion,
		CoverageLimitType,
		CoverageLimitValue,
		StrategicProfitCenterAKId,
		InsuranceSegmentAKId,
		PolicyOfferingAKId,
		ProductAKId,
		InsuranceReferenceLineOfBusinessAKId,
		RatingPlanAKId,
		OffsetOnsetCode,
		SourceQualifier
	FROM WorkLimitFact4
),
SRT_All_SQData AS (
	SELECT
	PolicyAKId AS pol_ak_id, 
	CoverageGUID, 
	CoverageLimitType, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionEnteredDate, 
	OffsetOnsetCode, 
	PremiumTransactionBookedDate, 
	PremiumTransactionID, 
	StandardInsuranceLineCode, 
	TypeBureauCode, 
	RiskUnitGroup, 
	RiskUnit, 
	RiskUnitSequenceNumber, 
	MajorPerilCode, 
	ClassCode, 
	SourceSystemID, 
	RiskType, 
	CoverageType, 
	PerilGroup, 
	SubCoverageTypeCode, 
	CoverageVersion, 
	CoverageLimitValue, 
	StrategicProfitCenterAKId, 
	InsuranceSegmentAKId, 
	PolicyOfferingAKId, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	RatingPlanAKId
	FROM SQ_WorkLimitFact
	ORDER BY pol_ak_id ASC, CoverageGUID ASC, CoverageLimitType ASC, PremiumTransactionEffectiveDate ASC, PremiumTransactionEnteredDate ASC, OffsetOnsetCode ASC, PremiumTransactionBookedDate ASC, PremiumTransactionID ASC
),
LKP_LimitTypeDim AS (
	SELECT
	LimitTypeDimID,
	LimitType
	FROM (
		SELECT 
			LimitTypeDimID,
			LimitType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.LimitTypeDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LimitType ORDER BY LimitTypeDimID) = 1
),
FIL_Invalid AS (
	SELECT
	LKP_LimitTypeDim.LimitTypeDimID, 
	SRT_All_SQData.pol_ak_id, 
	SRT_All_SQData.StandardInsuranceLineCode, 
	SRT_All_SQData.TypeBureauCode, 
	SRT_All_SQData.RiskUnitGroup, 
	SRT_All_SQData.RiskUnit, 
	SRT_All_SQData.RiskUnitSequenceNumber, 
	SRT_All_SQData.MajorPerilCode, 
	SRT_All_SQData.ClassCode, 
	SRT_All_SQData.CoverageGUID, 
	SRT_All_SQData.SourceSystemID, 
	SRT_All_SQData.PremiumTransactionID, 
	SRT_All_SQData.PremiumTransactionEnteredDate, 
	SRT_All_SQData.PremiumTransactionEffectiveDate, 
	SRT_All_SQData.PremiumTransactionBookedDate, 
	SRT_All_SQData.RiskType, 
	SRT_All_SQData.CoverageType, 
	SRT_All_SQData.PerilGroup, 
	SRT_All_SQData.SubCoverageTypeCode, 
	SRT_All_SQData.CoverageVersion, 
	SRT_All_SQData.CoverageLimitType, 
	SRT_All_SQData.CoverageLimitValue, 
	SRT_All_SQData.StrategicProfitCenterAKId, 
	SRT_All_SQData.InsuranceSegmentAKId, 
	SRT_All_SQData.PolicyOfferingAKId, 
	SRT_All_SQData.ProductAKId, 
	SRT_All_SQData.InsuranceReferenceLineOfBusinessAKId, 
	SRT_All_SQData.RatingPlanAKId
	FROM SRT_All_SQData
	LEFT JOIN LKP_LimitTypeDim
	ON LKP_LimitTypeDim.LimitType = SRT_All_SQData.CoverageLimitType
	WHERE NOT ISNULL(LimitTypeDimID)
),
mplt_PolicyDimID_PremiumMaster_Coverage AS (WITH
	Input AS (
		
	),
	EXP_Default AS (
		SELECT
		IN_PolicyAKID AS PolicyAKID,
		IN_Trans_Date
		FROM Input
	),
	LKP_V2_Policy AS (
		SELECT
		contract_cust_ak_id,
		agencyakid,
		pol_status_code,
		strtgc_bus_dvsn_ak_id,
		IN_Trans_Date,
		pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT policy.contract_cust_ak_id as contract_cust_ak_id, policy.agencyakid as agencyakid, policy.pol_status_code as pol_status_code, policy.strtgc_bus_dvsn_ak_id as strtgc_bus_dvsn_ak_id, policy.pol_ak_id as pol_ak_id, policy.eff_from_date as eff_from_date, policy.eff_to_date as eff_to_date FROM 
			V2.policy
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_ak_id DESC) = 1
	),
	LKP_PolicyDimID AS (
		SELECT
		pol_dim_id,
		pol_key,
		pol_eff_date,
		pol_exp_date,
		pms_pol_lob_code,
		ClassOfBusinessCode,
		IN_Trans_Date,
		edw_pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pol_dim_id,
				pol_key,
				pol_eff_date,
				pol_exp_date,
				pms_pol_lob_code,
				ClassOfBusinessCode,
				IN_Trans_Date,
				edw_pol_ak_id,
				eff_from_date,
				eff_to_date
			FROM policy_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
	),
	LKP_V3_AgencyDimID AS (
		SELECT
		agency_dim_id,
		edw_agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT AgencyDim.AgencyDimID as agency_dim_id, AgencyDim.EDWAgencyAKID as edw_agency_ak_id, AgencyDim.EffectiveDate as eff_from_date, AgencyDim.ExpirationDate as eff_to_date
			 FROM V3.AgencyDim as AgencyDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_ContractCustomerDim AS (
		SELECT
		contract_cust_dim_id,
		IN_Trans_Date,
		edw_contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_dim_id,
				IN_Trans_Date,
				edw_contract_cust_ak_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
	),
	lkp_StrategicBusinessDivisionDIM AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		edw_strtgc_bus_dvsn_ak_id
		FROM (
			SELECT strategic_business_division_dim.strtgc_bus_dvsn_dim_id as strtgc_bus_dvsn_dim_id, strategic_business_division_dim.edw_strtgc_bus_dvsn_ak_id as edw_strtgc_bus_dvsn_ak_id 
			FROM strategic_business_division_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_strtgc_bus_dvsn_ak_id ORDER BY strtgc_bus_dvsn_dim_id DESC) = 1
	),
	EXP_Values AS (
		SELECT
		LKP_V3_AgencyDimID.agency_dim_id,
		LKP_ContractCustomerDim.contract_cust_dim_id,
		LKP_PolicyDimID.pol_dim_id,
		LKP_V2_Policy.pol_status_code,
		LKP_PolicyDimID.pol_eff_date,
		LKP_PolicyDimID.pol_exp_date,
		lkp_StrategicBusinessDivisionDIM.strtgc_bus_dvsn_dim_id,
		LKP_PolicyDimID.pol_key,
		LKP_PolicyDimID.pms_pol_lob_code,
		LKP_PolicyDimID.ClassOfBusinessCode
		FROM 
		LEFT JOIN LKP_ContractCustomerDim
		ON LKP_ContractCustomerDim.edw_contract_cust_ak_id = LKP_V2_Policy.contract_cust_ak_id AND LKP_ContractCustomerDim.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_ContractCustomerDim.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_PolicyDimID
		ON LKP_PolicyDimID.edw_pol_ak_id = EXP_Default.PolicyAKID AND LKP_PolicyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_PolicyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V2_Policy
		ON LKP_V2_Policy.pol_ak_id = EXP_Default.PolicyAKID AND LKP_V2_Policy.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V2_Policy.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V3_AgencyDimID
		ON LKP_V3_AgencyDimID.edw_agency_ak_id = LKP_V2_Policy.agencyakid AND LKP_V3_AgencyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V3_AgencyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN lkp_StrategicBusinessDivisionDIM
		ON lkp_StrategicBusinessDivisionDIM.edw_strtgc_bus_dvsn_ak_id = LKP_V2_Policy.strtgc_bus_dvsn_ak_id
	),
	Output AS (
		SELECT
		agency_dim_id, 
		contract_cust_dim_id, 
		pol_dim_id, 
		pol_status_code, 
		pol_eff_date, 
		pol_exp_date, 
		strtgc_bus_dvsn_dim_id, 
		pol_key, 
		pms_pol_lob_code, 
		ClassOfBusinessCode
		FROM EXP_Values
	),
),
EXP_CalculateValue AS (
	SELECT
	mplt_PolicyDimID_PremiumMaster_Coverage.agency_dim_id AS lkp_AgencyDimID,
	mplt_PolicyDimID_PremiumMaster_Coverage.contract_cust_dim_id AS lkp_contract_cust_dim_id,
	mplt_PolicyDimID_PremiumMaster_Coverage.pol_dim_id AS lkp_pol_dim_id,
	FIL_Invalid.StandardInsuranceLineCode AS i_StandardInsuranceLineCode,
	FIL_Invalid.TypeBureauCode AS i_TypeBureauCode,
	FIL_Invalid.RiskUnitGroup AS i_RiskUnitGroup,
	FIL_Invalid.RiskUnit AS i_RiskUnit,
	FIL_Invalid.RiskUnitSequenceNumber AS i_RiskUnitSequenceNumber,
	FIL_Invalid.MajorPerilCode AS i_MajorPerilCode,
	FIL_Invalid.ClassCode AS i_ClassCode,
	FIL_Invalid.PremiumTransactionBookedDate AS i_PremiumTransactionBookedDate,
	FIL_Invalid.StrategicProfitCenterAKId AS i_StrategicProfitCenterAKId,
	FIL_Invalid.InsuranceSegmentAKId AS i_InsuranceSegmentAKId,
	FIL_Invalid.PolicyOfferingAKId AS i_PolicyOfferingAKId,
	FIL_Invalid.ProductAKId AS i_ProductAKId,
	FIL_Invalid.InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	FIL_Invalid.RatingPlanAKId AS i_RatingPlanAKId,
	FIL_Invalid.LimitTypeDimID,
	FIL_Invalid.CoverageGUID,
	FIL_Invalid.SourceSystemID,
	FIL_Invalid.PremiumTransactionID,
	FIL_Invalid.PremiumTransactionEffectiveDate,
	FIL_Invalid.RiskType,
	FIL_Invalid.CoverageType,
	FIL_Invalid.PerilGroup,
	FIL_Invalid.SubCoverageTypeCode,
	FIL_Invalid.CoverageVersion,
	FIL_Invalid.CoverageLimitType,
	FIL_Invalid.CoverageLimitValue,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE('21001231','YYYYMMDD'))
	LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD.clndr_id AS v_clndr_id,
	-- *INF*: IIF(NOT ISNULL(v_clndr_id),v_clndr_id,-1)
	IFF(v_clndr_id IS NOT NULL,
		v_clndr_id,
		- 1
	) AS v_default_clndr_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode
	) AS v_MajorPerilCode,
	-- *INF*: IIF(LTRIM(v_MajorPerilCode,'0')='' OR REG_MATCH(v_MajorPerilCode,'[^0-9a-zA-Z]'),'N/A',v_MajorPerilCode)
	IFF(LTRIM(v_MajorPerilCode, '0'
		) = '' 
		OR REGEXP_LIKE(v_MajorPerilCode, '[^0-9a-zA-Z]'
		),
		'N/A',
		v_MajorPerilCode
	) AS v_Reg_MajorPerilCode,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TRUNC(i_PremiumTransactionBookedDate,'D'))
	LKP_CALENDER_DIM_TRUNC_i_PremiumTransactionBookedDate_D.clndr_id AS v_PremiumTransactionBookedDateID,
	-- *INF*: IIF(REG_MATCH(i_StandardInsuranceLineCode,'[^0-9a-zA-Z]'),'N/A',i_StandardInsuranceLineCode)
	IFF(REGEXP_LIKE(i_StandardInsuranceLineCode, '[^0-9a-zA-Z]'
		),
		'N/A',
		i_StandardInsuranceLineCode
	) AS v_Reg_StandardInsuranceLineCode,
	-- *INF*: IIF(v_Reg_StandardInsuranceLineCode='N/A' AND (IN(i_TypeBureauCode,'AL','AN','AP') OR IN(v_Reg_MajorPerilCode,'930','931')),'CA',v_Reg_StandardInsuranceLineCode)
	IFF(v_Reg_StandardInsuranceLineCode = 'N/A' 
		AND ( i_TypeBureauCode IN ('AL','AN','AP') 
			OR v_Reg_MajorPerilCode IN ('930','931') 
		),
		'CA',
		v_Reg_StandardInsuranceLineCode
	) AS v_StandardInsuranceLineCode,
	-- *INF*: IIF(v_StandardInsuranceLineCode='N/A' AND IN(i_TypeBureauCode,'CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),1,0)
	IFF(v_StandardInsuranceLineCode = 'N/A' 
		AND i_TypeBureauCode IN ('CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),
		1,
		0
	) AS v_flag,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode
	) AS v_ClassCode,
	-- *INF*: IIF(IN(v_StandardInsuranceLineCode,'CR') OR v_flag=1,'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup))
	IFF(v_StandardInsuranceLineCode IN ('CR') 
		OR v_flag = 1,
		'N/A',
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup
		)
	) AS v_Risk_Unit_Group,
	-- *INF*: IIF(LTRIM(v_Risk_Unit_Group,'0')='','N/A',v_Risk_Unit_Group)
	IFF(LTRIM(v_Risk_Unit_Group, '0'
		) = '',
		'N/A',
		v_Risk_Unit_Group
	) AS v_Zero_Risk_Unit_Group,
	-- *INF*: IIF(   v_flag=1 OR   (v_StandardInsuranceLineCode='GL' AND (v_MajorPerilCode<>'540'    OR NOT IN(v_ClassCode,'11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058')))   OR IN(v_StandardInsuranceLineCode,'WC','IM','CG','CA')=1,  'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit) )
	IFF(v_flag = 1 
		OR ( v_StandardInsuranceLineCode = 'GL' 
			AND ( v_MajorPerilCode <> '540' 
				OR NOT v_ClassCode IN ('11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058') 
			) 
		) 
		OR v_StandardInsuranceLineCode IN ('WC','IM','CG','CA') = 1,
		'N/A',
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit
		)
	) AS v_RiskUnit,
	-- *INF*: IIF(LTRIM(v_RiskUnit,'0')='','N/A',v_RiskUnit)
	IFF(LTRIM(v_RiskUnit, '0'
		) = '',
		'N/A',
		v_RiskUnit
	) AS v_Zero_RiskUnit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber,2,1))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber, 2, 1
		)
	) AS v_ProductTypeCode,
	-- *INF*: :LKP.LKP_STRATEGICPROFITCENTER(i_StrategicProfitCenterAKId)
	LKP_STRATEGICPROFITCENTER_i_StrategicProfitCenterAKId.StrategicProfitCenterCode AS v_StrategicProfitCenterCode,
	-- *INF*: :LKP.LKP_ENTERPRISEGROUP(i_StrategicProfitCenterAKId)
	LKP_ENTERPRISEGROUP_i_StrategicProfitCenterAKId.EnterpriseGroupCode AS v_EnterpriseGroupCode,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCELEGALENTITY(i_StrategicProfitCenterAKId)
	LKP_INSURANCEREFERENCELEGALENTITY_i_StrategicProfitCenterAKId.InsuranceReferenceLegalEntityCode AS v_InsuranceReferenceLegalEntityCode,
	-- *INF*: :LKP.LKP_POLICYOFFERING(i_PolicyOfferingAKId)
	LKP_POLICYOFFERING_i_PolicyOfferingAKId.PolicyOfferingCode AS v_PolicyOfferingCode,
	-- *INF*: :LKP.LKP_INSURANCESEGMENT(i_InsuranceSegmentAKId)
	LKP_INSURANCESEGMENT_i_InsuranceSegmentAKId.InsuranceSegmentCode AS v_InsuranceSegmentCode,
	-- *INF*: :LKP.LKP_PRODUCT(i_ProductAKId)
	LKP_PRODUCT_i_ProductAKId.ProductCode AS v_ProductCode,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCELINEOFBUSINESS(i_InsuranceReferenceLineOfBusinessAKId)
	LKP_INSURANCEREFERENCELINEOFBUSINESS_i_InsuranceReferenceLineOfBusinessAKId.InsuranceReferenceLineOfBusinessCode AS v_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: :LKP.LKP_RATINGPLAN(i_RatingPlanAKId)
	LKP_RATINGPLAN_i_RatingPlanAKId.RatingPlanCode AS v_RatingPlanCode,
	-- *INF*: IIF(ISNULL(v_PremiumTransactionBookedDateID),v_default_clndr_id,v_PremiumTransactionBookedDateID)
	IFF(v_PremiumTransactionBookedDateID IS NULL,
		v_default_clndr_id,
		v_PremiumTransactionBookedDateID
	) AS o_RunDateID,
	-- *INF*: IIF(REG_MATCH(v_Zero_Risk_Unit_Group,'[^0-9a-zA-Z]'),'N/A',v_Zero_Risk_Unit_Group)
	IFF(REGEXP_LIKE(v_Zero_Risk_Unit_Group, '[^0-9a-zA-Z]'
		),
		'N/A',
		v_Zero_Risk_Unit_Group
	) AS o_RiskUnitGroup,
	-- *INF*: IIF(REG_MATCH(v_Zero_RiskUnit,'[^0-9a-zA-Z]'),'N/A',v_Zero_RiskUnit)
	IFF(REGEXP_LIKE(v_Zero_RiskUnit, '[^0-9a-zA-Z]'
		),
		'N/A',
		v_Zero_RiskUnit
	) AS o_RiskUnit,
	v_Reg_MajorPerilCode AS o_MajorPerilCode,
	-- *INF*: IIF(SourceSystemID='PMS',v_StandardInsuranceLineCode,v_Reg_StandardInsuranceLineCode)
	IFF(SourceSystemID = 'PMS',
		v_StandardInsuranceLineCode,
		v_Reg_StandardInsuranceLineCode
	) AS o_StandardInsuranceLineCode,
	-- *INF*: IIF(   REG_MATCH(v_ProductTypeCode,'[^0-9a-zA-Z]') OR v_Reg_StandardInsuranceLineCode<>'GL' OR v_ProductTypeCode='0',   'N/A',v_ProductTypeCode )
	IFF(REGEXP_LIKE(v_ProductTypeCode, '[^0-9a-zA-Z]'
		) 
		OR v_Reg_StandardInsuranceLineCode <> 'GL' 
		OR v_ProductTypeCode = '0',
		'N/A',
		v_ProductTypeCode
	) AS o_ProductTypeCode,
	-- *INF*: IIF(NOT ISNULL(v_ProductCode), v_ProductCode, '000')
	IFF(v_ProductCode IS NOT NULL,
		v_ProductCode,
		'000'
	) AS o_ProductCode,
	-- *INF*: IIF(NOT ISNULL(v_PolicyOfferingCode), v_PolicyOfferingCode, '000')
	IFF(v_PolicyOfferingCode IS NOT NULL,
		v_PolicyOfferingCode,
		'000'
	) AS o_PolicyOfferingCode,
	-- *INF*: IIF(NOT ISNULL(v_InsuranceReferenceLineOfBusinessCode), v_InsuranceReferenceLineOfBusinessCode, '000')
	IFF(v_InsuranceReferenceLineOfBusinessCode IS NOT NULL,
		v_InsuranceReferenceLineOfBusinessCode,
		'000'
	) AS o_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: IIF(NOT ISNULL(v_EnterpriseGroupCode), v_EnterpriseGroupCode, '1')
	IFF(v_EnterpriseGroupCode IS NOT NULL,
		v_EnterpriseGroupCode,
		'1'
	) AS o_EnterpriseGroupCode,
	-- *INF*: IIF(NOT ISNULL(v_InsuranceReferenceLegalEntityCode), v_InsuranceReferenceLegalEntityCode, '1')
	IFF(v_InsuranceReferenceLegalEntityCode IS NOT NULL,
		v_InsuranceReferenceLegalEntityCode,
		'1'
	) AS o_InsuranceReferenceLegalEntityCode,
	-- *INF*: IIF(NOT ISNULL(v_StrategicProfitCenterCode), v_StrategicProfitCenterCode, '6')
	IFF(v_StrategicProfitCenterCode IS NOT NULL,
		v_StrategicProfitCenterCode,
		'6'
	) AS o_StrategicProfitCenterCode,
	-- *INF*: IIF(NOT ISNULL(v_InsuranceSegmentCode), v_InsuranceSegmentCode, 'N/A')
	IFF(v_InsuranceSegmentCode IS NOT NULL,
		v_InsuranceSegmentCode,
		'N/A'
	) AS o_InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(lkp_contract_cust_dim_id),-1,lkp_contract_cust_dim_id)
	IFF(lkp_contract_cust_dim_id IS NULL,
		- 1,
		lkp_contract_cust_dim_id
	) AS o_contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(lkp_pol_dim_id),-1,lkp_pol_dim_id)
	IFF(lkp_pol_dim_id IS NULL,
		- 1,
		lkp_pol_dim_id
	) AS o_pol_dim_id,
	-- *INF*: IIF(ISNULL(lkp_AgencyDimID),-1,lkp_AgencyDimID)
	IFF(lkp_AgencyDimID IS NULL,
		- 1,
		lkp_AgencyDimID
	) AS o_AgencyDimID,
	-- *INF*: IIF(ISNULL(v_RatingPlanCode), '1', v_RatingPlanCode)
	IFF(v_RatingPlanCode IS NULL,
		'1',
		v_RatingPlanCode
	) AS o_RatingPlanCode,
	FIL_Invalid.pol_ak_id
	FROM FIL_Invalid
	 -- Manually join with mplt_PolicyDimID_PremiumMaster_Coverage
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD
	ON LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD.clndr_date = TO_DATE('21001231', 'YYYYMMDD'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TRUNC_i_PremiumTransactionBookedDate_D
	ON LKP_CALENDER_DIM_TRUNC_i_PremiumTransactionBookedDate_D.clndr_date = CAST(TRUNC(i_PremiumTransactionBookedDate, 'DAY') AS TIMESTAMP_NTZ(0))

	LEFT JOIN LKP_STRATEGICPROFITCENTER LKP_STRATEGICPROFITCENTER_i_StrategicProfitCenterAKId
	ON LKP_STRATEGICPROFITCENTER_i_StrategicProfitCenterAKId.StrategicProfitCenterAKId = i_StrategicProfitCenterAKId

	LEFT JOIN LKP_ENTERPRISEGROUP LKP_ENTERPRISEGROUP_i_StrategicProfitCenterAKId
	ON LKP_ENTERPRISEGROUP_i_StrategicProfitCenterAKId.StrategicProfitCenterAKId = i_StrategicProfitCenterAKId

	LEFT JOIN LKP_INSURANCEREFERENCELEGALENTITY LKP_INSURANCEREFERENCELEGALENTITY_i_StrategicProfitCenterAKId
	ON LKP_INSURANCEREFERENCELEGALENTITY_i_StrategicProfitCenterAKId.StrategicProfitCenterAKId = i_StrategicProfitCenterAKId

	LEFT JOIN LKP_POLICYOFFERING LKP_POLICYOFFERING_i_PolicyOfferingAKId
	ON LKP_POLICYOFFERING_i_PolicyOfferingAKId.PolicyOfferingAKId = i_PolicyOfferingAKId

	LEFT JOIN LKP_INSURANCESEGMENT LKP_INSURANCESEGMENT_i_InsuranceSegmentAKId
	ON LKP_INSURANCESEGMENT_i_InsuranceSegmentAKId.InsuranceSegmentAKId = i_InsuranceSegmentAKId

	LEFT JOIN LKP_PRODUCT LKP_PRODUCT_i_ProductAKId
	ON LKP_PRODUCT_i_ProductAKId.ProductAKId = i_ProductAKId

	LEFT JOIN LKP_INSURANCEREFERENCELINEOFBUSINESS LKP_INSURANCEREFERENCELINEOFBUSINESS_i_InsuranceReferenceLineOfBusinessAKId
	ON LKP_INSURANCEREFERENCELINEOFBUSINESS_i_InsuranceReferenceLineOfBusinessAKId.InsuranceReferenceLineOfBusinessAKId = i_InsuranceReferenceLineOfBusinessAKId

	LEFT JOIN LKP_RATINGPLAN LKP_RATINGPLAN_i_RatingPlanAKId
	ON LKP_RATINGPLAN_i_RatingPlanAKId.RatingPlanAKId = i_RatingPlanAKId

),
LKP_CoverageDetailDim AS (
	SELECT
	CoverageDetailDimId,
	EDWPremiumTransactionPKId
	FROM (
		SELECT CDD.CoverageDetailDimId AS CoverageDetailDimId
			,CDD.EDWPremiumTransactionPKId AS EDWPremiumTransactionPKId
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		where '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' OR 
		exists (
		select 1
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB
		on PT.PremiumTransactionAKId=CLB.PremiumTransactionAKId
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL
		on CLB.CoverageLimitId=CL.CoverageLimitId
		where CLB.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		and CDD.EDWPremiumTransactionPKId=PT.PremiumTransactionID)
		order by CDD.EDWPremiumTransactionPKId--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumTransactionPKId ORDER BY CoverageDetailDimId) = 1
),
LKP_InsuranceReferenceDim AS (
	SELECT
	InsuranceReferenceDimId,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	RatingPlanCode
	FROM (
		SELECT 
			InsuranceReferenceDimId,
			EnterpriseGroupCode,
			InsuranceReferenceLegalEntityCode,
			StrategicProfitCenterCode,
			InsuranceSegmentCode,
			PolicyOfferingCode,
			ProductCode,
			InsuranceReferenceLineOfBusinessCode,
			RatingPlanCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode ORDER BY InsuranceReferenceDimId) = 1
),
EXP_Calculate AS (
	SELECT
	LKP_CoverageDetailDim.CoverageDetailDimId AS lkp_CoverageDetailDimId,
	EXP_CalculateValue.SourceSystemID AS i_SourceSystemID,
	EXP_CalculateValue.RiskType AS i_RiskType,
	EXP_CalculateValue.CoverageType AS i_CoverageType,
	EXP_CalculateValue.PerilGroup AS i_PerilGroup,
	EXP_CalculateValue.SubCoverageTypeCode AS i_SubCoverageTypeCode,
	EXP_CalculateValue.CoverageVersion AS i_CoverageVersion,
	EXP_CalculateValue.o_RiskUnitGroup AS i_RiskUnitGroup,
	EXP_CalculateValue.o_RiskUnit AS i_RiskUnit,
	EXP_CalculateValue.o_MajorPerilCode AS i_MajorPerilCode,
	EXP_CalculateValue.o_StandardInsuranceLineCode AS i_StandardInsuranceLineCode,
	EXP_CalculateValue.o_ProductTypeCode AS i_ProductTypeCode,
	EXP_CalculateValue.LimitTypeDimID,
	EXP_CalculateValue.CoverageGUID,
	EXP_CalculateValue.CoverageLimitType,
	EXP_CalculateValue.PremiumTransactionEffectiveDate,
	EXP_CalculateValue.PremiumTransactionID,
	EXP_CalculateValue.o_contract_cust_dim_id AS contract_cust_dim_id,
	EXP_CalculateValue.o_pol_dim_id AS pol_dim_id,
	EXP_CalculateValue.o_AgencyDimID AS AgencyDimID,
	LKP_InsuranceReferenceDim.InsuranceReferenceDimId,
	EXP_CalculateValue.o_RunDateID AS RunDateID,
	EXP_CalculateValue.CoverageLimitValue,
	-- *INF*: DECODE(i_SourceSystemID,'PMS',:LKP.LKP_InsuranceReferenceCoverageDim_PMS(i_RiskUnitGroup,i_RiskUnit,i_MajorPerilCode,i_StandardInsuranceLineCode,i_ProductTypeCode),'DCT',:LKP.LKP_InsuranceReferenceCoverageDim_DCT(i_RiskType,i_CoverageType,i_StandardInsuranceLineCode,i_PerilGroup,i_SubCoverageTypeCode,i_CoverageVersion))
	DECODE(i_SourceSystemID,
		'PMS', LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_i_StandardInsuranceLineCode_i_ProductTypeCode.InsuranceReferenceCoverageDimId,
		'DCT', LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_i_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.InsuranceReferenceCoverageDimId
	) AS v_InsuranceReferenceCoverageDimId,
	-- *INF*: IIF(ISNULL(lkp_CoverageDetailDimId),-1,lkp_CoverageDetailDimId)
	IFF(lkp_CoverageDetailDimId IS NULL,
		- 1,
		lkp_CoverageDetailDimId
	) AS o_CoverageDetailDimId,
	-- *INF*: IIF(ISNULL(v_InsuranceReferenceCoverageDimId),-1,v_InsuranceReferenceCoverageDimId)
	IFF(v_InsuranceReferenceCoverageDimId IS NULL,
		- 1,
		v_InsuranceReferenceCoverageDimId
	) AS o_InsuranceReferenceCoverageDimId,
	EXP_CalculateValue.pol_ak_id
	FROM EXP_CalculateValue
	LEFT JOIN LKP_CoverageDetailDim
	ON LKP_CoverageDetailDim.EDWPremiumTransactionPKId = EXP_CalculateValue.PremiumTransactionID
	LEFT JOIN LKP_InsuranceReferenceDim
	ON LKP_InsuranceReferenceDim.EnterpriseGroupCode = EXP_CalculateValue.o_EnterpriseGroupCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLegalEntityCode = EXP_CalculateValue.o_InsuranceReferenceLegalEntityCode AND LKP_InsuranceReferenceDim.StrategicProfitCenterCode = EXP_CalculateValue.o_StrategicProfitCenterCode AND LKP_InsuranceReferenceDim.InsuranceSegmentCode = EXP_CalculateValue.o_InsuranceSegmentCode AND LKP_InsuranceReferenceDim.PolicyOfferingCode = EXP_CalculateValue.o_PolicyOfferingCode AND LKP_InsuranceReferenceDim.ProductCode = EXP_CalculateValue.o_ProductCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLineOfBusinessCode = EXP_CalculateValue.o_InsuranceReferenceLineOfBusinessCode AND LKP_InsuranceReferenceDim.RatingPlanCode = EXP_CalculateValue.o_RatingPlanCode
	LEFT JOIN LKP_INSURANCEREFERENCECOVERAGEDIM_PMS LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_i_StandardInsuranceLineCode_i_ProductTypeCode
	ON LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_i_StandardInsuranceLineCode_i_ProductTypeCode.InsuranceLineCode = i_RiskUnitGroup
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_i_StandardInsuranceLineCode_i_ProductTypeCode.PmsRiskUnitGroupCode = i_RiskUnit
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_i_StandardInsuranceLineCode_i_ProductTypeCode.PmsRiskUnitCode = i_MajorPerilCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_i_StandardInsuranceLineCode_i_ProductTypeCode.PmsMajorPerilCode = i_StandardInsuranceLineCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_i_StandardInsuranceLineCode_i_ProductTypeCode.PmsProductTypeCode = i_ProductTypeCode

	LEFT JOIN LKP_INSURANCEREFERENCECOVERAGEDIM_DCT LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_i_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion
	ON LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_i_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.InsuranceLineCode = i_RiskType
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_i_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctRiskTypeCode = i_CoverageType
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_i_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctCoverageTypeCode = i_StandardInsuranceLineCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_i_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctPerilGroup = i_PerilGroup
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_i_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctSubCoverageTypeCode = i_SubCoverageTypeCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_i_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctCoverageVersion = i_CoverageVersion

),
FIL_CoverageDetailDimId AS (
	SELECT
	CoverageGUID, 
	CoverageLimitType, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionID, 
	contract_cust_dim_id, 
	pol_dim_id, 
	AgencyDimID, 
	InsuranceReferenceDimId, 
	RunDateID, 
	CoverageLimitValue, 
	o_CoverageDetailDimId AS CoverageDetailDimId, 
	LimitTypeDimID, 
	o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId, 
	pol_ak_id
	FROM EXP_Calculate
	WHERE CoverageDetailDimId<>-1
),
EXP_DetectChange AS (
	SELECT
	CoverageGUID AS i_CoverageGUID,
	CoverageLimitType AS i_CoverageLimitType,
	PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	PremiumTransactionID AS i_PremiumTransactionID,
	contract_cust_dim_id AS i_contract_cust_dim_id,
	pol_dim_id AS i_pol_dim_id,
	AgencyDimID AS i_AgencyDimID,
	InsuranceReferenceDimId AS i_InsuranceReferenceDimId,
	RunDateID AS i_RunDateID,
	CoverageLimitValue AS i_CoverageLimitValue,
	CoverageDetailDimId AS i_CoverageDetailDimId,
	LimitTypeDimID AS i_LimitTypeDimID,
	InsuranceReferenceCoverageDimId AS i_InsuranceReferenceCoverageDimId,
	pol_ak_id AS i_pol_ak_id,
	-- *INF*: DECODE(TRUE, i_pol_ak_id=v_Prev_Pol_AK_ID and 
	-- i_CoverageGUID=v_Prev_CoverageGUID AND i_CoverageLimitType=v_Prev_CoverageLimitType
	-- ,TO_DECIMAL(i_CoverageLimitValue)-v_Prev_CoverageLimitValue,
	-- TO_DECIMAL(i_CoverageLimitValue)
	-- )
	DECODE(TRUE,
		i_pol_ak_id = v_Prev_Pol_AK_ID 
		AND i_CoverageGUID = v_Prev_CoverageGUID 
		AND i_CoverageLimitType = v_Prev_CoverageLimitType, CAST(i_CoverageLimitValue AS FLOAT) - v_Prev_CoverageLimitValue,
		CAST(i_CoverageLimitValue AS FLOAT)
	) AS v_CoverageLimitValue,
	i_CoverageLimitValue AS v_Prev_CoverageLimitValue,
	i_CoverageGUID AS v_Prev_CoverageGUID,
	i_pol_ak_id AS v_Prev_Pol_AK_ID,
	i_CoverageLimitType AS v_Prev_CoverageLimitType,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_pol_dim_id AS o_pol_dim_id,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	i_InsuranceReferenceDimId AS o_InsuranceReferenceDimId,
	i_InsuranceReferenceCoverageDimId AS o_InsuranceReferenceCoverageDimId,
	i_LimitTypeDimID AS o_LimitTypeDimID,
	i_RunDateID AS o_RunDateID,
	v_CoverageLimitValue AS o_ChangeInLimit,
	i_contract_cust_dim_id AS o_contract_cust_dim_id,
	i_AgencyDimID AS o_AgencyDimID
	FROM FIL_CoverageDetailDimId
),
LKP_LimitFactId AS (
	SELECT
	LimitFactId,
	ChangeInLimit,
	CoverageDetailDimId,
	LimitTypeDimID
	FROM (
		select a.LimitFactId as LimitFactId,
		a.ChangeInLimit as ChangeInLimit,
		a.CoverageDetailDimId as CoverageDetailDimId,
		a.LimitTypeDimId as LimitTypeDimId 
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.LimitFact a  Inner Hash Join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Policy_Dim D 
		on A.PolicyDimId= D.Pol_Dim_ID
		inner Hash Join @{pipeline().parameters.SOURCE_DATABASE_NAME}.V2.Policy E
		On D.EDW_Pol_AK_ID=E.Pol_AK_ID
		and E.crrnt_snpsht_flag=1
		Inner Join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim b
		on a.CoverageDetailDimId=b.CoverageDetailDimId
		join (
		select c.CoverageGUID, e.pol_ak_id from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge a
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction b
		on a.PremiumTransactionAKId=b.PremiumTransactionAKId
		and b.SourceSystemID='DCT'
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage c
		on b.RatingCoverageAKId=c.RatingCoverageAKID
		And c.SourcesystemID='DCT'
		and b.EffectiveDate=c.EffectiveDate
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage d
		on c.PolicyCoverageAkId=d.PolicyCoverageAkId
		and d.SourceSystemID='DCT'
		and d.CurrentSnapshotFlag=1
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.V2.Policy e
		on d.PolicyAkId=e.Pol_AK_ID
		and e.source_sys_id='DCT'
		and e.crrnt_snpsht_flag=1
		where a.SourceSystemID='DCT'
		and ('@{pipeline().parameters.SELECTION_START_TS}'>='01/01/1800' and a.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		)
		union
		select c.CoverageGUID,e.pol_ak_id from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge a
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction b
		on a.PremiumTransactionAKId=b.PremiumTransactionAKId
		and b.SourceSystemID='PMS'
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage c
		on b.StatisticalCoverageAKID=c.StatisticalCoverageAKID
		AND c.SourcesystemID='PMS'
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage d
		on c.PolicyCoverageAkId=d.PolicyCoverageAkId
		and d.SourceSystemID='PMS'
		and d.CurrentSnapshotFlag=1
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.V2.Policy e
		on d.PolicyAkId=e.Pol_AK_ID
		and e.source_sys_id='PMS'
		and e.crrnt_snpsht_flag=1
		where a.SourceSystemID='PMS'
		and ('@{pipeline().parameters.SELECTION_START_TS}'>='01/01/1800' and a.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		)
		union
		select a.CoverageGUID,e.pol_ak_id from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto a 
		Inner Hash Join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction b
		on a.PremiumtransactionId=b.PremiumtransactionId
		and b.SourceSystemID='DCT'
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage c
		on b.RatingCoverageAKId=c.RatingCoverageAKID
		and b.EffectiveDate=c.EffectiveDate
		And c.SourcesystemID='DCT'
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage d
		on c.PolicyCoverageAkId=d.PolicyCoverageAkId
		and d.SourceSystemID='DCT'
		and d.CurrentSnapshotFlag=1
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.V2.Policy e
		on d.PolicyAkId=e.Pol_AK_ID
		and e.source_sys_id='DCT'
		and e.crrnt_snpsht_flag=1
		where a.SourceSystemID='DCT'
		and ('@{pipeline().parameters.SELECTION_START_TS}'>='01/01/1800' and a.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		)
		union
		select a.CoverageGUID,e.pol_ak_id from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto a
		Inner Hash Join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction b
		on a.PremiumtransactionId=b.PremiumtransactionId
		and b.SourceSystemID='PMS'
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage c
		on b.StatisticalCoverageAKID=c.StatisticalCoverageAKID
		And c.SourcesystemID='PMS'
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage d
		on c.PolicyCoverageAkId=d.PolicyCoverageAkId
		and d.SourceSystemID='PMS'
		and d.CurrentSnapshotFlag=1
		inner hash join @{pipeline().parameters.SOURCE_DATABASE_NAME}.V2.Policy e
		on d.PolicyAkId=e.Pol_AK_ID
		and e.source_sys_id='PMS'
		and e.crrnt_snpsht_flag=1
		where a.SourceSystemID='PMS'
		and ('@{pipeline().parameters.SELECTION_START_TS}'>='01/01/1800' and a.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		) c
		on b.CoverageGuid=c.CoverageGUID
		and E.Pol_AK_ID=C.pol_ak_id
		where a.CoverageDetailDimId<>-1
		order by a.CoverageDetailDimId,a.LimitTypeDimId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDetailDimId,LimitTypeDimID ORDER BY LimitFactId) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_LimitFactId.LimitFactId AS LKP_LimitFactId,
	LKP_LimitFactId.ChangeInLimit AS LKP_ChangeInLimit,
	EXP_DetectChange.o_AuditId AS AuditId,
	EXP_DetectChange.o_pol_dim_id AS pol_dim_id,
	EXP_DetectChange.o_CoverageDetailDimId AS CoverageDetailDimId,
	EXP_DetectChange.o_InsuranceReferenceDimId AS InsuranceReferenceDimId,
	EXP_DetectChange.o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId,
	EXP_DetectChange.o_LimitTypeDimID AS LimitTypeDimID,
	EXP_DetectChange.o_RunDateID AS RunDateID,
	EXP_DetectChange.o_ChangeInLimit AS ChangeInLimit,
	EXP_DetectChange.o_contract_cust_dim_id AS contract_cust_dim_id,
	EXP_DetectChange.o_AgencyDimID AS AgencyDimID
	FROM EXP_DetectChange
	LEFT JOIN LKP_LimitFactId
	ON LKP_LimitFactId.CoverageDetailDimId = EXP_DetectChange.o_CoverageDetailDimId AND LKP_LimitFactId.LimitTypeDimID = EXP_DetectChange.o_LimitTypeDimID
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(LKP_LimitFactId) AND ChangeInLimit<>0),
RTR_Insert_Update_Update AS (SELECT * FROM RTR_Insert_Update WHERE NOT ISNULL(LKP_LimitFactId) AND LKP_ChangeInLimit != ChangeInLimit),
UPD_Existing AS (
	SELECT
	LKP_LimitFactId AS LKP_LimitFactId3, 
	AuditId AS AuditId3, 
	pol_dim_id AS pol_dim_id3, 
	CoverageDetailDimId AS CoverageDetailDimId3, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId3, 
	InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId3, 
	LimitTypeDimID AS LimitTypeDimID3, 
	RunDateID AS RunDateID3, 
	ChangeInLimit AS ChangeInLimit3, 
	contract_cust_dim_id AS contract_cust_dim_id3, 
	AgencyDimID AS AgencyDimID3
	FROM RTR_Insert_Update_Update
),
LimitFact_Coverage_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.LimitFact AS T
	USING UPD_Existing AS S
	ON T.LimitFactId = S.LKP_LimitFactId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId3, T.AgencyDimId = S.AgencyDimID3, T.PolicyDimId = S.pol_dim_id3, T.ContractCustomerDimId = S.contract_cust_dim_id3, T.CoverageDetailDimId = S.CoverageDetailDimId3, T.InsuranceReferenceDimId = S.InsuranceReferenceDimId3, T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDimId3, T.LimitTypeDimID = S.LimitTypeDimID3, T.RunDateId = S.RunDateID3, T.ChangeInLimit = S.ChangeInLimit3
),
LimitFact_Coverage_Insert AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.PRE_SQL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.LimitFact
	(AuditId, AgencyDimId, PolicyDimId, ContractCustomerDimId, CoverageDetailDimId, InsuranceReferenceDimId, InsuranceReferenceCoverageDimId, LimitTypeDimID, RunDateId, ChangeInLimit)
	SELECT 
	AUDITID, 
	AgencyDimID AS AGENCYDIMID, 
	pol_dim_id AS POLICYDIMID, 
	contract_cust_dim_id AS CONTRACTCUSTOMERDIMID, 
	COVERAGEDETAILDIMID, 
	INSURANCEREFERENCEDIMID, 
	INSURANCEREFERENCECOVERAGEDIMID, 
	LIMITTYPEDIMID, 
	RunDateID AS RUNDATEID, 
	CHANGEINLIMIT
	FROM RTR_Insert_Update_Insert
),
SQ_PolicyLimit AS (
	WITH cnt
	AS (
		SELECT 1 AS cnt
		
		UNION ALL
		
		SELECT cnt + 1
		FROM cnt
		WHERE cnt < 6
		)
	SELECT ISNULL(SIL.StandardInsuranceLineCode, 'N/A') AS StandardInsuranceLineCode,
		P.pol_sym,
		PL.EffectiveDate,
		PL.PolicyAKId,
		CASE WHEN cnt = 1 THEN 'PolicyPerOccurenceLimit' WHEN cnt = 2 THEN 'PolicyAggregateLimit' WHEN cnt = 3 THEN 'PolicyProductAggregateLimit' WHEN cnt = 4 THEN 'PolicyPerAccidentLimit' WHEN cnt = 5 THEN 'PolicyPerDiseaseLimit' WHEN cnt = 6 THEN 'PolicyPerClaimLimit' END AS LimitType,
		CASE WHEN cnt = 1 THEN PL.PolicyPerOccurenceLimit WHEN cnt = 2 THEN PL.PolicyAggregateLimit WHEN cnt = 3 THEN PL.PolicyProductAggregateLimit WHEN cnt = 4 THEN PL.PolicyPerAccidentLimit WHEN cnt = 5 THEN PL.PolicyPerDiseaseLimit WHEN cnt = 6 THEN PL.PolicyPerClaimLimit END AS LimitValue,
		P.StrategicProfitCenterAKId,
		P.InsuranceSegmentAKId,
		P.PolicyOfferingAKId,
		PL.SourceSystemID,
		PC.TypeBureauCode,
		SC.RiskUnitGroup,
		SC.RiskUnit,
		SC.RiskUnitSequenceNumber,
		SC.MajorPerilCode,
		SC.ClassCode,
		'N/A' AS RiskType,
		'N/A' AS CoverageType,
		'N/A' AS PerilGroup,
		'N/A' AS SubCoverageTypeCode,
		'N/A' AS CoverageVersion,
		SC.ProductAKId,
		SC.InsuranceReferenceLineOfBusinessAKID,
		PL.PolicyLimitId,
		PT.PremiumTransactionBookedDate,
		PC.RatingPlanAKId
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PL  with (nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock) ON PL.PolicyAKId = P.pol_ak_id
		AND P.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock) ON PC.PolicyAKID = P.pol_ak_id
		AND PC.InsuranceLine = PL.InsuranceLine
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC with (nolock) ON PC.PolicyCoverageAKID = SC.PolicyCoverageAKID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock) ON SC.StatisticalCoverageAKId = PT.StatisticalCoverageAKId
	JOIN cnt ON cnt <= 6
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL with (nolock) ON PC.InsuranceLine = SIL.ins_line_code
		AND SIL.crrnt_snpsht_flag = 1
		AND SIL.source_sys_id = 'EXCEED AND PMS'
	WHERE EXISTS (
				SELECT 1
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PL2 with (nolock)
				WHERE PL2.PolicyAKId = PL.PolicyAKID
					AND PL2.InsuranceLine = PL.InsuranceLine
					AND PL2.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
				)
		AND PL.SourceSystemID = 'PMS' 
	@{pipeline().parameters.WHERE_CLAUSE_POL_PMS}
	
	UNION ALL
	
	SELECT ISNULL(SIL.StandardInsuranceLineCode, 'N/A') AS StandardInsuranceLineCode,
		P.pol_sym,
		PL.EffectiveDate,
		PL.PolicyAKId,
		CASE WHEN cnt = 1 THEN 'PolicyPerOccurenceLimit' WHEN cnt = 2 THEN 'PolicyAggregateLimit' WHEN cnt = 3 THEN 'PolicyProductAggregateLimit' WHEN cnt = 4 THEN 'PolicyPerAccidentLimit' WHEN cnt = 5 THEN 'PolicyPerDiseaseLimit' WHEN cnt = 6 THEN 'PolicyPerClaimLimit' END AS LimitType,
		CASE WHEN cnt = 1 THEN PL.PolicyPerOccurenceLimit WHEN cnt = 2 THEN PL.PolicyAggregateLimit WHEN cnt = 3 THEN PL.PolicyProductAggregateLimit WHEN cnt = 4 THEN PL.PolicyPerAccidentLimit WHEN cnt = 5 THEN PL.PolicyPerDiseaseLimit WHEN cnt = 6 THEN PL.PolicyPerClaimLimit END AS LimitValue,
		P.StrategicProfitCenterAKId,
		P.InsuranceSegmentAKId,
		P.PolicyOfferingAKId,
		PL.SourceSystemID,
		PC.TypeBureauCode,
		'N/A' AS RiskUnitGroup,
		'N/A' AS RiskUnit,
		'N/A' AS RiskUnitSequenceNumber,
		'N/A' AS MajorPerilCode,
		'N/A' AS ClassCode,
		RC.RiskType,
		RC.CoverageType,
		RC.PerilGroup AS PerilGroup,
		RC.SubCoverageTypeCode AS SubCoverageTypeCode,
		RC.CoverageVersion AS CoverageVersion,
		RC.ProductAKId,
		RC.InsuranceReferenceLineOfBusinessAKID,
		PL.PolicyLimitId,
		PT.PremiumTransactionBookedDate,
		PC.RatingPlanAKId
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PL with (nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock) ON PL.PolicyAKId = P.pol_ak_id
		AND P.crrnt_snpsht_flag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock) ON PC.PolicyAKID = P.pol_ak_id
		AND PC.InsuranceLine = PL.InsuranceLine
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock) ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
		AND PC.CurrentSnapshotFlag = 1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock) ON RC.RatingCoverageAKId = PT.RatingCoverageAKId
		AND PT.EffectiveDate = RC.EffectiveDate
	JOIN cnt ON cnt <= 6
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL with (nolock) ON PC.InsuranceLine = SIL.ins_line_code
		AND SIL.crrnt_snpsht_flag = 1
		AND SIL.source_sys_id = 'DCT'
	WHERE EXISTS (
				SELECT 1
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PL2 with (nolock)
				WHERE PL2.PolicyAKId = PL.PolicyAKID
					AND PL2.InsuranceLine = PL.InsuranceLine
					AND PL2.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
				)
		AND PL.SourceSystemID = 'DCT'	
	@{pipeline().parameters.WHERE_CLAUSE_POL_DCT}
),
LKP_LimitTypeDim_Policy AS (
	SELECT
	LimitTypeDimID,
	LimitType
	FROM (
		SELECT 
			LimitTypeDimID,
			LimitType
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.LimitTypeDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LimitType ORDER BY LimitTypeDimID) = 1
),
FIL_Invalid_Policy AS (
	SELECT
	LKP_LimitTypeDim_Policy.LimitTypeDimID, 
	SQ_PolicyLimit.StandardInsuranceLineCode, 
	SQ_PolicyLimit.pol_sym, 
	SQ_PolicyLimit.EffectiveDate, 
	SQ_PolicyLimit.PolicyAKId, 
	SQ_PolicyLimit.LimitValue, 
	SQ_PolicyLimit.StrategicProfitCenterAKId, 
	SQ_PolicyLimit.InsuranceSegmentAKId, 
	SQ_PolicyLimit.PolicyOfferingAKId, 
	SQ_PolicyLimit.SourceSystemID, 
	SQ_PolicyLimit.TypeBureauCode, 
	SQ_PolicyLimit.RiskUnitGroup, 
	SQ_PolicyLimit.RiskUnit, 
	SQ_PolicyLimit.RiskUnitSequenceNumber, 
	SQ_PolicyLimit.MajorPerilCode, 
	SQ_PolicyLimit.ClassCode, 
	SQ_PolicyLimit.RiskType, 
	SQ_PolicyLimit.CoverageType, 
	SQ_PolicyLimit.PerilGroup, 
	SQ_PolicyLimit.SubCoverageTypeCode, 
	SQ_PolicyLimit.CoverageVersion, 
	SQ_PolicyLimit.ProductAKId, 
	SQ_PolicyLimit.InsuranceReferenceLineOfBusinessAKId, 
	SQ_PolicyLimit.PolicyLimitId, 
	SQ_PolicyLimit.PremiumTransactionBookedDate, 
	SQ_PolicyLimit.RatingPlanAKId
	FROM SQ_PolicyLimit
	LEFT JOIN LKP_LimitTypeDim_Policy
	ON LKP_LimitTypeDim_Policy.LimitType = SQ_PolicyLimit.LimitType
	WHERE NOT ISNULL(LimitTypeDimID) AND IS_NUMBER(LimitValue)=1
),
EXP_DefaultValue AS (
	SELECT
	StandardInsuranceLineCode AS i_StandardInsuranceLineCode,
	pol_sym AS i_pol_sym,
	EffectiveDate,
	StrategicProfitCenterAKId AS i_StrategicProfitCenterAKId,
	InsuranceSegmentAKId AS i_InsuranceSegmentAKId,
	PolicyOfferingAKId AS i_PolicyOfferingAKId,
	TypeBureauCode AS i_TypeBureauCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	RiskUnit AS i_RiskUnit,
	RiskUnitSequenceNumber AS i_RiskUnitSequenceNumber,
	MajorPerilCode AS i_MajorPerilCode,
	ClassCode AS i_ClassCode,
	ProductAKId AS i_ProductAKId,
	InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	LimitTypeDimID,
	PolicyAKId,
	LimitValue,
	SourceSystemID,
	RiskType,
	CoverageType,
	PerilGroup,
	SubCoverageTypeCode,
	CoverageVersion,
	PolicyLimitId,
	PremiumTransactionBookedDate,
	RatingPlanAKId AS i_RatingPlanAKId,
	-- *INF*: :LKP.LKP_ENTERPRISEGROUP(i_StrategicProfitCenterAKId)
	LKP_ENTERPRISEGROUP_i_StrategicProfitCenterAKId.EnterpriseGroupCode AS v_EnterpriseGroupCode,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCELEGALENTITY(i_StrategicProfitCenterAKId)
	LKP_INSURANCEREFERENCELEGALENTITY_i_StrategicProfitCenterAKId.InsuranceReferenceLegalEntityCode AS v_InsuranceReferenceLegalEntityCode,
	-- *INF*: :LKP.LKP_STRATEGICPROFITCENTER(i_StrategicProfitCenterAKId)
	LKP_STRATEGICPROFITCENTER_i_StrategicProfitCenterAKId.StrategicProfitCenterCode AS v_StrategicProfitCenterCode,
	-- *INF*: :LKP.LKP_INSURANCESEGMENT(i_InsuranceSegmentAKId)
	LKP_INSURANCESEGMENT_i_InsuranceSegmentAKId.InsuranceSegmentCode AS v_InsuranceSegmentCode,
	-- *INF*: :LKP.LKP_POLICYOFFERING(i_PolicyOfferingAKId)
	LKP_POLICYOFFERING_i_PolicyOfferingAKId.PolicyOfferingCode AS v_PolicyOfferingCode,
	-- *INF*: :LKP.LKP_PRODUCT(i_ProductAKId)
	LKP_PRODUCT_i_ProductAKId.ProductCode AS v_ProductCode,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCELINEOFBUSINESS(i_InsuranceReferenceLineOfBusinessAKId)
	LKP_INSURANCEREFERENCELINEOFBUSINESS_i_InsuranceReferenceLineOfBusinessAKId.InsuranceReferenceLineOfBusinessCode AS v_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: SUBSTR(i_pol_sym,1,2)
	SUBSTR(i_pol_sym, 1, 2
	) AS v_pol_sym_1_2,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE('21001231','YYYYMMDD'))
	LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD.clndr_id AS v_clndr_id,
	-- *INF*: IIF(NOT ISNULL(v_clndr_id),v_clndr_id,-1)
	IFF(v_clndr_id IS NOT NULL,
		v_clndr_id,
		- 1
	) AS v_default_clndr_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode
	) AS v_MajorPerilCode,
	-- *INF*: IIF(LTRIM(v_MajorPerilCode,'0')='' OR REG_MATCH(v_MajorPerilCode,'[^0-9a-zA-Z]'),'N/A',v_MajorPerilCode)
	IFF(LTRIM(v_MajorPerilCode, '0'
		) = '' 
		OR REGEXP_LIKE(v_MajorPerilCode, '[^0-9a-zA-Z]'
		),
		'N/A',
		v_MajorPerilCode
	) AS v_Reg_MajorPerilCode,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TRUNC(PremiumTransactionBookedDate,'D'))
	LKP_CALENDER_DIM_TRUNC_PremiumTransactionBookedDate_D.clndr_id AS v_RunDateID,
	-- *INF*: IIF(REG_MATCH(i_StandardInsuranceLineCode,'[^0-9a-zA-Z]'),'N/A',i_StandardInsuranceLineCode)
	IFF(REGEXP_LIKE(i_StandardInsuranceLineCode, '[^0-9a-zA-Z]'
		),
		'N/A',
		i_StandardInsuranceLineCode
	) AS v_Reg_StandardInsuranceLineCode,
	-- *INF*: IIF(v_Reg_StandardInsuranceLineCode='N/A' AND (IN(i_TypeBureauCode,'AL','AN','AP') OR IN(v_Reg_MajorPerilCode,'930','931')),'CA',v_Reg_StandardInsuranceLineCode)
	IFF(v_Reg_StandardInsuranceLineCode = 'N/A' 
		AND ( i_TypeBureauCode IN ('AL','AN','AP') 
			OR v_Reg_MajorPerilCode IN ('930','931') 
		),
		'CA',
		v_Reg_StandardInsuranceLineCode
	) AS v_StandardInsuranceLineCode,
	-- *INF*: IIF(v_StandardInsuranceLineCode='N/A' AND IN(i_TypeBureauCode,'CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),1,0)
	IFF(v_StandardInsuranceLineCode = 'N/A' 
		AND i_TypeBureauCode IN ('CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),
		1,
		0
	) AS v_flag,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode
	) AS v_ClassCode,
	-- *INF*: IIF(IN(v_StandardInsuranceLineCode,'CR') OR v_flag=1,'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup))
	IFF(v_StandardInsuranceLineCode IN ('CR') 
		OR v_flag = 1,
		'N/A',
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnitGroup
		)
	) AS v_Risk_Unit_Group,
	-- *INF*: IIF(LTRIM(v_Risk_Unit_Group,'0')='','N/A',v_Risk_Unit_Group)
	IFF(LTRIM(v_Risk_Unit_Group, '0'
		) = '',
		'N/A',
		v_Risk_Unit_Group
	) AS v_Zero_Risk_Unit_Group,
	-- *INF*: IIF(   v_flag=1 OR   (v_StandardInsuranceLineCode='GL' AND (v_MajorPerilCode<>'540'    OR NOT IN(v_ClassCode,'11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058')))   OR IN(v_StandardInsuranceLineCode,'WC','IM','CG','CA')=1,  'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit) )
	IFF(v_flag = 1 
		OR ( v_StandardInsuranceLineCode = 'GL' 
			AND ( v_MajorPerilCode <> '540' 
				OR NOT v_ClassCode IN ('11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058') 
			) 
		) 
		OR v_StandardInsuranceLineCode IN ('WC','IM','CG','CA') = 1,
		'N/A',
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RiskUnit
		)
	) AS v_RiskUnit,
	-- *INF*: IIF(LTRIM(v_RiskUnit,'0')='','N/A',v_RiskUnit)
	IFF(LTRIM(v_RiskUnit, '0'
		) = '',
		'N/A',
		v_RiskUnit
	) AS v_Zero_RiskUnit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber,2,1))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(SUBSTR(i_RiskUnitSequenceNumber, 2, 1
		)
	) AS v_ProductTypeCode,
	-- *INF*: :LKP.LKP_RATINGPLAN(i_RatingPlanAKId)
	LKP_RATINGPLAN_i_RatingPlanAKId.RatingPlanCode AS v_RatingPlanCode,
	-- *INF*: IIF(NOT ISNULL(v_EnterpriseGroupCode), v_EnterpriseGroupCode, '1')
	IFF(v_EnterpriseGroupCode IS NOT NULL,
		v_EnterpriseGroupCode,
		'1'
	) AS o_EnterpriseGroupCode,
	-- *INF*: IIF(NOT ISNULL(v_InsuranceReferenceLegalEntityCode), v_InsuranceReferenceLegalEntityCode, '1')
	IFF(v_InsuranceReferenceLegalEntityCode IS NOT NULL,
		v_InsuranceReferenceLegalEntityCode,
		'1'
	) AS o_InsuranceReferenceLegalEntityCode,
	-- *INF*: IIF(NOT ISNULL(v_StrategicProfitCenterCode), v_StrategicProfitCenterCode, '6')
	IFF(v_StrategicProfitCenterCode IS NOT NULL,
		v_StrategicProfitCenterCode,
		'6'
	) AS o_StrategicProfitCenterCode,
	-- *INF*: IIF(NOT ISNULL(v_InsuranceSegmentCode), v_InsuranceSegmentCode, 'N/A')
	IFF(v_InsuranceSegmentCode IS NOT NULL,
		v_InsuranceSegmentCode,
		'N/A'
	) AS o_InsuranceSegmentCode,
	-- *INF*: IIF(NOT ISNULL(v_PolicyOfferingCode), v_PolicyOfferingCode, '000')
	IFF(v_PolicyOfferingCode IS NOT NULL,
		v_PolicyOfferingCode,
		'000'
	) AS o_PolicyOfferingCode,
	-- *INF*: IIF(NOT ISNULL(v_ProductCode), v_ProductCode, '000')
	IFF(v_ProductCode IS NOT NULL,
		v_ProductCode,
		'000'
	) AS o_ProductCode,
	-- *INF*: IIF(NOT ISNULL(v_InsuranceReferenceLineOfBusinessCode), v_InsuranceReferenceLineOfBusinessCode, '000')
	IFF(v_InsuranceReferenceLineOfBusinessCode IS NOT NULL,
		v_InsuranceReferenceLineOfBusinessCode,
		'000'
	) AS o_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: IIF(ISNULL(v_RunDateID),v_default_clndr_id,v_RunDateID)
	IFF(v_RunDateID IS NULL,
		v_default_clndr_id,
		v_RunDateID
	) AS o_RunDateID,
	-- *INF*: IIF(REG_MATCH(v_Zero_Risk_Unit_Group,'[^0-9a-zA-Z]'),'N/A',v_Zero_Risk_Unit_Group)
	IFF(REGEXP_LIKE(v_Zero_Risk_Unit_Group, '[^0-9a-zA-Z]'
		),
		'N/A',
		v_Zero_Risk_Unit_Group
	) AS o_RiskUnitGroup,
	-- *INF*: IIF(REG_MATCH(v_Zero_RiskUnit,'[^0-9a-zA-Z]'),'N/A',v_Zero_RiskUnit)
	IFF(REGEXP_LIKE(v_Zero_RiskUnit, '[^0-9a-zA-Z]'
		),
		'N/A',
		v_Zero_RiskUnit
	) AS o_RiskUnit,
	v_Reg_MajorPerilCode AS o_MajorPerilCode,
	-- *INF*: IIF(SourceSystemID='PMS',v_StandardInsuranceLineCode,v_Reg_StandardInsuranceLineCode)
	IFF(SourceSystemID = 'PMS',
		v_StandardInsuranceLineCode,
		v_Reg_StandardInsuranceLineCode
	) AS o_StandardInsuranceLineCode,
	-- *INF*: IIF(   REG_MATCH(v_ProductTypeCode,'[^0-9a-zA-Z]') OR v_Reg_StandardInsuranceLineCode<>'GL' OR v_ProductTypeCode='0',   'N/A',v_ProductTypeCode )
	IFF(REGEXP_LIKE(v_ProductTypeCode, '[^0-9a-zA-Z]'
		) 
		OR v_Reg_StandardInsuranceLineCode <> 'GL' 
		OR v_ProductTypeCode = '0',
		'N/A',
		v_ProductTypeCode
	) AS o_ProductTypeCode,
	v_pol_sym_1_2 AS o_PolicySymbol,
	-- *INF*: IIF(ISNULL(v_RatingPlanCode), '1', v_RatingPlanCode)
	IFF(v_RatingPlanCode IS NULL,
		'1',
		v_RatingPlanCode
	) AS o_RatingPlanCode
	FROM FIL_Invalid_Policy
	LEFT JOIN LKP_ENTERPRISEGROUP LKP_ENTERPRISEGROUP_i_StrategicProfitCenterAKId
	ON LKP_ENTERPRISEGROUP_i_StrategicProfitCenterAKId.StrategicProfitCenterAKId = i_StrategicProfitCenterAKId

	LEFT JOIN LKP_INSURANCEREFERENCELEGALENTITY LKP_INSURANCEREFERENCELEGALENTITY_i_StrategicProfitCenterAKId
	ON LKP_INSURANCEREFERENCELEGALENTITY_i_StrategicProfitCenterAKId.StrategicProfitCenterAKId = i_StrategicProfitCenterAKId

	LEFT JOIN LKP_STRATEGICPROFITCENTER LKP_STRATEGICPROFITCENTER_i_StrategicProfitCenterAKId
	ON LKP_STRATEGICPROFITCENTER_i_StrategicProfitCenterAKId.StrategicProfitCenterAKId = i_StrategicProfitCenterAKId

	LEFT JOIN LKP_INSURANCESEGMENT LKP_INSURANCESEGMENT_i_InsuranceSegmentAKId
	ON LKP_INSURANCESEGMENT_i_InsuranceSegmentAKId.InsuranceSegmentAKId = i_InsuranceSegmentAKId

	LEFT JOIN LKP_POLICYOFFERING LKP_POLICYOFFERING_i_PolicyOfferingAKId
	ON LKP_POLICYOFFERING_i_PolicyOfferingAKId.PolicyOfferingAKId = i_PolicyOfferingAKId

	LEFT JOIN LKP_PRODUCT LKP_PRODUCT_i_ProductAKId
	ON LKP_PRODUCT_i_ProductAKId.ProductAKId = i_ProductAKId

	LEFT JOIN LKP_INSURANCEREFERENCELINEOFBUSINESS LKP_INSURANCEREFERENCELINEOFBUSINESS_i_InsuranceReferenceLineOfBusinessAKId
	ON LKP_INSURANCEREFERENCELINEOFBUSINESS_i_InsuranceReferenceLineOfBusinessAKId.InsuranceReferenceLineOfBusinessAKId = i_InsuranceReferenceLineOfBusinessAKId

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD
	ON LKP_CALENDER_DIM_TO_DATE_21001231_YYYYMMDD.clndr_date = TO_DATE('21001231', 'YYYYMMDD'
)

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TRUNC_PremiumTransactionBookedDate_D
	ON LKP_CALENDER_DIM_TRUNC_PremiumTransactionBookedDate_D.clndr_date = CAST(TRUNC(PremiumTransactionBookedDate, 'DAY') AS TIMESTAMP_NTZ(0))

	LEFT JOIN LKP_RATINGPLAN LKP_RATINGPLAN_i_RatingPlanAKId
	ON LKP_RATINGPLAN_i_RatingPlanAKId.RatingPlanAKId = i_RatingPlanAKId

),
EXP_DetectMainCoverage AS (
	SELECT
	EffectiveDate,
	LimitTypeDimID,
	PolicyAKId,
	LimitValue,
	SourceSystemID,
	RiskType,
	CoverageType,
	PerilGroup,
	SubCoverageTypeCode,
	CoverageVersion,
	PolicyLimitId,
	PremiumTransactionBookedDate,
	o_EnterpriseGroupCode AS EnterpriseGroupCode,
	o_InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode,
	o_StrategicProfitCenterCode AS StrategicProfitCenterCode,
	o_InsuranceSegmentCode AS InsuranceSegmentCode,
	o_PolicyOfferingCode AS PolicyOfferingCode,
	o_ProductCode AS ProductCode,
	o_InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode,
	o_RunDateID AS RunDateID,
	o_RiskUnitGroup AS RiskUnitGroup,
	o_RiskUnit AS RiskUnit,
	o_MajorPerilCode AS MajorPerilCode,
	o_StandardInsuranceLineCode AS StandardInsuranceLineCode,
	o_ProductTypeCode AS ProductTypeCode,
	o_RatingPlanCode AS RatingPlanCode,
	o_PolicySymbol AS PolicySymbol,
	-- *INF*: DECODE(SourceSystemID='PMS',
	-- IN(PolicySymbol,'CP', 'NS') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='500' AND ProductCode='300' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='340' AND MajorPerilCode='530' AND ProductTypeCode='O' AND RiskUnit='N/A', 
	-- 100,
	-- IN(PolicySymbol,'CP', 'NS') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='500' AND ProductCode='300' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='340' AND MajorPerilCode='540' AND ProductTypeCode= 'O' AND RiskUnit='N/A', 
	-- 99,
	-- IN(PolicySymbol,'CP', 'NS') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='500' AND ProductCode='300' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='340' AND MajorPerilCode='530' AND ProductTypeCode= 'P' AND RiskUnit='N/A', 
	-- 98,
	-- IN(PolicySymbol,'CP', 'NS') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='370' AND ProductCode='370' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='355' AND MajorPerilCode='530' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 97,
	-- IN(PolicySymbol,'CP', 'NS') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='365' AND ProductCode='365' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='345' AND MajorPerilCode='530' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 96,
	-- PolicySymbol='CP' AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='321' AND ProductCode='321' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='346' AND MajorPerilCode='530' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 95,
	-- PolicySymbol='CP' AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='380' AND ProductCode='380' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='365' AND MajorPerilCode='550' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 94,
	-- PolicySymbol='CD' AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='310' AND ProductCode='310' AND InsuranceReferenceLineOfBusinessCode='310' AND RiskUnitGroup='367' AND MajorPerilCode='540' AND ProductTypeCode='N/A' AND in(RiskUnit, '80054', '80055', '80056', '80058'), 
	-- 93,
	-- PolicySymbol='CD' AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='310' AND ProductCode='310' AND InsuranceReferenceLineOfBusinessCode='310' AND RiskUnitGroup='367' AND MajorPerilCode='540' AND ProductTypeCode='N/A' AND in(RiskUnit, '80051', '80052', '80053', '80057'), 
	-- 92,
	-- IN(PolicySymbol,'CU', 'NU') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='900' AND ProductCode='900' AND InsuranceReferenceLineOfBusinessCode='900' AND RiskUnitGroup='370' AND MajorPerilCode='517' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 91,
	-- PolicySymbol='NN' AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='312' AND ProductCode='312' AND InsuranceReferenceLineOfBusinessCode='312' AND RiskUnitGroup='286' AND MajorPerilCode='540' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 90,
	-- IN(PolicySymbol,'NE', 'ER') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='330' AND ProductCode='330' AND InsuranceReferenceLineOfBusinessCode='330' AND RiskUnitGroup='366' AND MajorPerilCode='540' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 89,
	-- PolicyOfferingCode='100' AND StandardInsuranceLineCode='WC' AND 
	-- (IN(PolicySymbol,'VA', 'VB', 'VC', 'VD', 'VE', 'VF', 'VG', 'VH', 'VI', 'VJ', 'VK', 'VL', 'VM', 'VN', 'VO', 'VP', 'VQ', 'VR', 'VS', 'VT', 'VU', 'VV', 'VW', 'WA', 'WB', 'WC', 'WD', 'WG', 'WH', 'WI', 'WK', 'WJ', 'WN', 'WO', 'WP', 'WR', 'WS', 'WT', 'WU', 'WV', 'WW', 'WY', 'YA', 'YB', 'YC', 'YD', 'YE', 'YF', 'YG', 'YH', 'YI', 'YJ', 'YK', 'YL', 'YM', 'YN', 'YO', 'YP', 'YQ', 'YR', 'YS', 'YT', 'YU', 'YV', 'YW', 'YX', 'ZZ')
	-- OR IN(PolicySymbol, 'A0', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'LA', 'LB', 'LC', 'LE', 'LF', 'LG', 'LH', 'LI', 'LJ', 'LK', 'LL', 'LM', 'LN', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK', 'AL', 'AM', 'AN', 'AO', 'AP', 'AQ', 'AR', 'AT', 'AU', 'AV', 'AW', 'AX', 'AY', 'AZ', 'JA', 'JB', 'JC', 'JD', 'JE', 'JF', 'JG', 'JH', 'JI', 'JJ', 'JK', 'JL', 'JM', 'JN', 'JO', 'JP', 'JQ', 'JR', 'JS', 'JT', 'JU', 'JV', 'JW', 'JX', 'JY', 'JZ', 'J1', 'J2', 'J3', 'J4', 'J5', 'J6')
	-- OR IN(PolicySymbol, 'RA', 'RB', 'RC', 'RD', 'RE', 'RF', 'RG', 'RH', 'RI', 'RJ', 'RK', 'RL', 'RM', 'RN', 'RO', 'RP', 'RQ', 'RR', 'RS', 'RT', 'RU', 'RV', 'RW', 'RX', 'RW', 'RX', 'SA', 'SB', 'SC', 'SD', 'SE', 'SF', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SO', 'SP', 'SR', 'SS', 'ST', 'SU', 'SV', 'SW', 'SX', 'SY', 'TH', 'TI', 'TJ', 'TK', 'TL', 'TM', 'TN', 'TO', 'TP', 'TQ', 'TR', 'TS', 'TT', 'TV', 'TW')) AND ProductCode='100' AND InsuranceReferenceLineOfBusinessCode='100' AND RiskUnitGroup='010' AND MajorPerilCode='032' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 88,
	-- IN(PolicySymbol, 'NA', 'NB') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='430' AND ProductCode='430' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='340' AND MajorPerilCode='530' AND ProductTypeCode='O' AND RiskUnit='N/A', 
	-- 87,
	-- IN(PolicySymbol, 'BC', 'BD') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='410' AND ProductCode='410' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='340' AND MajorPerilCode='530' AND ProductTypeCode='O' AND RiskUnit='N/A', 
	-- 86,
	-- PolicySymbol='BO' AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='450' AND ProductCode='450' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='340' AND MajorPerilCode='530' AND ProductTypeCode='O' AND RiskUnit='N/A', 
	-- 85,
	-- PolicySymbol='CM' AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='360' AND ProductCode='360' AND InsuranceReferenceLineOfBusinessCode='360' AND RiskUnitGroup='901' AND MajorPerilCode='540' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 84,
	-- PolicySymbol='NK' AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='311' AND ProductCode='311' AND InsuranceReferenceLineOfBusinessCode='330' AND RiskUnitGroup='287' AND MajorPerilCode='540' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 83,
	-- PolicySymbol='BG' AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='420' AND ProductCode='420' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='340' AND MajorPerilCode='530' AND ProductTypeCode='O' AND RiskUnit='N/A', 
	-- 82,
	-- IN(PolicySymbol,'WE', 'WF', 'WL', 'WZ') AND StandardInsuranceLineCode='WC' AND PolicyOfferingCode='100' AND ProductCode='100' AND InsuranceReferenceLineOfBusinessCode='100' AND RiskUnitGroup='N/A' AND MajorPerilCode='032' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 81,
	-- IN(PolicySymbol,'CP', 'NS') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='320' AND ProductCode='320' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='340' AND MajorPerilCode='530' AND ProductTypeCode='P' AND RiskUnit='N/A', 
	-- 80,
	-- IN(PolicySymbol,'CP', 'NS') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='500' AND ProductCode='320' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='340' AND MajorPerilCode='530' AND ProductTypeCode='P' AND RiskUnit='N/A', 
	-- 79,
	-- IN(PolicySymbol,'NN') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='312' AND ProductCode='312' AND InsuranceReferenceLineOfBusinessCode='312' AND RiskUnitGroup='287' AND MajorPerilCode='540' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 78,
	-- IN(PolicySymbol,'CP', 'NS') AND StandardInsuranceLineCode='GL' AND PolicyOfferingCode='500' AND ProductCode='370' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskUnitGroup='355' AND MajorPerilCode='530' AND ProductTypeCode='N/A' AND RiskUnit='N/A', 
	-- 77,
	-- 0)
	DECODE(SourceSystemID = 'PMS',
		PolicySymbol IN ('CP','NS') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '300' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '340' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'O' 
		AND RiskUnit = 'N/A', 100,
		PolicySymbol IN ('CP','NS') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '300' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '340' 
		AND MajorPerilCode = '540' 
		AND ProductTypeCode = 'O' 
		AND RiskUnit = 'N/A', 99,
		PolicySymbol IN ('CP','NS') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '300' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '340' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'P' 
		AND RiskUnit = 'N/A', 98,
		PolicySymbol IN ('CP','NS') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '370' 
		AND ProductCode = '370' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '355' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 97,
		PolicySymbol IN ('CP','NS') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '365' 
		AND ProductCode = '365' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '345' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 96,
		PolicySymbol = 'CP' 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '321' 
		AND ProductCode = '321' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '346' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 95,
		PolicySymbol = 'CP' 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '380' 
		AND ProductCode = '380' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '365' 
		AND MajorPerilCode = '550' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 94,
		PolicySymbol = 'CD' 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '310' 
		AND ProductCode = '310' 
		AND InsuranceReferenceLineOfBusinessCode = '310' 
		AND RiskUnitGroup = '367' 
		AND MajorPerilCode = '540' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit IN ('80054','80055','80056','80058'), 93,
		PolicySymbol = 'CD' 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '310' 
		AND ProductCode = '310' 
		AND InsuranceReferenceLineOfBusinessCode = '310' 
		AND RiskUnitGroup = '367' 
		AND MajorPerilCode = '540' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit IN ('80051','80052','80053','80057'), 92,
		PolicySymbol IN ('CU','NU') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '900' 
		AND ProductCode = '900' 
		AND InsuranceReferenceLineOfBusinessCode = '900' 
		AND RiskUnitGroup = '370' 
		AND MajorPerilCode = '517' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 91,
		PolicySymbol = 'NN' 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '312' 
		AND ProductCode = '312' 
		AND InsuranceReferenceLineOfBusinessCode = '312' 
		AND RiskUnitGroup = '286' 
		AND MajorPerilCode = '540' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 90,
		PolicySymbol IN ('NE','ER') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '330' 
		AND ProductCode = '330' 
		AND InsuranceReferenceLineOfBusinessCode = '330' 
		AND RiskUnitGroup = '366' 
		AND MajorPerilCode = '540' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 89,
		PolicyOfferingCode = '100' 
		AND StandardInsuranceLineCode = 'WC' 
		AND ( PolicySymbol IN ('VA','VB','VC','VD','VE','VF','VG','VH','VI','VJ','VK','VL','VM','VN','VO','VP','VQ','VR','VS','VT','VU','VV','VW','WA','WB','WC','WD','WG','WH','WI','WK','WJ','WN','WO','WP','WR','WS','WT','WU','WV','WW','WY','YA','YB','YC','YD','YE','YF','YG','YH','YI','YJ','YK','YL','YM','YN','YO','YP','YQ','YR','YS','YT','YU','YV','YW','YX','ZZ') 
			OR PolicySymbol IN ('A0','A1','A2','A3','A4','A5','A6','A7','A8','LA','LB','LC','LE','LF','LG','LH','LI','LJ','LK','LL','LM','LN','AB','AC','AD','AE','AF','AG','AH','AI','AJ','AK','AL','AM','AN','AO','AP','AQ','AR','AT','AU','AV','AW','AX','AY','AZ','JA','JB','JC','JD','JE','JF','JG','JH','JI','JJ','JK','JL','JM','JN','JO','JP','JQ','JR','JS','JT','JU','JV','JW','JX','JY','JZ','J1','J2','J3','J4','J5','J6') 
			OR PolicySymbol IN ('RA','RB','RC','RD','RE','RF','RG','RH','RI','RJ','RK','RL','RM','RN','RO','RP','RQ','RR','RS','RT','RU','RV','RW','RX','RW','RX','SA','SB','SC','SD','SE','SF','SG','SH','SI','SJ','SK','SL','SM','SN','SO','SP','SR','SS','ST','SU','SV','SW','SX','SY','TH','TI','TJ','TK','TL','TM','TN','TO','TP','TQ','TR','TS','TT','TV','TW') 
		) 
		AND ProductCode = '100' 
		AND InsuranceReferenceLineOfBusinessCode = '100' 
		AND RiskUnitGroup = '010' 
		AND MajorPerilCode = '032' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 88,
		PolicySymbol IN ('NA','NB') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '430' 
		AND ProductCode = '430' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '340' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'O' 
		AND RiskUnit = 'N/A', 87,
		PolicySymbol IN ('BC','BD') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '410' 
		AND ProductCode = '410' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '340' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'O' 
		AND RiskUnit = 'N/A', 86,
		PolicySymbol = 'BO' 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '450' 
		AND ProductCode = '450' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '340' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'O' 
		AND RiskUnit = 'N/A', 85,
		PolicySymbol = 'CM' 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '360' 
		AND ProductCode = '360' 
		AND InsuranceReferenceLineOfBusinessCode = '360' 
		AND RiskUnitGroup = '901' 
		AND MajorPerilCode = '540' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 84,
		PolicySymbol = 'NK' 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '311' 
		AND ProductCode = '311' 
		AND InsuranceReferenceLineOfBusinessCode = '330' 
		AND RiskUnitGroup = '287' 
		AND MajorPerilCode = '540' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 83,
		PolicySymbol = 'BG' 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '420' 
		AND ProductCode = '420' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '340' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'O' 
		AND RiskUnit = 'N/A', 82,
		PolicySymbol IN ('WE','WF','WL','WZ') 
		AND StandardInsuranceLineCode = 'WC' 
		AND PolicyOfferingCode = '100' 
		AND ProductCode = '100' 
		AND InsuranceReferenceLineOfBusinessCode = '100' 
		AND RiskUnitGroup = 'N/A' 
		AND MajorPerilCode = '032' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 81,
		PolicySymbol IN ('CP','NS') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '320' 
		AND ProductCode = '320' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '340' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'P' 
		AND RiskUnit = 'N/A', 80,
		PolicySymbol IN ('CP','NS') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '320' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '340' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'P' 
		AND RiskUnit = 'N/A', 79,
		PolicySymbol IN ('NN') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '312' 
		AND ProductCode = '312' 
		AND InsuranceReferenceLineOfBusinessCode = '312' 
		AND RiskUnitGroup = '287' 
		AND MajorPerilCode = '540' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 78,
		PolicySymbol IN ('CP','NS') 
		AND StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '370' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskUnitGroup = '355' 
		AND MajorPerilCode = '530' 
		AND ProductTypeCode = 'N/A' 
		AND RiskUnit = 'N/A', 77,
		0
	) AS v_DetectFlag_PMS,
	-- *INF*: DECODE(SourceSystemID='DCT',
	-- StandardInsuranceLineCode='GL' AND PolicyOfferingCode='500' AND ProductCode='300' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='PremOps' AND CoverageType='PremisesOperations' AND CoverageVersion='OCCURRENCE' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 100,
	-- StandardInsuranceLineCode='GL' AND PolicyOfferingCode='500' AND ProductCode='300' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='PremOpsProducts' AND CoverageType='PremisesOperations' AND CoverageVersion='CLAIMSMADE' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 99,
	-- StandardInsuranceLineCode='GL' AND PolicyOfferingCode='500' AND ProductCode='300' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='PremOpsProducts' AND CoverageType='PremisesOperations' AND CoverageVersion='OCCURRENCE' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 98,
	-- StandardInsuranceLineCode='GL' AND PolicyOfferingCode='500' AND ProductCode='300' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='ProductsCompletedOps' AND CoverageType='ProductsCompletedOps' AND CoverageVersion='OCCURRENCE' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 97,
	-- StandardInsuranceLineCode='GL' AND PolicyOfferingCode='370' AND ProductCode='370' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='N/A' AND CoverageType='LiquorLiability' AND CoverageVersion='OCCURRENCE' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 96,
	-- StandardInsuranceLineCode='GL' AND PolicyOfferingCode='365' AND ProductCode='365' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='N/A' AND CoverageType='OwnersContractorsOrPrincipals' AND CoverageVersion='OCCURRENCE' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 95,
	-- StandardInsuranceLineCode='GL' AND PolicyOfferingCode='321' AND ProductCode='321' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='N/A' AND CoverageType='RailroadProtectiveLiability' AND CoverageVersion='OCCURRENCE' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 94,
	-- StandardInsuranceLineCode='GL' AND PolicyOfferingCode='380' AND ProductCode='380' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='N/A' AND CoverageType='ProductWithdrawal' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 93,
	-- StandardInsuranceLineCode='CDO' AND PolicyOfferingCode='310' AND ProductCode='310' AND InsuranceReferenceLineOfBusinessCode='310' AND RiskType='N/A' AND CoverageType='DirectorsAndOfficersCondosCommercial' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A',
	-- 92,
	-- StandardInsuranceLineCode='CDO' AND PolicyOfferingCode='310' AND ProductCode='310' AND InsuranceReferenceLineOfBusinessCode='310' AND RiskType='N/A' AND CoverageType='DirectorsAndOfficersCondosResidential' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A',
	-- 91,
	-- StandardInsuranceLineCode='CU' AND PolicyOfferingCode='900' AND ProductCode='900' AND InsuranceReferenceLineOfBusinessCode='900' AND RiskType='N/A' AND CoverageType='BuiltUp' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 90,
	-- StandardInsuranceLineCode='CU' AND PolicyOfferingCode='500' AND ProductCode='900' AND InsuranceReferenceLineOfBusinessCode='900' AND RiskType='N/A' AND CoverageType='BuiltUp' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 89,
	-- StandardInsuranceLineCode='CU' AND PolicyOfferingCode='450' AND ProductCode='900' AND InsuranceReferenceLineOfBusinessCode='900' AND RiskType='N/A' AND CoverageType='BuiltUp' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 88,
	-- StandardInsuranceLineCode='CU' AND PolicyOfferingCode='430' AND ProductCode='900' AND InsuranceReferenceLineOfBusinessCode='900' AND RiskType='N/A' AND CoverageType='BuiltUp' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 87,
	-- StandardInsuranceLineCode='NDO' AND PolicyOfferingCode='312' AND ProductCode='312' AND InsuranceReferenceLineOfBusinessCode='312' AND RiskType='N/A' AND CoverageType='DirectorsAndOfficersNFP' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 86,
	-- StandardInsuranceLineCode='EPL' AND PolicyOfferingCode='330' AND ProductCode='330' AND InsuranceReferenceLineOfBusinessCode='330' AND RiskType='N/A' AND CoverageType='EmploymentPracticesLiability' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 85,
	-- StandardInsuranceLineCode='BP' AND PolicyOfferingCode='450' AND ProductCode='450' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='N/A' AND CoverageType='RiskLiability' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 84,
	-- StandardInsuranceLineCode='SBOPGL' AND PolicyOfferingCode='430' AND ProductCode='430' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='PremOps' AND CoverageType='PremisesOperations' AND CoverageVersion='OCCURRENCE' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 83,
	-- StandardInsuranceLineCode='WC' AND PolicyOfferingCode='100' AND ProductCode='100' AND InsuranceReferenceLineOfBusinessCode='100' AND RiskType='N/A' AND CoverageType='EmployersLiability' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 82,
	-- --2 New combinations Added by Luna, for EDWP-4808, 56 issue Policies
	-- StandardInsuranceLineCode='SBOPGL' AND PolicyOfferingCode='430' AND ProductCode='430' AND InsuranceReferenceLineOfBusinessCode='300' AND RiskType='PremOpsProducts' AND CoverageType='PremisesOperations' AND CoverageVersion='OCCURRENCE' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 81,
	-- StandardInsuranceLineCode='CU' AND PolicyOfferingCode='500' AND ProductCode='900' AND InsuranceReferenceLineOfBusinessCode='900' AND RiskType='N/A' AND CoverageType='Revised' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 80,
	-- -- Fixed AP-301
	-- StandardInsuranceLineCode='WC' AND PolicyOfferingCode='100' AND ProductCode='100' AND InsuranceReferenceLineOfBusinessCode='100' AND RiskType='N/A' AND CoverageType='EmpIoyersLiabilityIncreasedLimits' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 79,
	-- StandardInsuranceLineCode='WC' AND PolicyOfferingCode='100' AND ProductCode='100' AND InsuranceReferenceLineOfBusinessCode='100' AND RiskType='N/A' AND CoverageType='EmployersLiabilityIncreasedLimitsBalanceToMinimum' AND CoverageVersion='N/A' AND PerilGroup='N/A' AND SubCoverageTypeCode='N/A', 
	-- 78,
	-- 0)
	DECODE(SourceSystemID = 'DCT',
		StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '300' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'PremOps' 
		AND CoverageType = 'PremisesOperations' 
		AND CoverageVersion = 'OCCURRENCE' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 100,
		StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '300' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'PremOpsProducts' 
		AND CoverageType = 'PremisesOperations' 
		AND CoverageVersion = 'CLAIMSMADE' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 99,
		StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '300' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'PremOpsProducts' 
		AND CoverageType = 'PremisesOperations' 
		AND CoverageVersion = 'OCCURRENCE' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 98,
		StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '300' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'ProductsCompletedOps' 
		AND CoverageType = 'ProductsCompletedOps' 
		AND CoverageVersion = 'OCCURRENCE' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 97,
		StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '370' 
		AND ProductCode = '370' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'LiquorLiability' 
		AND CoverageVersion = 'OCCURRENCE' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 96,
		StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '365' 
		AND ProductCode = '365' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'OwnersContractorsOrPrincipals' 
		AND CoverageVersion = 'OCCURRENCE' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 95,
		StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '321' 
		AND ProductCode = '321' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'RailroadProtectiveLiability' 
		AND CoverageVersion = 'OCCURRENCE' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 94,
		StandardInsuranceLineCode = 'GL' 
		AND PolicyOfferingCode = '380' 
		AND ProductCode = '380' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'ProductWithdrawal' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 93,
		StandardInsuranceLineCode = 'CDO' 
		AND PolicyOfferingCode = '310' 
		AND ProductCode = '310' 
		AND InsuranceReferenceLineOfBusinessCode = '310' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'DirectorsAndOfficersCondosCommercial' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 92,
		StandardInsuranceLineCode = 'CDO' 
		AND PolicyOfferingCode = '310' 
		AND ProductCode = '310' 
		AND InsuranceReferenceLineOfBusinessCode = '310' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'DirectorsAndOfficersCondosResidential' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 91,
		StandardInsuranceLineCode = 'CU' 
		AND PolicyOfferingCode = '900' 
		AND ProductCode = '900' 
		AND InsuranceReferenceLineOfBusinessCode = '900' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'BuiltUp' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 90,
		StandardInsuranceLineCode = 'CU' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '900' 
		AND InsuranceReferenceLineOfBusinessCode = '900' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'BuiltUp' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 89,
		StandardInsuranceLineCode = 'CU' 
		AND PolicyOfferingCode = '450' 
		AND ProductCode = '900' 
		AND InsuranceReferenceLineOfBusinessCode = '900' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'BuiltUp' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 88,
		StandardInsuranceLineCode = 'CU' 
		AND PolicyOfferingCode = '430' 
		AND ProductCode = '900' 
		AND InsuranceReferenceLineOfBusinessCode = '900' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'BuiltUp' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 87,
		StandardInsuranceLineCode = 'NDO' 
		AND PolicyOfferingCode = '312' 
		AND ProductCode = '312' 
		AND InsuranceReferenceLineOfBusinessCode = '312' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'DirectorsAndOfficersNFP' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 86,
		StandardInsuranceLineCode = 'EPL' 
		AND PolicyOfferingCode = '330' 
		AND ProductCode = '330' 
		AND InsuranceReferenceLineOfBusinessCode = '330' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'EmploymentPracticesLiability' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 85,
		StandardInsuranceLineCode = 'BP' 
		AND PolicyOfferingCode = '450' 
		AND ProductCode = '450' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'RiskLiability' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 84,
		StandardInsuranceLineCode = 'SBOPGL' 
		AND PolicyOfferingCode = '430' 
		AND ProductCode = '430' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'PremOps' 
		AND CoverageType = 'PremisesOperations' 
		AND CoverageVersion = 'OCCURRENCE' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 83,
		StandardInsuranceLineCode = 'WC' 
		AND PolicyOfferingCode = '100' 
		AND ProductCode = '100' 
		AND InsuranceReferenceLineOfBusinessCode = '100' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'EmployersLiability' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 82,
		StandardInsuranceLineCode = 'SBOPGL' 
		AND PolicyOfferingCode = '430' 
		AND ProductCode = '430' 
		AND InsuranceReferenceLineOfBusinessCode = '300' 
		AND RiskType = 'PremOpsProducts' 
		AND CoverageType = 'PremisesOperations' 
		AND CoverageVersion = 'OCCURRENCE' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 81,
		StandardInsuranceLineCode = 'CU' 
		AND PolicyOfferingCode = '500' 
		AND ProductCode = '900' 
		AND InsuranceReferenceLineOfBusinessCode = '900' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'Revised' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 80,
		StandardInsuranceLineCode = 'WC' 
		AND PolicyOfferingCode = '100' 
		AND ProductCode = '100' 
		AND InsuranceReferenceLineOfBusinessCode = '100' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'EmpIoyersLiabilityIncreasedLimits' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 79,
		StandardInsuranceLineCode = 'WC' 
		AND PolicyOfferingCode = '100' 
		AND ProductCode = '100' 
		AND InsuranceReferenceLineOfBusinessCode = '100' 
		AND RiskType = 'N/A' 
		AND CoverageType = 'EmployersLiabilityIncreasedLimitsBalanceToMinimum' 
		AND CoverageVersion = 'N/A' 
		AND PerilGroup = 'N/A' 
		AND SubCoverageTypeCode = 'N/A', 78,
		0
	) AS v_DetectFlag_DCT,
	-- *INF*: IIF(SourceSystemID='PMS', v_DetectFlag_PMS, v_DetectFlag_DCT)
	IFF(SourceSystemID = 'PMS',
		v_DetectFlag_PMS,
		v_DetectFlag_DCT
	) AS o_DetectFlag
	FROM EXP_DefaultValue
),
FIL_MainCoverage AS (
	SELECT
	EffectiveDate, 
	LimitTypeDimID, 
	PolicyAKId, 
	LimitValue, 
	SourceSystemID, 
	RiskType, 
	CoverageType, 
	PerilGroup, 
	SubCoverageTypeCode, 
	CoverageVersion, 
	PolicyLimitId, 
	PremiumTransactionBookedDate, 
	EnterpriseGroupCode, 
	InsuranceReferenceLegalEntityCode, 
	StrategicProfitCenterCode, 
	InsuranceSegmentCode, 
	PolicyOfferingCode, 
	ProductCode, 
	InsuranceReferenceLineOfBusinessCode, 
	RunDateID, 
	RiskUnitGroup, 
	RiskUnit, 
	MajorPerilCode, 
	StandardInsuranceLineCode, 
	ProductTypeCode, 
	RatingPlanCode, 
	o_DetectFlag AS DetectFlag
	FROM EXP_DetectMainCoverage
	WHERE DetectFlag<>0
),
LKP_InsuranceReferenceDim_Policy AS (
	SELECT
	InsuranceReferenceDimId,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	RatingPlanCode
	FROM (
		SELECT 
			InsuranceReferenceDimId,
			EnterpriseGroupCode,
			InsuranceReferenceLegalEntityCode,
			StrategicProfitCenterCode,
			InsuranceSegmentCode,
			PolicyOfferingCode,
			ProductCode,
			InsuranceReferenceLineOfBusinessCode,
			RatingPlanCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode ORDER BY InsuranceReferenceDimId) = 1
),
mplt_PolicyDimID_PremiumMaster_Policy AS (WITH
	Input AS (
		
	),
	EXP_Default AS (
		SELECT
		IN_PolicyAKID AS PolicyAKID,
		IN_Trans_Date
		FROM Input
	),
	LKP_V2_Policy AS (
		SELECT
		contract_cust_ak_id,
		agencyakid,
		pol_status_code,
		strtgc_bus_dvsn_ak_id,
		IN_Trans_Date,
		pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT policy.contract_cust_ak_id as contract_cust_ak_id, policy.agencyakid as agencyakid, policy.pol_status_code as pol_status_code, policy.strtgc_bus_dvsn_ak_id as strtgc_bus_dvsn_ak_id, policy.pol_ak_id as pol_ak_id, policy.eff_from_date as eff_from_date, policy.eff_to_date as eff_to_date FROM 
			V2.policy
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_ak_id DESC) = 1
	),
	LKP_PolicyDimID AS (
		SELECT
		pol_dim_id,
		pol_key,
		pol_eff_date,
		pol_exp_date,
		pms_pol_lob_code,
		ClassOfBusinessCode,
		IN_Trans_Date,
		edw_pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pol_dim_id,
				pol_key,
				pol_eff_date,
				pol_exp_date,
				pms_pol_lob_code,
				ClassOfBusinessCode,
				IN_Trans_Date,
				edw_pol_ak_id,
				eff_from_date,
				eff_to_date
			FROM policy_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
	),
	LKP_V3_AgencyDimID AS (
		SELECT
		agency_dim_id,
		edw_agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT AgencyDim.AgencyDimID as agency_dim_id, AgencyDim.EDWAgencyAKID as edw_agency_ak_id, AgencyDim.EffectiveDate as eff_from_date, AgencyDim.ExpirationDate as eff_to_date
			 FROM V3.AgencyDim as AgencyDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_ContractCustomerDim AS (
		SELECT
		contract_cust_dim_id,
		IN_Trans_Date,
		edw_contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_dim_id,
				IN_Trans_Date,
				edw_contract_cust_ak_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
	),
	lkp_StrategicBusinessDivisionDIM AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		edw_strtgc_bus_dvsn_ak_id
		FROM (
			SELECT strategic_business_division_dim.strtgc_bus_dvsn_dim_id as strtgc_bus_dvsn_dim_id, strategic_business_division_dim.edw_strtgc_bus_dvsn_ak_id as edw_strtgc_bus_dvsn_ak_id 
			FROM strategic_business_division_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_strtgc_bus_dvsn_ak_id ORDER BY strtgc_bus_dvsn_dim_id DESC) = 1
	),
	EXP_Values AS (
		SELECT
		LKP_V3_AgencyDimID.agency_dim_id,
		LKP_ContractCustomerDim.contract_cust_dim_id,
		LKP_PolicyDimID.pol_dim_id,
		LKP_V2_Policy.pol_status_code,
		LKP_PolicyDimID.pol_eff_date,
		LKP_PolicyDimID.pol_exp_date,
		lkp_StrategicBusinessDivisionDIM.strtgc_bus_dvsn_dim_id,
		LKP_PolicyDimID.pol_key,
		LKP_PolicyDimID.pms_pol_lob_code,
		LKP_PolicyDimID.ClassOfBusinessCode
		FROM 
		LEFT JOIN LKP_ContractCustomerDim
		ON LKP_ContractCustomerDim.edw_contract_cust_ak_id = LKP_V2_Policy.contract_cust_ak_id AND LKP_ContractCustomerDim.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_ContractCustomerDim.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_PolicyDimID
		ON LKP_PolicyDimID.edw_pol_ak_id = EXP_Default.PolicyAKID AND LKP_PolicyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_PolicyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V2_Policy
		ON LKP_V2_Policy.pol_ak_id = EXP_Default.PolicyAKID AND LKP_V2_Policy.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V2_Policy.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V3_AgencyDimID
		ON LKP_V3_AgencyDimID.edw_agency_ak_id = LKP_V2_Policy.agencyakid AND LKP_V3_AgencyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V3_AgencyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN lkp_StrategicBusinessDivisionDIM
		ON lkp_StrategicBusinessDivisionDIM.edw_strtgc_bus_dvsn_ak_id = LKP_V2_Policy.strtgc_bus_dvsn_ak_id
	),
	Output AS (
		SELECT
		agency_dim_id, 
		contract_cust_dim_id, 
		pol_dim_id, 
		pol_status_code, 
		pol_eff_date, 
		pol_exp_date, 
		strtgc_bus_dvsn_dim_id, 
		pol_key, 
		pms_pol_lob_code, 
		ClassOfBusinessCode
		FROM EXP_Values
	),
),
EXP_Calculate_DimID AS (
	SELECT
	mplt_PolicyDimID_PremiumMaster_Policy.agency_dim_id AS lkp_AgencyDimId,
	mplt_PolicyDimID_PremiumMaster_Policy.contract_cust_dim_id AS lkp_contract_cust_dim_id,
	mplt_PolicyDimID_PremiumMaster_Policy.pol_dim_id AS lkp_pol_dim_id,
	FIL_MainCoverage.SourceSystemID AS i_SourceSystemID,
	FIL_MainCoverage.RiskType AS i_RiskType,
	FIL_MainCoverage.CoverageType AS i_CoverageType,
	FIL_MainCoverage.PerilGroup AS i_PerilGroup,
	FIL_MainCoverage.SubCoverageTypeCode AS i_SubCoverageTypeCode,
	FIL_MainCoverage.CoverageVersion AS i_CoverageVersion,
	FIL_MainCoverage.RiskUnitGroup AS i_RiskUnitGroup,
	FIL_MainCoverage.RiskUnit AS i_RiskUnit,
	FIL_MainCoverage.MajorPerilCode AS i_MajorPerilCode,
	FIL_MainCoverage.ProductTypeCode AS i_ProductTypeCode,
	FIL_MainCoverage.EffectiveDate,
	FIL_MainCoverage.PolicyAKId,
	FIL_MainCoverage.LimitTypeDimID,
	LKP_InsuranceReferenceDim_Policy.InsuranceReferenceDimId,
	FIL_MainCoverage.LimitValue,
	FIL_MainCoverage.RunDateID,
	FIL_MainCoverage.StandardInsuranceLineCode,
	FIL_MainCoverage.DetectFlag,
	FIL_MainCoverage.PolicyLimitId,
	-- *INF*: DECODE(i_SourceSystemID,'PMS',:LKP.LKP_InsuranceReferenceCoverageDim_PMS(i_RiskUnitGroup,i_RiskUnit,i_MajorPerilCode,StandardInsuranceLineCode,i_ProductTypeCode),'DCT',:LKP.LKP_InsuranceReferenceCoverageDim_DCT(i_RiskType,i_CoverageType,StandardInsuranceLineCode,i_PerilGroup,i_SubCoverageTypeCode,i_CoverageVersion))
	DECODE(i_SourceSystemID,
		'PMS', LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_StandardInsuranceLineCode_i_ProductTypeCode.InsuranceReferenceCoverageDimId,
		'DCT', LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.InsuranceReferenceCoverageDimId
	) AS v_InsuranceReferenceCoverageDimId,
	-- *INF*: IIF(ISNULL(lkp_contract_cust_dim_id),-1,lkp_contract_cust_dim_id)
	IFF(lkp_contract_cust_dim_id IS NULL,
		- 1,
		lkp_contract_cust_dim_id
	) AS o_contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(lkp_pol_dim_id),-1,lkp_pol_dim_id)
	IFF(lkp_pol_dim_id IS NULL,
		- 1,
		lkp_pol_dim_id
	) AS o_pol_dim_id,
	-- *INF*: IIF(ISNULL(lkp_AgencyDimId),-1,lkp_AgencyDimId)
	IFF(lkp_AgencyDimId IS NULL,
		- 1,
		lkp_AgencyDimId
	) AS o_AgencyDimId,
	-1 AS o_CoverageDetailDimId,
	-- *INF*: IIF(ISNULL(v_InsuranceReferenceCoverageDimId),-1,v_InsuranceReferenceCoverageDimId)
	IFF(v_InsuranceReferenceCoverageDimId IS NULL,
		- 1,
		v_InsuranceReferenceCoverageDimId
	) AS o_InsuranceReferenceCoverageDimId
	FROM FIL_MainCoverage
	 -- Manually join with mplt_PolicyDimID_PremiumMaster_Policy
	LEFT JOIN LKP_InsuranceReferenceDim_Policy
	ON LKP_InsuranceReferenceDim_Policy.EnterpriseGroupCode = FIL_MainCoverage.EnterpriseGroupCode AND LKP_InsuranceReferenceDim_Policy.InsuranceReferenceLegalEntityCode = FIL_MainCoverage.InsuranceReferenceLegalEntityCode AND LKP_InsuranceReferenceDim_Policy.StrategicProfitCenterCode = FIL_MainCoverage.StrategicProfitCenterCode AND LKP_InsuranceReferenceDim_Policy.InsuranceSegmentCode = FIL_MainCoverage.InsuranceSegmentCode AND LKP_InsuranceReferenceDim_Policy.PolicyOfferingCode = FIL_MainCoverage.PolicyOfferingCode AND LKP_InsuranceReferenceDim_Policy.ProductCode = FIL_MainCoverage.ProductCode AND LKP_InsuranceReferenceDim_Policy.InsuranceReferenceLineOfBusinessCode = FIL_MainCoverage.InsuranceReferenceLineOfBusinessCode AND LKP_InsuranceReferenceDim_Policy.RatingPlanCode = FIL_MainCoverage.RatingPlanCode
	LEFT JOIN LKP_INSURANCEREFERENCECOVERAGEDIM_PMS LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_StandardInsuranceLineCode_i_ProductTypeCode
	ON LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_StandardInsuranceLineCode_i_ProductTypeCode.InsuranceLineCode = i_RiskUnitGroup
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_StandardInsuranceLineCode_i_ProductTypeCode.PmsRiskUnitGroupCode = i_RiskUnit
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_StandardInsuranceLineCode_i_ProductTypeCode.PmsRiskUnitCode = i_MajorPerilCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_StandardInsuranceLineCode_i_ProductTypeCode.PmsMajorPerilCode = StandardInsuranceLineCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_PMS_i_RiskUnitGroup_i_RiskUnit_i_MajorPerilCode_StandardInsuranceLineCode_i_ProductTypeCode.PmsProductTypeCode = i_ProductTypeCode

	LEFT JOIN LKP_INSURANCEREFERENCECOVERAGEDIM_DCT LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion
	ON LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.InsuranceLineCode = i_RiskType
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctRiskTypeCode = i_CoverageType
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctCoverageTypeCode = StandardInsuranceLineCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctPerilGroup = i_PerilGroup
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctSubCoverageTypeCode = i_SubCoverageTypeCode
	AND LKP_INSURANCEREFERENCECOVERAGEDIM_DCT_i_RiskType_i_CoverageType_StandardInsuranceLineCode_i_PerilGroup_i_SubCoverageTypeCode_i_CoverageVersion.DctCoverageVersion = i_CoverageVersion

),
SRT_Source AS (
	SELECT
	PolicyAKId, 
	StandardInsuranceLineCode, 
	LimitTypeDimID, 
	EffectiveDate, 
	PolicyLimitId, 
	DetectFlag, 
	RunDateID, 
	InsuranceReferenceDimId, 
	LimitValue, 
	o_contract_cust_dim_id AS contract_cust_dim_id, 
	o_pol_dim_id AS pol_dim_id, 
	o_AgencyDimId AS AgencyDimId, 
	o_CoverageDetailDimId AS CoverageDetailDimId, 
	o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId
	FROM EXP_Calculate_DimID
	ORDER BY PolicyAKId ASC, StandardInsuranceLineCode ASC, LimitTypeDimID ASC, EffectiveDate ASC, PolicyLimitId ASC, DetectFlag ASC
),
AGG_RemoveDuplicates AS (
	SELECT
	PolicyAKId,
	StandardInsuranceLineCode,
	LimitTypeDimID,
	EffectiveDate,
	PolicyLimitId,
	DetectFlag,
	InsuranceReferenceDimId,
	LimitValue,
	RunDateID AS i_RunDateID,
	-- *INF*: MIN(i_RunDateID)
	MIN(i_RunDateID
	) AS o_RunDateID,
	contract_cust_dim_id,
	pol_dim_id,
	AgencyDimId,
	CoverageDetailDimId,
	InsuranceReferenceCoverageDimId
	FROM SRT_Source
	GROUP BY PolicyAKId, StandardInsuranceLineCode, LimitTypeDimID, EffectiveDate, PolicyLimitId
),
EXP_DetectChange_Policy AS (
	SELECT
	PolicyAKId,
	StandardInsuranceLineCode,
	LimitTypeDimID AS i_LimitTypeDimID,
	InsuranceReferenceDimId AS i_InsuranceReferenceDimId,
	LimitValue AS i_LimitValue,
	o_RunDateID AS i_RunDateID,
	contract_cust_dim_id AS i_contract_cust_dim_id,
	pol_dim_id AS i_pol_dim_id,
	AgencyDimId AS i_AgencyDimID,
	CoverageDetailDimId AS i_CoverageDetailDimId,
	InsuranceReferenceCoverageDimId AS i_InsuranceReferenceCoverageDimId,
	-- *INF*: DECODE(TRUE,
	-- PolicyAKId=v_Prev_PolicyAKId AND i_LimitTypeDimID = v_Prev_LimitTypeDimId AND StandardInsuranceLineCode = v_Prev_StandardInsuranceLineCode,
	-- TO_DECIMAL(i_LimitValue)-v_Prev_LimitValue,
	-- TO_DECIMAL(i_LimitValue)
	-- )
	-- 
	-- ---DECODE(TRUE,
	-- -- NOT ISNULL(lkp_MaxRunDateId) AND lkp_MaxRunDateId >--= i_RunDateID, 0,
	-- ---i_PolicyAKId=v_Prev_PolicyAKId 
	-- --AND i_LimitTypeDimID=v_Prev_LimitTypeDimId
	-- --AND --------i_StandardInsuranceLineCode=v_Prev_StandardInsuranceLin---eCode AND 
	-- -- NOT ISNULL(lkp_MaxRunDateId)
	-- --,TO_DECIMAL(i_LimitValue)---(v_Prev_LimitValue+lkp_ChangeInLimit),
	--  --NOT ISNULL(lkp_MaxRunDateId), 
	-- --TO_DECIMAL(i_LimitValue)-lkp_ChangeInLimit,
	-- --i_PolicyAKId=v_Prev_PolicyAKId AND --i_LimitTypeDimID=v_Prev_LimitTypeDimId
	-- --AND ----i_StandardInsuranceLineCode=v_Prev_StandardInsuranceLin----eCode,
	-- --TO_DECIMAL(i_LimitValue)-v_Prev_LimitValue,
	-- ---TO_DECIMAL(i_LimitValue))
	DECODE(TRUE,
		PolicyAKId = v_Prev_PolicyAKId 
		AND i_LimitTypeDimID = v_Prev_LimitTypeDimId 
		AND StandardInsuranceLineCode = v_Prev_StandardInsuranceLineCode, CAST(i_LimitValue AS FLOAT) - v_Prev_LimitValue,
		CAST(i_LimitValue AS FLOAT)
	) AS v_LimitValue,
	-- *INF*: DECODE(TRUE,
	-- PolicyAKId=v_Prev_PolicyAKId AND i_LimitTypeDimID = v_Prev_LimitTypeDimId AND StandardInsuranceLineCode = v_Prev_StandardInsuranceLineCode AND v_LimitValue != 0,
	-- v_Count+1,
	-- PolicyAKId=v_Prev_PolicyAKId AND i_LimitTypeDimID = v_Prev_LimitTypeDimId AND StandardInsuranceLineCode = v_Prev_StandardInsuranceLineCode AND v_LimitValue = 0,
	-- v_Count,
	-- 1
	-- )
	DECODE(TRUE,
		PolicyAKId = v_Prev_PolicyAKId 
		AND i_LimitTypeDimID = v_Prev_LimitTypeDimId 
		AND StandardInsuranceLineCode = v_Prev_StandardInsuranceLineCode 
		AND v_LimitValue != 0, v_Count + 1,
		PolicyAKId = v_Prev_PolicyAKId 
		AND i_LimitTypeDimID = v_Prev_LimitTypeDimId 
		AND StandardInsuranceLineCode = v_Prev_StandardInsuranceLineCode 
		AND v_LimitValue = 0, v_Count,
		1
	) AS v_Count,
	-- *INF*: TO_DECIMAL(i_LimitValue)
	-- ---DECODE(TRUE,
	-- -- NOT ISNULL(lkp_MaxRunDateId) AND lkp_MaxRunDateId  -->=  i_RunDateID, 0,
	-- --i_PolicyAKId=v_Prev_PolicyAKId 
	-- --AND i_LimitTypeDimID=v_Prev_LimitTypeDimId
	-- --AND i_StandardInsuranceLineCode=i_StandardInsuranceLineCode, 
	-- --v_Prev_LimitValue+v_LimitValue,
	-- --v_LimitValue
	-- --)
	CAST(i_LimitValue AS FLOAT) AS v_Prev_LimitValue,
	PolicyAKId AS v_Prev_PolicyAKId,
	i_LimitTypeDimID AS v_Prev_LimitTypeDimId,
	StandardInsuranceLineCode AS v_Prev_StandardInsuranceLineCode,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_AgencyDimID AS o_AgencyDimID,
	i_pol_dim_id AS o_pol_dim_id,
	i_contract_cust_dim_id AS o_contract_cust_dim_id,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	i_InsuranceReferenceDimId AS o_InsuranceReferenceDimId,
	i_InsuranceReferenceCoverageDimId AS o_InsuranceReferenceCoverageDimId,
	i_LimitTypeDimID AS o_LimitTypeDimID,
	i_RunDateID AS o_RunDateID,
	v_LimitValue AS o_ChangeInLimit,
	v_Count AS o_Count
	FROM AGG_RemoveDuplicates
),
FIL_Zero_Policy AS (
	SELECT
	PolicyAKId, 
	StandardInsuranceLineCode, 
	o_AuditId AS AuditId, 
	o_AgencyDimID AS AgencyDimID, 
	o_pol_dim_id AS pol_dim_id, 
	o_contract_cust_dim_id AS contract_cust_dim_id, 
	o_CoverageDetailDimId AS CoverageDetailDimId, 
	o_InsuranceReferenceDimId AS InsuranceReferenceDimId, 
	o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId, 
	o_LimitTypeDimID AS LimitTypeDimID, 
	o_RunDateID AS RunDateID, 
	o_ChangeInLimit AS ChangeInLimit, 
	o_Count AS Count
	FROM EXP_DetectChange_Policy
	WHERE ChangeInLimit<>0
),
LKP_LimitFact_Policy AS (
	SELECT
	LimitFactId,
	ChangeInLimit,
	PolicyAKId,
	LimitTypeDimId,
	InsuranceLineCode,
	Count
	FROM (
		SELECT LF.LimitFactId as LimitFactId,
		LF.ChangeInLimit as ChangeInLimit, 
		row_number() over (partition by PD.edw_pol_ak_id, LF.LimitTypeDimId,IRCD.InsuranceLineCode order by LF.LimitFactId) as Count,
		PD.edw_pol_ak_id as PolicyAKId, 
		LF.LimitTypeDimId as LimitTypeDimId, 
		IRCD.InsuranceLineCode as InsuranceLineCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.LimitFact LF
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Policy_Dim PD
		ON LF.PolicyDimId=PD.pol_dim_id
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim IRCD
		on LF.InsuranceReferenceCoverageDimId=IRCD.InsuranceReferenceCoverageDimId
		WHERE LF.CoverageDetailDimId=-1 and ('@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' or exists (select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PL
		where PL.PolicyAKId=PD.edw_pol_ak_id and PL.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,LimitTypeDimId,InsuranceLineCode,Count ORDER BY LimitFactId) = 1
),
RTR_InsertUpdate_Policy AS (
	SELECT
	LKP_LimitFact_Policy.LimitFactId AS lkp_LimitFactId,
	LKP_LimitFact_Policy.ChangeInLimit AS lkp_ChangeInLimit,
	FIL_Zero_Policy.AuditId,
	FIL_Zero_Policy.AgencyDimID,
	FIL_Zero_Policy.pol_dim_id,
	FIL_Zero_Policy.contract_cust_dim_id,
	FIL_Zero_Policy.CoverageDetailDimId,
	FIL_Zero_Policy.InsuranceReferenceDimId,
	FIL_Zero_Policy.InsuranceReferenceCoverageDimId,
	FIL_Zero_Policy.LimitTypeDimID,
	FIL_Zero_Policy.RunDateID,
	FIL_Zero_Policy.ChangeInLimit
	FROM FIL_Zero_Policy
	LEFT JOIN LKP_LimitFact_Policy
	ON LKP_LimitFact_Policy.PolicyAKId = FIL_Zero_Policy.PolicyAKId AND LKP_LimitFact_Policy.LimitTypeDimId = FIL_Zero_Policy.LimitTypeDimID AND LKP_LimitFact_Policy.InsuranceLineCode = FIL_Zero_Policy.StandardInsuranceLineCode AND LKP_LimitFact_Policy.Count = FIL_Zero_Policy.Count
),
RTR_InsertUpdate_Policy_Insert AS (SELECT * FROM RTR_InsertUpdate_Policy WHERE ISNULL(lkp_LimitFactId)),
RTR_InsertUpdate_Policy_Update AS (SELECT * FROM RTR_InsertUpdate_Policy WHERE NOT ISNULL(lkp_LimitFactId) AND lkp_ChangeInLimit != ChangeInLimit),
LimitFact_Policy_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.LimitFact
	(AuditId, AgencyDimId, PolicyDimId, ContractCustomerDimId, CoverageDetailDimId, InsuranceReferenceDimId, InsuranceReferenceCoverageDimId, LimitTypeDimID, RunDateId, ChangeInLimit)
	SELECT 
	AUDITID, 
	AgencyDimID AS AGENCYDIMID, 
	pol_dim_id AS POLICYDIMID, 
	contract_cust_dim_id AS CONTRACTCUSTOMERDIMID, 
	COVERAGEDETAILDIMID, 
	INSURANCEREFERENCEDIMID, 
	INSURANCEREFERENCECOVERAGEDIMID, 
	LIMITTYPEDIMID, 
	RunDateID AS RUNDATEID, 
	CHANGEINLIMIT
	FROM RTR_InsertUpdate_Policy_Insert
),
UPD_Existing_Policy AS (
	SELECT
	lkp_LimitFactId AS LKP_LimitFactId3, 
	AuditId AS AuditId3, 
	AgencyDimID AS AgencyDimID3, 
	pol_dim_id AS pol_dim_id3, 
	contract_cust_dim_id AS contract_cust_dim_id3, 
	CoverageDetailDimId AS CoverageDetailDimId3, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId3, 
	InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId3, 
	LimitTypeDimID AS LimitTypeDimID3, 
	RunDateID AS RunDateID3, 
	ChangeInLimit AS ChangeInLimit3
	FROM RTR_InsertUpdate_Policy_Update
),
LimitFact_Policy_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.LimitFact AS T
	USING UPD_Existing_Policy AS S
	ON T.LimitFactId = S.LKP_LimitFactId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId3, T.AgencyDimId = S.AgencyDimID3, T.PolicyDimId = S.pol_dim_id3, T.ContractCustomerDimId = S.contract_cust_dim_id3, T.CoverageDetailDimId = S.CoverageDetailDimId3, T.InsuranceReferenceDimId = S.InsuranceReferenceDimId3, T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDimId3, T.LimitTypeDimID = S.LimitTypeDimID3, T.RunDateId = S.RunDateID3, T.ChangeInLimit = S.ChangeInLimit3
),