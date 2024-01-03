WITH
SQ_RatingCoverage AS (
	SELECT DISTINCT RC.RiskType, RC.CoverageType, RC.CoverageVersion, RC.PerilGroup, RC.SubCoverageTypeCode, PC.InsuranceLine, ISNULL(SIL.StandardInsuranceLineCode,'N/A') StandardInsuranceLineCode, ISNULL(SIL.StandardInsuranceLineDescription,'N/A') StandardInsuranceLineDescription
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	INNER JOIN
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON RC.PolicyCoverageAKId=PC.PolicyCoverageAKId
	LEFT JOIN
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	ON SIL.ins_line_code=PC.InsuranceLine
	AND source_sys_id='DCT'
	AND crrnt_snpsht_flag=1
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_FormatValues AS (
	SELECT
	StandardInsuranceLineCode AS lkp_StandardInsuranceLineCode,
	StandardInsuranceLineDescription AS lkp_StandardInsuranceLineDescription,
	InsuranceLine AS i_LineType,
	CoverageType AS i_CoverageType,
	SubCoverageTypeCode AS i_SubCoverageType,
	RiskType AS i_RiskType,
	CoverageVersion AS i_CoverageVersion,
	PerilGroup AS i_PerilGroup,
	-- *INF*: IIF(IsNull(lkp_StandardInsuranceLineCode),'Line Not Found.  Source Line: ' || i_LineType, lkp_StandardInsuranceLineCode)
	IFF(lkp_StandardInsuranceLineCode IS NULL, 'Line Not Found.  Source Line: ' || i_LineType, lkp_StandardInsuranceLineCode) AS o_InsuranceLineCode,
	-- *INF*: IIF(IsNull(lkp_StandardInsuranceLineDescription), 'No Description Found For Line', lkp_StandardInsuranceLineDescription)
	IFF(lkp_StandardInsuranceLineDescription IS NULL, 'No Description Found For Line', lkp_StandardInsuranceLineDescription) AS o_InsuranceLineDescription,
	-- *INF*: IIF(IsNull(i_CoverageType) or IS_SPACES(i_CoverageType), 'N/A', rtrim(ltrim(i_CoverageType)))
	IFF(i_CoverageType IS NULL OR IS_SPACES(i_CoverageType), 'N/A', rtrim(ltrim(i_CoverageType))) AS o_CoverageType,
	-- *INF*: IIF(IsNull(i_SubCoverageType) or IS_SPACES(i_SubCoverageType), 'N/A', rtrim(ltrim(i_SubCoverageType)))
	-- 
	IFF(i_SubCoverageType IS NULL OR IS_SPACES(i_SubCoverageType), 'N/A', rtrim(ltrim(i_SubCoverageType))) AS o_SubCoverageType,
	-- *INF*: IIF(IsNull(i_RiskType) or IS_SPACES(i_RiskType), 'N/A', rtrim(ltrim(i_RiskType)))
	IFF(i_RiskType IS NULL OR IS_SPACES(i_RiskType), 'N/A', rtrim(ltrim(i_RiskType))) AS o_RiskType,
	-- *INF*: IIF(IsNull(i_CoverageVersion) or IS_SPACES(i_CoverageVersion), 'N/A', rtrim(ltrim(i_CoverageVersion)))
	-- 
	IFF(i_CoverageVersion IS NULL OR IS_SPACES(i_CoverageVersion), 'N/A', rtrim(ltrim(i_CoverageVersion))) AS o_CoverageVersion,
	-- *INF*: IIF(IsNull(i_PerilGroup) or IS_SPACES(i_PerilGroup), 'N/A', rtrim(ltrim(i_PerilGroup)))
	-- 
	IFF(i_PerilGroup IS NULL OR IS_SPACES(i_PerilGroup), 'N/A', rtrim(ltrim(i_PerilGroup))) AS o_PerilGroup
	FROM SQ_RatingCoverage
),
Agg_Distinct AS (
	SELECT
	o_InsuranceLineCode AS InsuranceLineCode, 
	o_InsuranceLineDescription AS InsuranceLineDescription, 
	o_CoverageType AS CoverageType, 
	o_SubCoverageType AS SubCoverageType, 
	o_RiskType AS RiskType, 
	o_CoverageVersion AS CoverageVersion, 
	o_PerilGroup AS PerilGroup
	FROM EXP_FormatValues
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode, InsuranceLineDescription, CoverageType, SubCoverageType, RiskType, CoverageVersion, PerilGroup ORDER BY NULL) = 1
),
LKP_SystemCoverage_IR AS (
	SELECT
	PmsRiskUnitGroupCode,
	PmsRiskUnitGroupDescription,
	PmsRiskUnitCode,
	PmsRiskUnitDescription,
	PmsMajorPerilCode,
	PmsMajorPerilDescription,
	PmsProductTypeCode,
	SystemCoverageId,
	ConformedCoverageId,
	InsuranceLineCode,
	DctCoverageTypeCode,
	DctSubCoverageTypeCode,
	DctRiskTypeCode,
	DctCoverageVersion,
	DctPerilGroup
	FROM (
		SELECT 
			PmsRiskUnitGroupCode,
			PmsRiskUnitGroupDescription,
			PmsRiskUnitCode,
			PmsRiskUnitDescription,
			PmsMajorPerilCode,
			PmsMajorPerilDescription,
			PmsProductTypeCode,
			SystemCoverageId,
			ConformedCoverageId,
			InsuranceLineCode,
			DctCoverageTypeCode,
			DctSubCoverageTypeCode,
			DctRiskTypeCode,
			DctCoverageVersion,
			DctPerilGroup
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SystemCoverage
		WHERE SourceSystemId = 'DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctCoverageTypeCode,DctSubCoverageTypeCode,DctRiskTypeCode,DctCoverageVersion,DctPerilGroup ORDER BY PmsRiskUnitGroupCode) = 1
),
LKP_CombinedConformedCoverage AS (
	SELECT
	ConformedCoverageId,
	CoverageCode,
	CoverageDescription,
	CoverageGroupCode,
	CoverageGroupDescription,
	CoverageSummaryCode,
	CoverageSummaryDescription,
	RatedCoverageCode,
	RatedCoverageDescription
	FROM (
		SELECT CoverageCode as CoverageCode,
		       CoverageDescription as CoverageDescription,
		       CoverageGroupCode as CoverageGroupCode,
		       CoverageGroupDescription as CoverageGroupDescription,
		       CoverageSummaryCode as CoverageSummaryCode,
		       CoverageSummaryDescription as CoverageSummaryDescription,
		       ConformedCoverageId as ConformedCoverageId,
			   RatedCoverageCode as RatedCoverageCode, 
			   RatedCoverageDescription as RatedCoverageDescription
		FROM   dbo.CONFORMEDCOVERAGE B
		       INNER JOIN dbo.COVERAGEGROUP C
		         ON B.CoverageGroupId = C.CoverageGroupId
		       INNER JOIN dbo.COVERAGESUMMARY D
		         ON C.CoverageSummaryId = D.CoverageSummaryId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ConformedCoverageId ORDER BY ConformedCoverageId) = 1
),
EXP_CheckForNew AS (
	SELECT
	LKP_SystemCoverage_IR.SystemCoverageId AS lkp_SystemCoverageId,
	LKP_CombinedConformedCoverage.ConformedCoverageId AS lkp_ConformedCoverageId,
	'NULL' AS NullValue,
	Agg_Distinct.InsuranceLineCode,
	Agg_Distinct.InsuranceLineDescription,
	Agg_Distinct.CoverageType,
	Agg_Distinct.SubCoverageType,
	Agg_Distinct.RiskType,
	Agg_Distinct.CoverageVersion,
	Agg_Distinct.PerilGroup,
	LKP_CombinedConformedCoverage.CoverageCode,
	LKP_CombinedConformedCoverage.CoverageDescription,
	LKP_CombinedConformedCoverage.CoverageGroupCode,
	LKP_CombinedConformedCoverage.CoverageGroupDescription,
	LKP_CombinedConformedCoverage.CoverageSummaryCode,
	LKP_CombinedConformedCoverage.CoverageSummaryDescription,
	LKP_SystemCoverage_IR.PmsRiskUnitGroupCode,
	LKP_SystemCoverage_IR.PmsRiskUnitGroupDescription,
	LKP_SystemCoverage_IR.PmsRiskUnitCode,
	LKP_SystemCoverage_IR.PmsRiskUnitDescription,
	LKP_SystemCoverage_IR.PmsMajorPerilCode,
	LKP_SystemCoverage_IR.PmsMajorPerilDescription,
	LKP_SystemCoverage_IR.PmsProductTypeCode,
	LKP_CombinedConformedCoverage.RatedCoverageCode,
	LKP_CombinedConformedCoverage.RatedCoverageDescription
	FROM Agg_Distinct
	LEFT JOIN LKP_CombinedConformedCoverage
	ON LKP_CombinedConformedCoverage.ConformedCoverageId = LKP_SystemCoverage_IR.ConformedCoverageId
	LEFT JOIN LKP_SystemCoverage_IR
	ON LKP_SystemCoverage_IR.InsuranceLineCode = Agg_Distinct.InsuranceLineCode AND LKP_SystemCoverage_IR.DctCoverageTypeCode = Agg_Distinct.CoverageType AND LKP_SystemCoverage_IR.DctSubCoverageTypeCode = Agg_Distinct.SubCoverageType AND LKP_SystemCoverage_IR.DctRiskTypeCode = Agg_Distinct.RiskType AND LKP_SystemCoverage_IR.DctCoverageVersion = Agg_Distinct.CoverageVersion AND LKP_SystemCoverage_IR.DctPerilGroup = Agg_Distinct.PerilGroup
),
FIL_UNK AS (
	SELECT
	NullValue AS ASLCode, 
	NullValue AS ASLCodeDescription, 
	NullValue AS SubASLCode, 
	NullValue AS SubASLCodeDescription, 
	NullValue AS NonSubASLCode, 
	NullValue AS NonSubASLCodeDescription, 
	InsuranceLineCode, 
	InsuranceLineDescription, 
	CoverageCode, 
	CoverageDescription, 
	CoverageGroupCode, 
	CoverageGroupDescription, 
	CoverageSummaryCode, 
	CoverageSummaryDescription, 
	RiskType AS DctRiskTypeCode, 
	CoverageType AS DctCoverageTypeCode, 
	PmsRiskUnitGroupCode, 
	PmsRiskUnitGroupDescription, 
	PmsRiskUnitCode, 
	PmsRiskUnitDescription, 
	PmsMajorPerilCode, 
	PmsMajorPerilDescription, 
	PmsProductTypeCode, 
	PerilGroup AS DctPerilGroup, 
	SubCoverageType AS DctSubCoverageTypeCode, 
	CoverageVersion AS DctCoverageVersion, 
	RatedCoverageCode, 
	RatedCoverageDescription
	FROM EXP_CheckForNew
	WHERE @{pipeline().parameters.FILTER_CONDITION}
),
MissingASLCoverageCombinations AS (
	INSERT INTO MissingASLCoverageCombinations
	(ASLCode, ASLCodeDescription, SubASLCode, SubASLCodeDescription, NonSubASLCode, NonSubASLCodeDescription, InsuranceLineCode, InsuranceLineDescription, RatedCoverageCode, RatedCoverageDescription, CoverageCode, CoverageDescription, CoverageGroupCode, CoverageGroupDescription, CoverageSummaryCode, CoverageSummaryDescription, DctRiskTypeCode, DctCoverageTypeCode, PmsRiskUnitGroupCode, PmsRiskUnitGroupDescription, PmsRiskUnitCode, PmsRiskUnitDescription, PmsMajorPerilCode, PmsMajorPerilDescription, PmsProductTypeCode, DctPerilGroup, DctSubCoverageTypeCode, DctCoverageVersion)
	SELECT 
	ASLCODE, 
	ASLCODEDESCRIPTION, 
	SUBASLCODE, 
	SUBASLCODEDESCRIPTION, 
	NONSUBASLCODE, 
	NONSUBASLCODEDESCRIPTION, 
	INSURANCELINECODE, 
	INSURANCELINEDESCRIPTION, 
	RATEDCOVERAGECODE, 
	RATEDCOVERAGEDESCRIPTION, 
	COVERAGECODE, 
	COVERAGEDESCRIPTION, 
	COVERAGEGROUPCODE, 
	COVERAGEGROUPDESCRIPTION, 
	COVERAGESUMMARYCODE, 
	COVERAGESUMMARYDESCRIPTION, 
	DCTRISKTYPECODE, 
	DCTCOVERAGETYPECODE, 
	PMSRISKUNITGROUPCODE, 
	PMSRISKUNITGROUPDESCRIPTION, 
	PMSRISKUNITCODE, 
	PMSRISKUNITDESCRIPTION, 
	PMSMAJORPERILCODE, 
	PMSMAJORPERILDESCRIPTION, 
	PMSPRODUCTTYPECODE, 
	DCTPERILGROUP, 
	DCTSUBCOVERAGETYPECODE, 
	DCTCOVERAGEVERSION
	FROM FIL_UNK
),