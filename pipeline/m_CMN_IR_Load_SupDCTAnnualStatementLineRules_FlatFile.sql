WITH
SQ_DCTASLCoverageCombinations AS (

-- TODO Manual --

),
EXP_Default AS (
	SELECT
	ASLCode AS i_ASLCode,
	ASLCodeDescription AS i_ASLCodeDescription,
	SubASLCode AS i_SubASLCode,
	SubASLCodeDescription AS i_SubASLCodeDescription,
	NonSubASLCode AS i_NonSubASLCode,
	NonSubASLCodeDescription AS i_NonSubASLCodeDescription,
	InsuranceLineCode AS i_InsuranceLineCode,
	InsuranceLineDescription AS i_InsuranceLineDescription,
	CoverageCode AS i_CoverageCode,
	CoverageDescription AS i_CoverageDescription,
	CoverageGroupCode AS i_CoverageGroupCode,
	RatedCoverageCode AS i_RatedCoverageCode,
	RatedCoverageDescription AS i_RatedCoverageDescription,
	CoverageGroupDescription AS i_CoverageGroupDescription,
	CoverageSummaryCode AS i_CoverageSummaryCode,
	CoverageSummaryDescription AS i_CoverageSummaryDescription,
	DctRiskTypeCode AS i_DctRiskTypeCode,
	DctCoverageTypeCode AS i_DctCoverageTypeCode,
	PmsRiskUnitGroupCode AS i_PmsRiskUnitGroupCode,
	PmsRiskUnitGroupDescription AS i_PmsRiskUnitGroupDescription,
	PmsRiskUnitCode AS i_PmsRiskUnitCode,
	PmsRiskUnitDescription AS i_PmsRiskUnitDescription,
	PmsMajorPerilCode AS i_PmsMajorPerilCode,
	PmsMajorPerilDescription AS i_PmsMajorPerilDescription,
	PmsProductTypeCode AS i_PmsProductTypeCode,
	DctPerilGroup AS i_DctPerilGroup,
	DctSubCoverageTypeCode AS i_DctSubCoverageTypeCode,
	DctCoverageVersion AS i_DctCoverageVersion,
	-- *INF*: LTRIM(RTRIM( i_ASLCode ))
	LTRIM(RTRIM(i_ASLCode
		)
	) AS v_ASLCode,
	-- *INF*: LTRIM(RTRIM( i_SubASLCode ))
	LTRIM(RTRIM(i_SubASLCode
		)
	) AS v_SubASLCode,
	-- *INF*: LTRIM(RTRIM( i_NonSubASLCode ))
	LTRIM(RTRIM(i_NonSubASLCode
		)
	) AS v_NonSubASLCode,
	-- *INF*: LTRIM(RTRIM( i_InsuranceLineCode ))
	LTRIM(RTRIM(i_InsuranceLineCode
		)
	) AS v_InsuranceLineCode,
	-- *INF*: LTRIM(RTRIM( i_CoverageCode ))
	LTRIM(RTRIM(i_CoverageCode
		)
	) AS v_CoverageCode,
	-- *INF*: LTRIM(RTRIM( i_RatedCoverageCode ))
	LTRIM(RTRIM(i_RatedCoverageCode
		)
	) AS v_RatedCoverageCode,
	-- *INF*: LTRIM(RTRIM( i_CoverageGroupCode ))
	LTRIM(RTRIM(i_CoverageGroupCode
		)
	) AS v_CoverageGroupCode,
	-- *INF*: LTRIM(RTRIM( i_CoverageSummaryCode ))
	LTRIM(RTRIM(i_CoverageSummaryCode
		)
	) AS v_CoverageSummaryCode,
	-- *INF*: LTRIM(RTRIM( i_DctRiskTypeCode ))
	LTRIM(RTRIM(i_DctRiskTypeCode
		)
	) AS v_DctRiskTypeCode,
	-- *INF*: LTRIM(RTRIM( i_DctCoverageTypeCode ))
	LTRIM(RTRIM(i_DctCoverageTypeCode
		)
	) AS v_DctCoverageTypeCode,
	-- *INF*: LTRIM(RTRIM( i_PmsRiskUnitGroupCode ))
	LTRIM(RTRIM(i_PmsRiskUnitGroupCode
		)
	) AS v_PmsRiskUnitGroupCode,
	-- *INF*: LTRIM(RTRIM( i_PmsRiskUnitCode ))
	LTRIM(RTRIM(i_PmsRiskUnitCode
		)
	) AS v_PmsRiskUnitCode,
	-- *INF*: LTRIM(RTRIM( i_PmsMajorPerilCode ))
	LTRIM(RTRIM(i_PmsMajorPerilCode
		)
	) AS v_PmsMajorPerilCode,
	-- *INF*: LTRIM(RTRIM( i_PmsProductTypeCode ))
	LTRIM(RTRIM(i_PmsProductTypeCode
		)
	) AS v_PmsProductTypeCode,
	-- *INF*: LTRIM(RTRIM( i_DctPerilGroup ))
	LTRIM(RTRIM(i_DctPerilGroup
		)
	) AS v_DctPerilGroup,
	-- *INF*: LTRIM(RTRIM( i_DctSubCoverageTypeCode ))
	LTRIM(RTRIM(i_DctSubCoverageTypeCode
		)
	) AS v_DctSubCoverageTypeCode,
	-- *INF*: LTRIM(RTRIM( i_DctCoverageVersion ))
	LTRIM(RTRIM(i_DctCoverageVersion
		)
	) AS v_DctCoverageVersion,
	-- *INF*: IIF(ISNULL(v_ASLCode) OR v_ASLCode='' OR v_ASLCode='NULL', 'N/A', v_ASLCode)
	IFF(v_ASLCode IS NULL 
		OR v_ASLCode = '' 
		OR v_ASLCode = 'NULL',
		'N/A',
		v_ASLCode
	) AS o_ASLCode,
	-- *INF*: IIF(ISNULL(v_SubASLCode) OR v_SubASLCode='' OR v_SubASLCode='NULL', 'N/A', v_SubASLCode)
	IFF(v_SubASLCode IS NULL 
		OR v_SubASLCode = '' 
		OR v_SubASLCode = 'NULL',
		'N/A',
		v_SubASLCode
	) AS o_SubASLCode,
	-- *INF*: IIF(ISNULL(v_NonSubASLCode) OR v_NonSubASLCode='' OR v_NonSubASLCode='NULL', 'N/A', v_NonSubASLCode)
	IFF(v_NonSubASLCode IS NULL 
		OR v_NonSubASLCode = '' 
		OR v_NonSubASLCode = 'NULL',
		'N/A',
		v_NonSubASLCode
	) AS o_NonSubASLCode,
	-- *INF*: IIF(ISNULL(v_InsuranceLineCode) OR v_InsuranceLineCode='' OR v_InsuranceLineCode='NULL', 'N/A', v_InsuranceLineCode)
	IFF(v_InsuranceLineCode IS NULL 
		OR v_InsuranceLineCode = '' 
		OR v_InsuranceLineCode = 'NULL',
		'N/A',
		v_InsuranceLineCode
	) AS o_InsuranceLineCode,
	-- *INF*: IIF(ISNULL(v_CoverageCode) OR v_CoverageCode='' OR v_CoverageCode='NULL', 'N/A', v_CoverageCode)
	IFF(v_CoverageCode IS NULL 
		OR v_CoverageCode = '' 
		OR v_CoverageCode = 'NULL',
		'N/A',
		v_CoverageCode
	) AS o_CoverageCode,
	-- *INF*: IIF(ISNULL(v_RatedCoverageCode) OR v_RatedCoverageCode='' OR v_RatedCoverageCode='NULL', 'N/A', v_RatedCoverageCode)
	IFF(v_RatedCoverageCode IS NULL 
		OR v_RatedCoverageCode = '' 
		OR v_RatedCoverageCode = 'NULL',
		'N/A',
		v_RatedCoverageCode
	) AS o_RatedCoverageCode,
	-- *INF*: IIF(ISNULL(v_CoverageGroupCode) OR v_CoverageGroupCode='' OR v_CoverageGroupCode='NULL', 'N/A', v_CoverageGroupCode)
	IFF(v_CoverageGroupCode IS NULL 
		OR v_CoverageGroupCode = '' 
		OR v_CoverageGroupCode = 'NULL',
		'N/A',
		v_CoverageGroupCode
	) AS o_CoverageGroupCode,
	-- *INF*: IIF(ISNULL(v_CoverageSummaryCode) OR v_CoverageSummaryCode='' OR v_CoverageSummaryCode='NULL', 'N/A', v_CoverageSummaryCode)
	IFF(v_CoverageSummaryCode IS NULL 
		OR v_CoverageSummaryCode = '' 
		OR v_CoverageSummaryCode = 'NULL',
		'N/A',
		v_CoverageSummaryCode
	) AS o_CoverageSummaryCode,
	-- *INF*: IIF(ISNULL(v_DctRiskTypeCode) OR v_DctRiskTypeCode='' OR v_DctRiskTypeCode='NULL', 'N/A', v_DctRiskTypeCode)
	IFF(v_DctRiskTypeCode IS NULL 
		OR v_DctRiskTypeCode = '' 
		OR v_DctRiskTypeCode = 'NULL',
		'N/A',
		v_DctRiskTypeCode
	) AS o_DctRiskTypeCode,
	-- *INF*: IIF(ISNULL(v_DctCoverageTypeCode) OR v_DctCoverageTypeCode='' OR v_DctCoverageTypeCode='NULL', 'N/A', v_DctCoverageTypeCode)
	IFF(v_DctCoverageTypeCode IS NULL 
		OR v_DctCoverageTypeCode = '' 
		OR v_DctCoverageTypeCode = 'NULL',
		'N/A',
		v_DctCoverageTypeCode
	) AS o_DctCoverageTypeCode,
	-- *INF*: IIF(ISNULL(v_PmsRiskUnitGroupCode) OR v_PmsRiskUnitGroupCode='' OR v_PmsRiskUnitGroupCode='NULL', 'N/A', v_PmsRiskUnitGroupCode)
	IFF(v_PmsRiskUnitGroupCode IS NULL 
		OR v_PmsRiskUnitGroupCode = '' 
		OR v_PmsRiskUnitGroupCode = 'NULL',
		'N/A',
		v_PmsRiskUnitGroupCode
	) AS o_PmsRiskUnitGroupCode,
	-- *INF*: IIF(ISNULL(v_PmsRiskUnitCode) OR v_PmsRiskUnitCode='' OR v_PmsRiskUnitCode='NULL', 'N/A', v_PmsRiskUnitCode)
	IFF(v_PmsRiskUnitCode IS NULL 
		OR v_PmsRiskUnitCode = '' 
		OR v_PmsRiskUnitCode = 'NULL',
		'N/A',
		v_PmsRiskUnitCode
	) AS o_PmsRiskUnitCode,
	-- *INF*: IIF(ISNULL(v_PmsMajorPerilCode) OR v_PmsMajorPerilCode='' OR v_PmsMajorPerilCode='NULL', 'N/A', v_PmsMajorPerilCode)
	IFF(v_PmsMajorPerilCode IS NULL 
		OR v_PmsMajorPerilCode = '' 
		OR v_PmsMajorPerilCode = 'NULL',
		'N/A',
		v_PmsMajorPerilCode
	) AS o_PmsMajorPerilCode,
	-- *INF*: IIF(ISNULL(v_PmsProductTypeCode) OR v_PmsProductTypeCode='' OR v_PmsProductTypeCode='NULL', 'N/A', v_PmsProductTypeCode)
	IFF(v_PmsProductTypeCode IS NULL 
		OR v_PmsProductTypeCode = '' 
		OR v_PmsProductTypeCode = 'NULL',
		'N/A',
		v_PmsProductTypeCode
	) AS o_PmsProductTypeCode,
	-- *INF*: IIF(ISNULL(v_DctPerilGroup) OR v_DctPerilGroup='' OR v_DctPerilGroup='NULL', 'N/A', v_DctPerilGroup)
	IFF(v_DctPerilGroup IS NULL 
		OR v_DctPerilGroup = '' 
		OR v_DctPerilGroup = 'NULL',
		'N/A',
		v_DctPerilGroup
	) AS o_DctPerilGroup,
	-- *INF*: IIF(ISNULL(v_DctSubCoverageTypeCode) OR v_DctSubCoverageTypeCode='' OR v_DctSubCoverageTypeCode='NULL', 'N/A', v_DctSubCoverageTypeCode)
	IFF(v_DctSubCoverageTypeCode IS NULL 
		OR v_DctSubCoverageTypeCode = '' 
		OR v_DctSubCoverageTypeCode = 'NULL',
		'N/A',
		v_DctSubCoverageTypeCode
	) AS o_DctSubCoverageTypeCode,
	-- *INF*: IIF(ISNULL(v_DctCoverageVersion) OR v_DctCoverageVersion='' OR v_DctCoverageVersion='NULL', 'N/A', v_DctCoverageVersion)
	IFF(v_DctCoverageVersion IS NULL 
		OR v_DctCoverageVersion = '' 
		OR v_DctCoverageVersion = 'NULL',
		'N/A',
		v_DctCoverageVersion
	) AS o_DctCoverageVersion
	FROM SQ_DCTASLCoverageCombinations
),
LKP_AnnualStatementLine AS (
	SELECT
	AnnualStatementLineId,
	AnnualStatementLineCode,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode
	FROM (
		SELECT 
			AnnualStatementLineId,
			AnnualStatementLineCode,
			SubAnnualStatementLineCode,
			SubNonAnnualStatementLineCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AnnualStatementLine
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AnnualStatementLineCode,SubAnnualStatementLineCode,SubNonAnnualStatementLineCode ORDER BY AnnualStatementLineId) = 1
),
LKP_SystemCoverageId AS (
	SELECT
	SystemCoverageId,
	CoverageSummaryCode,
	CoverageGroupCode,
	CoverageCode,
	InsuranceLineCode,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	PmsRiskUnitGroupCode,
	PmsRiskUnitCode,
	PmsMajorPerilCode,
	PmsProductTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	DctCoverageVersion
	FROM (
		select SC.SystemCoverageId as SystemCoverageId,
		CS.CoverageSummaryCode as CoverageSummaryCode,
		CG.CoverageGroupCode as CoverageGroupCode,
		CC.CoverageCode as CoverageCode,
		SC.InsuranceLineCode as InsuranceLineCode,
		SC.DctRiskTypeCode as DctRiskTypeCode,
		SC.DctCoverageTypeCode as DctCoverageTypeCode,
		SC.PmsRiskUnitGroupCode as PmsRiskUnitGroupCode,
		SC.PmsRiskUnitCode as PmsRiskUnitCode,
		SC.PmsMajorPerilCode as PmsMajorPerilCode,
		SC.PmsProductTypeCode as PmsProductTypeCode,
		SC.DctPerilGroup as DctPerilGroup,
		SC.DctSubCoverageTypeCode as DctSubCoverageTypeCode,
		SC.DctCoverageVersion as DctCoverageVersion
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage  SC
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage CC
		on SC.ConformedCoverageId = CC.ConformedcoverageId
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup CG
		on CC.CoverageGroupId = CG.CoverageGroupId
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary CS 
		on CG.CoverageSummaryId = CS.CoverageSummaryId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageSummaryCode,CoverageGroupCode,CoverageCode,InsuranceLineCode,DctRiskTypeCode,DctCoverageTypeCode,PmsRiskUnitGroupCode,PmsRiskUnitCode,PmsMajorPerilCode,PmsProductTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY SystemCoverageId DESC) = 1
),
RTR_InvalidCombinations AS (
	SELECT
	LKP_SystemCoverageId.SystemCoverageId,
	LKP_AnnualStatementLine.AnnualStatementLineId,
	EXP_Default.o_ASLCode AS ASLCode,
	EXP_Default.o_SubASLCode AS SubASLCode,
	EXP_Default.o_NonSubASLCode AS NonSubASLCode,
	EXP_Default.o_InsuranceLineCode AS InsuranceLineCode,
	EXP_Default.o_CoverageCode AS CoverageCode,
	EXP_Default.o_RatedCoverageCode AS RatedCoverageCode,
	EXP_Default.o_CoverageGroupCode AS CoverageGroupCode,
	EXP_Default.o_CoverageSummaryCode AS CoverageSummaryCode,
	EXP_Default.o_DctRiskTypeCode AS DctRiskTypeCode,
	EXP_Default.o_DctCoverageTypeCode AS DctCoverageTypeCode,
	EXP_Default.o_PmsRiskUnitGroupCode AS PmsRiskUnitGroupCode,
	EXP_Default.o_PmsRiskUnitCode AS PmsRiskUnitCode,
	EXP_Default.o_PmsMajorPerilCode AS PmsMajorPerilCode,
	EXP_Default.o_PmsProductTypeCode AS PmsProductTypeCode,
	EXP_Default.o_DctPerilGroup AS DctPerilGroup,
	EXP_Default.o_DctSubCoverageTypeCode AS DctSubCoverageTypeCode,
	EXP_Default.o_DctCoverageVersion AS DctCoverageVersion
	FROM EXP_Default
	LEFT JOIN LKP_AnnualStatementLine
	ON LKP_AnnualStatementLine.AnnualStatementLineCode = EXP_Default.o_ASLCode AND LKP_AnnualStatementLine.SubAnnualStatementLineCode = EXP_Default.o_SubASLCode AND LKP_AnnualStatementLine.SubNonAnnualStatementLineCode = EXP_Default.o_NonSubASLCode
	LEFT JOIN LKP_SystemCoverageId
	ON LKP_SystemCoverageId.CoverageSummaryCode = EXP_Default.o_CoverageSummaryCode AND LKP_SystemCoverageId.CoverageGroupCode = EXP_Default.o_CoverageGroupCode AND LKP_SystemCoverageId.CoverageCode = EXP_Default.o_CoverageCode AND LKP_SystemCoverageId.InsuranceLineCode = EXP_Default.o_InsuranceLineCode AND LKP_SystemCoverageId.DctRiskTypeCode = EXP_Default.o_DctRiskTypeCode AND LKP_SystemCoverageId.DctCoverageTypeCode = EXP_Default.o_DctCoverageTypeCode AND LKP_SystemCoverageId.PmsRiskUnitGroupCode = EXP_Default.o_PmsRiskUnitGroupCode AND LKP_SystemCoverageId.PmsRiskUnitCode = EXP_Default.o_PmsRiskUnitCode AND LKP_SystemCoverageId.PmsMajorPerilCode = EXP_Default.o_PmsMajorPerilCode AND LKP_SystemCoverageId.PmsProductTypeCode = EXP_Default.o_PmsProductTypeCode AND LKP_SystemCoverageId.DctPerilGroup = EXP_Default.o_DctPerilGroup AND LKP_SystemCoverageId.DctSubCoverageTypeCode = EXP_Default.o_DctSubCoverageTypeCode AND LKP_SystemCoverageId.DctCoverageVersion = EXP_Default.o_DctCoverageVersion
),
RTR_InvalidCombinations_Target AS (SELECT * FROM RTR_InvalidCombinations WHERE NOT ISNULL(SystemCoverageId) AND  NOT ISNULL(AnnualStatementLineId)),
RTR_InvalidCombinations_Records_Having_Issues AS (SELECT * FROM RTR_InvalidCombinations WHERE ISNULL(SystemCoverageId) OR ISNULL(AnnualStatementLineId)),
AGG_RemoveDuplicates AS (
	SELECT
	SystemCoverageId,
	AnnualStatementLineId
	FROM RTR_InvalidCombinations_Target
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SystemCoverageId, AnnualStatementLineId ORDER BY NULL) = 1
),
EXP_Values AS (
	SELECT
	SystemCoverageId,
	AnnualStatementLineId,
	'InformS' AS o_ModifiedUserId,
	CURRENT_TIMESTAMP AS o_ModifiedDate
	FROM AGG_RemoveDuplicates
),
TGT_SupDCTAnnualStatementLineRules AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRules;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRules
	(ModifiedUserId, ModifiedDate, SystemCoverageId, AnnualStatementLineId)
	SELECT 
	o_ModifiedUserId AS MODIFIEDUSERID, 
	o_ModifiedDate AS MODIFIEDDATE, 
	SYSTEMCOVERAGEID, 
	ANNUALSTATEMENTLINEID
	FROM EXP_Values
),
TGT_MissingDCTASLCoverageCombinations AS (
	INSERT INTO MissingDCTASLCoverageCombinations
	(SystemCoverageId, AnnualStatementLineId, ASLCode, SubASLCode, NonSubASLCode, InsuranceLineCode, RatedCoverageCode, CoverageCode, CoverageGroupCode, CoverageSummaryCode, DctRiskTypeCode, DctCoverageTypeCode, PmsRiskUnitGroupCode, PmsRiskUnitCode, PmsMajorPerilCode, PmsProductTypeCode, DctPerilGroup, DctSubCoverageTypeCode, DctCoverageVersion)
	SELECT 
	SYSTEMCOVERAGEID, 
	ANNUALSTATEMENTLINEID, 
	ASLCODE, 
	SUBASLCODE, 
	NONSUBASLCODE, 
	INSURANCELINECODE, 
	RATEDCOVERAGECODE, 
	COVERAGECODE, 
	COVERAGEGROUPCODE, 
	COVERAGESUMMARYCODE, 
	DCTRISKTYPECODE, 
	DCTCOVERAGETYPECODE, 
	PMSRISKUNITGROUPCODE, 
	PMSRISKUNITCODE, 
	PMSMAJORPERILCODE, 
	PMSPRODUCTTYPECODE, 
	DCTPERILGROUP, 
	DCTSUBCOVERAGETYPECODE, 
	DCTCOVERAGEVERSION
	FROM RTR_InvalidCombinations_Records_Having_Issues
),