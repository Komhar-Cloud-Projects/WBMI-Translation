WITH
SQ_PL_Conformed_Coverage_MasterFile AS (

-- TODO Manual --

),
EXP_Src_DataCollect AS (
	SELECT
	Source_System,
	Product_Code,
	Product_Description,
	Coverage_Summary_Code,
	Coverage_Summary_Description,
	Coverage_Group_Code,
	Coverage_Group_Description,
	Coverage_Code,
	Coverage_Description,
	Coverage_Code_ACORD,
	Rated_Coverage_Code,
	Rated_Coverage_Description,
	Annual_Statement_Line_Code,
	Annual_Statement_Line_Code_Description,
	Sub_Annual_Statement_Line_Code,
	Sub_Annual_Statement_Line_Code_Description,
	Sub_Non_Annual_Statement_Line_Code,
	Sub_Non_Annual_Statement_Line_Code_Description,
	-- *INF*: IIF(
	-- (ISNULL(Source_System) or IS_SPACES(Source_System)  or LENGTH(Source_System)=0)
	-- AND (ISNULL(Product_Code) or IS_SPACES(Product_Code)  or LENGTH(Product_Code)=0)
	-- AND (ISNULL(Product_Description) or IS_SPACES(Product_Description)  or LENGTH(Product_Description)=0)
	-- AND (ISNULL(Coverage_Summary_Code) or IS_SPACES(Coverage_Summary_Code)  or LENGTH(Coverage_Summary_Code)=0)
	-- AND (ISNULL(Coverage_Summary_Description) or IS_SPACES(Coverage_Summary_Description)  or LENGTH(Coverage_Summary_Description)=0)
	-- AND (ISNULL(Coverage_Group_Code) or IS_SPACES(Coverage_Group_Code)  or LENGTH(Coverage_Group_Code)=0)
	-- AND (ISNULL(Coverage_Group_Description) or IS_SPACES(Coverage_Group_Description)  or LENGTH(Coverage_Group_Description)=0)
	-- AND (ISNULL(Coverage_Code) or IS_SPACES(Coverage_Code)  or LENGTH(Coverage_Code)=0)
	-- AND (ISNULL(Coverage_Description) or IS_SPACES(Coverage_Description)  or LENGTH(Coverage_Description)=0)
	-- AND (ISNULL(Coverage_Code_ACORD) or IS_SPACES(Coverage_Code_ACORD)  or LENGTH(Coverage_Code_ACORD)=0)
	-- AND (ISNULL(Rated_Coverage_Code) or IS_SPACES(Rated_Coverage_Code)  or LENGTH(Rated_Coverage_Code)=0)
	-- AND (ISNULL(Rated_Coverage_Description) or IS_SPACES(Rated_Coverage_Description)  or LENGTH(Rated_Coverage_Description)=0)
	-- AND (ISNULL(Annual_Statement_Line_Code) or IS_SPACES(Annual_Statement_Line_Code)  or LENGTH(Annual_Statement_Line_Code)=0)
	-- AND (ISNULL(Annual_Statement_Line_Code_Description) or IS_SPACES(Annual_Statement_Line_Code_Description)  or LENGTH(Annual_Statement_Line_Code_Description)=0)
	-- AND (ISNULL(Sub_Annual_Statement_Line_Code) or IS_SPACES(Sub_Annual_Statement_Line_Code)  or LENGTH(Sub_Annual_Statement_Line_Code)=0)
	-- AND (ISNULL(Sub_Annual_Statement_Line_Code_Description) or IS_SPACES(Sub_Annual_Statement_Line_Code_Description)  or LENGTH(Sub_Annual_Statement_Line_Code_Description)=0)
	-- AND (ISNULL(Sub_Non_Annual_Statement_Line_Code) or IS_SPACES(Sub_Non_Annual_Statement_Line_Code)  or LENGTH(Sub_Non_Annual_Statement_Line_Code)=0)
	-- AND (ISNULL(Sub_Non_Annual_Statement_Line_Code_Description) or IS_SPACES(Sub_Non_Annual_Statement_Line_Code_Description)  or LENGTH(Sub_Non_Annual_Statement_Line_Code_Description)=0)
	-- ,1,0)
	IFF(( Source_System IS NULL 
			OR LENGTH(Source_System)>0 AND TRIM(Source_System)='' 
			OR LENGTH(Source_System
			) = 0 
		) 
		AND ( Product_Code IS NULL 
			OR LENGTH(Product_Code)>0 AND TRIM(Product_Code)='' 
			OR LENGTH(Product_Code
			) = 0 
		) 
		AND ( Product_Description IS NULL 
			OR LENGTH(Product_Description)>0 AND TRIM(Product_Description)='' 
			OR LENGTH(Product_Description
			) = 0 
		) 
		AND ( Coverage_Summary_Code IS NULL 
			OR LENGTH(Coverage_Summary_Code)>0 AND TRIM(Coverage_Summary_Code)='' 
			OR LENGTH(Coverage_Summary_Code
			) = 0 
		) 
		AND ( Coverage_Summary_Description IS NULL 
			OR LENGTH(Coverage_Summary_Description)>0 AND TRIM(Coverage_Summary_Description)='' 
			OR LENGTH(Coverage_Summary_Description
			) = 0 
		) 
		AND ( Coverage_Group_Code IS NULL 
			OR LENGTH(Coverage_Group_Code)>0 AND TRIM(Coverage_Group_Code)='' 
			OR LENGTH(Coverage_Group_Code
			) = 0 
		) 
		AND ( Coverage_Group_Description IS NULL 
			OR LENGTH(Coverage_Group_Description)>0 AND TRIM(Coverage_Group_Description)='' 
			OR LENGTH(Coverage_Group_Description
			) = 0 
		) 
		AND ( Coverage_Code IS NULL 
			OR LENGTH(Coverage_Code)>0 AND TRIM(Coverage_Code)='' 
			OR LENGTH(Coverage_Code
			) = 0 
		) 
		AND ( Coverage_Description IS NULL 
			OR LENGTH(Coverage_Description)>0 AND TRIM(Coverage_Description)='' 
			OR LENGTH(Coverage_Description
			) = 0 
		) 
		AND ( Coverage_Code_ACORD IS NULL 
			OR LENGTH(Coverage_Code_ACORD)>0 AND TRIM(Coverage_Code_ACORD)='' 
			OR LENGTH(Coverage_Code_ACORD
			) = 0 
		) 
		AND ( Rated_Coverage_Code IS NULL 
			OR LENGTH(Rated_Coverage_Code)>0 AND TRIM(Rated_Coverage_Code)='' 
			OR LENGTH(Rated_Coverage_Code
			) = 0 
		) 
		AND ( Rated_Coverage_Description IS NULL 
			OR LENGTH(Rated_Coverage_Description)>0 AND TRIM(Rated_Coverage_Description)='' 
			OR LENGTH(Rated_Coverage_Description
			) = 0 
		) 
		AND ( Annual_Statement_Line_Code IS NULL 
			OR LENGTH(Annual_Statement_Line_Code)>0 AND TRIM(Annual_Statement_Line_Code)='' 
			OR LENGTH(Annual_Statement_Line_Code
			) = 0 
		) 
		AND ( Annual_Statement_Line_Code_Description IS NULL 
			OR LENGTH(Annual_Statement_Line_Code_Description)>0 AND TRIM(Annual_Statement_Line_Code_Description)='' 
			OR LENGTH(Annual_Statement_Line_Code_Description
			) = 0 
		) 
		AND ( Sub_Annual_Statement_Line_Code IS NULL 
			OR LENGTH(Sub_Annual_Statement_Line_Code)>0 AND TRIM(Sub_Annual_Statement_Line_Code)='' 
			OR LENGTH(Sub_Annual_Statement_Line_Code
			) = 0 
		) 
		AND ( Sub_Annual_Statement_Line_Code_Description IS NULL 
			OR LENGTH(Sub_Annual_Statement_Line_Code_Description)>0 AND TRIM(Sub_Annual_Statement_Line_Code_Description)='' 
			OR LENGTH(Sub_Annual_Statement_Line_Code_Description
			) = 0 
		) 
		AND ( Sub_Non_Annual_Statement_Line_Code IS NULL 
			OR LENGTH(Sub_Non_Annual_Statement_Line_Code)>0 AND TRIM(Sub_Non_Annual_Statement_Line_Code)='' 
			OR LENGTH(Sub_Non_Annual_Statement_Line_Code
			) = 0 
		) 
		AND ( Sub_Non_Annual_Statement_Line_Code_Description IS NULL 
			OR LENGTH(Sub_Non_Annual_Statement_Line_Code_Description)>0 AND TRIM(Sub_Non_Annual_Statement_Line_Code_Description)='' 
			OR LENGTH(Sub_Non_Annual_Statement_Line_Code_Description
			) = 0 
		),
		1,
		0
	) AS o_FilterFlag
	FROM SQ_PL_Conformed_Coverage_MasterFile
),
FIL_EmptyRows AS (
	SELECT
	Source_System, 
	Product_Code, 
	Product_Description, 
	Coverage_Summary_Code, 
	Coverage_Summary_Description, 
	Coverage_Group_Code, 
	Coverage_Group_Description, 
	Coverage_Code, 
	Coverage_Description, 
	Coverage_Code_ACORD, 
	Rated_Coverage_Code, 
	Rated_Coverage_Description, 
	Annual_Statement_Line_Code, 
	Annual_Statement_Line_Code_Description, 
	Sub_Annual_Statement_Line_Code, 
	Sub_Annual_Statement_Line_Code_Description, 
	Sub_Non_Annual_Statement_Line_Code, 
	Sub_Non_Annual_Statement_Line_Code_Description, 
	o_FilterFlag AS FilterFlag
	FROM EXP_Src_DataCollect
	WHERE FilterFlag=0 AND NOT (ISNULL(Coverage_Code_ACORD) or IS_SPACES(Coverage_Code_ACORD)  or LENGTH(Coverage_Code_ACORD)=0)
),
EXP_DataPrep AS (
	SELECT
	2 AS SRT_Order,
	Source_System,
	Product_Code,
	Product_Description,
	Coverage_Summary_Code,
	Coverage_Summary_Description,
	Coverage_Group_Code,
	Coverage_Group_Description,
	Coverage_Code,
	Coverage_Description,
	Coverage_Code_ACORD,
	Rated_Coverage_Code,
	Rated_Coverage_Description,
	Annual_Statement_Line_Code,
	Annual_Statement_Line_Code_Description,
	Sub_Annual_Statement_Line_Code,
	Sub_Annual_Statement_Line_Code_Description,
	Sub_Non_Annual_Statement_Line_Code,
	Sub_Non_Annual_Statement_Line_Code_Description,
	'N/A' AS InsuranceLineCode,
	'N/A' AS InsuranceLineDescription,
	'N/A' AS DctRiskTypeCode,
	'N/A' AS DctPerilGroup,
	'N/A' AS DctCoverageVersion,
	'N/A' AS PmsRiskUnitGroupCode,
	'N/A' AS PmsRiskUnitGroupDescription,
	'N/A' AS PmsRiskUnitCode,
	'N/A' AS PmsRiskUnitDescription,
	'N/A' AS PmsMajorPerilCode,
	'N/A' AS PmsMajorPerilDescription,
	'N/A' AS PmsProductTypeCode,
	'N/A' AS LossHistoryCode,
	'N/A' AS LossHistoryDescription,
	'N/A' AS ISOMajorCrimeGroup
	FROM FIL_EmptyRows
),
SQ_CSV_ConformedCoverage AS (

-- TODO Manual --

),
EXP_Src_DataCollect_CC AS (
	SELECT
	1 AS SRT_order,
	CoverageSummaryCode,
	CoverageSummaryDescription,
	CoverageGroupCode,
	CoverageGroupDescription,
	CoverageCode,
	CoverageDescription,
	RatedCoverageCode,
	RatedCoverageDescription,
	InsuranceLineCode,
	InsuranceLineDescription,
	SourceSystemId,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	DctSubCoverageTypeCode,
	DctPerilGroup,
	DctCoverageVersion,
	PmsRiskUnitGroupCode,
	PmsRiskUnitGroupDescription,
	PmsRiskUnitCode,
	PmsRiskUnitDescription,
	PmsMajorPerilCode,
	PmsMajorPerilDescription,
	PmsProductTypeCode,
	LossHistoryCode,
	LossHistoryDescription,
	ISOMajorCrimeGroup,
	-- *INF*: IIF(
	-- (ISNULL(CoverageSummaryCode) or IS_SPACES(CoverageSummaryCode)  or LENGTH(CoverageSummaryCode)=0)
	-- AND (ISNULL(CoverageSummaryDescription) or IS_SPACES(CoverageSummaryDescription)  or LENGTH(CoverageSummaryDescription)=0)
	-- AND (ISNULL(CoverageGroupCode) or IS_SPACES(CoverageGroupCode)  or LENGTH(CoverageGroupCode)=0)
	-- AND (ISNULL(CoverageGroupDescription) or IS_SPACES(CoverageGroupDescription)  or LENGTH(CoverageGroupDescription)=0)
	-- AND (ISNULL(CoverageCode) or IS_SPACES(CoverageCode)  or LENGTH(CoverageCode)=0)
	-- AND (ISNULL(CoverageDescription) or IS_SPACES(CoverageDescription)  or LENGTH(CoverageDescription)=0)
	-- AND (ISNULL(RatedCoverageCode) or IS_SPACES(RatedCoverageCode)  or LENGTH(RatedCoverageCode)=0)
	-- AND (ISNULL(RatedCoverageDescription) or IS_SPACES(RatedCoverageDescription)  or LENGTH(RatedCoverageDescription)=0)
	-- AND (ISNULL(InsuranceLineCode) or IS_SPACES(InsuranceLineCode)  or LENGTH(InsuranceLineCode)=0)
	-- AND (ISNULL(InsuranceLineDescription) or IS_SPACES(InsuranceLineDescription)  or LENGTH(InsuranceLineDescription)=0)
	-- AND (ISNULL(SourceSystemId) or IS_SPACES(SourceSystemId)  or LENGTH(SourceSystemId)=0)
	-- AND (ISNULL(DctRiskTypeCode) or IS_SPACES(DctRiskTypeCode)  or LENGTH(DctRiskTypeCode)=0)
	-- AND (ISNULL(DctCoverageTypeCode) or IS_SPACES(DctCoverageTypeCode)  or LENGTH(DctCoverageTypeCode)=0)
	-- AND (ISNULL(DctSubCoverageTypeCode) or IS_SPACES(DctSubCoverageTypeCode)  or LENGTH(DctSubCoverageTypeCode)=0)
	-- AND (ISNULL(DctPerilGroup) or IS_SPACES(DctPerilGroup)  or LENGTH(DctPerilGroup)=0)
	-- AND (ISNULL(DctCoverageVersion) or IS_SPACES(DctCoverageVersion)  or LENGTH(DctCoverageVersion)=0)
	-- AND (ISNULL(PmsRiskUnitGroupCode) or IS_SPACES(PmsRiskUnitGroupCode)  or LENGTH(PmsRiskUnitGroupCode)=0)
	-- AND (ISNULL(PmsRiskUnitGroupDescription) or IS_SPACES(PmsRiskUnitGroupDescription)  or LENGTH(PmsRiskUnitGroupDescription)=0)
	-- AND (ISNULL(PmsRiskUnitCode) or IS_SPACES(PmsRiskUnitCode)  or LENGTH(PmsRiskUnitCode)=0)
	-- AND (ISNULL(PmsRiskUnitDescription) or IS_SPACES(PmsRiskUnitDescription)  or LENGTH(PmsRiskUnitDescription)=0)
	-- AND (ISNULL(PmsMajorPerilCode) or IS_SPACES(PmsMajorPerilCode)  or LENGTH(PmsMajorPerilCode)=0)
	-- AND (ISNULL(PmsMajorPerilDescription) or IS_SPACES(PmsMajorPerilDescription)  or LENGTH(PmsMajorPerilDescription)=0)
	-- AND (ISNULL(PmsProductTypeCode) or IS_SPACES(PmsProductTypeCode)  or LENGTH(PmsProductTypeCode)=0)
	-- AND (ISNULL(LossHistoryCode) or IS_SPACES(LossHistoryCode)  or LENGTH(LossHistoryCode)=0)
	-- AND (ISNULL(LossHistoryDescription) or IS_SPACES(LossHistoryDescription)  or LENGTH(LossHistoryDescription)=0)
	-- AND (ISNULL(ISOMajorCrimeGroup) or IS_SPACES(ISOMajorCrimeGroup)  or LENGTH(ISOMajorCrimeGroup)=0)
	-- ,1,0)
	IFF(( CoverageSummaryCode IS NULL 
			OR LENGTH(CoverageSummaryCode)>0 AND TRIM(CoverageSummaryCode)='' 
			OR LENGTH(CoverageSummaryCode
			) = 0 
		) 
		AND ( CoverageSummaryDescription IS NULL 
			OR LENGTH(CoverageSummaryDescription)>0 AND TRIM(CoverageSummaryDescription)='' 
			OR LENGTH(CoverageSummaryDescription
			) = 0 
		) 
		AND ( CoverageGroupCode IS NULL 
			OR LENGTH(CoverageGroupCode)>0 AND TRIM(CoverageGroupCode)='' 
			OR LENGTH(CoverageGroupCode
			) = 0 
		) 
		AND ( CoverageGroupDescription IS NULL 
			OR LENGTH(CoverageGroupDescription)>0 AND TRIM(CoverageGroupDescription)='' 
			OR LENGTH(CoverageGroupDescription
			) = 0 
		) 
		AND ( CoverageCode IS NULL 
			OR LENGTH(CoverageCode)>0 AND TRIM(CoverageCode)='' 
			OR LENGTH(CoverageCode
			) = 0 
		) 
		AND ( CoverageDescription IS NULL 
			OR LENGTH(CoverageDescription)>0 AND TRIM(CoverageDescription)='' 
			OR LENGTH(CoverageDescription
			) = 0 
		) 
		AND ( RatedCoverageCode IS NULL 
			OR LENGTH(RatedCoverageCode)>0 AND TRIM(RatedCoverageCode)='' 
			OR LENGTH(RatedCoverageCode
			) = 0 
		) 
		AND ( RatedCoverageDescription IS NULL 
			OR LENGTH(RatedCoverageDescription)>0 AND TRIM(RatedCoverageDescription)='' 
			OR LENGTH(RatedCoverageDescription
			) = 0 
		) 
		AND ( InsuranceLineCode IS NULL 
			OR LENGTH(InsuranceLineCode)>0 AND TRIM(InsuranceLineCode)='' 
			OR LENGTH(InsuranceLineCode
			) = 0 
		) 
		AND ( InsuranceLineDescription IS NULL 
			OR LENGTH(InsuranceLineDescription)>0 AND TRIM(InsuranceLineDescription)='' 
			OR LENGTH(InsuranceLineDescription
			) = 0 
		) 
		AND ( SourceSystemId IS NULL 
			OR LENGTH(SourceSystemId)>0 AND TRIM(SourceSystemId)='' 
			OR LENGTH(SourceSystemId
			) = 0 
		) 
		AND ( DctRiskTypeCode IS NULL 
			OR LENGTH(DctRiskTypeCode)>0 AND TRIM(DctRiskTypeCode)='' 
			OR LENGTH(DctRiskTypeCode
			) = 0 
		) 
		AND ( DctCoverageTypeCode IS NULL 
			OR LENGTH(DctCoverageTypeCode)>0 AND TRIM(DctCoverageTypeCode)='' 
			OR LENGTH(DctCoverageTypeCode
			) = 0 
		) 
		AND ( DctSubCoverageTypeCode IS NULL 
			OR LENGTH(DctSubCoverageTypeCode)>0 AND TRIM(DctSubCoverageTypeCode)='' 
			OR LENGTH(DctSubCoverageTypeCode
			) = 0 
		) 
		AND ( DctPerilGroup IS NULL 
			OR LENGTH(DctPerilGroup)>0 AND TRIM(DctPerilGroup)='' 
			OR LENGTH(DctPerilGroup
			) = 0 
		) 
		AND ( DctCoverageVersion IS NULL 
			OR LENGTH(DctCoverageVersion)>0 AND TRIM(DctCoverageVersion)='' 
			OR LENGTH(DctCoverageVersion
			) = 0 
		) 
		AND ( PmsRiskUnitGroupCode IS NULL 
			OR LENGTH(PmsRiskUnitGroupCode)>0 AND TRIM(PmsRiskUnitGroupCode)='' 
			OR LENGTH(PmsRiskUnitGroupCode
			) = 0 
		) 
		AND ( PmsRiskUnitGroupDescription IS NULL 
			OR LENGTH(PmsRiskUnitGroupDescription)>0 AND TRIM(PmsRiskUnitGroupDescription)='' 
			OR LENGTH(PmsRiskUnitGroupDescription
			) = 0 
		) 
		AND ( PmsRiskUnitCode IS NULL 
			OR LENGTH(PmsRiskUnitCode)>0 AND TRIM(PmsRiskUnitCode)='' 
			OR LENGTH(PmsRiskUnitCode
			) = 0 
		) 
		AND ( PmsRiskUnitDescription IS NULL 
			OR LENGTH(PmsRiskUnitDescription)>0 AND TRIM(PmsRiskUnitDescription)='' 
			OR LENGTH(PmsRiskUnitDescription
			) = 0 
		) 
		AND ( PmsMajorPerilCode IS NULL 
			OR LENGTH(PmsMajorPerilCode)>0 AND TRIM(PmsMajorPerilCode)='' 
			OR LENGTH(PmsMajorPerilCode
			) = 0 
		) 
		AND ( PmsMajorPerilDescription IS NULL 
			OR LENGTH(PmsMajorPerilDescription)>0 AND TRIM(PmsMajorPerilDescription)='' 
			OR LENGTH(PmsMajorPerilDescription
			) = 0 
		) 
		AND ( PmsProductTypeCode IS NULL 
			OR LENGTH(PmsProductTypeCode)>0 AND TRIM(PmsProductTypeCode)='' 
			OR LENGTH(PmsProductTypeCode
			) = 0 
		) 
		AND ( LossHistoryCode IS NULL 
			OR LENGTH(LossHistoryCode)>0 AND TRIM(LossHistoryCode)='' 
			OR LENGTH(LossHistoryCode
			) = 0 
		) 
		AND ( LossHistoryDescription IS NULL 
			OR LENGTH(LossHistoryDescription)>0 AND TRIM(LossHistoryDescription)='' 
			OR LENGTH(LossHistoryDescription
			) = 0 
		) 
		AND ( ISOMajorCrimeGroup IS NULL 
			OR LENGTH(ISOMajorCrimeGroup)>0 AND TRIM(ISOMajorCrimeGroup)='' 
			OR LENGTH(ISOMajorCrimeGroup
			) = 0 
		),
		1,
		0
	) AS o_FilterFlag
	FROM SQ_CSV_ConformedCoverage
),
FIL_CC_Eliminate_EmptyRows AS (
	SELECT
	SRT_order, 
	CoverageSummaryCode, 
	CoverageSummaryDescription, 
	CoverageGroupCode, 
	CoverageGroupDescription, 
	CoverageCode, 
	CoverageDescription, 
	RatedCoverageCode, 
	RatedCoverageDescription, 
	InsuranceLineCode, 
	InsuranceLineDescription, 
	SourceSystemId, 
	DctRiskTypeCode, 
	DctCoverageTypeCode, 
	DctSubCoverageTypeCode, 
	DctPerilGroup, 
	DctCoverageVersion, 
	PmsRiskUnitGroupCode, 
	PmsRiskUnitGroupDescription, 
	PmsRiskUnitCode, 
	PmsRiskUnitDescription, 
	PmsMajorPerilCode, 
	PmsMajorPerilDescription, 
	PmsProductTypeCode, 
	LossHistoryCode, 
	LossHistoryDescription, 
	ISOMajorCrimeGroup, 
	o_FilterFlag AS FilterFlag
	FROM EXP_Src_DataCollect_CC
	WHERE FilterFlag=0
),
UN_ConformedCoverage AS (
	SELECT CoverageSummaryCode, CoverageSummaryDescription, CoverageGroupCode, CoverageGroupDescription, CoverageCode, CoverageDescription, RatedCoverageCode, RatedCoverageDescription, InsuranceLineCode, InsuranceLineDescription, SourceSystemId, DctRiskTypeCode, DctCoverageTypeCode, DctSubCoverageTypeCode, DctPerilGroup, DctCoverageVersion, PmsRiskUnitGroupCode, PmsRiskUnitGroupDescription, PmsRiskUnitCode, PmsRiskUnitDescription, PmsMajorPerilCode, PmsMajorPerilDescription, PmsProductTypeCode, LossHistoryCode, LossHistoryDescription, ISOMajorCrimeGroup, SRT_order AS SRT_Order
	FROM FIL_CC_Eliminate_EmptyRows
	UNION
	SELECT Coverage_Summary_Code AS CoverageSummaryCode, Coverage_Summary_Description AS CoverageSummaryDescription, Coverage_Group_Code AS CoverageGroupCode, Coverage_Group_Description AS CoverageGroupDescription, Coverage_Code AS CoverageCode, Coverage_Description AS CoverageDescription, Rated_Coverage_Code AS RatedCoverageCode, Rated_Coverage_Description AS RatedCoverageDescription, InsuranceLineCode, InsuranceLineDescription, Source_System AS SourceSystemId, DctRiskTypeCode, Coverage_Code_ACORD AS DctCoverageTypeCode, Rated_Coverage_Code AS DctSubCoverageTypeCode, DctPerilGroup, DctCoverageVersion, PmsRiskUnitGroupCode, PmsRiskUnitGroupDescription, PmsRiskUnitCode, PmsRiskUnitDescription, PmsMajorPerilCode, PmsMajorPerilDescription, PmsProductTypeCode, LossHistoryCode, LossHistoryDescription, ISOMajorCrimeGroup, SRT_Order
	FROM EXP_DataPrep
),
SRT_Distinct_ConformedCoverage AS (
	SELECT
	SRT_Order, 
	CoverageSummaryCode, 
	CoverageSummaryDescription, 
	CoverageGroupCode, 
	CoverageGroupDescription, 
	CoverageCode, 
	CoverageDescription, 
	RatedCoverageCode, 
	RatedCoverageDescription, 
	InsuranceLineCode, 
	InsuranceLineDescription, 
	SourceSystemId, 
	DctRiskTypeCode, 
	DctCoverageTypeCode, 
	DctSubCoverageTypeCode, 
	DctPerilGroup, 
	DctCoverageVersion, 
	PmsRiskUnitGroupCode, 
	PmsRiskUnitGroupDescription, 
	PmsRiskUnitCode, 
	PmsRiskUnitDescription, 
	PmsMajorPerilCode, 
	PmsMajorPerilDescription, 
	PmsProductTypeCode, 
	LossHistoryCode, 
	LossHistoryDescription, 
	ISOMajorCrimeGroup
	FROM UN_ConformedCoverage
	ORDER BY SRT_Order ASC, CoverageSummaryCode ASC, CoverageSummaryDescription ASC, CoverageGroupCode ASC, CoverageGroupDescription ASC, CoverageCode ASC, CoverageDescription ASC, RatedCoverageCode ASC, RatedCoverageDescription ASC, InsuranceLineCode ASC, InsuranceLineDescription ASC, SourceSystemId ASC, DctRiskTypeCode ASC, DctCoverageTypeCode ASC, DctSubCoverageTypeCode ASC, DctPerilGroup ASC, DctCoverageVersion ASC, PmsRiskUnitGroupCode ASC, PmsRiskUnitGroupDescription ASC, PmsRiskUnitCode ASC, PmsRiskUnitDescription ASC, PmsMajorPerilCode ASC, PmsMajorPerilDescription ASC, PmsProductTypeCode ASC, LossHistoryCode ASC, LossHistoryDescription ASC, ISOMajorCrimeGroup ASC
),
EXP_Tgt_DataCollect_CC AS (
	SELECT
	CoverageSummaryCode,
	CoverageSummaryDescription,
	CoverageGroupCode,
	CoverageGroupDescription,
	CoverageCode,
	CoverageDescription,
	RatedCoverageCode,
	RatedCoverageDescription,
	InsuranceLineCode,
	InsuranceLineDescription,
	SourceSystemId,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	DctSubCoverageTypeCode,
	DctPerilGroup,
	DctCoverageVersion,
	PmsRiskUnitGroupCode,
	PmsRiskUnitGroupDescription,
	PmsRiskUnitCode,
	PmsRiskUnitDescription,
	PmsMajorPerilCode,
	PmsMajorPerilDescription,
	PmsProductTypeCode,
	LossHistoryCode,
	LossHistoryDescription,
	ISOMajorCrimeGroup
	FROM SRT_Distinct_ConformedCoverage
),
CSV_ConformedCoverage AS (
	INSERT INTO CSV_ConformedCoverage
	(CoverageSummaryCode, CoverageSummaryDescription, CoverageGroupCode, CoverageGroupDescription, CoverageCode, CoverageDescription, RatedCoverageCode, RatedCoverageDescription, InsuranceLineCode, InsuranceLineDescription, SourceSystemId, DctRiskTypeCode, DctCoverageTypeCode, DctSubCoverageTypeCode, DctPerilGroup, DctCoverageVersion, PmsRiskUnitGroupCode, PmsRiskUnitGroupDescription, PmsRiskUnitCode, PmsRiskUnitDescription, PmsMajorPerilCode, PmsMajorPerilDescription, PmsProductTypeCode, LossHistoryCode, LossHistoryDescription, ISOMajorCrimeGroup)
	SELECT 
	COVERAGESUMMARYCODE, 
	COVERAGESUMMARYDESCRIPTION, 
	COVERAGEGROUPCODE, 
	COVERAGEGROUPDESCRIPTION, 
	COVERAGECODE, 
	COVERAGEDESCRIPTION, 
	RATEDCOVERAGECODE, 
	RATEDCOVERAGEDESCRIPTION, 
	INSURANCELINECODE, 
	INSURANCELINEDESCRIPTION, 
	SOURCESYSTEMID, 
	DCTRISKTYPECODE, 
	DCTCOVERAGETYPECODE, 
	DCTSUBCOVERAGETYPECODE, 
	DCTPERILGROUP, 
	DCTCOVERAGEVERSION, 
	PMSRISKUNITGROUPCODE, 
	PMSRISKUNITGROUPDESCRIPTION, 
	PMSRISKUNITCODE, 
	PMSRISKUNITDESCRIPTION, 
	PMSMAJORPERILCODE, 
	PMSMAJORPERILDESCRIPTION, 
	PMSPRODUCTTYPECODE, 
	LOSSHISTORYCODE, 
	LOSSHISTORYDESCRIPTION, 
	ISOMAJORCRIMEGROUP
	FROM EXP_Tgt_DataCollect_CC
),
SQ_DCTASLCoverageCombinations AS (

-- TODO Manual --

),
EXP_Src_DataCollect_ASL AS (
	SELECT
	1 AS SRT_Order,
	ASLCode,
	ASLCodeDescription,
	SubASLCode,
	SubASLCodeDescription,
	NonSubASLCode,
	NonSubASLCodeDescription,
	InsuranceLineCode,
	InsuranceLineDescription,
	RatedCoverageCode,
	RatedCoverageDescription,
	CoverageCode,
	CoverageDescription,
	CoverageGroupCode,
	CoverageGroupDescription,
	CoverageSummaryCode,
	CoverageSummaryDescription,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	PmsRiskUnitGroupCode,
	PmsRiskUnitGroupDescription,
	PmsRiskUnitCode,
	PmsRiskUnitDescription,
	PmsMajorPerilCode,
	PmsMajorPerilDescription,
	PmsProductTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	DctCoverageVersion,
	-- *INF*: IIF(
	-- (ISNULL(ASLCode) or IS_SPACES(ASLCode)  or LENGTH(ASLCode)=0)
	-- AND (ISNULL(ASLCodeDescription) or IS_SPACES(ASLCodeDescription)  or LENGTH(ASLCodeDescription)=0)
	-- AND (ISNULL(SubASLCode) or IS_SPACES(SubASLCode)  or LENGTH(SubASLCode)=0)
	-- AND (ISNULL(SubASLCodeDescription) or IS_SPACES(SubASLCodeDescription)  or LENGTH(SubASLCodeDescription)=0)
	-- AND (ISNULL(NonSubASLCode) or IS_SPACES(NonSubASLCode)  or LENGTH(NonSubASLCode)=0)
	-- AND (ISNULL(NonSubASLCodeDescription) or IS_SPACES(NonSubASLCodeDescription)  or LENGTH(NonSubASLCodeDescription)=0)
	-- AND (ISNULL(InsuranceLineCode) or IS_SPACES(InsuranceLineCode)  or LENGTH(InsuranceLineCode)=0)
	-- AND (ISNULL(InsuranceLineDescription) or IS_SPACES(InsuranceLineDescription)  or LENGTH(InsuranceLineDescription)=0)
	-- AND (ISNULL(RatedCoverageCode) or IS_SPACES(RatedCoverageCode)  or LENGTH(RatedCoverageCode)=0)
	-- AND (ISNULL(RatedCoverageDescription) or IS_SPACES(RatedCoverageDescription)  or LENGTH(RatedCoverageDescription)=0)
	-- AND (ISNULL(CoverageSummaryCode) or IS_SPACES(CoverageSummaryCode)  or LENGTH(CoverageSummaryCode)=0)
	-- AND (ISNULL(CoverageSummaryDescription) or IS_SPACES(CoverageSummaryDescription)  or LENGTH(CoverageSummaryDescription)=0)
	-- AND (ISNULL(CoverageGroupCode) or IS_SPACES(CoverageGroupCode)  or LENGTH(CoverageGroupCode)=0)
	-- AND (ISNULL(CoverageGroupDescription) or IS_SPACES(CoverageGroupDescription)  or LENGTH(CoverageGroupDescription)=0)
	-- AND (ISNULL(CoverageCode) or IS_SPACES(CoverageCode)  or LENGTH(CoverageCode)=0)
	-- AND (ISNULL(CoverageDescription) or IS_SPACES(CoverageDescription)  or LENGTH(CoverageDescription)=0)
	-- AND (ISNULL(DctRiskTypeCode) or IS_SPACES(DctRiskTypeCode)  or LENGTH(DctRiskTypeCode)=0)
	-- AND (ISNULL(DctCoverageTypeCode) or IS_SPACES(DctCoverageTypeCode)  or LENGTH(DctCoverageTypeCode)=0)
	-- AND (ISNULL(DctSubCoverageTypeCode) or IS_SPACES(DctSubCoverageTypeCode)  or LENGTH(DctSubCoverageTypeCode)=0)
	-- AND (ISNULL(DctPerilGroup) or IS_SPACES(DctPerilGroup)  or LENGTH(DctPerilGroup)=0)
	-- AND (ISNULL(DctCoverageVersion) or IS_SPACES(DctCoverageVersion)  or LENGTH(DctCoverageVersion)=0)
	-- AND (ISNULL(PmsRiskUnitGroupCode) or IS_SPACES(PmsRiskUnitGroupCode)  or LENGTH(PmsRiskUnitGroupCode)=0)
	-- AND (ISNULL(PmsRiskUnitGroupDescription) or IS_SPACES(PmsRiskUnitGroupDescription)  or LENGTH(PmsRiskUnitGroupDescription)=0)
	-- AND (ISNULL(PmsRiskUnitCode) or IS_SPACES(PmsRiskUnitCode)  or LENGTH(PmsRiskUnitCode)=0)
	-- AND (ISNULL(PmsRiskUnitDescription) or IS_SPACES(PmsRiskUnitDescription)  or LENGTH(PmsRiskUnitDescription)=0)
	-- AND (ISNULL(PmsMajorPerilCode) or IS_SPACES(PmsMajorPerilCode)  or LENGTH(PmsMajorPerilCode)=0)
	-- AND (ISNULL(PmsMajorPerilDescription) or IS_SPACES(PmsMajorPerilDescription)  or LENGTH(PmsMajorPerilDescription)=0)
	-- AND (ISNULL(PmsProductTypeCode) or IS_SPACES(PmsProductTypeCode)  or LENGTH(PmsProductTypeCode)=0)
	-- ,1,0)
	IFF(( ASLCode IS NULL 
			OR LENGTH(ASLCode)>0 AND TRIM(ASLCode)='' 
			OR LENGTH(ASLCode
			) = 0 
		) 
		AND ( ASLCodeDescription IS NULL 
			OR LENGTH(ASLCodeDescription)>0 AND TRIM(ASLCodeDescription)='' 
			OR LENGTH(ASLCodeDescription
			) = 0 
		) 
		AND ( SubASLCode IS NULL 
			OR LENGTH(SubASLCode)>0 AND TRIM(SubASLCode)='' 
			OR LENGTH(SubASLCode
			) = 0 
		) 
		AND ( SubASLCodeDescription IS NULL 
			OR LENGTH(SubASLCodeDescription)>0 AND TRIM(SubASLCodeDescription)='' 
			OR LENGTH(SubASLCodeDescription
			) = 0 
		) 
		AND ( NonSubASLCode IS NULL 
			OR LENGTH(NonSubASLCode)>0 AND TRIM(NonSubASLCode)='' 
			OR LENGTH(NonSubASLCode
			) = 0 
		) 
		AND ( NonSubASLCodeDescription IS NULL 
			OR LENGTH(NonSubASLCodeDescription)>0 AND TRIM(NonSubASLCodeDescription)='' 
			OR LENGTH(NonSubASLCodeDescription
			) = 0 
		) 
		AND ( InsuranceLineCode IS NULL 
			OR LENGTH(InsuranceLineCode)>0 AND TRIM(InsuranceLineCode)='' 
			OR LENGTH(InsuranceLineCode
			) = 0 
		) 
		AND ( InsuranceLineDescription IS NULL 
			OR LENGTH(InsuranceLineDescription)>0 AND TRIM(InsuranceLineDescription)='' 
			OR LENGTH(InsuranceLineDescription
			) = 0 
		) 
		AND ( RatedCoverageCode IS NULL 
			OR LENGTH(RatedCoverageCode)>0 AND TRIM(RatedCoverageCode)='' 
			OR LENGTH(RatedCoverageCode
			) = 0 
		) 
		AND ( RatedCoverageDescription IS NULL 
			OR LENGTH(RatedCoverageDescription)>0 AND TRIM(RatedCoverageDescription)='' 
			OR LENGTH(RatedCoverageDescription
			) = 0 
		) 
		AND ( CoverageSummaryCode IS NULL 
			OR LENGTH(CoverageSummaryCode)>0 AND TRIM(CoverageSummaryCode)='' 
			OR LENGTH(CoverageSummaryCode
			) = 0 
		) 
		AND ( CoverageSummaryDescription IS NULL 
			OR LENGTH(CoverageSummaryDescription)>0 AND TRIM(CoverageSummaryDescription)='' 
			OR LENGTH(CoverageSummaryDescription
			) = 0 
		) 
		AND ( CoverageGroupCode IS NULL 
			OR LENGTH(CoverageGroupCode)>0 AND TRIM(CoverageGroupCode)='' 
			OR LENGTH(CoverageGroupCode
			) = 0 
		) 
		AND ( CoverageGroupDescription IS NULL 
			OR LENGTH(CoverageGroupDescription)>0 AND TRIM(CoverageGroupDescription)='' 
			OR LENGTH(CoverageGroupDescription
			) = 0 
		) 
		AND ( CoverageCode IS NULL 
			OR LENGTH(CoverageCode)>0 AND TRIM(CoverageCode)='' 
			OR LENGTH(CoverageCode
			) = 0 
		) 
		AND ( CoverageDescription IS NULL 
			OR LENGTH(CoverageDescription)>0 AND TRIM(CoverageDescription)='' 
			OR LENGTH(CoverageDescription
			) = 0 
		) 
		AND ( DctRiskTypeCode IS NULL 
			OR LENGTH(DctRiskTypeCode)>0 AND TRIM(DctRiskTypeCode)='' 
			OR LENGTH(DctRiskTypeCode
			) = 0 
		) 
		AND ( DctCoverageTypeCode IS NULL 
			OR LENGTH(DctCoverageTypeCode)>0 AND TRIM(DctCoverageTypeCode)='' 
			OR LENGTH(DctCoverageTypeCode
			) = 0 
		) 
		AND ( DctSubCoverageTypeCode IS NULL 
			OR LENGTH(DctSubCoverageTypeCode)>0 AND TRIM(DctSubCoverageTypeCode)='' 
			OR LENGTH(DctSubCoverageTypeCode
			) = 0 
		) 
		AND ( DctPerilGroup IS NULL 
			OR LENGTH(DctPerilGroup)>0 AND TRIM(DctPerilGroup)='' 
			OR LENGTH(DctPerilGroup
			) = 0 
		) 
		AND ( DctCoverageVersion IS NULL 
			OR LENGTH(DctCoverageVersion)>0 AND TRIM(DctCoverageVersion)='' 
			OR LENGTH(DctCoverageVersion
			) = 0 
		) 
		AND ( PmsRiskUnitGroupCode IS NULL 
			OR LENGTH(PmsRiskUnitGroupCode)>0 AND TRIM(PmsRiskUnitGroupCode)='' 
			OR LENGTH(PmsRiskUnitGroupCode
			) = 0 
		) 
		AND ( PmsRiskUnitGroupDescription IS NULL 
			OR LENGTH(PmsRiskUnitGroupDescription)>0 AND TRIM(PmsRiskUnitGroupDescription)='' 
			OR LENGTH(PmsRiskUnitGroupDescription
			) = 0 
		) 
		AND ( PmsRiskUnitCode IS NULL 
			OR LENGTH(PmsRiskUnitCode)>0 AND TRIM(PmsRiskUnitCode)='' 
			OR LENGTH(PmsRiskUnitCode
			) = 0 
		) 
		AND ( PmsRiskUnitDescription IS NULL 
			OR LENGTH(PmsRiskUnitDescription)>0 AND TRIM(PmsRiskUnitDescription)='' 
			OR LENGTH(PmsRiskUnitDescription
			) = 0 
		) 
		AND ( PmsMajorPerilCode IS NULL 
			OR LENGTH(PmsMajorPerilCode)>0 AND TRIM(PmsMajorPerilCode)='' 
			OR LENGTH(PmsMajorPerilCode
			) = 0 
		) 
		AND ( PmsMajorPerilDescription IS NULL 
			OR LENGTH(PmsMajorPerilDescription)>0 AND TRIM(PmsMajorPerilDescription)='' 
			OR LENGTH(PmsMajorPerilDescription
			) = 0 
		) 
		AND ( PmsProductTypeCode IS NULL 
			OR LENGTH(PmsProductTypeCode)>0 AND TRIM(PmsProductTypeCode)='' 
			OR LENGTH(PmsProductTypeCode
			) = 0 
		),
		1,
		0
	) AS o_FilterFlag
	FROM SQ_DCTASLCoverageCombinations
),
FIL_ASL_Eliminate_EmptyRows AS (
	SELECT
	SRT_Order, 
	ASLCode, 
	ASLCodeDescription, 
	SubASLCode, 
	SubASLCodeDescription, 
	NonSubASLCode, 
	NonSubASLCodeDescription, 
	InsuranceLineCode, 
	InsuranceLineDescription, 
	RatedCoverageCode, 
	RatedCoverageDescription, 
	CoverageCode, 
	CoverageDescription, 
	CoverageGroupCode, 
	CoverageGroupDescription, 
	CoverageSummaryCode, 
	CoverageSummaryDescription, 
	DctRiskTypeCode, 
	DctCoverageTypeCode, 
	PmsRiskUnitGroupCode, 
	PmsRiskUnitGroupDescription, 
	PmsRiskUnitCode, 
	PmsRiskUnitDescription, 
	PmsMajorPerilCode, 
	PmsMajorPerilDescription, 
	PmsProductTypeCode, 
	DctPerilGroup, 
	DctSubCoverageTypeCode, 
	DctCoverageVersion, 
	o_FilterFlag AS FilterFlag
	FROM EXP_Src_DataCollect_ASL
	WHERE FilterFlag=0
),
UN_ASL AS (
	SELECT SRT_Order, Annual_Statement_Line_Code AS ASLCode, Annual_Statement_Line_Code_Description AS ASLCodeDescription, Sub_Annual_Statement_Line_Code AS SubASLCode, Sub_Annual_Statement_Line_Code_Description AS SubASLCodeDescription, Sub_Non_Annual_Statement_Line_Code AS NonSubASLCode, Sub_Non_Annual_Statement_Line_Code_Description AS NonSubASLCodeDescription, InsuranceLineCode, InsuranceLineDescription, Rated_Coverage_Code AS RatedCoverageCode, Rated_Coverage_Description AS RatedCoverageDescription, Coverage_Code AS CoverageCode, Coverage_Description AS CoverageDescription, Coverage_Group_Code AS CoverageGroupCode, Coverage_Group_Description AS CoverageGroupDescription, Coverage_Summary_Code AS CoverageSummaryCode, Coverage_Summary_Description AS CoverageSummaryDescription, DctRiskTypeCode, Coverage_Code_ACORD AS DctCoverageTypeCode, PmsRiskUnitGroupCode, PmsRiskUnitGroupDescription, PmsRiskUnitCode, PmsRiskUnitDescription, PmsMajorPerilCode, PmsMajorPerilDescription, PmsProductTypeCode, DctPerilGroup, Rated_Coverage_Code AS DctSubCoverageTypeCode, DctCoverageVersion
	FROM EXP_DataPrep
	UNION
	SELECT SRT_Order, ASLCode, ASLCodeDescription, SubASLCode, SubASLCodeDescription, NonSubASLCode, NonSubASLCodeDescription, InsuranceLineCode, InsuranceLineDescription, RatedCoverageCode, RatedCoverageDescription, CoverageCode, CoverageDescription, CoverageGroupCode, CoverageGroupDescription, CoverageSummaryCode, CoverageSummaryDescription, DctRiskTypeCode, DctCoverageTypeCode, PmsRiskUnitGroupCode, PmsRiskUnitGroupDescription, PmsRiskUnitCode, PmsRiskUnitDescription, PmsMajorPerilCode, PmsMajorPerilDescription, PmsProductTypeCode, DctPerilGroup, DctSubCoverageTypeCode, DctCoverageVersion
	FROM FIL_ASL_Eliminate_EmptyRows
),
SRT_Distinct_ASL AS (
	SELECT
	SRT_Order, 
	ASLCode, 
	ASLCodeDescription, 
	SubASLCode, 
	SubASLCodeDescription, 
	NonSubASLCode, 
	NonSubASLCodeDescription, 
	InsuranceLineCode, 
	InsuranceLineDescription, 
	RatedCoverageCode, 
	RatedCoverageDescription, 
	CoverageCode, 
	CoverageDescription, 
	CoverageGroupCode, 
	CoverageGroupDescription, 
	CoverageSummaryCode, 
	CoverageSummaryDescription, 
	DctRiskTypeCode, 
	DctCoverageTypeCode, 
	PmsRiskUnitGroupCode, 
	PmsRiskUnitGroupDescription, 
	PmsRiskUnitCode, 
	PmsRiskUnitDescription, 
	PmsMajorPerilCode, 
	PmsMajorPerilDescription, 
	PmsProductTypeCode, 
	DctPerilGroup, 
	DctSubCoverageTypeCode, 
	DctCoverageVersion
	FROM UN_ASL
	ORDER BY SRT_Order ASC, ASLCode ASC, ASLCodeDescription ASC, SubASLCode ASC, SubASLCodeDescription ASC, NonSubASLCode ASC, NonSubASLCodeDescription ASC, InsuranceLineCode ASC, InsuranceLineDescription ASC, RatedCoverageCode ASC, RatedCoverageDescription ASC, CoverageCode ASC, CoverageDescription ASC, CoverageGroupCode ASC, CoverageGroupDescription ASC, CoverageSummaryCode ASC, CoverageSummaryDescription ASC, DctRiskTypeCode ASC, DctCoverageTypeCode ASC, PmsRiskUnitGroupCode ASC, PmsRiskUnitGroupDescription ASC, PmsRiskUnitCode ASC, PmsRiskUnitDescription ASC, PmsMajorPerilCode ASC, PmsMajorPerilDescription ASC, PmsProductTypeCode ASC, DctPerilGroup ASC, DctSubCoverageTypeCode ASC, DctCoverageVersion ASC
),
EXP_Tgt_DataCollect_ASL AS (
	SELECT
	ASLCode,
	ASLCodeDescription,
	SubASLCode,
	SubASLCodeDescription,
	NonSubASLCode,
	NonSubASLCodeDescription,
	InsuranceLineCode,
	InsuranceLineDescription,
	RatedCoverageCode,
	RatedCoverageDescription,
	CoverageCode,
	CoverageDescription,
	CoverageGroupCode,
	CoverageGroupDescription,
	CoverageSummaryCode,
	CoverageSummaryDescription,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	PmsRiskUnitGroupCode,
	PmsRiskUnitGroupDescription,
	PmsRiskUnitCode,
	PmsRiskUnitDescription,
	PmsMajorPerilCode,
	PmsMajorPerilDescription,
	PmsProductTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	DctCoverageVersion
	FROM SRT_Distinct_ASL
),
DCTASLCoverageCombinations AS (
	INSERT INTO DCTASLCoverageCombinations
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
	FROM EXP_Tgt_DataCollect_ASL
),