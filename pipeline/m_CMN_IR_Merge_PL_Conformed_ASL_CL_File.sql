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
	IFF(
	    (Source_System IS NULL
	    or LENGTH(Source_System)>0
	    and TRIM(Source_System)=''
	    or LENGTH(Source_System) = 0)
	    and (Product_Code IS NULL
	    or LENGTH(Product_Code)>0
	    and TRIM(Product_Code)=''
	    or LENGTH(Product_Code) = 0)
	    and (Product_Description IS NULL
	    or LENGTH(Product_Description)>0
	    and TRIM(Product_Description)=''
	    or LENGTH(Product_Description) = 0)
	    and (Coverage_Summary_Code IS NULL
	    or LENGTH(Coverage_Summary_Code)>0
	    and TRIM(Coverage_Summary_Code)=''
	    or LENGTH(Coverage_Summary_Code) = 0)
	    and (Coverage_Summary_Description IS NULL
	    or LENGTH(Coverage_Summary_Description)>0
	    and TRIM(Coverage_Summary_Description)=''
	    or LENGTH(Coverage_Summary_Description) = 0)
	    and (Coverage_Group_Code IS NULL
	    or LENGTH(Coverage_Group_Code)>0
	    and TRIM(Coverage_Group_Code)=''
	    or LENGTH(Coverage_Group_Code) = 0)
	    and (Coverage_Group_Description IS NULL
	    or LENGTH(Coverage_Group_Description)>0
	    and TRIM(Coverage_Group_Description)=''
	    or LENGTH(Coverage_Group_Description) = 0)
	    and (Coverage_Code IS NULL
	    or LENGTH(Coverage_Code)>0
	    and TRIM(Coverage_Code)=''
	    or LENGTH(Coverage_Code) = 0)
	    and (Coverage_Description IS NULL
	    or LENGTH(Coverage_Description)>0
	    and TRIM(Coverage_Description)=''
	    or LENGTH(Coverage_Description) = 0)
	    and (Coverage_Code_ACORD IS NULL
	    or LENGTH(Coverage_Code_ACORD)>0
	    and TRIM(Coverage_Code_ACORD)=''
	    or LENGTH(Coverage_Code_ACORD) = 0)
	    and (Rated_Coverage_Code IS NULL
	    or LENGTH(Rated_Coverage_Code)>0
	    and TRIM(Rated_Coverage_Code)=''
	    or LENGTH(Rated_Coverage_Code) = 0)
	    and (Rated_Coverage_Description IS NULL
	    or LENGTH(Rated_Coverage_Description)>0
	    and TRIM(Rated_Coverage_Description)=''
	    or LENGTH(Rated_Coverage_Description) = 0)
	    and (Annual_Statement_Line_Code IS NULL
	    or LENGTH(Annual_Statement_Line_Code)>0
	    and TRIM(Annual_Statement_Line_Code)=''
	    or LENGTH(Annual_Statement_Line_Code) = 0)
	    and (Annual_Statement_Line_Code_Description IS NULL
	    or LENGTH(Annual_Statement_Line_Code_Description)>0
	    and TRIM(Annual_Statement_Line_Code_Description)=''
	    or LENGTH(Annual_Statement_Line_Code_Description) = 0)
	    and (Sub_Annual_Statement_Line_Code IS NULL
	    or LENGTH(Sub_Annual_Statement_Line_Code)>0
	    and TRIM(Sub_Annual_Statement_Line_Code)=''
	    or LENGTH(Sub_Annual_Statement_Line_Code) = 0)
	    and (Sub_Annual_Statement_Line_Code_Description IS NULL
	    or LENGTH(Sub_Annual_Statement_Line_Code_Description)>0
	    and TRIM(Sub_Annual_Statement_Line_Code_Description)=''
	    or LENGTH(Sub_Annual_Statement_Line_Code_Description) = 0)
	    and (Sub_Non_Annual_Statement_Line_Code IS NULL
	    or LENGTH(Sub_Non_Annual_Statement_Line_Code)>0
	    and TRIM(Sub_Non_Annual_Statement_Line_Code)=''
	    or LENGTH(Sub_Non_Annual_Statement_Line_Code) = 0)
	    and (Sub_Non_Annual_Statement_Line_Code_Description IS NULL
	    or LENGTH(Sub_Non_Annual_Statement_Line_Code_Description)>0
	    and TRIM(Sub_Non_Annual_Statement_Line_Code_Description)=''
	    or LENGTH(Sub_Non_Annual_Statement_Line_Code_Description) = 0),
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
	IFF(
	    (CoverageSummaryCode IS NULL
	    or LENGTH(CoverageSummaryCode)>0
	    and TRIM(CoverageSummaryCode)=''
	    or LENGTH(CoverageSummaryCode) = 0)
	    and (CoverageSummaryDescription IS NULL
	    or LENGTH(CoverageSummaryDescription)>0
	    and TRIM(CoverageSummaryDescription)=''
	    or LENGTH(CoverageSummaryDescription) = 0)
	    and (CoverageGroupCode IS NULL
	    or LENGTH(CoverageGroupCode)>0
	    and TRIM(CoverageGroupCode)=''
	    or LENGTH(CoverageGroupCode) = 0)
	    and (CoverageGroupDescription IS NULL
	    or LENGTH(CoverageGroupDescription)>0
	    and TRIM(CoverageGroupDescription)=''
	    or LENGTH(CoverageGroupDescription) = 0)
	    and (CoverageCode IS NULL
	    or LENGTH(CoverageCode)>0
	    and TRIM(CoverageCode)=''
	    or LENGTH(CoverageCode) = 0)
	    and (CoverageDescription IS NULL
	    or LENGTH(CoverageDescription)>0
	    and TRIM(CoverageDescription)=''
	    or LENGTH(CoverageDescription) = 0)
	    and (RatedCoverageCode IS NULL
	    or LENGTH(RatedCoverageCode)>0
	    and TRIM(RatedCoverageCode)=''
	    or LENGTH(RatedCoverageCode) = 0)
	    and (RatedCoverageDescription IS NULL
	    or LENGTH(RatedCoverageDescription)>0
	    and TRIM(RatedCoverageDescription)=''
	    or LENGTH(RatedCoverageDescription) = 0)
	    and (InsuranceLineCode IS NULL
	    or LENGTH(InsuranceLineCode)>0
	    and TRIM(InsuranceLineCode)=''
	    or LENGTH(InsuranceLineCode) = 0)
	    and (InsuranceLineDescription IS NULL
	    or LENGTH(InsuranceLineDescription)>0
	    and TRIM(InsuranceLineDescription)=''
	    or LENGTH(InsuranceLineDescription) = 0)
	    and (SourceSystemId IS NULL
	    or LENGTH(SourceSystemId)>0
	    and TRIM(SourceSystemId)=''
	    or LENGTH(SourceSystemId) = 0)
	    and (DctRiskTypeCode IS NULL
	    or LENGTH(DctRiskTypeCode)>0
	    and TRIM(DctRiskTypeCode)=''
	    or LENGTH(DctRiskTypeCode) = 0)
	    and (DctCoverageTypeCode IS NULL
	    or LENGTH(DctCoverageTypeCode)>0
	    and TRIM(DctCoverageTypeCode)=''
	    or LENGTH(DctCoverageTypeCode) = 0)
	    and (DctSubCoverageTypeCode IS NULL
	    or LENGTH(DctSubCoverageTypeCode)>0
	    and TRIM(DctSubCoverageTypeCode)=''
	    or LENGTH(DctSubCoverageTypeCode) = 0)
	    and (DctPerilGroup IS NULL
	    or LENGTH(DctPerilGroup)>0
	    and TRIM(DctPerilGroup)=''
	    or LENGTH(DctPerilGroup) = 0)
	    and (DctCoverageVersion IS NULL
	    or LENGTH(DctCoverageVersion)>0
	    and TRIM(DctCoverageVersion)=''
	    or LENGTH(DctCoverageVersion) = 0)
	    and (PmsRiskUnitGroupCode IS NULL
	    or LENGTH(PmsRiskUnitGroupCode)>0
	    and TRIM(PmsRiskUnitGroupCode)=''
	    or LENGTH(PmsRiskUnitGroupCode) = 0)
	    and (PmsRiskUnitGroupDescription IS NULL
	    or LENGTH(PmsRiskUnitGroupDescription)>0
	    and TRIM(PmsRiskUnitGroupDescription)=''
	    or LENGTH(PmsRiskUnitGroupDescription) = 0)
	    and (PmsRiskUnitCode IS NULL
	    or LENGTH(PmsRiskUnitCode)>0
	    and TRIM(PmsRiskUnitCode)=''
	    or LENGTH(PmsRiskUnitCode) = 0)
	    and (PmsRiskUnitDescription IS NULL
	    or LENGTH(PmsRiskUnitDescription)>0
	    and TRIM(PmsRiskUnitDescription)=''
	    or LENGTH(PmsRiskUnitDescription) = 0)
	    and (PmsMajorPerilCode IS NULL
	    or LENGTH(PmsMajorPerilCode)>0
	    and TRIM(PmsMajorPerilCode)=''
	    or LENGTH(PmsMajorPerilCode) = 0)
	    and (PmsMajorPerilDescription IS NULL
	    or LENGTH(PmsMajorPerilDescription)>0
	    and TRIM(PmsMajorPerilDescription)=''
	    or LENGTH(PmsMajorPerilDescription) = 0)
	    and (PmsProductTypeCode IS NULL
	    or LENGTH(PmsProductTypeCode)>0
	    and TRIM(PmsProductTypeCode)=''
	    or LENGTH(PmsProductTypeCode) = 0)
	    and (LossHistoryCode IS NULL
	    or LENGTH(LossHistoryCode)>0
	    and TRIM(LossHistoryCode)=''
	    or LENGTH(LossHistoryCode) = 0)
	    and (LossHistoryDescription IS NULL
	    or LENGTH(LossHistoryDescription)>0
	    and TRIM(LossHistoryDescription)=''
	    or LENGTH(LossHistoryDescription) = 0)
	    and (ISOMajorCrimeGroup IS NULL
	    or LENGTH(ISOMajorCrimeGroup)>0
	    and TRIM(ISOMajorCrimeGroup)=''
	    or LENGTH(ISOMajorCrimeGroup) = 0),
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
	IFF(
	    (ASLCode IS NULL
	    or LENGTH(ASLCode)>0
	    and TRIM(ASLCode)=''
	    or LENGTH(ASLCode) = 0)
	    and (ASLCodeDescription IS NULL
	    or LENGTH(ASLCodeDescription)>0
	    and TRIM(ASLCodeDescription)=''
	    or LENGTH(ASLCodeDescription) = 0)
	    and (SubASLCode IS NULL
	    or LENGTH(SubASLCode)>0
	    and TRIM(SubASLCode)=''
	    or LENGTH(SubASLCode) = 0)
	    and (SubASLCodeDescription IS NULL
	    or LENGTH(SubASLCodeDescription)>0
	    and TRIM(SubASLCodeDescription)=''
	    or LENGTH(SubASLCodeDescription) = 0)
	    and (NonSubASLCode IS NULL
	    or LENGTH(NonSubASLCode)>0
	    and TRIM(NonSubASLCode)=''
	    or LENGTH(NonSubASLCode) = 0)
	    and (NonSubASLCodeDescription IS NULL
	    or LENGTH(NonSubASLCodeDescription)>0
	    and TRIM(NonSubASLCodeDescription)=''
	    or LENGTH(NonSubASLCodeDescription) = 0)
	    and (InsuranceLineCode IS NULL
	    or LENGTH(InsuranceLineCode)>0
	    and TRIM(InsuranceLineCode)=''
	    or LENGTH(InsuranceLineCode) = 0)
	    and (InsuranceLineDescription IS NULL
	    or LENGTH(InsuranceLineDescription)>0
	    and TRIM(InsuranceLineDescription)=''
	    or LENGTH(InsuranceLineDescription) = 0)
	    and (RatedCoverageCode IS NULL
	    or LENGTH(RatedCoverageCode)>0
	    and TRIM(RatedCoverageCode)=''
	    or LENGTH(RatedCoverageCode) = 0)
	    and (RatedCoverageDescription IS NULL
	    or LENGTH(RatedCoverageDescription)>0
	    and TRIM(RatedCoverageDescription)=''
	    or LENGTH(RatedCoverageDescription) = 0)
	    and (CoverageSummaryCode IS NULL
	    or LENGTH(CoverageSummaryCode)>0
	    and TRIM(CoverageSummaryCode)=''
	    or LENGTH(CoverageSummaryCode) = 0)
	    and (CoverageSummaryDescription IS NULL
	    or LENGTH(CoverageSummaryDescription)>0
	    and TRIM(CoverageSummaryDescription)=''
	    or LENGTH(CoverageSummaryDescription) = 0)
	    and (CoverageGroupCode IS NULL
	    or LENGTH(CoverageGroupCode)>0
	    and TRIM(CoverageGroupCode)=''
	    or LENGTH(CoverageGroupCode) = 0)
	    and (CoverageGroupDescription IS NULL
	    or LENGTH(CoverageGroupDescription)>0
	    and TRIM(CoverageGroupDescription)=''
	    or LENGTH(CoverageGroupDescription) = 0)
	    and (CoverageCode IS NULL
	    or LENGTH(CoverageCode)>0
	    and TRIM(CoverageCode)=''
	    or LENGTH(CoverageCode) = 0)
	    and (CoverageDescription IS NULL
	    or LENGTH(CoverageDescription)>0
	    and TRIM(CoverageDescription)=''
	    or LENGTH(CoverageDescription) = 0)
	    and (DctRiskTypeCode IS NULL
	    or LENGTH(DctRiskTypeCode)>0
	    and TRIM(DctRiskTypeCode)=''
	    or LENGTH(DctRiskTypeCode) = 0)
	    and (DctCoverageTypeCode IS NULL
	    or LENGTH(DctCoverageTypeCode)>0
	    and TRIM(DctCoverageTypeCode)=''
	    or LENGTH(DctCoverageTypeCode) = 0)
	    and (DctSubCoverageTypeCode IS NULL
	    or LENGTH(DctSubCoverageTypeCode)>0
	    and TRIM(DctSubCoverageTypeCode)=''
	    or LENGTH(DctSubCoverageTypeCode) = 0)
	    and (DctPerilGroup IS NULL
	    or LENGTH(DctPerilGroup)>0
	    and TRIM(DctPerilGroup)=''
	    or LENGTH(DctPerilGroup) = 0)
	    and (DctCoverageVersion IS NULL
	    or LENGTH(DctCoverageVersion)>0
	    and TRIM(DctCoverageVersion)=''
	    or LENGTH(DctCoverageVersion) = 0)
	    and (PmsRiskUnitGroupCode IS NULL
	    or LENGTH(PmsRiskUnitGroupCode)>0
	    and TRIM(PmsRiskUnitGroupCode)=''
	    or LENGTH(PmsRiskUnitGroupCode) = 0)
	    and (PmsRiskUnitGroupDescription IS NULL
	    or LENGTH(PmsRiskUnitGroupDescription)>0
	    and TRIM(PmsRiskUnitGroupDescription)=''
	    or LENGTH(PmsRiskUnitGroupDescription) = 0)
	    and (PmsRiskUnitCode IS NULL
	    or LENGTH(PmsRiskUnitCode)>0
	    and TRIM(PmsRiskUnitCode)=''
	    or LENGTH(PmsRiskUnitCode) = 0)
	    and (PmsRiskUnitDescription IS NULL
	    or LENGTH(PmsRiskUnitDescription)>0
	    and TRIM(PmsRiskUnitDescription)=''
	    or LENGTH(PmsRiskUnitDescription) = 0)
	    and (PmsMajorPerilCode IS NULL
	    or LENGTH(PmsMajorPerilCode)>0
	    and TRIM(PmsMajorPerilCode)=''
	    or LENGTH(PmsMajorPerilCode) = 0)
	    and (PmsMajorPerilDescription IS NULL
	    or LENGTH(PmsMajorPerilDescription)>0
	    and TRIM(PmsMajorPerilDescription)=''
	    or LENGTH(PmsMajorPerilDescription) = 0)
	    and (PmsProductTypeCode IS NULL
	    or LENGTH(PmsProductTypeCode)>0
	    and TRIM(PmsProductTypeCode)=''
	    or LENGTH(PmsProductTypeCode) = 0),
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