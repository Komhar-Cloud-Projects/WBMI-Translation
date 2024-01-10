WITH
SQ_CSV_ConformedCoverage AS (

-- TODO Manual --

),
EXP_Trim_Values AS (
	SELECT
	CoverageSummaryCode AS i_CoverageSummaryCode,
	CoverageGroupCode AS i_CoverageGroupCode,
	CoverageCode AS i_CoverageCode,
	RatedCoverageCode AS i_RatedCoverageCode,
	RatedCoverageDescription AS i_RatedCoverageDescription,
	InsuranceLineCode AS i_InsuranceLineCode,
	InsuranceLineDescription AS i_InsuranceLineDescription,
	SourceSystemId AS i_SourceSystemId,
	DctRiskTypeCode AS i_DctRiskTypeCode,
	DctCoverageTypeCode AS i_DctCoverageTypeCode,
	DctSubCoverageTypeCode AS i_DctSubCoverageTypeCode,
	DctPerilGroup AS i_DctPerilGroup,
	DctCoverageVersion AS i_DctCoverageVersion,
	PmsRiskUnitGroupCode AS i_PmsRiskUnitGroupCode,
	PmsRiskUnitGroupDescription AS i_PmsRiskUnitGroupDescription,
	PmsRiskUnitCode AS i_PmsRiskUnitCode,
	PmsRiskUnitDescription AS i_PmsRiskUnitDescription,
	PmsMajorPerilCode AS i_PmsMajorPerilCode,
	PmsMajorPerilDescription AS i_PmsMajorPerilDescription,
	PmsProductTypeCode AS i_PmsProductTypeCode,
	LossHistoryCode AS i_LossHistoryCode,
	LossHistoryDescription AS i_LossHistoryDescription,
	ISOMajorCrimeGroup AS i_ISOMajorCrimeGroup,
	-- *INF*: LTRIM(RTRIM(i_CoverageSummaryCode))
	LTRIM(RTRIM(i_CoverageSummaryCode)) AS o_CoverageSummaryCode,
	-- *INF*: LTRIM(RTRIM(i_CoverageGroupCode))
	LTRIM(RTRIM(i_CoverageGroupCode)) AS o_CoverageGroupCode,
	-- *INF*: LTRIM(RTRIM(i_CoverageCode))
	LTRIM(RTRIM(i_CoverageCode)) AS o_CoverageCode,
	-- *INF*: LTRIM(RTRIM(i_RatedCoverageCode))
	LTRIM(RTRIM(i_RatedCoverageCode)) AS o_RatedCoverageCode,
	-- *INF*: LTRIM(RTRIM(i_RatedCoverageDescription))
	-- 
	LTRIM(RTRIM(i_RatedCoverageDescription)) AS o_RatedCoverageDescription,
	-- *INF*: LTRIM(RTRIM(i_InsuranceLineCode))
	LTRIM(RTRIM(i_InsuranceLineCode)) AS o_InsuranceLineCode,
	-- *INF*: LTRIM(RTRIM(i_InsuranceLineDescription))
	LTRIM(RTRIM(i_InsuranceLineDescription)) AS o_InsuranceLineDescription,
	-- *INF*: LTRIM(RTRIM(i_SourceSystemId))
	LTRIM(RTRIM(i_SourceSystemId)) AS o_SourceSystemId,
	-- *INF*: LTRIM(RTRIM(i_DctRiskTypeCode))
	LTRIM(RTRIM(i_DctRiskTypeCode)) AS o_DctRiskTypeCode,
	-- *INF*: LTRIM(RTRIM(i_DctCoverageTypeCode))
	LTRIM(RTRIM(i_DctCoverageTypeCode)) AS o_DctCoverageTypeCode,
	-- *INF*: LTRIM(RTRIM(i_PmsRiskUnitGroupCode))
	LTRIM(RTRIM(i_PmsRiskUnitGroupCode)) AS o_PmsRiskUnitGroupCode,
	-- *INF*: LTRIM(RTRIM(i_PmsRiskUnitGroupDescription))
	LTRIM(RTRIM(i_PmsRiskUnitGroupDescription)) AS o_PmsRiskUnitGroupDescription,
	-- *INF*: LTRIM(RTRIM(i_PmsRiskUnitCode))
	LTRIM(RTRIM(i_PmsRiskUnitCode)) AS o_PmsRiskUnitCode,
	-- *INF*: LTRIM(RTRIM(i_PmsRiskUnitDescription))
	LTRIM(RTRIM(i_PmsRiskUnitDescription)) AS o_PmsRiskUnitDescription,
	-- *INF*: LTRIM(RTRIM(i_PmsMajorPerilCode))
	LTRIM(RTRIM(i_PmsMajorPerilCode)) AS o_PmsMajorPerilCode,
	-- *INF*: LTRIM(RTRIM(i_PmsMajorPerilDescription))
	LTRIM(RTRIM(i_PmsMajorPerilDescription)) AS o_PmsMajorPerilDescription,
	-- *INF*: LTRIM(RTRIM(i_PmsProductTypeCode))
	LTRIM(RTRIM(i_PmsProductTypeCode)) AS o_PmsProductTypeCode,
	-- *INF*: LTRIM(RTRIM(i_DctPerilGroup))
	LTRIM(RTRIM(i_DctPerilGroup)) AS o_DctPerilGroup,
	-- *INF*: LTRIM(RTRIM(i_DctSubCoverageTypeCode))
	LTRIM(RTRIM(i_DctSubCoverageTypeCode)) AS o_DctSubCoverageTypeCode,
	-- *INF*: LTRIM(RTRIM(i_DctCoverageVersion))
	LTRIM(RTRIM(i_DctCoverageVersion)) AS o_DctCoverageVersion,
	-- *INF*: IIF(ISNULL(i_LossHistoryCode),'N/A',
	-- IIF(LENGTH(i_LossHistoryCode)>=2,
	-- LTRIM(RTRIM(i_LossHistoryCode)),
	-- LTRIM(RTRIM(LPAD(i_LossHistoryCode,2,'0')))
	-- )
	-- )
	IFF(i_LossHistoryCode IS NULL, 'N/A', IFF(LENGTH(i_LossHistoryCode) >= 2, LTRIM(RTRIM(i_LossHistoryCode)), LTRIM(RTRIM(LPAD(i_LossHistoryCode, 2, '0'))))) AS o_LossHistoryCode,
	-- *INF*: IIF(ISNULL(i_LossHistoryDescription),'N/A',LTRIM(RTRIM(i_LossHistoryDescription)))
	IFF(i_LossHistoryDescription IS NULL, 'N/A', LTRIM(RTRIM(i_LossHistoryDescription))) AS o_LossHistoryDescription,
	-- *INF*: IIF(ISNULL(i_ISOMajorCrimeGroup) OR IS_SPACES(i_ISOMajorCrimeGroup) OR LENGTH(i_ISOMajorCrimeGroup)=0,'N/A',LTRIM(RTRIM(i_ISOMajorCrimeGroup)))
	IFF(i_ISOMajorCrimeGroup IS NULL OR IS_SPACES(i_ISOMajorCrimeGroup) OR LENGTH(i_ISOMajorCrimeGroup) = 0, 'N/A', LTRIM(RTRIM(i_ISOMajorCrimeGroup))) AS o_ISOMajorCrimeGroup
	FROM SQ_CSV_ConformedCoverage
),
AGG_Remove_Duplicate AS (
	SELECT
	o_CoverageSummaryCode AS CoverageSummaryCode,
	o_CoverageGroupCode AS CoverageGroupCode,
	o_CoverageCode AS CoverageCode,
	o_RatedCoverageCode AS RatedCoverageCode,
	o_RatedCoverageDescription AS RatedCoverageDescription,
	o_InsuranceLineCode AS InsuranceLineCode,
	o_InsuranceLineDescription AS InsuranceLineDescription,
	o_SourceSystemId AS SourceSystemId,
	o_DctRiskTypeCode AS DctRiskTypeCode,
	o_DctCoverageTypeCode AS DctCoverageTypeCode,
	o_PmsRiskUnitGroupCode AS PmsRiskUnitGroupCode,
	o_PmsRiskUnitGroupDescription AS PmsRiskUnitGroupDescription,
	o_PmsRiskUnitCode AS PmsRiskUnitCode,
	o_PmsRiskUnitDescription AS PmsRiskUnitDescription,
	o_PmsMajorPerilCode AS PmsMajorPerilCode,
	o_PmsMajorPerilDescription AS PmsMajorPerilDescription,
	o_PmsProductTypeCode AS PmsProductTypeCode,
	o_DctPerilGroup AS DctPerilGroup,
	o_DctSubCoverageTypeCode AS DctSubCoverageTypeCode,
	o_DctCoverageVersion AS DctCoverageVersion,
	o_LossHistoryCode AS LossHistoryCode,
	o_LossHistoryDescription AS LossHistoryDescription,
	o_ISOMajorCrimeGroup
	FROM EXP_Trim_Values
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageSummaryCode, CoverageGroupCode, InsuranceLineCode, DctRiskTypeCode, DctCoverageTypeCode, PmsRiskUnitGroupCode, PmsRiskUnitCode, PmsMajorPerilCode, PmsProductTypeCode, DctPerilGroup, DctSubCoverageTypeCode, DctCoverageVersion ORDER BY NULL) = 1
),
LKP_CoverageSummary AS (
	SELECT
	CoverageSummaryId,
	CoverageSummaryCode
	FROM (
		SELECT 
			CoverageSummaryId,
			CoverageSummaryCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageSummaryCode ORDER BY CoverageSummaryId) = 1
),
LKP_CoverageGroup AS (
	SELECT
	CoverageGroupId,
	CoverageGroupCode,
	CoverageSummaryId
	FROM (
		SELECT 
			CoverageGroupId,
			CoverageGroupCode,
			CoverageSummaryId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGroupCode,CoverageSummaryId ORDER BY CoverageGroupId) = 1
),
LKP_ConformedCoverage AS (
	SELECT
	ConformedCoverageId,
	i_CoverageGroupId,
	CoverageCode,
	CoverageGroupId,
	RatedCoverageCode,
	RatedCoverageDescription
	FROM (
		SELECT 
			ConformedCoverageId,
			i_CoverageGroupId,
			CoverageCode,
			CoverageGroupId,
			RatedCoverageCode,
			RatedCoverageDescription
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageCode,CoverageGroupId,RatedCoverageCode,RatedCoverageDescription ORDER BY ConformedCoverageId) = 1
),
LKP_SystemCoverage AS (
	SELECT
	SystemCoverageId,
	ConformedCoverageId,
	InsuranceLineDescription,
	SourceSystemId,
	PmsRiskUnitGroupDescription,
	PmsRiskUnitDescription,
	PmsMajorPerilDescription,
	LossHistoryCode,
	LossHistoryDescription,
	ISOMajorCrimeGroup,
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
		SELECT SystemCoverage.SystemCoverageId AS SystemCoverageId
			,SystemCoverage.ConformedCoverageId AS ConformedCoverageId
			,SystemCoverage.InsuranceLineDescription AS InsuranceLineDescription
			,SystemCoverage.SourceSystemId AS SourceSystemId
			,SystemCoverage.PmsRiskUnitGroupDescription AS PmsRiskUnitGroupDescription
			,SystemCoverage.PmsRiskUnitDescription AS PmsRiskUnitDescription
			,SystemCoverage.PmsMajorPerilDescription AS PmsMajorPerilDescription
			,SystemCoverage.InsuranceLineCode AS InsuranceLineCode
			,SystemCoverage.DctRiskTypeCode AS DctRiskTypeCode
			,SystemCoverage.DctCoverageTypeCode AS DctCoverageTypeCode
			,SystemCoverage.PmsRiskUnitGroupCode AS PmsRiskUnitGroupCode
			,SystemCoverage.PmsRiskUnitCode AS PmsRiskUnitCode
			,SystemCoverage.PmsMajorPerilCode AS PmsMajorPerilCode
			,LTRIM(RTRIM(SystemCoverage.PmsProductTypeCode)) AS PmsProductTypeCode
			,SystemCoverage.DctPerilGroup AS DctPerilGroup
			,SystemCoverage.DctSubCoverageTypeCode AS DctSubCoverageTypeCode
			,SystemCoverage.DctCoverageVersion AS DctCoverageVersion
			,LTRIM(RTRIM(SystemCoverage.LossHistoryCode)) As LossHistoryCode
			,LTRIM(RTRIM(SystemCoverage.LossHistoryDescription)) AS LossHistoryDescription 
			,LTRIM(RTRIM(SystemCoverage.ISOMajorCrimeGroup)) AS ISOMajorCrimeGroup
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctRiskTypeCode,DctCoverageTypeCode,PmsRiskUnitGroupCode,PmsRiskUnitCode,PmsMajorPerilCode,PmsProductTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY SystemCoverageId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_SystemCoverage.SystemCoverageId AS lkp_SystemCoverageId,
	LKP_SystemCoverage.ConformedCoverageId AS lkp_ConformedCoverageId,
	LKP_SystemCoverage.InsuranceLineDescription AS lkp_InsuranceLineDescription,
	LKP_SystemCoverage.SourceSystemId AS lkp_SourceSystemId,
	LKP_SystemCoverage.PmsRiskUnitGroupDescription AS lkp_PmsRiskUnitGroupDescription,
	LKP_SystemCoverage.PmsRiskUnitDescription AS lkp_PmsRiskUnitDescription,
	LKP_SystemCoverage.PmsMajorPerilDescription AS lkp_PmsMajorPerilDescription,
	LKP_SystemCoverage.LossHistoryCode AS lkp_LossHistoryCode,
	LKP_SystemCoverage.LossHistoryDescription AS lkp_LossHistoryDescription,
	LKP_SystemCoverage.ISOMajorCrimeGroup AS lkp_ISOMajorCrimeGroup,
	LKP_ConformedCoverage.ConformedCoverageId,
	AGG_Remove_Duplicate.InsuranceLineCode,
	AGG_Remove_Duplicate.InsuranceLineDescription,
	AGG_Remove_Duplicate.SourceSystemId,
	AGG_Remove_Duplicate.DctRiskTypeCode,
	AGG_Remove_Duplicate.DctCoverageTypeCode,
	AGG_Remove_Duplicate.PmsRiskUnitGroupCode,
	AGG_Remove_Duplicate.PmsRiskUnitGroupDescription,
	AGG_Remove_Duplicate.PmsRiskUnitCode,
	AGG_Remove_Duplicate.PmsRiskUnitDescription,
	AGG_Remove_Duplicate.PmsMajorPerilCode,
	AGG_Remove_Duplicate.PmsMajorPerilDescription,
	AGG_Remove_Duplicate.PmsProductTypeCode,
	AGG_Remove_Duplicate.DctPerilGroup,
	AGG_Remove_Duplicate.DctSubCoverageTypeCode,
	AGG_Remove_Duplicate.DctCoverageVersion,
	AGG_Remove_Duplicate.LossHistoryCode,
	AGG_Remove_Duplicate.LossHistoryDescription,
	AGG_Remove_Duplicate.o_ISOMajorCrimeGroup AS ISOMajorCrimeGroup,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: DECODE(TRUE,ISNULL(lkp_SystemCoverageId),1,
	-- lkp_ConformedCoverageId<>ConformedCoverageId OR
	-- lkp_InsuranceLineDescription<>InsuranceLineDescription OR
	-- lkp_SourceSystemId<>SourceSystemId OR
	-- lkp_PmsRiskUnitGroupDescription<>PmsRiskUnitGroupDescription OR
	-- lkp_PmsRiskUnitDescription<>PmsRiskUnitDescription OR
	-- lkp_PmsMajorPerilDescription<>PmsMajorPerilDescription OR lkp_LossHistoryCode<>LossHistoryCode OR lkp_LossHistoryDescription<>LossHistoryDescription OR 
	-- lkp_ISOMajorCrimeGroup<>ISOMajorCrimeGroup,2,0)
	DECODE(TRUE,
		lkp_SystemCoverageId IS NULL, 1,
		lkp_ConformedCoverageId <> ConformedCoverageId OR lkp_InsuranceLineDescription <> InsuranceLineDescription OR lkp_SourceSystemId <> SourceSystemId OR lkp_PmsRiskUnitGroupDescription <> PmsRiskUnitGroupDescription OR lkp_PmsRiskUnitDescription <> PmsRiskUnitDescription OR lkp_PmsMajorPerilDescription <> PmsMajorPerilDescription OR lkp_LossHistoryCode <> LossHistoryCode OR lkp_LossHistoryDescription <> LossHistoryDescription OR lkp_ISOMajorCrimeGroup <> ISOMajorCrimeGroup, 2,
		0) AS o_change_flag
	FROM AGG_Remove_Duplicate
	LEFT JOIN LKP_ConformedCoverage
	ON LKP_ConformedCoverage.CoverageCode = AGG_Remove_Duplicate.CoverageCode AND LKP_ConformedCoverage.CoverageGroupId = LKP_CoverageGroup.CoverageGroupId AND LKP_ConformedCoverage.RatedCoverageCode = AGG_Remove_Duplicate.RatedCoverageCode AND LKP_ConformedCoverage.RatedCoverageDescription = AGG_Remove_Duplicate.RatedCoverageDescription
	LEFT JOIN LKP_SystemCoverage
	ON LKP_SystemCoverage.InsuranceLineCode = AGG_Remove_Duplicate.InsuranceLineCode AND LKP_SystemCoverage.DctRiskTypeCode = AGG_Remove_Duplicate.DctRiskTypeCode AND LKP_SystemCoverage.DctCoverageTypeCode = AGG_Remove_Duplicate.DctCoverageTypeCode AND LKP_SystemCoverage.PmsRiskUnitGroupCode = AGG_Remove_Duplicate.PmsRiskUnitGroupCode AND LKP_SystemCoverage.PmsRiskUnitCode = AGG_Remove_Duplicate.PmsRiskUnitCode AND LKP_SystemCoverage.PmsMajorPerilCode = AGG_Remove_Duplicate.PmsMajorPerilCode AND LKP_SystemCoverage.PmsProductTypeCode = AGG_Remove_Duplicate.PmsProductTypeCode AND LKP_SystemCoverage.DctPerilGroup = AGG_Remove_Duplicate.DctPerilGroup AND LKP_SystemCoverage.DctSubCoverageTypeCode = AGG_Remove_Duplicate.DctSubCoverageTypeCode AND LKP_SystemCoverage.DctCoverageVersion = AGG_Remove_Duplicate.DctCoverageVersion
),
RTR_INSERT_UPDATE AS (
	SELECT
	lkp_SystemCoverageId AS SystemCoverageId,
	ConformedCoverageId,
	InsuranceLineCode,
	InsuranceLineDescription,
	SourceSystemId,
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
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_change_flag AS change_flag,
	DctSubCoverageTypeCode,
	DctCoverageVersion,
	LossHistoryCode,
	LossHistoryDescription,
	ISOMajorCrimeGroup AS i_ISOMajorCrimeGroup
	FROM EXP_Detect_Changes
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE change_flag=1 AND  NOT ISNULL(ConformedCoverageId) AND  NOT (ISNULL(InsuranceLineCode) OR IS_SPACES(InsuranceLineCode) OR LENGTH(InsuranceLineCode)=0)
AND  NOT (ISNULL(DctRiskTypeCode) OR IS_SPACES(DctRiskTypeCode) OR LENGTH(DctRiskTypeCode)=0)
AND  NOT (ISNULL(DctCoverageTypeCode) OR IS_SPACES(DctCoverageTypeCode) OR LENGTH(DctCoverageTypeCode)=0) 
AND  NOT (ISNULL(PmsRiskUnitGroupCode) OR IS_SPACES(PmsRiskUnitGroupCode) OR LENGTH(PmsRiskUnitGroupCode)=0)
AND  NOT (ISNULL(PmsRiskUnitCode) OR IS_SPACES(PmsRiskUnitCode) OR LENGTH(PmsRiskUnitCode)=0)
AND  NOT (ISNULL(PmsMajorPerilCode) OR IS_SPACES(PmsMajorPerilCode) OR LENGTH(PmsMajorPerilCode)=0)
AND  NOT (ISNULL(PmsProductTypeCode) OR IS_SPACES(PmsProductTypeCode) OR LENGTH(PmsProductTypeCode)=0)
AND  NOT (ISNULL(DctPerilGroup) OR IS_SPACES(DctPerilGroup) OR LENGTH(DctPerilGroup)=0)
AND  NOT (ISNULL(DctSubCoverageTypeCode) OR IS_SPACES(DctSubCoverageTypeCode) OR LENGTH(DctSubCoverageTypeCode)=0)
AND  NOT (ISNULL(DctCoverageVersion) OR IS_SPACES(DctCoverageVersion) OR LENGTH(DctCoverageVersion)=0)),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE change_flag=2 AND  NOT ISNULL(ConformedCoverageId) AND  NOT (ISNULL(InsuranceLineCode) OR IS_SPACES(InsuranceLineCode) OR LENGTH(InsuranceLineCode)=0)
AND  NOT (ISNULL(DctRiskTypeCode) OR IS_SPACES(DctRiskTypeCode) OR LENGTH(DctRiskTypeCode)=0)
AND  NOT (ISNULL(DctCoverageTypeCode) OR IS_SPACES(DctCoverageTypeCode) OR LENGTH(DctCoverageTypeCode)=0) 
AND  NOT (ISNULL(PmsRiskUnitGroupCode) OR IS_SPACES(PmsRiskUnitGroupCode) OR LENGTH(PmsRiskUnitGroupCode)=0)
AND  NOT (ISNULL(PmsRiskUnitCode) OR IS_SPACES(PmsRiskUnitCode) OR LENGTH(PmsRiskUnitCode)=0)
AND  NOT (ISNULL(PmsMajorPerilCode) OR IS_SPACES(PmsMajorPerilCode) OR LENGTH(PmsMajorPerilCode)=0)
AND  NOT (ISNULL(PmsProductTypeCode) OR IS_SPACES(PmsProductTypeCode) OR LENGTH(PmsProductTypeCode)=0)
AND  NOT (ISNULL(DctPerilGroup) OR IS_SPACES(DctPerilGroup) OR LENGTH(DctPerilGroup)=0)
AND  NOT (ISNULL(DctSubCoverageTypeCode) OR IS_SPACES(DctSubCoverageTypeCode) OR LENGTH(DctSubCoverageTypeCode)=0)
AND  NOT (ISNULL(DctCoverageVersion) OR IS_SPACES(DctCoverageVersion) OR LENGTH(DctCoverageVersion)=0)),
UPD_SystemCoverage_INSERT AS (
	SELECT
	CreatedDate, 
	ModifiedDate, 
	ConformedCoverageId, 
	InsuranceLineCode, 
	InsuranceLineDescription, 
	SourceSystemId, 
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
	LossHistoryCode, 
	LossHistoryDescription, 
	i_ISOMajorCrimeGroup AS i_ISOMajorCrimeGroup1
	FROM RTR_INSERT_UPDATE_INSERT
),
SystemCoverage_INSERT AS (
	SET QUOTED_IDENTIFIER ON
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage(CreatedDate,ModifiedDate,ConformedCoverageId,InsuranceLineCode,InsuranceLineDescription,SourceSystemId,DctRiskTypeCode,DctCoverageTypeCode,PmsRiskUnitGroupCode,PmsRiskUnitGroupDescription,PmsRiskUnitCode,PmsRiskUnitDescription,PmsMajorPerilCode,PmsMajorPerilDescription,PmsProductTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion,LossHistoryCode,LossHistoryDescription,ISOMajorCrimeGroup
	) 
	SELECT S.CreatedDate,S.ModifiedDate,S.ConformedCoverageId,S.InsuranceLineCode,S.InsuranceLineDescription,S.SourceSystemId,S.DctRiskTypeCode,S.DctCoverageTypeCode,S.PmsRiskUnitGroupCode,S.PmsRiskUnitGroupDescription,S.PmsRiskUnitCode,S.PmsRiskUnitDescription,S.PmsMajorPerilCode,S.PmsMajorPerilDescription, S.PmsProductTypeCode,S.DctPerilGroup,S.DctSubCoverageTypeCode,S.DctCoverageVersion,S.LossHistoryCode, S.LossHistoryDescription, S.ISOMajorCrimeGroup
	FROM UPD_SystemCoverage_INSERT S
),
UPD_SystemCoverage AS (
	SELECT
	SystemCoverageId, 
	ConformedCoverageId, 
	InsuranceLineDescription, 
	SourceSystemId, 
	PmsRiskUnitGroupDescription, 
	PmsRiskUnitDescription, 
	PmsMajorPerilDescription, 
	ModifiedDate, 
	LossHistoryCode, 
	LossHistoryDescription, 
	i_ISOMajorCrimeGroup AS i_ISOMajorCrimeGroup3
	FROM RTR_INSERT_UPDATE_UPDATE
),
SystemCoverage_UPDATE AS (
	SET QUOTED_IDENTIFIER ON
	UPDATE @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage SET ModifiedDate = S.ModifiedDate, ConformedCoverageId = S.ConformedCoverageId, InsuranceLineDescription = S.InsuranceLineDescription, SourceSystemId = S.SourceSystemId, PmsRiskUnitGroupDescription = S.PmsRiskUnitGroupDescription, PmsRiskUnitDescription = S.PmsRiskUnitDescription, PmsMajorPerilDescription = S.PmsMajorPerilDescription,LossHistoryCode = S.LossHistoryCode, LossHistoryDescription = S.LossHistoryDescription, ISOMajorCrimeGroup= S.ISOMajorCrimeGroup WHERE SystemCoverageId = S.SystemCoverageId
	FROM UPD_SystemCoverage S
),