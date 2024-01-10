WITH
SQ_CSV_ConformedCoverage AS (

-- TODO Manual --

),
EXP_Trim_Values AS (
	SELECT
	CoverageSummaryCode AS i_CoverageSummaryCode,
	CoverageGroupCode AS i_CoverageGroupCode,
	CoverageCode AS i_CoverageCode,
	CoverageDescription AS i_CoverageDescription,
	RatedCoverageCode AS i_RatedCoverageCode,
	RatedCoverageDescription AS i_RatedCoverageDescription,
	-- *INF*: LTRIM(RTRIM(i_CoverageSummaryCode))
	LTRIM(RTRIM(i_CoverageSummaryCode
		)
	) AS o_CoverageSummaryCode,
	-- *INF*: LTRIM(RTRIM(i_CoverageGroupCode))
	LTRIM(RTRIM(i_CoverageGroupCode
		)
	) AS o_CoverageGroupCode,
	-- *INF*: LTRIM(RTRIM(i_CoverageCode))
	LTRIM(RTRIM(i_CoverageCode
		)
	) AS o_CoverageCode,
	-- *INF*: LTRIM(RTRIM(i_CoverageDescription))
	LTRIM(RTRIM(i_CoverageDescription
		)
	) AS o_CoverageDescription,
	-- *INF*: LTRIM(RTRIM(i_RatedCoverageCode))
	LTRIM(RTRIM(i_RatedCoverageCode
		)
	) AS o_RatedCoverageCode,
	-- *INF*: LTRIM(RTRIM(i_RatedCoverageDescription))
	LTRIM(RTRIM(i_RatedCoverageDescription
		)
	) AS o_RatedCoverageDescription
	FROM SQ_CSV_ConformedCoverage
),
AGG_Remove_Duplicate AS (
	SELECT
	o_CoverageSummaryCode AS CoverageSummaryCode,
	o_CoverageGroupCode AS CoverageGroupCode,
	o_CoverageCode AS CoverageCode,
	o_CoverageDescription AS CoverageDescription,
	o_RatedCoverageCode AS RatedCoverageCode,
	o_RatedCoverageDescription AS RatedCoverageDescription
	FROM EXP_Trim_Values
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageSummaryCode, CoverageGroupCode, CoverageCode, RatedCoverageCode ORDER BY NULL) = 1
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
	CoverageGroupId,
	CoverageDescription,
	RatedCoverageCode,
	RatedCoverageDescription,
	i_CoverageGroupId,
	CoverageCode
	FROM (
		SELECT 
			ConformedCoverageId,
			CoverageGroupId,
			CoverageDescription,
			RatedCoverageCode,
			RatedCoverageDescription,
			i_CoverageGroupId,
			CoverageCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageCode,CoverageGroupId,RatedCoverageCode,RatedCoverageDescription ORDER BY ConformedCoverageId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ConformedCoverage.ConformedCoverageId AS lkp_ConformedCoverageId,
	LKP_ConformedCoverage.CoverageGroupId AS lkp_CoverageGroupId,
	LKP_ConformedCoverage.CoverageDescription AS lkp_CoverageDescription,
	LKP_ConformedCoverage.RatedCoverageCode AS lkp_RatedCoverageCode,
	LKP_ConformedCoverage.RatedCoverageDescription AS lkp_RatedCoverageDescription,
	LKP_CoverageGroup.CoverageGroupId,
	AGG_Remove_Duplicate.CoverageDescription,
	AGG_Remove_Duplicate.CoverageCode,
	AGG_Remove_Duplicate.RatedCoverageCode,
	AGG_Remove_Duplicate.RatedCoverageDescription,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: DECODE(TRUE,ISNULL(lkp_ConformedCoverageId),1,
	-- lkp_CoverageDescription<>CoverageDescription OR
	-- lkp_RatedCoverageDescription<>RatedCoverageDescription OR lkp_RatedCoverageCode<>RatedCoverageCode ,2,
	-- 0)
	DECODE(TRUE,
		lkp_ConformedCoverageId IS NULL, 1,
		lkp_CoverageDescription <> CoverageDescription 
		OR lkp_RatedCoverageDescription <> RatedCoverageDescription 
		OR lkp_RatedCoverageCode <> RatedCoverageCode, 2,
		0
	) AS o_change_flag
	FROM AGG_Remove_Duplicate
	LEFT JOIN LKP_ConformedCoverage
	ON LKP_ConformedCoverage.CoverageCode = AGG_Remove_Duplicate.CoverageCode AND LKP_ConformedCoverage.CoverageGroupId = LKP_CoverageGroup.CoverageGroupId AND LKP_ConformedCoverage.RatedCoverageCode = AGG_Remove_Duplicate.RatedCoverageCode AND LKP_ConformedCoverage.RatedCoverageDescription = AGG_Remove_Duplicate.RatedCoverageDescription
	LEFT JOIN LKP_CoverageGroup
	ON LKP_CoverageGroup.CoverageGroupCode = AGG_Remove_Duplicate.CoverageGroupCode AND LKP_CoverageGroup.CoverageSummaryId = LKP_CoverageSummary.CoverageSummaryId
),
RTR_INSERT_UPDATE AS (
	SELECT
	lkp_ConformedCoverageId AS ConformedCoverageId,
	CoverageGroupId,
	CoverageCode,
	CoverageDescription,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_change_flag AS change_flag,
	RatedCoverageCode,
	RatedCoverageDescription
	FROM EXP_Detect_Changes
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE change_flag=1 AND  NOT ISNULL(CoverageGroupId) AND  NOT (ISNULL(CoverageCode) OR IS_SPACES(CoverageCode) OR LENGTH(CoverageCode)=0)),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE change_flag=2 AND  NOT ISNULL(CoverageGroupId) AND  NOT (ISNULL(CoverageCode) OR IS_SPACES(CoverageCode) OR LENGTH(CoverageCode)=0)),
UPD_ConformedCoverage AS (
	SELECT
	ConformedCoverageId, 
	ModifiedDate, 
	CoverageGroupId, 
	CoverageDescription, 
	RatedCoverageCode, 
	RatedCoverageDescription
	FROM RTR_INSERT_UPDATE_UPDATE
),
ConformedCoverage_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage AS T
	USING UPD_ConformedCoverage AS S
	ON T.ConformedcoverageId = S.ConformedCoverageId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CoverageGroupId = S.CoverageGroupId, T.CoverageDescription = S.CoverageDescription, T.RatedCoverageCode = S.RatedCoverageCode, T.RatedCoverageDescription = S.RatedCoverageDescription
),
ConformedCoverage_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage
	(CreatedDate, ModifiedDate, CoverageGroupId, CoverageCode, CoverageDescription, RatedCoverageCode, RatedCoverageDescription)
	SELECT 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGROUPID, 
	COVERAGECODE, 
	COVERAGEDESCRIPTION, 
	RATEDCOVERAGECODE, 
	RATEDCOVERAGEDESCRIPTION
	FROM RTR_INSERT_UPDATE_INSERT
),