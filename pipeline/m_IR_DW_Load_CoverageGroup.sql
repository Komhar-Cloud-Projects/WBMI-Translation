WITH
SQ_CSV_ConformedCoverage AS (

-- TODO Manual --

),
EXP_Trim_Values AS (
	SELECT
	CoverageSummaryCode AS i_CoverageSummaryCode,
	CoverageGroupCode AS i_CoverageGroupCode,
	CoverageGroupDescription AS i_CoverageGroupDescription,
	-- *INF*: LTRIM(RTRIM(i_CoverageSummaryCode))
	LTRIM(RTRIM(i_CoverageSummaryCode)) AS o_CoverageSummaryCode,
	-- *INF*: LTRIM(RTRIM(i_CoverageGroupCode))
	LTRIM(RTRIM(i_CoverageGroupCode)) AS o_CoverageGroupCode,
	-- *INF*: LTRIM(RTRIM(i_CoverageGroupDescription))
	LTRIM(RTRIM(i_CoverageGroupDescription)) AS o_CoverageGroupDescription
	FROM SQ_CSV_ConformedCoverage
),
AGG_Remove_Duplicate AS (
	SELECT
	o_CoverageSummaryCode AS CoverageSummaryCode,
	o_CoverageGroupCode AS CoverageGroupCode,
	o_CoverageGroupDescription AS CoverageGroupDescription
	FROM EXP_Trim_Values
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageSummaryCode, CoverageGroupCode ORDER BY NULL) = 1
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
	CoverageSummaryId,
	CoverageGroupDescription,
	i_CoverageSummaryId,
	CoverageGroupCode
	FROM (
		SELECT 
			CoverageGroupId,
			CoverageSummaryId,
			CoverageGroupDescription,
			i_CoverageSummaryId,
			CoverageGroupCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGroupCode,CoverageSummaryId ORDER BY CoverageGroupId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_CoverageGroup.CoverageGroupId AS lkp_CoverageGroupId,
	LKP_CoverageGroup.CoverageSummaryId AS lkp_CoverageSummaryId,
	LKP_CoverageGroup.CoverageGroupDescription AS lkp_CoverageGroupDescription,
	LKP_CoverageSummary.CoverageSummaryId,
	AGG_Remove_Duplicate.CoverageGroupDescription,
	AGG_Remove_Duplicate.CoverageGroupCode,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: DECODE(TRUE,ISNULL(lkp_CoverageGroupId),1,
	-- lkp_CoverageGroupDescription<>CoverageGroupDescription,
	-- 2,0)
	DECODE(
	    TRUE,
	    lkp_CoverageGroupId IS NULL, 1,
	    lkp_CoverageGroupDescription <> CoverageGroupDescription, 2,
	    0
	) AS o_change_flag
	FROM AGG_Remove_Duplicate
	LEFT JOIN LKP_CoverageGroup
	ON LKP_CoverageGroup.CoverageGroupCode = AGG_Remove_Duplicate.CoverageGroupCode AND LKP_CoverageGroup.CoverageSummaryId = LKP_CoverageSummary.CoverageSummaryId
	LEFT JOIN LKP_CoverageSummary
	ON LKP_CoverageSummary.CoverageSummaryCode = AGG_Remove_Duplicate.CoverageSummaryCode
),
RTR_INSERT_UPDATE AS (
	SELECT
	lkp_CoverageGroupId AS CoverageGroupId,
	CoverageSummaryId,
	CoverageGroupDescription,
	CoverageGroupCode,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_change_flag AS change_flag
	FROM EXP_Detect_Changes
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE change_flag=1 AND  NOT ISNULL(CoverageSummaryId) AND  NOT (ISNULL(CoverageGroupCode) OR IS_SPACES(CoverageGroupCode) OR LENGTH(CoverageGroupCode)=0)),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE change_flag=2 AND  NOT ISNULL(CoverageSummaryId) AND  NOT (ISNULL(CoverageGroupCode) OR IS_SPACES(CoverageGroupCode) OR LENGTH(CoverageGroupCode)=0)),
UPD_CoverageGroup AS (
	SELECT
	CoverageGroupId, 
	CoverageSummaryId, 
	ModifiedDate, 
	CoverageGroupDescription
	FROM RTR_INSERT_UPDATE_UPDATE
),
CoverageGroup_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup AS T
	USING UPD_CoverageGroup AS S
	ON T.CoverageGroupId = S.CoverageGroupId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageSummaryId = S.CoverageSummaryId, T.CoverageGroupDescription = S.CoverageGroupDescription
),
CoverageGroup_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup
	(CreatedDate, ModifiedDate, CoverageSummaryId, CoverageGroupCode, CoverageGroupDescription)
	SELECT 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGESUMMARYID, 
	COVERAGEGROUPCODE, 
	COVERAGEGROUPDESCRIPTION
	FROM RTR_INSERT_UPDATE_INSERT
),