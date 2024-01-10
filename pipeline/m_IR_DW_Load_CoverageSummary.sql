WITH
SQ_CSV_ConformedCoverage AS (

-- TODO Manual --

),
EXP_Trim_Values AS (
	SELECT
	CoverageSummaryCode AS i_CoverageSummaryCode,
	CoverageSummaryDescription AS i_CoverageSummaryDescription,
	-- *INF*: LTRIM(RTRIM(i_CoverageSummaryCode))
	LTRIM(RTRIM(i_CoverageSummaryCode
		)
	) AS o_CoverageSummaryCode,
	-- *INF*: LTRIM(RTRIM(i_CoverageSummaryDescription))
	LTRIM(RTRIM(i_CoverageSummaryDescription
		)
	) AS o_CoverageSummaryDescription
	FROM SQ_CSV_ConformedCoverage
),
AGG_Remove_Duplicate AS (
	SELECT
	o_CoverageSummaryCode AS CoverageSummaryCode,
	o_CoverageSummaryDescription AS CoverageSummaryDescription
	FROM EXP_Trim_Values
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageSummaryCode ORDER BY NULL) = 1
),
LKP_CoverageSummary AS (
	SELECT
	CoverageSummaryId,
	CoverageSummaryDescription,
	i_CoverageSummaryCode,
	CoverageSummaryCode
	FROM (
		SELECT 
			CoverageSummaryId,
			CoverageSummaryDescription,
			i_CoverageSummaryCode,
			CoverageSummaryCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageSummaryCode ORDER BY CoverageSummaryId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_CoverageSummary.CoverageSummaryDescription AS lkp_CoverageSummaryDescription,
	AGG_Remove_Duplicate.CoverageSummaryDescription AS i_CoverageSummaryDescription,
	LKP_CoverageSummary.CoverageSummaryId,
	LKP_CoverageSummary.i_CoverageSummaryCode AS CoverageSummaryCode,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageSummaryDescription AS o_CoverageSummaryDescription,
	-- *INF*: DECODE(TRUE,ISNULL(CoverageSummaryId),1,
	-- lkp_CoverageSummaryDescription<>i_CoverageSummaryDescription,2,
	-- 0)
	DECODE(TRUE,
		CoverageSummaryId IS NULL, 1,
		lkp_CoverageSummaryDescription <> i_CoverageSummaryDescription, 2,
		0
	) AS o_change_flag
	FROM AGG_Remove_Duplicate
	LEFT JOIN LKP_CoverageSummary
	ON LKP_CoverageSummary.CoverageSummaryCode = AGG_Remove_Duplicate.CoverageSummaryCode
),
RTR_INSERT_UPDATE AS (
	SELECT
	CoverageSummaryId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	CoverageSummaryCode,
	o_CoverageSummaryDescription AS CoverageSummaryDescription,
	o_change_flag AS change_flag
	FROM EXP_Detect_Changes
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE change_flag=1 AND  NOT (ISNULL(CoverageSummaryCode) OR IS_SPACES(CoverageSummaryCode) OR LENGTH(CoverageSummaryCode)=0)),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE change_flag=2 AND  NOT (ISNULL(CoverageSummaryCode) OR IS_SPACES(CoverageSummaryCode) OR LENGTH(CoverageSummaryCode)=0)),
CoverageSummary_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary
	(CreatedDate, ModifiedDate, CoverageSummaryCode, CoverageSummaryDescription)
	SELECT 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGESUMMARYCODE, 
	COVERAGESUMMARYDESCRIPTION
	FROM RTR_INSERT_UPDATE_INSERT
),
UPD_CoverageSummary AS (
	SELECT
	CoverageSummaryId, 
	ModifiedDate, 
	CoverageSummaryDescription
	FROM RTR_INSERT_UPDATE_UPDATE
),
CoverageSummary_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary AS T
	USING UPD_CoverageSummary AS S
	ON T.CoverageSummaryId = S.CoverageSummaryId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CoverageSummaryDescription = S.CoverageSummaryDescription
),