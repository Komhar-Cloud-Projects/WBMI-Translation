WITH
SQ_CoverageGroup AS (
	SELECT
		CoverageGroupId,
		CoverageGroupCode,
		CoverageGroupDescription,
		CoverageSummaryId
	FROM CoverageGroup
	WHERE CoverageGroup.CoverageGroupCode IS NOT NULL
),
EXP_DefaultData AS (
	SELECT
	CoverageGroupId AS i_CoverageGroupId,
	CoverageGroupCode AS i_CoverageGroupCode,
	CoverageGroupDescription AS i_CoverageGroupDescription,
	CoverageSummaryId AS i_CoverageSummaryId,
	SYSDATE AS o_CurrentDate,
	-- *INF*: IIF(ISNULL(i_CoverageGroupId) OR LENGTH(i_CoverageGroupId)=0, Error('Missing Coverage Group Id'), i_CoverageGroupId)
	IFF(i_CoverageGroupId IS NULL OR LENGTH(i_CoverageGroupId) = 0, Error('Missing Coverage Group Id'), i_CoverageGroupId) AS o_CoverageGroupId,
	-- *INF*: IIF(ISNULL(i_CoverageGroupCode) OR LENGTH(i_CoverageGroupCode)=0 OR IS_SPACES(i_CoverageGroupCode), 'N/A', LTRIM(RTRIM(i_CoverageGroupCode)))
	IFF(i_CoverageGroupCode IS NULL OR LENGTH(i_CoverageGroupCode) = 0 OR IS_SPACES(i_CoverageGroupCode), 'N/A', LTRIM(RTRIM(i_CoverageGroupCode))) AS o_CoverageGroupCode,
	-- *INF*: IIF(ISNULL(i_CoverageGroupDescription) OR LENGTH(i_CoverageGroupDescription)=0 OR IS_SPACES(i_CoverageGroupDescription), 'N/A', LTRIM(RTRIM(i_CoverageGroupDescription)))
	IFF(i_CoverageGroupDescription IS NULL OR LENGTH(i_CoverageGroupDescription) = 0 OR IS_SPACES(i_CoverageGroupDescription), 'N/A', LTRIM(RTRIM(i_CoverageGroupDescription))) AS o_CoverageGroupDescription,
	-- *INF*: IIF(ISNULL(i_CoverageSummaryId) OR LENGTH(i_CoverageSummaryId)=0, -99, i_CoverageSummaryId)
	IFF(i_CoverageSummaryId IS NULL OR LENGTH(i_CoverageSummaryId) = 0, - 99, i_CoverageSummaryId) AS o_CoverageSummaryId
	FROM SQ_CoverageGroup
),
LKP_CoverageGroup AS (
	SELECT
	i_CoverageGroupId,
	CoverageGroupId,
	CoverageGroupCode,
	CoverageGroupDescription,
	CoverageSummaryId
	FROM (
		SELECT 
			i_CoverageGroupId,
			CoverageGroupId,
			CoverageGroupCode,
			CoverageGroupDescription,
			CoverageSummaryId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGroupId ORDER BY i_CoverageGroupId) = 1
),
LKP_CoverageSummary AS (
	SELECT
	CoverageSummaryId
	FROM (
		SELECT 
			CoverageSummaryId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageSummaryId ORDER BY CoverageSummaryId) = 1
),
EXP_Detect_Change AS (
	SELECT
	LKP_CoverageSummary.CoverageSummaryId AS lkp_CurrentCoverageSummaryId,
	LKP_CoverageGroup.CoverageGroupId AS lkp_CoverageGroupId,
	LKP_CoverageGroup.CoverageGroupCode AS lkp_CoverageGroupCode,
	LKP_CoverageGroup.CoverageGroupDescription AS lkp_CoverageGroupDescription,
	LKP_CoverageGroup.CoverageSummaryId AS lkp_ExistingCoverageSummaryId,
	EXP_DefaultData.o_CurrentDate AS CurrentDate,
	EXP_DefaultData.o_CoverageGroupId AS CoverageGroupId,
	EXP_DefaultData.o_CoverageGroupCode AS CoverageGroupCode,
	EXP_DefaultData.o_CoverageGroupDescription AS CoverageGroupDescription,
	EXP_DefaultData.o_CoverageSummaryId AS CoverageSummaryId,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_CoverageGroupId), 'Insert',
	-- lkp_ExistingCoverageSummaryId<>lkp_CurrentCoverageSummaryId, 'Update',
	-- lkp_CoverageGroupCode <> CoverageGroupCode, 'Update',
	-- lkp_CoverageGroupDescription<>CoverageGroupDescription, 'Update',
	-- 'Ignore')
	DECODE(TRUE,
		lkp_CoverageGroupId IS NULL, 'Insert',
		lkp_ExistingCoverageSummaryId <> lkp_CurrentCoverageSummaryId, 'Update',
		lkp_CoverageGroupCode <> CoverageGroupCode, 'Update',
		lkp_CoverageGroupDescription <> CoverageGroupDescription, 'Update',
		'Ignore') AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM EXP_DefaultData
	LEFT JOIN LKP_CoverageGroup
	ON LKP_CoverageGroup.CoverageGroupId = EXP_DefaultData.o_CoverageGroupId
	LEFT JOIN LKP_CoverageSummary
	ON LKP_CoverageSummary.CoverageSummaryId = EXP_DefaultData.o_CoverageSummaryId
),
RT_UpdateOrInsert AS (
	SELECT
	CoverageGroupId,
	CoverageGroupCode,
	CoverageGroupDescription,
	CoverageSummaryId,
	o_ChangeFlag AS ChangeFlag,
	CurrentDate
	FROM EXP_Detect_Change
),
RT_UpdateOrInsert_UPDATE AS (SELECT * FROM RT_UpdateOrInsert WHERE ChangeFlag='Update'),
RT_UpdateOrInsert_INSERT AS (SELECT * FROM RT_UpdateOrInsert WHERE ChangeFlag='Insert'),
UPD_Updates AS (
	SELECT
	CoverageGroupId AS lkp_CoverageGroupId, 
	CoverageGroupCode, 
	CoverageGroupDescription, 
	CoverageSummaryId, 
	CurrentDate
	FROM RT_UpdateOrInsert_UPDATE
),
CoverageGroup_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup AS T
	USING UPD_Updates AS S
	ON T.CoverageGroupId = S.lkp_CoverageGroupId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.CurrentDate, T.CoverageSummaryId = S.CoverageSummaryId, T.CoverageGroupCode = S.CoverageGroupCode, T.CoverageGroupDescription = S.CoverageGroupDescription
),
CoverageGroup_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup
	(CoverageGroupId, CreatedDate, ModifiedDate, CoverageSummaryId, CoverageGroupCode, CoverageGroupDescription)
	SELECT 
	COVERAGEGROUPID, 
	CurrentDate AS CREATEDDATE, 
	CurrentDate AS MODIFIEDDATE, 
	COVERAGESUMMARYID, 
	COVERAGEGROUPCODE, 
	COVERAGEGROUPDESCRIPTION
	FROM RT_UpdateOrInsert_INSERT
),