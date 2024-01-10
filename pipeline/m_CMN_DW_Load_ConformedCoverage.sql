WITH
SQ_ConformedCoverage AS (
	SELECT
		ConformedcoverageId,
		CoverageCode,
		CoverageDescription,
		CoverageGroupId,
		RatedCoverageCode,
		RatedCoverageDescription
	FROM ConformedCoverage
	WHERE ConformedCoverage.CoverageCode IS NOT NULL
),
EXP_Values AS (
	SELECT
	ConformedcoverageId AS i_ConformedCoverageId,
	CoverageCode AS i_CoverageCode,
	CoverageDescription AS i_CoverageDescription,
	CoverageGroupId AS i_CoverageGroupId,
	RatedCoverageCode AS i_RatedCoverageCode,
	RatedCoverageDescription AS i_RatedCoverageDescription,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_CoverageCode) OR LENGTH(i_CoverageCode)=0 OR IS_SPACES(i_CoverageCode), 'N/A', LTRIM(RTRIM(i_CoverageCode)))
	IFF(i_CoverageCode IS NULL 
		OR LENGTH(i_CoverageCode
		) = 0 
		OR LENGTH(i_CoverageCode)>0 AND TRIM(i_CoverageCode)='',
		'N/A',
		LTRIM(RTRIM(i_CoverageCode
			)
		)
	) AS o_CoverageCode,
	-- *INF*: IIF(ISNULL(i_CoverageDescription) OR LENGTH(i_CoverageDescription)=0 OR IS_SPACES(i_CoverageDescription), 'N/A', LTRIM(RTRIM(i_CoverageDescription)))
	IFF(i_CoverageDescription IS NULL 
		OR LENGTH(i_CoverageDescription
		) = 0 
		OR LENGTH(i_CoverageDescription)>0 AND TRIM(i_CoverageDescription)='',
		'N/A',
		LTRIM(RTRIM(i_CoverageDescription
			)
		)
	) AS o_CoverageDescription,
	-- *INF*: IIF(ISNULL(i_RatedCoverageCode) OR LENGTH(i_RatedCoverageCode)=0 OR IS_SPACES(i_RatedCoverageCode), 'N/A', LTRIM(RTRIM(i_RatedCoverageCode)))
	IFF(i_RatedCoverageCode IS NULL 
		OR LENGTH(i_RatedCoverageCode
		) = 0 
		OR LENGTH(i_RatedCoverageCode)>0 AND TRIM(i_RatedCoverageCode)='',
		'N/A',
		LTRIM(RTRIM(i_RatedCoverageCode
			)
		)
	) AS o_RatedCoverageCode,
	-- *INF*: IIF(ISNULL(i_RatedCoverageDescription) OR LENGTH(i_RatedCoverageDescription)=0 OR IS_SPACES(i_RatedCoverageDescription), 'N/A', LTRIM(RTRIM(i_RatedCoverageDescription)))
	IFF(i_RatedCoverageDescription IS NULL 
		OR LENGTH(i_RatedCoverageDescription
		) = 0 
		OR LENGTH(i_RatedCoverageDescription)>0 AND TRIM(i_RatedCoverageDescription)='',
		'N/A',
		LTRIM(RTRIM(i_RatedCoverageDescription
			)
		)
	) AS o_RatedCoverageDescription,
	-- *INF*: IIF(ISNULL(i_CoverageGroupId) OR LENGTH(i_CoverageGroupId)=0, -99, i_CoverageGroupId)
	IFF(i_CoverageGroupId IS NULL 
		OR LENGTH(i_CoverageGroupId
		) = 0,
		- 99,
		i_CoverageGroupId
	) AS o_CoverageGroupId
	FROM SQ_ConformedCoverage
),
LKP_ConformedCoverage AS (
	SELECT
	ConformedCoverageId,
	CoverageCode,
	CoverageDescription,
	CoverageGroupId,
	RatedCoverageCode,
	RatedCoverageDescription
	FROM (
		SELECT 
			ConformedCoverageId,
			CoverageCode,
			CoverageDescription,
			CoverageGroupId,
			RatedCoverageCode,
			RatedCoverageDescription
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ConformedCoverageId ORDER BY ConformedCoverageId) = 1
),
LKP_CoverageGroup AS (
	SELECT
	CoverageGroupId
	FROM (
		SELECT 
			CoverageGroupId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGroupId ORDER BY CoverageGroupId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_CoverageGroup.CoverageGroupId AS lkp_CurrentCoverageGroupId,
	LKP_ConformedCoverage.ConformedCoverageId AS lkp_ConformedCoverageId,
	LKP_ConformedCoverage.CoverageCode AS lkp_CoverageCode,
	LKP_ConformedCoverage.CoverageDescription AS lkp_CoverageDescription,
	LKP_ConformedCoverage.RatedCoverageCode AS lkp_RatedCoverageCode,
	LKP_ConformedCoverage.RatedCoverageDescription AS lkp_RatedCoverageDescription,
	LKP_ConformedCoverage.CoverageGroupId AS lkp_ExistingCoverageGroupId,
	EXP_Values.o_CreatedDate AS CreatedDate,
	EXP_Values.o_ModifiedDate AS ModifiedDate,
	EXP_Values.i_ConformedCoverageId AS ConformedCoverageId,
	EXP_Values.o_CoverageCode AS CoverageCode,
	EXP_Values.o_CoverageDescription AS CoverageDescription,
	EXP_Values.o_RatedCoverageCode AS RatedCoverageCode,
	EXP_Values.o_RatedCoverageDescription AS RatedCoverageDescription,
	-- *INF*: Decode(true,
	-- IsNull(lkp_ConformedCoverageId),  'Insert',
	-- lkp_ExistingCoverageGroupId <> lkp_CurrentCoverageGroupId, 'Update',
	-- lkp_CoverageCode <> CoverageCode, 'Update',
	-- lkp_CoverageDescription <> CoverageDescription, 'Update',
	-- lkp_RatedCoverageCode <> RatedCoverageCode, 'Update',
	-- lkp_RatedCoverageDescription <> RatedCoverageDescription , 'Update',
	-- 'Ignore')
	Decode(true,
		lkp_ConformedCoverageId IS NULL, 'Insert',
		lkp_ExistingCoverageGroupId <> lkp_CurrentCoverageGroupId, 'Update',
		lkp_CoverageCode <> CoverageCode, 'Update',
		lkp_CoverageDescription <> CoverageDescription, 'Update',
		lkp_RatedCoverageCode <> RatedCoverageCode, 'Update',
		lkp_RatedCoverageDescription <> RatedCoverageDescription, 'Update',
		'Ignore'
	) AS o_Change_Flag
	FROM EXP_Values
	LEFT JOIN LKP_ConformedCoverage
	ON LKP_ConformedCoverage.ConformedCoverageId = EXP_Values.i_ConformedCoverageId
	LEFT JOIN LKP_CoverageGroup
	ON LKP_CoverageGroup.CoverageGroupId = EXP_Values.o_CoverageGroupId
),
RTR_Insert_Update_Groups AS (
	SELECT
	ConformedCoverageId,
	CreatedDate,
	ModifiedDate,
	lkp_CurrentCoverageGroupId AS CoverageGroupId,
	CoverageCode,
	CoverageDescription,
	RatedCoverageCode,
	RatedCoverageDescription,
	o_Change_Flag AS Change_Flag
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_Groups_INSERT AS (SELECT * FROM RTR_Insert_Update_Groups WHERE Change_Flag='Insert'),
RTR_Insert_Update_Groups_UPDATE AS (SELECT * FROM RTR_Insert_Update_Groups WHERE Change_Flag='Update'),
UPD_Updates AS (
	SELECT
	ConformedCoverageId AS ConformedcoverageId, 
	ModifiedDate, 
	CoverageGroupId, 
	CoverageDescription, 
	RatedCoverageCode, 
	RatedCoverageDescription
	FROM RTR_Insert_Update_Groups_UPDATE
),
ConformedCoverage_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage AS T
	USING UPD_Updates AS S
	ON T.ConformedcoverageId = S.ConformedcoverageId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CoverageGroupId = S.CoverageGroupId, T.CoverageDescription = S.CoverageDescription, T.RatedCoverageCode = S.RatedCoverageCode, T.RatedCoverageDescription = S.RatedCoverageDescription
),
ConformedCoverage_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage
	(ConformedcoverageId, CreatedDate, ModifiedDate, CoverageGroupId, CoverageCode, CoverageDescription, RatedCoverageCode, RatedCoverageDescription)
	SELECT 
	ConformedCoverageId AS CONFORMEDCOVERAGEID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGROUPID, 
	COVERAGECODE, 
	COVERAGEDESCRIPTION, 
	RATEDCOVERAGECODE, 
	RATEDCOVERAGEDESCRIPTION
	FROM RTR_Insert_Update_Groups_INSERT
),