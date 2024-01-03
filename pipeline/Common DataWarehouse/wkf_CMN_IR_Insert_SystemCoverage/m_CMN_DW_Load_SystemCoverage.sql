WITH
SQ_SystemCoverage AS (
	SELECT
		SystemCoverageId,
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
		ISOMajorCrimeGroup
	FROM SystemCoverage
	WHERE SystemCoverage.SystemCoverageId is not null
),
EXP_Values AS (
	SELECT
	SystemCoverageId AS i_SystemCoverageId,
	CreatedDate AS i_CreatedDate,
	ModifiedDate AS i_ModifiedDate,
	ConformedCoverageId AS i_ConformedCoverageId,
	InsuranceLineCode AS i_InsuranceLineCode,
	InsuranceLineDescription AS i_InsuranceLineDescription,
	SourceSystemId AS i_SourceSystemId,
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
	LossHistoryCode AS i_LossHistoryCode,
	LossHistoryDescription AS i_LossHistoryDescription,
	ISOMajorCrimeGroup AS i_ISOMajorCrimeGroup,
	i_SystemCoverageId AS o_SystemCoverageId,
	-- *INF*: IIF(ISNULL(i_ConformedCoverageId), -99, i_ConformedCoverageId)
	IFF(i_ConformedCoverageId IS NULL, - 99, i_ConformedCoverageId) AS o_ConformedCoverageId,
	-- *INF*: IIF(ISNULL(i_InsuranceLineCode) OR LENGTH(i_InsuranceLineCode)=0 OR IS_SPACES(i_InsuranceLineCode), 'N/A', LTRIM(RTRIM(i_InsuranceLineCode)))
	IFF(i_InsuranceLineCode IS NULL OR LENGTH(i_InsuranceLineCode) = 0 OR IS_SPACES(i_InsuranceLineCode), 'N/A', LTRIM(RTRIM(i_InsuranceLineCode))) AS o_InsuranceLineCode,
	-- *INF*: IIF(ISNULL(i_InsuranceLineDescription) OR LENGTH(i_InsuranceLineDescription)=0 OR IS_SPACES(i_InsuranceLineDescription), 'Not Applicable', LTRIM(RTRIM(i_InsuranceLineDescription)))
	IFF(i_InsuranceLineDescription IS NULL OR LENGTH(i_InsuranceLineDescription) = 0 OR IS_SPACES(i_InsuranceLineDescription), 'Not Applicable', LTRIM(RTRIM(i_InsuranceLineDescription))) AS o_InsuranceLineDescription,
	-- *INF*: IIF(ISNULL(i_SourceSystemId) OR LENGTH(i_SourceSystemId)=0 OR IS_SPACES(i_SourceSystemId), 'N/A', LTRIM(RTRIM(i_SourceSystemId)))
	IFF(i_SourceSystemId IS NULL OR LENGTH(i_SourceSystemId) = 0 OR IS_SPACES(i_SourceSystemId), 'N/A', LTRIM(RTRIM(i_SourceSystemId))) AS o_SourceSystemId,
	-- *INF*: IIF(ISNULL(i_DctRiskTypeCode) OR LENGTH(i_DctRiskTypeCode)=0 OR IS_SPACES(i_DctRiskTypeCode), 'N/A', LTRIM(RTRIM(i_DctRiskTypeCode)))
	IFF(i_DctRiskTypeCode IS NULL OR LENGTH(i_DctRiskTypeCode) = 0 OR IS_SPACES(i_DctRiskTypeCode), 'N/A', LTRIM(RTRIM(i_DctRiskTypeCode))) AS o_DctRiskTypeCode,
	-- *INF*: IIF(ISNULL(i_DctCoverageTypeCode) OR LENGTH(i_DctCoverageTypeCode)=0 OR IS_SPACES(i_DctCoverageTypeCode), 'N/A', LTRIM(RTRIM(i_DctCoverageTypeCode)))
	IFF(i_DctCoverageTypeCode IS NULL OR LENGTH(i_DctCoverageTypeCode) = 0 OR IS_SPACES(i_DctCoverageTypeCode), 'N/A', LTRIM(RTRIM(i_DctCoverageTypeCode))) AS o_DctCoverageTypeCode,
	-- *INF*: IIF(ISNULL(i_PmsRiskUnitGroupCode) OR LENGTH(i_PmsRiskUnitGroupCode)=0 OR IS_SPACES(i_PmsRiskUnitGroupCode), 'N/A', LTRIM(RTRIM(i_PmsRiskUnitGroupCode)))
	IFF(i_PmsRiskUnitGroupCode IS NULL OR LENGTH(i_PmsRiskUnitGroupCode) = 0 OR IS_SPACES(i_PmsRiskUnitGroupCode), 'N/A', LTRIM(RTRIM(i_PmsRiskUnitGroupCode))) AS o_PmsRiskUnitGroupCode,
	-- *INF*: IIF(ISNULL(i_PmsRiskUnitGroupDescription) OR LENGTH(i_PmsRiskUnitGroupDescription)=0 OR IS_SPACES(i_PmsRiskUnitGroupDescription), 'Not Applicable', LTRIM(RTRIM(i_PmsRiskUnitGroupDescription)))
	IFF(i_PmsRiskUnitGroupDescription IS NULL OR LENGTH(i_PmsRiskUnitGroupDescription) = 0 OR IS_SPACES(i_PmsRiskUnitGroupDescription), 'Not Applicable', LTRIM(RTRIM(i_PmsRiskUnitGroupDescription))) AS o_PmsRiskUnitGroupDescription,
	-- *INF*: IIF(ISNULL(i_PmsRiskUnitCode) OR LENGTH(i_PmsRiskUnitCode)=0 OR IS_SPACES(i_PmsRiskUnitCode), 'N/A', LTRIM(RTRIM(i_PmsRiskUnitCode)))
	IFF(i_PmsRiskUnitCode IS NULL OR LENGTH(i_PmsRiskUnitCode) = 0 OR IS_SPACES(i_PmsRiskUnitCode), 'N/A', LTRIM(RTRIM(i_PmsRiskUnitCode))) AS o_PmsRiskUnitCode,
	-- *INF*: IIF(ISNULL(i_PmsRiskUnitDescription) OR LENGTH(i_PmsRiskUnitDescription)=0 OR IS_SPACES(i_PmsRiskUnitDescription), 'Not Applicable', LTRIM(RTRIM(i_PmsRiskUnitDescription)))
	IFF(i_PmsRiskUnitDescription IS NULL OR LENGTH(i_PmsRiskUnitDescription) = 0 OR IS_SPACES(i_PmsRiskUnitDescription), 'Not Applicable', LTRIM(RTRIM(i_PmsRiskUnitDescription))) AS o_PmsRiskUnitDescription,
	-- *INF*: IIF(ISNULL(i_PmsMajorPerilCode) OR LENGTH(i_PmsMajorPerilCode)=0 OR IS_SPACES(i_PmsMajorPerilCode), 'N/A', LTRIM(RTRIM(i_PmsMajorPerilCode)))
	IFF(i_PmsMajorPerilCode IS NULL OR LENGTH(i_PmsMajorPerilCode) = 0 OR IS_SPACES(i_PmsMajorPerilCode), 'N/A', LTRIM(RTRIM(i_PmsMajorPerilCode))) AS o_PmsMajorPerilCode,
	-- *INF*: IIF(ISNULL(i_PmsMajorPerilDescription) OR LENGTH(i_PmsMajorPerilDescription)=0 OR IS_SPACES(i_PmsMajorPerilDescription), 'Not Applicable', LTRIM(RTRIM(i_PmsMajorPerilDescription)))
	IFF(i_PmsMajorPerilDescription IS NULL OR LENGTH(i_PmsMajorPerilDescription) = 0 OR IS_SPACES(i_PmsMajorPerilDescription), 'Not Applicable', LTRIM(RTRIM(i_PmsMajorPerilDescription))) AS o_PmsMajorPerilDescription,
	-- *INF*: IIF(ISNULL(i_PmsProductTypeCode), 'N/A',LTRIM(RTRIM(i_PmsProductTypeCode)))
	IFF(i_PmsProductTypeCode IS NULL, 'N/A', LTRIM(RTRIM(i_PmsProductTypeCode))) AS o_PmsProductTypeCode,
	-- *INF*: IIF(ISNULL(i_DctPerilGroup),'N/A',i_DctPerilGroup)
	IFF(i_DctPerilGroup IS NULL, 'N/A', i_DctPerilGroup) AS o_DctPerilGroup,
	-- *INF*: IIF(ISNULL(i_DctSubCoverageTypeCode), 'N/A', i_DctSubCoverageTypeCode)
	IFF(i_DctSubCoverageTypeCode IS NULL, 'N/A', i_DctSubCoverageTypeCode) AS o_DctSubCoverageTypeCode,
	-- *INF*: IIF(ISNULL(i_DctCoverageVersion), 'N/A', i_DctCoverageVersion)
	IFF(i_DctCoverageVersion IS NULL, 'N/A', i_DctCoverageVersion) AS o_DctCoverageVersion,
	-- *INF*: IIF(ISNULL(i_LossHistoryCode), 'N/A', i_LossHistoryCode)
	IFF(i_LossHistoryCode IS NULL, 'N/A', i_LossHistoryCode) AS o_LossHistoryCode,
	-- *INF*: IIF(ISNULL(i_LossHistoryDescription), 'N/A', i_LossHistoryDescription)
	IFF(i_LossHistoryDescription IS NULL, 'N/A', i_LossHistoryDescription) AS o_LossHistoryDescription,
	-- *INF*: IIF(ISNULL(i_ISOMajorCrimeGroup)OR IS_SPACES(i_ISOMajorCrimeGroup)OR LENGTH(i_ISOMajorCrimeGroup)=0,'N/A',LTRIM(RTRIM(i_ISOMajorCrimeGroup)))
	IFF(i_ISOMajorCrimeGroup IS NULL OR IS_SPACES(i_ISOMajorCrimeGroup) OR LENGTH(i_ISOMajorCrimeGroup) = 0, 'N/A', LTRIM(RTRIM(i_ISOMajorCrimeGroup))) AS o_ISOMajorCrimeGroup
	FROM SQ_SystemCoverage
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
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctRiskTypeCode,DctCoverageTypeCode,PmsRiskUnitGroupCode,PmsRiskUnitCode,PmsMajorPerilCode,PmsProductTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY SystemCoverageId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_SystemCoverage.SystemCoverageId AS lkp_ExistingSystemCoverageId,
	LKP_SystemCoverage.ConformedCoverageId AS lkp_ExistingConformedCoverageId,
	LKP_SystemCoverage.InsuranceLineDescription AS lkp_InsuranceLineDescription,
	LKP_SystemCoverage.SourceSystemId AS lkp_SourceSystemId,
	LKP_SystemCoverage.PmsRiskUnitGroupDescription AS lkp_PmsRiskUnitGroupDescription,
	LKP_SystemCoverage.PmsRiskUnitDescription AS lkp_PmsRiskUnitDescription,
	LKP_SystemCoverage.PmsMajorPerilDescription AS lkp_PmsMajorPerilDescription,
	LKP_SystemCoverage.LossHistoryCode AS lkp_LossHistoryCode,
	LKP_SystemCoverage.LossHistoryDescription AS lkp_LossHistoryDescription,
	LKP_SystemCoverage.ISOMajorCrimeGroup AS lkp_ISOMajorCrimeGroup,
	EXP_Values.o_SystemCoverageId AS new_SystemCoverageId,
	EXP_Values.o_ConformedCoverageId AS new_ConformedCoverageId,
	EXP_Values.o_InsuranceLineCode AS InsuranceLineCode,
	EXP_Values.o_InsuranceLineDescription AS InsuranceLineDescription,
	EXP_Values.o_SourceSystemId AS SourceSystemId,
	EXP_Values.o_DctRiskTypeCode AS DctRiskTypeCode,
	EXP_Values.o_DctCoverageTypeCode AS DctCoverageTypeCode,
	EXP_Values.o_PmsRiskUnitGroupCode AS PmsRiskUnitGroupCode,
	EXP_Values.o_PmsRiskUnitGroupDescription AS PmsRiskUnitGroupDescription,
	EXP_Values.o_PmsRiskUnitCode AS PmsRiskUnitCode,
	EXP_Values.o_PmsRiskUnitDescription AS PmsRiskUnitDescription,
	EXP_Values.o_PmsMajorPerilCode AS PmsMajorPerilCode,
	EXP_Values.o_PmsMajorPerilDescription AS PmsMajorPerilDescription,
	EXP_Values.o_PmsProductTypeCode AS PmsProductTypeCode,
	EXP_Values.o_DctPerilGroup AS DctPerilGroup,
	EXP_Values.o_DctSubCoverageTypeCode AS DctSubCoverageTypeCode,
	EXP_Values.o_DctCoverageVersion AS DctCoverageVersion,
	EXP_Values.o_LossHistoryCode AS LossHistoryCode,
	EXP_Values.o_LossHistoryDescription AS LossHistoryDescription,
	EXP_Values.o_ISOMajorCrimeGroup AS ISOMajorCrimeGroup,
	-- *INF*: IIF(ISNULL(new_ConformedCoverageId), lkp_ExistingConformedCoverageId, new_ConformedCoverageId)
	IFF(new_ConformedCoverageId IS NULL, lkp_ExistingConformedCoverageId, new_ConformedCoverageId) AS o_ConformedCoverageId,
	-- *INF*: DECODE(true,
	-- ISNULL(lkp_ExistingSystemCoverageId),'Insert',
	-- new_ConformedCoverageId = lkp_ExistingConformedCoverageId
	-- AND lkp_InsuranceLineDescription=InsuranceLineDescription
	-- AND lkp_SourceSystemId=SourceSystemId
	-- AND lkp_PmsRiskUnitGroupDescription=PmsRiskUnitGroupDescription
	-- AND lkp_PmsRiskUnitDescription=PmsRiskUnitDescription
	-- AND lkp_PmsMajorPerilDescription=PmsMajorPerilDescription 
	-- AND lkp_LossHistoryCode=LossHistoryCode 
	-- AND lkp_LossHistoryDescription=LossHistoryDescription
	-- AND lkp_ISOMajorCrimeGroup=ISOMajorCrimeGroup,
	-- 'Ignore',
	-- 'Update')
	DECODE(true,
	lkp_ExistingSystemCoverageId IS NULL, 'Insert',
	new_ConformedCoverageId = lkp_ExistingConformedCoverageId AND lkp_InsuranceLineDescription = InsuranceLineDescription AND lkp_SourceSystemId = SourceSystemId AND lkp_PmsRiskUnitGroupDescription = PmsRiskUnitGroupDescription AND lkp_PmsRiskUnitDescription = PmsRiskUnitDescription AND lkp_PmsMajorPerilDescription = PmsMajorPerilDescription AND lkp_LossHistoryCode = LossHistoryCode AND lkp_LossHistoryDescription = LossHistoryDescription AND lkp_ISOMajorCrimeGroup = ISOMajorCrimeGroup, 'Ignore',
	'Update') AS o_InsertUpdateOrIgnore,
	SYSDATE AS o_CurrentDate
	FROM EXP_Values
	LEFT JOIN LKP_SystemCoverage
	ON LKP_SystemCoverage.InsuranceLineCode = EXP_Values.o_InsuranceLineCode AND LKP_SystemCoverage.DctRiskTypeCode = EXP_Values.o_DctRiskTypeCode AND LKP_SystemCoverage.DctCoverageTypeCode = EXP_Values.o_DctCoverageTypeCode AND LKP_SystemCoverage.PmsRiskUnitGroupCode = EXP_Values.o_PmsRiskUnitGroupCode AND LKP_SystemCoverage.PmsRiskUnitCode = EXP_Values.o_PmsRiskUnitCode AND LKP_SystemCoverage.PmsMajorPerilCode = EXP_Values.o_PmsMajorPerilCode AND LKP_SystemCoverage.PmsProductTypeCode = EXP_Values.o_PmsProductTypeCode AND LKP_SystemCoverage.DctPerilGroup = EXP_Values.o_DctPerilGroup AND LKP_SystemCoverage.DctSubCoverageTypeCode = EXP_Values.o_DctSubCoverageTypeCode AND LKP_SystemCoverage.DctCoverageVersion = EXP_Values.o_DctCoverageVersion
),
RTR_Insert_Update_Groups AS (
	SELECT
	lkp_ExistingSystemCoverageId,
	new_SystemCoverageId AS SystemCoverageId,
	o_ConformedCoverageId AS ConformedCoverageId,
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
	ISOMajorCrimeGroup,
	o_InsertUpdateOrIgnore AS InsertUpdateOrIgnore,
	o_CurrentDate AS CurrentDate
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_Groups_INSERT AS (SELECT * FROM RTR_Insert_Update_Groups WHERE InsertUpdateOrIgnore='Insert'),
RTR_Insert_Update_Groups_UPDATE AS (SELECT * FROM RTR_Insert_Update_Groups WHERE InsertUpdateOrIgnore='Update'),
UPD_SystemCoverage_Insert AS (
	SELECT
	SystemCoverageId, 
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
	ISOMajorCrimeGroup, 
	CurrentDate
	FROM RTR_Insert_Update_Groups_INSERT
),
SystemCoverage_Insert AS (
	SET QUOTED_IDENTIFIER ON;
	
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage(SystemCoverageId,CreatedDate,ModifiedDate,ConformedCoverageId,InsuranceLineCode,InsuranceLineDescription,SourceSystemId,DctRiskTypeCode,DctCoverageTypeCode,PmsRiskUnitGroupCode,PmsRiskUnitGroupDescription,PmsRiskUnitCode,PmsRiskUnitDescription,PmsMajorPerilCode,PmsMajorPerilDescription,PmsProductTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion,LossHistoryCode,LossHistoryDescription,ISOMajorCrimeGroup
	) 
	SELECT S.SystemCoverageId,S.CreatedDate,S.ModifiedDate,S.ConformedCoverageId,S.InsuranceLineCode,S.InsuranceLineDescription,S.SourceSystemId,S.DctRiskTypeCode,S.DctCoverageTypeCode,S.PmsRiskUnitGroupCode,S.PmsRiskUnitGroupDescription,S.PmsRiskUnitCode,S.PmsRiskUnitDescription,S.PmsMajorPerilCode,S.PmsMajorPerilDescription, S.PmsProductTypeCode,S.DctPerilGroup,S.DctSubCoverageTypeCode,S.DctCoverageVersion,S.LossHistoryCode,S.LossHistoryDescription,S.ISOMajorCrimeGroup
	FROM UPD_SystemCoverage_Insert S
),
UPD_SystemCoverage AS (
	SELECT
	lkp_ExistingSystemCoverageId AS lkp_ExistingSystemCoverageId3, 
	SystemCoverageId, 
	CurrentDate AS ModifiedDate, 
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
	ISOMajorCrimeGroup
	FROM RTR_Insert_Update_Groups_UPDATE
),
SystemCoverage_Update AS (
	SET QUOTED_IDENTIFIER ON;
	
	UPDATE @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage SET ModifiedDate = S.ModifiedDate, ConformedCoverageId = S.ConformedCoverageId, InsuranceLineCode = S.InsuranceLineCode, InsuranceLineDescription = S.InsuranceLineDescription, SourceSystemId = S.SourceSystemId, DctRiskTypeCode = S.DctRiskTypeCode, DctCoverageTypeCode = S.DctCoverageTypeCode, PmsRiskUnitGroupCode = S.PmsRiskUnitGroupCode, PmsRiskUnitGroupDescription = S.PmsRiskUnitGroupDescription, PmsRiskUnitCode = S.PmsRiskUnitCode, PmsRiskUnitDescription = S.PmsRiskUnitDescription, PmsMajorPerilCode = S.PmsMajorPerilCode, PmsMajorPerilDescription = S.PmsMajorPerilDescription, PmsProductTypeCode = S.PmsProductTypeCode, DctPerilGroup = S.DctPerilGroup, DctSubCoverageTypeCode = S.DctSubCoverageTypeCode, DctCoverageVersion = S.DctCoverageVersion,LossHistoryCode = S.LossHistoryCode, LossHistoryDescription=S.LossHistoryDescription,ISOMajorCrimeGroup = S.ISOMajorCrimeGroup WHERE SystemCoverageId = S.SystemCoverageId
	FROM UPD_SystemCoverage S
),