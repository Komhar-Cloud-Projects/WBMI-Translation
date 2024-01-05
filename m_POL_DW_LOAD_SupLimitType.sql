WITH
SQ_SupLimitType_CSV AS (

-- TODO Manual --

),
EXP_Default AS (
	SELECT
	SupLimitTypeId,
	CreatedDate,
	ModifiedDate,
	ModifiedUserId,
	InsuranceLine,
	CoverageType,
	LimitType,
	StandardLimitType,
	LimitLevel,
	AggregateableFlag,
	-- *INF*: SYSDATE
	-- 
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(LTRIM(RTRIM(CreatedDate))) OR LENGTH(LTRIM(RTRIM(CreatedDate)))=0, SYSDATE, TO_DATE(LTRIM(RTRIM(CreatedDate)),'MM/DD/YYYY'))
	SYSDATE AS o_CreatedDate,
	-- *INF*: SYSDATE
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(LTRIM(RTRIM(ModifiedDate))) OR LENGTH(LTRIM(RTRIM(ModifiedDate)))=0, SYSDATE, TO_DATE(LTRIM(RTRIM(ModifiedDate)),'MM/DD/YYYY'))
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ModifiedUserId))) OR LENGTH(LTRIM(RTRIM(ModifiedUserId)))=0 OR IS_SPACES(LTRIM(RTRIM(ModifiedUserId))), 'InformS', LTRIM(RTRIM(ModifiedUserId)))
	IFF(LTRIM(RTRIM(ModifiedUserId)) IS NULL OR LENGTH(LTRIM(RTRIM(ModifiedUserId))) = 0 OR IS_SPACES(LTRIM(RTRIM(ModifiedUserId))), 'InformS', LTRIM(RTRIM(ModifiedUserId))) AS o_ModifiedUserId,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(InsuranceLine))) OR LENGTH(LTRIM(RTRIM(InsuranceLine)))=0 OR IS_SPACES(LTRIM(RTRIM(InsuranceLine))), 'InformS', LTRIM(RTRIM(InsuranceLine)))
	IFF(LTRIM(RTRIM(InsuranceLine)) IS NULL OR LENGTH(LTRIM(RTRIM(InsuranceLine))) = 0 OR IS_SPACES(LTRIM(RTRIM(InsuranceLine))), 'InformS', LTRIM(RTRIM(InsuranceLine))) AS o_InsuranceLine,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(CoverageType))) OR LENGTH(LTRIM(RTRIM(CoverageType)))=0 OR IS_SPACES(LTRIM(RTRIM(CoverageType))), 'N/A', LTRIM(RTRIM(CoverageType)))
	IFF(LTRIM(RTRIM(CoverageType)) IS NULL OR LENGTH(LTRIM(RTRIM(CoverageType))) = 0 OR IS_SPACES(LTRIM(RTRIM(CoverageType))), 'N/A', LTRIM(RTRIM(CoverageType))) AS o_CoverageType,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(LimitType))) OR LENGTH(LTRIM(RTRIM(LimitType)))=0 OR IS_SPACES(LTRIM(RTRIM(LimitType))), 'N/A', LTRIM(RTRIM(LimitType)))
	IFF(LTRIM(RTRIM(LimitType)) IS NULL OR LENGTH(LTRIM(RTRIM(LimitType))) = 0 OR IS_SPACES(LTRIM(RTRIM(LimitType))), 'N/A', LTRIM(RTRIM(LimitType))) AS o_LimitType,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(StandardLimitType))) OR LENGTH(LTRIM(RTRIM(StandardLimitType)))=0 OR IS_SPACES(LTRIM(RTRIM(StandardLimitType))), 'N/A', LTRIM(RTRIM(StandardLimitType)))
	IFF(LTRIM(RTRIM(StandardLimitType)) IS NULL OR LENGTH(LTRIM(RTRIM(StandardLimitType))) = 0 OR IS_SPACES(LTRIM(RTRIM(StandardLimitType))), 'N/A', LTRIM(RTRIM(StandardLimitType))) AS o_StandardLimitType,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(LimitLevel))) OR LENGTH(LTRIM(RTRIM(LimitLevel)))=0 OR IS_SPACES(LTRIM(RTRIM(LimitLevel))), 'N/A', LTRIM(RTRIM(LimitLevel)))
	IFF(LTRIM(RTRIM(LimitLevel)) IS NULL OR LENGTH(LTRIM(RTRIM(LimitLevel))) = 0 OR IS_SPACES(LTRIM(RTRIM(LimitLevel))), 'N/A', LTRIM(RTRIM(LimitLevel))) AS o_LimitLevel,
	AggregateableFlag AS o_AdditiveFlag
	FROM SQ_SupLimitType_CSV
),
AGG_SourceFileChecks AS (
	SELECT
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_ModifiedUserId AS ModifiedUserId, 
	o_InsuranceLine AS InsuranceLine, 
	o_CoverageType AS CoverageType, 
	o_LimitType AS LimitType, 
	o_StandardLimitType AS StandardLimitType, 
	o_LimitLevel AS LimitLevel, 
	o_AdditiveFlag AS AdditiveFlag, 
	COUNT(1) AS o_Count
	FROM EXP_Default
	GROUP BY InsuranceLine, CoverageType, LimitType
),
LKP_SupLimitType AS (
	SELECT
	SupLimitTypeId,
	ModifiedUserId,
	StandardLimitType,
	LimitLevel,
	AdditiveFlag,
	InsuranceLine,
	CoverageType,
	LimitType
	FROM (
		SELECT 
			SupLimitTypeId,
			ModifiedUserId,
			StandardLimitType,
			LimitLevel,
			AdditiveFlag,
			InsuranceLine,
			CoverageType,
			LimitType
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupLimitType
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLine,CoverageType,LimitType ORDER BY SupLimitTypeId) = 1
),
EXP_DetectChange AS (
	SELECT
	LKP_SupLimitType.SupLimitTypeId AS lkp_SupLimitTypeId,
	LKP_SupLimitType.ModifiedUserId AS lkp_ModifiedUserId,
	LKP_SupLimitType.StandardLimitType AS lkp_StandardLimitType,
	LKP_SupLimitType.LimitLevel AS lkp_LimitLevel,
	LKP_SupLimitType.AdditiveFlag AS lkp_AdditiveFlag,
	AGG_SourceFileChecks.o_Count AS i_Count,
	AGG_SourceFileChecks.CreatedDate,
	AGG_SourceFileChecks.ModifiedDate,
	AGG_SourceFileChecks.ModifiedUserId,
	AGG_SourceFileChecks.InsuranceLine,
	AGG_SourceFileChecks.CoverageType,
	AGG_SourceFileChecks.LimitType,
	AGG_SourceFileChecks.StandardLimitType,
	AGG_SourceFileChecks.LimitLevel,
	AGG_SourceFileChecks.AdditiveFlag,
	'The source file is not good enough to be loaded into the target table. Please check and correct it. The source rows are - InsuranceLine: ' || InsuranceLine || ', CoverageType: ' || CoverageType || ', LimitType: ' || LimitType AS v_Message,
	-- *INF*: IIF(i_Count > 1, ABORT(v_Message))
	IFF(i_Count > 1, ABORT(v_Message)) AS v_ErrorOut,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_SupLimitTypeId),'New',
	-- lkp_ModifiedUserId!=ModifiedUserId OR 
	-- lkp_StandardLimitType!=StandardLimitType OR 
	-- lkp_LimitLevel != LimitLevel OR 
	-- IIF(lkp_AdditiveFlag='T',1,0) !=AdditiveFlag, 'Update',
	-- 'NoChange')
	DECODE(TRUE,
	lkp_SupLimitTypeId IS NULL, 'New',
	lkp_ModifiedUserId != ModifiedUserId OR lkp_StandardLimitType != StandardLimitType OR lkp_LimitLevel != LimitLevel OR IFF(lkp_AdditiveFlag = 'T', 1, 0) != AdditiveFlag, 'Update',
	'NoChange') AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM AGG_SourceFileChecks
	LEFT JOIN LKP_SupLimitType
	ON LKP_SupLimitType.InsuranceLine = AGG_SourceFileChecks.InsuranceLine AND LKP_SupLimitType.CoverageType = AGG_SourceFileChecks.CoverageType AND LKP_SupLimitType.LimitType = AGG_SourceFileChecks.LimitType
),
RTR_InsertOrUpdate AS (
	SELECT
	lkp_SupLimitTypeId,
	CreatedDate,
	ModifiedDate,
	ModifiedUserId,
	InsuranceLine,
	CoverageType,
	LimitType,
	StandardLimitType,
	LimitLevel,
	AdditiveFlag,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_DetectChange
),
RTR_InsertOrUpdate_Insert AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag='New'),
RTR_InsertOrUpdate_Update AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag='Update'),
SupLimitType_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupLimitType
	(CreatedDate, ModifiedDate, ModifiedUserId, InsuranceLine, CoverageType, LimitType, StandardLimitType, LimitLevel, AdditiveFlag)
	SELECT 
	CREATEDDATE, 
	MODIFIEDDATE, 
	MODIFIEDUSERID, 
	INSURANCELINE, 
	COVERAGETYPE, 
	LIMITTYPE, 
	STANDARDLIMITTYPE, 
	LIMITLEVEL, 
	ADDITIVEFLAG
	FROM RTR_InsertOrUpdate_Insert
),
UPD_Update AS (
	SELECT
	lkp_SupLimitTypeId AS lkp_SupLimitTypeId3, 
	ModifiedDate AS ModifiedDate3, 
	ModifiedUserId AS ModifiedUserId3, 
	StandardLimitType AS StandardLimitType3, 
	LimitLevel AS LimitLevel3, 
	AdditiveFlag AS AdditiveFlag3
	FROM RTR_InsertOrUpdate_Update
),
SupLimitType_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupLimitType AS T
	USING UPD_Update AS S
	ON T.SupLimitTypeId = S.lkp_SupLimitTypeId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate3, T.ModifiedUserId = S.ModifiedUserId3, T.StandardLimitType = S.StandardLimitType3, T.LimitLevel = S.LimitLevel3, T.AdditiveFlag = S.AdditiveFlag3
),