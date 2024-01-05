WITH
SQ_SupDeductibleType_CSV AS (

-- TODO Manual --

),
EXP_Default AS (
	SELECT
	SupDeductibleTypeId,
	CreatedDate,
	ModifiedDate,
	ModifiedUserId,
	SourceSystemId,
	InsuranceLine,
	CoverageType,
	DeductibleType,
	RiskUnitGroupCode,
	RiskUnitGroupDescription,
	StandardDeductibleType,
	DeductibleLevel,
	-- *INF*: SYSDATE
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(LTRIM(RTRIM(CreatedDate))) OR LENGTH(LTRIM(RTRIM(CreatedDate)))=0, SYSDATE, TO_DATE(LTRIM(RTRIM(CreatedDate)),'MM/DD/YYYY'))
	-- 
	SYSDATE AS o_CreatedDate,
	-- *INF*: SYSDATE
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(LTRIM(RTRIM(ModifiedDate))) OR LENGTH(LTRIM(RTRIM(ModifiedDate)))=0, SYSDATE, TO_DATE(LTRIM(RTRIM(ModifiedDate)),'MM/DD/YYYY'))
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ModifiedUserId))) OR LENGTH(LTRIM(RTRIM(ModifiedUserId)))=0 OR IS_SPACES(LTRIM(RTRIM(ModifiedUserId))), 'InformS', LTRIM(RTRIM(ModifiedUserId)))
	IFF(LTRIM(RTRIM(ModifiedUserId)) IS NULL OR LENGTH(LTRIM(RTRIM(ModifiedUserId))) = 0 OR IS_SPACES(LTRIM(RTRIM(ModifiedUserId))), 'InformS', LTRIM(RTRIM(ModifiedUserId))) AS o_ModifiedUserId,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(SourceSystemId))) OR LENGTH(LTRIM(RTRIM(SourceSystemId)))=0 OR IS_SPACES(LTRIM(RTRIM(SourceSystemId))), 'N/A', LTRIM(RTRIM(SourceSystemId)))
	IFF(LTRIM(RTRIM(SourceSystemId)) IS NULL OR LENGTH(LTRIM(RTRIM(SourceSystemId))) = 0 OR IS_SPACES(LTRIM(RTRIM(SourceSystemId))), 'N/A', LTRIM(RTRIM(SourceSystemId))) AS o_SourceSystemId,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(InsuranceLine))) OR LENGTH(LTRIM(RTRIM(InsuranceLine)))=0 OR IS_SPACES(LTRIM(RTRIM(InsuranceLine))), 'N/A', LTRIM(RTRIM(InsuranceLine)))
	IFF(LTRIM(RTRIM(InsuranceLine)) IS NULL OR LENGTH(LTRIM(RTRIM(InsuranceLine))) = 0 OR IS_SPACES(LTRIM(RTRIM(InsuranceLine))), 'N/A', LTRIM(RTRIM(InsuranceLine))) AS o_InsuranceLine,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(CoverageType))) OR LENGTH(LTRIM(RTRIM(CoverageType)))=0 OR IS_SPACES(LTRIM(RTRIM(CoverageType))), 'N/A', LTRIM(RTRIM(CoverageType)))
	IFF(LTRIM(RTRIM(CoverageType)) IS NULL OR LENGTH(LTRIM(RTRIM(CoverageType))) = 0 OR IS_SPACES(LTRIM(RTRIM(CoverageType))), 'N/A', LTRIM(RTRIM(CoverageType))) AS o_CoverageType,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(DeductibleType))) OR LENGTH(LTRIM(RTRIM(DeductibleType)))=0 OR IS_SPACES(LTRIM(RTRIM(DeductibleType))), 'N/A', LTRIM(RTRIM(DeductibleType)))
	IFF(LTRIM(RTRIM(DeductibleType)) IS NULL OR LENGTH(LTRIM(RTRIM(DeductibleType))) = 0 OR IS_SPACES(LTRIM(RTRIM(DeductibleType))), 'N/A', LTRIM(RTRIM(DeductibleType))) AS o_DeductibleType,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(RiskUnitGroupCode))) OR LENGTH(LTRIM(RTRIM(RiskUnitGroupCode)))=0 OR IS_SPACES(LTRIM(RTRIM(RiskUnitGroupCode))), 'N/A', LTRIM(RTRIM(RiskUnitGroupCode)))
	IFF(LTRIM(RTRIM(RiskUnitGroupCode)) IS NULL OR LENGTH(LTRIM(RTRIM(RiskUnitGroupCode))) = 0 OR IS_SPACES(LTRIM(RTRIM(RiskUnitGroupCode))), 'N/A', LTRIM(RTRIM(RiskUnitGroupCode))) AS o_RiskUnitGroupCode,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(RiskUnitGroupDescription))) OR LENGTH(LTRIM(RTRIM(RiskUnitGroupDescription)))=0 OR IS_SPACES(LTRIM(RTRIM(RiskUnitGroupDescription))), 'N/A', LTRIM(RTRIM(RiskUnitGroupDescription)))
	IFF(LTRIM(RTRIM(RiskUnitGroupDescription)) IS NULL OR LENGTH(LTRIM(RTRIM(RiskUnitGroupDescription))) = 0 OR IS_SPACES(LTRIM(RTRIM(RiskUnitGroupDescription))), 'N/A', LTRIM(RTRIM(RiskUnitGroupDescription))) AS o_RiskUnitGroupDescription,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(StandardDeductibleType))) OR LENGTH(LTRIM(RTRIM(StandardDeductibleType)))=0 OR IS_SPACES(LTRIM(RTRIM(StandardDeductibleType))), 'N/A', LTRIM(RTRIM(StandardDeductibleType)))
	IFF(LTRIM(RTRIM(StandardDeductibleType)) IS NULL OR LENGTH(LTRIM(RTRIM(StandardDeductibleType))) = 0 OR IS_SPACES(LTRIM(RTRIM(StandardDeductibleType))), 'N/A', LTRIM(RTRIM(StandardDeductibleType))) AS o_StandardDeductibleType,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(DeductibleLevel))) OR LENGTH(LTRIM(RTRIM(DeductibleLevel)))=0 OR IS_SPACES(LTRIM(RTRIM(DeductibleLevel))), 'N/A', LTRIM(RTRIM(DeductibleLevel)))
	IFF(LTRIM(RTRIM(DeductibleLevel)) IS NULL OR LENGTH(LTRIM(RTRIM(DeductibleLevel))) = 0 OR IS_SPACES(LTRIM(RTRIM(DeductibleLevel))), 'N/A', LTRIM(RTRIM(DeductibleLevel))) AS o_DeductibleLevel
	FROM SQ_SupDeductibleType_CSV
),
AGG_SourceFileChecks AS (
	SELECT
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_ModifiedUserId AS ModifiedUserId, 
	o_SourceSystemId AS SourceSystemId, 
	o_InsuranceLine AS InsuranceLine, 
	o_CoverageType AS CoverageType, 
	o_DeductibleType AS DeductibleType, 
	o_RiskUnitGroupCode AS RiskUnitGroupCode, 
	o_RiskUnitGroupDescription AS RiskUnitGroupDescription, 
	o_StandardDeductibleType AS StandardDeductibleType, 
	o_DeductibleLevel AS DeductibleLevel, 
	COUNT(1) AS o_Count
	FROM EXP_Default
	GROUP BY SourceSystemId, InsuranceLine, CoverageType, DeductibleType, RiskUnitGroupCode, RiskUnitGroupDescription
),
LKP_SupDeductibleType AS (
	SELECT
	SupDeductibleTypeId,
	ModifiedUserId,
	StandardDeductibleType,
	DeductibleLevel,
	SourceSystemId,
	InsuranceLine,
	CoverageType,
	DeductibleType,
	RiskUnitGroupCode,
	RiskUnitGroupDescription
	FROM (
		SELECT SupDeductibleTypeId as SupDeductibleTypeId, 
		LTRIM(RTRIM(ModifiedUserId)) as ModifiedUserId, 
		LTRIM(RTRIM(SourceSystemId)) as SourceSystemId, 
		LTRIM(RTRIM(InsuranceLine)) as InsuranceLine, 
		LTRIM(RTRIM(CoverageType)) as CoverageType, 
		LTRIM(RTRIM(DeductibleType)) as DeductibleType, 
		LTRIM(RTRIM(RiskUnitGroupCode)) as RiskUnitGroupCode, 
		LTRIM(RTRIM(RiskUnitGroupDescription)) as RiskUnitGroupDescription,
		LTRIM(RTRIM(StandardDeductibleType)) as StandardDeductibleType, 
		LTRIM(RTRIM(DeductibleLevel)) as DeductibleLevel 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDeductibleType
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourceSystemId,InsuranceLine,CoverageType,DeductibleType,RiskUnitGroupCode,RiskUnitGroupDescription ORDER BY SupDeductibleTypeId) = 1
),
EXP_DetectChange AS (
	SELECT
	LKP_SupDeductibleType.SupDeductibleTypeId AS lkp_SupDeductibleTypeId,
	LKP_SupDeductibleType.ModifiedUserId AS lkp_ModifiedUserId,
	LKP_SupDeductibleType.StandardDeductibleType AS lkp_StandardDeductibleType,
	LKP_SupDeductibleType.DeductibleLevel AS lkp_DeductibleLevel,
	AGG_SourceFileChecks.CreatedDate,
	AGG_SourceFileChecks.ModifiedDate,
	AGG_SourceFileChecks.ModifiedUserId,
	AGG_SourceFileChecks.SourceSystemId,
	AGG_SourceFileChecks.InsuranceLine,
	AGG_SourceFileChecks.CoverageType,
	AGG_SourceFileChecks.DeductibleType,
	AGG_SourceFileChecks.RiskUnitGroupCode,
	AGG_SourceFileChecks.RiskUnitGroupDescription,
	AGG_SourceFileChecks.StandardDeductibleType,
	AGG_SourceFileChecks.DeductibleLevel,
	AGG_SourceFileChecks.o_Count AS Count,
	'The source file is not good enough to be loaded into the target table. Please check and correct it. The source rows are - SourceSystemId: ' || SourceSystemId ||  ', InsuranceLine: ' || InsuranceLine || ', CoverageType: ' || CoverageType || ', DeductibleType: ' || DeductibleType  || ', RiskUnitGroupCode: ' || RiskUnitGroupCode || ', RiskUnitGroupDescription: ' || RiskUnitGroupDescription AS v_Message,
	-- *INF*: IIF(Count > 1, ABORT(v_Message))
	IFF(Count > 1, ABORT(v_Message)) AS v_ErrorOut,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_SupDeductibleTypeId), 'New',
	-- lkp_ModifiedUserId!=ModifiedUserId OR
	-- lkp_StandardDeductibleType!=StandardDeductibleType OR
	-- lkp_DeductibleLevel!=DeductibleLevel, 'Update',
	-- 'NoChange')
	DECODE(TRUE,
	lkp_SupDeductibleTypeId IS NULL, 'New',
	lkp_ModifiedUserId != ModifiedUserId OR lkp_StandardDeductibleType != StandardDeductibleType OR lkp_DeductibleLevel != DeductibleLevel, 'Update',
	'NoChange') AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM AGG_SourceFileChecks
	LEFT JOIN LKP_SupDeductibleType
	ON LKP_SupDeductibleType.SourceSystemId = AGG_SourceFileChecks.SourceSystemId AND LKP_SupDeductibleType.InsuranceLine = AGG_SourceFileChecks.InsuranceLine AND LKP_SupDeductibleType.CoverageType = AGG_SourceFileChecks.CoverageType AND LKP_SupDeductibleType.DeductibleType = AGG_SourceFileChecks.DeductibleType AND LKP_SupDeductibleType.RiskUnitGroupCode = AGG_SourceFileChecks.RiskUnitGroupCode AND LKP_SupDeductibleType.RiskUnitGroupDescription = AGG_SourceFileChecks.RiskUnitGroupDescription
),
RTR_InsertOrUpdate AS (
	SELECT
	lkp_SupDeductibleTypeId,
	CreatedDate,
	ModifiedDate,
	ModifiedUserId,
	SourceSystemId,
	InsuranceLine,
	CoverageType,
	DeductibleType,
	RiskUnitGroupCode,
	RiskUnitGroupDescription,
	StandardDeductibleType,
	DeductibleLevel,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_DetectChange
),
RTR_InsertOrUpdate_Insert AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag='New'),
RTR_InsertOrUpdate_Update AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag='Update'),
UPD_Insert AS (
	SELECT
	CreatedDate AS CreatedDate1, 
	ModifiedDate AS ModifiedDate1, 
	ModifiedUserId AS ModifiedUserId1, 
	SourceSystemId AS SourceSystemId1, 
	InsuranceLine AS InsuranceLine1, 
	CoverageType AS CoverageType1, 
	DeductibleType AS DeductibleType1, 
	RiskUnitGroupCode AS RiskUnitGroupCode1, 
	RiskUnitGroupDescription AS RiskUnitGroupDescription1, 
	StandardDeductibleType AS StandardDeductibleType1, 
	DeductibleLevel AS DeductibleLevel1
	FROM RTR_InsertOrUpdate_Insert
),
SupDeductibleType_Insert AS (
	SET QUOTED_IDENTIFIER ON;
	
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDeductibleType 
	(CreatedDate,ModifiedDate,ModifiedUserId ,SourceSystemId ,InsuranceLine ,CoverageType,DeductibleType,RiskUnitGroupCode, RiskUnitGroupDescription ,StandardDeductibleType ,DeductibleLevel 
	) 
	SELECT  S.CreatedDate, S.ModifiedDate, S.ModifiedUserId, S.SourceSystemId, S.InsuranceLine, S.CoverageType, S.DeductibleType, S.RiskUnitGroupCode, S.RiskUnitGroupDescription,  S.StandardDeductibleType, S.DeductibleLevel
	FROM UPD_Insert S
),
UPD_Update AS (
	SELECT
	lkp_SupDeductibleTypeId AS lkp_SupDeductibleTypeId3, 
	ModifiedDate AS ModifiedDate3, 
	ModifiedUserId AS ModifiedUserId3, 
	SourceSystemId AS SourceSystemId3, 
	InsuranceLine AS InsuranceLine3, 
	CoverageType AS CoverageType3, 
	DeductibleType AS DeductibleType3, 
	RiskUnitGroupCode AS RiskUnitGroupCode3, 
	RiskUnitGroupDescription AS RiskUnitGroupDescription3, 
	StandardDeductibleType AS StandardDeductibleType3, 
	DeductibleLevel AS DeductibleLevel3
	FROM RTR_InsertOrUpdate_Update
),
SupDeductibleType_Update AS (
	SET QUOTED_IDENTIFIER ON;
	
	UPDATE @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDeductibleType SET ModifiedDate = S.ModifiedDate, ModifiedUserId = S.ModifiedUserId, SourceSystemId = S.SourceSystemId, InsuranceLine = S.InsuranceLine, CoverageType = S.CoverageType, DeductibleType = S.DeductibleType, RiskUnitGroupCode = S.RiskUnitGroupCode, RiskUnitGroupDescription = S.RiskUnitGroupDescription, StandardDeductibleType = S.StandardDeductibleType, DeductibleLevel = S.DeductibleLevel WHERE SupDeductibleTypeId = S.SupDeductibleTypeId
	FROM UPD_Update S
),