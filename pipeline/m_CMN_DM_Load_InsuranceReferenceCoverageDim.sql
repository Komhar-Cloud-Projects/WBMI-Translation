WITH
SQ_CoverageSummary AS (
	SELECT SC.InsuranceLineCode InsuranceLineCode
	,SC.InsuranceLineDescription InsuranceLineDescription
	,CC.CoverageCode CoverageCode
	,CC.CoverageDescription CoverageDescription
	,CG.CoverageGroupCode CoverageGroupCode
	,CG.CoverageGroupDescription CoverageGroupDescription
	,CS.CoverageSummaryCode CoverageSummaryCode
	,CS.CoverageSummaryDescription CoverageSummaryDescription
	,SC.DctRiskTypeCode DctRiskTypeCode
	,SC.DctCoverageTypeCode DctCoverageTypeCode
	,SC.PmsRiskUnitGroupCode PmsRiskUnitGroupCode
	,SC.PmsRiskUnitGroupDescription PmsRiskUnitGroupDescription
	,SC.PmsRiskUnitCode PmsRiskUnitCode
	,SC.PmsRiskUnitDescription PmsRiskUnitDescription
	,SC.PmsMajorPerilCode PmsMajorPerilCode
	,SC.PmsMajorPerilDescription PmsMajorPerilDescription
	,SC.PmsProductTypeCode PmsProductTypeCode
	,SC.DctPerilGroup DctPerilGroup
	,SC.DctSubCoverageTypeCode DctSubCoverageTypeCode
	,SC.DctCoverageVersion DctCoverageVersion
	,SC.LossHistoryCode
	,SC.LossHistoryDescription
	,SC.ISOMajorCrimeGroup
	,CC.RatedCoverageCode RatedCoverageCode 
	,CC.RatedCoverageDescription RatedCoverageDescription 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageSummary CS
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageGroup CG
	ON CS.CoverageSummaryId=CG.CoverageSummaryId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.ConformedCoverage CC
	ON CG.CoverageGroupId=CC.CoverageGroupId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SystemCoverage SC
	ON CC.ConformedCoverageId=SC.ConformedCoverageId
),
LKP_InsuranceReferenceCoverageDim AS (
	SELECT
	InsuranceReferenceCoverageDimId,
	CoverageGroupCode,
	CoverageSummaryCode,
	CoverageGroupDescription,
	CoverageSummaryDescription,
	CoverageCode,
	CoverageDescription,
	InsuranceLineDescription,
	PmsRiskUnitGroupDescription,
	PmsRiskUnitDescription,
	PmsMajorPerilDescription,
	LossHistoryCode,
	LossHistoryDescription,
	ISOMajorCrimeGroup,
	RatedCoverageCode,
	RatedCoverageDescription,
	InsuranceLineCode,
	DctCoverageTypeCode,
	DctSubCoverageTypeCode,
	DctRiskTypeCode,
	DctPerilGroup,
	DctCoverageVersion,
	PmsRiskUnitGroupCode,
	PmsRiskUnitCode,
	PmsMajorPerilCode,
	PmsProductTypeCode
	FROM (
		SELECT 
			InsuranceReferenceCoverageDimId,
			CoverageGroupCode,
			CoverageSummaryCode,
			CoverageGroupDescription,
			CoverageSummaryDescription,
			CoverageCode,
			CoverageDescription,
			InsuranceLineDescription,
			PmsRiskUnitGroupDescription,
			PmsRiskUnitDescription,
			PmsMajorPerilDescription,
			LossHistoryCode,
			LossHistoryDescription,
			ISOMajorCrimeGroup,
			RatedCoverageCode,
			RatedCoverageDescription,
			InsuranceLineCode,
			DctCoverageTypeCode,
			DctSubCoverageTypeCode,
			DctRiskTypeCode,
			DctPerilGroup,
			DctCoverageVersion,
			PmsRiskUnitGroupCode,
			PmsRiskUnitCode,
			PmsMajorPerilCode,
			PmsProductTypeCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctCoverageTypeCode,DctSubCoverageTypeCode,DctRiskTypeCode,DctPerilGroup,DctCoverageVersion,PmsRiskUnitGroupCode,PmsRiskUnitCode,PmsMajorPerilCode,PmsProductTypeCode ORDER BY InsuranceReferenceCoverageDimId) = 1
),
EXP_Detect_Changes_Add_MetaData AS (
	SELECT
	LKP_InsuranceReferenceCoverageDim.InsuranceReferenceCoverageDimId AS lkp_InsuranceReferenceCoverageDimId,
	LKP_InsuranceReferenceCoverageDim.CoverageGroupCode AS lkp_CoverageGroupCode,
	LKP_InsuranceReferenceCoverageDim.CoverageSummaryCode AS lkp_CoverageSummaryCode,
	LKP_InsuranceReferenceCoverageDim.CoverageGroupDescription AS lkp_CoverageGroupDescription,
	LKP_InsuranceReferenceCoverageDim.CoverageSummaryDescription AS lkp_CoverageSummaryDescription,
	LKP_InsuranceReferenceCoverageDim.CoverageCode AS lkp_CoverageCode,
	LKP_InsuranceReferenceCoverageDim.CoverageDescription AS lkp_CoverageDescription,
	LKP_InsuranceReferenceCoverageDim.InsuranceLineDescription AS lkp_InsuranceLineDescription,
	LKP_InsuranceReferenceCoverageDim.PmsRiskUnitGroupDescription AS lkp_PmsRiskUnitGroupDescription,
	LKP_InsuranceReferenceCoverageDim.PmsRiskUnitDescription AS lkp_PmsRiskUnitDescription,
	LKP_InsuranceReferenceCoverageDim.PmsMajorPerilDescription AS lkp_PmsMajorPerilDescription,
	LKP_InsuranceReferenceCoverageDim.LossHistoryCode AS lkp_LossHistoryCode,
	LKP_InsuranceReferenceCoverageDim.LossHistoryDescription AS lkp_LossHistoryDescription,
	LKP_InsuranceReferenceCoverageDim.ISOMajorCrimeGroup AS lkp_ISOMajorCrimeGroup,
	LKP_InsuranceReferenceCoverageDim.RatedCoverageCode AS lkp_RatedCoverageCode,
	LKP_InsuranceReferenceCoverageDim.RatedCoverageDescription AS lkp_RatedCoverageDescription,
	SQ_CoverageSummary.InsuranceLineCode AS i_InsuranceLineCode,
	SQ_CoverageSummary.InsuranceLineDescription AS i_InsuranceLineDescription,
	SQ_CoverageSummary.CoverageCode AS i_CoverageCode,
	SQ_CoverageSummary.CoverageDescription AS i_CoverageDescription,
	SQ_CoverageSummary.CoverageGroupCode AS i_CoverageGroupCode,
	SQ_CoverageSummary.CoverageGroupDescription AS i_CoverageGroupDescription,
	SQ_CoverageSummary.CoverageSummaryCode AS i_CoverageSummaryCode,
	SQ_CoverageSummary.CoverageSummaryDescription AS i_CoverageSummaryDescription,
	SQ_CoverageSummary.DctRiskTypeCode AS i_DctRiskTypeCode,
	SQ_CoverageSummary.DctCoverageTypeCode AS i_DctCoverageTypeCode,
	SQ_CoverageSummary.PmsRiskUnitGroupCode AS i_PmsRiskUnitGroupCode,
	SQ_CoverageSummary.PmsRiskUnitGroupDescription AS i_PmsRiskUnitGroupDescription,
	SQ_CoverageSummary.PmsRiskUnitCode AS i_PmsRiskUnitCode,
	SQ_CoverageSummary.PmsRiskUnitDescription AS i_PmsRiskUnitDescription,
	SQ_CoverageSummary.PmsMajorPerilCode AS i_PmsMajorPerilCode,
	SQ_CoverageSummary.PmsMajorPerilDescription AS i_PmsMajorPerilDescription,
	SQ_CoverageSummary.PmsProductTypeCode AS i_PmsProductTypeCode,
	SQ_CoverageSummary.DctPerilGroup AS i_DctPerilGroup,
	SQ_CoverageSummary.DctSubCoverageTypeCode AS i_DctSubCoverageTypeCode,
	SQ_CoverageSummary.DctCoverageVersion AS i_DctCoverageVersion,
	SQ_CoverageSummary.LossHistoryCode AS i_LossHistoryCode,
	SQ_CoverageSummary.LossHistoryDescription AS i_LossHistoryDescription,
	SQ_CoverageSummary.ISOMajorCrimeGroup AS i_ISOMajorCrimeGroup,
	SQ_CoverageSummary.RatedCoverageCode AS i_RatedCoverageCode,
	SQ_CoverageSummary.RatedCoverageDescription AS i_RatedCoverageDescription,
	lkp_InsuranceReferenceCoverageDimId AS o_InsuranceReferenceCoverageDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_InsuranceLineCode AS o_InsuranceLineCode,
	i_InsuranceLineDescription AS o_InsuranceLineDescription,
	i_CoverageCode AS o_CoverageCode,
	i_CoverageDescription AS o_CoverageDescription,
	i_CoverageGroupCode AS o_CoverageGroupCode,
	i_CoverageGroupDescription AS o_CoverageGroupDescription,
	i_CoverageSummaryCode AS o_CoverageSummaryCode,
	i_CoverageSummaryDescription AS o_CoverageSummaryDescription,
	i_DctRiskTypeCode AS o_DctRiskTypeCode,
	i_DctCoverageTypeCode AS o_DctCoverageTypeCode,
	i_PmsRiskUnitGroupCode AS o_PmsRiskUnitGroupCode,
	i_PmsRiskUnitGroupDescription AS o_PmsRiskUnitGroupDescription,
	i_PmsRiskUnitCode AS o_PmsRiskUnitCode,
	i_PmsRiskUnitDescription AS o_PmsRiskUnitDescription,
	i_PmsMajorPerilCode AS o_PmsMajorPerilCode,
	i_PmsMajorPerilDescription AS o_PmsMajorPerilDescription,
	i_PmsProductTypeCode AS o_PmsProductTypeCode,
	i_DctPerilGroup AS o_DctPerilGroup,
	i_DctSubCoverageTypeCode AS o_DctSubCoverageTypeCode,
	i_DctCoverageVersion AS o_DctCoverageVersion,
	i_LossHistoryCode AS o_LossHistoryCode,
	i_LossHistoryDescription AS o_LossHistoryDescription,
	-- *INF*: IIF(ISNULL(i_ISOMajorCrimeGroup)OR IS_SPACES(i_ISOMajorCrimeGroup)OR LENGTH(i_ISOMajorCrimeGroup)=0,'N/A',LTRIM(RTRIM(i_ISOMajorCrimeGroup)))
	IFF(
	    i_ISOMajorCrimeGroup IS NULL
	    or LENGTH(i_ISOMajorCrimeGroup)>0
	    and TRIM(i_ISOMajorCrimeGroup)=''
	    or LENGTH(i_ISOMajorCrimeGroup) = 0,
	    'N/A',
	    LTRIM(RTRIM(i_ISOMajorCrimeGroup))
	) AS o_ISOMajorCrimeGroup,
	i_RatedCoverageCode AS o_RatedCoverageCode,
	i_RatedCoverageDescription AS o_RatedCoverageDescription,
	-- *INF*: Decode(TRUE,
	-- IsNull(lkp_InsuranceReferenceCoverageDimId), 'Insert', 
	-- lkp_CoverageCode=i_CoverageCode
	-- AND lkp_CoverageGroupCode=i_CoverageGroupCode
	-- AND lkp_CoverageSummaryCode=i_CoverageSummaryCode
	-- AND lkp_InsuranceLineDescription=i_InsuranceLineDescription
	-- AND lkp_CoverageDescription=i_CoverageDescription
	-- AND lkp_CoverageGroupDescription=i_CoverageGroupDescription
	-- AND lkp_CoverageSummaryDescription=i_CoverageSummaryDescription
	-- AND lkp_PmsRiskUnitGroupDescription=i_PmsRiskUnitGroupDescription
	-- AND lkp_PmsRiskUnitDescription=i_PmsRiskUnitDescription
	-- AND lkp_PmsMajorPerilDescription=i_PmsMajorPerilDescription
	-- AND lkp_LossHistoryCode=i_LossHistoryCode
	-- AND lkp_LossHistoryDescription=i_LossHistoryDescription
	-- AND lkp_ISOMajorCrimeGroup=i_ISOMajorCrimeGroup 
	-- AND lkp_RatedCoverageCode=i_RatedCoverageCode
	-- AND lkp_RatedCoverageDescription=i_RatedCoverageDescription
	-- ,'Ignore','Update')
	Decode(
	    TRUE,
	    lkp_InsuranceReferenceCoverageDimId IS NULL, 'Insert',
	    lkp_CoverageCode = i_CoverageCode AND lkp_CoverageGroupCode = i_CoverageGroupCode AND lkp_CoverageSummaryCode = i_CoverageSummaryCode AND lkp_InsuranceLineDescription = i_InsuranceLineDescription AND lkp_CoverageDescription = i_CoverageDescription AND lkp_CoverageGroupDescription = i_CoverageGroupDescription AND lkp_CoverageSummaryDescription = i_CoverageSummaryDescription AND lkp_PmsRiskUnitGroupDescription = i_PmsRiskUnitGroupDescription AND lkp_PmsRiskUnitDescription = i_PmsRiskUnitDescription AND lkp_PmsMajorPerilDescription = i_PmsMajorPerilDescription AND lkp_LossHistoryCode = i_LossHistoryCode AND lkp_LossHistoryDescription = i_LossHistoryDescription AND lkp_ISOMajorCrimeGroup = i_ISOMajorCrimeGroup AND lkp_RatedCoverageCode = i_RatedCoverageCode AND lkp_RatedCoverageDescription = i_RatedCoverageDescription, 'Ignore',
	    'Update'
	) AS o_InsertUpdateOrIgnore
	FROM SQ_CoverageSummary
	LEFT JOIN LKP_InsuranceReferenceCoverageDim
	ON LKP_InsuranceReferenceCoverageDim.InsuranceLineCode = SQ_CoverageSummary.InsuranceLineCode AND LKP_InsuranceReferenceCoverageDim.DctCoverageTypeCode = SQ_CoverageSummary.DctCoverageTypeCode AND LKP_InsuranceReferenceCoverageDim.DctSubCoverageTypeCode = SQ_CoverageSummary.DctSubCoverageTypeCode AND LKP_InsuranceReferenceCoverageDim.DctRiskTypeCode = SQ_CoverageSummary.DctRiskTypeCode AND LKP_InsuranceReferenceCoverageDim.DctPerilGroup = SQ_CoverageSummary.DctPerilGroup AND LKP_InsuranceReferenceCoverageDim.DctCoverageVersion = SQ_CoverageSummary.DctCoverageVersion AND LKP_InsuranceReferenceCoverageDim.PmsRiskUnitGroupCode = SQ_CoverageSummary.PmsRiskUnitGroupCode AND LKP_InsuranceReferenceCoverageDim.PmsRiskUnitCode = SQ_CoverageSummary.PmsRiskUnitCode AND LKP_InsuranceReferenceCoverageDim.PmsMajorPerilCode = SQ_CoverageSummary.PmsMajorPerilCode AND LKP_InsuranceReferenceCoverageDim.PmsProductTypeCode = SQ_CoverageSummary.PmsProductTypeCode
),
RTR_Classify_Insert_Update AS (
	SELECT
	o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId,
	o_AuditID AS AuditID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_InsuranceLineCode AS InsuranceLineCode,
	o_InsuranceLineDescription AS InsuranceLineDescription,
	o_CoverageCode AS CoverageCode,
	o_CoverageDescription AS CoverageDescription,
	o_CoverageGroupCode AS CoverageGroupCode,
	o_CoverageGroupDescription AS CoverageGroupDescription,
	o_CoverageSummaryCode AS CoverageSummaryCode,
	o_CoverageSummaryDescription AS CoverageSummaryDescription,
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
	o_ISOMajorCrimeGroup AS ISOMajorCrimeGroup,
	o_RatedCoverageCode AS RatedCoverageCode,
	o_RatedCoverageDescription AS RatedCoverageDescription,
	o_InsertUpdateOrIgnore AS InsertUpdateOrIgnore
	FROM EXP_Detect_Changes_Add_MetaData
),
RTR_Classify_Insert_Update_INSERT AS (SELECT * FROM RTR_Classify_Insert_Update WHERE InsertUpdateOrIgnore='Insert'),
RTR_Classify_Insert_Update_UPDATE AS (SELECT * FROM RTR_Classify_Insert_Update WHERE InsertUpdateOrIgnore='Update'),
UPD_InsuranceReferenceCoverageDim_Insert AS (
	SELECT
	AuditID AS AuditId, 
	CreatedDate AS CreateDate, 
	ModifiedDate AS ModifedDate, 
	InsuranceLineCode, 
	InsuranceLineDescription, 
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
	LossHistoryCode, 
	LossHistoryDescription, 
	ISOMajorCrimeGroup AS ISOMajorCrimeGroup1, 
	RatedCoverageCode, 
	RatedCoverageDescription
	FROM RTR_Classify_Insert_Update_INSERT
),
InsuranceReferenceCoverageDim_Insert AS (

	------------ PRE SQL ----------
	SET QUOTED_IDENTIFIER ON;
	ALTER INDEX [AK1InsuranceReferenceCoverageDim] ON dbo.InsuranceReferenceCoverageDim DISABLE;
	ALTER INDEX [AK2InsuranceReferenceCoverageDim] ON dbo.InsuranceReferenceCoverageDim DISABLE;
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim
	(AuditId, CreateDate, ModifedDate, InsuranceLineCode, InsuranceLineDescription, CoverageCode, CoverageDescription, CoverageGroupCode, CoverageGroupDescription, CoverageSummaryCode, CoverageSummaryDescription, DctRiskTypeCode, DctCoverageTypeCode, PmsRiskUnitGroupCode, PmsRiskUnitGroupDescription, PmsRiskUnitCode, PmsRiskUnitDescription, PmsMajorPerilCode, PmsMajorPerilDescription, DctPerilGroup, PmsProductTypeCode, DctSubCoverageTypeCode, DctCoverageVersion, LossHistoryCode, LossHistoryDescription, ISOMajorCrimeGroup, RatedCoverageCode, RatedCoverageDescription)
	SELECT 
	AUDITID, 
	CREATEDATE, 
	MODIFEDDATE, 
	INSURANCELINECODE, 
	INSURANCELINEDESCRIPTION, 
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
	DCTPERILGROUP, 
	PMSPRODUCTTYPECODE, 
	DCTSUBCOVERAGETYPECODE, 
	DCTCOVERAGEVERSION, 
	LOSSHISTORYCODE, 
	LOSSHISTORYDESCRIPTION, 
	ISOMajorCrimeGroup1 AS ISOMAJORCRIMEGROUP, 
	RATEDCOVERAGECODE, 
	RATEDCOVERAGEDESCRIPTION
	FROM UPD_InsuranceReferenceCoverageDim_Insert

	------------ POST SQL ----------
	SET QUOTED_IDENTIFIER ON;
	ALTER INDEX [AK1InsuranceReferenceCoverageDim] ON dbo.InsuranceReferenceCoverageDim REBUILD;
	ALTER INDEX [AK2InsuranceReferenceCoverageDim] ON dbo.InsuranceReferenceCoverageDim REBUILD;
	-------------------------------


),
UPD_InsuranceReferenceCoverageDim_Update AS (
	SELECT
	InsuranceReferenceCoverageDimId, 
	AuditID AS AuditId, 
	ModifiedDate AS ModifedDate, 
	InsuranceLineCode, 
	InsuranceLineDescription, 
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
	LossHistoryCode, 
	LossHistoryDescription, 
	ISOMajorCrimeGroup AS ISOMajorCrimeGroup3, 
	RatedCoverageCode, 
	RatedCoverageDescription
	FROM RTR_Classify_Insert_Update_UPDATE
),
InsuranceReferenceCoverageDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim AS T
	USING UPD_InsuranceReferenceCoverageDim_Update AS S
	ON T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.ModifedDate = S.ModifedDate, T.InsuranceLineCode = S.InsuranceLineCode, T.InsuranceLineDescription = S.InsuranceLineDescription, T.CoverageCode = S.CoverageCode, T.CoverageDescription = S.CoverageDescription, T.CoverageGroupCode = S.CoverageGroupCode, T.CoverageGroupDescription = S.CoverageGroupDescription, T.CoverageSummaryCode = S.CoverageSummaryCode, T.CoverageSummaryDescription = S.CoverageSummaryDescription, T.DctRiskTypeCode = S.DctRiskTypeCode, T.DctCoverageTypeCode = S.DctCoverageTypeCode, T.PmsRiskUnitGroupCode = S.PmsRiskUnitGroupCode, T.PmsRiskUnitGroupDescription = S.PmsRiskUnitGroupDescription, T.PmsRiskUnitCode = S.PmsRiskUnitCode, T.PmsRiskUnitDescription = S.PmsRiskUnitDescription, T.PmsMajorPerilCode = S.PmsMajorPerilCode, T.PmsMajorPerilDescription = S.PmsMajorPerilDescription, T.DctPerilGroup = S.DctPerilGroup, T.PmsProductTypeCode = S.PmsProductTypeCode, T.DctSubCoverageTypeCode = S.DctSubCoverageTypeCode, T.DctCoverageVersion = S.DctCoverageVersion, T.LossHistoryCode = S.LossHistoryCode, T.LossHistoryDescription = S.LossHistoryDescription, T.ISOMajorCrimeGroup = S.ISOMajorCrimeGroup3, T.RatedCoverageCode = S.RatedCoverageCode, T.RatedCoverageDescription = S.RatedCoverageDescription

	------------ POST SQL ----------
	SET QUOTED_IDENTIFIER ON;
	ALTER INDEX [AK1InsuranceReferenceCoverageDim] ON dbo.InsuranceReferenceCoverageDim REBUILD;
	ALTER INDEX [AK2InsuranceReferenceCoverageDim] ON dbo.InsuranceReferenceCoverageDim REBUILD;
	-------------------------------


),