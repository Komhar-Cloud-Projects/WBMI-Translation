WITH
SQ_RatingCoverage AS (
	SELECT distinct RC.RiskType, RC.CoverageType, RC.CoverageVersion, RC.PerilGroup, RC.SubCoverageTypeCode, PC.InsuranceLine, SIL.StandardInsuranceLineCode, SIL.StandardInsuranceLineDescription
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	INNER JOIN
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON RC.PolicyCoverageAKId=PC.PolicyCoverageAKId
	LEFT JOIN
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	ON SIL.ins_line_code=PC.InsuranceLine
	-- AND source_sys_id='DCT'
	AND crrnt_snpsht_flag=1
	WHERE NOT EXISTS (
	SELECT 1 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SystemCoverage SC
	WHERE SIL.StandardInsuranceLineCode=SC.InsuranceLineCode
	AND SC.DctRiskTypeCode=RC.RiskType
	AND SC.DctCoverageTypeCode=RC.CoverageType
	AND SC.DctPerilGroup=RC.PerilGroup
	AND SC.DctSubCoverageTypeCode=RC.SubCoverageTypeCode
	AND SC.DctCoverageVersion=RC.CoverageVersion
	AND NOT SC.ConformedCoverageId IN (-1,-99))
),
EXP_FormatValues AS (
	SELECT
	StandardInsuranceLineCode AS lkp_StandardInsuranceLineCode,
	StandardInsuranceLineDescription AS lkp_StandardInsuranceLineDescription,
	InsuranceLine AS i_LineType,
	CoverageType AS i_CoverageType,
	SubCoverageTypeCode AS i_SubCoverageType,
	RiskType AS i_RiskType,
	CoverageVersion AS i_CoverageVersion,
	PerilGroup AS i_PerilGroup,
	-- *INF*: IIF(IsNull(lkp_StandardInsuranceLineCode),'Line Not Found.  Source Line: ' || i_LineType, lkp_StandardInsuranceLineCode)
	IFF(lkp_StandardInsuranceLineCode IS NULL,
		'Line Not Found.  Source Line: ' || i_LineType,
		lkp_StandardInsuranceLineCode
	) AS o_InsuranceLineCode,
	-- *INF*: IIF(IsNull(lkp_StandardInsuranceLineDescription), 'No Description Found For Line', lkp_StandardInsuranceLineDescription)
	IFF(lkp_StandardInsuranceLineDescription IS NULL,
		'No Description Found For Line',
		lkp_StandardInsuranceLineDescription
	) AS o_InsuranceLineDescription,
	-- *INF*: IIF(IsNull(i_CoverageType) or IS_SPACES(i_CoverageType), 'N/A', rtrim(ltrim(i_CoverageType)))
	IFF(i_CoverageType IS NULL 
		OR LENGTH(i_CoverageType)>0 AND TRIM(i_CoverageType)='',
		'N/A',
		rtrim(ltrim(i_CoverageType
			)
		)
	) AS o_CoverageType,
	-- *INF*: IIF(IsNull(i_SubCoverageType) or IS_SPACES(i_SubCoverageType), 'N/A', rtrim(ltrim(i_SubCoverageType)))
	-- 
	IFF(i_SubCoverageType IS NULL 
		OR LENGTH(i_SubCoverageType)>0 AND TRIM(i_SubCoverageType)='',
		'N/A',
		rtrim(ltrim(i_SubCoverageType
			)
		)
	) AS o_SubCoverageType,
	-- *INF*: IIF(IsNull(i_RiskType) or IS_SPACES(i_RiskType), 'N/A', rtrim(ltrim(i_RiskType)))
	IFF(i_RiskType IS NULL 
		OR LENGTH(i_RiskType)>0 AND TRIM(i_RiskType)='',
		'N/A',
		rtrim(ltrim(i_RiskType
			)
		)
	) AS o_RiskType,
	-- *INF*: IIF(IsNull(i_CoverageVersion) or IS_SPACES(i_CoverageVersion), 'N/A', rtrim(ltrim(i_CoverageVersion)))
	-- 
	IFF(i_CoverageVersion IS NULL 
		OR LENGTH(i_CoverageVersion)>0 AND TRIM(i_CoverageVersion)='',
		'N/A',
		rtrim(ltrim(i_CoverageVersion
			)
		)
	) AS o_CoverageVersion,
	-- *INF*: IIF(IsNull(i_PerilGroup) or IS_SPACES(i_PerilGroup), 'N/A', rtrim(ltrim(i_PerilGroup)))
	-- 
	IFF(i_PerilGroup IS NULL 
		OR LENGTH(i_PerilGroup)>0 AND TRIM(i_PerilGroup)='',
		'N/A',
		rtrim(ltrim(i_PerilGroup
			)
		)
	) AS o_PerilGroup
	FROM SQ_RatingCoverage
),
Agg_Distinct AS (
	SELECT
	o_InsuranceLineCode AS InsuranceLineCode,
	o_InsuranceLineDescription AS InsuranceLineDescription,
	o_CoverageType AS CoverageType,
	o_SubCoverageType AS SubCoverageType,
	o_RiskType AS RiskType,
	o_CoverageVersion AS CoverageVersion,
	o_PerilGroup AS PerilGroup
	FROM EXP_FormatValues
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode, InsuranceLineDescription, CoverageType, SubCoverageType, RiskType, CoverageVersion, PerilGroup ORDER BY NULL) = 1
),
LKP_SystemCoverage_IR AS (
	SELECT
	SystemCoverageId,
	ConformedCoverageId,
	InsuranceLineCode,
	DctCoverageTypeCode,
	DctSubCoverageTypeCode,
	DctRiskTypeCode,
	DctCoverageVersion,
	DctPerilGroup
	FROM (
		SELECT 
			SystemCoverageId,
			ConformedCoverageId,
			InsuranceLineCode,
			DctCoverageTypeCode,
			DctSubCoverageTypeCode,
			DctRiskTypeCode,
			DctCoverageVersion,
			DctPerilGroup
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SystemCoverage
		WHERE SourceSystemId = 'DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctCoverageTypeCode,DctSubCoverageTypeCode,DctRiskTypeCode,DctCoverageVersion,DctPerilGroup ORDER BY SystemCoverageId) = 1
),
EXP_CheckForNew AS (
	SELECT
	LKP_SystemCoverage_IR.SystemCoverageId AS lkp_SystemCoverageId,
	LKP_SystemCoverage_IR.ConformedCoverageId AS lkp_ConformedCoverageId,
	Agg_Distinct.InsuranceLineCode AS i_InsuranceLineCode,
	Agg_Distinct.InsuranceLineDescription AS i_InsuranceLineDescription,
	Agg_Distinct.CoverageType AS i_CoverageType,
	Agg_Distinct.SubCoverageType AS i_SubCoverageType,
	Agg_Distinct.RiskType AS i_RiskType,
	Agg_Distinct.CoverageVersion AS i_CoverageVersion,
	Agg_Distinct.PerilGroup AS i_PerilGroup,
	-- *INF*: IIF(IsNull(lkp_SystemCoverageId), 'Insert', 'Ignore')
	IFF(lkp_SystemCoverageId IS NULL,
		'Insert',
		'Ignore'
	) AS o_InsertOrIgnore,
	-- *INF*: IIF(IsNull(lkp_ConformedCoverageId), -99, lkp_ConformedCoverageId)
	IFF(lkp_ConformedCoverageId IS NULL,
		- 99,
		lkp_ConformedCoverageId
	) AS o_ConformedCoverageId,
	'DCT' AS o_SourceSystemId,
	sysdate AS o_CurrentDate,
	'UNK' AS o_UnassignedCode,
	-- *INF*: 'Value not assigned (NEW)'
	'Value not assigned (NEW)' AS o_UnassignedDescription,
	'N/A' AS o_NACode,
	'Not Applicable' AS o_NADescription,
	'N/A' AS o_ISOMajorCrimeGroup
	FROM Agg_Distinct
	LEFT JOIN LKP_SystemCoverage_IR
	ON LKP_SystemCoverage_IR.InsuranceLineCode = Agg_Distinct.InsuranceLineCode AND LKP_SystemCoverage_IR.DctCoverageTypeCode = Agg_Distinct.CoverageType AND LKP_SystemCoverage_IR.DctSubCoverageTypeCode = Agg_Distinct.SubCoverageType AND LKP_SystemCoverage_IR.DctRiskTypeCode = Agg_Distinct.RiskType AND LKP_SystemCoverage_IR.DctCoverageVersion = Agg_Distinct.CoverageVersion AND LKP_SystemCoverage_IR.DctPerilGroup = Agg_Distinct.PerilGroup
),
RTR_MissingCoveragesOnly AS (
	SELECT
	i_InsuranceLineCode AS InsuranceLineCode,
	i_InsuranceLineDescription AS InsuranceLineDescription,
	i_CoverageType AS CoverageType,
	i_SubCoverageType AS SubCoverageType,
	i_RiskType AS RiskType,
	i_CoverageVersion AS CoverageVersion,
	i_PerilGroup AS PerilGroup,
	o_InsertOrIgnore AS InsertOrIgnore,
	o_ConformedCoverageId AS ConformedCoverageId,
	o_SourceSystemId AS SourceSystemId,
	o_CurrentDate AS CurrentDate,
	o_UnassignedCode AS UnassignedCode,
	o_UnassignedDescription AS UnassignedDescription,
	o_NACode AS NACode,
	o_NADescription AS NADescription,
	o_ISOMajorCrimeGroup AS ISOMajorCrimeGroup
	FROM EXP_CheckForNew
),
RTR_MissingCoveragesOnly_Insert AS (SELECT * FROM RTR_MissingCoveragesOnly WHERE InsertOrIgnore='Insert'),
RTR_MissingCoveragesOnly_File AS (SELECT * FROM RTR_MissingCoveragesOnly WHERE IN(ConformedCoverageId,-1,-99)),
UPDTRANS AS (
	SELECT
	InsuranceLineCode, 
	InsuranceLineDescription, 
	CoverageType, 
	SubCoverageType, 
	RiskType, 
	CoverageVersion, 
	PerilGroup, 
	ConformedCoverageId, 
	SourceSystemId, 
	CurrentDate, 
	NACode AS o_NACode, 
	NADescription AS o_NADescription, 
	ISOMajorCrimeGroup
	FROM RTR_MissingCoveragesOnly_Insert
),
SystemCoverage_InsertNew1 AS (
	SET QUOTED_IDENTIFIER ON;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage(CreatedDate,ModifiedDate,ConformedCoverageId,InsuranceLineCode,InsuranceLineDescription,SourceSystemId,DctRiskTypeCode,DctCoverageTypeCode,PmsRiskUnitGroupCode,PmsRiskUnitGroupDescription,PmsRiskUnitCode,PmsRiskUnitDescription,PmsMajorPerilCode,PmsMajorPerilDescription,PmsProductTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion,LossHistoryCode,LossHistoryDescription,ISOMajorCrimeGroup
	) 
	SELECT S.CreatedDate,S.ModifiedDate,S.ConformedCoverageId,S.InsuranceLineCode,S.InsuranceLineDescription,S.SourceSystemId,S.DctRiskTypeCode,S.DctCoverageTypeCode,S.PmsRiskUnitGroupCode,S.PmsRiskUnitGroupDescription,S.PmsRiskUnitCode,S.PmsRiskUnitDescription,S.PmsMajorPerilCode,S.PmsMajorPerilDescription, S.PmsProductTypeCode,S.DctPerilGroup,S.DctSubCoverageTypeCode,S.DctCoverageVersion,S.LossHistoryCode,S.LossHistoryDescription,S.ISOMajorCrimeGroup
	FROM UPDTRANS S
),
ReportNewCoverageCombinations AS (
	INSERT INTO NewCoverageCombinations
	(CoverageSummaryCode, CoverageSummaryDescription, CoverageGroupCode, CoverageGroupDescription, CoverageCode, CoverageDescription, RatedCoverageCode, RatedCoverageDescription, InsuranceLineCode, InsuranceLineDescription, SourceSystemId, DctRiskTypeCode, DctCoverageTypeCode, DctSubCoverageTypeCode, DCTPerilGroup, DctCoverageVersion, PmsRiskUnitGroupCode, PmsRiskUnitGroupDescription, PmsRiskUnitCode, PmsRiskUnitDescription, PmsMajorPerilCode, PmsMajorPerilDescription, PmsProductTypeCode, LossHistoryCode, LossHistoryDescription)
	SELECT 
	UnassignedCode AS COVERAGESUMMARYCODE, 
	UnassignedDescription AS COVERAGESUMMARYDESCRIPTION, 
	UnassignedCode AS COVERAGEGROUPCODE, 
	UnassignedDescription AS COVERAGEGROUPDESCRIPTION, 
	UnassignedCode AS COVERAGECODE, 
	UnassignedDescription AS COVERAGEDESCRIPTION, 
	UnassignedCode AS RATEDCOVERAGECODE, 
	UnassignedDescription AS RATEDCOVERAGEDESCRIPTION, 
	INSURANCELINECODE, 
	INSURANCELINEDESCRIPTION, 
	SOURCESYSTEMID, 
	RiskType AS DCTRISKTYPECODE, 
	CoverageType AS DCTCOVERAGETYPECODE, 
	SubCoverageType AS DCTSUBCOVERAGETYPECODE, 
	PerilGroup AS DCTPERILGROUP, 
	CoverageVersion AS DCTCOVERAGEVERSION, 
	NACode AS PMSRISKUNITGROUPCODE, 
	NACode AS PMSRISKUNITGROUPDESCRIPTION, 
	NACode AS PMSRISKUNITCODE, 
	NACode AS PMSRISKUNITDESCRIPTION, 
	NACode AS PMSMAJORPERILCODE, 
	NACode AS PMSMAJORPERILDESCRIPTION, 
	NACode AS PMSPRODUCTTYPECODE, 
	NACode AS LOSSHISTORYCODE, 
	NACode AS LOSSHISTORYDESCRIPTION
	FROM RTR_MissingCoveragesOnly_File
),