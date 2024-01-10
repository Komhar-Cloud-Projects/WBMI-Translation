WITH
SQ_CoverageDetailGeneralLiabilityDim AS (
	select  distinct CDGLD.CoverageDetailDimId as CoverageDetailDimId_GL,
	CDGLD.RetroactiveDate as RetroactiveDate_GL,
	CDGLD.PerOccurrenceLimit as PerOccurenceLimit_GL,
	CDGLD.AggregateLimit as AggregateLimit_GL,
	CDGLD.ProductAggregateLimit as ProductAggregateLimit_GL,
	CDGLD.EffectiveDate as EffectiveDate_GL,
	CDGLD.ExpirationDate as ExpirationDate_GL,
	CDGLD.PerClaimLimit as PolicyPerClaimLimit_GL,
	CDGLD.LiabilityFormCode as LiabilityFormCode_GL,
	CDGLD.ISOGeneralLiabilityClassSummary ISOGeneralLiabilityClassSummary_GL,
	CDGLD.ISOGeneralLiabilityClassGroupCode as ISOGeneralLiabilityClassGroupCode_GL,
	--CDGLD.ClassGroupDescription as ClassGroupDescription_GL,
	
	CDD.CoverageDetailDimId,
	CDGL.RetroactiveDate,
	PLT.PolicyPerOccurenceLimit,
	PLT.PolicyAggregateLimit,
	PLT.PolicyProductAggregateLimit,
	CDD.CoverageGuid,
	CDD.EffectiveDate,
	CDD.ExpirationDate,
	PLT.PolicyPerClaimLimit,
	CDGL.LiabilityFormCode,
	CDGL.ISOGeneralLiabilityClassSummary,
	CDGL.ISOGeneralLiabilityClassGroupCode,
	--CDGL.ClassGroupDescription
	SC.SublineCode
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	INNER JOIN dbo.StatisticalCoverage SC
	ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID 
	AND PT.SourceSystemID = 'PMS' 
	AND SC.SourceSystemID = 'PMS' 
	AND SC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on SC.PolicyCoverageAKId=PC.PolicyCoverageAKId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PLT
	on PLT.PolicyLimitAKID=PC.PolicyLimitAKID
	and PLT.CurrentSnapshotFlag=1
	join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	on CDD.EDWPremiumTransactionPKID=PT.PremiumTransactionID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability CDGL
	on CDGL.PremiumTransactionID=CDD.EDWPremiumTransactionPKID
	left join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailGeneralLiabilityDim CDGLD
	on CDGLD.CoverageDetailDimId=CDD.CoverageDetailDimId
	where PT.SourceSystemID='PMS' and CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}' and PC.InsuranceLine='GL' @{pipeline().parameters.WHERE_CLAUSE_PMS} 
	
	--AND CDGL.PremiumTransactionID = 104314880
	
	union all
	
	select  distinct CDGLD.CoverageDetailDimId as CoverageDetailDimId_GL,
	CDGLD.RetroactiveDate as RetroactiveDate_GL,
	CDGLD.PerOccurrenceLimit as PerOccurenceLimit_GL,
	CDGLD.AggregateLimit as AggregateLimit_GL,
	CDGLD.ProductAggregateLimit as ProductAggregateLimit_GL,
	CDGLD.EffectiveDate as EffectiveDate_GL,
	CDGLD.ExpirationDate as ExpirationDate_GL,
	CDGLD.PerClaimLimit as PolicyPerClaimLimit_GL,
	CDGLD.LiabilityFormCode as LiabilityFormCode_GL,
	CDGLD.ISOGeneralLiabilityClassSummary ISOGeneralLiabilityClassSummary_GL,
	CDGLD.ISOGeneralLiabilityClassGroupCode as ISOGeneralLiabilityClassGroupCode_GL,
	--CDGLD.ClassGroupDescription as ClassGroupDescription_GL,
	
	CDD.CoverageDetailDimId,
	CDGL.RetroactiveDate,
	PLT.PolicyPerOccurenceLimit,
	PLT.PolicyAggregateLimit,
	PLT.PolicyProductAggregateLimit,
	CDD.CoverageGuid,
	CDD.EffectiveDate,
	CDD.ExpirationDate,
	PLT.PolicyPerClaimLimit,
	CDGL.LiabilityFormCode,
	CDGL.ISOGeneralLiabilityClassSummary,
	CDGL.ISOGeneralLiabilityClassGroupCode,
	--CDGL.ClassGroupDescription
	RC.SublineCode
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	INNER JOIN RatingCoverage RC 
	ON PT.RatingCoverageAKId = RC.RatingCoverageAKId 
	AND PT.SourceSystemID = 'DCT' 
	AND RC.SourceSystemID = 'DCT' 
	--AND RC.CurrentSnapshotFlag=1
	and pt.EffectiveDate=rc.EffectiveDate
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on RC.PolicyCoverageAKId=PC.PolicyCoverageAKId and PC.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyLimit PLT
	on PLT.PolicyLimitAKID=PC.PolicyLimitAKID
	and PLT.EffectiveDate <= PT.PremiumTransactionEnteredDate 
	and PLT.ExpirationDate > PT.PremiumTransactionEnteredDate
	join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	on CDD.EDWPremiumTransactionPKID=PT.PremiumTransactionID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability CDGL
	on CDGL.PremiumTransactionID=CDD.EDWPremiumTransactionPKID
	left join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailGeneralLiabilityDim CDGLD
	on CDGLD.CoverageDetailDimId=CDD.CoverageDetailDimId
	where PT.SourceSystemID='DCT' and CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}' @{pipeline().parameters.WHERE_CLAUSE_DCT}
	
	--AND 1=0
),
LKP_SupISOClassGroup AS (
	SELECT
	ISOGeneralLiabilityClassGroupDescription,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode,
	SublineCode
	FROM (
		SELECT 
			ISOGeneralLiabilityClassGroupDescription,
			ISOGeneralLiabilityClassSummary,
			ISOGeneralLiabilityClassGroupCode,
			SublineCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupISOClassGroup
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ISOGeneralLiabilityClassSummary,ISOGeneralLiabilityClassGroupCode,SublineCode ORDER BY ISOGeneralLiabilityClassGroupDescription) = 1
),
EXP_GetMetaData AS (
	SELECT
	SQ_CoverageDetailGeneralLiabilityDim.CoverageDetailDimId_GL AS i_CoverageDetailDimId_GL,
	SQ_CoverageDetailGeneralLiabilityDim.RetroactiveDate_GL AS i_RetroactiveDate_GL,
	SQ_CoverageDetailGeneralLiabilityDim.PerOccurenceLimit_GL AS i_PerOccurenceLimit_GL,
	SQ_CoverageDetailGeneralLiabilityDim.AggregateLimit_GL AS i_AggregateLimit_GL,
	SQ_CoverageDetailGeneralLiabilityDim.ProductAggregateLimit_GL AS i_ProductAggregateLimit_GL,
	SQ_CoverageDetailGeneralLiabilityDim.EffectiveDate_GL AS i_EffectiveDate_GL,
	SQ_CoverageDetailGeneralLiabilityDim.ExpirationDate_GL AS i_ExpirationDate_GL,
	SQ_CoverageDetailGeneralLiabilityDim.PolicyPerClaimLimit_GL AS i_PolicyPerClaimLimit_GL,
	SQ_CoverageDetailGeneralLiabilityDim.LiabilityFormCode_GL AS i_LiabilityFormCode_GL,
	SQ_CoverageDetailGeneralLiabilityDim.ClassSummary_GL AS i_ClassSummary_GL,
	SQ_CoverageDetailGeneralLiabilityDim.ClassGroupCode_GL AS i_ClassGroupCode_GL,
	SQ_CoverageDetailGeneralLiabilityDim.CoverageDetailDimId AS i_CoverageDetailDimId,
	SQ_CoverageDetailGeneralLiabilityDim.RetroactiveDate AS i_RetroactiveDate,
	SQ_CoverageDetailGeneralLiabilityDim.PolicyPerOccurenceLimit AS i_PolicyPerOccurenceLimit,
	SQ_CoverageDetailGeneralLiabilityDim.PolicyAggregateLimit AS i_PolicyAggregateLimit,
	SQ_CoverageDetailGeneralLiabilityDim.PolicyProductAggregateLimit AS i_PolicyProductAggregateLimit,
	SQ_CoverageDetailGeneralLiabilityDim.CoverageGuid AS i_CoverageGuid,
	SQ_CoverageDetailGeneralLiabilityDim.EffectiveDate AS i_EffectiveDate,
	SQ_CoverageDetailGeneralLiabilityDim.ExpirationDate AS i_ExpirationDate,
	SQ_CoverageDetailGeneralLiabilityDim.PolicyPerClaimLimit AS i_PolicyPerClaimLimit,
	SQ_CoverageDetailGeneralLiabilityDim.LiabilityFormCode AS i_LiabilityFormCode,
	SQ_CoverageDetailGeneralLiabilityDim.ISOGeneralLiabilityClassSummary AS i_ISOGeneralLiabilityClassSummary,
	SQ_CoverageDetailGeneralLiabilityDim.ISOGeneralLiabilityClassGroupCode AS i_ISOGeneralLiabilityClassGroupCode,
	LKP_SupISOClassGroup.ISOGeneralLiabilityClassGroupDescription AS i_ISOGeneralLiabilityClassGroupDescription,
	-- *INF*: IIF(ISNULL(i_RetroactiveDate), TO_DATE('2100-12-31', 'YYYY-MM-DD'), i_RetroactiveDate)
	IFF(i_RetroactiveDate IS NULL,
		TO_DATE('2100-12-31', 'YYYY-MM-DD'
		),
		i_RetroactiveDate
	) AS v_RetroactiveDate,
	-- *INF*: IIF(ISNULL(i_PolicyPerOccurenceLimit), 'N/A', i_PolicyPerOccurenceLimit)
	IFF(i_PolicyPerOccurenceLimit IS NULL,
		'N/A',
		i_PolicyPerOccurenceLimit
	) AS v_PolicyPerOccurenceLimit,
	-- *INF*: IIF(ISNULL(i_PolicyAggregateLimit), 'N/A', i_PolicyAggregateLimit)
	IFF(i_PolicyAggregateLimit IS NULL,
		'N/A',
		i_PolicyAggregateLimit
	) AS v_PolicyAggregateLimit,
	-- *INF*: IIF(ISNULL(i_PolicyProductAggregateLimit), 'N/A', i_PolicyProductAggregateLimit)
	IFF(i_PolicyProductAggregateLimit IS NULL,
		'N/A',
		i_PolicyProductAggregateLimit
	) AS v_PolicyProductAggregateLimit,
	-- *INF*: IIF(ISNULL(i_PolicyPerClaimLimit), 'N/A', i_PolicyPerClaimLimit)
	IFF(i_PolicyPerClaimLimit IS NULL,
		'N/A',
		i_PolicyPerClaimLimit
	) AS v_PolicyPerClaimLimit,
	-- *INF*: IIF(ISNULL(i_LiabilityFormCode), 'N/A', i_LiabilityFormCode)
	IFF(i_LiabilityFormCode IS NULL,
		'N/A',
		i_LiabilityFormCode
	) AS v_LiabilityFormCode,
	-- *INF*: IIF(ISNULL(i_ISOGeneralLiabilityClassSummary), 'N/A', i_ISOGeneralLiabilityClassSummary)
	IFF(i_ISOGeneralLiabilityClassSummary IS NULL,
		'N/A',
		i_ISOGeneralLiabilityClassSummary
	) AS v_ISOGeneralLiabilityClassSummary,
	-- *INF*: IIF(ISNULL(i_ISOGeneralLiabilityClassGroupCode), 'N/A', i_ISOGeneralLiabilityClassGroupCode)
	IFF(i_ISOGeneralLiabilityClassGroupCode IS NULL,
		'N/A',
		i_ISOGeneralLiabilityClassGroupCode
	) AS v_ISOGeneralLiabilityClassGroupCode,
	-- *INF*: IIF(ISNULL(i_ISOGeneralLiabilityClassGroupDescription), 'N/A', i_ISOGeneralLiabilityClassGroupDescription)
	IFF(i_ISOGeneralLiabilityClassGroupDescription IS NULL,
		'N/A',
		i_ISOGeneralLiabilityClassGroupDescription
	) AS v_ISOGeneralLiabilityClassGroupDescription,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CoverageDetailDimId_GL), 'NEW', 
	-- i_RetroactiveDate_GL !=v_RetroactiveDate 
	-- OR i_PerOccurenceLimit_GL != v_PolicyPerOccurenceLimit 
	-- OR i_AggregateLimit_GL != v_PolicyAggregateLimit 
	-- OR i_ProductAggregateLimit_GL != v_PolicyProductAggregateLimit OR i_EffectiveDate_GL != i_EffectiveDate OR i_ExpirationDate_GL != i_ExpirationDate
	-- OR i_PolicyPerClaimLimit_GL != v_PolicyPerClaimLimit
	-- OR i_LiabilityFormCode_GL != v_LiabilityFormCode
	-- OR i_ClassSummary_GL != v_ISOGeneralLiabilityClassSummary
	-- OR i_ClassGroupCode_GL != v_ISOGeneralLiabilityClassGroupCode, 'UPDATE', 'NOCHANGE')
	DECODE(TRUE,
		i_CoverageDetailDimId_GL IS NULL, 'NEW',
		i_RetroactiveDate_GL != v_RetroactiveDate 
		OR i_PerOccurenceLimit_GL != v_PolicyPerOccurenceLimit 
		OR i_AggregateLimit_GL != v_PolicyAggregateLimit 
		OR i_ProductAggregateLimit_GL != v_PolicyProductAggregateLimit 
		OR i_EffectiveDate_GL != i_EffectiveDate 
		OR i_ExpirationDate_GL != i_ExpirationDate 
		OR i_PolicyPerClaimLimit_GL != v_PolicyPerClaimLimit 
		OR i_LiabilityFormCode_GL != v_LiabilityFormCode 
		OR i_ClassSummary_GL != v_ISOGeneralLiabilityClassSummary 
		OR i_ClassGroupCode_GL != v_ISOGeneralLiabilityClassGroupCode, 'UPDATE',
		'NOCHANGE'
	) AS o_ChangeFlag,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGuid AS o_CoverageGuid,
	v_RetroactiveDate AS o_RetroactiveDate,
	v_PolicyPerOccurenceLimit AS o_PolicyPerOccurenceLimit,
	v_PolicyAggregateLimit AS o_PolicyAggregateLimit,
	v_PolicyProductAggregateLimit AS o_PolicyProductAggregateLimit,
	v_PolicyPerClaimLimit AS o_PolicyPerClaimLimit,
	v_LiabilityFormCode AS o_LiabilityFormCode,
	v_ISOGeneralLiabilityClassSummary AS o_ISOGeneralLiabilityClassSummary,
	v_ISOGeneralLiabilityClassGroupCode AS o_ISOGeneralLiabilityClassGroupCode,
	v_ISOGeneralLiabilityClassGroupDescription AS o_ISOGeneralLiabilityClassGroupDescription
	FROM SQ_CoverageDetailGeneralLiabilityDim
	LEFT JOIN LKP_SupISOClassGroup
	ON LKP_SupISOClassGroup.ISOGeneralLiabilityClassSummary = SQ_CoverageDetailGeneralLiabilityDim.ISOGeneralLiabilityClassSummary AND LKP_SupISOClassGroup.ISOGeneralLiabilityClassGroupCode = SQ_CoverageDetailGeneralLiabilityDim.ISOGeneralLiabilityClassGroupCode AND LKP_SupISOClassGroup.SublineCode = SQ_CoverageDetailGeneralLiabilityDim.SublineCode
),
RTR_CoverageDetailGeneralLiabilityDim AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_CoverageDetailDimId AS CoverageDetailDimId,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_CoverageGuid AS CoverageGuid,
	o_RetroactiveDate AS RetroactiveDate,
	o_PolicyPerOccurenceLimit AS PolicyPerOccurenceLimit,
	o_PolicyAggregateLimit AS PolicyAggregateLimit,
	o_PolicyProductAggregateLimit AS PolicyProductAggregateLimit,
	o_PolicyPerClaimLimit AS PolicyPerClaimLimit,
	o_LiabilityFormCode AS LiabilityFormCode,
	o_ISOGeneralLiabilityClassSummary AS ISOGeneralLiabilityClassSummary,
	o_ISOGeneralLiabilityClassGroupCode AS ISOGeneralLiabilityClassGroupCode,
	o_ISOGeneralLiabilityClassGroupDescription AS ISOGeneralLiabilityClassGroupDescription
	FROM EXP_GetMetaData
),
RTR_CoverageDetailGeneralLiabilityDim_Insert AS (SELECT * FROM RTR_CoverageDetailGeneralLiabilityDim WHERE ChangeFlag='NEW'),
RTR_CoverageDetailGeneralLiabilityDim_Update AS (SELECT * FROM RTR_CoverageDetailGeneralLiabilityDim WHERE ChangeFlag='UPDATE'),
TGT_CoverageDetailGeneralLiabilityDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailGeneralLiabilityDim
	(CoverageDetailDimId, AuditId, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, CoverageGuid, RetroactiveDate, PerOccurrenceLimit, AggregateLimit, ProductAggregateLimit, PerClaimLimit, LiabilityFormCode, ISOGeneralLiabilityClassSummary, ISOGeneralLiabilityClassGroupCode, ISOGeneralLiabilityClassGroupDescription)
	SELECT 
	COVERAGEDETAILDIMID, 
	AuditID AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	RETROACTIVEDATE, 
	PolicyPerOccurenceLimit AS PEROCCURRENCELIMIT, 
	PolicyAggregateLimit AS AGGREGATELIMIT, 
	PolicyProductAggregateLimit AS PRODUCTAGGREGATELIMIT, 
	PolicyPerClaimLimit AS PERCLAIMLIMIT, 
	LIABILITYFORMCODE, 
	ISOGENERALLIABILITYCLASSSUMMARY, 
	ISOGENERALLIABILITYCLASSGROUPCODE, 
	ISOGENERALLIABILITYCLASSGROUPDESCRIPTION
	FROM RTR_CoverageDetailGeneralLiabilityDim_Insert
),
UPD_Existing AS (
	SELECT
	CoverageDetailDimId, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	ModifiedDate, 
	CoverageGuid, 
	RetroactiveDate, 
	PolicyPerOccurenceLimit, 
	PolicyAggregateLimit, 
	PolicyProductAggregateLimit, 
	PolicyPerClaimLimit, 
	LiabilityFormCode, 
	ISOGeneralLiabilityClassSummary, 
	ISOGeneralLiabilityClassGroupCode, 
	ISOGeneralLiabilityClassGroupDescription
	FROM RTR_CoverageDetailGeneralLiabilityDim_Update
),
TGT_CoverageDetailGeneralLiabilityDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailGeneralLiabilityDim AS T
	USING UPD_Existing AS S
	ON T.CoverageDetailDimId = S.CoverageDetailDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.CoverageGuid, T.RetroactiveDate = S.RetroactiveDate, T.PerOccurrenceLimit = S.PolicyPerOccurenceLimit, T.AggregateLimit = S.PolicyAggregateLimit, T.ProductAggregateLimit = S.PolicyProductAggregateLimit, T.PerClaimLimit = S.PolicyPerClaimLimit, T.LiabilityFormCode = S.LiabilityFormCode, T.ISOGeneralLiabilityClassSummary = S.ISOGeneralLiabilityClassSummary, T.ISOGeneralLiabilityClassGroupCode = S.ISOGeneralLiabilityClassGroupCode, T.ISOGeneralLiabilityClassGroupDescription = S.ISOGeneralLiabilityClassGroupDescription
),