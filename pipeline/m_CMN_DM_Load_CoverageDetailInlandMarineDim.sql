WITH
SQ_CoverageDetailInlandMarineDim AS (
	select distinct CDIMD.CoverageDetailDimId as CoverageDetailDimId_IM,
	CDIMD.AaisClassCode as AaisClassCode_IM,
	CDIMD.AaisClassDescription as AaisClassDescription_IM,
	CDIMD.EffectiveDate as EffectiveDate_IM,
	CDIMD.ExpirationDate as ExpirationDate_IM,
	CDIMD.IsoFireProtectionCode as IsoFireProtectionCode_IM,
	CDD.CoverageDetailDimId as CoverageDetailDimId,
	right(replicate('0',6) + CDD.ISOClassCode,6) as ClassCode,
	CDD.CoverageGuid as CoverageGuid,
	CDD.EffectiveDate as EffectiveDate,
	CDD.ExpirationDate as ExpirationDate,
	CDIM.IsoFireProtectionCode as IsoFireProtectionCode
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailInlandMarine CDIM
	on CDIM.PremiumTransactionId=CDD.EDWPremiumTransactionPKId
	left join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailInlandMarineDim CDIMD
	on CDIMD.CoverageDetailDimId=CDD.CoverageDetailDimId
	where CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}' 
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
),
LKP_SupReferenceData AS (
	SELECT
	ToCode,
	ToDescription,
	FromCode
	FROM (
		SELECT ToCode as ToCode, 
		ToDescription as ToDescription,
		right(replicate('0',6) + FromCode,6) as FromCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupReferenceData
		WHERE FromDomain = 'ISO Inland Marine Class Code' and ToDomain = 'AAIS Inland Marine Class Code'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FromCode ORDER BY ToCode) = 1
),
EXP_GetMetaData AS (
	SELECT
	SQ_CoverageDetailInlandMarineDim.CoverageDetailDimId_IM AS i_CoverageDetailDimId_IM,
	SQ_CoverageDetailInlandMarineDim.AaisClassCode_IM AS i_AaisClassCode_IM,
	SQ_CoverageDetailInlandMarineDim.AaisClassDescription_IM AS i_AaisClassDescription_IM,
	SQ_CoverageDetailInlandMarineDim.EffectiveDate_IM AS i_EffectiveDate_IM,
	SQ_CoverageDetailInlandMarineDim.ExpirationDate_IM AS i_ExpirationDate_IM,
	SQ_CoverageDetailInlandMarineDim.IsoFireProtectionCode_IM AS i_IsoFireProtectionCode_IM,
	SQ_CoverageDetailInlandMarineDim.CoverageDetailDimId AS i_CoverageDetailDimId,
	LKP_SupReferenceData.ToCode AS i_AaisClassCode,
	LKP_SupReferenceData.ToDescription AS i_AaisClassDescription,
	SQ_CoverageDetailInlandMarineDim.CoverageGuid AS i_CoverageGuid,
	SQ_CoverageDetailInlandMarineDim.EffectiveDate AS i_EffectiveDate,
	SQ_CoverageDetailInlandMarineDim.ExpirationDate AS i_ExpirationDate,
	SQ_CoverageDetailInlandMarineDim.IsoFireProtectionCode AS i_IsoFireProtectionCode,
	-- *INF*: IIF(ISNULL(i_AaisClassCode), 'N/A', i_AaisClassCode)
	IFF(i_AaisClassCode IS NULL, 'N/A', i_AaisClassCode) AS v_AaisClassCode,
	-- *INF*: IIF(ISNULL(i_AaisClassDescription), 'N/A', i_AaisClassDescription)
	IFF(i_AaisClassDescription IS NULL, 'N/A', i_AaisClassDescription) AS v_AaisClassDescription,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CoverageDetailDimId_IM), 'NEW',
	-- LTRIM(RTRIM(i_AaisClassCode_IM)) !=v_AaisClassCode OR i_AaisClassDescription_IM != v_AaisClassDescription OR i_EffectiveDate_IM != i_EffectiveDate OR i_ExpirationDate_IM != i_ExpirationDate OR i_IsoFireProtectionCode_IM != i_IsoFireProtectionCode, 'UPDATE', 'NOCHANGE')
	DECODE(
	    TRUE,
	    i_CoverageDetailDimId_IM IS NULL, 'NEW',
	    LTRIM(RTRIM(i_AaisClassCode_IM)) != v_AaisClassCode OR i_AaisClassDescription_IM != v_AaisClassDescription OR i_EffectiveDate_IM != i_EffectiveDate OR i_ExpirationDate_IM != i_ExpirationDate OR i_IsoFireProtectionCode_IM != i_IsoFireProtectionCode, 'UPDATE',
	    'NOCHANGE'
	) AS o_ChangeFlag,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGuid AS o_CoverageGuid,
	v_AaisClassCode AS o_AaisClassCode,
	v_AaisClassDescription AS o_AaisClassDescription,
	i_IsoFireProtectionCode AS o_IsoFireProtectionCode
	FROM SQ_CoverageDetailInlandMarineDim
	LEFT JOIN LKP_SupReferenceData
	ON LKP_SupReferenceData.FromCode = SQ_CoverageDetailInlandMarineDim.ClassCode
),
RTR_CoverageDetailInlandMarineDim AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_CoverageDetailDimId AS CoverageDetailDimId,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_CoverageGuid AS CoverageGuid,
	o_AaisClassCode AS AaisClassCode,
	o_AaisClassDescription AS AaisClassDescription,
	o_IsoFireProtectionCode AS IsoFireProtectionCode
	FROM EXP_GetMetaData
),
RTR_CoverageDetailInlandMarineDim_Insert AS (SELECT * FROM RTR_CoverageDetailInlandMarineDim WHERE ChangeFlag='NEW'),
RTR_CoverageDetailInlandMarineDim_Update AS (SELECT * FROM RTR_CoverageDetailInlandMarineDim WHERE ChangeFlag='UPDATE'),
TGT_CoverageDetailInlandMarineDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailInlandMarineDim
	(CoverageDetailDimId, AuditId, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, CoverageGuid, AaisClassCode, AaisClassDescription, IsoFireProtectionCode)
	SELECT 
	COVERAGEDETAILDIMID, 
	AuditID AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	AAISCLASSCODE, 
	AAISCLASSDESCRIPTION, 
	ISOFIREPROTECTIONCODE
	FROM RTR_CoverageDetailInlandMarineDim_Insert
),
UPD_Existing AS (
	SELECT
	CoverageDetailDimId, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	ModifiedDate, 
	CoverageGuid, 
	AaisClassCode, 
	AaisClassDescription, 
	IsoFireProtectionCode
	FROM RTR_CoverageDetailInlandMarineDim_Update
),
TGT_CoverageDetailInlandMarineDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailInlandMarineDim AS T
	USING UPD_Existing AS S
	ON 
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageDetailDimId = S.CoverageDetailDimId, T.AuditId = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.CoverageGuid, T.AaisClassCode = S.AaisClassCode, T.AaisClassDescription = S.AaisClassDescription, T.IsoFireProtectionCode = S.IsoFireProtectionCode
),