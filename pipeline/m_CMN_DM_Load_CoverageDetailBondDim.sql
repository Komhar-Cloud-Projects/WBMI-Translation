WITH
SQ_CoverageDetailCrimeDim AS (
	select distinct
	CDBD.CoverageDetailDimId as CoverageDetailDimId_BN ,
	CDBD.SFAAClassCode as SFAAClassCode_BN,
	CDBD.SFAAClassDescription as SFAAClassDescription_BN,
	CDBD.EffectiveDate as EffectiveDate_BN ,
	CDBD.ExpirationDate as ExpirationDate_BN,
	CDD.CoverageDetailDimId,
	SC.ClassCode,
	SC.StatisticalCoverageEffectiveDate as CoverageEffectiveDate,
	--'SFAA' as ClassCodeOrganizationCode,  
	SC.ClassCodeOrganizationCode,
	CDD.CoverageGUID,
	CDD.EffectiveDate,
	CDD.ExpirationDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on SC.PolicyCoverageAKId=PC.PolicyCoverageAKId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
	on PC.PolicyAKID=P.pol_ak_id and p.crrnt_snpsht_flag=1
	join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	on CDD.CoverageGUID=SC.CoverageGUID
	left join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBondDim CDBD
	on CDBD.CoverageDetailDimId=CDD.CoverageDetailDimId
	where  SC.CurrentSnapshotFlag=1 and SC.SourceSystemID='PMS' and CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}'  and  (
	substring(P.pol_sym,1,2) in ('NC', 'NJ', 'NL', 'NM', 'NO', 'NF')  
	--or (substring(P.pol_sym,1,2) = 'NF' and P.ClassOfBusiness in ('XN', 'XO', 'XP', 'XQ')  )
	) @{pipeline().parameters.WHERE_CLAUSE_PMS} 
	
	--It is not required for DCT
	union all
	
	select distinct CDBD.CoverageDetailDimId as CoverageDetailDimId_BN ,
	CDBD.SFAAClassCode as SFAAClassCode_BN,
	CDBD.SFAAClassDescription as SFAAClassDescription_BN,
	CDBD.EffectiveDate as EffectiveDate_BN ,
	CDBD.ExpirationDate as ExpirationDate_BN,
	CDD.CoverageDetailDimId,
	RC.ClassCode, 
	RC.RatingCoverageEffectiveDate as CoverageEffectiveDate,
	--'SFAA' as ClassCodeOrganizationCode, 
	RC.ClassCodeOrganizationCode,
	CDD.CoverageGUID,
	CDD.EffectiveDate,
	CDD.ExpirationDate
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.premiumtransaction PT on CDD.EDWPremiumTransactionPKId=PT.PremiumTransactionID
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC on PT.RatingCoverageAKId=RC.RatingCoverageAKID and RC.EffectiveDate= PT.EffectiveDate
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on RC.PolicyCoverageAKId=PC.PolicyCoverageAKId and PC.CurrentSnapshotFlag=1 and PC.SourceSystemID= 'DCT'
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
	on PC.PolicyAKID=P.pol_ak_id and p.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product Product
	on RC.ProductAKId=Product.ProductAKId
	and Product.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBondDim CDBD
	on CDBD.CoverageDetailDimId=CDD.CoverageDetailDimId
	where CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}' and PC.InsuranceLine='Crime'
	and Product.ProductCode='620'  
	 @{pipeline().parameters.WHERE_CLAUSE_DCT}
),
LKP_ClassificationReference AS (
	SELECT
	ClassDescription,
	OriginatingOrganizationCode,
	ClassCode,
	ClassCodeEffectiveDate,
	ClassCodeExpirationDate
	FROM (
		SELECT LTRIM(RTRIM(ClassDescription)) as ClassDescription, 
		LTRIM(RTRIM(OriginatingOrganizationCode)) as OriginatingOrganizationCode, 
		LTRIM(RTRIM(ClassCode)) as ClassCode, 
		ClassCodeEffectiveDate as ClassCodeEffectiveDate, 
		ClassCodeExpirationDate as ClassCodeExpirationDate 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.ClassificationReference
		WHERE OriginatingOrganizationCode='SFAA'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY OriginatingOrganizationCode,ClassCode,ClassCodeEffectiveDate,ClassCodeExpirationDate ORDER BY ClassDescription DESC) = 1
),
EXP_GetMetaData AS (
	SELECT
	SQ_CoverageDetailCrimeDim.CoverageDetailDimId_BN AS i_CoverageDetailDimId_BN,
	SQ_CoverageDetailCrimeDim.SFAAClassCode_BN AS i_SFAAClassCode_BN,
	SQ_CoverageDetailCrimeDim.SFAAClassDescription_BN AS i_SFAAClassDescription_BN,
	SQ_CoverageDetailCrimeDim.EffectiveDate_BN AS i_EffectiveDate_BN,
	SQ_CoverageDetailCrimeDim.ExpirationDate_BN AS i_ExpirationDate_BN,
	SQ_CoverageDetailCrimeDim.CoverageDetailDimId AS i_CoverageDetailDimId,
	SQ_CoverageDetailCrimeDim.ClassCode AS i_ClassCode,
	LKP_ClassificationReference.ClassDescription AS i_ClassDescription,
	SQ_CoverageDetailCrimeDim.ClassCodeOrganizationCode AS i_ClassCodeOrganizationCode,
	SQ_CoverageDetailCrimeDim.CoverageGuid AS i_CoverageGuid,
	SQ_CoverageDetailCrimeDim.EffectiveDate AS i_EffectiveDate,
	SQ_CoverageDetailCrimeDim.ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF( i_ClassCodeOrganizationCode<>'SFAA', 'N/A', IIF(ISNULL(i_ClassCode), 'N/A', i_ClassCode))
	IFF(i_ClassCodeOrganizationCode <> 'SFAA',
		'N/A',
		IFF(i_ClassCode IS NULL,
			'N/A',
			i_ClassCode
		)
	) AS v_SfaaClassCode,
	-- *INF*: IIF(ISNULL(i_ClassDescription), 'N/A', i_ClassDescription)
	IFF(i_ClassDescription IS NULL,
		'N/A',
		i_ClassDescription
	) AS v_SfaaClassDescription,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CoverageDetailDimId_BN), 'NEW',
	-- LTRIM(RTRIM(i_SFAAClassCode_BN)) != v_SfaaClassCode OR i_SFAAClassDescription_BN != v_SfaaClassDescription OR i_EffectiveDate_BN != i_EffectiveDate OR i_ExpirationDate_BN != i_ExpirationDate, 'UPDATE', 'NOCHANGE')
	DECODE(TRUE,
		i_CoverageDetailDimId_BN IS NULL, 'NEW',
		LTRIM(RTRIM(i_SFAAClassCode_BN
			)
		) != v_SfaaClassCode 
		OR i_SFAAClassDescription_BN != v_SfaaClassDescription 
		OR i_EffectiveDate_BN != i_EffectiveDate 
		OR i_ExpirationDate_BN != i_ExpirationDate, 'UPDATE',
		'NOCHANGE'
	) AS o_ChangeFlag,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGuid AS o_CoverageGuid,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	v_SfaaClassCode AS o_SfaaClassCode,
	v_SfaaClassDescription AS o_SfaaClassDescription
	FROM SQ_CoverageDetailCrimeDim
	LEFT JOIN LKP_ClassificationReference
	ON LKP_ClassificationReference.OriginatingOrganizationCode = SQ_CoverageDetailCrimeDim.ClassCodeOrganizationCode AND LKP_ClassificationReference.ClassCode = SQ_CoverageDetailCrimeDim.ClassCode AND LKP_ClassificationReference.ClassCodeEffectiveDate <= SQ_CoverageDetailCrimeDim.CoverageEffectiveDate AND LKP_ClassificationReference.ClassCodeExpirationDate >= SQ_CoverageDetailCrimeDim.CoverageEffectiveDate
),
RTR_CoverageDetailCrimeDim AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_CoverageDetailDimId AS CoverageDetailDimId,
	o_AuditID AS AuditID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_CoverageGuid AS CoverageGuid,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SfaaClassCode AS SfaaClassCode,
	o_SfaaClassDescription AS SfaaClassDescription
	FROM EXP_GetMetaData
),
RTR_CoverageDetailCrimeDim_Insert AS (SELECT * FROM RTR_CoverageDetailCrimeDim WHERE ChangeFlag='NEW'),
RTR_CoverageDetailCrimeDim_Update AS (SELECT * FROM RTR_CoverageDetailCrimeDim WHERE ChangeFlag='UPDATE'),
TGT_CoverageDetailBondDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBondDim
	(CoverageDetailDimId, AuditId, CreateDate, ModifedDate, CoverageGuid, EffectiveDate, ExpirationDate, SFAAClassCode, SFAAClassDescription)
	SELECT 
	COVERAGEDETAILDIMID, 
	AuditID AS AUDITID, 
	CreatedDate AS CREATEDATE, 
	ModifiedDate AS MODIFEDDATE, 
	COVERAGEGUID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SfaaClassCode AS SFAACLASSCODE, 
	SfaaClassDescription AS SFAACLASSDESCRIPTION
	FROM RTR_CoverageDetailCrimeDim_Insert
),
UPD_Existing AS (
	SELECT
	CoverageDetailDimId, 
	AuditID, 
	ModifiedDate, 
	CoverageGuid, 
	EffectiveDate, 
	ExpirationDate, 
	SfaaClassCode, 
	SfaaClassDescription
	FROM RTR_CoverageDetailCrimeDim_Update
),
TGT_CoverageDetailBondDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBondDim AS T
	USING UPD_Existing AS S
	ON 
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageDetailDimId = S.CoverageDetailDimId, T.AuditId = S.AuditID, T.ModifedDate = S.ModifiedDate, T.CoverageGuid = S.CoverageGuid, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.SFAAClassCode = S.SfaaClassCode, T.SFAAClassDescription = S.SfaaClassDescription
),