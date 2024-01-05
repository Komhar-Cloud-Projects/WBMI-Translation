WITH
SQ_CoverageDetailGeneralLiabilityDim AS (
	SELECT CDBPD.CoverageDetailDimId as CoverageDetailDimId_BP,
	CDBPD.EffectiveDate as EffectiveDate_BP,
	CDBPD.ExpirationDate as ExpirationDate_BP,
	CDBPD.IsoBusinessOwnersPropertyRateNumber as IsoBusinessOwnersPropertyRateNumber_BP,
	CDBPD.IsoBusinessOwnersLiabilityClassGroup as IsoBusinessOwnersLiabilityClassGroup_BP,
	CDBPD.ISOOccupancyType as ISOOccupancyType_BP,
	CDBPD.BuildingBCCCode as BuildingBCCCode_BP,
	CDBPD.BuildingClassCodeDescription as BuildingClassCodeDescription_BP,
	CDD.CoverageDetailDimId,
	CDD.CoverageGuid,
	CDD.EffectiveDate,
	CDD.ExpirationDate,
	CDBP.ISOBusinessOwnersPropertyRateNumber,
	CDBP.ISOBusinessOwnersLiabilityClassGroup,
	CDBP.ISOOccupancyType,
	CDBP.BuildingBCCCode,
	CDBP.BuildingClassCodeDescription
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailBusinessOwners CDBP
	on CDBP.PremiumTransactionID=CDD.EDWPremiumTransactionPKID
	left join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBusinessOwnersDim CDBPD
	on CDBPD.CoverageDetailDimId=CDD.CoverageDetailDimId
	where CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_GetMetaData AS (
	SELECT
	CoverageDetailDimId_BP AS i_CoverageDetailDimId_BP,
	EffectiveDate_BP AS i_EffectiveDate_BP,
	ExpirationDate_BP AS i_ExpirationDate_BP,
	IsoBusinessOwnersPropertyRateNumber_BP AS i_IsoBusinessOwnersPropertyRateNumber_BP,
	IsoBusinessOwnersLiabilityClassGroup_BP AS i_IsoBusinessOwnersLiabilityClassGroup_BP,
	ISOOccupancyType_BP AS i_ISOOccupancyType_BP,
	BuildingBCCCode_BP AS i_BuildingBCCCode_BP,
	BuildingClassCodeDescription_BP AS i_BuildingClassCodeDescription_BP,
	CoverageDetailDimId AS i_CoverageDetailDimId,
	CoverageGuid AS i_CoverageGuid,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	IsoBusinessOwnersPropertyRateNumber,
	IsoBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType,
	BuildingBCCCode,
	BuildingClassCodeDescription,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CoverageDetailDimId_BP), 'NEW', 
	-- i_IsoBusinessOwnersPropertyRateNumber_BP!=IsoBusinessOwnersPropertyRateNumber
	-- OR i_IsoBusinessOwnersLiabilityClassGroup_BP!=IsoBusinessOwnersLiabilityClassGroup 
	-- OR i_ISOOccupancyType_BP != ISOOccupancyType
	-- OR i_EffectiveDate_BP!=i_EffectiveDate
	-- OR i_ExpirationDate_BP!=i_ExpirationDate
	-- OR i_BuildingBCCCode_BP != BuildingBCCCode
	-- OR i_BuildingClassCodeDescription_BP != BuildingClassCodeDescription
	-- , 'UPDATE', 'NOCHANGE')
	DECODE(TRUE,
	i_CoverageDetailDimId_BP IS NULL, 'NEW',
	i_IsoBusinessOwnersPropertyRateNumber_BP != IsoBusinessOwnersPropertyRateNumber OR i_IsoBusinessOwnersLiabilityClassGroup_BP != IsoBusinessOwnersLiabilityClassGroup OR i_ISOOccupancyType_BP != ISOOccupancyType OR i_EffectiveDate_BP != i_EffectiveDate OR i_ExpirationDate_BP != i_ExpirationDate OR i_BuildingBCCCode_BP != BuildingBCCCode OR i_BuildingClassCodeDescription_BP != BuildingClassCodeDescription, 'UPDATE',
	'NOCHANGE') AS o_ChangeFlag,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	i_CoverageGuid AS o_CoverageGuid
	FROM SQ_CoverageDetailGeneralLiabilityDim
),
RTR_CoverageDetailGeneralBusinessOwnersDim AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_CoverageDetailDimId AS CoverageDetailDimId,
	o_AuditID AS AuditId,
	o_CreatedDate AS CreateDate,
	o_ModifiedDate AS ModifedDate,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_CoverageGuid AS CoverageGuid,
	IsoBusinessOwnersPropertyRateNumber,
	IsoBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType,
	BuildingBCCCode,
	BuildingClassCodeDescription
	FROM EXP_GetMetaData
),
RTR_CoverageDetailGeneralBusinessOwnersDim_Insert AS (SELECT * FROM RTR_CoverageDetailGeneralBusinessOwnersDim WHERE ChangeFlag='NEW'),
RTR_CoverageDetailGeneralBusinessOwnersDim_Update AS (SELECT * FROM RTR_CoverageDetailGeneralBusinessOwnersDim WHERE ChangeFlag='UPDATE'),
TGT_CoverageDetailBusinessOwnersDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBusinessOwnersDim
	(CoverageDetailDimId, AuditId, CreateDate, ModifedDate, EffectiveDate, ExpirationDate, CoverageGuid, IsoBusinessOwnersPropertyRateNumber, IsoBusinessOwnersLiabilityClassGroup, ISOOccupancyType, BuildingBCCCode, BuildingClassCodeDescription)
	SELECT 
	COVERAGEDETAILDIMID, 
	AUDITID, 
	CREATEDATE, 
	MODIFEDDATE, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	COVERAGEGUID, 
	ISOBUSINESSOWNERSPROPERTYRATENUMBER, 
	ISOBUSINESSOWNERSLIABILITYCLASSGROUP, 
	ISOOCCUPANCYTYPE, 
	BUILDINGBCCCODE, 
	BUILDINGCLASSCODEDESCRIPTION
	FROM RTR_CoverageDetailGeneralBusinessOwnersDim_Insert
),
UPD_Existing AS (
	SELECT
	CoverageDetailDimId, 
	AuditId AS AuditID, 
	ModifedDate AS ModifiedDate, 
	EffectiveDate, 
	ExpirationDate, 
	CoverageGuid, 
	IsoBusinessOwnersPropertyRateNumber, 
	IsoBusinessOwnersLiabilityClassGroup, 
	ISOOccupancyType, 
	BuildingBCCCode AS BuildingBCCCode3, 
	BuildingClassCodeDescription AS BuildingClassCodeDescription3
	FROM RTR_CoverageDetailGeneralBusinessOwnersDim_Update
),
TGT_CoverageDetailBusinessOwnersDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBusinessOwnersDim AS T
	USING UPD_Existing AS S
	ON 
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageDetailDimId = S.CoverageDetailDimId, T.AuditId = S.AuditID, T.ModifedDate = S.ModifiedDate, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.CoverageGuid = S.CoverageGuid, T.IsoBusinessOwnersPropertyRateNumber = S.IsoBusinessOwnersPropertyRateNumber, T.IsoBusinessOwnersLiabilityClassGroup = S.IsoBusinessOwnersLiabilityClassGroup, T.ISOOccupancyType = S.ISOOccupancyType, T.BuildingBCCCode = S.BuildingBCCCode3, T.BuildingClassCodeDescription = S.BuildingClassCodeDescription3
),