WITH
SQ_CoverageDetailBusinessOwners AS (
	SELECT CDBO.PremiumTransactionID,
	       CDBO.CoverageGuid,
	       CDBO.ISOBusinessOwnersPropertyRateNumber,
	       CDBO.ISOBusinessOwnersLiabilityClassGroup,
	       CDBO.ISOOccupancyType,
	       PT.PremiumTransactionID,
		CDBO.BuildingBCCCode as BuildingBCCCode,
		CDBO.BuildingClassCodeDescription as BuildingClassCodeDescription
	FROM   dbo.CoverageDetailBusinessOwners CDBO
	       INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPTDRN
	               ON CDBO.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	       INNER JOIN dbo.PremiumTransaction PT
	               ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID
	                  AND PT.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
Exp_CoverageDetailGeneralLiability AS (
	SELECT
	PremiumTransactionID AS Old_PremiumTransactionID,
	CoverageGuid,
	ISOBusinessOwnersPropertyRateNumber,
	ISOBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType,
	NewNegatePremiumTransactionID,
	BuildingBCCCode,
	BuildingClassCodeDescription
	FROM SQ_CoverageDetailBusinessOwners
),
EXP_Metadata AS (
	SELECT
	NewNegatePremiumTransactionID,
	CoverageGuid AS i_CoverageGUID,
	ISOBusinessOwnersPropertyRateNumber AS i_ISOBusinessOwnersPropertyRateNumber,
	ISOBusinessOwnersLiabilityClassGroup AS i_ISOBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType AS i_ISOOccupancyType,
	BuildingBCCCode AS i_BuildingBCCCode,
	BuildingClassCodeDescription AS i_BuildingClassCodeDescription,
	NewNegatePremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: RTRIM(LTRIM(i_CoverageGUID))
	RTRIM(LTRIM(i_CoverageGUID)) AS o_CoverageGUID,
	-- *INF*: IIF(NOT ISNULL(i_ISOBusinessOwnersPropertyRateNumber),i_ISOBusinessOwnersPropertyRateNumber,'N/A')
	IFF(
	    i_ISOBusinessOwnersPropertyRateNumber IS NOT NULL, i_ISOBusinessOwnersPropertyRateNumber,
	    'N/A'
	) AS o_ISOBusinessOwnersPropertyRateNumber,
	-- *INF*: IIF(NOT ISNULL(i_ISOBusinessOwnersLiabilityClassGroup),i_ISOBusinessOwnersLiabilityClassGroup,'N/A')
	IFF(
	    i_ISOBusinessOwnersLiabilityClassGroup IS NOT NULL, i_ISOBusinessOwnersLiabilityClassGroup,
	    'N/A'
	) AS o_ISOBusinessOwnersLiabilityClassGroup,
	i_ISOOccupancyType AS o_ISOOccupancyType,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_BuildingBCCCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_BuildingBCCCode) AS o_BuildingBCCCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_BuildingClassCodeDescription)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_BuildingClassCodeDescription) AS o_BuildingClassCodeDescription
	FROM Exp_CoverageDetailGeneralLiability
),
LKP_CoverageDetailBusinessOwners AS (
	SELECT
	PremiumTransactionID,
	CoverageGuid,
	ISOBusinessOwnersPropertyRateNumber,
	ISOBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType
	FROM (
		SELECT 
			PremiumTransactionID,
			CoverageGuid,
			ISOBusinessOwnersPropertyRateNumber,
			ISOBusinessOwnersLiabilityClassGroup,
			ISOOccupancyType
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBusinessOwners
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_CoverageDetailBusinessOwners.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailBusinessOwners.CoverageGuid AS lkp_CoverageGuid,
	LKP_CoverageDetailBusinessOwners.ISOBusinessOwnersPropertyRateNumber AS lkp_ISOBusinessOwnersPropertyRateNumber,
	LKP_CoverageDetailBusinessOwners.ISOBusinessOwnersLiabilityClassGroup AS lkp_ISOBusinessOwnersLiabilityClassGroup,
	LKP_CoverageDetailBusinessOwners.ISOOccupancyType AS lkp_ISOOccupancyType,
	EXP_Metadata.o_PremiumTransactionID AS i_PremiumTransactionID,
	EXP_Metadata.o_CoverageGUID AS i_CoverageGUID,
	EXP_Metadata.o_ISOBusinessOwnersPropertyRateNumber AS i_ISOBusinessOwnersPropertyRateNumber,
	EXP_Metadata.o_ISOBusinessOwnersLiabilityClassGroup AS i_ISOBusinessOwnersLiabilityClassGroup,
	EXP_Metadata.o_ISOOccupancyType AS i_ISOOccupancyType,
	EXP_Metadata.o_BuildingBCCCode AS i_BuildingBCCCode,
	EXP_Metadata.o_BuildingClassCodeDescription AS i_BuildingClassCodeDescription,
	-- *INF*: RTRIM(LTRIM(lkp_CoverageGuid))
	RTRIM(LTRIM(lkp_CoverageGuid)) AS v_lkp_CoverageGuid,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_TIMESTAMP('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_TIMESTAMP('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGUID AS o_CoverageGUID,
	i_ISOBusinessOwnersPropertyRateNumber AS o_ISOBusinessOwnersPropertyRateNumber,
	i_ISOBusinessOwnersLiabilityClassGroup AS o_ISOBusinessOwnersLiabilityClassGroup,
	i_ISOOccupancyType AS o_ISOOccupancyType,
	i_BuildingBCCCode AS o_BuildingBCCCode,
	i_BuildingClassCodeDescription AS o_BuildingClassCodeDescription,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),'NEW'
	-- ,'UPDATE'
	-- )
	DECODE(
	    TRUE,
	    lkp_PremiumTransactionID IS NULL, 'NEW',
	    'UPDATE'
	) AS o_ChangeFlag
	FROM EXP_Metadata
	LEFT JOIN LKP_CoverageDetailBusinessOwners
	ON LKP_CoverageDetailBusinessOwners.PremiumTransactionID = EXP_Metadata.o_PremiumTransactionID
),
RTR_Insert_Update AS (
	SELECT
	o_PremiumTransactionID AS PremiumTransactionID,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_CoverageGUID AS CoverageGuid,
	o_ISOBusinessOwnersPropertyRateNumber AS ISOBusinessOwnersPropertyRateNumber,
	o_ISOBusinessOwnersLiabilityClassGroup AS ISOBusinessOwnersLiabilityClassGroup,
	o_ISOOccupancyType AS ISOOccupancyType,
	o_BuildingBCCCode AS BuildingBCCCode,
	o_BuildingClassCodeDescription AS BuildingClassCodeDescription,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='UPDATE'),
TGT_CoverageDetailBusinessOwners_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBusinessOwners
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, ISOBusinessOwnersPropertyRateNumber, ISOBusinessOwnersLiabilityClassGroup, ISOOccupancyType, BuildingBCCCode, BuildingClassCodeDescription)
	SELECT 
	PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	ISOBUSINESSOWNERSPROPERTYRATENUMBER, 
	ISOBUSINESSOWNERSLIABILITYCLASSGROUP, 
	ISOOCCUPANCYTYPE, 
	BUILDINGBCCCODE, 
	BUILDINGCLASSCODEDESCRIPTION
	FROM RTR_Insert_Update_INSERT
),