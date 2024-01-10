WITH
SQ_CoverageDetailCommercialProperty AS (
	SELECT CDCP.PremiumTransactionID,
	       CDCP.CoverageGuid,
	       CDCP.IsoFireProtectionCode,
	       CDCP.MultiLocationCreditFactor,
	       CDCP.PreferredPropertyFactor,
	       CDCP.SprinklerFlag,
	       CDCP.RetroactiveDate,
	       CDCP.ISOCommercialPropertyCauseofLossGroup,
	       CDCP.ISOCommercialPropertyRatingGroupCode,
	       CDCP.ISOSpecialCauseOfLossCategoryCode,
	       CDCP.RateType,
	       CDCP.CommercialPropertySpecialClass,
	       PT.PremiumTransactionID
	FROM   dbo.CoverageDetailCommercialProperty CDCP
	       INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPTDRN
	               ON CDCP.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	       INNER JOIN dbo.PremiumTransaction PT
	               ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID
	                  AND PT.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_CoverageDetailCommercialProperty AS (
	SELECT
	NewNegatePremiumTransactionID,
	NewNegatePremiumTransactionID AS o_PremiumTransactionID,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	CoverageGuid,
	IsoFireProtectionCode,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	SprinklerFlag,
	RetroactiveDate,
	ISOCommercialPropertyCauseofLossGroup,
	ISOCommercialPropertyRatingGroupCode,
	ISOSpecialCauseOfLossCategoryCode,
	RateType,
	CommercialPropertySpecialClass,
	PremiumTransactionID
	FROM SQ_CoverageDetailCommercialProperty
),
LKP_CoverageDetailCommercialProperty AS (
	SELECT
	PremiumTransactionId,
	IsoFireProtectionCode,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	SprinklerFlag,
	ISOCommercialPropertyCauseofLossGroup,
	ISOCommercialPropertyRatingGroupCode,
	ISOSpecialCauseOfLossCategoryCode,
	RateType,
	CommercialPropertySpecialClass
	FROM (
		SELECT 
			PremiumTransactionId,
			IsoFireProtectionCode,
			MultiLocationCreditFactor,
			PreferredPropertyFactor,
			SprinklerFlag,
			ISOCommercialPropertyCauseofLossGroup,
			ISOCommercialPropertyRatingGroupCode,
			ISOSpecialCauseOfLossCategoryCode,
			RateType,
			CommercialPropertySpecialClass
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty
		WHERE PremiumTransactionID IN (SELECT pt.PremiumTransactionID FROM
		PremiumTransaction pt INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate wpt
		ON pt.PremiumTransactionAKID=wpt.NewNegatePremiumTransactionAKID)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId ORDER BY PremiumTransactionId) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailCommercialProperty.PremiumTransactionId AS lkp_PremiumTransactionId,
	LKP_CoverageDetailCommercialProperty.IsoFireProtectionCode AS lkp_IsoFireProtectionCode,
	LKP_CoverageDetailCommercialProperty.MultiLocationCreditFactor AS lkp_MultiLocationCreditFactor,
	LKP_CoverageDetailCommercialProperty.PreferredPropertyFactor AS lkp_PreferredPropertyFactor,
	LKP_CoverageDetailCommercialProperty.SprinklerFlag AS lkp_SprinklerFlag_origin,
	-- *INF*: DECODE(lkp_SprinklerFlag_origin,'T',1,'F',0,NULL)
	DECODE(lkp_SprinklerFlag_origin,
		'T', 1,
		'F', 0,
		NULL) AS lkp_SprinklerFlag,
	LKP_CoverageDetailCommercialProperty.ISOCommercialPropertyCauseofLossGroup AS lkp_ISOPropertyCauseofLossGroup,
	LKP_CoverageDetailCommercialProperty.ISOCommercialPropertyRatingGroupCode AS lkp_ISOCPRatingGroup,
	LKP_CoverageDetailCommercialProperty.ISOSpecialCauseOfLossCategoryCode AS lkp_ISOSpecialCauseOfLossCategory,
	LKP_CoverageDetailCommercialProperty.RateType AS lkp_RateType,
	LKP_CoverageDetailCommercialProperty.CommercialPropertySpecialClass AS lkp_PropertySpecialClass,
	EXP_CoverageDetailCommercialProperty.o_PremiumTransactionID AS PremiumTransactionID,
	EXP_CoverageDetailCommercialProperty.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_CoverageDetailCommercialProperty.o_AuditID AS AuditID,
	EXP_CoverageDetailCommercialProperty.o_EffectiveDate AS EffectiveDate,
	EXP_CoverageDetailCommercialProperty.o_ExpirationDate AS ExpirationDate,
	EXP_CoverageDetailCommercialProperty.o_SourceSystemID AS SourceSystemID,
	EXP_CoverageDetailCommercialProperty.o_CreatedDate AS CreatedDate,
	EXP_CoverageDetailCommercialProperty.o_ModifiedDate AS ModifiedDate,
	EXP_CoverageDetailCommercialProperty.CoverageGuid,
	EXP_CoverageDetailCommercialProperty.IsoFireProtectionCode,
	EXP_CoverageDetailCommercialProperty.MultiLocationCreditFactor,
	EXP_CoverageDetailCommercialProperty.PreferredPropertyFactor,
	EXP_CoverageDetailCommercialProperty.SprinklerFlag,
	EXP_CoverageDetailCommercialProperty.ISOCommercialPropertyCauseofLossGroup AS ISOPropertyCauseofLossGroup,
	EXP_CoverageDetailCommercialProperty.ISOSpecialCauseOfLossCategoryCode AS ISOSpecialCauseOfLossCategory,
	EXP_CoverageDetailCommercialProperty.RateType,
	EXP_CoverageDetailCommercialProperty.ISOCommercialPropertyRatingGroupCode AS ISOCPRatingGroup,
	EXP_CoverageDetailCommercialProperty.CommercialPropertySpecialClass AS PropertySpecialClass,
	EXP_CoverageDetailCommercialProperty.RetroactiveDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionId),'NEW',
	-- 'UPDATE'
	-- )
	DECODE(TRUE,
		lkp_PremiumTransactionId IS NULL, 'NEW',
		'UPDATE') AS ChangeFlag
	FROM EXP_CoverageDetailCommercialProperty
	LEFT JOIN LKP_CoverageDetailCommercialProperty
	ON LKP_CoverageDetailCommercialProperty.PremiumTransactionId = EXP_CoverageDetailCommercialProperty.o_PremiumTransactionID
),
RTR_Target AS (
	SELECT
	ChangeFlag,
	PremiumTransactionID AS PremiumTransactionId,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	CoverageGuid,
	IsoFireProtectionCode,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	SprinklerFlag,
	ISOPropertyCauseofLossGroup,
	ISOCPRatingGroup,
	ISOSpecialCauseOfLossCategory,
	RateType,
	PropertySpecialClass,
	RetroactiveDate
	FROM EXP_DetectChanges
),
RTR_Target_INSERT AS (SELECT * FROM RTR_Target WHERE ChangeFlag='NEW'),
RTR_Target_UPDATE AS (SELECT * FROM RTR_Target WHERE ChangeFlag='UPDATE'),
CoverageDetailCommercialProperty_Insert AS (
	INSERT INTO CoverageDetailCommercialProperty
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, IsoFireProtectionCode, MultiLocationCreditFactor, PreferredPropertyFactor, SprinklerFlag, RetroactiveDate, ISOCommercialPropertyCauseofLossGroup, ISOCommercialPropertyRatingGroupCode, ISOSpecialCauseOfLossCategoryCode, RateType, CommercialPropertySpecialClass)
	SELECT 
	PremiumTransactionId AS PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	ISOFIREPROTECTIONCODE, 
	MULTILOCATIONCREDITFACTOR, 
	PREFERREDPROPERTYFACTOR, 
	SPRINKLERFLAG, 
	RETROACTIVEDATE, 
	ISOPropertyCauseofLossGroup AS ISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP, 
	ISOCPRatingGroup AS ISOCOMMERCIALPROPERTYRATINGGROUPCODE, 
	ISOSpecialCauseOfLossCategory AS ISOSPECIALCAUSEOFLOSSCATEGORYCODE, 
	RATETYPE, 
	PropertySpecialClass AS COMMERCIALPROPERTYSPECIALCLASS
	FROM RTR_Target_INSERT
),