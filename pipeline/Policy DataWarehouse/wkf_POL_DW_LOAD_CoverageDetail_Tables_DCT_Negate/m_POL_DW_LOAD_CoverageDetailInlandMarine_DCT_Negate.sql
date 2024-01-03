WITH
SQ_CoverageDetailInlandMarine AS (
	SELECT CDIM.PremiumTransactionID,
	       CDIM.CoverageGuid,
	       CDIM.IsoFireProtectionCode,
	       PT.PremiumTransactionID 
	FROM  
	dbo.CoverageDetailInlandMarine CDIM
	INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPTDRN 
	ON CDIM.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	INNER JOIN dbo.PremiumTransaction PT ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID 
	AND PT.SourceSystemId= '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND CDIM.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_CoverageDetailInlandMarine AS (
	SELECT
	PremiumTransactionID,
	NewNegatePremiumTransactionID,
	NewNegatePremiumTransactionID AS o_PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	CoverageGuid,
	IsoFireProtectionCode
	FROM SQ_CoverageDetailInlandMarine
),
LKP_CoverageDetailInlandMarine AS (
	SELECT
	PremiumTransactionId
	FROM (
		SELECT 
			PremiumTransactionId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailInlandMarine
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId ORDER BY PremiumTransactionId DESC) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_CoverageDetailInlandMarine.PremiumTransactionId AS lkp_PremiumTransactionId,
	EXP_CoverageDetailInlandMarine.o_PremiumTransactionID AS PremiumTransactionId,
	EXP_CoverageDetailInlandMarine.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_CoverageDetailInlandMarine.o_AuditID AS AuditID,
	EXP_CoverageDetailInlandMarine.o_EffectiveDate AS EffectiveDate,
	EXP_CoverageDetailInlandMarine.o_ExpirationDate AS ExpirationDate,
	EXP_CoverageDetailInlandMarine.o_SourceSystemID AS SourceSystemID,
	EXP_CoverageDetailInlandMarine.o_CreatedDate AS CreatedDate,
	EXP_CoverageDetailInlandMarine.o_ModifiedDate AS ModifiedDate,
	EXP_CoverageDetailInlandMarine.CoverageGuid,
	EXP_CoverageDetailInlandMarine.IsoFireProtectionCode
	FROM EXP_CoverageDetailInlandMarine
	LEFT JOIN LKP_CoverageDetailInlandMarine
	ON LKP_CoverageDetailInlandMarine.PremiumTransactionId = EXP_CoverageDetailInlandMarine.o_PremiumTransactionID
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(lkp_PremiumTransactionId)),
TGT_CoverageDetailInlandMarine_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailInlandMarine
	(PremiumTransactionId, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, IsoFireProtectionCode)
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
	ISOFIREPROTECTIONCODE
	FROM RTR_Insert_Update_INSERT
),