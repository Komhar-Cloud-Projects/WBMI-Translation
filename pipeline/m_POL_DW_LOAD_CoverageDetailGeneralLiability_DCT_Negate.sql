WITH
SQ_CoverageDetailGeneralLiability AS (
	SELECT CDGL.PremiumTransactionID,
	       CDGL.CoverageGuid,
	       CDGL.RetroactiveDate,
	       CDGL.LiabilityFormCode,
	       CDGL.ISOGeneralLiabilityClassSummary,
	       CDGL.ISOGeneralLiabilityClassGroupCode,
	       PT.PremiumTransactionID 
	FROM  
	dbo.CoverageDetailGeneralLiability CDGL
	INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPTDRN
	ON CDGL.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	INNER JOIN dbo.PremiumTransaction PT ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID
	AND PT.SourceSystemId= 'DCT'
),
Exp_CoverageDetailGeneralLiability AS (
	SELECT
	PremiumTransactionID AS Old_PremiumTransactionID,
	CoverageGuid,
	RetroactiveDate,
	LiabilityFormCode,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode,
	NewNegatePremiumTransactionID
	FROM SQ_CoverageDetailGeneralLiability
),
EXP_Metadata AS (
	SELECT
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	CoverageGuid,
	RetroactiveDate,
	LiabilityFormCode,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode,
	Old_PremiumTransactionID AS Wrk_PremiumTransactionID,
	NewNegatePremiumTransactionID,
	NewNegatePremiumTransactionID AS PremiumTransactionID
	FROM Exp_CoverageDetailGeneralLiability
),
LKP_CoverageDetailGeneralLiability AS (
	SELECT
	PremiumTransactionID,
	RetroactiveDate,
	LiabilityFormCode,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode
	FROM (
		SELECT 
			PremiumTransactionID,
			RetroactiveDate,
			LiabilityFormCode,
			ISOGeneralLiabilityClassSummary,
			ISOGeneralLiabilityClassGroupCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailGeneralLiability
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND
		PremiumTransactionID IN (SELECT pt.PremiumTransactionID FROM
		PremiumTransaction pt INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate wpt
		ON pt.PremiumTransactionAKID=wpt.NewNegatePremiumTransactionAKID)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailGeneralLiability.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailGeneralLiability.RetroactiveDate AS lkp_RetroactiveDate,
	LKP_CoverageDetailGeneralLiability.LiabilityFormCode AS lkp_LiabilityFormCode,
	LKP_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassSummary AS lkp_ClassSummary,
	LKP_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassGroupCode AS lkp_ClassGroupCode,
	EXP_Metadata.PremiumTransactionID,
	EXP_Metadata.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_Metadata.o_AuditID AS AuditID,
	EXP_Metadata.o_EffectiveDate AS EffectiveDate,
	EXP_Metadata.o_ExpirationDate AS ExpirationDate,
	EXP_Metadata.o_SourceSystemID AS SourceSystemID,
	EXP_Metadata.o_CreatedDate AS CreatedDate,
	EXP_Metadata.o_ModifiedDate AS ModifiedDate,
	EXP_Metadata.CoverageGuid AS CoverageGUID,
	EXP_Metadata.RetroactiveDate,
	EXP_Metadata.LiabilityFormCode,
	EXP_Metadata.ISOGeneralLiabilityClassSummary AS ClassSummary,
	EXP_Metadata.ISOGeneralLiabilityClassGroupCode AS ClassGroup,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),'NEW',
	-- 'UPDATE'
	-- )
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 'NEW',
		'UPDATE'
	) AS o_ChangeFlag
	FROM EXP_Metadata
	LEFT JOIN LKP_CoverageDetailGeneralLiability
	ON LKP_CoverageDetailGeneralLiability.PremiumTransactionID = EXP_Metadata.PremiumTransactionID
),
RTR_Insert_Update AS (
	SELECT
	PremiumTransactionID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	CoverageGUID,
	RetroactiveDate,
	LiabilityFormCode,
	ClassSummary AS ISOGeneralLiabilityClassSummary,
	ClassGroup AS ISOGeneralLiabilityClassGroupCode,
	o_ChangeFlag AS ChangeFlag,
	lkp_PremiumTransactionID,
	lkp_RetroactiveDate,
	lkp_LiabilityFormCode,
	lkp_ClassSummary,
	lkp_ClassGroupCode
	FROM EXP_DetectChanges
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='UPDATE'),
CoverageDetailGeneralLiability_INSERT AS (
	INSERT INTO CoverageDetailGeneralLiability
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, RetroactiveDate, LiabilityFormCode, ISOGeneralLiabilityClassSummary, ISOGeneralLiabilityClassGroupCode)
	SELECT 
	PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	CoverageGUID AS COVERAGEGUID, 
	RETROACTIVEDATE, 
	LIABILITYFORMCODE, 
	ISOGENERALLIABILITYCLASSSUMMARY, 
	ISOGENERALLIABILITYCLASSGROUPCODE
	FROM RTR_Insert_Update_INSERT
),