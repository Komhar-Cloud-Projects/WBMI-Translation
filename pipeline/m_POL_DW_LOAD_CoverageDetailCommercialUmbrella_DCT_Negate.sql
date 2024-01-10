WITH
SQ_CoverageDetailCommercialUmbrella AS (
	SELECT CDCU.PremiumTransactionID,
	       CDCU.CoverageGuid,
	       CDCU.UmbrellaCoverageScope,
	       CDCU.RetroactiveDate,
	       CDCU.UmbrellaLayer,
	       PT.PremiumTransactionID
	FROM   dbo.CoverageDetailCommercialUmbrella CDCU
	       INNER JOIN WorkPremiumTransactionDataRepairNegate WPTDRN
	               ON CDCU.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	       INNER JOIN dbo.PremiumTransaction PT
	               ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID         
	AND PT.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND CDCU.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
Exp_CoverageDetailCommercialUmbrella AS (
	SELECT
	PremiumTransactionID AS Old_PremiumTransactionID,
	CoverageGuid,
	UmbrellaCoverageScope,
	RetroactiveDate,
	UmbrellaLayer,
	NewNegatePremiumTransactionID
	FROM SQ_CoverageDetailCommercialUmbrella
),
EXP_DefaultValue AS (
	SELECT
	CoverageGuid AS i_CoverageGuid,
	RetroactiveDate AS i_RetroActiveDate,
	UmbrellaCoverageScope,
	UmbrellaLayer,
	NewNegatePremiumTransactionID,
	NewNegatePremiumTransactionID AS o_PremiumTransactionID,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGuid AS o_CoverageGuid,
	-- *INF*: IIF(ISNULL(i_RetroActiveDate), TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'), i_RetroActiveDate)
	IFF(i_RetroActiveDate IS NULL,
		TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
		),
		i_RetroActiveDate
	) AS o_RetroActiveDate
	FROM Exp_CoverageDetailCommercialUmbrella
),
LKP_CoverageDetailCommercialUmbrella AS (
	SELECT
	PremiumTransactionID,
	CoverageGuid,
	UmbrellaCoverageScope,
	RetroActiveDate,
	UmbrellaLayer
	FROM (
		SELECT 
			PremiumTransactionID,
			CoverageGuid,
			UmbrellaCoverageScope,
			RetroActiveDate,
			UmbrellaLayer
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialUmbrella
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChange AS (
	SELECT
	LKP_CoverageDetailCommercialUmbrella.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailCommercialUmbrella.CoverageGuid AS lkp_CoverageGuid,
	LKP_CoverageDetailCommercialUmbrella.UmbrellaCoverageScope AS lkp_UmbrellaCoverageScope,
	LKP_CoverageDetailCommercialUmbrella.RetroActiveDate AS lkp_RetroActiveDate,
	LKP_CoverageDetailCommercialUmbrella.UmbrellaLayer AS lkp_UmbrellaLayer,
	EXP_DefaultValue.o_PremiumTransactionID AS PremiumTransactionID,
	EXP_DefaultValue.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_DefaultValue.o_AuditID AS AuditID,
	EXP_DefaultValue.o_EffectiveDate AS EffectiveDate,
	EXP_DefaultValue.o_ExpirationDate AS ExpirationDate,
	EXP_DefaultValue.o_SourceSystemID AS SourceSystemID,
	EXP_DefaultValue.o_CreatedDate AS CreatedDate,
	EXP_DefaultValue.o_ModifiedDate AS ModifiedDate,
	EXP_DefaultValue.o_CoverageGuid AS CoverageGuid,
	EXP_DefaultValue.o_RetroActiveDate AS RetroActiveDate,
	EXP_DefaultValue.UmbrellaCoverageScope,
	EXP_DefaultValue.UmbrellaLayer,
	-- *INF*: IIF(ISNULL(lkp_PremiumTransactionID), 'NEW',  'UPDATE') 
	-- 
	IFF(lkp_PremiumTransactionID IS NULL,
		'NEW',
		'UPDATE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM EXP_DefaultValue
	LEFT JOIN LKP_CoverageDetailCommercialUmbrella
	ON LKP_CoverageDetailCommercialUmbrella.PremiumTransactionID = EXP_DefaultValue.o_PremiumTransactionID
),
RTR_InsertElseUpdate AS (
	SELECT
	lkp_PremiumTransactionID,
	PremiumTransactionID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	CoverageGuid,
	UmbrellaCoverageScope,
	RetroActiveDate,
	o_ChangeFlag AS ChangeFlag,
	RetroActiveDate AS RetroActiveDate4,
	UmbrellaLayer
	FROM EXP_DetectChange
),
RTR_InsertElseUpdate_INSERT AS (SELECT * FROM RTR_InsertElseUpdate WHERE ChangeFlag='NEW'),
RTR_InsertElseUpdate_UPDATE AS (SELECT * FROM RTR_InsertElseUpdate WHERE ChangeFlag='UPDATE'),
TGT_CoverageDetailCommercialUmbrella_Insert AS (
	INSERT INTO CoverageDetailCommercialUmbrella
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, UmbrellaCoverageScope, RetroactiveDate, UmbrellaLayer)
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
	UMBRELLACOVERAGESCOPE, 
	RetroActiveDate AS RETROACTIVEDATE, 
	UMBRELLALAYER
	FROM RTR_InsertElseUpdate_INSERT
),