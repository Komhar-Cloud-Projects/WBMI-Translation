WITH
SQ_CoverageLimitBridge_DCT_Negate AS (
	SELECT CLB.PremiumTransactionAKId,
	                  CLB.CoverageLimitId,
		            CLB.CoverageLimitIDCount,
		            CLB.CoverageLimitControl,
				WPTDRN.NewNegatePremiumTransactionAKID
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionDataRepairNegate WPTDRN
	ON CLB.PremiumTransactionAKId = WPTDRN.OriginalPremiumTransactionAKID
	and CLB.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
Exp_CoverageLimitBridge_DCT_Negate AS (
	SELECT
	PremiumTransactionAKId AS OldPremiumTransactionAKId,
	CoverageLimitId,
	CoverageLimitIDCount,
	CoverageLimitControl,
	NewNegatePremiumTransactionAKID
	FROM SQ_CoverageLimitBridge_DCT_Negate
),
Exp_Metadata AS (
	SELECT
	CoverageLimitId,
	CoverageLimitIDCount,
	CoverageLimitControl,
	NewNegatePremiumTransactionAKID AS In_NewNegatePremiumTransactionAKID,
	In_NewNegatePremiumTransactionAKID AS o_PremiumTransactionAKID
	FROM Exp_CoverageLimitBridge_DCT_Negate
),
LKP_CoverageLimitBridge AS (
	SELECT
	CoverageLimitBridgeID,
	PremiumTransactionAKId,
	CoverageLimitId,
	CoverageLimitIDCount,
	CoverageLimitControl,
	In_PremiumTransactionAKID,
	In_CoverageLimitId
	FROM (
		SELECT 
			CoverageLimitBridgeID,
			PremiumTransactionAKId,
			CoverageLimitId,
			CoverageLimitIDCount,
			CoverageLimitControl,
			In_PremiumTransactionAKID,
			In_CoverageLimitId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimitBridge
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageLimitId ORDER BY CoverageLimitBridgeID) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_CoverageLimitBridge.CoverageLimitBridgeID AS lkp_CoverageLimitBridgeID,
	LKP_CoverageLimitBridge.PremiumTransactionAKId AS lkp_PremiumTransactionAKId,
	LKP_CoverageLimitBridge.CoverageLimitId AS lkp_CoverageLimitId,
	LKP_CoverageLimitBridge.CoverageLimitIDCount AS lkp_CoverageLimitIDCount,
	LKP_CoverageLimitBridge.CoverageLimitControl AS lkp_CoverageLimitControl,
	Exp_Metadata.o_PremiumTransactionAKID AS In_PremiumTransactionAKID,
	Exp_Metadata.CoverageLimitId AS In_CoverageLimitID,
	Exp_Metadata.CoverageLimitIDCount AS In_CoverageLimitIDCount,
	Exp_Metadata.CoverageLimitControl AS In_CoverageLimitControl,
	lkp_CoverageLimitBridgeID AS o_CoverageLimitBridgeID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	In_PremiumTransactionAKID AS o_PremiumTransactionAKID,
	In_CoverageLimitID AS o_CoverageLimitID,
	In_CoverageLimitIDCount AS o_CoverageLimitIDCount,
	In_CoverageLimitControl AS o_CoverageLimitControl,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_CoverageLimitBridgeID),'NEW'
	-- ,'UPDATE'
	-- )
	DECODE(TRUE,
		lkp_CoverageLimitBridgeID IS NULL, 'NEW',
		'UPDATE') AS o_ChangeFlag
	FROM Exp_Metadata
	LEFT JOIN LKP_CoverageLimitBridge
	ON LKP_CoverageLimitBridge.PremiumTransactionAKId = Exp_Metadata.o_PremiumTransactionAKID AND LKP_CoverageLimitBridge.CoverageLimitId = Exp_Metadata.CoverageLimitId
),
RTR_Insert_Update AS (
	SELECT
	o_CoverageLimitBridgeID AS CoverageLimitBridgeID,
	o_AuditID AS AuditID,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_PremiumTransactionAKID AS PremiumTransactionAKID,
	o_CoverageLimitID AS CoverageLimitID,
	o_CoverageLimitIDCount AS CoverageLimitIDCount,
	o_CoverageLimitControl AS CoverageLimitControl,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='UPDATE'),
EXP_Insert AS (
	SELECT
	AuditID,
	SourceSystemID,
	CreatedDate,
	PremiumTransactionAKID,
	CoverageLimitID,
	CoverageLimitIDCount,
	CoverageLimitControl
	FROM RTR_Insert_Update_INSERT
),
CoverageLimitBridge__Negate_Insert AS (
	INSERT INTO CoverageLimitBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageLimitId, CoverageLimitIDCount, CoverageLimitControl)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	CoverageLimitID AS COVERAGELIMITID, 
	COVERAGELIMITIDCOUNT, 
	COVERAGELIMITCONTROL
	FROM EXP_Insert
),