WITH
SQ_CoverageDeductibleBridge_DCT_Negate AS (
	SELECT CDB.PremiumTransactionAKId,
	                  CDB.CoverageDeductibleId,
		            CDB.CoverageDeductibleIdCount,
		            CDB.CoverageDeductibleControl,
					WPTDRN.NewNegatePremiumTransactionAKID
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionDataRepairNegate WPTDRN
	ON CDB.PremiumTransactionAKId = WPTDRN.OriginalPremiumTransactionAKID
	and CDB.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
Exp_CoverageDeductibleBridge_DCT_Negate AS (
	SELECT
	PremiumTransactionAKId,
	CoverageDeductibleId,
	CoverageDeductibleIdCount,
	CoverageDeductibleControl,
	NewNegatePremiumTransactionAKID
	FROM SQ_CoverageDeductibleBridge_DCT_Negate
),
EXP_Metadata AS (
	SELECT
	CoverageDeductibleId,
	CoverageDeductibleIdCount,
	CoverageDeductibleControl,
	NewNegatePremiumTransactionAKID,
	NewNegatePremiumTransactionAKID AS o_PremiumTransactionAKID
	FROM Exp_CoverageDeductibleBridge_DCT_Negate
),
LKP_CoverageDeductibleBridge AS (
	SELECT
	CoverageDeductibleBridgeId,
	PremiumTransactionAKId,
	CoverageDeductibleId,
	CoverageDeductibleIdCount,
	CoverageDeductibleControl
	FROM (
		SELECT 
			CoverageDeductibleBridgeId,
			PremiumTransactionAKId,
			CoverageDeductibleId,
			CoverageDeductibleIdCount,
			CoverageDeductibleControl
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDeductibleBridge
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageDeductibleId ORDER BY CoverageDeductibleBridgeId) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDeductibleBridge.CoverageDeductibleBridgeId AS lkp_CoverageDeductibleBridgeId,
	LKP_CoverageDeductibleBridge.PremiumTransactionAKId AS lkp_PremiumTransactionAKId,
	LKP_CoverageDeductibleBridge.CoverageDeductibleId AS lkp_CoverageDeductibleId,
	LKP_CoverageDeductibleBridge.CoverageDeductibleIdCount AS lkp_CoverageDeductibleIdCount,
	LKP_CoverageDeductibleBridge.CoverageDeductibleControl AS lkp_CoverageDeductibleControl,
	EXP_Metadata.CoverageDeductibleId AS In_CoverageDeductibleId,
	EXP_Metadata.CoverageDeductibleIdCount AS In_CoverageDeductibleIdCount,
	EXP_Metadata.CoverageDeductibleControl AS In_CoverageDeductibleControl,
	EXP_Metadata.o_PremiumTransactionAKID AS In_PremiumTransactionAKID,
	lkp_CoverageDeductibleBridgeId AS o_CoverageDeductibleBridgeId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AUDITID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	In_PremiumTransactionAKID AS o_PremiumTransactionAKID,
	In_CoverageDeductibleId AS o_CoverageDeductibleId,
	In_CoverageDeductibleIdCount AS o_CoverageDeductibleIdCount,
	In_CoverageDeductibleControl AS o_CoverageDeductibleControl,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_CoverageDeductibleBridgeId),'NEW'
	-- ,'UPDATE'
	-- )
	DECODE(TRUE,
		lkp_CoverageDeductibleBridgeId IS NULL, 'NEW',
		'UPDATE') AS o_ChangeFlag
	FROM EXP_Metadata
	LEFT JOIN LKP_CoverageDeductibleBridge
	ON LKP_CoverageDeductibleBridge.PremiumTransactionAKId = EXP_Metadata.o_PremiumTransactionAKID AND LKP_CoverageDeductibleBridge.CoverageDeductibleId = EXP_Metadata.CoverageDeductibleId
),
RTR_Insert_Update AS (
	SELECT
	o_CoverageDeductibleBridgeId AS CoverageDeductibleBridgeId,
	o_AUDITID AS AUDITID,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_PremiumTransactionAKID AS PremiumTransactionAKID,
	o_CoverageDeductibleId AS CoverageDeductibleId,
	o_CoverageDeductibleIdCount AS CoverageDeductibleIdCount,
	o_CoverageDeductibleControl AS CoverageDeductibleControl,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_DetectChanges
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='UPDATE'),
EXP_Insert AS (
	SELECT
	AUDITID,
	SourceSystemID,
	CreatedDate,
	PremiumTransactionAKID,
	CoverageDeductibleId,
	CoverageDeductibleIdCount,
	CoverageDeductibleControl
	FROM RTR_Insert_Update_INSERT
),
CoverageDeductibleBridge_Insert AS (
	INSERT INTO CoverageDeductibleBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageDeductibleId, CoverageDeductibleIdCount, CoverageDeductibleControl)
	SELECT 
	AUDITID AS AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	COVERAGEDEDUCTIBLEID, 
	COVERAGEDEDUCTIBLEIDCOUNT, 
	COVERAGEDEDUCTIBLECONTROL
	FROM EXP_Insert
),