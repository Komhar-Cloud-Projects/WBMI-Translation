WITH
SQ_WorkBlanketPremiumBreakOut_DCT_Negate AS (
	SELECT WBP.BlanketPremiumTransactionAKID,
	WBP.PremiumTransactionAKId,
	WBP.AnnualStatementLineId,
	WBP.SourceCoverageType,
	WBP.TotalBlanketPremium,
	WBP.BreakOutNumerator,
	WBP.BreakOutDenominator,
	WBP.BreakOutPremium,
	WPTDRN.NewNegatePremiumTransactionAKID
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkBlanketPremiumBreakOut WBP
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionDataRepairNegate WPTDRN
	ON WBP.PremiumTransactionAKId = WPTDRN.OriginalPremiumTransactionAKID
	and WPTDRN.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_WorkBlanketPremiumBreakOut_DCT_Negate AS (
	SELECT
	BlanketPremiumTransactionAKID,
	PremiumTransactionAKId AS Old_PremiumTransactionAKId,
	AnnualStatementLineId,
	SourceCoverageType,
	TotalBlanketPremium,
	BreakOutNumerator,
	BreakOutDenominator,
	BreakOutPremium,
	NewNegatePremiumTransactionAKID
	FROM SQ_WorkBlanketPremiumBreakOut_DCT_Negate
),
EXP_MetaData AS (
	SELECT
	BlanketPremiumTransactionAKID,
	AnnualStatementLineId,
	SourceCoverageType,
	TotalBlanketPremium,
	BreakOutNumerator,
	BreakOutDenominator,
	BreakOutPremium,
	NewNegatePremiumTransactionAKID,
	NewNegatePremiumTransactionAKID AS o_PremiumTransactionAKId
	FROM EXP_WorkBlanketPremiumBreakOut_DCT_Negate
),
LKP_WorkBlanketPremiumBreakOut AS (
	SELECT
	WorkBlanketPremiumBreakOutId,
	BlanketPremiumTransactionAKID,
	PremiumTransactionAKId,
	AnnualStatementLineId,
	SourceCoverageType,
	TotalBlanketPremium,
	BreakOutNumerator,
	BreakOutDenominator,
	BreakOutPremium
	FROM (
		SELECT 
			WorkBlanketPremiumBreakOutId,
			BlanketPremiumTransactionAKID,
			PremiumTransactionAKId,
			AnnualStatementLineId,
			SourceCoverageType,
			TotalBlanketPremium,
			BreakOutNumerator,
			BreakOutDenominator,
			BreakOutPremium
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkBlanketPremiumBreakOut
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BlanketPremiumTransactionAKID,PremiumTransactionAKId ORDER BY WorkBlanketPremiumBreakOutId) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_WorkBlanketPremiumBreakOut.WorkBlanketPremiumBreakOutId AS lkp_WorkBlanketPremiumBreakOutId,
	LKP_WorkBlanketPremiumBreakOut.BlanketPremiumTransactionAKID AS lkp_BlanketPremiumTransactionAKID,
	LKP_WorkBlanketPremiumBreakOut.PremiumTransactionAKId AS lkp_PremiumTransactionAKId,
	LKP_WorkBlanketPremiumBreakOut.AnnualStatementLineId AS lkp_AnnualStatementLineId,
	LKP_WorkBlanketPremiumBreakOut.SourceCoverageType AS lkp_SourceCoverageType,
	LKP_WorkBlanketPremiumBreakOut.TotalBlanketPremium AS lkp_TotalBlanketPremium,
	LKP_WorkBlanketPremiumBreakOut.BreakOutNumerator AS lkp_BreakOutNumerator,
	LKP_WorkBlanketPremiumBreakOut.BreakOutDenominator AS lkp_BreakOutDenominator,
	LKP_WorkBlanketPremiumBreakOut.BreakOutPremium AS lkp_BreakOutPremium,
	EXP_MetaData.BlanketPremiumTransactionAKID AS In_BlanketPremiumTransactionAKID,
	EXP_MetaData.AnnualStatementLineId AS In_AnnualStatementLineId,
	EXP_MetaData.SourceCoverageType AS In_SourceCoverageType,
	EXP_MetaData.TotalBlanketPremium AS In_TotalBlanketPremium,
	EXP_MetaData.BreakOutNumerator AS In_BreakOutNumerator,
	EXP_MetaData.BreakOutDenominator AS In_BreakOutDenominator,
	EXP_MetaData.BreakOutPremium AS In_BreakOutPremium,
	EXP_MetaData.o_PremiumTransactionAKId AS In_PremiumTransactionAKId,
	-- *INF*: IIF(ISNULL(lkp_WorkBlanketPremiumBreakOutId),'NEW','UPDATE')
	IFF(lkp_WorkBlanketPremiumBreakOutId IS NULL, 'NEW', 'UPDATE') AS o_ChangeFlag,
	lkp_WorkBlanketPremiumBreakOutId AS o_WorkBlanketPremiumBreakOutId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	In_BlanketPremiumTransactionAKID AS o_BlanketPremiumTransactionAKID,
	In_PremiumTransactionAKId AS o_PremiumTransactionAKId,
	In_AnnualStatementLineId AS o_AnnualStatementLineId,
	In_SourceCoverageType AS o_SourceCoverageType,
	In_TotalBlanketPremium AS o_TotalBlanketPremium,
	In_BreakOutNumerator AS o_BreakOutNumerator,
	In_BreakOutDenominator AS o_BreakOutDenominator,
	In_BreakOutPremium AS o_BreakOutPremium
	FROM EXP_MetaData
	LEFT JOIN LKP_WorkBlanketPremiumBreakOut
	ON LKP_WorkBlanketPremiumBreakOut.BlanketPremiumTransactionAKID = EXP_MetaData.BlanketPremiumTransactionAKID AND LKP_WorkBlanketPremiumBreakOut.PremiumTransactionAKId = EXP_MetaData.o_PremiumTransactionAKId
),
RTR_Insert_Update AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_WorkBlanketPremiumBreakOutId AS WorkBlanketPremiumBreakOutId,
	o_AuditID AS AuditID,
	o_CreatedDate AS CreatedDate,
	o_BlanketPremiumTransactionAKID AS BlanketPremiumTransactionAKID,
	o_PremiumTransactionAKId AS PremiumTransactionAKId,
	o_AnnualStatementLineId AS AnnualStatementLineId,
	o_SourceCoverageType AS SourceCoverageType,
	o_TotalBlanketPremium AS TotalBlanketPremium,
	o_BreakOutNumerator AS BreakOutNumerator,
	o_BreakOutDenominator AS BreakOutDenominator,
	o_BreakOutPremium AS BreakOutPremium
	FROM EXP_DetectChanges
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='UPDATE'),
EXP_Insert AS (
	SELECT
	AuditID,
	CreatedDate,
	BlanketPremiumTransactionAKID,
	PremiumTransactionAKId,
	AnnualStatementLineId,
	SourceCoverageType,
	TotalBlanketPremium,
	BreakOutNumerator,
	BreakOutDenominator,
	BreakOutPremium
	FROM RTR_Insert_Update_INSERT
),
WorkBlanketPremiumBreakOut_Inserts AS (
	INSERT INTO WorkBlanketPremiumBreakOut
	(AuditId, CreatedDate, BlanketPremiumTransactionAKID, PremiumTransactionAKId, AnnualStatementLineId, SourceCoverageType, TotalBlanketPremium, BreakOutNumerator, BreakOutDenominator, BreakOutPremium)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	BLANKETPREMIUMTRANSACTIONAKID, 
	PREMIUMTRANSACTIONAKID, 
	ANNUALSTATEMENTLINEID, 
	SOURCECOVERAGETYPE, 
	TOTALBLANKETPREMIUM, 
	BREAKOUTNUMERATOR, 
	BREAKOUTDENOMINATOR, 
	BREAKOUTPREMIUM
	FROM EXP_Insert
),