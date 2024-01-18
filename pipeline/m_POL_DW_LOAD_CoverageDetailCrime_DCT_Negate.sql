WITH
SQ_CoverageDetailCrime AS (
	SELECT CDCR.PremiumTransactionID,
	       CDCR.CoverageGuid,
	       CDCR.IndustryGroup,    
	PT.PremiumTransactionID 
	FROM  
	dbo.CoverageDetailCrime CDCR
	INNER JOIN dbo.WorkPremiumTransactionDataRepairNegate WPTDRN
	ON CDCR.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	INNER JOIN dbo.PremiumTransaction PT
	               ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID
	AND PT.SourceSystemId= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_Default AS (
	SELECT
	PremiumTransactionID,
	CoverageGuid,
	IndustryGroup,
	NewNegatePremiumTransactionID
	FROM SQ_CoverageDetailCrime
),
LKP_CoverageDetailCrime AS (
	SELECT
	PremiumTransactionID,
	IndustryGroup,
	NewNegatePremiumTransactionID
	FROM (
		SELECT 
			PremiumTransactionID,
			IndustryGroup,
			NewNegatePremiumTransactionID
		FROM CoverageDetailCrime
		WHERE SourceSystemID='DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailCrime.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailCrime.IndustryGroup AS lkp_IndustryGroup,
	EXP_Default.NewNegatePremiumTransactionID,
	EXP_Default.CoverageGuid AS i_CoverageGUID,
	EXP_Default.IndustryGroup,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800/01/01 00:00:00','YYYY/MM/DD HH24:MI:SS')
	TO_TIMESTAMP('1800/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100/12/31 23:59:59','YYYY/MM/DD HH24:MI:SS')
	TO_TIMESTAMP('2100/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS') AS o_ExpirationDate,
	'DCT' AS o_SourceSystemID,
	sysdate AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),
	-- 'INSERT',
	-- 'UPDATE')
	DECODE(
	    TRUE,
	    lkp_PremiumTransactionID IS NULL, 'INSERT',
	    'UPDATE'
	) AS o_changeflag
	FROM EXP_Default
	LEFT JOIN LKP_CoverageDetailCrime
	ON LKP_CoverageDetailCrime.PremiumTransactionID = EXP_Default.NewNegatePremiumTransactionID
),
RTR_Insert_Update AS (
	SELECT
	NewNegatePremiumTransactionID,
	i_CoverageGUID AS o_CoverageGUID,
	IndustryGroup AS o_IndustryGroup,
	o_CurrentSnapshotFlag,
	o_AuditID,
	o_EffectiveDate,
	o_ExpirationDate,
	o_SourceSystemID,
	o_CreatedDate,
	o_ModifiedDate,
	o_changeflag
	FROM EXP_DetectChanges
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE o_changeflag='INSERT'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE o_changeflag='UPDATE'),
CoverageDetailCrime_INSERT AS (
	INSERT INTO CoverageDetailCrime
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, IndustryGroup)
	SELECT 
	NewNegatePremiumTransactionID AS PREMIUMTRANSACTIONID, 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_CoverageGUID AS COVERAGEGUID, 
	o_IndustryGroup AS INDUSTRYGROUP
	FROM RTR_Insert_Update_INSERT
),