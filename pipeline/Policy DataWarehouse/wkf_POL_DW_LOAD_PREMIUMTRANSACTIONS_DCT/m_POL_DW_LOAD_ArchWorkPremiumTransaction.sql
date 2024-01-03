WITH
SQ_WorkPremiumTransaction AS (
	SELECT
		WorkPremiumTransactionId,
		AuditID,
		SourceSystemID,
		CreatedDate,
		PremiumTransactionAKId,
		PremiumTransactionStageId
	FROM WorkPremiumTransaction
),
ArchWorkPremiumTransaction AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkPremiumTransaction
	(WorkPremiumTransactionId, AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, PremiumTransactionStageId)
	SELECT 
	WORKPREMIUMTRANSACTIONID, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	PREMIUMTRANSACTIONAKID, 
	PREMIUMTRANSACTIONSTAGEID
	FROM SQ_WorkPremiumTransaction
),