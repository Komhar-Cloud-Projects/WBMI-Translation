WITH
SQ_WorkPremiumTransactionDataRepairNegate AS (
	select Max(B.WorkPremiumTransactionId) as WorkPremiumTransactionId, NewNegatePremiumTransactionAKID as PremiumTransactionAKId,Max(PremiumTransactionStageId) as PremiumTransactionStageId , Max(A.CreatedDate) as CreatedDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionDataRepairNegate A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchWorkPremiumTransaction B
	on A.OriginalPremiumTransactionAKID=B.PremiumTransactionAKId and B.SourceSystemID='DCT'
	group by NewNegatePremiumTransactionAKID
),
EXP_METADATA AS (
	SELECT
	-- *INF*: --v_WorkPremiumTransactionID+1
	-- --Removed as we figured WorkPremiumTransactionID shouldn't be assigned, but rather come from the source
	'' AS v_WorkPremiumTransactionID,
	WorkPremiumTransactionId AS WorkPremiumTransactionID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	CreatedDate,
	PremiumTransactionAKId,
	PremiumTransactionStageId
	FROM SQ_WorkPremiumTransactionDataRepairNegate
),
ArchWorkPremiumTransaction AS (
	INSERT INTO ArchWorkPremiumTransaction
	(WorkPremiumTransactionId, AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, PremiumTransactionStageId)
	SELECT 
	WorkPremiumTransactionID AS WORKPREMIUMTRANSACTIONID, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	PREMIUMTRANSACTIONAKID, 
	PREMIUMTRANSACTIONSTAGEID
	FROM EXP_METADATA
),