WITH
SQ_WorkPremiumTransactionDataRepairNegate AS (
	SELECT
		WorkPremiumTransactionDataRepairNegateId,
		SourceSystemId,
		CreatedDate,
		CreatedUserID,
		OriginalPremiumTransactionID,
		OriginalPremiumTransactionAKID,
		NewNegatePremiumTransactionID,
		NewNegatePremiumTransactionAKID
	FROM WorkPremiumTransactionDataRepairNegate
),
EXP_Default AS (
	SELECT
	WorkPremiumTransactionDataRepairNegateId,
	SourceSystemId,
	CreatedDate,
	CreatedUserID,
	OriginalPremiumTransactionID,
	OriginalPremiumTransactionAKID,
	NewNegatePremiumTransactionID,
	NewNegatePremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID
	FROM SQ_WorkPremiumTransactionDataRepairNegate
),
ArchWorkPremiumTransactionDataRepairNegate AS (
	INSERT INTO ArchWorkPremiumTransactionDataRepairNegate
	(SourceSystemId, CreatedDate, CreatedUserID, OriginalPremiumTransactionID, OriginalPremiumTransactionAKID, NewNegatePremiumTransactionID, NewNegatePremiumTransactionAKID, AuditId)
	SELECT 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	CREATEDUSERID, 
	ORIGINALPREMIUMTRANSACTIONID, 
	ORIGINALPREMIUMTRANSACTIONAKID, 
	NEWNEGATEPREMIUMTRANSACTIONID, 
	NEWNEGATEPREMIUMTRANSACTIONAKID, 
	o_AuditID AS AUDITID
	FROM EXP_Default
),