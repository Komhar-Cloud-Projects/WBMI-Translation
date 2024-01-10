WITH
SQ_WorkPremiumTransactionOffsetLineage AS (
	SELECT
		WorkPremiumTransactionOffsetLineageId,
		AuditID,
		CreatedDate,
		ModifiedDate,
		PolicyKey,
		RatingCoverageAKID,
		PremiumTransactionID,
		PremiumTransactionAKID,
		PreviousPremiumTransactionID,
		PreviousPremiumTransactionAKID,
		UpdateAttributeFlag
	FROM WorkPremiumTransactionOffsetLineage
),
EXP_WorkPremiumTransactionOffsetLineage AS (
	SELECT
	WorkPremiumTransactionOffsetLineageId,
	AuditID,
	CreatedDate,
	ModifiedDate,
	PolicyKey,
	RatingCoverageAKID,
	PremiumTransactionID,
	PremiumTransactionAKID,
	PreviousPremiumTransactionID,
	PreviousPremiumTransactionAKID,
	UpdateAttributeFlag AS i_UpdateAttributeFlag,
	-- *INF*: DECODE(TRUE,i_UpdateAttributeFlag='T','1','0')
	DECODE(TRUE,
		i_UpdateAttributeFlag = 'T', '1',
		'0') AS o_UpdateAttributeFlag
	FROM SQ_WorkPremiumTransactionOffsetLineage
),
TGT_WorkPremiumTransactionOffsetLineageHistory AS (
	INSERT INTO WorkPremiumTransactionOffsetLineageHistory
	(AuditID, CreatedDate, ModifiedDate, PolicyKey, RatingCoverageAKID, PremiumTransactionID, PremiumTransactionAKID, PreviousPremiumTransactionID, PreviousPremiumTransactionAKID, UpdateAttributeFlag)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYKEY, 
	RATINGCOVERAGEAKID, 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	PREVIOUSPREMIUMTRANSACTIONID, 
	PREVIOUSPREMIUMTRANSACTIONAKID, 
	o_UpdateAttributeFlag AS UPDATEATTRIBUTEFLAG
	FROM EXP_WorkPremiumTransactionOffsetLineage
),