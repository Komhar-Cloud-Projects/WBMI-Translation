WITH
SQ_WorkPremiumTransactionOnsetOffsetPreProcessing AS (
	select 
	Lineage.PolicyKey,
	Lineage.RatingCoverageAKID,
	Lineage.PremiumTransactionID,
	Lineage.PremiumTransactionAKID,
	Lineage.PreviousPremiumTransactionID,
	Lineage.PreviousPremiumTransactionAKID
	from 
	(select 
	offset.PolicyKey,
	offset.RatingCoverageAKID,
	offset.PremiumTransactionID,
	offset.PremiumTransactionAKID,
	onset.PremiumTransactionID as PreviousPremiumTransactionID,
	onset.PremiumTransactionAKID as PreviousPremiumTransactionAKID,
	ROW_NUMBER() OVER (PARTITION BY offset.PremiumTransactionAKID ORDER BY onset.WorkPremiumTransactionOnsetOffsetPreProcessingId DESC) AS RowNum
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOnsetOffsetPreProcessing offset WITH (NOLOCK)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOnsetOffsetPreProcessing onset WITH (NOLOCK) on
	onset.OffsetOnsetCode not in ('Offset','Deprecated')
	  AND onset.PolicyKey = offset.PolicyKey
	  AND onset.RatingCoverageAKID = offset.RatingCoverageAKID
	  AND onset.WorkPremiumTransactionOnsetOffsetPreProcessingId < offset.WorkPremiumTransactionOnsetOffsetPreProcessingId
	  -- The onset occurred before the Offset
	  AND (
	    (onset.DCTTransactionSequence < offset.DCTTransactionSequence)
	    OR 
	    (onset.DCTTransactionSequence = offset.DCTTransactionSequence AND onset.DCTTransactionIndex < offset.DCTTransactionIndex)
	   )
	  -- The onset should have been deprecated after the offset was created, or not be deprecated at all
	  AND (
	    (onset.DeprecatedOnDCTTransactionSequence is NULL and onset.DeprecatedOnDCTTransactionIndex is NULL)
	    or
	    (onset.DeprecatedOnDCTTransactionSequence > offset.DCTTransactionSequence)
	    or
	    (onset.DeprecatedOnDCTTransactionSequence = offset.DCTTransactionSequence and onset.DeprecatedOnDCTTransactionIndex > offset.DCTTransactionIndex)
	   )
	   where offset.OffsetOnsetCode = 'Offset'
	   @{pipeline().parameters.INCREMENTALLOADFILTER} 
	) Lineage
	WHERE Lineage.RowNum = 1
),
EXP_Pass_Value AS (
	SELECT
	PolicyKey,
	RatingCoverageAKID,
	PremiumTransactionID,
	PremiumTransactionAKID,
	PreviousPremiumTransactionID,
	PreviousPremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	'1' AS UpdateAttributeFlag
	FROM SQ_WorkPremiumTransactionOnsetOffsetPreProcessing
),
WorkPremiumTransactionOffsetLineage AS (
	TRUNCATE TABLE WorkPremiumTransactionOffsetLineage;
	INSERT INTO WorkPremiumTransactionOffsetLineage
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
	UPDATEATTRIBUTEFLAG
	FROM EXP_Pass_Value
),