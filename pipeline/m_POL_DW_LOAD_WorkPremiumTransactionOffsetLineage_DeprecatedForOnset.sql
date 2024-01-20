WITH
SQ_WorkPremiumTransactionOnsetOffsetPreProcessing1 AS (
	select 
	Lineage.PolicyKey,
	Lineage.RatingCoverageAKID,
	Lineage.PremiumTransactionID,
	Lineage.PremiumTransactionAKID,
	Lineage.PreviousPremiumTransactionID,
	Lineage.PreviousPremiumTransactionAKID
	from 
	(select 
	deprecatedOnset.PolicyKey,
	deprecatedOnset.RatingCoverageAKID,
	deprecatedOnset.PremiumTransactionID,
	deprecatedOnset.PremiumTransactionAKID,
	onset.PremiumTransactionID as PreviousPremiumTransactionID,
	onset.PremiumTransactionAKID as PreviousPremiumTransactionAKID,
	ROW_NUMBER() OVER (PARTITION BY deprecatedOnset.PremiumTransactionAKID ORDER BY onset.WorkPremiumTransactionOnsetOffsetPreProcessingId DESC) AS RowNum
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOnsetOffsetPreProcessing deprecatedOnset WITH (NOLOCK)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction ptDepOnset on
	deprecatedOnset.PremiumTransactionID = ptDepOnset.PremiumTransactionID
	  AND ptDepOnset.PremiumTransactionCode='Endorse'
	  
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOnsetOffsetPreProcessing deprecatedOffset WITH (NOLOCK) on
	deprecatedOffset.OffsetOnsetCode = 'Deprecated'
	  AND deprecatedOffset.DCTCoverageId = deprecatedOnset.DCTCoverageId 
	  AND deprecatedOnset.PremiumTransactionID <> deprecatedOffset.PremiumTransactionID
	  
	  INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction ptDepOffset on
	  deprecatedOffset.PremiumTransactionID = ptDepOffset.PremiumTransactionID
	  AND ptDepOffset.PremiumTransactionCode='Endorse'
	  
	  INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOnsetOffsetPreProcessing onset on
	  onset.OffsetOnsetCode = 'Onset'
	  AND onset.PolicyKey = deprecatedOnset.PolicyKey
	  AND onset.RatingCoverageAKID = deprecatedOnset.RatingCoverageAKID
	  AND onset.WorkPremiumTransactionOnsetOffsetPreProcessingId < deprecatedOnset.WorkPremiumTransactionOnsetOffsetPreProcessingId
	  -- Onset occurred before the Deprecated
	  AND (onset.DCTTransactionSequence < deprecatedOnset.DCTTransactionSequence 
	   OR 
	   (onset.DCTTransactionSequence = deprecatedOnset.DCTTransactionSequence AND onset.DCTTransactionIndex < deprecatedOnset.DCTTransactionIndex)
	   )
	  -- This Deprecated row deprecates this Onset
	  AND deprecatedOnset.DCTTransactionGuid = onset.DCTTransactionGuid
	  Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction ptOnset on
	  onset.PremiumTransactionID = ptOnset.PremiumTransactionID
	   and ptDepOnset.PremiumTransactionAmount = (ptOnset.PremiumTransactionAmount * -1.0)
	where deprecatedOnset.OffsetOnsetCode = 'Deprecated'
	@{pipeline().parameters.INCREMENTALLOADFILTER}
	) Lineage
	WHERE Lineage.RowNum = 1
),
LKP_WorkPremiumTransactionOffsetLineage AS (
	SELECT
	WorkPremiumTransactionOffsetLineageId,
	PremiumTransactionID,
	in_PremiumTransactionID
	FROM (
		SELECT 
			WorkPremiumTransactionOffsetLineageId,
			PremiumTransactionID,
			in_PremiumTransactionID
		FROM WorkPremiumTransactionOffsetLineage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY WorkPremiumTransactionOffsetLineageId) = 1
),
EXP_Pass_Value AS (
	SELECT
	LKP_WorkPremiumTransactionOffsetLineage.WorkPremiumTransactionOffsetLineageId,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing1.PolicyKey,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing1.RatingCoverageAKID,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing1.PremiumTransactionID,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing1.PremiumTransactionAKID,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing1.PreviousPremiumTransactionID,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing1.PreviousPremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	'1' AS UpdateAttributeFlag,
	-- *INF*: IIF(ISNULL(WorkPremiumTransactionOffsetLineageId), 'Insert','Update')
	IFF(WorkPremiumTransactionOffsetLineageId IS NULL, 'Insert', 'Update') AS o_Flag
	FROM SQ_WorkPremiumTransactionOnsetOffsetPreProcessing1
	LEFT JOIN LKP_WorkPremiumTransactionOffsetLineage
	ON LKP_WorkPremiumTransactionOffsetLineage.PremiumTransactionID = SQ_WorkPremiumTransactionOnsetOffsetPreProcessing1.PremiumTransactionID
),
RTR_Insert_Update AS (
	SELECT
	WorkPremiumTransactionOffsetLineageId,
	PolicyKey,
	RatingCoverageAKID,
	PremiumTransactionID,
	PremiumTransactionAKID,
	PreviousPremiumTransactionID,
	PreviousPremiumTransactionAKID,
	AuditID,
	CreatedDate,
	ModifiedDate,
	UpdateAttributeFlag,
	o_Flag
	FROM EXP_Pass_Value
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE o_Flag='Insert'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE o_Flag='Update'),
UPD_WorkPremiumTransactionOffsetLineage AS (
	SELECT
	WorkPremiumTransactionOffsetLineageId AS WorkPremiumTransactionOffsetLineageId3, 
	PolicyKey AS PolicyKey3, 
	RatingCoverageAKID AS RatingCoverageAKID3, 
	PremiumTransactionID AS PremiumTransactionID3, 
	PremiumTransactionAKID AS PremiumTransactionAKID3, 
	PreviousPremiumTransactionID AS PreviousPremiumTransactionID3, 
	PreviousPremiumTransactionAKID AS PreviousPremiumTransactionAKID3, 
	ModifiedDate AS ModifiedDate3, 
	UpdateAttributeFlag AS UpdateAttributeFlag3
	FROM RTR_Insert_Update_UPDATE
),
WorkPremiumTransactionOffsetLineage1 AS (
	MERGE INTO WorkPremiumTransactionOffsetLineage AS T
	USING UPD_WorkPremiumTransactionOffsetLineage AS S
	ON T.WorkPremiumTransactionOffsetLineageId = S.WorkPremiumTransactionOffsetLineageId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate3, T.PolicyKey = S.PolicyKey3, T.RatingCoverageAKID = S.RatingCoverageAKID3, T.PremiumTransactionID = S.PremiumTransactionID3, T.PremiumTransactionAKID = S.PremiumTransactionAKID3, T.PreviousPremiumTransactionID = S.PreviousPremiumTransactionID3, T.PreviousPremiumTransactionAKID = S.PreviousPremiumTransactionAKID3, T.UpdateAttributeFlag = S.UpdateAttributeFlag3
),
WorkPremiumTransactionOffsetLineage AS (
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
	FROM RTR_Insert_Update_INSERT
),