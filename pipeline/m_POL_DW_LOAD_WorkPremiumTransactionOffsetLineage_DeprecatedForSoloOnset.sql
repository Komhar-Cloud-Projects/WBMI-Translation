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
	deprecatedOnset.PolicyKey,
	deprecatedOnset.RatingCoverageAKID,
	deprecatedOnset.PremiumTransactionID,
	deprecatedOnset.PremiumTransactionAKID,
	onset.PremiumTransactionID as PreviousPremiumTransactionID,
	onset.PremiumTransactionAKID as PreviousPremiumTransactionAKID,
	ROW_NUMBER() OVER (PARTITION BY deprecatedOnset.PremiumTransactionAKID ORDER BY onset.WorkPremiumTransactionOnsetOffsetPreProcessingId DESC) AS RowNum
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOnsetOffsetPreProcessing deprecatedOnset WITH (NOLOCK)
	
	  INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOnsetOffsetPreProcessing onset on
	  onset.OffsetOnsetCode IN ('N/A', 'Onset')
	  AND onset.PolicyKey = deprecatedOnset.PolicyKey
	  AND onset.RatingCoverageAKID = deprecatedOnset.RatingCoverageAKID
	  AND onset.WorkPremiumTransactionOnsetOffsetPreProcessingId < deprecatedOnset.WorkPremiumTransactionOnsetOffsetPreProcessingId
	  -- The onset occurred before the Deprecated
	  AND (
	   onset.DCTTransactionSequence < deprecatedOnset.DCTTransactionSequence
	   OR (
	    onset.DCTTransactionSequence = deprecatedOnset.DCTTransactionSequence
	    AND onset.DCTTransactionIndex < deprecatedOnset.DCTTransactionIndex
	    )
	   )
	  -- This Deprecated row deprecates this onset
	  AND deprecatedOnset.DCTTransactionGuid = onset.DCTTransactionGuid
	  -- This onset came in without an Offset
	  AND NOT EXISTS (SELECT 1
	   FROM dbo.WorkPremiumTransactionOnsetOffsetPreProcessing offset WITH (NOLOCK)
	   WHERE offset.OffsetOnsetCode = 'Offset'
	   AND onset.PolicyKey = offset.PolicyKey
	   AND onset.RatingCoverageAKID = offset.RatingCoverageAKID
	   AND onset.DCTCoverageId = offset.DCTCoverageId
	   )
	   
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction ptDepOnset on
	   deprecatedOnset.PremiumTransactionID = ptDepOnset.PremiumTransactionID
	   INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction ptOnset on
	   onset.PremiumTransactionID = ptOnset.PremiumTransactionID   
	   AND ptDepOnset.PremiumTransactionAmount = (ptOnset.PremiumTransactionAmount * - 1.0)
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
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing.PolicyKey,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing.RatingCoverageAKID,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing.PremiumTransactionID,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing.PremiumTransactionAKID,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing.PreviousPremiumTransactionID,
	SQ_WorkPremiumTransactionOnsetOffsetPreProcessing.PreviousPremiumTransactionAKID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	'1' AS UpdateAttributeFlag,
	-- *INF*: IIF ( ISNULL(WorkPremiumTransactionOffsetLineageId ), 'Insert','Update')
	IFF(WorkPremiumTransactionOffsetLineageId IS NULL, 'Insert', 'Update') AS o_flag
	FROM SQ_WorkPremiumTransactionOnsetOffsetPreProcessing
	LEFT JOIN LKP_WorkPremiumTransactionOffsetLineage
	ON LKP_WorkPremiumTransactionOffsetLineage.PremiumTransactionID = SQ_WorkPremiumTransactionOnsetOffsetPreProcessing.PremiumTransactionID
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
	o_flag
	FROM EXP_Pass_Value
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE o_flag='Insert'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE o_flag='Update'),
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
WorkPremiumTransactionOffsetLineage_Upd AS (
	MERGE INTO WorkPremiumTransactionOffsetLineage AS T
	USING UPD_WorkPremiumTransactionOffsetLineage AS S
	ON T.WorkPremiumTransactionOffsetLineageId = S.WorkPremiumTransactionOffsetLineageId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate3, T.PolicyKey = S.PolicyKey3, T.RatingCoverageAKID = S.RatingCoverageAKID3, T.PremiumTransactionID = S.PremiumTransactionID3, T.PremiumTransactionAKID = S.PremiumTransactionAKID3, T.PreviousPremiumTransactionID = S.PreviousPremiumTransactionID3, T.PreviousPremiumTransactionAKID = S.PreviousPremiumTransactionAKID3, T.UpdateAttributeFlag = S.UpdateAttributeFlag3
),
WorkPremiumTransactionOffsetLineage_Ins AS (
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