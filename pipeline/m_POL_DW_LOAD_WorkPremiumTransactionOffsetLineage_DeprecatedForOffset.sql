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
	deprecated.PolicyKey,
	deprecated.RatingCoverageAKID,
	deprecated.PremiumTransactionID,
	deprecated.PremiumTransactionAKID,
	offset.PremiumTransactionID as PreviousPremiumTransactionID,
	offset.PremiumTransactionAKID as PreviousPremiumTransactionAKID,
	ROW_NUMBER() OVER (PARTITION BY deprecated.PremiumTransactionAKID ORDER BY offset.WorkPremiumTransactionOnsetOffsetPreProcessingId DESC) AS RowNum
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOnsetOffsetPreProcessing deprecated WITH (NOLOCK)
	INNER JOIN PremiumTransaction ptDep on
	deprecated.PremiumTransactionID = ptDep.PremiumTransactionID 
	  AND ptDep.PremiumTransactionCode = 'Endorse'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionOnsetOffsetPreProcessing offset WITH (NOLOCK) on
	offset.OffsetOnsetCode = 'Offset'
	  AND offset.PolicyKey = deprecated.PolicyKey
	  AND offset.RatingCoverageAKID = deprecated.RatingCoverageAKID
	  AND offset.WorkPremiumTransactionOnsetOffsetPreProcessingId < deprecated.WorkPremiumTransactionOnsetOffsetPreProcessingId
	  -- Offset occurred before the Deprecated
	  AND (offset.DCTTransactionSequence < deprecated.DCTTransactionSequence 
	   OR 
	   (offset.DCTTransactionSequence = deprecated.DCTTransactionSequence AND offset.DCTTransactionIndex < deprecated.DCTTransactionIndex)
	   )
	  -- This Deprecated row deprecates this Offset
	  AND offset.DCTTransactionGuid = deprecated.DCTTransactionGuid
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction ptOffset on
	offset.PremiumTransactionID = ptOffset.PremiumTransactionID
	   and ptDep.PremiumTransactionAmount = (ptOffset.PremiumTransactionAmount * -1.0)
	   
	  where deprecated.OffsetOnsetCode = 'Deprecated'
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
	-- *INF*: IIF(ISNULL(WorkPremiumTransactionOffsetLineageId), 'Insert', 'Update')
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