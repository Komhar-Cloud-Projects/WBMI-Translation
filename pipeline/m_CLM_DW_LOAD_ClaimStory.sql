WITH
SQ_ClaimStoryStage AS (
	SELECT 
	S.TCH_CLAIM_NBR,
	S.CREATED_TS, 
	S.CLAIM_TYPE_CATEGORY, 
	C.DESCRIPTION AS CAUSE, 
	P.DESCRIPTION AS PHYSICAL_ITEM_INVOLVED, 
	R.DESCRIPTION AS RESULTING_DAMAGE, 
	I.DESCRIPTION AS ITEM_DAMAGED,
	S.SourceSystemId
	FROM ClaimStoryStage S
	INNER JOIN SupClaimStoryStage C ON S.CAUSE_ID = C.ID AND C.LIST_TYPE = 'Cause'
	INNER JOIN SupClaimStoryStage P ON S.PHYSICAL_ITEM_INVOLVED_ID = P.ID AND P.LIST_TYPE = 'PhysicalItem'
	INNER JOIN SupClaimStoryStage R ON S.RESULTING_DAMAGE_ID = R.ID AND R.LIST_TYPE = 'ResultingDamage'
	INNER JOIN SupClaimStoryStage I ON S.ITEM_DAMAGED_ID = I.ID AND I.LIST_TYPE = 'ItemDamaged'
	@{pipeline().parameters.WHERE}
	order by 1,2
),
EXP_logic AS (
	SELECT
	TCH_CLAIM_NBR,
	CREATED_TS,
	CLAIM_TYPE_CATEGORY,
	PHYSICAL_ITEM_INVOLVED AS CAUSE,
	CAUSE AS PHYSICAL_ITEM_INVOLVED,
	RESULTING_DAMAGE,
	ITEM_DAMAGED,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	1 AS CurrentSnapshotFlag,
	CURRENT_TIMESTAMP AS CurrentDate
	FROM SQ_ClaimStoryStage
),
AGG_Remove_Dupe_Keys AS (
	SELECT
	TCH_CLAIM_NBR,
	CREATED_TS,
	CLAIM_TYPE_CATEGORY,
	CAUSE,
	PHYSICAL_ITEM_INVOLVED,
	RESULTING_DAMAGE,
	ITEM_DAMAGED,
	SourceSystemId,
	AuditId,
	CurrentSnapshotFlag,
	CurrentDate
	FROM EXP_logic
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TCH_CLAIM_NBR ORDER BY NULL) = 1
),
LKP_ClaimStory AS (
	SELECT
	ClaimOccurrenceKey,
	TCH_CLAIM_NBR
	FROM (
		SELECT 
			ClaimOccurrenceKey,
			TCH_CLAIM_NBR
		FROM ClaimStory
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimOccurrenceKey ORDER BY ClaimOccurrenceKey) = 1
),
RTR_InsOrUpdate AS (
	SELECT
	LKP_ClaimStory.ClaimOccurrenceKey AS lkp_ClaimOccurrenceKey,
	AGG_Remove_Dupe_Keys.TCH_CLAIM_NBR,
	AGG_Remove_Dupe_Keys.CREATED_TS,
	AGG_Remove_Dupe_Keys.CLAIM_TYPE_CATEGORY,
	AGG_Remove_Dupe_Keys.CAUSE,
	AGG_Remove_Dupe_Keys.PHYSICAL_ITEM_INVOLVED,
	AGG_Remove_Dupe_Keys.RESULTING_DAMAGE,
	AGG_Remove_Dupe_Keys.ITEM_DAMAGED,
	AGG_Remove_Dupe_Keys.SourceSystemId,
	AGG_Remove_Dupe_Keys.AuditId,
	AGG_Remove_Dupe_Keys.CurrentSnapshotFlag,
	AGG_Remove_Dupe_Keys.CurrentDate
	FROM AGG_Remove_Dupe_Keys
	LEFT JOIN LKP_ClaimStory
	ON LKP_ClaimStory.ClaimOccurrenceKey = AGG_Remove_Dupe_Keys.TCH_CLAIM_NBR
),
RTR_InsOrUpdate_Insert AS (SELECT * FROM RTR_InsOrUpdate WHERE isnull(lkp_ClaimOccurrenceKey)),
RTR_InsOrUpdate_Update AS (SELECT * FROM RTR_InsOrUpdate WHERE not isnull(lkp_ClaimOccurrenceKey)),
UPD_Insert AS (
	SELECT
	TCH_CLAIM_NBR, 
	CREATED_TS, 
	CLAIM_TYPE_CATEGORY, 
	CAUSE, 
	PHYSICAL_ITEM_INVOLVED, 
	RESULTING_DAMAGE, 
	ITEM_DAMAGED, 
	SourceSystemId, 
	AuditId, 
	CurrentSnapshotFlag, 
	CurrentDate
	FROM RTR_InsOrUpdate_Insert
),
ClaimStory_Insert AS (
	INSERT INTO ClaimStory
	(ClaimOccurrenceKey, CreatedTimeStamp, ClaimTypeCategory, Catalyst, CauseOfDamage, DamageCaused, ItemDamaged, CreatedDate, ModifiedDate, SourceSystemId, AuditID, CurrentSnapshotFlag)
	SELECT 
	TCH_CLAIM_NBR AS CLAIMOCCURRENCEKEY, 
	CREATED_TS AS CREATEDTIMESTAMP, 
	CLAIM_TYPE_CATEGORY AS CLAIMTYPECATEGORY, 
	CAUSE AS CATALYST, 
	PHYSICAL_ITEM_INVOLVED AS CAUSEOFDAMAGE, 
	RESULTING_DAMAGE AS DAMAGECAUSED, 
	ITEM_DAMAGED AS ITEMDAMAGED, 
	CurrentDate AS CREATEDDATE, 
	CurrentDate AS MODIFIEDDATE, 
	SOURCESYSTEMID, 
	AuditId AS AUDITID, 
	CURRENTSNAPSHOTFLAG
	FROM UPD_Insert
),
UPD_UPDATE AS (
	SELECT
	TCH_CLAIM_NBR AS TCH_CLAIM_NBR3, 
	CREATED_TS, 
	CLAIM_TYPE_CATEGORY, 
	CAUSE, 
	PHYSICAL_ITEM_INVOLVED, 
	RESULTING_DAMAGE, 
	ITEM_DAMAGED, 
	SourceSystemId, 
	AuditId, 
	CurrentSnapshotFlag, 
	CurrentDate
	FROM RTR_InsOrUpdate_Update
),
ClaimStory_Update AS (
	MERGE INTO ClaimStory AS T
	USING UPD_UPDATE AS S
	ON T.ClaimOccurrenceKey = S.TCH_CLAIM_NBR3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CreatedTimeStamp = S.CREATED_TS, T.ClaimTypeCategory = S.CLAIM_TYPE_CATEGORY, T.Catalyst = S.CAUSE, T.CauseOfDamage = S.PHYSICAL_ITEM_INVOLVED, T.DamageCaused = S.RESULTING_DAMAGE, T.ItemDamaged = S.ITEM_DAMAGED, T.ModifiedDate = S.CurrentDate
),