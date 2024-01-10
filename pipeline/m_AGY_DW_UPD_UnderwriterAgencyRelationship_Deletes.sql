WITH
SQ_UnderwriterAgencyRelationshipStage AS (
	SELECT
		UnderwriterAgencyRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AgencyCode,
		AssociateID,
		WestBendAssociateID,
		StrategicProfitCenterCode,
		StrategicProfitCenterDescription AS StrategicProfitCenterDescriptiong,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID,
		AgencyODSRelationshipId
	FROM UnderwriterAgencyRelationshipStage
	WHERE UnderwriterAgencyRelationshipStage.StrategicProfitCenterCode = 'X'
),
LKP_Agency AS (
	SELECT
	in_AgencyCode,
	AgencyAKID,
	AgencyCode
	FROM (
		SELECT 
			in_AgencyCode,
			AgencyAKID,
			AgencyCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY in_AgencyCode DESC) = 1
),
lkp_UnderwritingAssociate AS (
	SELECT
	UnderwritingAssociateAKID,
	WestBendAssociateID
	FROM (
		SELECT 
			UnderwritingAssociateAKID,
			WestBendAssociateID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingAssociate
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY UnderwritingAssociateAKID DESC) = 1
),
EXP_GetAKIDs AS (
	SELECT
	lkp_UnderwritingAssociate.UnderwritingAssociateAKID AS lkp_UnderwritingAssociateAKID,
	LKP_Agency.AgencyAKID AS lkp_AgencyAKID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SQ_UnderwriterAgencyRelationshipStage.ModifiedDate,
	SQ_UnderwriterAgencyRelationshipStage.AgencyID,
	SQ_UnderwriterAgencyRelationshipStage.AgencyCode,
	SQ_UnderwriterAgencyRelationshipStage.AssociateID,
	SQ_UnderwriterAgencyRelationshipStage.WestBendAssociateID,
	SQ_UnderwriterAgencyRelationshipStage.StrategicProfitCenterCode,
	SQ_UnderwriterAgencyRelationshipStage.StrategicProfitCenterDescriptiong,
	SQ_UnderwriterAgencyRelationshipStage.AgencyODSRelationshipId AS AgencyODSRelationshipID
	FROM SQ_UnderwriterAgencyRelationshipStage
	LEFT JOIN LKP_Agency
	ON LKP_Agency.AgencyCode = SQ_UnderwriterAgencyRelationshipStage.AgencyCode
	LEFT JOIN lkp_UnderwritingAssociate
	ON lkp_UnderwritingAssociate.WestBendAssociateID = SQ_UnderwriterAgencyRelationshipStage.WestBendAssociateID
),
LKP_ExistingRelationship AS (
	SELECT
	AgencyODSRelationshipID,
	UnderwriterAgencyRelationshipID
	FROM (
		SELECT 
			AgencyODSRelationshipID,
			UnderwriterAgencyRelationshipID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationship
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwriterAgencyRelationshipID ORDER BY AgencyODSRelationshipID DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ExistingRelationship.UnderwriterAgencyRelationshipID AS lkp_UnderwriterAgencyRelationshipID,
	EXP_GetAKIDs.lkp_AgencyAKID,
	EXP_GetAKIDs.lkp_UnderwritingAssociateAKID,
	EXP_GetAKIDs.SourceSystemID,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterAgencyRelationshipID), 'IGNORE', 'UPDATE')
	-- 
	IFF(lkp_UnderwriterAgencyRelationshipID IS NULL,
		'IGNORE',
		'UPDATE'
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	0 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	sysdate AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	EXP_GetAKIDs.ModifiedDate AS SourceModifiedDate
	FROM EXP_GetAKIDs
	LEFT JOIN LKP_ExistingRelationship
	ON LKP_ExistingRelationship.UnderwriterAgencyRelationshipID = EXP_GetAKIDs.AgencyODSRelationshipID
),
FIL_insert AS (
	SELECT
	lkp_UnderwriterAgencyRelationshipID AS UnderwriterAgencyRelationshipID, 
	changed_flag, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	SourceSystemID, 
	ModifiedDate, 
	lkp_AgencyAKID AS AgencyAKID, 
	lkp_UnderwritingAssociateAKID AS UnderwritingAssociateAKID, 
	SourceModifiedDate, 
	AuditID
	FROM EXP_Detect_Changes
	WHERE changed_flag='UPDATE'
),
UPD_DeletedRelationship AS (
	SELECT
	UnderwriterAgencyRelationshipID, 
	SourceModifiedDate AS ExpirationDate, 
	CurrentSnapshotFlag, 
	ModifiedDate, 
	AuditID
	FROM FIL_insert
),
UnderwriterAgencyRelationship_Expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwriterAgencyRelationship AS T
	USING UPD_DeletedRelationship AS S
	ON T.UnderwriterAgencyRelationshipId = S.UnderwriterAgencyRelationshipID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),