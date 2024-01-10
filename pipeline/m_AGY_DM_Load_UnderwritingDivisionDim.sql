WITH
SQ_UnderwritingAssociate AS (
	SELECT
		UnderwritingAssociateID,
		CurrentSnapshotFlag,
		UnderwritingAssociateAKID,
		UnderwritingManagerAKID,
		WestBendAssociateID,
		AssociateRole,
		UnderwriterCode,
		DisplayName,
		LastName,
		FirstName,
		MiddleName,
		Suffix,
		EmailAddress
	FROM UnderwritingAssociate
	WHERE CurrentSnapshotFlag = 1
),
LKP_UWManager AS (
	SELECT
	UnderwritingManagerID,
	WestBendAssociateID,
	UnderwriterManagerCode,
	DisplayName,
	LastName,
	FirstName,
	MiddleName,
	Suffix,
	EmailAddress,
	UnderwritingManagerAKID
	FROM (
		SELECT 
			UnderwritingManagerID,
			WestBendAssociateID,
			UnderwriterManagerCode,
			DisplayName,
			LastName,
			FirstName,
			MiddleName,
			Suffix,
			EmailAddress,
			UnderwritingManagerAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwritingManager
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwritingManagerAKID ORDER BY UnderwritingManagerID) = 1
),
LKP_UnderwritingRegion AS (
	SELECT
	UnderwritingRegionID,
	UnderwritingRegionAKID,
	UnderwritingRegionCode,
	UnderwritingRegionCodeDescription,
	UnderwritingManagerAKID
	FROM (
		SELECT 
			UnderwritingRegionID,
			UnderwritingRegionAKID,
			UnderwritingRegionCode,
			UnderwritingRegionCodeDescription,
			UnderwritingManagerAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwritingRegion
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwritingManagerAKID ORDER BY UnderwritingRegionID) = 1
),
EXP_CleanupUWDivisionDetails AS (
	SELECT
	LKP_UWManager.UnderwritingManagerID AS lkp_UnderwritingManagerPKID,
	LKP_UWManager.WestBendAssociateID AS lkp_UnderwriterManagerWestBendAssociateID,
	LKP_UWManager.UnderwriterManagerCode AS lkp_UnderwriterManagerCode,
	LKP_UWManager.DisplayName AS lkp_UnderwriterManagerDisplayName,
	LKP_UWManager.LastName AS lkp_UnderwriterManagerLastName,
	LKP_UWManager.FirstName AS lkp_UnderwriterManagerFirstName,
	LKP_UWManager.MiddleName AS lkp_UnderwriterManagerMiddleName,
	LKP_UWManager.Suffix AS lkp_UnderwriterManagerSuffix,
	LKP_UWManager.EmailAddress AS lkp_UnderwriterManagerEmailAddress,
	LKP_UnderwritingRegion.UnderwritingRegionID AS lkp_UnderwritingRegionPKID,
	LKP_UnderwritingRegion.UnderwritingRegionAKID AS lkp_UnderwritingRegionAKID,
	LKP_UnderwritingRegion.UnderwritingRegionCode AS lkp_UnderwritingRegionCode,
	LKP_UnderwritingRegion.UnderwritingRegionCodeDescription AS lkp_UnderwritingRegionCodeDescription,
	SQ_UnderwritingAssociate.UnderwritingAssociateID AS i_UnderwritingAssociatePKID,
	SQ_UnderwritingAssociate.CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	SQ_UnderwritingAssociate.UnderwritingAssociateAKID AS i_UnderwritingAssociateAKID,
	SQ_UnderwritingAssociate.UnderwritingManagerAKID AS i_UnderwritingManagerAKID,
	SQ_UnderwritingAssociate.WestBendAssociateID AS i_UnderwriterWestBendAssociateID,
	SQ_UnderwritingAssociate.AssociateRole AS i_UnderwriterAssociateRole,
	SQ_UnderwritingAssociate.UnderwriterCode AS i_UnderwriterCode,
	SQ_UnderwritingAssociate.DisplayName AS i_UnderwriterDisplayName,
	SQ_UnderwritingAssociate.LastName AS i_UnderwriterLastName,
	SQ_UnderwritingAssociate.FirstName AS i_UnderwriterFirstName,
	SQ_UnderwritingAssociate.MiddleName AS i_UnderwriterMiddleName,
	SQ_UnderwritingAssociate.Suffix AS i_UnderwriterSuffix,
	SQ_UnderwritingAssociate.EmailAddress AS i_UnderwriterEmailAddress,
	i_UnderwritingAssociatePKID AS o_UnderwritingAssociatePKID,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingManagerPKID), -1,lkp_UnderwritingManagerPKID)
	IFF(lkp_UnderwritingManagerPKID IS NULL,
		- 1,
		lkp_UnderwritingManagerPKID
	) AS o_UnderwritingManagerPKID,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingRegionPKID)  OR  i_UnderwritingManagerAKID = -1, -1, lkp_UnderwritingRegionPKID)
	IFF(lkp_UnderwritingRegionPKID IS NULL 
		OR i_UnderwritingManagerAKID = - 1,
		- 1,
		lkp_UnderwritingRegionPKID
	) AS o_UnderwritingRegionPKID,
	i_UnderwritingAssociateAKID AS o_UnderwritingAssociateAKID,
	i_UnderwritingManagerAKID AS o_UnderwritingManagerAKID,
	-- *INF*: IIF(ISNULL( lkp_UnderwritingRegionAKID)  OR  i_UnderwritingManagerAKID = -1, -1, lkp_UnderwritingRegionAKID)
	IFF(lkp_UnderwritingRegionAKID IS NULL 
		OR i_UnderwritingManagerAKID = - 1,
		- 1,
		lkp_UnderwritingRegionAKID
	) AS o_UnderwritingRegionAKID,
	i_UnderwriterWestBendAssociateID AS o_UnderwriterWestBendAssociateID,
	i_UnderwriterCode AS o_UnderwriterCode,
	i_UnderwriterDisplayName AS o_UnderwriterDisplayName,
	i_UnderwriterLastName AS o_UnderwriterLastName,
	i_UnderwriterFirstName AS o_UnderwriterFirstName,
	i_UnderwriterMiddleName AS o_UnderwriterMiddleName,
	i_UnderwriterSuffix AS o_UnderwriterSuffix,
	i_UnderwriterEmailAddress AS o_UnderwriterEmailAddress,
	i_UnderwriterAssociateRole AS o_UnderwriterAssociateRole,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterManagerWestBendAssociateID  ), 'N/A', lkp_UnderwriterManagerWestBendAssociateID  )
	IFF(lkp_UnderwriterManagerWestBendAssociateID IS NULL,
		'N/A',
		lkp_UnderwriterManagerWestBendAssociateID
	) AS o_UnderwriterManagerWestBendAssociateID,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterManagerCode  ), 'N/A', lkp_UnderwriterManagerCode  )
	IFF(lkp_UnderwriterManagerCode IS NULL,
		'N/A',
		lkp_UnderwriterManagerCode
	) AS o_UnderwriterManagerCode,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterManagerDisplayName ), 'N/A', lkp_UnderwriterManagerDisplayName )
	IFF(lkp_UnderwriterManagerDisplayName IS NULL,
		'N/A',
		lkp_UnderwriterManagerDisplayName
	) AS o_UnderwriterManagerDisplayName,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterManagerLastName), 'N/A', lkp_UnderwriterManagerLastName)
	IFF(lkp_UnderwriterManagerLastName IS NULL,
		'N/A',
		lkp_UnderwriterManagerLastName
	) AS o_UnderwriterManagerLastName,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterManagerFirstName), 'N/A', lkp_UnderwriterManagerFirstName)
	IFF(lkp_UnderwriterManagerFirstName IS NULL,
		'N/A',
		lkp_UnderwriterManagerFirstName
	) AS o_UnderwriterManagerFirstName,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterManagerMiddleName), 'N/A', lkp_UnderwriterManagerMiddleName)
	IFF(lkp_UnderwriterManagerMiddleName IS NULL,
		'N/A',
		lkp_UnderwriterManagerMiddleName
	) AS o_UnderwriterManagerMiddleName,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterManagerSuffix), 'N/A', lkp_UnderwriterManagerSuffix)
	IFF(lkp_UnderwriterManagerSuffix IS NULL,
		'N/A',
		lkp_UnderwriterManagerSuffix
	) AS o_UnderwriterManagerSuffix,
	-- *INF*: IIF(ISNULL(lkp_UnderwriterManagerEmailAddress), 'N/A', lkp_UnderwriterManagerEmailAddress )
	IFF(lkp_UnderwriterManagerEmailAddress IS NULL,
		'N/A',
		lkp_UnderwriterManagerEmailAddress
	) AS o_UnderwriterManagerEmailAddress,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingRegionCode)  OR  i_UnderwritingManagerAKID = -1, 'N/A', lkp_UnderwritingRegionCode)
	IFF(lkp_UnderwritingRegionCode IS NULL 
		OR i_UnderwritingManagerAKID = - 1,
		'N/A',
		lkp_UnderwritingRegionCode
	) AS o_UnderwritingRegionCode,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingRegionCodeDescription)  OR  i_UnderwritingManagerAKID = -1, 'N/A', lkp_UnderwritingRegionCodeDescription)
	IFF(lkp_UnderwritingRegionCodeDescription IS NULL 
		OR i_UnderwritingManagerAKID = - 1,
		'N/A',
		lkp_UnderwritingRegionCodeDescription
	) AS o_UnderwritingRegionCodeDescription
	FROM SQ_UnderwritingAssociate
	LEFT JOIN LKP_UWManager
	ON LKP_UWManager.UnderwritingManagerAKID = SQ_UnderwritingAssociate.UnderwritingManagerAKID
	LEFT JOIN LKP_UnderwritingRegion
	ON LKP_UnderwritingRegion.UnderwritingManagerAKID = SQ_UnderwritingAssociate.UnderwritingManagerAKID
),
LKP_Existing AS (
	SELECT
	UnderwritingDivisionDimID,
	UnderwritingDivisionDimHashKey,
	EDWUnderwritingAssociatePKID,
	EDWUnderwritingManagerPKID,
	EDWUnderwritingRegionPKID,
	EDWUnderwritingAssociateAKID
	FROM (
		SELECT 
			UnderwritingDivisionDimID,
			UnderwritingDivisionDimHashKey,
			EDWUnderwritingAssociatePKID,
			EDWUnderwritingManagerPKID,
			EDWUnderwritingRegionPKID,
			EDWUnderwritingAssociateAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingDivisionDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWUnderwritingAssociateAKID ORDER BY UnderwritingDivisionDimID) = 1
),
EXP_CheckForChange AS (
	SELECT
	LKP_Existing.UnderwritingDivisionDimID AS lkp_ExistingUnderwritingDivisionDimID,
	LKP_Existing.UnderwritingDivisionDimHashKey AS lkp_ExistingHashKey,
	LKP_Existing.EDWUnderwritingAssociatePKID AS lkp_EDWUnderwritingAssociatePKID,
	LKP_Existing.EDWUnderwritingManagerPKID AS lkp_EDWUnderwritingManagerPKID,
	LKP_Existing.EDWUnderwritingRegionPKID AS lkp_EDWUnderwritingRegionPKID,
	EXP_CleanupUWDivisionDetails.o_UnderwritingAssociatePKID AS i_UnderwritingAssociatePKID,
	EXP_CleanupUWDivisionDetails.o_UnderwritingManagerPKID AS i_UnderwritingManagerPKID,
	EXP_CleanupUWDivisionDetails.o_UnderwritingRegionPKID AS i_UnderwritingRegionPKID,
	EXP_CleanupUWDivisionDetails.o_UnderwritingAssociateAKID AS i_UnderwritingAssociateAKID,
	EXP_CleanupUWDivisionDetails.o_UnderwritingManagerAKID AS i_UnderwritingManagerAKID,
	EXP_CleanupUWDivisionDetails.o_UnderwritingRegionAKID AS i_UnderwritingRegionAKID,
	EXP_CleanupUWDivisionDetails.o_UnderwriterWestBendAssociateID AS i_UnderwriterWestBendAssociateID,
	EXP_CleanupUWDivisionDetails.o_UnderwriterCode AS i_UnderwriterCode,
	EXP_CleanupUWDivisionDetails.o_UnderwriterDisplayName AS i_UnderwriterDisplayName,
	EXP_CleanupUWDivisionDetails.o_UnderwriterLastName AS i_UnderwriterLastName,
	EXP_CleanupUWDivisionDetails.o_UnderwriterFirstName AS i_UnderwriterFirstName,
	EXP_CleanupUWDivisionDetails.o_UnderwriterMiddleName AS i_UnderwriterMiddleName,
	EXP_CleanupUWDivisionDetails.o_UnderwriterSuffix AS i_UnderwriterSuffix,
	EXP_CleanupUWDivisionDetails.o_UnderwriterEmailAddress AS i_UnderwriterEmailAddress,
	EXP_CleanupUWDivisionDetails.o_UnderwriterAssociateRole AS i_UnderwriterAssociateRole,
	EXP_CleanupUWDivisionDetails.o_UnderwriterManagerWestBendAssociateID AS i_UnderwriterManagerWestBendAssociateID,
	EXP_CleanupUWDivisionDetails.o_UnderwriterManagerCode AS i_UnderwriterManagerCode,
	EXP_CleanupUWDivisionDetails.o_UnderwriterManagerDisplayName AS i_UnderwriterManagerDisplayName,
	EXP_CleanupUWDivisionDetails.o_UnderwriterManagerLastName AS i_UnderwriterManagerLastName,
	EXP_CleanupUWDivisionDetails.o_UnderwriterManagerFirstName AS i_UnderwriterManagerFirstName,
	EXP_CleanupUWDivisionDetails.o_UnderwriterManagerMiddleName AS i_UnderwriterManagerMiddleName,
	EXP_CleanupUWDivisionDetails.o_UnderwriterManagerSuffix AS i_UnderwriterManagerSuffix,
	EXP_CleanupUWDivisionDetails.o_UnderwriterManagerEmailAddress AS i_UnderwriterManagerEmailAddress,
	EXP_CleanupUWDivisionDetails.o_UnderwritingRegionCode AS i_UnderwritingRegionCode,
	EXP_CleanupUWDivisionDetails.o_UnderwritingRegionCodeDescription AS i_UnderwritingRegionCodeDescription,
	-- *INF*: MD5(i_UnderwriterCode || '&' || TO_CHAR(i_UnderwriterWestBendAssociateID)|| '&' || i_UnderwriterAssociateRole|| '&' || TO_CHAR(  i_UnderwriterManagerWestBendAssociateID)|| '&' || i_UnderwriterManagerCode|| '&' || i_UnderwritingRegionCode)
	MD5(i_UnderwriterCode || '&' || TO_CHAR(i_UnderwriterWestBendAssociateID
		) || '&' || i_UnderwriterAssociateRole || '&' || TO_CHAR(i_UnderwriterManagerWestBendAssociateID
		) || '&' || i_UnderwriterManagerCode || '&' || i_UnderwritingRegionCode
	) AS v_New_HashKey,
	-- *INF*: DECODE(true,
	-- lkp_EDWUnderwritingAssociatePKID <> i_UnderwritingAssociatePKID, 'Y',
	-- lkp_EDWUnderwritingManagerPKID <> i_UnderwritingManagerPKID, 'Y',
	-- lkp_EDWUnderwritingRegionPKID <> i_UnderwritingRegionPKID, 'Y',
	-- 'N')
	DECODE(true,
		lkp_EDWUnderwritingAssociatePKID <> i_UnderwritingAssociatePKID, 'Y',
		lkp_EDWUnderwritingManagerPKID <> i_UnderwritingManagerPKID, 'Y',
		lkp_EDWUnderwritingRegionPKID <> i_UnderwritingRegionPKID, 'Y',
		'N'
	) AS v_ChangeToEDWRecord,
	-- *INF*: DECODE(true,
	-- ISNULL(lkp_ExistingHashKey), 'Insert',
	-- (lkp_ExistingHashKey = v_New_HashKey) and (v_ChangeToEDWRecord = 'N'), 'Ignore',
	-- (lkp_ExistingHashKey <> v_New_HashKey) or (v_ChangeToEDWRecord = 'Y'), 'Update',
	-- 'Ignore')
	-- 
	-- -- If the existing record is not found based on the AKID, it's always an insert
	-- -- If there are no changes, we ignore the record
	-- -- If one of the type 2 attributes changed, we expire the old record (also inserts a new record, see router)
	-- -- If there was no change to the type 2 attributes AND there was a change to the PKID in the EDW then we update the record.  Important to have the logic comparing the hash keys, otherwise we might attempt to update records where we are already expiring and inserting a new record.
	-- 	
	DECODE(true,
		lkp_ExistingHashKey IS NULL, 'Insert',
		( lkp_ExistingHashKey = v_New_HashKey 
		) 
		AND ( v_ChangeToEDWRecord = 'N' 
		), 'Ignore',
		( lkp_ExistingHashKey <> v_New_HashKey 
		) 
		OR ( v_ChangeToEDWRecord = 'Y' 
		), 'Update',
		'Ignore'
	) AS v_InsertUpdateOrIgnore,
	lkp_ExistingUnderwritingDivisionDimID AS o_ExistingUnderwritingDivisionDimID,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 01:00:00.000', 'YYYY-MM-DD HH24:MI:SS.MS')
	-- 
	TO_DATE('1800-01-01 01:00:00.000', 'YYYY-MM-DD HH24:MI:SS.MS'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.MS')
	-- 
	TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.MS'
	) AS o_ExpirationDate,
	SYSDATE AS o_CurrentDate,
	v_New_HashKey AS o_New_HashKey,
	v_InsertUpdateOrIgnore AS o_InsertUpdateOrIgnore
	FROM EXP_CleanupUWDivisionDetails
	LEFT JOIN LKP_Existing
	ON LKP_Existing.EDWUnderwritingAssociateAKID = EXP_CleanupUWDivisionDetails.o_UnderwritingAssociateAKID
),
RTR_InsertUpdateOrExpire AS (
	SELECT
	lkp_ExistingUnderwritingDivisionDimID AS existingUnderwritingDivisionDimID,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_CurrentDate AS CurrentDate,
	o_New_HashKey AS UnderwritingDivisionDimHashKey,
	i_UnderwritingAssociatePKID AS EDWUnderwritingAssociatePKID,
	i_UnderwritingManagerPKID AS EDWUnderwritingManagerPKID,
	i_UnderwritingRegionPKID AS EDWUnderwritingRegionPKID,
	i_UnderwritingAssociateAKID AS EDWUnderwritingAssociateAKID,
	i_UnderwritingManagerAKID AS EDWUnderwritingManagerAKID,
	i_UnderwritingRegionAKID AS EDWUnderwritingRegionAKID,
	i_UnderwriterWestBendAssociateID AS UnderwriterWestBendAssociateID,
	i_UnderwriterCode AS UnderwriterCode,
	i_UnderwriterDisplayName AS UnderwriterDisplayName,
	i_UnderwriterLastName AS UnderwriterLastName,
	i_UnderwriterFirstName AS UnderwriterFirstName,
	i_UnderwriterMiddleName AS UnderwriterMiddleName,
	i_UnderwriterSuffix AS UnderwriterSuffix,
	i_UnderwriterEmailAddress AS UnderwriterEmailAddress,
	i_UnderwriterAssociateRole AS AssociateRole,
	i_UnderwriterManagerWestBendAssociateID AS UnderwriterManagerWestBendAssociateID,
	i_UnderwriterManagerCode AS UnderwriterManagerCode,
	i_UnderwriterManagerDisplayName AS UnderwriterManagerDisplayName,
	i_UnderwriterManagerLastName AS UnderwriterManagerLastName,
	i_UnderwriterManagerFirstName AS UnderwriterManagerFirstName,
	i_UnderwriterManagerMiddleName AS UnderwriterManagerMiddleName,
	i_UnderwriterManagerSuffix AS UnderwriterManagerSuffix,
	i_UnderwriterManagerEmailAddress AS UnderwriterManagerEmailAddress,
	i_UnderwritingRegionCode AS UnderwritingRegionCode,
	i_UnderwritingRegionCodeDescription AS UnderwritingRegionCodeDescription,
	o_InsertUpdateOrIgnore AS InsertUpdateOrIgnore
	FROM EXP_CheckForChange
),
RTR_InsertUpdateOrExpire_Insert AS (SELECT * FROM RTR_InsertUpdateOrExpire WHERE InsertUpdateOrIgnore = 'Insert'),
RTR_InsertUpdateOrExpire_Update AS (SELECT * FROM RTR_InsertUpdateOrExpire WHERE InsertUpdateOrIgnore = 'Update'),
UPD_UpdateExisting AS (
	SELECT
	existingUnderwritingDivisionDimID AS UnderwritingDivisionDimID, 
	CurrentDate, 
	UnderwritingDivisionDimHashKey, 
	EDWUnderwritingAssociatePKID, 
	EDWUnderwritingManagerPKID, 
	EDWUnderwritingRegionPKID, 
	EDWUnderwritingAssociateAKID, 
	EDWUnderwritingManagerAKID, 
	EDWUnderwritingRegionAKID, 
	UnderwriterWestBendAssociateID, 
	UnderwriterCode, 
	UnderwriterDisplayName, 
	UnderwriterLastName, 
	UnderwriterFirstName, 
	UnderwriterMiddleName, 
	UnderwriterSuffix, 
	UnderwriterEmailAddress, 
	AssociateRole, 
	UnderwriterManagerWestBendAssociateID, 
	UnderwriterManagerCode, 
	UnderwriterManagerDisplayName, 
	UnderwriterManagerLastName, 
	UnderwriterManagerFirstName, 
	UnderwriterManagerMiddleName, 
	UnderwriterManagerSuffix, 
	UnderwriterManagerEmailAddress, 
	UnderwritingRegionCode, 
	UnderwritingRegionCodeDescription
	FROM RTR_InsertUpdateOrExpire_Update
),
TGT_UnderwritingDivisionDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingDivisionDim AS T
	USING UPD_UpdateExisting AS S
	ON T.UnderwritingDivisionDimID = S.UnderwritingDivisionDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.CurrentDate, T.UnderwritingDivisionDimHashKey = S.UnderwritingDivisionDimHashKey, T.EDWUnderwritingAssociatePKID = S.EDWUnderwritingAssociatePKID, T.EDWUnderwritingManagerPKID = S.EDWUnderwritingManagerPKID, T.EDWUnderwritingRegionPKID = S.EDWUnderwritingRegionPKID, T.EDWUnderwritingAssociateAKID = S.EDWUnderwritingAssociateAKID, T.EDWUnderwritingManagerAKID = S.EDWUnderwritingManagerAKID, T.EDWUnderwritingRegionAKID = S.EDWUnderwritingRegionAKID, T.UnderwriterWestBendAssociateID = S.UnderwriterWestBendAssociateID, T.UnderwriterCode = S.UnderwriterCode, T.UnderwriterDisplayName = S.UnderwriterDisplayName, T.UnderwriterLastName = S.UnderwriterLastName, T.UnderwriterFirstName = S.UnderwriterFirstName, T.UnderwriterMiddleName = S.UnderwriterMiddleName, T.UnderwriterSuffix = S.UnderwriterSuffix, T.UnderwriterEmailAddress = S.UnderwriterEmailAddress, T.AssociateRole = S.AssociateRole, T.UnderwriterManagerWestBendAssociateID = S.UnderwriterManagerWestBendAssociateID, T.UnderwriterManagerCode = S.UnderwriterManagerCode, T.UnderwriterManagerDisplayName = S.UnderwriterManagerDisplayName, T.UnderwriterManagerLastName = S.UnderwriterManagerLastName, T.UnderwriterManagerFirstName = S.UnderwriterManagerFirstName, T.UnderwriterManagerMiddleName = S.UnderwriterManagerMiddleName, T.UnderwriterManagerSuffix = S.UnderwriterManagerSuffix, T.UnderwriterManagerEmailAddress = S.UnderwriterManagerEmailAddress, T.UnderwritingRegionCode = S.UnderwritingRegionCode, T.UnderwritingRegionCodeDescription = S.UnderwritingRegionCodeDescription
),
UPD_InsertNew AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentDate, 
	UnderwritingDivisionDimHashKey, 
	EDWUnderwritingAssociatePKID, 
	EDWUnderwritingManagerPKID, 
	EDWUnderwritingRegionPKID, 
	EDWUnderwritingAssociateAKID, 
	EDWUnderwritingManagerAKID, 
	EDWUnderwritingRegionAKID, 
	UnderwriterWestBendAssociateID, 
	UnderwriterCode, 
	UnderwriterDisplayName, 
	UnderwriterLastName, 
	UnderwriterFirstName, 
	UnderwriterMiddleName, 
	UnderwriterSuffix, 
	UnderwriterEmailAddress, 
	AssociateRole, 
	UnderwriterManagerWestBendAssociateID, 
	UnderwriterManagerCode, 
	UnderwriterManagerDisplayName, 
	UnderwriterManagerLastName, 
	UnderwriterManagerFirstName, 
	UnderwriterManagerMiddleName, 
	UnderwriterManagerSuffix, 
	UnderwriterManagerEmailAddress, 
	UnderwritingRegionCode, 
	UnderwritingRegionCodeDescription
	FROM RTR_InsertUpdateOrExpire_Insert
),
TGT_UnderwritingDivisionDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingDivisionDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, UnderwritingDivisionDimHashKey, EDWUnderwritingAssociatePKID, EDWUnderwritingManagerPKID, EDWUnderwritingRegionPKID, EDWUnderwritingAssociateAKID, EDWUnderwritingManagerAKID, EDWUnderwritingRegionAKID, UnderwriterWestBendAssociateID, UnderwriterCode, UnderwriterDisplayName, UnderwriterLastName, UnderwriterFirstName, UnderwriterMiddleName, UnderwriterSuffix, UnderwriterEmailAddress, AssociateRole, UnderwriterManagerWestBendAssociateID, UnderwriterManagerCode, UnderwriterManagerDisplayName, UnderwriterManagerLastName, UnderwriterManagerFirstName, UnderwriterManagerMiddleName, UnderwriterManagerSuffix, UnderwriterManagerEmailAddress, UnderwritingRegionCode, UnderwritingRegionCodeDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CurrentDate AS CREATEDDATE, 
	CurrentDate AS MODIFIEDDATE, 
	UNDERWRITINGDIVISIONDIMHASHKEY, 
	EDWUNDERWRITINGASSOCIATEPKID, 
	EDWUNDERWRITINGMANAGERPKID, 
	EDWUNDERWRITINGREGIONPKID, 
	EDWUNDERWRITINGASSOCIATEAKID, 
	EDWUNDERWRITINGMANAGERAKID, 
	EDWUNDERWRITINGREGIONAKID, 
	UNDERWRITERWESTBENDASSOCIATEID, 
	UNDERWRITERCODE, 
	UNDERWRITERDISPLAYNAME, 
	UNDERWRITERLASTNAME, 
	UNDERWRITERFIRSTNAME, 
	UNDERWRITERMIDDLENAME, 
	UNDERWRITERSUFFIX, 
	UNDERWRITEREMAILADDRESS, 
	ASSOCIATEROLE, 
	UNDERWRITERMANAGERWESTBENDASSOCIATEID, 
	UNDERWRITERMANAGERCODE, 
	UNDERWRITERMANAGERDISPLAYNAME, 
	UNDERWRITERMANAGERLASTNAME, 
	UNDERWRITERMANAGERFIRSTNAME, 
	UNDERWRITERMANAGERMIDDLENAME, 
	UNDERWRITERMANAGERSUFFIX, 
	UNDERWRITERMANAGEREMAILADDRESS, 
	UNDERWRITINGREGIONCODE, 
	UNDERWRITINGREGIONCODEDESCRIPTION
	FROM UPD_InsertNew
),