WITH
SQ_UnderwritingRegionRelationshipStage AS (
	SELECT
		UnderwritingRegionRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		WestBendAssociateID,
		UnderwritingRegionID,
		UnderwritingRegionCode,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM UnderwritingRegionRelationshipStage1
),
SQ_UnderwritingRegionStage AS (
	SELECT
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		UnderwritingRegionCode,
		UnderwritingRegionCodeDescription,
		SourceSystemID
	FROM UnderwritingRegionStage
),
JNR_OuterToRelationship AS (SELECT
	SQ_UnderwritingRegionStage.UnderwritingRegionCode, 
	SQ_UnderwritingRegionStage.UnderwritingRegionCodeDescription, 
	SQ_UnderwritingRegionStage.SourceSystemID, 
	SQ_UnderwritingRegionRelationshipStage.UnderwritingRegionCode AS rel_UnderwritingRegionCode, 
	SQ_UnderwritingRegionRelationshipStage.WestBendAssociateID AS rel_WestBendAssociateID
	FROM SQ_UnderwritingRegionRelationshipStage
	RIGHT OUTER JOIN SQ_UnderwritingRegionStage
	ON SQ_UnderwritingRegionStage.UnderwritingRegionCode = SQ_UnderwritingRegionRelationshipStage.UnderwritingRegionCode
),
LKP_ExistingUnderwritingRegion AS (
	SELECT
	HashKey,
	UnderwritingRegionAKID,
	UnderwritingRegionCode
	FROM (
		SELECT 
			HashKey,
			UnderwritingRegionAKID,
			UnderwritingRegionCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingRegion
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwritingRegionCode ORDER BY HashKey) = 1
),
LKP_UWManagerAKID AS (
	SELECT
	in_WestBendAssociateID,
	WestBendAssociateID,
	UnderwritingManagerAKID
	FROM (
		SELECT 
			in_WestBendAssociateID,
			WestBendAssociateID,
			UnderwritingManagerAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingManager
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY in_WestBendAssociateID) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_UWManagerAKID.UnderwritingManagerAKID AS lkp_UnderwritingManagerAKID,
	-- *INF*: IIF(IsNull(lkp_UnderwritingManagerAKID), -1, lkp_UnderwritingManagerAKID)
	IFF(lkp_UnderwritingManagerAKID IS NULL,
		- 1,
		lkp_UnderwritingManagerAKID
	) AS o_UnderwritingManagerAKID,
	LKP_ExistingUnderwritingRegion.UnderwritingRegionAKID AS lkp_UnderwritingRegionAKID,
	LKP_ExistingUnderwritingRegion.HashKey AS lkp_HashKey,
	JNR_OuterToRelationship.UnderwritingRegionCode,
	JNR_OuterToRelationship.UnderwritingRegionCodeDescription,
	-- *INF*: MD5(UnderwritingRegionCode || UnderwritingRegionCodeDescription || to_char(lkp_UnderwritingManagerAKID))
	MD5(UnderwritingRegionCode || UnderwritingRegionCodeDescription || to_char(lkp_UnderwritingManagerAKID
		)
	) AS v_NewHashKey,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingRegionAKID), 'NEW', 
	-- IIF((v_NewHashKey <> lkp_HashKey), 'UPDATE', 'NOCHANGE'))
	IFF(lkp_UnderwritingRegionAKID IS NULL,
		'NEW',
		IFF(( v_NewHashKey <> lkp_HashKey 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS ExpirationDate,
	JNR_OuterToRelationship.SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM JNR_OuterToRelationship
	LEFT JOIN LKP_ExistingUnderwritingRegion
	ON LKP_ExistingUnderwritingRegion.UnderwritingRegionCode = JNR_OuterToRelationship.UnderwritingRegionCode
	LEFT JOIN LKP_UWManagerAKID
	ON LKP_UWManagerAKID.WestBendAssociateID = JNR_OuterToRelationship.rel_WestBendAssociateID
),
FIL_insert AS (
	SELECT
	changed_flag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	o_NewHashKey AS HashKey, 
	lkp_UnderwritingRegionAKID AS UnderwritingRegionAKID, 
	o_UnderwritingManagerAKID AS UnderwritingManagerAKID, 
	UnderwritingRegionCode, 
	UnderwritingRegionCodeDescription
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'or changed_flag='UPDATE'
),
SEQ_UnderwritingRegion_ak_id AS (
	CREATE SEQUENCE SEQ_UnderwritingRegion_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_Assign_AKID AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	UnderwritingRegionAKID AS lkp_UnderwritingRegionAKID,
	SEQ_UnderwritingRegion_ak_id.NEXTVAL,
	-- *INF*: iif(isnull(lkp_UnderwritingRegionAKID), NEXTVAL, lkp_UnderwritingRegionAKID)
	IFF(lkp_UnderwritingRegionAKID IS NULL,
		NEXTVAL,
		lkp_UnderwritingRegionAKID
	) AS UnderwritingRegionAKID,
	0 AS Default_Int,
	'N/A' AS Default_char,
	HashKey,
	UnderwritingManagerAKID,
	UnderwritingRegionCode,
	UnderwritingRegionCodeDescription
	FROM FIL_insert
),
UnderwritingRegion_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingRegion
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, UnderwritingRegionAKID, UnderwritingManagerAKID, UnderwritingRegionCode, UnderwritingRegionCodeDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	UNDERWRITINGREGIONAKID, 
	UNDERWRITINGMANAGERAKID, 
	UNDERWRITINGREGIONCODE, 
	UNDERWRITINGREGIONCODEDESCRIPTION
	FROM EXP_Assign_AKID
),
SQ_UnderwritingRegion AS (
	SELECT 
		a.UnderwritingRegionID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.UnderwritingRegionAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingRegion a
	WHERE  a.UnderwritingRegionAKID  IN
		( SELECT UnderwritingRegionAKID  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingRegion
		WHERE CurrentSnapshotFlag = 1 GROUP BY UnderwritingRegionAKID HAVING count(*) > 1) 
	ORDER BY a.UnderwritingRegionAKID,a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	UnderwritingRegionID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	UnderwritingRegionAKID,
	-- *INF*: DECODE(TRUE,
	-- UnderwritingRegionAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(TRUE,
		UnderwritingRegionAKID = v_prev_AKID, DATEADD(SECOND,- 1,v_prev_EffectiveFromDate),
		OriginalEffectiveToDate
	) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	UnderwritingRegionAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_UnderwritingRegion
),
FIL_FirstRowInAKGroup AS (
	SELECT
	UnderwritingRegionID, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	UnderwritingRegionID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
UnderwritingRegion_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingRegion AS T
	USING UPD_OldRecord AS S
	ON T.UnderwritingRegionID = S.UnderwritingRegionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),