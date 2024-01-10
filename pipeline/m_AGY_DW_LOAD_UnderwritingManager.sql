WITH
SQ_AssociateStage AS (
	SELECT
		ModifiedDate,
		WestBendAssociateID,
		AssociateRole,
		RoleSpecificUserCode,
		DisplayName,
		LastName,
		FirstName,
		MiddleName,
		Suffix,
		EmailAddress,
		SourceSystemID,
		UserId
	FROM AssociateStage
	WHERE AssociateStage.AssociateRole = 'UNDERWRITER MANAGER'
),
LKP_ExistingUWManager AS (
	SELECT
	in_WestBendAssociateID,
	HashKey,
	UnderwritingManagerAKID,
	WestBendAssociateID
	FROM (
		SELECT 
			in_WestBendAssociateID,
			HashKey,
			UnderwritingManagerAKID,
			WestBendAssociateID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingManager
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY in_WestBendAssociateID) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ExistingUWManager.HashKey AS lkp_HashKey,
	LKP_ExistingUWManager.UnderwritingManagerAKID AS lkp_UnderwritingManagerAKID,
	SQ_AssociateStage.WestBendAssociateID,
	SQ_AssociateStage.RoleSpecificUserCode AS UnderwriterManagerCode,
	SQ_AssociateStage.DisplayName,
	SQ_AssociateStage.LastName,
	SQ_AssociateStage.FirstName,
	SQ_AssociateStage.MiddleName,
	SQ_AssociateStage.Suffix,
	SQ_AssociateStage.EmailAddress,
	SQ_AssociateStage.SourceSystemID,
	SQ_AssociateStage.UserId,
	-- *INF*: IIF(IsNull(UserId), 'N/A', UserId)
	IFF(UserId IS NULL, 'N/A', UserId) AS o_UserId,
	-- *INF*: MD5(UnderwriterManagerCode || DisplayName || LastName || FirstName || MiddleName || Suffix || EmailAddress || UserId)
	MD5(UnderwriterManagerCode || DisplayName || LastName || FirstName || MiddleName || Suffix || EmailAddress || UserId) AS v_NewHashKey,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingManagerAKID), 'NEW', 
	-- IIF((lkp_HashKey <> v_NewHashKey), 'UPDATE', 'NOCHANGE'))
	IFF(lkp_UnderwritingManagerAKID IS NULL, 'NEW', IFF(( lkp_HashKey <> v_NewHashKey ), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM SQ_AssociateStage
	LEFT JOIN LKP_ExistingUWManager
	ON LKP_ExistingUWManager.WestBendAssociateID = SQ_AssociateStage.WestBendAssociateID
),
FIL_insert AS (
	SELECT
	lkp_UnderwritingManagerAKID, 
	changed_flag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	o_NewHashKey AS HashKey, 
	WestBendAssociateID, 
	UnderwriterManagerCode, 
	DisplayName, 
	LastName, 
	FirstName, 
	MiddleName, 
	Suffix, 
	EmailAddress, 
	o_UserId AS UserId
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'or changed_flag='UPDATE'
),
SEQ_UnderwritingManager_AKID AS (
	CREATE SEQUENCE SEQ_UnderwritingManager_AKID
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
	lkp_UnderwritingManagerAKID,
	SEQ_UnderwritingManager_AKID.NEXTVAL,
	HashKey,
	-- *INF*: iif(isnull(lkp_UnderwritingManagerAKID),NEXTVAL,lkp_UnderwritingManagerAKID)
	IFF(lkp_UnderwritingManagerAKID IS NULL, NEXTVAL, lkp_UnderwritingManagerAKID) AS UnderwritingManagerAKID,
	WestBendAssociateID,
	UnderwriterManagerCode,
	DisplayName,
	LastName,
	FirstName,
	MiddleName,
	Suffix,
	EmailAddress,
	UserId
	FROM FIL_insert
),
UnderwritingManager_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingManager
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, UnderwritingManagerAKID, WestBendAssociateID, UnderwriterManagerCode, DisplayName, LastName, FirstName, MiddleName, Suffix, EmailAddress, UserId)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	UNDERWRITINGMANAGERAKID, 
	WESTBENDASSOCIATEID, 
	UNDERWRITERMANAGERCODE, 
	DISPLAYNAME, 
	LASTNAME, 
	FIRSTNAME, 
	MIDDLENAME, 
	SUFFIX, 
	EMAILADDRESS, 
	USERID
	FROM EXP_Assign_AKID
),
SQ_UnderwritingManager AS (
	SELECT 
		a.UnderwritingManagerID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.UnderwritingManagerAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingManager a
	WHERE  a.UnderwritingManagerAKID  IN
		( SELECT UnderwritingManagerAKID  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingManager
		WHERE CurrentSnapshotFlag = 1 GROUP BY UnderwritingManagerAKID HAVING count(*) > 1) 
	ORDER BY a.UnderwritingManagerAKID ,a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	UnderwritingManagerID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	UnderwritingManagerAKID,
	-- *INF*: DECODE(TRUE,
	-- UnderwritingManagerAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(TRUE,
		UnderwritingManagerAKID = v_prev_AKID, ADD_TO_DATE(v_prev_EffectiveFromDate, 'SS', - 1),
		OriginalEffectiveToDate) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	UnderwritingManagerAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_UnderwritingManager
),
FIL_FirstRowInAKGroup AS (
	SELECT
	UnderwritingManagerID, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	UnderwritingManagerID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
UnderwritingManager_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingManager AS T
	USING UPD_OldRecord AS S
	ON T.UnderwritingManagerID = S.UnderwritingManagerID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),