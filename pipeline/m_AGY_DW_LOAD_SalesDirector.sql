WITH
SQ_AssociateStage AS (
	SELECT
		HashKey,
		ModifiedUserID,
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
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID,
		UserId,
		StrategicProfitCenterCode,
		StrategicProfitCenterDescription
	FROM AssociateStage
	WHERE AssociateStage.AssociateRole = 'SALES DIRECTOR'
),
LKP_ExistingSalesDirector AS (
	SELECT
	CurrentSnapshotFlag,
	HashKey,
	SalesDirectorAKID,
	WestBendAssociateID
	FROM (
		SELECT 
			CurrentSnapshotFlag,
			HashKey,
			SalesDirectorAKID,
			WestBendAssociateID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SalesDirector
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY CurrentSnapshotFlag) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ExistingSalesDirector.HashKey AS lkp_HashKey,
	LKP_ExistingSalesDirector.SalesDirectorAKID AS lkp_SalesDirectorAKID,
	SQ_AssociateStage.WestBendAssociateID,
	SQ_AssociateStage.DisplayName,
	SQ_AssociateStage.LastName,
	SQ_AssociateStage.FirstName,
	SQ_AssociateStage.MiddleName,
	SQ_AssociateStage.Suffix,
	SQ_AssociateStage.EmailAddress,
	SQ_AssociateStage.RoleSpecificUserCode AS SalesDirectorCode,
	SQ_AssociateStage.UserId,
	-- *INF*: IIF(IsNull(UserId), 'N/A', UserId)
	IFF(UserId IS NULL, 'N/A', UserId) AS o_UserId,
	-- *INF*: MD5(DisplayName || LastName || FirstName || MiddleName || Suffix || EmailAddress || SalesDirectorCode || UserId)
	MD5(DisplayName || LastName || FirstName || MiddleName || Suffix || EmailAddress || SalesDirectorCode || UserId) AS v_NewHashKey,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(ISNULL(lkp_HashKey), 'NEW', IIF((lkp_HashKey <> v_NewHashKey), 'UPDATE', 'NOCHANGE'))
	IFF(
	    lkp_HashKey IS NULL, 'NEW',
	    IFF(
	        (lkp_HashKey <> v_NewHashKey), 'UPDATE', 'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: iif(v_changed_flag='NEW', to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'), sysdate)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SQ_AssociateStage.SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM SQ_AssociateStage
	LEFT JOIN LKP_ExistingSalesDirector
	ON LKP_ExistingSalesDirector.WestBendAssociateID = SQ_AssociateStage.WestBendAssociateID
),
FIL_insert AS (
	SELECT
	lkp_SalesDirectorAKID AS lkp_SalesDirector_AKID, 
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
	DisplayName, 
	LastName, 
	FirstName, 
	MiddleName, 
	Suffix, 
	EmailAddress, 
	SalesDirectorCode, 
	o_UserId AS UserId
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'or changed_flag='UPDATE'
),
SEQ_SalesDirector_AKID AS (
	CREATE SEQUENCE SEQ_SalesDirector_AKID
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
	lkp_SalesDirector_AKID,
	SEQ_SalesDirector_AKID.NEXTVAL,
	-- *INF*: iif(isnull(lkp_SalesDirector_AKID),NEXTVAL,lkp_SalesDirector_AKID)
	IFF(lkp_SalesDirector_AKID IS NULL, NEXTVAL, lkp_SalesDirector_AKID) AS SalesDirectorAKID,
	0 AS Default_Int,
	'N/A' AS Default_char,
	HashKey,
	WestBendAssociateID,
	DisplayName,
	LastName,
	FirstName,
	MiddleName,
	Suffix,
	EmailAddress,
	SalesDirectorCode,
	UserId
	FROM FIL_insert
),
SalesDirector_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDirector
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, SalesDirectorAKID, WestBendAssociateID, SalesDirectorCode, DisplayName, LastName, FirstName, MiddleName, Suffix, EmailAddress, UserId)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	SALESDIRECTORAKID, 
	WESTBENDASSOCIATEID, 
	SALESDIRECTORCODE, 
	DISPLAYNAME, 
	LASTNAME, 
	FIRSTNAME, 
	MIDDLENAME, 
	SUFFIX, 
	EMAILADDRESS, 
	USERID
	FROM EXP_Assign_AKID
),
SQ_SalesDirector AS (
	SELECT 
		a.SalesDirectorID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.SalesDirectorAKID  
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDirector a
	WHERE  a.SalesDirectorAKID  IN
		( SELECT SalesDirectorAKID FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDirector
		WHERE CurrentSnapshotFlag = 1 GROUP BY SalesDirectorAKID HAVING count(*) > 1) 
	ORDER BY a.SalesDirectorAKID ,a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	SalesDirectorID,
	EffectiveDate,
	ExpirationDate AS OriginalExpirationDate,
	SalesDirectorAKID,
	-- *INF*: DECODE(TRUE,
	-- SalesDirectorAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveDate,'SS',-1),
	-- OriginalExpirationDate)
	DECODE(
	    TRUE,
	    SalesDirectorAKID = v_prev_AKID, DATEADD(SECOND,- 1,v_prev_EffectiveDate),
	    OriginalExpirationDate
	) AS v_ExpirationDate,
	v_ExpirationDate AS o_ExpirationDate,
	SalesDirectorAKID AS v_prev_AKID,
	EffectiveDate AS v_prev_EffectiveDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_SalesDirector
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SalesDirectorID, 
	OriginalExpirationDate AS ExpirationDate, 
	o_ExpirationDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE ExpirationDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	SalesDirectorID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
SalesDirector_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDirector AS T
	USING UPD_OldRecord AS S
	ON T.SalesDirectorID = S.SalesDirectorID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),