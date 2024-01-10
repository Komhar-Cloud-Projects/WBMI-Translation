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
		SourceSystemID,
		UserId,
		StrategicProfitCenterCode
	FROM AssociateStage
	WHERE AssociateStage.AssociateRole in ('UNDERWRITER', 'UNDERWRITER ASSISTANT')
),
SQ_UnderwritingReportingRelationshipStage AS (
	SELECT
		UnderwritingReportingRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		WestBendAssociateID,
		ReportToAssociateID,
		ReportToWestBendAssociateID,
		RelationshipType,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM UnderwritingReportingRelationshipStage1
),
JNR_OuterToRelationship AS (SELECT
	SQ_AssociateStage.WestBendAssociateID, 
	SQ_AssociateStage.AssociateRole, 
	SQ_AssociateStage.RoleSpecificUserCode, 
	SQ_AssociateStage.DisplayName, 
	SQ_AssociateStage.LastName, 
	SQ_AssociateStage.FirstName, 
	SQ_AssociateStage.MiddleName, 
	SQ_AssociateStage.Suffix, 
	SQ_AssociateStage.EmailAddress, 
	SQ_AssociateStage.SourceSystemID, 
	SQ_AssociateStage.UserId, 
	SQ_AssociateStage.StrategicProfitCenterCode, 
	SQ_UnderwritingReportingRelationshipStage.WestBendAssociateID AS rpt_WestBendAssociateID, 
	SQ_UnderwritingReportingRelationshipStage.ReportToWestBendAssociateID AS rpt_ReportToWestBendAssociateID
	FROM SQ_UnderwritingReportingRelationshipStage
	RIGHT OUTER JOIN SQ_AssociateStage
	ON SQ_AssociateStage.WestBendAssociateID = SQ_UnderwritingReportingRelationshipStage.WestBendAssociateID
),
LKP_ExistingUnderwritingAssociate AS (
	SELECT
	in_WestBendAssociateID,
	UnderwritingAssociateAKID,
	HashKey,
	WestBendAssociateID
	FROM (
		SELECT 
			in_WestBendAssociateID,
			UnderwritingAssociateAKID,
			HashKey,
			WestBendAssociateID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingAssociate
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY in_WestBendAssociateID) = 1
),
LKP_UWManagerAKID AS (
	SELECT
	in_ReportToWestBendAssociateID,
	WestBendAssociateID,
	UnderwritingManagerAKID
	FROM (
		SELECT 
			in_ReportToWestBendAssociateID,
			WestBendAssociateID,
			UnderwritingManagerAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingManager
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WestBendAssociateID ORDER BY in_ReportToWestBendAssociateID) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ExistingUnderwritingAssociate.UnderwritingAssociateAKID AS lkp_UnderwritingAssociateAKID,
	LKP_ExistingUnderwritingAssociate.HashKey AS lkp_HashKey,
	LKP_UWManagerAKID.UnderwritingManagerAKID AS lkp_UnderwritingManagerAKID,
	-- *INF*: IIF(IsNull(lkp_UnderwritingManagerAKID), -1, lkp_UnderwritingManagerAKID)
	IFF(lkp_UnderwritingManagerAKID IS NULL,
		- 1,
		lkp_UnderwritingManagerAKID
	) AS o_UnderwritingManagerAKID,
	JNR_OuterToRelationship.WestBendAssociateID,
	JNR_OuterToRelationship.AssociateRole,
	JNR_OuterToRelationship.RoleSpecificUserCode AS UnderwriterCode,
	JNR_OuterToRelationship.DisplayName,
	-- *INF*: IIF(IsNull(DisplayName), 'UNKNOWN', DisplayName)
	IFF(DisplayName IS NULL,
		'UNKNOWN',
		DisplayName
	) AS o_DisplayName,
	JNR_OuterToRelationship.LastName,
	-- *INF*: IIF(IsNull(LastName), 'N/A', LastName)
	IFF(LastName IS NULL,
		'N/A',
		LastName
	) AS o_LastName,
	JNR_OuterToRelationship.FirstName,
	-- *INF*: IIF(IsNull(FirstName), 'N/A', FirstName)
	IFF(FirstName IS NULL,
		'N/A',
		FirstName
	) AS o_FirstName,
	JNR_OuterToRelationship.MiddleName,
	JNR_OuterToRelationship.Suffix,
	JNR_OuterToRelationship.EmailAddress,
	-- *INF*: IIF(IsNull(rtrim(ltrim(EmailAddress))), 'N/A', EmailAddress)
	IFF(rtrim(ltrim(EmailAddress
			)
		) IS NULL,
		'N/A',
		EmailAddress
	) AS o_EmailAddress,
	JNR_OuterToRelationship.UserId,
	-- *INF*: IIF(IsNull(rtrim(ltrim(UserId))), 'N/A', UserId)
	IFF(rtrim(ltrim(UserId
			)
		) IS NULL,
		'N/A',
		UserId
	) AS o_UserId,
	-- *INF*: MD5(AssociateRole || UnderwriterCode || DisplayName || LastName || FirstName || MiddleName || Suffix || EmailAddress || UserId || to_char(lkp_UnderwritingManagerAKID))
	MD5(AssociateRole || UnderwriterCode || DisplayName || LastName || FirstName || MiddleName || Suffix || EmailAddress || UserId || to_char(lkp_UnderwritingManagerAKID
		)
	) AS v_NewHashKey,
	v_NewHashKey AS o_NewHashKey,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingAssociateAKID), 'NEW', 
	-- IIF((lkp_HashKey <> v_NewHashKey), 'UPDATE', 'NOCHANGE'))
	IFF(lkp_UnderwritingAssociateAKID IS NULL,
		'NEW',
		IFF(( lkp_HashKey <> v_NewHashKey 
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
	SYSDATE AS ModifiedDate,
	JNR_OuterToRelationship.StrategicProfitCenterCode
	FROM JNR_OuterToRelationship
	LEFT JOIN LKP_ExistingUnderwritingAssociate
	ON LKP_ExistingUnderwritingAssociate.WestBendAssociateID = JNR_OuterToRelationship.WestBendAssociateID
	LEFT JOIN LKP_UWManagerAKID
	ON LKP_UWManagerAKID.WestBendAssociateID = JNR_OuterToRelationship.rpt_ReportToWestBendAssociateID
),
FIL_insert AS (
	SELECT
	lkp_UnderwritingAssociateAKID, 
	changed_flag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	o_NewHashKey AS HashKey, 
	o_UnderwritingManagerAKID AS UnderwritingManagerAKID, 
	WestBendAssociateID, 
	AssociateRole, 
	UnderwriterCode, 
	o_DisplayName AS DisplayName, 
	o_LastName AS LastName, 
	o_FirstName AS FirstName, 
	MiddleName, 
	Suffix, 
	o_EmailAddress AS EmailAddress, 
	o_UserId AS UserId
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'or changed_flag='UPDATE'
),
SEQ_UnderwritingAssociate_AKID AS (
	CREATE SEQUENCE SEQ_UnderwritingAssociate_AKID
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
	lkp_UnderwritingAssociateAKID,
	SEQ_UnderwritingAssociate_AKID.NEXTVAL,
	HashKey,
	-- *INF*: iif(isnull(lkp_UnderwritingAssociateAKID),NEXTVAL,lkp_UnderwritingAssociateAKID)
	IFF(lkp_UnderwritingAssociateAKID IS NULL,
		NEXTVAL,
		lkp_UnderwritingAssociateAKID
	) AS UnderwritingAssociateAKID,
	UnderwritingManagerAKID,
	WestBendAssociateID,
	AssociateRole,
	UnderwriterCode,
	DisplayName,
	LastName,
	FirstName,
	MiddleName,
	Suffix,
	EmailAddress,
	UserId
	FROM FIL_insert
),
UnderwritingAssociate_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingAssociate
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, UnderwritingAssociateAKID, UnderwritingManagerAKID, WestBendAssociateID, AssociateRole, UnderwriterCode, DisplayName, LastName, FirstName, MiddleName, Suffix, EmailAddress, UserId)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	HASHKEY, 
	UNDERWRITINGASSOCIATEAKID, 
	UNDERWRITINGMANAGERAKID, 
	WESTBENDASSOCIATEID, 
	ASSOCIATEROLE, 
	UNDERWRITERCODE, 
	DISPLAYNAME, 
	LASTNAME, 
	FIRSTNAME, 
	MIDDLENAME, 
	SUFFIX, 
	EMAILADDRESS, 
	USERID
	FROM EXP_Assign_AKID
),
SQ_UnderwritingAssociate AS (
	SELECT 
		a.UnderwritingAssociateID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.UnderwritingAssociateAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingAssociate a
	WHERE  a.UnderwritingAssociateAKID  IN
		( SELECT UnderwritingAssociateAKID  
		  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingAssociate
		  WHERE CurrentSnapshotFlag = 1 
		  GROUP BY UnderwritingAssociateAKID HAVING count(*) > 1) 
	ORDER BY a.UnderwritingAssociateAKID, a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	UnderwritingAssociateID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	UnderwritingAssociateAKID,
	-- *INF*: DECODE(TRUE,
	-- UnderwritingAssociateAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(TRUE,
		UnderwritingAssociateAKID = v_prev_AKID, DATEADD(SECOND,- 1,v_prev_EffectiveFromDate),
		OriginalEffectiveToDate
	) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	UnderwritingAssociateAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_UnderwritingAssociate
),
FIL_FirstRowInAKGroup AS (
	SELECT
	UnderwritingAssociateID, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	UnderwritingAssociateID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
UnderwritingAssociate_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingAssociate AS T
	USING UPD_OldRecord AS S
	ON T.UnderwritingAssociateID = S.UnderwritingAssociateID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),