WITH
SQ_AgencyEmployeeStage AS (
	SELECT
		AgencyEmployeeStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID AS AgencyODSStageID,
		AgencyCode,
		AgencyEmployeeCode,
		AgencyEmployeeRole,
		ProducerCode,
		LastName,
		FirstName,
		MiddleName,
		Suffix,
		NickName,
		PrincipalFlag,
		PrimaryContactFlag,
		PhoneNumber,
		FaxNumber,
		EmailAddress,
		StatusCode,
		StatusCodeDescription,
		ListedDate,
		TerminatedDate,
		UserID,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM AgencyEmployeeStage
),
LKP_AgencyAKID AS (
	SELECT
	AgencyAKID,
	AgencyCode
	FROM (
		SELECT 
			AgencyAKID,
			AgencyCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY AgencyAKID DESC) = 1
),
EXP_CleanupData AS (
	SELECT
	SQ_AgencyEmployeeStage.AgencyCode,
	LKP_AgencyAKID.AgencyAKID,
	SQ_AgencyEmployeeStage.AgencyEmployeeCode,
	SQ_AgencyEmployeeStage.AgencyEmployeeRole,
	-- *INF*: IIF(IsNull(AgencyEmployeeRole), 'N/A', AgencyEmployeeRole)
	IFF(AgencyEmployeeRole IS NULL,
		'N/A',
		AgencyEmployeeRole
	) AS o_AgencyEmployeeRole,
	SQ_AgencyEmployeeStage.ProducerCode,
	-- *INF*: IIF(IsNull(ProducerCode), 'N/A', ProducerCode)
	IFF(ProducerCode IS NULL,
		'N/A',
		ProducerCode
	) AS o_ProducerCode,
	SQ_AgencyEmployeeStage.LastName,
	-- *INF*: IIF(IsNull(LastName), 'N/A', LastName)
	IFF(LastName IS NULL,
		'N/A',
		LastName
	) AS o_LastName,
	SQ_AgencyEmployeeStage.FirstName,
	-- *INF*: IIF(IsNull(FirstName), 'N/A', FirstName)
	IFF(FirstName IS NULL,
		'N/A',
		FirstName
	) AS o_FirstName,
	SQ_AgencyEmployeeStage.MiddleName,
	-- *INF*: IIF(IsNull(MiddleName), '', MiddleName)
	IFF(MiddleName IS NULL,
		'',
		MiddleName
	) AS o_MiddleName,
	SQ_AgencyEmployeeStage.Suffix,
	-- *INF*: IIF(IsNull(Suffix), '', Suffix)
	IFF(Suffix IS NULL,
		'',
		Suffix
	) AS o_Suffix,
	SQ_AgencyEmployeeStage.NickName,
	-- *INF*: IIF(IsNull(NickName), '', NickName)
	IFF(NickName IS NULL,
		'',
		NickName
	) AS o_NickName,
	SQ_AgencyEmployeeStage.PrincipalFlag,
	-- *INF*: Decode(true,
	-- AgencyEmployeeRole = 'PRINCIPAL', '1',
	-- IsNull(PrincipalFlag), '0', 
	-- PrincipalFlag)
	Decode(true,
		AgencyEmployeeRole = 'PRINCIPAL', '1',
		PrincipalFlag IS NULL, '0',
		PrincipalFlag
	) AS o_PrincipalFlag,
	SQ_AgencyEmployeeStage.PrimaryContactFlag,
	-- *INF*: IIF(IsNull(PrimaryContactFlag), 'N', PrimaryContactFlag)
	IFF(PrimaryContactFlag IS NULL,
		'N',
		PrimaryContactFlag
	) AS o_PrimaryContactFlag,
	SQ_AgencyEmployeeStage.PhoneNumber,
	-- *INF*: IIF(IsNull(PhoneNumber), '000-000-0000', PhoneNumber)
	IFF(PhoneNumber IS NULL,
		'000-000-0000',
		PhoneNumber
	) AS o_PhoneNumber,
	SQ_AgencyEmployeeStage.FaxNumber,
	-- *INF*: IIF(IsNull(FaxNumber), '000-000-0000', FaxNumber)
	IFF(FaxNumber IS NULL,
		'000-000-0000',
		FaxNumber
	) AS o_FaxNumber,
	SQ_AgencyEmployeeStage.EmailAddress,
	-- *INF*: IIF(IsNull(EmailAddress), 'N/A', EmailAddress)
	IFF(EmailAddress IS NULL,
		'N/A',
		EmailAddress
	) AS o_EmailAddress,
	SQ_AgencyEmployeeStage.StatusCode,
	-- *INF*: IIF(IsNull(StatusCode), 'A', StatusCode)
	IFF(StatusCode IS NULL,
		'A',
		StatusCode
	) AS o_StatusCode,
	SQ_AgencyEmployeeStage.StatusCodeDescription,
	-- *INF*: IIF(IsNull(StatusCodeDescription), 'Active', StatusCodeDescription)
	IFF(StatusCodeDescription IS NULL,
		'Active',
		StatusCodeDescription
	) AS o_StatusCodeDescription,
	SQ_AgencyEmployeeStage.ListedDate,
	-- *INF*: IIF(IsNull(ListedDate), to_date('1800-01-01', 'YYYY-MM-DD'), ListedDate)
	IFF(ListedDate IS NULL,
		to_date('1800-01-01', 'YYYY-MM-DD'
		),
		ListedDate
	) AS o_ListedDate,
	SQ_AgencyEmployeeStage.TerminatedDate,
	-- *INF*: IIF(IsNull(TerminatedDate), to_date('2999-12-31', 'YYYY-MM-DD'), TerminatedDate)
	IFF(TerminatedDate IS NULL,
		to_date('2999-12-31', 'YYYY-MM-DD'
		),
		TerminatedDate
	) AS o_TerminatedDate,
	SQ_AgencyEmployeeStage.UserID,
	-- *INF*: IIF(IsNull(UserID), 'N/A', UserID)
	IFF(UserID IS NULL,
		'N/A',
		UserID
	) AS o_UserID,
	SQ_AgencyEmployeeStage.SourceSystemID
	FROM SQ_AgencyEmployeeStage
	LEFT JOIN LKP_AgencyAKID
	ON LKP_AgencyAKID.AgencyCode = SQ_AgencyEmployeeStage.AgencyCode
),
LKP_ExistingEmployee AS (
	SELECT
	HashKey,
	AgencyEmployeeAKID,
	AgencyAKID,
	AgencyEmployeeCode
	FROM (
		SELECT 
			HashKey,
			AgencyEmployeeAKID,
			AgencyAKID,
			AgencyEmployeeCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployee
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID,AgencyEmployeeCode ORDER BY HashKey DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ExistingEmployee.HashKey AS lkp_HashKey,
	LKP_ExistingEmployee.AgencyEmployeeAKID AS lkp_AgencyEmployeeAKID,
	EXP_CleanupData.AgencyCode,
	EXP_CleanupData.AgencyAKID,
	EXP_CleanupData.AgencyEmployeeCode,
	EXP_CleanupData.o_AgencyEmployeeRole AS AgencyEmployeeRole,
	EXP_CleanupData.o_ProducerCode AS ProducerCode,
	EXP_CleanupData.o_LastName AS LastName,
	EXP_CleanupData.o_FirstName AS FirstName,
	EXP_CleanupData.o_MiddleName AS MiddleName,
	EXP_CleanupData.o_Suffix AS Suffix,
	EXP_CleanupData.o_NickName AS NickName,
	EXP_CleanupData.o_PrincipalFlag AS PrincipalFlag,
	EXP_CleanupData.o_PrimaryContactFlag AS PrimaryContactFlag,
	EXP_CleanupData.o_PhoneNumber AS PhoneNumber,
	EXP_CleanupData.o_FaxNumber AS FaxNumber,
	EXP_CleanupData.o_EmailAddress AS EmailAddress,
	EXP_CleanupData.o_StatusCode AS StatusCode,
	EXP_CleanupData.o_StatusCodeDescription AS StatusCodeDescription,
	EXP_CleanupData.o_ListedDate AS ListedDate,
	EXP_CleanupData.o_TerminatedDate AS TerminatedDate,
	EXP_CleanupData.o_UserID AS UserID,
	-- *INF*: MD5(AgencyEmployeeCode || AgencyEmployeeRole || ProducerCode || LastName || FirstName || MiddleName || Suffix || NickName || PrincipalFlag || PrimaryContactFlag || PhoneNumber || FaxNumber || EmailAddress || StatusCode || StatusCodeDescription || to_char(ListedDate) || to_char(TerminatedDate) || UserID)
	MD5(AgencyEmployeeCode || AgencyEmployeeRole || ProducerCode || LastName || FirstName || MiddleName || Suffix || NickName || PrincipalFlag || PrimaryContactFlag || PhoneNumber || FaxNumber || EmailAddress || StatusCode || StatusCodeDescription || to_char(ListedDate
		) || to_char(TerminatedDate
		) || UserID
	) AS v_NewHashKey,
	v_NewHashKey AS o_HashKey,
	-- *INF*: IIF(ISNULL(lkp_AgencyEmployeeAKID), 'NEW', 
	-- IIF((lkp_HashKey <> v_NewHashKey),'UPDATE', 'NOCHANGE'))
	IFF(lkp_AgencyEmployeeAKID IS NULL,
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
	) AS EffectiveFromDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS EffectiveToDate,
	EXP_CleanupData.SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM EXP_CleanupData
	LEFT JOIN LKP_ExistingEmployee
	ON LKP_ExistingEmployee.AgencyAKID = EXP_CleanupData.AgencyAKID AND LKP_ExistingEmployee.AgencyEmployeeCode = EXP_CleanupData.AgencyEmployeeCode
),
FIL_insert AS (
	SELECT
	lkp_AgencyEmployeeAKID AS lkp_AgencyEmployee_AKID, 
	AgencyCode, 
	changed_flag, 
	CurrentSnapshotFlag, 
	AuditID AS audit_id, 
	EffectiveFromDate, 
	EffectiveToDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	AgencyAKID, 
	AgencyEmployeeCode, 
	AgencyEmployeeRole, 
	ProducerCode, 
	LastName, 
	FirstName, 
	MiddleName, 
	Suffix, 
	NickName, 
	PrincipalFlag, 
	PrimaryContactFlag, 
	PhoneNumber, 
	FaxNumber, 
	EmailAddress, 
	StatusCode, 
	StatusCodeDescription, 
	ListedDate, 
	TerminatedDate, 
	UserID, 
	o_HashKey
	FROM EXP_Detect_Changes
	WHERE (changed_flag='NEW'or changed_flag='UPDATE') 


--and AgencyCode <> '99999'
),
SEQ_AgencyEmployeeAKID AS (
	CREATE SEQUENCE SEQ_AgencyEmployeeAKID
	START = 0
	INCREMENT = 1;
),
EXP_Assign_AKID AS (
	SELECT
	CurrentSnapshotFlag,
	audit_id AS AuditID,
	EffectiveFromDate,
	EffectiveToDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	lkp_AgencyEmployee_AKID AS lkp_AgencyEmployeeAKID,
	SEQ_AgencyEmployeeAKID.NEXTVAL,
	-- *INF*: iif(isnull(lkp_AgencyEmployeeAKID),NEXTVAL,lkp_AgencyEmployeeAKID)
	IFF(lkp_AgencyEmployeeAKID IS NULL,
		NEXTVAL,
		lkp_AgencyEmployeeAKID
	) AS o_AgencyEmployeeAKID,
	AgencyAKID,
	AgencyEmployeeCode,
	AgencyEmployeeRole,
	ProducerCode,
	LastName,
	FirstName,
	MiddleName,
	Suffix,
	NickName,
	PrincipalFlag,
	PrimaryContactFlag,
	PhoneNumber,
	FaxNumber,
	EmailAddress,
	StatusCode,
	StatusCodeDescription,
	ListedDate,
	TerminatedDate,
	UserID,
	o_HashKey
	FROM FIL_insert
),
AgencyEmployee_Inserts AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployee
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, HashKey, AgencyEmployeeAKID, AgencyAKID, AgencyEmployeeCode, AgencyEmployeeRole, ProducerCode, LastName, FirstName, MiddleName, Suffix, NickName, PrincipalFlag, PrimaryContactFlag, PhoneNumber, FaxNumber, EmailAddress, StatusCode, StatusCodeDescription, ListedDate, TerminatedDate, UserID)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EffectiveFromDate AS EFFECTIVEDATE, 
	EffectiveToDate AS EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	o_HashKey AS HASHKEY, 
	o_AgencyEmployeeAKID AS AGENCYEMPLOYEEAKID, 
	AGENCYAKID, 
	AGENCYEMPLOYEECODE, 
	AGENCYEMPLOYEEROLE, 
	PRODUCERCODE, 
	LASTNAME, 
	FIRSTNAME, 
	MIDDLENAME, 
	SUFFIX, 
	NICKNAME, 
	PRINCIPALFLAG, 
	PRIMARYCONTACTFLAG, 
	PHONENUMBER, 
	FAXNUMBER, 
	EMAILADDRESS, 
	STATUSCODE, 
	STATUSCODEDESCRIPTION, 
	LISTEDDATE, 
	TERMINATEDDATE, 
	USERID
	FROM EXP_Assign_AKID
),
SQ_AgencyEmployee AS (
	SELECT 
		a.AgencyEmployeeID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.AgencyEmployeeAKID  
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployee a
	WHERE  a.AgencyEmployeeAKID  IN
		( SELECT AgencyEmployeeAKID FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployee
		WHERE CurrentSnapshotFlag = 1 GROUP BY AgencyEmployeeAKID HAVING count(*) > 1) 
	ORDER BY a.AgencyEmployeeAKID, a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	AgencyEmployeeID,
	EffectiveDate AS EffectiveFromDate,
	ExpirationDate AS OriginalEffectiveToDate,
	AgencyEmployeeAKID,
	-- *INF*: DECODE(TRUE,
	-- AgencyEmployeeAKID = v_prev_AKID , ADD_TO_DATE(v_prev_EffectiveFromDate,'SS',-1),
	-- OriginalEffectiveToDate)
	DECODE(TRUE,
		AgencyEmployeeAKID = v_prev_AKID, DATEADD(SECOND,- 1,v_prev_EffectiveFromDate),
		OriginalEffectiveToDate
	) AS v_EffectiveToDate,
	v_EffectiveToDate AS o_EffectiveToDate,
	AgencyEmployeeAKID AS v_prev_AKID,
	EffectiveFromDate AS v_prev_EffectiveFromDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_AgencyEmployee
),
FIL_FirstRowInAKGroup AS (
	SELECT
	AgencyEmployeeID, 
	OriginalEffectiveToDate, 
	o_EffectiveToDate AS NewEffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalEffectiveToDate != NewEffectiveToDate
),
UPD_OldRecord AS (
	SELECT
	AgencyEmployeeID, 
	NewEffectiveToDate AS EffectiveToDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
AgencyEmployee_Updates AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployee AS T
	USING UPD_OldRecord AS S
	ON T.AgencyEmployeeID = S.AgencyEmployeeID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.EffectiveToDate, T.ModifiedDate = S.ModifiedDate
),