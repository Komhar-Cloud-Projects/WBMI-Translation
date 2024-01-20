WITH
SQ_AgencyEmployeeStage AS (
	SELECT
		AgencyEmployeeStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
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
LKP_ExistingArchive AS (
	SELECT
	HashKey,
	in_AgencyID,
	in_AgencyEmployeeCode,
	AgencyID,
	AgencyEmployeeCode,
	ModifiedDate
	FROM (
		select	a.ModifiedDate as ModifiedDate,
				a.HashKey as HashKey,
				a.AgencyID as AgencyID, 
				a.AgencyEmployeeCode as AgencyEmployeeCode
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyEmployeeStage a
		inner join (
					select AgencyID, AgencyEmployeeCode, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyEmployeeStage 
					group by AgencyID, AgencyEmployeeCode) b
		on a.AgencyID = b.AgencyID
		and a.AgencyEmployeeCode = b.AgencyEmployeeCode
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID,AgencyEmployeeCode ORDER BY HashKey) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_AgencyEmployeeStage.AgencyEmployeeStageID,
	SQ_AgencyEmployeeStage.AgencyODSSourceSystemID,
	SQ_AgencyEmployeeStage.HashKey,
	SQ_AgencyEmployeeStage.ModifiedUserID,
	SQ_AgencyEmployeeStage.ModifiedDate,
	SQ_AgencyEmployeeStage.AgencyID,
	SQ_AgencyEmployeeStage.AgencyCode,
	SQ_AgencyEmployeeStage.AgencyEmployeeCode,
	SQ_AgencyEmployeeStage.AgencyEmployeeRole,
	SQ_AgencyEmployeeStage.ProducerCode,
	SQ_AgencyEmployeeStage.LastName,
	SQ_AgencyEmployeeStage.FirstName,
	SQ_AgencyEmployeeStage.MiddleName,
	SQ_AgencyEmployeeStage.Suffix,
	SQ_AgencyEmployeeStage.NickName,
	SQ_AgencyEmployeeStage.PrincipalFlag,
	SQ_AgencyEmployeeStage.PrimaryContactFlag,
	SQ_AgencyEmployeeStage.PhoneNumber,
	SQ_AgencyEmployeeStage.FaxNumber,
	SQ_AgencyEmployeeStage.EmailAddress,
	SQ_AgencyEmployeeStage.StatusCode,
	SQ_AgencyEmployeeStage.StatusCodeDescription,
	SQ_AgencyEmployeeStage.ListedDate,
	SQ_AgencyEmployeeStage.TerminatedDate,
	SQ_AgencyEmployeeStage.UserID,
	SQ_AgencyEmployeeStage.ExtractDate,
	SQ_AgencyEmployeeStage.AsOfDate,
	SQ_AgencyEmployeeStage.RecordCount,
	SQ_AgencyEmployeeStage.SourceSystemID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS OUT_AUDIT_ID,
	LKP_ExistingArchive.HashKey AS lkp_HashKey,
	-- *INF*: Decode(true,
	-- HashKey = lkp_HashKey, 'IGNORE',
	-- IsNull(lkp_HashKey), 'INSERT',
	-- 'UPDATE')
	Decode(
	    true,
	    HashKey = lkp_HashKey, 'IGNORE',
	    lkp_HashKey IS NULL, 'INSERT',
	    'UPDATE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM SQ_AgencyEmployeeStage
	LEFT JOIN LKP_ExistingArchive
	ON LKP_ExistingArchive.AgencyID = SQ_AgencyEmployeeStage.AgencyID AND LKP_ExistingArchive.AgencyEmployeeCode = SQ_AgencyEmployeeStage.AgencyEmployeeCode
),
FIL_ChangesOnly AS (
	SELECT
	AgencyEmployeeStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AgencyID, 
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
	UserPassword, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	OUT_AUDIT_ID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchAgencyEmployeeStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchAgencyEmployeeStage
	(AgencyEmployeeStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, AgencyEmployeeCode, AgencyEmployeeRole, ProducerCode, LastName, FirstName, MiddleName, Suffix, NickName, PrincipalFlag, PrimaryContactFlag, PhoneNumber, FaxNumber, EmailAddress, StatusCode, StatusCodeDescription, ListedDate, TerminatedDate, UserID, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	AGENCYEMPLOYEESTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	AGENCYCODE, 
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
	USERID, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	OUT_AUDIT_ID AS AUDITID
	FROM FIL_ChangesOnly
),