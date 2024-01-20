WITH
SQ_AgencyEmployee AS (
	SELECT
		AgencyEmployeeID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
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
		UserID
	FROM AgencyEmployee
),
LKP_AgencyCode AS (
	SELECT
	AgencyCode,
	AgencyID
	FROM (
		SELECT 
			AgencyCode,
			AgencyID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agency
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID ORDER BY AgencyCode) = 1
),
EXP_Add_MetaDataFields AS (
	SELECT
	SQ_AgencyEmployee.AgencyEmployeeID,
	SQ_AgencyEmployee.SourceSystemID,
	SQ_AgencyEmployee.HashKey,
	SQ_AgencyEmployee.ModifiedUserID,
	SQ_AgencyEmployee.ModifiedDate,
	SQ_AgencyEmployee.AgencyID,
	LKP_AgencyCode.AgencyCode,
	SQ_AgencyEmployee.AgencyEmployeeCode,
	SQ_AgencyEmployee.AgencyEmployeeRole,
	SQ_AgencyEmployee.ProducerCode,
	SQ_AgencyEmployee.LastName,
	SQ_AgencyEmployee.FirstName,
	SQ_AgencyEmployee.MiddleName,
	SQ_AgencyEmployee.Suffix,
	SQ_AgencyEmployee.NickName,
	SQ_AgencyEmployee.PrincipalFlag,
	SQ_AgencyEmployee.PrimaryContactFlag,
	SQ_AgencyEmployee.PhoneNumber,
	SQ_AgencyEmployee.FaxNumber,
	SQ_AgencyEmployee.EmailAddress,
	SQ_AgencyEmployee.StatusCode,
	SQ_AgencyEmployee.StatusCodeDescription,
	SQ_AgencyEmployee.ListedDate,
	SQ_AgencyEmployee.TerminatedDate,
	SQ_AgencyEmployee.UserID,
	sysdate AS Extract_Date,
	Sysdate AS As_of_Date,
	1 AS Record_Count,
	@{pipeline().parameters.SOURCESYSTEMID} AS Source_System_ID
	FROM SQ_AgencyEmployee
	LEFT JOIN LKP_AgencyCode
	ON LKP_AgencyCode.AgencyID = SQ_AgencyEmployee.AgencyID
),
AgencyEmployeeStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployeeStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployeeStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, AgencyEmployeeCode, AgencyEmployeeRole, ProducerCode, LastName, FirstName, MiddleName, Suffix, NickName, PrincipalFlag, PrimaryContactFlag, PhoneNumber, FaxNumber, EmailAddress, StatusCode, StatusCodeDescription, ListedDate, TerminatedDate, UserID, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	SourceSystemID AS AGENCYODSSOURCESYSTEMID, 
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
	Extract_Date AS EXTRACTDATE, 
	As_of_Date AS ASOFDATE, 
	Record_Count AS RECORDCOUNT, 
	Source_System_ID AS SOURCESYSTEMID
	FROM EXP_Add_MetaDataFields
),