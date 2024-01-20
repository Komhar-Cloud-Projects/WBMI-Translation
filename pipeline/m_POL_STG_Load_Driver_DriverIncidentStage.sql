WITH
earsinfile_DriverandIncidentsTable AS (
),
EXPTRANS3 AS (
	SELECT
	LicenseNumber AS in_LicenseNumber,
	-- *INF*: RTRIM(in_LicenseNumber)
	RTRIM(in_LicenseNumber) AS out_LicenseNumber
	FROM earsinfile_DriverandIncidentsTable
),
LKP_ExploreEARSDriverRecord AS (
	SELECT
	Birthdate,
	FirstName,
	MiddleName,
	LastName,
	Gender,
	LicenseNumber,
	LicenseState
	FROM (
		SELECT 
			Birthdate,
			FirstName,
			MiddleName,
			LastName,
			Gender,
			LicenseNumber,
			LicenseState
		FROM ExploreEARSDriverRecord
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LicenseNumber,LicenseState ORDER BY Birthdate DESC) = 1
),
EXP_Convert_To_Date AS (
	SELECT
	earsinfile_DriverandIncidentsTable.FileCreationDate,
	-- *INF*: TO_Char(SYSDATE)
	TO_Char(CURRENT_TIMESTAMP) AS test_PolicyExpirationDate,
	-- *INF*: TO_DATE(FileCreationDate,'YYYYMMDD')
	TO_TIMESTAMP(FileCreationDate, 'YYYYMMDD') AS out_FileCreationDate,
	LKP_ExploreEARSDriverRecord.Birthdate AS LKP_DateOfBirth,
	earsinfile_DriverandIncidentsTable.BirthDate,
	-- *INF*: IIF (Is_Spaces(BirthDate),
	-- LKP_DateOfBirth, 
	-- TO_DATE(BirthDate,'YYYYMMDD'))
	IFF(
	    LENGTH(BirthDate)>0 AND TRIM(BirthDate)='', LKP_DateOfBirth,
	    TO_TIMESTAMP(BirthDate, 'YYYYMMDD')
	) AS out_BirthDate,
	earsinfile_DriverandIncidentsTable.PolicyExpirationDate,
	-- *INF*: TO_DATE(PolicyExpirationDate,'MM/DD/YYYY')
	TO_TIMESTAMP(PolicyExpirationDate, 'MM/DD/YYYY') AS out_PolicyExpiratonDate,
	earsinfile_DriverandIncidentsTable.IncidentEndDate,
	-- *INF*: --TO_DATE(IncidentEndDate,'YYYYMMDD')
	-- 
	-- IIF (Is_Spaces(IncidentEndDate),
	-- TO_DATE(PolicyExpirationDate,'MM/DD/YYYY'),
	-- TO_DATE(IncidentEndDate,'YYYYMMDD'))
	IFF(
	    LENGTH(IncidentEndDate)>0 AND TRIM(IncidentEndDate)='',
	    TO_TIMESTAMP(PolicyExpirationDate, 'MM/DD/YYYY'),
	    TO_TIMESTAMP(IncidentEndDate, 'YYYYMMDD')
	) AS out_IncidentEndDate,
	earsinfile_DriverandIncidentsTable.IncidentStartDate,
	-- *INF*: IIF (Is_Spaces(IncidentStartDate),
	-- TO_DATE(PolicyExpirationDate,'MM/DD/YYYY'),
	-- TO_DATE(IncidentStartDate,'YYYYMMDD'))
	-- 
	-- 
	-- --TO_DATE(IncidentStartDate,'YYYYMMDD')
	IFF(
	    LENGTH(IncidentStartDate)>0 AND TRIM(IncidentStartDate)='',
	    TO_TIMESTAMP(PolicyExpirationDate, 'MM/DD/YYYY'),
	    TO_TIMESTAMP(IncidentStartDate, 'YYYYMMDD')
	) AS out_IncedentStartDate,
	-- *INF*: RTRIM(in_LicenseNumber)
	RTRIM(in_LicenseNumber) AS out_LicenseNumber,
	earsinfile_DriverandIncidentsTable.LicenseState AS in_LicenseState,
	earsinfile_DriverandIncidentsTable.LastName AS in_LastName,
	earsinfile_DriverandIncidentsTable.FirstName AS in_FirstName,
	earsinfile_DriverandIncidentsTable.MiddleName AS in_MiddleName,
	earsinfile_DriverandIncidentsTable.Gender AS in_Gender,
	LKP_ExploreEARSDriverRecord.FirstName AS LKP_FirstName,
	LKP_ExploreEARSDriverRecord.MiddleName AS LKP_MiddleInitial,
	LKP_ExploreEARSDriverRecord.LastName AS LKP_LastName,
	LKP_ExploreEARSDriverRecord.Gender AS LKP_Gender,
	-- *INF*: IIF ((in_LicenseState = 'IN' OR in_LicenseState = 'IL'), LKP_FirstName, in_FirstName)
	-- 
	-- 
	IFF((in_LicenseState = 'IN' OR in_LicenseState = 'IL'), LKP_FirstName, in_FirstName) AS out_FirstName,
	-- *INF*: IIF ((in_LicenseState = 'IN' OR in_LicenseState = 'IL'), LKP_MiddleInitial, in_MiddleName)
	IFF((in_LicenseState = 'IN' OR in_LicenseState = 'IL'), LKP_MiddleInitial, in_MiddleName) AS out_MiddleInitial,
	-- *INF*: IIF ((in_LicenseState = 'IN' OR in_LicenseState = 'IL'), LKP_LastName, in_LastName)
	-- 
	IFF((in_LicenseState = 'IN' OR in_LicenseState = 'IL'), LKP_LastName, in_LastName) AS out_LastName,
	-- *INF*: IIF ((in_LicenseState = 'IN' OR in_LicenseState = 'IL'), LKP_Gender, in_Gender)
	IFF((in_LicenseState = 'IN' OR in_LicenseState = 'IL'), LKP_Gender, in_Gender) AS out_Gender
	FROM earsinfile_DriverandIncidentsTable
	LEFT JOIN LKP_ExploreEARSDriverRecord
	ON LKP_ExploreEARSDriverRecord.LicenseNumber = EXPTRANS3.out_LicenseNumber AND LKP_ExploreEARSDriverRecord.LicenseState = earsinfile_DriverandIncidentsTable.LicenseState
),
LKP_DriverStage AS (
	SELECT
	LicenseNumber,
	in_FileCreationDate,
	in_LicenseState,
	in_LicenseNumber,
	FileCreationDate,
	LicenseState
	FROM (
		SELECT 
			LicenseNumber,
			in_FileCreationDate,
			in_LicenseState,
			in_LicenseNumber,
			FileCreationDate,
			LicenseState
		FROM DriverStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FileCreationDate,LicenseState,LicenseNumber ORDER BY LicenseNumber) = 1
),
FIL_Null_LicenseNumber AS (
	SELECT
	LKP_DriverStage.LicenseNumber AS lkp_LicenseNumber, 
	EXP_Convert_To_Date.out_FileCreationDate AS CreatedDate, 
	EXP_Convert_To_Date.out_FileCreationDate AS FileCreationDate, 
	earsinfile_DriverandIncidentsTable.FileIDCode, 
	earsinfile_DriverandIncidentsTable.SenderName, 
	earsinfile_DriverandIncidentsTable.LicenseState, 
	EXPTRANS3.out_LicenseNumber AS LicenseNumber, 
	earsinfile_DriverandIncidentsTable.Quoteback AS QuoteBack, 
	EXP_Convert_To_Date.out_LastName AS LastName, 
	EXP_Convert_To_Date.out_FirstName AS FirstName, 
	EXP_Convert_To_Date.out_MiddleInitial AS MiddleName, 
	earsinfile_DriverandIncidentsTable.StreetAddress, 
	earsinfile_DriverandIncidentsTable.City, 
	earsinfile_DriverandIncidentsTable.State AS StateCode, 
	earsinfile_DriverandIncidentsTable.ZipCode, 
	EXP_Convert_To_Date.out_BirthDate AS BirthDate, 
	earsinfile_DriverandIncidentsTable.Weight AS BodyWeight, 
	earsinfile_DriverandIncidentsTable.Height, 
	earsinfile_DriverandIncidentsTable.EyeColor, 
	earsinfile_DriverandIncidentsTable.PolicyNumber, 
	EXP_Convert_To_Date.out_PolicyExpiratonDate AS PolicyExpirationDate, 
	earsinfile_DriverandIncidentsTable.PreviousLicense, 
	EXP_Convert_To_Date.out_Gender AS Gender, 
	earsinfile_DriverandIncidentsTable.AccountNumber, 
	earsinfile_DriverandIncidentsTable.IncidentResult, 
	earsinfile_DriverandIncidentsTable.CustomerCode, 
	EXP_Convert_To_Date.out_IncedentStartDate AS IncidentStartDate, 
	earsinfile_DriverandIncidentsTable.IncidentDescription, 
	EXP_Convert_To_Date.out_IncidentEndDate AS IncidentEndDate, 
	earsinfile_DriverandIncidentsTable.IncidentPoints, 
	earsinfile_DriverandIncidentsTable.StateNatCode AS StateCode1
	FROM EXPTRANS3
	 -- Manually join with EXP_Convert_To_Date
	 -- Manually join with earsinfile_DriverandIncidentsTable
	LEFT JOIN LKP_DriverStage
	ON LKP_DriverStage.FileCreationDate = EXP_Convert_To_Date.out_FileCreationDate AND LKP_DriverStage.LicenseState = earsinfile_DriverandIncidentsTable.LicenseState AND LKP_DriverStage.LicenseNumber = EXPTRANS3.out_LicenseNumber
	WHERE ISNULL(lkp_LicenseNumber)
),
SEQ_DriverStageID AS (
	CREATE SEQUENCE SEQ_DriverStageID
	START = 0
	INCREMENT = 1;
),
EXP_EARS_File AS (
	SELECT
	SEQ_DriverStageID.NEXTVAL,
	-- *INF*: IIF(ISNULL(v_LAST_LicenseState) OR ISNULL(v_LAST_LicenseNumber),'Y', IIF(LicenseState != v_LAST_LicenseState OR LicenseNumber != v_LAST_LicenseNumber, 'Y', 'N'))
	IFF(
	    v_LAST_LicenseState IS NULL OR v_LAST_LicenseNumber IS NULL, 'Y',
	    IFF(
	        LicenseState != v_LAST_LicenseState
	    or LicenseNumber != v_LAST_LicenseNumber, 'Y',
	        'N'
	    )
	) AS v_New_Driver_Ind,
	v_New_Driver_Ind AS out_New_Driver_Ind,
	-- *INF*: IIF(v_New_Driver_Ind = 'Y', NEXTVAL, v_DriverStageId)
	IFF(v_New_Driver_Ind = 'Y', NEXTVAL, v_DriverStageId) AS v_DriverStageId,
	v_DriverStageId AS out_DriverStageId,
	FileCreationDate,
	FileIDCode,
	SenderName,
	LicenseState,
	LicenseState AS v_LAST_LicenseState,
	LicenseNumber,
	LicenseNumber AS v_LAST_LicenseNumber,
	QuoteBack AS Quoteback,
	LastName,
	FirstName,
	MiddleName,
	StreetAddress,
	City,
	StateCode AS State,
	ZipCode,
	BirthDate,
	BodyWeight AS Weight,
	Height,
	EyeColor,
	PolicyNumber,
	-- *INF*: SUBSTR(PolicyNumber,1,7)
	SUBSTR(PolicyNumber, 1, 7) AS out_PolicyNumber,
	PolicyExpirationDate,
	PreviousLicense,
	Gender,
	AccountNumber,
	IncidentResult,
	CustomerCode,
	IncidentStartDate,
	IncidentDescription,
	IncidentEndDate,
	IncidentPoints,
	StateCode1 AS StateCode,
	NationalCode
	FROM FIL_Null_LicenseNumber
),
EXPTRANS2 AS (
	SELECT
	out_New_Driver_Ind,
	out_DriverStageId,
	FileCreationDate,
	FileIDCode,
	SenderName,
	VersionNumber,
	LicenseState,
	LicenseNumber,
	Quoteback,
	ReportType,
	LastName,
	FirstName,
	MiddleName,
	StreetAddress,
	City,
	State,
	ZipCode,
	BirthDate,
	Weight,
	Height,
	EyeColor,
	Alias,
	out_PolicyNumber AS PolicyNumber,
	PolicyExpirationDate,
	PreviousLicense,
	Gender,
	AccountNumber,
	IncidentResult,
	CustomerCode,
	IncidentStartDate,
	IncidentDescription,
	IncidentEndDate,
	IncidentPoints,
	StateCode,
	NationalCode
	FROM EXP_EARS_File
),
TC_NewDriver_Commit AS (
),
LKP_SupState AS (
	SELECT
	SupStateId,
	StateAbbreviation
	FROM (
		SELECT 
			SupStateId,
			StateAbbreviation
		FROM SupState
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateAbbreviation ORDER BY SupStateId) = 1
),
LKP_Violation AS (
	SELECT
	ViolationId,
	in_ViolationCode,
	ViolationCode
	FROM (
		SELECT 
			ViolationId,
			in_ViolationCode,
			ViolationCode
		FROM Violation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ViolationCode ORDER BY ViolationId) = 1
),
LKP_StateViolationCategory AS (
	SELECT
	ViolationCategoryId,
	ViolationId,
	SupStateId
	FROM (
		SELECT 
			ViolationCategoryId,
			ViolationId,
			SupStateId
		FROM StateViolationCategory
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ViolationId,SupStateId ORDER BY ViolationCategoryId) = 1
),
LKP_ViolationCategory AS (
	SELECT
	ModifiedUserId,
	ModifiedDate,
	ViolationCategoryDescription,
	ViolationCategoryId
	FROM (
		SELECT 
			ModifiedUserId,
			ModifiedDate,
			ViolationCategoryDescription,
			ViolationCategoryId
		FROM ViolationCategory
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ViolationCategoryId ORDER BY ModifiedUserId) = 1
),
EXPTRANS AS (
	SELECT
	PolicyNumber,
	-- *INF*: RTRIM(PolicyNumber,' ')
	RTRIM(PolicyNumber, ' ') AS out_PolicyNumber
	FROM TC_NewDriver_Commit
),
LKP_WB_Policy AS (
	SELECT
	SessionId,
	AssignedUnderwriterFirstName,
	AssignedUnderwriterLastName,
	PolicyNumber
	FROM (
		SELECT WB_Policy.SessionId as SessionId, 
		WB_Policy.AssignedUnderwriterFirstName as AssignedUnderwriterFirstName, 
		WB_Policy.AssignedUnderwriterLastName as AssignedUnderwriterLastName, 
		WB_Policy.PolicyNumber as PolicyNumber FROM WB_Policy
		order by SessionId asc
		--where WB_Policy.PolicyNumber = in_PolicyNumber
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber ORDER BY SessionId DESC) = 1
),
EXP_DriverIncidentStage AS (
	SELECT
	TC_NewDriver_Commit.FileCreationDate,
	TC_NewDriver_Commit.IncidentResult,
	TC_NewDriver_Commit.CustomerCode,
	TC_NewDriver_Commit.IncidentStartDate,
	TC_NewDriver_Commit.IncidentDescription,
	TC_NewDriver_Commit.IncidentEndDate,
	TC_NewDriver_Commit.IncidentPoints,
	TC_NewDriver_Commit.StateCode,
	TC_NewDriver_Commit.NationalCode,
	TC_NewDriver_Commit.out_DriverStageId AS DriverStageId,
	LKP_ViolationCategory.ViolationCategoryDescription AS in_Category,
	-- *INF*: IIF(ISNULL(in_Category),'Not found',in_Category) 
	IFF(in_Category IS NULL, 'Not found', in_Category) AS out_Category,
	TC_NewDriver_Commit.LicenseState,
	TC_NewDriver_Commit.LicenseNumber,
	TC_NewDriver_Commit.LastName,
	TC_NewDriver_Commit.FirstName,
	TC_NewDriver_Commit.MiddleName,
	LKP_WB_Policy.AssignedUnderwriterFirstName,
	LKP_WB_Policy.AssignedUnderwriterLastName,
	TC_NewDriver_Commit.PolicyNumber,
	LKP_WB_Policy.SessionId
	FROM TC_NewDriver_Commit
	LEFT JOIN LKP_ViolationCategory
	ON LKP_ViolationCategory.ViolationCategoryId = LKP_StateViolationCategory.ViolationCategoryId
	LEFT JOIN LKP_WB_Policy
	ON LKP_WB_Policy.PolicyNumber = EXPTRANS.out_PolicyNumber
),
FIL_Info_Trans AS (
	SELECT
	FileCreationDate, 
	IncidentResult, 
	CustomerCode, 
	IncidentStartDate, 
	IncidentDescription, 
	IncidentEndDate, 
	IncidentPoints, 
	StateCode, 
	NationalCode, 
	DriverStageId, 
	out_Category
	FROM EXP_DriverIncidentStage
	WHERE IncidentResult != 'INFO      '
),
TGT_DriverIncidentStage AS (
	INSERT INTO DriverIncidentStage
	(DriverStageId, CreatedDate, ModifiedDate, IncidentResult, CustomerCode, IncidentStartDate, IncidentDescription, IncidentEndDate, IncidentPoints, StateCode, NationalCode, Category)
	SELECT 
	DRIVERSTAGEID, 
	FileCreationDate AS CREATEDDATE, 
	FileCreationDate AS MODIFIEDDATE, 
	INCIDENTRESULT, 
	CUSTOMERCODE, 
	INCIDENTSTARTDATE, 
	INCIDENTDESCRIPTION, 
	INCIDENTENDDATE, 
	INCIDENTPOINTS, 
	STATECODE, 
	NATIONALCODE, 
	out_Category AS CATEGORY
	FROM FIL_Info_Trans
),
FILTRANS AS (
	SELECT
	LicenseState, 
	LicenseNumber, 
	LastName, 
	FirstName, 
	MiddleName, 
	IncidentStartDate, 
	IncidentDescription, 
	IncidentEndDate, 
	IncidentPoints, 
	CustomerCode, 
	AssignedUnderwriterFirstName, 
	AssignedUnderwriterLastName, 
	out_Category AS in_Category, 
	PolicyNumber
	FROM EXP_DriverIncidentStage
	WHERE in_Category='Major'
),
EXPTRANS1 AS (
	SELECT
	LicenseState,
	LicenseNumber AS i_LicenseNumber,
	-- *INF*: REPLACESTR(1, i_LicenseNumber, SUBSTR(i_LicenseNumber, 1, LENGTH(RTRIM	(i_LicenseNumber)) - 4), LPAD('', 8,  '*'))
	REGEXP_REPLACE(i_LicenseNumber,SUBSTR(i_LicenseNumber, 1, LENGTH(RTRIM(i_LicenseNumber)) - 4),LPAD('', 8, '*')) AS v_MaskedLicenseNumber,
	v_MaskedLicenseNumber AS o_LicenseNumber,
	LastName,
	FirstName,
	MiddleName,
	IncidentStartDate,
	IncidentDescription,
	IncidentEndDate,
	IncidentPoints,
	CustomerCode,
	AssignedUnderwriterFirstName,
	AssignedUnderwriterLastName,
	PolicyNumber
	FROM FILTRANS
),
DriverIncidentFile AS (
	INSERT INTO DriverIncidentFile
	(PolNumber, DriverState, DriverLicense, LastName, FirstName, MiddleName, IncidentStart, IncicentEnd, IncidentDesc, IncidentPoints, IncidentCode, UnderwriterFirstName, UnderwriterLastName)
	SELECT 
	PolicyNumber AS POLNUMBER, 
	LicenseState AS DRIVERSTATE, 
	o_LicenseNumber AS DRIVERLICENSE, 
	LASTNAME, 
	FIRSTNAME, 
	MIDDLENAME, 
	IncidentStartDate AS INCIDENTSTART, 
	IncidentEndDate AS INCICENTEND, 
	IncidentDescription AS INCIDENTDESC, 
	INCIDENTPOINTS, 
	CustomerCode AS INCIDENTCODE, 
	AssignedUnderwriterFirstName AS UNDERWRITERFIRSTNAME, 
	AssignedUnderwriterLastName AS UNDERWRITERLASTNAME
	FROM EXPTRANS1
),
AGG_LicenseInfo AS (
	SELECT
	out_DriverStageId AS DriverStageId,
	FileCreationDate,
	FileIDCode,
	SenderName,
	VersionNumber,
	LicenseState,
	LicenseNumber,
	Quoteback,
	ReportType,
	LastName,
	FirstName,
	MiddleName,
	StreetAddress,
	City,
	State,
	ZipCode,
	BirthDate,
	Weight,
	Height,
	EyeColor,
	Alias,
	PolicyNumber,
	PolicyExpirationDate,
	PreviousLicense,
	Gender,
	AccountNumber
	FROM TC_NewDriver_Commit
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FileCreationDate, LicenseState, LicenseNumber ORDER BY NULL) = 1
),
EXP_DriverStage AS (
	SELECT
	DriverStageId,
	'Request' AS out_ProcessStatus,
	FileCreationDate AS CreatedDate,
	FileCreationDate,
	FileIDCode,
	SenderName,
	LicenseState,
	LicenseNumber,
	LastName,
	FirstName,
	MiddleName,
	StreetAddress,
	City,
	State AS StateCode,
	ZipCode,
	BirthDate,
	Gender,
	Weight AS BodyWeight,
	Height,
	EyeColor,
	PolicyNumber,
	PolicyExpirationDate,
	-- *INF*: SUBSTR(QuoteBack,1,12)
	SUBSTR(QuoteBack, 1, 12) AS QuoteBackPolicyNumber,
	-- *INF*: SUBSTR(QuoteBack,14,7)
	SUBSTR(QuoteBack, 14, 7) AS QuoteBackAgencyNumber,
	-- *INF*: SUBSTR(QuoteBack,22,3)
	SUBSTR(QuoteBack, 22, 3) AS QuoteBackLineOfBusiness,
	-- *INF*: SUBSTR(QuoteBack,26,2)
	SUBSTR(QuoteBack, 26, 2) AS QuoteBackDriverId,
	-- *INF*: SUBSTR(QuoteBack,29,2)
	SUBSTR(QuoteBack, 29, 2) AS QuoteBackState,
	-- *INF*: SUBSTR(QuoteBack,32,3)
	SUBSTR(QuoteBack, 32, 3) AS QuoteBackUnderwriterNumber,
	Quoteback AS QuoteBack,
	AccountNumber,
	1 AS SupProcessStatusId,
	PreviousLicense
	FROM AGG_LicenseInfo
),
LKP_SupProcessStatus AS (
	SELECT
	SupProcessStatusId,
	ProcessStatus
	FROM (
		SELECT 
			SupProcessStatusId,
			ProcessStatus
		FROM SupProcessStatus
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProcessStatus ORDER BY SupProcessStatusId) = 1
),
TGT_DriverStage AS (
	INSERT INTO DriverStage
	(DriverStageId, CreatedDate, ModifiedDate, FileCreationDate, FileIDCode, SenderName, LicenseState, LicenseNumber, LastName, FirstName, MiddleName, StreetAddress, City, StateCode, ZipCode, BirthDate, Gender, BodyWeight, EyeColor, PolicyNumber, PolicyExpirationDate, QuoteBackPolicyNumber, QuoteBackAgencyNumber, QuoteBackLineOfBusiness, QuoteBackDriverId, QuoteBackState, QuoteBackUnderwriterNumber, QuoteBack, InsuranceIndicator, SupProcessStatusId, Height, PreviousLicense)
	SELECT 
	EXP_DriverStage.DRIVERSTAGEID, 
	EXP_DriverStage.CREATEDDATE, 
	EXP_DriverStage.MODIFIEDDATE, 
	EXP_DriverStage.FILECREATIONDATE, 
	EXP_DriverStage.FILEIDCODE, 
	EXP_DriverStage.SENDERNAME, 
	EXP_DriverStage.LICENSESTATE, 
	EXP_DriverStage.LICENSENUMBER, 
	EXP_DriverStage.LASTNAME, 
	EXP_DriverStage.FIRSTNAME, 
	EXP_DriverStage.MIDDLENAME, 
	EXP_DriverStage.STREETADDRESS, 
	EXP_DriverStage.CITY, 
	EXP_DriverStage.STATECODE, 
	EXP_DriverStage.ZIPCODE, 
	EXP_DriverStage.BIRTHDATE, 
	EXP_DriverStage.GENDER, 
	EXP_DriverStage.BODYWEIGHT, 
	EXP_DriverStage.EYECOLOR, 
	EXP_DriverStage.POLICYNUMBER, 
	EXP_DriverStage.POLICYEXPIRATIONDATE, 
	EXP_DriverStage.QUOTEBACKPOLICYNUMBER, 
	EXP_DriverStage.QUOTEBACKAGENCYNUMBER, 
	EXP_DriverStage.QUOTEBACKLINEOFBUSINESS, 
	EXP_DriverStage.QUOTEBACKDRIVERID, 
	EXP_DriverStage.QUOTEBACKSTATE, 
	EXP_DriverStage.QUOTEBACKUNDERWRITERNUMBER, 
	EXP_DriverStage.QUOTEBACK, 
	EXP_DriverStage.INSURANCEINDICATOR, 
	LKP_SupProcessStatus.SUPPROCESSSTATUSID, 
	EXP_DriverStage.HEIGHT, 
	EXP_DriverStage.PREVIOUSLICENSE
	FROM EXP_DriverStage
),