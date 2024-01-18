WITH
SEQ_DriverStageID AS (
	CREATE SEQUENCE SEQ_DriverStageID
	START = 0
	INCREMENT = 1;
),
earsreject_DriverRejectTable AS (
),
EXP_EARS_Reject AS (
	SELECT
	SEQ_DriverStageID.NEXTVAL,
	'Rejected' AS out_ProcessStatus,
	NEXTVAL AS out_DriverStageID,
	FileCreationDate,
	-- *INF*: TO_DATE(FileCreationDate,'YYYYMMDD')
	TO_TIMESTAMP(FileCreationDate, 'YYYYMMDD') AS out_FileCreationDate,
	FileIDCode,
	SenderName,
	SenderNumber,
	ReservedDriverRejectHeader1,
	LicenseState,
	LicenseNumber,
	LastName,
	FirstName,
	MiddleName,
	BirthDate,
	-- *INF*: TO_DATE(BirthDate,'YYYYMMDD')
	TO_TIMESTAMP(BirthDate, 'YYYYMMDD') AS out_BirthDate,
	ZipCode,
	StreetAddress,
	Gender,
	ReservedDriverReject1,
	PolicyNumber,
	PolicyExpirationDate,
	-- *INF*: TO_DATE(PolicyExpirationDate,'YYYYMMDD')
	TO_TIMESTAMP(PolicyExpirationDate, 'YYYYMMDD') AS out_PolicyExpirationDate,
	ReservedDriverReject2,
	Quoteback,
	-- *INF*: SUBSTR(Quoteback,1,12)
	SUBSTR(Quoteback, 1, 12) AS out_QuotebackPolicyNumber,
	-- *INF*: SUBSTR(Quoteback,14,7)
	SUBSTR(Quoteback, 14, 7) AS out_QuotebackAgency,
	-- *INF*: SUBSTR(Quoteback,22,3)
	SUBSTR(Quoteback, 22, 3) AS out_QuotebackLOB,
	-- *INF*: SUBSTR(Quoteback,26,2)
	SUBSTR(Quoteback, 26, 2) AS out_QuotebackDriverID,
	-- *INF*: SUBSTR(Quoteback,29,2)
	SUBSTR(Quoteback, 29, 2) AS out_QuotebackState,
	-- *INF*: SUBSTR(Quoteback,32,3)
	SUBSTR(Quoteback, 32, 3) AS out_QuotebackUnderwriter,
	InsuranceIndicatior,
	ReservedDriverReject3,
	AccountNumber,
	ReservedDriverReject4,
	RejectSource,
	RejectReason,
	RejectCount,
	-- *INF*: DECODE(RejectReason,'002','002-DOB mismatch for that driver license record','003','003-Missing Required Field','004','004-Invalid Drivers License Format','005','005-Driver Not Found','010','010-State Driver Not Found','Invalid Reject Reason: '  || RejectReason)
	-- 
	DECODE(
	    RejectReason,
	    '002', '002-DOB mismatch for that driver license record',
	    '003', '003-Missing Required Field',
	    '004', '004-Invalid Drivers License Format',
	    '005', '005-Driver Not Found',
	    '010', '010-State Driver Not Found',
	    'Invalid Reject Reason: ' || RejectReason
	) AS out_RejectDescription
	FROM earsreject_DriverRejectTable
),
LKP_DriverStage AS (
	SELECT
	FileCreationDate,
	LicenseState,
	LicenseNumber,
	LastName,
	FirstName,
	RejectReason,
	In_FileCreationDate,
	In_LicenseState,
	In_License_Number,
	In_FirstName,
	In_LastName,
	In_PolicyNumber,
	PolicyNumber
	FROM (
		SELECT 
			FileCreationDate,
			LicenseState,
			LicenseNumber,
			LastName,
			FirstName,
			RejectReason,
			In_FileCreationDate,
			In_LicenseState,
			In_License_Number,
			In_FirstName,
			In_LastName,
			In_PolicyNumber,
			PolicyNumber
		FROM DriverStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FileCreationDate,LicenseState,LicenseNumber,LastName,FirstName,PolicyNumber ORDER BY FileCreationDate) = 1
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
FILTRANS AS (
	SELECT
	LKP_DriverStage.RejectReason AS lkp_RejectReason, 
	EXP_EARS_Reject.out_ProcessStatus, 
	EXP_EARS_Reject.out_DriverStageID, 
	EXP_EARS_Reject.RecordID, 
	EXP_EARS_Reject.out_FileCreationDate AS FileCreationDate, 
	EXP_EARS_Reject.FileIDCode, 
	EXP_EARS_Reject.SenderName, 
	EXP_EARS_Reject.SenderNumber, 
	EXP_EARS_Reject.ReservedDriverRejectHeader1, 
	EXP_EARS_Reject.LicenseState, 
	EXP_EARS_Reject.LicenseNumber, 
	EXP_EARS_Reject.LastName, 
	EXP_EARS_Reject.FirstName, 
	EXP_EARS_Reject.MiddleName, 
	EXP_EARS_Reject.out_BirthDate AS BirthDate, 
	EXP_EARS_Reject.ZipCode, 
	EXP_EARS_Reject.StreetAddress, 
	EXP_EARS_Reject.Gender, 
	EXP_EARS_Reject.ReservedDriverReject1, 
	EXP_EARS_Reject.PolicyNumber, 
	EXP_EARS_Reject.out_PolicyExpirationDate AS PolicyExpirationDate, 
	EXP_EARS_Reject.ReservedDriverReject2, 
	EXP_EARS_Reject.Quoteback, 
	EXP_EARS_Reject.out_QuotebackPolicyNumber, 
	EXP_EARS_Reject.out_QuotebackAgency, 
	EXP_EARS_Reject.out_QuotebackLOB, 
	EXP_EARS_Reject.out_QuotebackDriverID, 
	EXP_EARS_Reject.out_QuotebackState, 
	EXP_EARS_Reject.out_QuotebackUnderwriter, 
	EXP_EARS_Reject.InsuranceIndicatior, 
	EXP_EARS_Reject.ReservedDriverReject3, 
	EXP_EARS_Reject.AccountNumber, 
	EXP_EARS_Reject.ReservedDriverReject4, 
	EXP_EARS_Reject.RejectSource, 
	EXP_EARS_Reject.RejectReason, 
	EXP_EARS_Reject.RejectCount, 
	EXP_EARS_Reject.out_RejectDescription, 
	LKP_SupProcessStatus.SupProcessStatusId AS out_SupProcessStatusId
	FROM EXP_EARS_Reject
	LEFT JOIN LKP_DriverStage
	ON LKP_DriverStage.FileCreationDate = EXP_EARS_Reject.out_FileCreationDate AND LKP_DriverStage.LicenseState = EXP_EARS_Reject.LicenseState AND LKP_DriverStage.LicenseNumber = EXP_EARS_Reject.LicenseNumber AND LKP_DriverStage.LastName = EXP_EARS_Reject.LastName AND LKP_DriverStage.FirstName = EXP_EARS_Reject.FirstName AND LKP_DriverStage.PolicyNumber = EXP_EARS_Reject.PolicyNumber
	LEFT JOIN LKP_SupProcessStatus
	ON LKP_SupProcessStatus.ProcessStatus = EXP_EARS_Reject.out_ProcessStatus
	WHERE ISNULL(lkp_RejectReason)
),
TGT_DriverStage AS (
	INSERT INTO DriverStage
	(DriverStageId, CreatedDate, FileCreationDate, FileIDCode, SenderName, LicenseState, LicenseNumber, LastName, FirstName, MiddleName, StreetAddress, ZipCode, BirthDate, Gender, PolicyNumber, PolicyExpirationDate, QuoteBackPolicyNumber, QuoteBackAgencyNumber, QuoteBackLineOfBusiness, QuoteBackDriverId, QuoteBackState, QuoteBackUnderwriterNumber, QuoteBack, InsuranceIndicator, AccountNumber, RejectSource, RejectReason, RejectCount, SupProcessStatusId, ErrorDescription)
	SELECT 
	out_DriverStageID AS DRIVERSTAGEID, 
	FileCreationDate AS CREATEDDATE, 
	FILECREATIONDATE, 
	FILEIDCODE, 
	SENDERNAME, 
	LICENSESTATE, 
	LICENSENUMBER, 
	LASTNAME, 
	FIRSTNAME, 
	MIDDLENAME, 
	STREETADDRESS, 
	ZIPCODE, 
	BIRTHDATE, 
	GENDER, 
	POLICYNUMBER, 
	POLICYEXPIRATIONDATE, 
	out_QuotebackPolicyNumber AS QUOTEBACKPOLICYNUMBER, 
	out_QuotebackAgency AS QUOTEBACKAGENCYNUMBER, 
	out_QuotebackLOB AS QUOTEBACKLINEOFBUSINESS, 
	out_QuotebackDriverID AS QUOTEBACKDRIVERID, 
	out_QuotebackState AS QUOTEBACKSTATE, 
	out_QuotebackUnderwriter AS QUOTEBACKUNDERWRITERNUMBER, 
	Quoteback AS QUOTEBACK, 
	InsuranceIndicatior AS INSURANCEINDICATOR, 
	ACCOUNTNUMBER, 
	REJECTSOURCE, 
	REJECTREASON, 
	REJECTCOUNT, 
	out_SupProcessStatusId AS SUPPROCESSSTATUSID, 
	out_RejectDescription AS ERRORDESCRIPTION
	FROM FILTRANS
),
EXP_MaskedLicense AS (
	SELECT
	PolicyNumber,
	LicenseState,
	LicenseNumber AS i_LicenseNumber,
	-- *INF*: REPLACESTR(1, i_LicenseNumber, SUBSTR(i_LicenseNumber, 1, LENGTH(RTRIM(i_LicenseNumber)) - 4),  LPAD('', 8,  '*'))
	REGEXP_REPLACE(i_LicenseNumber,SUBSTR(i_LicenseNumber, 1, LENGTH(RTRIM(i_LicenseNumber)) - 4),LPAD('', 8, '*')) AS v_LicenseNumber,
	v_LicenseNumber AS o_LicenseNumber,
	LastName,
	FirstName,
	MiddleName,
	out_RejectDescription
	FROM FILTRANS
),
DriverRejectFile AS (
	INSERT INTO DriverRejectFile
	(PolicyNumber, DriverState, DriverLicense, DriverLast, DriverFirst, DriverMiddle, RejectReason)
	SELECT 
	POLICYNUMBER, 
	LicenseState AS DRIVERSTATE, 
	o_LicenseNumber AS DRIVERLICENSE, 
	LastName AS DRIVERLAST, 
	FirstName AS DRIVERFIRST, 
	MiddleName AS DRIVERMIDDLE, 
	out_RejectDescription AS REJECTREASON
	FROM EXP_MaskedLicense
),