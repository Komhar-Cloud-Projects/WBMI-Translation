WITH
SQ_ExploreEARSDriverRecord AS (
	SELECT
		ExploreEARSDriverRecordId,
		AuditId,
		CreatedDate,
		LicenseState,
		LicenseNumber,
		LastName,
		FirstName,
		MiddleName,
		Birthdate,
		ZipCode,
		StreetAddress,
		Gender,
		PolicyNumber,
		PolicyExpirationDate,
		QuotebackPolicyNumber,
		QuotebackAgencyNumber,
		QuotebackDriverLicense,
		QuotebackState,
		InsuranceIndicator,
		ProductFlags,
		AccountNumber
	FROM ExploreEARSDriverRecord
),
EXP_Values AS (
	SELECT
	ExploreEARSDriverRecordId,
	AuditId,
	CreatedDate,
	LicenseState,
	LicenseNumber,
	LastName,
	FirstName,
	MiddleName,
	Birthdate,
	ZipCode,
	StreetAddress,
	Gender,
	PolicyNumber,
	PolicyExpirationDate,
	QuotebackPolicyNumber,
	QuotebackAgencyNumber,
	QuotebackDriverLicense,
	QuotebackState,
	InsuranceIndicator,
	ProductFlags,
	AccountNumber,
	-- *INF*: LTRIM(RTRIM(LicenseNumber))
	LTRIM(RTRIM(LicenseNumber)) AS v_LicenseNumber,
	-- *INF*: LTRIM(RTRIM(QuotebackDriverLicense))
	LTRIM(RTRIM(QuotebackDriverLicense)) AS v_QuotebackDriverLicense,
	-- *INF*: IIF(LENGTH(v_LicenseNumber) < 8, UPPER(LPAD(v_LicenseNumber, 8, '@')), UPPER(v_LicenseNumber))
	IFF(
	    LENGTH(v_LicenseNumber) < 8, UPPER(LPAD(v_LicenseNumber, 8, '@')), UPPER(v_LicenseNumber)
	) AS lkp_LicenseNumber,
	-- *INF*: IIF(LENGTH(v_QuotebackDriverLicense) < 8, UPPER(LPAD(v_QuotebackDriverLicense, 8, '@')), UPPER(v_QuotebackDriverLicense))
	IFF(
	    LENGTH(v_QuotebackDriverLicense) < 8, UPPER(LPAD(v_QuotebackDriverLicense, 8, '@')),
	    UPPER(v_QuotebackDriverLicense)
	) AS lkp_QuotebackDriverLicense
	FROM SQ_ExploreEARSDriverRecord
),
LKP_LicenseNumber AS (
),
LKP_QuotebackDriverLicense AS (
),
EXP_UpdatedLicenseNumbers AS (
	SELECT
	EXP_Values.ExploreEARSDriverRecordId,
	LKP_LicenseNumber.o_LicenseNumber AS i_LicenseNumber,
	LKP_QuotebackDriverLicense.o_QuotebackDriverLicense AS i_QuotebackDriverLicense,
	-- *INF*: LTRIM(LTRIM(RTRIM(i_LicenseNumber)), '@')
	LTRIM(LTRIM(RTRIM(i_LicenseNumber)), '@') AS o_LicenseNumber,
	-- *INF*: LTRIM(LTRIM(RTRIM(i_QuotebackDriverLicense)), '@')
	LTRIM(LTRIM(RTRIM(i_QuotebackDriverLicense)), '@') AS o_QuotebackDriverLicense
	FROM EXP_Values
	LEFT JOIN LKP_LicenseNumber
	ON LKP_LicenseNumber.lkp_LicenseNumber = EXP_Values.lkp_LicenseNumber
	LEFT JOIN LKP_QuotebackDriverLicense
	ON LKP_QuotebackDriverLicense.lkp_QuotebackDriverLicense = EXP_Values.lkp_QuotebackDriverLicense
),
UPDTRANS AS (
	SELECT
	ExploreEARSDriverRecordId, 
	o_LicenseNumber AS LicenseNumber, 
	o_QuotebackDriverLicense AS QuotebackDriverLicense
	FROM EXP_UpdatedLicenseNumbers
),
ExploreEARSDriverRecord1 AS (
	MERGE INTO ExploreEARSDriverRecord AS T
	USING UPDTRANS AS S
	ON T.ExploreEARSDriverRecordId = S.ExploreEARSDriverRecordId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.LicenseNumber = S.LicenseNumber, T.QuotebackDriverLicense = S.QuotebackDriverLicense
),