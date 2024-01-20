WITH
SQ_DataCompromisePolicyRecord AS (
	SELECT
		DataCompromisePolicyRecordId,
		AuditID,
		SourceSystemID,
		CreatedDate,
		ModifiedDate,
		RunDate,
		Company,
		ProductCode,
		ContractNumber,
		PolicyNumber,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		NameOfInsured,
		MailingAddressStreetName,
		MailingAddressCity,
		MailingAddressState,
		MailingAddressZipCode,
		TotalPackageGrossPremium,
		TotalPropertyGrossPremium,
		GrossPremium,
		NetPremium,
		FirstPartyLimit,
		DeductibleAmount,
		OccupancyCode,
		PolicyTotalInsuredValue,
		PreviousPolicyNumber,
		AgencyCode,
		BranchCode,
		ThirdPartyIndicator
	FROM DataCompromisePolicyRecord
	WHERE convert(date,DataCompromisePolicyRecord.PolicyEffectiveDate)<=@{pipeline().parameters.RUNDATE}
	AND 
	convert(date,DataCompromisePolicyRecord.PolicyExpirationDate)>=@{pipeline().parameters.RUNDATE}
),
EXP_PolicyRecords AS (
	SELECT
	Company,
	ProductCode,
	ContractNumber,
	PolicyNumber,
	PolicyEffectiveDate,
	-- *INF*: TO_CHAR(PolicyEffectiveDate, 'YYYYMMDD')
	TO_CHAR(PolicyEffectiveDate, 'YYYYMMDD') AS PolicyEffectiveDateYYYYMMDD,
	PolicyExpirationDate,
	-- *INF*: TO_CHAR(PolicyExpirationDate, 'YYYYMMDD')
	TO_CHAR(PolicyExpirationDate, 'YYYYMMDD') AS PolicyExpirationDateYYYYMMDD,
	NameOfInsured,
	MailingAddressStreetName,
	MailingAddressCity,
	MailingAddressState,
	MailingAddressZipCode,
	TotalPackageGrossPremium,
	TotalPropertyGrossPremium,
	GrossPremium,
	NetPremium,
	FirstPartyLimit,
	DeductibleAmount,
	OccupancyCode,
	PolicyTotalInsuredValue,
	PreviousPolicyNumber,
	AgencyCode,
	BranchCode,
	ThirdPartyIndicator
	FROM SQ_DataCompromisePolicyRecord
),
DataCompromisePolicyRecordFile AS (
	INSERT INTO DataCompromisePolicyRecordFile
	(Company, ProductCode, ContractNumber, PolicyNumber, CoverageEffectiveDate, CoverageExpirationDate, NameOfInsured, MailingAddressStreetName, MailingAddressCity, MailingAddressState, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, GrossPremium, NetPremium, FirstPartyLimit, DeductibleAmount, OccupancyCode, PolicyTotalInsuredValue, PreviousPolicyNumber, AgencyCode, BranchCode, ThirdPartyIndicator)
	SELECT 
	COMPANY, 
	PRODUCTCODE, 
	CONTRACTNUMBER, 
	POLICYNUMBER, 
	PolicyEffectiveDateYYYYMMDD AS COVERAGEEFFECTIVEDATE, 
	PolicyExpirationDateYYYYMMDD AS COVERAGEEXPIRATIONDATE, 
	NAMEOFINSURED, 
	MAILINGADDRESSSTREETNAME, 
	MAILINGADDRESSCITY, 
	MAILINGADDRESSSTATE, 
	MAILINGADDRESSZIPCODE, 
	TOTALPACKAGEGROSSPREMIUM, 
	TOTALPROPERTYGROSSPREMIUM, 
	GROSSPREMIUM, 
	NETPREMIUM, 
	FIRSTPARTYLIMIT, 
	DEDUCTIBLEAMOUNT, 
	OCCUPANCYCODE, 
	POLICYTOTALINSUREDVALUE, 
	PREVIOUSPOLICYNUMBER, 
	AGENCYCODE, 
	BRANCHCODE, 
	THIRDPARTYINDICATOR
	FROM EXP_PolicyRecords
),
SQ_DataCompromisePolicyRecord1 AS (
	SELECT
		DataCompromisePolicyRecordId,
		AuditID,
		SourceSystemID,
		CreatedDate,
		ModifiedDate,
		RunDate,
		Company,
		ProductCode,
		ContractNumber,
		PolicyNumber,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		NameOfInsured,
		MailingAddressStreetName,
		MailingAddressCity,
		MailingAddressState,
		MailingAddressZipCode,
		TotalPackageGrossPremium,
		TotalPropertyGrossPremium,
		GrossPremium,
		NetPremium,
		FirstPartyLimit,
		DeductibleAmount,
		OccupancyCode,
		PolicyTotalInsuredValue,
		PreviousPolicyNumber,
		AgencyCode,
		BranchCode,
		ThirdPartyIndicator
	FROM DataCompromisePolicyRecord1
	WHERE convert(date,DataCompromisePolicyRecord.PolicyEffectiveDate)<=@{pipeline().parameters.RUNDATE}
	AND 
	convert(date,DataCompromisePolicyRecord.PolicyExpirationDate)>=@{pipeline().parameters.RUNDATE}
),
AGGTRANS AS (
	SELECT
	RunDate,
	Company,
	ProductCode,
	'CONTROL' AS CONTROL,
	-- *INF*: COUNT(1)
	COUNT(1) AS RecordCount,
	-- *INF*: TO_CHAR(RunDate, 'YYYYMMDD')
	TO_CHAR(RunDate, 'YYYYMMDD') AS RunDateYYYYMMDD,
	'02.01' AS HardCodeString,
	'' AS HardCodeString2
	FROM SQ_DataCompromisePolicyRecord1
	GROUP BY 
),
DataCompromisePolicyRecordControlFile AS (
	INSERT INTO DataCompromisePolicyRecordControlFile
	(Company, ProductCode, Control, RecordCount, RunDateYYYYMMDD, HardCodeString, HardCodeString2)
	SELECT 
	COMPANY, 
	PRODUCTCODE, 
	CONTROL AS CONTROL, 
	RECORDCOUNT, 
	RUNDATEYYYYMMDD, 
	HARDCODESTRING, 
	HARDCODESTRING2
	FROM AGGTRANS
),
SQ_DataCompromisePaymentBordereauxRecord AS (
	SELECT
		DataCompromisePaymentBordereauxRecordId,
		AuditID,
		SourceSystemID,
		CreatedDate,
		ModifiedDate,
		PremiumMasterCalculationID,
		RunDate,
		ProductCode,
		Company,
		PolicyNumber,
		TransactionCode,
		TransactionEffectiveDate,
		CoverageEffectiveDate,
		CoverageExpirationDate,
		CoverageGrossPremium,
		CoverageNetPremium,
		PreviousPolicyNumber,
		ProgramID,
		NameOfInsured,
		ContractNumber
	FROM DataCompromisePaymentBordereauxRecord
	WHERE convert(date,DataCompromisePaymentBordereauxRecord.RunDate)=@{pipeline().parameters.RUNDATE}
),
EXPTRANS AS (
	SELECT
	ProductCode,
	Company,
	PolicyNumber,
	TransactionCode,
	TransactionEffectiveDate,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYYYMMDD')
	-- 
	TO_CHAR(TransactionEffectiveDate, 'YYYYMMDD') AS TransactionEffectiveDateYYYYMMDD,
	-- *INF*: TO_CHAR(CoverageEffectiveDate,'YYYYMMDD')
	TO_CHAR(CoverageEffectiveDate, 'YYYYMMDD') AS CoverageEffectiveDateYYYYMMDD,
	-- *INF*: TO_CHAR(CoverageExpirationDate,'YYYYMMDD')
	TO_CHAR(CoverageExpirationDate, 'YYYYMMDD') AS CoverageExpirationDateYYYYMMDD,
	CoverageGrossPremium,
	CoverageNetPremium,
	PreviousPolicyNumber,
	ProgramID,
	NameOfInsured,
	ContractNumber
	FROM SQ_DataCompromisePaymentBordereauxRecord
),
DataCompromisePaymentBordereauxRecordFile AS (
	INSERT INTO DataCompromisePaymentBordereauxRecordFile
	(ProductCode, Company, PolicyNumber, TransactionCode, TransactionEffectiveDate, CoverageEffectiveDate, CoverageExpirationDate, CoverageGrossPremium, CoverageNetPremium, PreviousPolicyNumber, ProgramID, NameOfInsured, ContractNumber)
	SELECT 
	PRODUCTCODE, 
	COMPANY, 
	POLICYNUMBER, 
	TRANSACTIONCODE, 
	TransactionEffectiveDateYYYYMMDD AS TRANSACTIONEFFECTIVEDATE, 
	CoverageEffectiveDateYYYYMMDD AS COVERAGEEFFECTIVEDATE, 
	CoverageExpirationDateYYYYMMDD AS COVERAGEEXPIRATIONDATE, 
	COVERAGEGROSSPREMIUM, 
	COVERAGENETPREMIUM, 
	PREVIOUSPOLICYNUMBER, 
	PROGRAMID, 
	NAMEOFINSURED, 
	CONTRACTNUMBER
	FROM EXPTRANS
),
SQ_DataCompromisePaymentBordereauxControlRecord AS (
	SELECT
		DataCompromisePaymentBordereauxRecordId,
		AuditID,
		SourceSystemID,
		CreatedDate,
		ModifiedDate,
		PremiumMasterCalculationID,
		RunDate,
		ProductCode,
		Company,
		PolicyNumber,
		TransactionCode,
		TransactionEffectiveDate,
		CoverageEffectiveDate,
		CoverageExpirationDate,
		CoverageGrossPremium,
		CoverageNetPremium,
		PreviousPolicyNumber,
		ProgramID,
		NameOfInsured,
		ContractNumber
	FROM DataCompromisePaymentBordereauxRecord1
	WHERE convert(date,DataCompromisePaymentBordereauxRecord.RunDate)=@{pipeline().parameters.RUNDATE}
),
AGG_PaymentBordereauxRecord AS (
	SELECT
	'CONTROL' AS Control,
	RunDate,
	Company,
	CoverageGrossPremium,
	CoverageNetPremium,
	-- *INF*: Count(1)
	Count(1) AS RecordCount,
	-- *INF*: Sum(CoverageGrossPremium)
	Sum(CoverageGrossPremium) AS SumCoverageGrossPremium,
	-- *INF*: Sum(CoverageNetPremium)
	Sum(CoverageNetPremium) AS SumCoverageNetPremium,
	-- *INF*: to_char(RunDate, 'YYYYMM')
	to_char(RunDate, 'YYYYMM') AS RunDateYYYYMM,
	'08.02' AS HardcodeString,
	'' AS HardCodeString2
	FROM SQ_DataCompromisePaymentBordereauxControlRecord
	GROUP BY Company, RunDateYYYYMM
),
DataCompromisePaymentBordereauxRecordControlFile AS (
	INSERT INTO DataCompromisePaymentBordereauxRecordControlFile
	(Control, Company, RecordCount, SumCoverageGrossPremium, SumCoverageNetPremium, RunDateYYYYMMDD, HardCodeString, HardCodeString2)
	SELECT 
	CONTROL, 
	COMPANY, 
	RECORDCOUNT, 
	SUMCOVERAGEGROSSPREMIUM, 
	SUMCOVERAGENETPREMIUM, 
	RunDateYYYYMM AS RUNDATEYYYYMMDD, 
	HardcodeString AS HARDCODESTRING, 
	HARDCODESTRING2
	FROM AGG_PaymentBordereauxRecord
),