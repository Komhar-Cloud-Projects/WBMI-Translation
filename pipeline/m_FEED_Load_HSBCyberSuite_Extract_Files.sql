WITH
SQ_HSBCyberPolicyExtract AS (
	SELECT
		HSBCyberSuitePolicyExtractId AS HSBCyberPolicyExtractId,
		AuditId AS AuditID,
		CreatedDate,
		ModifiedDate,
		RunDate,
		Company,
		ProductCode,
		ContractNumber,
		PolicyNumber,
		CBSCoverageEffectiveDate AS PolicyEffectiveDate,
		CBSCoverageExpirationDate AS PolicyExpirationDate,
		NameOfInsured,
		MailingAddressStreetName,
		MailingAddressCity,
		MailingAddressState,
		MailingAddressZipCode,
		TotalPackageGrossPremium,
		TotalPropertyGrossPremium,
		CBSGrossPremium AS GrossPremium,
		CBSNetPremium AS NetPremium,
		LimitAmount AS FirstPartyLimit,
		LimitType,
		DeductibleAmount AS FirstPartyDeductible,
		OccupancyCode,
		PolicyTotalInsuredValue,
		PreviousPolicyNumber,
		AgentCode AS AgencyCode,
		BranchCode,
		CBSPricingTier
	FROM HSBCyberSuitePolicyExtract_Source
	WHERE convert(date,CBSCoverageEffectiveDate)<=@{pipeline().parameters.RUNDATE}
	AND 
	convert(date,CBSCoverageExpirationDate)>=@{pipeline().parameters.RUNDATE}
),
EXP_PolicyExtract AS (
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
	FirstPartyLimit AS CyberOneFirstPartyLimit,
	LimitType,
	FirstPartyDeductible,
	OccupancyCode,
	PolicyTotalInsuredValue,
	PreviousPolicyNumber,
	AgencyCode,
	BranchCode,
	CBSPricingTier
	FROM SQ_HSBCyberPolicyExtract
),
HSBCyberSuitePolicyRecordFile AS (
	INSERT INTO HSBCyberSuitePolicyRecordFile
	(Company, ProductCode, ContractNumber, PolicyNumber, CBSCoverageEffectiveDate, CBSCoverageExpirationDate, NameOfInsured, MailingAddressStreetName, MailingAddressCity, MailingAddressState, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, CBSGrossPremium, CBSNetPremium, LimitAmount, LimitType, DeductibleAmount, OccupancyCode, PolicyTotalInsuredValue, PreviousPolicyNumber, AgencyCode, BranchCode, WebSite, EmailAddress, CBSPricingTier)
	SELECT 
	COMPANY, 
	PRODUCTCODE, 
	CONTRACTNUMBER, 
	POLICYNUMBER, 
	PolicyEffectiveDateYYYYMMDD AS CBSCOVERAGEEFFECTIVEDATE, 
	PolicyExpirationDateYYYYMMDD AS CBSCOVERAGEEXPIRATIONDATE, 
	NAMEOFINSURED, 
	MAILINGADDRESSSTREETNAME, 
	MAILINGADDRESSCITY, 
	MAILINGADDRESSSTATE, 
	MAILINGADDRESSZIPCODE, 
	TOTALPACKAGEGROSSPREMIUM, 
	TOTALPROPERTYGROSSPREMIUM, 
	GrossPremium AS CBSGROSSPREMIUM, 
	NetPremium AS CBSNETPREMIUM, 
	CyberOneFirstPartyLimit AS LIMITAMOUNT, 
	LIMITTYPE, 
	FirstPartyDeductible AS DEDUCTIBLEAMOUNT, 
	OCCUPANCYCODE, 
	POLICYTOTALINSUREDVALUE, 
	PREVIOUSPOLICYNUMBER, 
	AGENCYCODE, 
	BRANCHCODE, 
	WEBSITE, 
	EMAILADDRESS, 
	CBSPRICINGTIER
	FROM EXP_PolicyExtract
),
SQ_HSBCyberPaymentBordereauExtract AS (
	SELECT
		HSBCyberSuiteBordereauExtractId AS HSBCyberPaymentBordereauExtractId,
		AuditId AS AuditID,
		CreatedDate,
		ModifiedDate,
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
	FROM HSBCyberSuiteBordereauExtract1
	WHERE AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
EXP_BordereauExtract AS (
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
	FROM SQ_HSBCyberPaymentBordereauExtract
),
HSBCyberSuiteBordereauxRecordFile AS (
	INSERT INTO HSBCyberSuiteBordereauxRecordFile
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
	FROM EXP_BordereauExtract
),
SQ_HSBCyberPolicyExtractCntl AS (
	SELECT
		HSBCyberSuitePolicyExtractId AS HSBCyberPolicyExtractId,
		AuditId AS AuditID,
		CreatedDate,
		ModifiedDate,
		RunDate,
		Company,
		ProductCode,
		ContractNumber,
		PolicyNumber,
		CBSCoverageEffectiveDate AS PolicyEffectiveDate,
		CBSCoverageExpirationDate AS PolicyExpirationDate,
		NameOfInsured,
		MailingAddressStreetName,
		MailingAddressCity,
		MailingAddressState,
		MailingAddressZipCode,
		TotalPackageGrossPremium,
		TotalPropertyGrossPremium,
		CBSGrossPremium AS GrossPremium,
		CBSNetPremium AS NetPremium,
		LimitAmount AS FirstPartyLimit,
		DeductibleAmount AS FirstPartyDeductible,
		OccupancyCode,
		PolicyTotalInsuredValue,
		PreviousPolicyNumber,
		AgentCode AS AgencyCode,
		BranchCode,
		LimitType AS FirstPartyCoverage,
		WebSite,
		EmailAddress,
		CBSPricingTier
	FROM HSBCyberSuitePolicyExtract
	WHERE convert(date,CBSCoverageEffectiveDate)<=@{pipeline().parameters.RUNDATE}
	AND 
	convert(date,CBSCoverageExpirationDate)>=@{pipeline().parameters.RUNDATE}
),
AGG_Policy_Extract AS (
	SELECT
	RunDate,
	Company,
	ProductCode AS ProductName,
	'CONTROL' AS Record_Type,
	-- *INF*: COUNT(1)
	COUNT(1) AS Number_of_Records,
	-- *INF*: TO_CHAR(RunDate, 'YYYYMMDD')
	TO_CHAR(RunDate, 'YYYYMMDD') AS Inforce_Date,
	'01.01' AS Version_Number,
	'' AS Filler
	FROM SQ_HSBCyberPolicyExtractCntl
	GROUP BY 
),
HSBCyberSuitePolicyRecordControlFile AS (
	INSERT INTO HSBCyberSuitePolicyRecordControlFile
	(Company, ProductCode, Control, RecordCount, RunDateYYYYMMDD, HardCodeString, HardCodeString2)
	SELECT 
	COMPANY, 
	ProductName AS PRODUCTCODE, 
	Record_Type AS CONTROL, 
	Number_of_Records AS RECORDCOUNT, 
	Inforce_Date AS RUNDATEYYYYMMDD, 
	Version_Number AS HARDCODESTRING, 
	Filler AS HARDCODESTRING2
	FROM AGG_Policy_Extract
),
SQ_HSBCyberSuiteBordereauExtractCntl AS (
	SELECT
		HSBCyberSuiteBordereauExtractId AS HSBCyberPaymentBordereauExtractId,
		AuditId AS AuditID,
		CreatedDate,
		ModifiedDate,
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
	FROM HSBCyberSuiteBordereauExtract
	WHERE AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
AGG_PaymentBordereauxRecord AS (
	SELECT
	'CONTROL' AS Record_Type,
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
	to_char(RunDate, 'YYYYMM') AS Reporting_Period,
	'08.07' AS Version_Number,
	'' AS Filler
	FROM SQ_HSBCyberSuiteBordereauExtractCntl
	GROUP BY Company, Reporting_Period
),
HSBCyberSuiteBordereauxRecordControlFile AS (
	INSERT INTO HSBCyberSuiteBordereauxRecordControlFile
	(Control, Company, RecordCount, SumCoverageGrossPremium, SumCoverageNetPremium, RunDateYYYYMMDD, HardCodeString, HardCodeString2)
	SELECT 
	Record_Type AS CONTROL, 
	COMPANY, 
	RECORDCOUNT, 
	SUMCOVERAGEGROSSPREMIUM, 
	SUMCOVERAGENETPREMIUM, 
	Reporting_Period AS RUNDATEYYYYMMDD, 
	Version_Number AS HARDCODESTRING, 
	Filler AS HARDCODESTRING2
	FROM AGG_PaymentBordereauxRecord
),
SQ_HSBCyberSuiteReferralExtract AS (
	SELECT
		HSBCyberSuiteReferralExtractId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		RunDate,
		CreationDate,
		PolicyRequestDate,
		NameOfInsured,
		CustomerNumber,
		PolicyNumber,
		AgencyName,
		AgentCode,
		LimitAmount,
		OccupancyCode,
		Question1,
		Question2,
		Question3,
		Question4,
		Question5,
		Question6
	FROM HSBCyberSuiteReferralExtract
	WHERE AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
EXP_referral_record AS (
	SELECT
	RunDate,
	CreationDate,
	PolicyRequestDate,
	NameOfInsured,
	CustomerNumber,
	PolicyNumber,
	AgencyName,
	AgentCode AS AgencyCode,
	LimitAmount,
	OccupancyCode,
	Question1,
	Question2,
	Question3,
	Question4,
	Question5,
	Question6
	FROM SQ_HSBCyberSuiteReferralExtract
),
HSBCyberSuiteReferralRecordFile AS (
	INSERT INTO HSBCyberSuiteReferralRecordFile
	(RunDate, CreationDate, PolicyRequestDate, NameOfInsured, CustomerNumber, PolicyNumber, AgencyName, AgencyCode, LimitAmount, OccupancyCode, Question1, Question2, Question3, Question4, Question5, Question6)
	SELECT 
	RUNDATE, 
	CREATIONDATE, 
	POLICYREQUESTDATE, 
	NAMEOFINSURED, 
	CUSTOMERNUMBER, 
	POLICYNUMBER, 
	AGENCYNAME, 
	AGENCYCODE, 
	LIMITAMOUNT, 
	OCCUPANCYCODE, 
	QUESTION1, 
	QUESTION2, 
	QUESTION3, 
	QUESTION4, 
	QUESTION5, 
	QUESTION6
	FROM EXP_referral_record
),