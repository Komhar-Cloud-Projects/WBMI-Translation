WITH
SQ_HSBCyberPolicyExtract AS (
	SELECT
		HSBCyberPolicyExtractId,
		AuditId AS AuditID,
		CreatedDate,
		ModifiedDate,
		RunDate,
		Company,
		ProductCode,
		ContractNumber,
		PolicyNumber,
		CyberCoverageEffectiveDate AS PolicyEffectiveDate,
		CyberCoverageExpirationDate AS PolicyExpirationDate,
		NameOfInsured,
		MailingAddressStreetName,
		MailingAddressCity,
		MailingAddressState,
		MailingAddressZipCode,
		TotalPackageGrossPremium,
		TotalPropertyGrossPremium,
		CyberGrossPremium AS GrossPremium,
		CyberNetPremium AS NetPremium,
		FirstPartyLimit,
		FirstPartyDeductible,
		OccupancyCode,
		PolicyTotalInsuredValue,
		PreviousPolicyNumber,
		AgentCode AS AgencyCode,
		BranchCode,
		FirstPartyCoverage,
		ThirdPartyLimit,
		ThirdPartyDeductible,
		ThirdPartyCoverage,
		ExtortionSublimit
	FROM HSBCyberPolicyExtract_Record
	WHERE convert(date,CyberCoverageEffectiveDate)<=@{pipeline().parameters.RUNDATE}
	AND 
	convert(date,CyberCoverageExpirationDate)>=@{pipeline().parameters.RUNDATE}
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
	FirstPartyDeductible,
	OccupancyCode,
	PolicyTotalInsuredValue,
	PreviousPolicyNumber,
	AgencyCode,
	BranchCode,
	ThirdPartyLimit AS CyberOneThirdPartyLimit,
	ThirdPartyDeductible AS CyberOneThirdPartyDeductible,
	ThirdPartyCoverage,
	FirstPartyCoverage,
	ExtortionSublimit
	FROM SQ_HSBCyberPolicyExtract
),
HSBCyberPolicyRecordFile AS (
	INSERT INTO HSBCyberPolicyRecordFile
	(Company, ProductCode, ContractNumber, PolicyNumber, CyberOneCoverageEffectiveDate, CyberOneCoverageExpirationDate, NameOfInsured, MailingAddressStreetName, MailingAddressCity, MailingAddressState, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, CyberOneGrossPremium, CyberOneNetPremium, OccupancyCode, PolicyTotalInsuredValue, PreviousPolicyNumber, AgencyCode, BranchCode, CyberOneFirstPartyLimit, CyberOneFirstPartyDeductible, FirstPartyCoverage, CyberOneThirdPartyLimit, CyberOneThirdPartyDeductible, ThirdPartyCoverage, ExtortionSublimit)
	SELECT 
	COMPANY, 
	PRODUCTCODE, 
	CONTRACTNUMBER, 
	POLICYNUMBER, 
	PolicyEffectiveDateYYYYMMDD AS CYBERONECOVERAGEEFFECTIVEDATE, 
	PolicyExpirationDateYYYYMMDD AS CYBERONECOVERAGEEXPIRATIONDATE, 
	NAMEOFINSURED, 
	MAILINGADDRESSSTREETNAME, 
	MAILINGADDRESSCITY, 
	MAILINGADDRESSSTATE, 
	MAILINGADDRESSZIPCODE, 
	TOTALPACKAGEGROSSPREMIUM, 
	TOTALPROPERTYGROSSPREMIUM, 
	GrossPremium AS CYBERONEGROSSPREMIUM, 
	NetPremium AS CYBERONENETPREMIUM, 
	OCCUPANCYCODE, 
	POLICYTOTALINSUREDVALUE, 
	PREVIOUSPOLICYNUMBER, 
	AGENCYCODE, 
	BRANCHCODE, 
	CYBERONEFIRSTPARTYLIMIT, 
	FirstPartyDeductible AS CYBERONEFIRSTPARTYDEDUCTIBLE, 
	FIRSTPARTYCOVERAGE, 
	CYBERONETHIRDPARTYLIMIT, 
	CYBERONETHIRDPARTYDEDUCTIBLE, 
	THIRDPARTYCOVERAGE, 
	EXTORTIONSUBLIMIT
	FROM EXP_PolicyExtract
),
SQ_HSBCyberPaymentBordereauExtract AS (
	SELECT
		HSBCyberPaymentBordereauExtractId,
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
	FROM HSBCyberPaymentBordereauExtract2
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
HSBCyberPaymentBordereauxRecordFile AS (
	INSERT INTO HSBCyberPaymentBordereauxRecordFile
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
SQ_HSBCyberPaymentBordereauExtractCntl AS (
	SELECT
		HSBCyberPaymentBordereauExtractId,
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
	FROM HSBCyberPaymentBordereauExtract
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
	'08.04' AS Version_Number,
	'' AS Filler
	FROM SQ_HSBCyberPaymentBordereauExtractCntl
	GROUP BY Company, Reporting_Period
),
HSBCyberPaymentBordereauxRecordControlFile AS (
	INSERT INTO HSBCyberPaymentBordereauxRecordControlFile
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
SQ_HSBCyberPolicyExtractCntl AS (
	SELECT
		HSBCyberPolicyExtractId,
		AuditId AS AuditID,
		CreatedDate,
		ModifiedDate,
		RunDate,
		Company,
		ProductCode,
		ContractNumber,
		PolicyNumber,
		CyberCoverageEffectiveDate AS PolicyEffectiveDate,
		CyberCoverageExpirationDate AS PolicyExpirationDate,
		NameOfInsured,
		MailingAddressStreetName,
		MailingAddressCity,
		MailingAddressState,
		MailingAddressZipCode,
		TotalPackageGrossPremium,
		TotalPropertyGrossPremium,
		CyberGrossPremium AS GrossPremium,
		CyberNetPremium AS NetPremium,
		FirstPartyLimit,
		FirstPartyDeductible,
		OccupancyCode,
		PolicyTotalInsuredValue,
		PreviousPolicyNumber,
		AgentCode AS AgencyCode,
		BranchCode,
		FirstPartyCoverage,
		ThirdPartyLimit,
		ThirdPartyDeductible,
		ThirdPartyCoverage,
		ExtortionSublimit
	FROM HSBCyberPolicyExtract
	WHERE convert(date,CyberCoverageEffectiveDate)<=@{pipeline().parameters.RUNDATE}
	AND 
	convert(date,CyberCoverageExpirationDate)>=@{pipeline().parameters.RUNDATE}
),
AGGTRANS AS (
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
HSBCyberPolicyRecordControlFile AS (
	INSERT INTO HSBCyberPolicyRecordControlFile
	(Company, ProductCode, Control, RecordCount, RunDateYYYYMMDD, HardCodeString, HardCodeString2)
	SELECT 
	COMPANY, 
	ProductName AS PRODUCTCODE, 
	Record_Type AS CONTROL, 
	Number_of_Records AS RECORDCOUNT, 
	Inforce_Date AS RUNDATEYYYYMMDD, 
	Version_Number AS HARDCODESTRING, 
	Filler AS HARDCODESTRING2
	FROM AGGTRANS
),