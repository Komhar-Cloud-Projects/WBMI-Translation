WITH
SQ_PaymentRecordTable AS (
	SELECT   b.AuditId, b.PolicyKey,  b.RunDate,b.Company,  b.ProductCode,b.ContractNumber,b.InsuredName, b.PreviousPolicyNumber, b.coveragetype,b.PremiumTransactionCode, b.PremiumTransactionEffectiveDate,
	b.CoverageEffectiveDate, b.CoverageExpirationDate, b.CyberSuiteCoverageGrossPremium, b.CyberSuiteCoverageNetPremium, b.ProgramCode,b.premiumtransactionentereddate,
	b.Limit 
	from
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkHSBCyberSuite b
	where b.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
EXP_PaymentRecord AS (
	SELECT
	AuditID,
	Sysdate AS CreatedDate,
	Sysdate AS ModifiedDate,
	RunDate,
	ProductCode,
	PolKey,
	Company,
	-- *INF*: SUBSTR(PolKey,1,10)
	SUBSTR(PolKey, 1, 10) AS PolicyNumber,
	TransactionCode,
	TransactionEffectiveDate,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	CyberSuiteCoverageGrossPremium AS CoverageGrossPremium,
	CyberSuiteCoverageNetPremium AS CoverageNetPremium,
	CoverageNetPremium AS v_CoverageGrossPremium,
	v_CoverageGrossPremium AS out_CoverageGrossPremium,
	-- *INF*: ROUND(0.7*v_CoverageGrossPremium,2)
	-- 
	ROUND(0.7 * v_CoverageGrossPremium, 2) AS out_CoverageNetPremium,
	PreviousPolicyNumber,
	ProgramID,
	NameOfInsured,
	CoverageType,
	ContractNumber,
	PremiumTransactionEnteredDate,
	Limit
	FROM SQ_PaymentRecordTable
),
SRT_PolicyKey AS (
	SELECT
	PolicyNumber, 
	AuditID, 
	CreatedDate, 
	ModifiedDate, 
	RunDate, 
	ProductCode, 
	Company, 
	TransactionCode, 
	TransactionEffectiveDate, 
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	out_CoverageGrossPremium, 
	out_CoverageNetPremium, 
	PreviousPolicyNumber, 
	ProgramID, 
	NameOfInsured, 
	CoverageType, 
	ContractNumber, 
	PremiumTransactionEnteredDate, 
	Limit
	FROM EXP_PaymentRecord
	ORDER BY PolicyNumber ASC, RunDate ASC, PremiumTransactionEnteredDate ASC
),
EXP_PreFilter AS (
	SELECT
	AuditID,
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
	out_CoverageGrossPremium,
	out_CoverageNetPremium,
	PreviousPolicyNumber,
	ProgramID,
	NameOfInsured,
	CoverageType,
	ContractNumber,
	PremiumTransactionEnteredDate
	FROM SRT_PolicyKey
),
FLT_CoverageGrossPremium AS (
	SELECT
	AuditID, 
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
	out_CoverageGrossPremium AS CoverageGrossPremium, 
	out_CoverageNetPremium AS CoverageNetPremium, 
	PreviousPolicyNumber, 
	ProgramID, 
	NameOfInsured, 
	ContractNumber, 
	PremiumTransactionEnteredDate
	FROM EXP_PreFilter
	WHERE CoverageGrossPremium != 0
),
HSBCyberSuiteBordereauExtract AS (
	TRUNCATE TABLE HSBCyberSuiteBordereauExtract;
	INSERT INTO HSBCyberSuiteBordereauExtract
	(AuditId, CreatedDate, ModifiedDate, RunDate, ProductCode, Company, PolicyNumber, TransactionCode, TransactionEffectiveDate, CoverageEffectiveDate, CoverageExpirationDate, CoverageGrossPremium, CoverageNetPremium, PreviousPolicyNumber, ProgramID, NameOfInsured, ContractNumber)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RUNDATE, 
	PRODUCTCODE, 
	COMPANY, 
	POLICYNUMBER, 
	TRANSACTIONCODE, 
	TRANSACTIONEFFECTIVEDATE, 
	COVERAGEEFFECTIVEDATE, 
	COVERAGEEXPIRATIONDATE, 
	COVERAGEGROSSPREMIUM, 
	COVERAGENETPREMIUM, 
	PREVIOUSPOLICYNUMBER, 
	PROGRAMID, 
	NAMEOFINSURED, 
	CONTRACTNUMBER
	FROM FLT_CoverageGrossPremium
),