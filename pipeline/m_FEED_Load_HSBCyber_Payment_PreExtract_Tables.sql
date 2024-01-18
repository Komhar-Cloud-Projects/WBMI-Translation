WITH
SQ_PaymentRecordTable AS (
	SELECT   b.AuditId, b.PolicyKey,  b.RunDate,b.Company,  b.ProductCode,b.ContractNumber,b.InsuredName, b.PreviousPolicyNumber, b.coveragetype,b.PremiumTransactionCode, b.PremiumTransactionEffectiveDate,
	b.CoverageEffectiveDate, b.CoverageExpirationDate, b.CyberCoverageGrossPremium, b.CyberCoverageNetPremium, b.ProgramCode,b.premiumtransactionentereddate,
	b.FirstPartyLimit , b.ThirdPartyLimit
	from
	(select distinct policykey
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WORKHSBCYBER 
	where AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	) w
	inner join (select AuditId, PremiumMasterCalculationId, RunDate, ProductCode, PolicyKey, Company, PremiumTransactionCode, PremiumTransactionEffectiveDate,
	CoverageEffectiveDate, CoverageExpirationDate, CyberCoverageGrossPremium, CyberCoverageNetPremium, PreviousPolicyNumber, ProgramCode,
	InsuredName, ContractNumber,coveragetype,premiumtransactionentereddate ,
	WorkHSBCyber.FirstPartyLimit , WorkHSBCyber.ThirdPartyLimit
	 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WORKHSBCYBER ) b
	on b.policykey=w.policykey
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
	CoverageGrossPremium,
	CoverageNetPremium,
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
	FirstPartyLimit,
	ThirdPartyLimit
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
	FirstPartyLimit, 
	ThirdPartyLimit
	FROM EXP_PaymentRecord
	ORDER BY PolicyNumber ASC, RunDate ASC, PremiumTransactionEnteredDate ASC
),
AGG_PolicyCoverageCount AS (
	SELECT
	PolicyNumber,
	CoverageType,
	FirstPartyLimit,
	ThirdPartyLimit
	FROM SRT_PolicyKey
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber, CoverageType ORDER BY NULL) = 1
),
AGG_CoverageCount AS (
	SELECT
	PolicyNumber,
	CoverageType,
	-- *INF*: COUNT(1,
	-- (IN(CoverageType,'CyberComputerAttack') 
	-- AND NOT ISNULL(FirstPartyLimit))
	-- OR
	-- (IN(CoverageType,'CyberNetworkSecurity') 
	-- AND NOT ISNULL(ThirdPartyLimit))
	-- )
	-- 
	-- --count(1,IN(CoverageType,'CyberComputerAttack','CyberNetworkSecurity'))
	COUNT(1, (CoverageType IN ('CyberComputerAttack') AND FirstPartyLimit IS NOT NULL) OR (CoverageType IN ('CyberNetworkSecurity') AND ThirdPartyLimit IS NOT NULL)) AS count,
	FirstPartyLimit,
	ThirdPartyLimit
	FROM AGG_PolicyCoverageCount
	GROUP BY PolicyNumber
),
JNR_CountToRecords AS (SELECT
	SRT_PolicyKey.AuditID, 
	SRT_PolicyKey.CreatedDate, 
	SRT_PolicyKey.ModifiedDate, 
	SRT_PolicyKey.RunDate, 
	SRT_PolicyKey.ProductCode, 
	SRT_PolicyKey.Company, 
	SRT_PolicyKey.PolicyNumber, 
	SRT_PolicyKey.TransactionCode, 
	SRT_PolicyKey.TransactionEffectiveDate, 
	SRT_PolicyKey.CoverageEffectiveDate, 
	SRT_PolicyKey.CoverageExpirationDate, 
	SRT_PolicyKey.out_CoverageGrossPremium, 
	SRT_PolicyKey.out_CoverageNetPremium, 
	SRT_PolicyKey.PreviousPolicyNumber, 
	SRT_PolicyKey.ProgramID, 
	SRT_PolicyKey.NameOfInsured, 
	SRT_PolicyKey.CoverageType, 
	AGG_CoverageCount.PolicyNumber AS PolicyNumber_agg, 
	AGG_CoverageCount.count, 
	SRT_PolicyKey.ContractNumber, 
	SRT_PolicyKey.PremiumTransactionEnteredDate
	FROM SRT_PolicyKey
	LEFT OUTER JOIN AGG_CoverageCount
	ON AGG_CoverageCount.PolicyNumber = SRT_PolicyKey.PolicyNumber
),
EXP_ContractNumber AS (
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
	PolicyNumber_agg,
	count AS counter,
	ContractNumber,
	-- *INF*: IIF(counter=2,
	-- Decode(True,
	-- IN(ContractNumber,'1003696','1003697'),'1003654',
	-- IN(ContractNumber,'1003699','1003698'),'1003655',
	-- IN(ContractNumber,'1003703','1003702'),'1003656',
	-- IN(ContractNumber,'1003700','1003701'),'1003657'
	-- ),
	-- ContractNumber)
	-- 
	IFF(
	    counter = 2,
	    Decode(
	        True,
	        ContractNumber IN ('1003696','1003697'), '1003654',
	        ContractNumber IN ('1003699','1003698'), '1003655',
	        ContractNumber IN ('1003703','1003702'), '1003656',
	        ContractNumber IN ('1003700','1003701'), '1003657'
	    ),
	    ContractNumber
	) AS out_ContractNumber,
	PremiumTransactionEnteredDate
	FROM JNR_CountToRecords
),
FLTR_CoverageGrossPremium AS (
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
	out_ContractNumber AS ContractNumber, 
	PremiumTransactionEnteredDate
	FROM EXP_ContractNumber
	WHERE CoverageGrossPremium != 0
),
HSBCyberPaymentBordereauExtract AS (
	TRUNCATE TABLE HSBCyberPaymentBordereauExtract;
	INSERT INTO HSBCyberPaymentBordereauExtract
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
	FROM FLTR_CoverageGrossPremium
),