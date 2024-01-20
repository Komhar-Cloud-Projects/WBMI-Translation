WITH
SQ_WorkHSBCyberSuite AS (
	SELECT AuditId, RunDate, PolicyKey, Company, ProductCode, ContractNumber, InsuredName, MailingAddressStreetName, MailingAddressCityName, MailingAddressStateAbbreviation, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, Limit, Deductible, OccupancyCode, PreviousPolicyNumber, AgencyCode, BranchCode, CoverageEffectiveDate, CoverageExpirationDate, CyberSuiteCoverageGrossPremium, CyberSuiteCoverageNetPremium, LimitType,
	PremiumTransactionEnteredDate,
	PricingTier
	FROM 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkHSBCyberSuite
	WHERE  
	convert(date,WorkHSBCyberSuite.CoverageEffectiveDate)<=@{pipeline().parameters.RUNDATE} 
	AND  
	convert(date,WorkHSBCyberSuite.CoverageExpirationDate)>=@{pipeline().parameters.RUNDATE} 
	AND 
	convert(date,WorkHSBCyberSuite.PolicyCancellationdate)>@{pipeline().parameters.RUNDATE}
),
EXP_WorkHSBCyber AS (
	SELECT
	AuditID,
	RunDate,
	PolicyKey,
	Company,
	ProductCode,
	ContractNumber,
	NameOfInsured,
	MailingAddressStreetName,
	MailingAddressCity,
	MailingAddressState,
	MailingAddressZipCode,
	TotalPackageGrossPremium,
	TotalPropertyGrossPremium,
	Limit,
	Deductible,
	OccupancyCode,
	PreviousPolicyNumber,
	AgencyCode,
	BranchCode,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	CoverageGrossPremium,
	CoverageNetPremium,
	LimitType,
	PricingTier,
	premiumtransactionentereddate,
	-- *INF*: substr(PolicyKey,1.10)
	substr(PolicyKey, 1.10) AS o_PolicyNumber
	FROM SQ_WorkHSBCyberSuite
),
SRT_PTEnteredDate AS (
	SELECT
	AuditID, 
	RunDate, 
	o_PolicyNumber AS policyNumber, 
	Company, 
	ProductCode, 
	ContractNumber, 
	NameOfInsured, 
	MailingAddressStreetName, 
	MailingAddressCity, 
	MailingAddressState, 
	MailingAddressZipCode, 
	TotalPackageGrossPremium, 
	TotalPropertyGrossPremium, 
	Limit, 
	Deductible, 
	OccupancyCode, 
	PreviousPolicyNumber, 
	AgencyCode, 
	BranchCode, 
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	CoverageGrossPremium, 
	CoverageNetPremium, 
	LimitType, 
	PricingTier AS CBSPricingTier, 
	premiumtransactionentereddate
	FROM EXP_WorkHSBCyber
	ORDER BY premiumtransactionentereddate ASC
),
AGG_PolicyRecord AS (
	SELECT
	AuditID,
	RunDate,
	policyNumber AS PolKey,
	Company,
	ProductCode,
	ContractNumber,
	NameOfInsured,
	MailingAddressStreetName,
	MailingAddressCity,
	MailingAddressState,
	MailingAddressZipCode,
	TotalPackageGrossPremium,
	TotalPropertyGrossPremium,
	Limit,
	-- *INF*: LAST(Limit)
	LAST(Limit) AS out_Limit,
	Deductible,
	-- *INF*: LAST(Deductible)
	LAST(Deductible) AS out_Deductible,
	OccupancyCode,
	PreviousPolicyNumber,
	AgencyCode,
	BranchCode,
	CoverageEffectiveDate AS PolicyEffectiveDate,
	CoverageExpirationDate AS PolicyExpirationDate,
	CoverageGrossPremium,
	CoverageNetPremium,
	-- *INF*: SUM(CoverageNetPremium)
	-- --incase of cancellation the premium reported should be net premium
	-- 
	SUM(CoverageNetPremium) AS GrossPremium,
	-- *INF*: Round(0.7* Sum(CoverageGrossPremium),2)
	-- 
	Round(0.7 * Sum(CoverageGrossPremium), 2) AS NetPremium,
	LimitType,
	CBSPricingTier,
	-- *INF*: LAST(CBSPricingTier)
	LAST(CBSPricingTier) AS o_CBSPricingTier,
	premiumtransactionentereddate
	FROM SRT_PTEnteredDate
	GROUP BY PolKey
),
EXP_PolicyRecord AS (
	SELECT
	AuditID,
	Sysdate AS CreatedDate,
	Sysdate AS ModifiedDate,
	RunDate,
	Company,
	ProductCode,
	ContractNumber,
	PolKey AS PolicyNumber,
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
	out_Limit AS in_Limit,
	-- *INF*: IIF(ISNULL(in_Limit),'',in_Limit)
	IFF(in_Limit IS NULL, '', in_Limit) AS v_Limit,
	-- *INF*: TO_INTEGER(v_Limit)
	CAST(v_Limit AS INTEGER) AS out_Limit,
	out_Deductible AS in_Deductible,
	-- *INF*: IIF(ISNULL(in_Deductible),'',in_Deductible)
	IFF(in_Deductible IS NULL, '', in_Deductible) AS v_Deductible,
	-- *INF*: TO_INTEGER(v_Deductible)
	CAST(v_Deductible AS INTEGER) AS out_Deductible,
	OccupancyCode,
	0 AS PolicyTotalInsuredValue,
	PreviousPolicyNumber,
	AgencyCode,
	BranchCode,
	LimitType,
	'' AS WebSite,
	'' AS EmailAddress,
	o_CBSPricingTier AS CBSPricingTier,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(CBSPricingTier),'NA',
	-- CBSPricingTier)
	DECODE(
	    TRUE,
	    CBSPricingTier IS NULL, 'NA',
	    CBSPricingTier
	) AS o_CBSPricingTier
	FROM AGG_PolicyRecord
),
FLT_GrossPremium AS (
	SELECT
	AuditID, 
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
	out_Limit AS Limit, 
	out_Deductible AS Deductible, 
	OccupancyCode, 
	PolicyTotalInsuredValue, 
	PreviousPolicyNumber, 
	AgencyCode, 
	BranchCode, 
	LimitType, 
	WebSite, 
	EmailAddress, 
	o_CBSPricingTier AS CBSPricingTier
	FROM EXP_PolicyRecord
	WHERE GrossPremium>0  AND  CBSPricingTier != ''
),
HSBCyberSuitePolicyExtract AS (
	TRUNCATE TABLE HSBCyberSuitePolicyExtract;
	INSERT INTO HSBCyberSuitePolicyExtract
	(AuditId, CreatedDate, ModifiedDate, RunDate, Company, ProductCode, ContractNumber, PolicyNumber, CBSCoverageEffectiveDate, CBSCoverageExpirationDate, NameOfInsured, MailingAddressStreetName, MailingAddressCity, MailingAddressState, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, CBSGrossPremium, CBSNetPremium, LimitAmount, LimitType, DeductibleAmount, OccupancyCode, PolicyTotalInsuredValue, PreviousPolicyNumber, AgentCode, BranchCode, WebSite, EmailAddress, CBSPricingTier)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RUNDATE, 
	COMPANY, 
	PRODUCTCODE, 
	CONTRACTNUMBER, 
	POLICYNUMBER, 
	PolicyEffectiveDate AS CBSCOVERAGEEFFECTIVEDATE, 
	PolicyExpirationDate AS CBSCOVERAGEEXPIRATIONDATE, 
	NAMEOFINSURED, 
	MAILINGADDRESSSTREETNAME, 
	MAILINGADDRESSCITY, 
	MAILINGADDRESSSTATE, 
	MAILINGADDRESSZIPCODE, 
	TOTALPACKAGEGROSSPREMIUM, 
	TOTALPROPERTYGROSSPREMIUM, 
	GrossPremium AS CBSGROSSPREMIUM, 
	NetPremium AS CBSNETPREMIUM, 
	Limit AS LIMITAMOUNT, 
	LIMITTYPE, 
	Deductible AS DEDUCTIBLEAMOUNT, 
	OCCUPANCYCODE, 
	POLICYTOTALINSUREDVALUE, 
	PREVIOUSPOLICYNUMBER, 
	AgencyCode AS AGENTCODE, 
	BRANCHCODE, 
	WEBSITE, 
	EMAILADDRESS, 
	CBSPRICINGTIER
	FROM FLT_GrossPremium
),