WITH
SQ_WorkDataCompromise AS (
	SELECT WorkDataCompromise.AuditID, WorkDataCompromise.RunDate, WorkDataCompromise.PolKey, WorkDataCompromise.Company, WorkDataCompromise.ProductCode, WorkDataCompromise.ContractNumber, WorkDataCompromise.NameOfInsured, WorkDataCompromise.MailingAddressStreetName, WorkDataCompromise.MailingAddressCity, WorkDataCompromise.MailingAddressState, WorkDataCompromise.MailingAddressZipCode, WorkDataCompromise.TotalPackageGrossPremium, WorkDataCompromise.TotalPropertyGrossPremium, WorkDataCompromise.FirstPartyLimit, WorkDataCompromise.DeductibleAmount, WorkDataCompromise.OccupancyCode, WorkDataCompromise.PolicyTotalInsuredValue, WorkDataCompromise.PreviousPolicyNumber, WorkDataCompromise.AgencyCode, WorkDataCompromise.BranchCode, WorkDataCompromise.ThirdPartyIndicator, WorkDataCompromise.CoverageEffectiveDate, WorkDataCompromise.CoverageExpirationDate, WorkDataCompromise.CoverageGrossPremium, WorkDataCompromise.CoverageNetPremium 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDataCompromise 
	/*
	inner join
	@{pipeline().parameters.SOURCE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on (WorkDataCompromise.RatingCoverageID= RC.RatingCoverageID)
	left join
	@{pipeline().parameters.SOURCE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageMonthly WEP
	on (RC.RatingCoverageAKID=WEP.RatingCoverageAKID
	and  WorkDataCompromise.RunDate = WEP.RunDate )
	*/
	WHERE
	--WEP.StatisticalCoverageCancellationDate is null
	--AND
	convert(date,WorkDataCompromise.CoverageEffectiveDate)<=@{pipeline().parameters.RUNDATE}
	AND 
	convert(date,WorkDataCompromise.CoverageExpirationDate)>=@{pipeline().parameters.RUNDATE}
	AND
	convert(date,WorkDataCompromise.PolicyCancellationdate)>@{pipeline().parameters.RUNDATE}
),
AGG_PolicyRecord AS (
	SELECT
	AuditID,
	'DCT' AS SourceSystemID,
	Sysdate AS CreatedDate,
	Sysdate AS ModifiedDate,
	RunDate,
	PolKey,
	Company,
	ProductCode,
	ContractNumber,
	-- *INF*: SUBSTR(PolKey,1,10)
	SUBSTR(PolKey, 1, 10) AS PolicyNumber,
	CoverageEffectiveDate AS PolicyEffectiveDate,
	CoverageExpirationDate AS PolicyExpirationDate,
	NameOfInsured,
	MailingAddressStreetName,
	MailingAddressCity,
	MailingAddressState,
	MailingAddressZipCode,
	TotalPackageGrossPremium,
	TotalPropertyGrossPremium,
	-- *INF*: SUM(CoverageNetPremium)
	-- --incase of cancellation the premium reported should be net premium
	-- 
	SUM(CoverageNetPremium) AS GrossPremium,
	-- *INF*: --Fix for EDWP-3822
	-- Round(0.7* Sum(CoverageGrossPremium),2)
	-- --Round(0.3* Sum(CoverageGrossPremium),2)
	Round(0.7 * Sum(CoverageGrossPremium), 2) AS NetPremium,
	FirstPartyLimit,
	DeductibleAmount,
	OccupancyCode,
	PolicyTotalInsuredValue,
	PreviousPolicyNumber,
	AgencyCode,
	BranchCode,
	ThirdPartyIndicator,
	CoverageGrossPremium,
	CoverageNetPremium
	FROM SQ_WorkDataCompromise
	GROUP BY PolKey
),
EXP_PolicyRecord AS (
	SELECT
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
	FROM AGG_PolicyRecord
),
FILTRANS AS (
	SELECT
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
	FROM EXP_PolicyRecord
	WHERE GrossPremium>0
),
DataCompromisePolicyRecord AS (
	TRUNCATE TABLE DataCompromisePolicyRecord;
	INSERT INTO DataCompromisePolicyRecord
	(AuditID, SourceSystemID, CreatedDate, ModifiedDate, RunDate, Company, ProductCode, ContractNumber, PolicyNumber, PolicyEffectiveDate, PolicyExpirationDate, NameOfInsured, MailingAddressStreetName, MailingAddressCity, MailingAddressState, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, GrossPremium, NetPremium, FirstPartyLimit, DeductibleAmount, OccupancyCode, PolicyTotalInsuredValue, PreviousPolicyNumber, AgencyCode, BranchCode, ThirdPartyIndicator)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RUNDATE, 
	COMPANY, 
	PRODUCTCODE, 
	CONTRACTNUMBER, 
	POLICYNUMBER, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
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
	FROM FILTRANS
),
SQ_PaymentRecordTable AS (
	SELECT
		WorkDataCompromiseId,
		AuditID,
		SourceSystemID,
		CreatedDate,
		ModifiedDate,
		PolicyID,
		RiskLocationID,
		PolicyCoverageID,
		RatingCoverageID,
		PremiumTransactionID,
		PremiumMasterCalculationID,
		RunDate,
		ContractCustID,
		ContractCustAddrID,
		AgencyID,
		PolKey,
		Company,
		ProductCode,
		ContractNumber,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		NameOfInsured,
		MailingAddressStreetName,
		MailingAddressCity,
		MailingAddressState,
		MailingAddressZipCode,
		TotalPackageGrossPremium,
		TotalPropertyGrossPremium,
		FirstPartyLimit,
		DeductibleAmount,
		OccupancyCode,
		PolicyTotalInsuredValue,
		PreviousPolicyNumber,
		AgencyCode,
		BranchCode,
		ThirdPartyIndicator,
		TransactionCode,
		TransactionEffectiveDate,
		CoverageEffectiveDate,
		CoverageExpirationDate,
		CoverageGrossPremium,
		CoverageNetPremium,
		ProgramID,
		PolicyCancellationDate
	FROM WorkDataCompromise
	WHERE convert(date,WorkDataCompromise.RunDate)=@{pipeline().parameters.RUNDATE}
),
EXP_PaymentRecord AS (
	SELECT
	AuditID,
	'DCT' AS SourceSystemID,
	Sysdate AS CreatedDate,
	Sysdate AS ModifiedDate,
	PremiumMasterCalculationID,
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
	-- *INF*: CoverageNetPremium
	-- --incase of cancellation the premium reported should be net premium
	-- --iif(TransactionCode='03',CoverageNetPremium,CoverageGrossPremium)
	-- 
	CoverageNetPremium AS v_CoverageGrossPremium,
	v_CoverageGrossPremium AS out_CoverageGrossPremium,
	-- *INF*: --Fix for EDWP-3822
	-- ROUND(0.7*v_CoverageGrossPremium,2)
	-- --ROUND(0.3*CoverageNetPremium,2)
	ROUND(0.7 * v_CoverageGrossPremium, 2) AS out_CoverageNetPremium,
	PreviousPolicyNumber,
	ProgramID,
	NameOfInsured,
	ContractNumber
	FROM SQ_PaymentRecordTable
),
DataCompromisePaymentBordereauxRecord AS (
	TRUNCATE TABLE DataCompromisePaymentBordereauxRecord;
	INSERT INTO DataCompromisePaymentBordereauxRecord
	(AuditID, SourceSystemID, CreatedDate, ModifiedDate, PremiumMasterCalculationID, RunDate, ProductCode, Company, PolicyNumber, TransactionCode, TransactionEffectiveDate, CoverageEffectiveDate, CoverageExpirationDate, CoverageGrossPremium, CoverageNetPremium, PreviousPolicyNumber, ProgramID, NameOfInsured, ContractNumber)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PREMIUMMASTERCALCULATIONID, 
	RUNDATE, 
	PRODUCTCODE, 
	COMPANY, 
	POLICYNUMBER, 
	TRANSACTIONCODE, 
	TRANSACTIONEFFECTIVEDATE, 
	COVERAGEEFFECTIVEDATE, 
	COVERAGEEXPIRATIONDATE, 
	out_CoverageGrossPremium AS COVERAGEGROSSPREMIUM, 
	out_CoverageNetPremium AS COVERAGENETPREMIUM, 
	PREVIOUSPOLICYNUMBER, 
	PROGRAMID, 
	NAMEOFINSURED, 
	CONTRACTNUMBER
	FROM EXP_PaymentRecord
),