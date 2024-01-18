WITH
LKP_PaymentBordereauExtract AS (
	SELECT
	ContractNumber,
	PolicyNumber
	FROM (
		SELECT 
			ContractNumber,
			PolicyNumber
		FROM HSBCyberPaymentBordereauExtract
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber ORDER BY ContractNumber) = 1
),
SQ_PaymentRecordTable AS (
	select policyNumber,count(*) as cnt from (
	select policyNumber,coveragetype from (
	SELECT   b.AuditId, b.PolicyKey,  b.RunDate,b.Company,  b.ProductCode,b.ContractNumber,b.InsuredName, b.PreviousPolicyNumber, b.coveragetype,b.PremiumTransactionCode, b.PremiumTransactionEffectiveDate,
	b.CoverageEffectiveDate, b.CoverageExpirationDate, b.CyberCoverageGrossPremium, b.CyberCoverageNetPremium, b.ProgramCode,b.premiumtransactionentereddate
	,substring(b.PolicyKey,1,10) as policyNumber
	from
	(select distinct policykey
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WORKHSBCYBER 
	) w
	inner join (select AuditId, PremiumMasterCalculationId, RunDate, ProductCode, PolicyKey, Company, PremiumTransactionCode, PremiumTransactionEffectiveDate,
	CoverageEffectiveDate, CoverageExpirationDate, CyberCoverageGrossPremium, CyberCoverageNetPremium, PreviousPolicyNumber, ProgramCode,
	InsuredName, ContractNumber,coveragetype,premiumtransactionentereddate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WORKHSBCYBER
	where convert(date,WorkHSBCyber.CoverageEffectiveDate)<=@{pipeline().parameters.RUNDATE} 
	AND  
	convert(date,WorkHSBCyber.CoverageExpirationDate)>=@{pipeline().parameters.RUNDATE} 
	AND 
	convert(date,WorkHSBCyber.PolicyCancellationdate)>@{pipeline().parameters.RUNDATE}
	and (
			(coveragetype in ('CyberComputerAttack') and FirstPartyLimit is not null)
			or 
			(coveragetype in ('CyberNetworkSecurity') and ThirdPartyLimit is not null)
		)
	 ) b
	on b.policykey=w.policykey
	) a group by policyNumber,coveragetype
	) b  where coveragetype in ('CyberComputerAttack','CyberNetworkSecurity') group by policyNumber
),
EXP_ContractNumber AS (
	SELECT
	policyNumber,
	cnt
	FROM SQ_PaymentRecordTable
),
SQ_WorkHSBCyber AS (
	SELECT AuditId, RunDate, PolicyKey, Company, ProductCode, ContractNumber, InsuredName, MailingAddressStreetName, MailingAddressCityName, MailingAddressStateAbbreviation, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, FirstPartyLimit, FirstPartyDeductible, OccupancyCode, PreviousPolicyNumber, AgencyCode, BranchCode, CoverageEffectiveDate, CoverageExpirationDate, CyberCoverageGrossPremium, CyberCoverageNetPremium, FirstPartyCoverage, ThirdPartyLimit, ThirdPartyDeductible, ThirdPartyCoverage, ExtortionSublimit,premiumtransactionentereddate
	FROM 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkHSBCyber  
	WHERE  
	convert(date,WorkHSBCyber.CoverageEffectiveDate)<=@{pipeline().parameters.RUNDATE} 
	AND  
	convert(date,WorkHSBCyber.CoverageExpirationDate)>=@{pipeline().parameters.RUNDATE} 
	AND 
	convert(date,WorkHSBCyber.PolicyCancellationdate)>@{pipeline().parameters.RUNDATE}
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
	FirstPartyLimit,
	FirstPartyDeductible,
	OccupancyCode,
	PreviousPolicyNumber,
	AgencyCode,
	BranchCode,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	CoverageGrossPremium,
	CoverageNetPremium,
	FirstPartyCoverage,
	ThirdPartyLimit,
	ThirdPartyDeductible,
	ThirdPartyCoverage,
	ExtortionSublimit,
	premiumtransactionentereddate,
	-- *INF*: substr(PolicyKey,1.10)
	substr(PolicyKey, 1.10) AS o_PolicyNumber
	FROM SQ_WorkHSBCyber
),
JNR_ContractNumber AS (SELECT
	EXP_WorkHSBCyber.AuditID, 
	EXP_WorkHSBCyber.RunDate, 
	EXP_WorkHSBCyber.PolicyKey, 
	EXP_WorkHSBCyber.Company, 
	EXP_WorkHSBCyber.ProductCode, 
	EXP_WorkHSBCyber.ContractNumber, 
	EXP_WorkHSBCyber.NameOfInsured, 
	EXP_WorkHSBCyber.MailingAddressStreetName, 
	EXP_WorkHSBCyber.MailingAddressCity, 
	EXP_WorkHSBCyber.MailingAddressState, 
	EXP_WorkHSBCyber.MailingAddressZipCode, 
	EXP_WorkHSBCyber.TotalPackageGrossPremium, 
	EXP_WorkHSBCyber.TotalPropertyGrossPremium, 
	EXP_WorkHSBCyber.FirstPartyLimit, 
	EXP_WorkHSBCyber.FirstPartyDeductible, 
	EXP_WorkHSBCyber.OccupancyCode, 
	EXP_WorkHSBCyber.PreviousPolicyNumber, 
	EXP_WorkHSBCyber.AgencyCode, 
	EXP_WorkHSBCyber.BranchCode, 
	EXP_WorkHSBCyber.CoverageEffectiveDate, 
	EXP_WorkHSBCyber.CoverageExpirationDate, 
	EXP_WorkHSBCyber.CoverageGrossPremium, 
	EXP_WorkHSBCyber.CoverageNetPremium, 
	EXP_WorkHSBCyber.FirstPartyCoverage, 
	EXP_WorkHSBCyber.ThirdPartyLimit, 
	EXP_WorkHSBCyber.ThirdPartyDeductible, 
	EXP_WorkHSBCyber.ThirdPartyCoverage, 
	EXP_WorkHSBCyber.ExtortionSublimit, 
	EXP_WorkHSBCyber.premiumtransactionentereddate, 
	EXP_WorkHSBCyber.o_PolicyNumber, 
	EXP_ContractNumber.policyNumber, 
	EXP_ContractNumber.cnt
	FROM EXP_WorkHSBCyber
	LEFT OUTER JOIN EXP_ContractNumber
	ON EXP_ContractNumber.policyNumber = EXP_WorkHSBCyber.o_PolicyNumber
),
SRT_PTEnteredDtae AS (
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
	FirstPartyLimit, 
	FirstPartyDeductible, 
	OccupancyCode, 
	PreviousPolicyNumber, 
	AgencyCode, 
	BranchCode, 
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	CoverageGrossPremium, 
	CoverageNetPremium, 
	FirstPartyCoverage, 
	ThirdPartyLimit, 
	ThirdPartyDeductible, 
	ThirdPartyCoverage, 
	ExtortionSublimit, 
	premiumtransactionentereddate, 
	policyNumber, 
	cnt
	FROM JNR_ContractNumber
	ORDER BY premiumtransactionentereddate ASC
),
AGG_PolicyRecord AS (
	SELECT
	AuditID,
	Sysdate AS CreatedDate,
	Sysdate AS ModifiedDate,
	RunDate,
	PolicyKey AS PolKey,
	Company,
	ProductCode,
	ContractNumber,
	-- *INF*: SUBSTR(PolKey,1,10)
	SUBSTR(PolKey, 1, 10) AS PolicyNumber,
	NameOfInsured,
	MailingAddressStreetName,
	MailingAddressCity,
	MailingAddressState,
	MailingAddressZipCode,
	TotalPackageGrossPremium,
	TotalPropertyGrossPremium,
	FirstPartyLimit,
	-- *INF*: LAST(FirstPartyLimit)
	LAST(FirstPartyLimit) AS out_FirstPartyLimit,
	FirstPartyDeductible,
	-- *INF*: LAST(FirstPartyDeductible)
	LAST(FirstPartyDeductible) AS out_FirstPartyDeductible,
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
	FirstPartyCoverage,
	-- *INF*: LAST(FirstPartyCoverage)
	LAST(FirstPartyCoverage) AS out_FirstPartyCoverage,
	ThirdPartyLimit,
	-- *INF*: LAST(ThirdPartyLimit)
	LAST(ThirdPartyLimit) AS out_ThirdPartyLimit,
	ThirdPartyDeductible,
	-- *INF*: last(ThirdPartyDeductible)
	last(ThirdPartyDeductible) AS out_ThirdPartyDeductible,
	ThirdPartyCoverage,
	-- *INF*: last(ThirdPartyCoverage)
	last(ThirdPartyCoverage) AS out_ThirdPartyCoverage,
	ExtortionSublimit,
	-- *INF*: last(ExtortionSublimit)
	last(ExtortionSublimit) AS out_ExtortionSublimit,
	premiumtransactionentereddate,
	cnt
	FROM SRT_PTEnteredDtae
	GROUP BY PolKey
),
EXP_PolicyRecord AS (
	SELECT
	AuditID,
	CreatedDate,
	ModifiedDate,
	RunDate,
	Company,
	ProductCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_PAYMENTBORDEREAUEXTRACT(PolicyNumber)),'', :LKP.LKP_PAYMENTBORDEREAUEXTRACT(PolicyNumber))
	IFF(
	    LKP_PAYMENTBORDEREAUEXTRACT_PolicyNumber.ContractNumber IS NULL, '',
	    LKP_PAYMENTBORDEREAUEXTRACT_PolicyNumber.ContractNumber
	) AS ContractNumber,
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
	out_FirstPartyLimit AS in_FirstPartyLimit,
	-- *INF*: IIF(ISNULL(in_FirstPartyLimit),'',in_FirstPartyLimit)
	IFF(in_FirstPartyLimit IS NULL, '', in_FirstPartyLimit) AS v_FirstPartyLimit,
	-- *INF*: TO_INTEGER(v_FirstPartyLimit)
	CAST(v_FirstPartyLimit AS INTEGER) AS out_FirstPartyLimit,
	out_FirstPartyDeductible AS in_FirstPartyDeductible,
	-- *INF*: IIF(ISNULL(in_FirstPartyDeductible),'',in_FirstPartyDeductible)
	IFF(in_FirstPartyDeductible IS NULL, '', in_FirstPartyDeductible) AS v_FirstPartyDeductible,
	-- *INF*: TO_INTEGER(v_FirstPartyDeductible)
	CAST(v_FirstPartyDeductible AS INTEGER) AS out_FirstPartyDeductible,
	OccupancyCode,
	0 AS PolicyTotalInsuredValue,
	PreviousPolicyNumber,
	AgencyCode,
	BranchCode,
	out_FirstPartyCoverage AS in_FirstPartyCoverage,
	out_ThirdPartyLimit AS in_ThirdPartyLimit,
	-- *INF*: IIF(ISNULL(in_ThirdPartyLimit),'',in_ThirdPartyLimit)
	IFF(in_ThirdPartyLimit IS NULL, '', in_ThirdPartyLimit) AS v_ThirdPartyLimit,
	-- *INF*: TO_INTEGER(v_ThirdPartyLimit)
	CAST(v_ThirdPartyLimit AS INTEGER) AS out_ThirdPartyLimit,
	out_ThirdPartyDeductible AS in_ThirdPartyDeductible,
	-- *INF*: IIF(ISNULL(in_ThirdPartyDeductible),'',in_ThirdPartyDeductible)
	IFF(in_ThirdPartyDeductible IS NULL, '', in_ThirdPartyDeductible) AS v_ThirdPartyDeductible,
	-- *INF*: TO_INTEGER(v_ThirdPartyDeductible)
	CAST(v_ThirdPartyDeductible AS INTEGER) AS out_ThirdPartyDeductible,
	out_ThirdPartyCoverage AS in_ThirdPartyCoverage,
	out_ExtortionSublimit AS in_ExtortionSublimit,
	-- *INF*: IIF(ISNULL(in_ExtortionSublimit),'',in_ExtortionSublimit)
	IFF(in_ExtortionSublimit IS NULL, '', in_ExtortionSublimit) AS v_ExtortionSublimit,
	-- *INF*: DECODE(TRUE , 
	--  LTRIM(RTRIM(in_FirstPartyLimit))='50000','L',
	--  LTRIM(RTRIM(in_FirstPartyLimit))='100000','F',
	--  LTRIM(RTRIM(in_FirstPartyLimit))='250000','F',
	-- LTRIM(RTRIM(in_FirstPartyLimit))='500000','F',
	-- LTRIM(RTRIM(in_FirstPartyLimit))='1000000','F',
	-- 'N'
	-- )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '50000', 'L',
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '100000', 'F',
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '250000', 'F',
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '500000', 'F',
	    LTRIM(RTRIM(in_FirstPartyLimit)) = '1000000', 'F',
	    'N'
	) AS out_FirstPartyCoverage,
	-- *INF*: DECODE(TRUE , 
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='50000','L',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='10000','F',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='100000','F',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='250000','F',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='500000','F',
	-- LTRIM(RTRIM(in_ThirdPartyLimit))='1000000','F',
	-- 'N'
	-- )
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '50000', 'L',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '10000', 'F',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '100000', 'F',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '250000', 'F',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '500000', 'F',
	    LTRIM(RTRIM(in_ThirdPartyLimit)) = '1000000', 'F',
	    'N'
	) AS out_ThirdPartyCoverage,
	-- *INF*: TO_INTEGER(v_ExtortionSublimit)
	CAST(v_ExtortionSublimit AS INTEGER) AS out_ExtortionSublimit,
	ContractNumber AS ContractNumber1,
	cnt,
	-- *INF*: IIF(cnt=2,
	-- Decode(True,
	-- IN(ContractNumber1,'1003696','1003697'),'1003654',
	-- IN(ContractNumber1,'1003699','1003698'),'1003655',
	-- IN(ContractNumber1,'1003703','1003702'),'1003656',
	-- IN(ContractNumber1,'1003700','1003701'),'1003657'
	-- ),
	-- ContractNumber1)
	IFF(
	    cnt = 2,
	    Decode(
	        True,
	        ContractNumber1 IN ('1003696','1003697'), '1003654',
	        ContractNumber1 IN ('1003699','1003698'), '1003655',
	        ContractNumber1 IN ('1003703','1003702'), '1003656',
	        ContractNumber1 IN ('1003700','1003701'), '1003657'
	    ),
	    ContractNumber1
	) AS o_ContractNumber
	FROM AGG_PolicyRecord
	LEFT JOIN LKP_PAYMENTBORDEREAUEXTRACT LKP_PAYMENTBORDEREAUEXTRACT_PolicyNumber
	ON LKP_PAYMENTBORDEREAUEXTRACT_PolicyNumber.PolicyNumber = PolicyNumber

),
FLT_GrossPremium AS (
	SELECT
	AuditID, 
	CreatedDate, 
	ModifiedDate, 
	RunDate, 
	Company, 
	ProductCode, 
	o_ContractNumber AS ContractNumber, 
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
	out_FirstPartyLimit AS FirstPartyLimit, 
	out_FirstPartyDeductible AS FirstPartyDeductible, 
	OccupancyCode, 
	PolicyTotalInsuredValue, 
	PreviousPolicyNumber, 
	AgencyCode, 
	BranchCode, 
	out_FirstPartyCoverage AS FirstPartyCoverage, 
	out_ThirdPartyLimit AS ThirdPartyLimit, 
	out_ThirdPartyDeductible AS ThirdPartyDeductible, 
	out_ThirdPartyCoverage AS ThirdPartyCoverage, 
	out_ExtortionSublimit AS ExtortionSublimit
	FROM EXP_PolicyRecord
	WHERE GrossPremium>0
),
HSBCyberPolicyExtract AS (
	TRUNCATE TABLE HSBCyberPolicyExtract;
	INSERT INTO HSBCyberPolicyExtract
	(AuditId, CreatedDate, ModifiedDate, RunDate, Company, ProductCode, ContractNumber, PolicyNumber, CyberCoverageEffectiveDate, CyberCoverageExpirationDate, NameOfInsured, MailingAddressStreetName, MailingAddressCity, MailingAddressState, MailingAddressZipCode, TotalPackageGrossPremium, TotalPropertyGrossPremium, CyberGrossPremium, CyberNetPremium, OccupancyCode, PolicyTotalInsuredValue, PreviousPolicyNumber, AgentCode, BranchCode, FirstPartyLimit, FirstPartyDeductible, FirstPartyCoverage, ThirdPartyLimit, ThirdPartyDeductible, ThirdPartyCoverage, ExtortionSublimit)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RUNDATE, 
	COMPANY, 
	PRODUCTCODE, 
	CONTRACTNUMBER, 
	POLICYNUMBER, 
	PolicyEffectiveDate AS CYBERCOVERAGEEFFECTIVEDATE, 
	PolicyExpirationDate AS CYBERCOVERAGEEXPIRATIONDATE, 
	NAMEOFINSURED, 
	MAILINGADDRESSSTREETNAME, 
	MAILINGADDRESSCITY, 
	MAILINGADDRESSSTATE, 
	MAILINGADDRESSZIPCODE, 
	TOTALPACKAGEGROSSPREMIUM, 
	TOTALPROPERTYGROSSPREMIUM, 
	GrossPremium AS CYBERGROSSPREMIUM, 
	NetPremium AS CYBERNETPREMIUM, 
	OCCUPANCYCODE, 
	POLICYTOTALINSUREDVALUE, 
	PREVIOUSPOLICYNUMBER, 
	AgencyCode AS AGENTCODE, 
	BRANCHCODE, 
	FIRSTPARTYLIMIT, 
	FIRSTPARTYDEDUCTIBLE, 
	FIRSTPARTYCOVERAGE, 
	THIRDPARTYLIMIT, 
	THIRDPARTYDEDUCTIBLE, 
	THIRDPARTYCOVERAGE, 
	EXTORTIONSUBLIMIT
	FROM FLT_GrossPremium
),