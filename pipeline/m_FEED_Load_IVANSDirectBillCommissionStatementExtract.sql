WITH
SQ_AgencyODS AS (
	select LTRIM(RTRIM(agency.AgencyCode)) as AgencyCode,
	AssignedStateCode as AssignedStateCode,
	PayCode as PayCode
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency agency
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyPayCode apc
	on agency.AgencyCode=apc.AgencyCode
),
SQ_VWDCBILDirectBillCommissionStatementDetail AS (
	SET QUOTED_IDENTIFIER on
	
	select 
	dbcsd.Insured as Insured,
	--LTRIM(RTRIM(dbcsd.AccountReference)) as AccountReference, 
	LTRIM(RTRIM(agy.Reference)) as AccountReference, --added it as part of PROD-6520
	--pt.PolicyReference as PolicyReference,
	CASE WHEN pt.PolicyIssueSystemCode = 'PMS' THEN convert(xml,pt.PolicyTermExtendedData).value('(/PolicyTermExtendedData/PolicySymbol)[1]', 'nvarchar(10)') + pt.PolicyReference 
	                                         ELSE pt.PolicyReference  END as PolicyReference,
	
	pt.PolicyTermEffectiveDate as PolicyTermEffectiveDate,
	pt.PolicyTermExpirationDate as PolicyTermExpirationDate,
	ISNULL(dbcsd.ItemTierAmount,0) as ItemTierAmount,
	ISNULL(dbcsd.CommissionAuthorizedAmount,0) as CommissionAuthorizedAmount,
	ISNULL(dbcsd.ItemCommissionPercent,0) as ItemCommissionPercent,
	dbcsd.TransactionTypeCode as TransactionTypeCode,
	dbcss.StatementBeginDate as StatementBeginDate,
	a.AccountReference as AccountReference_Account,
	ca.AuthorizationDate as AuthorizationDate,
	DATEADD(MM,DATEDIFF(MM,0,@{pipeline().parameters.RUNDATE}),-1) as RunDate,
	pt.PolicyLineOfBusinessCode as PolicyLineOfBusinessCode
	from
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.VWDCBILDirectBillCommissionStatementDetail dbcsd
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBILDirectBillCommissionStatementSummary dbcss
	on dbcsd.DirectBillCommissionStatementSummaryId=dbcss.DirectBillCommissionStatementSummaryId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agency agy      --added it as part of PROD 6520
	on dbcss.AgencyReference=agy.AgencyID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBILPolicyTerm pt
	on dbcsd.PolicyTermId=pt.PolicyTermId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBILAccount a
	on pt.PrimaryAccountId=a.AccountId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBILCommissionAuthorization ca
	on ca.CommissionAuthorizationId=dbcsd.CommissionAuthorizationId
	where month(dbcss.StatementEndDate)=month(DATEADD(MM,DATEDIFF(MM,0,@{pipeline().parameters.RUNDATE}),-1)) and year(dbcss.StatementEndDate)=year(DATEADD(MM,DATEDIFF(MM,0,@{pipeline().parameters.RUNDATE}),-1)) and (ItemTierAmount<>0 OR AuthorizationTypeCode='MANU')
	@{pipeline().parameters.WHERE_CLAUSE}
),
JNR_IVANS AS (SELECT
	SQ_AgencyODS.AgencyCode, 
	SQ_AgencyODS.AssignedStateCode, 
	SQ_AgencyODS.PayCode, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.Insured, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.AccountReference, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.PolicyReference, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.PolicyTermEffectiveDate, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.PolicyTermExpirationDate, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.ItemTierAmount, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.CommissionAuthorizedAmount, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.ItemCommissionPercent, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.TransactionTypeCode, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.StatementBeginDate, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.AccountReference_Account, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.AuthorizationDate, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.RunDate, 
	SQ_VWDCBILDirectBillCommissionStatementDetail.PolicyLineOfBusinessCode
	FROM SQ_AgencyODS
	INNER JOIN SQ_VWDCBILDirectBillCommissionStatementDetail
	ON SQ_VWDCBILDirectBillCommissionStatementDetail.AccountReference = SQ_AgencyODS.AgencyCode
),
EXP_Rundate AS (
	SELECT
	Insured,
	AssignedStateCode,
	-- *INF*: --SUBSTR(AssignedStateCode,1,2)
	-- IIF ((AssignedStateCode = 'N/A'), '00', AssignedStateCode)
	-- 
	IFF((AssignedStateCode = 'N/A'), '00', AssignedStateCode) AS o_AssignedStateCode,
	PayCode,
	AccountReference,
	PolicyReference,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate,
	ItemTierAmount,
	CommissionAuthorizedAmount,
	ItemCommissionPercent,
	TransactionTypeCode,
	StatementBeginDate,
	AccountReference_Account,
	AuthorizationDate,
	RunDate,
	PolicyLineOfBusinessCode
	FROM JNR_IVANS
),
EXP_IVANS AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	RunDate,
	Insured,
	o_AssignedStateCode AS AssignedStateCode,
	PayCode,
	AccountReference AS i_AccountReference,
	-- *INF*: SUBSTR(LTRIM(RTRIM(i_AccountReference)),1,2)
	SUBSTR(LTRIM(RTRIM(i_AccountReference)), 1, 2) AS AgencyState,
	-- *INF*: SUBSTR(LTRIM(RTRIM(i_AccountReference)),3,3)
	SUBSTR(LTRIM(RTRIM(i_AccountReference)), 3, 3) AS o_AccountReference,
	-- *INF*: IIF ((SUBSTR(LTRIM(RTRIM(PolicyReference)),1,2) = 'HH'),
	--        SUBSTR(LTRIM(RTRIM(PolicyReference)),1,3), '000')
	-- 
	-- --'000'
	IFF(
	    (SUBSTR(LTRIM(RTRIM(PolicyReference)), 1, 2) = 'HH'),
	    SUBSTR(LTRIM(RTRIM(PolicyReference)), 1, 3),
	    '000'
	) AS o_PolicySymbol,
	PolicyReference,
	-- *INF*: IIF ((SUBSTR(LTRIM(RTRIM(PolicyReference)),1,2) = 'HH'),
	--        SUBSTR(LTRIM(RTRIM(PolicyReference)),4,7),
	--        SUBSTR(LTRIM(RTRIM(PolicyReference)),1,7))
	IFF(
	    (SUBSTR(LTRIM(RTRIM(PolicyReference)), 1, 2) = 'HH'),
	    SUBSTR(LTRIM(RTRIM(PolicyReference)), 4, 7),
	    SUBSTR(LTRIM(RTRIM(PolicyReference)), 1, 7)
	) AS o_PolicyReference,
	PolicyTermEffectiveDate,
	PolicyTermExpirationDate,
	ItemTierAmount,
	CommissionAuthorizedAmount,
	ItemCommissionPercent,
	TransactionTypeCode AS i_TransactionTypeCode,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(i_TransactionTypeCode))='RNEW','R',
	-- LTRIM(RTRIM(i_TransactionTypeCode))='ENDT','A',
	-- LTRIM(RTRIM(i_TransactionTypeCode))='WO','UU',
	-- IN(LTRIM(RTRIM(i_TransactionTypeCode)),'PCAN','FCAN','REIN'),'C',
	-- LTRIM(RTRIM(i_TransactionTypeCode))='NBUS','N',
	-- 'UU'
	-- )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(i_TransactionTypeCode)) = 'RNEW', 'R',
	    LTRIM(RTRIM(i_TransactionTypeCode)) = 'ENDT', 'A',
	    LTRIM(RTRIM(i_TransactionTypeCode)) = 'WO', 'UU',
	    LTRIM(RTRIM(i_TransactionTypeCode)) IN ('PCAN','FCAN','REIN'), 'C',
	    LTRIM(RTRIM(i_TransactionTypeCode)) = 'NBUS', 'N',
	    'UU'
	) AS o_TransactionTypeCode,
	StatementBeginDate,
	AccountReference_Account,
	-- *INF*: SUBSTR(LTRIM(RTRIM(AccountReference_Account)),1,LENGTH(LTRIM(RTRIM(AccountReference_Account)))-2)
	SUBSTR(LTRIM(RTRIM(AccountReference_Account)), 1, LENGTH(LTRIM(RTRIM(AccountReference_Account))) - 2) AS o_CustomerNumber,
	AuthorizationDate,
	-- *INF*: IIF ((i_TransactionTypeCode = 'FAUD' OR i_TransactionTypeCode = 'RAUD') OR  
	--  (AuthorizationDate > PolicyTermExpirationDate),
	--  PolicyTermExpirationDate, AuthorizationDate)
	-- 
	-- --(i_TransactionTypeCode = 'ENDT' AND (AuthorizationDate > --PolicyTermExpirationDate))
	-- 
	IFF(
	    (i_TransactionTypeCode = 'FAUD'
	    or i_TransactionTypeCode = 'RAUD')
	    or (AuthorizationDate > PolicyTermExpirationDate),
	    PolicyTermExpirationDate,
	    AuthorizationDate
	) AS o_AuthorizationDate,
	PolicyLineOfBusinessCode AS i_PolicyLineOfBusinessCode,
	-- *INF*: DECODE(LTRIM(RTRIM(i_PolicyLineOfBusinessCode)),
	-- 'AUTO','AUTOB',
	-- 'BOP','BOP',
	-- 'CODO','CGL',
	-- 'CRME','CRIM',
	-- 'EXLI','CGL',
	-- 'GL','CGL',
	-- 'GOCH','INMRC',
	-- 'HOLE','INMRC',
	-- 'IM','INMRC',
	-- 'LIAB','CGL',
	-- 'MEPL','CGL',
	-- 'NPDO','CGL',
	-- 'PACK','CPKGE',
	-- 'HH','PPKGE',
	-- 'PPKG','PPKGE',
	-- 'PROP','PROP',
	-- 'UMBR','CUMBR',
	-- 'WC','WORK',
	-- 'BND','BONDS',
	-- 'EVEN','CGL',
	-- 'TBD')
	DECODE(
	    LTRIM(RTRIM(i_PolicyLineOfBusinessCode)),
	    'AUTO', 'AUTOB',
	    'BOP', 'BOP',
	    'CODO', 'CGL',
	    'CRME', 'CRIM',
	    'EXLI', 'CGL',
	    'GL', 'CGL',
	    'GOCH', 'INMRC',
	    'HOLE', 'INMRC',
	    'IM', 'INMRC',
	    'LIAB', 'CGL',
	    'MEPL', 'CGL',
	    'NPDO', 'CGL',
	    'PACK', 'CPKGE',
	    'HH', 'PPKGE',
	    'PPKG', 'PPKGE',
	    'PROP', 'PROP',
	    'UMBR', 'CUMBR',
	    'WC', 'WORK',
	    'BND', 'BONDS',
	    'EVEN', 'CGL',
	    'TBD'
	) AS o_LineOfBusinessCode
	FROM EXP_Rundate
),
IVANSDirectBillCommissionStatementExtract AS (

	------------ PRE SQL ----------
	delete from @{pipeline().parameters.TARGET_TABLE_OWNER}.IVANSDirectBillCommissionStatementExtract where RunDate=DATEADD(MM,DATEDIFF(MM,0,@{pipeline().parameters.RUNDATE}),-1)
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.IVANSDirectBillCommissionStatementExtract
	(AuditId, CreatedDate, RunDate, InsuredName, AgencyState, AgencyPayCode, AgencyNumber, PolicySymbol, PolicyNumber, PolicyEffectiveDate, PolicyExpirationDate, Premium, CommissionAmount, CommissionRate, TransactionCode, AccountDate, CustomerNumber, TransactionDate, LineOfBusinessCode)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	RUNDATE, 
	Insured AS INSUREDNAME, 
	AGENCYSTATE, 
	PayCode AS AGENCYPAYCODE, 
	o_AccountReference AS AGENCYNUMBER, 
	o_PolicySymbol AS POLICYSYMBOL, 
	o_PolicyReference AS POLICYNUMBER, 
	PolicyTermEffectiveDate AS POLICYEFFECTIVEDATE, 
	PolicyTermExpirationDate AS POLICYEXPIRATIONDATE, 
	ItemTierAmount AS PREMIUM, 
	CommissionAuthorizedAmount AS COMMISSIONAMOUNT, 
	ItemCommissionPercent AS COMMISSIONRATE, 
	o_TransactionTypeCode AS TRANSACTIONCODE, 
	StatementBeginDate AS ACCOUNTDATE, 
	o_CustomerNumber AS CUSTOMERNUMBER, 
	o_AuthorizationDate AS TRANSACTIONDATE, 
	o_LineOfBusinessCode AS LINEOFBUSINESSCODE
	FROM EXP_IVANS
),