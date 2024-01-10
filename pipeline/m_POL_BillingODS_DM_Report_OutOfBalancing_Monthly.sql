WITH
SQ_BillingODS_Commission AS (
	SELECT PT.PolicyReference,
	PT.PolicyTermEffectiveDate,
	PT.PolicyTermExpirationDate, 
	CONVERT(decimal(19,2),SUM(CA.AuthorizedAmount)) AS Commission,
	max(A.AccountReference) as AccountReference
	FROM dbo.DCBILCommissionAuthorization CA
	INNER JOIN dbo.DCBILPolicyTerm PT ON CA.PolicyTermId = PT.PolicyTermID 
	INNER JOIN dbo.DCBILAccount A ON CA.AccountId=A.AccountId
	WHERE CA.TransactionTypeCode != 'WriteOffReversal'
	AND CA.TransactionTypeCode != 'WriteOff'
	AND CA.AuthorizationTypeCode = 'AUTO'
	AND CAST(CA.AuthorizationDateTime as date) between DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+DATEDIFF(MONTH,0,getdate()),0) and DATEADD(S,-1,DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+1+DATEDIFF(MONTH,0,getdate()),0))
	AND PT.PolicyIssueSystemCode = 'DCT'
	@{pipeline().parameters.WHERE_CLAUSE_BILLINGODS_COMM}
	GROUP BY PT.PolicyReference, PT.PolicyTermEffectiveDate, PT.PolicyTermExpirationDate
),
SQ_DM_Commission AS (
	SELECT C.pol_num,
	C.pol_eff_date,
	C.pol_exp_date,
	CONVERT(decimal(19,2),sum(PremiumMasterAgencyDirectWrittenCommission)) as PremiumMasterAgencyDirectWrittenCommission
	FROM dbo.PremiumMasterFact A
	INNER JOIN dbo.calendar_dim B
	ON A.PremiumMasterRunDateID = B.Clndr_id
	INNER JOIN dbo.policy_dim C
	ON C.Pol_dim_id = A.policyDimId and C.pol_sym='000'
	WHERE B.clndr_date between DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+DATEDIFF(MONTH,0,getdate()),0) and DATEADD(S,-1,DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+1+DATEDIFF(MONTH,0,getdate()),0))
	@{pipeline().parameters.WHERE_CLAUSE_DM_COMM}
	GROUP BY C.pol_num, C.pol_eff_date, C.pol_exp_date
),
JNR_Commission AS (SELECT
	SQ_BillingODS_Commission.PolicyReference, 
	SQ_BillingODS_Commission.PolicyTermEffectiveDate, 
	SQ_BillingODS_Commission.PolicyTermExpirationDate, 
	SQ_BillingODS_Commission.Commission, 
	SQ_BillingODS_Commission.AccountReference, 
	SQ_DM_Commission.pol_num, 
	SQ_DM_Commission.pol_eff_date, 
	SQ_DM_Commission.pol_exp_date, 
	SQ_DM_Commission.PremiumMasterAgencyDirectWrittenCommission
	FROM SQ_BillingODS_Commission
	FULL OUTER JOIN SQ_DM_Commission
	ON SQ_DM_Commission.pol_num = SQ_BillingODS_Commission.PolicyReference AND SQ_DM_Commission.pol_eff_date = SQ_BillingODS_Commission.PolicyTermEffectiveDate AND SQ_DM_Commission.pol_exp_date = SQ_BillingODS_Commission.PolicyTermExpirationDate
),
FIL_Commission AS (
	SELECT
	PolicyReference, 
	PolicyTermEffectiveDate, 
	PolicyTermExpirationDate, 
	Commission, 
	AccountReference, 
	pol_num, 
	pol_eff_date, 
	pol_exp_date, 
	PremiumMasterAgencyDirectWrittenCommission
	FROM JNR_Commission
	WHERE (
Commission<>PremiumMasterAgencyDirectWrittenCommission OR ISNULL(PolicyReference) OR ISNULL(pol_num)
) AND  NOT (
(ISNULL(Commission) AND PremiumMasterAgencyDirectWrittenCommission=0)
 OR 
(Commission=0 AND ISNULL(PremiumMasterAgencyDirectWrittenCommission))
)
),
EXP_Commission AS (
	SELECT
	PolicyReference AS i_PolicyReference,
	PolicyTermEffectiveDate AS i_PolicyTermEffectiveDate,
	PolicyTermExpirationDate AS i_PolicyTermExpirationDate,
	Commission AS i_Commission,
	AccountReference AS i_AccountReference,
	pol_num AS i_pol_num,
	pol_eff_date AS i_pol_eff_date,
	pol_exp_date AS i_pol_exp_date,
	PremiumMasterAgencyDirectWrittenCommission AS i_PremiumMasterAgencyDirectWrittenCommission,
	-- *INF*: ADD_TO_DATE(SESSSTARTTIME, 'MM', @{pipeline().parameters.NUMOFMONTH})
	DATEADD(MONTH,@{pipeline().parameters.NUMOFMONTH},SESSSTARTTIME) AS v_PreviousMonthDate,
	-- *INF*: TO_CHAR(v_PreviousMonthDate, 'Mon YYYY')
	TO_CHAR(v_PreviousMonthDate, 'Mon YYYY'
	) AS o_YearMonth,
	-- *INF*: IIF(ISNULL(i_PolicyReference), i_pol_num, i_PolicyReference)
	IFF(i_PolicyReference IS NULL,
		i_pol_num,
		i_PolicyReference
	) AS o_PolicyNumber,
	i_PolicyTermEffectiveDate AS o_SourceEffectiveDate,
	i_pol_eff_date AS o_TargetEffectiveDate,
	'Validate that $ Commission between BillingODS and DM match for month end reporting.' AS o_ComparisonType,
	i_Commission AS o_Amount_From_BillingODS,
	i_PremiumMasterAgencyDirectWrittenCommission AS o_Amount_From_DM,
	i_AccountReference AS o_BillingAcountNumber
	FROM FIL_Commission
),
SQ_BillingODS_DWP AS (
	SELECT PT.PolicyReference ,
	PT.PolicyTermEffectiveDate,
	PT.PolicyTermExpirationDate,
	CONVERT(DECIMAL(19, 2), COALESCE(SUM(BI.OriginalTransactionAmount - BI.TransferredAmount), 0)) AS DWP,
	max(A.AccountReference) as AccountReference
	FROM dbo.DCBILBillItem BI
	INNER JOIN dbo.DCBILPolicyTerm PT ON BI.PolicyTermId = PT.PolicyTermId
	INNER JOIN dbo.DCBILAccount A ON BI.AccountId=A.AccountId
	WHERE (Case when BI.ItemEffectiveDate>BI.TransactionDate then BI.ItemEffectiveDate else BI.TransactionDate end) between DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+DATEDIFF(MONTH,0,getdate()),0) and DATEADD(S,-1,DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+1+DATEDIFF(MONTH,0,getdate()),0))
	AND BI.ReceivableTypeCode = 'PREM'
	AND PT.PolicyIssueSystemCode = 'DCT'
	AND COALESCE(BI.OriginalTransactionAmount - BI.TransferredAmount, 0) <> 0
	@{pipeline().parameters.WHERE_CLAUSE_BILLINGODS_DWP}
	GROUP BY PT.PolicyReference
	, PT.PolicyTermEffectiveDate
	, PT.PolicyTermExpirationDate
),
SQ_DM_DWP AS (
	SELECT B.pol_num, 
	B.pol_eff_date, 
	B.pol_exp_date,
	CONVERT(decimal(19,2), sum(A.PremiumMasterDirectWrittenPremium)) AS PremiumMasterDirectWrittenPremium
	FROM dbo.PremiumMasterFact A
	INNER JOIN dbo.policy_dim B ON A.PolicyDimId = B.Pol_dim_id and B.pol_sym='000'
	INNER JOIN dbo.calendar_dim C ON A.PremiumMasterRunDateID = C.clndr_id
	WHERE C.CalendarDate between DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+DATEDIFF(MONTH,0,getdate()),0) and DATEADD(S,-1,DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+1+DATEDIFF(MONTH,0,getdate()),0))
	@{pipeline().parameters.WHERE_CLAUSE_DM_DWP}
	GROUP BY B.pol_num, B.pol_eff_date, B.pol_exp_date
),
JNR_DWP AS (SELECT
	SQ_BillingODS_DWP.PolicyReference, 
	SQ_BillingODS_DWP.PolicyTermEffectiveDate, 
	SQ_BillingODS_DWP.PolicyTermExpirationDate, 
	SQ_BillingODS_DWP.DWP, 
	SQ_BillingODS_DWP.AccountReference, 
	SQ_DM_DWP.pol_num, 
	SQ_DM_DWP.pol_eff_date, 
	SQ_DM_DWP.pol_exp_date, 
	SQ_DM_DWP.PremiumMasterDirectWrittenPremium
	FROM SQ_BillingODS_DWP
	FULL OUTER JOIN SQ_DM_DWP
	ON SQ_DM_DWP.pol_num = SQ_BillingODS_DWP.PolicyReference AND SQ_DM_DWP.pol_eff_date = SQ_BillingODS_DWP.PolicyTermEffectiveDate AND SQ_DM_DWP.pol_exp_date = SQ_BillingODS_DWP.PolicyTermExpirationDate
),
FIL_DWP AS (
	SELECT
	PolicyReference, 
	PolicyTermEffectiveDate, 
	PolicyTermExpirationDate, 
	DWP, 
	AccountReference, 
	pol_num, 
	pol_eff_date, 
	pol_exp_date, 
	PremiumMasterDirectWrittenPremium
	FROM JNR_DWP
	WHERE (
DWP<>PremiumMasterDirectWrittenPremium OR ISNULL(PolicyReference) OR ISNULL(pol_num)
) AND  
 NOT (
(ISNULL(DWP) AND PremiumMasterDirectWrittenPremium=0)
 OR 
(DWP=0 AND ISNULL(PremiumMasterDirectWrittenPremium))
)
),
EXP_DWP AS (
	SELECT
	PolicyReference AS i_PolicyReference,
	PolicyTermEffectiveDate AS i_PolicyTermEffectiveDate,
	PolicyTermExpirationDate AS i_PolicyTermExpirationDate,
	DWP AS i_DWP,
	AccountReference AS i_AccountReference,
	pol_num AS i_pol_num,
	pol_eff_date AS i_pol_eff_date,
	pol_exp_date AS i_pol_exp_date,
	PremiumMasterDirectWrittenPremium AS i_PremiumMasterDirectWrittenPremium,
	-- *INF*: ADD_TO_DATE(SESSSTARTTIME, 'MM', @{pipeline().parameters.NUMOFMONTH})
	DATEADD(MONTH,@{pipeline().parameters.NUMOFMONTH},SESSSTARTTIME) AS v_PreviousMonthDate,
	-- *INF*: TO_CHAR(v_PreviousMonthDate, 'Mon YYYY')
	TO_CHAR(v_PreviousMonthDate, 'Mon YYYY'
	) AS o_YearMonth,
	-- *INF*: IIF(ISNULL(i_PolicyReference), i_pol_num, i_PolicyReference)
	IFF(i_PolicyReference IS NULL,
		i_pol_num,
		i_PolicyReference
	) AS o_PolicyNumber,
	i_PolicyTermEffectiveDate AS o_SourceEffectiveDate,
	i_pol_eff_date AS o_TargetEffectiveDate,
	'Validate that $ DWP between BillingODS and DM match for month end reporting.' AS o_ComparisonType,
	i_DWP AS o_Amount_From_BillingODS,
	i_PremiumMasterDirectWrittenPremium AS o_Amount_From_DM,
	i_AccountReference AS o_BillingAccountNumber
	FROM FIL_DWP
),
SQ_BillingODS_PassThrough AS (
	SELECT PT.PolicyReference,
	PT.PolicyTermEffectiveDate,
	PT.PolicyTermExpirationDate,
	CONVERT(DECIMAL(19, 2), COALESCE(SUM(BI.OriginalTransactionAmount - BI.TransferredAmount), 0)) AS PassThroughAmount,
	max(A.AccountReference) as AccountReference
	FROM dbo.DCBILBillItem BI
	INNER JOIN dbo.DCBILPolicyTerm PT ON BI.PolicyTermId = PT.PolicyTermId
	INNER JOIN dbo.DCBILAccount A ON BI.AccountId=A.AccountId
	WHERE BI.ReceivableTypeCode = 'TAX'
	AND PT.PolicyIssueSystemCode = 'DCT'
	AND (Case when BI.ItemEffectiveDate>BI.TransactionDate then BI.ItemEffectiveDate else BI.TransactionDate end) between DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+DATEDIFF(MONTH,0,getdate()),0) and DATEADD(S,-1,DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+1+DATEDIFF(MONTH,0,getdate()),0))
	@{pipeline().parameters.WHERE_CLAUSE_BILLINGODS_PASS}
	GROUP BY PT.PolicyReference
	, PT.PolicyTermEffectiveDate
	, PT.PolicyTermExpirationDate
),
SQ_DM_PassThrough AS (
	SELECT B.pol_num,
	B.pol_eff_date,
	B.pol_exp_date,
	CONVERT(decimal(19,2), sum(PassThroughChargeTransactionAmount)) AS PassThroughChargeTransactionAmount
	FROM dbo.PassThroughChargeTransactionFact A
	INNER JOIN dbo.policy_dim B ON A.PolicyDimId = B.Pol_dim_id AND B.pol_sym='000'
	INNER JOIN dbo.calendar_dim C ON A.PassThroughChargeTransactionBookedDateId = C.clndr_id
	WHERE C.CalendarDate between DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+DATEDIFF(MONTH,0,getdate()),0) and DATEADD(S,-1,DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTH}+1+DATEDIFF(MONTH,0,getdate()),0))
	@{pipeline().parameters.WHERE_CLAUSE_DM_PASS}
	GROUP BY B.pol_num, B.pol_eff_date, B.pol_exp_date
),
JNR_PassThrough AS (SELECT
	SQ_BillingODS_PassThrough.PolicyReference, 
	SQ_BillingODS_PassThrough.PolicyTermEffectiveDate, 
	SQ_BillingODS_PassThrough.PolicyTermExpirationDate, 
	SQ_BillingODS_PassThrough.PassThroughAmount, 
	SQ_BillingODS_PassThrough.AccountReference, 
	SQ_DM_PassThrough.pol_num, 
	SQ_DM_PassThrough.pol_eff_date, 
	SQ_DM_PassThrough.pol_exp_date, 
	SQ_DM_PassThrough.PassThroughChargeTransactionAmount
	FROM SQ_BillingODS_PassThrough
	FULL OUTER JOIN SQ_DM_PassThrough
	ON SQ_DM_PassThrough.pol_num = SQ_BillingODS_PassThrough.PolicyReference AND SQ_DM_PassThrough.pol_eff_date = SQ_BillingODS_PassThrough.PolicyTermEffectiveDate AND SQ_DM_PassThrough.pol_exp_date = SQ_BillingODS_PassThrough.PolicyTermExpirationDate
),
FIL_PassThrough AS (
	SELECT
	PolicyReference, 
	PolicyTermEffectiveDate, 
	PolicyTermExpirationDate, 
	PassThroughAmount, 
	AccountReference, 
	pol_num, 
	pol_eff_date, 
	pol_exp_date, 
	PassThroughChargeTransactionAmount
	FROM JNR_PassThrough
	WHERE (
PassThroughAmount<>PassThroughChargeTransactionAmount OR ISNULL(PolicyReference) OR ISNULL(pol_num)
) AND  NOT (
(ISNULL(PassThroughAmount) AND PassThroughChargeTransactionAmount=0)
 OR 
(PassThroughAmount=0 AND ISNULL(PassThroughChargeTransactionAmount))
)
),
EXP_PassThrough AS (
	SELECT
	PolicyReference AS i_PolicyReference,
	PolicyTermEffectiveDate AS i_PolicyTermEffectiveDate,
	PolicyTermExpirationDate AS i_PolicyTermExpirationDate,
	PassThroughAmount AS i_PassThroughAmount,
	AccountReference AS i_AccountReference,
	pol_num AS i_pol_num,
	pol_eff_date AS i_pol_eff_date,
	pol_exp_date AS i_pol_exp_date,
	PassThroughChargeTransactionAmount AS i_PassThroughChargeTransactionAmount,
	-- *INF*: ADD_TO_DATE(SESSSTARTTIME, 'MM', @{pipeline().parameters.NUMOFMONTH})
	DATEADD(MONTH,@{pipeline().parameters.NUMOFMONTH},SESSSTARTTIME) AS v_PreviousMonthDate,
	-- *INF*: TO_CHAR(v_PreviousMonthDate, 'Mon YYYY')
	TO_CHAR(v_PreviousMonthDate, 'Mon YYYY'
	) AS o_YearMonth,
	-- *INF*: IIF(ISNULL(i_PolicyReference), i_pol_num, i_PolicyReference)
	IFF(i_PolicyReference IS NULL,
		i_pol_num,
		i_PolicyReference
	) AS o_PolicyNumber,
	i_PolicyTermEffectiveDate AS o_SourceEffectiveDate,
	i_pol_eff_date AS o_TargetEffectiveDate,
	'Validate that $ Pass Through between BillingODS and DM match for month end reporting.' AS o_ComparisonType,
	i_PassThroughAmount AS o_Amount_From_BillingODS,
	i_PassThroughChargeTransactionAmount AS o_Amount_From_DM,
	i_AccountReference AS o_BillingAccountNumber
	FROM FIL_PassThrough
),
Union AS (
	SELECT o_YearMonth AS YearMonth, o_PolicyNumber AS PolicyNumber, o_SourceEffectiveDate AS SourceEffectiveDate, o_TargetEffectiveDate AS TargetEffectiveDate, o_ComparisonType AS ComparisonType, o_Amount_From_BillingODS AS Amount_From_BillingODS, o_Amount_From_DM AS Amount_From_DM, o_BillingAccountNumber AS BillingAccountNumber
	FROM EXP_DWP
	UNION
	SELECT o_YearMonth AS YearMonth, o_PolicyNumber AS PolicyNumber, o_SourceEffectiveDate AS SourceEffectiveDate, o_TargetEffectiveDate AS TargetEffectiveDate, o_ComparisonType AS ComparisonType, o_Amount_From_BillingODS AS Amount_From_BillingODS, o_Amount_From_DM AS Amount_From_DM, o_BillingAccountNumber AS BillingAccountNumber
	FROM EXP_PassThrough
	UNION
	SELECT o_YearMonth AS YearMonth, o_PolicyNumber AS PolicyNumber, o_SourceEffectiveDate AS SourceEffectiveDate, o_TargetEffectiveDate AS TargetEffectiveDate, o_ComparisonType AS ComparisonType, o_Amount_From_BillingODS AS Amount_From_BillingODS, o_Amount_From_DM AS Amount_From_DM, o_BillingAcountNumber AS BillingAccountNumber
	FROM EXP_Commission
),
EXP_FileName AS (
	SELECT
	PolicyNumber AS i_PolicyNumber,
	SourceEffectiveDate,
	TargetEffectiveDate,
	ComparisonType,
	Amount_From_BillingODS,
	Amount_From_DM,
	BillingAccountNumber AS i_BillingAccountNumber,
	-- *INF*: IIF(ISNULL(Amount_From_BillingODS), 0, Amount_From_BillingODS)
	IFF(Amount_From_BillingODS IS NULL,
		0,
		Amount_From_BillingODS
	) AS v_Amount_From_BillingODS,
	-- *INF*: IIF(ISNULL(Amount_From_DM), 0, Amount_From_DM)
	IFF(Amount_From_DM IS NULL,
		0,
		Amount_From_DM
	) AS v_Amount_From_DM,
	-- *INF*: CHR(39) || i_BillingAccountNumber
	CHR(39
	) || i_BillingAccountNumber AS o_BillingAccountNumber,
	-- *INF*: IIF(SUBSTR(i_PolicyNumber, 1, 1)='0', CHR(39) || i_PolicyNumber, i_PolicyNumber)
	IFF(SUBSTR(i_PolicyNumber, 1, 1
		) = '0',
		CHR(39
		) || i_PolicyNumber,
		i_PolicyNumber
	) AS o_PolicyNumber,
	v_Amount_From_BillingODS - v_Amount_From_DM AS o_Difference
	FROM Union
),
LKP_wbmi_checkout AS (
	SELECT
	WBMIChecksAndBalancingRuleID,
	WBMIBalancingRuleDescription
	FROM (
		SELECT 
			WBMIChecksAndBalancingRuleID,
			WBMIBalancingRuleDescription
		FROM WBMIChecksAndBalancingRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WBMIBalancingRuleDescription ORDER BY WBMIChecksAndBalancingRuleID) = 1
),
TGT_BillingODS_DM_OutOfBalancing_Report AS (
	INSERT INTO BillingODS_DM_OutOfBalancing_Report
	(RuleId, RuleDescription, BillingAccountNumber, Policy, BillingODSAmount, DataMartAmount, Difference, BillingODSEffectiveDate, DataMartEffectiveDate)
	SELECT 
	LKP_wbmi_checkout.WBMIChecksAndBalancingRuleID AS RULEID, 
	EXP_FileName.ComparisonType AS RULEDESCRIPTION, 
	EXP_FileName.o_BillingAccountNumber AS BILLINGACCOUNTNUMBER, 
	EXP_FileName.o_PolicyNumber AS POLICY, 
	EXP_FileName.Amount_From_BillingODS AS BILLINGODSAMOUNT, 
	EXP_FileName.Amount_From_DM AS DATAMARTAMOUNT, 
	EXP_FileName.o_Difference AS DIFFERENCE, 
	EXP_FileName.SourceEffectiveDate AS BILLINGODSEFFECTIVEDATE, 
	EXP_FileName.TargetEffectiveDate AS DATAMARTEFFECTIVEDATE
	FROM EXP_FileName
),