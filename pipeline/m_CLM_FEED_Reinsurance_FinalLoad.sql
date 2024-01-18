WITH
SQ_SapiensReinsuranceClaim AS (
	SELECT
		SapiensReinsuranceClaimId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		ClaimNumber,
		PolicyKey,
		ProductCode,
		AccountingProductCode,
		StrategicProfitCenterAbbreviation,
		ASLCode,
		SubASLCode,
		InsuranceReferenceLineOfBusinessCode,
		RiskStateCode,
		SubClaim,
		FinancialTypeCode,
		FinancialTypeCodeDescription,
		CauseOfLoss,
		ClaimantNumber,
		ClaimantFullName,
		ClaimLossDate,
		ClaimReportedDate,
		ClaimCatastropheCode,
		ClaimCatastropheStartDate,
		ClaimTransactionDate,
		TransactionCode,
		TransactionCodeDescription,
		TransactionType,
		TransactionAmount,
		TransactionHistoryAmount,
		SourceSequenceNumber,
		TransactionNumber,
		WorkersCompensationMedicalLossPaid,
		WorkersCompensationMedicalExpensePaid,
		WorkersCompensationIndemnityExpensePaid,
		WorkersCompensationIndemnityLossPaid,
		PropertyCasualtyExpensePaid,
		PropertyCasualtyLossPaid,
		WorkersCompensationMedicalLossOutstanding,
		WorkersCompensationMedicalExpenseOutstanding,
		WorkersCompensationIndemnityExpenseOutstanding,
		WorkersCompensationIndemnityLossOutstanding,
		PropertyCasualtyExpenseOutstanding,
		PropertyCasualtyLossOutstanding,
		ContainsOutstandingReserveAmountFlag,
		ReinsuranceUmbrellaLayer,
		ClaimRelationshipId,
		ClaimTransactionCategory,
		SourceSystemID
	FROM SapiensReinsuranceClaim
	WHERE SapiensReinsuranceClaim.TransactionAmount <> 0.0 OR SapiensReinsuranceClaim.TransactionType = 'Reserve'
),
EXP_SRC AS (
	SELECT
	ClaimNumber,
	PolicyKey,
	ProductCode,
	AccountingProductCode,
	StrategicProfitCenterAbbreviation,
	ASLCode,
	SubASLCode AS in_SubASLCode,
	-- *INF*: IIF(RTRIM(in_SubASLCode)='',
	-- NULL,
	-- in_SubASLCode)
	IFF(RTRIM(in_SubASLCode) = '', NULL, in_SubASLCode) AS v_SubASLCode,
	v_SubASLCode AS out_SubASLCode,
	InsuranceReferenceLineOfBusinessCode,
	RiskStateCode,
	SubClaim,
	FinancialTypeCode,
	FinancialTypeCodeDescription,
	CauseOfLoss,
	ClaimantNumber,
	ClaimantFullName,
	ClaimLossDate,
	ClaimReportedDate,
	ClaimCatastropheCode,
	ClaimCatastropheStartDate,
	ClaimTransactionDate,
	TransactionCode,
	TransactionCodeDescription,
	TransactionType,
	TransactionAmount,
	TransactionHistoryAmount,
	SourceSequenceNumber,
	TransactionNumber,
	WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid,
	WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding,
	ContainsOutstandingReserveAmountFlag,
	ReinsuranceUmbrellaLayer,
	ClaimRelationshipId,
	SapiensReinsuranceClaimId,
	AuditId,
	-- *INF*: IIF(ISNULL(@{pipeline().parameters.NUMBEROFPOLICYQUEUES}) or @{pipeline().parameters.NUMBEROFPOLICYQUEUES}=0,
	-- 1,
	-- @{pipeline().parameters.NUMBEROFPOLICYQUEUES}
	-- )
	IFF(@{pipeline().parameters.NUMBEROFPOLICYQUEUES} IS NULL or @{pipeline().parameters.NUMBEROFPOLICYQUEUES} = 0, 1, @{pipeline().parameters.NUMBEROFPOLICYQUEUES}) AS v_NumberOfPolicyQueues,
	-- *INF*: IIF(v_NumberOfPolicyQueues<=1 and (ISNULL(@{pipeline().parameters.NUMBEROFCLAIMSQUEUES}) or @{pipeline().parameters.NUMBEROFCLAIMSQUEUES}<=1),
	-- 	1,	--Keep claims in the same queue as policies when both values are effectively 1
	-- 	v_NumberOfPolicyQueues
	-- 	+
	-- 	IIF(ISNULL(@{pipeline().parameters.NUMBEROFCLAIMSQUEUES}) or @{pipeline().parameters.NUMBEROFCLAIMSQUEUES}<=1,
	-- 		1,
	-- 		MOD(TO_INTEGER(REVERSE(SUBSTR(REVERSE(ClaimNumber),1,2))), @{pipeline().parameters.NUMBEROFCLAIMSQUEUES}) + 1
	-- 	)
	-- )
	IFF(
	    v_NumberOfPolicyQueues <= 1 and (@{pipeline().parameters.NUMBEROFCLAIMSQUEUES} IS NULL or @{pipeline().parameters.NUMBEROFCLAIMSQUEUES} <= 1),
	    1,
	    v_NumberOfPolicyQueues + 
	    IFF(
	        @{pipeline().parameters.NUMBEROFCLAIMSQUEUES} IS NULL
	    or @{pipeline().parameters.NUMBEROFCLAIMSQUEUES} <= 1, 1,
	        MOD(CAST(REVERSE(SUBSTR(REVERSE(ClaimNumber), 1, 2)) AS INTEGER), @{pipeline().parameters.NUMBEROFCLAIMSQUEUES}) + 1
	    )
	) AS ClaimsQueueNumber,
	ClaimTransactionCategory,
	SourceSystemID
	FROM SQ_SapiensReinsuranceClaim
),
RTR_Payments_Reserves AS (
	SELECT
	ClaimNumber,
	PolicyKey,
	ProductCode,
	AccountingProductCode,
	StrategicProfitCenterAbbreviation,
	ASLCode,
	out_SubASLCode AS SubASLCode,
	InsuranceReferenceLineOfBusinessCode,
	RiskStateCode,
	SubClaim,
	FinancialTypeCode,
	CauseOfLoss,
	ClaimantNumber,
	ClaimantFullName,
	ClaimLossDate,
	ClaimReportedDate,
	ClaimCatastropheCode,
	ClaimCatastropheStartDate,
	ClaimTransactionDate,
	TransactionCode,
	TransactionCodeDescription,
	TransactionType,
	TransactionAmount,
	TransactionHistoryAmount,
	SourceSequenceNumber,
	TransactionNumber,
	WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid,
	WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding,
	ContainsOutstandingReserveAmountFlag AS ContainsOutstandingReserveAmount,
	ReinsuranceUmbrellaLayer,
	ClaimRelationshipId,
	SapiensReinsuranceClaimId,
	AuditId,
	ClaimsQueueNumber,
	ClaimTransactionCategory,
	SourceSystemID
	FROM EXP_SRC
),
RTR_Payments_Reserves_ReinsurancePayments AS (SELECT * FROM RTR_Payments_Reserves WHERE TransactionType='Payment'),
RTR_Payments_Reserves_ReinsuranceReserves AS (SELECT * FROM RTR_Payments_Reserves WHERE TransactionType='Reserve' and ContainsOutstandingReserveAmount='T'),
SRT_BySubClaim AS (
	SELECT
	SubClaim AS SubClaim3, 
	SourceSequenceNumber AS SourceSequenceNumber3, 
	ClaimNumber AS ClaimNumber3, 
	PolicyKey AS PolicyKey3, 
	ProductCode AS ProductCode3, 
	AccountingProductCode AS AccountingProductCode3, 
	StrategicProfitCenterAbbreviation AS StrategicProfitCenterAbbreviation3, 
	ASLCode AS ASLCode3, 
	SubASLCode AS SubASLCode3, 
	InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode3, 
	RiskStateCode AS RiskStateCode3, 
	FinancialTypeCode AS FinancialTypeCode3, 
	CauseOfLoss AS CauseOfLoss3, 
	ClaimantNumber AS ClaimantNumber3, 
	ClaimantFullName AS ClaimantFullName3, 
	ClaimLossDate AS ClaimLossDate3, 
	ClaimReportedDate AS ClaimReportedDate3, 
	ClaimCatastropheCode AS ClaimCatastropheCode3, 
	ClaimCatastropheStartDate AS ClaimCatastropheStartDate3, 
	ClaimTransactionDate AS ClaimTransactionDate3, 
	TransactionCode AS TransactionCode3, 
	TransactionCodeDescription AS TransactionCodeDescription3, 
	TransactionType AS TransactionType3, 
	TransactionAmount AS TransactionAmount3, 
	TransactionHistoryAmount AS TransactionHistoryAmount3, 
	TransactionNumber AS TransactionNumber3, 
	WorkersCompensationMedicalLossOutstanding AS WorkersCompensationMedicalLossOutstanding3, 
	WorkersCompensationMedicalExpenseOutstanding AS WorkersCompensationMedicalExpenseOutstanding3, 
	WorkersCompensationIndemnityExpenseOutstanding AS WorkersCompensationIndemnityExpenseOutstanding3, 
	WorkersCompensationIndemnityLossOutstanding AS WorkersCompensationIndemnityLossOutstanding3, 
	PropertyCasualtyExpenseOutstanding AS PropertyCasualtyExpenseOutstanding3, 
	PropertyCasualtyLossOutstanding AS PropertyCasualtyLossOutstanding3, 
	ContainsOutstandingReserveAmount AS ContainsOutstandingReserveAmount3, 
	ReinsuranceUmbrellaLayer AS ReinsuranceUmbrellaLayer3, 
	ClaimRelationshipId AS ClaimRelationshipId3, 
	SapiensReinsuranceClaimId AS SapiensReinsuranceClaimId3, 
	AuditId AS AuditId3, 
	ClaimsQueueNumber AS ClaimsQueueNumber3
	FROM RTR_Payments_Reserves_ReinsuranceReserves
	ORDER BY SubClaim3 ASC, SourceSequenceNumber3 ASC
),
EXP_SameSSNAcrossFinancialTypes AS (
	SELECT
	SubClaim3,
	ClaimNumber3,
	PolicyKey3,
	ProductCode3,
	AccountingProductCode3,
	StrategicProfitCenterAbbreviation3,
	ASLCode3,
	SubASLCode3,
	InsuranceReferenceLineOfBusinessCode3,
	RiskStateCode3,
	FinancialTypeCode3,
	CauseOfLoss3,
	ClaimantNumber3,
	ClaimantFullName3,
	ClaimLossDate3,
	ClaimReportedDate3,
	ClaimCatastropheCode3,
	ClaimCatastropheStartDate3,
	ClaimTransactionDate3,
	TransactionCode3,
	TransactionCodeDescription3,
	TransactionType3,
	TransactionAmount3,
	TransactionHistoryAmount3,
	SourceSequenceNumber3,
	TransactionNumber3,
	WorkersCompensationMedicalLossOutstanding3,
	WorkersCompensationMedicalExpenseOutstanding3,
	WorkersCompensationIndemnityExpenseOutstanding3,
	WorkersCompensationIndemnityLossOutstanding3,
	PropertyCasualtyExpenseOutstanding3,
	PropertyCasualtyLossOutstanding3,
	ContainsOutstandingReserveAmount3,
	ReinsuranceUmbrellaLayer3,
	ClaimRelationshipId3,
	SapiensReinsuranceClaimId3,
	AuditId3,
	SYSDATE AS CurrentDateTime,
	ClaimsQueueNumber3,
	-- *INF*: IIF(SubClaim3=v_PreviousSubClaim,
	-- v_PreviousSSN,
	-- SourceSequenceNumber3)
	IFF(SubClaim3 = v_PreviousSubClaim, v_PreviousSSN, SourceSequenceNumber3) AS v_SSN,
	v_SSN AS o_SSN,
	v_SSN AS v_PreviousSSN,
	-- *INF*: IIF(SubClaim3=v_PreviousSubClaim,
	-- v_PreviousTransactionNumber,
	-- TransactionNumber3)
	IFF(SubClaim3 = v_PreviousSubClaim, v_PreviousTransactionNumber, TransactionNumber3) AS v_TransactionNumber,
	v_TransactionNumber AS o_TransactionNumber,
	v_TransactionNumber AS v_PreviousTransactionNumber,
	SubClaim3 AS v_PreviousSubClaim
	FROM SRT_BySubClaim
),
LKP_ArchSapiensReinsuranceClaim AS (
	SELECT
	ArchSapiensReinsuranceClaimId,
	i_SapiensReinsuranceClaimId,
	i_AuditId,
	SapiensReinsuranceClaimId,
	AuditId
	FROM (
		SELECT 
			ArchSapiensReinsuranceClaimId,
			i_SapiensReinsuranceClaimId,
			i_AuditId,
			SapiensReinsuranceClaimId,
			AuditId
		FROM ArchSapiensReinsuranceClaim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SapiensReinsuranceClaimId,AuditId ORDER BY ArchSapiensReinsuranceClaimId) = 1
),
UPD_ArchSapiensReinsuranceClaim AS (
	SELECT
	LKP_ArchSapiensReinsuranceClaim.ArchSapiensReinsuranceClaimId, 
	EXP_SameSSNAcrossFinancialTypes.o_SSN AS SourceSequenceNumber, 
	EXP_SameSSNAcrossFinancialTypes.o_TransactionNumber AS TransactionNumber, 
	EXP_SameSSNAcrossFinancialTypes.CurrentDateTime
	FROM EXP_SameSSNAcrossFinancialTypes
	LEFT JOIN LKP_ArchSapiensReinsuranceClaim
	ON LKP_ArchSapiensReinsuranceClaim.SapiensReinsuranceClaimId = EXP_SameSSNAcrossFinancialTypes.SapiensReinsuranceClaimId3 AND LKP_ArchSapiensReinsuranceClaim.AuditId = EXP_SameSSNAcrossFinancialTypes.AuditId3
),
ArchSapiensReinsuranceClaim AS (
	MERGE INTO ArchSapiensReinsuranceClaim AS T
	USING UPD_ArchSapiensReinsuranceClaim AS S
	ON T.ArchSapiensReinsuranceClaimId = S.ArchSapiensReinsuranceClaimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.CurrentDateTime, T.SourceSequenceNumber = S.SourceSequenceNumber, T.TransactionNumber = S.TransactionNumber
),
EXP_Accounting_Claims_Payments AS (
	SELECT
	SourceSequenceNumber AS Header_Source_Seq_Num,
	FinancialTypeCode AS i_FinancialTypeCode,
	ProductCode AS i_ProductCode,
	CauseOfLoss AS i_CauseOfLoss,
	-- *INF*: DECODE(TRUE,
	-- 	INDEXOF(i_FinancialTypeCode,'D','E') > 0,
	-- 	DECODE(TRUE,
	-- 		i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 		'WCM',
	-- 		i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 		'WME',
	-- 		i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 		'WCE',
	-- 		i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 		'WCI',
	-- 		i_FinancialTypeCode = 'D' AND i_ProductCode <> '100',
	-- 		'IDM',
	-- 		i_FinancialTypeCode = 'E' AND i_ProductCode <> '100',
	-- 		'EXP',
	-- 		''),
	-- 	INDEXOF(i_FinancialTypeCode,'R','S','B') > 0,
	-- 	DECODE(TRUE,
	-- 		ClaimTransactionCategory <> 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 		'WCM',
	-- 		ClaimTransactionCategory = 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 		'WME',
	-- 		ClaimTransactionCategory = 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 		'WCE',
	-- 		ClaimTransactionCategory <> 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 		'WCI',
	-- 		ClaimTransactionCategory <> 'EX' AND i_ProductCode <> '100',
	-- 		'IDM',
	-- 		ClaimTransactionCategory = 'EX' AND i_ProductCode <> '100',
	-- 		'EXP',
	-- 		'')
	-- )
	DECODE(
	    TRUE,
	    INDEXOF(i_FinancialTypeCode, 'D', 'E') > 0, DECODE(
	        TRUE,
	        i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', 'WCM',
	        i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', 'WME',
	        i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', 'WCE',
	        i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', 'WCI',
	        i_FinancialTypeCode = 'D' AND i_ProductCode <> '100', 'IDM',
	        i_FinancialTypeCode = 'E' AND i_ProductCode <> '100', 'EXP',
	        ''
	    ),
	    INDEXOF(i_FinancialTypeCode, 'R', 'S', 'B') > 0, DECODE(
	        TRUE,
	        ClaimTransactionCategory <> 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', 'WCM',
	        ClaimTransactionCategory = 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', 'WME',
	        ClaimTransactionCategory = 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', 'WCE',
	        ClaimTransactionCategory <> 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', 'WCI',
	        ClaimTransactionCategory <> 'EX' AND i_ProductCode <> '100', 'IDM',
	        ClaimTransactionCategory = 'EX' AND i_ProductCode <> '100', 'EXP',
	        ''
	    )
	) AS o_Accounting_Item,
	WorkersCompensationMedicalLossPaid AS i_WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid AS i_WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid AS i_WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid AS i_WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid AS i_PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid AS i_PropertyCasualtyLossPaid,
	-- *INF*: DECODE(TRUE,
	-- 	INDEXOF(i_FinancialTypeCode,'D','E') > 0,
	-- 	DECODE(TRUE,
	-- 		i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 		i_WorkersCompensationMedicalLossPaid,
	-- 		i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 		i_WorkersCompensationMedicalExpensePaid,
	-- 		i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 		i_WorkersCompensationIndemnityExpensePaid,
	-- 		i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 		i_WorkersCompensationIndemnityLossPaid,
	-- 		i_FinancialTypeCode = 'D' AND i_ProductCode <> '100',
	-- 		i_PropertyCasualtyLossPaid,
	-- 		i_FinancialTypeCode = 'E' AND i_ProductCode <> '100',
	-- 		i_PropertyCasualtyExpensePaid,
	-- 		NULL),
	-- 	INDEXOF(i_FinancialTypeCode,'R','S','B') > 0,
	-- 	DECODE(TRUE,
	-- 		ClaimTransactionCategory <> 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 		i_WorkersCompensationMedicalLossPaid,
	-- 		ClaimTransactionCategory = 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 		i_WorkersCompensationMedicalExpensePaid,
	-- 		ClaimTransactionCategory = 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 		i_WorkersCompensationIndemnityExpensePaid,
	-- 		ClaimTransactionCategory <> 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 		i_WorkersCompensationIndemnityLossPaid,
	-- 		ClaimTransactionCategory <> 'EX' AND i_ProductCode <> '100',
	-- 		i_PropertyCasualtyLossPaid,
	-- 		ClaimTransactionCategory = 'EX' AND i_ProductCode <> '100',
	-- 		i_PropertyCasualtyExpensePaid,
	-- 		NULL)
	-- )
	DECODE(
	    TRUE,
	    INDEXOF(i_FinancialTypeCode, 'D', 'E') > 0, DECODE(
	        TRUE,
	        i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', i_WorkersCompensationMedicalLossPaid,
	        i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', i_WorkersCompensationMedicalExpensePaid,
	        i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', i_WorkersCompensationIndemnityExpensePaid,
	        i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', i_WorkersCompensationIndemnityLossPaid,
	        i_FinancialTypeCode = 'D' AND i_ProductCode <> '100', i_PropertyCasualtyLossPaid,
	        i_FinancialTypeCode = 'E' AND i_ProductCode <> '100', i_PropertyCasualtyExpensePaid,
	        NULL
	    ),
	    INDEXOF(i_FinancialTypeCode, 'R', 'S', 'B') > 0, DECODE(
	        TRUE,
	        ClaimTransactionCategory <> 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', i_WorkersCompensationMedicalLossPaid,
	        ClaimTransactionCategory = 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', i_WorkersCompensationMedicalExpensePaid,
	        ClaimTransactionCategory = 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', i_WorkersCompensationIndemnityExpensePaid,
	        ClaimTransactionCategory <> 'EX' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', i_WorkersCompensationIndemnityLossPaid,
	        ClaimTransactionCategory <> 'EX' AND i_ProductCode <> '100', i_PropertyCasualtyLossPaid,
	        ClaimTransactionCategory = 'EX' AND i_ProductCode <> '100', i_PropertyCasualtyExpensePaid,
	        NULL
	    )
	) AS o_Accounting_Amount,
	'USD' AS Currency_Code,
	ClaimTransactionCategory,
	SourceSystemID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM RTR_Payments_Reserves_ReinsurancePayments
),
SapiensReinsuranceAccountingItemsExtract_Payments AS (
	TRUNCATE TABLE SapiensReinsuranceAccountingItemsExtract;
	INSERT INTO SapiensReinsuranceAccountingItemsExtract
	(SOURCE_SEQ_NUM, ACCOUNTING_ITEM, ACOUNTING_AMOUNT, CURRENCY_CODE, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	o_Accounting_Item AS ACCOUNTING_ITEM, 
	o_Accounting_Amount AS ACOUNTING_AMOUNT, 
	Currency_Code AS CURRENCY_CODE, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_Accounting_Claims_Payments
),
EXP_Dates_Claims_Payments AS (
	SELECT
	SourceSequenceNumber,
	'LSS' AS LossDateCode,
	ClaimLossDate AS i_ClaimLossDate,
	-- *INF*: TO_CHAR(i_ClaimLossDate,'YYYYMMDD')
	TO_CHAR(i_ClaimLossDate, 'YYYYMMDD') AS o_ClaimLossDate,
	'OSP' AS TransactionDateCode,
	ClaimTransactionDate AS i_ClaimTransactionDate,
	-- *INF*: TO_CHAR(i_ClaimTransactionDate,'YYYYMMDD')
	TO_CHAR(i_ClaimTransactionDate, 'YYYYMMDD') AS o_ClaimTransactionDate,
	'OPC' AS ClaimReportedDateCode,
	ClaimReportedDate AS i_ClaimReportedDate,
	-- *INF*: TO_CHAR(i_ClaimReportedDate,'YYYYMMDD')
	TO_CHAR(i_ClaimReportedDate, 'YYYYMMDD') AS o_ClaimReportedDate,
	'PRC' AS ProcessDateCode,
	-- *INF*: TO_CHAR(i_ClaimTransactionDate,'YYYYMMDD')
	TO_CHAR(i_ClaimTransactionDate, 'YYYYMMDD') AS o_ProcessDate,
	'EVT' AS CatastropheDateCode,
	ClaimCatastropheStartDate AS i_ClaimCatastropheStartDate,
	-- *INF*: DECODE(TRUE,
	-- 	TO_CHAR(i_ClaimCatastropheStartDate,'YYYYMMDD')='18000101',
	-- 	NULL,
	-- 	TO_CHAR(i_ClaimCatastropheStartDate,'YYYYMMDD'))
	DECODE(
	    TRUE,
	    TO_CHAR(i_ClaimCatastropheStartDate, 'YYYYMMDD') = '18000101', NULL,
	    TO_CHAR(i_ClaimCatastropheStartDate, 'YYYYMMDD')
	) AS o_CatastropheDate
	FROM RTR_Payments_Reserves_ReinsurancePayments
),
NRM_Claims_Dates AS (
),
FIL_Claims_Dates AS (
	SELECT
	Header_Source_Seq_Num, 
	Date_Code, 
	Dave_Value AS Date_Value
	FROM NRM_Claims_Dates
	WHERE NOT ISNULL(Date_Value)
),
EXP_Claims_Dates_Tgt_DataCollect AS (
	SELECT
	Header_Source_Seq_Num,
	Date_Code,
	Date_Value AS in_Date_Value,
	-- *INF*: to_date(in_Date_Value,'YYYYMMDD')
	TO_TIMESTAMP(in_Date_Value, 'YYYYMMDD') AS out_Date_Value,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM FIL_Claims_Dates
),
SapiensReinsuranceDatesExtract_Payments AS (
	TRUNCATE TABLE SapiensReinsuranceDatesExtract;
	INSERT INTO SapiensReinsuranceDatesExtract
	(SOURCE_SEQ_NUM, DATE_CODE, DATE_VALUE, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	Date_Code AS DATE_CODE, 
	out_Date_Value AS DATE_VALUE, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_Claims_Dates_Tgt_DataCollect
),
AGG_To_SubClaim AS (
	SELECT
	SubClaim3,
	o_SSN AS SourceSequenceNumber,
	ClaimLossDate3,
	ClaimTransactionDate3,
	-- *INF*: MAX(ClaimTransactionDate3)
	MAX(ClaimTransactionDate3) AS o_ClaimTransactionDate,
	ClaimReportedDate3,
	ClaimCatastropheStartDate3,
	InsuranceReferenceLineOfBusinessCode3,
	ProductCode3,
	AccountingProductCode3,
	ASLCode3,
	SubASLCode3,
	StrategicProfitCenterAbbreviation3,
	RiskStateCode3,
	ClaimCatastropheCode3,
	PolicyKey3,
	o_TransactionNumber AS TransactionNumber3,
	ClaimNumber3,
	ReinsuranceUmbrellaLayer3,
	ClaimRelationshipId3,
	ClaimantFullName3,
	ClaimsQueueNumber3
	FROM EXP_SameSSNAcrossFinancialTypes
	GROUP BY SubClaim3
),
EXP_Dates_Claims_Reserves AS (
	SELECT
	SourceSequenceNumber,
	'LSS' AS LossDateCode,
	ClaimLossDate3 AS i_ClaimLossDate,
	-- *INF*: TO_CHAR(i_ClaimLossDate,'YYYYMMDD')
	TO_CHAR(i_ClaimLossDate, 'YYYYMMDD') AS o_ClaimLossDate,
	'OSP' AS TransactionDateCode,
	o_ClaimTransactionDate AS i_ClaimTransactionDate,
	-- *INF*: TO_CHAR(i_ClaimTransactionDate,'YYYYMMDD')
	TO_CHAR(i_ClaimTransactionDate, 'YYYYMMDD') AS o_ClaimTransactionDate,
	'OPC' AS ClaimReportedDateCode,
	ClaimReportedDate3 AS i_ClaimReportedDate,
	-- *INF*: TO_CHAR(i_ClaimReportedDate,'YYYYMMDD')
	TO_CHAR(i_ClaimReportedDate, 'YYYYMMDD') AS o_ClaimReportedDate,
	'PRC' AS ProcessDateCode,
	-- *INF*: TO_CHAR(i_ClaimTransactionDate,'YYYYMMDD')
	TO_CHAR(i_ClaimTransactionDate, 'YYYYMMDD') AS o_ProcessDate,
	'EVT' AS CatastropheDateCode,
	ClaimCatastropheStartDate3 AS i_ClaimCatastropheStartDate,
	-- *INF*: DECODE(TRUE,
	-- 	TO_CHAR(i_ClaimCatastropheStartDate,'YYYYMMDD')='18000101',
	-- 	NULL,
	-- 	TO_CHAR(i_ClaimCatastropheStartDate,'YYYYMMDD'))
	DECODE(
	    TRUE,
	    TO_CHAR(i_ClaimCatastropheStartDate, 'YYYYMMDD') = '18000101', NULL,
	    TO_CHAR(i_ClaimCatastropheStartDate, 'YYYYMMDD')
	) AS o_ClaimCatastropheDate
	FROM AGG_To_SubClaim
),
NRM_Claims_Dates1 AS (
),
FIL_Claims_Dates1 AS (
	SELECT
	Header_Source_Seq_Num, 
	Date_Code, 
	Dave_Value AS Date_Value
	FROM NRM_Claims_Dates1
	WHERE NOT ISNULL(Date_Value)
),
EXP_Claims_Dates_Tgt_DataCollect1 AS (
	SELECT
	Header_Source_Seq_Num,
	Date_Code,
	Date_Value AS in_Date_Value,
	-- *INF*: to_date(in_Date_Value,'YYYYMMDD')
	TO_TIMESTAMP(in_Date_Value, 'YYYYMMDD') AS out_Date_Value,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM FIL_Claims_Dates1
),
SapiensReinsuranceDatesExtract_Reserves AS (
	INSERT INTO SapiensReinsuranceDatesExtract
	(SOURCE_SEQ_NUM, DATE_CODE, DATE_VALUE, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	Date_Code AS DATE_CODE, 
	out_Date_Value AS DATE_VALUE, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_Claims_Dates_Tgt_DataCollect1
),
EXP_Header_Claims_Payments AS (
	SELECT
	SourceSequenceNumber AS Header_Source_Seq_Num,
	'SRL' AS Data_Source,
	'WBMI' AS Company_Code,
	PolicyKey AS pol_key,
	'' AS Object_ID,
	'' AS Header_Endorsement_No,
	TransactionNumber AS Header_TransactionNumber,
	'A' AS Header_Document_Type,
	ClaimNumber AS Claim_Number,
	SubClaim AS Sub_Claim_Id,
	'' AS Is_Borderaeu,
	'CED' AS Business_Ind,
	'' AS Exception_Ind,
	-- *INF*: IIF(ISNULL(@{pipeline().parameters.NUMBEROFCLAIMSQUEUES}) or @{pipeline().parameters.NUMBEROFCLAIMSQUEUES}=0,
	-- 1,
	-- @{pipeline().parameters.NUMBEROFCLAIMSQUEUES}
	-- )
	IFF(@{pipeline().parameters.NUMBEROFCLAIMSQUEUES} IS NULL or @{pipeline().parameters.NUMBEROFCLAIMSQUEUES} = 0, 1, @{pipeline().parameters.NUMBEROFCLAIMSQUEUES}) AS v_NumberOfClaimsQueues,
	ClaimsQueueNumber,
	'P&C' AS Business_Deprtmt,
	'' AS XOL_Allocation,
	'' AS Assumed_Company,
	ClaimTransactionDate AS i_Accounting_Date,
	-- *INF*: TO_INTEGER(TO_CHAR(i_Accounting_Date,'YYYYMM'))
	CAST(TO_CHAR(i_Accounting_Date, 'YYYYMM') AS INTEGER) AS o_AccountingMonth,
	'1' AS Subsystem_Id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM RTR_Payments_Reserves_ReinsurancePayments
),
SapiensReinsuranceHeaderExtract_Payments AS (
	TRUNCATE TABLE SapiensReinsuranceHeaderExtract;
	INSERT INTO SapiensReinsuranceHeaderExtract
	(SOURCE_SEQ_NUM, DATA_SOURCE, COMPANY_CODE, POLICY_NO, OBJECT_ID, ENDORSEMENT_NO, TRAN_NO, DOCUMENT_TYPE, CLAIM_ID, SUB_CLAIM_ID, IS_BORDERAEU, BUSINESS_IND, EXCEPTION_IND, QUEUE_NO, BUSINESS_DEPRTMT, XOL_ALLOCATION, ASSUMED_COMPANY, ACCOUNTING_MONTH, SUBSYSTEM_ID, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	Data_Source AS DATA_SOURCE, 
	Company_Code AS COMPANY_CODE, 
	pol_key AS POLICY_NO, 
	Object_ID AS OBJECT_ID, 
	Header_Endorsement_No AS ENDORSEMENT_NO, 
	Header_TransactionNumber AS TRAN_NO, 
	Header_Document_Type AS DOCUMENT_TYPE, 
	Claim_Number AS CLAIM_ID, 
	Sub_Claim_Id AS SUB_CLAIM_ID, 
	Is_Borderaeu AS IS_BORDERAEU, 
	Business_Ind AS BUSINESS_IND, 
	Exception_Ind AS EXCEPTION_IND, 
	ClaimsQueueNumber AS QUEUE_NO, 
	Business_Deprtmt AS BUSINESS_DEPRTMT, 
	XOL_Allocation AS XOL_ALLOCATION, 
	Assumed_Company AS ASSUMED_COMPANY, 
	o_AccountingMonth AS ACCOUNTING_MONTH, 
	Subsystem_Id AS SUBSYSTEM_ID, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_Header_Claims_Payments
),
EXP_Header_Claims_Reserves AS (
	SELECT
	SourceSequenceNumber AS Header_Source_Seq_Num,
	'SRL' AS Data_Source,
	'WBMI' AS Company_Code,
	PolicyKey3 AS pol_key,
	'' AS Object_ID,
	'' AS Header_Endorsement_No,
	TransactionNumber3 AS Header_TransactionNumber,
	'O' AS Header_Document_Type,
	ClaimNumber3 AS Claim_Number,
	SubClaim3 AS Sub_Claim_Id,
	'' AS Is_Borderaeu,
	'CED' AS Business_Ind,
	'' AS Exception_Ind,
	-- *INF*: IIF(ISNULL(@{pipeline().parameters.NUMBEROFPOLICYQUEUES}) or @{pipeline().parameters.NUMBEROFPOLICYQUEUES}=0,
	-- 1,
	-- @{pipeline().parameters.NUMBEROFPOLICYQUEUES}
	-- )
	IFF(@{pipeline().parameters.NUMBEROFPOLICYQUEUES} IS NULL or @{pipeline().parameters.NUMBEROFPOLICYQUEUES} = 0, 1, @{pipeline().parameters.NUMBEROFPOLICYQUEUES}) AS v_NumberOfPolicyQueues,
	ClaimsQueueNumber3 AS ClaimsQueueNumber,
	'P&C' AS Business_Deprtmt,
	'' AS XOL_Allocation,
	'' AS Assumed_Company,
	o_ClaimTransactionDate AS i_Accounting_Date,
	-- *INF*: TO_INTEGER(TO_CHAR(i_Accounting_Date,'YYYYMM'))
	CAST(TO_CHAR(i_Accounting_Date, 'YYYYMM') AS INTEGER) AS o_AccountingMonth,
	'1' AS Subsystem_Id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM AGG_To_SubClaim
),
SapiensReinsuranceHeaderExtract_Reserves AS (
	INSERT INTO SapiensReinsuranceHeaderExtract
	(SOURCE_SEQ_NUM, DATA_SOURCE, COMPANY_CODE, POLICY_NO, OBJECT_ID, ENDORSEMENT_NO, TRAN_NO, DOCUMENT_TYPE, CLAIM_ID, SUB_CLAIM_ID, IS_BORDERAEU, BUSINESS_IND, EXCEPTION_IND, QUEUE_NO, BUSINESS_DEPRTMT, XOL_ALLOCATION, ASSUMED_COMPANY, ACCOUNTING_MONTH, SUBSYSTEM_ID, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	Data_Source AS DATA_SOURCE, 
	Company_Code AS COMPANY_CODE, 
	pol_key AS POLICY_NO, 
	Object_ID AS OBJECT_ID, 
	Header_Endorsement_No AS ENDORSEMENT_NO, 
	Header_TransactionNumber AS TRAN_NO, 
	Header_Document_Type AS DOCUMENT_TYPE, 
	Claim_Number AS CLAIM_ID, 
	Sub_Claim_Id AS SUB_CLAIM_ID, 
	Is_Borderaeu AS IS_BORDERAEU, 
	Business_Ind AS BUSINESS_IND, 
	Exception_Ind AS EXCEPTION_IND, 
	ClaimsQueueNumber AS QUEUE_NO, 
	Business_Deprtmt AS BUSINESS_DEPRTMT, 
	XOL_Allocation AS XOL_ALLOCATION, 
	Assumed_Company AS ASSUMED_COMPANY, 
	o_AccountingMonth AS ACCOUNTING_MONTH, 
	Subsystem_Id AS SUBSYSTEM_ID, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_Header_Claims_Reserves
),
EXP_Attributes_Claims_Payments AS (
	SELECT
	ClaimLossDate AS i_ClaimLossDate,
	SourceSequenceNumber AS Header_Source_Seq_Num,
	'LOB' AS out_LOB_Code,
	InsuranceReferenceLineOfBusinessCode AS LOB_Value,
	'PDT' AS out_ProductCode_Code,
	ProductCode AS ProductCode_Value,
	'ACP' AS out_AccountingProduct_Code,
	AccountingProductCode,
	'ASL' AS out_asl_code_Code,
	ASLCode AS asl_code_Value,
	'SAS' AS out_sub_asl_code_Code,
	SubASLCode AS sub_asl_code_Value,
	'PCN' AS out_StrategicProfitCenter_Code,
	StrategicProfitCenterAbbreviation AS StrategicProfitCenter_Value,
	-- *INF*: REPLACECHR(false,
	-- 	IIF(StrategicProfitCenter_Value='Argent',
	-- 		'A',
	-- 		StrategicProfitCenter_Value)
	-- , ' ', '')
	REGEXP_REPLACE(
	    IFF(
	        StrategicProfitCenter_Value = 'Argent', 'A', StrategicProfitCenter_Value
	    ),' ','') AS out_StrategicProfitCenter_Value,
	'RKS' AS out_RiskState_Code,
	RiskStateCode AS RiskState_Value,
	-- *INF*: substr(RiskState_Value, 1, 50)
	substr(RiskState_Value, 1, 50) AS out_RiskState_Value,
	'COM' AS out_Company_Code,
	'WBMI' AS Company_Value,
	'SNA' AS O_UmbrellaLayer_Code,
	ReinsuranceUmbrellaLayer AS i_ReinsuranceUmbrellalayer,
	-- *INF*: IIF(NOT ISNULL(i_ReinsuranceUmbrellalayer),
	-- TO_CHAR(i_ReinsuranceUmbrellalayer),
	-- NULL)
	IFF(i_ReinsuranceUmbrellalayer IS NOT NULL, TO_CHAR(i_ReinsuranceUmbrellalayer), NULL) AS O_ReinsuranceUmbrellalayer,
	'EVT' AS o_Catastrophe_Attribute,
	'EVN' AS o_Catastrophe_Code,
	ClaimCatastropheCode AS i_ClaimCatastropheCode,
	-- *INF*: DECODE(TRUE,
	-- i_ClaimLossDate <
	-- TO_DATE('2018-01-01','YYYY-MM-DD'), NULL,
	-- i_ClaimCatastropheCode='N/A',
	-- NULL,
	-- 'CAT')
	-- -- Only send CAT for date of losses 2018 and after if there is a cat code
	DECODE(
	    TRUE,
	    i_ClaimLossDate < TO_TIMESTAMP('2018-01-01', 'YYYY-MM-DD'), NULL,
	    i_ClaimCatastropheCode = 'N/A', NULL,
	    'CAT'
	) AS o_ClaimCatastropheValue,
	-- *INF*: IIF(i_ClaimCatastropheCode='N/A',
	-- NULL,
	-- i_ClaimCatastropheCode)
	IFF(i_ClaimCatastropheCode = 'N/A', NULL, i_ClaimCatastropheCode) AS o_ClaimCatastropheCode,
	'HIS' AS o_HistoricalLoad_Code,
	ClaimTransactionDate,
	-- *INF*: @{pipeline().parameters.HIS_VALUE}
	-- --IIF(RTRIM(@{pipeline().parameters.ACCOUNTINGFLAG})='', NULL, @{pipeline().parameters.ACCOUNTINGFLAG})
	@{pipeline().parameters.HIS_VALUE} AS o_HistoricalLoadValue,
	'EVR' AS o_ClaimRelationship_Code,
	ClaimRelationshipId AS i_ClaimRelationshipId,
	-- *INF*: IIF(NOT ISNULL(i_ClaimRelationshipId),
	-- TO_CHAR(i_ClaimRelationshipId),
	-- NULL)
	IFF(i_ClaimRelationshipId IS NOT NULL, TO_CHAR(i_ClaimRelationshipId), NULL) AS o_ClaimRelationshipId,
	'CLT' AS o_ClaimantName_Code,
	ClaimantFullName AS ClaimantName,
	0 AS Out_Obj_Val_Seq_no
	FROM RTR_Payments_Reserves_ReinsurancePayments
),
NRM_Claims_Attributes AS (
),
FIL_Claims_Attributes AS (
	SELECT
	Header_Source_Seq_Num, 
	Attr_Code, 
	Attr_Value, 
	Obj_Val_Seq_no
	FROM NRM_Claims_Attributes
	WHERE NOT ISNULL(Attr_Value)
),
EXP_AddMetadata_Attributes_Payments AS (
	SELECT
	Header_Source_Seq_Num,
	Attr_Code,
	Attr_Value,
	Obj_Val_Seq_no,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM FIL_Claims_Attributes
),
SapiensReinsuranceAttributesExtract_Payments AS (
	TRUNCATE TABLE SapiensReinsuranceAttributesExtract;
	INSERT INTO SapiensReinsuranceAttributesExtract
	(SOURCE_SEQ_NUM, ATTR_CODE, ATTR_VAL, OBJ_VAL_SEQ_NO, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	Attr_Code AS ATTR_CODE, 
	Attr_Value AS ATTR_VAL, 
	Obj_Val_Seq_no AS OBJ_VAL_SEQ_NO, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_AddMetadata_Attributes_Payments
),
EXP_Accounting_Claims_Reserves AS (
	SELECT
	o_SSN AS Header_Source_Seq_Num,
	FinancialTypeCode3 AS i_FinancialTypeCode,
	ProductCode3 AS i_ProductCode,
	CauseOfLoss3 AS i_CauseOfLoss,
	-- *INF*: DECODE(TRUE,
	-- 	i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 	'OWM',
	-- 	i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 	'OWE',
	-- 	i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 	'OWC',
	-- 	i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 	'OWI',
	-- 	i_FinancialTypeCode = 'D' AND i_ProductCode <> '100',
	-- 	'ODM',
	-- 	i_FinancialTypeCode = 'E' AND i_ProductCode <> '100',
	-- 	'OXP',
	-- 	'')
	DECODE(
	    TRUE,
	    i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', 'OWM',
	    i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', 'OWE',
	    i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', 'OWC',
	    i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', 'OWI',
	    i_FinancialTypeCode = 'D' AND i_ProductCode <> '100', 'ODM',
	    i_FinancialTypeCode = 'E' AND i_ProductCode <> '100', 'OXP',
	    ''
	) AS o_Accounting_Item,
	WorkersCompensationMedicalLossOutstanding3 AS i_WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding3 AS i_WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding3 AS i_WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding3 AS i_WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding3 AS i_PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding3 AS i_PropertyCasualtyLossOutstanding,
	-- *INF*: DECODE(TRUE,
	-- 	i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 	i_WorkersCompensationMedicalLossOutstanding,
	-- 	i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss = '06',
	-- 	i_WorkersCompensationMedicalExpenseOutstanding,
	-- 	i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 	i_WorkersCompensationIndemnityExpenseOutstanding,
	-- 	i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06',
	-- 	i_WorkersCompensationIndemnityLossOutstanding,
	-- 	i_FinancialTypeCode = 'D' AND i_ProductCode <> '100',
	-- 	i_PropertyCasualtyLossOutstanding,
	-- 	i_FinancialTypeCode = 'E' AND i_ProductCode <> '100',
	-- 	i_PropertyCasualtyExpenseOutstanding,
	-- 	NULL)
	DECODE(
	    TRUE,
	    i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', i_WorkersCompensationMedicalLossOutstanding,
	    i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss = '06', i_WorkersCompensationMedicalExpenseOutstanding,
	    i_FinancialTypeCode = 'E' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', i_WorkersCompensationIndemnityExpenseOutstanding,
	    i_FinancialTypeCode = 'D' AND i_ProductCode = '100' AND i_CauseOfLoss <> '06', i_WorkersCompensationIndemnityLossOutstanding,
	    i_FinancialTypeCode = 'D' AND i_ProductCode <> '100', i_PropertyCasualtyLossOutstanding,
	    i_FinancialTypeCode = 'E' AND i_ProductCode <> '100', i_PropertyCasualtyExpenseOutstanding,
	    NULL
	) AS o_Accounting_Amount,
	'USD' AS Currency_Code,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM EXP_SameSSNAcrossFinancialTypes
),
SapiensReinsuranceAccountingItemsExtract_Reserves AS (
	INSERT INTO SapiensReinsuranceAccountingItemsExtract
	(SOURCE_SEQ_NUM, ACCOUNTING_ITEM, ACOUNTING_AMOUNT, CURRENCY_CODE, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	o_Accounting_Item AS ACCOUNTING_ITEM, 
	o_Accounting_Amount AS ACOUNTING_AMOUNT, 
	Currency_Code AS CURRENCY_CODE, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_Accounting_Claims_Reserves
),
EXP_Attributes_Claims_Reserves AS (
	SELECT
	ClaimLossDate3 AS i_ClaimLossDate,
	SourceSequenceNumber AS Header_Source_Seq_Num,
	'LOB' AS out_LOB_Code,
	InsuranceReferenceLineOfBusinessCode3 AS LOB_Value,
	'PDT' AS out_ProductCode_Code,
	ProductCode3 AS ProductCode_Value,
	'ACP' AS out_AccountingProduct_Code,
	AccountingProductCode3,
	'ASL' AS out_asl_code_Code,
	ASLCode3 AS asl_code_Value,
	'SAS' AS out_sub_asl_code_Code,
	SubASLCode3 AS sub_asl_code_Value,
	'PCN' AS out_StrategicProfitCenter_Code,
	StrategicProfitCenterAbbreviation3 AS StrategicProfitCenter_Value,
	-- *INF*: REPLACECHR(false,
	-- 	IIF(StrategicProfitCenter_Value='Argent',
	-- 		'A',
	-- 		StrategicProfitCenter_Value)
	-- , ' ', '')
	REGEXP_REPLACE(
	    IFF(
	        StrategicProfitCenter_Value = 'Argent', 'A', StrategicProfitCenter_Value
	    ),' ','') AS out_StrategicProfitCenter_Value,
	'RKS' AS out_RiskState_Code,
	RiskStateCode3 AS RiskState_Value,
	-- *INF*: substr(RiskState_Value, 1, 50)
	substr(RiskState_Value, 1, 50) AS out_RiskState_Value,
	'COM' AS out_Company_Code,
	'WBMI' AS Company_Value,
	'SNA' AS O_UmbrellaLayer_Code,
	ReinsuranceUmbrellaLayer3 AS i_ReinsuranceUmbrellalayer,
	-- *INF*: IIF(NOT ISNULL(i_ReinsuranceUmbrellalayer),
	-- TO_CHAR(i_ReinsuranceUmbrellalayer),
	-- NULL)
	IFF(i_ReinsuranceUmbrellalayer IS NOT NULL, TO_CHAR(i_ReinsuranceUmbrellalayer), NULL) AS O_ReinsuranceUmbrellalayer,
	'EVN' AS o_Catastrophe_Code,
	'EVT' AS o_Catastrophe_Attribute,
	ClaimCatastropheCode3 AS i_ClaimCatastropheCode,
	-- *INF*: IIF(i_ClaimCatastropheCode='N/A',
	-- NULL,
	-- i_ClaimCatastropheCode)
	IFF(i_ClaimCatastropheCode = 'N/A', NULL, i_ClaimCatastropheCode) AS o_ClaimCatastropheCode,
	-- *INF*: DECODE(TRUE,
	-- i_ClaimLossDate <
	-- TO_DATE('2018-01-01','YYYY-MM-DD'), NULL,
	-- i_ClaimCatastropheCode='N/A',
	-- NULL,
	-- 'CAT')
	-- -- Only send CAT for date of losses 2018 and after if there is a cat code
	DECODE(
	    TRUE,
	    i_ClaimLossDate < TO_TIMESTAMP('2018-01-01', 'YYYY-MM-DD'), NULL,
	    i_ClaimCatastropheCode = 'N/A', NULL,
	    'CAT'
	) AS o_ClaimCatastropheValue,
	'HIS' AS o_HistoricalLoad_Code,
	o_ClaimTransactionDate AS ClaimTransactionDate,
	-- *INF*: @{pipeline().parameters.HIS_VALUE}
	-- --IIF(RTRIM(@{pipeline().parameters.ACCOUNTINGFLAG})='', NULL, @{pipeline().parameters.ACCOUNTINGFLAG})
	@{pipeline().parameters.HIS_VALUE} AS o_HistoricalLoadValue,
	'EVR' AS o_ClaimRelationship_Code,
	ClaimRelationshipId3 AS i_ClaimRelationshipId,
	-- *INF*: IIF(NOT ISNULL(i_ClaimRelationshipId),
	-- TO_CHAR(i_ClaimRelationshipId),
	-- NULL)
	IFF(i_ClaimRelationshipId IS NOT NULL, TO_CHAR(i_ClaimRelationshipId), NULL) AS o_ClaimRelationshipId,
	'CLT' AS o_ClaimantName_Code,
	ClaimantFullName3 AS ClaimantName,
	0 AS Out_Obj_Val_Seq_no
	FROM AGG_To_SubClaim
),
NRM_Claims_Attributes1 AS (
),
FIL_Claims_Attributes1 AS (
	SELECT
	Header_Source_Seq_Num, 
	Attr_Code, 
	Attr_Value, 
	Obj_Val_Seq_no
	FROM NRM_Claims_Attributes1
	WHERE NOT ISNULL(Attr_Value)
),
EXP_AddMetadata_Attributes_Reserves AS (
	SELECT
	Header_Source_Seq_Num,
	Attr_Code,
	Attr_Value,
	Obj_Val_Seq_no,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM FIL_Claims_Attributes1
),
SapiensReinsuranceAttributesExtract_Reserves AS (
	INSERT INTO SapiensReinsuranceAttributesExtract
	(SOURCE_SEQ_NUM, ATTR_CODE, ATTR_VAL, OBJ_VAL_SEQ_NO, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	Attr_Code AS ATTR_CODE, 
	Attr_Value AS ATTR_VAL, 
	Obj_Val_Seq_no AS OBJ_VAL_SEQ_NO, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_AddMetadata_Attributes_Reserves
),