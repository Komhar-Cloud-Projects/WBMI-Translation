WITH
LKP_Target AS (
	SELECT
	SapiensClaimErrorId,
	ClaimTransactionPKId
	FROM (
		SELECT 
			SapiensClaimErrorId,
			ClaimTransactionPKId
		FROM SapiensClaimError
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimTransactionPKId ORDER BY SapiensClaimErrorId) = 1
),
LKP_WorkSapiensValidCover AS (
	SELECT
	Value,
	Cover,
	ASL,
	SAS
	FROM (
		SELECT 
			Value,
			Cover,
			ASL,
			SAS
		FROM WorkSapiensValidCover
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Cover,Value,ASL,SAS ORDER BY Value) = 1
),
SQ_SapiensReinsuranceClaim AS (
	SELECT
		SapiensReinsuranceClaimId,
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
		ClaimCatastropheEndDate,
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
		ClaimTransactionPKId,
		ContainsOutstandingReserveAmountFlag,
		ReinsuranceUmbrellaLayer,
		ClaimRelationshipId,
		ClaimTransactionCategory,
		SourceSystemID
	FROM SapiensReinsuranceClaim
),
EXP_Evaluate_Covers AS (
	SELECT
	SapiensReinsuranceClaimId,
	ClaimNumber,
	PolicyKey,
	ProductCode,
	-- *INF*: rtrim(ProductCode)
	rtrim(ProductCode) AS v_ProductCode,
	-- *INF*: :LKP.LKP_WORKSAPIENSVALIDCOVER('PDT',v_ProductCode,NULL,NULL)
	LKP_WORKSAPIENSVALIDCOVER__PDT_v_ProductCode_NULL_NULL.Value AS v_ProductCode_Valid,
	-- *INF*: DECODE(TRUE,
	-- v_ProductCode = v_ProductCode_Valid, '1',
	-- '0')
	DECODE(
	    TRUE,
	    v_ProductCode = v_ProductCode_Valid, '1',
	    '0'
	) AS PDTInd,
	AccountingProductCode,
	-- *INF*: rtrim(AccountingProductCode)
	rtrim(AccountingProductCode) AS v_AccountingProductCode,
	-- *INF*: :LKP.LKP_WORKSAPIENSVALIDCOVER('ACP',v_AccountingProductCode,NULL,NULL)
	LKP_WORKSAPIENSVALIDCOVER__ACP_v_AccountingProductCode_NULL_NULL.Value AS v_AccountingProductCode_Valid,
	-- *INF*: DECODE(TRUE,
	-- v_AccountingProductCode = v_AccountingProductCode_Valid, '1',
	-- '0')
	DECODE(
	    TRUE,
	    v_AccountingProductCode = v_AccountingProductCode_Valid, '1',
	    '0'
	) AS ACPInd,
	StrategicProfitCenterAbbreviation,
	-- *INF*: DECODE(TRUE,
	-- StrategicProfitCenterAbbreviation  = 'NSI','NSI',
	-- StrategicProfitCenterAbbreviation =  'WB-PL', 'WB-PL',
	-- StrategicProfitCenterAbbreviation =  'WB - PL', 'WB-PL',
	-- StrategicProfitCenterAbbreviation =  'WB-CL', 'WB-CL',
	-- StrategicProfitCenterAbbreviation =  'WB - CL', 'WB-CL',
	-- StrategicProfitCenterAbbreviation =  'Argent', 'A'
	-- )
	DECODE(
	    TRUE,
	    StrategicProfitCenterAbbreviation = 'NSI', 'NSI',
	    StrategicProfitCenterAbbreviation = 'WB-PL', 'WB-PL',
	    StrategicProfitCenterAbbreviation = 'WB - PL', 'WB-PL',
	    StrategicProfitCenterAbbreviation = 'WB-CL', 'WB-CL',
	    StrategicProfitCenterAbbreviation = 'WB - CL', 'WB-CL',
	    StrategicProfitCenterAbbreviation = 'Argent', 'A'
	) AS v_StrategicProfitCenterAbbreviation,
	-- *INF*: :LKP.LKP_WORKSAPIENSVALIDCOVER('PCN',v_StrategicProfitCenterAbbreviation,NULL,NULL)
	LKP_WORKSAPIENSVALIDCOVER__PCN_v_StrategicProfitCenterAbbreviation_NULL_NULL.Value AS v_StrategicProfitCenter_Valid,
	v_StrategicProfitCenterAbbreviation AS o_StrategicProfitCenterAbbreviation,
	-- *INF*: DECODE(TRUE,
	-- v_StrategicProfitCenterAbbreviation = v_StrategicProfitCenter_Valid  AND  NOT ISNULL(v_StrategicProfitCenterAbbreviation), '1',
	-- '0')
	DECODE(
	    TRUE,
	    v_StrategicProfitCenterAbbreviation = v_StrategicProfitCenter_Valid AND v_StrategicProfitCenterAbbreviation IS NOT NULL, '1',
	    '0'
	) AS PCNInd,
	ASLCode,
	-- *INF*: rtrim(ASLCode)
	rtrim(ASLCode) AS v_ASLCode,
	-- *INF*: :LKP.LKP_WORKSAPIENSVALIDCOVER('ASL',v_ASLCode,NULL,NULL)
	LKP_WORKSAPIENSVALIDCOVER__ASL_v_ASLCode_NULL_NULL.Value AS v_ASLCode_Valid,
	-- *INF*: DECODE(TRUE,
	-- v_ASLCode = v_ASLCode_Valid, '1',
	-- '0')
	DECODE(
	    TRUE,
	    v_ASLCode = v_ASLCode_Valid, '1',
	    '0'
	) AS ASLInd,
	SubASLCode,
	-- *INF*: DECODE(TRUE,
	-- LENGTH(rtrim(SubASLCode)) = 0, NULL, 
	-- LENGTH(RTRIM(SubASLCode)) > 0, RTRIM(SubASLCode)
	-- )
	DECODE(
	    TRUE,
	    LENGTH(rtrim(SubASLCode)) = 0, NULL,
	    LENGTH(RTRIM(SubASLCode)) > 0, RTRIM(SubASLCode)
	) AS v_SubASLCode,
	-- *INF*: :LKP.LKP_WORKSAPIENSVALIDCOVER('SAS',v_SubASLCode,RTRIM(ASLCode),NULL)
	LKP_WORKSAPIENSVALIDCOVER__SAS_v_SubASLCode_RTRIM_ASLCode_NULL.Value AS v_SubASLCode_Valid,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_SubASLCode),'1',
	-- v_SubASLCode = v_SubASLCode_Valid  AND  NOT ISNULL(v_SubASLCode), '1',
	-- '0')
	DECODE(
	    TRUE,
	    v_SubASLCode IS NULL, '1',
	    v_SubASLCode = v_SubASLCode_Valid AND v_SubASLCode IS NOT NULL, '1',
	    '0'
	) AS SASInd,
	InsuranceReferenceLineOfBusinessCode,
	-- *INF*: rtrim(InsuranceReferenceLineOfBusinessCode)
	rtrim(InsuranceReferenceLineOfBusinessCode) AS v_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: :LKP.LKP_WORKSAPIENSVALIDCOVER('LOB',v_InsuranceReferenceLineOfBusinessCode,NULL,NULL)
	LKP_WORKSAPIENSVALIDCOVER__LOB_v_InsuranceReferenceLineOfBusinessCode_NULL_NULL.Value AS v_LineOfBusinessCode_Valid,
	-- *INF*: DECODE(TRUE,
	-- v_InsuranceReferenceLineOfBusinessCode = v_LineOfBusinessCode_Valid, '1',
	-- '0')
	DECODE(
	    TRUE,
	    v_InsuranceReferenceLineOfBusinessCode = v_LineOfBusinessCode_Valid, '1',
	    '0'
	) AS LOBInd,
	RiskStateCode,
	-- *INF*: rtrim(RiskStateCode)
	rtrim(RiskStateCode) AS v_RiskStateCode,
	-- *INF*: :LKP.LKP_WORKSAPIENSVALIDCOVER('RKS',v_RiskStateCode,NULL,NULL)
	LKP_WORKSAPIENSVALIDCOVER__RKS_v_RiskStateCode_NULL_NULL.Value AS v_RiskStateCode_Valid,
	-- *INF*: DECODE(TRUE,
	-- v_RiskStateCode = v_RiskStateCode_Valid, '1',
	-- '0')
	DECODE(
	    TRUE,
	    v_RiskStateCode = v_RiskStateCode_Valid, '1',
	    '0'
	) AS RKSInd,
	ReinsuranceUmbrellaLayer,
	-- *INF*: :LKP.LKP_WORKSAPIENSVALIDCOVER('SNA',ReinsuranceUmbrellaLayer,v_ASLCode,v_SubASLCode)
	LKP_WORKSAPIENSVALIDCOVER__SNA_ReinsuranceUmbrellaLayer_v_ASLCode_v_SubASLCode.Value AS v_ReinsuranceUmbrellaLayer_Valid,
	-- *INF*: DECODE(TRUE,
	--  ReinsuranceUmbrellaLayer = v_ReinsuranceUmbrellaLayer_Valid, '1',
	-- '0')
	-- 
	-- --commented out code where we treated specific returns because nullappears to be valid SNA for ASL/SNA of 220/220
	-- --(TRUE,
	-- --v_ASLCode = '220' AND v_SubASLCode = '220' AND isnull(ReinsuranceUmbrellaLayer),'0',
	-- --v_ASLCode = '220' AND v_SubASLCode = '220' AND NOT isnull(ReinsuranceUmbrellaLayer)  AND ReinsuranceUmbrellaLayer = v_ReinsuranceUmbrellaLayer_Valid, '1',
	-- --v_ASLCode  != '220' AND ReinsuranceUmbrellaLayer = v_ReinsuranceUmbrellaLayer_Valid, '1',
	-- --'0')
	DECODE(
	    TRUE,
	    ReinsuranceUmbrellaLayer = v_ReinsuranceUmbrellaLayer_Valid, '1',
	    '0'
	) AS SNAInd,
	ClaimTransactionPKId
	FROM SQ_SapiensReinsuranceClaim
	LEFT JOIN LKP_WORKSAPIENSVALIDCOVER LKP_WORKSAPIENSVALIDCOVER__PDT_v_ProductCode_NULL_NULL
	ON LKP_WORKSAPIENSVALIDCOVER__PDT_v_ProductCode_NULL_NULL.Cover = 'PDT'
	AND LKP_WORKSAPIENSVALIDCOVER__PDT_v_ProductCode_NULL_NULL.Value = v_ProductCode
	AND LKP_WORKSAPIENSVALIDCOVER__PDT_v_ProductCode_NULL_NULL.ASL = NULL
	AND LKP_WORKSAPIENSVALIDCOVER__PDT_v_ProductCode_NULL_NULL.SAS = NULL

	LEFT JOIN LKP_WORKSAPIENSVALIDCOVER LKP_WORKSAPIENSVALIDCOVER__ACP_v_AccountingProductCode_NULL_NULL
	ON LKP_WORKSAPIENSVALIDCOVER__ACP_v_AccountingProductCode_NULL_NULL.Cover = 'ACP'
	AND LKP_WORKSAPIENSVALIDCOVER__ACP_v_AccountingProductCode_NULL_NULL.Value = v_AccountingProductCode
	AND LKP_WORKSAPIENSVALIDCOVER__ACP_v_AccountingProductCode_NULL_NULL.ASL = NULL
	AND LKP_WORKSAPIENSVALIDCOVER__ACP_v_AccountingProductCode_NULL_NULL.SAS = NULL

	LEFT JOIN LKP_WORKSAPIENSVALIDCOVER LKP_WORKSAPIENSVALIDCOVER__PCN_v_StrategicProfitCenterAbbreviation_NULL_NULL
	ON LKP_WORKSAPIENSVALIDCOVER__PCN_v_StrategicProfitCenterAbbreviation_NULL_NULL.Cover = 'PCN'
	AND LKP_WORKSAPIENSVALIDCOVER__PCN_v_StrategicProfitCenterAbbreviation_NULL_NULL.Value = v_StrategicProfitCenterAbbreviation
	AND LKP_WORKSAPIENSVALIDCOVER__PCN_v_StrategicProfitCenterAbbreviation_NULL_NULL.ASL = NULL
	AND LKP_WORKSAPIENSVALIDCOVER__PCN_v_StrategicProfitCenterAbbreviation_NULL_NULL.SAS = NULL

	LEFT JOIN LKP_WORKSAPIENSVALIDCOVER LKP_WORKSAPIENSVALIDCOVER__ASL_v_ASLCode_NULL_NULL
	ON LKP_WORKSAPIENSVALIDCOVER__ASL_v_ASLCode_NULL_NULL.Cover = 'ASL'
	AND LKP_WORKSAPIENSVALIDCOVER__ASL_v_ASLCode_NULL_NULL.Value = v_ASLCode
	AND LKP_WORKSAPIENSVALIDCOVER__ASL_v_ASLCode_NULL_NULL.ASL = NULL
	AND LKP_WORKSAPIENSVALIDCOVER__ASL_v_ASLCode_NULL_NULL.SAS = NULL

	LEFT JOIN LKP_WORKSAPIENSVALIDCOVER LKP_WORKSAPIENSVALIDCOVER__SAS_v_SubASLCode_RTRIM_ASLCode_NULL
	ON LKP_WORKSAPIENSVALIDCOVER__SAS_v_SubASLCode_RTRIM_ASLCode_NULL.Cover = 'SAS'
	AND LKP_WORKSAPIENSVALIDCOVER__SAS_v_SubASLCode_RTRIM_ASLCode_NULL.Value = v_SubASLCode
	AND LKP_WORKSAPIENSVALIDCOVER__SAS_v_SubASLCode_RTRIM_ASLCode_NULL.ASL = RTRIM(ASLCode)
	AND LKP_WORKSAPIENSVALIDCOVER__SAS_v_SubASLCode_RTRIM_ASLCode_NULL.SAS = NULL

	LEFT JOIN LKP_WORKSAPIENSVALIDCOVER LKP_WORKSAPIENSVALIDCOVER__LOB_v_InsuranceReferenceLineOfBusinessCode_NULL_NULL
	ON LKP_WORKSAPIENSVALIDCOVER__LOB_v_InsuranceReferenceLineOfBusinessCode_NULL_NULL.Cover = 'LOB'
	AND LKP_WORKSAPIENSVALIDCOVER__LOB_v_InsuranceReferenceLineOfBusinessCode_NULL_NULL.Value = v_InsuranceReferenceLineOfBusinessCode
	AND LKP_WORKSAPIENSVALIDCOVER__LOB_v_InsuranceReferenceLineOfBusinessCode_NULL_NULL.ASL = NULL
	AND LKP_WORKSAPIENSVALIDCOVER__LOB_v_InsuranceReferenceLineOfBusinessCode_NULL_NULL.SAS = NULL

	LEFT JOIN LKP_WORKSAPIENSVALIDCOVER LKP_WORKSAPIENSVALIDCOVER__RKS_v_RiskStateCode_NULL_NULL
	ON LKP_WORKSAPIENSVALIDCOVER__RKS_v_RiskStateCode_NULL_NULL.Cover = 'RKS'
	AND LKP_WORKSAPIENSVALIDCOVER__RKS_v_RiskStateCode_NULL_NULL.Value = v_RiskStateCode
	AND LKP_WORKSAPIENSVALIDCOVER__RKS_v_RiskStateCode_NULL_NULL.ASL = NULL
	AND LKP_WORKSAPIENSVALIDCOVER__RKS_v_RiskStateCode_NULL_NULL.SAS = NULL

	LEFT JOIN LKP_WORKSAPIENSVALIDCOVER LKP_WORKSAPIENSVALIDCOVER__SNA_ReinsuranceUmbrellaLayer_v_ASLCode_v_SubASLCode
	ON LKP_WORKSAPIENSVALIDCOVER__SNA_ReinsuranceUmbrellaLayer_v_ASLCode_v_SubASLCode.Cover = 'SNA'
	AND LKP_WORKSAPIENSVALIDCOVER__SNA_ReinsuranceUmbrellaLayer_v_ASLCode_v_SubASLCode.Value = ReinsuranceUmbrellaLayer
	AND LKP_WORKSAPIENSVALIDCOVER__SNA_ReinsuranceUmbrellaLayer_v_ASLCode_v_SubASLCode.ASL = v_ASLCode
	AND LKP_WORKSAPIENSVALIDCOVER__SNA_ReinsuranceUmbrellaLayer_v_ASLCode_v_SubASLCode.SAS = v_SubASLCode

),
RTR_InsertUpdateErrors_DeleteClaims AS (
	SELECT
	SapiensReinsuranceClaimId,
	ClaimNumber,
	PolicyKey,
	ProductCode,
	PDTInd,
	AccountingProductCode,
	ACPInd,
	o_StrategicProfitCenterAbbreviation AS StrategicProfitCenterAbbreviation,
	PCNInd,
	ASLCode,
	ASLInd,
	SubASLCode,
	SASInd,
	InsuranceReferenceLineOfBusinessCode,
	LOBInd,
	RiskStateCode,
	RKSInd,
	ReinsuranceUmbrellaLayer,
	SNAInd,
	ClaimTransactionPKId
	FROM EXP_Evaluate_Covers
),
RTR_InsertUpdateErrors_DeleteClaims_BadCovers AS (SELECT * FROM RTR_InsertUpdateErrors_DeleteClaims WHERE PDTInd = '0'  OR ACPInd = '0'  OR PCNInd = '0'  OR ASLInd = '0'  OR SASInd = '0' OR LOBInd= '0' OR RKSInd = '0' OR SNAInd = '0'),
RTR_InsertUpdateErrors_DeleteClaims_GoodCovers AS (SELECT * FROM RTR_InsertUpdateErrors_DeleteClaims WHERE PDTInd = '1'  AND ACPInd = '1' AND PCNInd = '1' AND ASLInd = '1' AND SASInd = '1' AND LOBInd = '1' AND RKSInd = '1' AND SNAInd = '1'),
AGG_Error_Records_by_CTPKID AS (
	SELECT
	ClaimNumber,
	PolicyKey,
	ProductCode,
	PDTInd,
	AccountingProductCode,
	ACPInd,
	StrategicProfitCenterAbbreviation,
	PCNInd,
	ASLCode,
	ASLInd,
	SubASLCode,
	SASInd,
	InsuranceReferenceLineOfBusinessCode,
	LOBInd,
	RiskStateCode,
	RKSInd,
	ReinsuranceUmbrellaLayer,
	SNAInd,
	ClaimTransactionPKId
	FROM RTR_InsertUpdateErrors_DeleteClaims_BadCovers
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimTransactionPKId ORDER BY NULL) = 1
),
EXP_Error_table_insert_update AS (
	SELECT
	ClaimNumber,
	PolicyKey,
	ProductCode,
	PDTInd,
	AccountingProductCode,
	ACPInd,
	StrategicProfitCenterAbbreviation,
	PCNInd,
	ASLCode,
	ASLInd,
	SubASLCode,
	SASInd,
	InsuranceReferenceLineOfBusinessCode,
	LOBInd,
	RiskStateCode,
	RKSInd,
	ReinsuranceUmbrellaLayer,
	SNAInd,
	ClaimTransactionPKId,
	-- *INF*: :LKP.LKP_TARGET(ClaimTransactionPKId)
	LKP_TARGET_ClaimTransactionPKId.SapiensClaimErrorId AS SapiensClaimErrorId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM AGG_Error_Records_by_CTPKID
	LEFT JOIN LKP_TARGET LKP_TARGET_ClaimTransactionPKId
	ON LKP_TARGET_ClaimTransactionPKId.ClaimTransactionPKId = ClaimTransactionPKId

),
RTR_Insert_Update_Errors AS (
	SELECT
	ClaimNumber,
	PolicyKey,
	ProductCode,
	PDTInd,
	AccountingProductCode,
	ACPInd,
	StrategicProfitCenterAbbreviation,
	PCNInd,
	ASLCode,
	ASLInd,
	SubASLCode,
	SASInd,
	InsuranceReferenceLineOfBusinessCode,
	LOBInd,
	RiskStateCode,
	RKSInd,
	ReinsuranceUmbrellaLayer,
	SNAInd,
	ClaimTransactionPKId,
	SapiensClaimErrorId,
	CreatedDate,
	ModifiedDate
	FROM EXP_Error_table_insert_update
),
RTR_Insert_Update_Errors_InsertErrors AS (SELECT * FROM RTR_Insert_Update_Errors WHERE ISNULL(SapiensClaimErrorId)),
RTR_Insert_Update_Errors_UpdateErrors AS (SELECT * FROM RTR_Insert_Update_Errors WHERE NOT ISNULL(SapiensClaimErrorId)),
SapiensClaimError_Insert AS (
	INSERT INTO SapiensClaimError
	(CreatedDate, ModifiedDate, ClaimTransactionPKId, ClaimNumber, PolicyKey, ProductCode, PDTFlag, AccountingProductCode, ACPFlag, StrategicProfitCenterAbbreviation, PCNFlag, ASLCode, ASLFlag, SubASLCode, SASFlag, InsuranceReferenceLineOfBusinessCode, LOBFlag, RiskStateCode, RKSFlag, ReinsuranceUmbrellaLayer, SNAFlag)
	SELECT 
	CREATEDDATE, 
	MODIFIEDDATE, 
	CLAIMTRANSACTIONPKID, 
	CLAIMNUMBER, 
	POLICYKEY, 
	PRODUCTCODE, 
	PDTInd AS PDTFLAG, 
	ACCOUNTINGPRODUCTCODE, 
	ACPInd AS ACPFLAG, 
	STRATEGICPROFITCENTERABBREVIATION, 
	PCNInd AS PCNFLAG, 
	ASLCODE, 
	ASLInd AS ASLFLAG, 
	SUBASLCODE, 
	SASInd AS SASFLAG, 
	INSURANCEREFERENCELINEOFBUSINESSCODE, 
	LOBInd AS LOBFLAG, 
	RISKSTATECODE, 
	RKSInd AS RKSFLAG, 
	REINSURANCEUMBRELLALAYER, 
	SNAInd AS SNAFLAG
	FROM RTR_Insert_Update_Errors_InsertErrors
),
UPD_Error_Table AS (
	SELECT
	SapiensClaimErrorId, 
	ClaimTransactionPKId, 
	ClaimNumber, 
	PolicyKey, 
	ProductCode, 
	PDTInd AS PDTFlag, 
	AccountingProductCode, 
	ACPInd AS ACPFlag, 
	StrategicProfitCenterAbbreviation, 
	PCNInd AS PCNFlag, 
	ASLCode, 
	ASLInd AS ASLFlag, 
	SubASLCode, 
	SASInd AS SASFlag, 
	InsuranceReferenceLineOfBusinessCode, 
	LOBInd AS IRLOBFlag, 
	RiskStateCode, 
	RKSInd AS RKSFlag, 
	ReinsuranceUmbrellaLayer, 
	SNAInd AS SNAFlag, 
	CreatedDate AS CreatedDate3, 
	ModifiedDate AS ModifiedDate3
	FROM RTR_Insert_Update_Errors_UpdateErrors
),
SapiensClaimError_Update AS (
	MERGE INTO SapiensClaimError AS T
	USING UPD_Error_Table AS S
	ON T.SapiensClaimErrorId = S.SapiensClaimErrorId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate3, T.ClaimTransactionPKId = S.ClaimTransactionPKId, T.ClaimNumber = S.ClaimNumber, T.PolicyKey = S.PolicyKey, T.ProductCode = S.ProductCode, T.PDTFlag = S.PDTFlag, T.AccountingProductCode = S.AccountingProductCode, T.ACPFlag = S.ACPFlag, T.StrategicProfitCenterAbbreviation = S.StrategicProfitCenterAbbreviation, T.PCNFlag = S.PCNFlag, T.ASLCode = S.ASLCode, T.ASLFlag = S.ASLFlag, T.SubASLCode = S.SubASLCode, T.SASFlag = S.SASFlag, T.InsuranceReferenceLineOfBusinessCode = S.InsuranceReferenceLineOfBusinessCode, T.LOBFlag = S.IRLOBFlag, T.RiskStateCode = S.RiskStateCode, T.RKSFlag = S.RKSFlag, T.ReinsuranceUmbrellaLayer = S.ReinsuranceUmbrellaLayer, T.SNAFlag = S.SNAFlag
),
UPD_Delete_Error_Claims AS (
	SELECT
	SapiensReinsuranceClaimId
	FROM RTR_InsertUpdateErrors_DeleteClaims_BadCovers
),
SapiensReinsuranceClaim_Delete AS (
	DELETE FROM SapiensReinsuranceClaim
	WHERE (SapiensReinsuranceClaimId) IN (SELECT  SAPIENSREINSURANCECLAIMID FROM UPD_Delete_Error_Claims)
),