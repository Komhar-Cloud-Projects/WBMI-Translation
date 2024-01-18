WITH
LKP_TGT_Payment_Get_Seq_Num AS (
	SELECT
	ACP_SEQ_NUM,
	O_SOURCE_SEQ_NUM,
	SOURCE_SEQ_NUM
	FROM (
		select A.SOURCE_SEQ_NUM as SOURCE_SEQ_NUM,
		Max(A.ACP_SEQ_NUM) as ACP_SEQ_NUM
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePaymentsExtract A
		inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract B
		on A.SOURCE_SEQ_NUM=B.SOURCE_SEQ_NUM
		inner join
		(select POLICY_NO,max(case when DOCUMENT_TYPE='N' then SOURCE_SEQ_NUM else 0 end) max_SOURCE_SEQ_NUM 
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract C
		where DATA_SOURCE='SRP'
		group by POLICY_NO) D
		on B.POLICY_NO=D.POLICY_NO
		and B.SOURCE_SEQ_NUM>max_SOURCE_SEQ_NUM
		Group by A.SOURCE_SEQ_NUM
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE_SEQ_NUM ORDER BY ACP_SEQ_NUM) = 1
),
SQ_SapiensReinsurancePolicy AS (
	SELECT
		SapiensReinsurancePolicyId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		PolicyKey,
		DocumntType,
		AccountingDate,
		MonthlyTotalDirectWrittenPremium,
		ProductCode,
		StrategicProfitCenterAbbreviation,
		AccountingProductCode,
		InsuranceReferenceLineOfBusinessCode,
		ASLCode,
		SubASLCode,
		PrimaryStateCode,
		CoverageEffectiveDate,
		CoverageExpirationDate,
		EndorsementStartDate,
		EndorsementIssueDate,
		PolicyIssueDate,
		SourceSequenceNumber,
		TransactionNumber,
		EndorsementNumber,
		ASLCoversKey,
		DateKey,
		PremiumMasterCalculationPKId,
		ReinsuranceUmbrellaLayer,
		OSECode,
		PremiumTransactionPKID,
		EntryProcess,
		RatingPlanAbbreviation,
		FirstNameIsured
	FROM SapiensReinsurancePolicy
	WHERE @{pipeline().parameters.WHERE_SAPIENS}
),
SRC_DataCollect AS (
	SELECT
	PolicyKey AS pol_key,
	DocumntType,
	AccountingDate AS Accounting_Date,
	MonthlyTotalDirectWrittenPremium,
	-- *INF*: ROUND(MonthlyTotalDirectWrittenPremium,2)
	ROUND(MonthlyTotalDirectWrittenPremium, 2) AS Rounded_MonthlyTotalDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	ASLCode AS asl_code,
	SubASLCode AS sub_asl_code,
	PrimaryStateCode AS state_of_domicile_code,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	EndorsementStartDate AS Endorsement_Start_Date,
	EndorsementIssueDate AS Endorsement_Issue_Date,
	PolicyIssueDate AS Policy_Issue_Date,
	SourceSequenceNumber AS Source_Seq_Num,
	TransactionNumber AS Tran_No,
	EndorsementNumber AS Endorsement_No,
	ASLCoversKey,
	DateKey,
	PremiumMasterCalculationPKId AS PremiumMasterCalculationPKID,
	ReinsuranceUmbrellaLayer AS ReinsuranceUmbrellalayer,
	OSECode AS OSE_Flag,
	RatingPlanAbbreviation,
	FirstNameIsured,
	-- *INF*: IIF(ISNULL(@{pipeline().parameters.NUMBEROFPOLICYQUEUES}) or @{pipeline().parameters.NUMBEROFPOLICYQUEUES}=0,
	-- 1,
	-- MOD(TO_INTEGER(REVERSE(SUBSTR(REVERSE(pol_key),3,2))), @{pipeline().parameters.NUMBEROFPOLICYQUEUES}) + 1
	-- )
	IFF(
	    @{pipeline().parameters.NUMBEROFPOLICYQUEUES} IS NULL or @{pipeline().parameters.NUMBEROFPOLICYQUEUES} = 0, 1,
	    MOD(CAST(REVERSE(SUBSTR(REVERSE(pol_key), 3, 2)) AS INTEGER), @{pipeline().parameters.NUMBEROFPOLICYQUEUES}) + 1
	) AS QueueNumber,
	EntryProcess
	FROM SQ_SapiensReinsurancePolicy
),
AGG_HeaderInfo AS (
	SELECT
	pol_key,
	DocumntType,
	Accounting_Date,
	MonthlyTotalDirectWrittenPremium,
	-- *INF*: trunc(SUM(MonthlyTotalDirectWrittenPremium),4)
	TRUNC(SUM(MonthlyTotalDirectWrittenPremium),4) AS O_MonthlyTotalDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	sub_asl_code,
	state_of_domicile_code,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	Endorsement_Start_Date,
	Endorsement_Issue_Date,
	Policy_Issue_Date,
	-- *INF*: MIN(Policy_Issue_Date)
	MIN(Policy_Issue_Date) AS O_Policy_Issue_Date,
	Source_Seq_Num,
	Tran_No,
	Endorsement_No,
	ASLCoversKey,
	DateKey,
	ReinsuranceUmbrellalayer,
	OSE_Flag,
	-- *INF*: MAX(OSE_Flag)
	MAX(OSE_Flag) AS O_OSE_Flag,
	RatingPlanAbbreviation,
	FirstNameIsured,
	QueueNumber,
	EntryProcess,
	Rounded_MonthlyTotalDirectWrittenPremium,
	-- *INF*: sum(Rounded_MonthlyTotalDirectWrittenPremium)
	sum(Rounded_MonthlyTotalDirectWrittenPremium) AS O_Rounded_MonthlyTotalDirectWrittenPremium
	FROM SRC_DataCollect
	GROUP BY pol_key, DocumntType, Accounting_Date, ProductCode, StrategicProfitCenterAbbreviation, AccountingProductCode, InsuranceReferenceLineOfBusinessCode, asl_code, sub_asl_code, state_of_domicile_code, CoverageEffectiveDate, CoverageExpirationDate, Endorsement_Start_Date, Endorsement_Issue_Date, Source_Seq_Num, Tran_No, Endorsement_No, ReinsuranceUmbrellalayer
),
FIL_Nonzero_Premiums AS (
	SELECT
	pol_key, 
	DocumntType, 
	Accounting_Date, 
	O_MonthlyTotalDirectWrittenPremium, 
	ProductCode, 
	StrategicProfitCenterAbbreviation, 
	AccountingProductCode, 
	InsuranceReferenceLineOfBusinessCode, 
	asl_code, 
	sub_asl_code, 
	state_of_domicile_code, 
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	Endorsement_Start_Date, 
	Endorsement_Issue_Date, 
	O_Policy_Issue_Date AS Policy_Issue_Date, 
	Source_Seq_Num, 
	Tran_No, 
	Endorsement_No, 
	ASLCoversKey, 
	DateKey, 
	ReinsuranceUmbrellalayer, 
	O_OSE_Flag, 
	RatingPlanAbbreviation, 
	FirstNameIsured, 
	QueueNumber, 
	EntryProcess, 
	O_Rounded_MonthlyTotalDirectWrittenPremium AS Rounded_MonthlyTotalDirectWrittenPremium
	FROM AGG_HeaderInfo
	WHERE abs(O_MonthlyTotalDirectWrittenPremium) > 0.01
),
EXP_Pol_Attributes AS (
	SELECT
	Source_Seq_Num AS Header_Source_Seq_Num,
	'LOB' AS out_LOB_Code,
	InsuranceReferenceLineOfBusinessCode AS LOB_Value,
	'PDT' AS out_ProductCode_Code,
	ProductCode AS ProductCode_Value,
	'ASL' AS out_asl_code_Code,
	asl_code AS asl_code_Value,
	'SAS' AS out_sub_asl_code_Code,
	sub_asl_code AS sub_asl_code_Value,
	'PCN' AS out_StrategicProfitCenter_Code,
	StrategicProfitCenterAbbreviation AS StrategicProfitCenter_Value,
	-- *INF*: REPLACECHR(false,StrategicProfitCenter_Value, ' ', '')
	REGEXP_REPLACE(StrategicProfitCenter_Value,' ','') AS out_StrategicProfitCenter_Value,
	'RKS' AS out_RiskState_Code,
	state_of_domicile_code AS RiskState_Value,
	-- *INF*: substr(RiskState_Value, 1, 50)
	substr(RiskState_Value, 1, 50) AS out_RiskState_Value,
	'COM' AS out_Company_Code,
	'WBMI' AS Company_Value,
	0 AS Out_Obj_Val_Seq_no,
	'SNA' AS O_UmbrellaLayerCode,
	ReinsuranceUmbrellalayer,
	-- *INF*: TO_CHAR(ReinsuranceUmbrellalayer)
	TO_CHAR(ReinsuranceUmbrellalayer) AS O_ReinsuranceUmbrellalayer,
	'ACP' AS O_AccountingProductCode,
	AccountingProductCode,
	'ZRP' AS O_RatingPlanCode,
	RatingPlanAbbreviation,
	'INM' AS FirstNameInsuredCode,
	FirstNameIsured,
	'HIS' AS HISCode,
	@{pipeline().parameters.HIS_VALUE} AS HISValue,
	EntryProcess,
	'ZRS' AS EntryCode,
	'POLICY-'||EntryProcess AS EntryValue,
	Accounting_Date
	FROM FIL_Nonzero_Premiums
),
NRM_Pol_Attributes AS (
),
FIL_Pol_Attributes AS (
	SELECT
	Header_Source_Seq_Num, 
	Attr_Code, 
	Attr_Value, 
	Obj_Val_Seq_no
	FROM NRM_Pol_Attributes
	WHERE IIF(Attr_Code<>'SNA',
	IIF(Attr_Code<>'HIS',
		1,
		IIF(Attr_Value<>'NOACC',
			0,
			1)
		),
	IIF(Attr_Value='0',
		0,
		1)
	)
AND NOT ISNULL(Attr_Value)

--IIF(Attr_Code<>'SNA',IIF(Attr_Code<>'HIS',1,IIF(Attr_Value<>'NOACC',0,1)),IIF(Attr_Value='0',0,1))=1

--Attr_Code <> 'SAS' and Attr_Value <> 'N/A' OR (Attr_Code='SNA' and Attr_Value<>'0')
),
EXP_AddMetadata_Attributes AS (
	SELECT
	Header_Source_Seq_Num,
	Attr_Code,
	Attr_Value,
	Obj_Val_Seq_no,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM FIL_Pol_Attributes
),
SapiensReinsuranceAttributesExtract AS (
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
	FROM EXP_AddMetadata_Attributes
),
SRT_Dates AS (
	SELECT
	Source_Seq_Num, 
	CoverageEffectiveDate AS Policy_Start_date, 
	CoverageExpirationDate AS Policy_End_Date, 
	Policy_Issue_Date, 
	Endorsement_Start_Date, 
	Endorsement_Issue_Date, 
	Accounting_Date
	FROM FIL_Nonzero_Premiums
	ORDER BY Source_Seq_Num ASC, Policy_Start_date ASC, Policy_End_Date ASC, Policy_Issue_Date ASC, Endorsement_Start_Date ASC, Endorsement_Issue_Date ASC, Accounting_Date ASC
),
EXP_AllignData_Dates AS (
	SELECT
	Source_Seq_Num AS Header_Source_Seq_Num,
	'PLS' AS pol_eff_date_code,
	Policy_Start_date AS pol_eff_date_value,
	-- *INF*: TO_CHAR(pol_eff_date_value,'YYYYMMDD')
	TO_CHAR(pol_eff_date_value, 'YYYYMMDD') AS O_pol_eff_date_value,
	'PLE' AS pol_exp_date_code,
	Policy_End_Date AS pol_exp_date_value,
	-- *INF*: TO_CHAR(pol_exp_date_value,'YYYYMMDD')
	TO_CHAR(pol_exp_date_value, 'YYYYMMDD') AS O_pol_exp_date_value,
	'PIS' AS pol_incp_date_code,
	Policy_Issue_Date AS orig_incptn_date_value,
	-- *INF*: TO_CHAR(orig_incptn_date_value,'YYYYMMDD')
	TO_CHAR(orig_incptn_date_value, 'YYYYMMDD') AS O_orig_incptn_date_value,
	'ENS' AS endorsement_start_date_code,
	Endorsement_Start_Date AS endorsement_start_date_value,
	-- *INF*: TO_CHAR(endorsement_start_date_value,'YYYYMMDD')
	TO_CHAR(endorsement_start_date_value, 'YYYYMMDD') AS O_endorsement_start_date_value,
	'EIS' AS endorsement_end_date_code,
	Endorsement_Issue_Date AS endorsement_end_date_value,
	-- *INF*: TO_CHAR(endorsement_end_date_value,'YYYYMMDD')
	TO_CHAR(endorsement_end_date_value, 'YYYYMMDD') AS O_endorsement_end_date_value,
	'PRC' AS Process_Date,
	Accounting_Date,
	-- *INF*: TO_CHAR(Accounting_Date,'YYYYMMDD')
	TO_CHAR(Accounting_Date, 'YYYYMMDD') AS O_ProcessDate
	FROM SRT_Dates
),
NRM_Pol_Dates AS (
),
AGG_Eliminate_Data_Duplicates AS (
	SELECT
	Header_Source_Seq_Num,
	Date_Code,
	Date_Value
	FROM NRM_Pol_Dates
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Header_Source_Seq_Num, Date_Code, Date_Value ORDER BY NULL) = 1
),
EXP_Pol_Dates AS (
	SELECT
	Header_Source_Seq_Num,
	Date_Code,
	Date_Value,
	-- *INF*: to_date(Date_Value,'YYYYMMDD')
	TO_TIMESTAMP(Date_Value, 'YYYYMMDD') AS out_Date_Value,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM AGG_Eliminate_Data_Duplicates
),
SapiensReinsuranceDatesExtract AS (
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
	FROM EXP_Pol_Dates
),
EXP_Pol_Header AS (
	SELECT
	Source_Seq_Num AS Header_Source_Seq_Num,
	'SRP' AS Data_Source,
	'WBMI' AS Company_Code,
	pol_key,
	'' AS Object_ID,
	Endorsement_No AS Header_Endorsement_No,
	Tran_No AS Header_Trans_No,
	DocumntType AS Header_Document_Type,
	'' AS Claim_Id,
	'' AS Sub_Claim_Id,
	'' AS Is_Borderaeu,
	'CED' AS Business_Ind,
	O_OSE_Flag AS Exception_Ind,
	QueueNumber AS Queue_No,
	'P&C' AS Business_Deprtmt,
	'' AS XOL_Allocation,
	'' AS Assumed_Company,
	Accounting_Date,
	-- *INF*: TO_INTEGER(TO_CHAR(Accounting_Date,'YYYYMM'))
	CAST(TO_CHAR(Accounting_Date, 'YYYYMM') AS INTEGER) AS AccountingMonth,
	'1' AS Subsystem_Id,
	ASLCoversKey,
	DateKey
	FROM FIL_Nonzero_Premiums
),
SRT_Set_OrderSequence AS (
	SELECT
	pol_key, 
	ASLCoversKey, 
	DateKey, 
	Header_Source_Seq_Num, 
	Data_Source, 
	Company_Code, 
	Object_ID, 
	Header_Endorsement_No, 
	Header_Trans_No, 
	Header_Document_Type, 
	Claim_Id, 
	Sub_Claim_Id, 
	Is_Borderaeu, 
	Business_Ind, 
	Exception_Ind, 
	Queue_No, 
	Business_Deprtmt, 
	XOL_Allocation, 
	Assumed_Company, 
	AccountingMonth, 
	Subsystem_Id
	FROM EXP_Pol_Header
	ORDER BY pol_key ASC, ASLCoversKey ASC, DateKey ASC, Header_Source_Seq_Num ASC
),
LKP_GetPriorPolicyCoversActivity AS (
	SELECT
	ASLCoversKey,
	DateKey
	FROM (
		SELECT  A.ASLCoversKey AS ASLCoversKey,  A.DateKey AS DateKey 
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy A with (NOLOCK)
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract B with (NOLOCK)
		on A.SourceSequenceNumber=B.SOURCE_SEQ_NUM
		INNER JOIN
		(SELECT PolicyKey,max(case when DocumntType='N' then SourceSequenceNumber else 0 end) max_SourceSequenceNumber
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy X with (NOLOCK)
		WHERE EXISTS (SELECT 1 from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicy C with (NOLOCK) where X.PolicyKey=C.PolicyKey)
		GROUP BY PolicyKey) D
		ON A.PolicyKey=D.PolicyKey and A.SourceSequenceNumber>D.max_SourceSequenceNumber
		GROUP BY A.ASLCoversKey,A.DateKey
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ASLCoversKey,DateKey ORDER BY ASLCoversKey) = 1
),
EXP_SetDocumentType AS (
	SELECT
	SRT_Set_OrderSequence.Header_Source_Seq_Num,
	SRT_Set_OrderSequence.Data_Source,
	SRT_Set_OrderSequence.Company_Code,
	SRT_Set_OrderSequence.pol_key,
	SRT_Set_OrderSequence.Object_ID,
	SRT_Set_OrderSequence.Header_Endorsement_No,
	SRT_Set_OrderSequence.Header_Trans_No,
	SRT_Set_OrderSequence.Header_Document_Type,
	-- *INF*: IIF(ISNULL(LKP_ASLCoversKey),iif(pol_key=v_pol_key and ASLCoversKey=v_ASLCoversKey and DateKey=v_DateKey,'E','P'),'E')
	IFF(
	    LKP_ASLCoversKey IS NULL,
	    IFF(
	        pol_key = v_pol_key
	    and ASLCoversKey = v_ASLCoversKey
	    and DateKey = v_DateKey, 'E',
	        'P'
	    ),
	    'E'
	) AS v_Header_Document_Type,
	v_Header_Document_Type AS O_Header_Document_Type,
	SRT_Set_OrderSequence.Claim_Id,
	SRT_Set_OrderSequence.Sub_Claim_Id,
	SRT_Set_OrderSequence.Is_Borderaeu,
	SRT_Set_OrderSequence.Business_Ind,
	SRT_Set_OrderSequence.Exception_Ind,
	SRT_Set_OrderSequence.Queue_No,
	SRT_Set_OrderSequence.Business_Deprtmt,
	SRT_Set_OrderSequence.XOL_Allocation,
	SRT_Set_OrderSequence.Assumed_Company,
	SRT_Set_OrderSequence.AccountingMonth,
	SRT_Set_OrderSequence.Subsystem_Id,
	LKP_GetPriorPolicyCoversActivity.ASLCoversKey AS LKP_ASLCoversKey,
	SRT_Set_OrderSequence.ASLCoversKey,
	SRT_Set_OrderSequence.DateKey,
	pol_key AS v_pol_key,
	ASLCoversKey AS v_ASLCoversKey,
	DateKey AS v_DateKey,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM SRT_Set_OrderSequence
	LEFT JOIN LKP_GetPriorPolicyCoversActivity
	ON LKP_GetPriorPolicyCoversActivity.ASLCoversKey = SRT_Set_OrderSequence.ASLCoversKey AND LKP_GetPriorPolicyCoversActivity.DateKey = SRT_Set_OrderSequence.DateKey
),
SapiensReinsuranceHeaderExtract AS (
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
	Header_Trans_No AS TRAN_NO, 
	O_Header_Document_Type AS DOCUMENT_TYPE, 
	Claim_Id AS CLAIM_ID, 
	Sub_Claim_Id AS SUB_CLAIM_ID, 
	Is_Borderaeu AS IS_BORDERAEU, 
	Business_Ind AS BUSINESS_IND, 
	Exception_Ind AS EXCEPTION_IND, 
	Queue_No AS QUEUE_NO, 
	Business_Deprtmt AS BUSINESS_DEPRTMT, 
	XOL_Allocation AS XOL_ALLOCATION, 
	Assumed_Company AS ASSUMED_COMPANY, 
	AccountingMonth AS ACCOUNTING_MONTH, 
	Subsystem_Id AS SUBSYSTEM_ID, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_SetDocumentType
),
EXP_Pol_Accounting AS (
	SELECT
	Source_Seq_Num AS Header_Source_Seq_Num,
	'INS' AS Sum_Insured_Accounting_Item,
	1 AS Sum_Insured_Accounting_Amount,
	'USD' AS Sum_Insured_Currency_Code,
	'NPR' AS Incoming_Premium_Accounting_Item,
	O_MonthlyTotalDirectWrittenPremium AS Incoming_Premium_Accounting_Amount,
	'USD' AS Incomming_Premium_Currency_Code,
	Rounded_MonthlyTotalDirectWrittenPremium
	FROM FIL_Nonzero_Premiums
),
NRM_Pol_Accounting AS (
),
EXP_AddMetadata_AccountingItems AS (
	SELECT
	Header_Source_Seq_Num,
	Accounting_Item,
	Accounting_Amount,
	-- *INF*: ROUND(Accounting_Amount,2)
	ROUND(Accounting_Amount, 2) AS o_Accounting_Amount,
	Currency_Code,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM NRM_Pol_Accounting
),
SapiensReinsuranceAccountingItemsExtract AS (
	TRUNCATE TABLE SapiensReinsuranceAccountingItemsExtract;
	INSERT INTO SapiensReinsuranceAccountingItemsExtract
	(SOURCE_SEQ_NUM, ACCOUNTING_ITEM, ACOUNTING_AMOUNT, CURRENCY_CODE, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	Accounting_Item AS ACCOUNTING_ITEM, 
	o_Accounting_Amount AS ACOUNTING_AMOUNT, 
	Currency_Code AS CURRENCY_CODE, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_AddMetadata_AccountingItems
),
SQ_EarnedPremiumTransactionMonthlyFact AS (
	Declare @StartDate as datetime, 
		@EndDate as datetime
	set @EndDate = dateadd(dd,-1,dateadd(MM,-@{pipeline().parameters.NO_OF_MONTHS},dateadd(mm,Datediff(mm,0,getdate()),0)))
	set @StartDate = case when '@{pipeline().parameters.PMSESSIONNAME}' like '%Restate%' or '@{pipeline().parameters.PMSESSIONNAME}' like '%Historical%' then '2001-01-31 00:00:00'  else dateadd(MM,-@{pipeline().parameters.NO_OF_MONTHS}-1,dateadd(mm,Datediff(mm,0,getdate()),0)) end
	
	SELECT MonthlyChangeinDirectEarnedPremium,
		RD.clndr_date Process_Date,
		EP.EDWPremiumMasterCalculationPKId,
		P.Pol_Key,
		ASL.asl_code,
		CASE WHEN ASL.asl_code IN ('440', '500') 
			THEN CASE WHEN ASL.sub_asl_code = 'N/A' 
				THEN NULL 
				ELSE ASL.sub_asl_code 
				END 
			WHEN ASL.asl_code = '220' 
				THEN '220' 
				ELSE 'N/A' 
		END AS sub_asl_code
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumTransactionMonthlyFact EP
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionTypeDim PTT ON EP.PremiumTransactionTypeDimID = PTT.PremiumTransactionTypeDimID
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim RD ON EP.PremiumTransactionRunDateID = RD.clndr_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P ON EP.PolicyDimID = P.pol_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL ON EP.AnnualStatementLineDimID = ASL.asl_dim_id
	WHERE PTT.PremiumTypeCode = 'D'
		AND RD.clndr_date BETWEEN @StartDate AND @EndDate 
	@{pipeline().parameters.WHERE} 
	
	UNION
	
	SELECT MonthlyChangeinCededEarnedPremium as MonthlyChangeinDirectEarnedPremium,
		RD.clndr_date Process_Date,
		EP.EDWPremiumMasterCalculationPKId,
		P.Pol_Key,
		ASL.asl_code,
		CASE WHEN ASL.asl_code IN ('440', '500') 
			THEN CASE WHEN ASL.sub_asl_code = 'N/A' 
				THEN NULL 
				ELSE ASL.sub_asl_code 
				END 
			WHEN ASL.asl_code = '220' 
				THEN '220' 
				ELSE 'N/A' 
		END AS sub_asl_code
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumTransactionMonthlyFact EP
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionTypeDim PTT ON EP.PremiumTransactionTypeDimID = PTT.PremiumTransactionTypeDimID
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim RD ON EP.PremiumTransactionRunDateID = RD.clndr_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P ON EP.PolicyDimID = P.pol_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL ON EP.AnnualStatementLineDimID = ASL.asl_dim_id
	WHERE PTT.PremiumTypeCode = 'C' and P.pol_sym !='000'
		AND RD.clndr_date BETWEEN @StartDate AND @EndDate 
	@{pipeline().parameters.WHERE} 
	
	ORDER BY EP.EDWPremiumMasterCalculationPKId
),
EXP_SourceDataCollect AS (
	SELECT
	MonthlyChangeinDirectEarnedPremium,
	-- *INF*: ROUND(MonthlyChangeinDirectEarnedPremium,2)
	ROUND(MonthlyChangeinDirectEarnedPremium, 2) AS Rounded_MonthlyChangeinDirectEarnedPremium,
	Process_Date,
	EDWPremiumMasterCalculationPKId,
	PolicyKey,
	-- *INF*: IIF(ISNULL(@{pipeline().parameters.NUMBEROFPOLICYQUEUES}) or @{pipeline().parameters.NUMBEROFPOLICYQUEUES}=0,
	-- 1,
	-- MOD(TO_INTEGER(REVERSE(SUBSTR(REVERSE(PolicyKey),3,2))), @{pipeline().parameters.NUMBEROFPOLICYQUEUES}) + 1
	-- )
	IFF(
	    @{pipeline().parameters.NUMBEROFPOLICYQUEUES} IS NULL or @{pipeline().parameters.NUMBEROFPOLICYQUEUES} = 0, 1,
	    MOD(CAST(REVERSE(SUBSTR(REVERSE(PolicyKey), 3, 2)) AS INTEGER), @{pipeline().parameters.NUMBEROFPOLICYQUEUES}) + 1
	) AS Queue_Number,
	asl_code,
	sub_asl_code
	FROM SQ_EarnedPremiumTransactionMonthlyFact
),
LKP_Get_Src_Seq_Num AS (
	SELECT
	SourceSequenceNumber,
	Premium,
	PremiumMasterCalculationPKId,
	ASLCode,
	SubASLCode
	FROM (
		select SourceSequenceNumber as SourceSequenceNumber,PremiumMasterCalculationPKId as PremiumMasterCalculationPKId,sum(MonthlyTotalDirectWrittenPremium) over(partition by sourcesequencenumber) as Premium,A.ASLCode as ASLCode,isnull(A.SubASLCode,'N/A') as SubASLCode 
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy A
		inner join  
		(select PolicyKey,max(case when DocumntType='N' then SourceSequenceNumber else 0 end) max_SourceSequenceNumber
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy
		group by PolicyKey) B
		on A.PolicyKey=B.PolicyKey
		and A.SourceSequenceNumber>B.max_SourceSequenceNumber
		where EntryProcess='MONTHLY'
		and not exists (select 1 from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy C
		where A.SourceSequenceNumber=C.SourceSequenceNumber
		group by SourceSequenceNumber
		having abs(sum(C.MonthlyTotalDirectWrittenPremium))<=0.01)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationPKId,ASLCode,SubASLCode ORDER BY SourceSequenceNumber DESC) = 1
),
LKP_Get_Src_Seq_Num_WithoutASL AS (
	SELECT
	SourceSequenceNumber,
	Premium,
	PremiumMasterCalculationPKId
	FROM (
		select A.SourceSequenceNumber as SourceSequenceNumber,A.PremiumMasterCalculationPKId  as PremiumMasterCalculationPKId,A.Premium as Premium from (
		select SourceSequenceNumber ,PremiumMasterCalculationPKId,sum(MonthlyTotalDirectWrittenPremium) over(partition by sourcesequencenumber) as Premium,A.ASLCode as ASLCode,isnull(A.SubASLCode,'N/A') as SubASLCode 
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy A
		inner join  
		(select PolicyKey,max(case when DocumntType='N' then SourceSequenceNumber else 0 end) max_SourceSequenceNumber
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy
		group by PolicyKey) B
		on A.PolicyKey=B.PolicyKey
		and A.SourceSequenceNumber>B.max_SourceSequenceNumber
		where EntryProcess='MONTHLY'
		and not exists (select 1 from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy C
		where A.SourceSequenceNumber=C.SourceSequenceNumber
		group by SourceSequenceNumber
		having abs(sum(C.MonthlyTotalDirectWrittenPremium))<=0.01)
		) A
		group by SourceSequenceNumber,PremiumMasterCalculationPKId,Premium
		having count(1)=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationPKId ORDER BY SourceSequenceNumber DESC) = 1
),
EXP_Get_Source_Seq_Num AS (
	SELECT
	EXP_SourceDataCollect.MonthlyChangeinDirectEarnedPremium,
	EXP_SourceDataCollect.Process_Date,
	EXP_SourceDataCollect.EDWPremiumMasterCalculationPKId,
	LKP_Get_Src_Seq_Num.Premium AS MonthlyTotalDirectWrittenPremium_WithASL,
	LKP_Get_Src_Seq_Num.SourceSequenceNumber AS SourceSequenceNumber_WithASL,
	EXP_SourceDataCollect.Queue_Number,
	EXP_SourceDataCollect.Rounded_MonthlyChangeinDirectEarnedPremium,
	LKP_Get_Src_Seq_Num_WithoutASL.SourceSequenceNumber AS SourceSequenceNumber_WithoutASL,
	LKP_Get_Src_Seq_Num_WithoutASL.Premium AS MonthlyTotalDirectWrittenPremium_WithoutASL,
	-- *INF*: IIF(ISNULL(SourceSequenceNumber_WithASL),SourceSequenceNumber_WithoutASL,SourceSequenceNumber_WithASL)
	IFF(
	    SourceSequenceNumber_WithASL IS NULL, SourceSequenceNumber_WithoutASL,
	    SourceSequenceNumber_WithASL
	) AS SourceSequenceNumber,
	-- *INF*: IIF(ISNULL(MonthlyTotalDirectWrittenPremium_WithASL),MonthlyTotalDirectWrittenPremium_WithoutASL,MonthlyTotalDirectWrittenPremium_WithASL)
	IFF(
	    MonthlyTotalDirectWrittenPremium_WithASL IS NULL,
	    MonthlyTotalDirectWrittenPremium_WithoutASL,
	    MonthlyTotalDirectWrittenPremium_WithASL
	) AS MonthlyTotalDirectWrittenPremium
	FROM EXP_SourceDataCollect
	LEFT JOIN LKP_Get_Src_Seq_Num
	ON LKP_Get_Src_Seq_Num.PremiumMasterCalculationPKId = EXP_SourceDataCollect.EDWPremiumMasterCalculationPKId AND LKP_Get_Src_Seq_Num.ASLCode = EXP_SourceDataCollect.asl_code AND LKP_Get_Src_Seq_Num.SubASLCode = EXP_SourceDataCollect.sub_asl_code
	LEFT JOIN LKP_Get_Src_Seq_Num_WithoutASL
	ON LKP_Get_Src_Seq_Num_WithoutASL.PremiumMasterCalculationPKId = EXP_SourceDataCollect.EDWPremiumMasterCalculationPKId
),
AGG_Paryments AS (
	SELECT
	SourceSequenceNumber AS Source_Seq_Num,
	MonthlyChangeinDirectEarnedPremium,
	-- *INF*: Sum(MonthlyChangeinDirectEarnedPremium)
	Sum(MonthlyChangeinDirectEarnedPremium) AS O_MonthlyChangeinDirectEarnedPremium,
	Process_Date AS EP_Process_Date,
	MonthlyTotalDirectWrittenPremium,
	Queue_Number AS QueueNumber,
	Rounded_MonthlyChangeinDirectEarnedPremium,
	-- *INF*: sum(Rounded_MonthlyChangeinDirectEarnedPremium)
	sum(Rounded_MonthlyChangeinDirectEarnedPremium) AS O_Rounded_MonthlyChangeinDirectEarnedPremium
	FROM EXP_Get_Source_Seq_Num
	GROUP BY Source_Seq_Num, EP_Process_Date
),
FIL_Zero_Written AS (
	SELECT
	Source_Seq_Num, 
	O_MonthlyChangeinDirectEarnedPremium, 
	EP_Process_Date, 
	MonthlyTotalDirectWrittenPremium AS O_MonthlyTotalDirectWrittenPremium, 
	QueueNumber, 
	O_Rounded_MonthlyChangeinDirectEarnedPremium AS Rounded_MonthlyChangeinDirectEarnedPremium
	FROM AGG_Paryments
	WHERE (NOT ISNULL(Source_Seq_Num)) AND abs(O_MonthlyTotalDirectWrittenPremium) > 0.01
),
SRT_Payments AS (
	SELECT
	Source_Seq_Num, 
	O_MonthlyChangeinDirectEarnedPremium AS MonthlyChangeinDirectEarnedPremium, 
	EP_Process_Date, 
	O_MonthlyTotalDirectWrittenPremium, 
	QueueNumber, 
	Rounded_MonthlyChangeinDirectEarnedPremium
	FROM FIL_Zero_Written
	ORDER BY Source_Seq_Num ASC, EP_Process_Date ASC
),
EXP_Pol_Payments AS (
	SELECT
	Source_Seq_Num AS Header_Source_Seq_Num,
	-- *INF*: :LKP.LKP_TGT_PAYMENT_GET_SEQ_NUM(Header_Source_Seq_Num)
	LKP_TGT_PAYMENT_GET_SEQ_NUM_Header_Source_Seq_Num.ACP_SEQ_NUM AS v_LKP_ACP_SEQ_NUM,
	-- *INF*: IIF(Header_Source_Seq_Num<>v_Header_Source_Seq_Num,
	-- IIF(ISNULL(v_LKP_ACP_SEQ_NUM),1,v_LKP_ACP_SEQ_NUM+1),v_ACP_SEQ_NUM+1
	-- )
	IFF(
	    Header_Source_Seq_Num <> v_Header_Source_Seq_Num,
	    IFF(
	        v_LKP_ACP_SEQ_NUM IS NULL, 1, v_LKP_ACP_SEQ_NUM + 1
	    ),
	    v_ACP_SEQ_NUM + 1
	) AS v_ACP_SEQ_NUM,
	v_ACP_SEQ_NUM AS ACP_SEQ_NUM,
	Header_Source_Seq_Num AS v_Header_Source_Seq_Num,
	MonthlyChangeinDirectEarnedPremium,
	-- *INF*: ROUND(MonthlyChangeinDirectEarnedPremium,2)
	ROUND(MonthlyChangeinDirectEarnedPremium, 2) AS o_MonthlyChangeinDirectEarnedPremium,
	EP_Process_Date,
	-- *INF*: set_date_part(EP_Process_Date, 'DD', 1)
	DATEADD(DAY,1-DATE_PART(DAY,EP_Process_Date),EP_Process_Date) AS Calendar_Month_From_Date,
	EP_Process_Date AS Calendar_Month_To_Date,
	'NPR' AS out_Accounting_Item,
	'ERN' AS out_ACP_Type,
	QueueNumber,
	1 AS out_ACP_Total_Amount,
	1 AS out_ACP_Year_Amount,
	O_MonthlyTotalDirectWrittenPremium,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime,
	Rounded_MonthlyChangeinDirectEarnedPremium,
	@{pipeline().parameters.HIS_VALUE_EP} AS Payment_Attr
	FROM SRT_Payments
	LEFT JOIN LKP_TGT_PAYMENT_GET_SEQ_NUM LKP_TGT_PAYMENT_GET_SEQ_NUM_Header_Source_Seq_Num
	ON LKP_TGT_PAYMENT_GET_SEQ_NUM_Header_Source_Seq_Num.SOURCE_SEQ_NUM = Header_Source_Seq_Num

),
SapiensReinsurancePaymentsExtract AS (
	TRUNCATE TABLE SapiensReinsurancePaymentsExtract;
	INSERT INTO SapiensReinsurancePaymentsExtract
	(SOURCE_SEQ_NUM, ACCOUNTING_ITEM, ACP_SEQ_NUM, ACP_TYPE, ACP_FROM_DT, ACP_TO_DT, ACP_AMOUNT, QUEUE_NO, ACP_TOTAL_AMOUNT, ACP_YEAR_AMOUNT, AuditId, CreatedDate, ModifiedDate, PAYMENT_ATTR)
	SELECT 
	Header_Source_Seq_Num AS SOURCE_SEQ_NUM, 
	out_Accounting_Item AS ACCOUNTING_ITEM, 
	ACP_SEQ_NUM, 
	out_ACP_Type AS ACP_TYPE, 
	Calendar_Month_From_Date AS ACP_FROM_DT, 
	Calendar_Month_To_Date AS ACP_TO_DT, 
	o_MonthlyChangeinDirectEarnedPremium AS ACP_AMOUNT, 
	QueueNumber AS QUEUE_NO, 
	out_ACP_Total_Amount AS ACP_TOTAL_AMOUNT, 
	out_ACP_Year_Amount AS ACP_YEAR_AMOUNT, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE, 
	Payment_Attr AS PAYMENT_ATTR
	FROM EXP_Pol_Payments
),