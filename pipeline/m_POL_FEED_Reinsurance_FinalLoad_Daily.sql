WITH
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
	WHERE SapiensReinsurancePolicy.DocumntType='P'
	@{pipeline().parameters.WHERE}
),
EXP_SRC_DataCollect AS (
	SELECT
	PolicyKey AS pol_key,
	DocumntType,
	AccountingDate AS Accounting_Date,
	MonthlyTotalDirectWrittenPremium,
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
	ReinsuranceUmbrellaLayer AS ReinsuranceUmbrellalayer,
	OSECode AS OSE_Flag,
	RatingPlanAbbreviation,
	FirstNameIsured,
	EntryProcess
	FROM SQ_SapiensReinsurancePolicy
),
AGG_Data_DuplicateEliminate AS (
	SELECT
	pol_key,
	DocumntType,
	Accounting_Date,
	MonthlyTotalDirectWrittenPremium,
	-- *INF*: SUM(MonthlyTotalDirectWrittenPremium)
	SUM(MonthlyTotalDirectWrittenPremium) AS O_MonthlyTotalDirectWrittenPremium,
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
	Source_Seq_Num,
	Tran_No,
	Endorsement_No,
	ReinsuranceUmbrellalayer,
	OSE_Flag,
	-- *INF*: MAX(OSE_Flag)
	MAX(OSE_Flag) AS O_OSE_Flag,
	RatingPlanAbbreviation,
	FirstNameIsured,
	EntryProcess
	FROM EXP_SRC_DataCollect
	GROUP BY pol_key, DocumntType, Accounting_Date, ProductCode, StrategicProfitCenterAbbreviation, AccountingProductCode, InsuranceReferenceLineOfBusinessCode, asl_code, sub_asl_code, state_of_domicile_code, CoverageEffectiveDate, CoverageExpirationDate, Endorsement_Start_Date, Endorsement_Issue_Date, Policy_Issue_Date, Source_Seq_Num, Tran_No, Endorsement_No, ReinsuranceUmbrellalayer
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
	-- *INF*: IIF(ISNULL(@{pipeline().parameters.NUMBEROFPOLICYQUEUES}) or @{pipeline().parameters.NUMBEROFPOLICYQUEUES}=0,
	-- 1,
	-- MOD(TO_INTEGER(REVERSE(SUBSTR(REVERSE(pol_key),3,2))), @{pipeline().parameters.NUMBEROFPOLICYQUEUES}) + 1
	-- )
	IFF(
	    @{pipeline().parameters.NUMBEROFPOLICYQUEUES} IS NULL or @{pipeline().parameters.NUMBEROFPOLICYQUEUES} = 0, 1,
	    MOD(CAST(REVERSE(SUBSTR(REVERSE(pol_key), 3, 2)) AS INTEGER), @{pipeline().parameters.NUMBEROFPOLICYQUEUES}) + 1
	) AS Queue_No,
	'P&C' AS Business_Deprtmt,
	'' AS XOL_Allocation,
	'' AS Assumed_Company,
	'1' AS Subsystem_Id,
	Accounting_Date,
	-- *INF*: TO_INTEGER(TO_CHAR(Accounting_Date,'YYYYMM'))
	CAST(TO_CHAR(Accounting_Date, 'YYYYMM') AS INTEGER) AS AccountingMonth,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM AGG_Data_DuplicateEliminate
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
	Header_Document_Type AS DOCUMENT_TYPE, 
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
	FROM EXP_Pol_Header
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
	FROM AGG_Data_DuplicateEliminate
	ORDER BY Source_Seq_Num ASC, Policy_Start_date ASC, Policy_End_Date ASC, Policy_Issue_Date ASC, Endorsement_Start_Date ASC, Endorsement_Issue_Date ASC, Accounting_Date ASC
),
EXP_Pol_Dates AS (
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
AGG_Eliminate_DateDuplicates AS (
	SELECT
	Header_Source_Seq_Num,
	Date_Code,
	Date_Value
	FROM NRM_Pol_Dates
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Header_Source_Seq_Num, Date_Code, Date_Value ORDER BY NULL) = 1
),
EXP_Pol_Dates_Tgt_DataCollect AS (
	SELECT
	Header_Source_Seq_Num,
	Date_Code,
	Date_Value,
	-- *INF*: to_date(Date_Value,'YYYYMMDD')
	TO_TIMESTAMP(Date_Value, 'YYYYMMDD') AS out_Date_Value,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM AGG_Eliminate_DateDuplicates
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
	FROM EXP_Pol_Dates_Tgt_DataCollect
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
	'INM' AS O_FirstNameIsuredCode,
	FirstNameIsured,
	'HIS' AS HIS_CODE,
	@{pipeline().parameters.HISVALUE} AS HIS_VALUE,
	EntryProcess,
	'ZRS' AS EntryCode,
	'POLICY-'||EntryProcess AS EntryValue,
	Accounting_Date
	FROM AGG_Data_DuplicateEliminate
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

--IIF(Attr_Code<>'SNA',1,IIF(Attr_Value='0',0,1))=1

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
EXP_Pol_Accounting AS (
	SELECT
	Source_Seq_Num AS Header_Source_Seq_Num,
	'INS' AS Sum_Insured_Accounting_Item,
	1 AS Sum_Insured_Accounting_Amount,
	'USD' AS Sum_Insured_Currency_Code,
	'NPR' AS Incoming_Premium_Accounting_Item,
	O_MonthlyTotalDirectWrittenPremium AS Incoming_Premium_Accounting_Amount,
	'USD' AS Incomming_Premium_Currency_Code
	FROM AGG_Data_DuplicateEliminate
),
NRM_Pol_Accounting AS (
),
EXP_AddMetadata_AccountingItems AS (
	SELECT
	Header_Source_Seq_Num,
	Accounting_Item,
	Accounting_Amount,
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
	Accounting_Amount AS ACOUNTING_AMOUNT, 
	Currency_Code AS CURRENCY_CODE, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_AddMetadata_AccountingItems
),