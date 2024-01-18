WITH
SQ_SapiensReinsuranceClaimNegate AS (
	SELECT
		SapiensReinsuranceClaimRestateId,
		ClaimNumber,
		PreviousLossDate,
		CurrentLossDate,
		PreviousCatastropheCode,
		CurrentCatastropheCode,
		NegateDate,
		SourceSequenceNumber,
		TransactionNumber,
		PreviousClaimRelationshipId,
		CurrentClaimRelationshipId,
		PreviousPolicyKey,
		CurrentPolicyKey,
		NegateFlag
	FROM SapiensReinsuranceClaimRestate
	WHERE NegateFlag = '1'
),
EXP_Collect AS (
	SELECT
	'SRL' AS Data_Source,
	'WBMI' AS Company_Code,
	PreviousPolicyKey AS pol_key,
	'' AS Object_ID,
	'' AS Header_Endorsement_No,
	'G' AS Header_Document_Type,
	ClaimNumber AS Claim_Id,
	'' AS Sub_Claim_Id,
	'' AS Is_Borderaeu,
	'CED' AS Business_Ind,
	'' AS Exception_Ind,
	'1' AS Queue_No,
	'P&C' AS Business_Deprtmt,
	'' AS XOL_Allocation,
	'' AS Assumed_Company,
	NegateDate AS Accounting_Date,
	-- *INF*: TO_INTEGER(TO_CHAR(Accounting_Date,'YYYYMM'))
	CAST(TO_CHAR(Accounting_Date, 'YYYYMM') AS INTEGER) AS o_Accounting_Month,
	'1' AS Subsystem_Id,
	'PRC' AS Date_Date_Code,
	SourceSequenceNumber,
	TransactionNumber,
	'HIS' AS HistoricalLoad_Code,
	'NOACC' AS HistoricalLoad_Value,
	0 AS OBJ_VAL_SEQ_NO,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime
	FROM SQ_SapiensReinsuranceClaimNegate
),
SapiensReinsuranceHeaderExtract AS (
	INSERT INTO SapiensReinsuranceHeaderExtract
	(SOURCE_SEQ_NUM, DATA_SOURCE, COMPANY_CODE, POLICY_NO, OBJECT_ID, ENDORSEMENT_NO, TRAN_NO, DOCUMENT_TYPE, CLAIM_ID, SUB_CLAIM_ID, IS_BORDERAEU, BUSINESS_IND, EXCEPTION_IND, QUEUE_NO, BUSINESS_DEPRTMT, XOL_ALLOCATION, ASSUMED_COMPANY, ACCOUNTING_MONTH, SUBSYSTEM_ID, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	SourceSequenceNumber AS SOURCE_SEQ_NUM, 
	Data_Source AS DATA_SOURCE, 
	Company_Code AS COMPANY_CODE, 
	pol_key AS POLICY_NO, 
	Object_ID AS OBJECT_ID, 
	Header_Endorsement_No AS ENDORSEMENT_NO, 
	TransactionNumber AS TRAN_NO, 
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
	o_Accounting_Month AS ACCOUNTING_MONTH, 
	Subsystem_Id AS SUBSYSTEM_ID, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_Collect
),
SapiensReinsuranceAttributesExtract AS (
	INSERT INTO SapiensReinsuranceAttributesExtract
	(SOURCE_SEQ_NUM, ATTR_CODE, ATTR_VAL, OBJ_VAL_SEQ_NO, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	SourceSequenceNumber AS SOURCE_SEQ_NUM, 
	HistoricalLoad_Code AS ATTR_CODE, 
	HistoricalLoad_Value AS ATTR_VAL, 
	OBJ_VAL_SEQ_NO, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_Collect
),
SapiensReinsuranceDatesExtract AS (
	INSERT INTO SapiensReinsuranceDatesExtract
	(SOURCE_SEQ_NUM, DATE_CODE, DATE_VALUE, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	SourceSequenceNumber AS SOURCE_SEQ_NUM, 
	Date_Date_Code AS DATE_CODE, 
	Accounting_Date AS DATE_VALUE, 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CurrentDateTime AS MODIFIEDDATE
	FROM EXP_Collect
),