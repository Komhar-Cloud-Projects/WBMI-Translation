WITH
SQ_SapiensReinsuranceHeaderExtract AS (
	SELECT
		SOURCE_SEQ_NUM,
		DATA_SOURCE,
		COMPANY_CODE,
		POLICY_NO,
		OBJECT_ID,
		ENDORSEMENT_NO,
		TRAN_NO,
		DOCUMENT_TYPE,
		CLAIM_ID,
		SUB_CLAIM_ID,
		IS_BORDERAEU,
		BUSINESS_IND,
		EXCEPTION_IND,
		QUEUE_NO,
		BUSINESS_DEPRTMT,
		XOL_ALLOCATION,
		ASSUMED_COMPANY,
		ACCOUNTING_MONTH,
		SUBSYSTEM_ID
	FROM SapiensReinsuranceHeaderExtract
),
EXP_HEADER AS (
	SELECT
	SOURCE_SEQ_NUM,
	DATA_SOURCE,
	COMPANY_CODE,
	POLICY_NO,
	OBJECT_ID,
	ENDORSEMENT_NO,
	TRAN_NO,
	DOCUMENT_TYPE,
	CLAIM_ID,
	SUB_CLAIM_ID,
	IS_BORDERAEU,
	BUSINESS_IND,
	EXCEPTION_IND,
	QUEUE_NO,
	BUSINESS_DEPRTMT,
	XOL_ALLOCATION,
	ASSUMED_COMPANY,
	ACCOUNTING_MONTH,
	SUBSYSTEM_ID
	FROM SQ_SapiensReinsuranceHeaderExtract
),
RISRCINTRF AS (
	INSERT INTO RI.RISRCINTRF
	(SOURCE_SEQ_NUM, DATA_SOURCE, COMPANY_CODE, POLICY_NO, OBJECT_ID, ENDORSEMENT_NO, TRAN_NO, DOCUMENT_TYPE, CLAIM_ID, SUB_CLAIM_ID, IS_BORDERAEU, BUSINESS_IND, EXCEPTION_IND, QUEUE_NO, BUSINESS_DEPRTMT, XOL_ALLOCATION, ASSUMED_COMPANY, ACCOUNTING_MONTH, SUBSYSTEM_ID)
	SELECT 
	SOURCE_SEQ_NUM, 
	DATA_SOURCE, 
	COMPANY_CODE, 
	POLICY_NO, 
	OBJECT_ID, 
	ENDORSEMENT_NO, 
	TRAN_NO, 
	DOCUMENT_TYPE, 
	CLAIM_ID, 
	SUB_CLAIM_ID, 
	IS_BORDERAEU, 
	BUSINESS_IND, 
	EXCEPTION_IND, 
	QUEUE_NO, 
	BUSINESS_DEPRTMT, 
	XOL_ALLOCATION, 
	ASSUMED_COMPANY, 
	ACCOUNTING_MONTH, 
	SUBSYSTEM_ID
	FROM EXP_HEADER
),
SQ_SapiensReinsuranceAttributesExtract AS (
	SELECT
		SOURCE_SEQ_NUM,
		ATTR_CODE,
		ATTR_VAL,
		OBJ_VAL_SEQ_NO
	FROM SapiensReinsuranceAttributesExtract
),
EXP_ATTRIBUTES AS (
	SELECT
	SOURCE_SEQ_NUM,
	ATTR_CODE,
	ATTR_VAL,
	OBJ_VAL_SEQ_NO
	FROM SQ_SapiensReinsuranceAttributesExtract
),
RISATTRINT AS (
	INSERT INTO RI.RISATTRINT
	(SOURCE_SEQ_NUM, ATTR_CODE, ATTR_VAL, OBJ_VAL_SEQ_NO)
	SELECT 
	SOURCE_SEQ_NUM, 
	ATTR_CODE, 
	ATTR_VAL, 
	OBJ_VAL_SEQ_NO
	FROM EXP_ATTRIBUTES
),
SQ_SapiensReinsuranceAccountingItemsExtract AS (
	SELECT
		SOURCE_SEQ_NUM,
		ACCOUNTING_ITEM,
		ACOUNTING_AMOUNT,
		CURRENCY_CODE
	FROM SapiensReinsuranceAccountingItemsExtract
),
EXP_ACCOUNTING_ITEMS AS (
	SELECT
	SOURCE_SEQ_NUM,
	ACCOUNTING_ITEM,
	ACOUNTING_AMOUNT,
	CURRENCY_CODE
	FROM SQ_SapiensReinsuranceAccountingItemsExtract
),
RISAITMINT AS (
	INSERT INTO RI.RISAITMINT
	(SOURCE_SEQ_NUM, ACCOUNTING_ITEM, ACOUNTING_AMOUNT, CURRENCY_CODE)
	SELECT 
	SOURCE_SEQ_NUM, 
	ACCOUNTING_ITEM, 
	ACOUNTING_AMOUNT, 
	CURRENCY_CODE
	FROM EXP_ACCOUNTING_ITEMS
),
SQ_SapiensReinsurancePaymentsExtract AS (
	SELECT
		SOURCE_SEQ_NUM,
		ACCOUNTING_ITEM,
		ACP_SEQ_NUM,
		ACP_TYPE,
		ACP_FROM_DT,
		ACP_TO_DT,
		ACP_AMOUNT,
		QUEUE_NO,
		ACP_TOTAL_AMOUNT,
		ACP_YEAR_AMOUNT,
		PAYMENT_ATTR
	FROM SapiensReinsurancePaymentsExtract
),
EXP_PAYMENT AS (
	SELECT
	SOURCE_SEQ_NUM,
	ACCOUNTING_ITEM,
	ACP_SEQ_NUM,
	ACP_TYPE,
	ACP_FROM_DT,
	ACP_TO_DT,
	ACP_AMOUNT,
	QUEUE_NO,
	ACP_TOTAL_AMOUNT,
	ACP_YEAR_AMOUNT,
	PAYMENT_ATTR
	FROM SQ_SapiensReinsurancePaymentsExtract
),
RISAIPINT AS (
	INSERT INTO RI.RISAIPINT
	(SOURCE_SEQ_NUM, ACCOUNTING_ITEM, ACP_SEQ_NUM, ACP_TYPE, ACP_FROM_DT, ACP_TO_DT, ACP_AMOUNT, QUEUE_NO, ACP_TOTAL_AMOUNT, ACP_YEAR_AMOUNT, PAYMENT_ATTR)
	SELECT 
	SOURCE_SEQ_NUM, 
	ACCOUNTING_ITEM, 
	ACP_SEQ_NUM, 
	ACP_TYPE, 
	ACP_FROM_DT, 
	ACP_TO_DT, 
	ACP_AMOUNT, 
	QUEUE_NO, 
	ACP_TOTAL_AMOUNT, 
	ACP_YEAR_AMOUNT, 
	PAYMENT_ATTR
	FROM EXP_PAYMENT
),
SQ_SapiensReinsuranceDatesExtract AS (
	SELECT
		SOURCE_SEQ_NUM,
		DATE_CODE,
		DATE_VALUE
	FROM SapiensReinsuranceDatesExtract
),
EXP_DATES AS (
	SELECT
	SOURCE_SEQ_NUM,
	DATE_CODE,
	DATE_VALUE
	FROM SQ_SapiensReinsuranceDatesExtract
),
RISDATEINT AS (
	INSERT INTO RI.RISDATEINT
	(SOURCE_SEQ_NUM, DATE_CODE, DATE_VALUE)
	SELECT 
	SOURCE_SEQ_NUM, 
	DATE_CODE, 
	DATE_VALUE
	FROM EXP_DATES
),