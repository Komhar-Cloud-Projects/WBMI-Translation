WITH
SQ_WB_BIL_AccountActivity AS (
	SELECT
		AccountActivityId,
		ModifiedUserId,
		ModifiedDate,
		AccountId,
		ActivitySource,
		ProcessedStatusCode,
		ErrorDescription,
		BankingSystemCode
	FROM WB_BIL_AccountActivity
	WHERE WB_BIL_AccountActivity.BankingSystemCode = 'FIS' AND WB_BIL_AccountActivity.ProcessedStatusCode = 'N' @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Batch_WebService AS (
	SELECT
	AccountActivityId,
	-- *INF*: IIF(ISNULL(var_SequenceNumber), 0, var_SequenceNumber + 1)
	IFF(var_SequenceNumber IS NULL, 0, var_SequenceNumber + 1) AS var_SequenceNumber,
	-- *INF*: IIF(ISNULL(var_BatchNumber),
	--  1, 
	--  IIF(MOD(var_SequenceNumber, @{pipeline().parameters.DUCKCREEKFACADESERVICE_BATCHSIZE})  != 1,
	--   var_BatchNumber,
	--   var_BatchNumber + 1))
	IFF(
	    var_BatchNumber IS NULL, 1,
	    IFF(
	        MOD(var_SequenceNumber, @{pipeline().parameters.DUCKCREEKFACADESERVICE_BATCHSIZE}) != 1, var_BatchNumber,
	        var_BatchNumber + 1
	    )
	) AS var_BatchNumber,
	var_SequenceNumber AS out_SequenceNumber,
	var_BatchNumber AS out_BatchNumber,
	1 AS testNumber
	FROM SQ_WB_BIL_AccountActivity
),
AGG_EnvID_ByBatchID AS (
	SELECT
	out_BatchNumber
	FROM EXP_Batch_WebService
	QUALIFY ROW_NUMBER() OVER (PARTITION BY out_BatchNumber ORDER BY NULL) = 1
),
GetFisCustomerDataFile AS (-- GetFisCustomerDataFile

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_ACR_Output AS (
	SELECT
	tns3_OriginalAccountNumber AS OriginalAccountNumber,
	tns3_OriginalServiceId AS OriginalServiceId,
	tns3_NewAccountNumber AS NewAccountNumber,
	tns3_NewServiceId AS NewServiceId
	FROM GetFisCustomerDataFile
),
EXP_PARSE_ACR AS (
	SELECT
	OriginalAccountNumber,
	OriginalServiceId,
	NewAccountNumber,
	NewServiceId,
	'D' AS DetailIndicator,
	-- *INF*: IIF(NOT ISNULL(OriginalAccountNumber) AND  NOT ISNULL(NewAccountNumber),'Y','N')
	IFF(OriginalAccountNumber IS NULL AND NewAccountNumber IS NOT NOT NULL, 'Y', 'N') AS validACR,
	-- *INF*: TO_CHAR(SESSSTARTTIME,'YYYYMMDD_hhmmss')
	TO_CHAR(SESSSTARTTIME, 'YYYYMMDD_hhmmss') AS ACRBatchId,
	-- *INF*: IIF(NOT ISNULL(OriginalAccountNumber) AND  NOT ISNULL(NewAccountNumber),v_counter_ACR+1,0)
	-- 
	-- -- add null check in case we get empty files
	IFF(
	    OriginalAccountNumber IS NULL AND NewAccountNumber IS NOT NOT NULL, v_counter_ACR + 1, 0
	) AS v_counter_ACR,
	v_counter_ACR AS o_counter_ACR
	FROM EXP_ACR_Output
),
FIL_ValidACR AS (
	SELECT
	DetailIndicator AS Record_Indicator, 
	OriginalAccountNumber, 
	OriginalServiceId, 
	NewAccountNumber, 
	NewServiceId, 
	validACR
	FROM EXP_PARSE_ACR
	WHERE validACR = 'Y'
),
FIS_ARF_Detail AS (
	INSERT INTO FIS_ARF_Detail
	(Record_Indicator, OriginalAccountNumber, OriginalServiceId, NewAccountNumber, NewServiceId)
	SELECT 
	RECORD_INDICATOR, 
	ORIGINALACCOUNTNUMBER, 
	ORIGINALSERVICEID, 
	NEWACCOUNTNUMBER, 
	NEWSERVICEID
	FROM FIL_ValidACR
),
EXP_WS_Output AS (
	SELECT
	tns3_AccountId AS AccountId,
	tns3_AccountNumber AS AccountNumber,
	tns3_BusinessName AS BusinessName,
	tns3_CustomerType AS CustomerType,
	tns3_Balance AS Balance,
	tns3_InvoiceNumber AS InvoiceNumber,
	tns3_InvoiceDueDate AS InvoiceDueDate,
	tns3_CreditCardAccepted AS CreditCardAccepted,
	tns3_BankAccountAccepted AS BankAccountAccepted,
	tns3_FirstTimePin AS FirstTimePin,
	tns3_LocationAddressLine1 AS LocationAddressLine1,
	tns3_LocationAddressLine2 AS LocationAddressLine2,
	tns3_LocationCity AS LocationCity,
	tns3_LocationStateCode AS LocationStateCode,
	tns3_ServiceCode AS ServiceCode,
	tns3_Strategicprofitcenter AS Strategicprofitcenter,
	tns3_Signaturedebitallowed AS Signaturedebitallowed,
	tns3_LastPaymentDate AS LastPaymentDate,
	tns3_TotalAccountBalance AS TotalAccountBalance,
	tns3_LastPaymentAmount AS LastPaymentAmount,
	tns3_NewBillDrop AS NewBillDrop,
	tns3_NewBillDropDate AS NewBillDropDate,
	tns3_TotalAmountDue AS TotalAmountDue,
	tns3_DueDate AS DueDate,
	tns3_AccountActivityId0,
	tns3_AmountDue
	FROM GetFisCustomerDataFile
),
EXP_Match_TO_FIS_Fields AS (
	SELECT
	-- *INF*: TO_CHAR(SESSSTARTTIME,'YYYYMMDD_hhmmss')
	TO_CHAR(SESSSTARTTIME, 'YYYYMMDD_hhmmss') AS Batch_Id,
	'D' AS Record_Indicator,
	ServiceCode AS Service_Ext_Code,
	AccountNumber AS Primary_Account_Id,
	FirstTimePin AS Secondary_Id,
	TotalAmountDue AS Display_Info_1,
	'"'||Display_Info_1||'"' AS o_Display_Info_1,
	DueDate AS Display_Info_2,
	'"'||Display_Info_2||'"' AS o_Display_Info_2,
	TotalAccountBalance AS Display_Info_3,
	'"'||Display_Info_3||'"' AS o_Display_Info_3,
	LastPaymentAmount AS Display_Info_4,
	'"'||Display_Info_4||'"' AS o_Display_Info_4,
	LastPaymentDate AS Display_Info_5,
	'"'||Display_Info_5||'"' AS o_Display_Info_5,
	tns3_AmountDue AS Total_Amount_Due,
	InvoiceDueDate AS i_InvoiceDueDate,
	-- *INF*: TO_CHAR(i_InvoiceDueDate,'yyyyMMdd')
	TO_CHAR(i_InvoiceDueDate, 'yyyyMMdd') AS o_Date_Due,
	CreditCardAccepted AS Credit_Card_Allowed,
	BankAccountAccepted AS ACH_Allowed,
	CustomerType AS Customer_Type,
	BusinessName AS Business_Name___Full_Name,
	'"'||Business_Name___Full_Name||'"' AS o_BusinessNameFullName,
	LocationAddressLine1 AS Address_Line_1,
	'"'||Address_Line_1 ||'"' AS o_Address_Line_1,
	LocationAddressLine2 AS Address_Line_2,
	'"' || Address_Line_2 || '"' AS o_Address_Line_2,
	LocationCity AS City,
	'"' || City || '"' AS o_City,
	LocationStateCode AS State,
	FirstTimePin AS ZIP_Postal_Code,
	InvoiceNumber AS Invoice___PO_Number,
	AccountId AS Insert_Code,
	Strategicprofitcenter AS Custom_Account_Category,
	NewBillDrop AS New_Bill_Drop,
	NewBillDropDate AS i_NewBillDropDate,
	-- *INF*: TO_CHAR(i_NewBillDropDate,'yyyyMMdd')
	TO_CHAR(i_NewBillDropDate, 'yyyyMMdd') AS o_New_Bill_Drop_Date,
	Signaturedebitallowed AS Signature_Debit__Allowed,
	'' AS DefaultEmptyString,
	-- *INF*: IIF(NOT ISNULL(Primary_Account_Id),v_counter+1,0)
	-- 
	-- -- add null check in case we get empty files
	IFF(Primary_Account_Id IS NOT NULL, v_counter + 1, 0) AS v_counter,
	v_counter AS o_counter
	FROM EXP_WS_Output
),
AGG_Count_Records AS (
	SELECT
	Batch_Id,
	o_counter AS counter,
	-- *INF*: TO_CHAR(COUNT(counter))
	TO_CHAR(COUNT(counter)) AS Number_Of_Records
	FROM EXP_Match_TO_FIS_Fields
	GROUP BY Batch_Id
),
EXP_Output_Header_Trailer AS (
	SELECT
	Batch_Id,
	'H' AS Record_Indicator_Header,
	'T' AS Record_Indicator_Trailer,
	'723021300' AS Biller_Id,
	Number_Of_Records,
	'' AS DefaultEmptyString,
	'WBM' AS CustomerIdentifier,
	'ACR' AS FileIdentifier,
	-- *INF*: TO_CHAR(SESSSTARTTIME,'YYYYMMDDhhmm')
	TO_CHAR(SESSSTARTTIME, 'YYYYMMDDhhmm') AS DateGenerated,
	0 AS DefaultARFTrailerCount
	FROM AGG_Count_Records
),
FIS_ARF_Detail_Header AS (
	INSERT INTO FIS_ARF_Detail_Header
	(RecordType, CustomerIdentifier, FileTypeIdentifier, DateGenerated)
	SELECT 
	Record_Indicator_Header AS RECORDTYPE, 
	CUSTOMERIDENTIFIER, 
	FileIdentifier AS FILETYPEIDENTIFIER, 
	DATEGENERATED
	FROM EXP_Output_Header_Trailer
),
AGG_Count_ACR AS (
	SELECT
	ACRBatchId AS Batch_Id,
	o_counter_ACR AS counter_ACR,
	-- *INF*: COUNT(counter_ACR)
	COUNT(counter_ACR) AS NumberOfACRRecords
	FROM EXP_PARSE_ACR
	GROUP BY 
),
Union_ARF_TrailerCount AS (
	SELECT counter_ACR
	FROM AGG_Count_ACR
	UNION
	SELECT DefaultARFTrailerCount AS counter_ACR
	FROM EXP_Output_Header_Trailer
),
EXP_Legit_Trailer AS (
	SELECT
	'T' AS RecordIndicator,
	counter_ACR,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(counter_ACR),0,
	-- counter_ACR)
	DECODE(
	    TRUE,
	    counter_ACR IS NULL, 0,
	    counter_ACR
	) AS o_counter_ACR
	FROM Union_ARF_TrailerCount
),
AGG_Pick_Trailer AS (
	SELECT
	RecordIndicator,
	o_counter_ACR AS counter_ACR,
	-- *INF*: MAX(counter_ACR)
	MAX(counter_ACR) AS max_counter,
	-- *INF*: TO_CHAR(MAX(counter_ACR))
	TO_CHAR(MAX(counter_ACR)) AS ARFCounter
	FROM EXP_Legit_Trailer
	GROUP BY RecordIndicator
),
EXP_ARF_Trlr AS (
	SELECT
	RecordIndicator,
	ARFCounter
	FROM AGG_Pick_Trailer
),
FIS_ARF_Detail_Trailer AS (
	INSERT INTO FIS_ARF_Detail_Trailer
	(RecordIndicator, NumberOfRecords)
	SELECT 
	RECORDINDICATOR, 
	ARFCounter AS NUMBEROFRECORDS
	FROM EXP_ARF_Trlr
),
FIL_LogError AS (
	SELECT
	tns3_ErrorMessage
	FROM GetFisCustomerDataFile
	WHERE NOT ISNULL(tns3_ErrorMessage)
),
EXP_LogError AS (
	SELECT
	tns3_ErrorMessage,
	-- *INF*: --IIF(NOT ISNULL(tns3_ErrorMessage),ERROR(tns3_ErrorMessage),'')
	'' AS ForceFail
	FROM FIL_LogError
),
ErrorFile AS (
	INSERT INTO ServiceError
	(ErrorMessage)
	SELECT 
	tns3_ErrorMessage AS ERRORMESSAGE
	FROM EXP_LogError
),
EXP_WB_BIL_AccountActivity_Update AS (
	SELECT
	tns3_AccountActivityId0 AS AccountActivityId,
	@{pipeline().parameters.MODIFIEDUSERNAME} AS out_ModifiedUserId,
	SYSDATE AS out_ModifiedDate,
	'I' AS out_Status
	FROM EXP_WS_Output
),
UPD_WB_BIL_AccountActivity AS (
	SELECT
	AccountActivityId, 
	out_ModifiedUserId, 
	out_ModifiedDate, 
	out_Status
	FROM EXP_WB_BIL_AccountActivity_Update
),
WB_BIL_AccountActivity_Update AS (
	MERGE INTO WB_BIL_AccountActivity AS T
	USING UPD_WB_BIL_AccountActivity AS S
	ON T.AccountActivityId = S.AccountActivityId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedUserId = S.out_ModifiedUserId, T.ModifiedDate = S.out_ModifiedDate, T.ProcessedStatusCode = S.out_Status
),
FIS_CDF_Detail_Trailer1 AS (
	INSERT INTO FIS_CDF_Detail_Trailer
	(Batch_Id, Record_Indicator, Number_Of_Records)
	SELECT 
	BATCH_ID, 
	Record_Indicator_Trailer AS RECORD_INDICATOR, 
	NUMBER_OF_RECORDS
	FROM EXP_Output_Header_Trailer
),
FIS_CDF_Detail1 AS (
	INSERT INTO FIS_CDF_Detail
	(Batch_Id, Record_Indicator, Service_Ext_Code, Primary_Account_Id, Secondary_Id, Additional_Authentication_Id, Display_Info_1, Display_Info_2, Display_Info_3, Display_Info_4, Display_Info_5, Display_Info_6, Total_Amount_Due, Date_Due, Pre_fill_1, Pre_fill_2, Pre_fill_3, Payment_Future_Placeholder, Credit_Card_Allowed, ACH_Allowed, Customer_Type, Last_Name, First_Name, Business_Name_/_Full_Name, Secondary_Name_/_Joint_holder_Name, Address_Line_1, Address_Line_2, City, State, ZIP/Postal_Code, Day_time_telephone, E_mail, Marketing_URL, Detailed_Bill_URL, Sales_Tax_Amount, Pre_Fill_Field, Invoice_/_PO_Number, Insert_Code, Custom_Account_Category, New_Bill_Drop, New_Bill_Drop_Date, Bill_Amount_1, Bill_Amount_2, Bill_Amount_3, Bill_Amount_4, Signature_Debit__Allowed, Payment_Amount__Future_Use, Payment_Flag_Future_Use)
	SELECT 
	BATCH_ID, 
	RECORD_INDICATOR, 
	SERVICE_EXT_CODE, 
	PRIMARY_ACCOUNT_ID, 
	SECONDARY_ID, 
	DefaultEmptyString AS ADDITIONAL_AUTHENTICATION_ID, 
	o_Display_Info_1 AS DISPLAY_INFO_1, 
	o_Display_Info_2 AS DISPLAY_INFO_2, 
	o_Display_Info_3 AS DISPLAY_INFO_3, 
	o_Display_Info_4 AS DISPLAY_INFO_4, 
	o_Display_Info_5 AS DISPLAY_INFO_5, 
	DefaultEmptyString AS DISPLAY_INFO_6, 
	TOTAL_AMOUNT_DUE, 
	o_Date_Due AS DATE_DUE, 
	DefaultEmptyString AS PRE_FILL_1, 
	DefaultEmptyString AS PRE_FILL_2, 
	DefaultEmptyString AS PRE_FILL_3, 
	DefaultEmptyString AS PAYMENT_FUTURE_PLACEHOLDER, 
	CREDIT_CARD_ALLOWED, 
	ACH_ALLOWED, 
	CUSTOMER_TYPE, 
	DefaultEmptyString AS LAST_NAME, 
	DefaultEmptyString AS FIRST_NAME, 
	o_BusinessNameFullName AS BUSINESS_NAME_/_FULL_NAME, 
	DefaultEmptyString AS SECONDARY_NAME_/_JOINT_HOLDER_NAME, 
	o_Address_Line_1 AS ADDRESS_LINE_1, 
	o_Address_Line_2 AS ADDRESS_LINE_2, 
	o_City AS CITY, 
	STATE, 
	ZIP_Postal_Code AS ZIP/POSTAL_CODE, 
	DefaultEmptyString AS DAY_TIME_TELEPHONE, 
	DefaultEmptyString AS E_MAIL, 
	DefaultEmptyString AS MARKETING_URL, 
	DefaultEmptyString AS DETAILED_BILL_URL, 
	DefaultEmptyString AS SALES_TAX_AMOUNT, 
	DefaultEmptyString AS PRE_FILL_FIELD, 
	Invoice___PO_Number AS INVOICE_/_PO_NUMBER, 
	INSERT_CODE, 
	CUSTOM_ACCOUNT_CATEGORY, 
	NEW_BILL_DROP, 
	o_New_Bill_Drop_Date AS NEW_BILL_DROP_DATE, 
	DefaultEmptyString AS BILL_AMOUNT_1, 
	DefaultEmptyString AS BILL_AMOUNT_2, 
	DefaultEmptyString AS BILL_AMOUNT_3, 
	DefaultEmptyString AS BILL_AMOUNT_4, 
	SIGNATURE_DEBIT__ALLOWED, 
	DefaultEmptyString AS PAYMENT_AMOUNT__FUTURE_USE, 
	DefaultEmptyString AS PAYMENT_FLAG_FUTURE_USE
	FROM EXP_Match_TO_FIS_Fields
),
FIS_CDF_Detail_Header1 AS (
	INSERT INTO FIS_CDF_Detail_Header
	(Batch_Id, Record_Indicator, Date_Generated, Biller_Id)
	SELECT 
	BATCH_ID, 
	Record_Indicator_Header AS RECORD_INDICATOR, 
	DefaultEmptyString AS DATE_GENERATED, 
	BILLER_ID
	FROM EXP_Output_Header_Trailer
),