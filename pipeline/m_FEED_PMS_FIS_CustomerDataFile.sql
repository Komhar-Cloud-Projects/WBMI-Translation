WITH
SQ_PMS_FIS_PL_CustomerDataFile_Source_FlatFile AS (

-- TODO Manual --

),
EXPTRANS AS (
	SELECT
	FIELD1 AS i_Batch_Id,
	-- *INF*: rtrim(ltrim(i_Batch_Id))
	rtrim(ltrim(i_Batch_Id)) AS o_Batch_id,
	FIELD2 AS i_Record_Indicator,
	-- *INF*: ltrim(rtrim(i_Record_Indicator))
	ltrim(rtrim(i_Record_Indicator)) AS o_Record_Indicator,
	FIELD3 AS i_Service_Ext_Code,
	-- *INF*: ltrim(rtrim(i_Service_Ext_Code))
	ltrim(rtrim(i_Service_Ext_Code)) AS o_Service_Ext_Code,
	FIELD4 AS i_Primary_Account_Id,
	-- *INF*: ltrim(rtrim(i_Primary_Account_Id))
	ltrim(rtrim(i_Primary_Account_Id)) AS o_Primary_Account_Id,
	FIELD5 AS i_Secondary_Id,
	-- *INF*: ltrim(rtrim(i_Secondary_Id))
	ltrim(rtrim(i_Secondary_Id)) AS o_Secondary_Id,
	FIELD6 AS i_Additional_Authentication_Id,
	-- *INF*: rtrim(ltrim(i_Additional_Authentication_Id))
	rtrim(ltrim(i_Additional_Authentication_Id)) AS o_Additional_Authentication_Id,
	FIELD7 AS i_Display_Info_1,
	-- *INF*: rtrim(ltrim(i_Display_Info_1))
	rtrim(ltrim(i_Display_Info_1)) AS o_Display_Info_1,
	FIELD8 AS i_Display_Info_2,
	-- *INF*: rtrim(ltrim(i_Display_Info_2))
	rtrim(ltrim(i_Display_Info_2)) AS o_Display_Info_2,
	FIELD9 AS i_Display_Info_3,
	-- *INF*: rtrim(ltrim(i_Display_Info_3))
	rtrim(ltrim(i_Display_Info_3)) AS o_Display_Info_3,
	FIELD10 AS i_Display_Info_4,
	-- *INF*: rtrim(ltrim(i_Display_Info_4))
	rtrim(ltrim(i_Display_Info_4)) AS o_Display_Info_4,
	FIELD11 AS i_Display_Info_5,
	-- *INF*: rtrim(ltrim(i_Display_Info_5))
	rtrim(ltrim(i_Display_Info_5)) AS o_Display_Info_5,
	FIELD12 AS i_Display_Info_6,
	-- *INF*: rtrim(ltrim(i_Display_Info_6))
	rtrim(ltrim(i_Display_Info_6)) AS o_Display_Info_6,
	FIELD13 AS i_Total_Amount_Due,
	-- *INF*: TO_DECIMAL(i_Total_Amount_Due,2)
	CAST(i_Total_Amount_Due AS FLOAT) AS o_Total_Amount_Due,
	FIELD14 AS i_Date_Due,
	-- *INF*: rtrim(ltrim(i_Date_Due))
	rtrim(ltrim(i_Date_Due)) AS o_Date_Due,
	FIELD15 AS i_Pre_fill_1,
	-- *INF*: rtrim(ltrim(i_Pre_fill_1))
	rtrim(ltrim(i_Pre_fill_1)) AS o_Pre_fill_1,
	FIELD16 AS i_Pre_fill_2,
	-- *INF*: rtrim(ltrim(i_Pre_fill_2))
	rtrim(ltrim(i_Pre_fill_2)) AS o_Pre_fill_2,
	FIELD17 AS i_Pre_fill_3,
	-- *INF*: rtrim(ltrim(i_Pre_fill_3))
	rtrim(ltrim(i_Pre_fill_3)) AS o_Pre_fill_3,
	FIELD18 AS i_Payment_Future_Placeholder,
	-- *INF*: rtrim(ltrim(i_Payment_Future_Placeholder))
	rtrim(ltrim(i_Payment_Future_Placeholder)) AS o_Payment_Future_Placeholder,
	FIELD19 AS i_Credit_Card_Allowed,
	-- *INF*: ltrim(rtrim(i_Credit_Card_Allowed))
	ltrim(rtrim(i_Credit_Card_Allowed)) AS o_Credit_Card_Allowed,
	FIELD20 AS i_ACH_Allowed,
	-- *INF*: rtrim(ltrim(i_ACH_Allowed))
	rtrim(ltrim(i_ACH_Allowed)) AS o_ACH_Allowed,
	FIELD21 AS i_Customer_Type,
	-- *INF*: rtrim(ltrim(i_Customer_Type))
	rtrim(ltrim(i_Customer_Type)) AS o_Customer_Type,
	FIELD22 AS i_Last_Name,
	-- *INF*: rtrim(ltrim(i_Last_Name))
	rtrim(ltrim(i_Last_Name)) AS o_Last_Name,
	FIELD23 AS i_First_Name,
	-- *INF*: rtrim(ltrim(i_First_Name))
	rtrim(ltrim(i_First_Name)) AS o_First_Name,
	FIELD24 AS i_Business_Name___Full_Name,
	-- *INF*: rtrim(ltrim(i_Business_Name___Full_Name))
	rtrim(ltrim(i_Business_Name___Full_Name)) AS o_Business_Name___Full_Name,
	FIELD25 AS i_Secondary_Name___Joint_holder_Name,
	-- *INF*: rtrim(ltrim(i_Secondary_Name___Joint_holder_Name))
	rtrim(ltrim(i_Secondary_Name___Joint_holder_Name)) AS o_Secondary_Name___Joint_holder_Name,
	FIELD26 AS i_Address_Line_1,
	-- *INF*: rtrim(ltrim(i_Address_Line_1))
	rtrim(ltrim(i_Address_Line_1)) AS o_Address_Line_1,
	FIELD27 AS i_Address_Line_2,
	-- *INF*: rtrim(ltrim(i_Address_Line_2))
	rtrim(ltrim(i_Address_Line_2)) AS o_Address_Line_2,
	FIELD28 AS i_City,
	-- *INF*: rtrim(ltrim(i_City))
	rtrim(ltrim(i_City)) AS o_City,
	FIELD29 AS i_State,
	-- *INF*: rtrim(ltrim(i_State))
	rtrim(ltrim(i_State)) AS o_State,
	FIELD30 AS i_ZIP_Postal_Code,
	-- *INF*: rtrim(ltrim(i_ZIP_Postal_Code))
	rtrim(ltrim(i_ZIP_Postal_Code)) AS o_ZIP_Postal_Code,
	FIELD31 AS i_Day_time_telephone,
	-- *INF*: rtrim(ltrim(i_Day_time_telephone))
	rtrim(ltrim(i_Day_time_telephone)) AS o_Day_time_telephone,
	FIELD32 AS i_E_mail,
	-- *INF*: rtrim(ltrim(i_E_mail))
	rtrim(ltrim(i_E_mail)) AS o_E_mail,
	FIELD33 AS i_Marketing_URL,
	-- *INF*: rtrim(ltrim(i_Marketing_URL))
	rtrim(ltrim(i_Marketing_URL)) AS o_Marketing_URL,
	FIELD34 AS i_Detailed_Bill_URL,
	-- *INF*: rtrim(ltrim(i_Detailed_Bill_URL))
	rtrim(ltrim(i_Detailed_Bill_URL)) AS o_Detailed_Bill_URL,
	FIELD35 AS i_Sales_Tax_Amount,
	-- *INF*: rtrim(ltrim(i_Sales_Tax_Amount))
	rtrim(ltrim(i_Sales_Tax_Amount)) AS o_Sales_Tax_Amount,
	FIELD36 AS i_Pre_Fill_Field,
	-- *INF*: rtrim(ltrim(i_Pre_Fill_Field))
	rtrim(ltrim(i_Pre_Fill_Field)) AS o_Pre_Fill_Field,
	FIELD37 AS i_Invoice___PO_Number,
	-- *INF*: rtrim(ltrim(i_Invoice___PO_Number))
	rtrim(ltrim(i_Invoice___PO_Number)) AS o_Invoice___PO_Number,
	FIELD38 AS i_Insert_Code,
	-- *INF*: rtrim(ltrim(i_Insert_Code))
	rtrim(ltrim(i_Insert_Code)) AS o_Insert_Code,
	FIELD39 AS i_Custom_Account_Category,
	-- *INF*: rtrim(ltrim(i_Custom_Account_Category))
	rtrim(ltrim(i_Custom_Account_Category)) AS o_Custom_Account_Category,
	FIELD40 AS i_New_Bill_Drop,
	-- *INF*: rtrim(ltrim(i_New_Bill_Drop))
	rtrim(ltrim(i_New_Bill_Drop)) AS o_New_Bill_Drop,
	FIELD41 AS i_New_Bill_Drop_Date,
	-- *INF*: rtrim(ltrim(i_New_Bill_Drop_Date))
	rtrim(ltrim(i_New_Bill_Drop_Date)) AS o_New_Bill_Drop_Date,
	FIELD42 AS i_Bill_Amount_1,
	-- *INF*: rtrim(ltrim(i_Bill_Amount_1))
	rtrim(ltrim(i_Bill_Amount_1)) AS o_Bill_Amount_1,
	FIELD43 AS i_Bill_Amount_2,
	-- *INF*: rtrim(ltrim(i_Bill_Amount_2))
	rtrim(ltrim(i_Bill_Amount_2)) AS o_Bill_Amount_2,
	FIELD44 AS i_Bill_Amount_3,
	-- *INF*: rtrim(ltrim(i_Bill_Amount_3))
	rtrim(ltrim(i_Bill_Amount_3)) AS o_Bill_Amount_3,
	FIELD45 AS i_Bill_Amount_4,
	-- *INF*: rtrim(ltrim(i_Bill_Amount_4))
	rtrim(ltrim(i_Bill_Amount_4)) AS o_Bill_Amount_4,
	FIELD46 AS i_Signature_Debit__Allowed,
	-- *INF*: rtrim(ltrim(i_Signature_Debit__Allowed))
	rtrim(ltrim(i_Signature_Debit__Allowed)) AS o_Signature_Debit__Allowed,
	FIELD47 AS i_Payment_Amount__Future_Use,
	-- *INF*: rtrim(ltrim(i_Payment_Amount__Future_Use))
	rtrim(ltrim(i_Payment_Amount__Future_Use)) AS o_Payment_Amount__Future_Use,
	FIELD48 AS i_Payment_Flag_Future_Use,
	-- *INF*: rtrim(ltrim(i_Payment_Flag_Future_Use))
	rtrim(ltrim(i_Payment_Flag_Future_Use)) AS o_Payment_Flag_Future_Use
	FROM SQ_PMS_FIS_PL_CustomerDataFile_Source_FlatFile
),
FIS_CDF_Detail_PMS AS (
	INSERT INTO FIS_CDF_Detail
	(Batch_Id, Record_Indicator, Service_Ext_Code, Primary_Account_Id, Secondary_Id, Additional_Authentication_Id, Display_Info_1, Display_Info_2, Display_Info_3, Display_Info_4, Display_Info_5, Display_Info_6, Total_Amount_Due, Date_Due, Pre_fill_1, Pre_fill_2, Pre_fill_3, Payment_Future_Placeholder, Credit_Card_Allowed, ACH_Allowed, Customer_Type, Last_Name, First_Name, Business_Name_/_Full_Name, Secondary_Name_/_Joint_holder_Name, Address_Line_1, Address_Line_2, City, State, ZIP/Postal_Code, Day_time_telephone, E_mail, Marketing_URL, Detailed_Bill_URL, Sales_Tax_Amount, Pre_Fill_Field, Invoice_/_PO_Number, Insert_Code, Custom_Account_Category, New_Bill_Drop, New_Bill_Drop_Date, Bill_Amount_1, Bill_Amount_2, Bill_Amount_3, Bill_Amount_4, Signature_Debit__Allowed, Payment_Amount__Future_Use, Payment_Flag_Future_Use)
	SELECT 
	o_Batch_id AS BATCH_ID, 
	o_Record_Indicator AS RECORD_INDICATOR, 
	o_Service_Ext_Code AS SERVICE_EXT_CODE, 
	o_Primary_Account_Id AS PRIMARY_ACCOUNT_ID, 
	o_Secondary_Id AS SECONDARY_ID, 
	o_Additional_Authentication_Id AS ADDITIONAL_AUTHENTICATION_ID, 
	o_Display_Info_1 AS DISPLAY_INFO_1, 
	o_Display_Info_2 AS DISPLAY_INFO_2, 
	o_Display_Info_3 AS DISPLAY_INFO_3, 
	o_Display_Info_4 AS DISPLAY_INFO_4, 
	o_Display_Info_5 AS DISPLAY_INFO_5, 
	o_Display_Info_6 AS DISPLAY_INFO_6, 
	o_Total_Amount_Due AS TOTAL_AMOUNT_DUE, 
	o_Date_Due AS DATE_DUE, 
	o_Pre_fill_1 AS PRE_FILL_1, 
	o_Pre_fill_2 AS PRE_FILL_2, 
	o_Pre_fill_3 AS PRE_FILL_3, 
	o_Payment_Future_Placeholder AS PAYMENT_FUTURE_PLACEHOLDER, 
	o_Credit_Card_Allowed AS CREDIT_CARD_ALLOWED, 
	o_ACH_Allowed AS ACH_ALLOWED, 
	o_Customer_Type AS CUSTOMER_TYPE, 
	o_Last_Name AS LAST_NAME, 
	o_First_Name AS FIRST_NAME, 
	o_Business_Name___Full_Name AS BUSINESS_NAME_/_FULL_NAME, 
	o_Secondary_Name___Joint_holder_Name AS SECONDARY_NAME_/_JOINT_HOLDER_NAME, 
	o_Address_Line_1 AS ADDRESS_LINE_1, 
	o_Address_Line_2 AS ADDRESS_LINE_2, 
	o_City AS CITY, 
	o_State AS STATE, 
	o_ZIP_Postal_Code AS ZIP/POSTAL_CODE, 
	o_Day_time_telephone AS DAY_TIME_TELEPHONE, 
	o_E_mail AS E_MAIL, 
	o_Marketing_URL AS MARKETING_URL, 
	o_Detailed_Bill_URL AS DETAILED_BILL_URL, 
	o_Sales_Tax_Amount AS SALES_TAX_AMOUNT, 
	o_Pre_Fill_Field AS PRE_FILL_FIELD, 
	o_Invoice___PO_Number AS INVOICE_/_PO_NUMBER, 
	o_Insert_Code AS INSERT_CODE, 
	o_Custom_Account_Category AS CUSTOM_ACCOUNT_CATEGORY, 
	o_New_Bill_Drop AS NEW_BILL_DROP, 
	o_New_Bill_Drop_Date AS NEW_BILL_DROP_DATE, 
	o_Bill_Amount_1 AS BILL_AMOUNT_1, 
	o_Bill_Amount_2 AS BILL_AMOUNT_2, 
	o_Bill_Amount_3 AS BILL_AMOUNT_3, 
	o_Bill_Amount_4 AS BILL_AMOUNT_4, 
	o_Signature_Debit__Allowed AS SIGNATURE_DEBIT__ALLOWED, 
	o_Payment_Amount__Future_Use AS PAYMENT_AMOUNT__FUTURE_USE, 
	o_Payment_Flag_Future_Use AS PAYMENT_FLAG_FUTURE_USE
	FROM EXPTRANS
),
AGG_Count_Records AS (
	SELECT
	o_Batch_id AS Batch_Id,
	o_Record_Indicator AS Record_Indicator,
	-- *INF*: TO_CHAR(COUNT(Record_Indicator))
	TO_CHAR(COUNT(Record_Indicator)) AS Number_Of_Records
	FROM EXPTRANS
	GROUP BY Batch_Id
),
EXP_Output_Header_Trailer AS (
	SELECT
	Batch_Id,
	'H' AS Record_Indicator_Header,
	'T' AS Record_Indicator_Trailer,
	'723021300' AS Biller_Id,
	Number_Of_Records,
	'' AS DefaultEmptyString
	FROM AGG_Count_Records
),
FIS_CDF_Detail_Trailer_PMS AS (
	INSERT INTO FIS_CDF_Detail_Trailer
	(Batch_Id, Record_Indicator, Number_Of_Records)
	SELECT 
	BATCH_ID, 
	Record_Indicator_Trailer AS RECORD_INDICATOR, 
	NUMBER_OF_RECORDS
	FROM EXP_Output_Header_Trailer
),
FIS_CDF_Detail_Header_PMS AS (
	INSERT INTO FIS_CDF_Detail_Header
	(Batch_Id, Record_Indicator, Date_Generated, Biller_Id)
	SELECT 
	BATCH_ID, 
	Record_Indicator_Header AS RECORD_INDICATOR, 
	DefaultEmptyString AS DATE_GENERATED, 
	BILLER_ID
	FROM EXP_Output_Header_Trailer
),