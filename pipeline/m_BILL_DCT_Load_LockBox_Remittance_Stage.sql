WITH
SQ_LockBoxHeaderFile AS (

-- TODO Manual --

),
fil_Process_Header_Record_Type AS (
	SELECT
	Record_Type, 
	Header_filler AS Hearder_Filler, 
	Header_Process_Date, 
	Header_filler1 AS Header_Filler1, 
	Header_File_Description
	FROM SQ_LockBoxHeaderFile
	WHERE Record_Type = 'H'
),
exp_Header_Prep_4_Join AS (
	SELECT
	Record_Type,
	Hearder_Filler,
	Header_Process_Date,
	Header_Filler1,
	Header_File_Description,
	'1' AS hdr_join_condition
	FROM fil_Process_Header_Record_Type
),
SQ_LockBoxFile AS (

-- TODO Manual --

),
exp_Lockbox AS (
	SELECT
	Record_Type,
	Batch_number,
	Batch_Seq,
	File_ID,
	Filler_1,
	Account_Number,
	Filler_2,
	Full_Pay_Amt,
	Min_Pay_Amt,
	File_Filler,
	Amount_Paid,
	'1' AS detail_join_condition
	FROM SQ_LockBoxFile
),
fil_Process_Detail_S_M_Types AS (
	SELECT
	Record_Type, 
	Batch_number, 
	Batch_Seq, 
	File_ID, 
	Filler_1, 
	Account_Number, 
	Filler_2, 
	Full_Pay_Amt, 
	Min_Pay_Amt, 
	File_Filler, 
	Amount_Paid, 
	detail_join_condition
	FROM exp_Lockbox
	WHERE Record_Type = 'S'  OR  Record_Type = 'M'
),
jnr_Join_HeaderDate_2_DetailData AS (SELECT
	fil_Process_Detail_S_M_Types.Record_Type, 
	fil_Process_Detail_S_M_Types.Batch_number, 
	fil_Process_Detail_S_M_Types.Batch_Seq, 
	fil_Process_Detail_S_M_Types.File_ID, 
	fil_Process_Detail_S_M_Types.Filler_1, 
	fil_Process_Detail_S_M_Types.Account_Number, 
	fil_Process_Detail_S_M_Types.Filler_2, 
	fil_Process_Detail_S_M_Types.Full_Pay_Amt, 
	fil_Process_Detail_S_M_Types.Min_Pay_Amt, 
	fil_Process_Detail_S_M_Types.File_Filler, 
	fil_Process_Detail_S_M_Types.Amount_Paid, 
	fil_Process_Detail_S_M_Types.detail_join_condition, 
	exp_Header_Prep_4_Join.Record_Type AS Record_Type1, 
	exp_Header_Prep_4_Join.Header_Process_Date, 
	exp_Header_Prep_4_Join.Header_File_Description, 
	exp_Header_Prep_4_Join.hdr_join_condition
	FROM fil_Process_Detail_S_M_Types
	INNER JOIN exp_Header_Prep_4_Join
	ON exp_Header_Prep_4_Join.hdr_join_condition = fil_Process_Detail_S_M_Types.detail_join_condition
),
exp_Prepare_Data_4_Stage_Load AS (
	SELECT
	Record_Type,
	Batch_number,
	Batch_Seq,
	File_ID,
	Filler_1,
	Account_Number,
	Filler_2,
	Full_Pay_Amt,
	Min_Pay_Amt,
	File_Filler,
	Amount_Paid,
	Header_Process_Date AS Header_Process_Date1,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS out_ModifyDate,
	'L' AS out_RemittanceSource,
	'N' AS out_ProcessStatus,
	Batch_number || Batch_Seq  ||  Header_Process_Date1 AS out_TransactionID,
	'Payment' AS out_Transaction_Type,
	'InformS' AS out_ModifiedUserID,
	'LockBox' AS out_AuthorizeBy,
	Record_Type  ||  Batch_number  ||  Batch_Seq  ||  File_ID  || Account_Number  ||  Full_Pay_Amt  ||  Min_Pay_Amt  ||  Amount_Paid  ||  Header_Process_Date1 AS out_TransactionData,
	'LCK' AS out_PaymentMode,
	-- *INF*: (TO_DECIMAL(Full_Pay_Amt)  *  .01)
	(CAST(Full_Pay_Amt AS FLOAT) * .01) AS out_Full_Pay_Amt,
	-- *INF*: (TO_DECIMAL(Amount_Paid)  *  .01)
	(CAST(Amount_Paid AS FLOAT) * .01) AS out_Amount_Paid,
	-- *INF*: (TO_DECIMAL(Min_Pay_Amt) * .01)
	(CAST(Min_Pay_Amt AS FLOAT) * .01) AS out_Min_Pay_Amt,
	-- *INF*: TO_DATE(Header_Process_Date1,'MMDDYY')
	-- 
	--  --TO_CHAR(Header_Process_Date,'DD-MM-YYYY');
	TO_TIMESTAMP(Header_Process_Date1, 'MMDDYY') AS o_Header_process_date,
	-- *INF*: SUBSTR(Account_Number,1,1)
	SUBSTR(Account_Number, 1, 1) AS v_CC_1,
	-- *INF*: DECODE(true,v_CC_1 ='9',
	-- SUBSTR(Account_Number,3,7),
	-- ' ' 
	-- )
	DECODE(
	    true,
	    v_CC_1 = '9', SUBSTR(Account_Number, 3, 7),
	    ' '
	) AS v_CC_2_7,
	-- *INF*: DECODE(true,SUBSTR(Account_Number,10,1) = '4',
	--    'D',
	-- SUBSTR(Account_Number,10,1)= '8',
	--    'H',
	-- ' ')
	DECODE(
	    true,
	    SUBSTR(Account_Number, 10, 1) = '4', 'D',
	    SUBSTR(Account_Number, 10, 1) = '8', 'H',
	    ' '
	) AS v_CC_8,
	-- *INF*: SUBSTR(Account_Number,11,2)
	SUBSTR(Account_Number, 11, 2) AS v_CC_9_10,
	-- *INF*: DECODE(true,v_CC_1 ='9',
	-- SUBSTR(Account_Number,2,2),
	-- SUBSTR(Account_Number,2,2))
	DECODE(
	    true,
	    v_CC_1 = '9', SUBSTR(Account_Number, 2, 2),
	    SUBSTR(Account_Number, 2, 2)
	) AS v_CC_1_2,
	-- *INF*: DECODE(true,v_CC_1_2 = '01','A',
	-- v_CC_1_2 = '01','A',
	-- v_CC_1_2 = '02','B',
	-- v_CC_1_2 = '03','C',
	-- v_CC_1_2 = '04','D',
	-- v_CC_1_2 = '05','E',
	-- v_CC_1_2 = '06','F',
	-- v_CC_1_2 = '07','G',
	-- v_CC_1_2 = '08','H',
	-- v_CC_1_2 = '09','I',
	-- v_CC_1_2 = '10','J',
	-- v_CC_1_2 = '11','K',
	-- v_CC_1_2 = '12','L',
	-- v_CC_1_2 = '13','M',
	-- v_CC_1_2 = '14','N',
	-- v_CC_1_2 = '15','O',
	-- v_CC_1_2 = '16','P',
	-- v_CC_1_2 = '17','Q',
	-- v_CC_1_2 = '18','R',
	-- v_CC_1_2 = '19','S',
	-- v_CC_1_2 = '20','T',
	-- v_CC_1_2 = '21','U',
	-- v_CC_1_2 = '22','V',
	-- v_CC_1_2 = '23','W',
	-- v_CC_1_2 = '24','X',
	-- v_CC_1_2 = '25','Y',
	-- v_CC_1_2 = '26','Z',
	-- ' ')  
	-- 
	DECODE(
	    true,
	    v_CC_1_2 = '01', 'A',
	    v_CC_1_2 = '01', 'A',
	    v_CC_1_2 = '02', 'B',
	    v_CC_1_2 = '03', 'C',
	    v_CC_1_2 = '04', 'D',
	    v_CC_1_2 = '05', 'E',
	    v_CC_1_2 = '06', 'F',
	    v_CC_1_2 = '07', 'G',
	    v_CC_1_2 = '08', 'H',
	    v_CC_1_2 = '09', 'I',
	    v_CC_1_2 = '10', 'J',
	    v_CC_1_2 = '11', 'K',
	    v_CC_1_2 = '12', 'L',
	    v_CC_1_2 = '13', 'M',
	    v_CC_1_2 = '14', 'N',
	    v_CC_1_2 = '15', 'O',
	    v_CC_1_2 = '16', 'P',
	    v_CC_1_2 = '17', 'Q',
	    v_CC_1_2 = '18', 'R',
	    v_CC_1_2 = '19', 'S',
	    v_CC_1_2 = '20', 'T',
	    v_CC_1_2 = '21', 'U',
	    v_CC_1_2 = '22', 'V',
	    v_CC_1_2 = '23', 'W',
	    v_CC_1_2 = '24', 'X',
	    v_CC_1_2 = '25', 'Y',
	    v_CC_1_2 = '26', 'Z',
	    ' '
	) AS out_Conv_CC_1,
	-- *INF*: DECODE(true,v_CC_1 ='9',
	-- (out_Conv_CC_1 || v_CC_2_7 ||  ' ' || v_CC_8 || v_CC_9_10),
	-- SUBSTR(Account_Number,1,12) 
	-- 
	-- )
	DECODE(
	    true,
	    v_CC_1 = '9', (out_Conv_CC_1 || v_CC_2_7 || ' ' || v_CC_8 || v_CC_9_10),
	    SUBSTR(Account_Number, 1, 12)
	) AS v_Conv_acct_num,
	-- *INF*: DECODE(true,
	-- v_CC_1 <> '9',SUBSTR(Account_Number,1,12),
	-- null)
	DECODE(
	    true,
	    v_CC_1 <> '9', SUBSTR(Account_Number, 1, 12),
	    null
	) AS out_account,
	-- *INF*: DECODE(true,
	-- v_CC_1 ='9',v_Conv_acct_num,
	-- null) 
	DECODE(
	    true,
	    v_CC_1 = '9', v_Conv_acct_num,
	    null
	) AS out_Policy_number
	FROM jnr_Join_HeaderDate_2_DetailData
),
LKP_WB_Bill_Remittance_stage AS (
	SELECT
	TransactionId,
	In_TransactionId,
	IN_RemittanceSource,
	RemittanceSource
	FROM (
		SELECT 
			TransactionId,
			In_TransactionId,
			IN_RemittanceSource,
			RemittanceSource
		FROM WB_BIL_RemittanceStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TransactionId,RemittanceSource ORDER BY TransactionId) = 1
),
fil_Load_Ready_2_Stage_Data AS (
	SELECT
	LKP_WB_Bill_Remittance_stage.TransactionId AS Transaction_Id_Lookup, 
	LKP_WB_Bill_Remittance_stage.In_TransactionId AS IN_TransactionId, 
	exp_Prepare_Data_4_Stage_Load.Account_Number, 
	exp_Prepare_Data_4_Stage_Load.out_ModifyDate, 
	exp_Prepare_Data_4_Stage_Load.out_RemittanceSource, 
	exp_Prepare_Data_4_Stage_Load.out_ProcessStatus, 
	exp_Prepare_Data_4_Stage_Load.out_TransactionID, 
	exp_Prepare_Data_4_Stage_Load.out_ModifiedUserID, 
	exp_Prepare_Data_4_Stage_Load.out_Transaction_Type, 
	exp_Prepare_Data_4_Stage_Load.o_Header_process_date AS out_DateInitiated, 
	exp_Prepare_Data_4_Stage_Load.out_ModifyDate AS out_EffectiveDate, 
	exp_Prepare_Data_4_Stage_Load.out_account, 
	exp_Prepare_Data_4_Stage_Load.out_AuthorizeBy, 
	exp_Prepare_Data_4_Stage_Load.out_TransactionData, 
	exp_Prepare_Data_4_Stage_Load.out_PaymentMode, 
	exp_Prepare_Data_4_Stage_Load.out_Full_Pay_Amt, 
	exp_Prepare_Data_4_Stage_Load.out_Amount_Paid, 
	exp_Prepare_Data_4_Stage_Load.out_Min_Pay_Amt, 
	exp_Prepare_Data_4_Stage_Load.out_Policy_number AS out_policy_number
	FROM exp_Prepare_Data_4_Stage_Load
	LEFT JOIN LKP_WB_Bill_Remittance_stage
	ON LKP_WB_Bill_Remittance_stage.TransactionId = exp_Prepare_Data_4_Stage_Load.out_TransactionID AND LKP_WB_Bill_Remittance_stage.RemittanceSource = exp_Prepare_Data_4_Stage_Load.out_RemittanceSource
	WHERE ISNULL(Transaction_Id_Lookup)
),
WB_BIL_RemittanceStage AS (
	INSERT INTO WB_BIL_RemittanceStage
	(ModifiedUserId, ModifiedDate, TransactionId, TransactionType, TransactionData, RemittanceSource, ProcessedStatusCode, ReconciliationDate, VendorAccountNumber, EffectiveDate, DateInitiated, BillAmount, PaidAmount, PaymentMode, AuthorizedBy, MinimumPayment, PolicyNumber)
	SELECT 
	out_ModifiedUserID AS MODIFIEDUSERID, 
	out_ModifyDate AS MODIFIEDDATE, 
	IN_TransactionId AS TRANSACTIONID, 
	out_Transaction_Type AS TRANSACTIONTYPE, 
	out_TransactionData AS TRANSACTIONDATA, 
	out_RemittanceSource AS REMITTANCESOURCE, 
	out_ProcessStatus AS PROCESSEDSTATUSCODE, 
	out_ModifyDate AS RECONCILIATIONDATE, 
	out_account AS VENDORACCOUNTNUMBER, 
	out_EffectiveDate AS EFFECTIVEDATE, 
	out_DateInitiated AS DATEINITIATED, 
	out_Full_Pay_Amt AS BILLAMOUNT, 
	out_Amount_Paid AS PAIDAMOUNT, 
	out_PaymentMode AS PAYMENTMODE, 
	out_AuthorizeBy AS AUTHORIZEDBY, 
	out_Min_Pay_Amt AS MINIMUMPAYMENT, 
	out_policy_number AS POLICYNUMBER
	FROM fil_Load_Ready_2_Stage_Data
),