WITH
SQ_medical_bill AS (
	SELECT medical_bill.vendor_bill_num,
		medical_bill.acct_id,
		medical_bill.bill_review_vendor_code,
		medical_bill.draft_num,
		medical_bill.draft_amt,
		medical_bill.denial_rsn_code,
		CONVERT(VARCHAR(10), medical_bill.draft_paid_date, 111) AS draft_paid_date
	FROM medical_bill
	WHERE crrnt_snpsht_flag = 1
		AND (medical_bill.ebill_ind = 'Y'
			OR 
			medical_bill.bill_review_vendor_code = 'CPIQ')
		AND medical_bill.draft_paid_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	-- VSTS-US-27369: Send non-ebills for Equian (CPIQ).
),
EXP_Source AS (
	SELECT
	vendor_bill_num,
	draft_paid_date,
	draft_amt,
	draft_num,
	denial_rsn_code,
	bill_review_vendor_code,
	acct_id
	FROM SQ_medical_bill
),
Exp_Target AS (
	SELECT
	vendor_bill_num,
	draft_paid_date,
	-- *INF*: iif(draft_paid_date = to_date('01/01/1800','mm/dd/yyyy') ,' ' ,to_char(draft_paid_date,'yyyymmdd'))
	IFF(
	    draft_paid_date = TO_TIMESTAMP('01/01/1800', 'mm/dd/yyyy'), ' ',
	    to_char(draft_paid_date, 'yyyymmdd')
	) AS draft_paid_date_out,
	draft_amt,
	-- *INF*: IIF (draft_amt = ROUND(draft_amt),   
	-- lpad(draft_amt,7,'0') || '.00',
	-- lpad(draft_amt,10,'0'))
	-- 
	-- --IIF(draft_amt=0,'0000000000',	lpad(draft_amt,10,'0'))
	IFF(
	    draft_amt = ROUND(draft_amt), lpad(draft_amt, 7, '0') || '.00', lpad(draft_amt, 10, '0')
	) AS v_draft_amt,
	-- *INF*: IIF(Instr(v_draft_amt,'.')=9, substr(v_draft_amt,2) || '0', v_draft_amt)
	IFF(REGEXP_INSTR(v_draft_amt, '.') = 9, substr(v_draft_amt, 2) || '0', v_draft_amt) AS draft_amt_out,
	draft_num,
	-- *INF*: REPLACESTR(0, :UDF.DEFAULT_VALUE_TO_BLANKS(draft_num), 
	-- 'EFT', 
	-- '000')
	REGEXP_REPLACE(UDF_DEFAULT_VALUE_TO_BLANKS(draft_num),'EFT','000','i') AS v_draft_num,
	v_draft_num AS draft_num_out,
	-- *INF*: LTRIM(v_draft_num, ' 0')
	LTRIM(v_draft_num, ' 0') AS EquianCheckNumber,
	denial_rsn_code,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(denial_rsn_code)
	UDF_DEFAULT_VALUE_TO_BLANKS(denial_rsn_code) AS denial_rsn_code_out,
	sysdate AS Curent_date,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	bill_review_vendor_code,
	vendor_bill_num AS vendor_bill_num_len30,
	-- *INF*: LTRIM(:UDF.RIGHTMOST_N_NONBLANKCHARACTERS(vendor_bill_num_len30, 10), ' 0')
	LTRIM(UDF_RIGHTMOST_N_NONBLANKCHARACTERS(vendor_bill_num_len30, 10), ' 0') AS o_EquianControlNumber,
	acct_id,
	-- *INF*: RTRIM(SUBSTR(acct_id, 0, 10))
	RTRIM(SUBSTR(acct_id, 0, 10)) AS o_acct_id,
	-- *INF*: LPAD(REPLACECHR(FALSE,
	--     TO_CHAR(ROUND(draft_amt, 2)),
	--     '.',
	--     ''), 
	-- 12, 
	-- '0')
	LPAD(REGEXP_REPLACE(TO_CHAR(ROUND(draft_amt, 2)),'.',''), 12, '0') AS o_draft_amt_lpad_nodecimalpoint,
	'P' AS EquianPaymentStatus,
	-- *INF*: TO_CHAR(SESSSTARTTIME,'yyyymmddHH24MISS')
	TO_CHAR(SESSSTARTTIME, 'yyyymmddHH24MISS') AS ProcessTimeStamp,
	'' AS EmptyString
	FROM EXP_Source
),
EXP_rules AS (
	SELECT
	vendor_bill_num,
	draft_paid_date_out,
	draft_amt_out,
	draft_num_out,
	EquianCheckNumber,
	denial_rsn_code_out,
	Curent_date,
	audit_id,
	-- *INF*: IIF(bill_review_vendor_code = 'CPIQ',
	-- IIF(draft_num_out <> '',0,1),
	-- DECODE(TRUE,
	-- IS_SPACES(denial_rsn_code_out),0,
	-- ISNULL(denial_rsn_code_out),0,
	-- LENGTH(denial_rsn_code_out)=0,0,
	-- 1))
	IFF(
	    bill_review_vendor_code = 'CPIQ', IFF(
	        draft_num_out <> '', 0, 1
	    ),
	    DECODE(
	        TRUE,
	        LENGTH(denial_rsn_code_out)>0
	    and TRIM(denial_rsn_code_out)='', 0,
	        denial_rsn_code_out IS NULL, 0,
	        LENGTH(denial_rsn_code_out) = 0, 0,
	        1
	    )
	) AS v_apply_filter,
	v_apply_filter AS apply_filter,
	bill_review_vendor_code,
	vendor_bill_num_len30,
	o_acct_id AS EquianOfficeId,
	o_EquianControlNumber AS EquianControlNumber,
	o_draft_amt_lpad_nodecimalpoint AS draft_amt_lpad_nodecimalpoint,
	EquianPaymentStatus,
	ProcessTimeStamp,
	EmptyString
	FROM Exp_Target
),
FIL_check_filter AS (
	SELECT
	vendor_bill_num, 
	draft_paid_date_out, 
	draft_amt_out, 
	draft_num_out, 
	EquianCheckNumber, 
	denial_rsn_code_out, 
	Curent_date, 
	audit_id, 
	apply_filter, 
	bill_review_vendor_code, 
	vendor_bill_num_len30, 
	EquianOfficeId, 
	EquianControlNumber, 
	draft_amt_lpad_nodecimalpoint, 
	EquianPaymentStatus, 
	ProcessTimeStamp, 
	EmptyString
	FROM EXP_rules
	WHERE apply_filter=0
),
RTR_split_by_vendor_code AS (
	SELECT
	vendor_bill_num,
	draft_paid_date_out,
	draft_amt_out,
	draft_num_out,
	EquianCheckNumber,
	denial_rsn_code_out,
	Curent_date,
	audit_id,
	bill_review_vendor_code,
	vendor_bill_num_len30,
	EquianOfficeId,
	EquianControlNumber,
	draft_amt_lpad_nodecimalpoint,
	EquianPaymentStatus,
	ProcessTimeStamp,
	EmptyString
	FROM FIL_check_filter
),
RTR_split_by_vendor_code_RISG AS (SELECT * FROM RTR_split_by_vendor_code WHERE bill_review_vendor_code='RISG'),
RTR_split_by_vendor_code_MYMX AS (SELECT * FROM RTR_split_by_vendor_code WHERE bill_review_vendor_code='MYMX'),
RTR_split_by_vendor_code_ALGN AS (SELECT * FROM RTR_split_by_vendor_code WHERE bill_review_vendor_code='ALGN'),
RTR_split_by_vendor_code_CPIQ AS (SELECT * FROM RTR_split_by_vendor_code WHERE bill_review_vendor_code='CPIQ'),
RTR_split_by_vendor_code_All AS (SELECT * FROM RTR_split_by_vendor_code WHERE TRUE),
RTR_split_by_vendor_code_CRVL AS (SELECT * FROM RTR_split_by_vendor_code WHERE bill_review_vendor_code='CRVL'),
work_claim_eor_payment_extract AS (
	INSERT INTO work_claim_eor_payment_extract
	(vendor_bill_num, date_paid, paid_amt, check_num, denial_rsn_code, created_date, modified_date, audit_id, VendorCode)
	SELECT 
	VENDOR_BILL_NUM, 
	draft_paid_date_out AS DATE_PAID, 
	draft_amt_out AS PAID_AMT, 
	draft_num_out AS CHECK_NUM, 
	denial_rsn_code_out AS DENIAL_RSN_CODE, 
	Curent_date AS CREATED_DATE, 
	Curent_date AS MODIFIED_DATE, 
	AUDIT_ID, 
	bill_review_vendor_code AS VENDORCODE
	FROM RTR_split_by_vendor_code_All
),
EOR_Payments_Extract_CRVL_len30_DCN AS (
	INSERT INTO EOR_Payments_Extract_len30_DCN
	(DOCUMENT_CONTROL_NUM, DATE_PAID, PAID_AMT, CHECK_NUM, DENIAL_RSN_CODE)
	SELECT 
	vendor_bill_num_len30 AS DOCUMENT_CONTROL_NUM, 
	draft_paid_date_out AS DATE_PAID, 
	draft_amt_out AS PAID_AMT, 
	draft_num_out AS CHECK_NUM, 
	denial_rsn_code_out AS DENIAL_RSN_CODE
	FROM RTR_split_by_vendor_code_CRVL
),
EOR_Payments_Extract_ALGN AS (
	INSERT INTO EOR_Payments_Extract
	(DOCUMENT_CONTROL_NUM, DATE_PAID, PAID_AMT, CHECK_NUM, DENIAL_RSN_CODE)
	SELECT 
	vendor_bill_num AS DOCUMENT_CONTROL_NUM, 
	draft_paid_date_out AS DATE_PAID, 
	draft_amt_out AS PAID_AMT, 
	draft_num_out AS CHECK_NUM, 
	denial_rsn_code_out AS DENIAL_RSN_CODE
	FROM RTR_split_by_vendor_code_ALGN
),
EOR_Payments_Extract_MYMX AS (
	INSERT INTO EOR_Payments_Extract
	(DOCUMENT_CONTROL_NUM, DATE_PAID, PAID_AMT, CHECK_NUM, DENIAL_RSN_CODE)
	SELECT 
	vendor_bill_num AS DOCUMENT_CONTROL_NUM, 
	draft_paid_date_out AS DATE_PAID, 
	draft_amt_out AS PAID_AMT, 
	draft_num_out AS CHECK_NUM, 
	denial_rsn_code_out AS DENIAL_RSN_CODE
	FROM RTR_split_by_vendor_code_MYMX
),
EOR_Payments_Extract_RISG AS (
	INSERT INTO EOR_Payments_Extract
	(DOCUMENT_CONTROL_NUM, DATE_PAID, PAID_AMT, CHECK_NUM, DENIAL_RSN_CODE)
	SELECT 
	vendor_bill_num AS DOCUMENT_CONTROL_NUM, 
	draft_paid_date_out AS DATE_PAID, 
	draft_amt_out AS PAID_AMT, 
	draft_num_out AS CHECK_NUM, 
	denial_rsn_code_out AS DENIAL_RSN_CODE
	FROM RTR_split_by_vendor_code_RISG
),
EOR_Payments_Equian AS (
	INSERT INTO EOR_Payments_Equian
	(OfficeID, ControlNumber, CheckNumber, CheckDate, AmountPaid, BankAccountNumber, ClientSpecific1, ClientSpecific2, ClientSpecific3, ClientSpecific4, ClientSpecific5, PaymentStatus, ErrorCode, ErrorDescription, ProcessTimeStamp)
	SELECT 
	EquianOfficeId AS OFFICEID, 
	EquianControlNumber AS CONTROLNUMBER, 
	EquianCheckNumber AS CHECKNUMBER, 
	draft_paid_date_out AS CHECKDATE, 
	draft_amt_lpad_nodecimalpoint AS AMOUNTPAID, 
	EmptyString AS BANKACCOUNTNUMBER, 
	EmptyString AS CLIENTSPECIFIC1, 
	EmptyString AS CLIENTSPECIFIC2, 
	EmptyString AS CLIENTSPECIFIC3, 
	EmptyString AS CLIENTSPECIFIC4, 
	EmptyString AS CLIENTSPECIFIC5, 
	EquianPaymentStatus AS PAYMENTSTATUS, 
	EmptyString AS ERRORCODE, 
	EmptyString AS ERRORDESCRIPTION, 
	PROCESSTIMESTAMP
	FROM RTR_split_by_vendor_code_CPIQ
),