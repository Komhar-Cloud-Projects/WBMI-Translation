WITH
SQ_FIS_APR_Update AS (

-- TODO Manual --

),
FILTRANS AS (
	SELECT
	Frequency_code, 
	End_date, 
	Biller_id, 
	Catagory, 
	Unit_code, 
	Service_code, 
	Record_type, 
	Account_Num, 
	Acct_Status, 
	Status_timestamp, 
	Cust_type, 
	Rule_Status, 
	Setup_date, 
	Rule_Frequency, 
	Rule_Frequency_ID, 
	Max_Sched_Amount, 
	First_Occ, 
	Last_Occ, 
	User_Grp, 
	Login_ID, 
	Pay_Method, 
	Channel_of_Creation
	FROM SQ_FIS_APR_Update
	WHERE IS_NUMBER(SUBSTR(Account_Num,4,2))
),
EXPTRANS AS (
	SELECT
	Frequency_code,
	End_date,
	Biller_id,
	Catagory,
	Unit_code,
	Service_code,
	Record_type,
	Account_Num,
	Acct_Status,
	Status_timestamp,
	Cust_type,
	Rule_Status,
	Setup_date,
	Rule_Frequency,
	Rule_Frequency_ID,
	Max_Sched_Amount,
	First_Occ,
	Last_Occ,
	User_Grp,
	Login_ID,
	Pay_Method,
	Channel_of_Creation,
	'N' AS out_ProcessStatusCode,
	-- *INF*: IIF(Unit_code='3','PL','CL')
	IFF(Unit_code = '3', 'PL', 'CL') AS out_Unit_code,
	-- *INF*: IIF(Service_code = '9','PLPP','CLPP')
	IFF(Service_code = '9', 'PLPP', 'CLPP') AS out_Service_code,
	-- *INF*: TO_INTEGER((SUBSTR (End_date, 0, 4)))
	CAST((SUBSTR(End_date, 0, 4)) AS INTEGER) AS out_end_ccyy,
	-- *INF*: TO_INTEGER((SUBSTR (End_date, 5, 2)))
	CAST((SUBSTR(End_date, 5, 2)) AS INTEGER) AS out_end_mm,
	-- *INF*: TO_INTEGER((SUBSTR (End_date,7,2)))
	CAST((SUBSTR(End_date, 7, 2)) AS INTEGER) AS out_end_dd,
	-- *INF*: MAKE_DATE_TIME(out_end_ccyy, out_end_mm, out_end_dd)
	-- 
	-- 
	TIMESTAMP_FROM_PARTS(out_end_ccyy,out_end_mm,out_end_dd,00,00,00) AS out_End_Date,
	-- *INF*: TO_INTEGER((SUBSTR (Status_timestamp, 0, 4)))
	CAST((SUBSTR(Status_timestamp, 0, 4)) AS INTEGER) AS out_ccyy,
	-- *INF*: TO_INTEGER((SUBSTR (Status_timestamp,5, 2)))
	CAST((SUBSTR(Status_timestamp, 5, 2)) AS INTEGER) AS out_mm,
	-- *INF*: TO_INTEGER((SUBSTR (Status_timestamp, 7, 2)))
	CAST((SUBSTR(Status_timestamp, 7, 2)) AS INTEGER) AS out_dd,
	-- *INF*: TO_INTEGER((SUBSTR (Status_timestamp, 10, 2)))
	CAST((SUBSTR(Status_timestamp, 10, 2)) AS INTEGER) AS out_hh,
	-- *INF*: TO_INTEGER((SUBSTR(Status_timestamp, 12, 2)))
	CAST((SUBSTR(Status_timestamp, 12, 2)) AS INTEGER) AS out_min,
	-- *INF*: TO_INTEGER((SUBSTR (Status_timestamp, 14, 2)))
	CAST((SUBSTR(Status_timestamp, 14, 2)) AS INTEGER) AS out_sec,
	'0' AS out_nano,
	-- *INF*: MAKE_DATE_TIME(out_ccyy, out_mm, out_dd, out_hh, out_min, out_sec)
	TIMESTAMP_FROM_PARTS(out_ccyy,out_mm,out_dd,out_hh,out_min,out_sec) AS out_Status_Timestamp,
	-- *INF*: TO_INTEGER((SUBSTR (Setup_date, 0, 4)))
	CAST((SUBSTR(Setup_date, 0, 4)) AS INTEGER) AS out_set_ccyy,
	-- *INF*: TO_INTEGER((SUBSTR (Setup_date, 5, 2)))
	CAST((SUBSTR(Setup_date, 5, 2)) AS INTEGER) AS out_set_mm,
	-- *INF*: TO_INTEGER((SUBSTR (Setup_date,7, 2)))
	CAST((SUBSTR(Setup_date, 7, 2)) AS INTEGER) AS out_set_dd,
	-- *INF*: TO_INTEGER((SUBSTR (Setup_date, 10, 2))) 
	CAST((SUBSTR(Setup_date, 10, 2)) AS INTEGER) AS out_set_hh,
	-- *INF*: TO_INTEGER((SUBSTR (Setup_date, 12, 2)))
	CAST((SUBSTR(Setup_date, 12, 2)) AS INTEGER) AS out_set_min,
	-- *INF*: TO_INTEGER((SUBSTR (Setup_date, 14, 2)))
	CAST((SUBSTR(Setup_date, 14, 2)) AS INTEGER) AS out_set_sec,
	'0' AS out_set_nano,
	-- *INF*: MAKE_DATE_TIME(out_set_ccyy, out_set_mm, out_set_dd, out_set_hh, out_set_min, out_set_sec)
	TIMESTAMP_FROM_PARTS(out_set_ccyy,out_set_mm,out_set_dd,out_set_hh,out_set_min,out_set_sec) AS out_Set_Date
	FROM FILTRANS
),
WB_BIL_AutoPaymentRulesStage AS (
	INSERT INTO WB_BIL_AutoPaymentRulesStage
	(FrequencyCode, EndDate, BillerId, Catagory, UnitCode, ServiceCode, RecordType, AccountNum, AcctStatus, StatusTimeStamp, CustType, RuleStatus, SetupDate, RuleFrequency, RuleFrequencyId, MaxSchedAm, FirstOcc, LastOcc, UserGrp, LoginId, PayMethod, ChannelOfCreation, ProcessStatusCode)
	SELECT 
	Frequency_code AS FREQUENCYCODE, 
	out_End_Date AS ENDDATE, 
	Biller_id AS BILLERID, 
	CATAGORY, 
	out_Unit_code AS UNITCODE, 
	out_Service_code AS SERVICECODE, 
	Record_type AS RECORDTYPE, 
	Account_Num AS ACCOUNTNUM, 
	Acct_Status AS ACCTSTATUS, 
	out_Status_Timestamp AS STATUSTIMESTAMP, 
	Cust_type AS CUSTTYPE, 
	Rule_Status AS RULESTATUS, 
	out_Set_Date AS SETUPDATE, 
	Rule_Frequency AS RULEFREQUENCY, 
	Rule_Frequency_ID AS RULEFREQUENCYID, 
	Max_Sched_Amount AS MAXSCHEDAM, 
	First_Occ AS FIRSTOCC, 
	Last_Occ AS LASTOCC, 
	User_Grp AS USERGRP, 
	Login_ID AS LOGINID, 
	Pay_Method AS PAYMETHOD, 
	Channel_of_Creation AS CHANNELOFCREATION, 
	out_ProcessStatusCode AS PROCESSSTATUSCODE
	FROM EXPTRANS
),