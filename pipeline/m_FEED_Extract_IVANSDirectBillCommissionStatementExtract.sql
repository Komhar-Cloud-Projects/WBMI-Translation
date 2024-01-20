WITH
SQ_IVANSDirectBillCommissionStatementExtract AS (
	SELECT
		IVANSDirectBillCommissionStatementExtractId,
		AuditId,
		CreatedDate,
		RunDate,
		InsuredName,
		AgencyState,
		AgencyPayCode,
		AgencyNumber,
		PolicySymbol,
		PolicyNumber,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		Premium,
		CommissionAmount,
		CommissionRate,
		TransactionCode,
		AccountDate,
		CustomerNumber,
		TransactionDate,
		LineOfBusinessCode
	FROM IVANSDirectBillCommissionStatementExtract
	WHERE IVANSDirectBillCommissionStatementExtract.RunDate=(DATEADD(MM,DATEDIFF(MM,0,@{pipeline().parameters.RUNDATE}),-1))
),
AGG_IVANSDirectBillCommissionStatement AS (
	SELECT
	Premium AS i_Premium,
	CommissionAmount AS i_CommissionAmount,
	InsuredName,
	AgencyState,
	AgencyPayCode,
	AgencyNumber,
	PolicySymbol,
	PolicyNumber,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	CommissionRate,
	TransactionCode,
	AccountDate,
	CustomerNumber,
	TransactionDate,
	LineOfBusinessCode,
	-- *INF*: SUM(i_Premium)
	SUM(i_Premium) AS o_Premium,
	-- *INF*: SUM(i_CommissionAmount)
	SUM(i_CommissionAmount) AS o_CommissionAmount
	FROM SQ_IVANSDirectBillCommissionStatementExtract
	GROUP BY AgencyState, AgencyPayCode, AgencyNumber, PolicySymbol, PolicyNumber, PolicyEffectiveDate, PolicyExpirationDate, CommissionRate, TransactionCode, TransactionDate
),
EXP_TRANS AS (
	SELECT
	InsuredName AS i_InsuredName,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	PolicyExpirationDate AS i_PolicyExpirationDate,
	AccountDate AS i_AccountDate,
	TransactionDate AS i_TransactionDate,
	AgencyState,
	AgencyPayCode,
	AgencyNumber,
	PolicySymbol,
	PolicyNumber,
	o_Premium AS Premium,
	o_CommissionAmount AS CommissionAmount,
	CommissionRate,
	TransactionCode,
	CustomerNumber,
	LineOfBusinessCode,
	-- *INF*: IIF (LineOfBusinessCode = 'PPKGE',
	--        '1', '2')
	-- 
	--  --@{pipeline().parameters.RECORD_ID}))
	IFF(LineOfBusinessCode = 'PPKGE', '1', '2') AS o_RecordID,
	-- *INF*: SUBSTR(i_InsuredName,1,30)
	SUBSTR(i_InsuredName, 1, 30) AS o_InsuredName,
	-- *INF*: TO_CHAR(GET_DATE_PART(i_PolicyEffectiveDate,'YYYY'))
	TO_CHAR(DATE_PART(i_PolicyEffectiveDate, 'YYYY')) AS o_PolicyEffectiveDateYear,
	-- *INF*: LPAD(TO_CHAR(GET_DATE_PART(i_PolicyEffectiveDate,'MM')),2,'0')
	LPAD(TO_CHAR(DATE_PART(i_PolicyEffectiveDate, 'MM')), 2, '0') AS o_PolicyEffectiveDateMonth,
	-- *INF*: LPAD(TO_CHAR(GET_DATE_PART(i_PolicyEffectiveDate,'DD')),2,'0')
	LPAD(TO_CHAR(DATE_PART(i_PolicyEffectiveDate, 'DD')), 2, '0') AS o_PolicyEffectiveDateDay,
	-- *INF*: TO_CHAR(GET_DATE_PART(i_PolicyExpirationDate,'YYYY'))
	TO_CHAR(DATE_PART(i_PolicyExpirationDate, 'YYYY')) AS o_PolicyExpirationDateYear,
	-- *INF*: LPAD(TO_CHAR(GET_DATE_PART(i_PolicyExpirationDate,'MM')),2,'0')
	LPAD(TO_CHAR(DATE_PART(i_PolicyExpirationDate, 'MM')), 2, '0') AS o_PolicyExpirationDateMonth,
	-- *INF*: LPAD(TO_CHAR(GET_DATE_PART(i_PolicyExpirationDate,'DD')),2,'0')
	LPAD(TO_CHAR(DATE_PART(i_PolicyExpirationDate, 'DD')), 2, '0') AS o_PolicyExpirationDateDay,
	-- *INF*: TO_CHAR(i_AccountDate,'YYYYMM')
	TO_CHAR(i_AccountDate, 'YYYYMM') AS o_AccountDate,
	-- *INF*: TO_CHAR(i_TransactionDate,'YYYYMMDD')
	TO_CHAR(i_TransactionDate, 'YYYYMMDD') AS o_TransactionDate
	FROM AGG_IVANSDirectBillCommissionStatement
),
dbcs_AADBCS_RECORD AS (
	INSERT INTO dbcs_AADBCS_RECORD
	(AADBCS_RECORD_ID, AADBCS_INSUREDS_NAME, AADBCS_AGENCY_STATE, AADBCS_AGENCY_PAYCODE, AADBCS_AGENCY_NUMBER, AADBCS_POLICY_SYMBOL, AADBCS_POLICY_NUMBER, AADBCS_EXPIRATION_YYYY, AADBCS_EXPIRATION_MM, AADBCS_PREMIUM, AADBCS_COMMISSION, AADBCS_LOCAL_COMM_RATE, AADBCS_TRANSACTION_CODE, AADBCS_ACCOUNT_CCYYMM, AADBCS_CUSTOMER_NUMBER, AADBCS_POL_EFFECTIVE_YYYY, AADBCS_POL_EFFECTIVE_MM, AADBCS_POL_EFFECTIVE_DD, AADBCS_EXPIRATION_DAY, AADBCS_TRANSACTION_DATE, AADBCS_LOB_CD)
	SELECT 
	o_RecordID AS AADBCS_RECORD_ID, 
	o_InsuredName AS AADBCS_INSUREDS_NAME, 
	AgencyState AS AADBCS_AGENCY_STATE, 
	AgencyPayCode AS AADBCS_AGENCY_PAYCODE, 
	AgencyNumber AS AADBCS_AGENCY_NUMBER, 
	PolicySymbol AS AADBCS_POLICY_SYMBOL, 
	PolicyNumber AS AADBCS_POLICY_NUMBER, 
	o_PolicyExpirationDateYear AS AADBCS_EXPIRATION_YYYY, 
	o_PolicyExpirationDateMonth AS AADBCS_EXPIRATION_MM, 
	Premium AS AADBCS_PREMIUM, 
	CommissionAmount AS AADBCS_COMMISSION, 
	CommissionRate AS AADBCS_LOCAL_COMM_RATE, 
	TransactionCode AS AADBCS_TRANSACTION_CODE, 
	o_AccountDate AS AADBCS_ACCOUNT_CCYYMM, 
	CustomerNumber AS AADBCS_CUSTOMER_NUMBER, 
	o_PolicyEffectiveDateYear AS AADBCS_POL_EFFECTIVE_YYYY, 
	o_PolicyEffectiveDateMonth AS AADBCS_POL_EFFECTIVE_MM, 
	o_PolicyEffectiveDateDay AS AADBCS_POL_EFFECTIVE_DD, 
	o_PolicyExpirationDateDay AS AADBCS_EXPIRATION_DAY, 
	o_TransactionDate AS AADBCS_TRANSACTION_DATE, 
	LineOfBusinessCode AS AADBCS_LOB_CD
	FROM EXP_TRANS
),