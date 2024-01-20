WITH
SQ_IrsErrorFile AS (

-- TODO Manual --

),
FIL_KeepErrorRows AS (
	SELECT
	IdType, 
	TaxId, 
	LegalName, 
	ReplyCode
	FROM SQ_IrsErrorFile
	WHERE ReplyCode !=0
),
EXP_SetMappletValues AS (
	SELECT
	IdType,
	-- *INF*: IIF(IdType=1,'EIN','SSN')
	IFF(IdType = 1, 'EIN', 'SSN') AS o_IdType,
	TaxId,
	LegalName,
	ReplyCode,
	'Tokenize' AS TransactionName
	FROM FIL_KeepErrorRows
),
mplt_Token_WebService_Generic_Call AS (WITH
	SEQ_TokenizeWSCounter AS (
		CREATE SEQUENCE SEQ_TokenizeWSCounter
		START = 0
		INCREMENT = 1;
	),
	INPUT_Token_WebService_Call AS (
		
	),
	EXP_Input AS (
		SELECT
		TaxId,
		TransactionName AS TransactionType,
		-- *INF*: DECODE(TRUE,
		-- IN(TransactionType,'Tokenize','Detokenize',1),1,
		-- 0)
		DECODE(
		    TRUE,
		    TransactionType IN ('Tokenize','Detokenize',1), 1,
		    0
		) AS v_ValidateTransactionType,
		-- *INF*: IIF(v_ValidateTransactionType=0,ERROR('Invalid TransactionType value for Token Service'),'No Error')
		IFF(
		    v_ValidateTransactionType = 0, ERROR('Invalid TransactionType value for Token Service'),
		    'No Error'
		) AS ValidationCheck,
		Optional_TaxType AS Optional_TaxTypeOverride,
		-- *INF*: DECODE(TRUE,
		-- NOT(ISNULL(Optional_TaxTypeOverride)) and Optional_TaxTypeOverride!='N/A',Optional_TaxTypeOverride,
		-- SUBSTR(TaxId,3,1)='-','EIN',
		-- SUBSTR(TaxId,4,1)='-' and SUBSTR(TaxId,7,1)='-','SSN',
		--  'N/A'
		-- )
		-- 
		DECODE(
		    TRUE,
		    NOT (Optional_TaxTypeOverride IS NULL) and Optional_TaxTypeOverride != 'N/A', Optional_TaxTypeOverride,
		    SUBSTR(TaxId, 3, 1) = '-', 'EIN',
		    SUBSTR(TaxId, 4, 1) = '-' and SUBSTR(TaxId, 7, 1) = '-', 'SSN',
		    'N/A'
		) AS v_TaxType,
		v_TaxType AS o_TaxType,
		-- *INF*: DECODE(TRUE,
		-- INDEXOF(TaxId,'-') > 0, TaxId,
		-- v_TaxType='SSN',substr(TaxId,1,3)||'-'||substr(TaxId,4,2)||'-'||substr(TaxId,6,4),
		-- v_TaxType='EIN',substr(TaxId,1,2)||'-'||substr(TaxId,3,7),
		-- 'N/A'
		-- )
		DECODE(
		    TRUE,
		    INDEXOF(TaxId, '-') > 0, TaxId,
		    v_TaxType = 'SSN', substr(TaxId, 1, 3) || '-' || substr(TaxId, 4, 2) || '-' || substr(TaxId, 6, 4),
		    v_TaxType = 'EIN', substr(TaxId, 1, 2) || '-' || substr(TaxId, 3, 7),
		    'N/A'
		) AS v_TaxId_Dashes,
		-- *INF*: DECODE(TRUE,
		-- INDEXOF(TaxId,'-') = 0,TaxId,
		-- REPLACESTR(0,TaxId,'-','')
		-- )
		DECODE(
		    TRUE,
		    INDEXOF(TaxId, '-') = 0, TaxId,
		    REGEXP_REPLACE(TaxId,'-','','i')
		) AS v_TaxId_StripDashes,
		-- *INF*: DECODE(TRUE,
		-- TransactionType='Tokenize',v_TaxId_Dashes,
		-- v_TaxId_StripDashes)
		DECODE(
		    TRUE,
		    TransactionType = 'Tokenize', v_TaxId_Dashes,
		    v_TaxId_StripDashes
		) AS o_TaxId
		FROM INPUT_Token_WebService_Call
	),
	FIL_SSN AS (
		SELECT
		TaxId AS ref_TaxId, 
		TransactionType, 
		o_TaxType AS TaxType, 
		o_TaxId AS TaxId
		FROM EXP_Input
		WHERE TaxType='SSN'
	),
	FIL_NotSSN AS (
		SELECT
		TaxId AS ref_TaxId, 
		TransactionType, 
		o_TaxType AS TaxType, 
		o_TaxId AS TaxId
		FROM EXP_Input
		WHERE TaxType!='SSN'
	),
	EXP_ServiceRequest AS (
		SELECT
		ref_TaxId,
		TransactionType,
		TaxType,
		TaxId,
		'Data Feed Jobs' AS RequestedBy,
		'Claims' AS Application,
		'DATA FEED' AS Caller,
		@{pipeline().parameters.URL} AS URL
		FROM FIL_SSN
	),
	Token AS (-- Token
	
		##############################################
	
		# TODO: Place holder for Custom transformation
	
		##############################################
	),
	EXP_ServiceResponse AS (
		SELECT
		REF_InputID,
		tns_Data0,
		XPK_n3_Envelope0,
		faultcode,
		faultstring,
		REF_TransactionType,
		'SSN' AS Type
		FROM Token
	),
	Union AS (
		SELECT ref_TaxId AS input_TaxId, TransactionType, TaxType, TaxId AS Output_TaxId
		FROM FIL_NotSSN
		UNION
		SELECT REF_InputID AS input_TaxId, REF_TransactionType AS TransactionType, Type AS TaxType, tns_Data0 AS Output_TaxId, faultcode, faultstring
		FROM EXP_ServiceResponse
	),
	EXP_output AS (
		SELECT
		input_TaxId,
		TransactionType,
		TaxType,
		Output_TaxId,
		faultcode,
		faultstring,
		-- *INF*: IIF(ISNULL(faultcode),0,1)
		IFF(faultcode IS NULL, 0, 1) AS faultFlag
		FROM Union
	),
	OUTPUT__Token_WebService_Call AS (
		SELECT
		input_TaxId AS SubmittedTaxId, 
		TransactionType, 
		TaxType, 
		Output_TaxId AS ResponseTaxId, 
		faultcode, 
		faultstring, 
		faultFlag
		FROM EXP_output
	),
),
SRTTRANS1 AS (
	SELECT
	SubmittedTaxId, 
	TaxType, 
	TransactionType, 
	ResponseTaxId, 
	faultcode, 
	faultstring, 
	faultFlag
	FROM mplt_Token_WebService_Generic_Call
	ORDER BY SubmittedTaxId ASC, TaxType ASC
),
SRT_SortRows AS (
	SELECT
	TaxId, 
	o_IdType AS IdType, 
	LegalName, 
	ReplyCode
	FROM EXP_SetMappletValues
	ORDER BY TaxId ASC, IdType ASC
),
JNRTRANS AS (SELECT
	SRTTRANS1.SubmittedTaxId, 
	SRTTRANS1.TransactionType, 
	SRTTRANS1.TaxType, 
	SRTTRANS1.ResponseTaxId, 
	SRTTRANS1.faultcode, 
	SRTTRANS1.faultstring, 
	SRTTRANS1.faultFlag, 
	SRT_SortRows.IdType AS o_IdType, 
	SRT_SortRows.TaxId, 
	SRT_SortRows.LegalName, 
	SRT_SortRows.ReplyCode
	FROM SRTTRANS1
	INNER JOIN SRT_SortRows
	ON SRT_SortRows.TaxId = SRTTRANS1.SubmittedTaxId AND SRT_SortRows.IdType = SRTTRANS1.TaxType
),
RTR_ValidAndErrorResponse AS (
	SELECT
	SubmittedTaxId,
	TransactionType,
	TaxType,
	ResponseTaxId,
	faultcode,
	faultstring,
	faultFlag,
	LegalName,
	ReplyCode
	FROM JNRTRANS
),
RTR_ValidAndErrorResponse_ServiceFault AS (SELECT * FROM RTR_ValidAndErrorResponse WHERE faultFlag=1),
RTR_ValidAndErrorResponse_ValidRecords AS (SELECT * FROM RTR_ValidAndErrorResponse WHERE faultFlag=0 and NOT ISNULL(ResponseTaxId)),
EXP_ReportPath AS (
	SELECT
	SubmittedTaxId,
	TransactionType,
	TaxType,
	ResponseTaxId,
	-- *INF*: CHR(39)||ResponseTaxId||CHR(39)
	-- 
	CHR(39) || ResponseTaxId || CHR(39) AS o_ResponseTaxId,
	LegalName,
	ReplyCode,
	@{pipeline().parameters.YEAR} AS YearParameter
	FROM RTR_ValidAndErrorResponse_ValidRecords
),
SQL_FindClaimAndPaymentData AS (-- SQL_FindClaimAndPaymentData

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_FormatReportOutput AS (
	SELECT
	-- *INF*: 'IRSErrorClaimsReport_'||TO_CHAR(SYSDATE,'YYYYMMDDHHMMSS')||'.txt'
	'IRSErrorClaimsReport_' || TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHHMMSS') || '.txt' AS FileName,
	SubmittedTaxId_output,
	TaxType_output,
	-- *INF*: DECODE(TRUE,
	-- TaxType_output='EIN',SubmittedTaxId_output,
	-- reverse(substr(reverse(SubmittedTaxId_output),1,4))
	-- )
	DECODE(
	    TRUE,
	    TaxType_output = 'EIN', SubmittedTaxId_output,
	    reverse(substr(reverse(SubmittedTaxId_output), 1, 4))
	) AS TaxIdentificationNumber,
	LegalName_output AS IRSName,
	ClaimNum,
	PayDate AS PayIssuedDate,
	ReplyCode_output AS IRSRejectCode,
	EntryOperator AS PayEntryOperId,
	PayAmount AS TotalPayAmount
	FROM SQL_FindClaimAndPaymentData
),
IRSErrorClaimsReport AS (
	INSERT INTO IRSErrorClaimsReport
	(FileName, TaxIdentificationNumber, IRSName, ClaimNum, PayIssuedDate, IRSRejectCode, PayEntryOperId, TotalPayAmount)
	SELECT 
	FILENAME, 
	TAXIDENTIFICATIONNUMBER, 
	IRSNAME, 
	CLAIMNUM, 
	PAYISSUEDDATE, 
	IRSREJECTCODE, 
	PAYENTRYOPERID, 
	TOTALPAYAMOUNT
	FROM EXP_FormatReportOutput
),
EXP_logpathInput AS (
	SELECT
	SubmittedTaxId,
	-- *INF*: reverse(substr(reverse(SubmittedTaxId),1,4))
	reverse(substr(reverse(SubmittedTaxId), 1, 4)) AS TaxIdLastFour,
	TransactionType,
	TaxType,
	ResponseTaxId,
	faultcode,
	faultstring,
	faultFlag,
	LegalName,
	ReplyCode,
	SYSDATE AS CurrentDate
	FROM RTR_ValidAndErrorResponse_ServiceFault
),
EXP_ServiceFaultOutput AS (
	SELECT
	'TokenServiceFault.csv' AS FileName,
	TaxIdLastFour,
	TransactionType,
	TaxType,
	faultcode,
	faultstring,
	LegalName,
	ReplyCode,
	CurrentDate
	FROM EXP_logpathInput
),
ServiceFaultLog AS (
	INSERT INTO ServiceFaultLog
	(FileName, TaxIdLastFour, TransactionType, TaxType, faultcode, faultstring, LegalName, ReplyCode, CurrentDate)
	SELECT 
	FILENAME, 
	TAXIDLASTFOUR, 
	TRANSACTIONTYPE, 
	TAXTYPE, 
	FAULTCODE, 
	FAULTSTRING, 
	LEGALNAME, 
	REPLYCODE, 
	CURRENTDATE
	FROM EXP_ServiceFaultOutput
),