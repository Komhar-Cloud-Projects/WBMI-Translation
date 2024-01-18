WITH
SQ_Work1099Reporting AS (
	SELECT  
	Work1099Reporting.Work1099ReportingId,
	Work1099Reporting.IRSName, 
	Work1099Reporting.AddressLine1, 
	Work1099Reporting.AddressLine2, 
	Work1099Reporting.City, 
	Work1099Reporting.StateCode, 
	Work1099Reporting.ZipCode, 
	Work1099Reporting.CountryCode, 
	Work1099Reporting.SearchTaxId, 
	Work1099Reporting.TaxIdType, 
	Work1099Reporting.TaxId, 
	Work1099Reporting.VendorTypeCode, 
	sum(Work1099Reporting.PaidAmount) as PaidAmount
	FROM
	 Work1099Reporting as Work1099Reporting  with (nolock)
	@{pipeline().parameters.WHERE_CLAUSE}
	Group by 
	Work1099Reporting.Work1099ReportingId,
	Work1099Reporting.IRSName, 
	Work1099Reporting.AddressLine1, 
	Work1099Reporting.AddressLine2, 
	Work1099Reporting.City, 
	Work1099Reporting.StateCode, 
	Work1099Reporting.ZipCode, 
	Work1099Reporting.CountryCode, 
	Work1099Reporting.SearchTaxId, 
	Work1099Reporting.TaxIdType, 
	Work1099Reporting.TaxId, 
	Work1099Reporting.VendorTypeCode
	order by IRSName
),
SQ_Work1099Reporting_SSN AS (
	SELECT Work1099Reporting.Work1099ReportingId, Work1099Reporting.TaxId 
	FROM
	 Work1099Reporting
	@{pipeline().parameters.WHERE_CLAUSE}
),
mplt_Detokenize_WebService_call AS (WITH
	INPUT AS (
		
	),
	EXP_SSN_FEIN_TAXID AS (
		SELECT
		IN_id AS id,
		IN_ssn_fein_taxid AS ssn_fein_taxid,
		-- *INF*: LTRIM(RTRIM(ssn_fein_taxid))
		LTRIM(RTRIM(ssn_fein_taxid)) AS V_ssn_fein_taxid,
		-- *INF*: IIF(LENGTH(V_ssn_fein_taxid)=0  or IS_NUMBER(V_ssn_fein_taxid),'FEIN','NONFEIN')
		-- --IIF(LENGTH(V_ssn_fein_taxid)=11 AND REG_MATCH(V_ssn_fein_taxid,'[\da-zA-Z]+'),'NONFEIN','FEIN')
		-- 
		-- 
		IFF(
		    LENGTH(V_ssn_fein_taxid) = 0 or REGEXP_LIKE(V_ssn_fein_taxid, '^[0-9]+$'), 'FEIN', 'NONFEIN'
		) AS V_flag,
		V_flag AS flag,
		V_ssn_fein_taxid AS o_ssn_fein_taxid
		FROM INPUT
	),
	RTR_SSN_FEIN_TAXID AS (
		SELECT
		id,
		flag,
		o_ssn_fein_taxid AS ssn_fein_taxid,
		o_ssn_fein_taxid AS ssn_fein
		FROM EXP_SSN_FEIN_TAXID
	),
	RTR_SSN_FEIN_TAXID_FEIN_VALUES AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag = 'FEIN'),
	RTR_SSN_FEIN_TAXID_NONFEIN_VALUES AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag='NONFEIN'),
	EXP_Values AS (
		SELECT
		id AS ID,
		'Detokenize' AS Function,
		'SSN' AS Scheme,
		ssn_fein_taxid AS SSN_FEIN_TAXID,
		'DATAFEED_JOBS' AS Requestedby,
		'Claims' AS Application,
		'DataFeeds' AS Caller,
		-- *INF*: 'https://intsvc.wbconnect.com/services/TokenService/Token.svc'
		-- 
		-- 
		-- 
		'https://intsvc.wbconnect.com/services/TokenService/Token.svc' AS URL,
		ssn_fein AS SSN_FEIN3
		FROM RTR_SSN_FEIN_TAXID_NONFEIN_VALUES
	),
	Token AS (-- Token
	
		##############################################
	
		# TODO: Place holder for Custom transformation
	
		##############################################
	),
	EXP_Response AS (
		SELECT
		REF_Id AS ID,
		tns_Data0 AS TokenResponse,
		REF_SSN_FEIN_TAXID,
		-- *INF*: ltrim(rtrim(REPLACECHR(0,TokenResponse,'-',NULL)))
		-- 
		ltrim(rtrim(REGEXP_REPLACE(TokenResponse,'-','','i'))) AS o_TokenResponse
		FROM Token
	),
	Union_SSN_FEIN_TAXID AS (
		SELECT id AS ID, ssn_fein_taxid AS TokenReponse, ssn_fein_taxid AS SSN_FEIN
		FROM RTR_SSN_FEIN_TAXID_FEIN_VALUES
		UNION
		SELECT ID, o_TokenResponse AS TokenReponse, REF_SSN_FEIN_TAXID AS SSN_FEIN
		FROM EXP_Response
	),
	OUTPUT AS (
		SELECT
		ID AS OUT_id, 
		TokenReponse AS OUT_TokenResponse, 
		SSN_FEIN AS OUT_IN_ssn_fein_taxid
		FROM Union_SSN_FEIN_TAXID
	),
),
JNR_Work1099Reporting AS (SELECT
	SQ_Work1099Reporting.Work1099ReportingId, 
	SQ_Work1099Reporting.IRSName, 
	SQ_Work1099Reporting.AddressLine1, 
	SQ_Work1099Reporting.AddressLine2, 
	SQ_Work1099Reporting.City, 
	SQ_Work1099Reporting.StateCode, 
	SQ_Work1099Reporting.ZipCode, 
	SQ_Work1099Reporting.CountryCode, 
	SQ_Work1099Reporting.SearchTaxId, 
	SQ_Work1099Reporting.TaxIdType, 
	SQ_Work1099Reporting.TaxId, 
	SQ_Work1099Reporting.VendorTypeCode, 
	SQ_Work1099Reporting.PaidAmount, 
	mplt_Detokenize_WebService_call.OUT_id AS IN_mplt_id, 
	mplt_Detokenize_WebService_call.OUT_TokenResponse AS TokenReponse
	FROM SQ_Work1099Reporting
	INNER JOIN mplt_Detokenize_WebService_call
	ON mplt_Detokenize_WebService_call.OUT_id = SQ_Work1099Reporting.Work1099ReportingId
),
AGGTRANS AS (
	SELECT
	Work1099ReportingId,
	IRSName,
	AddressLine1,
	AddressLine2,
	City,
	StateCode,
	ZipCode,
	CountryCode,
	TokenReponse AS SearchTaxId,
	TaxIdType,
	TokenReponse AS TaxId,
	VendorTypeCode,
	PaidAmount,
	-- *INF*: SUM(PaidAmount)
	SUM(PaidAmount) AS O_PaidAmount
	FROM JNR_Work1099Reporting
	GROUP BY IRSName, AddressLine1, AddressLine2, City, StateCode, ZipCode, CountryCode, SearchTaxId, TaxIdType, TaxId, VendorTypeCode
),
EXP_input AS (
	SELECT
	Work1099ReportingId,
	IRSName,
	AddressLine1,
	AddressLine2,
	City,
	StateCode,
	ZipCode,
	CountryCode,
	SearchTaxId,
	TaxIdType,
	TaxId,
	VendorTypeCode,
	O_PaidAmount AS PaidAmount
	FROM AGGTRANS
),
EXP_cleasne_output AS (
	SELECT
	IRSName,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(IRSName)
	UDF_DEFAULT_VALUE_TO_BLANKS(IRSName) AS IRSName_out,
	AddressLine1,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(AddressLine1)
	UDF_DEFAULT_VALUE_TO_BLANKS(AddressLine1) AS AddressLine1_out,
	AddressLine2,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(AddressLine2)
	UDF_DEFAULT_VALUE_TO_BLANKS(AddressLine2) AS AddressLine2_out,
	City,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(City)
	UDF_DEFAULT_VALUE_TO_BLANKS(City) AS City_out,
	StateCode,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(StateCode)
	UDF_DEFAULT_VALUE_TO_BLANKS(StateCode) AS StateCode_out,
	ZipCode,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(ZipCode)
	UDF_DEFAULT_VALUE_TO_BLANKS(ZipCode) AS ZipCode_out,
	CountryCode,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(CountryCode)
	UDF_DEFAULT_VALUE_TO_BLANKS(CountryCode) AS CountryCode_out,
	SearchTaxId,
	-- *INF*: REPLACECHR(0,SearchTaxId,'-',NULL)
	REGEXP_REPLACE(SearchTaxId,'-','','i') AS v_SearchTaxId,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(v_SearchTaxId)
	UDF_DEFAULT_VALUE_TO_BLANKS(v_SearchTaxId) AS SearchTaxId_out,
	TaxIdType,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(TaxIdType)
	UDF_DEFAULT_VALUE_TO_BLANKS(TaxIdType) AS TaxIdType_out,
	TaxId,
	-- *INF*: IIF(TaxIdType='S' AND LENGTH(TaxId)=9,SUBSTR(TaxId, 1, 3) ||'-'||SUBSTR(TaxId, 4, 2)||'-'||SUBSTR(TaxId, 6, 4),IIF(TaxIdType='F' AND LENGTH(TaxId)=9,SUBSTR(TaxId, 1, 2) ||'-'||SUBSTR(TaxId, 3, 7),TaxId) )
	-- 
	-- 
	IFF(
	    TaxIdType = 'S' AND LENGTH(TaxId) = 9,
	    SUBSTR(TaxId, 1, 3) || '-' || SUBSTR(TaxId, 4, 2) || '-' || SUBSTR(TaxId, 6, 4),
	    IFF(
	        TaxIdType = 'F'
	    and LENGTH(TaxId) = 9,
	        SUBSTR(TaxId, 1, 2) || '-' || SUBSTR(TaxId, 3, 7),
	        TaxId
	    )
	) AS v_TaxId,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(v_TaxId)
	UDF_DEFAULT_VALUE_TO_BLANKS(v_TaxId) AS TaxId_out,
	VendorTypeCode,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(VendorTypeCode)
	UDF_DEFAULT_VALUE_TO_BLANKS(VendorTypeCode) AS VendorTypeCode_out,
	PaidAmount,
	PaidAmount AS PaidAmount_out
	FROM EXP_input
),
RTR_Claims1099ReportingFile AS (
	SELECT
	IRSName_out AS IRSName,
	AddressLine1_out AS AddressLine1,
	AddressLine2_out AS AddressLine2,
	City_out AS City,
	StateCode_out AS StateCode,
	ZipCode_out AS ZipCode,
	CountryCode_out AS CountryCode,
	SearchTaxId_out AS SearchTaxId,
	TaxIdType_out AS TaxIdType,
	TaxId_out AS TaxId,
	VendorTypeCode_out AS VendorTypeCode,
	PaidAmount_out AS PaidAmount
	FROM EXP_cleasne_output
),
RTR_Claims1099ReportingFile_NEWGROUP1 AS (SELECT * FROM RTR_Claims1099ReportingFile WHERE TRUE),
RTR_Claims1099ReportingFile_NEWGROUP2 AS (SELECT * FROM RTR_Claims1099ReportingFile WHERE TRUE),
Claims1099ReportingFile AS (
	INSERT INTO Claims1099ReportingFile
	(IRSName, AddressLine1, AddressLine2, City, StateCode, ZipCode, CountryCode, SearchTaxId, TaxIdType, TaxId, VendorTypeCode, PaidAmount)
	SELECT 
	IRSNAME, 
	AddressLine AS ADDRESSLINE1, 
	ADDRESSLINE2, 
	CITY, 
	STATECODE, 
	ZIPCODE, 
	COUNTRYCODE, 
	SEARCHTAXID, 
	TAXIDTYPE, 
	TAXID, 
	VENDORTYPECODE, 
	PAIDAMOUNT
	FROM RTR_Claims1099ReportingFile_NEWGROUP1
),
EXPTRANS AS (
	SELECT
	IRSName,
	@{pipeline().parameters.SPECIAL_CHARACTERS} AS v_special_characters,
	-- *INF*: ReplaceChr( 0, IRSName, v_special_characters, '')
	REGEXP_REPLACE(IRSName,v_special_characters,'','i') AS O_IRSName,
	SearchTaxId,
	TaxIdType,
	-- *INF*: IIF(TaxIdType='F','1','2')
	IFF(TaxIdType = 'F', '1', '2') AS O_TaxIdType,
	'' AS dummy
	FROM RTR_Claims1099ReportingFile_NEWGROUP2
),
claims1099reportingfile_monthly AS (
	INSERT INTO claims1099reportingfile_monthly
	(TaxIdType, SearchTaxId, IRSName, dummy)
	SELECT 
	O_TaxIdType AS TAXIDTYPE, 
	SEARCHTAXID, 
	O_IRSName AS IRSNAME, 
	DUMMY
	FROM EXPTRANS
),
SQ_Work1099ReportingDetail AS (
	SELECT Work1099ReportingDetail.Work1099ReportingId, Work1099ReportingDetail.PaidAmount, Work1099ReportingDetail.ClaimNumber, Work1099ReportingDetail.CheckNumber, Work1099ReportingDetail.PaymentIssueDate, Work1099ReportingDetail.LossDate, Work1099ReportingDetail.LossDescription 
	FROM
	 Work1099ReportingDetail
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Collect AS (
	SELECT
	Work1099ReportingId,
	PaidAmount,
	ClaimNumber,
	CheckNumber,
	PaymentIssueDate,
	LossDate,
	LossDescription
	FROM SQ_Work1099ReportingDetail
),
JNR_Work1099Reporting_Detail AS (SELECT
	EXP_Collect.Work1099ReportingId, 
	EXP_Collect.PaidAmount, 
	EXP_Collect.ClaimNumber, 
	EXP_Collect.CheckNumber, 
	EXP_Collect.PaymentIssueDate, 
	EXP_Collect.LossDate, 
	EXP_Collect.LossDescription, 
	mplt_Detokenize_WebService_call.OUT_id AS IN_id, 
	mplt_Detokenize_WebService_call.OUT_TokenResponse AS IN_TokenResponse
	FROM EXP_Collect
	INNER JOIN mplt_Detokenize_WebService_call
	ON mplt_Detokenize_WebService_call.OUT_id = EXP_Collect.Work1099ReportingId
),
EXP_PreTarget AS (
	SELECT
	IN_TokenResponse AS SearchTaxId,
	-- *INF*: REPLACECHR(0,SearchTaxId,'-',NULL)
	REGEXP_REPLACE(SearchTaxId,'-','','i') AS v_SearchTaxId,
	-- *INF*: :UDF.DEFAULT_VALUE_TO_BLANKS(v_SearchTaxId)
	UDF_DEFAULT_VALUE_TO_BLANKS(v_SearchTaxId) AS o_SearchTaxId,
	ClaimNumber,
	LossDate,
	CheckNumber,
	PaymentIssueDate AS CheckIssuedDate,
	LossDescription,
	PaidAmount
	FROM JNR_Work1099Reporting_Detail
),
claims1099reportingdetailfile AS (
	INSERT INTO claims1099reportingdetailfile
	(SearchTaxId, ClaimNumber, LossDate, CheckNumber, CheckIssuedDate, PaidAmount)
	SELECT 
	o_SearchTaxId AS SEARCHTAXID, 
	CLAIMNUMBER, 
	LOSSDATE, 
	CHECKNUMBER, 
	CHECKISSUEDDATE, 
	PAIDAMOUNT
	FROM EXP_PreTarget
),