WITH
SQ_ExploreEARSDriverRecord AS (
	SELECT
		LicenseNumber
	FROM ExploreEARSDriverRecord
),
SQ_ExploreEARSDriverRecord_QB AS (
	SELECT
		QuotebackDriverLicense
	FROM ExploreEARSDriverRecord_QB
),
Union AS (
	SELECT LicenseNumber
	FROM SQ_ExploreEARSDriverRecord
	UNION
	SELECT QuotebackDriverLicense AS LicenseNumber
	FROM SQ_ExploreEARSDriverRecord_QB
),
SRTTRANS AS (
	SELECT
	LicenseNumber
	FROM Union
	ORDER BY LicenseNumber ASC
),
EXP_Values AS (
	SELECT
	LicenseNumber AS i_LicenseNumber,
	-- *INF*: UPPER(LTRIM(RTRIM(i_LicenseNumber)))
	UPPER(LTRIM(RTRIM(i_LicenseNumber))) AS o_LicenseNumber,
	@{pipeline().parameters.FUNCTION} AS Function,
	'DriversLicense' AS Scheme,
	'EARS' AS Requestedby,
	'EARS' AS Application,
	'EARS' AS Caller,
	-- *INF*: --'DataMart_DL_Detokenized.csv'
	-- @{pipeline().parameters.FILE_NAME}
	-- --CHR(39)||@{pipeline().parameters.FILE_NAME}||'.csv'||CHR(39)
	@{pipeline().parameters.FILE_NAME} AS File_Name,
	-- *INF*: IIF(v_SeqNumber = 0, @{pipeline().parameters.BATCHSIZE} ,v_SeqNumber + 1)
	IFF(v_SeqNumber = 0, @{pipeline().parameters.BATCHSIZE}, v_SeqNumber + 1) AS v_SeqNumber,
	-- *INF*: TRUNC(v_SeqNumber / @{pipeline().parameters.BATCHSIZE},0)
	TRUNC(v_SeqNumber / @{pipeline().parameters.BATCHSIZE},0) AS v_BatchNumber,
	v_SeqNumber AS o_SeqNumber,
	v_BatchNumber AS o_BatchNumber
	FROM SRTTRANS
),
AGG_Values AS (
	SELECT
	o_BatchNumber,
	o_SeqNumber,
	o_LicenseNumber,
	Function,
	Scheme,
	Requestedby,
	Application,
	Caller,
	File_Name
	FROM EXP_Values
	QUALIFY ROW_NUMBER() OVER (PARTITION BY o_BatchNumber ORDER BY NULL) = 1
),
Tokenize_WebServiceCall AS (-- Tokenize_WebServiceCall

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
FILTRANS AS (
	SELECT
	tns_ResponseData0 AS TokenResponse
	FROM Tokenize_WebServiceCall
	WHERE FALSE
),
EARS_DummyFile_FF AS (
	INSERT INTO EARS_DummyFile_FF
	(EARS_DummyResponse)
	SELECT 
	TokenResponse AS EARS_DUMMYRESPONSE
	FROM FILTRANS
),