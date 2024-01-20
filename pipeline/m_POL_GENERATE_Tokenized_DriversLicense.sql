WITH
SQ_EARS_IncidentFile_FF AS (

-- TODO Manual --

),
EXP_EARS_Incident AS (
	SELECT
	EARSData,
	-- *INF*: LTRIM(RTRIM(SUBSTR(EARSData, 1, 2)))
	LTRIM(RTRIM(SUBSTR(EARSData, 1, 2))) AS Field
	FROM SQ_EARS_IncidentFile_FF
),
FIL_DL_Incident AS (
	SELECT
	EARSData, 
	Field
	FROM EXP_EARS_Incident
	WHERE IN(LTRIM(RTRIM(Field)), '01', '03')
),
EXP_Incident_DL AS (
	SELECT
	EARSData,
	Field,
	-- *INF*: IIF(Field = '01', LTRIM(RTRIM(SUBSTR(EARSData, 5, 22))))
	IFF(Field = '01', LTRIM(RTRIM(SUBSTR(EARSData, 5, 22)))) AS v_LicenseNumber,
	-- *INF*: IIF(Field = '01', LTRIM(RTRIM(SUBSTR(EARSData, 72, 22))))
	IFF(Field = '01', LTRIM(RTRIM(SUBSTR(EARSData, 72, 22)))) AS v_Quoteback,
	-- *INF*: IIF(Field = '03', LTRIM(RTRIM(SUBSTR(EARSData, 80, 22))))
	IFF(Field = '03', LTRIM(RTRIM(SUBSTR(EARSData, 80, 22)))) AS v_PreviousLicense,
	-- *INF*: LTRIM(v_LicenseNumber,'0')
	LTRIM(v_LicenseNumber, '0') AS v_LicenseNumber_Remove0,
	-- *INF*: LTRIM(v_Quoteback,'0')
	LTRIM(v_Quoteback, '0') AS v_Quoteback_Remove0,
	v_LicenseNumber AS LicenseNumber,
	v_Quoteback AS Quoteback,
	v_PreviousLicense AS PreviousLicense
	FROM FIL_DL_Incident
),
SQ_EARS_RejectFile_FF AS (

-- TODO Manual --

),
EXP_EARS_Reject AS (
	SELECT
	EARSData,
	-- *INF*: LTRIM(RTRIM(SUBSTR(EARSData, 1, 2)))
	LTRIM(RTRIM(SUBSTR(EARSData, 1, 2))) AS Field
	FROM SQ_EARS_RejectFile_FF
),
FIL_DL_Reject AS (
	SELECT
	EARSData, 
	Field
	FROM EXP_EARS_Reject
	WHERE NOT IN (LTRIM(RTRIM(Field)), 'FH', 'FT')
),
EXP_Reject_DL AS (
	SELECT
	EARSData,
	-- *INF*: LTRIM(RTRIM(SUBSTR(EARSData, 3, 22)))
	LTRIM(RTRIM(SUBSTR(EARSData, 3, 22))) AS v_LicenseNumber,
	-- *INF*: LTRIM(RTRIM(SUBSTR(EARSData, 213, 22)))
	LTRIM(RTRIM(SUBSTR(EARSData, 213, 22))) AS v_Quoteback,
	-- *INF*: LTRIM(v_LicenseNumber,'0')
	LTRIM(v_LicenseNumber, '0') AS v_LicenseNumber_Remove0,
	-- *INF*: LTRIM(v_Quoteback,'0')
	LTRIM(v_Quoteback, '0') AS v_Quoteback_Remove0,
	v_LicenseNumber AS LicenseNumber,
	v_Quoteback AS Quoteback
	FROM FIL_DL_Reject
),
Union AS (
	SELECT LicenseNumber
	FROM EXP_Incident_DL
	UNION
	SELECT Quoteback AS LicenseNumber
	FROM EXP_Incident_DL
	UNION
	SELECT LicenseNumber
	FROM EXP_Reject_DL
	UNION
	SELECT Quoteback AS LicenseNumber
	FROM EXP_Reject_DL
	UNION
	SELECT PreviousLicense AS LicenseNumber
	FROM EXP_Incident_DL
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
	'Tokenize' AS Function,
	'DriversLicense' AS Scheme,
	'EARS' AS Requestedby,
	'EARS' AS Application,
	'EARS' AS Caller,
	'EARS_DL_Tokenized.csv' AS File_Name,
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