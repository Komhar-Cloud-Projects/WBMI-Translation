WITH
SQ_EARS_RejectFile_FF AS (

-- TODO Manual --

),
EXP_Fields AS (
	SELECT
	EARSData,
	-- *INF*: SUBSTR(EARSData, 1, 2)
	SUBSTR(EARSData, 1, 2) AS State,
	-- *INF*: SUBSTR(EARSData, 3, 22)
	SUBSTR(EARSData, 3, 22) AS LicenseNumber,
	-- *INF*: SUBSTR(EARSData, 25, 188)
	SUBSTR(EARSData, 25, 188) AS InBetween,
	-- *INF*: SUBSTR(EARSData, 213, 22)
	SUBSTR(EARSData, 213, 22) AS Quoteback,
	-- *INF*: SUBSTR(EARSData, 235, 71)
	SUBSTR(EARSData, 235, 71) AS Remaining,
	-- *INF*: LTRIM(RTRIM(SUBSTR(EARSData, 3, 22)))
	LTRIM(RTRIM(SUBSTR(EARSData, 3, 22))) AS v_LicenseNumber,
	-- *INF*: LENGTH(v_LicenseNumber)
	LENGTH(v_LicenseNumber) AS v_LicenseNumber_LEN1,
	-- *INF*: LTRIM(v_LicenseNumber,'0')
	LTRIM(v_LicenseNumber, '0') AS v_LicenseNumber_Remove0,
	-- *INF*: LENGTH(v_LicenseNumber_Remove0)
	LENGTH(v_LicenseNumber_Remove0) AS v_LicenseNumber_LEN2,
	-- *INF*: DECODE(TRUE,
	-- v_LicenseNumber_LEN2<8,v_LicenseNumber_LEN1-8,
	-- v_LicenseNumber_LEN1-v_LicenseNumber_LEN2
	-- )
	DECODE(
	    TRUE,
	    v_LicenseNumber_LEN2 < 8, v_LicenseNumber_LEN1 - 8,
	    v_LicenseNumber_LEN1 - v_LicenseNumber_LEN2
	) AS v_LicenseNumber_LEN_Diff,
	-- *INF*: DECODE(TRUE,
	-- v_LicenseNumber_LEN_Diff>0,SUBSTR(v_LicenseNumber,1,v_LicenseNumber_LEN_Diff),
	-- '')
	DECODE(
	    TRUE,
	    v_LicenseNumber_LEN_Diff > 0, SUBSTR(v_LicenseNumber, 1, v_LicenseNumber_LEN_Diff),
	    ''
	) AS v_SubString_LicenseNumber,
	-- *INF*: LTRIM(RTRIM(SUBSTR(EARSData, 213, 22)))
	LTRIM(RTRIM(SUBSTR(EARSData, 213, 22))) AS v_Quoteback,
	-- *INF*: LENGTH(v_Quoteback)
	LENGTH(v_Quoteback) AS v_Quoteback_LEN1,
	-- *INF*: LTRIM(v_Quoteback,'0')
	LTRIM(v_Quoteback, '0') AS v_Quoteback_Remove0,
	-- *INF*: LENGTH(v_Quoteback_Remove0)
	LENGTH(v_Quoteback_Remove0) AS v_Quoteback_LEN2,
	-- *INF*: DECODE(TRUE,
	-- v_Quoteback_LEN2<8,v_Quoteback_LEN1-8,
	-- v_Quoteback_LEN1-v_Quoteback_LEN2
	-- )
	DECODE(
	    TRUE,
	    v_Quoteback_LEN2 < 8, v_Quoteback_LEN1 - 8,
	    v_Quoteback_LEN1 - v_Quoteback_LEN2
	) AS v_Quoteback_LEN_Diff,
	-- *INF*: DECODE(TRUE,
	-- v_Quoteback_LEN_Diff>0,SUBSTR(v_Quoteback,1,v_Quoteback_LEN_Diff),
	-- '')
	DECODE(
	    TRUE,
	    v_Quoteback_LEN_Diff > 0, SUBSTR(v_Quoteback, 1, v_Quoteback_LEN_Diff),
	    ''
	) AS v_SubString_Quoteback,
	-- *INF*: IIF(LENGTH(v_LicenseNumber) < 8, UPPER(LPAD(v_LicenseNumber, 8, '@')), UPPER(v_LicenseNumber))
	-- 
	-- --IIF(LENGTH(LTRIM(RTRIM(v_LicenseNumber_Remove0))) < 8, LPAD(LTRIM(RTRIM(v_LicenseNumber_Remove0)), 8, '@'), LTRIM(RTRIM(v_LicenseNumber_Remove0)))
	IFF(
	    LENGTH(v_LicenseNumber) < 8, UPPER(LPAD(v_LicenseNumber, 8, '@')), UPPER(v_LicenseNumber)
	) AS lkp_LicenseNumber,
	-- *INF*: IIF(LENGTH(v_Quoteback) < 8, UPPER(LPAD(v_Quoteback, 8, '@')), UPPER(v_Quoteback))
	-- 
	-- --IIF(LENGTH(LTRIM(RTRIM(v_Quoteback_Remove0))) < 8, LPAD(LTRIM(RTRIM(v_Quoteback_Remove0)), 8, '@'), LTRIM(RTRIM(v_Quoteback_Remove0)))
	-- 
	-- 
	IFF(LENGTH(v_Quoteback) < 8, UPPER(LPAD(v_Quoteback, 8, '@')), UPPER(v_Quoteback)) AS lkp_Quoteback,
	v_SubString_LicenseNumber AS o_SubString_LicenseNumber,
	v_SubString_Quoteback AS o_SubString_Quoteback
	FROM SQ_EARS_RejectFile_FF
),
LKP_TokenExFile_DriverStage AS (
),
LKP_TokenExFile_DriverStage_Quoteback AS (
),
EXP_TokenFields AS (
	SELECT
	EXP_Fields.State,
	EXP_Fields.LicenseNumber AS i_LicenseNumber,
	EXP_Fields.InBetween,
	EXP_Fields.Quoteback AS i_Quoteback,
	EXP_Fields.Remaining,
	LKP_TokenExFile_DriverStage.o_LicenseNumber AS lkp_LicenseNumber,
	LKP_TokenExFile_DriverStage_Quoteback.o_Quoteback AS lkp_Quoteback,
	-- *INF*: LTRIM(RTRIM(lkp_LicenseNumber))
	LTRIM(RTRIM(lkp_LicenseNumber)) AS v_lkp_LicenseNumber,
	-- *INF*: LTRIM(RTRIM(lkp_Quoteback))
	LTRIM(RTRIM(lkp_Quoteback)) AS v_lkp_Quoteback,
	-- *INF*: IIF(ISNULL(lkp_LicenseNumber),RPAD(LTRIM( i_LicenseNumber, '@'),22,' '), RPAD(LTRIM(RTRIM(v_lkp_LicenseNumber)),22, ' '))
	IFF(
	    lkp_LicenseNumber IS NULL, RPAD(LTRIM(i_LicenseNumber, '@'), 22, ' '),
	    RPAD(LTRIM(RTRIM(v_lkp_LicenseNumber)), 22, ' ')
	) AS v_LicenseNumber,
	-- *INF*: IIF(ISNULL(lkp_Quoteback),RPAD(LTRIM(i_Quoteback, '@'),22,' '), RPAD(LTRIM(RTRIM(v_lkp_Quoteback)),22, ' '))
	IFF(
	    lkp_Quoteback IS NULL, RPAD(LTRIM(i_Quoteback, '@'), 22, ' '),
	    RPAD(LTRIM(RTRIM(v_lkp_Quoteback)), 22, ' ')
	) AS v_Quoteback,
	v_LicenseNumber AS o_LicenseNumber,
	v_Quoteback AS o_Quoteback
	FROM EXP_Fields
	LEFT JOIN LKP_TokenExFile_DriverStage
	ON LKP_TokenExFile_DriverStage.lkp_LicenseNumber = EXP_Fields.lkp_LicenseNumber
	LEFT JOIN LKP_TokenExFile_DriverStage_Quoteback
	ON LKP_TokenExFile_DriverStage_Quoteback.lkp_Quoteback = EXP_Fields.lkp_Quoteback
),
EXP_Token_DL_Rows AS (
	SELECT
	State AS i_State,
	i_LicenseNumber,
	InBetween AS i_InBetween,
	i_Quoteback,
	Remaining AS i_Remaining,
	o_LicenseNumber AS TokenLicenseNumber,
	o_Quoteback AS TokenQuoteback,
	-- *INF*: DECODE(TRUE, i_State = 'FH', LTRIM(i_LicenseNumber, '@'),
	-- i_State = 'FT', LTRIM(i_LicenseNumber, '@'),
	-- TokenLicenseNumber)
	DECODE(
	    TRUE,
	    i_State = 'FH', LTRIM(i_LicenseNumber, '@'),
	    i_State = 'FT', LTRIM(i_LicenseNumber, '@'),
	    TokenLicenseNumber
	) AS v_LicenseNumber,
	-- *INF*: DECODE(TRUE, i_State = 'FH', LTRIM(i_Quoteback, '@'),
	-- i_State = 'FT', LTRIM(i_Quoteback, '@'),
	-- TokenQuoteback)
	DECODE(
	    TRUE,
	    i_State = 'FH', LTRIM(i_Quoteback, '@'),
	    i_State = 'FT', LTRIM(i_Quoteback, '@'),
	    TokenQuoteback
	) AS v_Quoteback,
	i_State || v_LicenseNumber || i_InBetween || v_Quoteback || i_Remaining AS v_EARSData,
	v_EARSData AS o_EARSData
	FROM EXP_TokenFields
),
EARS_RejectFile_FF AS (
	INSERT INTO EARS_RejectFile_FF
	(EARSData)
	SELECT 
	o_EARSData AS EARSDATA
	FROM EXP_Token_DL_Rows
),