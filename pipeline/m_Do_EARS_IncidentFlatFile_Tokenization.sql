WITH
SQ_EARS_IncidentFile_FF AS (

-- TODO Manual --

),
EXP_Fields AS (
	SELECT
	EARSData,
	-- *INF*: SUBSTR(EARSData, 1, 2)
	SUBSTR(EARSData, 1, 2) AS Field,
	-- *INF*: SUBSTR(EARSData, 3, 2)
	SUBSTR(EARSData, 3, 2) AS State,
	-- *INF*: SUBSTR(EARSData, 5, 22)
	SUBSTR(EARSData, 5, 22) AS LicenseNumber,
	-- *INF*: SUBSTR(EARSData, 27, 45)
	SUBSTR(EARSData, 27, 45) AS InBetween,
	-- *INF*: SUBSTR(EARSData, 72, 22)
	SUBSTR(EARSData, 72, 22) AS Quoteback,
	-- *INF*: SUBSTR(EARSData, 94, 32)
	SUBSTR(EARSData, 94, 32) AS Remaining,
	-- *INF*: SUBSTR(EARSData, 1, 79)
	SUBSTR(EARSData, 1, 79) AS PrevBeginning,
	-- *INF*: SUBSTR(EARSData, 80, 22)
	SUBSTR(EARSData, 80, 22) AS PreviousLicense,
	-- *INF*: SUBSTR(EARSData, 102, 24)
	SUBSTR(EARSData, 102, 24) AS PrevEnd,
	-- *INF*: LTRIM(RTRIM(SUBSTR(EARSData, 5, 22)))
	LTRIM(RTRIM(SUBSTR(EARSData, 5, 22))) AS v_LicenseNumber,
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
	-- 
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
	-- *INF*: LTRIM(RTRIM(SUBSTR(EARSData, 72, 22)))
	LTRIM(RTRIM(SUBSTR(EARSData, 72, 22))) AS v_Quoteback,
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
	-- *INF*: LTRIM(RTRIM(SUBSTR(EARSData, 80, 22)))
	LTRIM(RTRIM(SUBSTR(EARSData, 80, 22))) AS v_PreviousLicense,
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
	-- --LTRIM(RTRIM(SUBSTR(EARSData, 72, 22)))
	IFF(LENGTH(v_Quoteback) < 8, UPPER(LPAD(v_Quoteback, 8, '@')), UPPER(v_Quoteback)) AS lkp_Quoteback,
	-- *INF*: IIF(LENGTH(v_PreviousLicense) < 8, UPPER(LPAD(v_PreviousLicense, 8, '@')), UPPER(v_PreviousLicense))
	IFF(
	    LENGTH(v_PreviousLicense) < 8, UPPER(LPAD(v_PreviousLicense, 8, '@')),
	    UPPER(v_PreviousLicense)
	) AS lkp_PreviousLicense,
	v_SubString_LicenseNumber AS o_SubString_LicenseNumber,
	v_SubString_Quoteback AS o_SubString_Quoteback
	FROM SQ_EARS_IncidentFile_FF
),
LKP_TokenExFile_DriverStage AS (
),
LKP_TokenExFile_DriverStage_PreviousLicense AS (
),
LKP_TokenExFile_DriverStage_Quoteback AS (
),
EXP_TokenFields AS (
	SELECT
	EXP_Fields.Field,
	EXP_Fields.State,
	EXP_Fields.LicenseNumber AS i_LicenseNumber,
	EXP_Fields.InBetween,
	EXP_Fields.Quoteback AS i_Quoteback,
	EXP_Fields.Remaining,
	EXP_Fields.PrevBeginning,
	EXP_Fields.PreviousLicense AS i_PreviousLicense,
	EXP_Fields.PrevEnd,
	LKP_TokenExFile_DriverStage.o_LicenseNumber AS lkp_LicenseNumber,
	LKP_TokenExFile_DriverStage_Quoteback.o_Quoteback AS lkp_Quoteback,
	LKP_TokenExFile_DriverStage_PreviousLicense.o_PreviousLicense AS lkp_PreviousLicense,
	-- *INF*: LTRIM(RTRIM(lkp_PreviousLicense))
	LTRIM(RTRIM(lkp_PreviousLicense)) AS v_lkp_PreviousLicense,
	-- *INF*: LTRIM(RTRIM(lkp_LicenseNumber))
	LTRIM(RTRIM(lkp_LicenseNumber)) AS v_lkp_LicenseNumber,
	-- *INF*: LTRIM(RTRIM(lkp_Quoteback))
	LTRIM(RTRIM(lkp_Quoteback)) AS v_lkp_Quoteback,
	-- *INF*: IIF(ISNULL(lkp_LicenseNumber),RPAD(LTRIM( i_LicenseNumber, '@'),22,' '), RPAD(LTRIM(RTRIM(v_lkp_LicenseNumber)),22, ' '))
	IFF(
	    lkp_LicenseNumber IS NULL, RPAD(LTRIM(i_LicenseNumber, '@'), 22, ' '),
	    RPAD(LTRIM(RTRIM(v_lkp_LicenseNumber)), 22, ' ')
	) AS v_LicenseNumber,
	-- *INF*: IIF(ISNULL(lkp_Quoteback),RPAD(LTRIM( i_Quoteback, '@'),22,' '), RPAD(LTRIM(RTRIM(v_lkp_Quoteback)),22, ' '))
	IFF(
	    lkp_Quoteback IS NULL, RPAD(LTRIM(i_Quoteback, '@'), 22, ' '),
	    RPAD(LTRIM(RTRIM(v_lkp_Quoteback)), 22, ' ')
	) AS v_Quoteback,
	-- *INF*: IIF(ISNULL(lkp_PreviousLicense),RPAD(LTRIM(i_PreviousLicense, '@'),22,' '), RPAD(LTRIM(RTRIM(v_lkp_PreviousLicense)),22, ' '))
	IFF(
	    lkp_PreviousLicense IS NULL, RPAD(LTRIM(i_PreviousLicense, '@'), 22, ' '),
	    RPAD(LTRIM(RTRIM(v_lkp_PreviousLicense)), 22, ' ')
	) AS v_PreviousLicense,
	v_LicenseNumber AS o_LicenseNumber,
	v_Quoteback AS o_Quoteback,
	v_PreviousLicense AS o_PreviousLicense
	FROM EXP_Fields
	LEFT JOIN LKP_TokenExFile_DriverStage
	ON LKP_TokenExFile_DriverStage.lkp_LicenseNumber = EXP_Fields.lkp_LicenseNumber
	LEFT JOIN LKP_TokenExFile_DriverStage_PreviousLicense
	ON LKP_TokenExFile_DriverStage_PreviousLicense.lkp_PreviousLicense = EXP_Fields.lkp_PreviousLicense
	LEFT JOIN LKP_TokenExFile_DriverStage_Quoteback
	ON LKP_TokenExFile_DriverStage_Quoteback.lkp_Quoteback = EXP_Fields.lkp_Quoteback
),
EXP_Token_DL_Rows AS (
	SELECT
	Field AS i_Field,
	State AS i_State,
	i_LicenseNumber,
	InBetween AS i_InBetween,
	i_Quoteback,
	Remaining AS i_Remaining,
	PrevBeginning AS i_PrevBeginning,
	i_PreviousLicense,
	PrevEnd AS i_PrevEnd,
	o_LicenseNumber AS TokenLicenseNumber,
	o_Quoteback AS TokenQuoteback,
	o_PreviousLicense AS TokenPreviousLicense,
	-- *INF*: IIF(i_Field = '01', TokenLicenseNumber, LTRIM(i_LicenseNumber, '@'))
	IFF(i_Field = '01', TokenLicenseNumber, LTRIM(i_LicenseNumber, '@')) AS v_LicenseNumber,
	-- *INF*: IIF(i_Field = '01', TokenQuoteback, LTRIM(i_Quoteback, '@'))
	IFF(i_Field = '01', TokenQuoteback, LTRIM(i_Quoteback, '@')) AS v_Quoteback,
	-- *INF*: IIF(i_Field = '03', TokenPreviousLicense, LTRIM(i_PreviousLicense, '@'))
	IFF(i_Field = '03', TokenPreviousLicense, LTRIM(i_PreviousLicense, '@')) AS v_PreviousLicense,
	-- *INF*: IIF(i_Field = '03', i_PrevBeginning || v_PreviousLicense || i_PrevEnd,
	-- i_Field || i_State || v_LicenseNumber || i_InBetween || v_Quoteback || i_Remaining)
	IFF(
	    i_Field = '03', i_PrevBeginning || v_PreviousLicense || i_PrevEnd,
	    i_Field || i_State || v_LicenseNumber || i_InBetween || v_Quoteback || i_Remaining
	) AS v_EARSData,
	v_EARSData AS o_EARSData
	FROM EXP_TokenFields
),
EARS_IncidentFile_FF AS (
	INSERT INTO EARS_IncidentFile_FF
	(EARSData)
	SELECT 
	o_EARSData AS EARSDATA
	FROM EXP_Token_DL_Rows
),