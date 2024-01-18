WITH
SQ_ExploreEARSDriverRecord_Detail AS (
	SELECT
		ExploreEARSDriverRecordId,
		AuditId,
		CreatedDate,
		LicenseState,
		LicenseNumber,
		LastName,
		FirstName,
		MiddleName,
		Birthdate,
		ZipCode,
		StreetAddress,
		Gender,
		PolicyNumber,
		PolicyExpirationDate,
		QuotebackPolicyNumber,
		QuotebackAgencyNumber,
		QuotebackDriverLicense,
		QuotebackState,
		InsuranceIndicator,
		ProductFlags,
		AccountNumber
	FROM ExploreEARSDriverRecord
	WHERE @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Input AS (
	SELECT
	LicenseNumber AS i_LicenseNumber,
	-- *INF*: LTRIM(RTRIM(i_LicenseNumber))
	LTRIM(RTRIM(i_LicenseNumber)) AS v_LicenseNumber,
	-- *INF*: LTRIM(v_LicenseNumber, '0')
	LTRIM(v_LicenseNumber, '0') AS v_LicenseNumber_Remove0,
	-- *INF*: LENGTH(v_LicenseNumber)
	LENGTH(v_LicenseNumber) AS v_LicenseNumber_LEN1,
	-- *INF*: LENGTH(v_LicenseNumber_Remove0)
	LENGTH(v_LicenseNumber_Remove0) AS v_LicenseNumber_LEN2,
	v_LicenseNumber_LEN1 - v_LicenseNumber_LEN2 AS v_LicenseNumber_LEN_Diff,
	-- *INF*: DECODE(TRUE,
	-- v_LicenseNumber_LEN_Diff > 0, SUBSTR(v_LicenseNumber, 1, v_LicenseNumber_LEN_Diff), '')
	DECODE(
	    TRUE,
	    v_LicenseNumber_LEN_Diff > 0, SUBSTR(v_LicenseNumber, 1, v_LicenseNumber_LEN_Diff),
	    ''
	) AS v_SubString_LicenseNumber,
	-- *INF*: IIF(LENGTH(v_LicenseNumber) < 8, UPPER(LPAD(v_LicenseNumber, 8, '@')), UPPER(v_LicenseNumber))
	-- 
	-- --IIF(IS_NUMBER(v_LicenseNumber) = 1, v_LicenseNumber_Remove0, v_LicenseNumber)
	-- 
	-- --IIF(LENGTH(LTRIM(RTRIM(v_LicenseNumber_Remove0))) < 8, LPAD(LTRIM(RTRIM(v_LicenseNumber_Remove0)), 8, '@'), LTRIM(RTRIM(v_LicenseNumber_Remove0)))
	IFF(
	    LENGTH(v_LicenseNumber) < 8, UPPER(LPAD(v_LicenseNumber, 8, '@')), UPPER(v_LicenseNumber)
	) AS lkp_LicenseNumber
	FROM SQ_ExploreEARSDriverRecord_Detail
),
LKP_TokenExFile AS (
),
EXP_Input_QB AS (
	SELECT
	QuotebackDriverLicense AS i_Quoteback,
	-- *INF*: LTRIM(RTRIM(i_Quoteback))
	LTRIM(RTRIM(i_Quoteback)) AS v_Quoteback,
	-- *INF*: LTRIM(v_Quoteback, '0')
	LTRIM(v_Quoteback, '0') AS v_Quoteback_Remove0,
	-- *INF*: LENGTH(v_Quoteback)
	LENGTH(v_Quoteback) AS v_Quoteback_LEN1,
	-- *INF*: LENGTH(v_Quoteback_Remove0)
	LENGTH(v_Quoteback_Remove0) AS v_Quoteback_LEN2,
	v_Quoteback_LEN1 - v_Quoteback_LEN2 AS v_Quoteback_LEN_Diff,
	-- *INF*: DECODE(TRUE,
	-- v_Quoteback_LEN_Diff > 0, SUBSTR(v_Quoteback, 1, v_Quoteback_LEN_Diff), '')
	DECODE(
	    TRUE,
	    v_Quoteback_LEN_Diff > 0, SUBSTR(v_Quoteback, 1, v_Quoteback_LEN_Diff),
	    ''
	) AS v_SubString_Quoteback,
	-- *INF*: IIF(LENGTH(v_Quoteback) < 8, UPPER(LPAD(v_Quoteback, 8, '@')), UPPER(v_Quoteback))
	-- 
	-- --IIF(IS_NUMBER(v_Quoteback) = 1, v_Quoteback_Remove0, v_Quoteback)
	-- 
	-- --IIF(LENGTH(LTRIM(RTRIM(v_Quoteback_Remove0))) < 8, LPAD(LTRIM(RTRIM(v_Quoteback_Remove0)), 8, '@'), LTRIM(RTRIM(v_Quoteback_Remove0)))
	IFF(LENGTH(v_Quoteback) < 8, UPPER(LPAD(v_Quoteback, 8, '@')), UPPER(v_Quoteback)) AS lkp_Quoteback
	FROM SQ_ExploreEARSDriverRecord_Detail
),
LKP_TokenExFile_QB AS (
),
EXP_CalculateDetail AS (
	SELECT
	SQ_ExploreEARSDriverRecord_Detail.LicenseState AS i_LicenseState,
	SQ_ExploreEARSDriverRecord_Detail.LicenseNumber AS i_LicenseNumber,
	LKP_TokenExFile.o_LicenseNumber AS lkp_LicenseNumber,
	SQ_ExploreEARSDriverRecord_Detail.LastName AS i_LastName,
	SQ_ExploreEARSDriverRecord_Detail.FirstName AS i_FirstName,
	SQ_ExploreEARSDriverRecord_Detail.MiddleName AS i_MiddleName,
	SQ_ExploreEARSDriverRecord_Detail.Birthdate AS i_Birthdate,
	SQ_ExploreEARSDriverRecord_Detail.ZipCode AS i_ZipCode,
	SQ_ExploreEARSDriverRecord_Detail.StreetAddress AS i_StreetAddress,
	SQ_ExploreEARSDriverRecord_Detail.Gender AS i_Gender,
	SQ_ExploreEARSDriverRecord_Detail.PolicyNumber AS i_PolicyNumber,
	SQ_ExploreEARSDriverRecord_Detail.PolicyExpirationDate AS i_PolicyExpirationDate,
	SQ_ExploreEARSDriverRecord_Detail.QuotebackPolicyNumber AS i_QuotebackPolicyNumber,
	SQ_ExploreEARSDriverRecord_Detail.QuotebackAgencyNumber AS i_QuotebackAgencyNumber,
	SQ_ExploreEARSDriverRecord_Detail.QuotebackDriverLicense AS i_QuotebackDriverLicense,
	LKP_TokenExFile_QB.o_Quoteback AS lkp_QuotebackDriverLicense,
	SQ_ExploreEARSDriverRecord_Detail.QuotebackState AS i_QuotebackState,
	SQ_ExploreEARSDriverRecord_Detail.InsuranceIndicator AS i_InsuranceIndicator,
	SQ_ExploreEARSDriverRecord_Detail.ProductFlags AS i_ProductFlags,
	SQ_ExploreEARSDriverRecord_Detail.AccountNumber AS i_AccountNumber,
	-- *INF*: TO_CHAR(TRUNC(SYSDATE, 'DD'),'YYYYMMDD')
	TO_CHAR(CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)), 'YYYYMMDD') AS v_FileDate,
	-- *INF*: IIF(i_LicenseState='N/A', '  ', i_LicenseState)
	IFF(i_LicenseState = 'N/A', '  ', i_LicenseState) AS v_LicenseState,
	-- *INF*: LTRIM(LTRIM(RTRIM(lkp_LicenseNumber)), '@')
	LTRIM(LTRIM(RTRIM(lkp_LicenseNumber)), '@') AS v_lkp_LicenseNumber,
	-- *INF*: IIF(ISNULL(lkp_LicenseNumber), RPAD( i_LicenseNumber, 22, ' '), RPAD(LTRIM(RTRIM(v_lkp_LicenseNumber)), 22,  ' '))
	IFF(
	    lkp_LicenseNumber IS NULL, RPAD(i_LicenseNumber, 22, ' '),
	    RPAD(LTRIM(RTRIM(v_lkp_LicenseNumber)), 22, ' ')
	) AS v_LicenseNumber,
	-- *INF*: RPAD(IIF(i_LastName='N/A', '', i_LastName),25, ' ')
	RPAD(
	    IFF(
	        i_LastName = 'N/A', '', i_LastName
	    ), 25, ' ') AS v_LastName,
	-- *INF*: RPAD(IIF(i_FirstName='N/A', '', i_FirstName),20, ' ')
	RPAD(
	    IFF(
	        i_FirstName = 'N/A', '', i_FirstName
	    ), 20, ' ') AS v_FirstName,
	-- *INF*: RPAD(IIF(i_MiddleName='N/A', '', i_MiddleName),20, ' ')
	RPAD(
	    IFF(
	        i_MiddleName = 'N/A', '', i_MiddleName
	    ), 20, ' ') AS v_MiddleName,
	-- *INF*: RPAD(i_Birthdate,8, ' ')
	RPAD(i_Birthdate, 8, ' ') AS v_Birthdate,
	-- *INF*: RPAD( i_ZipCode,9, ' ')
	RPAD(i_ZipCode, 9, ' ') AS v_ZipCode,
	-- *INF*: RPAD(IIF(i_StreetAddress='N/A', '', SUBSTR(i_StreetAddress, 1, 25)),25, ' ')
	RPAD(
	    IFF(
	        i_StreetAddress = 'N/A', '', SUBSTR(i_StreetAddress, 1, 25)
	    ), 25, ' ') AS v_StreetAddress,
	-- *INF*: RPAD(i_Gender,1, ' ')
	RPAD(i_Gender, 1, ' ') AS v_Gender,
	' ' AS v_Reserved_133,
	-- *INF*: RPAD(i_PolicyNumber,32, ' ')
	RPAD(i_PolicyNumber, 32, ' ') AS v_PolicyNumber,
	-- *INF*: RPAD(IIF(i_PolicyExpirationDate='21001231', '', i_PolicyExpirationDate),8, ' ')
	RPAD(
	    IFF(
	        i_PolicyExpirationDate = '21001231', '', i_PolicyExpirationDate
	    ), 8, ' ') AS v_PolicyExpirationDate,
	'  ' AS v_Reserved_174_175,
	-- *INF*: RPAD(i_PolicyNumber,32, ' ')
	RPAD(i_PolicyNumber, 32, ' ') AS v_QuotebackPolicyNumber,
	-- *INF*: RPAD(i_QuotebackAgencyNumber,5, ' ')
	RPAD(i_QuotebackAgencyNumber, 5, ' ') AS v_QuotebackAgencyNumber,
	-- *INF*: LTRIM(LTRIM(RTRIM(lkp_QuotebackDriverLicense)), '@')
	LTRIM(LTRIM(RTRIM(lkp_QuotebackDriverLicense)), '@') AS v_lkp_QuotebackDriverLicense,
	-- *INF*: IIF(ISNULL(lkp_QuotebackDriverLicense), RPAD( i_QuotebackDriverLicense, 22, ' '), RPAD(LTRIM(RTRIM(v_lkp_QuotebackDriverLicense)), 22,  ' '))
	IFF(
	    lkp_QuotebackDriverLicense IS NULL, RPAD(i_QuotebackDriverLicense, 22, ' '),
	    RPAD(LTRIM(RTRIM(v_lkp_QuotebackDriverLicense)), 22, ' ')
	) AS v_QuotebackDriverLicense,
	-- *INF*: RPAD(IIF(i_QuotebackState='N/A', '', i_QuotebackState),3, ' ')
	RPAD(
	    IFF(
	        i_QuotebackState = 'N/A', '', i_QuotebackState
	    ), 3, ' ') AS v_QuotebackState,
	-- *INF*: RPAD('', 18, ' ')
	RPAD('', 18, ' ') AS v_Reserved_238_255,
	-- *INF*: RPAD(i_InsuranceIndicator,1, ' ')
	RPAD(i_InsuranceIndicator, 1, ' ') AS v_InsuranceIndicator,
	-- *INF*: RPAD('', 8, ' ')
	RPAD('', 8, ' ') AS v_Reserved_257_264,
	i_ProductFlags AS v_ProductFlags,
	-- *INF*: RPAD(i_AccountNumber,9, ' ')
	RPAD(i_AccountNumber, 9, ' ') AS v_AccountNumber,
	-- *INF*: RPAD('', 23, ' ')
	RPAD('', 23, ' ') AS v_Reserved_283_305,
	v_LicenseState || 
v_LicenseNumber || 
v_LastName || 
v_FirstName || 
v_MiddleName || 
v_Birthdate || 
v_ZipCode || 
v_StreetAddress || 
v_Gender ||
v_Reserved_133 || 
v_PolicyNumber || 
v_PolicyExpirationDate || 
v_Reserved_174_175|| 
v_QuotebackPolicyNumber || 
v_QuotebackAgencyNumber || 
v_QuotebackDriverLicense || 
v_QuotebackState || 
v_Reserved_238_255 || 
v_InsuranceIndicator || 
v_Reserved_257_264 || 
v_ProductFlags || 
v_AccountNumber || 
v_Reserved_283_305 AS o_Detail,
	-- *INF*: i_AccountNumber || @{pipeline().parameters.FILE_EXTENSION} || IIF(@{pipeline().parameters.PMINTEGRATIONSERVICENAME}='IntegrationServiceProd2', '', '.TEST')
	-- 
	-- --i_AccountNumber||IIF(@{pipeline().parameters.PMINTEGRATIONSERVICENAME}='IntegrationServiceProd2', '.EAR', '.EAR_TEST')||'_'||SUBSTR(v_FileDate,1,6)||--@{pipeline().parameters.FILE_EXTENSION}
	i_AccountNumber || @{pipeline().parameters.FILE_EXTENSION} || IFF(@{pipeline().parameters.PMINTEGRATIONSERVICENAME} = 'IntegrationServiceProd2', '', '.TEST') AS o_FileName,
	v_LicenseState || v_LicenseNumber AS o_SortOrder
	FROM SQ_ExploreEARSDriverRecord_Detail
	LEFT JOIN LKP_TokenExFile
	ON LKP_TokenExFile.lkp_LicenseNumber = EXP_Input.lkp_LicenseNumber
	LEFT JOIN LKP_TokenExFile_QB
	ON LKP_TokenExFile_QB.lkp_Quoteback = EXP_Input_QB.lkp_Quoteback
),
SQ_ExploreEARSDriverRecord_Header AS (
	SELECT DISTINCT AccountNumber 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.ExploreEARSDriverRecord 
	WHERE @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_CalculateHeader AS (
	SELECT
	AccountNumber AS i_AccountNumber,
	'FH' AS v_RecordID,
	-- *INF*: TO_CHAR(TRUNC(SYSDATE, 'DD'),'YYYYMMDD')
	TO_CHAR(CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)), 'YYYYMMDD') AS v_FileDate,
	'DR' AS v_FileIDCode,
	-- *INF*: RPAD('West Bend Insurance',30, ' ')
	RPAD('West Bend Insurance', 30, ' ') AS v_SenderName,
	-- *INF*: RPAD(i_AccountNumber,9, ' ')
	RPAD(i_AccountNumber, 9, ' ') AS v_SenderNumber,
	'0800' AS v_Version,
	-- *INF*: RPAD('',250, ' ')
	RPAD('', 250, ' ') AS v_Reserved,
	v_RecordID||v_FileDate||v_FileIDCode||v_SenderName||v_SenderNumber||v_Version||v_Reserved AS o_Header,
	-- *INF*: i_AccountNumber || @{pipeline().parameters.FILE_EXTENSION} || IIF(@{pipeline().parameters.PMINTEGRATIONSERVICENAME}='IntegrationServiceProd2', '', '.TEST')
	-- 
	-- 
	-- --i_AccountNumber||IIF(@{pipeline().parameters.PMINTEGRATIONSERVICENAME}='IntegrationServiceProd2', '.EAR', '.EAR_TEST')||'_'||SUBSTR(v_FileDate,1,6)||--@{pipeline().parameters.FILE_EXTENSION}
	i_AccountNumber || @{pipeline().parameters.FILE_EXTENSION} || IFF(@{pipeline().parameters.PMINTEGRATIONSERVICENAME} = 'IntegrationServiceProd2', '', '.TEST') AS o_FileName,
	-- *INF*: CHR(0)
	CHR(0) AS o_SortOrder
	FROM SQ_ExploreEARSDriverRecord_Header
),
SQ_ExploreEARSDriverRecord_Trailer AS (
	SELECT
		ExploreEARSDriverRecordId,
		AuditId,
		CreatedDate,
		LicenseState,
		LicenseNumber,
		LastName,
		FirstName,
		MiddleName,
		Birthdate,
		ZipCode,
		StreetAddress,
		Gender,
		PolicyNumber,
		PolicyExpirationDate,
		QuotebackPolicyNumber,
		QuotebackAgencyNumber,
		QuotebackDriverLicense,
		QuotebackState,
		InsuranceIndicator,
		ProductFlags,
		AccountNumber
	FROM ExploreEARSDriverRecord
	WHERE @{pipeline().parameters.WHERE_CLAUSE}
),
AGG_Count AS (
	SELECT
	AccountNumber,
	-- *INF*: COUNT(AccountNumber)
	COUNT(AccountNumber) AS o_Count
	FROM SQ_ExploreEARSDriverRecord_Trailer
	GROUP BY AccountNumber
),
EXP_CalculateTrailer AS (
	SELECT
	AccountNumber AS i_AccountNumber,
	o_Count AS i_Count,
	-- *INF*: TO_CHAR(TRUNC(SYSDATE, 'DD'),'YYYYMMDD')
	TO_CHAR(CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)), 'YYYYMMDD') AS v_FileDate,
	'FT' AS v_RecordID,
	-- *INF*: RPAD('',6, ' ')
	RPAD('', 6, ' ') AS v_Reserved,
	-- *INF*: LPAD(TO_CHAR(i_Count+2),10, '0')
	LPAD(TO_CHAR(i_Count + 2), 10, '0') AS v_TotalRecords,
	-- *INF*: RPAD('EXPLORE',30,' ')
	RPAD('EXPLORE', 30, ' ') AS v_ReceivingName,
	-- *INF*: RPAD('',9, ' ')
	RPAD('', 9, ' ') AS v_ReceivingNumber,
	'DR' AS v_FileIDCode,
	-- *INF*: RPAD('',246, ' ')
	RPAD('', 246, ' ') AS v_Filler,
	v_RecordID || 
v_Reserved || 
v_TotalRecords || 
v_ReceivingName ||
v_ReceivingNumber || 
v_FileIDCode || 
v_Filler AS o_Trailer,
	-- *INF*: i_AccountNumber || @{pipeline().parameters.FILE_EXTENSION} || IIF(@{pipeline().parameters.PMINTEGRATIONSERVICENAME}='IntegrationServiceProd2', '', '.TEST')
	-- 
	-- --i_AccountNumber||IIF(@{pipeline().parameters.PMINTEGRATIONSERVICENAME}='IntegrationServiceProd2', '.EAR', '.EAR_TEST')||'_'||SUBSTR(v_FileDate,1,6)||--@{pipeline().parameters.FILE_EXTENSION}
	i_AccountNumber || @{pipeline().parameters.FILE_EXTENSION} || IFF(@{pipeline().parameters.PMINTEGRATIONSERVICENAME} = 'IntegrationServiceProd2', '', '.TEST') AS o_FileName,
	-- *INF*: CHR(127)
	CHR(127) AS o_SortOrder
	FROM AGG_Count
),
UN_All AS (
	SELECT o_Header AS Record, o_FileName AS FileName, o_SortOrder AS SortOrder
	FROM EXP_CalculateHeader
	UNION
	SELECT o_Detail AS Record, o_FileName AS FileName, o_SortOrder AS SortOrder
	FROM EXP_CalculateDetail
	UNION
	SELECT o_Trailer AS Record, o_FileName AS FileName, o_SortOrder AS SortOrder
	FROM EXP_CalculateTrailer
),
SRT_Records AS (
	SELECT
	Record, 
	FileName, 
	SortOrder
	FROM UN_All
	ORDER BY SortOrder ASC
),
EARS_FlatFile AS (
	INSERT INTO EPLIFlatFile
	(Record, FileName)
	SELECT 
	RECORD, 
	FILENAME
	FROM SRT_Records
),