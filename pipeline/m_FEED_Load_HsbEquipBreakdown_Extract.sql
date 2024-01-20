WITH
SQ_WorkHSBEquipmentBreakdownExtract AS (
	SELECT
		PolicyAKId,
		LocationXMLId,
		TransactionNumber,
		CurrentPolicyNumber,
		CompanyCode,
		TransactionCode,
		NameOfInsured,
		MailingStreetAddress,
		MailingCity,
		MailingStateProvinceAbbreviation,
		MailingZipCode,
		TransactionEnteredDate,
		TransactionEffectiveDate,
		PolicyEffectiveDate,
		PolicyExpirationDate,
		Coverage,
		EBGrossPremium,
		EBNetPremium,
		Deductible,
		Occupancy,
		Value,
		ValueType,
		BranchCode,
		AgencyCode,
		PreviousPolicyNumber,
		ProgramId,
		TreatyNumber,
		ISOType,
		LocationName,
		RiskStreetAddress,
		RiskCity,
		RiskStateProvinceAbbreviation,
		RiskZipCode,
		OTIndicator,
		InspectionContactName,
		ContactPhoneNumber,
		ReportingPeriod,
		ActiveLocationFlag,
		OriginalTransactionalEnteredDate,
		AgencyName,
		AgencyEmailAddress,
		AgencyPhoneNumber
	FROM WorkHSBEquipmentBreakdownExtract
	WHERE ReportingPeriod=@{pipeline().parameters.RUNDATE}
),
EXP_Location AS (
	SELECT
	TransactionNumber,
	'3' AS v_RecordType,
	v_RecordType AS RecordType,
	CurrentPolicyNumber,
	CompanyCode,
	TransactionCode,
	ActiveLocationFlag,
	-- *INF*: DECODE(TRUE,
	-- TransactionCode='Cancel','42',
	-- TransactionCode='NonRenew','42',
	-- IN(ActiveLocationFlag,'F','0')=1,'42',
	-- '40')
	DECODE(
	    TRUE,
	    TransactionCode = 'Cancel', '42',
	    TransactionCode = 'NonRenew', '42',
	    ActiveLocationFlag IN ('F','0') = 1, '42',
	    '40'
	) AS v_TransactionCode,
	LocationName,
	RiskStreetAddress,
	RiskCity,
	RiskStateProvinceAbbreviation,
	RiskZipCode,
	OTIndicator,
	InspectionContactName,
	ContactPhoneNumber,
	-- *INF*: TO_CHAR(ContactPhoneNumber)
	TO_CHAR(ContactPhoneNumber) AS v_Char_ContactPhoneNumber,
	-- *INF*: RPAD(IIF(NOT ISNULL(v_Char_ContactPhoneNumber),v_Char_ContactPhoneNumber,''),13,' ')
	RPAD(
	    IFF(
	        v_Char_ContactPhoneNumber IS NOT NULL, v_Char_ContactPhoneNumber, ''
	    ), 13, ' ') AS v_ContactPhoneNumber,
	Occupancy,
	Value,
	-- *INF*: IIF(Value>0,LPAD(TO_CHAR(Value),9,'0'),RPAD('',9,'0'))
	IFF(Value > 0, LPAD(TO_CHAR(Value), 9, '0'), RPAD('', 9, '0')) AS v_Value,
	ValueType,
	-- *INF*: RPAD('',88,' ')
	RPAD('', 88, ' ') AS v_Filler1,
	-- *INF*: RPAD('',59,' ')
	RPAD('', 59, ' ') AS v_Filler2,
	-- *INF*: RPAD('',69,' ')
	RPAD('', 69, ' ') AS v_Filler3,
	ReportingPeriod,
	AgencyName,
	AgencyEmailAddress,
	AgencyPhoneNumber,
	-- *INF*: RPAD(TO_CHAR(AgencyName),100,'')
	RPAD(TO_CHAR(AgencyName), 100, '') AS v_AgencyName,
	-- *INF*: RPAD(TO_CHAR(AgencyEmailAddress),100,'')
	RPAD(TO_CHAR(AgencyEmailAddress), 100, '') AS v_AgencyEmailAddress,
	-- *INF*: RPAD(TO_CHAR(AgencyPhoneNumber),15,'')
	RPAD(TO_CHAR(AgencyPhoneNumber), 15, '') AS v_AgencyPhoneNumber,
	TransactionNumber||
v_RecordType||
CurrentPolicyNumber||
CompanyCode||
v_TransactionCode||
LocationName||
RiskStreetAddress||
RiskCity||
RiskStateProvinceAbbreviation||
RiskZipCode||
OTIndicator||
InspectionContactName||
v_ContactPhoneNumber||
Occupancy||
v_Value||
ValueType||
v_Filler1||
v_Filler2||
v_Filler3
|| v_AgencyName
|| v_AgencyEmailAddress
|| v_AgencyPhoneNumber AS Record
	FROM SQ_WorkHSBEquipmentBreakdownExtract
),
EXPTRANS AS (
	SELECT
	PolicyAKId,
	TransactionNumber,
	CurrentPolicyNumber,
	CompanyCode,
	TransactionCode,
	NameOfInsured,
	MailingStreetAddress,
	MailingCity,
	MailingStateProvinceAbbreviation,
	MailingZipCode,
	TransactionEnteredDate,
	TransactionEffectiveDate,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	Coverage,
	EBGrossPremium,
	EBNetPremium,
	Deductible,
	Value,
	ValueType,
	-- *INF*: DECODE(ValueType,
	-- 'B',1,
	-- 'C',10,
	-- 'T',11,
	-- 0)
	DECODE(
	    ValueType,
	    'B', 1,
	    'C', 10,
	    'T', 11,
	    0
	) AS Policy_ValueType,
	BranchCode,
	AgencyCode,
	PreviousPolicyNumber,
	ProgramId,
	TreatyNumber,
	ISOType,
	ReportingPeriod,
	OriginalTransactionalEnteredDate,
	AgencyName,
	AgencyEmailAddress,
	AgencyPhoneNumber
	FROM SQ_WorkHSBEquipmentBreakdownExtract
),
AGG_Policy AS (
	SELECT
	PolicyAKId,
	TransactionNumber,
	CurrentPolicyNumber,
	CompanyCode,
	TransactionCode,
	NameOfInsured,
	MailingStreetAddress,
	MailingCity,
	MailingStateProvinceAbbreviation,
	MailingZipCode,
	TransactionEnteredDate,
	TransactionEffectiveDate,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	Coverage,
	EBGrossPremium,
	EBNetPremium,
	Deductible,
	Value,
	-- *INF*: SUM(IIF(Value>0,Value,0))
	SUM(
	    IFF(
	        Value > 0, Value, 0
	    )) AS o_Value,
	Policy_ValueType,
	-- *INF*: MAX(Policy_ValueType)
	MAX(Policy_ValueType) AS o_Policy_ValueType_Max,
	-- *INF*: MIN(Policy_ValueType)
	MIN(Policy_ValueType) AS o_Policy_ValueType_Min,
	BranchCode,
	AgencyCode,
	PreviousPolicyNumber,
	ProgramId,
	TreatyNumber,
	ISOType,
	ReportingPeriod,
	OriginalTransactionalEnteredDate,
	AgencyName,
	AgencyEmailAddress,
	AgencyPhoneNumber
	FROM EXPTRANS
	GROUP BY PolicyAKId, TransactionNumber, TransactionEnteredDate, TransactionEffectiveDate, OriginalTransactionalEnteredDate
),
EXP_Policy AS (
	SELECT
	TransactionNumber,
	'2' AS v_RecordType,
	v_RecordType AS RecordType,
	CurrentPolicyNumber,
	CompanyCode,
	TransactionCode,
	-- *INF*: DECODE(TransactionCode,
	-- 'New','01',
	-- 'Endorse','30',
	-- 'Cancel','03',
	-- 'Reinstate','10',
	-- 'Rewrite','04',
	-- 'Reissue','04',
	-- 'Renew','07',
	-- 'NonRenew','03',
	-- '30')
	DECODE(
	    TransactionCode,
	    'New', '01',
	    'Endorse', '30',
	    'Cancel', '03',
	    'Reinstate', '10',
	    'Rewrite', '04',
	    'Reissue', '04',
	    'Renew', '07',
	    'NonRenew', '03',
	    '30'
	) AS v_TransactionCode,
	NameOfInsured,
	MailingStreetAddress,
	MailingCity,
	MailingStateProvinceAbbreviation,
	MailingZipCode,
	TransactionEnteredDate,
	-- *INF*: TO_CHAR(TransactionEnteredDate,'YYYYMMDD')
	TO_CHAR(TransactionEnteredDate, 'YYYYMMDD') AS v_TransactionEnteredDate,
	TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYYYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYYYMMDD') AS v_TransactionEffectiveDate,
	PolicyEffectiveDate,
	-- *INF*: TO_CHAR(PolicyEffectiveDate,'YYYYMMDD')
	TO_CHAR(PolicyEffectiveDate, 'YYYYMMDD') AS v_PolicyEffectiveDate,
	PolicyExpirationDate,
	-- *INF*: TO_CHAR(PolicyExpirationDate,'YYYYMMDD')
	TO_CHAR(PolicyExpirationDate, 'YYYYMMDD') AS v_PolicyExpirationDate,
	Coverage,
	EBGrossPremium,
	-- *INF*: LPAD(TO_CHAR(ABS(EBGrossPremium)),11,'0')
	LPAD(TO_CHAR(ABS(EBGrossPremium)), 11, '0') AS v_EBGrossPremium,
	EBGrossPremium AS o_EBGrossPremium,
	EBNetPremium,
	-- *INF*: LPAD(TO_CHAR(ABS(EBNetPremium)),11,'0')
	LPAD(TO_CHAR(ABS(EBNetPremium)), 11, '0') AS v_EBNetPremium,
	EBNetPremium AS o_EBNetPremium,
	Deductible,
	-- *INF*: RPAD('',6,' ')
	RPAD('', 6, ' ') AS v_Occupancy,
	o_Value AS Value,
	-- *INF*: IIF(Value>0,LPAD(TO_CHAR(Value),9,'0'),LPAD('',9,'0'))
	IFF(Value > 0, LPAD(TO_CHAR(Value), 9, '0'), LPAD('', 9, '0')) AS v_Value,
	o_Policy_ValueType_Max AS ValueType_Max,
	o_Policy_ValueType_Min AS ValueType_Min,
	-- *INF*: DECODE(TRUE,
	-- ValueType_Max=1,'B',
	-- ValueType_Max=10 and ValueType_Min=10, 'C',
	-- ValueType_Max=10 and ValueType_Max<>ValueType_Min,'T',
	-- ValueType_Max>10,'T')
	DECODE(
	    TRUE,
	    ValueType_Max = 1, 'B',
	    ValueType_Max = 10 and ValueType_Min = 10, 'C',
	    ValueType_Max = 10 and ValueType_Max <> ValueType_Min, 'T',
	    ValueType_Max > 10, 'T'
	) AS ValueType,
	BranchCode,
	AgencyCode,
	PreviousPolicyNumber,
	ProgramId,
	TreatyNumber,
	ISOType,
	-- *INF*: RPAD('',88,' ')
	RPAD('', 88, ' ') AS v_Filler1,
	-- *INF*: RPAD('',59,' ')
	RPAD('', 59, ' ') AS v_Filler2,
	-- *INF*: RPAD('',13,' ')
	RPAD('', 13, ' ') AS v_Filler3,
	ReportingPeriod,
	AgencyName,
	AgencyEmailAddress,
	AgencyPhoneNumber,
	-- *INF*: RPAD(TO_CHAR(AgencyName),100,'')
	RPAD(TO_CHAR(AgencyName), 100, '') AS v_AgencyName,
	-- *INF*: RPAD(TO_CHAR(AgencyEmailAddress),100,'')
	RPAD(TO_CHAR(AgencyEmailAddress), 100, '') AS v_AgencyEmailAddress,
	-- *INF*: RPAD(TO_CHAR(AgencyPhoneNumber),15,'')
	RPAD(TO_CHAR(AgencyPhoneNumber), 15, '') AS v_AgencyPhoneNumber,
	-- *INF*: TransactionNumber||
	-- v_RecordType||
	-- CurrentPolicyNumber||
	-- CompanyCode||
	-- v_TransactionCode||
	-- NameOfInsured||
	-- MailingStreetAddress||
	-- MailingCity||
	-- MailingStateProvinceAbbreviation||
	-- MailingZipCode||
	-- v_TransactionEnteredDate||
	-- v_TransactionEffectiveDate||
	-- v_PolicyEffectiveDate||
	-- v_PolicyExpirationDate||
	-- Coverage||
	-- IIF(EBGrossPremium>=0,v_EBGrossPremium,'-'||SUBSTR(v_EBGrossPremium,2,10))||
	-- IIF(EBNetPremium>=0,v_EBNetPremium,'-'||SUBSTR(v_EBNetPremium,2,10))||
	-- Deductible||
	-- v_Occupancy||
	-- v_Value||
	-- ValueType||
	-- BranchCode||
	-- AgencyCode||
	-- PreviousPolicyNumber||
	-- ProgramId||
	-- TreatyNumber||
	-- ISOType||
	-- v_Filler1||
	-- v_Filler2||
	-- v_Filler3
	-- || v_AgencyName
	-- || v_AgencyEmailAddress
	-- || v_AgencyPhoneNumber
	TransactionNumber || v_RecordType || CurrentPolicyNumber || CompanyCode || v_TransactionCode || NameOfInsured || MailingStreetAddress || MailingCity || MailingStateProvinceAbbreviation || MailingZipCode || v_TransactionEnteredDate || v_TransactionEffectiveDate || v_PolicyEffectiveDate || v_PolicyExpirationDate || Coverage || IFF(EBGrossPremium >= 0, v_EBGrossPremium, '-' || SUBSTR(v_EBGrossPremium, 2, 10)) || IFF(EBNetPremium >= 0, v_EBNetPremium, '-' || SUBSTR(v_EBNetPremium, 2, 10)) || Deductible || v_Occupancy || v_Value || ValueType || BranchCode || AgencyCode || PreviousPolicyNumber || ProgramId || TreatyNumber || ISOType || v_Filler1 || v_Filler2 || v_Filler3 || v_AgencyName || v_AgencyEmailAddress || v_AgencyPhoneNumber AS Record
	FROM AGG_Policy
),
Union_PolicyLocation AS (
	SELECT TransactionNumber, RecordType, Record, o_EBGrossPremium AS EBGrossPremium, o_EBNetPremium AS EBNetPremium, ReportingPeriod
	FROM EXP_Policy
	UNION
	SELECT TransactionNumber, RecordType, Record, ReportingPeriod
	FROM EXP_Location
),
AGG_Control AS (
	SELECT
	EBGrossPremium,
	EBNetPremium,
	-- *INF*: COUNT(1)
	COUNT(1) AS RecordCount,
	-- *INF*: SUM(EBGrossPremium)
	SUM(EBGrossPremium) AS TotalGrossPremium,
	-- *INF*: SUM(EBNetPremium)
	SUM(EBNetPremium) AS TotalNetPremium,
	ReportingPeriod
	FROM Union_PolicyLocation
	GROUP BY 
),
EXP_Control AS (
	SELECT
	RecordCount,
	TotalGrossPremium,
	TotalNetPremium,
	ReportingPeriod,
	-- *INF*: RPAD('',26,'1')
	RPAD('', 26, '1') AS v_ControlKey,
	'000118' AS v_CompanyCode,
	-- *INF*: LPAD(TO_CHAR(RecordCount),7,'0')
	LPAD(TO_CHAR(RecordCount), 7, '0') AS v_NumberOfRecords,
	-- *INF*: LPAD(TO_CHAR(ABS(TotalGrossPremium)),11,'0')
	LPAD(TO_CHAR(ABS(TotalGrossPremium)), 11, '0') AS v_TotalGrossPremium,
	-- *INF*: LPAD(TO_CHAR(ABS(TotalNetPremium)),11,'0')
	LPAD(TO_CHAR(ABS(TotalNetPremium)), 11, '0') AS v_TotalNetPremium,
	-- *INF*: TO_CHAR(ReportingPeriod)
	TO_CHAR(ReportingPeriod) AS v_ReportingPeriod,
	'06.00' AS v_VersionNumber,
	-- *INF*: RPAD('',200,' ')
	RPAD('', 200, ' ') AS v_Filer1,
	-- *INF*: RPAD('',208,' ')
	RPAD('', 208, ' ') AS v_Filter2,
	'99999' AS TransactionNumber,
	'9' AS RecordType,
	-- *INF*: v_ControlKey||
	-- v_CompanyCode||
	-- v_NumberOfRecords||
	-- IIF(TotalGrossPremium>=0,v_TotalGrossPremium,'-'||SUBSTR(v_TotalGrossPremium,2,10))||
	-- IIF(TotalNetPremium>=0,v_TotalNetPremium,'-'||SUBSTR(v_TotalNetPremium,2,10))||
	-- v_ReportingPeriod||
	-- v_VersionNumber||
	-- v_Filer1||
	-- v_Filter2
	v_ControlKey || v_CompanyCode || v_NumberOfRecords || IFF(TotalGrossPremium >= 0, v_TotalGrossPremium, '-' || SUBSTR(v_TotalGrossPremium, 2, 10)) || IFF(TotalNetPremium >= 0, v_TotalNetPremium, '-' || SUBSTR(v_TotalNetPremium, 2, 10)) || v_ReportingPeriod || v_VersionNumber || v_Filer1 || v_Filter2 AS Record
	FROM AGG_Control
),
Union_Control AS (
	SELECT TransactionNumber, RecordType, Record
	FROM Union_PolicyLocation
	UNION
	SELECT TransactionNumber, RecordType, Record
	FROM EXP_Control
),
SRT_Records AS (
	SELECT
	TransactionNumber, 
	RecordType, 
	Record
	FROM Union_Control
	ORDER BY TransactionNumber ASC, RecordType ASC
),
HSBEquipmentBreakdownExtractFile AS (
	INSERT INTO HSBEquipmentBreakdownExtractFile
	(Record)
	SELECT 
	RECORD
	FROM SRT_Records
),