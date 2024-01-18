WITH
SQ_WorkWCTrackHistory AS (
	SELECT 
	Track.WCTrackHistoryID as WCTrackHistoryID, 
	Case When SubString(Forms.FormName,1,8) in ( 'WC480314','WC480315','WC480317') then SubString(Forms.FormName,1,8) END as ParsedFormName,
	Forms.FormName as FormName,
	Party.Name as Name,
	Policy.TransactionEffectiveDate as TransactionEffectiveDate,
	Detail.Attribute, Detail.Value, Detail.ProcessID as ProcessID 
	FROM
	WorkWCForms Forms
	inner join WorkWCTrackHistory Track 	on 
		Forms.WCTrackHistoryID=Track.WCTrackHistoryID and 
		(Forms.FormName like 'WC480314%' or  
		 Forms.FormName like 'WC480315%' or 
		 Forms.FormName like 'WC480317%')
		and 
		((Forms.OnPolicy=1 OR Forms.[Add] = 1) AND (Forms.Remove is null OR Forms.Remove = 0))
	inner join WorkWCParty Party on 
		Party.WCTrackHistoryID=Track.WCTrackHistoryID AND 
		Party.PartyAssociationType='Account'
	inner join WorkWCPolicy Policy on 
		Policy.WCTrackHistoryID=Track.WCTrackHistoryID
	inner join WorkWCPolicyDetails Detail on 
		policy.WCTrackHistoryID=Detail.WCTrackHistoryID and policy.PolicyId=Detail.PolicyID
		and Detail.Attribute in ('IncludedClientName', 'IncludedClientAddress', 'ClientFEINNumber', 'EstimatedPremium', 'ClientNameEmployeeLeasing', 'ClientFEINNumberEmployeeLeasing', 'ClientAddressEmployeeLeasing', 'LaborContractorAddress', 'LaborContractor')
	WHERE
	Forms.Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_EC}
),
AGG_AlignWithPolicyDetails AS (
	SELECT
	WCTrackHistoryID,
	ParsedFormName,
	FormName,
	Name,
	TransactionEffectiveDate,
	ProcessID,
	Attribute,
	Value,
	-- *INF*: MAX(Value,Attribute='IncludedClientName')
	MAX(Value, Attribute = 'IncludedClientName') AS IncludedClientName,
	-- *INF*: MAX(Value,Attribute='IncludedClientAddress')
	-- 
	MAX(Value, Attribute = 'IncludedClientAddress') AS IncludedClientAddress,
	-- *INF*: MAX(Value,Attribute='ClientFEINNumber')
	-- 
	MAX(Value, Attribute = 'ClientFEINNumber') AS ClientFEINNumber,
	-- *INF*: MAX(Value,Attribute='EstimatedPremium')
	MAX(Value, Attribute = 'EstimatedPremium') AS EstimatedPremium,
	-- *INF*: MAX(Value,Attribute='ClientNameEmployeeLeasing')
	-- 
	MAX(Value, Attribute = 'ClientNameEmployeeLeasing') AS ClientNameEmployeeLeasing,
	-- *INF*: MAX(Value,Attribute='ClientFEINNumberEmployeeLeasing')
	-- 
	MAX(Value, Attribute = 'ClientFEINNumberEmployeeLeasing') AS ClientFEINNumberEmployeeLeasing,
	-- *INF*: MAX(Value,Attribute='ClientAddressEmployeeLeasing')
	-- 
	MAX(Value, Attribute = 'ClientAddressEmployeeLeasing') AS ClientAddressEmployeeLeasing,
	-- *INF*: MAX(Value,Attribute='LaborContractorAddress')
	-- 
	MAX(Value, Attribute = 'LaborContractorAddress') AS LaborContractorAddress,
	-- *INF*: MAX(Value,Attribute='LaborContractor')
	-- 
	MAX(Value, Attribute = 'LaborContractor') AS LaborContractor
	FROM SQ_WorkWCTrackHistory
	GROUP BY WCTrackHistoryID, ParsedFormName, FormName, Name, TransactionEffectiveDate, ProcessID
),
EXP_applyrules AS (
	SELECT
	WCTrackHistoryID,
	ParsedFormName,
	FormName,
	Name,
	TransactionEffectiveDate,
	ProcessID AS FormIterationID,
	IncludedClientName,
	IncludedClientAddress,
	ClientFEINNumber,
	EstimatedPremium,
	ClientNameEmployeeLeasing,
	ClientFEINNumberEmployeeLeasing,
	ClientAddressEmployeeLeasing,
	LaborContractorAddress,
	LaborContractor,
	-- *INF*: DECODE(TRUE,
	-- ParsedFormName='WC480314',IncludedClientName,
	-- ParsedFormName='WC480315',ClientNameEmployeeLeasing,
	-- ParsedFormName='WC480317',LaborContractor,
	-- '')
	DECODE(
	    TRUE,
	    ParsedFormName = 'WC480314', IncludedClientName,
	    ParsedFormName = 'WC480315', ClientNameEmployeeLeasing,
	    ParsedFormName = 'WC480317', LaborContractor,
	    ''
	) AS v_NameOfClientOrNameOfLaborContractor,
	-- *INF*: DECODE(TRUE,
	-- ParsedFormName='WC480314',ClientFEINNumber,
	-- ParsedFormName='WC480315',ClientFEINNumberEmployeeLeasing,
	-- '')
	DECODE(
	    TRUE,
	    ParsedFormName = 'WC480314', ClientFEINNumber,
	    ParsedFormName = 'WC480315', ClientFEINNumberEmployeeLeasing,
	    ''
	) AS v_FederalEmployerIdentificationNumber,
	-- *INF*: DECODE(TRUE,
	-- ParsedFormName='WC480314',LPAD(EstimatedPremium,10,'0'),
	-- '')
	DECODE(
	    TRUE,
	    ParsedFormName = 'WC480314', LPAD(EstimatedPremium, 10, '0'),
	    ''
	) AS v_ClientPremiumAmount,
	-- *INF*: DECODE(TRUE,
	-- ParsedFormName='WC480314',IncludedClientAddress,
	-- ParsedFormName='WC480315',ClientAddressEmployeeLeasing,
	-- ParsedFormName='WC480317',LaborContractorAddress,
	-- '')
	-- -- determine which record address to use
	DECODE(
	    TRUE,
	    ParsedFormName = 'WC480314', IncludedClientAddress,
	    ParsedFormName = 'WC480315', ClientAddressEmployeeLeasing,
	    ParsedFormName = 'WC480317', LaborContractorAddress,
	    ''
	) AS v_Address_Base,
	v_Address_Base AS o_Address,
	',' AS AddressDelimiter,
	v_NameOfClientOrNameOfLaborContractor AS o_NameOfClientOrNameOfLaborContractor,
	v_ClientPremiumAmount AS o_ClientPremiumAmount,
	v_FederalEmployerIdentificationNumber AS o_FederalEmployerIdentificationNumber
	FROM AGG_AlignWithPolicyDetails
),
jtx_split_string AS (-- jtx_split_string

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_Address_Check_And_Passthrough AS (
	SELECT
	EXP_applyrules.o_Address AS i_FullAddress,
	jtx_split_string.OUTPUT_Field1 AS i_StreetAddress,
	jtx_split_string.OUTPUT_Field2 AS i_City,
	jtx_split_string.OUTPUT_Field3 AS i_State,
	jtx_split_string.OUTPUT_Field4 AS i_Zip,
	-- *INF*: IIF(NOT ISNULL(i_StreetAddress) AND NOT ISNULL(i_City) AND NOT ISNULL(i_State) AND NOT ISNULL(i_Zip),1,0)
	IFF(
	    i_StreetAddress IS NULL
	    and i_City IS NULL
	    and i_State IS NULL
	    and i_Zip IS NOT NOT NOT NOT NULL,
	    1,
	    0
	) AS v_AddressFieldsPopulated,
	-- *INF*: IIF(v_AddressFieldsPopulated=1,rtrim(ltrim(i_StreetAddress)),i_FullAddress)
	IFF(v_AddressFieldsPopulated = 1, rtrim(ltrim(i_StreetAddress)), i_FullAddress) AS o_StreetAddress,
	-- *INF*: IIF(v_AddressFieldsPopulated=1,rtrim(ltrim(i_City)),'')
	IFF(v_AddressFieldsPopulated = 1, rtrim(ltrim(i_City)), '') AS o_City,
	-- *INF*: IIF(v_AddressFieldsPopulated=1,rtrim(ltrim(i_State)),'')
	IFF(v_AddressFieldsPopulated = 1, rtrim(ltrim(i_State)), '') AS o_State,
	-- *INF*: IIF(v_AddressFieldsPopulated=1,rtrim(ltrim(i_Zip)),'')
	IFF(v_AddressFieldsPopulated = 1, rtrim(ltrim(i_Zip)), '') AS o_Zip,
	EXP_applyrules.WCTrackHistoryID,
	EXP_applyrules.ParsedFormName,
	EXP_applyrules.FormName,
	EXP_applyrules.Name,
	EXP_applyrules.TransactionEffectiveDate,
	EXP_applyrules.o_NameOfClientOrNameOfLaborContractor AS NameOfClientOrNameOfLaborContractor,
	EXP_applyrules.o_ClientPremiumAmount AS ClientPremiumAmount,
	EXP_applyrules.o_FederalEmployerIdentificationNumber AS FederalEmployerIdentificationNumber
	FROM EXP_applyrules
	 -- Manually join with jtx_split_string
),
SQ_WCPols00Record AS (
	SELECT
		WCTrackHistoryID,
		LinkData,
	      AuditId
	FROM dbo.WCPols00Record
	WHERE 
	 AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY WCTrackHistoryID
),
JNR_DataCollect AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	EXP_Address_Check_And_Passthrough.WCTrackHistoryID AS WCTrackHistoryID1, 
	EXP_Address_Check_And_Passthrough.ParsedFormName, 
	EXP_Address_Check_And_Passthrough.FormName, 
	EXP_Address_Check_And_Passthrough.Name, 
	EXP_Address_Check_And_Passthrough.TransactionEffectiveDate, 
	EXP_Address_Check_And_Passthrough.NameOfClientOrNameOfLaborContractor, 
	EXP_Address_Check_And_Passthrough.ClientPremiumAmount, 
	EXP_Address_Check_And_Passthrough.o_StreetAddress AS AddressStreet, 
	EXP_Address_Check_And_Passthrough.o_City AS AddressCity, 
	EXP_Address_Check_And_Passthrough.o_State AS AddressState, 
	EXP_Address_Check_And_Passthrough.o_Zip AS AddressZipcode, 
	EXP_Address_Check_And_Passthrough.FederalEmployerIdentificationNumber
	FROM SQ_WCPols00Record
	INNER JOIN EXP_Address_Check_And_Passthrough
	ON EXP_Address_Check_And_Passthrough.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
),
mplt_Parse_FormNameField AS (WITH
	INPUT_FormName AS (
		
	),
	EXPTRANS AS (
		SELECT
		ParsedNameOfForm,
		FormNameFromSource,
		-- *INF*: REVERSE(FormNameFromSource)
		REVERSE(FormNameFromSource) AS vReversedFromNameFromSource,
		-- *INF*: REVERSE(substr(vReversedFromNameFromSource,1,4))
		REVERSE(substr(vReversedFromNameFromSource, 1, 4)) AS vFormEdition,
		-- *INF*: DECODE(TRUE,
		-- substr(vReversedFromNameFromSource,5,1) >='A' and substr(vReversedFromNameFromSource,5,1) <='Z', substr(vReversedFromNameFromSource,5,1),
		-- ' '
		-- )
		-- 
		-- -- check if within A and Z, if not then space
		DECODE(
		    TRUE,
		    substr(vReversedFromNameFromSource, 5, 1) >= 'A' and substr(vReversedFromNameFromSource, 5, 1) <= 'Z', substr(vReversedFromNameFromSource, 5, 1),
		    ' '
		) AS vBureauCode,
		vFormEdition AS oFormEdition,
		vBureauCode AS oBureauCode
		FROM INPUT_FormName
	),
	OUTPUT_FormName AS (
		SELECT
		ParsedNameOfForm, 
		FormNameFromSource, 
		oFormEdition AS FormEdition, 
		oBureauCode AS BureauCode
		FROM EXPTRANS
	),
),
EXP_PrepareOutput AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_DataCollect.AuditId,
	JNR_DataCollect.WCTrackHistoryID,
	JNR_DataCollect.LinkData,
	'EC' AS RecordTypeCode,
	mplt_Parse_FormNameField.ParsedNameOfForm1,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_DataCollect.NameOfClientOrNameOfLaborContractor,
	JNR_DataCollect.ClientPremiumAmount,
	JNR_DataCollect.Name,
	JNR_DataCollect.TransactionEffectiveDate AS i_TransactionEffectiveDate,
	-- *INF*: TO_CHAR(i_TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(i_TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	JNR_DataCollect.AddressStreet,
	JNR_DataCollect.AddressCity,
	JNR_DataCollect.AddressState,
	JNR_DataCollect.AddressZipcode,
	JNR_DataCollect.FederalEmployerIdentificationNumber,
	'48' AS o_StateCode
	FROM JNR_DataCollect
	 -- Manually join with mplt_Parse_FormNameField
),
WCPolsECRecord AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPolsECRecord
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPolsECRecord
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, NameOfClientOrNameOfLaborContractor, AddressStreet, AddressCity, AddressState, AddressZipcode, FederalEmployerIdentificationNumber, ClientPremiumAmount, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	o_StateCode AS STATECODE, 
	RECORDTYPECODE, 
	ParsedNameOfForm1 AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	NAMEOFCLIENTORNAMEOFLABORCONTRACTOR, 
	ADDRESSSTREET, 
	ADDRESSCITY, 
	ADDRESSSTATE, 
	ADDRESSZIPCODE, 
	FEDERALEMPLOYERIDENTIFICATIONNUMBER, 
	CLIENTPREMIUMAMOUNT, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_PrepareOutput
),