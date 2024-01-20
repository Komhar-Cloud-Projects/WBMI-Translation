WITH
SQ_WCPols00Record AS (
	SELECT
		WCTrackHistoryID,
		LinkData,
	     AuditId
		FROM dbo.WCPols00Record
	WHERE 1=1
	AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY WCTrackHistoryID
),
SQ_WorkWCPolicyDetails AS (
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,CLT_NM.Value ClientName
		,CLT_ADD.Value Address
		,CLT_CT.Value City
		,CLT_ST.Value State
		,CLT_ZIP.Value Zip
		,CLT_FEIN.Value FEINNumber
		,CLT_UI.Value UINumber
		,'1' LeasingAddressTypeCode
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC220304%' AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_NM
		ON CLT_NM.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_NM.Attribute='ClientNameWC220304'
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_ADD
		ON CLT_ADD.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_ADD.Attribute='ClientAddressWC220304'
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_CT
		ON CLT_CT.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_CT.Attribute='ClientCityWC220304'
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_ST
		ON CLT_ST.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_ST.Attribute='ClientStateWC220304'
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_ZIP
		ON CLT_ZIP.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_ZIP.Attribute='ClientZipWC220304'
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_FEIN
		ON CLT_FEIN.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_FEIN.Attribute='ClientFEINNumberWC220304'
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_UI
		ON CLT_UI.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_UI.Attribute='ClientUINumberwC220304'
	
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_HC_RECORD}
	
	UNION 
	
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,'' ClientName
		,CLT_ADD.Value Address
		,CLT_CT.Value City
		,CLT_ST.Value State
		,CLT_ZIP.Value Zip
		,'' FEINNumber
		,'' UINumber
		,'2' LeasingAddressTypeCode
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC220304%' AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_ADD
		ON CLT_ADD.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_ADD.Attribute='IncludedClientAddress'
		
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_CT
		ON CLT_CT.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_CT.Attribute='IncludedClientCity'
		AND CLT_ADD.ProcessID=CLT_CT.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_ST
		ON CLT_ST.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_ST.Attribute='IncludedClientState'
		AND CLT_ADD.ProcessID=CLT_ST.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails CLT_ZIP
		ON CLT_ZIP.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CLT_ZIP.Attribute='IncludedClientZipcode'
		AND CLT_ADD.ProcessID=CLT_ZIP.ProcessID
	
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_HC_CLIENT}
	
	ORDER BY ST.WCTrackHistoryID,LeasingAddressTypeCode
),
JNR_RecordHC AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID AS WCTrackHistoryID_00Recod, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCPolicyDetails.WCTrackHistoryID, 
	SQ_WorkWCPolicyDetails.FormName, 
	SQ_WorkWCPolicyDetails.Name, 
	SQ_WorkWCPolicyDetails.TransactionEffectiveDate, 
	SQ_WorkWCPolicyDetails.ClientName, 
	SQ_WorkWCPolicyDetails.Address, 
	SQ_WorkWCPolicyDetails.City, 
	SQ_WorkWCPolicyDetails.State, 
	SQ_WorkWCPolicyDetails.Zip, 
	SQ_WorkWCPolicyDetails.FEINNumber, 
	SQ_WorkWCPolicyDetails.UINumber, 
	SQ_WorkWCPolicyDetails.LeasingAddressTypeCode
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WorkWCPolicyDetails
	ON SQ_WorkWCPolicyDetails.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
EXP_HC_Record AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_RecordHC.AuditId,
	JNR_RecordHC.WCTrackHistoryID_00Recod AS WCTrackHistoryID,
	JNR_RecordHC.LinkData,
	'22' AS StateCode,
	'HC' AS RecordTypeCode,
	'WC220304' AS EndorsementNumber,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_RecordHC.FormName,
	JNR_RecordHC.ClientName,
	JNR_RecordHC.Address,
	JNR_RecordHC.City,
	JNR_RecordHC.State,
	JNR_RecordHC.Zip,
	JNR_RecordHC.FEINNumber,
	-- *INF*: REPLACECHR(1,FEINNumber,'-','')
	REGEXP_REPLACE(FEINNumber,'-','') AS o_FEINNumber,
	JNR_RecordHC.UINumber,
	JNR_RecordHC.LeasingAddressTypeCode,
	JNR_RecordHC.Name,
	JNR_RecordHC.TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate
	FROM JNR_RecordHC
	 -- Manually join with mplt_Parse_FormNameField
),
SRTTRANS AS (
	SELECT
	ExtractDate, 
	AuditId, 
	WCTrackHistoryID, 
	LinkData, 
	StateCode, 
	RecordTypeCode, 
	EndorsementNumber, 
	BureauCode, 
	FormEdition, 
	ClientName, 
	LeasingAddressTypeCode, 
	Address, 
	City, 
	State, 
	Zip, 
	o_FEINNumber AS FEINNumber, 
	UINumber, 
	Name, 
	o_TransactionEffectiveDate AS TransactionEffectiveDate
	FROM EXP_HC_Record
	ORDER BY WCTrackHistoryID ASC, LeasingAddressTypeCode ASC
),
WCPolsHCRecord AS (
	INSERT INTO WCPolsHCRecord
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, NameOfClient, LeasingAddressTypeCode, AddressStreet, AddressCity, AddressState, AddressZipCode, ClientFederalEmployerIdentificationNumber, ClientsUnemploymentInsuranceNumber, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	RECORDTYPECODE, 
	ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	ClientName AS NAMEOFCLIENT, 
	LEASINGADDRESSTYPECODE, 
	Address AS ADDRESSSTREET, 
	City AS ADDRESSCITY, 
	State AS ADDRESSSTATE, 
	Zip AS ADDRESSZIPCODE, 
	FEINNumber AS CLIENTFEDERALEMPLOYERIDENTIFICATIONNUMBER, 
	UINumber AS CLIENTSUNEMPLOYMENTINSURANCENUMBER, 
	Name AS NAMEOFINSURED, 
	TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM SRTTRANS
),