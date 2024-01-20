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
SQ_WorkWC_24_Record AS (
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,AEName.Value as AlternateEmployerName
	    ,AEAdd.Value as AddressOfAlternateEmployer
		,AEEmp.Value as AlternateEmployerStateOfEmployment
		,AECP.Value as AlternateEmployerContractOrProject
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC000301%'AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCLine L
		ON L.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCTrackHistory TH
		ON TH.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	INNER JOIN dbo.WorkWCPolicyDetails AEName
		ON AEName.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND AEName.Attribute='AlternateEmployerName'
	
	INNER JOIN dbo.WorkWCPolicyDetails AEAdd
		ON AEAdd.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND AEAdd.Attribute='AddressOfAlternateEmployer'
		AND AEAdd.ProcessID=AEName.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails AEEmp
		ON AEEmp.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND AEEmp.Attribute='AlternateEmployerStateOfEmployment'
		AND AEEmp.ProcessID=AEName.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails AECP
		ON AECP.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND AECP.Attribute='AlternateEmployerContractOrProject'
		AND AECP.ProcessID=AEName.ProcessID
	
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_24}
	
	ORDER BY ST.WCTrackHistoryID
),
JNR_24_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.AuditId, 
	SQ_WCPols00Record.LinkData, 
	SQ_WorkWC_24_Record.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWC_24_Record.FormName, 
	SQ_WorkWC_24_Record.Name, 
	SQ_WorkWC_24_Record.TransactionEffectiveDate, 
	SQ_WorkWC_24_Record.AlternateEmployerName, 
	SQ_WorkWC_24_Record.AddressOfAlternateEmployer, 
	SQ_WorkWC_24_Record.AlternateEmployerStateOfEmployment, 
	SQ_WorkWC_24_Record.AlternateEmployerContractOrProject
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WorkWC_24_Record
	ON SQ_WorkWC_24_Record.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
EXP_24_Record AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_24_Record.AuditId,
	JNR_24_Record.WCTrackHistoryID,
	JNR_24_Record.LinkData,
	'24' AS o_RecordTypeCode,
	'WC000301' AS o_EndorsementNumber,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_24_Record.AlternateEmployerName,
	JNR_24_Record.AddressOfAlternateEmployer,
	JNR_24_Record.AlternateEmployerStateOfEmployment,
	JNR_24_Record.AlternateEmployerContractOrProject,
	WCTrackHistoryID AS v_CurrentRecord,
	-- *INF*: IIF(v_CurrentRecord=v_PreviousRecord,v_RecordCount+1,1)
	IFF(v_CurrentRecord = v_PreviousRecord, v_RecordCount + 1, 1) AS v_RecordCount,
	WCTrackHistoryID AS v_PreviousRecord,
	v_RecordCount AS o_EndorsementSequenceNumber,
	JNR_24_Record.Name,
	JNR_24_Record.TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate
	FROM JNR_24_Record
	 -- Manually join with mplt_Parse_FormNameField
),
WCPols24Record AS (
	INSERT INTO WCPols24Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, NameOfAlternateEmployer, AddressOfAlternateEmployer, StateOfSpecialTemporaryEmployment, NameOfContractOrProject, EndorsementSequenceNumber, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	o_RecordTypeCode AS RECORDTYPECODE, 
	o_EndorsementNumber AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	AlternateEmployerName AS NAMEOFALTERNATEEMPLOYER, 
	ADDRESSOFALTERNATEEMPLOYER, 
	AlternateEmployerStateOfEmployment AS STATEOFSPECIALTEMPORARYEMPLOYMENT, 
	AlternateEmployerContractOrProject AS NAMEOFCONTRACTORPROJECT, 
	o_EndorsementSequenceNumber AS ENDORSEMENTSEQUENCENUMBER, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_24_Record
),