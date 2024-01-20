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
SQ_Work_29_Record AS (
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,PDName.Value as NameOfEmployeeGroup
	      ,PDEmp.Value as EmployeeGroupStateList
		,PDLaw.Value as EmployeeDesgWCLaw
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC000311%'AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCLine L
		ON L.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCTrackHistory TH
		ON TH.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	INNER JOIN dbo.WorkWCPolicyDetails PDName
		ON PDName.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDName.Attribute='NameOfEmployeeGroup'
	
	INNER JOIN dbo.WorkWCPolicyDetails PDEmp
		ON PDEmp.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDEmp.Attribute='EmployeeGroupStateList'
		AND PDEmp.ProcessID=PDName.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails PDLaw
		ON PDLaw.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDLaw.Attribute='EmployeeDesgWCLaw'
		AND PDLaw.ProcessID=PDName.ProcessID
	
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_29}
	
	ORDER BY ST.WCTrackHistoryID
),
JNR_29_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.AuditId, 
	SQ_WCPols00Record.LinkData, 
	SQ_Work_29_Record.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_Work_29_Record.FormName, 
	SQ_Work_29_Record.Name, 
	SQ_Work_29_Record.TransactionEffectiveDate, 
	SQ_Work_29_Record.NameOfEmployeeGroup, 
	SQ_Work_29_Record.EmployeeGroupStateList, 
	SQ_Work_29_Record.EmployeeDesgWCLaw
	FROM SQ_WCPols00Record
	INNER JOIN SQ_Work_29_Record
	ON SQ_Work_29_Record.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
EXP_29_Record AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_29_Record.WCTrackHistoryID,
	JNR_29_Record.AuditId,
	JNR_29_Record.LinkData,
	'29' AS o_RecordTypeCode,
	'WC000311' AS o_EndorsementNumber,
	JNR_29_Record.FormName,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_29_Record.Name,
	JNR_29_Record.TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	JNR_29_Record.NameOfEmployeeGroup,
	JNR_29_Record.EmployeeGroupStateList,
	JNR_29_Record.EmployeeDesgWCLaw,
	WCTrackHistoryID AS v_CurrentRecord,
	-- *INF*: IIF(v_CurrentRecord=v_PrevRecord,v_RecordCount+1,1)
	IFF(v_CurrentRecord = v_PrevRecord, v_RecordCount + 1, 1) AS v_RecordCount,
	WCTrackHistoryID AS v_PrevRecord,
	-- *INF*: TO_CHAR(v_RecordCount)
	TO_CHAR(v_RecordCount) AS o_EndorsementSequenceNumber
	FROM JNR_29_Record
	 -- Manually join with mplt_Parse_FormNameField
),
WCPols29Record AS (
	INSERT INTO WCPols29Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, IdentifyEmployees, StateOfEmployment, DesignatedWorkersCompensationLawOrDescription, EndorsementSequenceNumber, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	o_RecordTypeCode AS RECORDTYPECODE, 
	o_EndorsementNumber AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	NameOfEmployeeGroup AS IDENTIFYEMPLOYEES, 
	EmployeeGroupStateList AS STATEOFEMPLOYMENT, 
	EmployeeDesgWCLaw AS DESIGNATEDWORKERSCOMPENSATIONLAWORDESCRIPTION, 
	o_EndorsementSequenceNumber AS ENDORSEMENTSEQUENCENUMBER, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_29_Record
),