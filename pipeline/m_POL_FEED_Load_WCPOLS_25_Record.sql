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
SQ_WorkWCForms_Record25 AS (
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,PD.Value ExcludedWorkplace
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC000302%'AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCLine L
		ON L.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCTrackHistory TH
		ON TH.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	INNER JOIN dbo.WorkWCPolicyDetails PD
		ON PD.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PD.Attribute='ExcludedWorkplace' AND PD.Value IS NOT NULL
		
	WHERE 1 = 1
	AND PD.Attribute='ExcludedWorkplace' AND PD.Value IS NOT NULL
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_25}
	
	ORDER BY ST.WCTrackHistoryID
),
JNR_25_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.AuditId, 
	SQ_WCPols00Record.LinkData, 
	SQ_WorkWCForms_Record25.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCForms_Record25.FormName, 
	SQ_WorkWCForms_Record25.Name, 
	SQ_WorkWCForms_Record25.TransactionEffectiveDate, 
	SQ_WorkWCForms_Record25.ExcludedWorkplace
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WorkWCForms_Record25
	ON SQ_WorkWCForms_Record25.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
EXP_25_Record AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_25_Record.AuditId,
	JNR_25_Record.WCTrackHistoryID,
	JNR_25_Record.LinkData,
	'25' AS RecordTypeCode,
	'WC000302' AS o_EndorsementNumber,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_25_Record.ExcludedWorkplace AS Value,
	-- *INF*: LTRIM(RTRIM(SUBSTR(Value,1,120)))
	LTRIM(RTRIM(SUBSTR(Value, 1, 120))) AS o_Value,
	WCTrackHistoryID AS v_CurrentRecord,
	-- *INF*: IIF(v_PrevRecord=v_CurrentRecord,v_RecordCount+1,1)
	IFF(v_PrevRecord = v_CurrentRecord, v_RecordCount + 1, 1) AS v_RecordCount,
	WCTrackHistoryID AS v_PrevRecord,
	-- *INF*: TO_CHAR(v_RecordCount)
	TO_CHAR(v_RecordCount) AS o_EndorsementSequenceNumber,
	JNR_25_Record.FormName,
	JNR_25_Record.Name,
	JNR_25_Record.TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate
	FROM JNR_25_Record
	 -- Manually join with mplt_Parse_FormNameField
),
WCPols25Record AS (
	INSERT INTO WCPols25Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, AddressNotCovered, EndorsementSequenceNumber, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	RECORDTYPECODE, 
	o_EndorsementNumber AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	o_Value AS ADDRESSNOTCOVERED, 
	o_EndorsementSequenceNumber AS ENDORSEMENTSEQUENCENUMBER, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_25_Record
),