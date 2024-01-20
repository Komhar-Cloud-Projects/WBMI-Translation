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
SQ_WorkWCForms_Record36 AS (
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,L.AnniversaryRatingDate
		,PT.Name
		,Pol.TransactionEffectiveDate
		,Det.Value as AlternateEmployerWaiverDescription
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC000313%'
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCLine L
		ON L.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicyDetails Det
		ON Pol.WCTrackHistoryID =  Det.WCTrackHistoryID
		AND Pol.PolicyId = Det.PolicyId
		AND Det.Attribute = 'AlternateEmployerWaiverDescription'
	
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	AND Det.Value IS NOT NULL
	@{pipeline().parameters.WHERE_CLAUSE_36}
	
	ORDER BY ST.WCTrackHistoryID
),
JNR_Record36 AS (SELECT
	SQ_WCPols00Record.AuditId, 
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WorkWCForms_Record36.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCForms_Record36.FormName, 
	SQ_WorkWCForms_Record36.AnniversaryRatingDate, 
	SQ_WorkWCForms_Record36.Name, 
	SQ_WorkWCForms_Record36.TransactionEffectiveDate, 
	SQ_WorkWCForms_Record36.AlternateEmployerWaiverDescription
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WorkWCForms_Record36
	ON SQ_WorkWCForms_Record36.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
EXP_Record36 AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_Record36.AuditId,
	JNR_Record36.WCTrackHistoryID,
	JNR_Record36.LinkData,
	'36' AS RecordTypeCode,
	JNR_Record36.FormName,
	JNR_Record36.AnniversaryRatingDate,
	mplt_Parse_FormNameField.ParsedNameOfForm1,
	mplt_Parse_FormNameField.FormNameFromSource1,
	'WC000313' AS o_EndorsementNumber,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_Record36.Name,
	JNR_Record36.TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	JNR_Record36.AlternateEmployerWaiverDescription
	FROM JNR_Record36
	 -- Manually join with mplt_Parse_FormNameField
),
WCPols36Record AS (
	INSERT INTO WCPols36Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, NameOfOrganization, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	RECORDTYPECODE, 
	o_EndorsementNumber AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	AlternateEmployerWaiverDescription AS NAMEOFORGANIZATION, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_Record36
),