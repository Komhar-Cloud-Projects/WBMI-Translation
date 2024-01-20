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
SQ_WorkWC_23_Record AS (
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,PDVName.Value as VesselName
	    ,PDWC.Value as WorkersCompensationLaw
		,PDMWC.Value as MaritimeWorkDescriptionWC0002030484
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC000203%'AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCLine L
		ON L.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCTrackHistory TH
		ON TH.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	INNER JOIN dbo.WorkWCPolicyDetails PDVName
		ON PDVName.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDVName.Attribute='VesselName'
	
	INNER JOIN dbo.WorkWCPolicyDetails PDWC
		ON PDWC.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDWC.Attribute='WorkersCompensationLaw'
		AND PDWC.ProcessID=PDVName.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails PDMWC
		ON PDMWC.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDMWC.Attribute='MaritimeWorkDescriptionWC0002030484'
		AND PDMWC.ProcessID=PDVName.ProcessID
	
	WHERE 1 = 1
	AND PDVName.Value IS NOT NULL
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_23}
	
	ORDER BY ST.WCTrackHistoryID
),
JNR_23_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.AuditId, 
	SQ_WCPols00Record.LinkData, 
	SQ_WorkWC_23_Record.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWC_23_Record.FormName, 
	SQ_WorkWC_23_Record.Name, 
	SQ_WorkWC_23_Record.TransactionEffectiveDate, 
	SQ_WorkWC_23_Record.VesselName, 
	SQ_WorkWC_23_Record.WorkersCompensationLaw, 
	SQ_WorkWC_23_Record.MaritimeWorkDescriptionWC0002030484
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WorkWC_23_Record
	ON SQ_WorkWC_23_Record.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
EXP_23_Record AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_23_Record.AuditId,
	JNR_23_Record.WCTrackHistoryID,
	JNR_23_Record.LinkData,
	'23' AS o_RecordTypeCode,
	'WC000203' AS o_EndorsementNumber,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_23_Record.VesselName,
	JNR_23_Record.WorkersCompensationLaw,
	JNR_23_Record.MaritimeWorkDescriptionWC0002030484,
	WCTrackHistoryID AS v_CurrentRecord,
	-- *INF*: IIF(v_CurrentRecord=v_PreviousRecord,v_RecordCount+1, 1)
	IFF(v_CurrentRecord = v_PreviousRecord, v_RecordCount + 1, 1) AS v_RecordCount,
	WCTrackHistoryID AS v_PreviousRecord,
	v_RecordCount AS o_RecordCount,
	JNR_23_Record.Name,
	JNR_23_Record.TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate
	FROM JNR_23_Record
	 -- Manually join with mplt_Parse_FormNameField
),
WCPols23Record AS (
	INSERT INTO WCPols23Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, NameOfVessel, WorkersCompensationLaw, DescriptionOfWork, EndorsementSequenceNumber, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	o_RecordTypeCode AS RECORDTYPECODE, 
	o_EndorsementNumber AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	VesselName AS NAMEOFVESSEL, 
	WORKERSCOMPENSATIONLAW, 
	MaritimeWorkDescriptionWC0002030484 AS DESCRIPTIONOFWORK, 
	o_RecordCount AS ENDORSEMENTSEQUENCENUMBER, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_23_Record
),