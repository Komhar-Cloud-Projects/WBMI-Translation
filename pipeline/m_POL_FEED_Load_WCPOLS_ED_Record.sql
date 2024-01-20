WITH
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
SQ_WorkWCTrackHistory AS (
	SELECT 
	Track.WCTrackHistoryID as WCTrackHistoryID, 
	'WC480316' as ParsedFormName,
	Forms.FormName as FormName, 
	
	Details.value as EmployeeLeasingCompanyNameWC480316,
	Details1.value as ClientNameWC480316,
	Details2.value as TerminatedEffectiveDateWC480316,
	Details3.value as EntitiesWC480316,
	Details4.value as DateSentWC480316,
	
	Party.Name as Name,
	Policy.TransactionEffectiveDate as TransactionEffectiveDate  
	FROM
	WorkWCForms Forms
	inner join WorkWCTrackHistory Track 	on 
		Forms.WCTrackHistoryID=Track.WCTrackHistoryID and 
		Forms.FormName like 'WC480316%' and 
		((Forms.OnPolicy=1 OR Forms.[Add] = 1) AND (Forms.Remove is null OR Forms.Remove = 0))
	inner join WorkWCParty Party on 
		Party.WCTrackHistoryID=Track.WCTrackHistoryID AND 
		Party.PartyAssociationType='Account'
	inner join WorkWCPolicy Policy on 
		Policy.WCTrackHistoryID=Track.WCTrackHistoryID
	
	inner join WorkWCPolicyDetails Details on 
	      Policy.PolicyID=Details.PolicyID and Details.Attribute = 'EmployeeLeasingCompanyNameWC480316' AND
		Details.WCTrackHistoryID=Track.WCTrackHistoryID
	
	inner join WorkWCPolicyDetails Details1 on 
	      Policy.PolicyID=Details1.PolicyID and Details1.Attribute = 'ClientNameWC480316' AND
		Details1.WCTrackHistoryID=Track.WCTrackHistoryID
	
	inner join WorkWCPolicyDetails Details2 on 
	      Policy.PolicyID=Details2.PolicyID and Details2.Attribute = 'TerminatedEffectiveDateWC480316' AND
		Details2.WCTrackHistoryID=Track.WCTrackHistoryID
	
	inner join WorkWCPolicyDetails Details3 on 
	      Policy.PolicyID=Details3.PolicyID and Details3.Attribute ='EntitiesWC480316' AND
		Details3.WCTrackHistoryID=Track.WCTrackHistoryID
	
	inner join WorkWCPolicyDetails Details4 on 
	      Policy.PolicyID=Details4.PolicyID and Details4.Attribute ='DateSentWC480316' AND
		Details4.WCTrackHistoryID=Track.WCTrackHistoryID
	
	WHERE
	Forms.Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_ED}
	order by 1
),
JNR_DataCollect AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCTrackHistory.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCTrackHistory.ParsedFormName, 
	SQ_WorkWCTrackHistory.FormName, 
	SQ_WorkWCTrackHistory.EmployeeLeasingCompanyNameWC480316, 
	SQ_WorkWCTrackHistory.ClientNameWC480316, 
	SQ_WorkWCTrackHistory.TerminatedEffectiveDateWC480316, 
	SQ_WorkWCTrackHistory.EntitiesWC480316, 
	SQ_WorkWCTrackHistory.DateSentWC480316, 
	SQ_WorkWCTrackHistory.Name, 
	SQ_WorkWCTrackHistory.TransactionEffectiveDate
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WorkWCTrackHistory
	ON SQ_WorkWCTrackHistory.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
	JNR_DataCollect.WCTrackHistoryID,
	CURRENT_TIMESTAMP AS ExtractDate,
	'ED' AS RecordTypeCode,
	mplt_Parse_FormNameField.ParsedNameOfForm1,
	mplt_Parse_FormNameField.FormEdition,
	mplt_Parse_FormNameField.BureauCode,
	JNR_DataCollect.LinkData,
	JNR_DataCollect.AuditId,
	JNR_DataCollect.EmployeeLeasingCompanyNameWC480316,
	JNR_DataCollect.ClientNameWC480316,
	JNR_DataCollect.TerminatedEffectiveDateWC480316 AS i_TerminatedEffectiveDateWC480316,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_TerminatedEffectiveDateWC480316),'',
	-- TO_CHAR((TO_DATE(i_TerminatedEffectiveDateWC480316,'YYYYMMDD')),'YYMMDD')
	-- )
	DECODE(
	    TRUE,
	    i_TerminatedEffectiveDateWC480316 IS NULL, '',
	    TO_CHAR((TO_TIMESTAMP(i_TerminatedEffectiveDateWC480316, 'YYYYMMDD')), 'YYMMDD')
	) AS o_TerminatedEffectiveDateWC480316,
	JNR_DataCollect.EntitiesWC480316,
	JNR_DataCollect.DateSentWC480316 AS i_DateSentWC480316,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_DateSentWC480316),'',
	-- TO_CHAR((TO_DATE(i_DateSentWC480316,'YYYYMMDD')),'YYMMDD')
	-- )
	-- 
	-- 
	DECODE(
	    TRUE,
	    i_DateSentWC480316 IS NULL, '',
	    TO_CHAR((TO_TIMESTAMP(i_DateSentWC480316, 'YYYYMMDD')), 'YYMMDD')
	) AS o_DateSentWC480316,
	JNR_DataCollect.Name,
	JNR_DataCollect.TransactionEffectiveDate AS i_TransactionEffectiveDate,
	-- *INF*: TO_CHAR(i_TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(i_TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	'48' AS StateCode
	FROM JNR_DataCollect
	 -- Manually join with mplt_Parse_FormNameField
),
WCPolsEDRecord AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPolsEDRecord
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPolsEDRecord
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, NameOfEmployeeLeasingCompany, NameOfClient, TerminationEffectiveDate, EntitiesReceivingThisForm, DateSent, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	RECORDTYPECODE, 
	ParsedNameOfForm1 AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	EmployeeLeasingCompanyNameWC480316 AS NAMEOFEMPLOYEELEASINGCOMPANY, 
	ClientNameWC480316 AS NAMEOFCLIENT, 
	o_TerminatedEffectiveDateWC480316 AS TERMINATIONEFFECTIVEDATE, 
	EntitiesWC480316 AS ENTITIESRECEIVINGTHISFORM, 
	o_DateSentWC480316 AS DATESENT, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_PrepareOutput
),