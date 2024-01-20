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
SQ_WorkWCForms AS (
	SELECT
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,Lim.LimitValue
		,PD.Value
	    ,ROW_NUMBER() OVER(PARTITION BY ST.WCTrackHistoryID ORDER BY F.FormName DESC) AS EndorsementSeqNbr
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC000201%'
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCLimit Lim
		ON Lim.WCTrackHistoryID=ST.WCTrackHistoryID
			AND Lim.LimitType='AdmiraltyIncreased'
			AND Lim.CoverageType = 'AdmiraltyIncreasedLimits'
	
	INNER JOIN DBO.WorkWCPolicyDetails PD
		ON PD.WCTrackHistoryID=ST.WCTrackHistoryID
			AND PD.Attribute='MaritimeWorkDescription201A'
	
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_21}
	
	ORDER BY ST.WCTrackHistoryID
),
JNR_21_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCForms.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCForms.FormName, 
	SQ_WorkWCForms.Name, 
	SQ_WorkWCForms.TransactionEffectiveDate, 
	SQ_WorkWCForms.LimitValue, 
	SQ_WorkWCForms.Value, 
	SQ_WorkWCForms.EndorsementSeqNbr
	FROM SQ_WorkWCForms
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = SQ_WorkWCForms.WCTrackHistoryID
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
EXP_21_Format_Output AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_21_Record.AuditId,
	JNR_21_Record.WCTrackHistoryID,
	JNR_21_Record.LinkData,
	JNR_21_Record.FormName,
	'21' AS o_RecordTypeCode,
	'WC000201' AS o_EndorsementNumber,
	-- *INF*: SUBSTR(FormName, Length(FormName)-4, 1)
	SUBSTR(FormName, Length(FormName) - 4, 1) AS v_BureauID,
	-- *INF*: IIF(v_BureauID >= 'A' and v_BureauID <= 'Z', v_BureauID,' ')
	IFF(v_BureauID >= 'A' and v_BureauID <= 'Z', v_BureauID, ' ') AS o_BureauVersionIdentifierEditionIdentifier,
	-- *INF*: SUBSTR(FormName, Length(FormName)-3, 4)
	SUBSTR(FormName, Length(FormName) - 3, 4) AS o_CarrierVersionIdentifier,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_21_Record.Name AS NameOfInsured,
	JNR_21_Record.TransactionEffectiveDate,
	-- *INF*: To_Char(TransactionEffectiveDate, 'YYMMDD')
	To_Char(TransactionEffectiveDate, 'YYMMDD') AS o_EndorsementEffectiveDate,
	JNR_21_Record.LimitValue,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(LimitValue)),'NoCoverage','No Coverage'),'',
	-- ISNULL(LimitValue),'',
	-- TO_CHAR(TO_INTEGER(LimitValue)*1000))
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(LimitValue)) IN ('NoCoverage','No Coverage'), '',
	    LimitValue IS NULL, '',
	    TO_CHAR(CAST(LimitValue AS INTEGER) * 1000)
	) AS o_LimitValue,
	JNR_21_Record.Value,
	-- *INF*: IIF(ISNULL(Value),'',Value)
	IFF(Value IS NULL, '', Value) AS WorkDescription,
	JNR_21_Record.EndorsementSeqNbr
	FROM JNR_21_Record
	 -- Manually join with mplt_Parse_FormNameField
),
WCPols21Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols21Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols21Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, EmployerLiabilityLimitAmountMaritimeBodilyInjuryByAccidentAmount, EmployerLiabilityLimitAmountMaritimeBodilyInjuryByDiseaseAmount, WorkDescription, EndorsementSequenceNumber, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	o_RecordTypeCode AS RECORDTYPECODE, 
	o_EndorsementNumber AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	o_LimitValue AS EMPLOYERLIABILITYLIMITAMOUNTMARITIMEBODILYINJURYBYACCIDENTAMOUNT, 
	o_LimitValue AS EMPLOYERLIABILITYLIMITAMOUNTMARITIMEBODILYINJURYBYDISEASEAMOUNT, 
	WORKDESCRIPTION, 
	EndorsementSeqNbr AS ENDORSEMENTSEQUENCENUMBER, 
	NAMEOFINSURED, 
	o_EndorsementEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_21_Format_Output
),