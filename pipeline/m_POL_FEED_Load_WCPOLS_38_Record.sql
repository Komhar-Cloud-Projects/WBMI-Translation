WITH
SQ_WCPOLS_38_Record AS (
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,PDSP.Value as SoleProprietorExcluded
	    ,PDP.Value as NameOfPartnerExcluded
		,PDOFF.Value as NameOfOfficersExcluded
		,PDOTH.Value as NameOfOthersExcluded
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC000308%'AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	LEFT JOIN dbo.WorkWCPolicyDetails PDSP
		ON PDSP.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDSP.Attribute='SoleProprietorExcluded'
	
	LEFT JOIN dbo.WorkWCPolicyDetails PDP
		ON PDP.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDP.Attribute='NameOfPartnerExcluded'
		
	LEFT JOIN dbo.WorkWCPolicyDetails PDOFF
		ON PDOFF.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDOFF.Attribute='NameOfOfficersExcluded'
		
	LEFT JOIN dbo.WorkWCPolicyDetails PDOTH
		ON PDOTH.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND PDOTH.Attribute='NameOfOthersExcluded'
		
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	AND (PDSP.Value IS NOT NULL OR PDP.Value IS NOT NULL OR	PDOFF.Value IS NOT NULL OR PDOTH.Value IS NOT NULL)
	@{pipeline().parameters.WHERE_CLAUSE_38}
	
	ORDER BY ST.WCTrackHistoryID
),
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
JNR_38_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WCPOLS_38_Record.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WCPOLS_38_Record.FormName, 
	SQ_WCPOLS_38_Record.Name, 
	SQ_WCPOLS_38_Record.TransactionEffectiveDate, 
	SQ_WCPOLS_38_Record.SoleProprietorExcluded, 
	SQ_WCPOLS_38_Record.NameOfPartnerExcluded, 
	SQ_WCPOLS_38_Record.NameOfOfficersExcluded, 
	SQ_WCPOLS_38_Record.NameOfOthersExcluded
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WCPOLS_38_Record
	ON SQ_WCPOLS_38_Record.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
EXP_Counter AS (
	SELECT
	SYSDATE AS ExtractDate,
	JNR_38_Record.AuditId,
	JNR_38_Record.WCTrackHistoryID,
	JNR_38_Record.LinkData,
	'38' AS RecordTypeCode,
	'WC000308' AS EndorsementNumber,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_38_Record.Name,
	JNR_38_Record.TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS O_TransactionEffectiveDate,
	JNR_38_Record.SoleProprietorExcluded,
	-- *INF*: IIF(INSTR(SoleProprietorExcluded,'(',1,1)>0,
	-- SUBSTR(SoleProprietorExcluded,1,INSTR(SoleProprietorExcluded,'(',1,1)-1),SoleProprietorExcluded)
	-- 
	-- 
	IFF(
	    REGEXP_INSTR(SoleProprietorExcluded, '(', 1, 1) > 0,
	    SUBSTR(SoleProprietorExcluded, 1, REGEXP_INSTR(SoleProprietorExcluded, '(', 1, 1) - 1),
	    SoleProprietorExcluded
	) AS v_SoleProprietorExcluded,
	JNR_38_Record.NameOfPartnerExcluded,
	-- *INF*: IIF(INSTR(NameOfPartnerExcluded,'(',1,1)>0,
	-- SUBSTR(NameOfPartnerExcluded,1,INSTR(NameOfPartnerExcluded,'(',1,1)-1),NameOfPartnerExcluded)
	IFF(
	    REGEXP_INSTR(NameOfPartnerExcluded, '(', 1, 1) > 0,
	    SUBSTR(NameOfPartnerExcluded, 1, REGEXP_INSTR(NameOfPartnerExcluded, '(', 1, 1) - 1),
	    NameOfPartnerExcluded
	) AS v_NameOfPartnerExcluded,
	JNR_38_Record.NameOfOfficersExcluded,
	-- *INF*: IIF(INSTR(NameOfOfficersExcluded,'(',1,1)>0,
	-- SUBSTR(NameOfOfficersExcluded,1,INSTR(NameOfOfficersExcluded,'(',1,1)-1),NameOfOfficersExcluded)
	IFF(
	    REGEXP_INSTR(NameOfOfficersExcluded, '(', 1, 1) > 0,
	    SUBSTR(NameOfOfficersExcluded, 1, REGEXP_INSTR(NameOfOfficersExcluded, '(', 1, 1) - 1),
	    NameOfOfficersExcluded
	) AS v_NameOfOfficersExcluded,
	JNR_38_Record.NameOfOthersExcluded,
	-- *INF*: IIF(INSTR(NameOfOthersExcluded,'(',1,1)>0,
	-- SUBSTR(NameOfOthersExcluded,1,INSTR(NameOfOthersExcluded,'(',1,1)-1),NameOfOthersExcluded)
	IFF(
	    REGEXP_INSTR(NameOfOthersExcluded, '(', 1, 1) > 0,
	    SUBSTR(NameOfOthersExcluded, 1, REGEXP_INSTR(NameOfOthersExcluded, '(', 1, 1) - 1),
	    NameOfOthersExcluded
	) AS v_NameOfOthersExcluded,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(SoleProprietorExcluded),'S',
	-- NOT ISNULL(NameOfPartnerExcluded),'P',
	-- NOT ISNULL(NameOfOfficersExcluded),'O',
	-- NOT ISNULL(NameOfOthersExcluded),'X',
	-- '')
	DECODE(
	    TRUE,
	    SoleProprietorExcluded IS NOT NULL, 'S',
	    NameOfPartnerExcluded IS NOT NULL, 'P',
	    NameOfOfficersExcluded IS NOT NULL, 'O',
	    NameOfOthersExcluded IS NOT NULL, 'X',
	    ''
	) AS DescriptorCode,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(SoleProprietorExcluded),v_SoleProprietorExcluded,
	-- NOT ISNULL(NameOfPartnerExcluded),v_NameOfPartnerExcluded,
	-- NOT ISNULL(NameOfOfficersExcluded),v_NameOfOfficersExcluded,
	-- NOT ISNULL(NameOfOthersExcluded),v_NameOfOthersExcluded,
	-- '')
	DECODE(
	    TRUE,
	    SoleProprietorExcluded IS NOT NULL, v_SoleProprietorExcluded,
	    NameOfPartnerExcluded IS NOT NULL, v_NameOfPartnerExcluded,
	    NameOfOfficersExcluded IS NOT NULL, v_NameOfOfficersExcluded,
	    NameOfOthersExcluded IS NOT NULL, v_NameOfOthersExcluded,
	    ''
	) AS NameOfPersonToBeExcluded,
	WCTrackHistoryID AS v_CurrentRecord,
	-- *INF*: IIF(v_CurrentRecord<>v_PreviousRecord,0,1)
	IFF(v_CurrentRecord <> v_PreviousRecord, 0, 1) AS ChangeFlag,
	WCTrackHistoryID AS v_PreviousRecord,
	-- *INF*: IIF(ChangeFlag=1,v_Counter+1,1)
	IFF(ChangeFlag = 1, v_Counter + 1, 1) AS v_Counter,
	-- *INF*: MOD(v_Counter,3)
	MOD(v_Counter, 3) AS Split
	FROM JNR_38_Record
	 -- Manually join with mplt_Parse_FormNameField
),
EXP_Split AS (
	SELECT
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	RecordTypeCode,
	EndorsementNumber,
	BureauCode,
	FormEdition,
	Name,
	O_TransactionEffectiveDate AS TransactionEffectiveDate,
	DescriptorCode,
	NameOfPersonToBeExcluded,
	Split,
	-- *INF*: IIF(Split=1,DescriptorCode,'')
	IFF(Split = 1, DescriptorCode, '') AS O_DescriptorCode,
	-- *INF*: IIF(Split=1,NameOfPersonToBeExcluded,'')
	IFF(Split = 1, NameOfPersonToBeExcluded, '') AS O_NameOfPersonToBeExcluded,
	-- *INF*: IIF(Split=2,DescriptorCode,'')
	IFF(Split = 2, DescriptorCode, '') AS O_DescriptorCode2,
	-- *INF*: IIF(Split=2,NameOfPersonToBeExcluded,'')
	IFF(Split = 2, NameOfPersonToBeExcluded, '') AS O_NameOfPersonToBeExcluded2,
	-- *INF*: IIF(Split=0,DescriptorCode,'')
	IFF(Split = 0, DescriptorCode, '') AS O_DescriptorCode3,
	-- *INF*: IIF(Split=0,NameOfPersonToBeExcluded,'')
	IFF(Split = 0, NameOfPersonToBeExcluded, '') AS O_NameOfPersonToBeExcluded3,
	WCTrackHistoryID AS v_CurrentGroup,
	-- *INF*: DECODE(TRUE,
	-- v_CurrentGroup<>v_PreviousGroup,1,
	-- v_CurrentGroup=v_PreviousGroup AND Split=1,v_GroupKey+1,
	-- v_GroupKey)
	DECODE(
	    TRUE,
	    v_CurrentGroup <> v_PreviousGroup, 1,
	    v_CurrentGroup = v_PreviousGroup AND Split = 1, v_GroupKey + 1,
	    v_GroupKey
	) AS v_GroupKey,
	WCTrackHistoryID AS v_PreviousGroup,
	v_GroupKey AS GroupKey
	FROM EXP_Counter
),
AGG_GroupData AS (
	SELECT
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	RecordTypeCode,
	EndorsementNumber,
	BureauCode,
	FormEdition,
	O_DescriptorCode AS IN_DescriptorCode,
	O_NameOfPersonToBeExcluded AS IN_NameOfPersonToBeExcluded,
	O_DescriptorCode2 AS IN_DescriptorCode2,
	O_NameOfPersonToBeExcluded2 AS IN_NameOfPersonToBeExcluded2,
	O_DescriptorCode3 AS IN_DescriptorCode3,
	O_NameOfPersonToBeExcluded3 AS IN_NameOfPersonToBeExcluded3,
	GroupKey,
	-- *INF*: MAX(IN_DescriptorCode)
	MAX(IN_DescriptorCode) AS DescriptorCode,
	-- *INF*: MAX(IN_NameOfPersonToBeExcluded)
	MAX(IN_NameOfPersonToBeExcluded) AS NameOfPersonToBeExcluded,
	-- *INF*: MAX(IN_DescriptorCode2)
	MAX(IN_DescriptorCode2) AS DescriptorCode2,
	-- *INF*: MAX(IN_NameOfPersonToBeExcluded2)
	MAX(IN_NameOfPersonToBeExcluded2) AS NameOfPersonToBeExcluded2,
	-- *INF*: MAX(IN_DescriptorCode3)
	MAX(IN_DescriptorCode3) AS DescriptorCode3,
	-- *INF*: MAX(IN_NameOfPersonToBeExcluded3)
	MAX(IN_NameOfPersonToBeExcluded3) AS NameOfPersonToBeExcluded3,
	Name,
	TransactionEffectiveDate
	FROM EXP_Split
	GROUP BY WCTrackHistoryID, GroupKey
),
EXP_Target AS (
	SELECT
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	RecordTypeCode,
	EndorsementNumber,
	BureauCode,
	FormEdition,
	DescriptorCode,
	NameOfPersonToBeExcluded,
	DescriptorCode2,
	NameOfPersonToBeExcluded2,
	DescriptorCode3,
	NameOfPersonToBeExcluded3,
	Name,
	TransactionEffectiveDate
	FROM AGG_GroupData
),
WCPols38Record AS (
	INSERT INTO WCPols38Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, DescriptorCode, NameOfPersonToBeExcluded, DescriptorCode2, NameOfPersonToBeExcluded2, DescriptorCode3, NameOfPersonToBeExcluded3, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	RECORDTYPECODE, 
	ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	DESCRIPTORCODE, 
	NAMEOFPERSONTOBEEXCLUDED, 
	DESCRIPTORCODE2, 
	NAMEOFPERSONTOBEEXCLUDED2, 
	DESCRIPTORCODE3, 
	NAMEOFPERSONTOBEEXCLUDED3, 
	Name AS NAMEOFINSURED, 
	TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_Target
),