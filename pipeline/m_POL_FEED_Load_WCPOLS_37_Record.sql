WITH
LKP_SupWCPOLS AS (
	SELECT
	WCPOLSCode,
	SourcesystemID,
	SourceCode,
	TableName,
	ProcessName,
	i_SourcesystemID,
	i_SourceCode,
	i_TableName,
	i_ProcessName
	FROM (
		SELECT
		     WCPOLSCode as WCPOLSCode
			,SourcesystemID as SourcesystemID
			,SourceCode as SourceCode
			,TableName as TableName
			,ProcessName as ProcessName
		FROM SupWCPOLS
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourcesystemID,SourceCode,TableName,ProcessName ORDER BY WCPOLSCode) = 1
),
SQ_WCPOLS_37_Record AS (
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,PDSP.NameOfSoleProprietorIncluded
		,PDSP.IncludedSolePropState
	    ,PDP.NameOfPartnersIncluded
		,PDP.IncludedPartnersState
		,PDOFF.NameOfOfficersIncluded
		,PDOFF.IncludedOfficersState
		,PDOTH.NameOfOthersIncluded
		,PDOTH.IncludedOthersState
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC000310%'AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	LEFT JOIN 
	(Select SPName.WCTrackHistoryID,SPName.Value as NameOfSoleProprietorIncluded,SPState.Value as IncludedSolePropState,SPName.ProcessID from 
	dbo.WorkWCPolicyDetails SPName
	INNER JOIN WorkWCPolicyDetails SPState
		ON SPName.WCTrackHistoryID = SPState.WCTrackHistoryID 
		AND SPName.ProcessID=SPState.ProcessID
		AND SPName.PolicyID=SPState.PolicyID
		AND SPName.Attribute='NameOfSoleProprietorIncluded'
		AND SPState.Attribute='IncludedSolePropState'
	)PDSP
	ON PDSP.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	LEFT JOIN 
	(Select PName.WCTrackHistoryID,PName.Value as NameOfPartnersIncluded,PState.Value as IncludedPartnersState,PName.ProcessID from 
	dbo.WorkWCPolicyDetails PName
	INNER JOIN WorkWCPolicyDetails PState
		ON PName.WCTrackHistoryID = PState.WCTrackHistoryID 
		AND PName.ProcessID=PState.ProcessID
		AND PName.PolicyID=PState.PolicyID
		AND PName.Attribute='NameOfPartnersIncluded'
		AND PState.Attribute='IncludedPartnersState'
	)PDP
	ON PDP.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	LEFT JOIN 
	(Select OFFName.WCTrackHistoryID,OFFName.Value as NameOfOfficersIncluded,OFFState.Value as IncludedOfficersState,OFFName.ProcessID from 
	dbo.WorkWCPolicyDetails OFFName
	INNER JOIN WorkWCPolicyDetails OFFState
		ON OFFName.WCTrackHistoryID = OFFState.WCTrackHistoryID 
		AND OFFName.ProcessID=OFFState.ProcessID
		AND OFFName.PolicyID=OFFState.PolicyID
		AND OFFName.Attribute='NameOfOfficersIncluded'
		AND OFFState.Attribute='IncludedOfficersState'
	)PDOFF
	ON PDOFF.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	LEFT JOIN 
	(Select OTHName.WCTrackHistoryID,OTHName.Value as NameOfOthersIncluded,OTHState.Value as IncludedOthersState,OTHName.ProcessID from 
	dbo.WorkWCPolicyDetails OTHName
	INNER JOIN WorkWCPolicyDetails OTHState
		ON OTHName.WCTrackHistoryID = OTHState.WCTrackHistoryID 
		AND OTHName.ProcessID=OTHState.ProcessID
		AND OTHName.PolicyID=OTHState.PolicyID
		AND OTHName.Attribute='NameOfOthersIncluded'
		AND OTHState.Attribute='IncludedOthersState'
	)PDOTH
	ON PDOTH.WCTrackHistoryID = ST.WCTrackHistoryID 
	
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	AND (PDSP.NameOfSoleProprietorIncluded IS NOT NULL OR PDP.NameOfPartnersIncluded IS NOT NULL 
	OR PDOFF.NameOfOfficersIncluded IS NOT NULL OR PDOTH.NameOfOthersIncluded IS NOT NULL)
	@{pipeline().parameters.WHERE_CLAUSE_37}
	
	ORDER BY ST.WCTrackHistoryID
),
EXP_StateLogic AS (
	SELECT
	WCTrackHistoryID,
	FormName,
	Name,
	TransactionEffectiveDate,
	NameOfSoleProprietorIncluded,
	IncludedSolePropState AS IN_IncludedSolePropState,
	-- *INF*:  IIF(IsNull(:LKP.LKP_SupWCPOLS('DCT',IN_IncludedSolePropState,'WCPOLS37Record','StateCodeRecord37')),'',:LKP.LKP_SupWCPOLS('DCT',IN_IncludedSolePropState,'WCPOLS37Record','StateCodeRecord37'))
	IFF(
	    LKP_SUPWCPOLS__DCT_IN_IncludedSolePropState_WCPOLS37Record_StateCodeRecord37.WCPOLSCode IS NULL,
	    '',
	    LKP_SUPWCPOLS__DCT_IN_IncludedSolePropState_WCPOLS37Record_StateCodeRecord37.WCPOLSCode
	) AS O_IncludedSolePropState,
	NameOfPartnersIncluded,
	IncludedPartnersState AS IN_IncludedPartnersState,
	-- *INF*:  IIF(IsNull(:LKP.LKP_SupWCPOLS('DCT',IN_IncludedPartnersState,'WCPOLS37Record','StateCodeRecord37')),'',:LKP.LKP_SupWCPOLS('DCT',IN_IncludedPartnersState,'WCPOLS37Record','StateCodeRecord37'))
	IFF(
	    LKP_SUPWCPOLS__DCT_IN_IncludedPartnersState_WCPOLS37Record_StateCodeRecord37.WCPOLSCode IS NULL,
	    '',
	    LKP_SUPWCPOLS__DCT_IN_IncludedPartnersState_WCPOLS37Record_StateCodeRecord37.WCPOLSCode
	) AS O_IncludedPartnersState,
	NameOfOfficersIncluded,
	IncludedOfficersState AS IN_IncludedOfficersState,
	-- *INF*:  IIF(IsNull(:LKP.LKP_SupWCPOLS('DCT',IN_IncludedOfficersState,'WCPOLS37Record','StateCodeRecord37')),'',:LKP.LKP_SupWCPOLS('DCT',IN_IncludedOfficersState,'WCPOLS37Record','StateCodeRecord37'))
	IFF(
	    LKP_SUPWCPOLS__DCT_IN_IncludedOfficersState_WCPOLS37Record_StateCodeRecord37.WCPOLSCode IS NULL,
	    '',
	    LKP_SUPWCPOLS__DCT_IN_IncludedOfficersState_WCPOLS37Record_StateCodeRecord37.WCPOLSCode
	) AS O_IncludedOfficersState,
	NameOfOthersIncluded,
	IncludedOthersState AS IN_IncludedOthersState,
	-- *INF*:  IIF(IsNull(:LKP.LKP_SupWCPOLS('DCT',IN_IncludedOthersState,'WCPOLS37Record','StateCodeRecord37')),'',:LKP.LKP_SupWCPOLS('DCT',IN_IncludedOthersState,'WCPOLS37Record','StateCodeRecord37'))
	IFF(
	    LKP_SUPWCPOLS__DCT_IN_IncludedOthersState_WCPOLS37Record_StateCodeRecord37.WCPOLSCode IS NULL,
	    '',
	    LKP_SUPWCPOLS__DCT_IN_IncludedOthersState_WCPOLS37Record_StateCodeRecord37.WCPOLSCode
	) AS O_IncludedOthersState
	FROM SQ_WCPOLS_37_Record
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_IN_IncludedSolePropState_WCPOLS37Record_StateCodeRecord37
	ON LKP_SUPWCPOLS__DCT_IN_IncludedSolePropState_WCPOLS37Record_StateCodeRecord37.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_IN_IncludedSolePropState_WCPOLS37Record_StateCodeRecord37.SourceCode = IN_IncludedSolePropState
	AND LKP_SUPWCPOLS__DCT_IN_IncludedSolePropState_WCPOLS37Record_StateCodeRecord37.TableName = 'WCPOLS37Record'
	AND LKP_SUPWCPOLS__DCT_IN_IncludedSolePropState_WCPOLS37Record_StateCodeRecord37.ProcessName = 'StateCodeRecord37'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_IN_IncludedPartnersState_WCPOLS37Record_StateCodeRecord37
	ON LKP_SUPWCPOLS__DCT_IN_IncludedPartnersState_WCPOLS37Record_StateCodeRecord37.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_IN_IncludedPartnersState_WCPOLS37Record_StateCodeRecord37.SourceCode = IN_IncludedPartnersState
	AND LKP_SUPWCPOLS__DCT_IN_IncludedPartnersState_WCPOLS37Record_StateCodeRecord37.TableName = 'WCPOLS37Record'
	AND LKP_SUPWCPOLS__DCT_IN_IncludedPartnersState_WCPOLS37Record_StateCodeRecord37.ProcessName = 'StateCodeRecord37'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_IN_IncludedOfficersState_WCPOLS37Record_StateCodeRecord37
	ON LKP_SUPWCPOLS__DCT_IN_IncludedOfficersState_WCPOLS37Record_StateCodeRecord37.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_IN_IncludedOfficersState_WCPOLS37Record_StateCodeRecord37.SourceCode = IN_IncludedOfficersState
	AND LKP_SUPWCPOLS__DCT_IN_IncludedOfficersState_WCPOLS37Record_StateCodeRecord37.TableName = 'WCPOLS37Record'
	AND LKP_SUPWCPOLS__DCT_IN_IncludedOfficersState_WCPOLS37Record_StateCodeRecord37.ProcessName = 'StateCodeRecord37'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_IN_IncludedOthersState_WCPOLS37Record_StateCodeRecord37
	ON LKP_SUPWCPOLS__DCT_IN_IncludedOthersState_WCPOLS37Record_StateCodeRecord37.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_IN_IncludedOthersState_WCPOLS37Record_StateCodeRecord37.SourceCode = IN_IncludedOthersState
	AND LKP_SUPWCPOLS__DCT_IN_IncludedOthersState_WCPOLS37Record_StateCodeRecord37.TableName = 'WCPOLS37Record'
	AND LKP_SUPWCPOLS__DCT_IN_IncludedOthersState_WCPOLS37Record_StateCodeRecord37.ProcessName = 'StateCodeRecord37'

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
JNR_WCPols_37_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.AuditId, 
	SQ_WCPols00Record.LinkData, 
	EXP_StateLogic.WCTrackHistoryID AS WCTrackHistoryID1, 
	EXP_StateLogic.FormName, 
	EXP_StateLogic.Name, 
	EXP_StateLogic.TransactionEffectiveDate, 
	EXP_StateLogic.NameOfSoleProprietorIncluded, 
	EXP_StateLogic.O_IncludedSolePropState AS IncludedSolePropState, 
	EXP_StateLogic.NameOfPartnersIncluded, 
	EXP_StateLogic.O_IncludedPartnersState AS IncludedPartnersState, 
	EXP_StateLogic.NameOfOfficersIncluded, 
	EXP_StateLogic.O_IncludedOfficersState AS IncludedOfficersState, 
	EXP_StateLogic.NameOfOthersIncluded, 
	EXP_StateLogic.O_IncludedOthersState AS IncludedOthersState
	FROM SQ_WCPols00Record
	INNER JOIN EXP_StateLogic
	ON EXP_StateLogic.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
	JNR_WCPols_37_Record.AuditId,
	JNR_WCPols_37_Record.WCTrackHistoryID,
	JNR_WCPols_37_Record.LinkData,
	'37' AS RecordTypeCode,
	'WC000310' AS EndorsementNumber,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_WCPols_37_Record.Name,
	JNR_WCPols_37_Record.TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	-- 
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	JNR_WCPols_37_Record.NameOfSoleProprietorIncluded,
	-- *INF*: IIF(INSTR(NameOfSoleProprietorIncluded,'(',1,1)>0,
	-- SUBSTR(NameOfSoleProprietorIncluded,1,INSTR(NameOfSoleProprietorIncluded,'(',1,1)-1),NameOfSoleProprietorIncluded)
	IFF(
	    REGEXP_INSTR(NameOfSoleProprietorIncluded, '(', 1, 1) > 0,
	    SUBSTR(NameOfSoleProprietorIncluded, 1, REGEXP_INSTR(NameOfSoleProprietorIncluded, '(', 1, 1) - 1),
	    NameOfSoleProprietorIncluded
	) AS v_NameOfSoleProprietorIncluded,
	JNR_WCPols_37_Record.IncludedSolePropState,
	JNR_WCPols_37_Record.NameOfPartnersIncluded,
	-- *INF*: IIF(INSTR(NameOfPartnersIncluded,'(',1,1)>0,
	-- SUBSTR(NameOfPartnersIncluded,1,INSTR(NameOfPartnersIncluded,'(',1,1)-1),NameOfPartnersIncluded)
	IFF(
	    REGEXP_INSTR(NameOfPartnersIncluded, '(', 1, 1) > 0,
	    SUBSTR(NameOfPartnersIncluded, 1, REGEXP_INSTR(NameOfPartnersIncluded, '(', 1, 1) - 1),
	    NameOfPartnersIncluded
	) AS v_NameOfPartnersIncluded,
	JNR_WCPols_37_Record.IncludedPartnersState,
	JNR_WCPols_37_Record.NameOfOfficersIncluded,
	-- *INF*: IIF(INSTR(NameOfOfficersIncluded,'(',1,1)>0,
	-- SUBSTR(NameOfOfficersIncluded,1,INSTR(NameOfOfficersIncluded,'(',1,1)-1),NameOfOfficersIncluded)
	IFF(
	    REGEXP_INSTR(NameOfOfficersIncluded, '(', 1, 1) > 0,
	    SUBSTR(NameOfOfficersIncluded, 1, REGEXP_INSTR(NameOfOfficersIncluded, '(', 1, 1) - 1),
	    NameOfOfficersIncluded
	) AS v_NameOfOfficersIncluded,
	JNR_WCPols_37_Record.IncludedOfficersState,
	JNR_WCPols_37_Record.NameOfOthersIncluded,
	-- *INF*: IIF(INSTR(NameOfOthersIncluded,'(',1,1)>0,
	-- SUBSTR(NameOfOthersIncluded,1,INSTR(NameOfOthersIncluded,'(',1,1)-1),NameOfOthersIncluded)
	IFF(
	    REGEXP_INSTR(NameOfOthersIncluded, '(', 1, 1) > 0,
	    SUBSTR(NameOfOthersIncluded, 1, REGEXP_INSTR(NameOfOthersIncluded, '(', 1, 1) - 1),
	    NameOfOthersIncluded
	) AS v_NameOfOthersIncluded,
	JNR_WCPols_37_Record.IncludedOthersState,
	-- *INF*: DECODE(TRUE,
	-- ((NOT ISNULL(NameOfSoleProprietorIncluded) AND LTRIM(RTRIM(NameOfSoleProprietorIncluded))<>'') OR (NOT ISNULL(IncludedSolePropState) AND LTRIM(RTRIM(IncludedSolePropState))<>'')),'S',
	-- ((NOT ISNULL(NameOfPartnersIncluded) AND LTRIM(RTRIM(NameOfPartnersIncluded))<>'') OR (NOT ISNULL(IncludedPartnersState) AND LTRIM(RTRIM(IncludedPartnersState))<>'')),'P',
	-- ((NOT ISNULL(NameOfOfficersIncluded) AND LTRIM(RTRIM(NameOfOfficersIncluded))<>'') OR (NOT ISNULL(IncludedOfficersState) AND LTRIM(RTRIM(IncludedOfficersState))<>'')),'O',
	-- ((NOT ISNULL(NameOfOthersIncluded) AND LTRIM(RTRIM(NameOfOthersIncluded))<>'') OR (NOT ISNULL(IncludedOthersState) AND LTRIM(RTRIM(IncludedOthersState))<>'')),'X',
	-- '')
	DECODE(
	    TRUE,
	    ((NameOfSoleProprietorIncluded IS NULL AND LTRIM(RTRIM(NameOfSoleProprietorIncluded)) <NOT > '') OR (IncludedSolePropState IS NULL AND LTRIM(RTRIM(IncludedSolePropState)) <NOT > '')), 'S',
	    ((NameOfPartnersIncluded IS NULL AND LTRIM(RTRIM(NameOfPartnersIncluded)) <NOT > '') OR (IncludedPartnersState IS NULL AND LTRIM(RTRIM(IncludedPartnersState)) <NOT > '')), 'P',
	    ((NameOfOfficersIncluded IS NULL AND LTRIM(RTRIM(NameOfOfficersIncluded)) <NOT > '') OR (IncludedOfficersState IS NULL AND LTRIM(RTRIM(IncludedOfficersState)) <NOT > '')), 'O',
	    ((NameOfOthersIncluded IS NULL AND LTRIM(RTRIM(NameOfOthersIncluded)) <NOT > '') OR (IncludedOthersState IS NULL AND LTRIM(RTRIM(IncludedOthersState)) <NOT > '')), 'X',
	    ''
	) AS DescriptorCode,
	-- *INF*: DECODE(TRUE,
	-- (NOT ISNULL(v_NameOfSoleProprietorIncluded) AND LTRIM(RTRIM(v_NameOfSoleProprietorIncluded))<>'') ,LTRIM(RTRIM(v_NameOfSoleProprietorIncluded)),
	-- (NOT ISNULL(v_NameOfPartnersIncluded) AND LTRIM(RTRIM(v_NameOfPartnersIncluded))<>'') ,LTRIM(RTRIM(v_NameOfPartnersIncluded)),
	-- (NOT ISNULL(v_NameOfOfficersIncluded) AND LTRIM(RTRIM(v_NameOfOfficersIncluded))<>'') ,LTRIM(RTRIM(v_NameOfOfficersIncluded)),
	-- (NOT ISNULL(v_NameOfOthersIncluded) AND LTRIM(RTRIM(v_NameOfOthersIncluded))<>'') ,LTRIM(RTRIM(v_NameOfOthersIncluded)),
	-- '')
	-- 
	-- 
	DECODE(
	    TRUE,
	    (v_NameOfSoleProprietorIncluded IS NULL AND LTRIM(RTRIM(v_NameOfSoleProprietorIncluded)) <NOT > ''), LTRIM(RTRIM(v_NameOfSoleProprietorIncluded)),
	    (v_NameOfPartnersIncluded IS NULL AND LTRIM(RTRIM(v_NameOfPartnersIncluded)) <NOT > ''), LTRIM(RTRIM(v_NameOfPartnersIncluded)),
	    (v_NameOfOfficersIncluded IS NULL AND LTRIM(RTRIM(v_NameOfOfficersIncluded)) <NOT > ''), LTRIM(RTRIM(v_NameOfOfficersIncluded)),
	    (v_NameOfOthersIncluded IS NULL AND LTRIM(RTRIM(v_NameOfOthersIncluded)) <NOT > ''), LTRIM(RTRIM(v_NameOfOthersIncluded)),
	    ''
	) AS NameOfPersonToBeIncluded,
	-- *INF*: DECODE(TRUE,
	-- (NOT ISNULL(IncludedSolePropState) AND LTRIM(RTRIM(IncludedSolePropState))<>''),LTRIM(RTRIM(IncludedSolePropState)),
	-- (NOT ISNULL(IncludedPartnersState) AND LTRIM(RTRIM(IncludedPartnersState))<>''),LTRIM(RTRIM(IncludedPartnersState)),
	-- (NOT ISNULL(IncludedOfficersState) AND LTRIM(RTRIM(IncludedOfficersState))<>''),LTRIM(RTRIM(IncludedOfficersState)),
	-- (NOT ISNULL(IncludedOthersState) AND LTRIM(RTRIM(IncludedOthersState))<>''),LTRIM(RTRIM(IncludedOthersState)),
	-- '')
	DECODE(
	    TRUE,
	    (IncludedSolePropState IS NULL AND LTRIM(RTRIM(IncludedSolePropState)) <NOT > ''), LTRIM(RTRIM(IncludedSolePropState)),
	    (IncludedPartnersState IS NULL AND LTRIM(RTRIM(IncludedPartnersState)) <NOT > ''), LTRIM(RTRIM(IncludedPartnersState)),
	    (IncludedOfficersState IS NULL AND LTRIM(RTRIM(IncludedOfficersState)) <NOT > ''), LTRIM(RTRIM(IncludedOfficersState)),
	    (IncludedOthersState IS NULL AND LTRIM(RTRIM(IncludedOthersState)) <NOT > ''), LTRIM(RTRIM(IncludedOthersState)),
	    ''
	) AS StateCode,
	WCTrackHistoryID AS v_CurrentRecord,
	-- *INF*: IIF(v_CurrentRecord<>v_PreviousRecord,0,1)
	IFF(v_CurrentRecord <> v_PreviousRecord, 0, 1) AS v_ChangeFlag,
	WCTrackHistoryID AS v_PreviousRecord,
	-- *INF*: IIF(v_ChangeFlag=1,v_Counter+1,1)
	IFF(v_ChangeFlag = 1, v_Counter + 1, 1) AS v_Counter,
	-- *INF*: MOD(v_Counter,3)
	MOD(v_Counter, 3) AS Split,
	-- *INF*: CEIL(v_Counter/3)
	CEIL(v_Counter / 3) AS GroupKey
	FROM JNR_WCPols_37_Record
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
	o_TransactionEffectiveDate AS TransactionEffectiveDate,
	DescriptorCode,
	NameOfPersonToBeIncluded,
	StateCode,
	Split,
	GroupKey,
	-- *INF*: IIF(Split=1,DescriptorCode,'')
	IFF(Split = 1, DescriptorCode, '') AS O_DescriptorCode,
	-- *INF*: IIF(Split=1,NameOfPersonToBeIncluded,'')
	IFF(Split = 1, NameOfPersonToBeIncluded, '') AS O_NameOfPersonToBeIncluded,
	-- *INF*: IIF(Split=1,StateCode,'')
	IFF(Split = 1, StateCode, '') AS O_StateCode,
	-- *INF*: IIF(Split=2,DescriptorCode,'')
	IFF(Split = 2, DescriptorCode, '') AS O_DescriptorCode2,
	-- *INF*: IIF(Split=2,NameOfPersonToBeIncluded,'')
	IFF(Split = 2, NameOfPersonToBeIncluded, '') AS O_NameOfPersonToBeIncluded2,
	-- *INF*: IIF(Split=2,StateCode,'')
	IFF(Split = 2, StateCode, '') AS O_StateCode2,
	-- *INF*: IIF(Split=0,DescriptorCode,'')
	IFF(Split = 0, DescriptorCode, '') AS O_DescriptorCode3,
	-- *INF*: IIF(Split=0,NameOfPersonToBeIncluded,'')
	IFF(Split = 0, NameOfPersonToBeIncluded, '') AS O_NameOfPersonToBeIncluded3,
	-- *INF*: IIF(Split=0,StateCode,'')
	IFF(Split = 0, StateCode, '') AS O_StateCode3
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
	Split,
	GroupKey,
	O_DescriptorCode AS IN_DescriptorCode,
	O_NameOfPersonToBeIncluded AS IN_NameOfPersonToBeIncluded,
	O_StateCode AS IN_StateCode,
	O_DescriptorCode2 AS IN_DescriptorCode2,
	O_NameOfPersonToBeIncluded2 AS IN_NameOfPersonToBeIncluded2,
	O_StateCode2 AS IN_StateCode2,
	O_DescriptorCode3 AS IN_DescriptorCode3,
	O_NameOfPersonToBeIncluded3 AS IN_NameOfPersonToBeIncluded3,
	O_StateCode3 AS IN_StateCode3,
	-- *INF*: MAX(IN_DescriptorCode)
	MAX(IN_DescriptorCode) AS DescriptorCode,
	-- *INF*: MAX(IN_NameOfPersonToBeIncluded)
	MAX(IN_NameOfPersonToBeIncluded) AS NameOfPersonToBeIncluded,
	-- *INF*: MAX(IN_StateCode)
	MAX(IN_StateCode) AS StateCode,
	-- *INF*: MAX(IN_DescriptorCode2)
	MAX(IN_DescriptorCode2) AS DescriptorCode2,
	-- *INF*: MAX(IN_NameOfPersonToBeIncluded2)
	MAX(IN_NameOfPersonToBeIncluded2) AS NameOfPersonToBeIncluded2,
	-- *INF*: MAX(IN_StateCode2)
	MAX(IN_StateCode2) AS StateCode2,
	-- *INF*: MAX(IN_DescriptorCode3)
	MAX(IN_DescriptorCode3) AS DescriptorCode3,
	-- *INF*: MAX(IN_NameOfPersonToBeIncluded3)
	MAX(IN_NameOfPersonToBeIncluded3) AS NameOfPersonToBeIncluded3,
	-- *INF*: MAX(IN_StateCode3)
	MAX(IN_StateCode3) AS StateCode3,
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
	NameOfPersonToBeIncluded,
	StateCode,
	DescriptorCode2,
	NameOfPersonToBeIncluded2,
	StateCode2,
	DescriptorCode3,
	NameOfPersonToBeIncluded3,
	StateCode3,
	Name,
	TransactionEffectiveDate
	FROM AGG_GroupData
),
WCPols37Record AS (
	INSERT INTO WCPols37Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, DescriptorCode, NameOfPersonToBeIncluded, StateCode, DescriptorCode2, NameOfPersonToBeIncluded2, StateCode2, DescriptorCode3, NameOfPersonToBeIncluded3, StateCode3, NameOfInsured, EndorsementEffectiveDate)
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
	NAMEOFPERSONTOBEINCLUDED, 
	STATECODE, 
	DESCRIPTORCODE2, 
	NAMEOFPERSONTOBEINCLUDED2, 
	STATECODE2, 
	DESCRIPTORCODE3, 
	NAMEOFPERSONTOBEINCLUDED3, 
	STATECODE3, 
	Name AS NAMEOFINSURED, 
	TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_Target
),