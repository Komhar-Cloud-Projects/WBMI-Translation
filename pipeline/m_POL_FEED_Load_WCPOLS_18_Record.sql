WITH
SQ_WorkWCStateTerm AS (
	SELECT DISTINCT 
	Term.WCTrackHistoryID as WCTrackHistoryID, 
	Term.Auditid as Auditid, 
	Term.State as State,
	Sup.WCPOLSCode as WCPOLSCode
	FROM
	 WorkWCStateTerm Term
	INNER JOIN  SupWCPOLS Sup ON
		Term.State =  Sup.SourceCode
	WHERE 
	Sup.Tablename='WCPOLS18Record' AND 
	ProcessName='StateCodeRecord18' AND
	Term.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	order by 1,4
),
EXP_State_Input AS (
	SELECT
	WCTrackHistoryID,
	Auditid,
	State,
	WCPOLSCode
	FROM SQ_WorkWCStateTerm
),
AGG_combine_States AS (
	SELECT
	WCTrackHistoryID,
	Auditid,
	WCPOLSCode AS i_WCPOLSCode,
	-- *INF*: IIF(WCTrackHistoryID=v_PrevWCTrackHistoryID,0,1)
	IFF(WCTrackHistoryID = v_PrevWCTrackHistoryID, 0, 1) AS v_NewTrackId,
	-- *INF*: DECODE(TRUE,
	-- v_NewTrackId=1, i_WCPOLSCode,
	-- v_NewTrackId=0 AND i_WCPOLSCode!= v_PrevPolsState, v_PolsState ||i_WCPOLSCode,
	-- v_NewTrackId=0, v_PolsState,
	-- ''
	-- )
	-- 
	-- -- if new record overwrite WcPolsCode (which is State)
	-- -- if not new record and State != previous State then concatenate comma and State
	-- -- if not new record retain State value (assumed State =  prevState)
	-- -- else blank out the field
	DECODE(
	    TRUE,
	    v_NewTrackId = 1, i_WCPOLSCode,
	    v_NewTrackId = 0 AND i_WCPOLSCode != v_PrevPolsState, v_PolsState || i_WCPOLSCode,
	    v_NewTrackId = 0, v_PolsState,
	    ''
	) AS v_PolsState,
	i_WCPOLSCode AS v_PrevPolsState,
	WCTrackHistoryID AS v_PrevWCTrackHistoryID,
	v_PolsState AS o_State
	FROM EXP_State_Input
	GROUP BY WCTrackHistoryID, Auditid
),
EXP_AggOutput AS (
	SELECT
	WCTrackHistoryID,
	o_State AS StateList
	FROM AGG_combine_States
),
SQ_WorkWCTrackHistory AS (
	SELECT 
	Track.WCTrackHistoryID as WCTrackHistoryID, 
	'WC000104' as ParsedFormName,
	Forms.FormName as FormName, 
	Limit.LimitType as FELAIncreasedLimitType,
	Limit.LimitValue as AccidentAmount,
	Limit.LimitValue as DiseaseAmount,
	Party.Name as Name,
	Policy.TransactionEffectiveDate as TransactionEffectiveDate  
	FROM
	WorkWCForms Forms
	inner join WorkWCTrackHistory Track 	on 
		Forms.WCTrackHistoryID=Track.WCTrackHistoryID and 
		Forms.FormName like 'WC000104%' and 
		((Forms.OnPolicy=1 OR Forms.[Add] = 1) AND (Forms.Remove is null OR Forms.Remove = 0))
	inner join WorkWCParty Party on 
		Party.WCTrackHistoryID=Track.WCTrackHistoryID AND 
		Party.PartyAssociationType='Account'
	inner join WorkWCPolicy Policy on 
		Policy.WCTrackHistoryID=Track.WCTrackHistoryID
	left join WorkWCLimit Limit on
		Limit.WCTrackHistoryID=Track.WCTrackHistoryID AND
		LimitType='FELAIncreased'
	WHERE
	Forms.Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_18}
	order by 1
),
EXP_DataInput AS (
	SELECT
	WCTrackHistoryID,
	ParsedFormName,
	FormName,
	FELAIncreasedLimitType,
	AccidentAmount,
	DiseaseAmount,
	Name,
	TransactionEffectiveDate
	FROM SQ_WorkWCTrackHistory
),
JNR_Data AS (SELECT
	EXP_DataInput.WCTrackHistoryID, 
	EXP_DataInput.ParsedFormName, 
	EXP_DataInput.FormName, 
	EXP_DataInput.FELAIncreasedLimitType, 
	EXP_DataInput.AccidentAmount, 
	EXP_DataInput.DiseaseAmount, 
	EXP_DataInput.Name, 
	EXP_DataInput.TransactionEffectiveDate, 
	EXP_AggOutput.WCTrackHistoryID AS WCTrackHistoryID1, 
	EXP_AggOutput.StateList
	FROM EXP_DataInput
	INNER JOIN EXP_AggOutput
	ON EXP_AggOutput.WCTrackHistoryID = EXP_DataInput.WCTrackHistoryID
),
EXP_PostDataJoin AS (
	SELECT
	WCTrackHistoryID,
	ParsedFormName,
	FormName,
	FELAIncreasedLimitType,
	AccidentAmount,
	DiseaseAmount,
	Name,
	TransactionEffectiveDate,
	StateList
	FROM JNR_Data
),
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
JNR_finalJoin AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	EXP_PostDataJoin.WCTrackHistoryID AS WCTrackHistoryID1, 
	EXP_PostDataJoin.ParsedFormName, 
	EXP_PostDataJoin.FormName, 
	EXP_PostDataJoin.FELAIncreasedLimitType, 
	EXP_PostDataJoin.AccidentAmount, 
	EXP_PostDataJoin.DiseaseAmount, 
	EXP_PostDataJoin.Name, 
	EXP_PostDataJoin.TransactionEffectiveDate, 
	EXP_PostDataJoin.StateList
	FROM SQ_WCPols00Record
	INNER JOIN EXP_PostDataJoin
	ON EXP_PostDataJoin.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
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
EXP_output AS (
	SELECT
	JNR_finalJoin.WCTrackHistoryID,
	JNR_finalJoin.LinkData,
	CURRENT_TIMESTAMP AS ExtractDate,
	18 AS RecordTypeCode,
	JNR_finalJoin.ParsedFormName,
	JNR_finalJoin.FELAIncreasedLimitType,
	JNR_finalJoin.AccidentAmount,
	JNR_finalJoin.DiseaseAmount,
	JNR_finalJoin.Name,
	JNR_finalJoin.TransactionEffectiveDate AS i_TransactionEffectiveDate,
	-- *INF*: TO_CHAR(i_TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(i_TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	JNR_finalJoin.StateList,
	mplt_Parse_FormNameField.FormEdition,
	mplt_Parse_FormNameField.BureauCode
	FROM JNR_finalJoin
	 -- Manually join with mplt_Parse_FormNameField
),
WCPols18Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols18Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols18Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, EmployerLiabilityLimitAmountFederalBodilyInjuryByAccidentAmount, EmployerLiabilityLimitAmountFederalBodilyInjuryByDiseaseAmount, ScheduleStateCode, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	Auditid AS AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	RECORDTYPECODE, 
	ParsedFormName AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	AccidentAmount AS EMPLOYERLIABILITYLIMITAMOUNTFEDERALBODILYINJURYBYACCIDENTAMOUNT, 
	DiseaseAmount AS EMPLOYERLIABILITYLIMITAMOUNTFEDERALBODILYINJURYBYDISEASEAMOUNT, 
	StateList AS SCHEDULESTATECODE, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_output
),