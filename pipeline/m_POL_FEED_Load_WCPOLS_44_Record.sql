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
	SELECT Distinct 
	Track.WCTrackHistoryID as WCTrackHistoryID, 
	Forms.FormName as FormName, 
	'WC000424' as ParsedFormName,
	Party.Name as Name,
	Policy.TransactionEffectiveDate as TransactionEffectiveDate,
	Term.[State] as State,
	Term.BasisOfAuditNonComplianceCharge as BasisOfAuditNonComplianceCharge,
	Term.AuditNoncomplianceChargeMultiplier as AuditNoncomplianceChargeMultiplier
	FROM
	WorkWCForms Forms
	inner join WorkWCTrackHistory Track 	on 
		Forms.WCTrackHistoryID=Track.WCTrackHistoryID and 
		Forms.FormName like 'WC000424%' and 
		((Forms.OnPolicy=1 OR Forms.[Add] = 1) AND (Forms.Remove is null OR Forms.Remove = 0))
	inner join WorkWCParty Party on 
		Party.WCTrackHistoryID=Track.WCTrackHistoryID AND 
		Party.PartyAssociationType='Account'
	inner join WorkWCPolicy Policy on 
		Policy.WCTrackHistoryID=Track.WCTrackHistoryID
	inner join WorkWCStateTerm Term on
		Policy.WCTrackHistoryID=Term.WCTrackHistoryID
	WHERE
	Forms.Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_44}
	order by Track.WCTrackHistoryID, Forms.FormName,Party.Name,Policy.TransactionEffectiveDate
),
JNR_CombineRecordData AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID AS WCTrackHistoryID_Record00, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCTrackHistory.WCTrackHistoryID AS WCTrackHistoryID_tracking, 
	SQ_WorkWCTrackHistory.FormName, 
	SQ_WorkWCTrackHistory.ParsedFormName, 
	SQ_WorkWCTrackHistory.Name, 
	SQ_WorkWCTrackHistory.TransactionEffectiveDate, 
	SQ_WorkWCTrackHistory.State, 
	SQ_WorkWCTrackHistory.BasisOfAuditNonComplianceCharge, 
	SQ_WorkWCTrackHistory.AuditNoncomplianceChargeMultiplier
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
EXP_Counter AS (
	SELECT
	JNR_CombineRecordData.WCTrackHistoryID_Record00 AS WCTrackHistoryID,
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_CombineRecordData.AuditId,
	JNR_CombineRecordData.LinkData,
	'44' AS RecordTypeCode,
	mplt_Parse_FormNameField.ParsedNameOfForm1 AS ParsedNameOfForm,
	mplt_Parse_FormNameField.FormEdition,
	mplt_Parse_FormNameField.BureauCode,
	JNR_CombineRecordData.Name,
	JNR_CombineRecordData.TransactionEffectiveDate AS i_TransactionEffectiveDate,
	-- *INF*: TO_CHAR(i_TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(i_TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	WCTrackHistoryID AS v_CurrentRecord,
	-- *INF*: IIF(v_CurrentRecord<>v_PreviousRecord,0,1)
	IFF(v_CurrentRecord <> v_PreviousRecord, 0, 1) AS v_ChangeFlag,
	WCTrackHistoryID AS v_PreviousRecord,
	-- *INF*: IIF(v_ChangeFlag=1,v_Counter+1,1)
	IFF(v_ChangeFlag = 1, v_Counter + 1, 1) AS v_Counter,
	-- *INF*: MOD(v_Counter,3)
	MOD(v_Counter, 3) AS Split,
	-- *INF*: CEIL(v_Counter/3)
	CEIL(v_Counter / 3) AS GroupKey,
	JNR_CombineRecordData.State,
	JNR_CombineRecordData.BasisOfAuditNonComplianceCharge,
	JNR_CombineRecordData.AuditNoncomplianceChargeMultiplier,
	-- *INF*: IIF(ISNULL(AuditNoncomplianceChargeMultiplier),AuditNoncomplianceChargeMultiplier,AuditNoncomplianceChargeMultiplier*1000)
	IFF(
	    AuditNoncomplianceChargeMultiplier IS NULL, AuditNoncomplianceChargeMultiplier,
	    AuditNoncomplianceChargeMultiplier * 1000
	) AS O_AuditNoncomplianceChargeMultiplier
	FROM JNR_CombineRecordData
	 -- Manually join with mplt_Parse_FormNameField
),
EXP_Split AS (
	SELECT
	WCTrackHistoryID,
	ExtractDate,
	AuditId,
	LinkData,
	RecordTypeCode,
	ParsedNameOfForm,
	FormEdition,
	BureauCode,
	Name,
	o_TransactionEffectiveDate,
	Split,
	GroupKey,
	State AS IN_State,
	BasisOfAuditNonComplianceCharge AS IN_BasisOfAuditNonComplianceCharge,
	O_AuditNoncomplianceChargeMultiplier AS IN_AuditNoncomplianceChargeMultiplier,
	-- *INF*: IIF(Split=1,IN_State,'')
	IFF(Split = 1, IN_State, '') AS State,
	-- *INF*: IIF(Split=1,IN_BasisOfAuditNonComplianceCharge,'')
	IFF(Split = 1, IN_BasisOfAuditNonComplianceCharge, '') AS BasisOfAuditNonComplianceCharge,
	-- *INF*: IIF(Split=1,IN_AuditNoncomplianceChargeMultiplier,0)
	IFF(Split = 1, IN_AuditNoncomplianceChargeMultiplier, 0) AS AuditNoncomplianceChargeMultiplier,
	-- *INF*: IIF(Split=2,IN_State,'')
	IFF(Split = 2, IN_State, '') AS State2,
	-- *INF*: IIF(Split=2,IN_BasisOfAuditNonComplianceCharge,'')
	IFF(Split = 2, IN_BasisOfAuditNonComplianceCharge, '') AS BasisOfAuditNonComplianceCharge2,
	-- *INF*: IIF(Split=2,IN_AuditNoncomplianceChargeMultiplier,0)
	IFF(Split = 2, IN_AuditNoncomplianceChargeMultiplier, 0) AS AuditNoncomplianceChargeMultiplier2,
	-- *INF*: IIF(Split=0,IN_State,'')
	IFF(Split = 0, IN_State, '') AS State3,
	-- *INF*: IIF(Split=0,IN_BasisOfAuditNonComplianceCharge,'')
	IFF(Split = 0, IN_BasisOfAuditNonComplianceCharge, '') AS BasisOfAuditNonComplianceCharge3,
	-- *INF*: IIF(Split=0,IN_AuditNoncomplianceChargeMultiplier,0)
	IFF(Split = 0, IN_AuditNoncomplianceChargeMultiplier, 0) AS AuditNoncomplianceChargeMultiplier3
	FROM EXP_Counter
),
AGG_GroupData AS (
	SELECT
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	RecordTypeCode,
	ParsedNameOfForm,
	BureauCode,
	FormEdition,
	Split,
	GroupKey,
	State AS IN_State,
	BasisOfAuditNonComplianceCharge AS IN_BasisOfAuditNonComplianceCharge,
	AuditNoncomplianceChargeMultiplier AS IN_AuditNoncomplianceChargeMultiplier,
	State2 AS IN_State2,
	BasisOfAuditNonComplianceCharge2 AS IN_BasisOfAuditNonComplianceCharge2,
	AuditNoncomplianceChargeMultiplier2 AS IN_AuditNoncomplianceChargeMultiplier2,
	State3 AS IN_State3,
	BasisOfAuditNonComplianceCharge3 AS IN_BasisOfAuditNonComplianceCharge3,
	AuditNoncomplianceChargeMultiplier3 AS IN_AuditNoncomplianceChargeMultiplier3,
	-- *INF*: MAX(IN_State)
	MAX(IN_State) AS State,
	-- *INF*: MAX(IN_BasisOfAuditNonComplianceCharge)
	MAX(IN_BasisOfAuditNonComplianceCharge) AS BasisOfAuditNonComplianceCharge,
	-- *INF*: MAX(IN_AuditNoncomplianceChargeMultiplier)
	MAX(IN_AuditNoncomplianceChargeMultiplier) AS AuditNoncomplianceChargeMultiplier,
	-- *INF*: MAX(IN_State2)
	MAX(IN_State2) AS State2,
	-- *INF*: MAX(IN_BasisOfAuditNonComplianceCharge2)
	MAX(IN_BasisOfAuditNonComplianceCharge2) AS BasisOfAuditNonComplianceCharge2,
	-- *INF*: MAX(IN_AuditNoncomplianceChargeMultiplier2)
	MAX(IN_AuditNoncomplianceChargeMultiplier2) AS AuditNoncomplianceChargeMultiplier2,
	-- *INF*: MAX(IN_State3)
	MAX(IN_State3) AS State3,
	-- *INF*: MAX(IN_BasisOfAuditNonComplianceCharge3)
	MAX(IN_BasisOfAuditNonComplianceCharge3) AS BasisOfAuditNonComplianceCharge3,
	-- *INF*: MAX(IN_AuditNoncomplianceChargeMultiplier3)
	MAX(IN_AuditNoncomplianceChargeMultiplier3) AS AuditNoncomplianceChargeMultiplier3,
	Name,
	o_TransactionEffectiveDate AS TransactionEffectiveDate
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
	ParsedNameOfForm,
	BureauCode,
	FormEdition,
	State,
	BasisOfAuditNonComplianceCharge,
	AuditNoncomplianceChargeMultiplier,
	State2,
	BasisOfAuditNonComplianceCharge2,
	AuditNoncomplianceChargeMultiplier2,
	State3,
	BasisOfAuditNonComplianceCharge3,
	AuditNoncomplianceChargeMultiplier3,
	Name,
	TransactionEffectiveDate
	FROM AGG_GroupData
),
WCPols44Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols44Record
	WHERE AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols44Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, StateAbbreviation, BasisOfAuditNoncomplianceCharge, MaximumAuditNoncomplianceChargeMultiplier, StateAbbreviation2, BasisOfAuditNoncomplianceCharge2, MaximumAuditNoncomplianceChargeMultiplier2, StateAbbreviation3, BasisOfAuditNoncomplianceCharge3, MaximumAuditNoncomplianceChargeMultiplier3, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	RECORDTYPECODE, 
	ParsedNameOfForm AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	State AS STATEABBREVIATION, 
	BasisOfAuditNonComplianceCharge AS BASISOFAUDITNONCOMPLIANCECHARGE, 
	AuditNoncomplianceChargeMultiplier AS MAXIMUMAUDITNONCOMPLIANCECHARGEMULTIPLIER, 
	State2 AS STATEABBREVIATION2, 
	BasisOfAuditNonComplianceCharge2 AS BASISOFAUDITNONCOMPLIANCECHARGE2, 
	AuditNoncomplianceChargeMultiplier2 AS MAXIMUMAUDITNONCOMPLIANCECHARGEMULTIPLIER2, 
	State3 AS STATEABBREVIATION3, 
	BasisOfAuditNonComplianceCharge3 AS BASISOFAUDITNONCOMPLIANCECHARGE3, 
	AuditNoncomplianceChargeMultiplier3 AS MAXIMUMAUDITNONCOMPLIANCECHARGEMULTIPLIER3, 
	Name AS NAMEOFINSURED, 
	TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_Target
),