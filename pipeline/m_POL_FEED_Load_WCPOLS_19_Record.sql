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
	'WC000106' as ParsedFormName,
	Forms.FormName as FormName, 
	Case When Not  ST.USLAndHFormsPercentage Is Null Then ST.State Else '' End as State,
	Case When Not  ST.USLAndHFormsPercentage Is Null Then Sup.WCPOLSCode Else '' End as StateCode,
	-- rules -- if two char then put 0's at beginning and end.  if three chars then leading 0
	Case 
	When ST.USLAndHFormsPercentage Is Null or ST.USLAndHFormsPercentage='' Then ''
	When Len(ST.USLAndHFormsPercentage)=2 Then '0' +  ST.USLAndHFormsPercentage + '0'
	When Len(ST.USLAndHFormsPercentage)=3 Then '0' +  ST.USLAndHFormsPercentage 
	Else ST.USLAndHFormsPercentage End as  USLAndHFormsPercentage,
	
	Party.Name as Name,
	Policy.TransactionEffectiveDate as TransactionEffectiveDate ,
	ROW_NUMBER() over (partition by Track.WCTrackHistoryID order by ST.WC_StateID ) as rn
	FROM
	WorkWCForms Forms
	inner join WorkWCTrackHistory Track 	on 
		Forms.WCTrackHistoryID=Track.WCTrackHistoryID and 
		Forms.FormName like 'WC000106%' and 
		((Forms.OnPolicy=1 OR Forms.[Add] = 1) AND (Forms.Remove is null OR Forms.Remove = 0))
	inner join WorkWCParty Party on 
		Party.WCTrackHistoryID=Track.WCTrackHistoryID AND 
		Party.PartyAssociationType='Account'
	inner join WorkWCPolicy Policy on 
		Policy.WCTrackHistoryID=Track.WCTrackHistoryID
	inner join WorkWCStateTerm ST on
		ST.WCTrackHistoryID=Track.WCTrackHistoryID
	INNER JOIN  SupWCPOLS Sup ON
		ST.State =  Sup.SourceCode AND
		Sup.Tablename='WCPOLS19Record' AND 
		Sup.ProcessName='StateCodeRecord19' 
	WHERE
	Forms.Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_19}
	order by 1
),
JNR_Record19 AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCTrackHistory.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCTrackHistory.ParsedFormName, 
	SQ_WorkWCTrackHistory.FormName, 
	SQ_WorkWCTrackHistory.State, 
	SQ_WorkWCTrackHistory.StateCode, 
	SQ_WorkWCTrackHistory.USLAndHFormsPercentage, 
	SQ_WorkWCTrackHistory.Name, 
	SQ_WorkWCTrackHistory.TransactionEffectiveDate, 
	SQ_WorkWCTrackHistory.rn
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WorkWCTrackHistory
	ON SQ_WorkWCTrackHistory.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
),
SRT_OrderSource AS (
	SELECT
	WCTrackHistoryID, 
	FormName, 
	rn, 
	LinkData, 
	AuditId, 
	ParsedFormName, 
	State, 
	StateCode, 
	USLAndHFormsPercentage, 
	Name, 
	TransactionEffectiveDate
	FROM JNR_Record19
	ORDER BY WCTrackHistoryID ASC, FormName ASC, rn ASC
),
AGGTRANS AS (
	SELECT
	WCTrackHistoryID,
	FormName,
	LinkData,
	AuditId,
	ParsedFormName,
	Name,
	TransactionEffectiveDate,
	rn,
	StateCode AS i_StateCode,
	-- *INF*: MAX(i_StateCode, rn=1)
	MAX(i_StateCode, rn = 1) AS o_StateCode1,
	-- *INF*: MAX(i_StateCode, rn=2)
	MAX(i_StateCode, rn = 2) AS o_StateCode2,
	-- *INF*: MAX(i_StateCode, rn=3)
	MAX(i_StateCode, rn = 3) AS o_StateCode3,
	-- *INF*: MAX(i_StateCode, rn=4)
	MAX(i_StateCode, rn = 4) AS o_StateCode4,
	-- *INF*: MAX(i_StateCode, rn=5)
	MAX(i_StateCode, rn = 5) AS o_StateCode5,
	-- *INF*: MAX(i_StateCode, rn=6)
	MAX(i_StateCode, rn = 6) AS o_StateCode6,
	-- *INF*: MAX(i_StateCode, rn=7)
	MAX(i_StateCode, rn = 7) AS o_StateCode7,
	-- *INF*: MAX(i_StateCode, rn=8)
	MAX(i_StateCode, rn = 8) AS o_StateCode8,
	-- *INF*: MAX(i_StateCode, rn=9)
	MAX(i_StateCode, rn = 9) AS o_StateCode9,
	-- *INF*: MAX(i_StateCode, rn=10)
	MAX(i_StateCode, rn = 10) AS o_StateCode10,
	-- *INF*: MAX(i_StateCode, rn=11)
	MAX(i_StateCode, rn = 11) AS o_StateCode11,
	-- *INF*: MAX(i_StateCode, rn=12)
	MAX(i_StateCode, rn = 12) AS o_StateCode12,
	-- *INF*: MAX(i_StateCode, rn=13)
	MAX(i_StateCode, rn = 13) AS o_StateCode13,
	-- *INF*: MAX(i_StateCode, rn=14)
	MAX(i_StateCode, rn = 14) AS o_StateCode14,
	-- *INF*: MAX(i_StateCode, rn=15)
	MAX(i_StateCode, rn = 15) AS o_StateCode15,
	-- *INF*: MAX(i_StateCode, rn=16)
	MAX(i_StateCode, rn = 16) AS o_StateCode16,
	-- *INF*: MAX(i_StateCode, rn=17)
	MAX(i_StateCode, rn = 17) AS o_StateCode17,
	-- *INF*: MAX(i_StateCode, rn=18)
	MAX(i_StateCode, rn = 18) AS o_StateCode18,
	-- *INF*: MAX(i_StateCode, rn=19)
	MAX(i_StateCode, rn = 19) AS o_StateCode19,
	-- *INF*: MAX(i_StateCode, rn=20)
	MAX(i_StateCode, rn = 20) AS o_StateCode20,
	-- *INF*: MAX(i_StateCode, rn=21)
	MAX(i_StateCode, rn = 21) AS o_StateCode21,
	-- *INF*: MAX(i_StateCode, rn=22)
	MAX(i_StateCode, rn = 22) AS o_StateCode22,
	-- *INF*: MAX(i_StateCode, rn=23)
	MAX(i_StateCode, rn = 23) AS o_StateCode23,
	-- *INF*: MAX(i_StateCode, rn=24)
	MAX(i_StateCode, rn = 24) AS o_StateCode24,
	-- *INF*: MAX(i_StateCode, rn=25)
	MAX(i_StateCode, rn = 25) AS o_StateCode25,
	USLAndHFormsPercentage AS i_USLAndHFormsPercentage,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=1)
	MAX(i_USLAndHFormsPercentage, rn = 1) AS o_USLAndHFormsPercentage1,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=2)
	MAX(i_USLAndHFormsPercentage, rn = 2) AS o_USLAndHFormsPercentage2,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=3)
	MAX(i_USLAndHFormsPercentage, rn = 3) AS o_USLAndHFormsPercentage3,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=4)
	MAX(i_USLAndHFormsPercentage, rn = 4) AS o_USLAndHFormsPercentage4,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=5)
	MAX(i_USLAndHFormsPercentage, rn = 5) AS o_USLAndHFormsPercentage5,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=6)
	MAX(i_USLAndHFormsPercentage, rn = 6) AS o_USLAndHFormsPercentage6,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=7)
	MAX(i_USLAndHFormsPercentage, rn = 7) AS o_USLAndHFormsPercentage7,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=8)
	MAX(i_USLAndHFormsPercentage, rn = 8) AS o_USLAndHFormsPercentage8,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=9)
	MAX(i_USLAndHFormsPercentage, rn = 9) AS o_USLAndHFormsPercentage9,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=10)
	MAX(i_USLAndHFormsPercentage, rn = 10) AS o_USLAndHFormsPercentage10,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=11)
	MAX(i_USLAndHFormsPercentage, rn = 11) AS o_USLAndHFormsPercentage11,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=12)
	MAX(i_USLAndHFormsPercentage, rn = 12) AS o_USLAndHFormsPercentage12,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=13)
	MAX(i_USLAndHFormsPercentage, rn = 13) AS o_USLAndHFormsPercentage13,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=14)
	MAX(i_USLAndHFormsPercentage, rn = 14) AS o_USLAndHFormsPercentage14,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=15)
	MAX(i_USLAndHFormsPercentage, rn = 15) AS o_USLAndHFormsPercentage15,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=16)
	MAX(i_USLAndHFormsPercentage, rn = 16) AS o_USLAndHFormsPercentage16,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=17)
	MAX(i_USLAndHFormsPercentage, rn = 17) AS o_USLAndHFormsPercentage17,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=18)
	MAX(i_USLAndHFormsPercentage, rn = 18) AS o_USLAndHFormsPercentage18,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=19)
	MAX(i_USLAndHFormsPercentage, rn = 19) AS o_USLAndHFormsPercentage19,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=20)
	MAX(i_USLAndHFormsPercentage, rn = 20) AS o_USLAndHFormsPercentage20,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=21)
	MAX(i_USLAndHFormsPercentage, rn = 21) AS o_USLAndHFormsPercentage21,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=22)
	MAX(i_USLAndHFormsPercentage, rn = 22) AS o_USLAndHFormsPercentage22,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=23)
	MAX(i_USLAndHFormsPercentage, rn = 23) AS o_USLAndHFormsPercentage23,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=24)
	MAX(i_USLAndHFormsPercentage, rn = 24) AS o_USLAndHFormsPercentage24,
	-- *INF*: MAX(i_USLAndHFormsPercentage, rn=25)
	MAX(i_USLAndHFormsPercentage, rn = 25) AS o_USLAndHFormsPercentage25
	FROM SRT_OrderSource
	GROUP BY WCTrackHistoryID, FormName, LinkData, AuditId, ParsedFormName, Name, TransactionEffectiveDate
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
EXP_DataCollect AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	AGGTRANS.AuditId,
	AGGTRANS.WCTrackHistoryID,
	AGGTRANS.LinkData,
	'19' AS RecordTypeCode,
	mplt_Parse_FormNameField.ParsedNameOfForm1 AS ParsedNameOfForm,
	mplt_Parse_FormNameField.FormEdition,
	mplt_Parse_FormNameField.BureauCode,
	AGGTRANS.Name,
	AGGTRANS.TransactionEffectiveDate AS i_TransactionEffectiveDate,
	-- *INF*: TO_CHAR(i_TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(i_TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	AGGTRANS.o_StateCode1 AS StateCode1,
	AGGTRANS.o_StateCode2 AS StateCode2,
	AGGTRANS.o_StateCode3 AS StateCode3,
	AGGTRANS.o_StateCode4 AS StateCode4,
	AGGTRANS.o_StateCode5 AS StateCode5,
	AGGTRANS.o_StateCode6 AS StateCode6,
	AGGTRANS.o_StateCode7 AS StateCode7,
	AGGTRANS.o_StateCode8 AS StateCode8,
	AGGTRANS.o_StateCode9 AS StateCode9,
	AGGTRANS.o_StateCode10 AS StateCode10,
	AGGTRANS.o_StateCode11 AS StateCode11,
	AGGTRANS.o_StateCode12 AS StateCode12,
	AGGTRANS.o_StateCode13 AS StateCode13,
	AGGTRANS.o_StateCode14 AS StateCode14,
	AGGTRANS.o_StateCode15 AS StateCode15,
	AGGTRANS.o_StateCode16 AS StateCode16,
	AGGTRANS.o_StateCode17 AS StateCode17,
	AGGTRANS.o_StateCode18 AS StateCode18,
	AGGTRANS.o_StateCode19 AS StateCode19,
	AGGTRANS.o_StateCode20 AS StateCode20,
	AGGTRANS.o_StateCode21 AS StateCode21,
	AGGTRANS.o_StateCode22 AS StateCode22,
	AGGTRANS.o_StateCode23 AS StateCode23,
	AGGTRANS.o_StateCode24 AS StateCode24,
	AGGTRANS.o_StateCode25 AS StateCode25,
	AGGTRANS.o_USLAndHFormsPercentage1 AS USLAndHFormsPercentage1,
	AGGTRANS.o_USLAndHFormsPercentage2 AS USLAndHFormsPercentage2,
	AGGTRANS.o_USLAndHFormsPercentage3 AS USLAndHFormsPercentage3,
	AGGTRANS.o_USLAndHFormsPercentage4 AS USLAndHFormsPercentage4,
	AGGTRANS.o_USLAndHFormsPercentage5 AS USLAndHFormsPercentage5,
	AGGTRANS.o_USLAndHFormsPercentage6 AS USLAndHFormsPercentage6,
	AGGTRANS.o_USLAndHFormsPercentage7 AS USLAndHFormsPercentage7,
	AGGTRANS.o_USLAndHFormsPercentage8 AS USLAndHFormsPercentage8,
	AGGTRANS.o_USLAndHFormsPercentage9 AS USLAndHFormsPercentage9,
	AGGTRANS.o_USLAndHFormsPercentage10 AS USLAndHFormsPercentage10,
	AGGTRANS.o_USLAndHFormsPercentage11 AS USLAndHFormsPercentage11,
	AGGTRANS.o_USLAndHFormsPercentage12 AS USLAndHFormsPercentage12,
	AGGTRANS.o_USLAndHFormsPercentage13 AS USLAndHFormsPercentage13,
	AGGTRANS.o_USLAndHFormsPercentage14 AS USLAndHFormsPercentage14,
	AGGTRANS.o_USLAndHFormsPercentage15 AS USLAndHFormsPercentage15,
	AGGTRANS.o_USLAndHFormsPercentage16 AS USLAndHFormsPercentage16,
	AGGTRANS.o_USLAndHFormsPercentage17 AS USLAndHFormsPercentage17,
	AGGTRANS.o_USLAndHFormsPercentage18 AS USLAndHFormsPercentage18,
	AGGTRANS.o_USLAndHFormsPercentage19 AS USLAndHFormsPercentage19,
	AGGTRANS.o_USLAndHFormsPercentage20 AS USLAndHFormsPercentage20,
	AGGTRANS.o_USLAndHFormsPercentage21 AS USLAndHFormsPercentage21,
	AGGTRANS.o_USLAndHFormsPercentage22 AS USLAndHFormsPercentage22,
	AGGTRANS.o_USLAndHFormsPercentage23 AS USLAndHFormsPercentage23,
	AGGTRANS.o_USLAndHFormsPercentage24 AS USLAndHFormsPercentage24,
	AGGTRANS.o_USLAndHFormsPercentage25 AS USLAndHFormsPercentage25
	FROM AGGTRANS
	 -- Manually join with mplt_Parse_FormNameField
),
WCPols19Record AS (
	INSERT INTO WCPols19Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, StateCode, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor, StateCode2, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor2, StateCode3, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor3, StateCode4, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor4, StateCode5, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor5, StateCode6, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor6, StateCode7, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor7, StateCode8, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor8, StateCode9, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor9, StateCode10, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor10, StateCode11, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor11, StateCode12, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor12, StateCode13, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor13, StateCode14, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor14, StateCode15, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor15, StateCode16, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor16, StateCode17, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor17, StateCode18, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor18, StateCode19, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor19, StateCode20, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor20, StateCode21, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor21, StateCode22, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor22, StateCode23, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor23, StateCode24, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor24, StateCode25, UnitedStatesLongshoreAndHarborWorkersCoveragePercentageFactor25, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	RECORDTYPECODE, 
	ParsedNameOfForm AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	StateCode1 AS STATECODE, 
	USLAndHFormsPercentage1 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR, 
	STATECODE2, 
	USLAndHFormsPercentage2 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR2, 
	STATECODE3, 
	USLAndHFormsPercentage3 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR3, 
	STATECODE4, 
	USLAndHFormsPercentage4 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR4, 
	STATECODE5, 
	USLAndHFormsPercentage5 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR5, 
	STATECODE6, 
	USLAndHFormsPercentage6 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR6, 
	STATECODE7, 
	USLAndHFormsPercentage7 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR7, 
	STATECODE8, 
	USLAndHFormsPercentage8 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR8, 
	STATECODE9, 
	USLAndHFormsPercentage9 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR9, 
	STATECODE10, 
	USLAndHFormsPercentage10 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR10, 
	STATECODE11, 
	USLAndHFormsPercentage11 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR11, 
	STATECODE12, 
	USLAndHFormsPercentage12 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR12, 
	STATECODE13, 
	USLAndHFormsPercentage13 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR13, 
	STATECODE14, 
	USLAndHFormsPercentage14 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR14, 
	STATECODE15, 
	USLAndHFormsPercentage15 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR15, 
	STATECODE16, 
	USLAndHFormsPercentage16 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR16, 
	STATECODE17, 
	USLAndHFormsPercentage17 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR17, 
	STATECODE18, 
	USLAndHFormsPercentage18 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR18, 
	STATECODE19, 
	USLAndHFormsPercentage19 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR19, 
	STATECODE20, 
	USLAndHFormsPercentage20 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR20, 
	STATECODE21, 
	USLAndHFormsPercentage21 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR21, 
	STATECODE22, 
	USLAndHFormsPercentage22 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR22, 
	STATECODE23, 
	USLAndHFormsPercentage23 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR23, 
	STATECODE24, 
	USLAndHFormsPercentage24 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR24, 
	STATECODE25, 
	USLAndHFormsPercentage25 AS UNITEDSTATESLONGSHOREANDHARBORWORKERSCOVERAGEPERCENTAGEFACTOR25, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_DataCollect
),