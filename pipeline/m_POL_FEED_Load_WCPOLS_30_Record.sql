WITH
SQ_WorkWCTrackHistory AS (
	declare  @GroupSize float = 6.0  -- need float type to force a decimal when dividing so they can be rounded up
	
	select
	B.WCTrackHistoryID as WCTrackHistoryID,
	B.ParsedFormName as ParsedFormName,
	B.FormName as FormName,
	B.[State] as [State],
	B.StateCode as StateCode,
	B.PremiumDiscountLevel1Factor as PremiumDiscountLevel1Factor,
	B.PremiumDiscountLevel2Factor as PremiumDiscountLevel2Factor,
	B.PremiumDiscountLevel3Factor as PremiumDiscountLevel3Factor,
	B.PremiumDiscountLevel4Factor as PremiumDiscountLevel4Factor,
	B.PremiumDiscountAveragePercentageDiscount as PremiumDiscountAveragePercentageDiscount,
	B.Name as Name,
	B.TransactionEffectiveDate as TransactionEffectiveDate,
	ceiling(round(cast(B.rn as float)/@GroupSize,1)) as GroupId, 
	B.rn%cast(@GroupSize as int) as GroupRowNumber 
	--B.rn as GroupRowNumber
	FROM
	(
	Select 
	A.WCTrackHistoryID as WCTrackHistoryID,
	A.ParsedFormName as ParsedFormName,
	A.FormName as FormName,
	A.[State] as [State],
	A.StateCode as StateCode,
	A.PremiumDiscountLevel1Factor as PremiumDiscountLevel1Factor,
	A.PremiumDiscountLevel2Factor as PremiumDiscountLevel2Factor,
	A.PremiumDiscountLevel3Factor as PremiumDiscountLevel3Factor,
	A.PremiumDiscountLevel4Factor as PremiumDiscountLevel4Factor,
	A.PremiumDiscountAveragePercentageDiscount as PremiumDiscountAveragePercentageDiscount,
	A.Name as Name,
	A.TransactionEffectiveDate as TransactionEffectiveDate,
	
	ROW_NUMBER() over (partition by A.WCTrackHistoryID, A.FormName order by A.RateEffectiveDate, A.[State] ) as rn
	From 
	(SELECT Distinct
	Track.WCTrackHistoryID as WCTrackHistoryID, 
	'WC000406' as ParsedFormName,
	Forms.FormName as FormName, 
	
	StateTerm.[State] as State,
	sup.WCPOLSCode as StateCode,
	StateTerm.PremiumDiscountLevel1Factor as PremiumDiscountLevel1Factor,
	StateTerm.PremiumDiscountLevel2Factor as PremiumDiscountLevel2Factor,
	StateTerm.PremiumDiscountLevel3Factor as PremiumDiscountLevel3Factor,
	StateTerm.PremiumDiscountLevel4Factor as PremiumDiscountLevel4Factor,
	StateTerm.PremiumDiscountAveragePercentageDiscount  as PremiumDiscountAveragePercentageDiscount , 
	StateTerm.wc_stateid,
	Party.Name as Name,
	Policy.TransactionEffectiveDate as TransactionEffectiveDate,
	StateTerm.RateEffectiveDate
	FROM
	WorkWCForms Forms
	inner join WorkWCTrackHistory Track 	on 
		Forms.WCTrackHistoryID=Track.WCTrackHistoryID and 
		Forms.FormName like 'WC000406%' and 
		((Forms.OnPolicy=1 OR Forms.[Add] = 1) AND (Forms.Remove is null OR Forms.Remove = 0))
	inner join WorkWCParty Party on 
		Party.WCTrackHistoryID=Track.WCTrackHistoryID AND 
		Party.PartyAssociationType='Account'
	inner join WorkWCPolicy Policy on 
		Policy.WCTrackHistoryID=Track.WCTrackHistoryID
	inner join WorkWCStateTerm StateTerm on 
		StateTerm.WCTrackHistoryID=Track.WCTrackHistoryID
	inner join SupWCPOLS Sup on StateTerm.[State]=SourceCode AND
	TableName='WCPOLS30Record' and ProcessName='StateCodeRecord30'
	
	WHERE
	Forms.Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_30}
	
	) A
	) B
	order by WCTrackHistoryID,FormName, GroupId, GroupRowNumber
),
EXP_InputCheck AS (
	SELECT
	WCTrackHistoryID,
	ParsedFormName,
	FormName,
	State,
	StateCode,
	PremiumDiscountLevel1Factor,
	PremiumDiscountLevel2Factor,
	PremiumDiscountLevel3Factor,
	PremiumDiscountLevel4Factor,
	PremiumDiscountAveragePercentageDiscount,
	Name,
	TransactionEffectiveDate,
	GroupId,
	GroupRowNumber
	FROM SQ_WorkWCTrackHistory
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
JNR_JoinSQs AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	EXP_InputCheck.WCTrackHistoryID AS WCTrackHistoryID1, 
	EXP_InputCheck.ParsedFormName, 
	EXP_InputCheck.FormName, 
	EXP_InputCheck.State, 
	EXP_InputCheck.StateCode, 
	EXP_InputCheck.PremiumDiscountLevel1Factor, 
	EXP_InputCheck.PremiumDiscountLevel2Factor, 
	EXP_InputCheck.PremiumDiscountLevel3Factor, 
	EXP_InputCheck.PremiumDiscountLevel4Factor, 
	EXP_InputCheck.PremiumDiscountAveragePercentageDiscount, 
	EXP_InputCheck.Name, 
	EXP_InputCheck.TransactionEffectiveDate, 
	EXP_InputCheck.GroupId, 
	EXP_InputCheck.GroupRowNumber
	FROM SQ_WCPols00Record
	INNER JOIN EXP_InputCheck
	ON EXP_InputCheck.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
),
SRT_OrderByKeys AS (
	SELECT
	WCTrackHistoryID, 
	FormName, 
	GroupId, 
	GroupRowNumber, 
	State, 
	StateCode, 
	LinkData, 
	AuditId, 
	ParsedFormName, 
	PremiumDiscountLevel1Factor, 
	PremiumDiscountLevel2Factor, 
	PremiumDiscountLevel3Factor, 
	PremiumDiscountLevel4Factor, 
	PremiumDiscountAveragePercentageDiscount, 
	Name, 
	TransactionEffectiveDate
	FROM JNR_JoinSQs
	ORDER BY WCTrackHistoryID ASC, FormName ASC, GroupId ASC, GroupRowNumber ASC
),
AGG_GroupByTrackIdFormName AS (
	SELECT
	WCTrackHistoryID,
	FormName,
	GroupId,
	GroupRowNumber AS rn,
	StateCode,
	LinkData,
	AuditId,
	ParsedFormName,
	PremiumDiscountLevel1Factor,
	PremiumDiscountLevel2Factor,
	PremiumDiscountLevel3Factor,
	PremiumDiscountLevel4Factor,
	PremiumDiscountAveragePercentageDiscount,
	-- *INF*: MAX(StateCode, rn=1)
	MAX(StateCode, rn = 1) AS State_1,
	-- *INF*: MAX(StateCode, rn=2)
	MAX(StateCode, rn = 2) AS State_2,
	-- *INF*: MAX(StateCode, rn=3)
	MAX(StateCode, rn = 3) AS State_3,
	-- *INF*: MAX(StateCode, rn=4)
	MAX(StateCode, rn = 4) AS State_4,
	-- *INF*: MAX(StateCode, rn=5)
	MAX(StateCode, rn = 5) AS State_5,
	-- *INF*: MAX(StateCode, rn=0)
	MAX(StateCode, rn = 0) AS State_6,
	-- *INF*: MAX(PremiumDiscountLevel1Factor, rn=1)
	MAX(PremiumDiscountLevel1Factor, rn = 1) AS PremiumDiscountLevel1Factor_1,
	-- *INF*: MAX(PremiumDiscountLevel2Factor, rn=1)
	MAX(PremiumDiscountLevel2Factor, rn = 1) AS PremiumDiscountLevel2Factor_1,
	-- *INF*: MAX(PremiumDiscountLevel3Factor, rn=1)
	MAX(PremiumDiscountLevel3Factor, rn = 1) AS PremiumDiscountLevel3Factor_1,
	-- *INF*: MAX(PremiumDiscountLevel4Factor, rn=1)
	MAX(PremiumDiscountLevel4Factor, rn = 1) AS PremiumDiscountLevel4Factor_1,
	-- *INF*: MAX(PremiumDiscountAveragePercentageDiscount, rn=1)
	MAX(PremiumDiscountAveragePercentageDiscount, rn = 1) AS PremiumDiscountAveragePercentageDiscount_1,
	-- *INF*: MAX(PremiumDiscountLevel1Factor, rn=2)
	MAX(PremiumDiscountLevel1Factor, rn = 2) AS PremiumDiscountLevel1Factor_2,
	-- *INF*: MAX(PremiumDiscountLevel2Factor, rn=2)
	MAX(PremiumDiscountLevel2Factor, rn = 2) AS PremiumDiscountLevel2Factor_2,
	-- *INF*: MAX(PremiumDiscountLevel3Factor, rn=2)
	MAX(PremiumDiscountLevel3Factor, rn = 2) AS PremiumDiscountLevel3Factor_2,
	-- *INF*: MAX(PremiumDiscountLevel4Factor, rn=2)
	MAX(PremiumDiscountLevel4Factor, rn = 2) AS PremiumDiscountLevel4Factor_2,
	-- *INF*: MAX(PremiumDiscountAveragePercentageDiscount, rn=2)
	MAX(PremiumDiscountAveragePercentageDiscount, rn = 2) AS PremiumDiscountAveragePercentageDiscount_2,
	-- *INF*: MAX(PremiumDiscountLevel1Factor, rn=3)
	MAX(PremiumDiscountLevel1Factor, rn = 3) AS PremiumDiscountLevel1Factor_3,
	-- *INF*: MAX(PremiumDiscountLevel2Factor, rn=3)
	MAX(PremiumDiscountLevel2Factor, rn = 3) AS PremiumDiscountLevel2Factor_3,
	-- *INF*: MAX(PremiumDiscountLevel3Factor, rn=3)
	MAX(PremiumDiscountLevel3Factor, rn = 3) AS PremiumDiscountLevel3Factor_3,
	-- *INF*: MAX(PremiumDiscountLevel4Factor, rn=3)
	MAX(PremiumDiscountLevel4Factor, rn = 3) AS PremiumDiscountLevel4Factor_3,
	-- *INF*: MAX(PremiumDiscountAveragePercentageDiscount, rn=3)
	MAX(PremiumDiscountAveragePercentageDiscount, rn = 3) AS PremiumDiscountAveragePercentageDiscount_3,
	-- *INF*: MAX(PremiumDiscountLevel1Factor, rn=4)
	MAX(PremiumDiscountLevel1Factor, rn = 4) AS PremiumDiscountLevel1Factor_4,
	-- *INF*: MAX(PremiumDiscountLevel2Factor, rn=4)
	MAX(PremiumDiscountLevel2Factor, rn = 4) AS PremiumDiscountLevel2Factor_4,
	-- *INF*: MAX(PremiumDiscountLevel3Factor, rn=4)
	MAX(PremiumDiscountLevel3Factor, rn = 4) AS PremiumDiscountLevel3Factor_4,
	-- *INF*: MAX(PremiumDiscountLevel4Factor, rn=4)
	MAX(PremiumDiscountLevel4Factor, rn = 4) AS PremiumDiscountLevel4Factor_4,
	-- *INF*: MAX(PremiumDiscountAveragePercentageDiscount, rn=4)
	MAX(PremiumDiscountAveragePercentageDiscount, rn = 4) AS PremiumDiscountAveragePercentageDiscount_4,
	-- *INF*: MAX(PremiumDiscountLevel1Factor, rn=5)
	MAX(PremiumDiscountLevel1Factor, rn = 5) AS PremiumDiscountLevel1Factor_5,
	-- *INF*: MAX(PremiumDiscountLevel2Factor, rn=5)
	MAX(PremiumDiscountLevel2Factor, rn = 5) AS PremiumDiscountLevel2Factor_5,
	-- *INF*: MAX(PremiumDiscountLevel3Factor, rn=5)
	MAX(PremiumDiscountLevel3Factor, rn = 5) AS PremiumDiscountLevel3Factor_5,
	-- *INF*: MAX(PremiumDiscountLevel4Factor, rn=5)
	MAX(PremiumDiscountLevel4Factor, rn = 5) AS PremiumDiscountLevel4Factor_5,
	-- *INF*: MAX(PremiumDiscountAveragePercentageDiscount, rn=5)
	MAX(PremiumDiscountAveragePercentageDiscount, rn = 5) AS PremiumDiscountAveragePercentageDiscount_5,
	-- *INF*: MAX(PremiumDiscountLevel1Factor, rn=0)
	MAX(PremiumDiscountLevel1Factor, rn = 0) AS PremiumDiscountLevel1Factor_6,
	-- *INF*: MAX(PremiumDiscountLevel2Factor, rn=0)
	MAX(PremiumDiscountLevel2Factor, rn = 0) AS PremiumDiscountLevel2Factor_6,
	-- *INF*: MAX(PremiumDiscountLevel3Factor, rn=0)
	MAX(PremiumDiscountLevel3Factor, rn = 0) AS PremiumDiscountLevel3Factor_6,
	-- *INF*: MAX(PremiumDiscountLevel4Factor, rn=0)
	MAX(PremiumDiscountLevel4Factor, rn = 0) AS PremiumDiscountLevel4Factor_6,
	-- *INF*: MAX(PremiumDiscountAveragePercentageDiscount, rn=0)
	MAX(PremiumDiscountAveragePercentageDiscount, rn = 0) AS PremiumDiscountAveragePercentageDiscount_6,
	Name,
	TransactionEffectiveDate
	FROM SRT_OrderByKeys
	GROUP BY WCTrackHistoryID, FormName, GroupId
),
EXP_AggOutput AS (
	SELECT
	WCTrackHistoryID,
	LinkData,
	CURRENT_TIMESTAMP AS ExtractDate,
	AuditId,
	'30' AS RecordTypeCode,
	ParsedFormName,
	FormName,
	State_1 AS i_State_1,
	State_2 AS i_State_2,
	State_3 AS i_State_3,
	State_4 AS i_State_4,
	State_5 AS i_State_5,
	State_6 AS i_State_6,
	-- *INF*: IIF(not ISNULL(i_State_1) AND length(rtrim(ltrim(i_State_1)))>0,1,0)
	IFF(i_State_1 IS NULL AND length(rtrim(ltrim(i_State_1)))NOT  > 0, 1, 0) AS o_State_1_Valid,
	-- *INF*: IIF(not ISNULL(i_State_2) AND length(rtrim(ltrim(i_State_2)))>0,1,0)
	IFF(i_State_2 IS NULL AND length(rtrim(ltrim(i_State_2)))NOT  > 0, 1, 0) AS o_State_2_Valid,
	-- *INF*: IIF(not ISNULL(i_State_3) AND length(rtrim(ltrim(i_State_3)))>0,1,0)
	IFF(i_State_3 IS NULL AND length(rtrim(ltrim(i_State_3)))NOT  > 0, 1, 0) AS o_State_3_Valid,
	-- *INF*: IIF(not ISNULL(i_State_4) AND length(rtrim(ltrim(i_State_4)))>0,1,0)
	IFF(i_State_4 IS NULL AND length(rtrim(ltrim(i_State_4)))NOT  > 0, 1, 0) AS o_State_4_Valid,
	-- *INF*: IIF(not ISNULL(i_State_5) AND length(rtrim(ltrim(i_State_5)))>0,1,0)
	IFF(i_State_5 IS NULL AND length(rtrim(ltrim(i_State_5)))NOT  > 0, 1, 0) AS o_State_5_Valid,
	-- *INF*: IIF(not ISNULL(i_State_6) AND length(rtrim(ltrim(i_State_6)))>0,1,0)
	IFF(i_State_6 IS NULL AND length(rtrim(ltrim(i_State_6)))NOT  > 0, 1, 0) AS o_State_6_Valid,
	PremiumDiscountLevel1Factor_1 AS i_PremiumDiscountLevel1Factor_1,
	PremiumDiscountLevel2Factor_1 AS i_PremiumDiscountLevel2Factor_1,
	PremiumDiscountLevel3Factor_1 AS i_PremiumDiscountLevel3Factor_1,
	PremiumDiscountLevel4Factor_1 AS i_PremiumDiscountLevel4Factor_1,
	PremiumDiscountAveragePercentageDiscount_1 AS i_PremiumDiscountAveragePercentageDiscount_1,
	PremiumDiscountLevel1Factor_2,
	PremiumDiscountLevel2Factor_2,
	PremiumDiscountLevel3Factor_2,
	PremiumDiscountLevel4Factor_2,
	PremiumDiscountAveragePercentageDiscount_2,
	PremiumDiscountLevel1Factor_3,
	PremiumDiscountLevel2Factor_3,
	PremiumDiscountLevel3Factor_3,
	PremiumDiscountLevel4Factor_3,
	PremiumDiscountAveragePercentageDiscount_3,
	PremiumDiscountLevel1Factor_4,
	PremiumDiscountLevel2Factor_4,
	PremiumDiscountLevel3Factor_4,
	PremiumDiscountLevel4Factor_4,
	PremiumDiscountAveragePercentageDiscount_4,
	PremiumDiscountLevel1Factor_5,
	PremiumDiscountLevel2Factor_5,
	PremiumDiscountLevel3Factor_5,
	PremiumDiscountLevel4Factor_5,
	PremiumDiscountAveragePercentageDiscount_5,
	PremiumDiscountLevel1Factor_6,
	PremiumDiscountLevel2Factor_6,
	PremiumDiscountLevel3Factor_6,
	PremiumDiscountLevel4Factor_6,
	PremiumDiscountAveragePercentageDiscount_6,
	Name,
	TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate
	FROM AGG_GroupByTrackIdFormName
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
mplt_WCPOLS_FormatPremiumFactors_1 AS (WITH
	INPUT_WCPOLS_FormatPremiumFactors AS (
		
	),
	EXP_mplt_Input AS (
		SELECT
		IsValid,
		StateCode,
		PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage,
		'0010' AS FirstPremiumDiscountLayer,
		'0190' AS SecondPremiumDiscountLayer,
		'1550' AS ThirdPremiumDiscountLayer,
		'1550' AS BalancePremiumDiscountLayer
		FROM INPUT_WCPOLS_FormatPremiumFactors
	),
	EXP_ApplyFormatting AS (
		SELECT
		IsValid,
		StateCode AS i_StateCode,
		PremiumDiscountLevel1Factor AS i_PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor AS i_PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor AS i_PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor AS i_PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage AS i_PremiumDiscountAveragePercentage,
		FirstPremiumDiscountLayer AS i_FirstPremiumDiscountLayer,
		SecondPremiumDiscountLayer AS i_SecondPremiumDiscountLayer,
		ThirdPremiumDiscountLayer AS i_ThirdPremiumDiscountLayer,
		BalancePremiumDiscountLayer AS i_BalancePremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1 and NOT ISNULL(i_StateCode),rtrim(ltrim(i_StateCode)),'')
		IFF(IsValid = 1 and i_StateCode IS NOT NULL, rtrim(ltrim(i_StateCode)), '') AS o_StateCode,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel1Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel1Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel1Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel1Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel1Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel2Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel2Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel2Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel2Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel2Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel3Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel3Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel3Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel3Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel3Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel4Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel4Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel4Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel4Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel4Factor,
		-- *INF*: DECODE(TRUE,
		-- --IsValid=1, LPAD(REPLACECHR(0,i_PremiumDiscountAveragePercentage,'.',''),3,'0'),
		-- IsValid=1, LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountAveragePercentage*10)),3,'0'),
		-- '')
		-- 
		-- 
		DECODE(
		    TRUE,
		    IsValid = 1, LPAD(TO_CHAR(CAST(i_PremiumDiscountAveragePercentage * 10 AS INTEGER)), 3, '0'),
		    ''
		) AS o_PremiumDiscountAveragePercentage,
		-- *INF*: IIF(IsValid=1, i_FirstPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_FirstPremiumDiscountLayer, '') AS o_FirstPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_SecondPremiumDiscountLayer, '')
		IFF(IsValid = 1, i_SecondPremiumDiscountLayer, '') AS o_SecondPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_ThirdPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_ThirdPremiumDiscountLayer, '') AS o_ThirdPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1,i_BalancePremiumDiscountLayer,'')
		IFF(IsValid = 1, i_BalancePremiumDiscountLayer, '') AS o_BalancePremiumDiscountLayer
		FROM EXP_mplt_Input
	),
	OUTPUT_WCPOLS_FormatPremiumFactors AS (
		SELECT
		o_StateCode AS StateCode, 
		o_PremiumDiscountLevel1Factor AS PremiumDiscountLevel1Factor, 
		o_PremiumDiscountLevel2Factor AS PremiumDiscountLevel2Factor, 
		o_PremiumDiscountLevel3Factor AS PremiumDiscountLevel3Factor, 
		o_PremiumDiscountLevel4Factor AS PremiumDiscountLevel4Factor, 
		o_PremiumDiscountAveragePercentage AS PremiumDiscountAveragePercentage, 
		o_FirstPremiumDiscountLayer AS FirstPremiumDiscountLayer, 
		o_SecondPremiumDiscountLayer AS SecondPremiumDiscountLayer, 
		o_ThirdPremiumDiscountLayer AS ThirdPremiumDiscountLayer, 
		o_BalancePremiumDiscountLayer AS BalancePremiumDiscountLayer
		FROM EXP_ApplyFormatting
	),
),
mplt_WCPOLS_FormatPremiumFactors_2 AS (WITH
	INPUT_WCPOLS_FormatPremiumFactors AS (
		
	),
	EXP_mplt_Input AS (
		SELECT
		IsValid,
		StateCode,
		PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage,
		'0010' AS FirstPremiumDiscountLayer,
		'0190' AS SecondPremiumDiscountLayer,
		'1550' AS ThirdPremiumDiscountLayer,
		'1550' AS BalancePremiumDiscountLayer
		FROM INPUT_WCPOLS_FormatPremiumFactors
	),
	EXP_ApplyFormatting AS (
		SELECT
		IsValid,
		StateCode AS i_StateCode,
		PremiumDiscountLevel1Factor AS i_PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor AS i_PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor AS i_PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor AS i_PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage AS i_PremiumDiscountAveragePercentage,
		FirstPremiumDiscountLayer AS i_FirstPremiumDiscountLayer,
		SecondPremiumDiscountLayer AS i_SecondPremiumDiscountLayer,
		ThirdPremiumDiscountLayer AS i_ThirdPremiumDiscountLayer,
		BalancePremiumDiscountLayer AS i_BalancePremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1 and NOT ISNULL(i_StateCode),rtrim(ltrim(i_StateCode)),'')
		IFF(IsValid = 1 and i_StateCode IS NOT NULL, rtrim(ltrim(i_StateCode)), '') AS o_StateCode,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel1Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel1Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel1Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel1Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel1Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel2Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel2Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel2Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel2Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel2Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel3Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel3Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel3Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel3Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel3Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel4Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel4Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel4Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel4Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel4Factor,
		-- *INF*: DECODE(TRUE,
		-- --IsValid=1, LPAD(REPLACECHR(0,i_PremiumDiscountAveragePercentage,'.',''),3,'0'),
		-- IsValid=1, LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountAveragePercentage*10)),3,'0'),
		-- '')
		-- 
		-- 
		DECODE(
		    TRUE,
		    IsValid = 1, LPAD(TO_CHAR(CAST(i_PremiumDiscountAveragePercentage * 10 AS INTEGER)), 3, '0'),
		    ''
		) AS o_PremiumDiscountAveragePercentage,
		-- *INF*: IIF(IsValid=1, i_FirstPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_FirstPremiumDiscountLayer, '') AS o_FirstPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_SecondPremiumDiscountLayer, '')
		IFF(IsValid = 1, i_SecondPremiumDiscountLayer, '') AS o_SecondPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_ThirdPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_ThirdPremiumDiscountLayer, '') AS o_ThirdPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1,i_BalancePremiumDiscountLayer,'')
		IFF(IsValid = 1, i_BalancePremiumDiscountLayer, '') AS o_BalancePremiumDiscountLayer
		FROM EXP_mplt_Input
	),
	OUTPUT_WCPOLS_FormatPremiumFactors AS (
		SELECT
		o_StateCode AS StateCode, 
		o_PremiumDiscountLevel1Factor AS PremiumDiscountLevel1Factor, 
		o_PremiumDiscountLevel2Factor AS PremiumDiscountLevel2Factor, 
		o_PremiumDiscountLevel3Factor AS PremiumDiscountLevel3Factor, 
		o_PremiumDiscountLevel4Factor AS PremiumDiscountLevel4Factor, 
		o_PremiumDiscountAveragePercentage AS PremiumDiscountAveragePercentage, 
		o_FirstPremiumDiscountLayer AS FirstPremiumDiscountLayer, 
		o_SecondPremiumDiscountLayer AS SecondPremiumDiscountLayer, 
		o_ThirdPremiumDiscountLayer AS ThirdPremiumDiscountLayer, 
		o_BalancePremiumDiscountLayer AS BalancePremiumDiscountLayer
		FROM EXP_ApplyFormatting
	),
),
mplt_WCPOLS_FormatPremiumFactors_3 AS (WITH
	INPUT_WCPOLS_FormatPremiumFactors AS (
		
	),
	EXP_mplt_Input AS (
		SELECT
		IsValid,
		StateCode,
		PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage,
		'0010' AS FirstPremiumDiscountLayer,
		'0190' AS SecondPremiumDiscountLayer,
		'1550' AS ThirdPremiumDiscountLayer,
		'1550' AS BalancePremiumDiscountLayer
		FROM INPUT_WCPOLS_FormatPremiumFactors
	),
	EXP_ApplyFormatting AS (
		SELECT
		IsValid,
		StateCode AS i_StateCode,
		PremiumDiscountLevel1Factor AS i_PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor AS i_PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor AS i_PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor AS i_PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage AS i_PremiumDiscountAveragePercentage,
		FirstPremiumDiscountLayer AS i_FirstPremiumDiscountLayer,
		SecondPremiumDiscountLayer AS i_SecondPremiumDiscountLayer,
		ThirdPremiumDiscountLayer AS i_ThirdPremiumDiscountLayer,
		BalancePremiumDiscountLayer AS i_BalancePremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1 and NOT ISNULL(i_StateCode),rtrim(ltrim(i_StateCode)),'')
		IFF(IsValid = 1 and i_StateCode IS NOT NULL, rtrim(ltrim(i_StateCode)), '') AS o_StateCode,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel1Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel1Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel1Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel1Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel1Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel2Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel2Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel2Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel2Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel2Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel3Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel3Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel3Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel3Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel3Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel4Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel4Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel4Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel4Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel4Factor,
		-- *INF*: DECODE(TRUE,
		-- --IsValid=1, LPAD(REPLACECHR(0,i_PremiumDiscountAveragePercentage,'.',''),3,'0'),
		-- IsValid=1, LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountAveragePercentage*10)),3,'0'),
		-- '')
		-- 
		-- 
		DECODE(
		    TRUE,
		    IsValid = 1, LPAD(TO_CHAR(CAST(i_PremiumDiscountAveragePercentage * 10 AS INTEGER)), 3, '0'),
		    ''
		) AS o_PremiumDiscountAveragePercentage,
		-- *INF*: IIF(IsValid=1, i_FirstPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_FirstPremiumDiscountLayer, '') AS o_FirstPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_SecondPremiumDiscountLayer, '')
		IFF(IsValid = 1, i_SecondPremiumDiscountLayer, '') AS o_SecondPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_ThirdPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_ThirdPremiumDiscountLayer, '') AS o_ThirdPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1,i_BalancePremiumDiscountLayer,'')
		IFF(IsValid = 1, i_BalancePremiumDiscountLayer, '') AS o_BalancePremiumDiscountLayer
		FROM EXP_mplt_Input
	),
	OUTPUT_WCPOLS_FormatPremiumFactors AS (
		SELECT
		o_StateCode AS StateCode, 
		o_PremiumDiscountLevel1Factor AS PremiumDiscountLevel1Factor, 
		o_PremiumDiscountLevel2Factor AS PremiumDiscountLevel2Factor, 
		o_PremiumDiscountLevel3Factor AS PremiumDiscountLevel3Factor, 
		o_PremiumDiscountLevel4Factor AS PremiumDiscountLevel4Factor, 
		o_PremiumDiscountAveragePercentage AS PremiumDiscountAveragePercentage, 
		o_FirstPremiumDiscountLayer AS FirstPremiumDiscountLayer, 
		o_SecondPremiumDiscountLayer AS SecondPremiumDiscountLayer, 
		o_ThirdPremiumDiscountLayer AS ThirdPremiumDiscountLayer, 
		o_BalancePremiumDiscountLayer AS BalancePremiumDiscountLayer
		FROM EXP_ApplyFormatting
	),
),
mplt_WCPOLS_FormatPremiumFactors_4 AS (WITH
	INPUT_WCPOLS_FormatPremiumFactors AS (
		
	),
	EXP_mplt_Input AS (
		SELECT
		IsValid,
		StateCode,
		PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage,
		'0010' AS FirstPremiumDiscountLayer,
		'0190' AS SecondPremiumDiscountLayer,
		'1550' AS ThirdPremiumDiscountLayer,
		'1550' AS BalancePremiumDiscountLayer
		FROM INPUT_WCPOLS_FormatPremiumFactors
	),
	EXP_ApplyFormatting AS (
		SELECT
		IsValid,
		StateCode AS i_StateCode,
		PremiumDiscountLevel1Factor AS i_PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor AS i_PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor AS i_PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor AS i_PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage AS i_PremiumDiscountAveragePercentage,
		FirstPremiumDiscountLayer AS i_FirstPremiumDiscountLayer,
		SecondPremiumDiscountLayer AS i_SecondPremiumDiscountLayer,
		ThirdPremiumDiscountLayer AS i_ThirdPremiumDiscountLayer,
		BalancePremiumDiscountLayer AS i_BalancePremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1 and NOT ISNULL(i_StateCode),rtrim(ltrim(i_StateCode)),'')
		IFF(IsValid = 1 and i_StateCode IS NOT NULL, rtrim(ltrim(i_StateCode)), '') AS o_StateCode,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel1Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel1Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel1Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel1Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel1Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel2Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel2Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel2Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel2Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel2Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel3Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel3Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel3Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel3Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel3Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel4Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel4Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel4Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel4Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel4Factor,
		-- *INF*: DECODE(TRUE,
		-- --IsValid=1, LPAD(REPLACECHR(0,i_PremiumDiscountAveragePercentage,'.',''),3,'0'),
		-- IsValid=1, LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountAveragePercentage*10)),3,'0'),
		-- '')
		-- 
		-- 
		DECODE(
		    TRUE,
		    IsValid = 1, LPAD(TO_CHAR(CAST(i_PremiumDiscountAveragePercentage * 10 AS INTEGER)), 3, '0'),
		    ''
		) AS o_PremiumDiscountAveragePercentage,
		-- *INF*: IIF(IsValid=1, i_FirstPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_FirstPremiumDiscountLayer, '') AS o_FirstPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_SecondPremiumDiscountLayer, '')
		IFF(IsValid = 1, i_SecondPremiumDiscountLayer, '') AS o_SecondPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_ThirdPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_ThirdPremiumDiscountLayer, '') AS o_ThirdPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1,i_BalancePremiumDiscountLayer,'')
		IFF(IsValid = 1, i_BalancePremiumDiscountLayer, '') AS o_BalancePremiumDiscountLayer
		FROM EXP_mplt_Input
	),
	OUTPUT_WCPOLS_FormatPremiumFactors AS (
		SELECT
		o_StateCode AS StateCode, 
		o_PremiumDiscountLevel1Factor AS PremiumDiscountLevel1Factor, 
		o_PremiumDiscountLevel2Factor AS PremiumDiscountLevel2Factor, 
		o_PremiumDiscountLevel3Factor AS PremiumDiscountLevel3Factor, 
		o_PremiumDiscountLevel4Factor AS PremiumDiscountLevel4Factor, 
		o_PremiumDiscountAveragePercentage AS PremiumDiscountAveragePercentage, 
		o_FirstPremiumDiscountLayer AS FirstPremiumDiscountLayer, 
		o_SecondPremiumDiscountLayer AS SecondPremiumDiscountLayer, 
		o_ThirdPremiumDiscountLayer AS ThirdPremiumDiscountLayer, 
		o_BalancePremiumDiscountLayer AS BalancePremiumDiscountLayer
		FROM EXP_ApplyFormatting
	),
),
mplt_WCPOLS_FormatPremiumFactors_5 AS (WITH
	INPUT_WCPOLS_FormatPremiumFactors AS (
		
	),
	EXP_mplt_Input AS (
		SELECT
		IsValid,
		StateCode,
		PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage,
		'0010' AS FirstPremiumDiscountLayer,
		'0190' AS SecondPremiumDiscountLayer,
		'1550' AS ThirdPremiumDiscountLayer,
		'1550' AS BalancePremiumDiscountLayer
		FROM INPUT_WCPOLS_FormatPremiumFactors
	),
	EXP_ApplyFormatting AS (
		SELECT
		IsValid,
		StateCode AS i_StateCode,
		PremiumDiscountLevel1Factor AS i_PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor AS i_PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor AS i_PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor AS i_PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage AS i_PremiumDiscountAveragePercentage,
		FirstPremiumDiscountLayer AS i_FirstPremiumDiscountLayer,
		SecondPremiumDiscountLayer AS i_SecondPremiumDiscountLayer,
		ThirdPremiumDiscountLayer AS i_ThirdPremiumDiscountLayer,
		BalancePremiumDiscountLayer AS i_BalancePremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1 and NOT ISNULL(i_StateCode),rtrim(ltrim(i_StateCode)),'')
		IFF(IsValid = 1 and i_StateCode IS NOT NULL, rtrim(ltrim(i_StateCode)), '') AS o_StateCode,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel1Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel1Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel1Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel1Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel1Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel2Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel2Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel2Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel2Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel2Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel3Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel3Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel3Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel3Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel3Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel4Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel4Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel4Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel4Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel4Factor,
		-- *INF*: DECODE(TRUE,
		-- --IsValid=1, LPAD(REPLACECHR(0,i_PremiumDiscountAveragePercentage,'.',''),3,'0'),
		-- IsValid=1, LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountAveragePercentage*10)),3,'0'),
		-- '')
		-- 
		-- 
		DECODE(
		    TRUE,
		    IsValid = 1, LPAD(TO_CHAR(CAST(i_PremiumDiscountAveragePercentage * 10 AS INTEGER)), 3, '0'),
		    ''
		) AS o_PremiumDiscountAveragePercentage,
		-- *INF*: IIF(IsValid=1, i_FirstPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_FirstPremiumDiscountLayer, '') AS o_FirstPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_SecondPremiumDiscountLayer, '')
		IFF(IsValid = 1, i_SecondPremiumDiscountLayer, '') AS o_SecondPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_ThirdPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_ThirdPremiumDiscountLayer, '') AS o_ThirdPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1,i_BalancePremiumDiscountLayer,'')
		IFF(IsValid = 1, i_BalancePremiumDiscountLayer, '') AS o_BalancePremiumDiscountLayer
		FROM EXP_mplt_Input
	),
	OUTPUT_WCPOLS_FormatPremiumFactors AS (
		SELECT
		o_StateCode AS StateCode, 
		o_PremiumDiscountLevel1Factor AS PremiumDiscountLevel1Factor, 
		o_PremiumDiscountLevel2Factor AS PremiumDiscountLevel2Factor, 
		o_PremiumDiscountLevel3Factor AS PremiumDiscountLevel3Factor, 
		o_PremiumDiscountLevel4Factor AS PremiumDiscountLevel4Factor, 
		o_PremiumDiscountAveragePercentage AS PremiumDiscountAveragePercentage, 
		o_FirstPremiumDiscountLayer AS FirstPremiumDiscountLayer, 
		o_SecondPremiumDiscountLayer AS SecondPremiumDiscountLayer, 
		o_ThirdPremiumDiscountLayer AS ThirdPremiumDiscountLayer, 
		o_BalancePremiumDiscountLayer AS BalancePremiumDiscountLayer
		FROM EXP_ApplyFormatting
	),
),
mplt_WCPOLS_FormatPremiumFactors_6 AS (WITH
	INPUT_WCPOLS_FormatPremiumFactors AS (
		
	),
	EXP_mplt_Input AS (
		SELECT
		IsValid,
		StateCode,
		PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage,
		'0010' AS FirstPremiumDiscountLayer,
		'0190' AS SecondPremiumDiscountLayer,
		'1550' AS ThirdPremiumDiscountLayer,
		'1550' AS BalancePremiumDiscountLayer
		FROM INPUT_WCPOLS_FormatPremiumFactors
	),
	EXP_ApplyFormatting AS (
		SELECT
		IsValid,
		StateCode AS i_StateCode,
		PremiumDiscountLevel1Factor AS i_PremiumDiscountLevel1Factor,
		PremiumDiscountLevel2Factor AS i_PremiumDiscountLevel2Factor,
		PremiumDiscountLevel3Factor AS i_PremiumDiscountLevel3Factor,
		PremiumDiscountLevel4Factor AS i_PremiumDiscountLevel4Factor,
		PremiumDiscountAveragePercentage AS i_PremiumDiscountAveragePercentage,
		FirstPremiumDiscountLayer AS i_FirstPremiumDiscountLayer,
		SecondPremiumDiscountLayer AS i_SecondPremiumDiscountLayer,
		ThirdPremiumDiscountLayer AS i_ThirdPremiumDiscountLayer,
		BalancePremiumDiscountLayer AS i_BalancePremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1 and NOT ISNULL(i_StateCode),rtrim(ltrim(i_StateCode)),'')
		IFF(IsValid = 1 and i_StateCode IS NOT NULL, rtrim(ltrim(i_StateCode)), '') AS o_StateCode,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel1Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel1Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel1Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel1Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel1Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel2Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel2Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel2Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel2Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel2Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel3Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel3Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel3Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel3Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel3Factor,
		-- *INF*: DECODE(TRUE,
		-- IsValid=1 AND NOT ISNULL(i_PremiumDiscountLevel4Factor),LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountLevel4Factor*1000)),3,'0'),
		-- IsValid=1,'000',
		-- ''
		-- )
		DECODE(
		    TRUE,
		    IsValid = 1 AND i_PremiumDiscountLevel4Factor IS NOT NULL, LPAD(TO_CHAR(CAST(i_PremiumDiscountLevel4Factor * 1000 AS INTEGER)), 3, '0'),
		    IsValid = 1, '000',
		    ''
		) AS o_PremiumDiscountLevel4Factor,
		-- *INF*: DECODE(TRUE,
		-- --IsValid=1, LPAD(REPLACECHR(0,i_PremiumDiscountAveragePercentage,'.',''),3,'0'),
		-- IsValid=1, LPAD(TO_CHAR(TO_INTEGER(i_PremiumDiscountAveragePercentage*10)),3,'0'),
		-- '')
		-- 
		-- 
		DECODE(
		    TRUE,
		    IsValid = 1, LPAD(TO_CHAR(CAST(i_PremiumDiscountAveragePercentage * 10 AS INTEGER)), 3, '0'),
		    ''
		) AS o_PremiumDiscountAveragePercentage,
		-- *INF*: IIF(IsValid=1, i_FirstPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_FirstPremiumDiscountLayer, '') AS o_FirstPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_SecondPremiumDiscountLayer, '')
		IFF(IsValid = 1, i_SecondPremiumDiscountLayer, '') AS o_SecondPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1, i_ThirdPremiumDiscountLayer,'')
		IFF(IsValid = 1, i_ThirdPremiumDiscountLayer, '') AS o_ThirdPremiumDiscountLayer,
		-- *INF*: IIF(IsValid=1,i_BalancePremiumDiscountLayer,'')
		IFF(IsValid = 1, i_BalancePremiumDiscountLayer, '') AS o_BalancePremiumDiscountLayer
		FROM EXP_mplt_Input
	),
	OUTPUT_WCPOLS_FormatPremiumFactors AS (
		SELECT
		o_StateCode AS StateCode, 
		o_PremiumDiscountLevel1Factor AS PremiumDiscountLevel1Factor, 
		o_PremiumDiscountLevel2Factor AS PremiumDiscountLevel2Factor, 
		o_PremiumDiscountLevel3Factor AS PremiumDiscountLevel3Factor, 
		o_PremiumDiscountLevel4Factor AS PremiumDiscountLevel4Factor, 
		o_PremiumDiscountAveragePercentage AS PremiumDiscountAveragePercentage, 
		o_FirstPremiumDiscountLayer AS FirstPremiumDiscountLayer, 
		o_SecondPremiumDiscountLayer AS SecondPremiumDiscountLayer, 
		o_ThirdPremiumDiscountLayer AS ThirdPremiumDiscountLayer, 
		o_BalancePremiumDiscountLayer AS BalancePremiumDiscountLayer
		FROM EXP_ApplyFormatting
	),
),
WCPols30Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols30Record
	WHERE AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols30Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, StateCode, FirstPremiumDiscountLayer, FirstPremiumDiscountPercentage, SecondNextPremiumDiscountLayer, SecondNextPremiumDiscountPercentage, ThirdNextPremiumDiscountLayer, ThirdNextPremiumDiscountPercentage, BalancePremiumDiscountLayer, BalancePremiumDiscountPercentage, AveragePercentageDiscount, StateCode2, FirstPremiumDiscountLayer2, FirstPremiumDiscountPercentage2, SecondNextPremiumDiscountLayer2, SecondNextPremiumDiscountPercentage2, ThirdNextPremiumDiscountLayer2, ThirdNextPremiumDiscountPercentage2, BalancePremiumDiscountLayer2, BalancePremiumDiscountPercentage2, StateCode3, FirstPremiumDiscountLayer3, FirstPremiumDiscountPercentage3, SecondNextPremiumDiscountLayer3, SecondNextPremiumDiscountPercentage3, ThirdNextPremiumDiscountLayer3, ThirdNextPremiumDiscountPercentage3, BalancePremiumDiscountLayer3, BalancePremiumDiscountPercentage3, StateCode4, FirstPremiumDiscountLayer4, FirstPremiumDiscountPercentage4, SecondNextPremiumDiscountLayer4, SecondNextPremiumDiscountPercentage4, ThirdNextPremiumDiscountLayer4, ThirdNextPremiumDiscountPercentage4, BalancePremiumDiscountLayer4, BalancePremiumDiscountPercentage4, StateCode5, FirstPremiumDiscountLayer5, FirstPremiumDiscountPercentage5, SecondNextPremiumDiscountLayer5, SecondNextPremiumDiscountPercentage5, ThirdNextPremiumDiscountLayer5, ThirdNextPremiumDiscountPercentage5, BalancePremiumDiscountLayer5, BalancePremiumDiscountPercentage5, StateCode6, FirstPremiumDiscountLayer6, FirstPremiumDiscountPercentage6, SecondNextPremiumDiscountLayer6, SecondNextPremiumDiscountPercentage6, ThirdNextPremiumDiscountLayer6, ThirdNextPremiumDiscountPercentage6, BalancePremiumDiscountLayer6, BalancePremiumDiscountPercentage6, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXP_AggOutput.EXTRACTDATE, 
	EXP_AggOutput.AUDITID, 
	EXP_AggOutput.WCTRACKHISTORYID, 
	EXP_AggOutput.LINKDATA, 
	EXP_AggOutput.RECORDTYPECODE, 
	mplt_Parse_FormNameField.ParsedNameOfForm1 AS ENDORSEMENTNUMBER, 
	mplt_Parse_FormNameField.BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	mplt_Parse_FormNameField.FormEdition AS CARRIERVERSIONIDENTIFIER, 
	mplt_WCPOLS_FormatPremiumFactors_1.StateCode1 AS STATECODE, 
	mplt_WCPOLS_FormatPremiumFactors_1.FIRSTPREMIUMDISCOUNTLAYER, 
	mplt_WCPOLS_FormatPremiumFactors_1.PremiumDiscountLevel1Factor1 AS FIRSTPREMIUMDISCOUNTPERCENTAGE, 
	mplt_WCPOLS_FormatPremiumFactors_1.SecondPremiumDiscountLayer AS SECONDNEXTPREMIUMDISCOUNTLAYER, 
	mplt_WCPOLS_FormatPremiumFactors_1.PremiumDiscountLevel2Factor1 AS SECONDNEXTPREMIUMDISCOUNTPERCENTAGE, 
	mplt_WCPOLS_FormatPremiumFactors_1.ThirdPremiumDiscountLayer AS THIRDNEXTPREMIUMDISCOUNTLAYER, 
	mplt_WCPOLS_FormatPremiumFactors_1.PremiumDiscountLevel3Factor1 AS THIRDNEXTPREMIUMDISCOUNTPERCENTAGE, 
	mplt_WCPOLS_FormatPremiumFactors_1.BALANCEPREMIUMDISCOUNTLAYER, 
	mplt_WCPOLS_FormatPremiumFactors_1.PremiumDiscountLevel4Factor1 AS BALANCEPREMIUMDISCOUNTPERCENTAGE, 
	mplt_WCPOLS_FormatPremiumFactors_1.PremiumDiscountAveragePercentage1 AS AVERAGEPERCENTAGEDISCOUNT, 
	mplt_WCPOLS_FormatPremiumFactors_2.StateCode1 AS STATECODE2, 
	mplt_WCPOLS_FormatPremiumFactors_2.FirstPremiumDiscountLayer AS FIRSTPREMIUMDISCOUNTLAYER2, 
	mplt_WCPOLS_FormatPremiumFactors_2.PremiumDiscountLevel1Factor1 AS FIRSTPREMIUMDISCOUNTPERCENTAGE2, 
	mplt_WCPOLS_FormatPremiumFactors_2.SecondPremiumDiscountLayer AS SECONDNEXTPREMIUMDISCOUNTLAYER2, 
	mplt_WCPOLS_FormatPremiumFactors_2.PremiumDiscountLevel2Factor1 AS SECONDNEXTPREMIUMDISCOUNTPERCENTAGE2, 
	mplt_WCPOLS_FormatPremiumFactors_2.ThirdPremiumDiscountLayer AS THIRDNEXTPREMIUMDISCOUNTLAYER2, 
	mplt_WCPOLS_FormatPremiumFactors_2.PremiumDiscountLevel3Factor1 AS THIRDNEXTPREMIUMDISCOUNTPERCENTAGE2, 
	mplt_WCPOLS_FormatPremiumFactors_2.BalancePremiumDiscountLayer AS BALANCEPREMIUMDISCOUNTLAYER2, 
	mplt_WCPOLS_FormatPremiumFactors_2.PremiumDiscountLevel4Factor1 AS BALANCEPREMIUMDISCOUNTPERCENTAGE2, 
	mplt_WCPOLS_FormatPremiumFactors_3.StateCode1 AS STATECODE3, 
	mplt_WCPOLS_FormatPremiumFactors_3.FirstPremiumDiscountLayer AS FIRSTPREMIUMDISCOUNTLAYER3, 
	mplt_WCPOLS_FormatPremiumFactors_3.PremiumDiscountLevel1Factor1 AS FIRSTPREMIUMDISCOUNTPERCENTAGE3, 
	mplt_WCPOLS_FormatPremiumFactors_3.SecondPremiumDiscountLayer AS SECONDNEXTPREMIUMDISCOUNTLAYER3, 
	mplt_WCPOLS_FormatPremiumFactors_3.PremiumDiscountLevel2Factor1 AS SECONDNEXTPREMIUMDISCOUNTPERCENTAGE3, 
	mplt_WCPOLS_FormatPremiumFactors_3.ThirdPremiumDiscountLayer AS THIRDNEXTPREMIUMDISCOUNTLAYER3, 
	mplt_WCPOLS_FormatPremiumFactors_3.PremiumDiscountLevel3Factor1 AS THIRDNEXTPREMIUMDISCOUNTPERCENTAGE3, 
	mplt_WCPOLS_FormatPremiumFactors_3.BalancePremiumDiscountLayer AS BALANCEPREMIUMDISCOUNTLAYER3, 
	mplt_WCPOLS_FormatPremiumFactors_3.PremiumDiscountLevel4Factor1 AS BALANCEPREMIUMDISCOUNTPERCENTAGE3, 
	mplt_WCPOLS_FormatPremiumFactors_4.StateCode1 AS STATECODE4, 
	mplt_WCPOLS_FormatPremiumFactors_4.FirstPremiumDiscountLayer AS FIRSTPREMIUMDISCOUNTLAYER4, 
	mplt_WCPOLS_FormatPremiumFactors_4.PremiumDiscountLevel1Factor1 AS FIRSTPREMIUMDISCOUNTPERCENTAGE4, 
	mplt_WCPOLS_FormatPremiumFactors_4.SecondPremiumDiscountLayer AS SECONDNEXTPREMIUMDISCOUNTLAYER4, 
	mplt_WCPOLS_FormatPremiumFactors_4.PremiumDiscountLevel2Factor1 AS SECONDNEXTPREMIUMDISCOUNTPERCENTAGE4, 
	mplt_WCPOLS_FormatPremiumFactors_4.ThirdPremiumDiscountLayer AS THIRDNEXTPREMIUMDISCOUNTLAYER4, 
	mplt_WCPOLS_FormatPremiumFactors_4.PremiumDiscountLevel3Factor1 AS THIRDNEXTPREMIUMDISCOUNTPERCENTAGE4, 
	mplt_WCPOLS_FormatPremiumFactors_4.BalancePremiumDiscountLayer AS BALANCEPREMIUMDISCOUNTLAYER4, 
	mplt_WCPOLS_FormatPremiumFactors_4.PremiumDiscountLevel4Factor1 AS BALANCEPREMIUMDISCOUNTPERCENTAGE4, 
	mplt_WCPOLS_FormatPremiumFactors_5.StateCode1 AS STATECODE5, 
	mplt_WCPOLS_FormatPremiumFactors_5.FirstPremiumDiscountLayer AS FIRSTPREMIUMDISCOUNTLAYER5, 
	mplt_WCPOLS_FormatPremiumFactors_5.PremiumDiscountLevel1Factor1 AS FIRSTPREMIUMDISCOUNTPERCENTAGE5, 
	mplt_WCPOLS_FormatPremiumFactors_5.SecondPremiumDiscountLayer AS SECONDNEXTPREMIUMDISCOUNTLAYER5, 
	mplt_WCPOLS_FormatPremiumFactors_5.PremiumDiscountLevel2Factor1 AS SECONDNEXTPREMIUMDISCOUNTPERCENTAGE5, 
	mplt_WCPOLS_FormatPremiumFactors_5.ThirdPremiumDiscountLayer AS THIRDNEXTPREMIUMDISCOUNTLAYER5, 
	mplt_WCPOLS_FormatPremiumFactors_5.PremiumDiscountLevel3Factor1 AS THIRDNEXTPREMIUMDISCOUNTPERCENTAGE5, 
	mplt_WCPOLS_FormatPremiumFactors_5.BalancePremiumDiscountLayer AS BALANCEPREMIUMDISCOUNTLAYER5, 
	mplt_WCPOLS_FormatPremiumFactors_5.PremiumDiscountLevel4Factor1 AS BALANCEPREMIUMDISCOUNTPERCENTAGE5, 
	mplt_WCPOLS_FormatPremiumFactors_6.StateCode1 AS STATECODE6, 
	mplt_WCPOLS_FormatPremiumFactors_6.FirstPremiumDiscountLayer AS FIRSTPREMIUMDISCOUNTLAYER6, 
	mplt_WCPOLS_FormatPremiumFactors_6.PremiumDiscountLevel1Factor1 AS FIRSTPREMIUMDISCOUNTPERCENTAGE6, 
	mplt_WCPOLS_FormatPremiumFactors_6.SecondPremiumDiscountLayer AS SECONDNEXTPREMIUMDISCOUNTLAYER6, 
	mplt_WCPOLS_FormatPremiumFactors_6.PremiumDiscountLevel2Factor1 AS SECONDNEXTPREMIUMDISCOUNTPERCENTAGE6, 
	mplt_WCPOLS_FormatPremiumFactors_6.ThirdPremiumDiscountLayer AS THIRDNEXTPREMIUMDISCOUNTLAYER6, 
	mplt_WCPOLS_FormatPremiumFactors_6.PremiumDiscountLevel3Factor1 AS THIRDNEXTPREMIUMDISCOUNTPERCENTAGE6, 
	mplt_WCPOLS_FormatPremiumFactors_6.BalancePremiumDiscountLayer AS BALANCEPREMIUMDISCOUNTLAYER6, 
	mplt_WCPOLS_FormatPremiumFactors_6.PremiumDiscountLevel4Factor1 AS BALANCEPREMIUMDISCOUNTPERCENTAGE6, 
	EXP_AggOutput.Name AS NAMEOFINSURED, 
	EXP_AggOutput.o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM mplt_WCPOLS_FormatPremiumFactors_5
),