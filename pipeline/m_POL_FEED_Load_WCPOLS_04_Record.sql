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
LKP_SupClaimAdministratorFEIN AS (
	SELECT
	ClaimAdministratorFEIN,
	StateCode
	FROM (
		SELECT
			 ClaimAdministratorFEIN as ClaimAdministratorFEIN,
			 StateCode as StateCode
		FROM SupClaimAdministratorFEIN
		WHERE ClaimAdministratorFEIN <> 'Monopolistic'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateCode ORDER BY ClaimAdministratorFEIN) = 1
),
SQ_WorkWCTrackHistory AS (
	SELECT 
		 WCTrackHistoryID
		,[State]
		,TransactionType
		,BureauNumber
		,ExperienceModificationFactorMeritRatingFactor
		,ExpenseConstant
		,PremiumDiscount
		,TransactionEffectiveDate
		,TransactionExpirationDate
		,PolicyEffectiveDate
		,PolicyExpirationDate
		,PolicyKey
		,ReasonCode
		,TotalStandardPremium
		,ExperienceModType
		,PeriodStartDate
		,PeriodEndDate
		,TermType
		,ExperienceModEffectiveDate
		,AnniversaryRatingDate
		,AnniversaryRating
		,AuditPeriod
	    ,WCStateAddedThisTransaction
		,StateDeletedFlag
		,SUM(ExpenseConstant) OVER(PARTITION BY WCTrackHistoryID, State) ROLLUP_ExpenseConstant
		,SUM(PremiumDiscount) OVER(PARTITION BY WCTrackHistoryID, State) ROLLUP_PremiumDiscount
		,SUM(TotalStandardPremium) OVER(PARTITION BY WCTrackHistoryID, State) ROLLUP_TotalStandardPremium
		,MIN(PeriodStartDate) OVER(PARTITION BY WCTrackHistoryID, State) FIRST_PeriodStartDate
		,MAX(PeriodStartDate) OVER(PARTITION BY WCTrackHistoryID, State) LAST_PeriodStartDate
		,MIN(TransactionEffectiveDate) OVER(PARTITION BY WCTrackHistoryID, State) FIRST_TransactionEffectiveDate
		,MAX(CASE WHEN TermType in ('ARD','EMF') THEN '1' ELSE '0' END)  OVER(PARTITION BY PolicyKey,State) SplitRated
	FROM
	(
	SELECT
		 st.WCTrackHistoryID
		,st.[State]
		,th.TransactionType
		,PE.BureauNumber
		,st.ExperienceModificationFactorMeritRatingFactor
		,max(IIF(c_EC.ParentCoverageType = 'ExpenseConstant', c_EC.Premium, 0)) as ExpenseConstant
		,max(IIF(c_EC.ParentCoverageType = 'PremiumDiscount', ABS(c_EC.Premium), 0)) as PremiumDiscount
		,pol.TransactionEffectiveDate
		,pol.TransactionExpirationDate
		,pol.PolicyEffectiveDate
		,pol.PolicyExpirationDate
		,pol.PolicyKey
		,th.ReasonCode
		,TotalStandardPremium
		,st.ExperienceModType
		,st.PeriodStartDate
		,st.PeriodEndDate
		,st.TermType
		,st.ExperienceModEffectiveDate
		,lin.AnniversaryRatingDate
	      ,lin.AnniversaryRating
		,lin.AuditPeriod
		,st.WCStateAddedThisTransaction
		,st.StateDeletedFlag
	
	FROM dbo.WorkWCStateTerm st
	LEFT JOIN dbo.WorkWCCoverage c_EC
		ON st.WCTrackHistoryID = c_EC.WCTrackHistoryID
			AND st.WC_StateTermId = c_EC.ParentObjectId
			AND c_EC.ParentObjectName = 'DC_WC_StateTerm'
			AND c_EC.ParentCoverageType IN ('ExpenseConstant', 'PremiumDiscount')
	             AND c_EC.ParentCoverageDeleteFlag=0	
	
	INNER JOIN dbo.WorkWCTrackHistory th
		ON th.WCTrackHistoryID = st.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCLine lin
		ON lin.WCTrackHistoryID = st.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCParty p
		ON st.WCTrackHistoryID = p.WCTrackHistoryID
			AND p.PartyAssociationType = 'Account'
	
	LEFT JOIN dbo.WorkWCParty PE
		ON P.WCTrackHistoryID=PE.WCTrackHistoryID
		and ISNULL(P.Name,'N/A')=ISNULL(PE.Name,'N/A')  and P.Name IS NOT NULL and PE.Name IS NOT NULL
			AND PE.PartyAssociationType = 'PrimaryEntity'
	
	INNER JOIN dbo.WorkWCPolicy pol
		ON st.WCTrackHistoryID = pol.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCLocation loc
		ON st.WCTrackHistoryID = loc.WCTrackHistoryID
		and st.[State]=loc.StateProv
		AND (loc.LocationDeletedIndicator != 1 or st.StateDeletedFlag=1)
	
	WHERE 1 = 1
	AND st.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_04}
	
	GROUP BY st.WCTrackHistoryID
		,st.[State]
		,th.TransactionType
		,PE.BureauNumber
		,st.ExperienceModificationFactorMeritRatingFactor
		,pol.TransactionEffectiveDate
		,pol.TransactionExpirationDate
		,pol.PolicyEffectiveDate
		,pol.PolicyExpirationDate
		,pol.PolicyKey
		,th.ReasonCode
		,TotalStandardPremium
		,st.ExperienceModType
		,st.PeriodStartDate
		,st.PeriodEndDate
		,st.TermType
		,st.ExperienceModEffectiveDate
		,lin.AnniversaryRatingDate
	      ,lin.AnniversaryRating
		,lin.AuditPeriod
	      ,st.WCStateAddedThisTransaction
		  ,st.StateDeletedFlag
	) T1
	
	ORDER BY WCTrackHistoryID,State,PeriodStartDate
),
EXP_DataCollect_Input AS (
	SELECT
	WCTrackHistoryID,
	State,
	TransactionType,
	BureauNumber,
	ExperienceModificationFactorMeritRatingFactor,
	ExpenseConstant,
	PremiumDiscount,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	PolicyKey,
	ReasonCode,
	TotalStandardPremium,
	ExperienceModType,
	PeriodStartDate,
	PeriodEndDate,
	TermType,
	ExperienceModEffectiveDate,
	AnniversaryRatingDate,
	AnniversaryRating,
	AuditPeriod,
	WCStateAddedThisTransaction,
	StateDeletedFlag,
	ROLLUP_ExpenseConstant,
	ROLLUP_PremiumDiscount,
	ROLLUP_TotalStandardPremium,
	FIRST_PeriodStartDate,
	LAST_PeriodStartDate,
	FIRST_TransactionEffectiveDate,
	SplitRated
	FROM SQ_WorkWCTrackHistory
),
SQ_WCPols00Record AS (
	SELECT
		WCTrackHistoryID,
		LinkData,
	     AuditId,
		TransactionCode
	FROM dbo.WCPols00Record
	WHERE 1=1
	AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY WCTrackHistoryID
),
JNR_Record04LinkData AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WCPols00Record.TransactionCode, 
	EXP_DataCollect_Input.WCTrackHistoryID AS WCTrackHistoryID1, 
	EXP_DataCollect_Input.State, 
	EXP_DataCollect_Input.TransactionType, 
	EXP_DataCollect_Input.BureauNumber, 
	EXP_DataCollect_Input.ExperienceModificationFactorMeritRatingFactor, 
	EXP_DataCollect_Input.ExpenseConstant, 
	EXP_DataCollect_Input.PremiumDiscount, 
	EXP_DataCollect_Input.TransactionEffectiveDate, 
	EXP_DataCollect_Input.TransactionExpirationDate, 
	EXP_DataCollect_Input.PolicyEffectiveDate, 
	EXP_DataCollect_Input.PolicyExpirationDate, 
	EXP_DataCollect_Input.PolicyKey, 
	EXP_DataCollect_Input.ReasonCode, 
	EXP_DataCollect_Input.TotalStandardPremium, 
	EXP_DataCollect_Input.ExperienceModType, 
	EXP_DataCollect_Input.PeriodStartDate, 
	EXP_DataCollect_Input.PeriodEndDate, 
	EXP_DataCollect_Input.TermType, 
	EXP_DataCollect_Input.ExperienceModEffectiveDate, 
	EXP_DataCollect_Input.AnniversaryRatingDate, 
	EXP_DataCollect_Input.AnniversaryRating, 
	EXP_DataCollect_Input.AuditPeriod, 
	EXP_DataCollect_Input.WCStateAddedThisTransaction, 
	EXP_DataCollect_Input.StateDeletedFlag, 
	EXP_DataCollect_Input.ROLLUP_ExpenseConstant, 
	EXP_DataCollect_Input.ROLLUP_PremiumDiscount, 
	EXP_DataCollect_Input.ROLLUP_TotalStandardPremium, 
	EXP_DataCollect_Input.FIRST_PeriodStartDate, 
	EXP_DataCollect_Input.LAST_PeriodStartDate, 
	EXP_DataCollect_Input.FIRST_TransactionEffectiveDate, 
	EXP_DataCollect_Input.SplitRated
	FROM EXP_DataCollect_Input
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = EXP_DataCollect_Input.WCTrackHistoryID
),
SRT_LatestTerm AS (
	SELECT
	WCTrackHistoryID, 
	LinkData, 
	AuditId, 
	TransactionCode, 
	WCTrackHistoryID1, 
	State, 
	TransactionType, 
	BureauNumber, 
	ExperienceModificationFactorMeritRatingFactor, 
	ExpenseConstant, 
	PremiumDiscount, 
	TransactionEffectiveDate, 
	TransactionExpirationDate, 
	PolicyEffectiveDate, 
	PolicyExpirationDate, 
	PolicyKey, 
	ReasonCode, 
	TotalStandardPremium, 
	ExperienceModType, 
	PeriodStartDate, 
	PeriodEndDate, 
	TermType, 
	ExperienceModEffectiveDate, 
	AnniversaryRatingDate, 
	AnniversaryRating, 
	AuditPeriod, 
	WCStateAddedThisTransaction, 
	StateDeletedFlag, 
	ROLLUP_ExpenseConstant, 
	ROLLUP_PremiumDiscount, 
	ROLLUP_TotalStandardPremium, 
	FIRST_PeriodStartDate, 
	LAST_PeriodStartDate, 
	FIRST_TransactionEffectiveDate, 
	SplitRated
	FROM JNR_Record04LinkData
	ORDER BY WCTrackHistoryID ASC, State ASC, PeriodStartDate DESC, PeriodEndDate DESC
),
EXP_Joinercollect AS (
	SELECT
	WCTrackHistoryID,
	LinkData,
	AuditId,
	TransactionCode,
	WCTrackHistoryID1,
	State,
	TransactionType,
	BureauNumber,
	ExperienceModificationFactorMeritRatingFactor,
	-- *INF*: RPAD(ExperienceModificationFactorMeritRatingFactor,4,'0')
	RPAD(ExperienceModificationFactorMeritRatingFactor, 4, '0') AS o_ExperienceModificationFactorMeritRatingFactor,
	ExpenseConstant,
	PremiumDiscount,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	PolicyKey,
	ReasonCode,
	TotalStandardPremium,
	ExperienceModType,
	PeriodStartDate,
	PeriodEndDate,
	TermType,
	ExperienceModEffectiveDate,
	AnniversaryRatingDate,
	AnniversaryRating,
	AuditPeriod,
	WCStateAddedThisTransaction,
	-- *INF*: DECODE(TRUE,
	-- WCStateAddedThisTransaction='T','1',
	-- '0')
	DECODE(
	    TRUE,
	    WCStateAddedThisTransaction = 'T', '1',
	    '0'
	) AS o_WCStateAddedThisTransaction,
	StateDeletedFlag,
	ROLLUP_ExpenseConstant,
	ROLLUP_PremiumDiscount,
	ROLLUP_TotalStandardPremium,
	FIRST_PeriodStartDate,
	LAST_PeriodStartDate,
	FIRST_TransactionEffectiveDate,
	SplitRated,
	-- *INF*:  IIF(IsNull(:LKP.LKP_SupWCPOLS('DCT',State,'WCPOLS04Record','SplitRateStateList')),'NO','YES')
	IFF(
	    LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_SplitRateStateList.WCPOLSCode IS NULL, 'NO', 'YES'
	) AS v_SplitState,
	-- *INF*: IIF(v_SplitState = 'YES' or PeriodStartDate = LAST_PeriodStartDate, 'YES', 'NO')
	IFF(v_SplitState = 'YES' or PeriodStartDate = LAST_PeriodStartDate, 'YES', 'NO') AS o_PassRecord,
	-- *INF*: IIF(PeriodStartDate < LAST_PeriodStartDate, 0, ROLLUP_ExpenseConstant)
	IFF(PeriodStartDate < LAST_PeriodStartDate, 0, ROLLUP_ExpenseConstant) AS o_ExpenseConstant,
	-- *INF*: IIF(PeriodStartDate < LAST_PeriodStartDate, 0, ROLLUP_PremiumDiscount)
	IFF(PeriodStartDate < LAST_PeriodStartDate, 0, ROLLUP_PremiumDiscount) AS o_PremiumDiscount,
	-- *INF*: IIF(v_SplitState = 'NO', FIRST_TransactionEffectiveDate, TransactionEffectiveDate)
	IFF(v_SplitState = 'NO', FIRST_TransactionEffectiveDate, TransactionEffectiveDate) AS o_TransactionEffectiveDate,
	-- *INF*: IIF(PeriodStartDate < LAST_PeriodStartDate, 0, ROLLUP_TotalStandardPremium)
	IFF(PeriodStartDate < LAST_PeriodStartDate, 0, ROLLUP_TotalStandardPremium) AS o_TotalStandardPremium,
	-- *INF*: IIF(WCTrackHistoryID<>v_WCTrackHistoryID,'1',
	-- IIF(State<>v_state,'1','0'))
	IFF(
	    WCTrackHistoryID <> v_WCTrackHistoryID, '1',
	    IFF(
	        State <> v_state, '1', '0'
	    )
	) AS v_Latest_Flag,
	v_Latest_Flag AS o_Latest_Flag,
	WCTrackHistoryID AS v_WCTrackHistoryID,
	State AS v_state
	FROM SRT_LatestTerm
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_SplitRateStateList
	ON LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_SplitRateStateList.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_SplitRateStateList.SourceCode = State
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_SplitRateStateList.TableName = 'WCPOLS04Record'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_SplitRateStateList.ProcessName = 'SplitRateStateList'

),
EXP_04_Output AS (
	SELECT
	o_PassRecord AS PassRecord,
	CURRENT_TIMESTAMP AS ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	State,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',State,'WCPOLS04Record','StateCodeRecord04')
	LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_StateCodeRecord04.WCPOLSCode AS v_StateCode,
	v_StateCode AS o_StateCode,
	'04' AS o_RecordTypeCode,
	TransactionCode,
	-- *INF*: DECODE(TRUE,
	-- TransactionCode='15' and WCStateAddedThisTransaction='1','A',
	-- TransactionCode='15' and StateDeletedFlag='1','D',
	-- ' ')
	DECODE(
	    TRUE,
	    TransactionCode = '15' and WCStateAddedThisTransaction = '1', 'A',
	    TransactionCode = '15' and StateDeletedFlag = '1', 'D',
	    ' '
	) AS v_StateAddDeleteCode,
	-- *INF*: :LKP.LKP_SupClaimAdministratorFEIN(v_StateCode)
	LKP_SUPCLAIMADMINISTRATORFEIN_v_StateCode.ClaimAdministratorFEIN AS o_ClaimAdministratorFEIN,
	v_StateAddDeleteCode AS o_StateAddDeleteCode,
	BureauNumber AS IndependentDCORiskIDNumberFileNumberAccountNumber,
	o_ExperienceModificationFactorMeritRatingFactor AS ExperienceModificationFactorMeritRatingFactor,
	-- *INF*: DECODE(TRUE,
	-- ExperienceModificationFactorMeritRatingFactor<'1.00',
	-- CONCAT('0',SUBSTR(ExperienceModificationFactorMeritRatingFactor,INSTR(ExperienceModificationFactorMeritRatingFactor,'.',1,1),3))
	-- ,ExperienceModificationFactorMeritRatingFactor)
	DECODE(
	    TRUE,
	    ExperienceModificationFactorMeritRatingFactor < '1.00', CONCAT('0', SUBSTR(ExperienceModificationFactorMeritRatingFactor, REGEXP_INSTR(ExperienceModificationFactorMeritRatingFactor, '.', 1, 1), 3)),
	    ExperienceModificationFactorMeritRatingFactor
	) AS v_ExperienceModificationFactorMeritRatingFactor,
	-- *INF*: Decode(TRUE,
	-- IN(v_ExperienceModificationFactorMeritRatingFactor,'0.0','0000','0'),'1000',
	-- IN(v_ExperienceModificationFactorMeritRatingFactor,'1.00','1'),'1000',
	-- Rpad(Replacechr(1,v_ExperienceModificationFactorMeritRatingFactor,'.',''),4,'0')
	-- )
	Decode(
	    TRUE,
	    v_ExperienceModificationFactorMeritRatingFactor IN ('0.0','0000','0'), '1000',
	    v_ExperienceModificationFactorMeritRatingFactor IN ('1.00','1'), '1000',
	    Rpad(REGEXP_REPLACE(v_ExperienceModificationFactorMeritRatingFactor,'.',''), 4, '0')
	) AS o_ExperienceModificationFactorMeritRatingFactor,
	ExperienceModType,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',ExperienceModType,'WCPOLS04Record','ExperienceModificationStatusCode')
	LKP_SUPWCPOLS__DCT_ExperienceModType_WCPOLS04Record_ExperienceModificationStatusCode.WCPOLSCode AS o_ExperienceModificationStatusCode,
	'1' AS o_ExperienceModificationPlanTypeCode,
	'1000' AS OtherIndividualRiskRatingFactor,
	'1000' AS InsurerPremiumDeviationFactor,
	'4' AS TypeOfPremiumDeviationCode,
	TotalStandardPremium,
	-- *INF*: To_Char(Round(TotalStandardPremium))
	To_Char(Round(TotalStandardPremium)) AS o_TotalStandardPremium,
	o_TotalStandardPremium AS ROLLUP_TotalStandardPremium,
	-- *INF*: To_Char(Round(ROLLUP_TotalStandardPremium))
	To_Char(Round(ROLLUP_TotalStandardPremium)) AS o_EstimatedStateStandardPremiumTotal,
	ReasonCode,
	-- *INF*: Decode(TRUE,
	-- TransactionCode = '15' and WCStateAddedThisTransaction='1'  and ReasonCode = 'AddingRatingStatePerAudit',
	--  :LKP.LKP_SupWCPOLS('DCT','AddingRatingStatePerAudit','WCPOLS04Record','ReasonStateWasAddedToThePolicyCode')
	-- ,TransactionCode = '15'  and WCStateAddedThisTransaction='1'  and ReasonCode <> 'AddingRatingStatePerAudit',
	--  :LKP.LKP_SupWCPOLS('DCT','AddingRatingStateOther','WCPOLS04Record','ReasonStateWasAddedToThePolicyCode'),
	-- '0')
	Decode(
	    TRUE,
	    TransactionCode = '15' and WCStateAddedThisTransaction = '1' and ReasonCode = 'AddingRatingStatePerAudit', LKP_SUPWCPOLS__DCT_AddingRatingStatePerAudit_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.WCPOLSCode,
	    TransactionCode = '15' and WCStateAddedThisTransaction = '1' and ReasonCode <> 'AddingRatingStatePerAudit', LKP_SUPWCPOLS__DCT_AddingRatingStateOther_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.WCPOLSCode,
	    '0'
	) AS o_ReasonStateWasAddedToThePolicyCode,
	PeriodStartDate,
	PeriodEndDate,
	TermType,
	-- *INF*: IIF(TermType = 'EMF',To_Char(PeriodStartDate,'YYMMDD'),NULL)
	IFF(TermType = 'EMF', To_Char(PeriodStartDate, 'YYMMDD'), NULL) AS o_ExperienceModificationEffectiveDate,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	-- *INF*: IIF(DATE_DIFF(PolicyExpirationDate,PolicyEffectiveDate,'DD')<=366,'2','0')
	IFF(DATEDIFF(DAY,PolicyExpirationDate,PolicyEffectiveDate) <= 366, '2', '0') AS o_ProRatedExpenseConstantAmountReasonCode,
	-- *INF*: IIF(DATE_DIFF(PolicyExpirationDate,PolicyEffectiveDate,'DD')<=366,'2','0')
	IFF(DATEDIFF(DAY,PolicyExpirationDate,PolicyEffectiveDate) <= 366, '2', '0') AS o_ProRatedMinimumPremiumAmountReasonCode,
	AnniversaryRatingDate,
	AnniversaryRating,
	-- *INF*: IIF (AnniversaryRating='T' AND TermType='ARD' AND AnniversaryRatingDate > PolicyEffectiveDate, To_Char(AnniversaryRatingDate,'YYMMDD'),NULL)
	IFF(
	    AnniversaryRating = 'T' AND TermType = 'ARD' AND AnniversaryRatingDate > PolicyEffectiveDate,
	    To_Char(AnniversaryRatingDate, 'YYMMDD'),
	    NULL
	) AS o_AnniversaryRatingDate,
	'1000' AS o_AssignedRiskAdjustmentProgramFactor,
	AuditPeriod,
	o_WCStateAddedThisTransaction AS WCStateAddedThisTransaction,
	StateDeletedFlag,
	-- *INF*: IIF(ISNULL( :LKP.LKP_SupWCPOLS('DCT',AuditPeriod,'WCPOLS04Record','PremiumAdjustmentPeriodCode')),'1',
	--  :LKP.LKP_SupWCPOLS('DCT',AuditPeriod,'WCPOLS04Record','PremiumAdjustmentPeriodCode') )
	-- 
	-- 
	-- 
	-- 
	IFF(
	    LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS04Record_PremiumAdjustmentPeriodCode.WCPOLSCode IS NULL,
	    '1',
	    LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS04Record_PremiumAdjustmentPeriodCode.WCPOLSCode
	) AS o_PremiumAdjustmentPeriodCode,
	'01' AS o_TypeOfNonStandardIDCode,
	o_TransactionEffectiveDate AS TransactionEffectiveDate,
	TransactionExpirationDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_PolicyChangeEffectiveDate,
	-- *INF*: IIF((FIRST_PeriodStartDate<>LAST_PeriodStartDate) AND (SplitRated='1'),TO_CHAR(PeriodStartDate,'YYMMDD'),TO_CHAR(TransactionEffectiveDate,'YYMMDD'))
	IFF(
	    (FIRST_PeriodStartDate <> LAST_PeriodStartDate) AND (SplitRated = '1'),
	    TO_CHAR(PeriodStartDate, 'YYMMDD'),
	    TO_CHAR(TransactionEffectiveDate, 'YYMMDD')
	) AS v_PolicyChangeEffectiveDate_NonNCCI,
	v_PolicyChangeEffectiveDate_NonNCCI AS o_PolicyChangeEffectiveDate_NonNCCI,
	-- *INF*: DECODE(TRUE,
	-- v_StateAddDeleteCode='D',TO_CHAR(TransactionEffectiveDate,'YYMMDD'),
	-- TO_CHAR(TransactionExpirationDate,'YYMMDD')
	-- )
	DECODE(
	    TRUE,
	    v_StateAddDeleteCode = 'D', TO_CHAR(TransactionEffectiveDate, 'YYMMDD'),
	    TO_CHAR(TransactionExpirationDate, 'YYMMDD')
	) AS o_PolicyChangeExpirationDate,
	-- *INF*: IIF((FIRST_PeriodStartDate<>LAST_PeriodStartDate) AND (SplitRated='1'),TO_CHAR(PeriodEndDate,'YYMMDD'),TO_CHAR(TransactionExpirationDate,'YYMMDD'))
	IFF(
	    (FIRST_PeriodStartDate <> LAST_PeriodStartDate) AND (SplitRated = '1'),
	    TO_CHAR(PeriodEndDate, 'YYMMDD'),
	    TO_CHAR(TransactionExpirationDate, 'YYMMDD')
	) AS v_PolicyChangeExpirationDate_NonNCCI,
	-- *INF*: DECODE(TRUE,
	-- v_StateAddDeleteCode='D',v_PolicyChangeEffectiveDate_NonNCCI,
	-- v_PolicyChangeExpirationDate_NonNCCI)
	DECODE(
	    TRUE,
	    v_StateAddDeleteCode = 'D', v_PolicyChangeEffectiveDate_NonNCCI,
	    v_PolicyChangeExpirationDate_NonNCCI
	) AS o_PolicyChangeExpirationDate_NonNCCI,
	o_ExpenseConstant AS ExpenseConstant,
	SplitRated,
	o_PremiumDiscount AS PremiumDiscount,
	-- *INF*: TO_CHAR(ROUND(ExpenseConstant))
	TO_CHAR(ROUND(ExpenseConstant)) AS o_EC_Written,
	-- *INF*: TO_CHAR(ABS(ROUND(PremiumDiscount)))
	TO_CHAR(ABS(ROUND(PremiumDiscount))) AS o_PD_Written,
	o_Latest_Flag AS Latest_Flag,
	FIRST_PeriodStartDate,
	LAST_PeriodStartDate
	FROM EXP_Joinercollect
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_StateCodeRecord04
	ON LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_StateCodeRecord04.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_StateCodeRecord04.SourceCode = State
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_StateCodeRecord04.TableName = 'WCPOLS04Record'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS04Record_StateCodeRecord04.ProcessName = 'StateCodeRecord04'

	LEFT JOIN LKP_SUPCLAIMADMINISTRATORFEIN LKP_SUPCLAIMADMINISTRATORFEIN_v_StateCode
	ON LKP_SUPCLAIMADMINISTRATORFEIN_v_StateCode.StateCode = v_StateCode

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_ExperienceModType_WCPOLS04Record_ExperienceModificationStatusCode
	ON LKP_SUPWCPOLS__DCT_ExperienceModType_WCPOLS04Record_ExperienceModificationStatusCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_ExperienceModType_WCPOLS04Record_ExperienceModificationStatusCode.SourceCode = ExperienceModType
	AND LKP_SUPWCPOLS__DCT_ExperienceModType_WCPOLS04Record_ExperienceModificationStatusCode.TableName = 'WCPOLS04Record'
	AND LKP_SUPWCPOLS__DCT_ExperienceModType_WCPOLS04Record_ExperienceModificationStatusCode.ProcessName = 'ExperienceModificationStatusCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_AddingRatingStatePerAudit_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode
	ON LKP_SUPWCPOLS__DCT_AddingRatingStatePerAudit_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_AddingRatingStatePerAudit_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.SourceCode = 'AddingRatingStatePerAudit'
	AND LKP_SUPWCPOLS__DCT_AddingRatingStatePerAudit_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.TableName = 'WCPOLS04Record'
	AND LKP_SUPWCPOLS__DCT_AddingRatingStatePerAudit_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.ProcessName = 'ReasonStateWasAddedToThePolicyCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_AddingRatingStateOther_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode
	ON LKP_SUPWCPOLS__DCT_AddingRatingStateOther_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_AddingRatingStateOther_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.SourceCode = 'AddingRatingStateOther'
	AND LKP_SUPWCPOLS__DCT_AddingRatingStateOther_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.TableName = 'WCPOLS04Record'
	AND LKP_SUPWCPOLS__DCT_AddingRatingStateOther_WCPOLS04Record_ReasonStateWasAddedToThePolicyCode.ProcessName = 'ReasonStateWasAddedToThePolicyCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS04Record_PremiumAdjustmentPeriodCode
	ON LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS04Record_PremiumAdjustmentPeriodCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS04Record_PremiumAdjustmentPeriodCode.SourceCode = AuditPeriod
	AND LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS04Record_PremiumAdjustmentPeriodCode.TableName = 'WCPOLS04Record'
	AND LKP_SUPWCPOLS__DCT_AuditPeriod_WCPOLS04Record_PremiumAdjustmentPeriodCode.ProcessName = 'PremiumAdjustmentPeriodCode'

),
SRT_ExpMod AS (
	SELECT
	PassRecord, 
	ExtractDate, 
	AuditId, 
	WCTrackHistoryID, 
	LinkData, 
	o_StateCode AS StateCode, 
	o_ClaimAdministratorFEIN AS ClaimAdministratorFEIN, 
	o_RecordTypeCode AS RecordTypeCode, 
	o_StateAddDeleteCode AS StateAddDeleteCode, 
	PeriodStartDate, 
	IndependentDCORiskIDNumberFileNumberAccountNumber, 
	o_ExperienceModificationFactorMeritRatingFactor AS ExperienceModificationFactorMeritRatingFactor, 
	o_ExperienceModificationStatusCode AS ExperienceModificationStatusCode, 
	o_ExperienceModificationPlanTypeCode AS ExperienceModificationPlanTypeCode, 
	OtherIndividualRiskRatingFactor, 
	InsurerPremiumDeviationFactor, 
	TypeOfPremiumDeviationCode, 
	o_TotalStandardPremium AS TotalStandardPremium, 
	o_EstimatedStateStandardPremiumTotal AS EstimatedStateStandardPremiumTotal, 
	o_ReasonStateWasAddedToThePolicyCode AS ReasonStateWasAddedToThePolicyCode, 
	TermType, 
	o_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate, 
	o_ProRatedExpenseConstantAmountReasonCode AS ProRatedExpenseConstantAmountReasonCode, 
	o_ProRatedMinimumPremiumAmountReasonCode AS ProRatedMinimumPremiumAmountReasonCode, 
	o_AnniversaryRatingDate AS AnniversaryRatingDate, 
	o_AssignedRiskAdjustmentProgramFactor AS AssignedRiskAdjustmentProgramFactor, 
	o_PremiumAdjustmentPeriodCode AS PremiumAdjustmentPeriodCode, 
	o_TypeOfNonStandardIDCode AS TypeOfNonStandardIDCode, 
	o_PolicyChangeEffectiveDate AS PolicyChangeEffectiveDate, 
	o_PolicyChangeEffectiveDate_NonNCCI AS PolicyChangeEffectiveDate_NonNCCI, 
	o_PolicyChangeExpirationDate AS PolicyChangeExpirationDate, 
	o_PolicyChangeExpirationDate_NonNCCI AS PolicyChangeExpirationDate_NonNCCI, 
	o_EC_Written AS EC_Written, 
	o_PD_Written AS PD_Written, 
	Latest_Flag, 
	SplitRated
	FROM EXP_04_Output
	ORDER BY WCTrackHistoryID ASC, StateCode ASC, PeriodStartDate ASC
),
EXP_ExpModCalc AS (
	SELECT
	PassRecord,
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	StateCode,
	ClaimAdministratorFEIN,
	RecordTypeCode,
	StateAddDeleteCode,
	PeriodStartDate,
	IndependentDCORiskIDNumberFileNumberAccountNumber,
	ExperienceModificationFactorMeritRatingFactor,
	ExperienceModificationStatusCode,
	ExperienceModificationPlanTypeCode,
	OtherIndividualRiskRatingFactor,
	InsurerPremiumDeviationFactor,
	TypeOfPremiumDeviationCode,
	TotalStandardPremium,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(SplitRated))='1',TotalStandardPremium,
	-- EstimatedStateStandardPremiumTotal)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(SplitRated)) = '1', TotalStandardPremium,
	    EstimatedStateStandardPremiumTotal
	) AS o_TotalStandardPremium,
	EstimatedStateStandardPremiumTotal,
	ReasonStateWasAddedToThePolicyCode,
	TermType,
	ExperienceModificationEffectiveDate,
	ProRatedExpenseConstantAmountReasonCode,
	ProRatedMinimumPremiumAmountReasonCode,
	AnniversaryRatingDate,
	AssignedRiskAdjustmentProgramFactor,
	PremiumAdjustmentPeriodCode,
	TypeOfNonStandardIDCode,
	PolicyChangeEffectiveDate,
	PolicyChangeEffectiveDate_NonNCCI,
	PolicyChangeExpirationDate,
	PolicyChangeExpirationDate_NonNCCI,
	EC_Written,
	PD_Written,
	Latest_Flag,
	WCTrackHistoryID AS v_CurrentRecord,
	StateCode AS v_CurrentState,
	-- *INF*: IIF(v_CurrentRecord<>v_PreviousRecord,1,
	-- IIF(v_CurrentState<>v_PreviousState,1,v_CompareFlag+1))
	IFF(
	    v_CurrentRecord <> v_PreviousRecord, 1,
	    IFF(
	        v_CurrentState <> v_PreviousState, 1, v_CompareFlag + 1
	    )
	) AS v_CompareFlag,
	WCTrackHistoryID AS v_PreviousRecord,
	StateCode AS v_PreviousState,
	ExperienceModificationFactorMeritRatingFactor AS v_CurrentExpModRatingFactor,
	-- *INF*: IIF(v_CompareFlag>1 AND v_CurrentExpModRatingFactor<>v_PreviousExpModRatingFactor,To_Char(PeriodStartDate,'YYMMDD'),NULL)
	IFF(
	    v_CompareFlag > 1 AND v_CurrentExpModRatingFactor <> v_PreviousExpModRatingFactor,
	    To_Char(PeriodStartDate, 'YYMMDD'),
	    NULL
	) AS v_ExperienceModificationEffectiveDate,
	ExperienceModificationFactorMeritRatingFactor AS v_PreviousExpModRatingFactor,
	v_ExperienceModificationEffectiveDate AS o_ExperienceModificationEffectiveDate,
	SplitRated
	FROM SRT_ExpMod
),
RTR_WCPOLS_04_Record AS (
	SELECT
	PassRecord,
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	StateCode,
	ClaimAdministratorFEIN,
	RecordTypeCode,
	StateAddDeleteCode,
	IndependentDCORiskIDNumberFileNumberAccountNumber,
	ExperienceModificationFactorMeritRatingFactor,
	ExperienceModificationStatusCode,
	ExperienceModificationPlanTypeCode,
	OtherIndividualRiskRatingFactor,
	InsurerPremiumDeviationFactor,
	TypeOfPremiumDeviationCode,
	o_TotalStandardPremium AS TotalStandardPremium,
	EstimatedStateStandardPremiumTotal,
	TermType,
	ProRatedExpenseConstantAmountReasonCode,
	ProRatedMinimumPremiumAmountReasonCode,
	ReasonStateWasAddedToThePolicyCode,
	o_ExperienceModificationEffectiveDate AS ExperienceModificationEffectiveDate,
	AnniversaryRatingDate,
	AssignedRiskAdjustmentProgramFactor,
	PremiumAdjustmentPeriodCode,
	TypeOfNonStandardIDCode,
	PolicyChangeEffectiveDate,
	PolicyChangeEffectiveDate_NonNCCI,
	PolicyChangeExpirationDate,
	PolicyChangeExpirationDate_NonNCCI,
	EC_Written,
	PD_Written,
	Latest_Flag,
	SplitRated
	FROM EXP_ExpModCalc
),
RTR_WCPOLS_04_Record_WCPOLS_04_Record_NCCI AS (SELECT * FROM RTR_WCPOLS_04_Record WHERE PassRecord='YES' and Latest_Flag='1'),
RTR_WCPOLS_04_Record_WCPOLS_04_Record AS (SELECT * FROM RTR_WCPOLS_04_Record WHERE ((LTRIM(RTRIM(SplitRated))='1') OR(PassRecord='YES' and Latest_Flag='1'))),
EXP_ExpModFactor AS (
	SELECT
	AuditId,
	ExtractDate,
	WCTrackHistoryID,
	ExperienceModificationFactorMeritRatingFactor
	FROM RTR_WCPOLS_04_Record_WCPOLS_04_Record
),
LKP_WCPols01Record AS (
	SELECT
	WCTrackHistoryID,
	AuditId
	FROM (
		select A.WCTrackHistoryID as WCTrackHistoryID,
		A.AuditId as AuditId
		from WCPols01Record A with (nolock)
		inner join WorkWCTrackHistory B with (nolock)
		on A.WCTrackHistoryID = B.WCTrackHistoryID
		and A.AuditId = B.Auditid
		where A.ExperienceRatingCode = '5'
		and B.WIRequiredFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AuditId,WCTrackHistoryID ORDER BY WCTrackHistoryID) = 1
),
FIL_ExpModFactor AS (
	SELECT
	EXP_ExpModFactor.AuditId, 
	EXP_ExpModFactor.ExtractDate, 
	LKP_WCPols01Record.WCTrackHistoryID, 
	EXP_ExpModFactor.ExperienceModificationFactorMeritRatingFactor
	FROM EXP_ExpModFactor
	LEFT JOIN LKP_WCPols01Record
	ON LKP_WCPols01Record.AuditId = EXP_ExpModFactor.AuditId AND LKP_WCPols01Record.WCTrackHistoryID = EXP_ExpModFactor.WCTrackHistoryID
	WHERE ExperienceModificationFactorMeritRatingFactor = '1000' AND NOT ISNULL(WCTrackHistoryID)
),
EXP_WorkWCProcessUpdateTable AS (
	SELECT
	AuditId,
	ExtractDate,
	WCTrackHistoryID,
	'ExpModFactor-04' AS ProcessName,
	ExperienceModificationFactorMeritRatingFactor AS AttributeValue
	FROM FIL_ExpModFactor
),
WorkWCProcessUpdateTable AS (
	INSERT INTO WorkWCProcessUpdateTable
	(Auditid, ExtractDate, WCTrackHistoryID, ProcessName, AttributeValue)
	SELECT 
	AuditId AS AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	PROCESSNAME, 
	ATTRIBUTEVALUE
	FROM EXP_WorkWCProcessUpdateTable
),
WCPols04Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols04Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols04Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, StateAddDeleteCode, ClaimAdministratorFEIN, IndependentDCORiskIDNumberFileNumberAccountNumber, ExperienceModificationFactorMeritRatingFactor, ExperienceModificationStatusCode, ExperienceModificationPlanTypeCode, OtherIndividualRiskRatingFactor, InsurerPremiumDeviationFactor, TypeOfPremiumDeviationCode, EstimatedStateStandardPremiumTotal, ExpenseConstantAmount, PremiumDiscountAmount, ProRatedExpenseConstantAmountReasonCode, ProRatedMinimumPremiumAmountReasonCode, ReasonStateWasAddedToThePolicyCode, ExperienceModificationEffectiveDate, AnniversaryRatingDate, AssignedRiskAdjustmentProgramFactor, PremiumAdjustmentPeriodCode, TypeOfNonStandardIDCode, PolicyChangeEffectiveDate, PolicyChangeExpirationDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	RECORDTYPECODE, 
	STATEADDDELETECODE, 
	CLAIMADMINISTRATORFEIN, 
	INDEPENDENTDCORISKIDNUMBERFILENUMBERACCOUNTNUMBER, 
	EXPERIENCEMODIFICATIONFACTORMERITRATINGFACTOR, 
	EXPERIENCEMODIFICATIONSTATUSCODE, 
	EXPERIENCEMODIFICATIONPLANTYPECODE, 
	OTHERINDIVIDUALRISKRATINGFACTOR, 
	INSURERPREMIUMDEVIATIONFACTOR, 
	TYPEOFPREMIUMDEVIATIONCODE, 
	TotalStandardPremium AS ESTIMATEDSTATESTANDARDPREMIUMTOTAL, 
	EC_Written AS EXPENSECONSTANTAMOUNT, 
	PD_Written AS PREMIUMDISCOUNTAMOUNT, 
	PRORATEDEXPENSECONSTANTAMOUNTREASONCODE, 
	PRORATEDMINIMUMPREMIUMAMOUNTREASONCODE, 
	REASONSTATEWASADDEDTOTHEPOLICYCODE, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	ANNIVERSARYRATINGDATE, 
	ASSIGNEDRISKADJUSTMENTPROGRAMFACTOR, 
	PREMIUMADJUSTMENTPERIODCODE, 
	TYPEOFNONSTANDARDIDCODE, 
	PolicyChangeEffectiveDate_NonNCCI AS POLICYCHANGEEFFECTIVEDATE, 
	PolicyChangeExpirationDate_NonNCCI AS POLICYCHANGEEXPIRATIONDATE
	FROM RTR_WCPOLS_04_Record_WCPOLS_04_Record
),
WCPols04RecordNCCI AS (
	INSERT INTO WCPols04RecordNCCI
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, StateAddDeleteCode, ClaimAdministratorFEIN, IndependentDCORiskIDNumberFileNumberAccountNumber, ExperienceModificationFactorMeritRatingFactor, ExperienceModificationStatusCode, ExperienceModificationPlanTypeCode, OtherIndividualRiskRatingFactor, InsurerPremiumDeviationFactor, TypeOfPremiumDeviationCode, EstimatedStateStandardPremiumTotal, ExpenseConstantAmount, PremiumDiscountAmount, ProRatedExpenseConstantAmountReasonCode, ProRatedMinimumPremiumAmountReasonCode, ReasonStateWasAddedToThePolicyCode, ExperienceModificationEffectiveDate, AnniversaryRatingDate, AssignedRiskAdjustmentProgramFactor, PremiumAdjustmentPeriodCode, TypeOfNonStandardIDCode, PolicyChangeEffectiveDate, PolicyChangeExpirationDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	RECORDTYPECODE, 
	STATEADDDELETECODE, 
	CLAIMADMINISTRATORFEIN, 
	INDEPENDENTDCORISKIDNUMBERFILENUMBERACCOUNTNUMBER, 
	EXPERIENCEMODIFICATIONFACTORMERITRATINGFACTOR, 
	EXPERIENCEMODIFICATIONSTATUSCODE, 
	EXPERIENCEMODIFICATIONPLANTYPECODE, 
	OTHERINDIVIDUALRISKRATINGFACTOR, 
	INSURERPREMIUMDEVIATIONFACTOR, 
	TYPEOFPREMIUMDEVIATIONCODE, 
	ESTIMATEDSTATESTANDARDPREMIUMTOTAL, 
	EC_Written AS EXPENSECONSTANTAMOUNT, 
	PD_Written AS PREMIUMDISCOUNTAMOUNT, 
	PRORATEDEXPENSECONSTANTAMOUNTREASONCODE, 
	PRORATEDMINIMUMPREMIUMAMOUNTREASONCODE, 
	REASONSTATEWASADDEDTOTHEPOLICYCODE, 
	EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	ANNIVERSARYRATINGDATE, 
	ASSIGNEDRISKADJUSTMENTPROGRAMFACTOR, 
	PREMIUMADJUSTMENTPERIODCODE, 
	TYPEOFNONSTANDARDIDCODE, 
	POLICYCHANGEEFFECTIVEDATE, 
	POLICYCHANGEEXPIRATIONDATE
	FROM RTR_WCPOLS_04_Record_WCPOLS_04_Record_NCCI
),