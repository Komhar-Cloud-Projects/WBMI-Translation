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
SQ_WorkWCPolicy AS (
	SELECT DISTINCT
	       P.WCTrackHistoryID
	      ,P.PolicyKey
	      ,P.PolicyEffectiveDate
	      ,P.TransactionDate
		,P.PreviousPolicyKey
		,th.HistoryID
	      ,case when th.TransactionType in ('Rewrite','Reissue') then th.PreviousPolicyTransactionType else th.TransactionType end TransactionType
		,th.PremiumBearingFlag
		,th.StateAddFlag
	  FROM dbo.WorkWCPolicy P
	
	INNER JOIN dbo.WorkWCTrackHistory th
		ON th.WCTrackHistoryID = P.WCTrackHistoryID
	
	  WHERE 1=1
	  AND P.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	  @{pipeline().parameters.WHERE_CLAUSE_00}
),
EXP_PrepLKP_00 AS (
	SELECT
	WCTrackHistoryID,
	PolicyKey,
	PolicyEffectiveDate,
	TransactionDate,
	PreviousPolicyKey,
	HistoryID,
	TransactionType,
	PremiumBearingFlag,
	StateAddFlag,
	-- *INF*: IIF(TransactionType='Endorse',IIF(StateAddFlag='1','EndorseAddDeleteState',IIF(PremiumBearingFlag='1','EndorsePremiumBearing','EndorseNonPremiumBearing')),TransactionType)
	IFF(
	    TransactionType = 'Endorse',
	    IFF(
	        StateAddFlag = '1', 'EndorseAddDeleteState',
	        IFF(
	            PremiumBearingFlag = '1', 'EndorsePremiumBearing',
	            'EndorseNonPremiumBearing'
	        )
	    ),
	    TransactionType
	) AS v_TransactionType,
	-- *INF*: TO_CHAR(PolicyEffectiveDate, 'YYMMDD')
	TO_CHAR(PolicyEffectiveDate, 'YYMMDD') AS o_PolicyEffectiveDate_YYMMDD,
	-- *INF*: TO_CHAR(TransactionDate,'YYDDD')
	TO_CHAR(TransactionDate, 'YYDDD') AS o_TransactionDate_YYDDD,
	-- *INF*: :LKP.LKP_SupWCPOLS('DCT',v_TransactionType,'WCPOLS00Record','TransactionCode')
	LKP_SUPWCPOLS__DCT_v_TransactionType_WCPOLS00Record_TransactionCode.WCPOLSCode AS o_TransactionCode
	FROM SQ_WorkWCPolicy
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_v_TransactionType_WCPOLS00Record_TransactionCode
	ON LKP_SUPWCPOLS__DCT_v_TransactionType_WCPOLS00Record_TransactionCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_v_TransactionType_WCPOLS00Record_TransactionCode.SourceCode = v_TransactionType
	AND LKP_SUPWCPOLS__DCT_v_TransactionType_WCPOLS00Record_TransactionCode.TableName = 'WCPOLS00Record'
	AND LKP_SUPWCPOLS__DCT_v_TransactionType_WCPOLS00Record_TransactionCode.ProcessName = 'TransactionCode'

),
LKP_ValidTransactions AS (
	SELECT
	HistoryID,
	IN_PolicyKey,
	IN_HistoryID,
	PolicyKey
	FROM (
		Select PolicyKey AS PolicyKey,Max(HistoryID) AS HistoryID from WorkWCTrackHistory
		where TransactionType like '%Endorse%'
		AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		group by PolicyKey,TransactionDate
		
		UNION
		
		Select PolicyKey AS PolicyKey,HistoryID AS HistoryID from WorkWCTrackHistory
		where TransactionType NOT like '%Endorse%'
		AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		group by PolicyKey,HistoryID,TransactionDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,HistoryID ORDER BY HistoryID) = 1
),
FILTRANS AS (
	SELECT
	EXP_PrepLKP_00.WCTrackHistoryID, 
	EXP_PrepLKP_00.PolicyKey, 
	EXP_PrepLKP_00.PreviousPolicyKey, 
	EXP_PrepLKP_00.TransactionType, 
	EXP_PrepLKP_00.o_PolicyEffectiveDate_YYMMDD AS PolicyEffectiveDate_YYMMDD, 
	EXP_PrepLKP_00.o_TransactionDate_YYDDD AS TransactionDate_YYDDD, 
	EXP_PrepLKP_00.o_TransactionCode AS TransactionCode, 
	LKP_ValidTransactions.HistoryID
	FROM EXP_PrepLKP_00
	LEFT JOIN LKP_ValidTransactions
	ON LKP_ValidTransactions.PolicyKey = EXP_PrepLKP_00.PolicyKey AND LKP_ValidTransactions.HistoryID = EXP_PrepLKP_00.HistoryID
	WHERE NOT IsNull(TransactionCode) AND NOT ISNULL(HistoryID)
),
EXP_Format_00_Output AS (
	SELECT
	WCTrackHistoryID,
	PolicyKey,
	PreviousPolicyKey,
	TransactionType,
	PolicyEffectiveDate_YYMMDD,
	TransactionDate_YYDDD AS TransactionEffectiveDate_YYDDD,
	TransactionCode,
	CURRENT_TIMESTAMP AS o_ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_Auditid,
	'17124' AS o_CarrierCode,
	-- *INF*: --IIF(IN(TransactionType,'New','Renew'),IIF(IsNull(@{pipeline().parameters.TEST_FLAG_INSERT}) or @{pipeline().parameters.TEST_FLAG_INSERT} = '',' ',Substr(@{pipeline().parameters.TEST_FLAG_INSERT},1,1)),' ')
	-- 
	-- IIF(IsNull(@{pipeline().parameters.TEST_FLAG_INSERT}) or @{pipeline().parameters.TEST_FLAG_INSERT} = '',' ',Substr(@{pipeline().parameters.TEST_FLAG_INSERT},1,1))
	IFF(
	    @{pipeline().parameters.TEST_FLAG_INSERT} IS NULL or @{pipeline().parameters.TEST_FLAG_INSERT} = '', ' ', Substr(@{pipeline().parameters.TEST_FLAG_INSERT}, 1, 1)
	) AS v_TestFlag,
	-- *INF*: '17124'||RPAD((PolicyKey || v_TestFlag),25)||PolicyEffectiveDate_YYMMDD||TransactionEffectiveDate_YYDDD||RPAD(TransactionCode,2)
	'17124' || RPAD((PolicyKey || v_TestFlag), 25) || PolicyEffectiveDate_YYMMDD || TransactionEffectiveDate_YYDDD || RPAD(TransactionCode, 2) AS o_LinkData,
	HistoryID
	FROM FILTRANS
),
WCPols00Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols00Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols00Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, CarrierCode, PolicyNumberID, PolicyEffectiveDate, TransactionIssueDate, TransactionCode, OriginalLinkData)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_Auditid AS AUDITID, 
	WCTRACKHISTORYID, 
	o_LinkData AS LINKDATA, 
	o_CarrierCode AS CARRIERCODE, 
	PolicyKey AS POLICYNUMBERID, 
	PolicyEffectiveDate_YYMMDD AS POLICYEFFECTIVEDATE, 
	TransactionEffectiveDate_YYDDD AS TRANSACTIONISSUEDATE, 
	TRANSACTIONCODE, 
	o_LinkData AS ORIGINALLINKDATA
	FROM EXP_Format_00_Output
),