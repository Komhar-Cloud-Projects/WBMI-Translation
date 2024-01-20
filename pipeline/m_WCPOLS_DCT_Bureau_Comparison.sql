WITH
SQ_WorkWCTrackHistory AS (
	Select distinct HistoryID,PolicyKey from WorkWCTrackHistory
	where AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_TrackHistory AS (
	SELECT
	HistoryID,
	PolicyKey
	FROM SQ_WorkWCTrackHistory
),
SQ_WCPOLS_Reconciliation_File AS (

-- TODO Manual --

),
FIL_BureauData AS (
	SELECT
	ReconciliationID, 
	AuditID, 
	BureauName, 
	PolicyKey, 
	TransactionType, 
	HistoryID
	FROM SQ_WCPOLS_Reconciliation_File
	WHERE AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
SRT_BureauData AS (
	SELECT
	ReconciliationID, 
	AuditID, 
	BureauName, 
	PolicyKey AS PolicyKey_Bureau, 
	TransactionType AS TransactionType_Bureau, 
	HistoryID AS HistoryID_Bureau
	FROM FIL_BureauData
	ORDER BY HistoryID_Bureau ASC
),
SQ_WCPOLS_DCT_Data AS (

-- TODO Manual --

),
FIL_DCT_Data AS (
	SELECT
	WCPOLS_DCT_Data_ID, 
	AuditID, 
	HistoryID, 
	PolicyKey, 
	Type, 
	ChangeDate, 
	RecordType, 
	Comments
	FROM SQ_WCPOLS_DCT_Data
	WHERE AuditID=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
SRT_DCT_Data AS (
	SELECT
	WCPOLS_DCT_Data_ID, 
	AuditID, 
	HistoryID AS HistoryID_DCT, 
	PolicyKey AS PolicyKey_DCT, 
	Type AS TransactionType_DCT, 
	ChangeDate, 
	RecordType, 
	Comments
	FROM FIL_DCT_Data
	ORDER BY HistoryID_DCT ASC
),
JNR_Comparison AS (SELECT
	SRT_BureauData.BureauName, 
	SRT_BureauData.PolicyKey_Bureau, 
	SRT_BureauData.TransactionType_Bureau, 
	SRT_BureauData.HistoryID_Bureau, 
	SRT_DCT_Data.HistoryID_DCT, 
	SRT_DCT_Data.PolicyKey_DCT, 
	SRT_DCT_Data.TransactionType_DCT, 
	SRT_DCT_Data.ChangeDate, 
	SRT_DCT_Data.RecordType, 
	SRT_DCT_Data.Comments
	FROM SRT_BureauData
	FULL OUTER JOIN SRT_DCT_Data
	ON SRT_DCT_Data.HistoryID_DCT = SRT_BureauData.HistoryID_Bureau
),
EXP_Comparison AS (
	SELECT
	BureauName,
	PolicyKey_Bureau AS IN_PolicyKey_Bureau,
	-- *INF*: LPAD(IN_PolicyKey_Bureau,9,'0')
	LPAD(IN_PolicyKey_Bureau, 9, '0') AS PolicyKey_Bureau,
	TransactionType_Bureau,
	HistoryID_Bureau,
	HistoryID_DCT,
	PolicyKey_DCT AS IN_PolicyKey_DCT,
	-- *INF*: LPAD(IN_PolicyKey_DCT,9,'0')
	LPAD(IN_PolicyKey_DCT, 9, '0') AS PolicyKey_DCT,
	TransactionType_DCT,
	ChangeDate,
	RecordType,
	Comments,
	-- *INF*: DECODE(TRUE,
	-- (NOT ISNULL(IN_PolicyKey_Bureau)),LPAD(IN_PolicyKey_Bureau,9,'0'),
	-- (NOT ISNULL(IN_PolicyKey_DCT)),LPAD(IN_PolicyKey_DCT,9,'0'),
	-- '')
	DECODE(
	    TRUE,
	    (IN_PolicyKey_Bureau IS NOT NULL), LPAD(IN_PolicyKey_Bureau, 9, '0'),
	    (IN_PolicyKey_DCT IS NOT NULL), LPAD(IN_PolicyKey_DCT, 9, '0'),
	    ''
	) AS PolicyKey_Comp,
	-- *INF*: DECODE(TRUE,
	-- (NOT ISNULL(HistoryID_Bureau)),HistoryID_Bureau,
	-- (NOT ISNULL(HistoryID_DCT)),HistoryID_DCT,
	-- '')
	DECODE(
	    TRUE,
	    (HistoryID_Bureau IS NOT NULL), HistoryID_Bureau,
	    (HistoryID_DCT IS NOT NULL), HistoryID_DCT,
	    ''
	) AS HistoryID_Comp
	FROM JNR_Comparison
),
SQ_WCPOLS_DG_ExclusionList AS (

-- TODO Manual --

),
EXP_Exclusion AS (
	SELECT
	ExtractDate,
	-- *INF*: ADD_TO_DATE(TO_DATE(ExtractDate,'MM/DD/YYYY'),'HH',13)
	DATEADD(HOUR,13,TO_TIMESTAMP(ExtractDate, 'MM/DD/YYYY')) AS O_ExtractDate,
	Policykey,
	-- *INF*: LPAD(Policykey,9,'0')
	LPAD(Policykey, 9, '0') AS O_Policykey,
	TransactionType
	FROM SQ_WCPOLS_DG_ExclusionList
),
FIL_Exclusion AS (
	SELECT
	O_ExtractDate AS ExtractDate, 
	O_Policykey AS Policykey, 
	TransactionType
	FROM EXP_Exclusion
	WHERE ExtractDate>=@{pipeline().parameters.SELECTION_START_TS}
),
SRT_Exclusion AS (
	SELECT
	Policykey, 
	TransactionType
	FROM FIL_Exclusion
	ORDER BY Policykey ASC, TransactionType ASC
),
JNR_Exclusion AS (SELECT
	EXP_Comparison.BureauName, 
	EXP_Comparison.PolicyKey_Bureau, 
	EXP_Comparison.TransactionType_Bureau, 
	EXP_Comparison.HistoryID_Bureau, 
	EXP_Comparison.HistoryID_DCT, 
	EXP_Comparison.PolicyKey_DCT, 
	EXP_Comparison.TransactionType_DCT, 
	EXP_Comparison.ChangeDate, 
	EXP_Comparison.RecordType, 
	EXP_Comparison.Comments, 
	EXP_Comparison.PolicyKey_Comp, 
	EXP_Comparison.HistoryID_Comp, 
	SRT_Exclusion.Policykey, 
	SRT_Exclusion.TransactionType
	FROM EXP_Comparison
	LEFT OUTER JOIN SRT_Exclusion
	ON SRT_Exclusion.Policykey = EXP_Comparison.PolicyKey_Comp
),
JNR_WIData AS (SELECT
	EXP_TrackHistory.HistoryID, 
	EXP_TrackHistory.PolicyKey, 
	JNR_Exclusion.BureauName, 
	JNR_Exclusion.PolicyKey_Bureau, 
	JNR_Exclusion.TransactionType_Bureau, 
	JNR_Exclusion.HistoryID_Bureau, 
	JNR_Exclusion.HistoryID_DCT, 
	JNR_Exclusion.PolicyKey_DCT, 
	JNR_Exclusion.TransactionType_DCT, 
	JNR_Exclusion.ChangeDate, 
	JNR_Exclusion.RecordType, 
	JNR_Exclusion.Comments, 
	JNR_Exclusion.PolicyKey_Comp, 
	JNR_Exclusion.Policykey AS Policykey_Exclusion, 
	JNR_Exclusion.TransactionType AS TransactionType_Exclusion, 
	JNR_Exclusion.HistoryID_Comp
	FROM EXP_TrackHistory
	RIGHT OUTER JOIN JNR_Exclusion
	ON JNR_Exclusion.PolicyKey_DCT = EXP_TrackHistory.PolicyKey AND JNR_Exclusion.HistoryID_DCT = EXP_TrackHistory.HistoryID
),
SRT_Bureau AS (
	SELECT
	HistoryID_DCT, 
	HistoryID_Bureau, 
	ChangeDate, 
	TransactionType_DCT, 
	TransactionType_Bureau, 
	PolicyKey_DCT, 
	PolicyKey_Bureau, 
	BureauName, 
	RecordType, 
	Comments, 
	PolicyKey_Comp, 
	HistoryID_Comp, 
	Policykey_Exclusion, 
	TransactionType_Exclusion, 
	HistoryID AS HistoryID_TrackHistory, 
	PolicyKey AS PolicyKey_TrackHistory
	FROM JNR_WIData
	ORDER BY PolicyKey_Comp ASC
),
EXP_Target AS (
	SELECT
	v_Counter+1 AS v_Counter,
	v_Counter AS WCPOLS_DCT_Bureau_ComparisonID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	HistoryID_DCT,
	-- *INF*: TO_INTEGER(HistoryID_DCT)
	CAST(HistoryID_DCT AS INTEGER) AS o_HistoryID_DCT,
	HistoryID_Bureau,
	ChangeDate,
	-- *INF*: IIF(ISNULL (HistoryID_DCT) ,TO_CHAR(SYSDATE,'YYYYMMDD'),ChangeDate)
	IFF(HistoryID_DCT IS NULL, TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD'), ChangeDate) AS v_UnBalancedChangeDate,
	-- *INF*: DECODE(TRUE,
	-- v_BalanceFlag='1',ChangeDate,
	-- v_BalanceFlag='0',v_UnBalancedChangeDate,
	-- '')
	DECODE(
	    TRUE,
	    v_BalanceFlag = '1', ChangeDate,
	    v_BalanceFlag = '0', v_UnBalancedChangeDate,
	    ''
	) AS o_ChangeDate,
	TransactionType_DCT,
	TransactionType_Bureau,
	PolicyKey_DCT,
	PolicyKey_Bureau,
	BureauName,
	RecordType,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(BureauName))='','NO',
	-- ISNULL(BureauName),'NO',
	-- NOT ISNULL(BureauName),'YES',
	-- '')
	-- 
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(BureauName)) = '', 'NO',
	    BureauName IS NULL, 'NO',
	    BureauName IS NOT NULL, 'YES',
	    ''
	) AS ToBeSent,
	Comments,
	-- *INF*: IIF(HistoryID_Bureau=HistoryID_DCT,'1','0')
	IFF(HistoryID_Bureau = HistoryID_DCT, '1', '0') AS v_BalanceFlag,
	-- *INF*: DECODE(TRUE,
	-- ISNULL (HistoryID_DCT) ,'Data did not come through the DCT validation',
	-- ISNULL(HistoryID_Bureau) AND (Policykey=PolicyKey_DCT)
	-- ,'PolicyKey Listed in Exclusion File',
	-- ISNULL(HistoryID_Bureau) AND ISNULL(HistoryID_TrackHistory),'Shred Failure',
	-- ISNULL(HistoryID_Bureau) AND (ISNULL(Comments) OR LTRIM(RTRIM(Comments))=''),'Shred Failure',
	-- Comments)
	DECODE(
	    TRUE,
	    HistoryID_DCT IS NULL, 'Data did not come through the DCT validation',
	    HistoryID_Bureau IS NULL AND (Policykey = PolicyKey_DCT), 'PolicyKey Listed in Exclusion File',
	    HistoryID_Bureau IS NULL AND HistoryID_TrackHistory IS NULL, 'Shred Failure',
	    HistoryID_Bureau IS NULL AND (Comments IS NULL OR LTRIM(RTRIM(Comments)) = ''), 'Shred Failure',
	    Comments
	) AS v_UnBalancedComments,
	'' AS v_BalancedComments,
	-- *INF*: DECODE(TRUE,
	-- v_BalanceFlag='1',v_BalancedComments,
	-- v_BalanceFlag='0',v_UnBalancedComments,
	-- 'comments')
	DECODE(
	    TRUE,
	    v_BalanceFlag = '1', v_BalancedComments,
	    v_BalanceFlag = '0', v_UnBalancedComments,
	    'comments'
	) AS o_Comments,
	PolicyKey_Comp,
	Policykey_Exclusion AS Policykey,
	HistoryID_TrackHistory,
	PolicyKey_TrackHistory
	FROM SRT_Bureau
),
LKP_WorkWCTrackHistory AS (
	SELECT
	HistoryID,
	PolicyKey,
	HistoryID_DCT
	FROM (
		Select distinct HistoryID as HistoryID,PolicyKey as PolicyKey from WorkWCTrackHistory
		where AuditID<>@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		@{pipeline().parameters.WHERE_CLAUSE_LKP}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID ORDER BY HistoryID) = 1
),
EXP_Filter AS (
	SELECT
	EXP_Target.WCPOLS_DCT_Bureau_ComparisonID,
	EXP_Target.AuditID,
	EXP_Target.HistoryID_DCT,
	EXP_Target.HistoryID_Bureau,
	EXP_Target.o_ChangeDate AS ChangeDate,
	EXP_Target.TransactionType_DCT,
	EXP_Target.TransactionType_Bureau,
	EXP_Target.PolicyKey_DCT,
	EXP_Target.PolicyKey_Bureau,
	EXP_Target.BureauName,
	EXP_Target.ToBeSent,
	EXP_Target.o_Comments AS Comments,
	LKP_WorkWCTrackHistory.HistoryID AS LKP_HistoryID,
	-- *INF*: IIF((NOT ISNULL(LKP_HistoryID) AND LTRIM(RTRIM(Comments))='Shred Failure'),'0','1')
	IFF((LKP_HistoryID IS NULL AND LTRIM(RTRIM(Comments)) = 'Shred FailNOT ure'), '0', '1') AS FilterFlag
	FROM EXP_Target
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.HistoryID = EXP_Target.o_HistoryID_DCT
),
FIL_ChangeDateIssues AS (
	SELECT
	WCPOLS_DCT_Bureau_ComparisonID, 
	AuditID, 
	HistoryID_DCT, 
	HistoryID_Bureau, 
	ChangeDate, 
	TransactionType_DCT, 
	TransactionType_Bureau, 
	PolicyKey_DCT, 
	PolicyKey_Bureau, 
	BureauName, 
	ToBeSent, 
	Comments, 
	LKP_HistoryID, 
	FilterFlag
	FROM EXP_Filter
	WHERE FilterFlag='1'
),
WCPOLS_DCT_Bureau_Comparison AS (
	INSERT INTO WCPOLS_DCT_Bureau_Comparison
	(WCPOLS_DCT_Bureau_Comparison, AuditID, HistoryID_DCT, HistoryID_Bureau, ChangeDate, TransactionType_DCT, TransactionType_Bureau, PolicyKey_DCT, PolicyKey_Bureau, BureauName, ToBeSent, Comments)
	SELECT 
	WCPOLS_DCT_Bureau_ComparisonID AS WCPOLS_DCT_BUREAU_COMPARISON, 
	AUDITID, 
	HISTORYID_DCT, 
	HISTORYID_BUREAU, 
	CHANGEDATE, 
	TRANSACTIONTYPE_DCT, 
	TRANSACTIONTYPE_BUREAU, 
	POLICYKEY_DCT, 
	POLICYKEY_BUREAU, 
	BUREAUNAME, 
	TOBESENT, 
	COMMENTS
	FROM FIL_ChangeDateIssues
),