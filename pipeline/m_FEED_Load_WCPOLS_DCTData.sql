WITH
SQ_History AS (
	Select A.HistoryID,A.PolicyNumber+B.PolicyVersionFormatted POLKEY,A.Type,A.ChangeDate, 'Rewrite_Transaction' RecordType 
	From History  (nolock) A
	inner join @{pipeline().parameters.DBCONNECTION_WBEXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_History B
	on A.HistoryID=B.HistoryId
	WHERE
	A.Change='Open' and A.Comment='New'
	and A.ManuScriptID like '%Carrier_Work%' 
	and A.TransactionStatus = 'Committed'  and A.Type = 'Rewrite' 
	and A.ChangeDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	and A.Type not in (@{pipeline().parameters.EXCLUDE_TRANSACTIONTYPES})
	@{pipeline().parameters.WHERE_CLAUSE_REWRITE}
	
	union
	
	Select A.HistoryID,A.PolicyNumber+B.PolicyVersionFormatted POLKEY,A.Type,A.ChangeDate, 'Non_Rewrite_Endorse_Transactions' RecordType
	From 
	DBO.History A (nolock)
	inner join @{pipeline().parameters.DBCONNECTION_WBEXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_History B
	on A.HistoryID=B.HistoryId
	WHERE Comment <> 'Rewrite-Committed' and 
	ManuScriptID like '%Carrier_Work%' 
	and A.TransactionStatus = 'Committed' 
	and A.change in ('Committed','Rescind')
	and A.ChangeDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	and A.Type not in (@{pipeline().parameters.EXCLUDE_TRANSACTIONTYPES})
	and A.Type not in ('Endorse')
	@{pipeline().parameters.WHERE_CLAUSE}
	
	union
	
	SELECT HistoryID,POLKEY,TYPE,CHANGEDATE, 'Endorse_Transaction' RecordType FROM 
	(
	Select A.HistoryID,A.PolicyNumber+B.PolicyVersionFormatted POLKEY,A.Type,A.ChangeDate,A.Change,RANK() OVER (PARTITION BY A.PolicyNumber+B.PolicyVersionFormatted ORDER BY A.HistoryID DESC) ENDORSE_RANK
	From 
	DBO.History A (nolock)
	inner join @{pipeline().parameters.DBCONNECTION_WBEXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_History B
	on A.HistoryID=B.HistoryId
	WHERE Type='Endorse' and 
	ManuScriptID like '%Carrier_Work%' 
	and A.TransactionStatus = 'Committed' 
	and A.change in ('Committed','Rescind')
	and A.ChangeDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	and A.Type not in (@{pipeline().parameters.EXCLUDE_TRANSACTIONTYPES})
	@{pipeline().parameters.WHERE_CLAUSE_ENDORSE}
	) A
	WHERE A.ENDORSE_RANK=1
	ORDER BY Type
),
EXP_History AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	HistoryID,
	PolicyKey,
	Type,
	ChangeDate,
	RecordType
	FROM SQ_History
),
SQ_SupWCPOLSTransactionTypeNeeded1 AS (
	Select distinct case when SourceTransactionType in ('EndorseAddDeleteState','EndorseNonPremiumBearing','EndorsePremiumBearing') then 'Endorse' else SourceTransactionType end as SourceTransactionType,
	NCCIRequiredFlag,WIRequiredFlag,MIRequiredFlag,MNRequiredFlag
	from 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSTransactionTypeNeeded
	WHERE (NCCIRequiredFlag=1 OR MIRequiredFlag=1 OR WIRequiredFlag=1 OR MNRequiredFlag=1)
	AND CurrentSnapshotFlag=1
	ORDER BY SourceTransactionType
),
JNR_DCTData AS (SELECT
	SQ_SupWCPOLSTransactionTypeNeeded1.SourceTransactionType, 
	SQ_SupWCPOLSTransactionTypeNeeded1.NCCIRequiredFlag, 
	SQ_SupWCPOLSTransactionTypeNeeded1.WIRequiredFlag, 
	SQ_SupWCPOLSTransactionTypeNeeded1.MIRequiredFlag, 
	SQ_SupWCPOLSTransactionTypeNeeded1.MNRequiredFlag, 
	EXP_History.AuditID, 
	EXP_History.HistoryID, 
	EXP_History.PolicyKey, 
	EXP_History.Type, 
	EXP_History.ChangeDate, 
	EXP_History.RecordType
	FROM EXP_History
	LEFT OUTER JOIN SQ_SupWCPOLSTransactionTypeNeeded1
	ON SQ_SupWCPOLSTransactionTypeNeeded1.SourceTransactionType = EXP_History.Type
),
EXP_BureauRequired AS (
	SELECT
	SourceTransactionType,
	NCCIRequiredFlag,
	WIRequiredFlag,
	MIRequiredFlag,
	MNRequiredFlag,
	v_counter+1 AS v_counter,
	v_counter AS WCPOLS_DCT_Data_ID,
	AuditID,
	HistoryID,
	PolicyKey,
	Type,
	ChangeDate,
	-- *INF*: TO_CHAR(ChangeDate,'YYYYMMDD')
	TO_CHAR(ChangeDate, 'YYYYMMDD') AS o_ChangeDate,
	RecordType,
	-- *INF*: IIF(NCCIRequiredFlag='T','','NCCI')
	IFF(NCCIRequiredFlag = 'T', '', 'NCCI') AS v_NCCI,
	-- *INF*: IIF(WIRequiredFlag='T','','WI')
	IFF(WIRequiredFlag = 'T', '', 'WI') AS v_WI,
	-- *INF*: IIF(MIRequiredFlag='T','','MI')
	IFF(MIRequiredFlag = 'T', '', 'MI') AS v_MI,
	-- *INF*: IIF(MNRequiredFlag='T','','MN')
	IFF(MNRequiredFlag = 'T', '', 'MN') AS v_MN,
	-- *INF*: DECODE(TRUE,
	-- v_NCCI<>'' OR v_WI<>'' OR v_MI<>'' OR v_MN<>'','Transaction Type will not be reported to these bureaus'||'('||v_NCCI||'-'||v_WI||'-'||v_MI||'-'||v_MN||')',
	-- '')
	DECODE(
	    TRUE,
	    v_NCCI <> '' OR v_WI <> '' OR v_MI <> '' OR v_MN <> '', 'Transaction Type will not be reported to these bureaus' || '(' || v_NCCI || '-' || v_WI || '-' || v_MI || '-' || v_MN || ')',
	    ''
	) AS Comments
	FROM JNR_DCTData
),
WCPOLS_DCT_Data AS (
	INSERT INTO WCPOLS_DCT_Data
	(WCPOLS_DCT_Data_ID, AuditID, HistoryID, PolicyKey, Type, ChangeDate, RecordType, Comments)
	SELECT 
	WCPOLS_DCT_DATA_ID, 
	AUDITID, 
	HISTORYID, 
	POLICYKEY, 
	TYPE, 
	o_ChangeDate AS CHANGEDATE, 
	RECORDTYPE, 
	COMMENTS
	FROM EXP_BureauRequired
),