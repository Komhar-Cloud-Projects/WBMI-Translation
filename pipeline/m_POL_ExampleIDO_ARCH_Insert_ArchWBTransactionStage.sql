WITH
SQ_WBTransactionStage AS (
	SELECT
		WBTransactionStageId,
		ExtractDate,
		SourceSystemId,
		TransactionId,
		WBTransactionId,
		SessionId,
		ProRataFactor,
		QuoteActionUserClassification,
		QuoteActionTimeStamp,
		QuoteActionUserName,
		QuoteActionStatus,
		VerifiedDate,
		DataFix,
		DataFixDate,
		DataFixType,
		DeclaredEvent,
		HistoryIDOriginal,
		OriginalID,
		OnsetBy
	FROM WBTransactionStage
),
EXP_Values AS (
	SELECT
	WBTransactionStageId,
	ExtractDate,
	SourceSystemId,
	TransactionId,
	WBTransactionId,
	SessionId,
	ProRataFactor,
	QuoteActionUserClassification,
	QuoteActionTimeStamp,
	QuoteActionUserName,
	QuoteActionStatus,
	VerifiedDate,
	DataFix,
	DataFixDate,
	DataFixType,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DeclaredEvent AS i_DeclaredEvent,
	-- *INF*: DECODE(i_DeclaredEvent, 'T', 1, 'F', 0,Null)
	DECODE(
	    i_DeclaredEvent,
	    'T', 1,
	    'F', 0,
	    Null
	) AS o_DeclaredEvent,
	HistoryIDOriginal,
	OriginalID,
	OnsetBy
	FROM SQ_WBTransactionStage
),
ArchWBTransactionStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBTransactionStage
	(ExtractDate, SourceSystemId, AuditId, WBTransactionStageId, TransactionId, WBTransactionId, SessionId, ProRataFactor, QuoteActionUserClassification, QuoteActionTimeStamp, QuoteActionUserName, QuoteActionStatus, VerifiedDate, DataFix, DataFixDate, DataFixType, DeclaredEvent, HistoryIDOriginal, OriginalID, OnsetBy)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBTRANSACTIONSTAGEID, 
	TRANSACTIONID, 
	WBTRANSACTIONID, 
	SESSIONID, 
	PRORATAFACTOR, 
	QUOTEACTIONUSERCLASSIFICATION, 
	QUOTEACTIONTIMESTAMP, 
	QUOTEACTIONUSERNAME, 
	QUOTEACTIONSTATUS, 
	VERIFIEDDATE, 
	DATAFIX, 
	DATAFIXDATE, 
	DATAFIXTYPE, 
	o_DeclaredEvent AS DECLAREDEVENT, 
	HISTORYIDORIGINAL, 
	ORIGINALID, 
	ONSETBY
	FROM EXP_Values
),