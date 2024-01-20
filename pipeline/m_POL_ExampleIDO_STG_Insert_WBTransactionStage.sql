WITH
SQ_WB_Transaction AS (
	WITH cte_WBTransaction(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.TransactionId, 
	X.WB_TransactionId, 
	X.SessionId, 
	X.ProRataFactor, 
	X.QuoteActionUserClassification, 
	X.QuoteActionTimeStamp, 
	X.QuoteActionUserName, 
	X.QuoteActionStatus, 
	X.VerifiedDate, 
	X.DataFix, 
	X.DataFixDate, 
	X.DataFixType,
	X.DeclaredEvent,
	X.HistoryIDOriginal,
	X.OriginalID, 
	X.OnsetBy
	FROM  
	WB_Transaction X
	inner join
	cte_WBTransaction Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Values AS (
	SELECT
	TransactionId,
	WB_TransactionId,
	SessionId,
	ProRataFactor,
	QuoteActionUserClassification,
	QuoteActionTimeStamp,
	QuoteActionUserName,
	QuoteActionStatus,
	VerifiedDate,
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	DataFix,
	DataFixDate,
	DataFixType,
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
	FROM SQ_WB_Transaction
),
WBTransactionStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBTransactionStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBTransactionStage
	(ExtractDate, SourceSystemId, TransactionId, WBTransactionId, SessionId, ProRataFactor, QuoteActionUserClassification, QuoteActionTimeStamp, QuoteActionUserName, QuoteActionStatus, VerifiedDate, DataFix, DataFixDate, DataFixType, DeclaredEvent, HistoryIDOriginal, OriginalID, OnsetBy)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	TRANSACTIONID, 
	WB_TransactionId AS WBTRANSACTIONID, 
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