WITH
SQ_DC_TransactionReportEntry AS (
	WITH cte_TransactionReportEntry(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.TransactionReportEntryId, 
	X.SessionId, 
	X.Charge, 
	X.Count, 
	X.DateTimeStamp, 
	X.EffectiveDate, 
	X.[Index], 
	X.ExampleQuoteId, 
	X.Sequence, 
	X.TransactionRef, 
	X.TransactionType, 
	X.Type
	FROM
	DC_TransactionReportEntry X
	inner join
	cte_TransactionReportEntry Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	TransactionReportEntryId,
	SessionId,
	Charge,
	Count,
	DateTimeStamp,
	EffectiveDate,
	Index,
	ExampleQuoteId,
	Sequence,
	TransactionRef,
	TransactionType,
	Type,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_TransactionReportEntry
),
DCTransactionReportEntryStaging AS (
	TRUNCATE TABLE DCTransactionReportEntryStaging;
	INSERT INTO DCTransactionReportEntryStaging
	(TransactionReportEntryId, SessionId, Charge, Count, DateTimeStamp, EffectiveDate, Index, ExampleQuoteId, Sequence, TransactionRef, TransactionType, Type, ExtractDate, SourceSystemId)
	SELECT 
	TRANSACTIONREPORTENTRYID, 
	SESSIONID, 
	CHARGE, 
	COUNT, 
	DATETIMESTAMP, 
	EFFECTIVEDATE, 
	INDEX, 
	EXAMPLEQUOTEID, 
	SEQUENCE, 
	TRANSACTIONREF, 
	TRANSACTIONTYPE, 
	TYPE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),