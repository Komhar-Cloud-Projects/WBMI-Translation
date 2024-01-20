WITH
SQ_DCTransactionReportEntryStaging AS (
	SELECT DCTransactionReportEntryStaging.DCTransactionReportEntryStagingId, DCTransactionReportEntryStaging.TransactionReportEntryId, DCTransactionReportEntryStaging.SessionId, DCTransactionReportEntryStaging.Charge, DCTransactionReportEntryStaging.Count, DCTransactionReportEntryStaging.DateTimeStamp, DCTransactionReportEntryStaging.EffectiveDate, DCTransactionReportEntryStaging.[Index], DCTransactionReportEntryStaging.ExampleQuoteId, DCTransactionReportEntryStaging.Sequence, DCTransactionReportEntryStaging.TransactionRef, DCTransactionReportEntryStaging.TransactionType, DCTransactionReportEntryStaging.Type, DCTransactionReportEntryStaging.ExtractDate, DCTransactionReportEntryStaging.SourceSystemId 
	FROM
	 DCTransactionReportEntryStaging
),
EXP_Metadata AS (
	SELECT
	DCTransactionReportEntryStagingId,
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
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCTransactionReportEntryStaging
),
archDCTransactionReportEntryStaging AS (
	INSERT INTO archDCTransactionReportEntryStaging
	(TransactionReportEntryId, SessionId, Charge, Count, DateTimeStamp, EffectiveDate, Index, ExampleQuoteId, Sequence, TransactionRef, TransactionType, Type, ExtractDate, SourceSystemId, AuditId)
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),