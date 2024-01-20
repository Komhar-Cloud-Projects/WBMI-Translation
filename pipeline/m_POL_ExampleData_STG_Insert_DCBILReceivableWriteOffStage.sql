WITH
SQ_DC_BIL_ReceivableWriteOff AS (
	SELECT
		ReceivableWriteOffId,
		ReceivableSourceId,
		AccountId,
		PolicyTermId,
		ReceivableSourceTypeCode,
		WriteOffStatusCode,
		WriteOffAmount,
		WriteOffTypeCode,
		WriteOffRequestDate,
		WriteOffProcessedDateTime,
		WriteOffReasonCode,
		ReversalDateTime,
		ReversalReasonCode,
		LastUpdatedTimestamp,
		LastUpdatedUserId,
		ReceivableWriteOffLockingTS,
		WriteOffNetAmount,
		WriteOffCommissionAmount,
		TransactionGUID
	FROM DC_BIL_ReceivableWriteOff
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	ReceivableWriteOffId,
	ReceivableSourceId,
	AccountId,
	PolicyTermId,
	ReceivableSourceTypeCode,
	WriteOffStatusCode,
	WriteOffAmount,
	WriteOffTypeCode,
	WriteOffRequestDate,
	WriteOffProcessedDateTime,
	WriteOffReasonCode,
	ReversalDateTime,
	ReversalReasonCode,
	LastUpdatedTimestamp,
	LastUpdatedUserId,
	ReceivableWriteOffLockingTS,
	WriteOffNetAmount,
	WriteOffCommissionAmount,
	TransactionGUID
	FROM SQ_DC_BIL_ReceivableWriteOff
),
DCBILReceivableWriteOffStage AS (
	TRUNCATE TABLE DCBILReceivableWriteOffStage;
	INSERT INTO DCBILReceivableWriteOffStage
	(ExtractDate, SourceSystemId, ReceivableWriteOffId, ReceivableSourceId, AccountId, PolicyTermId, ReceivableSourceTypeCode, WriteOffStatusCode, WriteOffAmount, WriteOffTypeCode, WriteOffRequestDate, WriteOffProcessedDateTime, WriteOffReasonCode, ReversalDateTime, ReversalReasonCode, LastUpdatedTimestamp, LastUpdatedUserId, ReceivableWriteOffLockingTS, WriteOffNetAmount, WriteOffCommissionAmount, TransactionGUID)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	RECEIVABLEWRITEOFFID, 
	RECEIVABLESOURCEID, 
	ACCOUNTID, 
	POLICYTERMID, 
	RECEIVABLESOURCETYPECODE, 
	WRITEOFFSTATUSCODE, 
	WRITEOFFAMOUNT, 
	WRITEOFFTYPECODE, 
	WRITEOFFREQUESTDATE, 
	WRITEOFFPROCESSEDDATETIME, 
	WRITEOFFREASONCODE, 
	REVERSALDATETIME, 
	REVERSALREASONCODE, 
	LASTUPDATEDTIMESTAMP, 
	LASTUPDATEDUSERID, 
	RECEIVABLEWRITEOFFLOCKINGTS, 
	WRITEOFFNETAMOUNT, 
	WRITEOFFCOMMISSIONAMOUNT, 
	TRANSACTIONGUID
	FROM EXP_Metadata
),