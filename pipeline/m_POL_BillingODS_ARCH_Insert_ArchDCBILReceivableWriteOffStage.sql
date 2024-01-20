WITH
SQ_DCBILReceivableWriteOffStage AS (
	SELECT
		DCBILReceivableWriteOffStageId,
		ExtractDate,
		SourceSystemId,
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
	FROM DCBILReceivableWriteOffStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCBILReceivableWriteOffStageId,
	ExtractDate,
	SourceSystemId,
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
	FROM SQ_DCBILReceivableWriteOffStage
),
LKP_ArchExist AS (
	SELECT
	ArchDCBILReceivableWriteOffStageId,
	ReceivableWriteOffId
	FROM (
		SELECT 
			ArchDCBILReceivableWriteOffStageId,
			ReceivableWriteOffId
		FROM ArchDCBILReceivableWriteOffStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ReceivableWriteOffId ORDER BY ArchDCBILReceivableWriteOffStageId) = 1
),
FIL_Exist AS (
	SELECT
	LKP_ArchExist.ArchDCBILReceivableWriteOffStageId AS lkp_ArchDCBILReceivableWriteOffStageId, 
	EXP_Metadata.o_AuditId, 
	EXP_Metadata.DCBILReceivableWriteOffStageId, 
	EXP_Metadata.ExtractDate, 
	EXP_Metadata.SourceSystemId, 
	EXP_Metadata.ReceivableWriteOffId, 
	EXP_Metadata.ReceivableSourceId, 
	EXP_Metadata.AccountId, 
	EXP_Metadata.PolicyTermId, 
	EXP_Metadata.ReceivableSourceTypeCode, 
	EXP_Metadata.WriteOffStatusCode, 
	EXP_Metadata.WriteOffAmount, 
	EXP_Metadata.WriteOffTypeCode, 
	EXP_Metadata.WriteOffRequestDate, 
	EXP_Metadata.WriteOffProcessedDateTime, 
	EXP_Metadata.WriteOffReasonCode, 
	EXP_Metadata.ReversalDateTime, 
	EXP_Metadata.ReversalReasonCode, 
	EXP_Metadata.LastUpdatedTimestamp, 
	EXP_Metadata.LastUpdatedUserId, 
	EXP_Metadata.ReceivableWriteOffLockingTS, 
	EXP_Metadata.WriteOffNetAmount, 
	EXP_Metadata.WriteOffCommissionAmount, 
	EXP_Metadata.TransactionGUID
	FROM EXP_Metadata
	LEFT JOIN LKP_ArchExist
	ON LKP_ArchExist.ReceivableWriteOffId = EXP_Metadata.ReceivableWriteOffId
	WHERE ISNULL(lkp_ArchDCBILReceivableWriteOffStageId)
),
ArchDCBILReceivableWriteOffStage AS (
	INSERT INTO ArchDCBILReceivableWriteOffStage
	(ExtractDate, SourceSystemId, AuditId, DCBILReceivableWriteOffStageId, ReceivableWriteOffId, ReceivableSourceId, AccountId, PolicyTermId, ReceivableSourceTypeCode, WriteOffStatusCode, WriteOffAmount, WriteOffTypeCode, WriteOffRequestDate, WriteOffProcessedDateTime, WriteOffReasonCode, ReversalDateTime, ReversalReasonCode, LastUpdatedTimestamp, LastUpdatedUserId, ReceivableWriteOffLockingTS, WriteOffNetAmount, WriteOffCommissionAmount, TransactionGUID)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBILRECEIVABLEWRITEOFFSTAGEID, 
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
	FROM FIL_Exist
),