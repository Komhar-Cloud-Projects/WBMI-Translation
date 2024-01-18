WITH
SQ_DCBILBillItemScheduleStage AS (
	SELECT
		DCBILBillItemScheduleStageId,
		ExtractDate,
		SourceSystemId,
		ItemScheduleId,
		AccountId,
		PolicyTermId,
		ItemId,
		InstallmentTypeCode,
		InstallmentDate,
		DueDate,
		ReceivableTypeCode,
		ReceivableSubTypeCode,
		TransactionTypeCode,
		CoverageReference,
		UnitReference,
		AggregationReference,
		AllocationPriority,
		CurrencyCulture,
		ItemScheduleAmount,
		ItemClosedToCashAmount,
		ItemClosedToCreditAmount,
		ItemWrittenOffAmount,
		ItemRedistributedAmount,
		ItemClosedIndicator,
		FirstInvoiceId,
		LastUpdatedTimestamp,
		LastUpdatedUserId,
		ItemScheduleLockingTS,
		ItemScheduleExtendedData,
		TransactionGUID,
		InvoiceStatus
	FROM DCBILBillItemScheduleStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCBILBillItemScheduleStageId,
	ExtractDate,
	SourceSystemId,
	ItemScheduleId,
	AccountId,
	PolicyTermId,
	ItemId,
	InstallmentTypeCode,
	InstallmentDate,
	DueDate,
	ReceivableTypeCode,
	ReceivableSubTypeCode,
	TransactionTypeCode,
	CoverageReference,
	UnitReference,
	AggregationReference,
	AllocationPriority,
	CurrencyCulture,
	ItemScheduleAmount,
	ItemClosedToCashAmount,
	ItemClosedToCreditAmount,
	ItemWrittenOffAmount,
	ItemRedistributedAmount,
	ItemClosedIndicator,
	FirstInvoiceId,
	LastUpdatedTimestamp,
	LastUpdatedUserId,
	ItemScheduleLockingTS,
	ItemScheduleExtendedData,
	TransactionGUID,
	InvoiceStatus
	FROM SQ_DCBILBillItemScheduleStage
),
LKP_ArchExsit AS (
	SELECT
	ArchDCBILBillItemScheduleStageId,
	ItemScheduleId
	FROM (
		SELECT 
			ArchDCBILBillItemScheduleStageId,
			ItemScheduleId
		FROM ArchDCBILBillItemScheduleStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ItemScheduleId ORDER BY ArchDCBILBillItemScheduleStageId) = 1
),
FIL_Exist AS (
	SELECT
	LKP_ArchExsit.ArchDCBILBillItemScheduleStageId AS lkp_ArchDCBILBillItemScheduleStageId, 
	EXP_Metadata.o_AuditId AS AuditId, 
	EXP_Metadata.DCBILBillItemScheduleStageId, 
	EXP_Metadata.ExtractDate, 
	EXP_Metadata.SourceSystemId, 
	EXP_Metadata.ItemScheduleId, 
	EXP_Metadata.AccountId, 
	EXP_Metadata.PolicyTermId, 
	EXP_Metadata.ItemId, 
	EXP_Metadata.InstallmentTypeCode, 
	EXP_Metadata.InstallmentDate, 
	EXP_Metadata.DueDate, 
	EXP_Metadata.ReceivableTypeCode, 
	EXP_Metadata.ReceivableSubTypeCode, 
	EXP_Metadata.TransactionTypeCode, 
	EXP_Metadata.CoverageReference, 
	EXP_Metadata.UnitReference, 
	EXP_Metadata.AggregationReference, 
	EXP_Metadata.AllocationPriority, 
	EXP_Metadata.CurrencyCulture, 
	EXP_Metadata.ItemScheduleAmount, 
	EXP_Metadata.ItemClosedToCashAmount, 
	EXP_Metadata.ItemClosedToCreditAmount, 
	EXP_Metadata.ItemWrittenOffAmount, 
	EXP_Metadata.ItemRedistributedAmount, 
	EXP_Metadata.ItemClosedIndicator, 
	EXP_Metadata.FirstInvoiceId, 
	EXP_Metadata.LastUpdatedTimestamp, 
	EXP_Metadata.LastUpdatedUserId, 
	EXP_Metadata.ItemScheduleLockingTS, 
	EXP_Metadata.ItemScheduleExtendedData, 
	EXP_Metadata.TransactionGUID, 
	EXP_Metadata.InvoiceStatus
	FROM EXP_Metadata
	LEFT JOIN LKP_ArchExsit
	ON LKP_ArchExsit.ItemScheduleId = EXP_Metadata.ItemScheduleId
	WHERE ISNULL(lkp_ArchDCBILBillItemScheduleStageId)
),
ArchDCBILBillItemScheduleStage AS (
	INSERT INTO ArchDCBILBillItemScheduleStage
	(ExtractDate, SourceSystemId, AuditId, DCBILBillItemScheduleStageId, ItemScheduleId, AccountId, PolicyTermId, ItemId, InstallmentTypeCode, InstallmentDate, DueDate, ReceivableTypeCode, ReceivableSubTypeCode, TransactionTypeCode, CoverageReference, UnitReference, AggregationReference, AllocationPriority, CurrencyCulture, ItemScheduleAmount, ItemClosedToCashAmount, ItemClosedToCreditAmount, ItemWrittenOffAmount, ItemRedistributedAmount, ItemClosedIndicator, FirstInvoiceId, LastUpdatedTimestamp, LastUpdatedUserId, ItemScheduleLockingTS, ItemScheduleExtendedData, TransactionGUID, InvoiceStatus)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	DCBILBILLITEMSCHEDULESTAGEID, 
	ITEMSCHEDULEID, 
	ACCOUNTID, 
	POLICYTERMID, 
	ITEMID, 
	INSTALLMENTTYPECODE, 
	INSTALLMENTDATE, 
	DUEDATE, 
	RECEIVABLETYPECODE, 
	RECEIVABLESUBTYPECODE, 
	TRANSACTIONTYPECODE, 
	COVERAGEREFERENCE, 
	UNITREFERENCE, 
	AGGREGATIONREFERENCE, 
	ALLOCATIONPRIORITY, 
	CURRENCYCULTURE, 
	ITEMSCHEDULEAMOUNT, 
	ITEMCLOSEDTOCASHAMOUNT, 
	ITEMCLOSEDTOCREDITAMOUNT, 
	ITEMWRITTENOFFAMOUNT, 
	ITEMREDISTRIBUTEDAMOUNT, 
	ITEMCLOSEDINDICATOR, 
	FIRSTINVOICEID, 
	LASTUPDATEDTIMESTAMP, 
	LASTUPDATEDUSERID, 
	ITEMSCHEDULELOCKINGTS, 
	ITEMSCHEDULEEXTENDEDDATA, 
	TRANSACTIONGUID, 
	INVOICESTATUS
	FROM FIL_Exist
),