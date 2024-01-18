WITH
SQ_DC_BIL_BillItemSchedule AS (
	SELECT B.ItemScheduleId, B.AccountId, B.PolicyTermId, B.ItemId, B.InstallmentTypeCode, B.InstallmentDate, B.DueDate, B.ReceivableTypeCode, B.ReceivableSubTypeCode, B.TransactionTypeCode, B.CoverageReference,
	B.UnitReference, B.AggregationReference, B.AllocationPriority, B.CurrencyCulture, B.ItemScheduleAmount, B.ItemClosedToCashAmount, B.ItemClosedToCreditAmount, B.ItemWrittenOffAmount, B.ItemRedistributedAmount,
	B.ItemClosedIndicator, B.FirstInvoiceId, B.LastUpdatedTimestamp, B.LastUpdatedUserId, null as ItemScheduleLockingTS, null as ItemScheduleExtendedData, null as TransactionGUID 
	FROM
	 DC_BIL_BillItemSchedule B with(nolock)
	WHERE
	B.LastUpdatedTimestamp >= DATEADD(DD,@{pipeline().parameters.NO_OF_DAYS},'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	B.ItemScheduleId % @{pipeline().parameters.NUM_OF_PARTITIONS}=1
	
	UNION ALL
	SELECT B.ItemScheduleId, B.AccountId, B.PolicyTermId, B.ItemId, B.InstallmentTypeCode, B.InstallmentDate, B.DueDate, B.ReceivableTypeCode, B.ReceivableSubTypeCode, B.TransactionTypeCode, B.CoverageReference,
	B.UnitReference, B.AggregationReference, B.AllocationPriority, B.CurrencyCulture, B.ItemScheduleAmount, B.ItemClosedToCashAmount, B.ItemClosedToCreditAmount, B.ItemWrittenOffAmount, B.ItemRedistributedAmount,
	B.ItemClosedIndicator, B.FirstInvoiceId, B.LastUpdatedTimestamp, B.LastUpdatedUserId, null as ItemScheduleLockingTS, null as ItemScheduleExtendedData, null as TransactionGUID 
	FROM
	 DC_BIL_BillItemSchedule B with(nolock)
	WHERE
	B.LastUpdatedTimestamp >= DATEADD(DD,@{pipeline().parameters.NO_OF_DAYS},'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	B.ItemScheduleId % @{pipeline().parameters.NUM_OF_PARTITIONS}=2
	
	UNION ALL
	SELECT B.ItemScheduleId, B.AccountId, B.PolicyTermId, B.ItemId, B.InstallmentTypeCode, B.InstallmentDate, B.DueDate, B.ReceivableTypeCode, B.ReceivableSubTypeCode, B.TransactionTypeCode, B.CoverageReference,
	B.UnitReference, B.AggregationReference, B.AllocationPriority, B.CurrencyCulture, B.ItemScheduleAmount, B.ItemClosedToCashAmount, B.ItemClosedToCreditAmount, B.ItemWrittenOffAmount, B.ItemRedistributedAmount,
	B.ItemClosedIndicator, B.FirstInvoiceId, B.LastUpdatedTimestamp, B.LastUpdatedUserId, null as ItemScheduleLockingTS, null as ItemScheduleExtendedData, null as TransactionGUID 
	FROM
	 DC_BIL_BillItemSchedule B with(nolock)
	WHERE
	B.LastUpdatedTimestamp >= DATEADD(DD,@{pipeline().parameters.NO_OF_DAYS},'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	B.ItemScheduleId % @{pipeline().parameters.NUM_OF_PARTITIONS}=3
	
	UNION ALL
	SELECT B.ItemScheduleId, B.AccountId, B.PolicyTermId, B.ItemId, B.InstallmentTypeCode, B.InstallmentDate, B.DueDate, B.ReceivableTypeCode, B.ReceivableSubTypeCode, B.TransactionTypeCode, B.CoverageReference,
	B.UnitReference, B.AggregationReference, B.AllocationPriority, B.CurrencyCulture, B.ItemScheduleAmount, B.ItemClosedToCashAmount, B.ItemClosedToCreditAmount, B.ItemWrittenOffAmount, B.ItemRedistributedAmount,
	B.ItemClosedIndicator, B.FirstInvoiceId, B.LastUpdatedTimestamp, B.LastUpdatedUserId, null as ItemScheduleLockingTS, null as ItemScheduleExtendedData, null as TransactionGUID 
	FROM
	 DC_BIL_BillItemSchedule B with(nolock)
	WHERE
	B.LastUpdatedTimestamp >= DATEADD(DD,@{pipeline().parameters.NO_OF_DAYS},'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	B.ItemScheduleId % @{pipeline().parameters.NUM_OF_PARTITIONS}=4
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	FROM SQ_DC_BIL_BillItemSchedule
),
DCBILBillItemScheduleStage AS (
	TRUNCATE TABLE DCBILBillItemScheduleStage;
	INSERT INTO DCBILBillItemScheduleStage
	(ExtractDate, SourceSystemId, ItemScheduleId, AccountId, PolicyTermId, ItemId, InstallmentTypeCode, InstallmentDate, DueDate, ReceivableTypeCode, ReceivableSubTypeCode, TransactionTypeCode, CoverageReference, UnitReference, AggregationReference, AllocationPriority, CurrencyCulture, ItemScheduleAmount, ItemClosedToCashAmount, ItemClosedToCreditAmount, ItemWrittenOffAmount, ItemRedistributedAmount, ItemClosedIndicator, FirstInvoiceId, LastUpdatedTimestamp, LastUpdatedUserId, ItemScheduleLockingTS, ItemScheduleExtendedData, TransactionGUID, InvoiceStatus)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
	FROM EXP_Metadata
),