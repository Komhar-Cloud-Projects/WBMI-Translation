WITH
SQ_DC_BIL_BillItem AS (
	SELECT BI.ItemId, BI.AccountId, BI.PolicyTermId, BI.TransactionDate, null as TransactionReference, BI.ItemEffectiveDate, BI.ItemExpirationDate, BI.ReceivableTypeCode, BI.ReceivableSubTypeCode, 
	BI.TransactionTypeCode, BI.CoverageReference, BI.UnitReference, BI.AggregationReference, BI.CommissionGroupReference, BI.StateCode, BI.Description, BI.BillInFullIndicator, BI.CurrencyCulture, 
	BI.OriginalTransactionAmount, BI.ItemAmount, BI.TransferredAmount, BI.ItemCommissionType, BI.ItemCommissionAmount, BI.ItemCommissionPercent, BI.CommissionAuthorizedAmount, BI.CommissionPlanId, 
	BI.ScheduledIndicator, BI.PostedTimestamp, BI.LastUpdatedTimestamp, BI.LastUpdatedUserId, null as BillItemLockingTS, null as BillItemExtendedData, BI.EquityBearingIndicator, BI.TriggerTransactionIndicator,
	null as FinancialReportingGroupReference, null as TransactionGUID 
	FROM
	 DC_BIL_BillItem BI with(nolock)
	WHERE
	BI.LastUpdatedTimestamp > DATEADD(DD, @{pipeline().parameters.NO_OF_DAYS} ,'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	BI.ItemId % @{pipeline().parameters.NUM_OF_PARTITIONS}=1
	
	UNION ALL
	SELECT BI.ItemId, BI.AccountId, BI.PolicyTermId, BI.TransactionDate, null as TransactionReference, BI.ItemEffectiveDate, BI.ItemExpirationDate, BI.ReceivableTypeCode, BI.ReceivableSubTypeCode, 
	BI.TransactionTypeCode, BI.CoverageReference, BI.UnitReference, BI.AggregationReference, BI.CommissionGroupReference, BI.StateCode, BI.Description, BI.BillInFullIndicator, BI.CurrencyCulture, 
	BI.OriginalTransactionAmount, BI.ItemAmount, BI.TransferredAmount, BI.ItemCommissionType, BI.ItemCommissionAmount, BI.ItemCommissionPercent, BI.CommissionAuthorizedAmount, BI.CommissionPlanId, 
	BI.ScheduledIndicator, BI.PostedTimestamp, BI.LastUpdatedTimestamp, BI.LastUpdatedUserId, null as BillItemLockingTS, null as BillItemExtendedData, BI.EquityBearingIndicator, BI.TriggerTransactionIndicator,
	null as FinancialReportingGroupReference, null as TransactionGUID 
	FROM
	 DC_BIL_BillItem BI with(nolock)
	WHERE
	BI.LastUpdatedTimestamp > DATEADD(DD, @{pipeline().parameters.NO_OF_DAYS} ,'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	BI.ItemId % @{pipeline().parameters.NUM_OF_PARTITIONS}=2
	
	UNION ALL
	SELECT BI.ItemId, BI.AccountId, BI.PolicyTermId, BI.TransactionDate, null as TransactionReference, BI.ItemEffectiveDate, BI.ItemExpirationDate, BI.ReceivableTypeCode, BI.ReceivableSubTypeCode, 
	BI.TransactionTypeCode, BI.CoverageReference, BI.UnitReference, BI.AggregationReference, BI.CommissionGroupReference, BI.StateCode, BI.Description, BI.BillInFullIndicator, BI.CurrencyCulture, 
	BI.OriginalTransactionAmount, BI.ItemAmount, BI.TransferredAmount, BI.ItemCommissionType, BI.ItemCommissionAmount, BI.ItemCommissionPercent, BI.CommissionAuthorizedAmount, BI.CommissionPlanId, 
	BI.ScheduledIndicator, BI.PostedTimestamp, BI.LastUpdatedTimestamp, BI.LastUpdatedUserId, null as BillItemLockingTS, null as BillItemExtendedData, BI.EquityBearingIndicator, BI.TriggerTransactionIndicator,
	null as FinancialReportingGroupReference, null as TransactionGUID 
	FROM
	 DC_BIL_BillItem BI with(nolock)
	WHERE
	BI.LastUpdatedTimestamp > DATEADD(DD, @{pipeline().parameters.NO_OF_DAYS} ,'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	BI.ItemId % @{pipeline().parameters.NUM_OF_PARTITIONS}=3
	
	UNION ALL
	SELECT BI.ItemId, BI.AccountId, BI.PolicyTermId, BI.TransactionDate, null as TransactionReference, BI.ItemEffectiveDate, BI.ItemExpirationDate, BI.ReceivableTypeCode, BI.ReceivableSubTypeCode, 
	BI.TransactionTypeCode, BI.CoverageReference, BI.UnitReference, BI.AggregationReference, BI.CommissionGroupReference, BI.StateCode, BI.Description, BI.BillInFullIndicator, BI.CurrencyCulture, 
	BI.OriginalTransactionAmount, BI.ItemAmount, BI.TransferredAmount, BI.ItemCommissionType, BI.ItemCommissionAmount, BI.ItemCommissionPercent, BI.CommissionAuthorizedAmount, BI.CommissionPlanId, 
	BI.ScheduledIndicator, BI.PostedTimestamp, BI.LastUpdatedTimestamp, BI.LastUpdatedUserId, null as BillItemLockingTS, null as BillItemExtendedData, BI.EquityBearingIndicator, BI.TriggerTransactionIndicator,
	null as FinancialReportingGroupReference, null as TransactionGUID 
	FROM
	 DC_BIL_BillItem BI with(nolock)
	WHERE
	BI.LastUpdatedTimestamp > DATEADD(DD, @{pipeline().parameters.NO_OF_DAYS} ,'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	BI.ItemId % @{pipeline().parameters.NUM_OF_PARTITIONS}=4
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	ItemId,
	AccountId,
	PolicyTermId,
	TransactionDate,
	TransactionReference,
	ItemEffectiveDate,
	ItemExpirationDate,
	ReceivableTypeCode,
	ReceivableSubTypeCode,
	TransactionTypeCode,
	CoverageReference,
	UnitReference,
	AggregationReference,
	CommissionGroupReference,
	StateCode,
	Description,
	BillInFullIndicator,
	CurrencyCulture,
	OriginalTransactionAmount,
	ItemAmount,
	TransferredAmount,
	ItemCommissionType,
	ItemCommissionAmount,
	ItemCommissionPercent,
	CommissionAuthorizedAmount,
	CommissionPlanId,
	ScheduledIndicator,
	PostedTimestamp,
	LastUpdatedTimestamp,
	LastUpdatedUserId,
	BillItemLockingTS,
	BillItemExtendedData,
	EquityBearingIndicator,
	TriggerTransactionIndicator,
	FinancialReportingGroupReference,
	TransactionGUID
	FROM SQ_DC_BIL_BillItem
),
DCBILBillItemStage AS (
	TRUNCATE TABLE DCBILBillItemStage;
	INSERT INTO DCBILBillItemStage
	(ExtractDate, SourceSystemId, ItemId, AccountId, PolicyTermId, TransactionDate, TransactionReference, ItemEffectiveDate, ItemExpirationDate, ReceivableTypeCode, ReceivableSubTypeCode, TransactionTypeCode, CoverageReference, UnitReference, AggregationReference, CommissionGroupReference, StateCode, Description, BillInFullIndicator, CurrencyCulture, OriginalTransactionAmount, ItemAmount, TransferredAmount, ItemCommissionType, ItemCommissionAmount, ItemCommissionPercent, CommissionAuthorizedAmount, CommissionPlanId, ScheduledIndicator, PostedTimestamp, LastUpdatedTimestamp, LastUpdatedUserId, BillItemLockingTS, BillItemExtendedData, EquityBearingIndicator, TriggerTransactionIndicator, FinancialReportingGroupReference, TransactionGUID)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	ITEMID, 
	ACCOUNTID, 
	POLICYTERMID, 
	TRANSACTIONDATE, 
	TRANSACTIONREFERENCE, 
	ITEMEFFECTIVEDATE, 
	ITEMEXPIRATIONDATE, 
	RECEIVABLETYPECODE, 
	RECEIVABLESUBTYPECODE, 
	TRANSACTIONTYPECODE, 
	COVERAGEREFERENCE, 
	UNITREFERENCE, 
	AGGREGATIONREFERENCE, 
	COMMISSIONGROUPREFERENCE, 
	STATECODE, 
	DESCRIPTION, 
	BILLINFULLINDICATOR, 
	CURRENCYCULTURE, 
	ORIGINALTRANSACTIONAMOUNT, 
	ITEMAMOUNT, 
	TRANSFERREDAMOUNT, 
	ITEMCOMMISSIONTYPE, 
	ITEMCOMMISSIONAMOUNT, 
	ITEMCOMMISSIONPERCENT, 
	COMMISSIONAUTHORIZEDAMOUNT, 
	COMMISSIONPLANID, 
	SCHEDULEDINDICATOR, 
	POSTEDTIMESTAMP, 
	LASTUPDATEDTIMESTAMP, 
	LASTUPDATEDUSERID, 
	BILLITEMLOCKINGTS, 
	BILLITEMEXTENDEDDATA, 
	EQUITYBEARINGINDICATOR, 
	TRIGGERTRANSACTIONINDICATOR, 
	FINANCIALREPORTINGGROUPREFERENCE, 
	TRANSACTIONGUID
	FROM EXP_Metadata
),