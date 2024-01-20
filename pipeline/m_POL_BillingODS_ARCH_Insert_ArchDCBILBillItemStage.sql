WITH
SQ_DCBILBillItemStage AS (
	SELECT
		DCBILBillItemStageId,
		ExtractDate,
		SourceSystemId,
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
	FROM DCBILBillItemStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCBILBillItemStageId,
	ExtractDate,
	SourceSystemId,
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
	FROM SQ_DCBILBillItemStage
),
LKP_ArchExsit AS (
	SELECT
	ArchDCBILBillItemStageId,
	ItemId
	FROM (
		SELECT 
			ArchDCBILBillItemStageId,
			ItemId
		FROM ArchDCBILBillItemStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ItemId ORDER BY ArchDCBILBillItemStageId) = 1
),
FIL_Exist AS (
	SELECT
	LKP_ArchExsit.ArchDCBILBillItemStageId AS lkp_ArchDCBILBillItemStageId, 
	EXP_Metadata.o_AuditId AS AuditId, 
	EXP_Metadata.DCBILBillItemStageId, 
	EXP_Metadata.ExtractDate, 
	EXP_Metadata.SourceSystemId, 
	EXP_Metadata.ItemId, 
	EXP_Metadata.AccountId, 
	EXP_Metadata.PolicyTermId, 
	EXP_Metadata.TransactionDate, 
	EXP_Metadata.TransactionReference, 
	EXP_Metadata.ItemEffectiveDate, 
	EXP_Metadata.ItemExpirationDate, 
	EXP_Metadata.ReceivableTypeCode, 
	EXP_Metadata.ReceivableSubTypeCode, 
	EXP_Metadata.TransactionTypeCode, 
	EXP_Metadata.CoverageReference, 
	EXP_Metadata.UnitReference, 
	EXP_Metadata.AggregationReference, 
	EXP_Metadata.CommissionGroupReference, 
	EXP_Metadata.StateCode, 
	EXP_Metadata.Description, 
	EXP_Metadata.BillInFullIndicator, 
	EXP_Metadata.CurrencyCulture, 
	EXP_Metadata.OriginalTransactionAmount, 
	EXP_Metadata.ItemAmount, 
	EXP_Metadata.TransferredAmount, 
	EXP_Metadata.ItemCommissionType, 
	EXP_Metadata.ItemCommissionAmount, 
	EXP_Metadata.ItemCommissionPercent, 
	EXP_Metadata.CommissionAuthorizedAmount, 
	EXP_Metadata.CommissionPlanId, 
	EXP_Metadata.ScheduledIndicator, 
	EXP_Metadata.PostedTimestamp, 
	EXP_Metadata.LastUpdatedTimestamp, 
	EXP_Metadata.LastUpdatedUserId, 
	EXP_Metadata.BillItemLockingTS, 
	EXP_Metadata.BillItemExtendedData, 
	EXP_Metadata.EquityBearingIndicator, 
	EXP_Metadata.TriggerTransactionIndicator, 
	EXP_Metadata.FinancialReportingGroupReference, 
	EXP_Metadata.TransactionGUID
	FROM EXP_Metadata
	LEFT JOIN LKP_ArchExsit
	ON LKP_ArchExsit.ItemId = EXP_Metadata.ItemId
	WHERE ISNULL(lkp_ArchDCBILBillItemStageId)
),
ArchDCBILBillItemStage AS (
	INSERT INTO ArchDCBILBillItemStage
	(ExtractDate, SourceSystemId, AuditId, DCBILBillItemStageId, ItemId, AccountId, PolicyTermId, TransactionDate, TransactionReference, ItemEffectiveDate, ItemExpirationDate, ReceivableTypeCode, ReceivableSubTypeCode, TransactionTypeCode, CoverageReference, UnitReference, AggregationReference, CommissionGroupReference, StateCode, Description, BillInFullIndicator, CurrencyCulture, OriginalTransactionAmount, ItemAmount, TransferredAmount, ItemCommissionType, ItemCommissionAmount, ItemCommissionPercent, CommissionAuthorizedAmount, CommissionPlanId, ScheduledIndicator, PostedTimestamp, LastUpdatedTimestamp, LastUpdatedUserId, BillItemLockingTS, BillItemExtendedData, EquityBearingIndicator, TriggerTransactionIndicator, FinancialReportingGroupReference, TransactionGUID)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	DCBILBILLITEMSTAGEID, 
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
	FROM FIL_Exist
),