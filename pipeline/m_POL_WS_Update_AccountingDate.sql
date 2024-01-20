WITH
SQ_WorkDCTInBalancePolicy AS (
	SELECT
		WorkDCTInBalancePolicyId,
		HistoryID,
		AccountingDate
	FROM WorkDCTInBalancePolicy
	WHERE ProcessedFlag=0 @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_DefaultValues AS (
	SELECT
	WorkDCTInBalancePolicyId,
	HistoryID,
	'Carrier_BasicTransaction_Rules_2_1_0' AS ManuScriptID,
	'ActiveTransaction.AccountingDate' AS FieldID,
	-- *INF*: '/session/data/policyAdmin/transactions/transaction[HistoryID=' || TO_CHAR(HistoryID) || ']'
	'/session/data/policyAdmin/transactions/transaction[HistoryID=' || TO_CHAR(HistoryID) || ']' AS ContextXpath,
	AccountingDate,
	-- *INF*: TO_CHAR(AccountingDate,'MM/YYYY')
	TO_CHAR(AccountingDate, 'MM/YYYY') AS o_AccountingDate
	FROM SQ_WorkDCTInBalancePolicy
),
RepairAccountingDate AS (-- RepairAccountingDate

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXPTRANS AS (
	SELECT
	REF_PassThoughPKId,
	faultstring,
	1 AS ProcessedFlag,
	XPK_n2_Envelope0
	FROM RepairAccountingDate
),
FILTRANS AS (
	SELECT
	REF_PassThoughPKId, 
	faultstring, 
	ProcessedFlag
	FROM EXPTRANS
	WHERE ISNULL(faultstring)
),
UPD_AccountingDate AS (
	SELECT
	REF_PassThoughPKId AS REF_PassThroughPKId, 
	ProcessedFlag
	FROM FILTRANS
),
TGT_WorkDCTInBalancePolicy_UPDATE AS (
	MERGE INTO WorkDCTInBalancePolicy AS T
	USING UPD_AccountingDate AS S
	ON T.WorkDCTInBalancePolicyId = S.REF_PassThroughPKId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ProcessedFlag = S.ProcessedFlag
),