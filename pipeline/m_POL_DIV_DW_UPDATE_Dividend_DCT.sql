WITH
SQ_Dividend AS (
	SELECT
		DividendId,
		CurrentSnapshotFlag,
		AuditID,
		EffectiveDate,
		ExpirationDate,
		SourceSystemId,
		CreatedDate,
		ModifiedDate,
		DividendAKId,
		PolicyAKId,
		DividendPayableAmount,
		DividendTransactionEnteredDate,
		DividendRunDate,
		StateCode,
		DividendPlan,
		DividendType,
		SupStateId,
		SupDividendTypeId,
		DividendPaidAmount
	FROM Dividend
	WHERE 1=2
),
Dividend1 AS (
	INSERT INTO Dividend
	(DividendAKId)
	SELECT 
	DIVIDENDAKID
	FROM SQ_Dividend
),