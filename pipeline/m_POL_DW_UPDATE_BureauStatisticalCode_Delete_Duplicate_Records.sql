WITH
SQ_BureauStatisticalCode_PremiumTransactionAKID AS (
	SELECT 
	MIN(BureauStatisticalCodeID) as BureauStatisticalCodeID,
	PremiumTransactionAKID as PremiumTransactionAKID 
	FROM BureauStatisticalCode
	WHERE PassThroughChargeTransactionAKID = -1
	AND CurrentSnapshotFlag=1
	GROUP BY PremiumTransactionAKID
	HAVING COUNT(*) >1
),
EXP_Set_Flag_And_Date_PremiumTransactionAKID AS (
	SELECT
	BureauStatisticalCodeID,
	PremiumTransactionAKID,
	'0' AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_BureauStatisticalCode_PremiumTransactionAKID
),
UPD_BureauStatisticalCode_PremiumTransactionAKID AS (
	SELECT
	BureauStatisticalCodeID, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Set_Flag_And_Date_PremiumTransactionAKID
),
BureauStatisticalCode_PremiumTransactionAKID AS (
	MERGE INTO BureauStatisticalCode AS T
	USING UPD_BureauStatisticalCode_PremiumTransactionAKID AS S
	ON T.BureauStatisticalCodeID = S.BureauStatisticalCodeID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ModifiedDate = S.ModifiedDate
),
SQ_BureauStatisticalCode_PassThroughChargeTransactionAKID AS (
	SELECT
	MIN(BureauStatisticalCodeID) as BureauStatisticalCodeID,
	PassThroughChargeTransactionAKID as PassThroughChargeTransactionAKID 
	FROM BureauStatisticalCode
	WHERE PremiumTransactionAKID = -1
	AND CurrentSnapshotFlag=1
	GROUP BY PassThroughChargeTransactionAKID
	HAVING COUNT(*) >1
),
EXP_Set_Flag_And_Date_PassThroughChargeTransactionAKID AS (
	SELECT
	BureauStatisticalCodeID,
	PassThroughChargeTransactionAKID,
	'0' AS CurrentSnapshotFlag,
	SYSDATE AS ModifiedDate
	FROM SQ_BureauStatisticalCode_PassThroughChargeTransactionAKID
),
UPD_BureauStatisticalCode_PassThroughChargeTransactionAKID AS (
	SELECT
	BureauStatisticalCodeID, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Set_Flag_And_Date_PassThroughChargeTransactionAKID
),
BureauStatisticalCode_PassThroughChargeTransactionAKID AS (
	MERGE INTO BureauStatisticalCode AS T
	USING UPD_BureauStatisticalCode_PassThroughChargeTransactionAKID AS S
	ON T.BureauStatisticalCodeID = S.BureauStatisticalCodeID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ModifiedDate = S.ModifiedDate
),