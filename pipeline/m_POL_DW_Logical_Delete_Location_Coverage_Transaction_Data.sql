WITH
SQ_Work_Policy_Transaction_Status AS (
	SELECT 
	       loc.RiskLocationID,
	       polcov.PolicyCoverageID,
	       covdet.StatisticalCoverageID,
	       PT.PremiumTransactionID,
	       -1 as PassThroughChargeTransactionID,
	       sar.BureauStatisticalCodeID,
	       work.PolicyStatus
	FROM   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Work_PolicyTransactionStatus work on pol.pol_key = work.PolicyKey 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation Loc on pol.pol_ak_id = loc.PolicyAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage polcov ON Loc.RiskLocationAKID = polcov.RiskLocationAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage covdet ON polcov.PolicyCoverageAKID = covdet.PolicyCoverageAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT ON covdet.StatisticalCoverageAKID = PT.StatisticalCoverageAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode sar on PT.PremiumTransactionAKID = sar.PremiumTransactionAKID
	WHERE  pol.crrnt_snpsht_flag = 1 and loc.CurrentSnapshotFlag  = 1  AND polcov.CurrentSnapshotFlag = 1 AND covdet.CurrentSnapshotFlag = 1
	       AND PT.CurrentSnapshotFlag = 1  AND sar.CurrentSnapshotFlag = 1 and work.PolicyStatus in (@{pipeline().parameters.POLICY_STATUS})
	AND work.AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION 
	
	SELECT 
	       loc.RiskLocationID,
	       polcov.PolicyCoverageID,
	       covdet.StatisticalCoverageID,
	       -1 as PremiumTransactionID,
	       PTPT.PassThroughChargeTransactionID,
	       sar.BureauStatisticalCodeID,
	       work.PolicyStatus
	FROM   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Work_PolicyTransactionStatus work on pol.pol_key = work.PolicyKey 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation Loc on pol.pol_ak_id = loc.PolicyAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage polcov ON Loc.RiskLocationAKID = polcov.RiskLocationAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage covdet ON polcov.PolicyCoverageAKID = covdet.PolicyCoverageAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction PTPT on covdet.StatisticalCoverageAKID = PTPT.StatisticalCoverageAKID 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode sar on PTPT.PassThroughChargeTransactionAKID = sar.PassThroughChargeTransactionAKID
	WHERE  pol.crrnt_snpsht_flag = 1 and loc.CurrentSnapshotFlag  = 1  AND polcov.CurrentSnapshotFlag = 1 AND covdet.CurrentSnapshotFlag = 1
	       AND   PTPT.CurrentSnapshotFlag = 1
	       AND work.PolicyStatus IN (@{pipeline().parameters.POLICY_STATUS})
	       AND work.AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	       @{pipeline().parameters.WHERE_CLAUSE}
	
	--the where clause variable takes care of the audit_id. All tables are joined with inner join for PremiumTransaction table and PassThroughChargeTransaction table respectively since the records are always mutually exclusive and a union is performed on both datasets to bring in all rows
),
Exp_Prep_Data AS (
	SELECT
	RiskLocationID,
	PolicyCoverageID,
	StatisticalCoverageDetailID,
	PremiumTransactionID,
	PassThroughChargeTransactionID,
	BureauStatisticalCodeID,
	PolicyStatus,
	0 AS CurrentSnapshotFlag,
	sysdate AS CurrentDate
	FROM SQ_Work_Policy_Transaction_Status
),
RTR_Delete_Data AS (
	SELECT
	RiskLocationID,
	PolicyCoverageID,
	StatisticalCoverageDetailID,
	PremiumTransactionID,
	PassThroughChargeTransactionID,
	BureauStatisticalCodeID,
	PolicyStatus,
	CurrentSnapshotFlag,
	CurrentDate
	FROM Exp_Prep_Data
),
RTR_Delete_Data_DeleteFromAllTables AS (SELECT * FROM RTR_Delete_Data WHERE PolicyStatus='RISKLOCATIONLEVELCHANGE' OR PolicyStatus='REPROCESS'),
RTR_Delete_Data_DeleteCoverageDetailAndPremiumTransaction AS (SELECT * FROM RTR_Delete_Data WHERE PolicyStatus='COVERAGEDETAILLEVELCHANGE'),
UPD_CoverageDetailLevelData AS (
	SELECT
	RiskLocationID AS RiskLocationID1, 
	PolicyCoverageID AS PolicyCoverageID1, 
	StatisticalCoverageDetailID, 
	PremiumTransactionID AS PremiumTransactionID1, 
	PassThroughChargeTransactionID, 
	BureauStatisticalCodeID AS BureauStatisticalCodeID1, 
	CurrentSnapshotFlag AS CurrentSnapshotFlag1, 
	CurrentDate AS CurrentDate3
	FROM RTR_Delete_Data_DeleteCoverageDetailAndPremiumTransaction
),
FLT_FilterPassThroughChargeTransaction AS (
	SELECT
	PassThroughChargeTransactionID, 
	CurrentSnapshotFlag1, 
	BureauStatisticalCodeID1, 
	CurrentDate3
	FROM UPD_CoverageDetailLevelData
	WHERE PassThroughChargeTransactionID<>-1
),
Agg_PassThroughChargeTransaction_UniqueID AS (
	SELECT
	PassThroughChargeTransactionID,
	CurrentSnapshotFlag1,
	BureauStatisticalCodeID1,
	CurrentDate3 AS CurrentDate1
	FROM FLT_FilterPassThroughChargeTransaction
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionID, BureauStatisticalCodeID1 ORDER BY NULL) = 1
),
BureauStatisticalCodeforPassThrough AS (
	INSERT INTO BureauStatisticalCode
	(BureauStatisticalCodeID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	BureauStatisticalCodeID1 AS BUREAUSTATISTICALCODEID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate1 AS MODIFIEDDATE
	FROM Agg_PassThroughChargeTransaction_UniqueID
),
FLT_FilterPremiumTransaction AS (
	SELECT
	PremiumTransactionID1, 
	CurrentSnapshotFlag1, 
	BureauStatisticalCodeID1, 
	CurrentDate3
	FROM UPD_CoverageDetailLevelData
	WHERE PremiumTransactionID1<>-1
),
Agg_PremiumTransaction_UniqueID AS (
	SELECT
	PremiumTransactionID1,
	CurrentSnapshotFlag1,
	BureauStatisticalCodeID1,
	CurrentDate3
	FROM FLT_FilterPremiumTransaction
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID1, BureauStatisticalCodeID1 ORDER BY NULL) = 1
),
PremiumTransaction1 AS (
	INSERT INTO PremiumTransaction
	(PremiumTransactionID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PremiumTransactionID1 AS PREMIUMTRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate3 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID
),
UPD_RiskLocationLevelData AS (
	SELECT
	RiskLocationID AS RiskLocationID1, 
	PolicyCoverageID AS PolicyCoverageID1, 
	StatisticalCoverageDetailID, 
	PremiumTransactionID AS PremiumTransactionID1, 
	PassThroughChargeTransactionID, 
	BureauStatisticalCodeID AS BureauStatisticalCodeID1, 
	CurrentSnapshotFlag AS CurrentSnapshotFlag1, 
	CurrentDate AS CurrentDate1
	FROM RTR_Delete_Data_DeleteFromAllTables
),
FLT_FilterPassThroughChargeTransaction1 AS (
	SELECT
	PassThroughChargeTransactionID, 
	CurrentSnapshotFlag1, 
	BureauStatisticalCodeID1, 
	CurrentDate1
	FROM UPD_RiskLocationLevelData
	WHERE PassThroughChargeTransactionID <> -1
),
Agg_PassThroughChargeTransaction_UniqueID1 AS (
	SELECT
	PassThroughChargeTransactionID,
	CurrentSnapshotFlag1,
	BureauStatisticalCodeID1,
	CurrentDate1
	FROM FLT_FilterPassThroughChargeTransaction1
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionID, BureauStatisticalCodeID1 ORDER BY NULL) = 1
),
BureauStatisticalCodeforPassThrough1 AS (
	INSERT INTO BureauStatisticalCode
	(BureauStatisticalCodeID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	BureauStatisticalCodeID1 AS BUREAUSTATISTICALCODEID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate1 AS MODIFIEDDATE
	FROM Agg_PassThroughChargeTransaction_UniqueID1
),
BureauStatisticalCode1 AS (
	INSERT INTO BureauStatisticalCode
	(BureauStatisticalCodeID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	BureauStatisticalCodeID1 AS BUREAUSTATISTICALCODEID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate3 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID
),
Agg_RiskLocation_UniqueID AS (
	SELECT
	RiskLocationID1,
	CurrentSnapshotFlag1,
	CurrentDate1
	FROM UPD_RiskLocationLevelData
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationID1 ORDER BY NULL) = 1
),
RiskLocation AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'RISKLOCATION', @IndexWildcard = 'Ak1RiskLocation'
	-------------------------------


	INSERT INTO RiskLocation
	(RiskLocationID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	RiskLocationID1 AS RISKLOCATIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate1 AS MODIFIEDDATE
	FROM Agg_RiskLocation_UniqueID

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'RISKLOCATION', @IndexWildcard = 'Ak1RiskLocation'
	-------------------------------


),
CoverageDetailCommercialProperty AS (
	INSERT INTO CoverageDetailCommercialProperty
	(PremiumTransactionID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PremiumTransactionID1 AS PREMIUMTRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate3 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID
),
FLT_FilterPremiumTransaction1 AS (
	SELECT
	PremiumTransactionID1, 
	CurrentSnapshotFlag1, 
	BureauStatisticalCodeID1, 
	CurrentDate1 AS CurrentDate3
	FROM UPD_RiskLocationLevelData
	WHERE PremiumTransactionID1<>-1
),
Agg_PremiumTransaction_UniqueID1 AS (
	SELECT
	PremiumTransactionID1,
	CurrentSnapshotFlag1,
	BureauStatisticalCodeID1,
	CurrentDate3 AS CurrentDate1
	FROM FLT_FilterPremiumTransaction1
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID1, BureauStatisticalCodeID1 ORDER BY NULL) = 1
),
BureauStatisticalCode AS (
	INSERT INTO BureauStatisticalCode
	(BureauStatisticalCodeID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	BureauStatisticalCodeID1 AS BUREAUSTATISTICALCODEID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate1 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID1
),
CoverageDetailInlandMarine AS (
	INSERT INTO CoverageDetailInlandMarine
	(PremiumTransactionId, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PremiumTransactionID1 AS PREMIUMTRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate3 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID
),
PremiumTransaction AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


	INSERT INTO PremiumTransaction
	(PremiumTransactionID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PremiumTransactionID1 AS PREMIUMTRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate1 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID1

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'PREMIUMTRANSACTION', @IndexWildcard = 'AK1PremiumTransaction'
	-------------------------------


),
Agg_PolicyCoverage_UniqueID AS (
	SELECT
	PolicyCoverageID1,
	CurrentSnapshotFlag1,
	CurrentDate1
	FROM UPD_RiskLocationLevelData
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageID1 ORDER BY NULL) = 1
),
PolicyCoverage AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'POLICYCOVERAGE', @IndexWildcard = 'Ak1PolicyCoverage'
	-------------------------------


	INSERT INTO PolicyCoverage
	(PolicyCoverageID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PolicyCoverageID1 AS POLICYCOVERAGEID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate1 AS MODIFIEDDATE
	FROM Agg_PolicyCoverage_UniqueID

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'POLICYCOVERAGE', @IndexWildcard = 'Ak1PolicyCoverage'
	-------------------------------


),
CoverageDetailCommercialUmbrella AS (
	INSERT INTO CoverageDetailCommercialUmbrella
	(PremiumTransactionID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PremiumTransactionID1 AS PREMIUMTRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate3 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID
),
PassThroughChargeTransaction AS (
	INSERT INTO PassThroughChargeTransaction
	(PassThroughChargeTransactionID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PASSTHROUGHCHARGETRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate1 AS MODIFIEDDATE
	FROM Agg_PassThroughChargeTransaction_UniqueID
),
CoverageDetailWorkersCompensation AS (
	INSERT INTO CoverageDetailWorkersCompensation
	(PremiumTransactionID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PremiumTransactionID1 AS PREMIUMTRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate3 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID
),
Agg_CoverageDetail_UniqueID AS (
	SELECT
	StatisticalCoverageDetailID,
	CurrentSnapshotFlag1,
	CurrentDate1
	FROM UPD_RiskLocationLevelData
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageDetailID ORDER BY NULL) = 1
),
StatisticalCoverage1 AS (
	INSERT INTO StatisticalCoverage
	(StatisticalCoverageID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	StatisticalCoverageDetailID AS STATISTICALCOVERAGEID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate1 AS MODIFIEDDATE
	FROM Agg_CoverageDetail_UniqueID
),
Agg_CoverageDetail_UniqueID1 AS (
	SELECT
	StatisticalCoverageDetailID,
	CurrentSnapshotFlag1,
	CurrentDate3
	FROM UPD_CoverageDetailLevelData
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageDetailID ORDER BY NULL) = 1
),
StatisticalCoverage AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'dbo', @TableName = 'STATISTICALCOVERAGE', @IndexWildcard = 'AK1StatisticalCoverage'
	-------------------------------


	INSERT INTO StatisticalCoverage
	(StatisticalCoverageID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	StatisticalCoverageDetailID AS STATISTICALCOVERAGEID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate3 AS MODIFIEDDATE
	FROM Agg_CoverageDetail_UniqueID1

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'dbo', @TableName = 'STATISTICALCOVERAGE', @IndexWildcard = 'AK1StatisticalCoverage'
	-------------------------------


),
PassThroughChargeTransaction1 AS (
	INSERT INTO PassThroughChargeTransaction
	(PassThroughChargeTransactionID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PASSTHROUGHCHARGETRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate1 AS MODIFIEDDATE
	FROM Agg_PassThroughChargeTransaction_UniqueID1
),
CoverageDetailCommercialAuto AS (
	INSERT INTO CoverageDetailCommercialAuto
	(PremiumTransactionID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PremiumTransactionID1 AS PREMIUMTRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate3 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID
),
CoverageDetailGeneralLiability AS (
	INSERT INTO CoverageDetailGeneralLiability
	(PremiumTransactionID, CurrentSnapshotFlag, ModifiedDate)
	SELECT 
	PremiumTransactionID1 AS PREMIUMTRANSACTIONID, 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	CurrentDate3 AS MODIFIEDDATE
	FROM Agg_PremiumTransaction_UniqueID
),