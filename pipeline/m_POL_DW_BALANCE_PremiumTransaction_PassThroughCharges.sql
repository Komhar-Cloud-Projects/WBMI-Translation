WITH
SQ_EDW_Tables_PassThroughCharges AS (
	SELECT 
	       COUNT(*)                      AS CoverageDetailID,
	       RTRIM(STATCOV.MajorPerilCode) AS MajorPerilCode,
	       PassThru.PassThroughChargeTransactionEnteredDate AS PremiumTransactionEnteredDate ,
	       SUM(PassThroughChargeTransactionAmount) AS PremiumTransactionAmount,
	       RTRIM(POL.pol_key)                   AS pol_key
	FROM   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC,
	       @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL,
	       @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV,
	       @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage STATCOV,
	       @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction PassThru
	WHERE  LOC.PolicyAKID = POL.pol_ak_id
	       AND LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
	       AND POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
	       AND STATCOV.StatisticalCoverageAKID = PassThru.StatisticalCoverageAKID 
	       AND POL.crrnt_snpsht_flag = 1
	       AND LOC.CurrentSnapshotFlag = 1
	       AND STATCOV.CurrentSnapshotFlag = 1
	       AND POLCOV.CurrentSnapshotFlag = 1
	       AND PassThru.CurrentSnapshotFlag = 1
		AND STATCOV.MajorPerilCode IN ( '078', '088', '089', '183','255', '499', '256', '257','258', '259', '898', '899' )
	GROUP  BY RTRIM(POL.pol_key),PassThru.PassThroughChargeTransactionEnteredDate,STATCOV.MajorPerilCode
	ORDER BY RTRIM(POL.pol_key)
	
	---- '078', '088', '089', '183','255', '499', '256', '257','258', '259', '898', '899'  are major perils which are Pass Through Charges (Taxes and Surcharges).
),
EXP_Default_PassthroughCharges AS (
	SELECT
	StatisticalCoverageID,
	MajorPerilCode,
	PremiumTransactionAmount AS TotalPremiumTransactionAmount,
	pol_key,
	PremiumTransactionEnteredDate
	FROM SQ_EDW_Tables_PassThroughCharges
),
SQ_Stage_Tables_PassThroughCharges AS (
	SELECT COUNT(*)                                    AS RowCnt_Arch,
		 sar_major_peril AS sar_major_peril,
	       SUM(sar_premium)                            AS TotalPremiumAmtArch,
	       UPPER(RTRIM(pif_symbol))  + pif_policy_number + pif_module AS PolicyKey,
	       CAST(sar_entrd_date AS DATETIME)            AS extract_date
	FROM   @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514}
	WHERE  logical_flag IN ( '0', '1','2','3' )
	       AND sar_major_peril IN ( '078', '088', '089', '183','255', '499', '256', '257','258', '259', '898', '899' )
	       AND @{pipeline().parameters.WHERE_CLAUSE}
	GROUP BY UPPER(RTRIM(pif_symbol)) + pif_policy_number + pif_module,CAST(sar_entrd_date AS DATETIME), sar_major_peril
	ORDER BY UPPER(RTRIM(pif_symbol)) + pif_policy_number + pif_module
),
EXP_Stage_Default_PassThru AS (
	SELECT
	pif_4514_stage_id AS Arch_Row_Count,
	sar_major_peril,
	sar_premium AS TotalPremiumAmount,
	sar_cession_number AS PolicyKey,
	extract_date AS TransactionEnteredDate
	FROM SQ_Stage_Tables_PassThroughCharges
),
JNR_Stage_EDW_Data_PassThroughCharges AS (SELECT
	EXP_Default_PassthroughCharges.StatisticalCoverageID, 
	EXP_Default_PassthroughCharges.MajorPerilCode, 
	EXP_Default_PassthroughCharges.TotalPremiumTransactionAmount, 
	EXP_Default_PassthroughCharges.pol_key, 
	EXP_Default_PassthroughCharges.PremiumTransactionEnteredDate, 
	EXP_Stage_Default_PassThru.Arch_Row_Count, 
	EXP_Stage_Default_PassThru.sar_major_peril AS Arch_sar_major_peril, 
	EXP_Stage_Default_PassThru.TotalPremiumAmount AS Arch_TotalPremiumAmount, 
	EXP_Stage_Default_PassThru.PolicyKey AS Arch_PolicyKey, 
	EXP_Stage_Default_PassThru.TransactionEnteredDate AS Arch_TransactionEnteredDate
	FROM EXP_Default_PassthroughCharges
	RIGHT OUTER JOIN EXP_Stage_Default_PassThru
	ON EXP_Stage_Default_PassThru.PolicyKey = EXP_Default_PassthroughCharges.pol_key AND EXP_Stage_Default_PassThru.TransactionEnteredDate = EXP_Default_PassthroughCharges.PremiumTransactionEnteredDate AND EXP_Stage_Default_PassThru.sar_major_peril = EXP_Default_PassthroughCharges.MajorPerilCode
),
EXP_Evaluate_PassThroughCharges AS (
	SELECT
	StatisticalCoverageID,
	MajorPerilCode,
	TotalPremiumTransactionAmount,
	pol_key,
	PremiumTransactionEnteredDate,
	Arch_Row_Count,
	Arch_sar_major_peril,
	Arch_TotalPremiumAmount,
	Arch_PolicyKey,
	Arch_TransactionEnteredDate,
	-- *INF*: DECODE(TRUE,
	-- TotalPremiumTransactionAmount <> Arch_TotalPremiumAmount , 'AmountDoesNotMatch',
	-- StatisticalCoverageID <> Arch_Row_Count , 'RowCountDoesNotMatch',
	-- ISNULL(pol_key),'MissingEDWPolicy',
	-- StatisticalCoverageID = Arch_Row_Count  AND TotalPremiumTransactionAmount = Arch_TotalPremiumAmount,'MatchFound')
	DECODE(TRUE,
		TotalPremiumTransactionAmount <> Arch_TotalPremiumAmount, 'AmountDoesNotMatch',
		StatisticalCoverageID <> Arch_Row_Count, 'RowCountDoesNotMatch',
		pol_key IS NULL, 'MissingEDWPolicy',
		StatisticalCoverageID = Arch_Row_Count AND TotalPremiumTransactionAmount = Arch_TotalPremiumAmount, 'MatchFound') AS V_Difference_Flag,
	V_Difference_Flag AS Flag,
	'PassThroughChargesTransaction' AS EDWPolicyTransactionType,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM JNR_Stage_EDW_Data_PassThroughCharges
),
FIL_Data_PassThroughCharges AS (
	SELECT
	pol_key, 
	MajorPerilCode, 
	PremiumTransactionEnteredDate, 
	StatisticalCoverageID, 
	TotalPremiumTransactionAmount, 
	Arch_PolicyKey, 
	Arch_sar_major_peril, 
	Arch_TransactionEnteredDate, 
	Arch_Row_Count, 
	Arch_TotalPremiumAmount, 
	Flag, 
	EDWPolicyTransactionType, 
	AuditId, 
	CreatedDate, 
	ModifiedDate
	FROM EXP_Evaluate_PassThroughCharges
	WHERE IIF(IN(Flag,'AmountDoesNotMatch', 'RowCountDoesNotMatch','MissingEDWPolicy'),TRUE,FALSE)
),
WorkBalanceStageToEDWPolicyTransaction_PassthroughChargesTransaction AS (
	INSERT INTO WorkBalanceStageToEDWPolicyTransaction
	(StagePolicyKey, StageMajorPerilCode, StagePremiumTransactionEnteredDate, StageRowCount, StageTotalPremiumAmount, EDWPolicykey, EDWMajorPerilCode, EDWPremiumTransactionEnteredDate, EDWRowCount, EDWTotalPremiumAmount, EDWPolicyTransactionType, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Arch_PolicyKey AS STAGEPOLICYKEY, 
	Arch_sar_major_peril AS STAGEMAJORPERILCODE, 
	Arch_TransactionEnteredDate AS STAGEPREMIUMTRANSACTIONENTEREDDATE, 
	Arch_Row_Count AS STAGEROWCOUNT, 
	Arch_TotalPremiumAmount AS STAGETOTALPREMIUMAMOUNT, 
	pol_key AS EDWPOLICYKEY, 
	MajorPerilCode AS EDWMAJORPERILCODE, 
	PremiumTransactionEnteredDate AS EDWPREMIUMTRANSACTIONENTEREDDATE, 
	StatisticalCoverageID AS EDWROWCOUNT, 
	TotalPremiumTransactionAmount AS EDWTOTALPREMIUMAMOUNT, 
	EDWPOLICYTRANSACTIONTYPE, 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE
	FROM FIL_Data_PassThroughCharges
),
SQ_EDW_Tables_PremiumTransaction AS (
	SELECT 
	       COUNT(*)                      AS CoverageDetailID,
	       RTRIM(STATCOV.MajorPerilCode) AS MajorPerilCode,
	       PT.PremiumTransactionEnteredDate AS PremiumTransactionEnteredDate ,
	       SUM(PremiumTransactionAmount) AS PremiumTransactionAmount,
	       RTRIM(POL.pol_key)                   AS pol_key
	FROM   @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation LOC,
	       @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy POL,
	       @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage POLCOV,
	       @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage STATCOV,
	       @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.premiumtransaction PT
	WHERE  LOC.PolicyAKID = POL.pol_ak_id
	       AND LOC.RiskLocationAKID = POLCOV.RiskLocationAKID
	       AND POLCOV.PolicyCoverageAKID = STATCOV.PolicyCoverageAKID
	       AND STATCOV.StatisticalCoverageAKID = PT.StatisticalCoverageAKID
	       AND POL.crrnt_snpsht_flag = 1
	       AND LOC.CurrentSnapshotFlag = 1
	       AND STATCOV.CurrentSnapshotFlag = 1
	       AND POLCOV.CurrentSnapshotFlag = 1
	       AND PT.CurrentSnapshotFlag = 1
		AND STATCOV.MajorPerilCode NOT IN ( '078', '088', '089', '183','255', '499', '256', '257','258', '259', '898', '899' )
	      AND Pol.pol_key in (select policykey from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPolicyKeyList)
	GROUP  BY RTRIM(POL.pol_key),PT.PremiumTransactionEnteredDate,STATCOV.MajorPerilCode
	ORDER BY RTRIM(POL.pol_key)
),
EXP_Default AS (
	SELECT
	CoverageDetailID AS EDW_Row_Count,
	MajorPerilCode,
	PremiumTransactionAmount AS TotalPremiumTransactionAmount,
	pol_key,
	PremiumTransactionEnteredDate
	FROM SQ_EDW_Tables_PremiumTransaction
),
SQ_Stage_Tables_PremiumTransaction AS (
	SELECT COUNT(*)                                    AS RowCnt_Arch,
		 sar_major_peril AS sar_major_peril,
	       SUM(sar_premium)                            AS TotalPremiumAmtArch,
	       UPPER(RTRIM(pif_symbol))  + pif_policy_number + pif_module AS PolicyKey,
	       CAST(sar_entrd_date AS DATETIME)            AS extract_date
	FROM   @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514}
	WHERE  logical_flag IN ( '0', '1','2','3' )
	       AND sar_major_peril NOT IN ( '078', '088', '089', '183','255', '499', '256', '257','258', '259', '898', '899' )
	       AND @{pipeline().parameters.WHERE_CLAUSE}
	GROUP BY UPPER(RTRIM(pif_symbol)) + pif_policy_number + pif_module,CAST(sar_entrd_date AS DATETIME), sar_major_peril
	ORDER BY UPPER(RTRIM(pif_symbol)) + pif_policy_number + pif_module
),
EXP_Stage_Default AS (
	SELECT
	pif_4514_stage_id AS Arch_Row_Count,
	sar_major_peril,
	sar_premium AS TotalPremiumAmount,
	sar_cession_number AS PolicyKey,
	extract_date AS TransactionEnteredDate
	FROM SQ_Stage_Tables_PremiumTransaction
),
JNR_Stage_EDW_Data AS (SELECT
	EXP_Default.EDW_Row_Count, 
	EXP_Default.MajorPerilCode, 
	EXP_Default.TotalPremiumTransactionAmount, 
	EXP_Default.pol_key, 
	EXP_Default.PremiumTransactionEnteredDate, 
	EXP_Stage_Default.Arch_Row_Count, 
	EXP_Stage_Default.sar_major_peril AS Arch_sar_major_peril, 
	EXP_Stage_Default.TotalPremiumAmount AS Arch_TotalPremiumAmount, 
	EXP_Stage_Default.PolicyKey AS Arch_PolicyKey, 
	EXP_Stage_Default.TransactionEnteredDate AS Arch_TransactionEnteredDate
	FROM EXP_Default
	RIGHT OUTER JOIN EXP_Stage_Default
	ON EXP_Stage_Default.PolicyKey = EXP_Default.pol_key AND EXP_Stage_Default.TransactionEnteredDate = EXP_Default.PremiumTransactionEnteredDate AND EXP_Stage_Default.sar_major_peril = EXP_Default.MajorPerilCode
),
EXP_Evaluate AS (
	SELECT
	EDW_Row_Count,
	MajorPerilCode,
	TotalPremiumTransactionAmount,
	pol_key,
	PremiumTransactionEnteredDate,
	Arch_Row_Count,
	Arch_sar_major_peril,
	Arch_TotalPremiumAmount,
	Arch_PolicyKey,
	Arch_TransactionEnteredDate,
	-- *INF*: DECODE(TRUE,
	-- TotalPremiumTransactionAmount <> Arch_TotalPremiumAmount , 'AmountDoesNotMatch',
	-- EDW_Row_Count <> Arch_Row_Count , 'RowCountDoesNotMatch',
	-- ISNULL(pol_key),'MissingEDWPolicy',
	-- EDW_Row_Count = Arch_Row_Count  AND TotalPremiumTransactionAmount = Arch_TotalPremiumAmount,'MatchFound')
	DECODE(TRUE,
		TotalPremiumTransactionAmount <> Arch_TotalPremiumAmount, 'AmountDoesNotMatch',
		EDW_Row_Count <> Arch_Row_Count, 'RowCountDoesNotMatch',
		pol_key IS NULL, 'MissingEDWPolicy',
		EDW_Row_Count = Arch_Row_Count AND TotalPremiumTransactionAmount = Arch_TotalPremiumAmount, 'MatchFound') AS V_Difference_Flag,
	V_Difference_Flag AS Flag,
	'PremiumTransaction' AS EDWPolicyTransactionType,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate
	FROM JNR_Stage_EDW_Data
),
FIL_Data AS (
	SELECT
	pol_key, 
	MajorPerilCode, 
	PremiumTransactionEnteredDate, 
	EDW_Row_Count, 
	TotalPremiumTransactionAmount, 
	Arch_PolicyKey, 
	Arch_sar_major_peril, 
	Arch_TransactionEnteredDate, 
	Arch_Row_Count, 
	Arch_TotalPremiumAmount, 
	Flag, 
	EDWPolicyTransactionType, 
	AuditId, 
	CreatedDate, 
	ModifiedDate
	FROM EXP_Evaluate
	WHERE IIF(IN(Flag,'AmountDoesNotMatch', 'RowCountDoesNotMatch','MissingEDWPolicy'),TRUE,FALSE)
),
WorkBalanceStageToEDWPolicyTransaction_PremiumTransaction AS (
	INSERT INTO WorkBalanceStageToEDWPolicyTransaction
	(StagePolicyKey, StageMajorPerilCode, StagePremiumTransactionEnteredDate, StageRowCount, StageTotalPremiumAmount, EDWPolicykey, EDWMajorPerilCode, EDWPremiumTransactionEnteredDate, EDWRowCount, EDWTotalPremiumAmount, EDWPolicyTransactionType, AuditId, CreatedDate, ModifiedDate)
	SELECT 
	Arch_PolicyKey AS STAGEPOLICYKEY, 
	Arch_sar_major_peril AS STAGEMAJORPERILCODE, 
	Arch_TransactionEnteredDate AS STAGEPREMIUMTRANSACTIONENTEREDDATE, 
	Arch_Row_Count AS STAGEROWCOUNT, 
	Arch_TotalPremiumAmount AS STAGETOTALPREMIUMAMOUNT, 
	pol_key AS EDWPOLICYKEY, 
	MajorPerilCode AS EDWMAJORPERILCODE, 
	PremiumTransactionEnteredDate AS EDWPREMIUMTRANSACTIONENTEREDDATE, 
	EDW_Row_Count AS EDWROWCOUNT, 
	TotalPremiumTransactionAmount AS EDWTOTALPREMIUMAMOUNT, 
	EDWPOLICYTRANSACTIONTYPE, 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE
	FROM FIL_Data
),