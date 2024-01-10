WITH
SQ_PremiumTransaction_WorkCompNonAudits AS (
	With PolicyList
	(Policyakid)
	AS
	(
	SELECT 
	distinct PC.PolicyAKID AS PolicyAKId 
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKId=RC.RatingCoverageAKID
	and PT.EffectiveDate=RC.EffectiveDate
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.v2.policy p
	on p.pol_ak_id = PC.PolicyAKID and p.crrnt_snpsht_flag = 1
	@{pipeline().parameters.JOIN_POLICY_LIST}
	WHERE
	PT.SourceSystemID='DCT' and 
	PT.PremiumTransactionCode in 
	('RevisedFinalAudit',
	'VoidFinalAudit',
	'FinalAudit') 
	AND RC.CoverageType = 'ManualPremium'
	AND PC.TypeBureauCode = 'WorkersCompensation'
	@{pipeline().parameters.WHERE_CLAUSE}
	)
	,
	cte_transactionlist as
	(SELECT 
	PC.PolicyAKID, 
	RC.RatingCoverageAKID,
	PT.PremiumTransactionEffectiveDate,
	PT.PremiumTransactionEnteredDate, 
	PT.PremiumTransactionID, 
	PT.Exposure, 
	PT.PremiumTransactionCode,
	PT.OffsetOnsetCode,
	PT.WrittenExposure
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKId=RC.RatingCoverageAKID
	and PT.EffectiveDate=RC.EffectiveDate and RC.CoverageType = 'ManualPremium'
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	inner join PolicyList PL
	on PC.PolicyAKID = PL.PolicyAKID
	)
	SELECT 
	nonaudittran.PolicyAKID, 
	nonaudittran.RatingCoverageAKID,
	nonaudittran.PremiumTransactionID, 
	nonaudittran.WrittenExposure,
	audittran.PremiumTransactionID as ParentAuditTransactionId
	from cte_transactionlist audittran
	left join cte_transactionlist nonaudittran
	on
	audittran.PolicyAKID = nonaudittran.PolicyAKID and
	audittran.RatingCoverageAKID = nonaudittran.RatingCoverageAKID and
	nonaudittran.PremiumTransactionEffectiveDate <= audittran.PremiumTransactionEffectiveDate and
	nonaudittran.PremiumTransactionEnteredDate < audittran.PremiumTransactionEnteredDate
	where audittran.PremiumTransactionCode in ('RevisedFinalAudit',
	'FinalAudit')
	and nonaudittran.PremiumTransactionCode not in ('RevisedFinalAudit',
	'VoidFinalAudit',
	'FinalAudit')
),
SRT_Non_Audits AS (
	SELECT
	PolicyAKID, 
	RatingCoverageAKId, 
	ParentAuditTransactionID, 
	PremiumTransactionID, 
	WrittenExposure
	FROM SQ_PremiumTransaction_WorkCompNonAudits
	ORDER BY PolicyAKID ASC, RatingCoverageAKId ASC, ParentAuditTransactionID ASC, PremiumTransactionID ASC
),
AGG_Non_Audit_Transaction_WrittenExposure AS (
	SELECT
	PolicyAKID,
	RatingCoverageAKId,
	ParentAuditTransactionID,
	PremiumTransactionID,
	WrittenExposure,
	-- *INF*: SUM(WrittenExposure)
	SUM(WrittenExposure) AS OutWrittenExposure
	FROM SRT_Non_Audits
	GROUP BY PolicyAKID, RatingCoverageAKId, ParentAuditTransactionID
),
SRT_CoverageExposures AS (
	SELECT
	PolicyAKID, 
	RatingCoverageAKId, 
	ParentAuditTransactionID, 
	OutWrittenExposure
	FROM AGG_Non_Audit_Transaction_WrittenExposure
	ORDER BY PolicyAKID ASC, RatingCoverageAKId ASC, ParentAuditTransactionID ASC
),
SQ_PremiumTransaction_WorkCompAudits AS (
	SELECT 
	PC.PolicyAKID, 
	RC.RatingCoverageAKID,
	PT.PremiumTransactionEffectiveDate,
	PT.PremiumTransactionEnteredDate, 
	PT.PremiumTransactionID, 
	PT.Exposure, 
	PT.PremiumTransactionCode,
	PT.OffsetOnsetCode,
	PT.WrittenExposure
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKId=RC.RatingCoverageAKID
	and PT.EffectiveDate=RC.EffectiveDate
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.v2.policy p
	on p.pol_ak_id = PC.PolicyAKID and p.crrnt_snpsht_flag = 1
	@{pipeline().parameters.JOIN_POLICY_LIST}
	WHERE
	PT.SourceSystemID='DCT' and 
	PT.PremiumTransactionCode in 
	('RevisedFinalAudit',
	'VoidFinalAudit',
	'FinalAudit') 
	AND RC.CoverageType = 'ManualPremium'
	AND PC.TypeBureauCode = 'WorkersCompensation'
	@{pipeline().parameters.WHERE_CLAUSE}
	order by
	PC.PolicyAKID,
	PT.RatingCoverageAKID,
	PT.PremiumTransactionId
),
EXP_Collect AS (
	SELECT
	PolicyAKID,
	RatingCoverageAKId,
	PremiumTransactionEffectiveDate,
	PremiumTransactionEnteredDate,
	PremiumTransactionID,
	Exposure,
	PremiumTransactionCode,
	OffsetOnsetCode,
	WrittenExposure,
	-- *INF*: DECODE(TRUE,
	-- INSTR(PremiumTransactionCode,'Audit')>0,0,
	-- WrittenExposure)
	DECODE(TRUE,
		INSTR(PremiumTransactionCode, 'Audit') > 0, 0,
		WrittenExposure) AS BaseWrittenExposure
	FROM SQ_PremiumTransaction_WorkCompAudits
),
SRT_Transactions AS (
	SELECT
	PolicyAKID, 
	RatingCoverageAKId, 
	PremiumTransactionID, 
	Exposure, 
	PremiumTransactionCode, 
	OffsetOnsetCode, 
	WrittenExposure
	FROM EXP_Collect
	ORDER BY PolicyAKID ASC, RatingCoverageAKId ASC, PremiumTransactionID ASC
),
JNR_Transaction_NetWrittenExposure_Coverage AS (SELECT
	SRT_Transactions.PolicyAKID, 
	SRT_Transactions.RatingCoverageAKId, 
	SRT_Transactions.PremiumTransactionID, 
	SRT_Transactions.Exposure, 
	SRT_Transactions.PremiumTransactionCode, 
	SRT_Transactions.OffsetOnsetCode, 
	SRT_Transactions.WrittenExposure, 
	SRT_CoverageExposures.PolicyAKID AS in_PolicyAKID, 
	SRT_CoverageExposures.RatingCoverageAKId AS in_RatingCoverageAKId, 
	SRT_CoverageExposures.ParentAuditTransactionID AS in_ParentAuditTransactionID, 
	SRT_CoverageExposures.OutWrittenExposure AS NetNonAuditWrittenExposure
	FROM SRT_CoverageExposures
	RIGHT OUTER JOIN SRT_Transactions
	ON SRT_Transactions.PolicyAKID = SRT_CoverageExposures.PolicyAKID AND SRT_Transactions.RatingCoverageAKId = SRT_CoverageExposures.RatingCoverageAKId AND SRT_Transactions.PremiumTransactionID = SRT_CoverageExposures.ParentAuditTransactionID
),
EXP_EVAL_WrittenExposure AS (
	SELECT
	PolicyAKID,
	RatingCoverageAKId,
	PremiumTransactionID,
	Exposure,
	PremiumTransactionCode,
	OffsetOnsetCode,
	WrittenExposure,
	NetNonAuditWrittenExposure AS i_NonAuditNetWrittenExposure,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_NonAuditNetWrittenExposure),0,
	-- i_NonAuditNetWrittenExposure)
	-- -- Cleansing nulls from Master Joins of Void audit Transactions as well as incoming audit transactions with ratingcoverageakids that appear to have no corresponding non audit transactions
	DECODE(TRUE,
		i_NonAuditNetWrittenExposure IS NULL, 0,
		i_NonAuditNetWrittenExposure) AS v_NonAuditNetWrittenExposure,
	-- *INF*: DECODE(TRUE,
	-- OffsetOnsetCode = 'Deprecated',-1,
	-- 1)
	DECODE(TRUE,
		OffsetOnsetCode = 'Deprecated', - 1,
		1) AS v_Multiplier,
	-- *INF*: Exposure - v_NonAuditNetWrittenExposure
	-- -- True up exposure in magnitude is equal to the mathematical difference between incoming InForce Exposure on Audit Transaction less the cumulative computed written exposure from the non audit transaction effective and entered prior to the audit transaction
	Exposure - v_NonAuditNetWrittenExposure AS v_TrueUpExposureValue,
	-- *INF*: DECODE(TRUE,
	-- PremiumTransactionCode = 'VoidFinalAudit',0,
	-- ROUND(v_TrueUpExposureValue * v_Multiplier,4)
	-- )
	-- --we zero out the value of writtenexposure for VoidFinalAudits else we compute the mathematical value with sign correct to four decimal places. Note that deprecated audit transactions have the multipler of -1 effectively nullify what was added by the original audit transaction
	DECODE(TRUE,
		PremiumTransactionCode = 'VoidFinalAudit', 0,
		ROUND(v_TrueUpExposureValue * v_Multiplier, 4)) AS v_WrittenExposureTruedUp,
	v_WrittenExposureTruedUp AS o_WrittenExposure,
	-- *INF*: DECODE(TRUE,
	-- INSTR(PremiumTransactionCode,'Audit') > 0  AND v_WrittenExposureTruedUp  !=  WrittenExposure,1,
	-- 0)
	-- -- We only permit Audit type transactions where the recalculated Audit true up exposure is different than the incoming WrittenExposure on the record
	DECODE(TRUE,
		INSTR(PremiumTransactionCode, 'Audit') > 0 AND v_WrittenExposureTruedUp != WrittenExposure, 1,
		0) AS FilterFlag
	FROM JNR_Transaction_NetWrittenExposure_Coverage
),
FIL_Unchanged_WrittenExposure AS (
	SELECT
	PremiumTransactionID, 
	FilterFlag, 
	o_WrittenExposure
	FROM EXP_EVAL_WrittenExposure
	WHERE FilterFlag = 1
),
UPD_PremiumTransaction_WrittenExposure_changes AS (
	SELECT
	PremiumTransactionID, 
	o_WrittenExposure
	FROM FIL_Unchanged_WrittenExposure
),
PremiumTransaction_Update_Audit_TrueUp AS (
	MERGE INTO PremiumTransaction AS T
	USING UPD_PremiumTransaction_WrittenExposure_changes AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.WrittenExposure = S.o_WrittenExposure
),