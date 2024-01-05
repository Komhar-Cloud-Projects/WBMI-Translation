WITH
SQ_IL AS (
	SELECT pt.PremiumTransactionID,
	 pt.AuditID,
	 pt.PremiumTransactionAKID,
	 pt.PremiumTransactionEffectiveDate, 
	 pt.OffsetOnsetCode, 
	 pt.RatingCoverageAKId,
	 p.pol_key, 
	 wpt.PremiumTransactionStageId 
	FROM
	 PremiumTransaction pt
	 inner join RatingCoverage rc on
	 pt.RatingCoverageAKId = rc.RatingCoverageAKID and pt.EffectiveDate = rc.EffectiveDate
	 inner join PolicyCoverage pc on
	 rc.PolicyCoverageAKID = pc.PolicyCoverageAKID and pc.CurrentSnapshotFlag = 1
	 inner join RiskLocation rl on
	 pc.RiskLocationAKID = rl.RiskLocationAKID and rl.CurrentSnapshotFlag = 1
	 inner join v2.policy p on
	 rl.PolicyAKID = p.pol_ak_id and p.crrnt_snpsht_flag = 1
	 inner join WorkPremiumTransaction wpt on
	 pt.PremiumTransactionAKID = wpt.PremiumTransactionAKId
	 left join WorkDCTPremiumTransactionTracking wdctpt on 
	 wdctpt.PremiumTransactionID=pt.PremiumTransactionID
	where
	pt.SourceSystemID = 'DCT' and 
	pt.ReasonAmendedCode not in ('CWO','Claw Back') and 
	pt.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and
	wdctpt.PremiumTransactionID is null
	@{pipeline().parameters.WHERE_CLAUSE_IL}
),
EXP_IL AS (
	SELECT
	PremiumTransactionID,
	AuditID,
	PremiumTransactionAKID,
	PremiumTransactionEffectiveDate,
	OffsetOnsetCode,
	RatingCoverageAKId,
	pol_key,
	PremiumTransactionStageId
	FROM SQ_IL
),
SQ_Stage AS (
	SELECT re.[Index],
	 re.[Sequence], 
	 t.Id,
	c.CoverageId
	FROM
	DCTransactionStaging t
	 inner join DCCoverageStaging c on
	 c.SessionId = t.SessionId
	 left join DCTransactionReportEntryStaging re on
	 c.SessionId = re.SessionId
),
EXP_Stage AS (
	SELECT
	Index,
	Sequence,
	Id,
	CoverageId
	FROM SQ_Stage
),
JNR_IL_Stage AS (SELECT
	EXP_IL.PremiumTransactionID, 
	EXP_IL.AuditID, 
	EXP_IL.PremiumTransactionAKID, 
	EXP_IL.PremiumTransactionEffectiveDate, 
	EXP_IL.OffsetOnsetCode, 
	EXP_IL.RatingCoverageAKId, 
	EXP_IL.pol_key, 
	EXP_IL.PremiumTransactionStageId, 
	EXP_Stage.Index, 
	EXP_Stage.Sequence, 
	EXP_Stage.Id, 
	EXP_Stage.CoverageId
	FROM EXP_IL
	INNER JOIN EXP_Stage
	ON EXP_Stage.CoverageId = EXP_IL.PremiumTransactionStageId
),
AGGTRANS AS (
	SELECT
	PremiumTransactionID, 
	AuditID, 
	PremiumTransactionAKID, 
	PremiumTransactionEffectiveDate, 
	OffsetOnsetCode, 
	RatingCoverageAKId, 
	pol_key, 
	PremiumTransactionStageId, 
	Index, 
	Sequence, 
	Id, 
	CoverageId, 
	count(pol_key) AS count_pol_key, 
	count(RatingCoverageAKId) AS count_RatingCoverageAKId
	FROM JNR_IL_Stage
	GROUP BY PremiumTransactionID
),
FILTRANS AS (
	SELECT
	PremiumTransactionID, 
	AuditID, 
	PremiumTransactionAKID, 
	PremiumTransactionEffectiveDate, 
	OffsetOnsetCode, 
	RatingCoverageAKId, 
	pol_key, 
	PremiumTransactionStageId, 
	Index, 
	Sequence, 
	Id, 
	CoverageId, 
	count_pol_key, 
	count_RatingCoverageAKId
	FROM AGGTRANS
	WHERE count_pol_key=1 and count_RatingCoverageAKId=1
),
EXP_PassValue AS (
	SELECT
	PremiumTransactionID,
	AuditID,
	PremiumTransactionAKID,
	PremiumTransactionEffectiveDate,
	OffsetOnsetCode,
	RatingCoverageAKId,
	pol_key,
	PremiumTransactionStageId,
	Index,
	Sequence,
	Id,
	CoverageId,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	0 AS DeletedFromPremiumTransactionFlag
	FROM FILTRANS
),
WorkDCTPremiumTransactionTracking AS (
	INSERT INTO WorkDCTPremiumTransactionTracking
	(PremiumTransactionID, PremiumTransactionAKID, AuditId, CreatedDate, ModifiedDate, PolicyKey, RatingCoverageAKID, PremiumTransactionEffectiveDate, DCTTransactionGuid, DCTTransactionSequence, DCTTransactionIndex, OffsetOnsetCode, DCTCoverageId, DeletedFromPremiumTransactionFlag)
	SELECT 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	AuditID AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	pol_key AS POLICYKEY, 
	RatingCoverageAKId AS RATINGCOVERAGEAKID, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	Id AS DCTTRANSACTIONGUID, 
	Sequence AS DCTTRANSACTIONSEQUENCE, 
	Index AS DCTTRANSACTIONINDEX, 
	OFFSETONSETCODE, 
	CoverageId AS DCTCOVERAGEID, 
	DELETEDFROMPREMIUMTRANSACTIONFLAG
	FROM EXP_PassValue
),