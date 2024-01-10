WITH
SQ_IDO_Data AS (
	Select distinct WBE.PolicyNumber,
	WBE.PolicyVersion,
	DT.HistoryID,
	DT.Type,
	ISNULL(DT.TransactionDate,DT.CreatedDate) TransactionDate, 
	WBE.Purpose,
	WBE.SessionId,
	DC.CoverageId,
	DC.ID CoverageGuid,
	DTR.[Index],
	DTR.[Sequence]
	From @{pipeline().parameters.WBEXAMPLEDATA_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions WBE
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
	on WBE.SessionId=DT.SessionID
	and WBE.HistoryID=DT.HistoryID
	and WBE.ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage DC
	on DT.SessionId=DC.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_TransactionReportEntry DTR
	on DT.Sessionid=DTR.Sessionid
	where WBE.Autoshred='1'
	and WBE.Indicator='1'
	and DT.Type @{pipeline().parameters.EXCLUDE_TTYPE}
),
EXP_IDO_Data AS (
	SELECT
	PolicyNumber,
	PolicyVersion,
	PolicyNumber || PolicyVersion AS Pol_key,
	HistoryID,
	Type,
	TransactionDate,
	Purpose,
	SessionId,
	CoverageId,
	Id,
	Index,
	Sequence
	FROM SQ_IDO_Data
),
LKP_RatingCoverageAKID AS (
	SELECT
	RatingCoverageAKID,
	IN_pol_key,
	IN_CoverageGUID,
	IN_TransactionDate,
	Pol_key,
	CoverageGUID,
	EffectiveDate
	FROM (
		select distinct RC.RatingCoverageAKID as RatingCoverageAKID,
		P.Pol_key as Pol_key,
		RC.CoverageGUID as CoverageGUID,
		RC.EffectiveDate as EffectiveDate
		from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		inner join @{pipeline().parameters.STAGE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBEDWIncrementalDataQualitySessions W
		on P.pol_key=W.PolicyNumber+W.PolicyVersion
		and P.source_sys_id='DCT'
		and P.crrnt_snpsht_flag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on P.pol_ak_id=cast(substring(RC.RatingCoverageKey,1,charindex('~',RC.RatingCoverageKey,1)-1) as bigint)
		and RC.PolicyCoverageAKID<>-1
		where W.Autoshred='1'
		and W.Indicator='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Pol_key,CoverageGUID,EffectiveDate ORDER BY RatingCoverageAKID) = 1
),
EXP_IDO_PTHashKey AS (
	SELECT
	EXP_IDO_Data.Pol_key,
	EXP_IDO_Data.HistoryID,
	EXP_IDO_Data.Type,
	EXP_IDO_Data.TransactionDate,
	EXP_IDO_Data.Purpose,
	EXP_IDO_Data.SessionId,
	EXP_IDO_Data.CoverageId,
	EXP_IDO_Data.Id,
	EXP_IDO_Data.Index,
	EXP_IDO_Data.Sequence,
	LKP_RatingCoverageAKID.RatingCoverageAKID,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'Onset' || 'Onset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'Onset' || 'Onset') AS Onset_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'Offset' || 'Onset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'Offset' || 'Onset') AS Offset_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'N/A' || 'Onset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'N/A' || 'Onset') AS NA_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'Onset' || 'Offset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'Onset' || 'Offset') AS DepOnset_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'Offset' || 'Offset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'Offset' || 'Offset') AS DepOffset_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'N/A' || 'Offset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'N/A' || 'Offset') AS DepNA_HashKey
	FROM EXP_IDO_Data
	LEFT JOIN LKP_RatingCoverageAKID
	ON LKP_RatingCoverageAKID.Pol_key = EXP_IDO_Data.Pol_key AND LKP_RatingCoverageAKID.CoverageGUID = EXP_IDO_Data.Id AND LKP_RatingCoverageAKID.EffectiveDate = EXP_IDO_Data.TransactionDate
),
SQ_PremiumTransaction AS (
	SELECT Distinct D.PremiumTransactionID, D.PremiumTransactionHashKey, D.PremiumTransactionAKID 
	FROM @{pipeline().parameters.STAGE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCCoverageStaging A
	inner join (
	select B.Historyid,C.Purpose,max(B.Sessionid) Sessionid from @{pipeline().parameters.STAGE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBEDWIncrementalDataQualitySessions A
	inner Join @{pipeline().parameters.STAGE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCTransactionStaging B
	on A.HistoryID=B.HistoryID
	inner join @{pipeline().parameters.STAGE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchDCSessionStaging C
	on B.SessionId=C.SessionId
	where A.Autoshred='1'
	and A.Indicator='1'
	group by B.Historyid,C.Purpose) B
	on A.SessionId=B.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchWorkPremiumTransaction C
	on A.CoverageId=C.PremiumTransactionStageId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction D
	on C.PremiumTransactionAKId=D.PremiumTransactionAKID
),
JNR_DEP_NA AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.DepNA_HashKey, 
	EXP_IDO_PTHashKey.Index, 
	EXP_IDO_PTHashKey.Sequence, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.DepNA_HashKey
),
JNR_DEP_OFFSET AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.DepOffset_HashKey, 
	EXP_IDO_PTHashKey.Index, 
	EXP_IDO_PTHashKey.Sequence, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.DepOffset_HashKey
),
JNR_DEP_ONSET AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.DepOnset_HashKey, 
	EXP_IDO_PTHashKey.Index, 
	EXP_IDO_PTHashKey.Sequence, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.DepOnset_HashKey
),
JNR_NA AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.NA_HashKey, 
	EXP_IDO_PTHashKey.Index, 
	EXP_IDO_PTHashKey.Sequence, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.NA_HashKey
),
JNR_OFFSET AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.Offset_HashKey, 
	EXP_IDO_PTHashKey.Index, 
	EXP_IDO_PTHashKey.Sequence, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.Offset_HashKey
),
JNR_ONSET AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.Onset_HashKey, 
	EXP_IDO_PTHashKey.Index, 
	EXP_IDO_PTHashKey.Sequence, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.Onset_HashKey
),
Union_ALL AS (
	SELECT CoverageId, Index, Sequence, PremiumTransactionID, PremiumTransactionAKID
	FROM JNR_ONSET
	UNION
	SELECT CoverageId, Index, Sequence, PremiumTransactionID, PremiumTransactionAKID
	FROM JNR_OFFSET
	UNION
	SELECT CoverageId, Index, Sequence, PremiumTransactionID, PremiumTransactionAKID
	FROM JNR_NA
	UNION
	SELECT CoverageId, Index, Sequence, PremiumTransactionID, PremiumTransactionAKID
	FROM JNR_DEP_ONSET
	UNION
	SELECT CoverageId, Index, Sequence, PremiumTransactionID, PremiumTransactionAKID
	FROM JNR_DEP_OFFSET
	UNION
	SELECT CoverageId, Index, Sequence, PremiumTransactionID, PremiumTransactionAKID
	FROM JNR_DEP_NA
),
UPD_Tracking AS (
	SELECT
	PremiumTransactionID, 
	Index, 
	Sequence
	FROM Union_ALL
),
WorkDCTPremiumTransactionTracking AS (
	MERGE INTO WorkDCTPremiumTransactionTracking AS T
	USING UPD_Tracking AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.DCTTransactionSequence = S.Sequence, T.DCTTransactionIndex = S.Index
),
EXP_TGT_DataCollect AS (
	SELECT
	-999 AS AuditID,
	'DCT' AS SourceSystemID,
	CURRENT_TIMESTAMP AS CreatedDate,
	PremiumTransactionAKID AS PremiumTransactionAKId,
	CoverageId AS PremiumTransactionStageId,
	Index,
	Sequence
	FROM Union_ALL
),
ArchWorkPremiumTransaction AS (
	INSERT INTO ArchWorkPremiumTransaction
	(WorkPremiumTransactionId, AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, PremiumTransactionStageId)
	SELECT 
	AuditID AS WORKPREMIUMTRANSACTIONID, 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	PREMIUMTRANSACTIONAKID, 
	PREMIUMTRANSACTIONSTAGEID
	FROM EXP_TGT_DataCollect
),