WITH
SQ_WorkDCTPremiumTransactionTracking AS (
	SELECT nonDep.PremiumTransactionID,
		dep.DCTTransactionSequence,
		dep.DCTTransactionIndex
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPremiumTransactionTracking dep with (nolock)
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction ptDep with (nolock) ON dep.PremiumTransactionID = ptDep.PremiumTransactionID 
	@{pipeline().parameters.HISTORICALPOLICYFILTER}
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPremiumTransactionTracking nonDep with (nolock) ON dep.policykey = nonDep.policykey
		AND dep.RatingCoverageAKID = nonDep.RatingCoverageAKID
		AND dep.DCTTransactionGuid = nonDep.DCTTransactionGuid
		AND nonDep.OffsetOnsetCode <> 'Deprecated'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction ptNonDep with (nolock) ON ptNonDep.PremiumTransactionID = nondep.PremiumTransactionID
		AND ptNonDep.PremiumTransactionAmount = (ptDep.PremiumTransactionAmount * -1.0)
	@{pipeline().parameters.INCREMENTALCOVERAGEFILTER}
	WHERE dep.OffsetOnsetCode = 'Deprecated'
	and (
	COALESCE(dep.DCTTransactionSequence,-9999)!=COALESCE(nonDep.DeprecatedOnDCTTransactionSequence,-9999) 
	or 
	COALESCE(dep.DCTTransactionIndex,-9999)!=COALESCE(nonDep.DeprecatedOnDCTTransactionIndex,-9999))
),
EXP_Pass_Value AS (
	SELECT
	PremiumTransactionID_NonDeprecated,
	DCTTransactionSequence,
	DCTTransactionIndex,
	SYSDATE AS ModifiedDate
	FROM SQ_WorkDCTPremiumTransactionTracking
),
UPD_WorkDCTPremiumTransactionTracking AS (
	SELECT
	PremiumTransactionID_NonDeprecated, 
	DCTTransactionSequence, 
	DCTTransactionIndex, 
	ModifiedDate AS o_ModifiedDate
	FROM EXP_Pass_Value
),
WorkDCTPremiumTransactionTracking_Update AS (
	MERGE INTO WorkDCTPremiumTransactionTracking AS T
	USING UPD_WorkDCTPremiumTransactionTracking AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID_NonDeprecated
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.o_ModifiedDate, T.DeprecatedOnDCTTransactionSequence = S.DCTTransactionSequence, T.DeprecatedOnDCTTransactionIndex = S.DCTTransactionIndex
),