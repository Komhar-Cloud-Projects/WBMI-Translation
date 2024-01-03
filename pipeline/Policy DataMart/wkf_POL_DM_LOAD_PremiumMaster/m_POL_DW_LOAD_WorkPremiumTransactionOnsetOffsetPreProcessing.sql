WITH
SQ_WorkDCTPremiumTransactionTracking AS (

------------ PRE SQL ----------
@{pipeline().parameters.PRE_SQL}
----------------------


	SELECT trk.PolicyKey,
		trk.RatingCoverageAKID,
		trk.PremiumTransactionEffectiveDate,
		trk.DCTTransactionSequence,
		trk.DCTTransactionIndex,
		trk.DCTTransactionGuid,
		trk.OffsetOnsetCode,
		trk.PremiumTransactionID,
		trk.PremiumTransactionAKID,
		trk.DCTCoverageId,
		trk.DeprecatedOnDCTTransactionSequence,
		trk.DeprecatedOnDCTTransactionIndex
	FROM WorkDCTPremiumTransactionTracking trk with (nolock)
	INNER JOIN V2.policy P on trk.PolicyKey = P.pol_key AND  P.crrnt_snpsht_flag =1
	@{pipeline().parameters.INCREMENTALCOVERAGEFILTER}
	WHERE trk.DeletedFromPremiumTransactionFlag<>1 
	AND P.pol_ak_id%5= 0
	@{pipeline().parameters.WHERE_CLAUSE}
	ORDER BY trk.PolicyKey, trk.RatingCoverageAKID, trk.PremiumTransactionEffectiveDate, trk.DCTTransactionSequence, trk.DCTTransactionIndex, trk.DCTTransactionGuid, trk.OffsetOnsetCode
),
EXP_Pass_value AS (
	SELECT
	PolicyKey,
	RatingCoverageAKID,
	PremiumTransactionEffectiveDate,
	DCTTransactionSequence,
	DCTTransactionIndex,
	DCTTransactionGuid,
	OffsetOnsetCode,
	PremiumTransactionID,
	PremiumTransactionAKID,
	DCTCoverageId,
	DeprecatedOnDCTTransactionSequence,
	DeprecatedOnDCTTransactionIndex,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate
	FROM SQ_WorkDCTPremiumTransactionTracking
),
WorkPremiumTransactionOnsetOffsetPreProcessing AS (
	TRUNCATE TABLE WorkPremiumTransactionOnsetOffsetPreProcessing;
	INSERT INTO WorkPremiumTransactionOnsetOffsetPreProcessing
	(AuditID, CreatedDate, ModifiedDate, PolicyKey, RatingCoverageAKID, PremiumTransactionID, PremiumTransactionAKID, PremiumTransactionEffectiveDate, PremiumTransactionEnteredDate, OffsetOnsetCode, DCTTransactionSequence, DCTTransactionIndex, DCTTransactionGuid, DCTCoverageId, DeprecatedOnDCTTransactionSequence, DeprecatedOnDCTTransactionIndex)
	SELECT 
	o_AuditID AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	POLICYKEY, 
	RATINGCOVERAGEAKID, 
	PREMIUMTRANSACTIONID, 
	PREMIUMTRANSACTIONAKID, 
	PREMIUMTRANSACTIONEFFECTIVEDATE, 
	PremiumTransactionEffectiveDate AS PREMIUMTRANSACTIONENTEREDDATE, 
	OFFSETONSETCODE, 
	DCTTRANSACTIONSEQUENCE, 
	DCTTRANSACTIONINDEX, 
	DCTTRANSACTIONGUID, 
	DCTCOVERAGEID, 
	DEPRECATEDONDCTTRANSACTIONSEQUENCE, 
	DEPRECATEDONDCTTRANSACTIONINDEX
	FROM EXP_Pass_value
),