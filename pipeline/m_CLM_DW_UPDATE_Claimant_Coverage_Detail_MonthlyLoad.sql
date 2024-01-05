WITH
SQ_claimant_coverage_detail1 AS (
	select ccd.claimant_cov_det_id ,  
	iseg.InsuranceSegmentCode  ,   ccd.major_peril_code, ccd.cause_of_loss   
	from claimant_coverage_detail CCD 
	inner join claim_party_occurrence CPO 
	on CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id and CPO.crrnt_snpsht_flag=1 
	inner join claim_occurrence CO on CO.claim_occurrence_ak_id=CPO.claim_occurrence_ak_id and CPO.crrnt_snpsht_flag=1 and CO.crrnt_snpsht_flag=1 
	inner join V2.policy P on P.pol_ak_id=CO.pol_key_ak_id and P.crrnt_snpsht_flag=1 and CO.crrnt_snpsht_flag=1 
	inner join InsuranceSegment ISeg on ISeg.InsuranceSegmentAKId=P.InsuranceSegmentAKId 
	where    
	(CCD.TypeOfLoss = 'N/A' OR CCD.ClaimTypeCategory = 'N/A' OR CCD.ClaimTypeGroup = 'N/A' OR CCD.SubrogationEligibleIndicator = 'N/A' )
),
LKP_SupTypeOfLossRules AS (
	SELECT
	TypeOfLoss,
	ClaimTypeCategory,
	ClaimTypeGroup,
	SubrogationEligibleIndicator,
	MajorPerilCode,
	CauseOfLoss,
	InsuranceSegmentCode
	FROM (
		SELECT 
			TypeOfLoss,
			ClaimTypeCategory,
			ClaimTypeGroup,
			SubrogationEligibleIndicator,
			MajorPerilCode,
			CauseOfLoss,
			InsuranceSegmentCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupTypeOfLossRules
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MajorPerilCode,CauseOfLoss,InsuranceSegmentCode ORDER BY TypeOfLoss DESC) = 1
),
EXP_Default AS (
	SELECT
	SQ_claimant_coverage_detail1.claimant_cov_det_id,
	LKP_SupTypeOfLossRules.TypeOfLoss AS i_TypeOfLoss,
	LKP_SupTypeOfLossRules.ClaimTypeCategory AS i_ClaimTypeCategory,
	LKP_SupTypeOfLossRules.ClaimTypeGroup AS i_ClaimTypeGroup,
	LKP_SupTypeOfLossRules.SubrogationEligibleIndicator AS i_SubrogationEligibleIndicator,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TypeOfLoss)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_TypeOfLoss) AS v_TypeOfLoss,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClaimTypeCategory)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClaimTypeCategory) AS v_ClaimTypeCategory,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClaimTypeGroup)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClaimTypeGroup) AS v_ClaimTypeGroup,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SubrogationEligibleIndicator)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_SubrogationEligibleIndicator) AS v_SubrogationEligibleIndicator,
	-- *INF*: IIF(ISNULL(v_TypeOfLoss) ,'N/A',v_TypeOfLoss)
	-- --IIF(v_TypeOfLoss = 'Unassigned'   ,'N/A',v_TypeOfLoss)
	IFF(v_TypeOfLoss IS NULL, 'N/A', v_TypeOfLoss) AS o_TypeOfLoss,
	-- *INF*: IIF(ISNULL(v_ClaimTypeCategory) ,'N/A',v_ClaimTypeCategory)
	-- 
	IFF(v_ClaimTypeCategory IS NULL, 'N/A', v_ClaimTypeCategory) AS o_ClaimTypeCategory,
	-- *INF*: IIF(ISNULL(v_ClaimTypeGroup) ,'N/A',v_ClaimTypeGroup)
	-- 
	IFF(v_ClaimTypeGroup IS NULL, 'N/A', v_ClaimTypeGroup) AS o_ClaimTypeGroup,
	-- *INF*: IIF(ISNULL(v_SubrogationEligibleIndicator) ,'N/A',v_SubrogationEligibleIndicator)
	-- 
	IFF(v_SubrogationEligibleIndicator IS NULL, 'N/A', v_SubrogationEligibleIndicator) AS o_SubrogationEligibleIndicator
	FROM SQ_claimant_coverage_detail1
	LEFT JOIN LKP_SupTypeOfLossRules
	ON LKP_SupTypeOfLossRules.MajorPerilCode = SQ_claimant_coverage_detail1.major_peril_code AND LKP_SupTypeOfLossRules.CauseOfLoss = SQ_claimant_coverage_detail1.cause_of_loss AND LKP_SupTypeOfLossRules.InsuranceSegmentCode = SQ_claimant_coverage_detail1.InsuranceSegmentCode
),
UPD_UpdateTarget AS (
	SELECT
	claimant_cov_det_id, 
	o_TypeOfLoss AS TypeOfLoss, 
	o_ClaimTypeCategory AS ClaimTypeCategory, 
	o_ClaimTypeGroup AS ClaimTypeGroup, 
	o_SubrogationEligibleIndicator AS SubrogationEligibleIndicator
	FROM EXP_Default
),
TGT_claimant_coverage_detail AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail AS T
	USING UPD_UpdateTarget AS S
	ON T.claimant_cov_det_id = S.claimant_cov_det_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.TypeOfLoss = S.TypeOfLoss, T.ClaimTypeCategory = S.ClaimTypeCategory, T.ClaimTypeGroup = S.ClaimTypeGroup, T.SubrogationEligibleIndicator = S.SubrogationEligibleIndicator
),