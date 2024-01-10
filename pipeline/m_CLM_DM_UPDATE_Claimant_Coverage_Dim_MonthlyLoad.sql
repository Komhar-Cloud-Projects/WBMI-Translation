WITH
SQ_claimant_coverage_dim AS (
	select dim.claimant_cov_dim_id,   ccd.TypeOfLoss , ccd.ClaimTypeCategory , ccd.ClaimTypeGroup, ccd.SubrogationEligibleIndicator
	from 
		claimant_coverage_dim dim inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail ccd  on  ccd.claimant_cov_det_id = dim.edw_claimant_cov_det_pk_id
		where @{pipeline().parameters.WHERE_CLAUSE}
	      --   and ( ccd.TypeOfLoss is null OR  ccd.TypeOfLoss='N/A')
),
EXP_Default AS (
	SELECT
	claimant_cov_dim_id,
	TypeOfLoss AS i_TypeOfLoss,
	ClaimTypeCategory AS i_ClaimTypeCategory,
	ClaimTypeGroup AS i_ClaimTypeGroup,
	SubrogationEligibleIndicator AS i_SubrogationEligibleIndicator,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TypeOfLoss)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_TypeOfLoss) AS v_TypeOfLoss,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClaimTypeCategory)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClaimTypeCategory) AS v_ClaimTypeCategory,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClaimTypeGroup)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClaimTypeGroup) AS v_ClaimTypeGroup,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SubrogationEligibleIndicator)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_SubrogationEligibleIndicator) AS v_SubrogationEligibleIndicator,
	-- *INF*: IIF(ISNULL(v_TypeOfLoss)  ,'N/A',v_TypeOfLoss)
	-- 
	-- --IIF(v_TypeOfLoss = 'Unassigned'   ,'N/A',v_TypeOfLoss)
	IFF(v_TypeOfLoss IS NULL, 'N/A', v_TypeOfLoss) AS o_TypeOfLoss,
	-- *INF*: IIF(ISNULL(v_ClaimTypeCategory)  ,'N/A',v_ClaimTypeCategory)
	-- 
	-- 
	IFF(v_ClaimTypeCategory IS NULL, 'N/A', v_ClaimTypeCategory) AS o_ClaimTypeCategory,
	-- *INF*: IIF(ISNULL(v_ClaimTypeGroup)  ,'N/A',v_ClaimTypeGroup)
	-- 
	-- 
	IFF(v_ClaimTypeGroup IS NULL, 'N/A', v_ClaimTypeGroup) AS o_ClaimTypeGroup,
	-- *INF*: IIF(ISNULL(v_SubrogationEligibleIndicator)  ,'N/A',v_SubrogationEligibleIndicator)
	-- 
	-- 
	IFF(v_SubrogationEligibleIndicator IS NULL, 'N/A', v_SubrogationEligibleIndicator) AS o_SubrogationEligibleIndicator
	FROM SQ_claimant_coverage_dim
),
UPD_claimant_coverage_dim_update AS (
	SELECT
	claimant_cov_dim_id AS o_claimant_cov_dim_id, 
	o_TypeOfLoss, 
	o_ClaimTypeCategory, 
	o_ClaimTypeGroup, 
	o_SubrogationEligibleIndicator
	FROM EXP_Default
),
claimant_coverage_dim AS (
	MERGE INTO claimant_coverage_dim AS T
	USING UPD_claimant_coverage_dim_update AS S
	ON T.claimant_cov_dim_id = S.o_claimant_cov_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.TypeOfLoss = S.o_TypeOfLoss, T.ClaimTypeCategory = S.o_ClaimTypeCategory, T.ClaimTypeGroup = S.o_ClaimTypeGroup, T.SubrogationEligibleIndicator = S.o_SubrogationEligibleIndicator
),