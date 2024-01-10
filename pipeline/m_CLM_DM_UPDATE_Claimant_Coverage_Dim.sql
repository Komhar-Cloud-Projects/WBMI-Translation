WITH
SQ_claimant_coverage_dim AS (
	SELECT 
	CLAIMANT_COVERAGE_DIM.CLAIMANT_COV_DIM_ID, 
	CLAIMANT_COVERAGE_DIM.EFF_FROM_DATE, 
	CLAIMANT_COVERAGE_DIM.EFF_TO_DATE, 
	CLAIMANT_COVERAGE_DIM.EDW_CLAIMANT_COV_DET_AK_ID 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIMANT_COVERAGE_DIM CLAIMANT_COVERAGE_DIM
	WHERE 
	EFF_FROM_DATE <> EFF_TO_DATE AND  CLAIMANT_COVERAGE_DIM.EDW_CLAIMANT_COV_DET_AK_ID 
	IN
	(
	SELECT CLAIMANT_COVERAGE_DIM2.EDW_CLAIMANT_COV_DET_AK_ID FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIMANT_COVERAGE_DIM CLAIMANT_COVERAGE_DIM2
	WHERE CRRNT_SNPSHT_FLAG = 1 
	GROUP BY CLAIMANT_COVERAGE_DIM2.EDW_CLAIMANT_COV_DET_AK_ID HAVING COUNT(*) > 1
	)
	ORDER BY CLAIMANT_COVERAGE_DIM.EDW_CLAIMANT_COV_DET_AK_ID, CLAIMANT_COVERAGE_DIM.EFF_FROM_DATE DESC
),
EXP_Lag_eff_from_date1 AS (
	SELECT
	claimant_cov_dim_id,
	edw_claimant_cov_det_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claimant_cov_det_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		edw_claimant_cov_det_ak_id = v_PREV_ROW_occurrence_key, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claimant_cov_det_ak_id AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claimant_coverage_dim
),
FILTRANS AS (
	SELECT
	claimant_cov_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date1
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_EFF_TO_DATE AS (
	SELECT
	claimant_cov_dim_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FILTRANS
),
claimant_coverage_dim_EXPIRE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_dim AS T
	USING UPD_EFF_TO_DATE AS S
	ON T.claimant_cov_dim_id = S.claimant_cov_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),