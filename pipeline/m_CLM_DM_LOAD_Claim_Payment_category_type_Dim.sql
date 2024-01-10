WITH
SQ_claim_payment_category AS (
	SELECT DISTINCT claim_payment_category.claim_pay_ctgry_type , 
	claim_payment_category.claim_pay_ctgry_lump_sum_ind,
	claim_payment_category.cov_ctgry_code
	FROM
	 claim_payment_category
),
EXP_get_values AS (
	SELECT
	claim_pay_ctgry_type,
	claim_pay_ctgry_lump_sum_ind,
	cov_ctgry_code
	FROM SQ_claim_payment_category
),
LKP_sup_claim_benefit_type AS (
	SELECT
	benefit_type_code_descript,
	benefit_type_code
	FROM (
		SELECT LTRIM(RTRIM(sup_claim_benefit_type.benefit_type_code_descript)) as benefit_type_code_descript, LTRIM(RTRIM(sup_claim_benefit_type.benefit_type_code)) as benefit_type_code FROM sup_claim_benefit_type
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY benefit_type_code ORDER BY benefit_type_code_descript) = 1
),
LKP_sup_coverage_category AS (
	SELECT
	cov_ctgry_descript,
	cov_ctgry_code
	FROM (
		SELECT 
		LTRIM(RTRIM(sup_coverage_category.cov_ctgry_code )) as cov_ctgry_code,
		LTRIM(RTRIM(sup_coverage_category.cov_ctgry_descript)) as cov_ctgry_descript   
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_coverage_category
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cov_ctgry_code ORDER BY cov_ctgry_descript DESC) = 1
),
EXP_set_values AS (
	SELECT
	EXP_get_values.claim_pay_ctgry_type,
	LKP_sup_claim_benefit_type.benefit_type_code_descript AS IN_benefit_type_code_descript,
	-- *INF*: IIF(ISNULL(IN_benefit_type_code_descript), 'N/A', IN_benefit_type_code_descript)
	IFF(IN_benefit_type_code_descript IS NULL,
		'N/A',
		IN_benefit_type_code_descript
	) AS claim_pay_ctgry_type_descript,
	EXP_get_values.claim_pay_ctgry_lump_sum_ind AS IN_claim_pay_ctgry_lump_sum_ind,
	-- *INF*: IIF(ISNULL(IN_claim_pay_ctgry_lump_sum_ind), 'N/A', IN_claim_pay_ctgry_lump_sum_ind)
	IFF(IN_claim_pay_ctgry_lump_sum_ind IS NULL,
		'N/A',
		IN_claim_pay_ctgry_lump_sum_ind
	) AS claim_pay_ctgry_lump_sum_ind,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	EXP_get_values.cov_ctgry_code AS IN_cov_ctgry_code,
	-- *INF*: IIF(ISNULL(IN_cov_ctgry_code), 'N/A', IN_cov_ctgry_code)
	IFF(IN_cov_ctgry_code IS NULL,
		'N/A',
		IN_cov_ctgry_code
	) AS cov_ctgry_code,
	LKP_sup_coverage_category.cov_ctgry_descript AS IN_cov_ctgry_descript,
	-- *INF*: IIF(ISNULL(IN_cov_ctgry_descript), 'N/A', IN_cov_ctgry_descript)
	IFF(IN_cov_ctgry_descript IS NULL,
		'N/A',
		IN_cov_ctgry_descript
	) AS cov_ctgry_descript
	FROM EXP_get_values
	LEFT JOIN LKP_sup_claim_benefit_type
	ON LKP_sup_claim_benefit_type.benefit_type_code = EXP_get_values.claim_pay_ctgry_type
	LEFT JOIN LKP_sup_coverage_category
	ON LKP_sup_coverage_category.cov_ctgry_code = EXP_get_values.cov_ctgry_code
),
LKP_claim_payment_category_type_dim AS (
	SELECT
	claim_pay_ctgry_type_dim_id,
	claim_pay_ctgry_type_descript,
	claim_pay_ctgry_lump_sum_ind,
	cov_ctgry_code,
	cov_ctgry_descript,
	claim_pay_ctgry_type
	FROM (
		SELECT 
			claim_pay_ctgry_type_dim_id,
			claim_pay_ctgry_type_descript,
			claim_pay_ctgry_lump_sum_ind,
			cov_ctgry_code,
			cov_ctgry_descript,
			claim_pay_ctgry_type
		FROM claim_payment_category_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ctgry_type,claim_pay_ctgry_lump_sum_ind ORDER BY claim_pay_ctgry_type_dim_id) = 1
),
RTR_claim_payment_category_type_dim AS (
	SELECT
	LKP_claim_payment_category_type_dim.claim_pay_ctgry_type_dim_id,
	LKP_claim_payment_category_type_dim.claim_pay_ctgry_type_descript AS claim_pay_ctgry_type_descript_lkp,
	LKP_claim_payment_category_type_dim.claim_pay_ctgry_lump_sum_ind AS claim_pay_ctgry_lump_sum_ind_lkp,
	LKP_claim_payment_category_type_dim.cov_ctgry_code AS cov_ctgry_code_lkp,
	LKP_claim_payment_category_type_dim.cov_ctgry_descript AS cov_ctgry_descript_lkp,
	EXP_set_values.claim_pay_ctgry_type,
	EXP_set_values.claim_pay_ctgry_type_descript,
	EXP_set_values.crrnt_snpsht_flag,
	EXP_set_values.audit_id,
	EXP_set_values.eff_from_date,
	EXP_set_values.eff_to_date,
	EXP_set_values.created_date,
	EXP_set_values.modified_date,
	EXP_set_values.claim_pay_ctgry_lump_sum_ind,
	EXP_set_values.cov_ctgry_code,
	EXP_set_values.cov_ctgry_descript
	FROM EXP_set_values
	LEFT JOIN LKP_claim_payment_category_type_dim
	ON LKP_claim_payment_category_type_dim.claim_pay_ctgry_type = EXP_set_values.claim_pay_ctgry_type AND LKP_claim_payment_category_type_dim.claim_pay_ctgry_lump_sum_ind = EXP_set_values.claim_pay_ctgry_lump_sum_ind
),
RTR_claim_payment_category_type_dim_INSERT AS (SELECT * FROM RTR_claim_payment_category_type_dim WHERE ISNULL(claim_pay_ctgry_type_dim_id)),
RTR_claim_payment_category_type_dim_UPDATE AS (SELECT * FROM RTR_claim_payment_category_type_dim WHERE NOT ISNULL(claim_pay_ctgry_type_dim_id) AND (claim_pay_ctgry_type_descript_lkp != claim_pay_ctgry_type_descript OR claim_pay_ctgry_lump_sum_ind != claim_pay_ctgry_lump_sum_ind_lkp OR 
cov_ctgry_code != cov_ctgry_code_lkp OR 
cov_ctgry_descript != 
cov_ctgry_descript_lkp
)),
UPD_claim_payment_category_type_dim_update AS (
	SELECT
	claim_pay_ctgry_type_dim_id AS claim_pay_ctgry_type_dim_id3, 
	claim_pay_ctgry_type_descript_lkp AS claim_pay_ctgry_type_descript_lkp3, 
	claim_pay_ctgry_type_descript AS claim_pay_ctgry_type_descript3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	claim_pay_ctgry_lump_sum_ind AS claim_pay_ctgry_lump_sum_ind3, 
	cov_ctgry_code AS cov_ctgry_code3, 
	cov_ctgry_descript AS cov_ctgry_descript3
	FROM RTR_claim_payment_category_type_dim_UPDATE
),
claim_payment_category_type_dim_update AS (
	MERGE INTO claim_payment_category_type_dim AS T
	USING UPD_claim_payment_category_type_dim_update AS S
	ON T.claim_pay_ctgry_type_dim_id = S.claim_pay_ctgry_type_dim_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_pay_ctgry_type_descript = S.claim_pay_ctgry_type_descript3, T.audit_id = S.audit_id3, T.modified_date = S.modified_date3, T.claim_pay_ctgry_lump_sum_ind = S.claim_pay_ctgry_lump_sum_ind3, T.cov_ctgry_code = S.cov_ctgry_code3, T.cov_ctgry_descript = S.cov_ctgry_descript3
),
UPD_claim_payment_category_type_dim_Insert AS (
	SELECT
	claim_pay_ctgry_type AS claim_pay_ctgry_type1, 
	claim_pay_ctgry_type_descript AS claim_pay_ctgry_type_descript1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	claim_pay_ctgry_lump_sum_ind AS claim_pay_ctgry_lump_sum_ind1, 
	cov_ctgry_code AS cov_ctgry_code1, 
	cov_ctgry_descript AS cov_ctgry_descript1
	FROM RTR_claim_payment_category_type_dim_INSERT
),
claim_payment_category_type_dim_insert AS (
	INSERT INTO claim_payment_category_type_dim
	(claim_pay_ctgry_type, claim_pay_ctgry_type_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, claim_pay_ctgry_lump_sum_ind, cov_ctgry_code, cov_ctgry_descript)
	SELECT 
	claim_pay_ctgry_type1 AS CLAIM_PAY_CTGRY_TYPE, 
	claim_pay_ctgry_type_descript1 AS CLAIM_PAY_CTGRY_TYPE_DESCRIPT, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	claim_pay_ctgry_lump_sum_ind1 AS CLAIM_PAY_CTGRY_LUMP_SUM_IND, 
	cov_ctgry_code1 AS COV_CTGRY_CODE, 
	cov_ctgry_descript1 AS COV_CTGRY_DESCRIPT
	FROM UPD_claim_payment_category_type_dim_Insert
),