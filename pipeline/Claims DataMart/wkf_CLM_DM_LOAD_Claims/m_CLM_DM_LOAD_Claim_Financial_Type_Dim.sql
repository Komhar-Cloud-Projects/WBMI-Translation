WITH
SQ_sup_claim_financial_code AS (
	SELECT DISTINCT
	sup_claim_financial_code.financial_code, sup_claim_financial_code.financial_descript 
	FROM
	  @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_financial_code
),
EXP_get_new_values AS (
	SELECT
	financial_code,
	financial_descript,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	sysdate AS created_date,
	sysdate AS modified_date
	FROM SQ_sup_claim_financial_code
),
LKP_financial_type_dim AS (
	SELECT
	claim_financial_type_dim_id,
	financial_type_code
	FROM (
		SELECT 
			claim_financial_type_dim_id,
			financial_type_code
		FROM claim_financial_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY financial_type_code ORDER BY claim_financial_type_dim_id) = 1
),
RTR_financial_type_dim AS (
	SELECT
	LKP_financial_type_dim.claim_financial_type_dim_id,
	EXP_get_new_values.financial_code,
	EXP_get_new_values.financial_descript,
	EXP_get_new_values.crrnt_snpsht_flag,
	EXP_get_new_values.audit_id,
	EXP_get_new_values.eff_from_date,
	EXP_get_new_values.eff_to_date,
	EXP_get_new_values.source_sys_id,
	EXP_get_new_values.created_date,
	EXP_get_new_values.modified_date
	FROM EXP_get_new_values
	LEFT JOIN LKP_financial_type_dim
	ON LKP_financial_type_dim.financial_type_code = EXP_get_new_values.financial_code
),
RTR_financial_type_dim_INSERT AS (SELECT * FROM RTR_financial_type_dim WHERE ISNULL(claim_financial_type_dim_id)),
RTR_financial_type_dim_DEFAULT1 AS (SELECT * FROM RTR_financial_type_dim WHERE NOT ( (ISNULL(claim_financial_type_dim_id)) )),
UPD_financial_type_dim_Insert AS (
	SELECT
	financial_code AS financial_code_s1, 
	financial_descript AS financial_descript1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	source_sys_id AS source_sys_id1, 
	created_date AS created_date1, 
	modified_date AS modified_date1
	FROM RTR_financial_type_dim_INSERT
),
claim_financial_type_dim_INSERT AS (
	INSERT INTO claim_financial_type_dim
	(financial_type_code, financial_type_code_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date)
	SELECT 
	financial_code_s1 AS FINANCIAL_TYPE_CODE, 
	financial_descript1 AS FINANCIAL_TYPE_CODE_DESCRIPT, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE
	FROM UPD_financial_type_dim_Insert
),
UPD_financial_type_dim_Update AS (
	SELECT
	claim_financial_type_dim_id AS claim_financial_type_dim_id2, 
	financial_code AS financial_code_s2, 
	financial_descript AS financial_descript2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	source_sys_id AS source_sys_id2, 
	created_date AS created_date2, 
	modified_date AS modified_date2
	FROM RTR_financial_type_dim_DEFAULT1
),
claim_financial_type_dim_UPDATE AS (
	MERGE INTO claim_financial_type_dim AS T
	USING UPD_financial_type_dim_Update AS S
	ON T.claim_financial_type_dim_id = S.claim_financial_type_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.financial_type_code = S.financial_code_s2, T.financial_type_code_descript = S.financial_descript2, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.created_date = S.created_date2, T.modified_date = S.modified_date2
),