WITH
SQ_sup_benefit_type_stage AS (
	SELECT
		sup_benefit_type_id,
		code,
		descript,
		modified_date,
		modified_user_id,
		fin_type_cd,
		cause_of_loss,
		filter_type,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_benefit_type_stage
),
EXP_default AS (
	SELECT
	code,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(code)))OR IS_SPACES(LTRIM(RTRIM(code))) OR LENGTH(LTRIM(RTRIM(code))) =0, 'N/A',LTRIM(RTRIM(code)))
	IFF(LTRIM(RTRIM(code)) IS NULL OR IS_SPACES(LTRIM(RTRIM(code))) OR LENGTH(LTRIM(RTRIM(code))) = 0, 'N/A', LTRIM(RTRIM(code))) AS benefit_code_out,
	descript,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(descript))) OR IS_SPACES(LTRIM(RTRIM(descript))) OR LENGTH(LTRIM(RTRIM(descript))) = 0 ,'N/A' , LTRIM(RTRIM(descript)))
	IFF(LTRIM(RTRIM(descript)) IS NULL OR IS_SPACES(LTRIM(RTRIM(descript))) OR LENGTH(LTRIM(RTRIM(descript))) = 0, 'N/A', LTRIM(RTRIM(descript))) AS benefit_code_descript_out,
	fin_type_cd,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(fin_type_cd))) OR IS_SPACES(LTRIM(RTRIM(fin_type_cd))) OR LENGTH(LTRIM(RTRIM(fin_type_cd))) = 0 ,'N/A' , LTRIM(RTRIM(fin_type_cd)))
	IFF(LTRIM(RTRIM(fin_type_cd)) IS NULL OR IS_SPACES(LTRIM(RTRIM(fin_type_cd))) OR LENGTH(LTRIM(RTRIM(fin_type_cd))) = 0, 'N/A', LTRIM(RTRIM(fin_type_cd))) AS fin_type_cd_out,
	cause_of_loss,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(cause_of_loss))) OR IS_SPACES(LTRIM(RTRIM(cause_of_loss))) OR LENGTH(LTRIM(RTRIM(cause_of_loss))) = 0 ,'N/A' , LTRIM(RTRIM(cause_of_loss)))
	IFF(LTRIM(RTRIM(cause_of_loss)) IS NULL OR IS_SPACES(LTRIM(RTRIM(cause_of_loss))) OR LENGTH(LTRIM(RTRIM(cause_of_loss))) = 0, 'N/A', LTRIM(RTRIM(cause_of_loss))) AS cause_of_loss_out,
	filter_type,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(filter_type))) OR IS_SPACES(LTRIM(RTRIM(filter_type))) OR LENGTH(LTRIM(RTRIM(filter_type))) = 0 ,'N/A' , LTRIM(RTRIM(filter_type)))
	IFF(LTRIM(RTRIM(filter_type)) IS NULL OR IS_SPACES(LTRIM(RTRIM(filter_type))) OR LENGTH(LTRIM(RTRIM(filter_type))) = 0, 'N/A', LTRIM(RTRIM(filter_type))) AS filter_type_out
	FROM SQ_sup_benefit_type_stage
),
LKP_Claim_benefit_type AS (
	SELECT
	sup_claim_benefit_type_id,
	benefit_type_code_descript,
	financial_type_code,
	cause_of_loss,
	filter_type_code,
	benefit_type_code
	FROM (
		SELECT sup_claim_benefit_type.sup_claim_benefit_type_id as sup_claim_benefit_type_id, sup_claim_benefit_type.benefit_type_code_descript as benefit_type_code_descript, sup_claim_benefit_type.financial_type_code as financial_type_code, sup_claim_benefit_type.cause_of_loss as cause_of_loss, sup_claim_benefit_type.filter_type_code as filter_type_code, sup_claim_benefit_type.benefit_type_code as benefit_type_code FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_benefit_type
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY benefit_type_code ORDER BY sup_claim_benefit_type_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_Claim_benefit_type.sup_claim_benefit_type_id,
	LKP_Claim_benefit_type.benefit_type_code_descript AS old_benefit_type_code_description,
	LKP_Claim_benefit_type.financial_type_code AS old_fin_type_code,
	LKP_Claim_benefit_type.cause_of_loss AS old_cause_of_loss,
	LKP_Claim_benefit_type.filter_type_code AS old_filter_type_code,
	EXP_default.benefit_code_out AS benefit_code,
	EXP_default.benefit_code_descript_out AS benefit_code_descript,
	EXP_default.fin_type_cd_out AS fin_type_cd,
	EXP_default.cause_of_loss_out AS cause_of_loss,
	EXP_default.filter_type_out AS filter_type,
	-- *INF*: IIF(ISNULL(sup_claim_benefit_type_id), 'NEW', 
	-- IIF(LTRIM(RTRIM(old_benefit_type_code_description)) != (LTRIM(RTRIM(benefit_code_descript))) or 
	--        LTRIM(RTRIM(old_fin_type_code)) != (LTRIM(RTRIM(fin_type_cd))) or
	--        LTRIM(RTRIM(old_cause_of_loss)) != (LTRIM(RTRIM(cause_of_loss))) or
	--        LTRIM(RTRIM(old_filter_type_code)) != (LTRIM(RTRIM(filter_type)))
	-- , 'UPDATE', 'NOCHANGE'))
	IFF(sup_claim_benefit_type_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(old_benefit_type_code_description)) != ( LTRIM(RTRIM(benefit_code_descript)) ) OR LTRIM(RTRIM(old_fin_type_code)) != ( LTRIM(RTRIM(fin_type_cd)) ) OR LTRIM(RTRIM(old_cause_of_loss)) != ( LTRIM(RTRIM(cause_of_loss)) ) OR LTRIM(RTRIM(old_filter_type_code)) != ( LTRIM(RTRIM(filter_type)) ), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS Changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*:  TO_DATE('12/31/2100 11:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 11:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_default
	LEFT JOIN LKP_Claim_benefit_type
	ON LKP_Claim_benefit_type.benefit_type_code = EXP_default.benefit_code_out
),
FIL_new_update AS (
	SELECT
	benefit_code, 
	benefit_code_descript, 
	fin_type_cd, 
	cause_of_loss, 
	filter_type, 
	Changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_detect_changes
	WHERE Changed_flag = 'NEW' or Changed_flag = 'UPDATE'
),
sup_claim_benefit_type_insert AS (
	INSERT INTO sup_claim_benefit_type
	(benefit_type_code, benefit_type_code_descript, financial_type_code, cause_of_loss, filter_type_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	benefit_code AS BENEFIT_TYPE_CODE, 
	benefit_code_descript AS BENEFIT_TYPE_CODE_DESCRIPT, 
	fin_type_cd AS FINANCIAL_TYPE_CODE, 
	CAUSE_OF_LOSS, 
	filter_type AS FILTER_TYPE_CODE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_new_update
),
SQ_sup_claim_benefit_type AS (
	SELECT a.sup_claim_benefit_type_id, a.benefit_type_code, a.financial_type_code,a.cause_of_loss,a.filter_type_code,a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_benefit_type a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_benefit_type b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.benefit_type_code = a.benefit_type_code
	            AND  a.financial_type_code =  b.financial_type_code
	            AND  a.cause_of_loss = a.cause_of_loss
	            AND  a.filter_type_code = a.filter_type_code
			GROUP BY b.benefit_type_code,b.financial_type_code,b.cause_of_loss,b.filter_type_code
			HAVING COUNT(*) > 1)
	ORDER BY a.benefit_type_code,a.financial_type_code,a.cause_of_loss,a.filter_type_code, a.eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_claim_benefit_type_id,
	benefit_type_code,
	financial_type_code,
	cause_of_loss,
	filter_type_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(
	-- TRUE,benefit_type_code= v_Prev_row_benefit_type_code AND financial_type_code=v_Prev_row_fin_type_code AND cause_of_loss = v_Prev_row_cause_of_loss  AND filter_type_code=v_Prev_row_filter_type_code
	-- , ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	-- 	
	DECODE(TRUE,
	benefit_type_code = v_Prev_row_benefit_type_code AND financial_type_code = v_Prev_row_fin_type_code AND cause_of_loss = v_Prev_row_cause_of_loss AND filter_type_code = v_Prev_row_filter_type_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	benefit_type_code AS v_Prev_row_benefit_type_code,
	financial_type_code AS v_Prev_row_fin_type_code,
	cause_of_loss AS v_Prev_row_cause_of_loss,
	filter_type_code AS v_Prev_row_filter_type_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_claim_benefit_type
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_claim_benefit_type_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_eff_from_date AS (
	SELECT
	sup_claim_benefit_type_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_claim_benefit_type_update AS (
	MERGE INTO sup_claim_benefit_type AS T
	USING UPD_eff_from_date AS S
	ON T.sup_claim_benefit_type_id = S.sup_claim_benefit_type_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),