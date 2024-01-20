WITH
SQ_gtam_tm517c_stage AS (
	SELECT
		tm517c_stage_id,
		table_fld,
		key_len,
		line_of_business,
		insurance_line,
		risk_unit_group,
		product_type_code,
		language_indicator,
		data_len,
		risk_unit_group_literal,
		extract_date,
		as_of_date,
		record_count,
		source_sytem_id
	FROM gtam_tm517c_stage
),
EXP_Default_Values AS (
	SELECT
	risk_unit_group AS RISK_UNIT_GROUP,
	-- *INF*: IIF(ISNULL(RISK_UNIT_GROUP), 'N/A', LTRIM(RTRIM(RISK_UNIT_GROUP)))
	IFF(RISK_UNIT_GROUP IS NULL, 'N/A', LTRIM(RTRIM(RISK_UNIT_GROUP))) AS RISK_UNIT_GROUP_OUT,
	risk_unit_group_literal AS RISK_UNIT_GROUP_LITERAL,
	-- *INF*: IIF(ISNULL(RISK_UNIT_GROUP_LITERAL), 'N/A', LTRIM(RTRIM(RISK_UNIT_GROUP_LITERAL)))
	IFF(RISK_UNIT_GROUP_LITERAL IS NULL, 'N/A', LTRIM(RTRIM(RISK_UNIT_GROUP_LITERAL))) AS RISK_UNIT_GROUP_LITERAL_OUT,
	line_of_business AS IN_line_of_business,
	-- *INF*: IIF(ISNULL(IN_line_of_business),'N/A',LTRIM(RTRIM(IN_line_of_business)))
	IFF(IN_line_of_business IS NULL, 'N/A', LTRIM(RTRIM(IN_line_of_business))) AS line_of_business_OUT,
	insurance_line AS IN_insurance_line,
	-- *INF*: iif(isnull(IN_insurance_line),'N/A',LTRIM(RTRIM(IN_insurance_line)))
	IFF(IN_insurance_line IS NULL, 'N/A', LTRIM(RTRIM(IN_insurance_line))) AS insurance_line_OUT,
	product_type_code AS IN_product_type_code,
	-- *INF*: iif(isnull(IN_product_type_code),'N/A',LTRIM(RTRIM(IN_product_type_code)))
	IFF(IN_product_type_code IS NULL, 'N/A', LTRIM(RTRIM(IN_product_type_code))) AS product_type_code_OUT
	FROM SQ_gtam_tm517c_stage
),
LKP_sup_risk_unit_group AS (
	SELECT
	sup_risk_unit_grp_id,
	risk_unit_grp_code,
	risk_unit_grp_descript,
	prdct_type_code,
	lob,
	ins_line,
	IN_RISK_UNIT_GROUP,
	IN_line_of_business,
	IN_insurance_line,
	IN_product_type_code
	FROM (
		SELECT sup_risk_unit_group.sup_risk_unit_grp_id as sup_risk_unit_grp_id, 
		sup_risk_unit_group.risk_unit_grp_descript as risk_unit_grp_descript, 
		LTRIM(RTRIM(sup_risk_unit_group.risk_unit_grp_code)) as risk_unit_grp_code, 
		LTRIM(RTRIM(sup_risk_unit_group.prdct_type_code)) as prdct_type_code,
		 LTRIM(RTRIM(sup_risk_unit_group.lob)) as lob, 
		LTRIM(RTRIM(sup_risk_unit_group.ins_line)) as ins_line
		 FROM sup_risk_unit_group where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_grp_code,prdct_type_code,lob,ins_line ORDER BY sup_risk_unit_grp_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_sup_risk_unit_group.sup_risk_unit_grp_id AS old_sup_risk_unit_grp_id,
	LKP_sup_risk_unit_group.risk_unit_grp_descript AS old_risk_unit_grp_descript,
	EXP_Default_Values.RISK_UNIT_GROUP_OUT,
	EXP_Default_Values.RISK_UNIT_GROUP_LITERAL_OUT,
	EXP_Default_Values.line_of_business_OUT,
	EXP_Default_Values.insurance_line_OUT,
	EXP_Default_Values.product_type_code_OUT,
	-- *INF*: IIF(ISNULL(old_sup_risk_unit_grp_id), 'NEW', IIF(old_risk_unit_grp_descript != RISK_UNIT_GROUP_LITERAL_OUT, 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(
	    old_sup_risk_unit_grp_id IS NULL, 'NEW',
	    IFF(
	        old_risk_unit_grp_descript != RISK_UNIT_GROUP_LITERAL_OUT, 'UPDATE', 'NOCHANGE'
	    )
	) AS v_CHANGED_FLAG,
	v_CHANGED_FLAG AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_CHANGED_FLAG='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_CHANGED_FLAG = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_sup_risk_unit_group
	ON LKP_sup_risk_unit_group.risk_unit_grp_code = EXP_Default_Values.RISK_UNIT_GROUP_OUT AND LKP_sup_risk_unit_group.prdct_type_code = EXP_Default_Values.product_type_code_OUT AND LKP_sup_risk_unit_group.lob = EXP_Default_Values.line_of_business_OUT AND LKP_sup_risk_unit_group.ins_line = EXP_Default_Values.insurance_line_OUT
),
FIL_sup_insurance_line_insert AS (
	SELECT
	RISK_UNIT_GROUP_OUT, 
	RISK_UNIT_GROUP_LITERAL_OUT, 
	line_of_business_OUT AS IN_line_of_business, 
	insurance_line_OUT AS IN_ins_line, 
	product_type_code_OUT AS IN_product_type_code, 
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	source_sys_id
	FROM EXP_detect_changes
	WHERE CHANGED_FLAG = 'NEW'  OR  CHANGED_FLAG = 'UPDATE'
),
sup_risk_unit_group_Insert AS (
	INSERT INTO sup_risk_unit_group
	(risk_unit_grp_code, risk_unit_grp_descript, prdct_type_code, lob, ins_line, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	RISK_UNIT_GROUP_OUT AS RISK_UNIT_GRP_CODE, 
	RISK_UNIT_GROUP_LITERAL_OUT AS RISK_UNIT_GRP_DESCRIPT, 
	IN_product_type_code AS PRDCT_TYPE_CODE, 
	IN_line_of_business AS LOB, 
	IN_ins_line AS INS_LINE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_insurance_line_insert
),
SQ_sup_risk_unit_group AS (
	SELECT a.sup_risk_unit_grp_id,
	a.risk_unit_grp_code,
	a.prdct_type_code, 
	a.lob, 
	a.ins_line,
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit_group a
	WHERE EXISTS(SELECT 1
	                  FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit_group b
	                  WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
	                  AND a.risk_unit_grp_code = b.risk_unit_grp_code
	                  AND a.prdct_type_code=b.prdct_type_code
	                  AND a.lob=b.lob
	                  AND a.ins_line=b.ins_line
	                  GROUP BY risk_unit_grp_code, prdct_type_code, lob, ins_line
	                  HAVING COUNT(*) > 1)
	ORDER BY risk_unit_grp_code,prdct_type_code,lob, ins_line, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_risk_unit_grp_id,
	risk_unit_grp_code,
	prdct_type_code,
	lob,
	ins_line,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	risk_unit_grp_code = v_Prev_row_risk_unit_grp_code and 
	-- prdct_type_code = v_Prev_row_prdct_type_code and
	-- lob = v_Prev_row_lob and
	-- v_Prev_row_ins_line = v_Prev_row_ins_line, 
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(
	    TRUE,
	    risk_unit_grp_code = v_Prev_row_risk_unit_grp_code and prdct_type_code = v_Prev_row_prdct_type_code and lob = v_Prev_row_lob and v_Prev_row_ins_line = v_Prev_row_ins_line, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	risk_unit_grp_code AS v_Prev_row_risk_unit_grp_code,
	prdct_type_code AS v_Prev_row_prdct_type_code,
	lob AS v_Prev_row_lob,
	ins_line AS v_Prev_row_ins_line,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_risk_unit_group
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_risk_unit_grp_id, 
	risk_unit_grp_code, 
	prdct_type_code, 
	lob, 
	ins_line, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_risk_unit_group AS (
	SELECT
	sup_risk_unit_grp_id, 
	risk_unit_grp_code, 
	prdct_type_code, 
	lob, 
	ins_line, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_risk_unit_group_Update AS (
	MERGE INTO sup_risk_unit_group AS T
	USING UPD_sup_risk_unit_group AS S
	ON T.sup_risk_unit_grp_id = S.sup_risk_unit_grp_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.risk_unit_grp_code = S.risk_unit_grp_code, T.lob = S.lob, T.ins_line = S.ins_line, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),