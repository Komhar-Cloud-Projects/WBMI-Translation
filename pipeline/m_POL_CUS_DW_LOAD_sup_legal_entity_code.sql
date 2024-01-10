WITH
SQ_gtam_tl79_stage AS (
	SELECT
		b.xtdu01_code as location, 
		b.verbal_description as master_company_number,
		a.lineof_business,
		a.legal_entity, 
		a.legal_entity_literal 
	FROM  
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.gtam_tl79_stage a
	LEFT OUTER JOIN
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.gtam_xtdu01_stage b ON 
		SUBSTRING(xtdu01_code,1,1) = LTRIM(RTRIM(legal_entity))AND
	 	a.legal_entity = 'O'
	WHERE
		a.master_company_number <> '99'AND
		a.location <>'99'
	
	-- Here we are performing left outer join between gtam_tl79_stage and gtam_xtdu01_stage tables based on 
	-- substring (xtdu01_code, 1, 1)  & legal_entity columns
),
EXP_values AS (
	SELECT
	location AS in_xtdu01_code,
	master_company_number AS in_verbal_description,
	lineof_business AS in_lineof_business,
	-- *INF*: iif(isnull(in_lineof_business) or IS_SPACES(in_lineof_business) or LENGTH(in_lineof_business)=0,'N/A',LTRIM(RTRIM(in_lineof_business)))
	IFF(in_lineof_business IS NULL OR IS_SPACES(in_lineof_business) OR LENGTH(in_lineof_business) = 0, 'N/A', LTRIM(RTRIM(in_lineof_business))) AS line_of_business,
	lgl_ent_code,
	legal_entity_literal AS in_legal_entity_literal,
	-- *INF*: IIF(lgl_ent_code<>'O','N/A',ltrim(rtrim(in_xtdu01_code)))
	IFF(lgl_ent_code <> 'O', 'N/A', ltrim(rtrim(in_xtdu01_code))) AS lgl_ent_sub_code,
	-- *INF*: IIF(lgl_ent_code<>'O',in_legal_entity_literal,in_verbal_description)
	IFF(lgl_ent_code <> 'O', in_legal_entity_literal, in_verbal_description) AS v_legal_entity_description,
	-- *INF*: iif(isnull(v_legal_entity_description) or IS_SPACES(v_legal_entity_description) or LENGTH(v_legal_entity_description)=0,'Not Avaliable',ltrim(rtrim(v_legal_entity_description)))
	IFF(v_legal_entity_description IS NULL OR IS_SPACES(v_legal_entity_description) OR LENGTH(v_legal_entity_description) = 0, 'Not Avaliable', ltrim(rtrim(v_legal_entity_description))) AS lgl_ent_code_descript
	FROM SQ_gtam_tl79_stage
),
LKP_sup_legal_entity_code AS (
	SELECT
	sup_lgl_ent_code_id,
	lgl_ent_code_descript,
	lob,
	lgl_ent_code,
	lgl_ent_sub_code
	FROM (
		SELECT 
		sup_legal_entity_code.sup_lgl_ent_code_id as sup_lgl_ent_code_id, sup_legal_entity_code.lgl_ent_code_descript as lgl_ent_code_descript, 
		ltrim(rtrim(sup_legal_entity_code.lob)) as lob, 
		sup_legal_entity_code.lgl_ent_code as lgl_ent_code, 
		ltrim(rtrim(sup_legal_entity_code.lgl_ent_sub_code)) as lgl_ent_sub_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_legal_entity_code
		where sup_legal_entity_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY lob,lgl_ent_code,lgl_ent_sub_code ORDER BY sup_lgl_ent_code_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_sup_legal_entity_code.sup_lgl_ent_code_id AS lkp_sup_legal_entity_id,
	LKP_sup_legal_entity_code.lgl_ent_code_descript AS lkp_legal_entity_description,
	EXP_values.line_of_business,
	EXP_values.lgl_ent_code,
	EXP_values.lgl_ent_sub_code,
	EXP_values.lgl_ent_code_descript,
	-- *INF*: iif(isnull(lkp_sup_legal_entity_id),'NEW',
	-- IIF(LTRIM(RTRIM(lkp_legal_entity_description)) != LTRIM(RTRIM(lgl_ent_code_descript)),'UPDATE','NOCHANGE'))
	IFF(lkp_sup_legal_entity_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(lkp_legal_entity_description)) != LTRIM(RTRIM(lgl_ent_code_descript)), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_values
	LEFT JOIN LKP_sup_legal_entity_code
	ON LKP_sup_legal_entity_code.lob = EXP_values.line_of_business AND LKP_sup_legal_entity_code.lgl_ent_code = EXP_values.lgl_ent_code AND LKP_sup_legal_entity_code.lgl_ent_sub_code = EXP_values.lgl_ent_sub_code
),
FIL_insert AS (
	SELECT
	line_of_business, 
	lgl_ent_code, 
	lgl_ent_sub_code, 
	lgl_ent_code_descript, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
TGT_sup_legal_entity_code_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_legal_entity_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, lob, lgl_ent_code, lgl_ent_sub_code, lgl_ent_code_descript, StandardLegalEntityCode, StandardLegalEntityDescription)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	line_of_business AS LOB, 
	LGL_ENT_CODE, 
	LGL_ENT_SUB_CODE, 
	LGL_ENT_CODE_DESCRIPT, 
	lgl_ent_code AS STANDARDLEGALENTITYCODE, 
	lgl_ent_code_descript AS STANDARDLEGALENTITYDESCRIPTION
	FROM FIL_insert
),
SQ_sup_legal_entity_code AS (
	SELECT 
	a.sup_lgl_ent_code_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.lob, 
	a.lgl_ent_code, 
	a.lgl_ent_sub_code 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_legal_entity_code a
	WHERE EXISTS(SELECT 1			
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_legal_entity_code b
	WHERE b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'   
	AND b.crrnt_snpsht_flag = 1 AND  
	a.lob=b.lob   AND
	a.lgl_ent_code=b.lgl_ent_code AND
	a.lgl_ent_sub_code = b.lgl_ent_sub_code
	GROUP BY b.lob,b.lgl_ent_code,b.lgl_ent_sub_code 
	HAVING COUNT(*) > 1)
	ORDER BY  
	a.lob, 
	a.lgl_ent_code, 
	a.lgl_ent_sub_code,
	a.eff_from_date  DESC
	
	
	
	
	--EXISTS Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	sup_lgl_ent_code_id AS sup_legal_entity_id,
	eff_from_date AS in_eff_from_date,
	eff_to_date AS orig_eff_to_date,
	lob,
	lgl_ent_code AS in_legal_entity_code,
	lgl_ent_sub_code AS in_legal_entity_sub_code,
	-- *INF*: DECODE(TRUE,
	-- lob = v_prev_line_of_business
	--  AND
	-- in_legal_entity_code=v_prev_legal_entity_code 
	-- AND
	-- in_legal_entity_sub_code= v_prev_legal_entity_sub_code ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		lob = v_prev_line_of_business AND in_legal_entity_code = v_prev_legal_entity_code AND in_legal_entity_sub_code = v_prev_legal_entity_sub_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	lob AS v_prev_line_of_business,
	in_legal_entity_code AS v_prev_legal_entity_code,
	in_legal_entity_sub_code AS v_prev_legal_entity_sub_code,
	in_eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_legal_entity_code
),
FIL_FirstRowInAKGroup AS (
	SELECT
	sup_legal_entity_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_legal_entity_code AS (
	SELECT
	sup_legal_entity_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_sup_legal_entity_code_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_legal_entity_code AS T
	USING UPD_sup_legal_entity_code AS S
	ON T.sup_lgl_ent_code_id = S.sup_legal_entity_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),