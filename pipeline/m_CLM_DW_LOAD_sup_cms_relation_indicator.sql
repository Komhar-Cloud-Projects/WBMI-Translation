WITH
SQ_cms_relation_ind_stage AS (
	SELECT
		cms_relation_ind_stage_id,
		cms_party_type,
		cms_relation_ind,
		is_individual,
		cms_relation_desc,
		cms_rel_file_code,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM cms_relation_ind_stage
),
EXP_Default_Values AS (
	SELECT
	cms_party_type,
	cms_relation_ind,
	is_individual,
	cms_relation_desc,
	cms_rel_file_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(cms_party_type)
	UDF_DEFAULT_VALUE_FOR_STRINGS(cms_party_type) AS cms_party_type1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(cms_relation_ind)
	UDF_DEFAULT_VALUE_FOR_STRINGS(cms_relation_ind) AS cms_relation_ind1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(is_individual)
	UDF_DEFAULT_VALUE_FOR_STRINGS(is_individual) AS is_individual1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(cms_relation_desc)
	UDF_DEFAULT_VALUE_FOR_STRINGS(cms_relation_desc) AS cms_relation_desc1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(cms_rel_file_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(cms_rel_file_code) AS cms_rel_file_code1
	FROM SQ_cms_relation_ind_stage
),
LKP_Target AS (
	SELECT
	sup_cms_relation_ind_id,
	cms_relation_descript,
	cms_relation_file_code,
	cms_party_type,
	cms_relation_ind,
	is_cms_party_individ
	FROM (
		SELECT 
		a.sup_cms_relation_ind_id as sup_cms_relation_ind_id,
		ltrim(rtrim(a.cms_relation_descript)) as cms_relation_descript, 
		ltrim(rtrim(a.cms_relation_file_code)) as cms_relation_file_code, 
		ltrim(rtrim(a.cms_party_type)) as cms_party_type,
		ltrim(rtrim(a.cms_relation_ind)) as cms_relation_ind,
		ltrim(rtrim(a.is_cms_party_individ)) as is_cms_party_individ
		FROM 
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cms_relation_indicator a
		WHERE 
			crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cms_party_type,cms_relation_ind,is_cms_party_individ ORDER BY sup_cms_relation_ind_id DESC) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_Target.sup_cms_relation_ind_id,
	LKP_Target.cms_relation_descript,
	LKP_Target.cms_relation_file_code,
	EXP_Default_Values.cms_party_type1,
	EXP_Default_Values.cms_relation_ind1,
	EXP_Default_Values.is_individual1,
	EXP_Default_Values.cms_relation_desc1,
	EXP_Default_Values.cms_rel_file_code1,
	-- *INF*: IIF(ISNULL(sup_cms_relation_ind_id), 'NEW', 
	-- IIF(
	-- (cms_relation_descript != cms_relation_desc1 OR
	-- cms_relation_file_code != cms_rel_file_code1)
	-- , 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(
	    sup_cms_relation_ind_id IS NULL, 'NEW',
	    IFF(
	        (cms_relation_descript != cms_relation_desc1
	        or cms_relation_file_code != cms_rel_file_code1),
	        'UPDATE',
	        'NOCHANGE'
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
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_Default_Values
	LEFT JOIN LKP_Target
	ON LKP_Target.cms_party_type = EXP_Default_Values.cms_party_type1 AND LKP_Target.cms_relation_ind = EXP_Default_Values.cms_relation_ind1 AND LKP_Target.is_cms_party_individ = EXP_Default_Values.is_individual1
),
FIL_sup_insurance_line_insert AS (
	SELECT
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	cms_party_type1, 
	cms_relation_ind1, 
	is_individual1, 
	cms_relation_desc1, 
	cms_rel_file_code1
	FROM EXP_detect_changes
	WHERE CHANGED_FLAG = 'NEW' or CHANGED_FLAG = 'UPDATE'
),
sup_cms_relation_indicator_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cms_relation_indicator
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, cms_party_type, cms_relation_ind, is_cms_party_individ, cms_relation_descript, cms_relation_file_code)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	cms_party_type1 AS CMS_PARTY_TYPE, 
	cms_relation_ind1 AS CMS_RELATION_IND, 
	is_individual1 AS IS_CMS_PARTY_INDIVID, 
	cms_relation_desc1 AS CMS_RELATION_DESCRIPT, 
	cms_rel_file_code1 AS CMS_RELATION_FILE_CODE
	FROM FIL_sup_insurance_line_insert
),
SQ_sup_cms_relation_indicator AS (
	SELECT 
	a.sup_cms_relation_ind_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.cms_party_type,
	a.cms_relation_ind,
	is_cms_party_individ
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cms_relation_indicator a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cms_relation_indicator b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			                  AND a.cms_party_type = b.cms_party_type 
					     AND a.cms_relation_ind = b.cms_relation_ind
		                        AND a.is_cms_party_individ = b.is_cms_party_individ
			GROUP BY cms_party_type,cms_relation_ind,is_cms_party_individ
			HAVING COUNT(*) > 1)
	ORDER BY cms_party_type,cms_relation_ind, is_cms_party_individ, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_cms_relation_ind_id,
	cms_party_type,
	cms_relation_ind,
	is_cms_party_individ,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	cms_party_type = v_Prev_row_cms_party_type AND cms_relation_ind = v_Prev_row_cms_relation_ind AND is_cms_party_individ = v_Prev_row_cms_party_individ,
	--       ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(
	    TRUE,
	    cms_party_type = v_Prev_row_cms_party_type AND cms_relation_ind = v_Prev_row_cms_relation_ind AND is_cms_party_individ = v_Prev_row_cms_party_individ, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	cms_party_type AS v_Prev_row_cms_party_type,
	cms_relation_ind AS v_Prev_row_cms_relation_ind,
	is_cms_party_individ AS v_Prev_row_cms_party_individ,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_cms_relation_indicator
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_cms_relation_ind_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_insurance_line AS (
	SELECT
	sup_cms_relation_ind_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_cms_relation_indicator_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cms_relation_indicator AS T
	USING UPD_sup_insurance_line AS S
	ON T.sup_cms_relation_ind_id = S.sup_cms_relation_ind_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),