WITH
SQ_cms_tin_offices_stage AS (
	SELECT
		cms_tin_offices_stage_id,
		cms_rre_id,
		office_tin_num,
		office_cd,
		office_name,
		office_mail_addr1,
		office_mail_addr2,
		office_mail_city,
		office_mail_state,
		office_mail_zip,
		office_mail_zip4,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM cms_tin_offices_stage
),
EXP_Default_Values AS (
	SELECT
	cms_rre_id,
	office_tin_num,
	office_cd,
	office_name,
	office_mail_addr1,
	office_mail_addr2,
	office_mail_city,
	office_mail_state,
	office_mail_zip,
	office_mail_zip4,
	cms_rre_id AS cms_rre_id1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(office_tin_num)
	-- 
	UDF_DEFAULT_VALUE_FOR_STRINGS(office_tin_num) AS office_tin_num1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(office_cd)
	UDF_DEFAULT_VALUE_FOR_STRINGS(office_cd) AS office_cd1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(office_name)
	UDF_DEFAULT_VALUE_FOR_STRINGS(office_name) AS office_name1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(office_mail_addr1)
	UDF_DEFAULT_VALUE_FOR_STRINGS(office_mail_addr1) AS office_mail_addr11,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(office_mail_addr2)
	UDF_DEFAULT_VALUE_FOR_STRINGS(office_mail_addr2) AS office_mail_addr21,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(office_mail_city)
	UDF_DEFAULT_VALUE_FOR_STRINGS(office_mail_city) AS office_mail_city1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(office_mail_state)
	UDF_DEFAULT_VALUE_FOR_STRINGS(office_mail_state) AS office_mail_state1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(office_mail_zip)
	UDF_DEFAULT_VALUE_FOR_STRINGS(office_mail_zip) AS office_mail_zip1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(office_mail_zip4)
	UDF_DEFAULT_VALUE_FOR_STRINGS(office_mail_zip4) AS office_mail_zip41
	FROM SQ_cms_tin_offices_stage
),
LKP_Target AS (
	SELECT
	sup_cms_tin_office_id,
	cms_rre_id,
	office_tin_num,
	office_code,
	office_name,
	office_mail_address1,
	office_mail_address2,
	office_mail_city,
	office_mail_state,
	office_mail_zip,
	office_mail_zip4
	FROM (
		SELECT sup_cms_tin_office.sup_cms_tin_office_id as sup_cms_tin_office_id, sup_cms_tin_office.office_tin_num as office_tin_num, sup_cms_tin_office.office_code as office_code, sup_cms_tin_office.office_name as office_name, sup_cms_tin_office.office_mail_address1 as office_mail_address1, sup_cms_tin_office.office_mail_address2 as office_mail_address2, sup_cms_tin_office.office_mail_city as office_mail_city, sup_cms_tin_office.office_mail_state as office_mail_state, sup_cms_tin_office.office_mail_zip as office_mail_zip, sup_cms_tin_office.office_mail_zip4 as office_mail_zip4, sup_cms_tin_office.cms_rre_id as cms_rre_id FROM sup_cms_tin_office
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cms_rre_id ORDER BY sup_cms_tin_office_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_Target.sup_cms_tin_office_id,
	LKP_Target.cms_rre_id,
	LKP_Target.office_tin_num,
	LKP_Target.office_code AS office_cd,
	LKP_Target.office_name,
	LKP_Target.office_mail_address1,
	LKP_Target.office_mail_address2,
	LKP_Target.office_mail_city,
	LKP_Target.office_mail_state,
	LKP_Target.office_mail_zip,
	LKP_Target.office_mail_zip4,
	EXP_Default_Values.cms_rre_id1,
	EXP_Default_Values.office_tin_num1,
	EXP_Default_Values.office_cd1,
	EXP_Default_Values.office_name1,
	EXP_Default_Values.office_mail_addr11,
	EXP_Default_Values.office_mail_addr21,
	EXP_Default_Values.office_mail_city1,
	EXP_Default_Values.office_mail_state1,
	EXP_Default_Values.office_mail_zip1,
	EXP_Default_Values.office_mail_zip41,
	-- *INF*: IIF(ISNULL(sup_cms_tin_office_id), 'NEW', 
	-- IIF(
	-- (office_tin_num != office_tin_num1 OR
	-- office_cd != office_cd1 OR
	-- office_name != office_name1 OR
	-- office_mail_address1 != office_mail_addr11 OR
	-- office_mail_address2 != office_mail_addr21 OR
	-- office_mail_city != office_mail_city1 OR
	-- office_mail_state != office_mail_state1 OR
	-- office_mail_zip != office_mail_zip1 OR
	-- office_mail_zip4 != office_mail_zip41)
	-- , 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(
	    sup_cms_tin_office_id IS NULL, 'NEW',
	    IFF(
	        (office_tin_num != office_tin_num1
	        or office_cd != office_cd1
	        or office_name != office_name1
	        or office_mail_address1 != office_mail_addr11
	        or office_mail_address2 != office_mail_addr21
	        or office_mail_city != office_mail_city1
	        or office_mail_state != office_mail_state1
	        or office_mail_zip != office_mail_zip1
	        or office_mail_zip4 != office_mail_zip41),
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
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_Target
	ON LKP_Target.cms_rre_id = EXP_Default_Values.cms_rre_id1
),
FIL_sup_insurance_line_insert AS (
	SELECT
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	source_sys_id, 
	cms_rre_id1, 
	office_tin_num1, 
	office_cd1, 
	office_name1, 
	office_mail_addr11, 
	office_mail_addr21, 
	office_mail_city1, 
	office_mail_state1, 
	office_mail_zip1, 
	office_mail_zip41
	FROM EXP_detect_changes
	WHERE CHANGED_FLAG = 'NEW' or CHANGED_FLAG = 'UPDATE'
),
sup_cms_tin_office_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cms_tin_office
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, cms_rre_id, office_tin_num, office_code, office_name, office_mail_address1, office_mail_address2, office_mail_city, office_mail_state, office_mail_zip, office_mail_zip4)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	cms_rre_id1 AS CMS_RRE_ID, 
	office_tin_num1 AS OFFICE_TIN_NUM, 
	office_cd1 AS OFFICE_CODE, 
	office_name1 AS OFFICE_NAME, 
	office_mail_addr11 AS OFFICE_MAIL_ADDRESS1, 
	office_mail_addr21 AS OFFICE_MAIL_ADDRESS2, 
	office_mail_city1 AS OFFICE_MAIL_CITY, 
	office_mail_state1 AS OFFICE_MAIL_STATE, 
	office_mail_zip1 AS OFFICE_MAIL_ZIP, 
	office_mail_zip41 AS OFFICE_MAIL_ZIP4
	FROM FIL_sup_insurance_line_insert
),
SQ_sup_cms_tin_office AS (
	SELECT 
	a.sup_cms_tin_office_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.cms_rre_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cms_tin_office a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cms_tin_office b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.cms_rre_id = b.cms_rre_id
			GROUP BY cms_rre_id
			HAVING COUNT(*) > 1)
	ORDER BY cms_rre_id, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_cms_tin_office_id,
	cms_rre_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	cms_rre_id = v_Prev_row_cms_rre_id, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(
	    TRUE,
	    cms_rre_id = v_Prev_row_cms_rre_id, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	cms_rre_id AS v_Prev_row_cms_rre_id,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_cms_tin_office
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_cms_tin_office_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_insurance_line AS (
	SELECT
	sup_cms_tin_office_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_cms_tin_office_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cms_tin_office AS T
	USING UPD_sup_insurance_line AS S
	ON T.sup_cms_tin_office_id = S.sup_cms_tin_office_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),