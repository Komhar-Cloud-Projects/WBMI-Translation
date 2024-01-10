WITH
SQ_application_stage AS (
	SELECT  app_guid 
	      ,display_name
	      ,published_to_prod_flag
	      ,enabled_flag
	      ,version_num
	, eff_date
	,exp_date
	      ,source_system_id
	  FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.application_stage
),
EXP_VALIDATE AS (
	SELECT
	app_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(app_guid))) OR IS_SPACES(LTRIM(RTRIM(app_guid))) OR LENGTH(LTRIM(RTRIM(app_guid)))=0,'N/A',LTRIM(RTRIM(app_guid)))
	-- 
	-- 
	IFF(LTRIM(RTRIM(app_guid)) IS NULL OR IS_SPACES(LTRIM(RTRIM(app_guid))) OR LENGTH(LTRIM(RTRIM(app_guid))) = 0, 'N/A', LTRIM(RTRIM(app_guid))) AS v_app_guid,
	v_app_guid AS o_app_guid,
	display_name,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(display_name))) OR IS_SPACES(LTRIM(RTRIM(display_name))) OR LENGTH(LTRIM(RTRIM(display_name)))=0,'N/A',LTRIM(RTRIM(display_name)))
	-- 
	-- 
	IFF(LTRIM(RTRIM(display_name)) IS NULL OR IS_SPACES(LTRIM(RTRIM(display_name))) OR LENGTH(LTRIM(RTRIM(display_name))) = 0, 'N/A', LTRIM(RTRIM(display_name))) AS v_display_name,
	v_display_name AS o_display_name,
	published_to_prod_flag,
	-- *INF*: IIF(ISNULL(published_to_prod_flag),' ',published_to_prod_flag)
	IFF(published_to_prod_flag IS NULL, ' ', published_to_prod_flag) AS v_published_to_prod_flag,
	v_published_to_prod_flag AS o_published_to_prod_flag,
	enabled_flag,
	-- *INF*: IIF(ISNULL(enabled_flag),' ',enabled_flag)
	IFF(enabled_flag IS NULL, ' ', enabled_flag) AS v_enabled_flag,
	v_enabled_flag AS o_enabled_flag,
	version_num,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(version_num))) OR IS_SPACES(LTRIM(RTRIM(version_num))) OR LENGTH(LTRIM(RTRIM(version_num)))=0,'N/A',LTRIM(RTRIM(version_num)))
	IFF(LTRIM(RTRIM(version_num)) IS NULL OR IS_SPACES(LTRIM(RTRIM(version_num))) OR LENGTH(LTRIM(RTRIM(version_num))) = 0, 'N/A', LTRIM(RTRIM(version_num))) AS v_version_num,
	v_version_num AS o_version_num,
	source_system_id,
	eff_date,
	-- *INF*: IIF(ISNULL(eff_date),TO_DATE('1/1/1800','MM/DD/YYYY'),eff_date)
	IFF(eff_date IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), eff_date) AS v_eff_date,
	v_eff_date AS o_eff_date,
	exp_date,
	-- *INF*: IIF(ISNULL(exp_date),TO_DATE('1/1/1800','MM/DD/YYYY'),exp_date)
	IFF(exp_date IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), exp_date) AS v_exp_date,
	v_exp_date AS o_exp_date
	FROM SQ_application_stage
),
LKP_APPLICATION AS (
	SELECT
	app_id,
	app_ak_id,
	display_name,
	app_guid,
	published_to_prod_flag,
	enabled_flag,
	version_num,
	eff_date,
	exp_date,
	source_sys_id
	FROM (
		select a.app_id as app_id,
		       a.app_ak_Id as app_ak_id,
		       a.app_guid as app_guid,
		       a.display_name as display_name,
		       a.published_to_prod_flag as published_to_prod_flag,
		       a.enabled_flag as enabled_flag,
		       a.version_num as version_num,
		       a.source_sys_id as source_sys_id ,
		       a.eff_date as eff_date,
		       a.exp_date as exp_date
		       from @{pipeline().parameters.TARGET_TABLE_OWNER}.application a
		       where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY app_guid,source_sys_id ORDER BY app_id DESC) = 1
),
EXP_DETECT_CHANGES AS (
	SELECT
	LKP_APPLICATION.app_id AS LKP_app_id,
	LKP_APPLICATION.app_ak_id AS LKP_app_ak_id,
	LKP_APPLICATION.display_name AS LKP_display_name,
	LKP_APPLICATION.app_guid AS LKP_app_guid,
	LKP_APPLICATION.published_to_prod_flag AS LKP_published_to_prod_flag,
	LKP_APPLICATION.enabled_flag AS LKP_enabled_flag,
	LKP_APPLICATION.version_num AS LKP_version_num,
	LKP_APPLICATION.eff_date AS LKP_eff_date,
	LKP_APPLICATION.exp_date AS LKP_exp_date,
	EXP_VALIDATE.o_app_guid,
	EXP_VALIDATE.o_display_name,
	EXP_VALIDATE.o_published_to_prod_flag,
	EXP_VALIDATE.o_enabled_flag,
	EXP_VALIDATE.o_version_num,
	-- *INF*: IIF(ISNULL(LKP_app_id),'NEW',
	--      IIF(LTRIM(RTRIM(o_display_name)) <> LTRIM(RTRIM(LKP_display_name)) OR 
	-- 	LTRIM(RTRIM(o_published_to_prod_flag)) <> LTRIM(RTRIM(LKP_published_to_prod_flag)) OR 
	-- 	LTRIM(RTRIM(o_enabled_flag)) <> LTRIM(RTRIM(LKP_enabled_flag)) OR 
	-- 	LTRIM(RTRIM(o_version_num)) <> LTRIM(RTRIM(LKP_version_num))    OR LKP_eff_date <> o_eff_date  OR LKP_exp_date <> o_exp_date ,
	-- 	'UPDATE','NOCHANGE'))
	IFF(LKP_app_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(o_display_name)) <> LTRIM(RTRIM(LKP_display_name)) OR LTRIM(RTRIM(o_published_to_prod_flag)) <> LTRIM(RTRIM(LKP_published_to_prod_flag)) OR LTRIM(RTRIM(o_enabled_flag)) <> LTRIM(RTRIM(LKP_enabled_flag)) OR LTRIM(RTRIM(o_version_num)) <> LTRIM(RTRIM(LKP_version_num)) OR LKP_eff_date <> o_eff_date OR LKP_exp_date <> o_exp_date, 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	sysdate AS created_date,
	sysdate AS modified_date,
	-- *INF*: IIF(v_changed_flag='NEW',TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	EXP_VALIDATE.o_eff_date,
	EXP_VALIDATE.o_exp_date
	FROM EXP_VALIDATE
	LEFT JOIN LKP_APPLICATION
	ON LKP_APPLICATION.app_guid = EXP_VALIDATE.o_app_guid AND LKP_APPLICATION.source_sys_id = EXP_VALIDATE.source_system_id
),
FIL_INSERT1 AS (
	SELECT
	LKP_app_ak_id AS old_app_ak_id, 
	o_app_guid AS app_guid, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	modified_date AS default_date, 
	o_app_guid, 
	o_display_name, 
	o_published_to_prod_flag, 
	o_enabled_flag, 
	o_version_num, 
	o_eff_date, 
	o_exp_date
	FROM EXP_DETECT_CHANGES
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
SEQ_APPLICATION AS (
	CREATE SEQUENCE SEQ_APPLICATION
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	old_app_ak_id,
	-- *INF*: IIF(changed_flag ='NEW',NEXTVAL,old_app_ak_id)
	IFF(changed_flag = 'NEW', NEXTVAL, old_app_ak_id) AS app_ak_id,
	changed_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	default_date,
	SEQ_APPLICATION.NEXTVAL,
	o_app_guid,
	o_display_name,
	o_published_to_prod_flag,
	o_enabled_flag,
	o_version_num,
	o_eff_date,
	o_exp_date
	FROM FIL_INSERT1
),
application_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.application
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, app_ak_id, app_guid, display_name, published_to_prod_flag, enabled_flag, version_num, eff_date, exp_date)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	APP_AK_ID, 
	o_app_guid AS APP_GUID, 
	o_display_name AS DISPLAY_NAME, 
	o_published_to_prod_flag AS PUBLISHED_TO_PROD_FLAG, 
	o_enabled_flag AS ENABLED_FLAG, 
	o_version_num AS VERSION_NUM, 
	o_eff_date AS EFF_DATE, 
	o_exp_date AS EXP_DATE
	FROM EXP_Determine_AK
),
SQ_application AS (
	SELECT 
	a.app_id, 
	a.eff_from_date, 
	a.eff_to_date ,
	a.app_guid 
	
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.application a 
	WHERE 
	a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	EXISTS(SELECT 1 
	                 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.application b     
	                 
	                 WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	                 and crrnt_snpsht_flag = 1
	                 AND a.app_guid = b.app_guid     
	 	           GROUP BY b.app_guid               
	                 HAVING COUNT(*) >1) 
	ORDER BY a.app_guid , a.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	app_id,
	app_guid,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,app_guid=v_prev_row_app_guid  ,ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		app_guid = v_prev_row_app_guid, ADD_TO_DATE(v_prev_row_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	app_guid AS v_prev_row_app_guid,
	eff_from_date AS v_prev_row_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_application
),
FIL_Firstrow_INAKIDGROUP1 AS (
	SELECT
	app_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <>eff_to_date
),
UPD_APPLICATION AS (
	SELECT
	app_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_Firstrow_INAKIDGROUP1
),
application_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.application AS T
	USING UPD_APPLICATION AS S
	ON T.app_id = S.app_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),