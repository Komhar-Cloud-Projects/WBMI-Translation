WITH
SQ_application_context_stage AS (
	SELECT  
	      a.app_context_guid 
	      ,a.app_guid
	      ,a.app_context_ent_name
	      ,    a.display_name
	      ,    a.source_system_id
	  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.application_context_stage a
),
EXP_VALIDATE AS (
	SELECT
	app_context_guid AS IN_app_context_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_app_context_guid))) OR IS_SPACES(LTRIM(RTRIM(IN_app_context_guid))) OR LENGTH(LTRIM(RTRIM(IN_app_context_guid)))=0,'N/A' ,LTRIM(RTRIM(IN_app_context_guid)))
	IFF(LTRIM(RTRIM(IN_app_context_guid
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_app_context_guid
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_app_context_guid
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_app_context_guid
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_app_context_guid
			)
		)
	) AS app_context_guid,
	app_guid AS IN_app_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_app_guid))) OR IS_SPACES(LTRIM(RTRIM(IN_app_guid))) OR LENGTH(LTRIM(RTRIM(IN_app_guid)))=0,'N/A' ,LTRIM(RTRIM(IN_app_guid)))
	IFF(LTRIM(RTRIM(IN_app_guid
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_app_guid
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_app_guid
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_app_guid
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_app_guid
			)
		)
	) AS app_guid,
	app_context_ent_name AS IN_app_context_ent_name,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_app_context_ent_name))) OR IS_SPACES(LTRIM(RTRIM(IN_app_context_ent_name))) OR LENGTH(LTRIM(RTRIM(IN_app_context_ent_name)))=0,'N/A' ,LTRIM(RTRIM(IN_app_context_ent_name)))
	IFF(LTRIM(RTRIM(IN_app_context_ent_name
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_app_context_ent_name
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_app_context_ent_name
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_app_context_ent_name
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_app_context_ent_name
			)
		)
	) AS app_context_ent_name,
	display_name AS IN_display_name,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_display_name))) OR IS_SPACES(LTRIM(RTRIM(IN_display_name))) OR LENGTH(LTRIM(RTRIM(IN_display_name)))=0,'N/A' ,LTRIM(RTRIM(IN_display_name)))
	IFF(LTRIM(RTRIM(IN_display_name
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_display_name
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_display_name
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_display_name
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_display_name
			)
		)
	) AS display_name,
	source_system_id
	FROM SQ_application_context_stage
),
LKP_APPLICATION AS (
	SELECT
	app_guid,
	source_sys_id,
	app_ak_id,
	IN_app_guid,
	IN_source_sys_id
	FROM (
		select       
		       a.app_guid as app_guid,      
		       a.source_sys_id as source_sys_id ,
		       a.app_ak_id as app_ak_id        
		       from application a
		       where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY app_guid,source_sys_id ORDER BY app_guid DESC) = 1
),
LKP_APPLICATION_CONTEXT AS (
	SELECT
	source_sys_id,
	app_context_guid,
	app_context_ak_id,
	app_context_id,
	app_ak_id,
	app_context_entity_name,
	display_name
	FROM (
		SELECT 
		       a.source_sys_id as source_sys_id
		       ,a.app_context_guid as app_context_guid
		      ,a.app_context_ak_id  as app_context_ak_id 
		      ,a.app_context_id as app_context_id
		      ,a.app_ak_id as app_ak_id    
		      ,a.app_context_entity_name as app_context_entity_name
		      ,a.display_name as display_name
		  FROM  application_context a
		  where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY source_sys_id,app_context_guid ORDER BY source_sys_id DESC) = 1
),
EXP_DETECT_CHANGES1 AS (
	SELECT
	LKP_APPLICATION.app_ak_id AS IN_app_ak_id,
	-- *INF*: IIF(ISNULL(IN_app_ak_id),-1,IN_app_ak_id)
	IFF(IN_app_ak_id IS NULL,
		- 1,
		IN_app_ak_id
	) AS v_app_ak_id,
	v_app_ak_id AS app_ak_id,
	LKP_APPLICATION_CONTEXT.app_context_guid AS LKP_app_context_guid,
	LKP_APPLICATION_CONTEXT.app_context_ak_id AS LKP_app_context_ak_id,
	LKP_APPLICATION_CONTEXT.app_context_id AS LKP_app_context_id,
	LKP_APPLICATION_CONTEXT.app_ak_id AS LKP_app_ak_id,
	LKP_APPLICATION_CONTEXT.app_context_entity_name AS LKP_app_context_entity_name,
	LKP_APPLICATION_CONTEXT.display_name AS LKP_display_name,
	EXP_VALIDATE.app_context_guid,
	EXP_VALIDATE.app_guid,
	EXP_VALIDATE.app_context_ent_name AS app_context_entity_name,
	EXP_VALIDATE.display_name,
	-- *INF*: IIF(ISNULL(LKP_app_context_ak_id),'NEW',
	--      IIF(LKP_app_ak_id <> v_app_ak_id OR
	-- 
	-- 
	-- LTRIM(RTRIM( LKP_app_context_guid)) <> LTRIM(RTRIM(app_context_guid)) OR 
	-- 	LTRIM(RTRIM(LKP_display_name)) <> LTRIM(RTRIM(display_name)) OR 
	-- 	LTRIM(RTRIM(LKP_app_context_entity_name)) <> LTRIM(RTRIM(app_context_entity_name))     
	--      ,
	-- 	'UPDATE','NOCHANGE'))
	-- 
	IFF(LKP_app_context_ak_id IS NULL,
		'NEW',
		IFF(LKP_app_ak_id <> v_app_ak_id 
			OR LTRIM(RTRIM(LKP_app_context_guid
				)
			) <> LTRIM(RTRIM(app_context_guid
				)
			) 
			OR LTRIM(RTRIM(LKP_display_name
				)
			) <> LTRIM(RTRIM(display_name
				)
			) 
			OR LTRIM(RTRIM(LKP_app_context_entity_name
				)
			) <> LTRIM(RTRIM(app_context_entity_name
				)
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_changed_flag='NEW',TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_changed_flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		SYSDATE
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM:DD:YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM:DD:YYYY HH24:MI:SS'
	) AS default_date
	FROM EXP_VALIDATE
	LEFT JOIN LKP_APPLICATION
	ON LKP_APPLICATION.app_guid = EXP_VALIDATE.app_guid AND LKP_APPLICATION.source_sys_id = EXP_VALIDATE.source_system_id
	LEFT JOIN LKP_APPLICATION_CONTEXT
	ON LKP_APPLICATION_CONTEXT.source_sys_id = EXP_VALIDATE.source_system_id AND LKP_APPLICATION_CONTEXT.app_context_guid = EXP_VALIDATE.app_context_guid
),
FIL_INSERT AS (
	SELECT
	LKP_app_context_ak_id, 
	app_context_guid, 
	app_guid, 
	app_context_entity_name, 
	display_name, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	default_date, 
	app_ak_id
	FROM EXP_DETECT_CHANGES1
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
SEQ_APPLICATION_CONTEXT AS (
	CREATE SEQUENCE SEQ_APPLICATION_CONTEXT
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK11 AS (
	SELECT
	LKP_app_context_ak_id,
	-- *INF*: IIF(changed_flag ='NEW',NEXTVAL,LKP_app_context_ak_id)
	IFF(changed_flag = 'NEW',
		NEXTVAL,
		LKP_app_context_ak_id
	) AS app_context_ak_id,
	app_context_guid,
	app_guid,
	app_context_entity_name,
	display_name,
	changed_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	default_date,
	SEQ_APPLICATION_CONTEXT.NEXTVAL,
	app_ak_id
	FROM FIL_INSERT
),
application_context_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.application_context
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, app_context_ak_id, app_ak_id, app_context_guid, app_context_entity_name, display_name)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	APP_CONTEXT_AK_ID, 
	APP_AK_ID, 
	APP_CONTEXT_GUID, 
	APP_CONTEXT_ENTITY_NAME, 
	DISPLAY_NAME
	FROM EXP_Determine_AK11
),
SQ_application_context AS (
	SELECT 
	a.app_context_id, 
	a.eff_from_date, 
	a.eff_to_date ,
	a.app_context_guid 
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.application_context a 
	WHERE 
	a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	EXISTS(SELECT 1 
	                 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.application_context b     
	                 
	                 WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	                 and crrnt_snpsht_flag = 1
	                 AND a.app_context_guid = b.app_context_guid               
	 	           GROUP BY b.app_context_guid                       
	                 HAVING COUNT(*) >1) 
	ORDER BY a.app_context_guid , a.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	app_context_id,
	app_context_guid,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,app_context_guid=v_prev_row_app_context_guid,ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		app_context_guid = v_prev_row_app_context_guid, DATEADD(SECOND,- 1,v_prev_row_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	app_context_guid AS v_prev_row_app_context_guid,
	eff_from_date AS v_prev_row_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_application_context
),
FIL_Firstrow_INAKIDGROUP AS (
	SELECT
	app_context_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <>eff_to_date
),
UPD_APPLICATION_CONTEXT AS (
	SELECT
	app_context_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_Firstrow_INAKIDGROUP
),
application_context_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.application_context AS T
	USING UPD_APPLICATION_CONTEXT AS S
	ON T.app_context_id = S.app_context_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),