WITH
SQ_question_stage AS (
	SELECT  
	      question_guid
	      , optn_set_guid
	       ,app_context_guid    
	      ,app_context_grp_guid
	      ,display_name
	      ,logical_name
	      ,published_to_prod_flag
	      ,enabled_flag
	      ,help_text
	      ,prompt
	      ,notes
	      ,surrogate_question_guid      
	      ,eff_date
	      ,exp_date
	       ,source_system_id      
	  FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.question_stage
),
EXP_VALIDATE AS (
	SELECT
	question_guid AS IN_question_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_question_guid))) OR IS_SPACES(LTRIM(RTRIM(IN_question_guid))) OR LENGTH(LTRIM(RTRIM(IN_question_guid)))=0,'N/A' ,LTRIM(RTRIM(IN_question_guid)))
	IFF(
	    LTRIM(RTRIM(IN_question_guid)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_question_guid)))>0
	    and TRIM(LTRIM(RTRIM(IN_question_guid)))=''
	    or LENGTH(LTRIM(RTRIM(IN_question_guid))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_question_guid))
	) AS question_guid,
	optn_set_guid AS IN_optn_set_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_optn_set_guid))) OR IS_SPACES(LTRIM(RTRIM(IN_optn_set_guid))) OR LENGTH(LTRIM(RTRIM(IN_optn_set_guid)))=0,'N/A' ,LTRIM(RTRIM(IN_optn_set_guid)))
	IFF(
	    LTRIM(RTRIM(IN_optn_set_guid)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_optn_set_guid)))>0
	    and TRIM(LTRIM(RTRIM(IN_optn_set_guid)))=''
	    or LENGTH(LTRIM(RTRIM(IN_optn_set_guid))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_optn_set_guid))
	) AS optn_set_guid,
	app_context_guid AS IN_app_context_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_app_context_guid))) OR IS_SPACES(LTRIM(RTRIM(IN_app_context_guid))) OR LENGTH(LTRIM(RTRIM(IN_app_context_guid)))=0,'N/A' ,LTRIM(RTRIM(IN_app_context_guid)))
	IFF(
	    LTRIM(RTRIM(IN_app_context_guid)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_app_context_guid)))>0
	    and TRIM(LTRIM(RTRIM(IN_app_context_guid)))=''
	    or LENGTH(LTRIM(RTRIM(IN_app_context_guid))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_app_context_guid))
	) AS app_context_guid,
	app_context_grp_guid AS IN_app_context_group_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_app_context_group_guid))) OR IS_SPACES(LTRIM(RTRIM(IN_app_context_group_guid))) OR LENGTH(LTRIM(RTRIM(IN_app_context_group_guid)))=0,'N/A' ,LTRIM(RTRIM(IN_app_context_group_guid)))
	IFF(
	    LTRIM(RTRIM(IN_app_context_group_guid)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_app_context_group_guid)))>0
	    and TRIM(LTRIM(RTRIM(IN_app_context_group_guid)))=''
	    or LENGTH(LTRIM(RTRIM(IN_app_context_group_guid))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_app_context_group_guid))
	) AS app_context_group_guid,
	display_name AS IN_display_name,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_display_name))) OR IS_SPACES(LTRIM(RTRIM(IN_display_name))) OR LENGTH(LTRIM(RTRIM(IN_display_name)))=0,'N/A' ,LTRIM(RTRIM(IN_display_name)))
	IFF(
	    LTRIM(RTRIM(IN_display_name)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_display_name)))>0
	    and TRIM(LTRIM(RTRIM(IN_display_name)))=''
	    or LENGTH(LTRIM(RTRIM(IN_display_name))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_display_name))
	) AS display_name,
	logical_name AS IN_logical_name,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_logical_name))) OR IS_SPACES(LTRIM(RTRIM(IN_logical_name))) OR LENGTH(LTRIM(RTRIM(IN_logical_name)))=0,'N/A' ,LTRIM(RTRIM(IN_logical_name)))
	IFF(
	    LTRIM(RTRIM(IN_logical_name)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_logical_name)))>0
	    and TRIM(LTRIM(RTRIM(IN_logical_name)))=''
	    or LENGTH(LTRIM(RTRIM(IN_logical_name))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_logical_name))
	) AS logical_name,
	published_to_prod_flag AS IN_published_to_prod_flag,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_published_to_prod_flag))) OR IS_SPACES(LTRIM(RTRIM(IN_published_to_prod_flag))) OR LENGTH(LTRIM(RTRIM(IN_published_to_prod_flag)))=0,' ' ,LTRIM(RTRIM(IN_published_to_prod_flag)))
	IFF(
	    LTRIM(RTRIM(IN_published_to_prod_flag)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_published_to_prod_flag)))>0
	    and TRIM(LTRIM(RTRIM(IN_published_to_prod_flag)))=''
	    or LENGTH(LTRIM(RTRIM(IN_published_to_prod_flag))) = 0,
	    ' ',
	    LTRIM(RTRIM(IN_published_to_prod_flag))
	) AS published_to_prod_flag,
	enabled_flag AS IN_enabled_flag,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_enabled_flag))) OR IS_SPACES(LTRIM(RTRIM(IN_enabled_flag))) OR LENGTH(LTRIM(RTRIM(IN_enabled_flag)))=0,' ' ,LTRIM(RTRIM(IN_enabled_flag)))
	IFF(
	    LTRIM(RTRIM(IN_enabled_flag)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_enabled_flag)))>0
	    and TRIM(LTRIM(RTRIM(IN_enabled_flag)))=''
	    or LENGTH(LTRIM(RTRIM(IN_enabled_flag))) = 0,
	    ' ',
	    LTRIM(RTRIM(IN_enabled_flag))
	) AS enabled_flag,
	help_text AS IN_help_text,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_help_text))) OR IS_SPACES(LTRIM(RTRIM(IN_help_text))) OR LENGTH(LTRIM(RTRIM(IN_help_text)))=0,'N/A' ,LTRIM(RTRIM(IN_help_text)))
	IFF(
	    LTRIM(RTRIM(IN_help_text)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_help_text)))>0
	    and TRIM(LTRIM(RTRIM(IN_help_text)))=''
	    or LENGTH(LTRIM(RTRIM(IN_help_text))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_help_text))
	) AS help_text,
	prompt AS IN_prompt,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_prompt))) OR IS_SPACES(LTRIM(RTRIM(IN_prompt))) OR LENGTH(LTRIM(RTRIM(IN_prompt)))=0,'N/A' ,LTRIM(RTRIM(IN_prompt)))
	IFF(
	    LTRIM(RTRIM(IN_prompt)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_prompt)))>0
	    and TRIM(LTRIM(RTRIM(IN_prompt)))=''
	    or LENGTH(LTRIM(RTRIM(IN_prompt))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_prompt))
	) AS prompt,
	notes AS IN_notes,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_notes))) OR IS_SPACES(LTRIM(RTRIM(IN_notes))) OR LENGTH(LTRIM(RTRIM(IN_notes)))=0,'N/A' ,LTRIM(RTRIM(IN_notes)))
	IFF(
	    LTRIM(RTRIM(IN_notes)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_notes)))>0
	    and TRIM(LTRIM(RTRIM(IN_notes)))=''
	    or LENGTH(LTRIM(RTRIM(IN_notes))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_notes))
	) AS notes,
	surrogate_question_guid AS IN_surrogate_question_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_surrogate_question_guid))) OR IS_SPACES(LTRIM(RTRIM(IN_surrogate_question_guid))) OR LENGTH(LTRIM(RTRIM(IN_surrogate_question_guid)))=0,'N/A' ,LTRIM(RTRIM(IN_surrogate_question_guid)))
	IFF(
	    LTRIM(RTRIM(IN_surrogate_question_guid)) IS NULL
	    or LENGTH(LTRIM(RTRIM(IN_surrogate_question_guid)))>0
	    and TRIM(LTRIM(RTRIM(IN_surrogate_question_guid)))=''
	    or LENGTH(LTRIM(RTRIM(IN_surrogate_question_guid))) = 0,
	    'N/A',
	    LTRIM(RTRIM(IN_surrogate_question_guid))
	) AS surrogate_question_guid,
	eff_date AS IN_eff_date,
	-- *INF*: IIF(ISNULL(IN_eff_date) ,TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') ,IN_eff_date)
	IFF(
	    IN_eff_date IS NULL, TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    IN_eff_date
	) AS eff_date,
	exp_date AS IN_exp_date,
	-- *INF*: IIF(ISNULL(IN_exp_date) ,TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') ,IN_exp_date)
	IFF(
	    IN_exp_date IS NULL, TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    IN_exp_date
	) AS exp_date,
	source_system_id
	FROM SQ_question_stage
),
LKP_APP_CONTEXT AS (
	SELECT
	app_context_ak_id,
	app_context_guid
	FROM (
		select  
		         q.app_context_ak_id as app_context_ak_id,
		         q.app_context_guid as app_context_guid
		  FROM  application_context q
		  where q.crrnt_snpsht_flag = 1
		  and q.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY app_context_guid ORDER BY app_context_ak_id DESC) = 1
),
LKP_QUESTION AS (
	SELECT
	question_ak_id,
	question_guid,
	eff_date,
	optn_set_guid,
	display_name,
	logical_name,
	published_to_prod_flag,
	enabled_flag,
	help_text,
	prompt,
	notes,
	surrogate_question_guid,
	exp_date,
	app_context_ak_id,
	app_context_group_guid
	FROM (
		select  q.question_ak_id as question_ak_id ,
		         q.eff_date as eff_date,
		         q.question_guid as question_guid, 
		           q.optn_set_guid  as optn_set_guid  ,    
		          q.display_name as display_name,
		          q.logical_name as logical_name,
		           q.published_to_prod_flag as published_to_prod_flag  ,
		           q.enabled_flag as enabled_flag,
		         q.help_text as help_text,
		          q.prompt as prompt,
		           q.notes as notes,
		           q.surrogate_question_guid            as surrogate_question_guid,
		           q.exp_date as exp_date,   
		            q.app_context_ak_id as app_context_ak_id       ,
		             q.app_context_group_guid as app_context_group_guid     
		  FROM  question q
		  where q.crrnt_snpsht_flag = 1
		  and q.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY question_guid ORDER BY question_ak_id DESC) = 1
),
EXP_DETECT_CHANGES AS (
	SELECT
	LKP_QUESTION.question_ak_id AS LKP_question_ak_id,
	EXP_VALIDATE.question_guid,
	EXP_VALIDATE.optn_set_guid,
	LKP_APP_CONTEXT.app_context_ak_id AS IN_app_context_ak_id,
	-- *INF*: IIF(ISNULL(IN_app_context_ak_id),-1,IN_app_context_ak_id)
	--  
	--  
	IFF(IN_app_context_ak_id IS NULL, - 1, IN_app_context_ak_id) AS v_app_context_ak_id,
	v_app_context_ak_id AS app_context_ak_id,
	EXP_VALIDATE.app_context_group_guid,
	EXP_VALIDATE.display_name,
	EXP_VALIDATE.logical_name,
	EXP_VALIDATE.published_to_prod_flag,
	EXP_VALIDATE.enabled_flag,
	EXP_VALIDATE.help_text,
	EXP_VALIDATE.prompt,
	EXP_VALIDATE.notes,
	EXP_VALIDATE.surrogate_question_guid,
	EXP_VALIDATE.eff_date,
	EXP_VALIDATE.exp_date,
	LKP_QUESTION.eff_date AS LKP_eff_date,
	LKP_QUESTION.optn_set_guid AS LKP_optn_set_guid,
	LKP_QUESTION.display_name AS LKP_display_name,
	LKP_QUESTION.logical_name AS LKP_logical_name,
	LKP_QUESTION.published_to_prod_flag AS LKP_published_to_prod_flag,
	LKP_QUESTION.enabled_flag AS LKP_enabled_flag,
	LKP_QUESTION.help_text AS LKP_help_text,
	LKP_QUESTION.prompt AS LKP_prompt,
	LKP_QUESTION.notes AS LKP_notes,
	LKP_QUESTION.surrogate_question_guid AS LKP_surrogate_question_guid,
	LKP_QUESTION.exp_date AS LKP_exp_date,
	LKP_QUESTION.app_context_ak_id AS LKP_app_context_ak_id,
	LKP_QUESTION.app_context_group_guid AS LKP_app_context_group_guid,
	-- *INF*: IIF(ISNULL(LKP_question_ak_id),'NEW',
	--      IIF(LTRIM(RTRIM(optn_set_guid)) <> LTRIM(RTRIM(LKP_optn_set_guid)) OR 
	-- 	LTRIM(RTRIM(display_name)) <> LTRIM(RTRIM(LKP_display_name)) OR 
	-- 	LTRIM(RTRIM(logical_name)) <> LTRIM(RTRIM(LKP_logical_name)) OR 
	-- LTRIM(RTRIM(published_to_prod_flag)) <> LTRIM(RTRIM(LKP_published_to_prod_flag)) OR 
	-- 	exp_date <> LKP_exp_date OR 
	-- eff_date <> LKP_eff_date OR 
	-- 	LTRIM(RTRIM(enabled_flag)) <> LTRIM(RTRIM(LKP_enabled_flag)) OR 
	-- 	LTRIM(RTRIM(help_text)) <> LTRIM(RTRIM(LKP_help_text)) OR 
	-- 	LTRIM(RTRIM(prompt)) <> LTRIM(RTRIM(LKP_prompt)) OR 
	-- v_app_context_ak_id <> LKP_app_context_ak_id  OR 
	-- 	LTRIM(RTRIM(notes)) <> LTRIM(RTRIM(LKP_notes)) OR  
	-- 	LTRIM(RTRIM(app_context_group_guid)) <> LTRIM(RTRIM(LKP_app_context_group_guid)) 
	-- OR
	-- 	LTRIM(RTRIM(surrogate_question_guid)) <> LTRIM(RTRIM(LKP_surrogate_question_guid) ) ,
	-- 	'UPDATE','NOCHANGE'))
	-- 
	IFF(
	    LKP_question_ak_id IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(optn_set_guid)) <> LTRIM(RTRIM(LKP_optn_set_guid))
	        or LTRIM(RTRIM(display_name)) <> LTRIM(RTRIM(LKP_display_name))
	        or LTRIM(RTRIM(logical_name)) <> LTRIM(RTRIM(LKP_logical_name))
	        or LTRIM(RTRIM(published_to_prod_flag)) <> LTRIM(RTRIM(LKP_published_to_prod_flag))
	        or exp_date <> LKP_exp_date
	        or eff_date <> LKP_eff_date
	        or LTRIM(RTRIM(enabled_flag)) <> LTRIM(RTRIM(LKP_enabled_flag))
	        or LTRIM(RTRIM(help_text)) <> LTRIM(RTRIM(LKP_help_text))
	        or LTRIM(RTRIM(prompt)) <> LTRIM(RTRIM(LKP_prompt))
	        or v_app_context_ak_id <> LKP_app_context_ak_id
	        or LTRIM(RTRIM(notes)) <> LTRIM(RTRIM(LKP_notes))
	        or LTRIM(RTRIM(app_context_group_guid)) <> LTRIM(RTRIM(LKP_app_context_group_guid))
	        or LTRIM(RTRIM(surrogate_question_guid)) <> LTRIM(RTRIM(LKP_surrogate_question_guid)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_changed_flag='NEW',TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM:DD:YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:00', 'MM:DD:YYYY HH24:MI:SS') AS default_date
	FROM EXP_VALIDATE
	LEFT JOIN LKP_APP_CONTEXT
	ON LKP_APP_CONTEXT.app_context_guid = EXP_VALIDATE.app_context_guid
	LEFT JOIN LKP_QUESTION
	ON LKP_QUESTION.question_guid = EXP_VALIDATE.question_guid
),
FIL_INSERT AS (
	SELECT
	LKP_question_ak_id AS LKP_questoin_ak_id, 
	changed_flag, 
	app_context_ak_id, 
	question_guid, 
	optn_set_guid, 
	app_context_group_guid, 
	display_name, 
	logical_name, 
	published_to_prod_flag, 
	enabled_flag, 
	help_text, 
	prompt, 
	notes, 
	surrogate_question_guid, 
	eff_date, 
	exp_date, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	default_date
	FROM EXP_DETECT_CHANGES
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
SEQ_Question_AK_ID AS (
	CREATE SEQUENCE SEQ_Question_AK_ID
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK1 AS (
	SELECT
	LKP_questoin_ak_id AS LKP_question_ak_id,
	-- *INF*: IIF(changed_flag ='NEW',NEXTVAL,LKP_question_ak_id)
	IFF(changed_flag = 'NEW', NEXTVAL, LKP_question_ak_id) AS question_ak_id,
	app_context_ak_id,
	question_guid,
	optn_set_guid,
	app_context_group_guid,
	display_name,
	logical_name,
	published_to_prod_flag,
	enabled_flag,
	help_text,
	prompt,
	notes,
	surrogate_question_guid,
	eff_date,
	exp_date,
	changed_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	default_date,
	SEQ_Question_AK_ID.NEXTVAL
	FROM FIL_INSERT
),
question_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.question
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, question_ak_id, app_context_ak_id, question_guid, optn_set_guid, app_context_group_guid, display_name, logical_name, published_to_prod_flag, enabled_flag, help_text, prompt, notes, surrogate_question_guid, eff_date, exp_date)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	QUESTION_AK_ID, 
	APP_CONTEXT_AK_ID, 
	QUESTION_GUID, 
	OPTN_SET_GUID, 
	APP_CONTEXT_GROUP_GUID, 
	DISPLAY_NAME, 
	LOGICAL_NAME, 
	PUBLISHED_TO_PROD_FLAG, 
	ENABLED_FLAG, 
	HELP_TEXT, 
	PROMPT, 
	NOTES, 
	SURROGATE_QUESTION_GUID, 
	EFF_DATE, 
	EXP_DATE
	FROM EXP_Determine_AK1
),
SQ_question AS (
	SELECT 
	a.question_id, 
	a.eff_from_date, 
	a.eff_to_date ,
	a.question_guid
	 
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.question a 
	WHERE 
	a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	EXISTS(SELECT 1 
	                 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.question b                      
	                 WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	                 and crrnt_snpsht_flag = 1
	                 AND a.question_guid = b.question_guid                                     
	 	           GROUP BY b.question_guid                 
	                 HAVING COUNT(*) >1) 
	ORDER BY a.question_guid , a.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	question_id,
	question_guid AS question_quid,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE, question_quid=v_prev_row_question_guid  ,ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(
	    TRUE,
	    question_quid = v_prev_row_question_guid, DATEADD(SECOND,- 1,v_prev_row_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	question_quid AS v_prev_row_question_guid,
	eff_from_date AS v_prev_row_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_question
),
FIL_Firstrow_INAKIDGROUP AS (
	SELECT
	question_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <>eff_to_date
),
UPD_QUESTION AS (
	SELECT
	question_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_Firstrow_INAKIDGROUP
),
question_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.question AS T
	USING UPD_QUESTION AS S
	ON T.question_id = S.question_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),