WITH
SQ_claim_quest_data_stage AS (
	SELECT  
	    a. claim_nbr
	      ,a.claimant_id      
	      , a.question_guid
	      ,a.optn_set_item_guid
	      ,a.optn_set_item_val
	      ,a.optn_text   
	    
	  FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_quest_data_stage a
),
EXP_VALIDATE AS (
	SELECT
	claim_nbr AS IN_claim_nbr,
	claimant_id AS IN_claimant_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_claim_nbr))) OR IS_SPACES(LTRIM(RTRIM(IN_claim_nbr))) OR LENGTH(LTRIM(RTRIM(IN_claim_nbr)))=0,'N/A',LTRIM(RTRIM(IN_claim_nbr)))
	IFF(LTRIM(RTRIM(IN_claim_nbr
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_claim_nbr
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_claim_nbr
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_claim_nbr
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_claim_nbr
			)
		)
	) AS v_claim_nbr,
	v_claim_nbr AS claim_nbr,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_claimant_id))) OR IS_SPACES(LTRIM(RTRIM(IN_claimant_id))) OR LENGTH(LTRIM(RTRIM(IN_claimant_id)))=0,'N/A',LTRIM(RTRIM(IN_claimant_id)))
	IFF(LTRIM(RTRIM(IN_claimant_id
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_claimant_id
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_claimant_id
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_claimant_id
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_claimant_id
			)
		)
	) AS v_client_id,
	v_client_id AS client_id,
	optn_set_item_guid AS IN_optn_set_item_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_optn_set_item_guid))) OR IS_SPACES(LTRIM(RTRIM(IN_optn_set_item_guid))) OR LENGTH(LTRIM(RTRIM(IN_optn_set_item_guid)))=0,'N/A' ,LTRIM(RTRIM(IN_optn_set_item_guid)))
	IFF(LTRIM(RTRIM(IN_optn_set_item_guid
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_optn_set_item_guid
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_optn_set_item_guid
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_optn_set_item_guid
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_optn_set_item_guid
			)
		)
	) AS optn_set_item_guid,
	optn_set_item_val AS IN_optn_set_item_val,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_optn_set_item_val))) OR IS_SPACES(LTRIM(RTRIM(IN_optn_set_item_val))) OR LENGTH(LTRIM(RTRIM(IN_optn_set_item_val)))=0,'N/A' ,LTRIM(RTRIM(IN_optn_set_item_val)))
	IFF(LTRIM(RTRIM(IN_optn_set_item_val
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_optn_set_item_val
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_optn_set_item_val
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_optn_set_item_val
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_optn_set_item_val
			)
		)
	) AS optn_set_item_val,
	optn_text AS IN_optn_text,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_optn_text))) OR IS_SPACES(LTRIM(RTRIM(IN_optn_text))) OR LENGTH(LTRIM(RTRIM(IN_optn_text)))=0,'N/A' ,LTRIM(RTRIM(IN_optn_text)))
	IFF(LTRIM(RTRIM(IN_optn_text
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_optn_text
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_optn_text
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_optn_text
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_optn_text
			)
		)
	) AS optn_text,
	question_guid AS IN_question_guid,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_question_guid))) OR IS_SPACES(LTRIM(RTRIM(IN_question_guid))) OR LENGTH(LTRIM(RTRIM(IN_question_guid)))=0,'N/A' ,LTRIM(RTRIM(IN_question_guid)))
	-- 
	-- 
	-- 
	IFF(LTRIM(RTRIM(IN_question_guid
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_question_guid
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_question_guid
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_question_guid
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_question_guid
			)
		)
	) AS question_guid
	FROM SQ_claim_quest_data_stage
),
LKP_CLAIM_OCCURRENCE_AK_IDS AS (
	SELECT
	claimant_num,
	claimant_id,
	claim_party_occurrence_ak_id,
	claim_occurrence_ak_id
	FROM (
		select
		   LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num,
		   LTRIM(RTRIM(CP.claim_party_key))  as claimant_id,
		     CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id , 
		CPO.claim_occurrence_ak_id as claim_occurrence_ak_id
		   FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  
		AND CP.claim_party_ak_id = CPO.claim_party_ak_id  
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND CP.source_sys_id =  '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND CPO.source_sys_id =  '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND CPO.claim_party_role_code = 'CLMT'  
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claimant_id ORDER BY claimant_num DESC) = 1
),
LKP_QUESTION AS (
	SELECT
	question_guid,
	question_ak_id
	FROM (
		select        
		         q.question_guid as question_guid, 
		         q.question_ak_id as question_ak_id         
		  FROM  question q
		  where q.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY question_guid ORDER BY question_guid DESC) = 1
),
EXP_LKP_VALUES1 AS (
	SELECT
	EXP_VALIDATE.claim_nbr,
	EXP_VALIDATE.client_id,
	EXP_VALIDATE.optn_set_item_guid,
	EXP_VALIDATE.optn_set_item_val,
	EXP_VALIDATE.optn_text,
	LKP_CLAIM_OCCURRENCE_AK_IDS.claim_party_occurrence_ak_id,
	LKP_CLAIM_OCCURRENCE_AK_IDS.claim_occurrence_ak_id,
	LKP_QUESTION.question_ak_id
	FROM EXP_VALIDATE
	LEFT JOIN LKP_CLAIM_OCCURRENCE_AK_IDS
	ON LKP_CLAIM_OCCURRENCE_AK_IDS.claimant_num = EXP_VALIDATE.claim_nbr AND LKP_CLAIM_OCCURRENCE_AK_IDS.claimant_id = EXP_VALIDATE.client_id
	LEFT JOIN LKP_QUESTION
	ON LKP_QUESTION.question_guid = EXP_VALIDATE.question_guid
),
LKP_CLAIM_ANSWER AS (
	SELECT
	claim_answer_ak_id,
	optn_set_item_guid,
	optn_set_item_val,
	optn_text,
	question_ak_id,
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id
	FROM (
		SELECT
		       a.claim_answer_ak_id as claim_answer_ak_id
		       ,a.question_ak_id as question_ak_id
		      ,a.claim_occurrence_ak_id as claim_occurrence_ak_id
		      ,a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		      ,a.optn_set_item_guid as optn_set_item_guid
		      ,a.optn_set_item_val as optn_set_item_val
		      ,a.optn_text as optn_text
		  FROM  claim_answer a
		  WHERE a.crrnt_snpsht_flag = 1
		  AND a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY question_ak_id,claim_occurrence_ak_id,claim_party_occurrence_ak_id ORDER BY claim_answer_ak_id DESC) = 1
),
EXP_DETECT_CHANGES1 AS (
	SELECT
	LKP_CLAIM_ANSWER.claim_answer_ak_id,
	LKP_CLAIM_ANSWER.optn_set_item_guid AS LKP_optn_set_item_guid,
	LKP_CLAIM_ANSWER.optn_set_item_val AS LKP_optn_set_item_val,
	LKP_CLAIM_ANSWER.optn_text AS LKP_optn_text,
	EXP_LKP_VALUES1.optn_set_item_guid,
	EXP_LKP_VALUES1.optn_set_item_val,
	EXP_LKP_VALUES1.optn_text,
	-- *INF*: IIF(ISNULL(claim_answer_ak_id),'NEW',
	--      IIF(LTRIM(RTRIM(LKP_optn_set_item_guid)) <> LTRIM(RTRIM(optn_set_item_guid)) OR 
	-- 	LTRIM(RTRIM(LKP_optn_set_item_val)) <> LTRIM(RTRIM(optn_set_item_val)) OR 
	-- 	LTRIM(RTRIM(LKP_optn_text)) <> LTRIM(RTRIM(optn_text))     ,
	-- 	'UPDATE','NOCHANGE'))
	-- 
	IFF(claim_answer_ak_id IS NULL,
		'NEW',
		IFF(LTRIM(RTRIM(LKP_optn_set_item_guid
				)
			) <> LTRIM(RTRIM(optn_set_item_guid
				)
			) 
			OR LTRIM(RTRIM(LKP_optn_set_item_val
				)
			) <> LTRIM(RTRIM(optn_set_item_val
				)
			) 
			OR LTRIM(RTRIM(LKP_optn_text
				)
			) <> LTRIM(RTRIM(optn_text
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
	) AS default_date,
	EXP_LKP_VALUES1.claim_party_occurrence_ak_id,
	EXP_LKP_VALUES1.claim_occurrence_ak_id,
	EXP_LKP_VALUES1.question_ak_id
	FROM EXP_LKP_VALUES1
	LEFT JOIN LKP_CLAIM_ANSWER
	ON LKP_CLAIM_ANSWER.question_ak_id = EXP_LKP_VALUES1.question_ak_id AND LKP_CLAIM_ANSWER.claim_occurrence_ak_id = EXP_LKP_VALUES1.claim_occurrence_ak_id AND LKP_CLAIM_ANSWER.claim_party_occurrence_ak_id = EXP_LKP_VALUES1.claim_party_occurrence_ak_id
),
FIL_INSERT1 AS (
	SELECT
	claim_answer_ak_id, 
	optn_set_item_guid, 
	optn_set_item_val, 
	optn_text, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	default_date, 
	claim_party_occurrence_ak_id, 
	claim_occurrence_ak_id, 
	question_ak_id
	FROM EXP_DETECT_CHANGES1
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
SEQ_CLAIM_ANSWER AS (
	CREATE SEQUENCE SEQ_CLAIM_ANSWER
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK1 AS (
	SELECT
	claim_answer_ak_id AS IN_claim_answer_ak_id,
	-- *INF*: IIF(changed_flag ='NEW',NEXTVAL,IN_claim_answer_ak_id)
	IFF(changed_flag = 'NEW',
		NEXTVAL,
		IN_claim_answer_ak_id
	) AS claim_answer_ak_id,
	optn_set_item_guid,
	optn_set_item_val,
	optn_text,
	changed_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	default_date,
	SEQ_CLAIM_ANSWER.NEXTVAL,
	question_ak_id,
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id
	FROM FIL_INSERT1
),
claim_answer_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_answer
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_answer_ak_id, question_ak_id, claim_occurrence_ak_id, claim_party_occurrence_ak_id, optn_set_item_guid, optn_set_item_val, optn_text)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	CLAIM_ANSWER_AK_ID, 
	QUESTION_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	OPTN_SET_ITEM_GUID, 
	OPTN_SET_ITEM_VAL, 
	OPTN_TEXT
	FROM EXP_Determine_AK1
),
SQ_claim_answer1 AS (
	SELECT 
	a.claim_answer_id,
	a.source_sys_id,
	a.claim_answer_ak_id,
	a.question_ak_id,
	a.claim_occurrence_ak_id,
	a.claim_party_occurrence_ak_id,
	a.optn_set_item_guid,
	a.optn_set_item_val,
	a.optn_text 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_answer a
	WHERE 
	a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	a.crrnt_snpsht_flag = 1 AND 
	(a.optn_text <> 'N/A'  OR a.optn_set_item_val <> 'N/A' ) --AND
	---a.audit_id  <>   '@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}'
),
EXP_Pass_Through AS (
	SELECT
	claim_answer_id,
	question_ak_id,
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id,
	source_sys_id,
	claim_answer_ak_id,
	optn_set_item_guid,
	optn_set_item_val,
	optn_text
	FROM SQ_claim_answer1
),
LKP_CLAIM_OCCURRENCE_AK_IDS2 AS (
	SELECT
	claimant_num,
	claimant_id,
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id
	FROM (
		select
		   LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num,
		   LTRIM(RTRIM(CP.claim_party_key))  as claimant_id,
		     CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id , 
		CPO.claim_occurrence_ak_id as claim_occurrence_ak_id
		   FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  
		AND CP.claim_party_ak_id = CPO.claim_party_ak_id  
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND CP.source_sys_id =  '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND CPO.source_sys_id =  '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND CPO.claim_party_role_code = 'CLMT'  
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_party_occurrence_ak_id ORDER BY claimant_num DESC) = 1
),
LKP_QUESTION2 AS (
	SELECT
	question_guid,
	question_ak_id
	FROM (
		select        
		         q.question_guid as question_guid, 
		         q.question_ak_id as question_ak_id         
		  FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.question q
		  where q.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY question_ak_id ORDER BY question_guid DESC) = 1
),
EXP_LKP_VALUES_STAGE AS (
	SELECT
	LKP_CLAIM_OCCURRENCE_AK_IDS2.claimant_num AS claim_nbr,
	LKP_CLAIM_OCCURRENCE_AK_IDS2.claimant_id,
	sysdate AS v_sysdate,
	'N/A' AS o_optn_set_item_val,
	LKP_QUESTION2.question_guid,
	v_sysdate AS o_created_date,
	v_sysdate AS o_modified_date,
	EXP_Pass_Through.claim_answer_id,
	EXP_Pass_Through.question_ak_id,
	EXP_Pass_Through.claim_occurrence_ak_id AS claim_occurrence_ak_id1,
	EXP_Pass_Through.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id1,
	EXP_Pass_Through.claim_answer_ak_id,
	EXP_Pass_Through.optn_set_item_guid,
	EXP_Pass_Through.optn_text,
	EXP_Pass_Through.source_sys_id,
	EXP_Pass_Through.optn_set_item_val
	FROM EXP_Pass_Through
	LEFT JOIN LKP_CLAIM_OCCURRENCE_AK_IDS2
	ON LKP_CLAIM_OCCURRENCE_AK_IDS2.claim_occurrence_ak_id = EXP_Pass_Through.claim_occurrence_ak_id AND LKP_CLAIM_OCCURRENCE_AK_IDS2.claim_party_occurrence_ak_id = EXP_Pass_Through.claim_party_occurrence_ak_id
	LEFT JOIN LKP_QUESTION2
	ON LKP_QUESTION2.question_ak_id = EXP_Pass_Through.question_ak_id
),
LKP_CLAIM_QUEST_DATA_STAGE AS (
	SELECT
	claim_quest_data_stage_id,
	claim_nbr,
	claimant_id,
	question_guid
	FROM (
		SELECT a.claim_quest_data_stage_id  as claim_quest_data_stage_id 
		       ,a.claim_nbr as claim_nbr
		      ,a.claimant_id as claimant_id
		      ,a.question_guid as question_guid 
		  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_quest_data_stage a --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_nbr,claimant_id,question_guid ORDER BY claim_quest_data_stage_id DESC) = 1
),
EXPTRANS1 AS (
	SELECT
	EXP_LKP_VALUES_STAGE.claim_answer_id,
	EXP_LKP_VALUES_STAGE.o_optn_set_item_val,
	EXP_LKP_VALUES_STAGE.o_created_date,
	EXP_LKP_VALUES_STAGE.o_modified_date,
	LKP_CLAIM_QUEST_DATA_STAGE.claim_quest_data_stage_id,
	sysdate AS v_sysdate,
	v_sysdate AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	v_sysdate AS eff_to_date_update,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	1 AS crrnt_snpsht_flag_1,
	0 AS crrnt_snpsht_flag_0,
	EXP_LKP_VALUES_STAGE.question_ak_id,
	EXP_LKP_VALUES_STAGE.claim_occurrence_ak_id1,
	EXP_LKP_VALUES_STAGE.claim_party_occurrence_ak_id1,
	EXP_LKP_VALUES_STAGE.claim_answer_ak_id,
	EXP_LKP_VALUES_STAGE.optn_set_item_guid,
	'N/A' AS o_optn_text,
	EXP_LKP_VALUES_STAGE.source_sys_id,
	EXP_LKP_VALUES_STAGE.optn_set_item_val,
	EXP_LKP_VALUES_STAGE.optn_text
	FROM EXP_LKP_VALUES_STAGE
	LEFT JOIN LKP_CLAIM_QUEST_DATA_STAGE
	ON LKP_CLAIM_QUEST_DATA_STAGE.claim_nbr = EXP_LKP_VALUES_STAGE.claim_nbr AND LKP_CLAIM_QUEST_DATA_STAGE.claimant_id = EXP_LKP_VALUES_STAGE.claimant_id AND LKP_CLAIM_QUEST_DATA_STAGE.question_guid = EXP_LKP_VALUES_STAGE.question_guid
),
FILT_DEL AS (
	SELECT
	claim_answer_id, 
	o_created_date AS created_date, 
	o_modified_date AS modified_date, 
	claim_quest_data_stage_id, 
	crrnt_snpsht_flag_1, 
	crrnt_snpsht_flag_0, 
	eff_to_date_update, 
	eff_from_date, 
	eff_to_date, 
	audit_id, 
	question_ak_id, 
	claim_occurrence_ak_id1, 
	claim_party_occurrence_ak_id1 AS claim_party_occurrence_ak_id11, 
	claim_answer_ak_id, 
	optn_set_item_guid, 
	o_optn_text AS optn_text, 
	source_sys_id, 
	optn_set_item_val, 
	optn_text AS optn_text1
	FROM EXPTRANS1
	WHERE ISNULL(claim_quest_data_stage_id)
),
EXP_Determine_AK11 AS (
	SELECT
	claim_answer_ak_id,
	optn_set_item_guid,
	optn_set_item_val,
	optn_text,
	crrnt_snpsht_flag_1 AS crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	question_ak_id,
	claim_occurrence_ak_id1 AS claim_occurrence_ak_id,
	claim_party_occurrence_ak_id11 AS claim_party_occurrence_ak_id,
	'N/A' AS o_default_NA
	FROM FILT_DEL
),
claim_answer_upd_del AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_answer
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_answer_ak_id, question_ak_id, claim_occurrence_ak_id, claim_party_occurrence_ak_id, optn_set_item_guid, optn_set_item_val, optn_text)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	CLAIM_ANSWER_AK_ID, 
	QUESTION_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	OPTN_SET_ITEM_GUID, 
	o_default_NA AS OPTN_SET_ITEM_VAL, 
	o_default_NA AS OPTN_TEXT
	FROM EXP_Determine_AK11
),
SQ_claim_answer AS (
	SELECT 
	a.claim_answer_id, 
	a.eff_from_date, 
	a.eff_to_date ,
	a.question_ak_id,
	a.claim_occurrence_ak_id,
	a.claim_party_occurrence_ak_id
	 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_answer a 
	WHERE 
	a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	EXISTS(SELECT 1 
	                 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_answer b               
	                 WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	                 AND a.question_ak_id = b.question_ak_id
	                 AND a.claim_occurrence_ak_id = b.claim_occurrence_ak_id
	                 AND a.claim_party_occurrence_ak_id = b.claim_party_occurrence_ak_id
	 	           GROUP BY b.question_ak_id,
	                        b.claim_occurrence_ak_id,
	                         b.claim_party_occurrence_ak_id
	                 HAVING COUNT(*) >1) 
	ORDER BY a.question_ak_id,
	a.claim_occurrence_ak_id,
	a.claim_party_occurrence_ak_id, a.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_answer_id,
	question_ak_id,
	claim_occurrence_ak_id AS claim_occurence_ak_id,
	claim_party_occurrence_ak_id AS claim_party_ccurrence_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE, question_ak_id=v_prev_row_question_ak_id AND claim_occurence_ak_id = v_prev_row_claim_occurrence_ak_id AND claim_party_ccurrence_ak_id = v_prev_row_claim_party_ccurrence_ak_id,ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		question_ak_id = v_prev_row_question_ak_id 
		AND claim_occurence_ak_id = v_prev_row_claim_occurrence_ak_id 
		AND claim_party_ccurrence_ak_id = v_prev_row_claim_party_ccurrence_ak_id, DATEADD(SECOND,- 1,v_prev_row_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	question_ak_id AS v_prev_row_question_ak_id,
	claim_occurence_ak_id AS v_prev_row_claim_occurrence_ak_id,
	claim_party_ccurrence_ak_id AS v_prev_row_claim_party_ccurrence_ak_id,
	eff_from_date AS v_prev_row_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_claim_answer
),
FIL_Firstrow_INAKIDGROUP AS (
	SELECT
	claim_answer_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <>eff_to_date
),
UPD_CLAIM_ANSWER AS (
	SELECT
	claim_answer_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_Firstrow_INAKIDGROUP
),
claim_answer_UPD AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_answer AS T
	USING UPD_CLAIM_ANSWER AS S
	ON T.claim_answer_id = S.claim_answer_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),