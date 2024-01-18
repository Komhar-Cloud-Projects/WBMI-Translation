WITH
LKP_CLAIM_OCCURRENCE AS (
	SELECT
	claim_occurrence_ak_id,
	claim_occurrence_key
	FROM (
		SELECT 
		   claim_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		   claim_occurrence.claim_occurrence_key as claim_occurrence_key 
		FROM 
		   claim_occurrence
		WHERE
		   source_sys_id = '@{pipeline().parameters.SOURCE_SYS_ID}' AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_ak_id) = 1
),
LKP_CLAIM_PARTY AS (
	SELECT
	claim_party_ak_id,
	claim_party_key
	FROM (
		SELECT 
		   claim_party.claim_party_ak_id as claim_party_ak_id, 
		   claim_party.claim_party_key as claim_party_key 
		FROM 
		   claim_party
		WHERE 
		   source_sys_id = 'PMS' AND crrnt_snpsht_flag = '1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_ak_id) = 1
),
LKP_CLAIM_PARTY_OCCURRENCE AS (
	SELECT
	claim_party_occurrence_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id
	FROM (
		SELECT 
		a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		a.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		a.claim_party_ak_id as claim_party_ak_id 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence a
		WHERE 
		claim_party_role_code = 'CMT' and 
		   source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_party_ak_id ORDER BY claim_party_occurrence_ak_id) = 1
),
SQ_claim_party_relation1 AS (
	SELECT claim_party_relation.claim_party_relation_id, claim_party_relation.claim_party_occurrence_ak_id, claim_party_relation.claim_party_relation_from_ak_id, claim_party_relation.claim_party_relation_to_ak_id, 
	rtrim(claim_party_relation.claim_party_relation_role_code) as claim_party_relation_role_code, 
	rtrim(claim_party_relation.cms_party_type) as cms_party_type
	FROM
	 claim_party_relation 
	WHERE
	 source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
),
EXP_1 AS (
	SELECT
	claim_party_relation_id,
	claim_party_occurrence_ak_id,
	claim_party_relation_from_ak_id,
	claim_party_relation_to_ak_id,
	claim_party_relation_role_code,
	cms_party_type
	FROM SQ_claim_party_relation1
),
SQ_cms_pms_relation_stage AS (
	SELECT cms_pms_relation_stage.cms_party_type, cms_pms_relation_stage.cms_relation_ind, cms_pms_relation_stage.is_individual, claim_medical_stage.pms_policy_sym, claim_medical_stage.pms_policy_num, claim_medical_stage.pms_policy_mod, claim_medical_stage.pms_date_of_loss, claim_medical_stage.pms_loss_occurence, claim_medical_stage.pms_loss_claimant 
	FROM
	 cms_pms_relation_stage, claim_medical_stage
	WHERE
	claim_medical_stage.injured_party_id=cms_pms_relation_stage.injured_party_id
	AND cms_pms_relation_stage.cms_party_type <> 'MINJ'
	
	
	--MINJ (injured party) will be already present with CMT suffix in the claim_party_key) so need not insert these records
	--AND claim_medical_stage.cms_source_system_id = 'PMS'  (This is not required as cms_pms_relation_stage contains data only for doc_nums where PMS injured partties.)
),
EXP_Values AS (
	SELECT
	cms_party_type,
	cms_relation_ind AS cre_client_role_cd,
	is_individual,
	pms_policy_sym,
	pms_policy_num,
	pms_policy_mod,
	pms_date_of_loss,
	pms_loss_occurence,
	pms_loss_claimant,
	-- *INF*: replaceChr(0,to_char(pms_date_of_loss),'/','')
	REGEXP_REPLACE(to_char(pms_date_of_loss),'/','','i') AS v_pms_date_of_loss,
	pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence AS cre_claim_nbr,
	pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence || pms_loss_claimant || cms_party_type AS cre_client_id,
	pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence || pms_loss_claimant || 'CMT' AS cre_rel_to_clt_id
	FROM SQ_cms_pms_relation_stage
),
EXP_Lkp_Values AS (
	SELECT
	cre_claim_nbr,
	cre_client_id,
	cre_rel_to_clt_id,
	-- *INF*: :LKP.LKP_CLAIM_OCCURRENCE(cre_claim_nbr)
	LKP_CLAIM_OCCURRENCE_cre_claim_nbr.claim_occurrence_ak_id AS v_claim_occurrence_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY(cre_client_id)
	LKP_CLAIM_PARTY_cre_client_id.claim_party_ak_id AS v_claim_party_relation_from_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY(cre_rel_to_clt_id)
	LKP_CLAIM_PARTY_cre_rel_to_clt_id.claim_party_ak_id AS v_claim_party_relation_to_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_OCCURRENCE(v_claim_occurrence_ak_id,v_claim_party_relation_to_ak_id)
	LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurrence_ak_id_v_claim_party_relation_to_ak_id.claim_party_occurrence_ak_id AS v_claim_party_occurrence_ak_id,
	-- *INF*: v_claim_party_occurrence_ak_id
	-- 
	-- --IIF(ISNULL(v_CLAIM_PARTY_OCCURRENCE_AK_ID), 0, v_CLAIM_PARTY_OCCURRENCE_AK_ID)
	v_claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id,
	v_claim_party_relation_from_ak_id AS claim_party_relation_from_ak_id,
	v_claim_party_relation_to_ak_id AS claim_party_relation_to_ak_id,
	cre_client_role_cd AS in_cre_client_role_cd,
	-- *INF*: IIF(ISNULL(in_cre_client_role_cd), 'N/A',
	--  IIF(IS_SPACES(in_cre_client_role_cd), 'N/A',
	-- in_cre_client_role_cd))
	IFF(
	    in_cre_client_role_cd IS NULL, 'N/A',
	    IFF(
	        LENGTH(in_cre_client_role_cd)>0
	    and TRIM(in_cre_client_role_cd)='', 'N/A',
	        in_cre_client_role_cd
	    )
	) AS cre_client_role_cd,
	cms_party_type AS in_cms_party_type,
	-- *INF*: IIF(ISNULL(in_cms_party_type), 'N/A',
	--  IIF(IS_SPACES(in_cms_party_type), 'N/A',
	-- in_cms_party_type))
	IFF(
	    in_cms_party_type IS NULL, 'N/A',
	    IFF(
	        LENGTH(in_cms_party_type)>0
	    and TRIM(in_cms_party_type)='', 'N/A', in_cms_party_type
	    )
	) AS cms_party_type,
	is_individual AS in_is_individual,
	-- *INF*: IIF(ISNULL(in_is_individual), 'N/A',
	--  IIF(IS_SPACES(in_is_individual), 'N/A',
	-- in_is_individual))
	-- 
	-- 
	IFF(
	    in_is_individual IS NULL, 'N/A',
	    IFF(
	        LENGTH(in_is_individual)>0 AND TRIM(in_is_individual)='', 'N/A', in_is_individual
	    )
	) AS is_individual
	FROM EXP_Values
	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_cre_claim_nbr
	ON LKP_CLAIM_OCCURRENCE_cre_claim_nbr.claim_occurrence_key = cre_claim_nbr

	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_cre_client_id
	ON LKP_CLAIM_PARTY_cre_client_id.claim_party_key = cre_client_id

	LEFT JOIN LKP_CLAIM_PARTY LKP_CLAIM_PARTY_cre_rel_to_clt_id
	ON LKP_CLAIM_PARTY_cre_rel_to_clt_id.claim_party_key = cre_rel_to_clt_id

	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurrence_ak_id_v_claim_party_relation_to_ak_id
	ON LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurrence_ak_id_v_claim_party_relation_to_ak_id.claim_occurrence_ak_id = v_claim_occurrence_ak_id
	AND LKP_CLAIM_PARTY_OCCURRENCE_v_claim_occurrence_ak_id_v_claim_party_relation_to_ak_id.claim_party_ak_id = v_claim_party_relation_to_ak_id

),
JNR_Source_Deleted_Rows AS (SELECT
	EXP_1.claim_party_relation_id AS tgt_claim_party_relation_id, 
	EXP_1.claim_party_occurrence_ak_id AS tgt_claim_party_occurrence_ak_id, 
	EXP_1.claim_party_relation_from_ak_id AS tgt_claim_party_relation_from_ak_id, 
	EXP_1.claim_party_relation_to_ak_id AS tgt_claim_party_relation_to_ak_id, 
	EXP_1.claim_party_relation_role_code AS tgt_claim_party_relation_role_code, 
	EXP_1.cms_party_type AS tgt_cms_party_type, 
	EXP_Lkp_Values.claim_party_occurrence_ak_id AS src_claim_party_occurrence_ak_id, 
	EXP_Lkp_Values.claim_party_relation_from_ak_id AS src_claim_party_relation_from_ak_id, 
	EXP_Lkp_Values.claim_party_relation_to_ak_id AS src_claim_party_relation_to_ak_id, 
	EXP_Lkp_Values.cre_client_role_cd AS src_claim_party_relation_role_code, 
	EXP_Lkp_Values.cms_party_type AS src_cms_party_type
	FROM EXP_Lkp_Values
	RIGHT OUTER JOIN EXP_1
	ON EXP_1.claim_party_occurrence_ak_id = EXP_Lkp_Values.claim_party_occurrence_ak_id AND EXP_1.claim_party_relation_from_ak_id = EXP_Lkp_Values.claim_party_relation_from_ak_id AND EXP_1.claim_party_relation_to_ak_id = EXP_Lkp_Values.claim_party_relation_to_ak_id AND EXP_1.claim_party_relation_role_code = EXP_Lkp_Values.cre_client_role_cd AND EXP_1.cms_party_type = EXP_Lkp_Values.cms_party_type
),
FIL_Deleted_Rows AS (
	SELECT
	tgt_claim_party_relation_id, 
	tgt_claim_party_occurrence_ak_id, 
	tgt_claim_party_relation_from_ak_id, 
	tgt_claim_party_relation_to_ak_id, 
	tgt_claim_party_relation_role_code, 
	tgt_cms_party_type, 
	src_claim_party_occurrence_ak_id, 
	src_claim_party_relation_from_ak_id, 
	src_claim_party_relation_to_ak_id, 
	src_claim_party_relation_role_code, 
	src_cms_party_type
	FROM JNR_Source_Deleted_Rows
	WHERE ISNULL(src_claim_party_occurrence_ak_id)
),
EXP_deleted_rows AS (
	SELECT
	tgt_claim_party_relation_id,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS Modified_date
	FROM FIL_Deleted_Rows
),
UPD_Claim_Party_Relation1 AS (
	SELECT
	tgt_claim_party_relation_id, 
	crrnt_snpsht_flag, 
	Modified_date
	FROM EXP_deleted_rows
),
claim_party_relation_Deleted_Rows_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_relation AS T
	USING UPD_Claim_Party_Relation1 AS S
	ON T.claim_party_relation_id = S.tgt_claim_party_relation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.modified_date = S.Modified_date
),
LKP_Target AS (
	SELECT
	claim_party_relation_id,
	claim_party_relation_ak_id,
	claim_party_relation_role_code,
	cms_party_type,
	is_cms_party_individ,
	claim_party_occurrence_ak_id,
	claim_party_relation_from_ak_id,
	claim_party_relation_to_ak_id
	FROM (
		SELECT 
		a.claim_party_relation_id as claim_party_relation_id, 
		a.claim_party_relation_ak_id as claim_party_relation_ak_id, 
		ltrim(rtrim(a.is_cms_party_individ)) as is_cms_party_individ,
		a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		a.claim_party_relation_from_ak_id as claim_party_relation_from_ak_id, 
		a.claim_party_relation_to_ak_id as claim_party_relation_to_ak_id, 
		ltrim(rtrim(a.claim_party_relation_role_code)) as claim_party_relation_role_code,
		ltrim(rtrim(a.cms_party_type)) as cms_party_type
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_relation a
		WHERE  
		a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND a.crrnt_snpsht_flag = 1
		ORDER BY 
		claim_party_occurrence_ak_id, claim_party_relation_from_ak_id, claim_party_relation_to_ak_id, claim_party_relation_role_code --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,claim_party_relation_from_ak_id,claim_party_relation_to_ak_id,claim_party_relation_role_code,cms_party_type ORDER BY claim_party_relation_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lkp_Values.claim_party_occurrence_ak_id,
	EXP_Lkp_Values.claim_party_relation_from_ak_id,
	EXP_Lkp_Values.claim_party_relation_to_ak_id,
	EXP_Lkp_Values.cre_client_role_cd,
	EXP_Lkp_Values.cms_party_type,
	EXP_Lkp_Values.is_individual,
	LKP_Target.claim_party_relation_id AS lkp_claim_party_relation_id,
	LKP_Target.claim_party_relation_ak_id AS lkp_claim_party_relation_ak_id,
	LKP_Target.claim_party_relation_role_code AS lkp_claim_party_relation_role_code,
	LKP_Target.cms_party_type AS lkp_cms_party_type,
	LKP_Target.is_cms_party_individ AS lkp_is_cms_party_individ,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: iif(isnull(lkp_claim_party_relation_id),'NEW',	
	-- 	iif (ltrim(rtrim(lkp_is_cms_party_individ)) <> ltrim(rtrim(is_individual))
	-- 	, 'UPDATE','NOCHANGE'))
	-- 
	-- 
	-- --iif(isnull(lkp_claim_party_relation_id),'NEW','NOCHANGE')
	-- 
	-- 
	IFF(
	    lkp_claim_party_relation_id IS NULL, 'NEW',
	    IFF(
	        ltrim(rtrim(lkp_is_cms_party_individ)) <> ltrim(rtrim(is_individual)), 'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(
	    v_Changed_Flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_Target
	ON LKP_Target.claim_party_occurrence_ak_id = EXP_Lkp_Values.claim_party_occurrence_ak_id AND LKP_Target.claim_party_relation_from_ak_id = EXP_Lkp_Values.claim_party_relation_from_ak_id AND LKP_Target.claim_party_relation_to_ak_id = EXP_Lkp_Values.claim_party_relation_to_ak_id AND LKP_Target.claim_party_relation_role_code = EXP_Lkp_Values.cre_client_role_cd AND LKP_Target.cms_party_type = EXP_Lkp_Values.cms_party_type
),
FIL_Insert AS (
	SELECT
	lkp_claim_party_relation_ak_id, 
	claim_party_occurrence_ak_id, 
	claim_party_relation_from_ak_id, 
	claim_party_relation_to_ak_id, 
	cre_client_role_cd, 
	cms_party_type, 
	is_individual, 
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date, 
	Changed_Flag
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_party_relation AS (
	CREATE SEQUENCE SEQ_claim_party_relation
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	lkp_claim_party_relation_ak_id,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, lkp_claim_party_relation_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, lkp_claim_party_relation_ak_id) AS claim_party_relation_ak_id,
	claim_party_occurrence_ak_id,
	claim_party_relation_from_ak_id,
	claim_party_relation_to_ak_id,
	cre_client_role_cd,
	cms_party_type,
	is_individual,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	SOURCE_SYSTEM_ID,
	Created_Date,
	Modified_Date,
	Changed_Flag,
	SEQ_claim_party_relation.NEXTVAL
	FROM FIL_Insert
),
claim_party_relation_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_relation
	(claim_party_relation_ak_id, claim_party_occurrence_ak_id, claim_party_relation_from_ak_id, claim_party_relation_to_ak_id, claim_party_relation_role_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, cms_party_type, is_cms_party_individ)
	SELECT 
	CLAIM_PARTY_RELATION_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_RELATION_FROM_AK_ID, 
	CLAIM_PARTY_RELATION_TO_AK_ID, 
	cre_client_role_cd AS CLAIM_PARTY_RELATION_ROLE_CODE, 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	CMS_PARTY_TYPE, 
	is_individual AS IS_CMS_PARTY_INDIVID
	FROM EXP_Determine_AK
),
SQ_claim_party_relation AS (
	SELECT 
	a.claim_party_relation_id, 
	a.claim_party_occurrence_ak_id, 
	a.claim_party_relation_from_ak_id, 
	a.claim_party_relation_to_ak_id,
	a.claim_party_relation_role_code,
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_relation a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND  
	 EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_relation b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_party_occurrence_ak_id = b.claim_party_occurrence_ak_id
	             AND a.claim_party_relation_from_ak_id = b.claim_party_relation_from_ak_id
		       AND a.claim_party_relation_to_ak_id = b.claim_party_relation_to_ak_id
		      AND a.claim_party_relation_role_code = b.claim_party_relation_role_code
		GROUP BY claim_party_occurrence_ak_id, claim_party_relation_from_ak_id, claim_party_relation_to_ak_id, claim_party_relation_role_code
		HAVING COUNT(*) > 1)
	ORDER BY claim_party_occurrence_ak_id, claim_party_relation_from_ak_id, claim_party_relation_to_ak_id, claim_party_relation_role_code, eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_party_relation_id,
	claim_party_occurrence_ak_id,
	claim_party_relation_from_ak_id,
	claim_party_relation_to_ak_id,
	claim_party_relation_role_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_party_occurrence_ak_id = v_PREV_ROW_claim_party_occurrence_ak_id AND 
	-- 	claim_party_relation_from_ak_id = v_PREV_ROW_claim_party_relation_from_ak_id  AND 
	-- 	claim_party_relation_to_ak_id = v_PREV_ROW_claim_party_relation_to_ak_id AND
	--       claim_party_relation_role_code = v_claim_party_relation_role_code,
	--   ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	--   orig_eff_to_date)
	DECODE(
	    TRUE,
	    claim_party_occurrence_ak_id = v_PREV_ROW_claim_party_occurrence_ak_id AND claim_party_relation_from_ak_id = v_PREV_ROW_claim_party_relation_from_ak_id AND claim_party_relation_to_ak_id = v_PREV_ROW_claim_party_relation_to_ak_id AND claim_party_relation_role_code = v_claim_party_relation_role_code, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_party_occurrence_ak_id AS v_PREV_ROW_claim_party_occurrence_ak_id,
	claim_party_relation_from_ak_id AS v_PREV_ROW_claim_party_relation_from_ak_id,
	claim_party_relation_to_ak_id AS v_PREV_ROW_claim_party_relation_to_ak_id,
	claim_party_relation_role_code AS v_claim_party_relation_role_code,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_party_relation
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_party_relation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_Claim_Party_Relation AS (
	SELECT
	claim_party_relation_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
claim_party_relation_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_relation AS T
	USING UPD_Claim_Party_Relation AS S
	ON T.claim_party_relation_id = S.claim_party_relation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),