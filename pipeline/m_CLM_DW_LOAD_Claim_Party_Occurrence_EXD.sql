WITH
LKP_Claim_Occurrence_id AS (
	SELECT
	claim_occurrence_ak_id,
	claim_occurrence_key
	FROM (
		SELECT claim_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_occurrence.claim_occurrence_key as claim_occurrence_key 
		FROM claim_occurrence
		WHERE source_sys_id = 'EXCEED' AND crrnt_snpsht_flag = '1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_ak_id) = 1
),
LKP_Claimant_num AS (
	SELECT
	ccn_claimant_nbr,
	ccn_claim_nbr,
	ccn_client_id
	FROM (
		SELECT 
			ccn_claimant_nbr,
			ccn_claim_nbr,
			ccn_client_id
		FROM claim_claimant_nbr_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ccn_claim_nbr,ccn_client_id ORDER BY ccn_claimant_nbr) = 1
),
LKP_Claim_Party_ak_id AS (
	SELECT
	claim_party_ak_id,
	claim_party_key
	FROM (
		SELECT 
		claim_party.claim_party_ak_id as claim_party_ak_id, 
		claim_party.claim_party_key as claim_party_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party
		WHERE crrnt_snpsht_flag  =1 and source_sys_id ='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_ak_id) = 1
),
LKP_CLAIM_CASE_AK_ID AS (
	SELECT
	claim_case_ak_id,
	claim_case_key
	FROM (
		SELECT 
		claim_case.claim_case_ak_id as claim_case_ak_id, 
		claim_case.claim_case_key as claim_case_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case 
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_key ORDER BY claim_case_ak_id) = 1
),
LKP_Claim_Occurrence_Key AS (
	SELECT
	claim_occurrence_key,
	claim_occurrence_ak_id
	FROM (
		SELECT 
		LTRIM(RTRIM(claim_occurrence.claim_occurrence_key)) as claim_occurrence_key, 
		claim_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY claim_occurrence_key) = 1
),
LKP_Claim_Party_Key AS (
	SELECT
	claim_party_key,
	claim_party_ak_id
	FROM (
		SELECT 
		claim_party.claim_party_key as claim_party_key, 
		claim_party.claim_party_ak_id as claim_party_ak_id 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party, @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence 
		WHERE 
		claim_party.claim_party_ak_id  = claim_party_occurrence.claim_party_ak_id
		AND claim_party.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND claim_party.crrnt_snpsht_flag = 1
		AND claim_party_occurrence.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND claim_party_occurrence.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_ak_id ORDER BY claim_party_key) = 1
),
SQ_CLAIM_OBJECT_CLT_STAGE AS (
	SELECT DISTINCT
	CLAIM_OBJECT_CLT_STAGE.CCT_CLAIM_NBR, 
	CLAIM_OBJECT_CLT_STAGE.CCT_CLIENT_ID, 
	CLAIM_OBJECT_CLT_STAGE.CCT_CLIENT_ROLE_CD 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OBJECT_CLT_STAGE
	WHERE (CLAIM_OBJECT_CLT_STAGE.CCT_CREATE_TS > =  DATEADD(DD, @{pipeline().parameters.NO_OF_DAYS} ,'@{pipeline().parameters.SELECTION_START_TS}') OR
	CLAIM_OBJECT_CLT_STAGE.CCT_UPD_TS > =  DATEADD(DD, @{pipeline().parameters.NO_OF_DAYS} ,'@{pipeline().parameters.SELECTION_START_TS}'))
),
EXP_Values_Claim_Party_Occurrence AS (
	SELECT
	CCT_CLAIM_NBR,
	CCT_CLIENT_ID,
	CCT_CLIENT_ROLE_CD
	FROM SQ_CLAIM_OBJECT_CLT_STAGE
),
EXP_LKP_Values_Claim_Party_Occurrence AS (
	SELECT
	CCT_CLAIM_NBR,
	CCT_CLIENT_ID,
	-- *INF*: LTRIM(RTRIM(CCT_CLIENT_ID))
	LTRIM(RTRIM(CCT_CLIENT_ID)) AS v_CCT_CLIENT_ID,
	CCT_CLIENT_ROLE_CD AS In_CCT_CLIENT_ROLE_CD,
	-- *INF*: LTRIM(RTRIM(In_CCT_CLIENT_ROLE_CD))
	LTRIM(RTRIM(In_CCT_CLIENT_ROLE_CD)) AS Out_CCT_CLIENT_ROLE_CD,
	-- *INF*: :LKP.LKP_Claim_Occurrence_id(CCT_CLAIM_NBR)
	LKP_CLAIM_OCCURRENCE_ID_CCT_CLAIM_NBR.claim_occurrence_ak_id AS claim_occurrence_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_AK_ID(v_CCT_CLIENT_ID)
	LKP_CLAIM_PARTY_AK_ID_v_CCT_CLIENT_ID.claim_party_ak_id AS claim_party_ak_id,
	-- *INF*: CCT_CLAIM_NBR|| '//'||CCT_CLIENT_ID
	CCT_CLAIM_NBR || '//' || CCT_CLIENT_ID AS Claim_Case_key,
	-- *INF*: :LKP.LKP_CLAIM_CASE_AK_ID(Claim_Case_key)
	LKP_CLAIM_CASE_AK_ID_Claim_Case_key.claim_case_ak_id AS v_claim_case_ak_id,
	-- *INF*: IIF(ISNULL(v_claim_case_ak_id),-1,v_claim_case_ak_id)
	IFF(v_claim_case_ak_id IS NULL, - 1, v_claim_case_ak_id) AS claim_case_ak_id,
	-- *INF*: :LKP.LKP_CLAIMANT_NUM(CCT_CLAIM_NBR,CCT_CLIENT_ID)
	LKP_CLAIMANT_NUM_CCT_CLAIM_NBR_CCT_CLIENT_ID.ccn_claimant_nbr AS v_claimant_num,
	-- *INF*: IIF(NOT ISNULL(v_claimant_num) AND In_CCT_CLIENT_ROLE_CD ='CLMT' , v_claimant_num,'N/A')
	IFF(NOT v_claimant_num IS NULL AND In_CCT_CLIENT_ROLE_CD = 'CLMT', v_claimant_num, 'N/A') AS out_Claimant_num,
	'N/A' AS OFFSET_ONSET_IND,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	1 AS CRRNT_SNPSHT_FLAG
	FROM EXP_Values_Claim_Party_Occurrence
	LEFT JOIN LKP_CLAIM_OCCURRENCE_ID LKP_CLAIM_OCCURRENCE_ID_CCT_CLAIM_NBR
	ON LKP_CLAIM_OCCURRENCE_ID_CCT_CLAIM_NBR.claim_occurrence_key = CCT_CLAIM_NBR

	LEFT JOIN LKP_CLAIM_PARTY_AK_ID LKP_CLAIM_PARTY_AK_ID_v_CCT_CLIENT_ID
	ON LKP_CLAIM_PARTY_AK_ID_v_CCT_CLIENT_ID.claim_party_key = v_CCT_CLIENT_ID

	LEFT JOIN LKP_CLAIM_CASE_AK_ID LKP_CLAIM_CASE_AK_ID_Claim_Case_key
	ON LKP_CLAIM_CASE_AK_ID_Claim_Case_key.claim_case_key = Claim_Case_key

	LEFT JOIN LKP_CLAIMANT_NUM LKP_CLAIMANT_NUM_CCT_CLAIM_NBR_CCT_CLIENT_ID
	ON LKP_CLAIMANT_NUM_CCT_CLAIM_NBR_CCT_CLIENT_ID.ccn_claim_nbr = CCT_CLAIM_NBR
	AND LKP_CLAIMANT_NUM_CCT_CLAIM_NBR_CCT_CLIENT_ID.ccn_client_id = CCT_CLIENT_ID

),
LKP_Claim_Party_Occurrence AS (
	SELECT
	claim_party_occurrence_id,
	claim_party_occurrence_ak_id,
	claim_party_role_code,
	claim_occurrence_ak_id,
	claim_party_ak_id
	FROM (
		SELECT 
		a.claim_party_occurrence_id as claim_party_occurrence_id, 
		a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		a.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		ltrim(rtrim(a.claim_party_role_code)) as claim_party_role_code, 
		a.claim_party_ak_id as claim_party_ak_id
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence a
		WHERE 
		a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_party_role_code,claim_party_ak_id ORDER BY claim_party_occurrence_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_LKP_Values_Claim_Party_Occurrence.claim_occurrence_ak_id,
	EXP_LKP_Values_Claim_Party_Occurrence.Out_CCT_CLIENT_ROLE_CD AS CCT_CLIENT_ROLE_CD,
	EXP_LKP_Values_Claim_Party_Occurrence.claim_party_ak_id,
	EXP_LKP_Values_Claim_Party_Occurrence.claim_case_ak_id,
	LKP_Claim_Party_Occurrence.claim_party_occurrence_id AS lkp_claim_party_occurrence_id,
	LKP_Claim_Party_Occurrence.claim_party_occurrence_ak_id AS lkp_claim_party_occurrence_ak_id,
	LKP_Claim_Party_Occurrence.claim_party_role_code AS lkp_claim_party_role_code,
	-- *INF*: TO_DATE('1/1/1800','MM/DD/YYYY')
	TO_DATE('1/1/1800', 'MM/DD/YYYY') AS denial_date,
	0 AS logical_flag,
	1 AS crrnt_snpsht_flag,
	-- *INF*: iif(isnull(lkp_claim_party_occurrence_id), 'NEW', 'NOCHANGE')
	IFF(lkp_claim_party_occurrence_id IS NULL, 'NEW', 'NOCHANGE') AS v_changed_flag,
	v_changed_flag AS Changed_Flag,
	EXP_LKP_Values_Claim_Party_Occurrence.OFFSET_ONSET_IND AS offset_onset_ind,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	EXP_LKP_Values_Claim_Party_Occurrence.SOURCE_SYSTEM_ID AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	EXP_LKP_Values_Claim_Party_Occurrence.out_Claimant_num
	FROM EXP_LKP_Values_Claim_Party_Occurrence
	LEFT JOIN LKP_Claim_Party_Occurrence
	ON LKP_Claim_Party_Occurrence.claim_occurrence_ak_id = EXP_LKP_Values_Claim_Party_Occurrence.claim_occurrence_ak_id AND LKP_Claim_Party_Occurrence.claim_party_role_code = EXP_LKP_Values_Claim_Party_Occurrence.Out_CCT_CLIENT_ROLE_CD AND LKP_Claim_Party_Occurrence.claim_party_ak_id = EXP_LKP_Values_Claim_Party_Occurrence.claim_party_ak_id
),
FIL_Insert_Party_Occurrence AS (
	SELECT
	lkp_claim_party_occurrence_ak_id, 
	claim_occurrence_ak_id, 
	CCT_CLIENT_ROLE_CD, 
	claim_party_ak_id, 
	claim_case_ak_id, 
	denial_date, 
	logical_flag, 
	crrnt_snpsht_flag, 
	Changed_Flag, 
	offset_onset_ind, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	out_Claimant_num AS out_claimant_num
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_party_occurrence AS (
	CREATE SEQUENCE SEQ_claim_party_occurrence
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	SEQ_claim_party_occurrence.NEXTVAL,
	lkp_claim_party_occurrence_ak_id,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, lkp_claim_party_occurrence_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, lkp_claim_party_occurrence_ak_id) AS claim_party_occurrence_ak_id,
	claim_occurrence_ak_id,
	CCT_CLIENT_ROLE_CD,
	claim_party_ak_id,
	claim_case_ak_id,
	denial_date,
	logical_flag,
	crrnt_snpsht_flag,
	Changed_Flag,
	offset_onset_ind,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	out_claimant_num
	FROM FIL_Insert_Party_Occurrence
),
claim_party_occurrence_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence
	(claim_party_occurrence_ak_id, claim_occurrence_ak_id, claim_party_ak_id, claim_case_ak_id, claim_party_role_code, claimant_num, denial_date, offset_onset_ind, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID, 
	CLAIM_CASE_AK_ID, 
	CCT_CLIENT_ROLE_CD AS CLAIM_PARTY_ROLE_CODE, 
	out_claimant_num AS CLAIMANT_NUM, 
	DENIAL_DATE, 
	OFFSET_ONSET_IND, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_Determine_AK
),
SQ_claim_party_occurrence AS (
	SELECT 
	a.claim_party_occurrence_id, 
	a.claim_occurrence_ak_id, 
	a.claim_party_role_code,
	a.claim_party_ak_id, 
	a.denial_date, 
	a.crrnt_snpsht_flag, 
	a.audit_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.source_sys_id, 
	a.created_date, 
	a.modified_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND 
	 EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence b
			WHERE b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND 
	b.crrnt_snpsht_flag = '1'
			AND a.claim_occurrence_ak_id =  b.claim_occurrence_ak_id
			AND a.claim_party_role_code = b.claim_party_role_code
			AND a.claim_party_ak_id = b.claim_party_ak_id
			GROUP BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id 
			HAVING COUNT(*) > 1) 
	ORDER BY claim_occurrence_ak_id, claim_party_role_code, claim_party_ak_id, eff_from_date  DESC
),
EXP_expire_eff_from_date AS (
	SELECT
	claim_party_occurrence_id,
	claim_occurrence_ak_id AS claim_occurrence_id,
	claim_party_role_code,
	claim_party_ak_id AS claim_party_id,
	-- *INF*: to_char(claim_occurrence_id)||to_char(claim_party_role_code)||to_char(claim_party_id)
	to_char(claim_occurrence_id) || to_char(claim_party_role_code) || to_char(claim_party_id) AS v_claim_party_occurrence_key,
	denial_date,
	crrnt_snpsht_flag,
	0 AS new_crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	v_claim_party_occurrence_key = v_prev_row_claim_party_occurrence_key, ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),
	-- 	eff_to_date)
	DECODE(TRUE,
		v_claim_party_occurrence_key = v_prev_row_claim_party_occurrence_key, ADD_TO_DATE(v_prev_row_eff_from_date, 'SS', - 1),
		eff_to_date) AS v_new_eff_to_Date,
	eff_from_date AS v_prev_row_eff_from_date,
	-- *INF*: to_char(claim_occurrence_id)||to_char(claim_party_role_code)||to_char(claim_party_id)
	to_char(claim_occurrence_id) || to_char(claim_party_role_code) || to_char(claim_party_id) AS v_prev_row_claim_party_occurrence_key,
	v_new_eff_to_Date AS new_eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	sysdate AS new_modified_date
	FROM SQ_claim_party_occurrence
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_party_occurrence_id, 
	new_crrnt_snpsht_flag, 
	eff_to_date, 
	new_eff_to_date, 
	new_modified_date
	FROM EXP_expire_eff_from_date
	WHERE eff_to_date<>new_eff_to_date
),
UPD_claim_party_occurrence AS (
	SELECT
	claim_party_occurrence_id, 
	new_crrnt_snpsht_flag, 
	new_eff_to_date, 
	new_modified_date
	FROM FIL_FirstRowInAKGroup
),
claim_party_occurrence_Update AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence
	(claim_party_occurrence_id, crrnt_snpsht_flag, eff_to_date, modified_date)
	SELECT 
	CLAIM_PARTY_OCCURRENCE_ID, 
	new_crrnt_snpsht_flag AS CRRNT_SNPSHT_FLAG, 
	new_eff_to_date AS EFF_TO_DATE, 
	new_modified_date AS MODIFIED_DATE
	FROM UPD_claim_party_occurrence
),
SQ_claim_party_occurrence_Update_Claim_Case_Ak_id AS (
	SELECT claim_party_occurrence.claim_party_occurrence_id, claim_party_occurrence.claim_party_occurrence_ak_id, claim_party_occurrence.claim_occurrence_ak_id, claim_party_occurrence.claim_party_ak_id, claim_party_occurrence.claim_case_ak_id 
	FROM
	 claim_party_occurrence 
	WHERE
	 claim_party_occurrence.claim_case_ak_id = -1 
	AND claim_party_occurrence.crrnt_snpsht_flag = 1
	AND claim_party_occurrence.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_Evaluate AS (
	SELECT
	claim_party_occurrence_id,
	claim_party_occurrence_ak_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claim_case_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_KEY(claim_party_ak_id)
	LKP_CLAIM_PARTY_KEY_claim_party_ak_id.claim_party_key AS v_claim_party_key,
	-- *INF*: :LKP.LKP_CLAIM_OCCURRENCE_KEY(claim_occurrence_ak_id)
	LKP_CLAIM_OCCURRENCE_KEY_claim_occurrence_ak_id.claim_occurrence_key AS v_claim_occurrence_key,
	-- *INF*: LTRIM(RTRIM(v_claim_occurrence_key|| '//'||v_claim_party_key))
	LTRIM(RTRIM(v_claim_occurrence_key || '//' || v_claim_party_key)) AS v_Claim_Case_Key,
	-- *INF*: :LKP.LKP_CLAIM_CASE_AK_ID(v_Claim_Case_Key)
	LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key.claim_case_ak_id AS v_Claim_Case_Ak_id,
	-- *INF*: IIF(ISNULL(v_Claim_Case_Ak_id),-1,v_Claim_Case_Ak_id)
	IFF(v_Claim_Case_Ak_id IS NULL, - 1, v_Claim_Case_Ak_id) AS Out_Claim_Case_Ak_id
	FROM SQ_claim_party_occurrence_Update_Claim_Case_Ak_id
	LEFT JOIN LKP_CLAIM_PARTY_KEY LKP_CLAIM_PARTY_KEY_claim_party_ak_id
	ON LKP_CLAIM_PARTY_KEY_claim_party_ak_id.claim_party_ak_id = claim_party_ak_id

	LEFT JOIN LKP_CLAIM_OCCURRENCE_KEY LKP_CLAIM_OCCURRENCE_KEY_claim_occurrence_ak_id
	ON LKP_CLAIM_OCCURRENCE_KEY_claim_occurrence_ak_id.claim_occurrence_ak_id = claim_occurrence_ak_id

	LEFT JOIN LKP_CLAIM_CASE_AK_ID LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key
	ON LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key.claim_case_key = v_Claim_Case_Key

),
FIL_Claim_Case_Ak_id AS (
	SELECT
	claim_party_occurrence_id, 
	Out_Claim_Case_Ak_id
	FROM EXP_Evaluate
	WHERE Out_Claim_Case_Ak_id != -1
),
UPD_Claim_Case_Ak_id_Update AS (
	SELECT
	claim_party_occurrence_id, 
	Out_Claim_Case_Ak_id
	FROM FIL_Claim_Case_Ak_id
),
claim_party_occurrence_Update_Claim_Case_Ak_id AS (
	MERGE INTO claim_party_occurrence AS T
	USING UPD_Claim_Case_Ak_id_Update AS S
	ON T.claim_party_occurrence_id = S.claim_party_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_case_ak_id = S.Out_Claim_Case_Ak_id
),
SQ_claim_party_occurrence_UPD_Claimant_Num AS (
	SELECT claim_party_occurrence.claim_party_occurrence_id, claim_party_occurrence.claim_occurrence_ak_id, claim_party_occurrence.claim_party_ak_id, claim_party_occurrence.claimant_num
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence
	WHERE  claim_party_role_code = 'CLMT' AND claimant_num = 'N/A' 
	AND source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	AND crrnt_snpsht_flag = 1
),
EXP_Lkp_values AS (
	SELECT
	claim_party_occurrence_id,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	claimant_num,
	-- *INF*: :LKP.LKP_CLAIM_OCCURRENCE_KEY(claim_occurrence_ak_id)
	LKP_CLAIM_OCCURRENCE_KEY_claim_occurrence_ak_id.claim_occurrence_key AS v_Claim_Occurrence_Key,
	-- *INF*: :LKP.LKP_CLAIM_PARTY_KEY(claim_party_ak_id)
	LKP_CLAIM_PARTY_KEY_claim_party_ak_id.claim_party_key AS v_Claim_Party_Key,
	-- *INF*: :LKP.LKP_CLAIMANT_NUM(v_Claim_Occurrence_Key,v_Claim_Party_Key)
	LKP_CLAIMANT_NUM_v_Claim_Occurrence_Key_v_Claim_Party_Key.ccn_claimant_nbr AS v_claimant_num,
	-- *INF*: IIF(ISNULL(v_claimant_num),'N/A',v_claimant_num)
	IFF(v_claimant_num IS NULL, 'N/A', v_claimant_num) AS Out_Claimant_num,
	SYSDATE AS Modified_date
	FROM SQ_claim_party_occurrence_UPD_Claimant_Num
	LEFT JOIN LKP_CLAIM_OCCURRENCE_KEY LKP_CLAIM_OCCURRENCE_KEY_claim_occurrence_ak_id
	ON LKP_CLAIM_OCCURRENCE_KEY_claim_occurrence_ak_id.claim_occurrence_ak_id = claim_occurrence_ak_id

	LEFT JOIN LKP_CLAIM_PARTY_KEY LKP_CLAIM_PARTY_KEY_claim_party_ak_id
	ON LKP_CLAIM_PARTY_KEY_claim_party_ak_id.claim_party_ak_id = claim_party_ak_id

	LEFT JOIN LKP_CLAIMANT_NUM LKP_CLAIMANT_NUM_v_Claim_Occurrence_Key_v_Claim_Party_Key
	ON LKP_CLAIMANT_NUM_v_Claim_Occurrence_Key_v_Claim_Party_Key.ccn_claim_nbr = v_Claim_Occurrence_Key
	AND LKP_CLAIMANT_NUM_v_Claim_Occurrence_Key_v_Claim_Party_Key.ccn_client_id = v_Claim_Party_Key

),
UPD_Claimant_num AS (
	SELECT
	claim_party_occurrence_id, 
	Out_Claimant_num, 
	Modified_date
	FROM EXP_Lkp_values
),
claim_party_occurrence_UPD_Claimant_num AS (
	MERGE INTO claim_party_occurrence AS T
	USING UPD_Claimant_num AS S
	ON T.claim_party_occurrence_id = S.claim_party_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claimant_num = S.Out_Claimant_num, T.modified_date = S.Modified_date
),
SQ_ClaimClientStage AS (
	SELECT ccs.PREFERRED_CONTACT_METHOD,
		cpo.claim_party_occurrence_id, 
		cpo.preferred_contact_method AS preferred_contact_method_target
	FROM dbo.ClaimClientStage ccs 
	JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence co ON ccs.CCI_CLAIM_NBR = co.claim_occurrence_key 
		AND co.crrnt_snpsht_flag = 1
	JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_party cp ON ccs.CCI_CLIENT_ID = cp.claim_party_key 
		AND cp.crrnt_snpsht_flag = 1
	JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_party_occurrence cpo ON cpo.claim_occurrence_ak_id = co.claim_occurrence_ak_id 
		AND cpo.claim_party_ak_id = cp.claim_party_ak_id 
		AND cpo.source_sys_id = 'EXCEED' 
		AND cpo.crrnt_snpsht_flag = 1
),
EXP_SQ AS (
	SELECT
	PREFERRED_CONTACT_METHOD,
	claim_party_occurrence_id,
	preferred_contact_method_target,
	-- *INF*: UPPER(:UDF.DEFAULT_VALUE_FOR_STRINGS(PREFERRED_CONTACT_METHOD))
	UPPER(:UDF.DEFAULT_VALUE_FOR_STRINGS(PREFERRED_CONTACT_METHOD)) AS o_PREFERRED_CONTACT_METHOD
	FROM SQ_ClaimClientStage
),
LKP_PreferredContactMethod AS (
	SELECT
	PreferredContactMethodDescription,
	PreferredContactMethodCode
	FROM (
		SELECT 
			PreferredContactMethodDescription,
			PreferredContactMethodCode
		FROM SupClaimPreferredContactMethod
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PreferredContactMethodCode ORDER BY PreferredContactMethodDescription) = 1
),
EXP_Derive_PreferredContactMethod_Description AS (
	SELECT
	EXP_SQ.claim_party_occurrence_id,
	EXP_SQ.preferred_contact_method_target,
	LKP_PreferredContactMethod.PreferredContactMethodDescription AS lkp_PreferredContactMethodDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(lkp_PreferredContactMethodDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(lkp_PreferredContactMethodDescription) AS o_PreferredContactMethodDescription,
	-- *INF*: IIF((NOT ISNULL(claim_party_occurrence_id)) AND 
	-- preferred_contact_method_target <> lkp_PreferredContactMethodDescription, 
	-- 'UPDATE', 
	-- 'NOACTION')
	IFF(( NOT claim_party_occurrence_id IS NULL ) AND preferred_contact_method_target <> lkp_PreferredContactMethodDescription, 'UPDATE', 'NOACTION') AS o_Action,
	SYSDATE AS modified_date
	FROM EXP_SQ
	LEFT JOIN LKP_PreferredContactMethod
	ON LKP_PreferredContactMethod.PreferredContactMethodCode = EXP_SQ.o_PREFERRED_CONTACT_METHOD
),
FIL_UpdateRequired AS (
	SELECT
	claim_party_occurrence_id, 
	o_PreferredContactMethodDescription AS PreferredContactMethodDescription, 
	o_Action, 
	modified_date
	FROM EXP_Derive_PreferredContactMethod_Description
	WHERE o_Action='UPDATE'
),
UPDTRANS AS (
	SELECT
	claim_party_occurrence_id, 
	PreferredContactMethodDescription, 
	modified_date
	FROM FIL_UpdateRequired
),
claim_party_occurrence_UPD_ContactMethod AS (
	MERGE INTO claim_party_occurrence AS T
	USING UPDTRANS AS S
	ON T.claim_party_occurrence_id = S.claim_party_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.modified_date = S.modified_date, T.preferred_contact_method = S.PreferredContactMethodDescription
),