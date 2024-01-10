WITH
LKP_LIEN_CLIENT_NAME AS (
	SELECT
	claim_party_full_name,
	claim_party_key
	FROM (
		SELECT 
		claim_party.claim_party_full_name as claim_party_full_name, 
		claim_party.claim_party_key as claim_party_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_full_name) = 1
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
SQ_CLM_LIEN_DETAILS_STAGE AS (
	SELECT 
	cld.tch_claim_nbr, cld.tch_client_id, cld.lien_client_id, cld.lien_role, cld.lien_amt 
	FROM
	  @{pipeline().parameters.SOURCE_TABLE_OWNER}.clm_lien_details_stage cld
),
EXP_VALIDATE AS (
	SELECT
	tch_claim_nbr3 AS tch_claim_nbr_cld,
	tch_client_id3 AS tch_client_id_cld,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_claim_nbr_cld))) OR IS_SPACES(LTRIM(RTRIM(tch_claim_nbr_cld))) OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_cld)))=0,'N/A',LTRIM(RTRIM(tch_claim_nbr_cld)))
	--                                                                                 
	IFF(LTRIM(RTRIM(tch_claim_nbr_cld
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_cld
			)
		))>0 AND TRIM(LTRIM(RTRIM(tch_claim_nbr_cld
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_cld
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(tch_claim_nbr_cld
			)
		)
	) AS v_tch_claim_nbr,
	v_tch_claim_nbr AS tch_claim_nbr,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_client_id_cld))) OR IS_SPACES(LTRIM(RTRIM(tch_client_id_cld))) OR LENGTH(LTRIM(RTRIM(tch_client_id_cld)))=0,'N/A',LTRIM(RTRIM(tch_client_id_cld)))
	--                                                                            
	IFF(LTRIM(RTRIM(tch_client_id_cld
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(tch_client_id_cld
			)
		))>0 AND TRIM(LTRIM(RTRIM(tch_client_id_cld
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(tch_client_id_cld
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(tch_client_id_cld
			)
		)
	) AS v_tch_client_id,
	v_tch_client_id AS tch_client_id,
	-- *INF*: v_tch_claim_nbr || '//'||v_tch_client_id
	v_tch_claim_nbr || '//' || v_tch_client_id AS CLAIM_CASE_KEY,
	CLAIM_CASE_KEY AS Out_Claim_Case_key,
	-- *INF*: :LKP.LKP_CLAIM_CASE_AK_ID(CLAIM_CASE_KEY)
	LKP_CLAIM_CASE_AK_ID_CLAIM_CASE_KEY.claim_case_ak_id AS v_claim_case_ak_id,
	v_claim_case_ak_id AS claim_case_ak_id,
	lien_client_id AS IN_lien_client_id,
	-- *INF*: :LKP.LKP_LIEN_CLIENT_NAME(IN_lien_client_id)
	LKP_LIEN_CLIENT_NAME_IN_lien_client_id.claim_party_full_name AS v_lien_client,
	-- *INF*: IIF(ISNULL(v_lien_client),'N/A',v_lien_client)
	IFF(v_lien_client IS NULL,
		'N/A',
		v_lien_client
	) AS lien_client,
	lien_role AS IN_lien_role,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_lien_role))) OR IS_SPACES(LTRIM(RTRIM(IN_lien_role))) OR LENGTH(LTRIM(RTRIM(IN_lien_role)))=0,'N/A',LTRIM(RTRIM(IN_lien_role)))
	IFF(LTRIM(RTRIM(IN_lien_role
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_lien_role
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_lien_role
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_lien_role
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_lien_role
			)
		)
	) AS lien_role,
	lien_amt AS IN_lien_amt,
	-- *INF*: IIF(ISNULL(IN_lien_amt) ,0,IN_lien_amt)
	IFF(IN_lien_amt IS NULL,
		0,
		IN_lien_amt
	) AS lien_amt
	FROM SQ_CLM_LIEN_DETAILS_STAGE
	LEFT JOIN LKP_CLAIM_CASE_AK_ID LKP_CLAIM_CASE_AK_ID_CLAIM_CASE_KEY
	ON LKP_CLAIM_CASE_AK_ID_CLAIM_CASE_KEY.claim_case_key = CLAIM_CASE_KEY

	LEFT JOIN LKP_LIEN_CLIENT_NAME LKP_LIEN_CLIENT_NAME_IN_lien_client_id
	ON LKP_LIEN_CLIENT_NAME_IN_lien_client_id.claim_party_key = IN_lien_client_id

),
LKP_CLAIM_CASE_LIEN_DETAILS AS (
	SELECT
	claim_case_lien_det_id,
	claim_case_lien_det_ak_id,
	claim_case_ak_id,
	lien_client,
	lien_client_role_code,
	lien_amt
	FROM (
		SELECT 
		A.claim_case_lien_det_id as claim_case_lien_det_id, 
		A.claim_case_lien_det_ak_id as claim_case_lien_det_ak_id, 
		A.lien_client as lien_client, 
		A.lien_client_role_code as lien_client_role_code, 
		A.lien_amt as lien_amt, 
		A.claim_case_ak_id as claim_case_ak_id 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case_lien_detail A
		WHERE A.crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id,lien_client,lien_client_role_code ORDER BY claim_case_lien_det_id) = 1
),
EXP_DETECT_CHANGES AS (
	SELECT
	EXP_VALIDATE.claim_case_ak_id,
	EXP_VALIDATE.Out_Claim_Case_key,
	EXP_VALIDATE.lien_client,
	EXP_VALIDATE.lien_role,
	EXP_VALIDATE.lien_amt,
	LKP_CLAIM_CASE_LIEN_DETAILS.claim_case_lien_det_id AS old_claim_case_lien_det_id,
	LKP_CLAIM_CASE_LIEN_DETAILS.claim_case_lien_det_ak_id AS old_claim_case_lien_det_ak_id,
	LKP_CLAIM_CASE_LIEN_DETAILS.lien_client AS old_lien_client,
	LKP_CLAIM_CASE_LIEN_DETAILS.lien_client_role_code AS old_lien_client_role_code,
	LKP_CLAIM_CASE_LIEN_DETAILS.lien_amt AS old_lien_amt,
	-- *INF*: IIF(ISNULL(old_claim_case_lien_det_id),'NEW',
	--      IIF(lien_amt <> old_lien_amt,'UPDATE','NOCHANGE'))
	-- 
	IFF(old_claim_case_lien_det_id IS NULL,
		'NEW',
		IFF(lien_amt <> old_lien_amt,
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
	SYSDATE AS modified_date
	FROM EXP_VALIDATE
	LEFT JOIN LKP_CLAIM_CASE_LIEN_DETAILS
	ON LKP_CLAIM_CASE_LIEN_DETAILS.claim_case_ak_id = EXP_VALIDATE.claim_case_ak_id AND LKP_CLAIM_CASE_LIEN_DETAILS.lien_client = EXP_VALIDATE.lien_client AND LKP_CLAIM_CASE_LIEN_DETAILS.lien_client_role_code = EXP_VALIDATE.lien_role
),
FIL_INSERT AS (
	SELECT
	old_claim_case_lien_det_ak_id, 
	Out_Claim_Case_key, 
	claim_case_ak_id, 
	lien_client, 
	lien_role, 
	lien_amt, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_DETECT_CHANGES
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
SEQ_Claim_Case_Lien_Det_ak_id AS (
	CREATE SEQUENCE SEQ_Claim_Case_Lien_Det_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	old_claim_case_lien_det_ak_id,
	-- *INF*: IIF(changed_flag ='NEW',NEXTVAL,old_claim_case_lien_det_ak_id)
	IFF(changed_flag = 'NEW',
		NEXTVAL,
		old_claim_case_lien_det_ak_id
	) AS claim_case_lien_det_ak_id,
	Out_Claim_Case_key,
	claim_case_ak_id,
	lien_client,
	lien_role,
	lien_amt,
	changed_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	SEQ_Claim_Case_Lien_Det_ak_id.NEXTVAL
	FROM FIL_INSERT
),
claim_case_lien_detail_insert AS (
	INSERT INTO claim_case_lien_detail
	(claim_case_lien_det_ak_id, claim_case_ak_id, claim_case_key, lien_client, lien_client_role_code, lien_amt, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	CLAIM_CASE_LIEN_DET_AK_ID, 
	CLAIM_CASE_AK_ID, 
	Out_Claim_Case_key AS CLAIM_CASE_KEY, 
	LIEN_CLIENT, 
	lien_role AS LIEN_CLIENT_ROLE_CODE, 
	LIEN_AMT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_Determine_AK
),
SQ_claim_case_lien_detail AS (
	SELECT 
	a.claim_case_lien_det_id, 
	a.claim_case_ak_id, 
	a.lien_client, 
	a.lien_client_role_code, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case_lien_detail a
	WHERE 
	a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	EXISTS(SELECT 1 
	                 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case_lien_detail b
	                 WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	                 AND a.claim_case_ak_id = b.claim_case_ak_id
	                 AND a.lien_client = b.lien_client
	                 AND a.lien_client_role_code =b.lien_client_role_code
	 	           GROUP BY b.claim_case_ak_id,b.lien_client,b.lien_client_role_code
			     HAVING COUNT(*) >1) 
	ORDER BY a.claim_case_ak_id, a.lien_client, a.lien_client_role_code,  a.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_case_lien_det_id,
	claim_case_ak_id,
	lien_client,
	lien_client_role_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,claim_case_ak_id=v_prev_row_claim_case_ak_id 
	-- and lien_client = v_prev_row_lien_client 
	-- and lien_client_role_code = v_prev_row_lien_client_role_code ,ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		claim_case_ak_id = v_prev_row_claim_case_ak_id 
		AND lien_client = v_prev_row_lien_client 
		AND lien_client_role_code = v_prev_row_lien_client_role_code, DATEADD(SECOND,- 1,v_prev_row_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_case_ak_id AS v_prev_row_claim_case_ak_id,
	lien_client AS v_prev_row_lien_client,
	lien_client_role_code AS v_prev_row_lien_client_role_code,
	eff_from_date AS v_prev_row_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_claim_case_lien_detail
),
FIL_First_Row_InAKGroup AS (
	SELECT
	claim_case_lien_det_id AS claim_case_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <>eff_to_date
),
UPD_Claim_Case_Lien_Details AS (
	SELECT
	claim_case_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_Row_InAKGroup
),
claim_case_lien_detail_update AS (
	MERGE INTO claim_case_lien_detail AS T
	USING UPD_Claim_Case_Lien_Details AS S
	ON T.claim_case_lien_det_id = S.claim_case_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),