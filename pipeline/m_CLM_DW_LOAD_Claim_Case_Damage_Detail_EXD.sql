WITH
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
SQ_CLAIM_CASE_STAGE_TABLES AS (
	SELECT 
	ccd.tch_claim_nbr, ccd.tch_client_id, ccd.damage_seq, ccd.damage_cd, ccd.damage_amt, ccd.damage_desc, ccd.damage_high_amt, ccd.damage_type 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.clm_clmt_damages_stage ccd
),
EXP_VALIDATE AS (
	SELECT
	tch_claim_nbr1 AS tch_claim_nbr_ccd,
	tch_client_id1 AS tch_client_id_ccd,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_claim_nbr_ccd))) OR IS_SPACES(LTRIM(RTRIM(tch_claim_nbr_ccd))) OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_ccd)))=0,'N/A',LTRIM(RTRIM(tch_claim_nbr_ccd))) 
	--                                                                                                                
	IFF(LTRIM(RTRIM(tch_claim_nbr_ccd)) IS NULL OR IS_SPACES(LTRIM(RTRIM(tch_claim_nbr_ccd))) OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_ccd))) = 0, 'N/A', LTRIM(RTRIM(tch_claim_nbr_ccd))) AS v_tch_claim_nbr,
	v_tch_claim_nbr AS tch_claim_nbr,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_client_id_ccd))) OR IS_SPACES(LTRIM(RTRIM(tch_client_id_ccd))) OR LENGTH(LTRIM(RTRIM(tch_client_id_ccd)))=0,'N/A',LTRIM(RTRIM(tch_client_id_ccd)))
	--                                                                                                              
	IFF(LTRIM(RTRIM(tch_client_id_ccd)) IS NULL OR IS_SPACES(LTRIM(RTRIM(tch_client_id_ccd))) OR LENGTH(LTRIM(RTRIM(tch_client_id_ccd))) = 0, 'N/A', LTRIM(RTRIM(tch_client_id_ccd))) AS v_tch_client_id,
	v_tch_client_id AS tch_client_id,
	-- *INF*: v_tch_claim_nbr || '//'||v_tch_client_id
	v_tch_claim_nbr || '//' || v_tch_client_id AS v_Claim_Case_Key,
	v_Claim_Case_Key AS out_Claim_Case_Key,
	-- *INF*: :LKP.LKP_CLAIM_CASE_AK_ID(v_Claim_Case_Key)
	LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key.claim_case_ak_id AS v_Claim_case_ak_id,
	-- *INF*: IIF(ISNULL(v_Claim_case_ak_id),-1,v_Claim_case_ak_id)
	-- 
	-- 
	-- ---v_Claim_case_ak_id
	IFF(v_Claim_case_ak_id IS NULL, - 1, v_Claim_case_ak_id) AS Claim_case_ak_id,
	damage_seq AS IN_damage_seq,
	-- *INF*: IIF(ISNULL(IN_damage_seq) ,-1 ,IN_damage_seq)
	IFF(IN_damage_seq IS NULL, - 1, IN_damage_seq) AS damage_seq,
	damage_cd AS IN_damage_cd,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_damage_cd))) OR IS_SPACES(LTRIM(RTRIM(IN_damage_cd))) OR LENGTH(LTRIM(RTRIM(IN_damage_cd)))=0,'N/A' ,LTRIM(RTRIM(IN_damage_cd)))
	IFF(LTRIM(RTRIM(IN_damage_cd)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_damage_cd))) OR LENGTH(LTRIM(RTRIM(IN_damage_cd))) = 0, 'N/A', LTRIM(RTRIM(IN_damage_cd))) AS damage_cd,
	damage_amt AS IN_damage_amt,
	-- *INF*: IIF(ISNULL(IN_damage_amt),0.0 ,IN_damage_amt)
	IFF(IN_damage_amt IS NULL, 0.0, IN_damage_amt) AS damage_amt,
	damage_high_amt AS IN_damage_high_amt,
	-- *INF*: IIF(ISNULL(IN_damage_high_amt) ,0.0 ,IN_damage_high_amt)
	IFF(IN_damage_high_amt IS NULL, 0.0, IN_damage_high_amt) AS damage_high_amt,
	damage_desc AS IN_damage_desc,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_damage_desc))) OR IS_SPACES(LTRIM(RTRIM(IN_damage_desc))) OR LENGTH(LTRIM(RTRIM(IN_damage_desc)))=0,'N/A' ,LTRIM(RTRIM(IN_damage_desc)))
	IFF(LTRIM(RTRIM(IN_damage_desc)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_damage_desc))) OR LENGTH(LTRIM(RTRIM(IN_damage_desc))) = 0, 'N/A', LTRIM(RTRIM(IN_damage_desc))) AS damage_desc,
	damage_type AS IN_damage_type,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_damage_type))) OR IS_SPACES(LTRIM(RTRIM(IN_damage_type))) OR LENGTH(LTRIM(RTRIM(IN_damage_type)))=0,'N/A' ,LTRIM(RTRIM(IN_damage_type)))
	IFF(LTRIM(RTRIM(IN_damage_type)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_damage_type))) OR LENGTH(LTRIM(RTRIM(IN_damage_type))) = 0, 'N/A', LTRIM(RTRIM(IN_damage_type))) AS damage_type
	FROM SQ_CLAIM_CASE_STAGE_TABLES
	LEFT JOIN LKP_CLAIM_CASE_AK_ID LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key
	ON LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key.claim_case_key = v_Claim_Case_Key

),
LKP_CLAIM_CASE_DAM_DETAIL AS (
	SELECT
	claim_case_dam_det_id,
	claim_case_dam_det_ak_id,
	claim_case_ak_id,
	dam_seq_num,
	dam_code,
	dam_low_amt,
	dam_high_amt,
	dam_comment,
	dam_type_code
	FROM (
		SELECT ccd.claim_case_dam_det_id as claim_case_dam_det_id, 
		ccd.claim_case_dam_det_ak_id as claim_case_dam_det_ak_id, 
		ccd.dam_low_amt as dam_low_amt, ccd.dam_high_amt as dam_high_amt, 
		ccd.dam_comment as dam_comment, ccd.dam_type_code as dam_type_code, 
		ccd.claim_case_ak_id as claim_case_ak_id, ccd.dam_seq_num as dam_seq_num, 
		LTRIM(RTRIM(ccd.dam_code)) as dam_code 
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case_damage_detail ccd
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id,dam_seq_num,dam_code ORDER BY claim_case_dam_det_id) = 1
),
EXP_DETECT_CHANGES AS (
	SELECT
	EXP_VALIDATE.Claim_case_ak_id AS claim_case_ak_id,
	EXP_VALIDATE.out_Claim_Case_Key,
	EXP_VALIDATE.damage_seq,
	EXP_VALIDATE.damage_cd,
	EXP_VALIDATE.damage_amt,
	EXP_VALIDATE.damage_high_amt,
	EXP_VALIDATE.damage_desc,
	EXP_VALIDATE.damage_type,
	LKP_CLAIM_CASE_DAM_DETAIL.claim_case_dam_det_id AS old_claim_case_dam_det_id,
	LKP_CLAIM_CASE_DAM_DETAIL.claim_case_dam_det_ak_id AS old_claim_case_dam_det_ak_id,
	LKP_CLAIM_CASE_DAM_DETAIL.dam_seq_num AS old_dam_seq_num,
	LKP_CLAIM_CASE_DAM_DETAIL.dam_code AS old_dam_code,
	LKP_CLAIM_CASE_DAM_DETAIL.dam_low_amt AS old_dam_low_amt,
	LKP_CLAIM_CASE_DAM_DETAIL.dam_high_amt AS old_dam_high_amt,
	LKP_CLAIM_CASE_DAM_DETAIL.dam_comment AS old_dam_comment,
	LKP_CLAIM_CASE_DAM_DETAIL.dam_type_code AS old_dam_type_code,
	-- *INF*: IIF(ISNULL(old_claim_case_dam_det_id),'NEW',
	--      IIF(
	-- 		damage_amt <> old_dam_low_amt OR 
	-- 		damage_high_amt <> old_dam_high_amt OR 
	-- 		LTRIM(RTRIM(damage_desc)) <> LTRIM(RTRIM(old_dam_comment)) OR 
	-- 		LTRIM(RTRIM(damage_type)) <> LTRIM(RTRIM(old_dam_type_code)) 
	-- 		,'UPDATE','NOCHANGE'))
	-- 
	IFF(old_claim_case_dam_det_id IS NULL, 'NEW', IFF(damage_amt <> old_dam_low_amt OR damage_high_amt <> old_dam_high_amt OR LTRIM(RTRIM(damage_desc)) <> LTRIM(RTRIM(old_dam_comment)) OR LTRIM(RTRIM(damage_type)) <> LTRIM(RTRIM(old_dam_type_code)), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_changed_flag='NEW',TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_VALIDATE
	LEFT JOIN LKP_CLAIM_CASE_DAM_DETAIL
	ON LKP_CLAIM_CASE_DAM_DETAIL.claim_case_ak_id = EXP_VALIDATE.Claim_case_ak_id AND LKP_CLAIM_CASE_DAM_DETAIL.dam_seq_num = EXP_VALIDATE.damage_seq AND LKP_CLAIM_CASE_DAM_DETAIL.dam_code = EXP_VALIDATE.damage_cd
),
FIL_INSERT AS (
	SELECT
	old_claim_case_dam_det_ak_id, 
	claim_case_ak_id, 
	out_Claim_Case_Key, 
	damage_seq, 
	damage_cd, 
	damage_amt, 
	damage_high_amt, 
	damage_desc, 
	damage_type, 
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
SEQ_Claim_Case_Dam_Det_ak_id AS (
	CREATE SEQUENCE SEQ_Claim_Case_Dam_Det_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	old_claim_case_dam_det_ak_id,
	-- *INF*: IIF(changed_flag ='NEW',NEXTVAL,old_claim_case_dam_det_ak_id)
	IFF(changed_flag = 'NEW', NEXTVAL, old_claim_case_dam_det_ak_id) AS claim_case_dam_det_ak_id,
	claim_case_ak_id,
	out_Claim_Case_Key,
	damage_seq,
	damage_cd,
	damage_amt,
	damage_high_amt,
	damage_desc,
	damage_type,
	changed_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	SEQ_Claim_Case_Dam_Det_ak_id.NEXTVAL
	FROM FIL_INSERT
),
claim_case_damage_detail_insert AS (
	INSERT INTO claim_case_damage_detail
	(claim_case_dam_det_ak_id, claim_case_ak_id, claim_case_key, dam_seq_num, dam_code, dam_low_amt, dam_high_amt, dam_comment, dam_type_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	CLAIM_CASE_DAM_DET_AK_ID, 
	CLAIM_CASE_AK_ID, 
	out_Claim_Case_Key AS CLAIM_CASE_KEY, 
	damage_seq AS DAM_SEQ_NUM, 
	damage_cd AS DAM_CODE, 
	damage_amt AS DAM_LOW_AMT, 
	damage_high_amt AS DAM_HIGH_AMT, 
	damage_desc AS DAM_COMMENT, 
	damage_type AS DAM_TYPE_CODE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_Determine_AK
),
SQ_claim_case_damage_detail AS (
	SELECT 
	a.claim_case_dam_det_id, 
	a.claim_case_ak_id, 
	a.dam_seq_num, 
	a.dam_code, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case_damage_detail a
	WHERE 
	a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	EXISTS(SELECT 1 
	                 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case_damage_detail b
	                 WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	                 AND a.claim_case_ak_id = b.claim_case_ak_id
	                 AND a.dam_seq_num = b.dam_seq_num
	                 AND a.dam_code =b.dam_code 
	 	           GROUP BY b.claim_case_ak_id,b.dam_seq_num,b.dam_code 
	                 HAVING COUNT(*) >1) 
	ORDER BY a.claim_case_ak_id, a.dam_seq_num, a.dam_code, a.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_case_dam_det_id,
	claim_case_ak_id,
	dam_seq_num,
	dam_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,claim_case_ak_id=v_prev_row_claim_case_ak_id
	--  and dam_seq_num = v_prev_row_dam_seq_num 
	-- and dam_code=v_prev_row_dam_code,ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		claim_case_ak_id = v_prev_row_claim_case_ak_id AND dam_seq_num = v_prev_row_dam_seq_num AND dam_code = v_prev_row_dam_code, ADD_TO_DATE(v_prev_row_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_case_ak_id AS v_prev_row_claim_case_ak_id,
	dam_seq_num AS v_prev_row_dam_seq_num,
	dam_code AS v_prev_row_dam_code,
	eff_from_date AS v_prev_row_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_claim_case_damage_detail
),
FIL_Firstrow_InAk_Group AS (
	SELECT
	claim_case_dam_det_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <>eff_to_date
),
UPD_CLAIM_CASE_DAMAGE_DETAIL AS (
	SELECT
	claim_case_dam_det_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_Firstrow_InAk_Group
),
claim_case_damage_detail_update AS (
	MERGE INTO claim_case_damage_detail AS T
	USING UPD_CLAIM_CASE_DAMAGE_DETAIL AS S
	ON T.claim_case_dam_det_id = S.claim_case_dam_det_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),