WITH
SQ_claim_party_occurrence AS (
	SELECT 
	CPO.claim_occurrence_ak_id, 
	CPO.claim_party_role_code, 
	CPO.claim_party_ak_id, 
	CPO.eff_from_date, 
	CPO.preferred_contact_method 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO 
	WHERE (created_date >= '@{pipeline().parameters.SELECTION_START_TS}' OR modified_date >= '@{pipeline().parameters.SELECTION_START_TS}')
),
EXP_default AS (
	SELECT
	claim_occurrence_ak_id,
	claim_party_role_code,
	-- *INF*: DECODE(LTRIM(RTRIM(claim_party_role_code)), 'CMT','CLMT',
	--                                                                                                           'DRV','DRVR',
	--                                                                                                           'HS','HOSP',
	--                                                                                                           'PS','PSTH',
	--                                                                                                            'UNL','UNSP',   
	--  				LTRIM(RTRIM(claim_party_role_code)))
	-- 
	-- 
	-- --IIF(LTRIM(RTRIM(claim_party_role_code))='CMT','CLMT',LTRIM(RTRIM(claim_party_role_code)))
	DECODE(LTRIM(RTRIM(claim_party_role_code)),
		'CMT', 'CLMT',
		'DRV', 'DRVR',
		'HS', 'HOSP',
		'PS', 'PSTH',
		'UNL', 'UNSP',
		LTRIM(RTRIM(claim_party_role_code))) AS v_claim_party_role_code,
	v_claim_party_role_code AS claim_party_role_code_out,
	claim_party_ak_id AS edw_claim_party_ak_id,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	preferred_contact_method
	FROM SQ_claim_party_occurrence
),
LKP_sup_claim_party_role_code AS (
	SELECT
	claim_party_role_descript,
	claim_party_role_code
	FROM (
		SELECT 
		A.claim_party_role_descript as claim_party_role_descript, 
		LTRIM(RTRIM(A.claim_party_role_code)) as claim_party_role_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_party_role_code A
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_role_code ORDER BY claim_party_role_descript) = 1
),
EXP_Lookup_Values AS (
	SELECT
	EXP_default.claim_occurrence_ak_id,
	EXP_default.claim_party_role_code_out,
	LKP_sup_claim_party_role_code.claim_party_role_descript,
	-- *INF*: IIF(ISNULL(claim_party_role_descript),'N/A',claim_party_role_descript)
	IFF(claim_party_role_descript IS NULL, 'N/A', claim_party_role_descript) AS claim_party_role_descript_Out,
	EXP_default.edw_claim_party_ak_id,
	EXP_default.crrnt_snpsht_flag,
	EXP_default.audit_id,
	EXP_default.eff_from_date,
	EXP_default.eff_to_date,
	EXP_default.created_date,
	EXP_default.modified_date,
	EXP_default.preferred_contact_method
	FROM EXP_default
	LEFT JOIN LKP_sup_claim_party_role_code
	ON LKP_sup_claim_party_role_code.claim_party_role_code = EXP_default.claim_party_role_code_out
),
LKP_Claim_Party_Role_Dim AS (
	SELECT
	claim_party_role_dim_id,
	preferred_contact_method,
	edw_claim_occurrence_ak_id,
	edw_claim_party_ak_id,
	claim_party_role_code
	FROM (
		SELECT 
		A.claim_party_role_dim_id as claim_party_role_dim_id, 
		A.edw_claim_occurrence_ak_id as edw_claim_occurrence_ak_id, 
		A.edw_claim_party_ak_id as edw_claim_party_ak_id, 
		LTRIM(RTRIM(A.claim_party_role_code)) as claim_party_role_code, 
		A.preferred_contact_method as preferred_contact_method 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_role_dim A
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id,edw_claim_party_ak_id,claim_party_role_code ORDER BY claim_party_role_dim_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Claim_Party_Role_Dim.claim_party_role_dim_id AS LKP_claim_party_role_dim_id,
	LKP_Claim_Party_Role_Dim.preferred_contact_method AS LKP_preferred_contact_method,
	EXP_Lookup_Values.claim_occurrence_ak_id,
	EXP_Lookup_Values.edw_claim_party_ak_id,
	EXP_Lookup_Values.claim_party_role_code_out,
	EXP_Lookup_Values.claim_party_role_descript_Out,
	EXP_Lookup_Values.crrnt_snpsht_flag,
	EXP_Lookup_Values.audit_id,
	EXP_Lookup_Values.eff_from_date,
	EXP_Lookup_Values.eff_to_date,
	EXP_Lookup_Values.created_date,
	EXP_Lookup_Values.modified_date,
	EXP_Lookup_Values.preferred_contact_method,
	-- *INF*: IIF(ISNULL(LKP_claim_party_role_dim_id),
	-- 	'NEW',
	-- 	IIF(ltrim(rtrim(LKP_preferred_contact_method)) <> ltrim(rtrim(preferred_contact_method)), 
	-- 			'UPDATE',
	-- 		'NOCHANGE'))
	IFF(LKP_claim_party_role_dim_id IS NULL, 'NEW', IFF(ltrim(rtrim(LKP_preferred_contact_method)) <> ltrim(rtrim(preferred_contact_method)), 'UPDATE', 'NOCHANGE')) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM EXP_Lookup_Values
	LEFT JOIN LKP_Claim_Party_Role_Dim
	ON LKP_Claim_Party_Role_Dim.edw_claim_occurrence_ak_id = EXP_Lookup_Values.claim_occurrence_ak_id AND LKP_Claim_Party_Role_Dim.edw_claim_party_ak_id = EXP_Lookup_Values.edw_claim_party_ak_id AND LKP_Claim_Party_Role_Dim.claim_party_role_code = EXP_Lookup_Values.claim_party_role_code_out
),
RTR_INSERT_UPDATE AS (
	SELECT
	LKP_claim_party_role_dim_id AS claim_party_role_dim_id,
	claim_occurrence_ak_id AS edw_claim_occurrence_ak_id,
	edw_claim_party_ak_id,
	claim_party_role_code_out AS claim_party_role_code,
	claim_party_role_descript_Out AS claim_party_role_descript,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	preferred_contact_method,
	o_ChangeFlag
	FROM EXP_Detect_Changes
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE o_ChangeFlag='NEW'),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE o_ChangeFlag='UPDATE'),
UPD_Insert AS (
	SELECT
	edw_claim_occurrence_ak_id AS edw_claim_occurrence_ak_id1, 
	edw_claim_party_ak_id, 
	claim_party_role_code AS claim_party_role_code1, 
	claim_party_role_descript AS claim_party_role_descript1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	preferred_contact_method AS preferred_contact_method1
	FROM RTR_INSERT_UPDATE_INSERT
),
claim_party_role_dim_insert AS (
	INSERT INTO claim_party_role_dim
	(edw_claim_occurrence_ak_id, edw_claim_party_ak_id, claim_party_role_code, claim_party_role_code_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, preferred_contact_method)
	SELECT 
	edw_claim_occurrence_ak_id1 AS EDW_CLAIM_OCCURRENCE_AK_ID, 
	EDW_CLAIM_PARTY_AK_ID, 
	claim_party_role_code1 AS CLAIM_PARTY_ROLE_CODE, 
	claim_party_role_descript1 AS CLAIM_PARTY_ROLE_CODE_DESCRIPT, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	preferred_contact_method1 AS PREFERRED_CONTACT_METHOD
	FROM UPD_Insert
),
UPD_update AS (
	SELECT
	claim_party_role_dim_id AS claim_party_role_dim_id2, 
	edw_claim_occurrence_ak_id AS edw_claim_occurrence_ak_id2, 
	edw_claim_party_ak_id, 
	claim_party_role_code AS claim_party_role_code2, 
	claim_party_role_descript AS claim_party_role_descript2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	created_date AS created_date2, 
	modified_date AS modified_date2, 
	preferred_contact_method AS preferred_contact_method3
	FROM RTR_INSERT_UPDATE_UPDATE
),
claim_party_role_dim_update AS (
	MERGE INTO claim_party_role_dim AS T
	USING UPD_update AS S
	ON T.claim_party_role_dim_id = S.claim_party_role_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_claim_occurrence_ak_id = S.edw_claim_occurrence_ak_id2, T.edw_claim_party_ak_id = S.edw_claim_party_ak_id, T.claim_party_role_code = S.claim_party_role_code2, T.claim_party_role_code_descript = S.claim_party_role_descript2, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.modified_date = S.modified_date2, T.preferred_contact_method = S.preferred_contact_method3
),
SQ_claim_party_role_dim AS (
	SELECT A.claim_party_role_dim_id, A.edw_claim_occurrence_ak_id, A.edw_claim_party_ak_id, A.claim_party_role_code, A.eff_from_date, A.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_role_dim A
	WHERE 
	EXISTS
	(
	SELECT  1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_role_dim B
	WHERE CRRNT_SNPSHT_FLAG = 1 AND 
	A.edw_claim_occurrence_ak_id =B.edw_claim_occurrence_ak_id AND
	A.edw_claim_party_ak_id = B.edw_claim_party_ak_id AND
	A.claim_party_role_code = B.claim_party_role_code
	GROUP BY B.edw_claim_occurrence_ak_id, B.edw_claim_party_ak_id, B.claim_party_role_code  
	HAVING COUNT(*) > 1
	)
	ORDER BY  A.edw_claim_occurrence_ak_id, A.edw_claim_party_ak_id, A.claim_party_role_code, A.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_party_role_dim_id,
	edw_claim_occurrence_ak_id,
	edw_claim_party_ak_id,
	claim_party_role_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claim_occurrence_ak_id = v_PREV_ROW_edw_claim_occurrence_ak_id AND 
	--       edw_claim_party_ak_id = v_PREV_ROW_edw_claim_party_ak_id AND
	--        claim_party_role_code = v_PREV_ROW_claim_party_role_code
	--        , ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		edw_claim_occurrence_ak_id = v_PREV_ROW_edw_claim_occurrence_ak_id AND edw_claim_party_ak_id = v_PREV_ROW_edw_claim_party_ak_id AND claim_party_role_code = v_PREV_ROW_claim_party_role_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claim_occurrence_ak_id AS v_PREV_ROW_edw_claim_occurrence_ak_id,
	claim_party_role_code AS v_PREV_ROW_claim_party_role_code,
	edw_claim_party_ak_id AS v_PREV_ROW_edw_claim_party_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_party_role_dim
),
FILTRANS AS (
	SELECT
	claim_party_role_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_EFF_TO_DATE AS (
	SELECT
	claim_party_role_dim_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FILTRANS
),
claim_party_role_dim_expire AS (
	MERGE INTO claim_party_role_dim AS T
	USING UPD_EFF_TO_DATE AS S
	ON T.claim_party_role_dim_id = S.claim_party_role_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),